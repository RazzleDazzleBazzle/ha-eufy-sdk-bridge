// Periodic snapshot warm-up for the "current still" endpoint (/snapshot/<sn>) — a DIFFERENT concern
// from warmup.mjs's event-driven "Last event" thumbnail, though it deliberately piggybacks on it below.
//
// Right now /snapshot/<sn> pays for a live P2P wake on EVERY request, with no throttling at all —
// fine for an occasional manual check, but Apple Home (and anything else polling a camera tile) decides
// entirely on its own when to ask, on a cadence this bridge has no influence over. A burst of those
// requests today means a burst of live wakes. This sweeps every camera on its own schedule instead and
// caches the result, so a request costs at most one wake per `snapshotWarmMs`, however many callers ask
// in between — the same reason the old eufy_security_guard integration had its own interval+stagger
// snapshot loop, just moved here so every /snapshot consumer (HomeKit, the HA dashboard, anything else)
// benefits uniformly instead of one integration's camera platform doing it alone.
import fs from "node:fs/promises";
import path from "node:path";

export function createSnapshotWarmup(ctx) {
  const { cfg, eufy, eventImageDir } = ctx;
  const { snapshotCache, activeStreams } = ctx.state;

  let sweeping = false; // re-entrancy guard: a slow sweep must not overlap the next interval tick
  const consecutiveFailures = new Map(); // sn -> count of consecutive failed LIVE PULL attempts only

  /**
   * Adopt warmup.mjs's persisted last-event-<sn>.jpg if it's newer than our own last capture.
   *
   * A real detection already produced that picture for free (no P2P cost at all — it rides the push
   * channel), so paying for a live pull just to get another one is wasted battery on a battery camera.
   * File mtime is the freshness signal: the SDK's own `snapshotStored()` returns bytes with no
   * timestamp, but warmup.mjs's `persistIfChanged` overwrites this exact file every time a genuinely
   * newer event image lands, so its mtime already says what we need without any SDK changes.
   */
  async function adoptEventImageIfFresher(sn) {
    const file = path.join(eventImageDir, `last-event-${sn}.jpg`);
    try {
      const stat = await fs.stat(file);
      const cached = snapshotCache.get(sn);
      if (cached && cached.capturedAt >= stat.mtimeMs) return false;
      const jpeg = await fs.readFile(file);
      snapshotCache.set(sn, { jpeg, capturedAt: stat.mtimeMs });
      return true;
    } catch {
      return false; // no event-image file yet for this device
    }
  }

  /**
   * Crossing the alert threshold means every natural retry the sweep itself already made (one per
   * 30-minute cycle) has failed identically — confirmed on real hardware (2026-09-20) to persist for
   * 4+ hours across an ENTIRE station (every camera behind one HomeBase, not just one), while a fresh
   * dedicated session (the SDK's own live-view path) worked immediately. That's the signature of a
   * stuck shared session, not a busy or genuinely offline one, so eufy-sdk v0.1.14+'s
   * resetStationSession is worth trying here — it's safe even if this diagnosis is wrong: it defers to
   * SessionManager's own "not while retained" rule, so it can't drop a real viewer, and if the station
   * turns out to be genuinely offline the reset is simply a no-op that changes nothing.
   */
  async function tryAutoReset(sn) {
    try {
      await eufy.resetStationSession?.(sn);
      ctx.eventLog?.(`snapshot warm-up: ${sn} — attempted a session reset after sustained failures`);
    } catch (e) {
      ctx.eventLog?.(`snapshot warm-up: ${sn} — session reset attempt failed (${e?.message ?? e})`);
    }
  }

  /**
   * A live pull just succeeded or failed — track the CONSECUTIVE streak (piggyback/no-live/already-
   * streaming skips never call this at all, so they neither build nor clear a streak) and broadcast
   * once crossing cfg.snapshotWarmupAlertThreshold, so a real, sustained problem (confirmed on real
   * hardware: 8+ hours straight, one specific camera, nothing else affected) is something a user can
   * build an HA automation/notification on instead of only ever finding out by reading bridge logs.
   * Also broadcasts the recovery, so an alert this fired for doesn't stay looking "still broken"
   * forever once it clears on its own. The SAME crossing also triggers one auto-reset attempt (see
   * tryAutoReset) — once per episode, not on every failure past the threshold.
   */
  function recordLiveAttempt(sn, ok, error) {
    const was = consecutiveFailures.get(sn) ?? 0;
    if (ok) {
      consecutiveFailures.delete(sn);
      if (was >= cfg.snapshotWarmupAlertThreshold)
        ctx.broadcast?.({ event: "snapshotWarmupRecovered", sn, afterFailures: was });
      return;
    }
    const now = was + 1;
    consecutiveFailures.set(sn, now);
    if (now === cfg.snapshotWarmupAlertThreshold) {
      ctx.broadcast?.({
        event: "snapshotWarmupDegraded",
        sn,
        consecutiveFailures: now,
        error: String(error?.message ?? error),
      });
      void tryAutoReset(sn);
    }
  }

  /** Refresh one camera's cached snapshot — piggyback first, a live P2P pull only if that didn't land. */
  async function warmOne(sn) {
    if (await adoptEventImageIfFresher(sn)) return;
    // This device must never pay for a live pull, full stop — the piggyback above is the only source
    // its cache ever gets from this sweep. See snapshotNoLiveDevices in config.mjs.
    if (cfg.snapshotNoLiveDevices?.has(sn)) return;
    // Someone's actively watching this camera right now (our own /stream) — a warm-up pull would
    // contend for the same station's P2P slot for no reason; the live viewer already keeps things fresh.
    if (activeStreams.has(sn)) return;
    try {
      const cam = (await eufy.getDevice(sn)).camera?.();
      if (!cam?.snapshotLive) return;
      const { jpeg } = await cam.snapshotLive();
      if (jpeg?.length) {
        snapshotCache.set(sn, { jpeg, capturedAt: Date.now() });
        recordLiveAttempt(sn, true);
      }
    } catch (e) {
      ctx.eventLog?.(`snapshot warm-up: ${sn} failed (${e?.message ?? e})`);
      recordLiveAttempt(sn, false, e);
    }
  }

  /**
   * One full staggered sweep across every (optionally allow-listed) camera.
   *
   * Reads the camera roster from `ctx.state.cameraTargets` — populated as a side effect of the last
   * full deviceList() ANY caller made (boot, or an HA `devices.list` poll) — rather than calling
   * deviceList() itself. deviceList() pays for one Eufy-cloud HTTP round-trip per device; doing that
   * again here on every sweep tick meant this sweep and HA's own poll could both be mid-fan-out at
   * once, doubling concurrent cloud load right when a coincidental overlap landed. Which devices ARE
   * cameras essentially never changes, so riding whatever the last real fetch already learned is fine.
   */
  async function snapshotWarmupTick() {
    if (!cfg.snapshotWarmMs || sweeping) return;
    sweeping = true;
    try {
      const targets = ctx.state.cameraTargets.filter(
        (sn) => !cfg.snapshotWarmDevices || cfg.snapshotWarmDevices.has(sn),
      );
      for (const sn of targets) {
        await warmOne(sn);
        if (cfg.snapshotWarmStaggerMs) await new Promise((r) => setTimeout(r, cfg.snapshotWarmStaggerMs));
      }
    } finally {
      sweeping = false;
    }
  }

  return { snapshotWarmupTick };
}
