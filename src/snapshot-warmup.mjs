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

  /** Refresh one camera's cached snapshot — piggyback first, a live P2P pull only if that didn't land. */
  async function warmOne(sn) {
    if (await adoptEventImageIfFresher(sn)) return;
    // Someone's actively watching this camera right now (our own /stream) — a warm-up pull would
    // contend for the same station's P2P slot for no reason; the live viewer already keeps things fresh.
    if (activeStreams.has(sn)) return;
    try {
      const cam = (await eufy.getDevice(sn)).camera?.();
      if (!cam?.snapshotLive) return;
      const { jpeg } = await cam.snapshotLive();
      if (jpeg?.length) snapshotCache.set(sn, { jpeg, capturedAt: Date.now() });
    } catch (e) {
      ctx.eventLog?.(`snapshot warm-up: ${sn} failed (${e?.message ?? e})`);
    }
  }

  /** One full staggered sweep across every (optionally allow-listed) camera. */
  async function snapshotWarmupTick() {
    if (!cfg.snapshotWarmMs || sweeping) return;
    sweeping = true;
    try {
      let devices;
      try {
        devices = await ctx.deviceList();
      } catch {
        return;
      }
      const targets = devices.filter(
        (d) => d.stream && (!cfg.snapshotWarmDevices || cfg.snapshotWarmDevices.has(d.sn)),
      );
      for (const d of targets) {
        await warmOne(d.sn);
        if (cfg.snapshotWarmStaggerMs) await new Promise((r) => setTimeout(r, cfg.snapshotWarmStaggerMs));
      }
    } finally {
      sweeping = false;
    }
  }

  return { snapshotWarmupTick };
}
