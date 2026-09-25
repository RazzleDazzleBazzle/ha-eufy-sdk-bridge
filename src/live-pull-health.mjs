// Shared failure tracking for the SHARED eufy client's own live-pull capability (cam.snapshotLive()),
// fed by every caller that exercises it — the periodic snapshot sweep AND a direct or forced /snapshot
// request — so a real, usage-driven failure (a user's own repeated attempt, not just whatever the
// sweep's own 30-minute probe happened to see) is caught by the same alert + auto-reset machinery,
// not left waiting on the sweep's own schedule to notice. Confirmed on real hardware (2026-09-25):
// a camera can be genuinely broken through direct use for many minutes while the periodic sweep's own
// counter sits at zero, because a coincidental sweep-timed success (or a bridge restart, which wipes
// this same in-memory tracking) reset the streak the sweep alone was keeping.
//
// Deliberately does NOT include /stream (HomeKit live view): that goes through a completely separate,
// dedicated EufyMega client per camera (see streams.mjs) with its own independent session state — a
// failure there says nothing reliable about the SHARED client's own session health, which is what this
// module (and resetStationSession, which acts on the shared client) is about. Confirmed repeatedly this
// week: /stream keeps working fine on a fresh dedicated session while the shared client stays stuck.

export function createLivePullHealth(ctx) {
  const { cfg, eufy } = ctx;
  const consecutiveFailures = new Map(); // sn -> count of consecutive failed SHARED-CLIENT live pulls

  /**
   * Crossing the alert threshold means the shared client's live pull has now failed this many times in
   * a row for this one serial — confirmed on real hardware (2026-09-20) to persist for 4+ hours across
   * an ENTIRE station (every camera behind one HomeBase, not just one), while a fresh dedicated session
   * (the SDK's own live-view path) worked immediately. That's the signature of a stuck shared session,
   * not a busy or genuinely offline one, so eufy-sdk v0.1.14+'s resetStationSession is worth trying
   * here — it's safe even if this diagnosis is wrong: it defers to SessionManager's own "not while
   * retained" rule, so it can't drop a real viewer, and if the station turns out to be genuinely
   * offline the reset is simply a no-op that changes nothing.
   */
  async function tryAutoReset(sn) {
    try {
      await eufy.resetStationSession?.(sn);
      ctx.eventLog?.(`live pull: ${sn} — attempted a session reset after sustained failures`);
    } catch (e) {
      ctx.eventLog?.(`live pull: ${sn} — session reset attempt failed (${e?.message ?? e})`);
    }
  }

  /**
   * A shared-client live pull just succeeded or failed, from WHICHEVER caller made it — the periodic
   * sweep's own attempt, or a direct/forced /snapshot request. Broadcasts once crossing
   * cfg.snapshotWarmupAlertThreshold, so a real, sustained problem is something a user can build an HA
   * automation/notification on instead of only ever finding out by reading bridge logs. Also broadcasts
   * the recovery, so an alert this fired for doesn't stay looking "still broken" forever once it clears.
   * The SAME crossing also triggers one auto-reset attempt (see tryAutoReset) — once per episode, not
   * on every failure past the threshold.
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

  return { recordLiveAttempt };
}
