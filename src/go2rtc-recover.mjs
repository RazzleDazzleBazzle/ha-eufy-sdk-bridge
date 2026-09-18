// Recovers a single go2rtc stream stuck by go2rtc's own exec-producer race — NOT a P2P/bridge
// problem, a go2rtc-internal one (see AlexxIT/go2rtc#163 and #1204): if an RTSP consumer
// disconnects before a slow-to-start exec producer has begun publishing, go2rtc can be left
// thinking a consumer is still attached, and silently stops re-invoking the producer for every
// future consumer of that stream — until something resets its internal bookkeeping for that one
// stream. Still not reliably fixed upstream as of go2rtc 1.9.14 (that version was tried in this
// project and reverted for making producer stability WORSE, not better — see git history).
//
// This is exactly the shape of failure HA's HomeKit component reliably triggers: its own RTSP
// client hard-codes a ~5s read timeout with no override hook (confirmed against HA core), which
// is routinely shorter than this bridge's P2P cold-start (commonly 10-25s, confirmed in real
// logs) — so a camera's FIRST HomeKit view after any idle period almost always has HA give up and
// disconnect while go2rtc's producer is still warming up, landing squarely in the race window.
//
// The fix here is reactive, not preventative: watch go2rtc's own stdout/stderr for the specific
// line it prints when this happens, and immediately reset JUST that one stream via go2rtc's own
// REST API (DELETE then PUT, rebuilding the identical source string from go2rtc-config.mjs) —
// not a restart of the whole go2rtc process (which would drop every camera's stream at once) and
// not keeping every stream permanently warm (a real, ongoing P2P/battery cost for a battery-tier
// camera, for a problem that only bites on the first cold view after a while).
import { go2rtcSourceUrl } from "../go2rtc-config.mjs";

// Confirmed against real go2rtc 1.9.9 logs (see project memory / commit history for the captured
// line): `WRN [rtsp] error="streams: exec: timeout" stream=<sn>`. go2rtc's own log format prefixes
// this with a timestamp and level that vary by build, so the pattern only anchors on the stable
// middle: the literal error string plus the trailing `stream=<sn>` this project's stream ids
// (Eufy serials) never contain whitespace in.
const STUCK_PATTERN = /\[rtsp\] error="streams: exec: timeout" stream=(\S+)/;

// A stuck stream's own consumer disconnect/reconnect cycle can print this line more than once in
// quick succession (see the captured ~one-per-second flapping in the reverted 1.9.14 test) — this
// bounds how often any one stream gets reset, so a genuinely repeating failure doesn't turn into a
// tight reset loop hammering go2rtc's API.
const MIN_RESET_GAP_MS = 5_000;

export function createGo2rtcRecovery(cfg, { eventLog, fetchImpl = fetch } = {}) {
  const lastResetAt = new Map(); // sn -> ms of this module's last reset for it

  async function resetStream(sn) {
    const now = Date.now();
    if (now - (lastResetAt.get(sn) ?? 0) < MIN_RESET_GAP_MS) return;
    lastResetAt.set(sn, now);

    const api = `http://127.0.0.1:${cfg.go2rtcApiPort}/api/streams`;
    try {
      // DELETE clears go2rtc's in-memory Stream object for this id entirely — the surgical version
      // of the reset a full go2rtc restart would otherwise be needed for. PUT then redefines it
      // fresh, identical to what go2rtc-config.mjs already wrote for this camera at boot.
      await fetchImpl(`${api}?src=${encodeURIComponent(sn)}`, { method: "DELETE" });
      await fetchImpl(`${api}?name=${encodeURIComponent(sn)}&src=${encodeURIComponent(go2rtcSourceUrl(cfg, sn))}`, {
        method: "PUT",
      });
      eventLog?.(`go2rtc producer stuck for ${sn} — reset stream`);
    } catch (e) {
      eventLog?.(`go2rtc producer stuck for ${sn} — reset FAILED (${e?.message ?? e})`);
    }
  }

  /** Feed one line of go2rtc's own stdout/stderr through this; reacts only on the stuck pattern. */
  function watchLine(line) {
    const m = STUCK_PATTERN.exec(line);
    if (m) void resetStream(m[1]);
  }

  return { watchLine };
}
