// Environment → the immutable config + constants the whole bridge reads. Side-effect free (no mkdir,
// no process.exit) so it can be imported from tests; server.mjs owns the startup guards.
import path from "node:path";

export const SCHEMA_VERSION = 1; // bump on any breaking protocol change so an old frontend fails loudly

const truthy = (v) => /^(1|true|yes|on)$/i.test(String(v ?? ""));

/** The SDK event names broadcast to every connected WS client. */
export const FORWARDED_EVENTS = [
  "motion",
  "personDetected",
  "strangerDetected",
  "doorbellPress",
  "petDetection",
  "packageDelivered",
  "packageTaken",
  "packageStranded",
  "soundDetected",
  "cryingDetected",
  "vehicleDetected",
  "dogDetected",
  "armingModeChanged",
  "alarm",
  "lockState",
  "contactState",
  "batteryLevel",
  "batteryAlert",
  "ptzNotify",
  "smartLightState",
];

// The "something happened" pushes (not battery/arming/state changes) — these keep a camera's live
// feed warm and reset the battery rtspStream idle clock (see stream-idle.mjs).
export const DETECTION_EVENTS = new Set([
  "motion",
  "personDetected",
  "strangerDetected",
  "petDetection",
  "vehicleDetected",
  "dogDetected",
  "doorbellPress",
  "packageDelivered",
  "packageTaken",
  "packageStranded",
  "soundDetected",
  "cryingDetected",
]);

export const PUSH_STALL_MS = 5 * 60_000; // push down (or never up) this long ⇒ events are dead ⇒ recover
export const SUSPEND_RELEASE_MS = 30_000; // no /stream pull this long while suspended ⇒ nobody's watching

/**
 * Parse the environment into the config + derived constants. `dbg` is a no-op unless BRIDGE_DEBUG is on.
 * BRIDGE_DEBUG=1 logs each WS command + control timing + P2P lifecycle; BRIDGE_DEBUG_P2P=1 additionally
 * routes the SDK's raw per-frame ConsoleLogger (very noisy).
 */
export function loadConfig(env = process.env) {
  const cfg = {
    email: env.EUFY_EMAIL,
    password: env.EUFY_PASSWORD,
    country: env.EUFY_COUNTRY || "GB",
    host: env.BRIDGE_HOST || "0.0.0.0",
    port: Number(env.BRIDGE_PORT || 3000),
    session: env.EUFY_SESSION || "./data/.eufy-session.json",
    go2rtcConfig: env.GO2RTC_CONFIG || "./go2rtc.yaml",
    selfHost: env.BRIDGE_SELF_HOST || "127.0.0.1",
    // Cloud poll interval (ms). Unset → the SDK default (600000 = 10 min). Changeable live via the
    // config.set WS command. 0 disables polling.
    pollMs: env.EUFY_POLL_MS ? Number(env.EUFY_POLL_MS) : undefined,
    // Auto-off a live stream after this many ms with no detection event. A battery camera bleeds power
    // while its P2P live session is up, and go2rtc holds /stream open as long as anything consumes it —
    // so keep the feed only while detections are recent. Default 5 min; 0 disables.
    streamIdleMs: env.STREAM_IDLE_MS != null ? Number(env.STREAM_IDLE_MS) : 300_000,
    // Battery-saver: a BATTERY camera left with the device's native `rtspStream` publish ON encodes
    // continuously and drains, even when nobody consumes it. If a battery device has rtspStream=true and
    // has been idle this long, turn rtspStream OFF on the device. Default 5 min; 0 disables.
    rtspIdleOffMs: env.RTSP_IDLE_OFF_MS != null ? Number(env.RTSP_IDLE_OFF_MS) : 300_000,
    // Event pre-warm: the SDK can speculatively open a camera's P2P session on a high-intent event
    // (doorbell/person/pet/package) so a following live view starts instantly. OFF by default here — it
    // holds a battery camera's radio open for ~28s per event. Set BRIDGE_PREWARM=1 to enable the SDK's
    // default pre-warm events.
    prewarm: truthy(env.BRIDGE_PREWARM),
    // Declared frame rate go2rtc's ffmpeg pull uses to derive real timestamps for a camera's raw,
    // untimed Annex-B feed (see go2rtc-config.mjs) — the P2P wire carries no fps of its own. 15 matches
    // what real Eufy cameras were consistently measured at; override only if a specific model is
    // confirmed to run at a different real rate (a wrong guess here shows up as visibly wrong playback
    // speed, not a silent failure).
    streamFps: env.BRIDGE_STREAM_FPS ? Number(env.BRIDGE_STREAM_FPS) : 15,
    // go2rtc's own ffmpeg-subprocess log level (see go2rtc-config.mjs) — "error" (go2rtc's own default)
    // is normal-operation quiet; set GO2RTC_FFMPEG_LOG=debug to diagnose a stream that won't play.
    go2rtcFfmpegLog: env.GO2RTC_FFMPEG_LOG || "error",
    // go2rtc's GLOBAL log level — the fallback every internal module without its own override uses,
    // including the "exec" module that actually spawns ffmpeg and decides whether to forward its stderr
    // into the visible log at all. Unset by default (go2rtc keeps its own built-in "info" default);
    // GO2RTC_FFMPEG_LOG alone is NOT enough to see ffmpeg's output — set this to "debug" too.
    go2rtcLogLevel: env.GO2RTC_LOG_LEVEL || undefined,
  };

  const DEBUG = truthy(env.BRIDGE_DEBUG);
  const DEBUG_P2P = truthy(env.BRIDGE_DEBUG_P2P);
  const dbg = (...a) => {
    if (DEBUG) console.log("[bridge:dbg]", ...a);
  };

  // Event log — a NARROW, always-on-by-default trace of just the realtime-event path: a push/semantic
  // event arriving, how many frontend (WS) clients it was broadcast to, and each "Last event" image
  // fetch. This is NOT the BRIDGE_DEBUG firehose — it fires only on real events, so it's quiet on an idle
  // system and is what a "why isn't Last event updating" report needs. Set BRIDGE_EVENT_LOG=0 to silence.
  const EVENT_LOG = env.BRIDGE_EVENT_LOG == null ? true : truthy(env.BRIDGE_EVENT_LOG);
  const eventLog = (...a) => {
    if (EVENT_LOG) console.log("[bridge:event]", ...a);
  };

  return {
    cfg,
    SCHEMA_VERSION,
    DEBUG,
    DEBUG_P2P,
    dbg,
    EVENT_LOG,
    eventLog,
    eventImageDir: path.dirname(cfg.session), // last-event thumbnails live beside the session file
    FORWARDED_EVENTS,
    DETECTION_EVENTS,
    PUSH_STALL_MS,
    SUSPEND_RELEASE_MS,
  };
}
