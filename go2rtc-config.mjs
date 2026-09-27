// Write go2rtc.yaml from the live device list, so a camera appears with nobody editing YAML.
//
// go2rtc (bundled in the image) is the ONLY media-protocol code in this project: it pulls the bridge's
// one HTTP Annex-B feed per camera and turns it into RTSP / WebRTC / MSE / HLS.
//
// The bridge's `/stream/<sn>` feed is raw Annex-B with NO embedded timing (a P2P elementary stream
// carries none) — confirmed locally (ffmpeg 8.1.2, a synthetic bursty raw-H264 HTTP source standing in
// for the real P2P feed, see PR discussion) that go2rtc's plain `#video=copy` (a stream copy, no `-r`
// hint) doesn't just risk corruption, it fails outright: any timestamped muxer (RTSP/RTP included, not
// just the matroska this was verified against) refuses a packet with no PTS/DTS and the stream never
// starts. `-r <fps>` alone — the fix that worked for this project's sibling `eufy-security-ws-RDB` fork
// — did NOT reproduce as sufficient here; only a real decode+encode pass (which derives its own
// timing from decoded frame order, independent of what the input lacks) produced clean, correctly-timed
// output in that same local test. So this uses go2rtc's `video=h264` transcode template, not `copy`.
//
// Trade-off, stated plainly: transcoding costs real per-stream CPU that stream-copy doesn't — exactly
// what the sibling fork's own history found NAS-class hardware struggles with with two concurrent
// streams. This is the safe default to actually get streaming working at all; if CPU load turns out to
// be a real problem on specific hardware, that's the next thing to optimize (test on real hardware
// first, per this project's own practice — don't pre-guess it away).
//
// `-r 15` matches the fps the sibling fork consistently measured across its real Eufy cameras; override
// via BRIDGE_STREAM_FPS if a specific camera model's real rate is confirmed to differ.
//
// The `-r <fps> -i <src>` override lives in a NAMED go2rtc ffmpeg template (INPUT_TEMPLATE_NAME below),
// not inline in each camera's own source string — confirmed the hard way (2026-09-27) that go2rtc's
// runtime `PUT`/`PATCH /api/streams` (used by go2rtc-recover.mjs to rebuild a stream go2rtc's own
// exec-producer bug left stuck) runs the `src` value through a `Validate()` check
// (`internal/streams/handlers.go`) that HARD-REJECTS any source containing a space, even though the
// YAML-config boot path (`NewStream()`, called directly from `Init()`) never runs that check at all. A
// literal `-r 15 -i <url>` inline always contains a space, so a live recovery attempt against this
// go2rtc version (confirmed on 1.9.9) can DELETE a stuck stream but can never successfully re-PUT it —
// it 400s every time, deterministically, not just on the transient producer-race collision
// go2rtc-recover.mjs's retry loop was originally built to survive. This most likely explains real
// stream-recovery failures attributed at the time to that race alone. Routing the fps override through
// a named template (go2rtc's own `internal/ffmpeg/ffmpeg.go`: `inputTemplate()`/`configTemplate()`,
// loaded once from this file's own `ffmpeg:` YAML section, never touched by the runtime API's
// Validate()) keeps every per-camera source string in `streams:` space-free, so both the boot path AND
// go2rtc-recover.mjs's runtime PUT resolve to the exact same template value either way.
//
// The file contains real serials, so it is gitignored and generated at startup.
import { writeFile, mkdir } from "node:fs/promises";
import { dirname } from "node:path";

// A device that carries live video: a describeDevice() summary with a `stream` path (capabilities
// include camera/video). This matches what HA turns into a camera entity — using deviceClass here
// would miss a camera the SDK downgrades to "other" for sitting behind a HomeBase (not direct P2P).
const isCamera = (d) => Boolean(d.stream);

// A go2rtc-native named ffmpeg input template (see the module header) — referenced by name (a single,
// space-free token) from every camera's own source string, so the actual `-r <fps> -i {input}` value
// (which DOES contain spaces) only ever has to satisfy go2rtc's boot-time YAML loader, never its
// space-rejecting runtime API validation.
const INPUT_TEMPLATE_NAME = "bridge_input";

/**
 * The exact `ffmpeg:...#video=h264#input=bridge_input` source string go2rtc uses for one camera's
 * stream. Shared with go2rtc-recover.mjs, which has to rebuild this identically when re-PUTting a
 * stream that go2rtc's own exec-producer race (see that file) left stuck — any drift between the two
 * would mean a "recovered" stream comes back with different transcode params than every other one.
 *
 * A serial listed in cfg.go2rtcCopyAsyncDevices gets `video=copy#async` instead of this project's
 * default `video=h264` full transcode — an EXPERIMENTAL per-device trial of the community fork's
 * PR #64 fix (see config.mjs): a stream-copy re-stamped by go2rtc's own `-use_wallclock_as_timestamps
 * 1 -async 1` (native go2rtc, `internal/ffmpeg/ffmpeg.go`) costs none of the transcode's CPU, IF it
 * turns out to produce clean output on this project's actual camera feeds — not yet confirmed on real
 * hardware here, which is exactly why this is opt-in rather than the new default.
 */
export function go2rtcSourceUrl(cfg, sn) {
  const src = `http://${cfg.selfHost}:${cfg.port}/stream/${sn}`;
  const video = cfg.go2rtcCopyAsyncDevices?.has(sn) ? "copy#async" : "h264";
  return `ffmpeg:${src}#video=${video}#input=${INPUT_TEMPLATE_NAME}`;
}

export async function writeGo2rtcConfig(cfg, devices) {
  const cams = devices.filter(isCamera);
  const fps = cfg.streamFps || 15;
  const lines = [
    "# generated by ha-eufy-sdk-bridge at startup — do not edit (contains real serials)",
    "api:",
    `  listen: ":${cfg.go2rtcApiPort}"`,
    "rtsp:",
    '  listen: ":8554"',
    "webrtc:",
    '  listen: ":8555"',
    // A NAMED ffmpeg input template (see INPUT_TEMPLATE_NAME/go2rtcSourceUrl above for the full
    // reasoning) — every camera's own source string below references this by name only, so the actual
    // space-containing `-r <fps> -i {input}` value never has to pass through go2rtc's runtime API
    // validation, only its (unchecked) YAML config loader.
    "ffmpeg:",
    `  ${INPUT_TEMPLATE_NAME}: "-r ${fps} -i {input}"`,
    // go2rtc's ffmpeg module defaults its OWN subprocess to `-v error`, which swallows exactly the
    // warnings (e.g. "Timestamps are unset in a packet") that would explain a stream failing on real
    // camera footage the way it didn't on the synthetic feed this was verified against locally.
    //
    // Raising ONLY `ffmpeg` here is not enough to actually SEE that output, though — confirmed by
    // reading go2rtc's own source: the process that spawns ffmpeg and decides whether to forward its
    // stderr into go2rtc's visible logs at all is a SEPARATE internal module ("exec"), whose own level
    // this never touched, so ffmpeg could be producing detail that go2rtc was silently discarding.
    // `level` (unset unless GO2RTC_LOG_LEVEL is given, so normal operation keeps go2rtc's own built-in
    // default rather than us silently overriding it) is the global fallback every module without its
    // own override uses, "exec" included — so both need to move together to see anything at all. Set
    // BOTH GO2RTC_FFMPEG_LOG=debug and GO2RTC_LOG_LEVEL=debug (redeploy required — this file is
    // regenerated fresh at every boot) to diagnose a stream that won't play.
    "log:",
    `  ffmpeg: ${cfg.go2rtcFfmpegLog || "error"}`,
    ...(cfg.go2rtcLogLevel ? [`  level: ${cfg.go2rtcLogLevel}`] : []),
    "streams:",
  ];
  for (const d of cams) {
    // A stream id per camera serial; the source is this bridge's own HTTP feed. `input=bridge_input`
    // (see INPUT_TEMPLATE_NAME above) resolves via the `ffmpeg:` section just above to `-r <fps> -i
    // {input}`, so the untimed raw feed gets a declared frame rate before ffmpeg's h264 encoder
    // (video=h264) derives real timestamps from it — WITHOUT this per-camera line itself containing
    // either a literal space (see the module header) or the literal substring "{input}".
    //
    // That second part matters independently: confirmed via go2rtc's source (internal/streams/
    // producer.go) that literal substring, if it appeared in a per-camera stream's OWN top-level source
    // (as it would if this line spelled out `#input=-r <fps> -i {input}` directly instead of naming a
    // template), makes go2rtc register that stream as a "template" producer whose real `url` is only
    // ever filled in by a later SetSource() call — one that never happens for a plain top-level camera
    // stream, so `prod.url` stays permanently empty. That empty url then satisfies `add_consumer.go`'s
    // loop-request guard (`prod.url == consumer.GetSource()`, both empty) and every consumer gets
    // silently skipped in milliseconds — go2rtc never even dials, so nothing reaches this bridge, and
    // ffmpeg is never wrong because it's never run. Confirmed end to end on real hardware: the exact
    // same transcode invoked directly (bypassing go2rtc) plays 300 clean frames with zero errors; only
    // the `{input}`-shaped config line fails. Naming a template here (`{input}` only ever appears in
    // the SEPARATE `ffmpeg:` section's own template definition, never in a `streams:` entry) avoids the
    // literal placeholder in the one place that matters, so this bug doesn't trigger.
    lines.push(`  ${d.sn}: "${go2rtcSourceUrl(cfg, d.sn)}"`);
  }
  const yaml = lines.join("\n") + "\n";
  await mkdir(dirname(cfg.go2rtcConfig), { recursive: true }).catch(() => {});
  await writeFile(cfg.go2rtcConfig, yaml, "utf8");
  return cams.map((d) => d.sn);
}
