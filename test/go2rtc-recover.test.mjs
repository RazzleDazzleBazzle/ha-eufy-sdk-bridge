// go2rtc-recover.mjs: watches go2rtc's own log lines for its documented exec-producer stuck state
// (AlexxIT/go2rtc#163 / #1204) and resets just the affected stream via go2rtc's REST API — see that
// file for the full rationale.
import { test } from "node:test";
import assert from "node:assert/strict";

import { createGo2rtcRecovery } from "../src/go2rtc-recover.mjs";

const STUCK_LINE = '00:33:27.790 WRN [rtsp] error="streams: exec: timeout" stream=T8160P1122453F5B';

const noDelay = async () => {};

function buildRecovery(overrides = {}) {
  const calls = [];
  const fetchImpl = async (url, init) => {
    calls.push({ url, method: init.method });
    return { ok: true };
  };
  const cfg = { go2rtcApiPort: 1984, selfHost: "127.0.0.1", port: 3000, streamFps: 15, ...overrides };
  const logs = [];
  const recovery = createGo2rtcRecovery(cfg, { eventLog: (m) => logs.push(m), fetchImpl, delay: noDelay });
  return { recovery, calls, logs };
}

test("a stuck-producer line triggers a DELETE then a PUT for that exact stream", async () => {
  const { recovery, calls } = buildRecovery();
  recovery.watchLine(STUCK_LINE);
  await new Promise((r) => setImmediate(r)); // let the fire-and-forget async work run

  assert.equal(calls.length, 2);
  assert.equal(calls[0].method, "DELETE");
  assert.match(calls[0].url, /\/api\/streams\?src=T8160P1122453F5B$/);
  assert.equal(calls[1].method, "PUT");
  assert.match(calls[1].url, /\/api\/streams\?name=T8160P1122453F5B&src=/);
  assert.match(decodeURIComponent(calls[1].url), /ffmpeg:http:\/\/127\.0\.0\.1:3000\/stream\/T8160P1122453F5B/);
});

test("an unrelated log line does nothing", async () => {
  const { recovery, calls } = buildRecovery();
  recovery.watchLine("00:33:27.790 INF [rtsp] listen addr=:8554");
  await new Promise((r) => setImmediate(r));
  assert.equal(calls.length, 0);
});

test("a second stuck line for the same stream within the cooldown does not reset again", async () => {
  const { recovery, calls } = buildRecovery();
  recovery.watchLine(STUCK_LINE);
  recovery.watchLine(STUCK_LINE);
  await new Promise((r) => setImmediate(r));
  assert.equal(calls.length, 2, "only the first line's DELETE+PUT pair, the second was within the cooldown");
});

test("a stuck line for a DIFFERENT stream resets independently of another stream's cooldown", async () => {
  const { recovery, calls } = buildRecovery();
  recovery.watchLine(STUCK_LINE);
  recovery.watchLine('00:33:28.000 WRN [rtsp] error="streams: exec: timeout" stream=T8160P1122453F48');
  await new Promise((r) => setImmediate(r));

  // Both streams reset (a DELETE + a PUT each) — concurrently, so the two streams' calls interleave
  // rather than one finishing before the other starts; check per-stream rather than by fixed index.
  assert.equal(calls.length, 4);
  const forF48 = calls.filter((c) => c.url.includes("T8160P1122453F48"));
  assert.equal(forF48.length, 2);
  assert.deepEqual(forF48.map((c) => c.method).sort(), ["DELETE", "PUT"]);
});

test("a network-level fetch failure is logged, not thrown", async () => {
  const cfg = { go2rtcApiPort: 1984, selfHost: "127.0.0.1", port: 3000, streamFps: 15 };
  const logs = [];
  const recovery = createGo2rtcRecovery(cfg, {
    eventLog: (m) => logs.push(m),
    fetchImpl: async () => {
      throw new Error("connection refused");
    },
    delay: noDelay,
  });
  recovery.watchLine(STUCK_LINE);
  await new Promise((r) => setImmediate(r));
  assert.equal(logs.length, 1);
  assert.match(logs[0], /reset FAILED.*connection refused/);
});

test("an HTTP error response (not just a network failure) is treated as a failure and logged with detail", async () => {
  // The actual bug this guards against: go2rtc's own PUT/DELETE handlers answer a non-2xx status on
  // failure rather than dropping the connection — `fetch` does NOT reject on that by itself, so a
  // naive `await fetchImpl(...)` with no status check would silently treat this as success.
  const cfg = { go2rtcApiPort: 1984, selfHost: "127.0.0.1", port: 3000, streamFps: 15 };
  const logs = [];
  const recovery = createGo2rtcRecovery(cfg, {
    eventLog: (m) => logs.push(m),
    fetchImpl: async (url, init) =>
      init.method === "PUT" ? { ok: false, status: 400, text: async () => "already exists" } : { ok: true },
    delay: noDelay,
  });
  recovery.watchLine(STUCK_LINE);
  await new Promise((r) => setImmediate(r));
  assert.equal(logs.length, 1);
  assert.match(logs[0], /reset FAILED/);
  assert.match(logs[0], /400/);
  assert.match(logs[0], /already exists/);
});
