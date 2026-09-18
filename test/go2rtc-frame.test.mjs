// go2rtc-frame.mjs: piggybacks a fresh /snapshot cache entry on a genuine live viewer's own P2P
// feed by asking go2rtc's own api/frame.jpeg for a frame from the producer it's already running —
// see that file for the full rationale.
import { test } from "node:test";
import assert from "node:assert/strict";

import { createGo2rtcFrame } from "../src/go2rtc-frame.mjs";

function buildFrame(fetchImpl) {
  const logs = [];
  const { grabFrameToCache } = createGo2rtcFrame({ go2rtcApiPort: 1984 }, { eventLog: (m) => logs.push(m), fetchImpl });
  return { grabFrameToCache, logs };
}

test("a successful frame.jpeg response is cached as a snapshot", async () => {
  const jpegBytes = Buffer.from("fake-jpeg-bytes");
  let requestedUrl;
  const { grabFrameToCache, logs } = buildFrame(async (url) => {
    requestedUrl = url;
    return { ok: true, arrayBuffer: async () => jpegBytes };
  });

  const snapshotCache = new Map();
  await grabFrameToCache("CAM1", snapshotCache);

  assert.equal(requestedUrl, "http://127.0.0.1:1984/api/frame.jpeg?src=CAM1");
  assert.deepEqual(snapshotCache.get("CAM1").jpeg, jpegBytes);
  assert.equal(typeof snapshotCache.get("CAM1").capturedAt, "number");
  assert.match(logs[0], /snapshot cache refreshed from live view/);
});

test("a non-ok response is not cached, and logs the status", async () => {
  const { grabFrameToCache, logs } = buildFrame(async () => ({ ok: false, status: 404 }));

  const snapshotCache = new Map();
  await grabFrameToCache("CAM1", snapshotCache);

  assert.equal(snapshotCache.has("CAM1"), false);
  assert.match(logs[0], /failed.*404/);
});

test("a network failure is logged, not thrown", async () => {
  const { grabFrameToCache, logs } = buildFrame(async () => {
    throw new Error("connection refused");
  });

  const snapshotCache = new Map();
  await assert.doesNotReject(grabFrameToCache("CAM1", snapshotCache));
  assert.equal(snapshotCache.has("CAM1"), false);
  assert.match(logs[0], /failed.*connection refused/);
});

test("an empty body is not cached", async () => {
  const { grabFrameToCache } = buildFrame(async () => ({ ok: true, arrayBuffer: async () => Buffer.alloc(0) }));

  const snapshotCache = new Map();
  await grabFrameToCache("CAM1", snapshotCache);
  assert.equal(snapshotCache.has("CAM1"), false);
});
