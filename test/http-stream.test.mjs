// Multi-consumer /stream sharing: a second simultaneous consumer of the same camera (e.g. go2rtc's own
// dashboard pull running alongside a separate direct puller) must join the ONE already-open P2P feed
// instead of racing a second, independent P2P session against the same camera — the exact channel
// contention the SDK's own warm-up retry fix was about, just from two HTTP clients instead of one
// client's internal retries. Uses real PassThrough streams so .pipe()/.unpipe() behave exactly as they
// do in production, with a fake `ctx.streamClientFor` (dependency-injected, same as everything else on
// ctx) standing in for a real EufyMega login.
import { PassThrough } from "node:stream";
import { EventEmitter } from "node:events";
import { test } from "node:test";
import assert from "node:assert/strict";

import { loadConfig } from "../src/config.mjs";
import { createState } from "../src/state.mjs";
import { createHttpHandler } from "../src/http-routes.mjs";

let openReadableCalls = 0;
let lastFeed;

// NOCAM has no camera at all, to exercise the 404 (not 502) path; every other sn gets a fresh
// PassThrough feed, counted so tests can assert only one P2P session opened for N joiners.
async function fakeStreamClientFor(sn) {
  return {
    getDevice: async () => ({
      camera: () =>
        sn === "NOCAM"
          ? undefined
          : {
              openReadable: async () => {
                openReadableCalls++;
                lastFeed = new PassThrough();
                return lastFeed;
              },
            },
    }),
  };
}

function buildCtx(overrides = {}) {
  const config = loadConfig({ EUFY_EMAIL: "x@y.z", EUFY_PASSWORD: "pw", ...overrides });
  const state = createState();
  const ctx = {
    ...config,
    eufy: {},
    state,
    streamClientFor: fakeStreamClientFor,
    eventLog() {},
    broadcast() {},
    authStatus: () => ({ state: "ok" }),
  };
  state.flags.ready = true;
  return ctx;
}

/** A fake req/res pair: req is a real EventEmitter (for req.on("close", ...)), res is a real
 * PassThrough (so feed.pipe(res)/unpipe(res) exercise the actual stream machinery). */
function fakeExchange() {
  const req = new EventEmitter();
  const res = new PassThrough();
  res.writeHead = () => {};
  return { req, res };
}

async function get(handler, sn) {
  const { req, res } = fakeExchange();
  const done = handler({ url: `/stream/${sn}`, headers: { host: "localhost" }, on: (...a) => req.on(...a) }, res);
  return { req, res, done };
}

test.beforeEach(() => {
  openReadableCalls = 0;
  lastFeed = undefined;
});

test("two simultaneous requests for the same camera share one P2P feed", async () => {
  const ctx = buildCtx();
  const handler = createHttpHandler(ctx);

  const a = await get(handler, "CAM1");
  const b = await get(handler, "CAM1");
  await Promise.all([a.done, b.done]);

  assert.equal(openReadableCalls, 1, "only one P2P session opened for two concurrent consumers");
});

test("a second consumer disconnecting leaves the feed running for the first", async () => {
  const ctx = buildCtx({ STREAM_RECONNECT_GRACE_MS: "5000" });
  const handler = createHttpHandler(ctx);

  const a = await get(handler, "CAM1");
  const b = await get(handler, "CAM1");
  await Promise.all([a.done, b.done]);

  b.req.emit("close");
  assert.equal(ctx.state.activeStreams.has("CAM1"), true, "feed still active — the first consumer remains");
  assert.equal(lastFeed.destroyed, false);
});

test("the feed tears down only once the LAST consumer disconnects", async () => {
  const ctx = buildCtx({ STREAM_RECONNECT_GRACE_MS: "0" }); // no grace period → immediate real teardown
  const handler = createHttpHandler(ctx);

  const a = await get(handler, "CAM1");
  const b = await get(handler, "CAM1");
  await Promise.all([a.done, b.done]);

  a.req.emit("close");
  assert.equal(ctx.state.activeStreams.has("CAM1"), true, "still one consumer left");
  assert.equal(lastFeed.destroyed, false);

  b.req.emit("close");
  assert.equal(ctx.state.activeStreams.has("CAM1"), false);
  assert.equal(lastFeed.destroyed, true);
});

test("a camera with no live video is a 404, not a 502", async () => {
  const ctx = buildCtx();
  const handler = createHttpHandler(ctx);
  const { res, done } = await get(handler, "NOCAM");
  let status;
  res.writeHead = (code) => {
    status = code;
  };
  await done;
  assert.equal(status, 404);
});
