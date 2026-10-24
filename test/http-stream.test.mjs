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
  const fakeReq = {
    url: `/stream/${sn}`,
    headers: { host: "localhost" },
    on: (...a) => req.on(...a),
    once: (...a) => req.once(...a),
    removeListener: (...a) => req.removeListener(...a),
  };
  const done = handler(fakeReq, res);
  return { req, res, done };
}

test.beforeEach(() => {
  openReadableCalls = 0;
  lastFeed = undefined;
});

/** Poll until `cond()` is truthy — for a condition reached a few await-hops inside the handler,
 * past the point `get()`'s own await already waits for (its synchronous prefix only). */
async function until(cond) {
  for (let i = 0; i < 50 && !cond(); i++) await new Promise((r) => setImmediate(r));
}

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

test("the first real video byte piggybacks a snapshot-cache refresh from go2rtc's own frame.jpeg", async () => {
  const frameCalls = [];
  const ctx = buildCtx();
  ctx.fetchImpl = async (url) => {
    frameCalls.push(url);
    return { ok: true, arrayBuffer: async () => Buffer.from("jpeg-bytes") };
  };
  const handler = createHttpHandler(ctx);

  const { done } = await get(handler, "CAM1");
  await done;
  lastFeed.emit("data", Buffer.from("h264-bytes"));
  await new Promise((r) => setImmediate(r)); // let the fire-and-forget grab run

  assert.equal(frameCalls.length, 1);
  assert.match(frameCalls[0], /\/api\/frame\.jpeg\?src=CAM1$/);
  assert.deepEqual(ctx.state.snapshotCache.get("CAM1").jpeg, Buffer.from("jpeg-bytes"));
});

test("a feed that fails mid-stream aborts the response instead of leaving it open", async () => {
  const ctx = buildCtx();
  const handler = createHttpHandler(ctx);

  const { res, done } = await get(handler, "CAM1");
  await done;
  assert.equal(res.destroyed, false);

  // pipe() only ends a destination on the SOURCE's clean 'end' — a feed that dies abnormally (P2P
  // drop, warm-up timeout) must not leave the response sitting open on a silent socket.
  lastFeed.destroy(new Error("P2P session lost"));
  await new Promise((r) => setImmediate(r));

  assert.equal(res.destroyed, true, "the response must not be left open on a silent socket");
  assert.equal(ctx.state.activeStreams.has("CAM1"), false);
  assert.equal(ctx.state.streaming.has("CAM1"), false);
});

test("a feed shared by two consumers that fails aborts both responses", async () => {
  const ctx = buildCtx();
  const handler = createHttpHandler(ctx);

  const a = await get(handler, "CAM1");
  const b = await get(handler, "CAM1");
  await Promise.all([a.done, b.done]);

  lastFeed.destroy(new Error("P2P session lost"));
  await new Promise((r) => setImmediate(r));

  assert.equal(a.res.destroyed, true, "consumer A's response must be aborted too");
  assert.equal(b.res.destroyed, true, "consumer B's response must be aborted too");
});

test("a viewer that leaves while the feed is still opening is not registered as a phantom consumer", async () => {
  let releaseOpen;
  // A slow-resolving openReadable() holds the handler inside its "P2P handshake" await long enough
  // for the test to disconnect from underneath it.
  async function slowStreamClientFor() {
    return {
      getDevice: async () => ({
        camera: () => ({
          openReadable: async () => {
            openReadableCalls++;
            lastFeed = new PassThrough();
            await new Promise((r) => (releaseOpen = r));
            return lastFeed;
          },
        }),
      }),
    };
  }
  const ctx = buildCtx({ STREAM_RECONNECT_GRACE_MS: "0" });
  ctx.streamClientFor = slowStreamClientFor;
  const handler = createHttpHandler(ctx);

  const { req, done } = await get(handler, "CAM1");
  await until(() => openReadableCalls === 1); // the handler is now mid-"handshake"
  req.emit("close"); // the viewer gives up (e.g. HA's 5s RTSP read timeout) mid-handshake
  releaseOpen(); // the P2P "handshake" finally completes, after the viewer already left
  await done;

  assert.equal(ctx.state.streaming.has("CAM1"), false, "never reported as streaming for a gone viewer");
  assert.equal(ctx.state.activeStreams.has("CAM1"), false);
  assert.equal(lastFeed.destroyed, true, "the feed opened for a departed viewer is torn down, not left running");
});

test("a viewer that leaves mid-open does not steal the feed from another consumer still watching", async () => {
  let releaseOpen;
  async function slowStreamClientFor() {
    return {
      getDevice: async () => ({
        camera: () => ({
          openReadable: async () => {
            openReadableCalls++;
            lastFeed = new PassThrough();
            await new Promise((r) => (releaseOpen = r));
            return lastFeed;
          },
        }),
      }),
    };
  }
  const ctx = buildCtx();
  ctx.streamClientFor = slowStreamClientFor;
  const handler = createHttpHandler(ctx);

  const a = await get(handler, "CAM1"); // triggers the (slow) open
  await until(() => openReadableCalls === 1);
  a.req.emit("close"); // A gives up before the handshake finishes
  releaseOpen();
  await a.done;
  const b = await get(handler, "CAM1"); // B joins once the feed is actually open
  await b.done;

  assert.equal(ctx.state.streaming.has("CAM1"), true, "B is genuinely watching");
  assert.equal(lastFeed.destroyed, false, "A's early exit must not tear down B's feed");
});

test("a second data event on the same feed does not re-trigger the snapshot grab", async () => {
  const frameCalls = [];
  const ctx = buildCtx();
  ctx.fetchImpl = async (url) => {
    frameCalls.push(url);
    return { ok: true, arrayBuffer: async () => Buffer.from("jpeg-bytes") };
  };
  const handler = createHttpHandler(ctx);

  const { done } = await get(handler, "CAM1");
  await done;
  lastFeed.emit("data", Buffer.from("h264-bytes-1"));
  lastFeed.emit("data", Buffer.from("h264-bytes-2"));
  await new Promise((r) => setImmediate(r));

  assert.equal(frameCalls.length, 1, "feed.once — only the first data event of this feed's lifetime grabs a frame");
});
