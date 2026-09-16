// /snapshot/<sn>: serves the periodic warm-up cache instantly when populated (see
// src/snapshot-warmup.mjs), falling back to today's on-demand live-then-stored pull when it isn't —
// so a bridge with the feature off (the default) behaves exactly as before.
import { PassThrough } from "node:stream";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { test } from "node:test";
import assert from "node:assert/strict";

import { loadConfig } from "../src/config.mjs";
import { createState } from "../src/state.mjs";
import { createHttpHandler } from "../src/http-routes.mjs";

function buildCtx({ snapshotLive, snapshotStored, overrides = {}, eventImageDir = "/nonexistent" } = {}) {
  const config = loadConfig({ EUFY_EMAIL: "x@y.z", EUFY_PASSWORD: "pw", ...overrides });
  const state = createState();
  const ctx = {
    ...config,
    eventImageDir,
    eufy: {
      getDevice: async () => ({
        camera: () => ({
          snapshotLive: snapshotLive ?? (async () => ({ jpeg: Buffer.from("live") })),
          snapshotStored: snapshotStored,
        }),
      }),
    },
    state,
    eventLog() {},
    broadcast() {},
    authStatus: () => ({ state: "ok" }),
  };
  state.flags.ready = true;
  return { ctx, state };
}

async function get(handler, sn) {
  const res = new PassThrough();
  const chunks = [];
  res.on("data", (c) => chunks.push(c));
  let status;
  let headers;
  res.writeHead = (code, h) => {
    status = code;
    headers = h;
  };
  await handler({ url: `/snapshot/${sn}`, headers: { host: "localhost" }, on() {} }, res);
  return { status, headers, body: () => Buffer.concat(chunks) };
}

test("serves the warm-up cache instantly, never touching the SDK", async () => {
  const { ctx, state } = buildCtx({
    snapshotLive: async () => {
      throw new Error("should not be called — the cache should have served this");
    },
  });
  state.snapshotCache.set("CAM1", { jpeg: Buffer.from("cached"), capturedAt: Date.now() });

  const { status, body } = await get(createHttpHandler(ctx), "CAM1");
  assert.equal(status, 200);
  assert.deepEqual(body(), Buffer.from("cached"));
});

test("falls back to a live pull when the cache is empty (feature off, or not warmed yet)", async () => {
  const { ctx } = buildCtx({ snapshotLive: async () => ({ jpeg: Buffer.from("live") }) });
  const { status, body } = await get(createHttpHandler(ctx), "CAM1");
  assert.equal(status, 200);
  assert.deepEqual(body(), Buffer.from("live"));
});

test("a genuine live win populates the warm cache, so a later loser gets it instead of an old Last-event file", async () => {
  const { ctx, state } = buildCtx({ snapshotLive: async () => ({ jpeg: Buffer.from("live") }) });
  const handler = createHttpHandler(ctx);

  const first = await get(handler, "CAM1");
  assert.equal(first.status, 200);
  assert.deepEqual(state.snapshotCache.get("CAM1")?.jpeg, Buffer.from("live"));

  // A later request for the same camera — even one that would itself fail to go live (e.g. lost the
  // sibling race) — must hit the now-warm cache before ever trying live again.
  const failingCtx = {
    ...ctx,
    eufy: {
      getDevice: async () => ({
        camera: () => ({
          snapshotLive: () => Promise.reject(new Error("should not be called — the cache should have served this")),
        }),
      }),
    },
  };
  const second = await get(createHttpHandler(failingCtx), "CAM1");
  assert.equal(second.status, 200);
  assert.deepEqual(second.body(), Buffer.from("live"));
});

test("does NOT cache the SDK's own retained fallback as if it were a fresh live win", async () => {
  const { ctx, state } = buildCtx({
    snapshotLive: async () => ({ jpeg: Buffer.from("retained-bytes"), retained: true }),
  });
  const { status } = await get(createHttpHandler(ctx), "CAM1");
  assert.equal(status, 200);
  assert.equal(state.snapshotCache.has("CAM1"), false);
});

test("falls back to the stored snapshot when a live pull fails and nothing is cached", async () => {
  const { ctx } = buildCtx({
    snapshotLive: async () => {
      throw new Error("camera unreachable");
    },
    snapshotStored: async () => Buffer.from("stored"),
  });
  const { status, body } = await get(createHttpHandler(ctx), "CAM1");
  assert.equal(status, 200);
  assert.deepEqual(body(), Buffer.from("stored"));
});

test("a SNAPSHOT_NO_LIVE_DEVICES camera never calls snapshotLive, even when it would succeed", async () => {
  const { ctx } = buildCtx({
    snapshotLive: async () => {
      throw new Error("must never be called for a no-live device");
    },
    snapshotStored: async () => Buffer.from("stored"),
    overrides: { SNAPSHOT_NO_LIVE_DEVICES: "CAM1" },
  });
  const { status, body } = await get(createHttpHandler(ctx), "CAM1");
  assert.equal(status, 200);
  assert.deepEqual(body(), Buffer.from("stored"));
});

test("a no-live camera still uses the warm-up cache when the sweep already populated it", async () => {
  const { ctx, state } = buildCtx({
    snapshotStored: async () => {
      throw new Error("must not be reached — the cache should already have served this");
    },
    overrides: { SNAPSHOT_NO_LIVE_DEVICES: "CAM1" },
  });
  state.snapshotCache.set("CAM1", { jpeg: Buffer.from("cached"), capturedAt: Date.now() });
  const { status, body } = await get(createHttpHandler(ctx), "CAM1");
  assert.equal(status, 200);
  assert.deepEqual(body(), Buffer.from("cached"));
});

test("a no-live camera falls back to the persisted Last-event file when nothing else is available", async (t) => {
  const tmpDir = await fs.mkdtemp(path.join(os.tmpdir(), "http-snapshot-"));
  t.after(() => fs.rm(tmpDir, { recursive: true, force: true }));
  await fs.writeFile(path.join(tmpDir, "last-event-CAM1.jpg"), Buffer.from("last-event"));

  const { ctx } = buildCtx({
    snapshotStored: async () => {
      throw new Error("nothing retained");
    },
    overrides: { SNAPSHOT_NO_LIVE_DEVICES: "CAM1" },
    eventImageDir: tmpDir,
  });
  const { status, body } = await get(createHttpHandler(ctx), "CAM1");
  assert.equal(status, 200);
  assert.deepEqual(body(), Buffer.from("last-event"));
});

test("a no-live camera 404s (not 502) when truly nothing is available anywhere", async () => {
  const { ctx } = buildCtx({
    snapshotStored: async () => {
      throw new Error("nothing retained");
    },
    overrides: { SNAPSHOT_NO_LIVE_DEVICES: "CAM1" },
  });
  const { status } = await get(createHttpHandler(ctx), "CAM1");
  assert.equal(status, 404);
});

test("the default (live) path also falls back to the persisted Last-event file when live AND stored both fail", async (t) => {
  // Exactly the StationBusyError case: a sibling camera on the same HomeBase holds the station, the
  // SDK's own retained-still fallback has nothing either, and snapshotStored() rejects — this used to
  // 502 outright even though a perfectly good "Last event" cover was sitting right there on disk.
  const tmpDir = await fs.mkdtemp(path.join(os.tmpdir(), "http-snapshot-"));
  t.after(() => fs.rm(tmpDir, { recursive: true, force: true }));
  await fs.writeFile(path.join(tmpDir, "last-event-CAM1.jpg"), Buffer.from("last-event"));

  const { ctx } = buildCtx({
    snapshotLive: async () => {
      throw new Error("the station is already serving channel 4 to a viewer");
    },
    snapshotStored: async () => {
      throw new Error("No stored snapshot is available");
    },
    eventImageDir: tmpDir,
  });
  const { status, body } = await get(createHttpHandler(ctx), "CAM1");
  assert.equal(status, 200);
  assert.deepEqual(body(), Buffer.from("last-event"));
});

test("the default (live) path 404s (not 502) when live, stored, AND the last-event file are all unavailable", async () => {
  const { ctx } = buildCtx({
    snapshotLive: async () => {
      throw new Error("camera unreachable");
    },
    snapshotStored: async () => {
      throw new Error("nothing retained");
    },
  });
  const { status } = await get(createHttpHandler(ctx), "CAM1");
  assert.equal(status, 404);
});
