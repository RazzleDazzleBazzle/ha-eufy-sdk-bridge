// Periodic /snapshot cache: a staggered sweep across every camera, piggybacking on a fresher
// "Last event" image when one exists, skipping a camera someone's actively streaming, and otherwise
// falling back to a live P2P pull — see src/snapshot-warmup.mjs for the full rationale.
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { test } from "node:test";
import assert from "node:assert/strict";

import { loadConfig } from "../src/config.mjs";
import { createState } from "../src/state.mjs";
import { createSnapshotWarmup } from "../src/snapshot-warmup.mjs";

const CAM1 = { sn: "CAM1", stream: "/stream/CAM1" };
const CAM2 = { sn: "CAM2", stream: "/stream/CAM2" };
const NOT_A_CAM = { sn: "SENSOR1", stream: undefined };

function jpeg(tag) {
  // Content doesn't matter to this module — just something distinguishable per call.
  return Buffer.from(`jpeg:${tag}`);
}

async function buildCtx({ devices = [CAM1, CAM2], overrides = {}, liveJpegFor = (sn) => jpeg(sn) } = {}) {
  const tmpDir = await fs.mkdtemp(path.join(os.tmpdir(), "snapshot-warmup-"));
  const config = loadConfig({
    EUFY_EMAIL: "x@y.z",
    EUFY_PASSWORD: "pw",
    SNAPSHOT_WARM_STAGGER_MS: "0", // no test needs the real delay; staggering itself isn't timing-sensitive here
    ...overrides,
  });
  const state = createState();
  const liveCalls = [];
  const ctx = {
    ...config,
    eventImageDir: tmpDir,
    state,
    eventLog() {},
    deviceList: async () => devices,
    eufy: {
      getDevice: async (sn) => ({
        camera: () => ({
          snapshotLive: async () => {
            liveCalls.push(sn);
            return { jpeg: liveJpegFor(sn) };
          },
        }),
      }),
    },
  };
  Object.assign(ctx, createSnapshotWarmup(ctx));
  return { ctx, state, liveCalls, tmpDir };
}

test("does nothing when the feature is off (no interval configured)", async () => {
  const { ctx, state, liveCalls } = await buildCtx();
  await ctx.snapshotWarmupTick();
  assert.equal(liveCalls.length, 0);
  assert.equal(state.snapshotCache.size, 0);
});

test("warms every camera via a live pull, skipping non-camera devices", async () => {
  const { ctx, state, liveCalls } = await buildCtx({
    devices: [CAM1, CAM2, NOT_A_CAM],
    overrides: { SNAPSHOT_WARM_INTERVAL_MIN: "30" },
  });
  await ctx.snapshotWarmupTick();
  assert.deepEqual(liveCalls.sort(), ["CAM1", "CAM2"]);
  assert.deepEqual(state.snapshotCache.get("CAM1").jpeg, jpeg("CAM1"));
  assert.deepEqual(state.snapshotCache.get("CAM2").jpeg, jpeg("CAM2"));
});

test("honours an explicit device allowlist", async () => {
  const { ctx, liveCalls } = await buildCtx({
    overrides: { SNAPSHOT_WARM_INTERVAL_MIN: "30", SNAPSHOT_WARM_DEVICES: "CAM1" },
  });
  await ctx.snapshotWarmupTick();
  assert.deepEqual(liveCalls, ["CAM1"]);
});

test("skips a camera someone is actively streaming right now", async () => {
  const { ctx, state, liveCalls } = await buildCtx({ overrides: { SNAPSHOT_WARM_INTERVAL_MIN: "30" } });
  state.activeStreams.set("CAM1", { feed: {}, startedAt: Date.now(), consumers: new Set() });
  await ctx.snapshotWarmupTick();
  assert.deepEqual(liveCalls, ["CAM2"]);
  assert.equal(state.snapshotCache.has("CAM1"), false);
});

test("adopts a fresher last-event image instead of paying for a live pull", async (t) => {
  const { ctx, state, liveCalls, tmpDir } = await buildCtx({
    devices: [CAM1],
    overrides: { SNAPSHOT_WARM_INTERVAL_MIN: "30" },
  });
  await fs.writeFile(path.join(tmpDir, "last-event-CAM1.jpg"), jpeg("event"));
  t.after(() => fs.rm(tmpDir, { recursive: true, force: true }));

  await ctx.snapshotWarmupTick();

  assert.equal(liveCalls.length, 0, "no live pull — the event image covered it");
  assert.deepEqual(state.snapshotCache.get("CAM1").jpeg, jpeg("event"));
});

test("does NOT re-adopt the same last-event image on every sweep, and re-warms live once it's stale", async (t) => {
  const { ctx, state, liveCalls, tmpDir } = await buildCtx({
    devices: [CAM1],
    overrides: { SNAPSHOT_WARM_INTERVAL_MIN: "30" },
  });
  const file = path.join(tmpDir, "last-event-CAM1.jpg");
  t.after(() => fs.rm(tmpDir, { recursive: true, force: true }));

  // Cache already holds something NEWER than the (stale) event file — must ignore it and fall through
  // to a live pull, not adopt something older than what it already has.
  await fs.writeFile(file, jpeg("stale-event"));
  const staleStat = await fs.stat(file);
  state.snapshotCache.set("CAM1", { jpeg: jpeg("already-fresher"), capturedAt: staleStat.mtimeMs + 10_000 });

  await ctx.snapshotWarmupTick();

  assert.deepEqual(liveCalls, ["CAM1"], "the stale event file was ignored, so it fell through to a live pull");
  assert.deepEqual(state.snapshotCache.get("CAM1").jpeg, jpeg("CAM1"), "live result wins over the stale file");
});
