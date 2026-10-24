// The on-detection "Last event" image refresh (src/warmup.mjs). Focused on refreshLastEventImageFor's
// linked-camera fallback: a detection attributed to a standalone sensor (motion/entry) must still
// advance the CAMERA's own last-event-<sn>.jpg, since the recording — and its crop — lands on the
// camera's device_sn, never the sensor's, even though the push that reported the detection is.
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { EventEmitter } from "node:events";
import { test } from "node:test";
import assert from "node:assert/strict";

import { createWarmup } from "../src/warmup.mjs";

/** A fake P2P session: `queryDatabase` replies on "dbChunk" with the canned history-table JSON;
 * `requestImage` replies on "image" with the matching bytes from `images`. */
class FakeSession extends EventEmitter {
  constructor({ historyJson, images }) {
    super();
    this.isConnected = true;
    this.historyJson = historyJson;
    this.images = images;
  }
  queryDatabase() {
    setImmediate(() => this.emit("dbChunk", { text: this.historyJson }));
  }
  requestImage(filePath) {
    const data = this.images.get(filePath);
    if (data) setImmediate(() => this.emit("image", { file: filePath, data }));
  }
}

function historyRecord(deviceSn, recordId, cropPath) {
  return { device_sn: deviceSn, record_id: recordId, payload: { crop_hb3_path: cropPath } };
}

/** fetchImage only accepts bytes starting with the JPEG magic number (0xFF 0xD8). */
function jpeg(tag) {
  return Buffer.concat([Buffer.from([0xff, 0xd8]), Buffer.from(tag)]);
}

async function buildCtx({ cameraTargets, historyRecords, images }) {
  const tmpDir = await fs.mkdtemp(path.join(os.tmpdir(), "warmup-"));
  const session = new FakeSession({
    historyJson: JSON.stringify({ data: historyRecords }),
    images,
  });
  const state = { cameraTargets };
  const logs = [];
  const ctx = {
    eventImageDir: tmpDir,
    state,
    eventLog: (msg) => logs.push(msg),
    broadcast() {},
    eufy: {
      getDevices: async () => [],
      getP2pSessions: () => new Map([["station1", session]]),
    },
  };
  Object.assign(ctx, createWarmup(ctx));
  return { ctx, tmpDir, logs };
}

test("a sensor's detection advances its linked camera's Last-event image, not the sensor's own", async () => {
  const { ctx, tmpDir } = await buildCtx({
    cameraTargets: ["CAM1"],
    historyRecords: [historyRecord("CAM1", 5, "/evt/cam1.jpg")],
    images: new Map([["/evt/cam1.jpg", jpeg("fresh-camera-crop")]]),
  });

  const changedSn = await ctx.refreshLastEventImageFor("SENSOR1");

  assert.equal(changedSn, "CAM1", "reports the CAMERA that actually changed, not the triggering sensor");
  const written = await fs.readFile(path.join(tmpDir, "last-event-CAM1.jpg"));
  assert.deepEqual(written, jpeg("fresh-camera-crop"));
  await assert.rejects(fs.readFile(path.join(tmpDir, "last-event-SENSOR1.jpg")), "no file for the sensor itself");
});

test("a camera-sn detection whose own crop hasn't landed yet does not touch other cameras on the station", async () => {
  const { ctx, tmpDir } = await buildCtx({
    cameraTargets: ["CAM1", "CAM2"],
    // Only CAM2 has a record on the station this round — CAM1's own crop simply hasn't been written yet.
    historyRecords: [historyRecord("CAM2", 9, "/evt/cam2.jpg")],
    images: new Map([["/evt/cam2.jpg", jpeg("cam2-crop")]]),
  });

  const changedSn = await ctx.refreshLastEventImageFor("CAM1");

  assert.equal(changedSn, false, "CAM1 itself has nothing yet — must not borrow CAM2's unrelated cover");
  await assert.rejects(fs.readFile(path.join(tmpDir, "last-event-CAM2.jpg")), "CAM2 must be left untouched");
});

test("a direct camera-sn hit still works exactly as before", async () => {
  const { ctx, tmpDir } = await buildCtx({
    cameraTargets: ["CAM1"],
    historyRecords: [historyRecord("CAM1", 3, "/evt/cam1.jpg")],
    images: new Map([["/evt/cam1.jpg", jpeg("cam1-own-crop")]]),
  });

  const changedSn = await ctx.refreshLastEventImageFor("CAM1");

  assert.equal(changedSn, "CAM1");
  const written = await fs.readFile(path.join(tmpDir, "last-event-CAM1.jpg"));
  assert.deepEqual(written, jpeg("cam1-own-crop"));
});

test("a sensor's detection with no camera cover on the station at all changes nothing", async () => {
  const { ctx } = await buildCtx({
    cameraTargets: ["CAM1"],
    historyRecords: [historyRecord("SENSOR1", 1, undefined)], // sensor has a record, but no crop (no camera either)
    images: new Map(),
  });

  const changedSn = await ctx.refreshLastEventImageFor("SENSOR1");

  assert.equal(changedSn, false);
});
