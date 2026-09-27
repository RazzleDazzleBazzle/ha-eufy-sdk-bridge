// go2rtc-config.mjs: writes go2rtc.yaml from the live device list, and exports go2rtcSourceUrl —
// the exact per-camera source string shared with go2rtc-recover.mjs, which has to rebuild it
// identically when resetting a stuck stream. See both files for the full rationale.
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { test } from "node:test";
import assert from "node:assert/strict";

import { writeGo2rtcConfig, go2rtcSourceUrl } from "../go2rtc-config.mjs";

const CAM = { sn: "CAM1", stream: "/stream/CAM1" };
const NOT_A_CAM = { sn: "SENSOR1", stream: undefined };

test("go2rtcSourceUrl builds the ffmpeg source string, referencing the named input template (no inline fps/spaces)", () => {
  const cfg = { selfHost: "127.0.0.1", port: 3000, streamFps: 15 };
  assert.equal(go2rtcSourceUrl(cfg, "CAM1"), "ffmpeg:http://127.0.0.1:3000/stream/CAM1#video=h264#input=bridge_input");
});

test("go2rtcSourceUrl never contains a space — go2rtc's runtime API rejects any source that does", () => {
  const cfg = { selfHost: "127.0.0.1", port: 3000, streamFps: 15, go2rtcCopyAsyncDevices: new Set(["CAM1"]) };
  assert.doesNotMatch(go2rtcSourceUrl(cfg, "CAM1"), /\s/);
  assert.doesNotMatch(go2rtcSourceUrl(cfg, "CAM2"), /\s/);
});

test("go2rtcSourceUrl uses video=copy#async for a serial listed in go2rtcCopyAsyncDevices", () => {
  const cfg = { selfHost: "127.0.0.1", port: 3000, streamFps: 15, go2rtcCopyAsyncDevices: new Set(["CAM1"]) };
  assert.equal(
    go2rtcSourceUrl(cfg, "CAM1"),
    "ffmpeg:http://127.0.0.1:3000/stream/CAM1#video=copy#async#input=bridge_input",
  );
});

test("go2rtcSourceUrl leaves an unlisted serial on the default video=h264, even with the trial set configured", () => {
  const cfg = { selfHost: "127.0.0.1", port: 3000, streamFps: 15, go2rtcCopyAsyncDevices: new Set(["CAM1"]) };
  assert.match(go2rtcSourceUrl(cfg, "CAM2"), /#video=h264#/);
});

test("writeGo2rtcConfig writes the api port from cfg and one stream line per camera, skipping non-cameras", async (t) => {
  const tmpDir = await fs.mkdtemp(path.join(os.tmpdir(), "go2rtc-config-"));
  const go2rtcConfig = path.join(tmpDir, "go2rtc.yaml");
  t.after(() => fs.rm(tmpDir, { recursive: true, force: true }));

  const cfg = { selfHost: "127.0.0.1", port: 3000, streamFps: 15, go2rtcApiPort: 1984, go2rtcConfig };
  const cams = await writeGo2rtcConfig(cfg, [CAM, NOT_A_CAM]);

  assert.deepEqual(cams, ["CAM1"]);
  const yaml = await fs.readFile(go2rtcConfig, "utf8");
  assert.match(yaml, /api:\n {2}listen: ":1984"/);
  assert.match(yaml, new RegExp(`CAM1: "${go2rtcSourceUrl(cfg, "CAM1").replace(/[/*]/g, "\\$&")}"`));
  assert.doesNotMatch(yaml, /SENSOR1/);
});

test("writeGo2rtcConfig honours a custom go2rtcApiPort", async (t) => {
  const tmpDir = await fs.mkdtemp(path.join(os.tmpdir(), "go2rtc-config-"));
  const go2rtcConfig = path.join(tmpDir, "go2rtc.yaml");
  t.after(() => fs.rm(tmpDir, { recursive: true, force: true }));

  const cfg = { selfHost: "127.0.0.1", port: 3000, streamFps: 15, go2rtcApiPort: 1985, go2rtcConfig };
  await writeGo2rtcConfig(cfg, [CAM]);

  const yaml = await fs.readFile(go2rtcConfig, "utf8");
  assert.match(yaml, /api:\n {2}listen: ":1985"/);
});

test("writeGo2rtcConfig writes the named ffmpeg input template with the configured fps", async (t) => {
  const tmpDir = await fs.mkdtemp(path.join(os.tmpdir(), "go2rtc-config-"));
  const go2rtcConfig = path.join(tmpDir, "go2rtc.yaml");
  t.after(() => fs.rm(tmpDir, { recursive: true, force: true }));

  const cfg = { selfHost: "127.0.0.1", port: 3000, streamFps: 20, go2rtcApiPort: 1984, go2rtcConfig };
  await writeGo2rtcConfig(cfg, [CAM]);

  const yaml = await fs.readFile(go2rtcConfig, "utf8");
  assert.match(yaml, /ffmpeg:\n {2}bridge_input: "-r 20 -i \{input\}"/);
});

test("writeGo2rtcConfig's ffmpeg input template falls back to 15fps when streamFps is unset", async (t) => {
  const tmpDir = await fs.mkdtemp(path.join(os.tmpdir(), "go2rtc-config-"));
  const go2rtcConfig = path.join(tmpDir, "go2rtc.yaml");
  t.after(() => fs.rm(tmpDir, { recursive: true, force: true }));

  const cfg = { selfHost: "127.0.0.1", port: 3000, go2rtcApiPort: 1984, go2rtcConfig };
  await writeGo2rtcConfig(cfg, [CAM]);

  const yaml = await fs.readFile(go2rtcConfig, "utf8");
  assert.match(yaml, /bridge_input: "-r 15 -i \{input\}"/);
});
