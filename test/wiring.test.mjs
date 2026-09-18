// Smoke test the module wiring with a FAKE eufy (no network, no go2rtc). Builds the same `ctx` server.mjs
// does, then drives the device view, the auth state machine, the WS command handler, and the HTTP
// /healthz route — enough to catch a broken import or a mis-referenced ctx field after the split.
import http from "node:http";
import { test } from "node:test";
import assert from "node:assert/strict";

import { loadConfig } from "../src/config.mjs";
import { createState } from "../src/state.mjs";
import { createFaces } from "../src/faces.mjs";
import { createDeviceView } from "../src/device-view.mjs";
import { createWarmup } from "../src/warmup.mjs";
import { createStreamIdle } from "../src/stream-idle.mjs";
import { createWatchdog } from "../src/watchdog.mjs";
import { createAuth } from "../src/auth.mjs";
import { createBoot } from "../src/boot.mjs";
import { createHttpHandler } from "../src/http-routes.mjs";
import { createWsServer } from "../src/ws-server.mjs";

function fakeEufy() {
  const devices = {
    CAM1: {
      describe: () => ({
        sn: "CAM1",
        name: "Cam",
        model: "T8410",
        modelName: "Indoor",
        codec: "camera",
        capabilities: ["camera", "video", "battery"],
      }),
      getProperties: () => ({ battery: { value: 74 }, motion: { value: false } }),
    },
    SENSOR1: {
      describe: () => ({
        sn: "SENSOR1",
        name: "Sensor",
        model: "T8900",
        modelName: "Entry",
        codec: "sensor",
        capabilities: ["contact", "battery"],
      }),
      getProperties: () => ({ contact: { value: true } }),
    },
  };
  return {
    pollIntervalMs: 600000,
    async getDevices() {
      return [{ sn: "CAM1" }, { sn: "SENSOR1" }];
    },
    async getDevice(sn) {
      const d = devices[sn];
      if (!d) throw new Error(`no device ${sn}`);
      return d;
    },
    setPollInterval(ms) {
      this.pollIntervalMs = ms;
    },
    on() {}, // event wiring is a no-op in the smoke harness (completeBoot isn't run)
  };
}

/** The same assembly server.mjs performs, against a fake client + a non-listening http server. */
function buildCtx() {
  const config = loadConfig({ EUFY_EMAIL: "x@y.z", EUFY_PASSWORD: "pw" });
  const state = createState();
  const eufy = fakeEufy();
  const ctx = { ...config, eufy, state };
  Object.assign(
    ctx,
    createFaces(ctx),
    createDeviceView(ctx),
    createWarmup(ctx),
    createStreamIdle(ctx),
    createWatchdog(ctx),
    createAuth(ctx),
    createBoot(ctx),
  );
  const httpServer = http.createServer(createHttpHandler(ctx));
  Object.assign(ctx, createWsServer(ctx, httpServer)); // attaches to httpServer without listening
  return { ctx, state, httpServer };
}

/** Drive one WS command through the real handler with a stub socket; return the frames it sent. */
async function wsCall(ctx, msg) {
  const sent = [];
  const ws = { readyState: 1, OPEN: 1, send: (s) => sent.push(JSON.parse(s)) };
  await ctx.handleMessage(ws, Buffer.from(JSON.stringify(msg)));
  return sent;
}

test("device view: describe shape + camera vs sensor", async () => {
  const { ctx, state, httpServer } = buildCtx();
  const list = await ctx.deviceList();
  const cam = list.find((d) => d.sn === "CAM1");
  const sensor = list.find((d) => d.sn === "SENSOR1");
  assert.equal(list.length, 2);
  assert.equal(cam.stream, "/stream/CAM1");
  assert.equal(cam.streaming, false); // nothing piping
  assert.equal(cam.canReboot, false);
  assert.deepEqual(cam.state, { battery: 74, motion: false });
  assert.equal(sensor.stream, undefined); // not a camera
  // The snapshot sweep reads this instead of paying for its own deviceList() — see state.mjs.
  assert.deepEqual(state.cameraTargets, ["CAM1"]);
  httpServer.close();
});

test("auth state machine: pending → ok → reauth", () => {
  const { ctx, state } = buildCtx();
  assert.deepEqual(ctx.authStatus(), { state: "pending" });
  state.flags.ready = true;
  assert.deepEqual(ctx.authStatus(), { state: "ok" });
  state.flags.sessionLost = true; // post-boot loss outranks ready
  assert.deepEqual(ctx.authStatus(), { state: "reauth" });
});

test("ws: auth.status, unknown cmd, and the auth gate", async () => {
  const { ctx, state, httpServer } = buildCtx();

  assert.deepEqual((await wsCall(ctx, { id: 1, cmd: "auth.status" }))[0], {
    id: 1,
    ok: true,
    auth: { state: "pending" },
  });

  const unknown = await wsCall(ctx, { id: 2, cmd: "nope" });
  assert.equal(unknown[0].ok, false);
  assert.match(unknown[0].error, /unknown cmd/);

  // device command before auth is rejected
  const gated = await wsCall(ctx, { id: 3, cmd: "devices.list" });
  assert.equal(gated[0].ok, false);
  assert.match(gated[0].error, /not authenticated/);

  // once ready, it returns the device list + config.get reads the poll interval
  state.flags.ready = true;
  const listed = await wsCall(ctx, { id: 4, cmd: "devices.list" });
  assert.equal(listed[0].ok, true);
  assert.equal(listed[0].devices.length, 2);
  assert.deepEqual((await wsCall(ctx, { id: 5, cmd: "config.get" }))[0], { id: 5, ok: true, pollMs: 600000 });
  httpServer.close();
});

test("http: /healthz reports ok + auth + empty streaming", async () => {
  const { ctx, httpServer } = buildCtx();
  const handler = createHttpHandler(ctx);
  const req = { url: "/healthz", headers: { host: "localhost" }, on() {} };
  let body;
  const res = {
    writeHead() {},
    end(s) {
      body = s;
    },
  };
  await handler(req, res);
  const out = JSON.parse(body);
  assert.equal(out.ok, true);
  assert.deepEqual(out.auth, { state: "pending" });
  assert.deepEqual(out.streaming, []);
  assert.equal(out.streamIdleMs, 300000);
  httpServer.close();
});
