// live-pull-health.mjs: shared failure tracking for the SHARED eufy client's own live-pull capability,
// fed by both the periodic sweep and a direct/forced /snapshot request — see that file for the full
// rationale (and why /stream, the dedicated per-camera client, is deliberately NOT part of this).
import { test } from "node:test";
import assert from "node:assert/strict";

import { createLivePullHealth } from "../src/live-pull-health.mjs";

function build(overrides = {}) {
  const broadcasts = [];
  const logs = [];
  const resetCalls = [];
  const cfg = { snapshotWarmupAlertThreshold: 3, ...overrides.cfg };
  const eufy = {
    resetStationSession: async (sn) => {
      resetCalls.push(sn);
      return overrides.resetStationSession?.(sn);
    },
  };
  const ctx = { cfg, eufy, broadcast: (evt) => broadcasts.push(evt), eventLog: (m) => logs.push(m) };
  const { recordLiveAttempt } = createLivePullHealth(ctx);
  return { recordLiveAttempt, broadcasts, logs, resetCalls };
}

const settle = () => new Promise((r) => setImmediate(r)); // let the fire-and-forget reset attempt run

test("broadcasts snapshotWarmupDegraded and attempts one session reset only once the failure streak reaches the alert threshold", async () => {
  const { recordLiveAttempt, broadcasts, resetCalls } = build();

  recordLiveAttempt("CAM1", false, new Error("boom"));
  await settle();
  assert.equal(broadcasts.length, 0, "1st failure — below threshold");
  assert.equal(resetCalls.length, 0);

  recordLiveAttempt("CAM1", false, new Error("boom"));
  await settle();
  assert.equal(broadcasts.length, 0, "2nd failure — still below threshold");
  assert.equal(resetCalls.length, 0);

  recordLiveAttempt("CAM1", false, new Error("boom"));
  await settle();
  assert.equal(broadcasts.length, 1, "3rd failure — crosses the threshold");
  assert.deepEqual(broadcasts[0], {
    event: "snapshotWarmupDegraded",
    sn: "CAM1",
    consecutiveFailures: 3,
    error: "boom",
  });
  assert.deepEqual(resetCalls, ["CAM1"], "one reset attempted at the same crossing");

  recordLiveAttempt("CAM1", false, new Error("boom"));
  await settle();
  assert.equal(broadcasts.length, 1, "4th failure — already alerted, does not fire again");
  assert.deepEqual(resetCalls, ["CAM1"], "4th failure — reset already attempted this episode, not repeated");
});

test("a session reset failure is logged, not thrown", async () => {
  const { recordLiveAttempt, logs } = build({
    cfg: { snapshotWarmupAlertThreshold: 1 },
    resetStationSession: async () => {
      throw new Error("reset boom");
    },
  });

  assert.doesNotThrow(() => recordLiveAttempt("CAM1", false, new Error("boom")));
  await settle();
  assert.ok(logs.some((m) => /session reset attempt failed.*reset boom/.test(m)));
});

test("broadcasts snapshotWarmupRecovered when a live pull succeeds after crossing the alert threshold", async () => {
  const { recordLiveAttempt, broadcasts } = build({ cfg: { snapshotWarmupAlertThreshold: 2 } });

  recordLiveAttempt("CAM1", false, new Error("boom"));
  recordLiveAttempt("CAM1", false, new Error("boom"));
  assert.equal(broadcasts.length, 1, "degraded fired at the threshold");

  recordLiveAttempt("CAM1", true);
  assert.equal(broadcasts.length, 2);
  assert.deepEqual(broadcasts[1], { event: "snapshotWarmupRecovered", sn: "CAM1", afterFailures: 2 });
});

test("a success before ever reaching the alert threshold does not broadcast a recovery", () => {
  const { recordLiveAttempt, broadcasts } = build();

  recordLiveAttempt("CAM1", false, new Error("boom")); // 1 failure, below the threshold of 3
  recordLiveAttempt("CAM1", true); // recovers before ever alerting
  assert.equal(broadcasts.length, 0);
});

test("two different serials keep independent streaks", async () => {
  const { recordLiveAttempt, broadcasts } = build({ cfg: { snapshotWarmupAlertThreshold: 2 } });

  recordLiveAttempt("CAM1", false, new Error("boom"));
  recordLiveAttempt("CAM2", false, new Error("boom"));
  recordLiveAttempt("CAM1", false, new Error("boom"));
  await settle();

  assert.equal(broadcasts.length, 1, "only CAM1 crossed its own threshold");
  assert.equal(broadcasts[0].sn, "CAM1");
});
