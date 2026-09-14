// Realtime liveness watchdog. The SDK's poll loop re-arms via `pollOnce().finally(schedulePoll)`, so a
// cloud call that HANGS (half-open socket, no timeout) never settles → the loop stalls forever while the
// WS server stays up serving stale state. `deviceState` fires on every healthy poll (proof-of-life even
// when nothing changed), so its silence is the stall signal; the FCM push channel is tracked separately
// (events ride push, state rides poll). On a stall we re-establish realtime in place, and exit for a
// clean restart (container `restart: unless-stopped`) if that fails.
import { LoginStatus } from "@razzledazzlebazzle/eufy-sdk";

export function createWatchdog(ctx) {
  const { eufy, PUSH_STALL_MS } = ctx;
  const { flags } = ctx.state;

  const bumpActivity = () => {
    flags.lastActivity = Date.now();
  };

  /** No poll heartbeat for max(3 polls, 30 min) ⇒ treat the realtime/poll channel as stalled. */
  function stallThresholdMs() {
    return Math.max(3 * (eufy.pollIntervalMs || 600_000), 30 * 60_000);
  }

  async function watchdogTick() {
    if (!flags.ready || flags.recovering) return;
    const idleMs = Date.now() - flags.lastActivity;
    const pushDeadMs = flags.pushConnected ? 0 : Date.now() - flags.pushSince;
    const pollStalled = idleMs >= stallThresholdMs();
    const pushStalled = pushDeadMs >= PUSH_STALL_MS;
    if (!pollStalled && !pushStalled) return;
    flags.recovering = true;
    const why = pollStalled ? `poll idle ${Math.round(idleMs / 1000)}s` : `push down ${Math.round(pushDeadMs / 1000)}s`;
    console.error(`[bridge] realtime stalled (${why}) — re-establishing`);
    try {
      await eufy.disconnect();
      const result = await eufy.login();
      await ctx.applyLogin(result); // refreshes auth state; completeBoot is a no-op once ready
      if (result.status !== LoginStatus.Ok) {
        console.error(`[bridge] re-login not OK (${result.status}) — exiting for a clean restart`);
        process.exit(1);
      }
      eufy.setPollInterval(eufy.pollIntervalMs); // re-arm the poll loop under the new realtime epoch
      flags.lastActivity = Date.now();
      flags.pushSince = Date.now(); // give push a fresh window to reconnect before flagging it again
      console.log("[bridge] realtime re-established after stall");
    } catch (e) {
      console.error(`[bridge] stall recovery failed (${e?.message ?? e}) — exiting for a clean restart`);
      process.exit(1);
    } finally {
      flags.recovering = false;
    }
  }

  return { bumpActivity, stallThresholdMs, watchdogTick };
}
