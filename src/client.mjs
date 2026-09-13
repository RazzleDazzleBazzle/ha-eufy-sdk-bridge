// Construct the one EufyMega SDK client the bridge logs in with. Kept tiny and dependency-light so the
// heavier modules depend on the instance via `ctx.eufy`, not on how it was built. Event wiring that
// needs other modules (error → session recovery, push liveness) lives in server.mjs, after ctx is whole.
import { EufyMega, FileSessionStore, ConsoleLogger } from "@mega-yfue/eufy-sdk";

/** Build the SDK client from config. `logger` is attached only under BRIDGE_DEBUG_P2P (raw transport logs). */
export function createEufy({ cfg, DEBUG_P2P }) {
  return new EufyMega({
    email: cfg.email,
    password: cfg.password,
    countryCode: cfg.country,
    store: new FileSessionStore(cfg.session),
    pollMs: cfg.pollMs, // undefined → SDK default; changeable live via config.set
    // Event pre-warm is OFF by default (`[]` = no event opens P2P speculatively) so a battery camera's
    // radio isn't held open ~28s per doorbell/person/pet/package event. BRIDGE_PREWARM=1 → undefined,
    // which lets the SDK use its default high-intent pre-warm events.
    prewarmEvents: cfg.prewarm ? undefined : [],
    // "debug", not "info": the SDK's own per-phase P2P handshake trace (sendLookups/beginCheckCam/
    // onConnected/live-start-ack, everything a slow-connect report needs) is logged at .debug() level
    // (confirmed in the SDK's own core/logger.ts: debug=0 < info=1 in ConsoleLogger's rank order) — so
    // "info" was silently swallowing all of it the whole time this flag has existed. Real transport
    // traffic only, still gated behind BRIDGE_DEBUG_P2P — quiet by default either way.
    logger: DEBUG_P2P ? new ConsoleLogger("debug") : undefined,
  });
}
