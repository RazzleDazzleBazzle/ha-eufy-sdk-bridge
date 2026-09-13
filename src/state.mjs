// The bridge's shared, mutable runtime state — one place instead of a dozen module-level `let`s. Every
// module reads/writes through the object returned here (passed on `ctx`), so a flag set in auth.mjs is
// visible to the watchdog and the WS handler without exporting reassignable bindings across modules.
//
// `createState()` (rather than a bare singleton) lets a test build a fresh, isolated state per case.

/** Build a fresh runtime-state container: the mutable flags plus the live collections. */
export function createState() {
  return {
    // Mutable scalar flags — grouped so cross-module writes stay legible.
    flags: {
      ready: false, // logged in + booted
      sessionLost: false, // cloud token kicked/expired AFTER boot → re-auth needed (ready stays true so
      // completeBoot's one-time wiring is not re-run; authStatus reflects the loss)
      lastLogin: undefined, // the most recent LoginResult (undefined until the first attempt)
      booting: false,
      recovering: false, // a re-auth / stall recovery is in flight — blocks the watchdog racing it
      lastActivity: Date.now(), // ms of the last poll heartbeat / realtime event (liveness clock)
      pushConnected: false,
      pushSince: Date.now(),
      go2rtcProc: undefined,
    },
    // Interval handles, armed once at boot and cleared on shutdown.
    timers: { watchdog: null, streamIdle: null, rtspIdle: null },

    clients: new Set(), // connected WS clients (broadcast targets)
    streaming: new Set(), // sns with a live P2P feed piping right now

    // person_id -> { name, familiar } — the HomeBase edge-AI face roster, built once at startup.
    faceNames: new Map(),

    // ── live-stream idle auto-off bookkeeping ──
    lastDetect: new Map(), // sn -> ms of the most recent detection
    activeStreams: new Map(), // sn -> { feed, startedAt } for feeds currently piping
    idleSuspended: new Set(), // sns torn down for idleness; reopen blocked until motion or consumer-gone
    lastPullAttempt: new Map(), // sn -> ms go2rtc last asked for /stream (even while suspended)
    rtspLastActive: new Map(), // sn -> ms of last detection/stream, for the battery rtspStream auto-off

    // sn -> { feed, drain, timer } — a feed kept briefly open after its consumer disconnected, so a
    // quick reconnect (e.g. a consumer that gives up and retries a few seconds later) reuses the
    // already-open P2P session instead of paying a fresh handshake. See http-routes.mjs.
    pendingTeardown: new Map(),
  };
}
