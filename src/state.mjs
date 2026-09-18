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
    timers: { watchdog: null, streamIdle: null, rtspIdle: null, snapshotWarm: null },

    clients: new Set(), // connected WS clients (broadcast targets)
    streaming: new Set(), // sns with a live P2P feed piping right now

    // person_id -> { name, familiar } — the HomeBase edge-AI face roster, built once at startup.
    faceNames: new Map(),

    // ── live-stream idle auto-off bookkeeping ──
    lastDetect: new Map(), // sn -> ms of the most recent detection
    // sn -> { feed, startedAt, consumers: Set<ServerResponse> } for feeds currently piping — consumers
    // holds every simultaneously-attached HTTP response sharing this ONE P2P feed (e.g. go2rtc's own
    // pull for the dashboard alongside a second, independent puller), not just the first/only one.
    activeStreams: new Map(),
    idleSuspended: new Set(), // sns torn down for idleness; reopen blocked until motion or consumer-gone
    lastPullAttempt: new Map(), // sn -> ms go2rtc last asked for /stream (even while suspended)
    rtspLastActive: new Map(), // sn -> ms of last detection/stream, for the battery rtspStream auto-off

    // sn -> { feed, drain, timer } — a feed kept briefly open after its LAST consumer disconnected, so a
    // quick reconnect (e.g. a consumer that gives up and retries a few seconds later) reuses the
    // already-open P2P session instead of paying a fresh handshake. See http-routes.mjs.
    pendingTeardown: new Map(),

    // sn -> Promise<feed> for a P2P open currently in flight, so a second /stream request arriving
    // while the first is still opening awaits the SAME open instead of racing a competing P2P session
    // for the same camera. See http-routes.mjs's openFeedFor.
    pendingOpens: new Map(),

    // sn -> { jpeg, capturedAt } — the periodic snapshot warm-up's own cache for /snapshot/<sn>, kept
    // fresh on its own schedule instead of every request paying for a live P2P pull. See
    // snapshot-warmup.mjs. Empty (feature off, or not yet run once) falls back to today's on-demand
    // live-then-stored behaviour.
    snapshotCache: new Map(),

    // Serials with a `/stream` (i.e. cameras), as of the last full deviceList() from ANY caller — boot,
    // an HA `devices.list` poll, or a previous sweep. Which devices are cameras essentially never
    // changes, so the periodic snapshot sweep reads this instead of paying for its own fresh
    // account-wide fetch every tick — see deviceList() in device-view.mjs and snapshotWarmupTick() in
    // snapshot-warmup.mjs. Populated before boot arms the sweep's timer, so never empty when it's read.
    cameraTargets: [],
  };
}
