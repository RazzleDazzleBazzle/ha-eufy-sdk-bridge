// HTTP surface: live video (go2rtc pulls /stream/<sn>), a snapshot still, the persisted last-event
// thumbnail, and /healthz. Video is deliberately OFF the WS — connecting to /stream is what opens the
// camera, disconnecting is what stops it, so there's no "is it streaming" flag to drift. Returns the
// request handler; server.mjs wraps it in http.createServer.
import fs from "node:fs";
import path from "node:path";
import { createGo2rtcFrame } from "./go2rtc-frame.mjs";

function json(res, code, body) {
  const s = JSON.stringify(body);
  res.writeHead(code, { "content-type": "application/json", "content-length": Buffer.byteLength(s) });
  res.end(s);
}

/** Marks the one "no live video on this device" case as a 404, not the generic 502 every other open failure gets. */
class NoLiveVideoError extends Error {}

export function createHttpHandler(ctx) {
  const { cfg, eufy, SCHEMA_VERSION, eventImageDir, streamClientFor } = ctx;
  const { flags } = ctx.state;
  const {
    streaming,
    idleSuspended,
    activeStreams,
    lastPullAttempt,
    rtspLastActive,
    pendingTeardown,
    pendingOpens,
    snapshotCache,
  } = ctx.state;
  const { grabFrameToCache } = createGo2rtcFrame(cfg, { eventLog: ctx.eventLog, fetchImpl: ctx.fetchImpl });
  // Shared with the periodic sweep (see snapshot-warmup.mjs) — a real, usage-driven live-pull failure
  // (or success) here counts toward the SAME streak the sweep's own attempts do. See live-pull-health.mjs.
  const { recordLiveAttempt } = ctx;

  // Actually tear a feed down: stop the P2P pull for real and, if it had been reported as streaming,
  // broadcast the "off" edge. Idempotent — safe to call from either lifecycle path (a consumer that
  // never reconnected within its grace window, or the feed itself dying on its own).
  function reallyDestroy(sn, feed) {
    const pending = pendingTeardown.get(sn);
    if (pending) clearTimeout(pending.timer);
    pendingTeardown.delete(sn);
    activeStreams.delete(sn);
    feed.destroy();
    if (streaming.delete(sn)) ctx.broadcast({ event: "streamState", deviceSn: sn, active: false });
  }

  // The LAST consumer went away, but the P2P feed itself is still healthy — keep it open and drained
  // (so the source sees no backpressure) for cfg.streamReconnectGraceMs, in case a fresh request for
  // the same sn arrives shortly (Home Assistant's own RTSP client, in particular, hard-codes a 5s read
  // timeout with no override hook — a camera whose P2P handshake is slower than that always fails HA's
  // FIRST attempt, but HA auto-retries ~10s later; reusing an already-flowing feed on that retry
  // sidesteps a second full handshake, landing well inside HA's 5s window instead of racing it again
  // from cold). Only called once `entry.consumers` is empty — see attachConsumer.
  function scheduleTeardown(sn, feed) {
    activeStreams.delete(sn);
    if (!cfg.streamReconnectGraceMs) {
      reallyDestroy(sn, feed);
      return;
    }
    const drain = () => {};
    feed.on("data", drain);
    const timer = setTimeout(() => {
      const p = pendingTeardown.get(sn);
      if (p) reallyDestroy(sn, p.feed);
    }, cfg.streamReconnectGraceMs);
    pendingTeardown.set(sn, { feed, drain, timer });
    ctx.eventLog(
      `/stream ${sn} → consumer gone, keeping P2P warm ${cfg.streamReconnectGraceMs}ms for a possible reconnect`,
    );
  }

  // Open (or join) the one P2P feed for `sn`, so a second simultaneous consumer (e.g. go2rtc's own
  // dashboard pull running alongside a separate direct puller, such as HomeKit's ffmpeg bypassing
  // go2rtc entirely) shares it instead of opening a competing P2P session against the same camera —
  // exactly the channel contention the SDK's own warm-up retry fix was about, just from two independent
  // HTTP clients instead of one client's internal retries.
  //
  // `pendingOpens` closes the race a plain `activeStreams.get(sn)` check would miss: two requests
  // arriving while the first is still mid-handshake would otherwise both see no active feed yet and
  // each start their own P2P session. A request that arrives once an open is already in flight awaits
  // that SAME promise instead.
  async function openFeedFor(sn) {
    const inFlight = pendingOpens.get(sn);
    if (inFlight) return inFlight;
    const active = activeStreams.get(sn);
    if (active) return active.feed;

    const opening = (async () => {
      const t0 = Date.now();
      ctx.eventLog(`/stream ${sn} → request received`);
      const client = await streamClientFor(sn, cfg); // its OWN P2P session — see streams.mjs
      ctx.eventLog(`/stream ${sn} → stream client ready (${Date.now() - t0}ms)`);
      const cam = (await client.getDevice(sn)).camera?.();
      if (!cam?.openReadable) throw new NoLiveVideoError("no live video on this device");
      const feed = await cam.openReadable(); // node Readable of Annex-B
      ctx.eventLog(`/stream ${sn} → P2P feed open (${Date.now() - t0}ms)`);
      // "Feed open" above is just the Readable object existing — openReadable() doesn't wait for a
      // frame. This is the timestamp that actually matters for a "why does Home take 20s" investigation:
      // the first real Annex-B byte the P2P layer delivers, which is also the earliest moment go2rtc (and
      // everything downstream of it — HAFFmpeg, then Home) could possibly have anything to work with.
      feed.once("data", () => {
        ctx.eventLog(`/stream ${sn} → first video byte (${Date.now() - t0}ms)`);
        // Free side effect of a genuine live viewer: go2rtc is already decoding this same feed for
        // the live view itself, so grabbing a frame from it costs nothing extra — see go2rtc-frame.mjs.
        void grabFrameToCache(sn, snapshotCache);
      });
      // The feed's own lifecycle → real teardown, attached once for as long as this feed object lives
      // (a reconnect within the grace period, or a second consumer joining it, reuses the SAME feed,
      // so this must not be re-attached each time — see attachConsumer's own comment).
      feed.on("error", () => reallyDestroy(sn, feed));
      feed.on("close", () => reallyDestroy(sn, feed));
      return feed;
    })();
    pendingOpens.set(sn, opening);
    try {
      return await opening;
    } finally {
      pendingOpens.delete(sn);
    }
  }

  // Wire one feed (freshly opened, reused from pendingTeardown, or already actively serving another
  // consumer) to the current request/response. Node's Readable.pipe() natively fans one source out to
  // several destinations, so joining an already-piping feed is just another `.pipe()` call — each
  // consumer's own `close` only unpipes ITSELF and leaves the others (and the feed) alone; the feed is
  // only scheduled for teardown once the LAST consumer has left.
  function attachConsumer(sn, feed, req, res) {
    let entry = activeStreams.get(sn);
    if (!entry) {
      entry = { feed, startedAt: Date.now(), consumers: new Set() };
      activeStreams.set(sn, entry);
    }
    entry.consumers.add(res);
    if (!streaming.has(sn)) ctx.broadcast({ event: "streamState", deviceSn: sn, active: true });
    streaming.add(sn);
    rtspLastActive.set(sn, Date.now()); // a live stream counts as activity for the rtspStream auto-off
    res.writeHead(200, { "content-type": "video/H264", "cache-control": "no-cache" });
    feed.pipe(res);
    req.on("close", () => {
      feed.unpipe(res);
      entry.consumers.delete(res);
      if (entry.consumers.size === 0) scheduleTeardown(sn, feed);
    });
  }

  return async function handleHttp(req, res) {
    const url = new URL(req.url, `http://${req.headers.host}`);
    const [, kind, sn] = url.pathname.split("/");

    if (url.pathname === "/healthz") {
      const idleSec = Math.round((Date.now() - flags.lastActivity) / 1000);
      return json(res, 200, {
        ok: true,
        schemaVersion: SCHEMA_VERSION,
        auth: ctx.authStatus(),
        sessionLost: flags.sessionLost, // cloud token kicked/expired since boot → re-auth in progress/needed
        streaming: [...streaming],
        idleSuspended: [...idleSuspended], // cameras auto-off for no recent detection (awaiting next one)
        streamIdleMs: cfg.streamIdleMs, // 0 = idle auto-off disabled
        lastActivitySec: idleSec, // seconds since the last poll heartbeat / realtime event
        stalled: flags.ready && idleSec * 1000 >= ctx.stallThresholdMs(),
        pushConnected: flags.pushConnected, // FCM push channel — events (motion/doorbell/…) ride this
        pushIdleSec: flags.pushConnected ? 0 : Math.round((Date.now() - flags.pushSince) / 1000),
      });
    }
    if (!flags.ready) return json(res, 503, { error: "not authenticated", auth: ctx.authStatus() });

    // A current still: a fresh live burst, falling back to the retained push thumbnail.
    if (kind === "snapshot" && sn) {
      const t0 = Date.now();
      // `?force=true` skips reading the cache below for THIS request only — never clears or writes
      // over it — so a diagnostic check ("is a live pull actually working for this camera right now,
      // the same way the periodic sweep does it") can't leave the cache empty for anyone else, and a
      // successful forced pull still repopulates it same as any other live win. Does NOT override
      // snapshotNoLiveDevices below — that's a deliberate per-camera do-not-pay-for-live-P2P setting,
      // not a cache freshness knob, so forcing a live check must not bypass it.
      const force = url.searchParams.get("force") === "true";
      // Warmed by snapshot-warmup.mjs's own schedule (when SNAPSHOT_WARM_INTERVAL_MIN is set) — serve it
      // instantly instead of paying for a live P2P pull on every request. Empty until that feature is
      // enabled AND has run at least once, so this never changes behaviour for anyone not opting in.
      const cached = force ? undefined : snapshotCache.get(sn);
      if (cached) {
        ctx.eventLog(`/snapshot ${sn} → 200 warm cache (${cached.jpeg.length}B)`);
        res.writeHead(200, { "content-type": "image/jpeg", "content-length": cached.jpeg.length });
        return res.end(cached.jpeg);
      }
      // This device must never pay for a live pull — not on the periodic sweep, and not here either.
      // Only ever serve what's already sitting around: Eufy's own retained picture, or (last resort)
      // this bridge's own persisted "Last event" cover. Independent of the cache above, which a no-live
      // device can still land in via the sweep's free piggyback (see snapshot-warmup.mjs). Best-effort
      // throughout — every step is a soft fallback to the next, ending in 404 rather than a hard error.
      if (cfg.snapshotNoLiveDevices?.has(sn)) {
        const cam = (await eufy.getDevice(sn).catch(() => undefined))?.camera?.();
        if (!cam) {
          ctx.eventLog(`/snapshot ${sn} → 404 no camera on device (no-live)`);
          return json(res, 404, { error: "no camera on this device" });
        }
        let jpeg = await cam.snapshotStored?.().catch(() => undefined);
        if (jpeg) {
          ctx.eventLog(`/snapshot ${sn} → 200 retained (${jpeg.length}B, no-live) (${Date.now() - t0}ms)`);
        } else {
          jpeg = await fs.promises.readFile(path.join(eventImageDir, `last-event-${sn}.jpg`)).catch(() => undefined);
          if (jpeg)
            ctx.eventLog(`/snapshot ${sn} → 200 last-event file (${jpeg.length}B, no-live) (${Date.now() - t0}ms)`);
        }
        if (!jpeg) {
          ctx.eventLog(`/snapshot ${sn} → 404 no image available (no-live) (${Date.now() - t0}ms)`);
          return json(res, 404, { error: "no image available" });
        }
        res.writeHead(200, { "content-type": "image/jpeg", "content-length": jpeg.length });
        return res.end(jpeg);
      }
      try {
        const cam = (await eufy.getDevice(sn)).camera?.();
        if (!cam) {
          ctx.eventLog(`/snapshot ${sn} → 404 no camera on device`);
          return json(res, 404, { error: "no camera on this device" });
        }
        let jpeg, retained;
        try {
          ({ jpeg, retained } = await cam.snapshotLive());
          recordLiveAttempt(sn, true);
        } catch (liveError) {
          ctx.eventLog(`/snapshot ${sn} → live failed (${Date.now() - t0}ms): ${liveError?.message ?? liveError}`);
          recordLiveAttempt(sn, false, liveError);
          jpeg = await cam.snapshotStored?.().catch(() => undefined);
        }
        if (jpeg) {
          // A genuine live win (not the SDK's own retained fallback) is the freshest picture anyone's
          // going to get for this camera for a while — cache it so the NEXT request that loses the
          // sibling race to this same camera gets this recent frame instead of falling all the way
          // back to a possibly much older "Last event" motion capture. Doesn't touch that file at all:
          // this is purely a /snapshot freshness cache, not a claim that anything was detected.
          if (!retained) snapshotCache.set(sn, { jpeg, capturedAt: Date.now() });
          ctx.eventLog(
            `/snapshot ${sn} → 200 ${retained ? "retained (SDK fallback)" : "live"} (${jpeg.length}B) (${Date.now() - t0}ms)`,
          );
          res.writeHead(200, { "content-type": "image/jpeg", "content-length": jpeg.length });
          return res.end(jpeg);
        }
        // Same last resort the no-live path above already has: live AND the SDK's own retained still
        // both came up empty (e.g. a sibling on the same HomeBase held the station AND nothing was ever
        // retained for this camera), so reach for this bridge's own persisted "Last event" cover before
        // giving up. Cheap, already on disk, and a real answer beats a 502 for a tile that just wants
        // SOMETHING to show.
        jpeg = await fs.promises.readFile(path.join(eventImageDir, `last-event-${sn}.jpg`)).catch(() => undefined);
        if (!jpeg) {
          ctx.eventLog(`/snapshot ${sn} → 404 no image available (${Date.now() - t0}ms)`);
          return json(res, 404, { error: "no image available" });
        }
        ctx.eventLog(`/snapshot ${sn} → 200 last-event file (${jpeg.length}B) (${Date.now() - t0}ms)`);
        res.writeHead(200, { "content-type": "image/jpeg", "content-length": jpeg.length });
        return res.end(jpeg);
      } catch (e) {
        ctx.eventLog(`/snapshot ${sn} → 502 FAILED (${Date.now() - t0}ms): ${e?.message ?? e}`);
        return json(res, 502, { error: String(e?.message ?? e) });
      }
    }

    // The latest detection thumbnail the SDK downloaded + retained (no live capture). The SDK's cache is
    // in-memory (cleared on restart / watchdog recovery), so we also persist each served thumbnail to disk
    // and fall back to it when nothing is retained — the "Last event" image then survives restarts.
    if (kind === "event-image" && sn) {
      // HA fetches this to render "Last event" (usually right after a detection event). Trace the
      // outcome so a "Last event never updates" report shows whether HA even asked and what it got back.
      const file = path.join(eventImageDir, `last-event-${sn}.jpg`);
      try {
        const cam = (await eufy.getDevice(sn)).camera?.();
        if (!cam?.snapshotStored) {
          ctx.eventLog(`/event-image ${sn} → 404 no camera on device`);
          return json(res, 404, { error: "no camera on this device" });
        }
        const jpeg = await cam.snapshotStored();
        fs.writeFile(file, jpeg, () => {}); // best-effort persist for restart survival
        ctx.eventLog(`/event-image ${sn} → 200 live thumbnail (${jpeg.length}B) — Last event updated`);
        res.writeHead(200, { "content-type": "image/jpeg", "content-length": jpeg.length });
        return res.end(jpeg);
      } catch (e) {
        // Nothing retained live — serve the last persisted thumbnail if we have one.
        try {
          const cached = await fs.promises.readFile(file);
          // Include WHY the live cache was empty (not-observed / pending / download-failed / invalid-image)
          // even though we can still serve a disk copy — on a local-storage account this is expected to be
          // "not-observed" (no push thumbnail), and the on-detection local refresh is what advances it.
          ctx.eventLog(
            `/event-image ${sn} → 200 cached thumbnail (${cached.length}B, from disk; live unavailable: ${e?.reason ?? e?.message ?? e}) — Last event served`,
          );
          res.writeHead(200, { "content-type": "image/jpeg", "content-length": cached.length });
          return res.end(cached);
        } catch {
          // No live and no persisted image. Surface the SDK reason (not-observed / pending /
          // download-failed / invalid-image) so a caller can tell "no event yet" from a failure.
          ctx.eventLog(
            `/event-image ${sn} → 404 no image (reason=${e?.reason ?? e?.message ?? e}) — Last event NOT updated`,
          );
          return json(res, 404, { error: String(e?.message ?? e), reason: e?.reason });
        }
      }
    }

    if (kind === "stream" && sn) {
      if (cfg.streamIdleMs) lastPullAttempt.set(sn, Date.now()); // consumer is asking (watched vs. gone)

      // A feed kept warm from a just-departed consumer — reuse it outright, skipping straight past the
      // idle-suspend check and a fresh P2P handshake entirely. This is the whole point of the grace
      // period: a quick reconnect (e.g. Home Assistant's own RTSP client retrying ~10s after its
      // hard-coded 5s read timeout) gets data immediately instead of racing that same handshake again.
      const pending = pendingTeardown.get(sn);
      if (pending) {
        clearTimeout(pending.timer);
        pendingTeardown.delete(sn);
        pending.feed.removeListener("data", pending.drain);
        ctx.eventLog(`/stream ${sn} → reconnect reused warm P2P feed`);
        attachConsumer(sn, pending.feed, req, res);
        return;
      }

      // Idle-suspended: no detection recently, so don't reopen the P2P session. go2rtc's ffmpeg source
      // retries into this until a detection or the consumer giving up lifts it (see streamIdleTick).
      // Skipped when a feed is already active/opening — a second consumer joining one already justified
      // by an earlier request shouldn't be refused for a staleness check that request already passed.
      if (cfg.streamIdleMs && idleSuspended.has(sn) && !activeStreams.has(sn) && !pendingOpens.has(sn))
        return json(res, 503, {
          error: "stream idle-suspended — no recent detection, waiting for motion or a fresh viewer",
        });
      const t0 = Date.now();
      try {
        const feed = await openFeedFor(sn);
        attachConsumer(sn, feed, req, res);
        return;
      } catch (e) {
        if (e instanceof NoLiveVideoError) return json(res, 404, { error: e.message });
        ctx.eventLog(`/stream ${sn} → 502 FAILED (${Date.now() - t0}ms): ${e?.message ?? e}`);
        return json(res, 502, { error: String(e?.message ?? e) });
      }
    }

    return json(res, 404, { error: "not found" });
  };
}
