// Piggybacks a fresh /snapshot cache entry on a genuine live viewer's own P2P feed — a Home App
// (or any other) live-stream session already pays for the P2P pull; grabbing a frame from go2rtc's
// OWN producer for that same stream costs nothing extra, since go2rtc is already decoding it to
// serve the live view. See src/http-routes.mjs's openFeedFor, which calls grabFrameToCache once per
// freshly-opened feed (not on every consumer attach/reattach — a feed reused across a reconnect or a
// second simultaneous consumer doesn't refire this).
//
// go2rtc's own `api/frame.jpeg` (internal/mjpeg/mjpeg.go) attaches a lightweight temporary consumer
// to whatever's already producing for `src` and waits for its next frame — it does NOT need a
// separate P2P session of its own, it rides the one the live viewer already opened.

export function createGo2rtcFrame(cfg, { eventLog, fetchImpl = fetch } = {}) {
  /** Best-effort: grab one frame from go2rtc's live producer for `sn` and cache it as a snapshot. */
  async function grabFrameToCache(sn, snapshotCache) {
    const url = `http://127.0.0.1:${cfg.go2rtcApiPort}/api/frame.jpeg?src=${encodeURIComponent(sn)}`;
    try {
      const res = await fetchImpl(url);
      if (!res.ok) {
        eventLog?.(`/stream ${sn} → snapshot-from-live-view failed: go2rtc frame.jpeg ${res.status}`);
        return;
      }
      const jpeg = Buffer.from(await res.arrayBuffer());
      if (!jpeg.length) return;
      snapshotCache.set(sn, { jpeg, capturedAt: Date.now() });
      eventLog?.(`/stream ${sn} → snapshot cache refreshed from live view (${jpeg.length}B)`);
    } catch (e) {
      eventLog?.(`/stream ${sn} → snapshot-from-live-view failed: ${e?.message ?? e}`);
    }
  }

  return { grabFrameToCache };
}
