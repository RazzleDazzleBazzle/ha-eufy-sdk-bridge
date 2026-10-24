// Startup warm-ups + the on-detection "Last event" image refresh, all reading the on-HomeBase P2P
// database. Best-effort: any failure just leaves the relevant cache smaller / the image stale. Every
// DB read here shares the session's single `dbChunk`/`image` stream, so they MUST NOT overlap — a
// shared lock (`withDbLock`) serialises the face-roster warm, the boot image warm, and every live
// refresh into one at-a-time queue. Kicked off the boot critical path (they don't gate `ready`).
import fs from "node:fs";
import path from "node:path";
import { parseFaceRoster, firstJsonObject } from "./faces.mjs";

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

// How long after a detection to wait before pulling the fresh cover: the HomeBase needs a moment to
// finish writing the new event's crop before `history_record_info` reports it. Also debounces a burst
// (auto-track fires many pushes) down to one refresh. Override with EVENT_IMAGE_REFRESH_DELAY_MS.
const REFRESH_DELAY_MS = Number(process.env.EVENT_IMAGE_REFRESH_DELAY_MS) || 3000;

// The HomeBase writes a new event's crop SECONDS after the motion push, and not on a fixed delay — a
// single early query gets the previous cover ("unchanged") or a crop-less record, so "Last event"
// advances its date but shows the previous image. So the local-cover refresh RETRIES on an escalating
// schedule until a genuinely-new image lands (then nudges HA once and stops). ~60s of coverage total.
const LOCAL_REFRESH_SCHEDULE = (process.env.EVENT_IMAGE_REFRESH_SCHEDULE_MS || "3000,4000,6000,10000,15000,20000")
  .split(",")
  .map((n) => Number(n.trim()))
  .filter((n) => Number.isFinite(n) && n > 0);

// The fast schedule above (~58s) often expires BEFORE the HomeBase overwrites the rolling cover with
// the new frame — which is exactly why the manual "Refresh Last Event" button (pressed later) works
// when the auto-refresh didn't. So after the fast ramp, keep polling at a steady interval up to a cap,
// so the late write is caught automatically and the button becomes unnecessary. Env-tunable.
const LOCAL_REFRESH_TAIL_MS = Number(process.env.EVENT_IMAGE_REFRESH_TAIL_MS) || 30000;
const LOCAL_REFRESH_MAX_MS = Number(process.env.EVENT_IMAGE_REFRESH_MAX_MS) || 240000;

export function createWarmup(ctx) {
  const { eufy, eventImageDir } = ctx;
  const { faceNames } = ctx.state;

  // ── shared serialisation for every P2P DB read (they share one dbChunk/image stream per session) ──
  let dbChain = Promise.resolve();
  function withDbLock(fn) {
    const result = dbChain.then(fn, fn); // run after the previous op settles, success or failure
    dbChain = result.then(
      () => undefined,
      () => undefined,
    );
    return result;
  }

  /** The admin/account id the P2P DB queries key off — from a shared device's member record, else the login. */
  async function accountIdOf(devs) {
    return devs.find((d) => d.raw?.member?.admin_user_id)?.raw?.member?.admin_user_id ?? eufy.api?.auth?.userId ?? "";
  }

  /** Sessions come up asynchronously after login — wait (up to ~20s) for at least one to appear. */
  async function awaitSessions() {
    let sessions = eufy.getP2pSessions();
    for (let i = 0; i < 20 && sessions.size === 0; i++) {
      await sleep(1000);
      sessions = eufy.getP2pSessions();
    }
    return sessions;
  }

  // Standard "read the whole table" query the app uses (inner cmd 10000). We only need the newest few
  // rows, so `count` is modest — but ordering isn't guaranteed, so we still sort by `start_time` below.
  // Kept small (50): the direct read spans many P2P datagrams, and a big response often didn't finish
  // reassembling before we parsed it, which returned no records ("no local cover found") intermittently.
  const HISTORY_TABLE_QUERY = {
    count: 50,
    start_date: "",
    end_date: "",
    start_id: 0,
    end_id: 1,
    flag: 0,
    need_ai: 1,
    res_unzip: 1,
    update_time: "0",
    start_time: "0",
    alarm_id: "",
  };

  /**
   * The on-HomeBase per-event crop path for a `history_record_info` record.
   *
   * On HomeBase-3 local storage the record has NO dedicated crop field — only a bare `thumb_path`
   * ("snapshort.jpg") that resolves to one generic rolling image (so bytes never change), plus a
   * `storage_path` = the event's recording under a per-event directory
   * (…/<yyyymmddHHMMSS>/<…>.zxvid). The event still lives beside the video, so we join the recording's
   * DIRECTORY with the thumb filename → a path that's unique per event and actually advances. Absolute
   * crop fields (other firmwares) are used as-is; a bare thumb with no storage_path is the last resort.
   */
  function cropPathOf(rec) {
    const pl = rec?.payload ?? rec;
    const absolute = pl?.crop_hb3_path || pl?.crop_path || pl?.pic_path;
    if (typeof absolute === "string" && absolute.startsWith("/")) return absolute;

    const thumb = pl?.thumb_path || pl?.thumbnail_path || absolute;
    if (typeof thumb !== "string" || !thumb) return undefined;
    if (thumb.startsWith("/")) return thumb; // already a full path
    const storage = pl?.storage_path;
    if (typeof storage === "string" && storage.includes("/")) {
      const dir = storage.slice(0, storage.lastIndexOf("/"));
      if (dir) return `${dir}/${thumb}`; // per-event dir + thumb filename
    }
    return thumb; // bare filename fallback (generic, but better than nothing)
  }

  /** Recency key for a history record; higher = newer. `record_id` is monotonic per event; fall back to
   * the `start_time`/`end_time` datetime string (epoch ms). 0 only when the firmware gives us nothing. */
  function recordTime(rec) {
    const pl = rec?.payload ?? rec;
    const rid = Number(pl?.record_id ?? rec?.record_id);
    if (Number.isFinite(rid) && rid > 0) return rid;
    const t = Date.parse(pl?.start_time ?? pl?.end_time ?? pl?.create_time ?? "");
    return Number.isFinite(t) ? t : 0;
  }

  /**
   * Query each device's LATEST event cover over P2P and return `device_sn -> on-HomeBase cover path`
   * (the plain-JPEG crop the /event-image serves).
   *
   * Uses the DIRECT table read of `history_record_info` (inner cmd **10000**, mChannel **255** — the
   * station channel), NOT the old `10013`. Per the reversed P2P catalog (eufy-mega docs/p2p/faces.md)
   * 10013 is only a "sync/poke" that returns a cached "latest" snapshot which never advances as new
   * events complete — which is why the crop came back byte-identical run-to-run and "Last event" never
   * moved; ch0 also returns nothing on a HomeBase (DB reads live on 255). We then pick, per device, the
   * record with the greatest `start_time` and use its crop path.
   *
   * Sent twice (the first can be dropped during handshake). Caller must hold the DB lock.
   */
  async function queryStationCovers(session, accountId) {
    let chunk = "";
    const onChunk = ({ text }) => (chunk += text);
    session.on("dbChunk", onChunk);
    const send = () =>
      session.isConnected &&
      session.queryDatabase("history_record_info", { accountId, innerCmd: 10000, query: HISTORY_TABLE_QUERY });
    send();
    setTimeout(send, 1500);

    // Poll for a FULLY-reassembled response rather than parsing once after a fixed wait: the direct read
    // can span many datagrams, and a fixed 6s window frequently closed mid-reassembly → `firstJsonObject`
    // returned nothing → "no local cover found" even though the cover existed. Exit as soon as we have a
    // parseable object carrying `data[]`; give it up to ~14s (the resend lands at 1.5s).
    let parsed;
    for (let i = 0; i < 70; i++) {
      await sleep(200);
      const obj = firstJsonObject(chunk);
      if (obj && Array.isArray(obj.data) && obj.data.length) {
        parsed = obj;
        break;
      }
    }
    session.off?.("dbChunk", onChunk);

    const records = parsed?.data ?? firstJsonObject(chunk)?.data ?? [];
    if (!records.length) {
      // Diagnostic: distinguish "response never arrived" (chunk empty) from "arrived but unparseable /
      // no data" (chunk large) so a recurring failure points straight at transport vs shape.
      ctx.eventLog?.(`history query: no records parsed (raw ${chunk.length}B received)`);
    }
    // Keep the newest record per device (max start_time), then take its crop path. Ties (or a firmware
    // that omits start_time) fall back to last-wins, matching the old behaviour for that degenerate case.
    // Also track, per device, how many records mention it and the newest ts seen even when it has no crop
    // — so a stale/"unchanged" cover can be told apart from a newer record we skipped for lacking a crop.
    const newest = new Map(); // device_sn -> { ts, path }
    const seen = new Map(); // device_sn -> { count, maxTs, maxTsHasCrop }
    for (const rec of records) {
      const dsn = rec?.device_sn;
      if (!dsn) continue;
      const ts = recordTime(rec);
      const p = cropPathOf(rec);
      const s = seen.get(dsn) ?? { count: 0, maxTs: -1, maxTsHasCrop: false };
      s.count += 1;
      if (ts > s.maxTs) {
        s.maxTs = ts;
        s.maxTsHasCrop = Boolean(p);
      }
      seen.set(dsn, s);
      if (!p) continue;
      const cur = newest.get(dsn);
      if (!cur || ts >= cur.ts) newest.set(dsn, { ts, path: p, rec });
    }
    return { covers: newest, recordCount: records.length, seen };
  }

  /** Request one on-HomeBase cover path over P2P and return its JPEG bytes (≤8s), or undefined. */
  async function fetchImage(session, filePath, accountId) {
    const images = new Map();
    const onImage = ({ file, data }) => {
      if (data?.[0] === 0xff && data?.[1] === 0xd8) images.set(file, data);
    };
    session.on("image", onImage);
    session.requestImage(filePath, { accountId });
    for (let i = 0; i < 40 && !images.has(filePath); i++) await sleep(200);
    session.off?.("image", onImage);
    return images.get(filePath);
  }

  /** Persist bytes to last-event-<sn>.jpg only when they differ from what's on disk; report whether it changed. */
  function persistIfChanged(dsn, data) {
    const file = path.join(eventImageDir, `last-event-${dsn}.jpg`);
    let prev;
    try {
      prev = fs.readFileSync(file);
    } catch {
      prev = undefined;
    }
    if (prev && prev.equals(data)) return false;
    fs.writeFileSync(file, data);
    return true;
  }

  /**
   * Build the face-recognition roster by reading `person_basic_info` off each connected HomeBase over P2P
   * (CMD_DATABASE 1306 / inner cmd 10000, mChannel 255). Faces are account-wide, so every station's rows
   * merge into one map. On any failure the map just stays smaller and `personDetected` falls back to Unknown.
   */
  async function warmFaceRoster() {
    return withDbLock(async () => {
      try {
        const devs = await eufy.getDevices();
        const accountId = await accountIdOf(devs);
        for (const [, session] of await awaitSessions()) {
          for (let i = 0; i < 30 && !session.isConnected; i++) await sleep(500);
          if (!session.isConnected) continue;

          let chunk = "";
          const onChunk = ({ text }) => (chunk += text);
          session.on("dbChunk", onChunk);
          session.requestFaces({ accountId });
          setTimeout(() => session.isConnected && session.requestFaces({ accountId }), 1500);
          await sleep(6000);
          session.off?.("dbChunk", onChunk);

          let added = 0;
          for (const [id, rec] of parseFaceRoster(chunk)) {
            if (!faceNames.has(id)) added++;
            faceNames.set(id, rec);
          }
          if (added) console.log(`[bridge] face roster: +${added} person(s) (${faceNames.size} total)`);
        }
      } catch (e) {
        console.error(`[bridge] warm face roster failed: ${e?.message ?? e}`);
      }
    });
  }

  /**
   * Warm the "Last event" thumbnails from LOCAL (HomeBase) storage, so images are populated on first HA
   * load even before any live push. This is the only startup source for local-storage accounts (the cloud
   * events/list + cover_path are empty without cloud storage), and — because push notifications on such
   * accounts carry no `pic_url` — {@link refreshLastEventImageFor} is also the only LIVE source, so this
   * path and that one share the same helpers.
   */
  async function warmLastEventImages() {
    return withDbLock(async () => {
      try {
        const devs = await eufy.getDevices();
        const accountId = await accountIdOf(devs);
        for (const [, session] of await awaitSessions()) {
          for (let i = 0; i < 30 && !session.isConnected; i++) await sleep(500); // await handshake
          if (!session.isConnected) continue;

          const { covers } = await queryStationCovers(session, accountId);
          if (!covers.size) continue;

          for (const [dsn, { path: filePath }] of covers) {
            const data = await fetchImage(session, filePath, accountId);
            if (data && persistIfChanged(dsn, data)) {
              console.log(`[bridge] warmed last-event image for ${dsn} (${data.length}B, local)`);
            }
          }
        }
      } catch (e) {
        console.error(`[bridge] warm last-event images failed: ${e?.message ?? e}`);
      }
    });
  }

  /** Fetch one station device's cover and persist it; logs + returns whether it actually changed. */
  async function persistCover(session, accountId, dsn, entry, recordCount, viaSn) {
    const filePath = entry.path;
    const crop = filePath.split("/").pop();
    const data = await fetchImage(session, filePath, accountId);
    if (!data) {
      ctx.eventLog?.(`local refresh: ${dsn} — cover fetch returned no image (crop=${crop})`);
      return false;
    }
    if (persistIfChanged(dsn, data)) {
      ctx.eventLog?.(
        `local refresh: ${dsn} → last-event image updated (${data.length}B, local) ` +
          `[ts=${entry.ts} crop=${crop} of ${recordCount} recs]` +
          (viaSn !== dsn ? ` [triggered by ${viaSn} — linked sensor on the same station]` : ""),
      );
      return true;
    }
    ctx.eventLog?.(
      `local refresh: ${dsn} — cover unchanged (${data.length}B) [ts=${entry.ts} crop=${crop} of ${recordCount} recs]`,
    );
    // One-shot structure dump on the failing path: the picked record's field names + a truncated
    // JSON, so we can find the REAL per-event crop-path field and timestamp (crop_hb3_path here is a
    // generic rolling "snapshort.jpg" and start_time reads 0). Trimmed to keep the log sane.
    try {
      const rec = entry.rec ?? {};
      const keys = Object.keys(rec).join(",");
      const pkeys = Object.keys(rec.payload ?? {}).join(",");
      ctx.eventLog?.(`local refresh: ${dsn} — record keys=[${keys}] payload=[${pkeys}]`);
      ctx.eventLog?.(`local refresh: ${dsn} — record sample=${JSON.stringify(rec).slice(0, 700)}`);
    } catch {
      /* best-effort diagnostic */
    }
    return false;
  }

  /**
   * Pull the freshest local cover for ONE device from HomeBase storage and persist it to
   * last-event-<sn>.jpg. This is what actually advances "Last event" on a local-storage account: the SDK's
   * push-thumbnail cache (`camera.snapshotStored()`) stays empty because such accounts' pushes carry no
   * cloud `pic_url`, so /event-image would otherwise serve the boot-warmed image forever.
   *
   * Returns the serial that actually changed — `sn` itself, OR, when `sn` is a non-camera device whose
   * detection landed on a linked camera instead (see below), that camera's own serial — so the caller
   * nudges HA about the device whose /event-image genuinely moved, not `sn` unconditionally. `false` when
   * nothing changed. Serialised against every other DB read.
   */
  function refreshLastEventImageFor(sn) {
    return withDbLock(async () => {
      if (!sn) return false;
      try {
        const sessions = eufy.getP2pSessions();
        if (!sessions.size) return false;
        const devs = await eufy.getDevices();
        const accountId = await accountIdOf(devs);
        for (const [, session] of sessions) {
          if (!session.isConnected) continue;
          const { covers, recordCount, seen } = await queryStationCovers(session, accountId);
          if (covers.has(sn)) {
            return (await persistCover(session, accountId, sn, covers.get(sn), recordCount, sn)) && sn;
          }
          const cameraTargets = ctx.state.cameraTargets ?? [];
          if (!cameraTargets.includes(sn)) {
            // `sn` is not itself one of this account's cameras — e.g. a standalone motion/entry sensor
            // with wider range than its linked camera's own PIR. A detection push is attributed to
            // whichever device actually sensed it, but the RECORDING still lands on the camera, under the
            // camera's own device_sn, never the sensor's, so a sensor-sn lookup here always misses.
            // `covers` already holds every device this station reported a crop for in this ONE query — no
            // extra P2P cost — so fall back to whichever of this account's known cameras this station just
            // gave us a cover for. (Gated on `sn` not being a camera itself, so an ordinary camera-sn
            // detection whose OWN crop simply hasn't landed yet — the common early-retry case this file
            // already handles below — doesn't start touching every other camera on the station.)
            const linked = cameraTargets.filter((csn) => csn !== sn && covers.has(csn));
            if (linked.length) {
              let changedSn = false;
              for (const csn of linked) {
                if (await persistCover(session, accountId, csn, covers.get(csn), recordCount, sn)) changedSn = csn;
              }
              if (changedSn) return changedSn;
              continue; // this station had covers, just none newer than what's already on disk
            }
          }
          // Diagnostic: did this station return records for this device at all? If it has a newer record
          // whose newest ts carries NO crop, that's "HomeBase wrote the event but not (yet) a crop";
          // if no records mention it, this simply isn't its station.
          const s = seen?.get(sn);
          if (s) {
            ctx.eventLog?.(
              `local refresh: ${sn} — no crop in its ${s.count} record(s) here ` +
                `(newest ts=${s.maxTs}, hasCrop=${s.maxTsHasCrop}; ${recordCount} total)`,
            );
          }
          continue; // this device isn't on this station — try the next
        }
        ctx.eventLog?.(`local refresh: ${sn} — no local cover found on any connected station`);
        return false;
      } catch (e) {
        console.error(`[bridge] local refresh for ${sn} failed: ${e?.message ?? e}`);
        return false;
      }
    });
  }

  /**
   * Advance "Last event" from the SDK's PUSHED thumbnail (cloud `pic_url`) — the path a doorbell / cloud
   * camera uses (its P2P `history_record_info` returns nothing, so {@link refreshLastEventImageFor} can't
   * help it). `StoredImageCache` downloads the thumbnail lazily and emits no "ready" event, so right after
   * the push `snapshotStored()` is often still `pending`/`download-failed`; HA's immediate fetch then gets
   * the stale disk copy and never re-fetches. So we retry across a bounded window and persist+report the
   * moment a NEW image lands, letting the caller nudge HA. `not-observed`/`invalid-image` mean there is no
   * pushed thumbnail (a pure local-storage cam) — give up immediately and let the P2P path handle it.
   */
  async function refreshStoredSnapshotFor(sn) {
    let cam;
    try {
      cam = (await eufy.getDevice(sn)).camera?.();
    } catch {
      return false;
    }
    if (!cam?.snapshotStored) return false;
    for (let i = 0; i < 12; i++) {
      // ~30s total (12 × 2.5s) — long enough for a slow cloud thumbnail download to land.
      let jpeg;
      try {
        jpeg = await cam.snapshotStored();
      } catch (e) {
        const reason = e?.reason ?? e?.message ?? e;
        if (reason !== "pending" && reason !== "download-failed") return false; // not-observed / invalid
        await sleep(2500);
        continue;
      }
      if (jpeg?.length && persistIfChanged(sn, jpeg)) {
        ctx.eventLog?.(`stored snapshot: ${sn} → last-event image updated (${jpeg.length}B, push)`);
        return true;
      }
      return false; // got bytes but unchanged — nothing new to nudge about
    }
    ctx.eventLog?.(`stored snapshot: ${sn} — no push thumbnail landed in time`);
    return false;
  }

  /** Nudge HA to re-pull /event-image — the detection broadcast already fetched the (stale) image, so
   *  without this HA would not update until its next poll. */
  const nudge = (sn, changed) => {
    if (changed) ctx.broadcast?.({ event: "eventImageUpdated", deviceSn: sn });
  };

  // In-flight guard per device: a burst of pushes (auto-track fires many) collapses to the one retry
  // loop already running for that device.
  const pendingRefresh = new Set(); // sn
  function onDetectionRefresh(sn) {
    if (!sn || pendingRefresh.has(sn)) return;
    pendingRefresh.add(sn);
    // Pushed cloud thumbnail (doorbell / cloud cams): its own internal retry window; nudge if it lands.
    void refreshStoredSnapshotFor(sn).then((changed) => nudge(sn, changed));
    // On-HomeBase local cover (local-storage cams): RETRY across the escalating schedule until a fresh
    // image lands — the HomeBase writes the new crop a few seconds after the push, so a single early
    // query is exactly what leaves "Last event" one image behind. Stop at the first genuine change.
    void (async () => {
      try {
        // Fast escalating ramp for the common case, then a steady tail up to LOCAL_REFRESH_MAX_MS so a
        // late crop write is still caught without the user pressing "Refresh Last Event". Stop at the
        // first genuine change.
        let elapsed = 0;
        for (let i = 0; elapsed < LOCAL_REFRESH_MAX_MS; i++) {
          const delay = i < LOCAL_REFRESH_SCHEDULE.length ? LOCAL_REFRESH_SCHEDULE[i] : LOCAL_REFRESH_TAIL_MS;
          await sleep(delay);
          elapsed += delay;
          // The device that actually changed — `sn` itself, or a linked camera when `sn` is a sensor.
          const changedSn = await refreshLastEventImageFor(sn);
          if (changedSn) {
            nudge(changedSn, true);
            return;
          }
        }
        ctx.eventLog?.(
          `local refresh: ${sn} — no fresh crop after ${Math.round(elapsed / 1000)}s; ` +
            `will catch on the next detection (or press "Refresh Last Event")`,
        );
      } finally {
        pendingRefresh.delete(sn);
      }
    })();
  }

  /**
   * Force an immediate "Last event" image refresh for a device, bypassing the debounce/retry schedule —
   * runs both sources once, right now, and nudges HA if a fresh image lands. Backs the manual "Refresh
   * Last Event" control, and answers whether the image actually changed so the caller can report it.
   */
  async function forceRefreshEventImage(sn) {
    if (!sn) return false;
    const [stored, local] = await Promise.all([
      refreshStoredSnapshotFor(sn).catch(() => false),
      refreshLastEventImageFor(sn).catch(() => false),
    ]);
    // `local` names the device that actually changed — `sn` itself, or a linked camera when `sn` is a
    // sensor — so a manual refresh on a sensor still nudges the right camera, not the sensor.
    if (stored) nudge(sn, true);
    if (local) nudge(local, true);
    const changed = Boolean(stored || local);
    ctx.eventLog?.(`force refresh: ${sn} → ${changed ? "image updated" : "no newer image available yet"}`);
    return changed;
  }

  return {
    warmFaceRoster,
    warmLastEventImages,
    refreshLastEventImageFor,
    refreshStoredSnapshotFor,
    onDetectionRefresh,
    forceRefreshEventImage,
  };
}
