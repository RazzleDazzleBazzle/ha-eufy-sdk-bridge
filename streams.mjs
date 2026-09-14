// Session-per-streaming-camera.
//
// The SDK keeps ONE P2P session per station, and the HomeBase tags every inbound media frame channel 0
// regardless of which camera was started — so two cameras streamed through one session arrive
// byte-identical (measured: two handles got the same frames, bitrate doubled). A separate EufyMega
// instance per streaming camera means a separate session, which keeps them apart. Measured working:
// five cameras concurrently, every pair byte-distinct, ~4.4 Mbps aggregate.
//
// These clients share the session FILE, so they hydrate the same token instead of logging in again —
// eufy permits one active login per account, and a second login kicks the first.
//
// This is a workaround at the wrong layer; the right fix is session-per-stream INSIDE the SDK, after
// which this whole file collapses to reusing the one control client.
import { EufyMega, FileSessionStore, LoginStatus } from "@razzledazzlebazzle/eufy-sdk";

const clients = new Map(); // sn -> EufyMega

/** Get (or lazily create + hydrate) the dedicated stream client for a camera. */
export async function streamClientFor(sn, cfg) {
  let client = clients.get(sn);
  if (client) return client;
  client = new EufyMega({
    email: cfg.email,
    password: cfg.password,
    countryCode: cfg.country,
    store: new FileSessionStore(cfg.session), // shared session file → hydrate, no fresh login
  });
  client.on("error", (e) => console.error(`[bridge] stream(${sn}) sdk error: ${e?.message ?? e}`));
  const result = await client.login();
  if (result.status !== LoginStatus.Ok) {
    clients.delete(sn);
    throw new Error(`stream client for ${sn} could not hydrate session (${result.status})`);
  }
  clients.set(sn, client);
  return client;
}

/** Tear down every stream client (on shutdown). */
export async function closeStreamClients() {
  await Promise.all([...clients.values()].map((c) => c.disconnect?.().catch(() => {})));
  clients.clear();
}
