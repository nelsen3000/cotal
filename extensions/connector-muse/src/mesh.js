/**
 * Minimal Cotal mesh client over NATS.
 *
 * Speaks the Cotal wire protocol directly (subjects per src/subjects.js,
 * CotalMessage per src/wire.js). It is NOT a reimplementation of the
 * broker or manager — it only:
 *   - connects with minted JWT creds (0600 file, path in env),
 *   - binds the pre-provisioned per-incarnation DM durable for offline replay,
 *   - subscribes the DM receive filter + configured channel filters,
 *   - publishes DMs/channel messages with `Nats-Msg-Id`,
 *   - writes presence heartbeats to the presence KV bucket.
 *
 * Provisioning (cotal mint --provision) creates the durable; this client only
 * BINDS it (never creates), matching the agent profile's bind-only grant.
 */

const { readFile } = require("node:fs/promises");
const { connect, headers } = require("@nats-io/transport-node");
const { credsAuthenticator } = require("@nats-io/nats-core");
const _te = new TextEncoder();
const _td = new TextDecoder();
const StringCodec = () => ({ encode: (s) => _te.encode(s), decode: (b) => _td.decode(b) });
const { jetstream } = require("@nats-io/jetstream");
const { Kvm } = require("@nats-io/kv");
const nkeys = require("nkeys.js");
const {
  spacePrefix,
  chatSubject,
  chatRecvFilter,
  unicastSubject,
  unicastRecvFilter,
  anycastSubject,
  anycastServeFilter,
  presenceBucket,
  dmStream,
  dmDurable,
  assertValidOwnerToken,
} = require("./subjects.js");
const { buildDm, buildChannel, parseMessage } = require("./wire.js");

const sc = StringCodec();
const NATS_MSG_ID = "Nats-Msg-Id";

function withMsgId(id) {
  const h = headers();
  h.set(NATS_MSG_ID, id);
  return h;
}

/** Read the nkey seed from a creds file and derive the public key (presence key). */
async function publicKeyFromCreds(credsFile) {
  const text = await readFile(credsFile, "utf8");
  const m = text.match(/-----BEGIN USER NKEY SEED-----\s*([A-Za-z0-9_-]+)\s*-+END USER NKEY SEED-+/);
  if (!m) throw new Error(`no USER NKEY SEED found in ${credsFile}`);
  return nkeys.fromSeed(Buffer.from(m[1])).getPublicKey();
}

class MeshClient {
  constructor({ space, owner, actor, role, credsFile, lifecycleUid, server }) {
    assertValidOwnerToken(owner);
    assertValidOwnerToken(actor);
    if (!credsFile) throw new Error("COTAL_CREDS (minted creds file) is required");
    if (!lifecycleUid) throw new Error("COTAL_LIFECYCLE_UID is required");
    this.space = space;
    this.owner = owner;
    this.actor = actor;
    this.role = role || null;
    this.credsFile = credsFile;
    this.lifecycleUid = lifecycleUid;
    this.server = server || "nats://127.0.0.1:4222";
    this.nc = null;
    this.js = null;
    this.pubkey = null;
    this.dmConsumer = null;
    this.channelSubs = [];
  }

  principalName() {
    return `${this.owner}.${this.actor}`;
  }

  /** Fail-before-presence: connect AND bind the DM durable before any presence write. */
  async connect() {
    this.pubkey = await publicKeyFromCreds(this.credsFile);
    // Presence KV key: the principal dot-form `owner.actor`. The minted JWT
    // grants put ONLY on `$KV.cotal_presence_<space>.<owner>.<actor>`, so the
    // key must match the grant exactly (not the bare nkey pubkey).
    // NOTE: @nats-io/transport-node (node) does not implement the `credsFile`
    // connect option (it is silently ignored) — build the authenticator from
    // the 0600 creds file bytes explicitly. The bytes are used transiently for
    // the connection only and are never logged or persisted.
    const credsBytes = await readFile(this.credsFile);
    this.nc = await connect({
      servers: [this.server],
      authenticator: credsAuthenticator(credsBytes),
      // The agent JWT grants the API-response inbox ONLY at `_INBOX_<nkey>.>`
      // (not the default `_INBOX.<nuid>`), so the client must use it.
      inboxPrefix: `_INBOX_${this.pubkey}`,
    });
    this.js = jetstream(this.nc);

    // Bind (never create) the pre-provisioned per-incarnation DM durable.
    // This durable pull is the ONLY DM receive path: the minted JWT's
    // sub.allow does NOT include inst.<owner>.<actor>.>, so a plain core
    // subscription on the DM filter hears nothing (durable-consumer-only).
    const stream = dmStream(this.space);
    const durable = dmDurable(this.owner, this.actor, this.lifecycleUid);
    try {
      this.dmConsumer = await this.js.consumers.get(stream, durable);
    } catch (err) {
      await this.nc.close();
      throw new Error(
        `fail-before-presence: could not bind DM durable "${durable}" on stream "${stream}" ` +
          `(mint with --provision first): ${err.message}`
      );
    }
    return this;
  }

  async subscribeChannels(channels) {
    for (const ch of channels || []) {
      if (!ch) continue;
      const sub = this.nc.subscribe(chatRecvFilter(this.space, ch));
      this.channelSubs.push({ channel: ch, sub });
      // Drain in the background into the pending queue; the poll loop reads it.
      void (async () => {
        for await (const m of sub) {
          this._livePending.push(m);
        }
      })();
    }
  }

  /** Live-pending buffer for channel (non-durable) messages. */
  _livePending = [];

  /**
   * Pull pending DMs from the bound durable (offline replay included).
   * Returns parsed { msg, ack, nak } items; the caller acks after surfacing
   * (file drop) — ack binds to surfacing, never to receipt.
   */
  async fetchPending({ maxMessages = 50, expiresMs = 1500 } = {}) {
    const out = [];
    if (!this.dmConsumer) return out;
    let iter;
    try {
      iter = await this.dmConsumer.fetch({ max_messages: maxMessages, expires: expiresMs });
    } catch {
      return out; // no messages waiting — not an error
    }
    for await (const m of iter) {
      try {
        const msg = parseMessage(m.data);
        out.push({
          msg,
          ack: () => m.ack(),
          nak: (delayMs = 5000) => m.nak(delayMs),
        });
      } catch (err) {
        // Malformed wire: log and ack to keep the durable moving — a message
        // no parser can read can never be surfaced, so redelivery is pure noise.
        console.error(`[mesh] dropping malformed wire message: ${err.message}`);
        m.ack();
      }
    }
    // Drain live channel messages collected since the last tick.
    const live = this._livePending.splice(0, this._livePending.length);
    for (const m of live) {
      try {
        const msg = parseMessage(m.data);
        out.push({ msg, ack: () => {}, nak: () => {} });
      } catch (err) {
        console.error(`[mesh] dropping malformed live message: ${err.message}`);
      }
    }
    return out;
  }

  /** Publish a DM. Returns the built CotalMessage. */
  async dm({ toOwner, toActor, text, replyTo, contextId, mentions }) {
    const msg = buildDm({
      space: this.space,
      fromId: this.pubkey,
      fromName: this.principalName(),
      fromRole: this.role,
      to: `${toOwner}.${toActor}`,
      text,
      replyTo,
      contextId,
      mentions,
    });
    const subject = unicastSubject(
      this.space,
      toOwner,
      toActor,
      this.owner,
      this.actor
    );
    // Core publish (ACL-safe subject grant); the DM_<space> stream captures it
    // by subject, so offline peers replay it from their durable.
    this.nc.publish(subject, sc.encode(JSON.stringify(msg)), {
      headers: withMsgId(msg.id),
    });
    await this.nc.flush();
    return msg;
  }

  /** Publish a channel message. Returns the built CotalMessage. */
  async sendChannel({ channel, text, replyTo, contextId, mentions }) {
    const msg = buildChannel({
      space: this.space,
      fromId: this.pubkey,
      fromName: this.principalName(),
      fromRole: this.role,
      channel,
      text,
      replyTo,
      contextId,
      mentions,
    });
    const subject = chatSubject(this.space, this.owner, this.actor, channel);
    this.nc.publish(subject, sc.encode(JSON.stringify(msg)), {
      headers: withMsgId(msg.id),
    });
    await this.nc.flush();
    return msg;
  }

  /** Write a presence heartbeat to the presence KV bucket (best-effort). */
  async publishPresence(status, extra = {}) {
    try {
      const kvm = new Kvm(this.nc);
      const bucket = await kvm.open(presenceBucket(this.space));
      const record = {
        card: { id: this.pubkey, name: this.principalName(), role: this.role },
        lifecycleUid: this.lifecycleUid,
        status,
        ts: Date.now(),
        connector: "muse",
        ...extra,
      };
      await bucket.put(this.principalName(), sc.encode(JSON.stringify(record)));
    } catch (err) {
      // Presence writes are best-effort and never gate delivery.
      console.error(`[mesh] presence write failed (non-fatal): ${err.message}`);
    }
  }

  /** Read the whole presence roster (peer view). */
  async readRoster() {
    const kvm = new Kvm(this.nc);
    const bucket = await kvm.open(presenceBucket(this.space));
    const keys = await bucket.keys();
    const roster = [];
    for await (const k of keys) {
      try {
        const e = await bucket.get(k);
        if (e) roster.push({ key: k, ...JSON.parse(sc.decode(e.value)) });
      } catch {
        /* skip unreadable entries */
      }
    }
    return roster;
  }

  async close() {
    try {
      for (const { sub } of this.channelSubs) sub.unsubscribe();
    } catch { /* noop */ }
    if (this.nc) {
      try {
        await this.nc.drain();
      } catch { /* noop */ }
    }
  }
}

module.exports = {
  MeshClient,
  chatSubject,
  chatRecvFilter,
  unicastSubject,
  unicastRecvFilter,
  anycastSubject,
  anycastServeFilter,
  presenceBucket,
  dmStream,
  dmDurable,
  spacePrefix,
};
