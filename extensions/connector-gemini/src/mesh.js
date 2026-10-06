'use strict';
/**
 * mesh.js — minimal Cotal mesh client over the `nats` npm package.
 *
 * Follows the subject rules from the structure study EXACTLY (never
 * string-concat ad hoc — all subjects go through the builders below):
 *   DM send:    cotal.<space>.inst.<recipOwner>.<recipActor>.<sndOwner>.<sndActor>
 *   DM receive: cotal.<space>.inst.<owner>.<actor>.>
 *   channel:    cotal.<space>.chat.<owner>.<actor>.<channel>   (publish)
 *               cotal.<space>.chat.*.*.<channel>               (subscribe)
 *   presence:   KV bucket cotal_presence_<space>, key <owner>.<actor>
 *   DM durable: dm_<owner>-<actor>-<uid> on stream DM_<space>
 *
 * Publish dedup via Nats-Msg-Id. Handlers are idempotent by message id.
 */
const { connect, credsAuthenticator, StringCodec, headers } = require('nats');

const sc = StringCodec();
function newId() {
  return `${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 10)}`;
}

/** Subject builders — the ONLY place subjects are constructed. */
function subjects(cfg) {
  const p = cfg.subjectPrefix; // "cotal.<space>."
  return {
    dmSend: (recipOwner, recipActor) =>
      `${p}inst.${recipOwner}.${recipActor}.${cfg.owner}.${cfg.actor}`,
    dmRecvFilter: () => `${p}inst.${cfg.owner}.${cfg.actor}.>`,
    channelSend: (channel) => `${p}chat.${cfg.owner}.${cfg.actor}.${channel}`,
    channelRecvFilter: (channel) => `${p}chat.*.*.${channel}`,
  };
}

/**
 * Parse an inbound NATS message into a CotalMessage-shaped object.
 * Sender and delivery class come from the SUBJECT, never the payload.
 */
function parseInbound(cfg, natsMsg) {
  const subject = natsMsg.subject;
  const p = cfg.subjectPrefix;
  if (!subject.startsWith(p)) return null;
  const rest = subject.slice(p.length).split('.');
  let kind = null;
  let sender = null;
  let channel = null;
  if (rest[0] === 'inst' && rest.length >= 5) {
    kind = 'dm';
    sender = { owner: rest[3], actor: rest[4] };
  } else if (rest[0] === 'chat' && rest.length >= 4) {
    kind = 'channel';
    sender = { owner: rest[1], actor: rest[2] };
    channel = rest.slice(3).join('.');
  } else {
    return null; // not a subject class this connector handles
  }

  let body = {};
  let text = '';
  try {
    body = JSON.parse(sc.decode(natsMsg.data));
  } catch (_e) {
    body = { parts: [{ type: 'text', text: sc.decode(natsMsg.data) }] };
  }
  const parts = Array.isArray(body.parts) ? body.parts : [];
  text = parts.filter(pt => pt && pt.type === 'text' && typeof pt.text === 'string')
    .map(pt => pt.text).join('\n');
  if (!text && typeof body.text === 'string') text = body.text;

  return {
    id: body.id || natsMsg.headers?.get('Nats-Msg-Id') || newId(),
    ts: body.ts || Date.now(),
    space: cfg.space,
    kind,
    channel,
    from: {
      id: `${sender.owner}.${sender.actor}`,
      name: (body.from && body.from.name) || sender.actor,
      owner: sender.owner,
      actor: sender.actor,
    },
    text,
    replyTo: body.replyTo || null,
    subject,
    _jsMsg: natsMsg, // JetStream ack handle (DM durable); undefined for core subs
  };
}

function buildMessage(cfg, { text, replyTo, channel, to }) {
  const msg = {
    id: newId(),
    ts: Date.now(),
    space: cfg.space,
    from: { id: `${cfg.owner}.${cfg.actor}`, name: cfg.agentName },
    parts: [{ type: 'text', text }],
  };
  if (replyTo) msg.replyTo = replyTo;
  if (channel) msg.channel = channel;
  if (to) msg.to = to;
  return msg;
}

class MeshClient {
  constructor(cfg) {
    this.cfg = cfg;
    this.subj = subjects(cfg);
    this.nc = null;
    this.js = null;
    this.kv = null;
    this.channelQueue = [];
    this.seenIds = new Set();
  }

  async connect() {
    const nc = await connect({
      servers: this.cfg.server,
      authenticator: credsAuthenticator(Buffer.from(this.cfg.credsText, 'utf8')),
      inboxPrefix: `_INBOX_${this.cfg.nkey}`,
      name: `connector-gemini/${this.cfg.agentName}`,
    });
    this.nc = nc;
    this.js = nc.jetstream();
    this.kv = await this.js.views.kv(this.cfg.presenceBucket);
    return this;
  }

  /**
   * FAIL-BEFORE-PRESENCE: bind the pre-provisioned DM durable BEFORE any
   * presence write. Throws (no presence ghost) if the lifecycle uid is wrong.
   */
  async bindDmDurable() {
    const cons = await this.js.consumers.get(this.cfg.dmStream, this.cfg.dmDurable);
    this.dmConsumer = cons;
    return cons;
  }

  /** Subscribe to granted channel read filters; inbound lands in channelQueue. */
  async subscribeChannels() {
    for (const ch of this.cfg.subChannels) {
      const sub = this.nc.subscribe(this.subj.channelRecvFilter(ch));
      (async () => {
        for await (const m of sub) {
          const parsed = parseInbound(this.cfg, m);
          if (parsed && !this.seenIds.has(parsed.id)) {
            this.seenIds.add(parsed.id);
            this.channelQueue.push(parsed);
          }
        }
      })().catch(err => console.error(`[mesh] channel sub ${ch} ended:`, err.message));
    }
  }

  /**
   * Pull pending DMs off the durable (explicit-ack) as ONE bounded batch.
   * Uses fetch() (single batch — the iterator ENDS) rather than consume()
   * (continuous — never ends). Returned messages are UNACKED — the caller acks
   * only after the surfacing proof (API round-trip complete + reply published).
   * Crash before ack => redelivery (at-least-once).
   */
  async pullPendingDMs(maxMessages = 100, expiresMs = 2000) {
    const out = [];
    const iter = await this.dmConsumer.fetch({ max_messages: maxMessages, expires: expiresMs });
    for await (const m of iter) {
      const parsed = parseInbound(this.cfg, m);
      if (parsed && !this.seenIds.has(parsed.id)) {
        this.seenIds.add(parsed.id);
        out.push(parsed);
      } else if (parsed) {
        // duplicate delivery — safe to ack, we already hold it
        try { m.ack(); } catch (_e) { /* best-effort */ }
      }
    }
    return out;
  }

  drainChannelQueue() {
    const q = this.channelQueue;
    this.channelQueue = [];
    return q;
  }

  async publish(subject, msgObj) {
    const h = headers();
    h.set('Nats-Msg-Id', msgObj.id);
    await this.nc.publish(subject, sc.encode(JSON.stringify(msgObj)), { headers: h });
  }

  /** Reply to a DM inbound: DM the original sender, carrying replyTo. */
  async replyDM(inbound, text) {
    const msg = buildMessage(this.cfg, {
      text,
      replyTo: inbound.id,
      to: inbound.from.id,
    });
    await this.publish(
      this.subj.dmSend(inbound.from.owner, inbound.from.actor),
      msg
    );
    return msg;
  }

  /** Reply to a channel inbound: publish back to the channel (grant-checked). */
  async replyChannel(inbound, text) {
    const msg = buildMessage(this.cfg, {
      text,
      replyTo: inbound.id,
      channel: inbound.channel,
    });
    await this.publish(this.subj.channelSend(inbound.channel), msg);
    return msg;
  }

  /** Best-effort presence put. NEVER gates delivery — failures are swallowed. */
  async putPresence(status, condition) {
    try {
      const rec = {
        card: { id: this.cfg.presenceKey, name: this.cfg.agentName },
        lifecycleUid: this.cfg.lifecycleUid,
        status,
        ts: Date.now(),
      };
      if (condition) rec.condition = condition;
      await this.kv.put(this.cfg.presenceKey, sc.encode(JSON.stringify(rec)));
    } catch (err) {
      console.error('[mesh] presence write failed (non-fatal):', err.message);
    }
  }

  canPublishChannel(channel) {
    return this.cfg.pubChannels.includes(channel);
  }

  async drain() {
    try { await this.nc.drain(); } catch (_e) { /* best-effort */ }
  }
}

module.exports = { MeshClient, subjects, parseInbound, buildMessage, newId };
