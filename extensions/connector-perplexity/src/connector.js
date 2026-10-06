// Arena Hub v1 — Perplexity connector (poll-only sidecar).
//
// HONEST WAKE PATH (interface spec §5/§6):
//   inbound : poll mesh -> Perplexity API read path (scheduled, PERPLEXITY_POLL_MS)
//   wake    : none — the Perplexity API is request/response only; there is no
//             push, webhook, or agent-wake primitive to call. Pending inbound
//             messages are folded into the context of the next scheduled call.
//   steer   : false
//   ack site: the API round-trip completing (reply published, THEN the
//             JetStream DM message is acked). Channel messages are live
//             at-most-once (no ack exists on a core subscription) — documented,
//             not hidden.
//
// Mesh wire rules (from cotal-structure-study.md, minimal reimplementation):
//   - subjects:  cotal.<space>.inst.<owner>.<actor>.>            (DM receive)
//                cotal.<space>.chat.*.*.<channel>                (channel receive)
//                cotal.<space>.inst.<recipO>.<recipA>.<myO>.<myA> (DM send)
//                cotal.<space>.chat.<myO>.<myA>.<channel>         (channel send)
//   - presence:  KV bucket cotal_presence_<space>, key <owner>.<actor>,
//                heartbeat 2000ms / TTL 6000ms (bucket default)
//   - publish dedup: Nats-Msg-Id header
//   - DM durable: pre-provisioned dm_<owner>-<actor>-<uid> on DM_<space>,
//                 bound (never created) BEFORE first presence write.

"use strict";

const { execFileSync } = require("child_process");
const crypto = require("crypto");
const fs = require("fs");
const { connect, headers, JSONCodec, StringCodec } = require("nats");
const { parseCredsFile, credsAuthFromFile } = require("./creds");
const { clientFromEnv } = require("./perplexity-client");

const jc = JSONCodec();
const sc = StringCodec();
const log = (...a) => console.log(new Date().toISOString(), "[perplexity]", ...a);
const err = (...a) => console.error(new Date().toISOString(), "[perplexity]", ...a);

const env = (k, d) => (process.env[k] !== undefined && process.env[k] !== "" ? process.env[k] : d);
const envInt = (k, d) => {
  const v = parseInt(env(k, ""), 10);
  return Number.isFinite(v) && v > 0 ? v : d;
};

const CONFIG = {
  credsPath: env("COTAL_CREDS", ""),
  server: env("COTAL_SERVER", "nats://127.0.0.1:4222"),
  channels: env("COTAL_CHANNELS", "general").split(",").map((s) => s.trim()).filter(Boolean),
  pollMs: envInt("PERPLEXITY_POLL_MS", 30000),
  presenceMs: envInt("PERPLEXITY_PRESENCE_MS", 2000),
  model: env("PERPLEXITY_MODEL", "sonar"),
  maxTokens: envInt("PERPLEXITY_MAX_TOKENS", 1024),
  timeoutMs: envInt("PERPLEXITY_TIMEOUT_MS", 60000),
  systemPrompt: env(
    "PERPLEXITY_SYSTEM_PROMPT",
    "You are the Perplexity research agent on the Arena Hub mesh. " +
      "Answer concisely and cite sources when the search results provide them. " +
      "Each user message is prefixed with the mesh sender's identity."
  ),
  beadsDir: env("BEADS_DIR", process.cwd()),
  lifecycleUid: env("COTAL_LIFECYCLE_UID", ""),
};

if (!CONFIG.credsPath) {
  err("FATAL: COTAL_CREDS is required (path to `cotal mint`-ed creds file)");
  process.exit(2);
}
fs.accessSync(CONFIG.credsPath, fs.constants.R_OK);

const ident = parseCredsFile(CONFIG.credsPath);
const ME = { owner: ident.owner, actor: ident.actor };
const UID = CONFIG.lifecycleUid || (ident.durable ? ident.durable.split("-").pop() : "");
const SPACE = ident.space;
const ROOT = `cotal.${SPACE}`;
const PRESENCE_BUCKET = `cotal_presence_${SPACE}`;
const PRESENCE_KEY = `${ME.owner}.${ME.actor}`;

const dmRecvFilter = `${ROOT}.inst.${ME.owner}.${ME.actor}.>`;
const chatRecvFilter = (ch) => `${ROOT}.chat.*.*.${ch}`;
const dmSendSubject = (o, a) => `${ROOT}.inst.${o}.${a}.${ME.owner}.${ME.actor}`;
const chatSendSubject = (ch) => `${ROOT}.chat.${ME.owner}.${ME.actor}.${ch}`;

let nc = null;
let js = null;
let kv = null;
let api = null;
let status = "idle";
const pending = []; // {msgId, kind:'dm'|'channel', from:{owner,actor}, channel?, text, ack}
const seenIds = new Set();
const MAX_SEEN = 5000;
let shuttingDown = false;

function rememberId(id) {
  if (seenIds.has(id)) return false;
  seenIds.add(id);
  if (seenIds.size > MAX_SEEN) {
    const it = seenIds.values();
    for (let i = 0; i < 1000; i++) seenIds.delete(it.next().value);
  }
  return true;
}

// Sender/class come from the SUBJECT (parseSubject rule), never the payload.
function parseSubject(subject) {
  const t = subject.split(".");
  // cotal.<space>.inst.<recipO>.<recipA>.<sndO>.<sndA>
  if (t[2] === "inst" && t.length >= 7) {
    return {
      kind: "dm",
      recip: { owner: t[3], actor: t[4] },
      from: { owner: t[5], actor: t[6] },
    };
  }
  // cotal.<space>.chat.<pubO>.<pubA>.<channel...>
  if (t[2] === "chat" && t.length >= 6) {
    return {
      kind: "channel",
      from: { owner: t[3], actor: t[4] },
      channel: t.slice(5).join("."),
    };
  }
  return null;
}

function textOf(msg) {
  if (!msg || typeof msg !== "object") return String(msg);
  const parts = Array.isArray(msg.parts) ? msg.parts : [];
  const tp = parts.find((p) => p && (p.kind === "text" || typeof p.text === "string"));
  if (tp) return String(tp.text || "");
  if (typeof msg.text === "string") return msg.text;
  return JSON.stringify(msg).slice(0, 2000);
}

function ingest(subject, data, ackFn) {
  let msg;
  try {
    msg = jc.decode(data);
  } catch {
    msg = { parts: [{ kind: "text", text: sc.decode(data) }] };
  }
  const parsed = parseSubject(subject);
  if (!parsed) return;
  // Never ingest our own publications (our channel replies match our own
  // receive filter — without this the connector answers itself forever).
  if (parsed.from.owner === ME.owner && parsed.from.actor === ME.actor) return;
  const msgId = (msg && msg.id) || crypto.randomUUID();
  if (!rememberId(msgId)) return; // idempotent handler: dedup on message id
  pending.push({
    msgId,
    kind: parsed.kind,
    from: parsed.from,
    channel: parsed.channel,
    text: textOf(msg),
    ack: ackFn || null,
  });
  log(`queued ${parsed.kind} ${msgId} from ${parsed.from.owner}.${parsed.from.actor}`);
}

async function publish(toSubject, body) {
  const h = headers();
  h.append("Nats-Msg-Id", body.id);
  await nc.publish(toSubject, jc.encode(body), { headers: h });
}

function makeReply(to, text, replyTo) {
  return {
    id: crypto.randomUUID(),
    ts: new Date().toISOString(),
    space: SPACE,
    from: { id: `${ME.owner}.${ME.actor}`, name: "perplexity", role: "agent" },
    parts: [{ kind: "text", text }],
    replyTo,
    ...(to.kind === "dm" ? { to: { owner: to.from.owner, actor: to.from.actor } } : { channel: to.channel }),
  };
}

// --- Beads: /claim + /release. Beads is the source of truth; the connector
// only surfaces the intent and reports the result back to the mesh.
const CMD_RE = /^\/(claim|release)\s+(\S+)/i;
function runBeads(op, taskId) {
  try {
    const out = execFileSync("bd", [op === "claim" ? "claim" : "release", taskId], {
      cwd: CONFIG.beadsDir,
      timeout: 15000,
      encoding: "utf8",
      stdio: ["ignore", "pipe", "pipe"],
    });
    return { ok: true, output: (out || "").trim() || `${op} ${taskId}: done` };
  } catch (e) {
    const hint = e.code === "ENOENT" ? " (bd CLI not installed here)" : "";
    return {
      ok: false,
      output: `beads ${op} ${taskId} failed${hint}: ${(e.stderr || e.message || "").toString().slice(0, 400)}`,
    };
  }
}

async function setStatus(s) {
  status = s;
  await putPresence();
}

async function putPresence() {
  if (!kv) return;
  try {
    await kv.put(
      PRESENCE_KEY,
      jc.encode({
        card: { id: PRESENCE_KEY, name: "perplexity", role: "agent" },
        lifecycleUid: UID,
        status,
        ts: new Date().toISOString(),
        connector: "perplexity",
        version: "1.0.0",
      })
    );
  } catch (e) {
    // Presence writes are best-effort and NEVER gate delivery.
    err("presence put failed (non-fatal):", e.message);
  }
}

async function pollTick() {
  if (shuttingDown || pending.length === 0) return;

  // 1. Beads commands never go to the model — they go to `bd`.
  const rest = [];
  for (const item of pending.splice(0)) {
    const m = item.text.trim().match(CMD_RE);
    if (m) {
      const [, op, taskId] = m;
      log(`beads ${op} ${taskId} from ${item.from.owner}.${item.from.actor}`);
      const res = runBeads(op.toLowerCase(), taskId);
      const replyText = res.ok
        ? `beads: ${res.output}`
        : `beads: REJECTED — ${res.output}`;
      await publish(
        item.kind === "dm" ? dmSendSubject(item.from.owner, item.from.actor) : chatSendSubject(item.channel),
        makeReply(item, replyText, item.msgId)
      );
      if (item.ack) { try { item.ack(); } catch (e) { err("ack failed:", e.message); } }
    } else {
      rest.push(item);
    }
  }
  pending.push(...rest);
  if (pending.length === 0) return;

  // 2. Scheduled API round-trip: fold all pending inbound into one call.
  await setStatus("working");
  const batch = pending.splice(0);
  const messages = batch.map((b) => ({
    role: "user",
    content: `[mesh:${b.from.owner}.${b.from.actor} id:${b.msgId}] ${b.text}`,
  }));
  log(`API round-trip: ${batch.length} pending message(s) -> ${CONFIG.model}`);
  let result;
  try {
    result = await api.chat({
      system: CONFIG.systemPrompt,
      messages,
      model: CONFIG.model,
      maxTokens: CONFIG.maxTokens,
      timeoutMs: CONFIG.timeoutMs,
    });
  } catch (e) {
    // Do NOT ack: the messages redeliver after ack_wait and are retried.
    // Nothing is published for a failed round-trip (no half-answers).
    err("API round-trip FAILED, leaving", batch.length, "un-acked for redelivery:", e.message);
    pending.unshift(...batch);
    await setStatus("idle");
    return;
  }

  const replyText =
    result.text +
    (result.citations && result.citations.length
      ? "\n\nSources:\n" + result.citations.map((c) => `- ${c}`).join("\n")
      : "");
  for (const item of batch) {
    const subject =
      item.kind === "dm"
        ? dmSendSubject(item.from.owner, item.from.actor)
        : chatSendSubject(item.channel);
    await publish(subject, makeReply(item, replyText, item.msgId));
    // ACK SITE: the JetStream DM message is acked ONLY after the reply is on
    // the mesh. This is the surfacing proof — ack-at-receipt is forbidden.
    if (item.ack) {
      try { item.ack(); } catch (e) { err("ack failed:", e.message); }
    }
  }
  log(`API round-trip complete: replied to ${batch.length}, acked`);
  await setStatus("idle");
}

async function main() {
  api = require("./perplexity-client").clientFromEnv();
  log(`connecting to ${CONFIG.server} as ${ME.owner}.${ME.actor} (space ${SPACE})`);
  nc = await connect({
    servers: CONFIG.server,
    authenticator: credsAuthFromFile(CONFIG.credsPath),
    // The minted JWT pins core subscriptions to `_INBOX_<nkey>.>` — the
    // client's inbox prefix must match or JetStream/API calls are refused.
    inboxPrefix: `_INBOX_${ME.actor}`,
  });
  js = nc.jetstream();

  // FAIL-BEFORE-PRESENCE: bind durable consumers BEFORE publishing presence.
  if (!ident.durable) throw new Error("no pre-provisioned DM durable in creds (mint with --provision)");
  const consumer = await js.consumers.get(ident.dmStream, ident.durable);
  log(`bound DM durable ${ident.dmStream}/${ident.durable}`);
  const dmIter = await consumer.consume();
  (async () => {
    for await (const m of dmIter) {
      if (shuttingDown) break;
      try {
        ingest(m.subject, m.data, () => m.ack());
      } catch (e) {
        err("dm ingest failed:", e.message);
      }
    }
  })().catch((e) => err("dm consumer loop ended:", e.message));

  for (const ch of CONFIG.channels) {
    const sub = nc.subscribe(chatRecvFilter(ch));
    log(`subscribed channel filter ${chatRecvFilter(ch)}`);
    (async () => {
      for await (const m of sub) {
        if (shuttingDown) break;
        try { ingest(m.subject, m.data, null); } catch (e) { err("chat ingest failed:", e.message); }
      }
    })().catch((e) => err("chat loop ended:", e.message));
  }

  kv = await js.views.kv(PRESENCE_BUCKET);
  await putPresence(); // first presence only AFTER consumers are bound
  log(`presence up at ${PRESENCE_BUCKET}/${PRESENCE_KEY}`);
  setInterval(putPresence, CONFIG.presenceMs).unref();
  setInterval(() => pollTick().catch((e) => err("poll tick failed:", e.message)), CONFIG.pollMs).unref();

  const shutdown = async (sig) => {
    if (shuttingDown) return;
    shuttingDown = true;
    log(`received ${sig}, going offline`);
    try {
      status = "offline";
      await putPresence();
      await nc.drain();
    } catch (e) { /* best effort */ }
    process.exit(0);
  };
  process.on("SIGINT", () => shutdown("SIGINT"));
  process.on("SIGTERM", () => shutdown("SIGTERM"));

  log(`ready. poll=${CONFIG.pollMs}ms presence=${CONFIG.presenceMs}ms channels=[${CONFIG.channels.join(",")}] model=${CONFIG.model}`);
}

main().catch((e) => {
  err("FATAL:", e.message);
  process.exit(1);
});
