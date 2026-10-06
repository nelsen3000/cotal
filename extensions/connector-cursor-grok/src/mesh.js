/*
 * cursor-grok connector — shared mesh plumbing.
 *
 * Minimal NATS/JetStream client for the grok agent's mesh identity. This is
 * deliberately NOT a reimplementation of @cotal-ai/core's MeshAgent: it covers
 * only what the Cursor stop-hook and sidecar need (connect, bind the
 * pre-provisioned DM durable, pull, ack, publish with Nats-Msg-Id, presence KV).
 *
 * Subject rules are copied from the Cotal wire contract (see cotal-structure-study):
 *   DM:      cotal.<space>.inst.<recipOwner>.<recipActor>.<sndOwner>.<sndActor>
 *   Channel: cotal.<space>.chat.<owner>.<actor>.<channel...>
 *   Presence KV bucket: cotal_presence_<space>
 *
 * Secrets: the creds file path is the ONLY secret input and it arrives via
 * ARENA_CREDS_PATH. The seed inside the file is never logged or printed.
 */
"use strict";

const fs = require("fs");
const os = require("os");
const path = require("path");
const { connect, credsAuthenticator, headers } = require("nats");

const NKEY_RE = /^[A-Z0-9]{56}$/;

function log(...args) {
  // Never stdout — the stop-hook owns stdout. Sidecar also keeps stdout clean-ish.
  process.stderr.write(`[cursor-grok] ${args.join(" ")}\n`);
}

/** Decode the JWT inside a NATS creds file. Returns { bytes, payload }. */
function parseCreds(credsPath) {
  const bytes = fs.readFileSync(credsPath); // Buffer: also the authenticator input
  const raw = bytes.toString("utf8");
  const lines = raw.split("\n").map((l) => l.trim()).filter(Boolean);
  const b64 = lines.filter(
    (l) => !l.startsWith("-----") && !/NKEY SEED/i.test(l)
  );
  if (b64.length === 0) throw new Error(`no JWT found in ${credsPath}`);
  const payload = JSON.parse(
    Buffer.from(b64[0].split(".")[1], "base64url").toString("utf8")
  );
  return { bytes, payload };
}

/** Pull the durable-consumer name + space out of the minted JWT's allow-list. */
function durableFromJwt(payload) {
  const allow = (payload.nats && payload.nats.pub && payload.nats.pub.allow) || [];
  const dmInfo = allow.find((s) => s.includes("$JS.API.CONSUMER.INFO.DM_"));
  if (!dmInfo) throw new Error("creds JWT has no DM durable grant");
  // e.g. $JS.API.CONSUMER.INFO.DM_main.dm_local-<nkey>-<uid>
  const m = dmInfo.match(/\$JS\.API\.CONSUMER\.INFO\.(DM_[^.]+)\.(dm_.+)$/);
  if (!m) throw new Error(`cannot parse DM durable from grant: ${dmInfo}`);
  return { dmStream: m[1], dmDurable: m[2] };
}

/** Split dm_<owner>-<actor>-<uid> using the known nkey as the anchor. */
function splitDurable(dmDurable, nkey) {
  const rest = dmDurable.replace(/^dm_/, "");
  const i = rest.indexOf(nkey);
  if (i < 0) throw new Error(`nkey not found in durable name ${dmDurable}`);
  const owner = rest.slice(0, i - 1); // strip trailing '-'
  const uid = rest.slice(i + nkey.length + 1); // strip leading '-'
  if (!owner || !uid) throw new Error(`cannot split durable name ${dmDurable}`);
  return { owner, uid };
}

function spaceFromJwt(payload) {
  const allow = (payload.nats && payload.nats.pub && payload.nats.pub.allow) || [];
  const subj = allow.find((s) => s.startsWith("cotal."));
  const m = subj && subj.match(/^cotal\.([^.]+)\./);
  return m ? m[1] : "main";
}

/** Exact own presence-KV key from the minted JWT (e.g. $KV.cotal_presence_main.local.<nkey>). */
function presenceKeyFromJwt(payload) {
  const allow = (payload.nats && payload.nats.pub && payload.nats.pub.allow) || [];
  const entry = allow.find((s) => s.startsWith("$KV.cotal_presence_"));
  if (!entry) throw new Error("creds JWT has no presence KV grant");
  return entry.replace(/^\$KV\.[^.]+\./, "");
}
function resolveServerUrl(space) {
  if (process.env.ARENA_NATS_URL) return process.env.ARENA_NATS_URL;
  const regFile = path.join(
    os.homedir(),
    ".cotal",
    "meshes",
    `space.${Buffer.from(space, "utf8").toString("hex")}.json`
  );
  try {
    const rec = JSON.parse(fs.readFileSync(regFile, "utf8"));
    if (rec.server) return rec.server;
  } catch {
    /* fall through */
  }
  throw new Error(
    `no broker URL: set ARENA_NATS_URL or ensure the mesh registry has space "${space}"`
  );
}

/** Full identity + connection config derived from env + the creds file. */
function loadIdentity() {
  const credsPath = process.env.ARENA_CREDS_PATH;
  if (!credsPath) throw new Error("ARENA_CREDS_PATH is not set");
  const { bytes, payload } = parseCreds(credsPath);
  const nkey = payload.sub;
  if (!NKEY_RE.test(nkey)) throw new Error("creds JWT sub is not an nkey");
  const space = process.env.ARENA_SPACE || spaceFromJwt(payload);
  const { dmStream, dmDurable } = durableFromJwt(payload);
  const { owner, uid } = splitDurable(dmDurable, nkey);
  const agentName = process.env.ARENA_AGENT_NAME || "grok";
  return {
    credsPath,
    credsBytes: bytes, // authenticator input (Buffer); never log this
    nkey,
    space,
    owner,
    lifecycleUid: uid,
    dmStream,
    dmDurable,
    presenceBucket: `cotal_presence_${space}`,
    presenceKey: presenceKeyFromJwt(payload), // e.g. local.<nkey>
    agentName,
    serverUrl: resolveServerUrl(space),
    principal: `${owner}.${nkey}`,
  };
}

async function meshConnect(identity, { timeoutMs = 5000 } = {}) {
  const nc = await connect({
    servers: identity.serverUrl,
    name: `arena-hub/cursor-grok:${identity.agentName}`,
    // Private-inbox pattern: the minted JWT only allows _INBOX_<nkey>.>
    inboxPrefix: `_INBOX_${identity.nkey}`,
    authenticator: credsAuthenticator(identity.credsBytes),
    timeout: timeoutMs,
    maxReconnectAttempts: 2,
  });
  return nc;
}

// ---- CotalMessage helpers (wire shape: packages/core/src/types.ts) ----

function textOf(msg) {
  const parts = Array.isArray(msg.parts) ? msg.parts : [];
  const texts = parts.filter((p) => p && p.kind === "text").map((p) => p.text);
  if (texts.length) return texts.join("\n");
  const kinds = parts.map((p) => (p && p.kind) || "?").join(",");
  return `[no text parts; part kinds: ${kinds || "none"}]`;
}

function dmSubject(space, recipOwner, recipActor, sndOwner, sndActor) {
  return `cotal.${space}.inst.${recipOwner}.${recipActor}.${sndOwner}.${sndActor}`;
}

/** Parse a cotal DM subject into { recipOwner, recipActor, sndOwner, sndActor }. */
function parseDmSubject(subject, space) {
  const prefix = `cotal.${space}.inst.`;
  if (!subject.startsWith(prefix)) return null;
  const toks = subject.slice(prefix.length).split(".");
  if (toks.length !== 4) return null;
  return {
    recipOwner: toks[0],
    recipActor: toks[1],
    sndOwner: toks[2],
    sndActor: toks[3],
  };
}

function natsMsgIdHeader(id) {
  const h = headers();
  h.set("Nats-Msg-Id", id);
  return h;
}

module.exports = {
  log,
  parseCreds,
  loadIdentity,
  meshConnect,
  textOf,
  dmSubject,
  parseDmSubject,
  natsMsgIdHeader,
  presenceKeyFromJwt,
  NKEY_RE,
};
