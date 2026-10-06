#!/usr/bin/env node
/*
 * cursor-grok connector — mesh sidecar.
 *
 * Long-running process holding the grok agent's mesh identity. It does three
 * things and nothing else:
 *
 *   1. Presence: heartbeat every 2000 ms into KV cotal_presence_<space>
 *      (bucket TTL 6000 ms, per the Cotal contract). Status mapping is
 *      honest: "idle" while the sidecar is up (the agent is reachable on the
 *      next Cursor turn / stop-hook fire), "offline" on shutdown. Cursor
 *      gives us no mid-turn signal, so "working"/"waiting" are NOT invented
 *      here (see README.md).
 *   2. Fail-before-presence: the pre-provisioned DM durable
 *      (dm_<owner>-<actor>-<uid>) is info-checked BEFORE the first presence
 *      write. A launch with the wrong lifecycle uid dies with no ghost.
 *      Presence writes are best-effort and never gate delivery.
 *   3. Outbound: watches <stateDir>/outbox/*.md. The Grok agent (instructed
 *      via Cursor rules) drops one plain-text reply file per inbound
 *      message id. The sidecar publishes each as a DM (or channel post)
 *      with replyTo set and Nats-Msg-Id for idempotent publish, then moves
 *      the file to sent/. Malformed/unroutable files go to quarantine/.
 *
 * v1 SCOPE (documented, not faked): outbound is file-pickup only. The
 * stop-hook has no access to the agent's reply text (the Cursor stop-hook
 * input carries no transcript), so replies CANNOT ride the hook return.
 * The companion-watcher path below is the entire outbound mechanism.
 *
 * Beads: claim/release intents are agent-executed — Grok runs
 * `bd claim <id>` / `bd release <id>` in its own shell and reports the
 * result; the connector posts results to the mesh via the outbox path.
 * The sidecar never shells out to bd itself.
 *
 * Env: same as stop-hook.js (ARENA_CREDS_PATH required, ARENA_NATS_URL,
 * ARENA_SPACE, ARENA_AGENT_NAME, ARENA_STATE_DIR).
 */
"use strict";

const fs = require("fs");
const os = require("os");
const path = require("path");
const {
  log,
  loadIdentity,
  meshConnect,
  dmSubject,
  natsMsgIdHeader,
} = require("./mesh");
const beads = require("./beads");

const HEARTBEAT_MS = 2000;
const OUTBOX_POLL_MS = 1000;

function stateDir() {
  return (
    process.env.ARENA_STATE_DIR ||
    path.join(os.homedir(), ".arena-hub", "cursor-grok")
  );
}

function ensureDirs(dir) {
  for (const sub of ["", "outbox", "sent", "quarantine"]) {
    fs.mkdirSync(path.join(dir, sub), { recursive: true, mode: 0o700 });
  }
}

function readJson(file, fallback) {
  try {
    return JSON.parse(fs.readFileSync(file, "utf8"));
  } catch {
    return fallback;
  }
}

function writeJsonAtomic(file, obj) {
  const tmp = `${file}.tmp.${process.pid}`;
  fs.writeFileSync(tmp, JSON.stringify(obj, null, 2), { mode: 0o600 });
  fs.renameSync(tmp, file);
}

function presenceRecord(identity, status) {
  return {
    card: { id: identity.nkey, name: identity.agentName, role: "agent" },
    lifecycleUid: identity.lifecycleUid,
    status,
    condition: null,
    environment: "arena-hub/cursor-grok",
    activity: "mesh sidecar: stop-hook inbox pull + file outbox",
    statusSince: Date.now(),
    activeAt: Date.now(),
    attention: "closed",
    channelModes: {},
    ts: Date.now(),
  };
}

async function publishCotal(nc, identity, subject, payload, natsMsgId) {
  const js = nc.jetstream();
  await js.publish(subject, Buffer.from(JSON.stringify(payload), "utf8"), {
    headers: natsMsgIdHeader(natsMsgId),
  });
  log(`published ${payload.id} -> ${subject}`);
}

function cotalEnvelope(identity, id, text) {
  return {
    id,
    ts: Date.now(),
    space: identity.space,
    from: { id: identity.nkey, name: identity.agentName, role: "agent" },
    parts: [{ kind: "text", text }],
  };
}

async function publishReply(nc, identity, msgId, text, route) {
  const payload = cotalEnvelope(identity, `reply-${msgId}`, text);
  payload.replyTo = msgId;
  let subject;
  if (route.route === "dm") {
    if (!route.fromOwner || !route.fromActor) {
      throw new Error(`no sender principal recorded for ${msgId}`);
    }
    payload.to = `${route.fromOwner}.${route.fromActor}`;
    subject = dmSubject(
      identity.space,
      route.fromOwner,
      route.fromActor,
      identity.owner,
      identity.nkey
    );
  } else if (route.route === "channel") {
    payload.channel = route.channel;
    subject =
      `cotal.${identity.space}.chat.` +
      `${identity.owner}.${identity.nkey}.${route.channel}`;
  } else {
    throw new Error(`unknown route for ${msgId}: ${route.route}`);
  }
  await publishCotal(nc, identity, subject, payload, `reply-${msgId}`);
}

/*
 * Beads intent file: execute `bd` and post the result back. The result goes
 * to the original requester (DM, replyTo = the message that asked for the
 * claim) when inReplyTo resolves via pending.json; otherwise it is logged
 * and archived (v1 has no default broadcast channel — documented).
 */
async function handleClaimFile(nc, identity, dir, file, pending) {
  const full = path.join(dir, "outbox", file);
  const text = fs.readFileSync(full, "utf8");
  const { intent, taskId, inReplyTo } = beads.parseIntentFile(text); // throws -> quarantine
  const result = await beads.executeIntent(intent, taskId);
  const line = beads.formatResult(identity.agentName, intent, taskId, result);
  log(line);

  const route = inReplyTo ? pending[inReplyTo] : null;
  if (route && route.route === "dm" && route.fromOwner && route.fromActor) {
    const payload = cotalEnvelope(
      identity,
      `beads-${intent}-${taskId}-${Date.now()}`,
      line
    );
    payload.replyTo = inReplyTo;
    payload.to = `${route.fromOwner}.${route.fromActor}`;
    const subject = dmSubject(
      identity.space,
      route.fromOwner,
      route.fromActor,
      identity.owner,
      identity.nkey
    );
    await publishCotal(
      nc,
      identity,
      subject,
      payload,
      `beads-${intent}-${taskId}-${Date.now()}`
    );
    delete pending[inReplyTo];
  } else if (route && route.route === "channel" && route.channel) {
    const payload = cotalEnvelope(
      identity,
      `beads-${intent}-${taskId}-${Date.now()}`,
      line
    );
    payload.replyTo = inReplyTo;
    payload.channel = route.channel;
    const subject =
      `cotal.${identity.space}.chat.` +
      `${identity.owner}.${identity.nkey}.${route.channel}`;
    await publishCotal(
      nc,
      identity,
      subject,
      payload,
      `beads-${intent}-${taskId}-${Date.now()}`
    );
    delete pending[inReplyTo];
  } else {
    log(`beads result for ${taskId} has no routable requester; archived without mesh post`);
  }
}

async function drainOutbox(nc, identity, dir) {
  const outbox = path.join(dir, "outbox");
  const sent = path.join(dir, "sent");
  const quarantine = path.join(dir, "quarantine");
  const pendingFile = path.join(dir, "pending.json");
  let files;
  try {
    files = fs.readdirSync(outbox);
  } catch {
    return;
  }
  if (files.length === 0) return;
  const pending = readJson(pendingFile, {});
  let dirty = false;
  for (const file of files) {
    const full = path.join(outbox, file);
    // Beads claim/release intent files.
    if (file.endsWith(".claim.json")) {
      try {
        await handleClaimFile(nc, identity, dir, file, pending);
        fs.renameSync(full, path.join(sent, file));
        dirty = true;
      } catch (e) {
        log(`claim ${file}: ${e.message} -> quarantine`);
        try {
          fs.renameSync(full, path.join(quarantine, file));
        } catch {
          /* ignore */
        }
      }
      continue;
    }
    // Plain-text reply files.
    if (!file.endsWith(".md")) continue;
    const msgId = file.slice(0, -3);
    try {
      const text = fs.readFileSync(full, "utf8").trim();
      const route = pending[msgId];
      if (!text) throw new Error("empty reply file");
      if (!route) throw new Error("no pending inbound with this id");
      await publishReply(nc, identity, msgId, text, route);
      fs.renameSync(full, path.join(sent, file));
      delete pending[msgId];
      dirty = true;
    } catch (e) {
      log(`outbox ${file}: ${e.message} -> quarantine`);
      try {
        fs.renameSync(full, path.join(quarantine, file));
      } catch {
        /* ignore */
      }
    }
  }
  if (dirty) writeJsonAtomic(pendingFile, pending);
}

async function main() {
  const identity = loadIdentity(); // throws before any presence on bad config
  const dir = stateDir();
  ensureDirs(dir);
  log(`agent=${identity.agentName} principal=${identity.principal} space=${identity.space}`);
  log(`broker=${identity.serverUrl} state=${dir}`);

  const nc = await meshConnect(identity);
  const js = nc.jetstream();

  // FAIL-BEFORE-PRESENCE: prove the lifecycle-keyed durable binds first.
  try {
    await js.consumers.get(identity.dmStream, identity.dmDurable);
    log(`bound DM durable ${identity.dmDurable}`);
  } catch (e) {
    log(`FATAL: cannot bind DM durable ${identity.dmDurable}: ${e.message}`);
    await nc.close().catch(() => {});
    process.exit(1); // die with no presence ghost
  }

  const kv = await js.views.kv(identity.presenceBucket);
  const beat = async (status) => {
    try {
      await kv.put(
        identity.presenceKey,
        Buffer.from(JSON.stringify(presenceRecord(identity, status)), "utf8")
      );
    } catch (e) {
      log(`presence write failed (best-effort): ${e.message}`);
    }
  };

  await beat("idle");
  const heartbeat = setInterval(() => beat("idle"), HEARTBEAT_MS);
  const watcher = setInterval(
    () => drainOutbox(nc, identity, dir).catch((e) => log(`outbox: ${e.message}`)),
    OUTBOX_POLL_MS
  );
  log("sidecar up: presence heartbeat 2s/6s, outbox watcher 1s");

  let stopping = false;
  const shutdown = async (sig) => {
    if (stopping) return;
    stopping = true;
    log(`received ${sig}; writing offline presence and exiting`);
    clearInterval(heartbeat);
    clearInterval(watcher);
    await beat("offline").catch(() => {});
    await nc.close().catch(() => {});
    process.exit(0);
  };
  process.on("SIGINT", () => shutdown("SIGINT"));
  process.on("SIGTERM", () => shutdown("SIGTERM"));
}

main().catch((e) => {
  log(`fatal: ${e && e.message}`);
  process.exit(1);
});
