#!/usr/bin/env node
/*
 * cursor-grok connector — Cursor `stop` hook.
 *
 * Invoked by Cursor at every agent-turn end (JSON on stdin, JSON on stdout).
 * Pulls pending inbound mesh messages for the grok agent (DM durable +
 * subscribed channels) and returns them as `followup_message`, which Cursor
 * auto-submits as the next user message.
 *
 * Verified contract (https://cursor.com/docs/hooks, "stop" section):
 *   - stdin:  JSON hook input ({conversation_id, generation_id,
 *             hook_event_name, loop_count, ...})
 *   - stdout: JSON; {"followup_message": "<text>"} continues the turn loop.
 *             When provided and non-empty, Cursor submits it as the next
 *             user message. `{}` (or no followup_message) ends the turn.
 *   - exit 0: hook succeeded, output is used. Any failure below is fail-open:
 *             we print `{}` and exit 0 so the agent turn is never broken by
 *             mesh trouble.
 *   - loop_limit (hooks.json, default 5, null = uncapped): after this many
 *     consecutive auto-followups Cursor stops submitting them. The example
 *     hooks.json sets "loop_limit": null — REQUIRED, otherwise sustained
 *     inbound stalls and acked messages would never surface.
 *
 * Ack site (per the connector interface spec §6): a message is acked only
 * after its followup payload has been written to stdout — the hook's proof
 * the turn received it. Ack-at-format-time is silent loss and is forbidden.
 * Ordering inside this script: compose payload -> write stdout -> ack.
 * If the process dies between stdout and ack, the message redelivers on the
 * next pull (at-least-once; every block carries its id for dedup).
 *
 * Idle agents: the stop hook only fires at turn end, so there is NO true
 * push. An idle agent picks these messages up on its next turn / hook fire.
 * That boundary is documented in README.md — it is not faked here.
 *
 * Env:
 *   ARENA_CREDS_PATH  path to the minted grok creds file (0600). REQUIRED.
 *   ARENA_NATS_URL    broker URL. Falls back to the local mesh registry.
 *   ARENA_SPACE       mesh space (default derived from creds, usually "main").
 *   ARENA_AGENT_NAME  display name for presence/from (default "grok").
 *   ARENA_STATE_DIR   state dir (default ~/.arena-hub/cursor-grok).
 *   ARENA_CHANNELS    comma-separated subscribed channels to pull (optional;
 *                     the agent needs channel grants minted for these).
 */
"use strict";

const fs = require("fs");
const os = require("os");
const path = require("path");
const {
  log,
  loadIdentity,
  meshConnect,
  textOf,
  parseDmSubject,
} = require("./mesh");

const MAX_MESSAGES = 25;
const MAX_TEXT_CHARS = 6000;
const FETCH_EXPIRES_MS = 2000;

function readStdin() {
  return new Promise((resolve) => {
    if (process.stdin.isTTY) return resolve("");
    let data = "";
    process.stdin.setEncoding("utf8");
    process.stdin.on("data", (c) => (data += c));
    process.stdin.on("end", () => resolve(data));
  });
}

function stateDir() {
  return process.env.ARENA_STATE_DIR || path.join(os.homedir(), ".arena-hub", "cursor-grok");
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

function trunc(text, n) {
  if (text.length <= n) return text;
  return text.slice(0, n) + `\n[truncated — ${text.length - n} more chars]`;
}

function fmtTs(ts) {
  try {
    return new Date(ts).toISOString();
  } catch {
    return String(ts);
  }
}

function formatBlock(m) {
  const route =
    m.route === "dm"
      ? `DM from ${m.fromName}`
      : `#${m.channel} from ${m.fromName}`;
  return `--- ${m.id} (${route} @ ${fmtTs(m.ts)})\n${trunc(m.text, MAX_TEXT_CHARS)}`;
}

function buildFollowup(messages, outboxDir) {
  const n = messages.length;
  const header = [
    `[MESH INBOX — ${n} new message(s) via Arena Hub]`,
    `Reply to a message by writing your plain-text reply to:`,
    `  ${path.join(outboxDir, "<message-id>.md")}`,
    `One file per message id. The connector forwards each reply to the mesh`,
    `with replyTo set, then archives it. (No mid-turn steer exists for Cursor;`,
    `this hook-pull at turn end is the delivery path.)`,
    ``,
  ].join("\n");
  return header + messages.map(formatBlock).join("\n\n") + "\n";
}

async function pullDMs(js, identity) {
  const consumer = await js.consumers.get(identity.dmStream, identity.dmDurable);
  const out = [];
  const iter = await consumer.fetch({
    max_messages: MAX_MESSAGES,
    expires: FETCH_EXPIRES_MS,
  });
  for await (const m of iter) {
    let payload = null;
    try {
      payload = JSON.parse(Buffer.from(m.data).toString("utf8"));
    } catch {
      // Not a CotalMessage: cannot be surfaced, so per the ack rule it is
      // NOT acked — it will be re-fetched (and re-logged) on the next pull
      // rather than silently dropped.
      log(`skipping non-JSON DM on ${m.subject}; leaving unacked`);
      out.push({ poison: true, ack: () => {} });
      continue;
    }
    const parsed = parseDmSubject(m.subject, identity.space);
    out.push({
      id: payload.id || `unknown-${Date.now()}`,
      ts: payload.ts || Date.now(),
      route: "dm",
      channel: null,
      fromName:
        (payload.from && payload.from.name) ||
        (parsed ? `${parsed.sndOwner}.${parsed.sndActor}` : "unknown"),
      fromOwner: parsed ? parsed.sndOwner : null,
      fromActor: parsed ? parsed.sndActor : null,
      subject: m.subject,
      text: textOf(payload),
      raw: payload,
      ack: () => m.ack(),
    });
  }
  return out;
}

/*
 * Channel pull via the single pinned history consumer the creds allow:
 * chathist_<owner>-<nkey>-<uid> on CHAT_<space>. Ephemeral per hook run;
 * the resume cursor lives in the state file (at-least-once: the cursor only
 * advances after the followup payload is on stdout).
 */
async function pullChannels(nc, identity, dir) {
  const channels = (process.env.ARENA_CHANNELS || "")
    .split(",")
    .map((s) => s.trim())
    .filter(Boolean);
  if (channels.length === 0) return [];
  const js = nc.jetstream();
  const jsm = await nc.jetstreamManager();
  const stream = `CHAT_${identity.space}`;
  const consumerName = `chathist_${identity.owner}-${identity.nkey}-${identity.lifecycleUid}`;
  const cursorFile = path.join(dir, "channel-cursors.json");
  const cursors = readJson(cursorFile, {});
  const out = [];
  const newCursors = { ...cursors };

  for (const channel of channels) {
    try {
      try {
        await jsm.consumers.delete(stream, consumerName);
      } catch {
        /* absent — fine */
      }
      const cfg = {
        name: consumerName,
        durable_name: consumerName,
        filter_subject: `cotal.${identity.space}.chat.*.*.${channel}`,
        ack_policy: "explicit",
      };
      if (cursors[channel]) {
        cfg.opt_start_seq = cursors[channel] + 1;
        cfg.deliver_policy = "by_start_sequence";
      } else cfg.deliver_policy = "new"; // no backfill before first hook run
      // NOTE: nats.js 2.x exposes consumer create/delete only on the
      // JetStreamManager (js.consumers has get/fetch only), and the signature
      // is add(stream, cfg) — the consumer name rides in cfg.name. The minted
      // JWT pins CREATE to this exact consumer name + filter (filter-token
      // pin), so the broker bounds the read to the granted channel.
      await jsm.consumers.add(stream, cfg);
      const consumer = await js.consumers.get(stream, consumerName);
      const iter = await consumer.fetch({
        max_messages: MAX_MESSAGES,
        expires: FETCH_EXPIRES_MS,
      });
      let maxSeq = cursors[channel] || 0;
      for await (const m of iter) {
        const seq = m.info.streamSequence || 0;
        if (seq > maxSeq) maxSeq = seq;
        let payload = null;
        try {
          payload = JSON.parse(Buffer.from(m.data).toString("utf8"));
        } catch {
          continue; // cursor still advances past poison below
        }
        out.push({
          id: payload.id || `unknown-${Date.now()}`,
          ts: payload.ts || Date.now(),
          route: "channel",
          channel,
          fromName:
            (payload.from && payload.from.name) || "unknown",
          fromOwner: null, // replies go back to the channel, not the sender
          fromActor: null,
          subject: m.subject,
          text: textOf(payload),
          raw: payload,
          ack: () => {}, // cursor advance IS the ack for channels
        });
      }
      try {
        await jsm.consumers.delete(stream, consumerName);
      } catch {
        /* best effort */
      }
      if (maxSeq > (cursors[channel] || 0)) newCursors[channel] = maxSeq;
    } catch (e) {
      log(`channel pull failed for #${channel}: ${e.message}`);
    }
  }
  writeJsonAtomic(cursorFile, newCursors);
  return out;
}

async function main() {
  const stdinRaw = await readStdin();
  let hookInput = {};
  try {
    hookInput = stdinRaw.trim() ? JSON.parse(stdinRaw) : {};
  } catch {
    log("stdin was not JSON; continuing with empty hook input");
  }
  const loopCount =
    typeof hookInput.loop_count === "number" ? hookInput.loop_count : 0;
  if (loopCount >= 4) {
    log(
      `WARNING: loop_count=${loopCount} is near Cursor's default loop_limit (5). ` +
        `Set "loop_limit": null for this hook or followups will stop being submitted.`
    );
  }

  let identity;
  try {
    identity = loadIdentity();
  } catch (e) {
    log(`config error: ${e.message}`);
    process.stdout.write("{}\n");
    return;
  }
  const dir = stateDir();
  try {
    ensureDirs(dir);
  } catch (e) {
    log(`state dir error: ${e.message}`);
    process.stdout.write("{}\n");
    return;
  }

  let nc = null;
  try {
    nc = await meshConnect(identity);
    const js = nc.jetstream();
    const dmMessages = await pullDMs(js, identity);
    const chMessages = await pullChannels(nc, identity, dir);
    const messages = [...dmMessages, ...chMessages]
      .filter((m) => !m.poison)
      .sort((a, b) => a.ts - b.ts)
      .slice(0, MAX_MESSAGES);

    if (messages.length === 0) {
      process.stdout.write("{}\n"); // empty inbox: end the turn quietly
      return;
    }

    // Record reply routing BEFORE emitting, so the sidecar can route even
    // if this process dies right after stdout.
    const pendingFile = path.join(dir, "pending.json");
    const pending = readJson(pendingFile, {});
    for (const m of messages) {
      pending[m.id] = {
        route: m.route,
        channel: m.channel,
        fromOwner: m.fromOwner,
        fromActor: m.fromActor,
        fromName: m.fromName,
        subject: m.subject,
        ts: m.ts,
      };
    }
    writeJsonAtomic(pendingFile, pending);

    const followup = buildFollowup(messages, path.join(dir, "outbox"));

    // SURFACING PROOF: stdout first, then ack. Never the reverse.
    fs.writeSync(1, JSON.stringify({ followup_message: followup }) + "\n");

    for (const m of messages) {
      try {
        m.ack();
      } catch (e) {
        log(`ack failed for ${m.id}: ${e.message} (will redeliver)`);
      }
    }
    log(`delivered ${messages.length} message(s) as followup_message`);
  } catch (e) {
    log(`mesh error: ${e.message}`);
    process.stdout.write("{}\n");
  } finally {
    if (nc) {
      try {
        await nc.close();
      } catch {
        /* ignore */
      }
    }
  }
}

main().then(
  () => process.exit(0),
  (e) => {
    log(`fatal: ${e && e.message}`);
    try {
      process.stdout.write("{}\n");
    } catch {
      /* ignore */
    }
    process.exit(0); // fail-open: never break the agent turn
  }
);
