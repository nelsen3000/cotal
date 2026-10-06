/**
 * CotalMessage wire helpers + the connector's markdown file format.
 *
 * Wire shape (packages/core/src/types.ts:360-399):
 *   { id, ts, space, from:{id,name,role?}, mentions?, parts, replyTo?,
 *     contextId?, channel? | to? | toService? }
 * Sender and delivery class come from the SUBJECT, never the payload.
 */

const { randomUUID } = require("node:crypto");

/** Build a DM CotalMessage. `to` = recipient principal key `owner.actor`. */
function buildDm({
  space,
  fromId,
  fromName,
  fromRole,
  to,
  text,
  replyTo,
  contextId,
  mentions,
}) {
  const msg = {
    id: `muse-${randomUUID()}`,
    ts: Date.now(),
    space,
    from: { id: fromId, name: fromName, ...(fromRole ? { role: fromRole } : {}) },
    parts: [{ kind: "text", text }],
    to,
  };
  if (mentions && mentions.length) msg.mentions = mentions;
  if (replyTo) msg.replyTo = replyTo;
  if (contextId) msg.contextId = contextId;
  return msg;
}

/** Build a channel CotalMessage. */
function buildChannel({
  space,
  fromId,
  fromName,
  fromRole,
  channel,
  text,
  replyTo,
  contextId,
  mentions,
}) {
  const msg = {
    id: `muse-${randomUUID()}`,
    ts: Date.now(),
    space,
    from: { id: fromId, name: fromName, ...(fromRole ? { role: fromRole } : {}) },
    parts: [{ kind: "text", text }],
    channel,
  };
  if (mentions && mentions.length) msg.mentions = mentions;
  if (replyTo) msg.replyTo = replyTo;
  if (contextId) msg.contextId = contextId;
  return msg;
}

/** Concatenate the text parts of an inbound message for the file drop. */
function messageText(msg) {
  if (!msg || !Array.isArray(msg.parts)) return "";
  return msg.parts
    .filter((p) => p && p.kind === "text" && typeof p.text === "string")
    .map((p) => p.text)
    .join("\n");
}

/** Parse a received payload into a CotalMessage; throws on malformed wire. */
function parseMessage(bytes) {
  const msg = JSON.parse(Buffer.from(bytes).toString("utf8"));
  if (!msg || typeof msg.id !== "string" || !msg.id) {
    throw new Error("wire message has no usable string id");
  }
  if (!msg.from || typeof msg.from.id !== "string" || typeof msg.from.name !== "string") {
    throw new Error(`wire message ${msg.id} has no usable from{id,name}`);
  }
  if (!Number.isFinite(msg.ts)) throw new Error(`wire message ${msg.id} has no finite ts`);
  if (!Array.isArray(msg.parts)) throw new Error(`wire message ${msg.id} has no parts array`);
  const route = msg.channel ?? msg.to ?? msg.toService;
  if (typeof route !== "string") {
    throw new Error(`wire message ${msg.id} has no channel/to/toService route`);
  }
  return msg;
}

/** Describe the route of a parsed message for the file header. */
function routeLabel(msg) {
  if (msg.channel) return { kind: "channel", value: msg.channel };
  if (msg.to) return { kind: "dm", value: msg.to };
  if (msg.toService) return { kind: "anycast", value: msg.toService };
  return { kind: "unknown", value: "?" };
}

/** Sanitize a message id for use as a filename (ids are connector-generated; still defensive). */
function safeFileName(id) {
  const s = String(id).replace(/[^A-Za-z0-9_.-]/g, "_");
  return s.length ? s : "unnamed";
}

/** Render an inbound message as the markdown file Muse reads from inbox/. */
function renderInboxFile(msg) {
  const route = routeLabel(msg);
  const fromKey = msg.from?.name ?? "?";
  const lines = [
    "---",
    `From: ${fromKey}`,
    route.kind === "channel" ? `Channel: ${route.value}` : `To: ${route.value}`,
    `Kind: ${route.kind}`,
    `Ts: ${new Date(msg.ts).toISOString()}`,
    `Id: ${msg.id}`,
  ];
  if (msg.replyTo) lines.push(`ReplyTo: ${msg.replyTo}`);
  if (msg.contextId) lines.push(`ContextId: ${msg.contextId}`);
  if (msg.mentions && msg.mentions.length) lines.push(`Mentions: ${msg.mentions.join(", ")}`);
  lines.push("---", "", messageText(msg), "");
  return lines.join("\n");
}

const HEADER_RE = /^(To|Channel|ReplyTo|ContextId|Mentions)\s*:\s*(.+)$/i;

/**
 * Parse an outbox file into { to, channel, replyTo, contextId, mentions, body }.
 * Throws with a human reason when the file is malformed — callers quarantine it.
 */
function parseOutboxFile(text, filename) {
  const lines = String(text).split("\n");
  const headers = {};
  let i = 0;
  // Skip a leading --- front-matter fence if present.
  if (lines[0] && lines[0].trim() === "---") i = 1;
  for (; i < lines.length; i++) {
    const line = lines[i];
    if (line.trim() === "---") {
      i++;
      break;
    }
    if (line.trim() === "") {
      i++;
      break;
    }
    const m = line.match(HEADER_RE);
    if (m) {
      headers[m[1].toLowerCase()] = m[2].trim();
    } else if (line.trim().length > 0) {
      // A non-header, non-blank line before the body separator = body starts here.
      break;
    }
  }
  const body = lines.slice(i).join("\n").trim();

  const to = headers.to || null;
  const channel = headers.channel || null;
  if (!to && !channel) {
    throw new Error(
      `malformed outbox file ${filename}: needs a "To: <owner>.<actor>" or "Channel: <name>" header`
    );
  }
  if (to && channel) {
    throw new Error(
      `malformed outbox file ${filename}: "To:" and "Channel:" are mutually exclusive`
    );
  }
  if (!body) {
    throw new Error(`malformed outbox file ${filename}: empty body`);
  }
  return {
    to,
    channel,
    replyTo: headers.replyto || null,
    contextId: headers.contextid || null,
    mentions: headers.mentions
      ? headers.mentions.split(",").map((s) => s.trim()).filter(Boolean)
      : undefined,
    body,
  };
}

module.exports = { buildDm, buildChannel, messageText, parseMessage, routeLabel, safeFileName, renderInboxFile, HEADER_RE, parseOutboxFile };
