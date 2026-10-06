"use strict";
/*
 * cursor-grok connector — Beads claim/release intents.
 *
 * The connector surfaces two intents (interface spec §7): claim(taskId) and
 * release(taskId), executed as `bd claim <id>` / `bd release <id>` against
 * the shared Beads store. Beads is the source of truth; this module never
 * keeps a claim registry.
 *
 * The agent signals an intent by writing a JSON file to the outbox:
 *   {"intent": "claim"|"release", "taskId": "<id>", "inReplyTo": "<msgId>"}
 * The sidecar executes it here and posts the formatted result back to the
 * mesh (see sidecar.js). A rejection is reported, never retried silently.
 *
 * NOTE (2026-10-06): the `bd` CLI is not installed on this host, so the
 * shell-out path is unit-tested with a fake `bd` (test/t6-beads-unit.js)
 * and documented as not E2E-verified. When `bd` is absent the intent
 * resolves to a graceful UNAVAILABLE result instead of crashing.
 */
const { execFile } = require("child_process");

const BD_TIMEOUT_MS = 15000;
const MAX_OUTPUT = 500;

function parseIntentFile(text) {
  let data;
  try {
    data = JSON.parse(text);
  } catch {
    throw new Error("not JSON");
  }
  if (!data || typeof data !== "object") throw new Error("not an object");
  const { intent, taskId, inReplyTo } = data;
  if (intent !== "claim" && intent !== "release") {
    throw new Error(`intent must be "claim" or "release", got ${JSON.stringify(intent)}`);
  }
  if (typeof taskId !== "string" || taskId.trim() === "") {
    throw new Error("taskId must be a non-empty string");
  }
  if (inReplyTo !== undefined && typeof inReplyTo !== "string") {
    throw new Error("inReplyTo must be a string when present");
  }
  return { intent, taskId: taskId.trim(), inReplyTo };
}

/*
 * Run `bd <intent> <taskId>`. Never throws for bd-level outcomes — every
 * failure mode resolves to a result object:
 *   ok:true                          → claimed/released
 *   status "unavailable"             → bd not on PATH
 *   status "timeout"                 → bd hung past the timeout
 *   status "rejected"                → bd ran and refused (e.g. already claimed)
 *   status "invalid-intent"/"invalid-task" → caller bug (validated earlier too)
 */
function executeIntent(intent, taskId, { timeoutMs = BD_TIMEOUT_MS } = {}) {
  return new Promise((resolve) => {
    if (intent !== "claim" && intent !== "release") {
      resolve({ ok: false, status: "invalid-intent", output: `unknown intent ${String(intent)}` });
      return;
    }
    if (typeof taskId !== "string" || taskId.trim() === "") {
      resolve({ ok: false, status: "invalid-task", output: "missing taskId" });
      return;
    }
    execFile(
      "bd",
      [intent, taskId],
      { timeout: timeoutMs, maxBuffer: 64 * 1024 },
      (err, stdout, stderr) => {
        if (err && err.code === "ENOENT") {
          resolve({
            ok: false,
            status: "unavailable",
            output: "bd CLI not on PATH — install beads to enable task claiming",
          });
          return;
        }
        if (err && err.killed) {
          resolve({
            ok: false,
            status: "timeout",
            output: `bd ${intent} ${taskId} timed out after ${timeoutMs}ms`,
          });
          return;
        }
        const output = `${stdout || ""}${stderr || ""}`.trim().slice(0, MAX_OUTPUT);
        if (err) {
          resolve({
            ok: false,
            status: "rejected",
            output: output || `bd exited with code ${err.code}`,
          });
          return;
        }
        resolve({ ok: true, status: "ok", output });
      }
    );
  });
}

/** One-line mesh-visible result, prefixed so peers can parse it. */
function formatResult(agentName, intent, taskId, result) {
  const what = intent === "claim" ? "claimed" : "released";
  switch (result.status) {
    case "ok":
      return `[BEADS] ${intent} ${taskId}: OK — ${what} by ${agentName}${result.output ? ` — ${result.output}` : ""}`;
    case "unavailable":
      return `[BEADS] ${intent} ${taskId}: UNAVAILABLE — ${result.output}`;
    case "rejected":
      return `[BEADS] ${intent} ${taskId}: REJECTED — ${result.output}`;
    case "timeout":
      return `[BEADS] ${intent} ${taskId}: TIMEOUT — ${result.output}`;
    default:
      return `[BEADS] ${intent} ${taskId}: FAILED (${result.status}) — ${result.output}`;
  }
}

module.exports = { parseIntentFile, executeIntent, formatResult, BD_TIMEOUT_MS };
