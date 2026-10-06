// Beads command parsing + execution for the 1min.ai connector.
//
// Beads is the source of truth for task claims; the connector only surfaces
// the intent (`/claim <id>` / `/release <id>` in a mesh message) by shelling
// out to the `bd` CLI and reporting the result back to the mesh.
//
// NOTE: `bd` is not installed on this host, so the E2E suite cannot exercise
// the live path — parsing is unit-tested (test/commands.test.js) and the
// graceful "not installed" degradation was verified manually (see
// test/evidence-2026-10-06.md).

"use strict";

const { execFileSync } = require("child_process");

const CMD_RE = /^\/(claim|release)\s+(\S+)\s*$/i;

// Returns {op:'claim'|'release', taskId} or null when the text is not a command.
function parseCommand(text) {
  const m = String(text == null ? "" : text).trim().match(CMD_RE);
  if (!m) return null;
  return { op: m[1].toLowerCase(), taskId: m[2] };
}

// Runs `bd <op> <taskId>` in beadsDir. Never throws: a rejection from Beads
// (already claimed) or a missing `bd` binary comes back as {ok:false}.
function runBeads(op, taskId, { beadsDir = process.cwd(), timeoutMs = 15000 } = {}) {
  try {
    const out = execFileSync("bd", [op, taskId], {
      cwd: beadsDir,
      timeout: timeoutMs,
      encoding: "utf8",
      stdio: ["ignore", "pipe", "pipe"],
    });
    return { ok: true, output: (out || "").trim() || `${op} ${taskId}: done` };
  } catch (e) {
    const hint = e.code === "ENOENT" ? " (bd CLI not installed here)" : "";
    return {
      ok: false,
      output: `beads ${op} ${taskId} failed${hint}: ${(e.stderr || e.message || "")
        .toString()
        .slice(0, 400)}`,
    };
  }
}

module.exports = { parseCommand, runBeads };
