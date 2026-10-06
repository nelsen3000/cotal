'use strict';
/**
 * beads.js — Beads task-claim handoff.
 *
 * Cotal has no task-claiming primitive and v1 does not invent one: claiming is
 * Beads' job. This module surfaces the two intents as shell-outs to the `bd`
 * CLI against the shared Beads store:
 *     /claim <taskId>    ->  bd claim <taskId>
 *     /release <taskId>  ->  bd release <taskId>
 * Beads is the source of truth — this module keeps no claim registry.
 * A Beads rejection (already claimed) is REPORTED, never retried silently.
 */
const { spawnSync } = require('child_process');

const CLAIM_RE = /^\s*\/claim\s+([A-Za-z0-9][\w\-.]*)\s*$/;
const RELEASE_RE = /^\s*\/release\s+([A-Za-z0-9][\w\-.]*)\s*$/;

/** Extract command intents from a message: one per command line. */
function extractCommands(text) {
  const out = [];
  for (const line of String(text || '').split('\n')) {
    let m = line.match(CLAIM_RE);
    if (m) { out.push({ op: 'claim', taskId: m[1], line: line.trim() }); continue; }
    m = line.match(RELEASE_RE);
    if (m) { out.push({ op: 'release', taskId: m[1], line: line.trim() }); }
  }
  return out;
}

/** True when the message carries nothing but command lines (no API call needed). */
function isPureCommand(text) {
  const lines = String(text || '').split('\n').map(l => l.trim()).filter(Boolean);
  if (!lines.length) return false;
  return lines.every(l => CLAIM_RE.test(l) || RELEASE_RE.test(l));
}

function runBeads(op, taskId) {
  const argv = op === 'claim' ? ['claim', taskId] : ['release', taskId];
  let res;
  try {
    res = spawnSync('bd', argv, { encoding: 'utf8', timeout: 15000 });
  } catch (err) {
    return { ok: false, op, taskId, error: `could not run bd: ${err.message}` };
  }
  if (res.error) {
    const unavailable = res.error.code === 'ENOENT';
    return {
      ok: false, op, taskId,
      error: unavailable
        ? 'bd CLI not on PATH — Beads unavailable on this host'
        : `bd failed to start: ${res.error.message}`,
    };
  }
  const output = ((res.stdout || '') + (res.stderr || '')).trim();
  if (res.status === 0) {
    return { ok: true, op, taskId, output: output || '(no output)' };
  }
  return { ok: false, op, taskId, error: output || `bd exited ${res.status}` };
}

function formatResult(r) {
  if (r.ok) return `/${r.op} ${r.taskId}: OK — ${r.output}`;
  return `/${r.op} ${r.taskId}: FAILED — ${r.error}`;
}

module.exports = { extractCommands, isPureCommand, runBeads, formatResult };
