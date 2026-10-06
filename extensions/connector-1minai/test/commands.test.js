// Unit tests for Beads command parsing + the `bd` shell-out wrapper.
// No mesh needed. Run: node test/commands.test.js

"use strict";

const assert = require("assert");
const fs = require("fs");
const os = require("os");
const path = require("path");
const { parseCommand, runBeads } = require("../src/commands");

let n = 0;
function ok(name, fn) {
  n++;
  try {
    fn();
    console.log(`PASS  ${name}`);
  } catch (e) {
    console.error(`FAIL  ${name}: ${e.message}`);
    process.exitCode = 1;
  }
}

// --- parsing ---
ok("parses /claim <id>", () => {
  assert.deepStrictEqual(parseCommand("/claim bead-42"), { op: "claim", taskId: "bead-42" });
});
ok("parses /release <id>", () => {
  assert.deepStrictEqual(parseCommand("/release bead-42"), { op: "release", taskId: "bead-42" });
});
ok("case-insensitive op", () => {
  assert.deepStrictEqual(parseCommand("/CLAIM X1"), { op: "claim", taskId: "X1" });
});
ok("tolerates surrounding whitespace", () => {
  assert.deepStrictEqual(parseCommand("  /release   task-9  \n"), { op: "release", taskId: "task-9" });
});
ok("rejects command not at start", () => {
  assert.strictEqual(parseCommand("please /claim x"), null);
});
ok("rejects /claim with no id", () => {
  assert.strictEqual(parseCommand("/claim"), null);
  assert.strictEqual(parseCommand("/claim   "), null);
});
ok("rejects unknown verbs", () => {
  assert.strictEqual(parseCommand("/steal bead-1"), null);
  assert.strictEqual(parseCommand("/claimx bead-1"), null);
});
ok("rejects non-command chatter", () => {
  assert.strictEqual(parseCommand("what is the capital of France?"), null);
  assert.strictEqual(parseCommand(""), null);
  assert.strictEqual(parseCommand(null), null);
});

// --- runBeads: missing bd binary degrades gracefully, never throws ---
ok("missing bd -> ok:false with 'not installed' hint", () => {
  const res = runBeads("claim", "bead-1", { beadsDir: os.tmpdir() });
  assert.strictEqual(res.ok, false);
  assert.match(res.output, /bd CLI not installed/);
});

// --- runBeads: success path via a stub `bd` on PATH ---
ok("stub bd on PATH -> ok:true with its output", () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "fake-bd-"));
  const stub = path.join(dir, "bd");
  fs.writeFileSync(stub, "#!/bin/sh\necho \"claimed $2\"\n");
  fs.chmodSync(stub, 0o755);
  const oldPath = process.env.PATH;
  process.env.PATH = dir + path.delimiter + oldPath;
  try {
    const res = runBeads("claim", "bead-7", { beadsDir: os.tmpdir() });
    assert.strictEqual(res.ok, true);
    assert.match(res.output, /claimed bead-7/);
  } finally {
    process.env.PATH = oldPath;
    fs.rmSync(dir, { recursive: true, force: true });
  }
});

// --- runBeads: a failing bd (e.g. already claimed) is a rejection, not a throw ---
ok("stub bd exiting non-zero -> ok:false rejection", () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "fake-bd-"));
  const stub = path.join(dir, "bd");
  fs.writeFileSync(stub, "#!/bin/sh\necho \"already claimed\" >&2\nexit 1\n");
  fs.chmodSync(stub, 0o755);
  const oldPath = process.env.PATH;
  process.env.PATH = dir + path.delimiter + oldPath;
  try {
    const res = runBeads("claim", "bead-8", { beadsDir: os.tmpdir() });
    assert.strictEqual(res.ok, false);
    assert.match(res.output, /already claimed/);
    assert.doesNotMatch(res.output, /not installed/);
  } finally {
    process.env.PATH = oldPath;
    fs.rmSync(dir, { recursive: true, force: true });
  }
});

console.log(process.exitCode ? `\n${n} tests, FAILURES` : `\n${n}/${n} unit tests passed`);
