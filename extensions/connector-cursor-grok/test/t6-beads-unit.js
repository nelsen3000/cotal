"use strict";
// T6: Beads intent unit tests. `bd` is not installed on this host, so the
// shell-out path is tested with a fake `bd` on PATH; the live round-trip
// against a real Beads store is documented as not E2E-verified.
const fs = require("fs");
const os = require("os");
const path = require("path");
const assert = require("assert");
const beads = require("../src/beads");

const REAL_PATH = process.env.PATH;

function fakeBdDir(mode) {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "fake-bd-"));
  const script =
    mode === "ok"
      ? '#!/bin/sh\necho "claimed $2"\n'
      : '#!/bin/sh\necho "already claimed by grok" >&2\nexit 1\n';
  fs.writeFileSync(path.join(dir, "bd"), script, { mode: 0o755 });
  return dir;
}

async function main() {
  // parseIntentFile
  {
    const p = beads.parseIntentFile('{"intent":"claim","taskId":"t-1","inReplyTo":"m-9"}');
    assert.deepStrictEqual(p, { intent: "claim", taskId: "t-1", inReplyTo: "m-9" });
    assert.throws(() => beads.parseIntentFile("not json"), /not JSON/);
    assert.throws(() => beads.parseIntentFile('{"intent":"steal","taskId":"t"}'), /claim.*release/);
    assert.throws(() => beads.parseIntentFile('{"intent":"claim"}'), /taskId/);
    assert.throws(() => beads.parseIntentFile('{"intent":"claim","taskId":"  "}'), /taskId/);
    assert.throws(
      () => beads.parseIntentFile('{"intent":"release","taskId":"t","inReplyTo":5}'),
      /inReplyTo/
    );
    console.log("ok: parseIntentFile validates intent/taskId/inReplyTo");
  }

  // executeIntent: caller-bug cases need no bd at all.
  {
    const r1 = await beads.executeIntent("steal", "t-1");
    assert.strictEqual(r1.ok, false);
    assert.strictEqual(r1.status, "invalid-intent");
    const r2 = await beads.executeIntent("claim", "");
    assert.strictEqual(r2.status, "invalid-task");
    console.log("ok: executeIntent rejects bad intent/taskId without shelling out");
  }

  // executeIntent: bd not on PATH -> graceful UNAVAILABLE.
  {
    process.env.PATH = "/nonexistent-dir-xyz";
    try {
      const r = await beads.executeIntent("claim", "t-1");
      assert.strictEqual(r.ok, false);
      assert.strictEqual(r.status, "unavailable");
      assert.match(r.output, /not on PATH/);
    } finally {
      process.env.PATH = REAL_PATH;
    }
    console.log("ok: missing bd -> unavailable (graceful, no crash)");
  }

  // executeIntent: fake bd success.
  {
    const dir = fakeBdDir("ok");
    process.env.PATH = `${dir}:${REAL_PATH}`;
    try {
      const r = await beads.executeIntent("claim", "task-42");
      assert.strictEqual(r.ok, true);
      assert.strictEqual(r.status, "ok");
      assert.match(r.output, /claimed task-42/);
    } finally {
      process.env.PATH = REAL_PATH;
      fs.rmSync(dir, { recursive: true, force: true });
    }
    console.log("ok: fake bd success -> ok:true with output");
  }

  // executeIntent: fake bd rejection (already claimed).
  {
    const dir = fakeBdDir("reject");
    process.env.PATH = `${dir}:${REAL_PATH}`;
    try {
      const r = await beads.executeIntent("claim", "task-7");
      assert.strictEqual(r.ok, false);
      assert.strictEqual(r.status, "rejected");
      assert.match(r.output, /already claimed/);
    } finally {
      process.env.PATH = REAL_PATH;
      fs.rmSync(dir, { recursive: true, force: true });
    }
    console.log("ok: fake bd rejection -> rejected with reason (not retried)");
  }

  // formatResult shapes.
  {
    const ok = beads.formatResult("grok", "claim", "t-1", { ok: true, status: "ok", output: "claimed t-1" });
    assert.match(ok, /^\[BEADS\] claim t-1: OK/);
    const rej = beads.formatResult("grok", "claim", "t-1", { ok: false, status: "rejected", output: "taken" });
    assert.match(rej, /^\[BEADS\] claim t-1: REJECTED — taken/);
    const un = beads.formatResult("grok", "release", "t-2", { ok: false, status: "unavailable", output: "no bd" });
    assert.match(un, /^\[BEADS\] release t-2: UNAVAILABLE/);
    console.log("ok: formatResult emits parseable [BEADS] lines");
  }

  console.log("T6 PASS");
}

main().catch((e) => {
  console.error(`T6 FAIL: ${e.message}`);
  process.exit(1);
});
