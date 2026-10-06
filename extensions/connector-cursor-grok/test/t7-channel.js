"use strict";
// T7: channel pull — hook with ARENA_CHANNELS=ops delivers a channel post as
// followup, advances the cursor, and does not redeliver on the next run.
// Uses dedicated agents (grokchan: subscribe ops, chanwriter: publish ops)
// so the main grok identity is untouched.
const fs = require("fs");
const path = require("path");
const assert = require("assert");
const { headers } = require("nats");
const lib = require("./lib");

const GROKCHAN_CREDS = path.join(lib.ROOT, "creds", "grokchan.creds");
const CHANWRITER_CREDS = path.join(lib.ROOT, "creds", "chanwriter.creds");
const CHANNEL = "ops";
const MSG_ID = `test-ch-t7-${Date.now()}`;
const MSG_TEXT = "T7 probe: channel ops broadcast 271828.";

function hookEnv(stateDir) {
  return {
    ARENA_CREDS_PATH: GROKCHAN_CREDS,
    ARENA_NATS_URL: lib.NATS_URL,
    ARENA_SPACE: lib.SPACE,
    ARENA_AGENT_NAME: "grokchan",
    ARENA_STATE_DIR: stateDir,
    ARENA_CHANNELS: CHANNEL,
    PATH: process.env.PATH,
    HOME: process.env.HOME,
  };
}

function invokeHookWith(env) {
  const { spawn } = require("child_process");
  return new Promise((resolve) => {
    const child = spawn("node", [path.join(lib.CONNECTOR, "src", "stop-hook.js")], {
      env,
      stdio: ["pipe", "pipe", "pipe"],
    });
    let stdout = "";
    let stderr = "";
    child.stdout.on("data", (d) => (stdout += d));
    child.stderr.on("data", (d) => (stderr += d));
    const timer = setTimeout(() => {
      child.kill("SIGKILL");
      resolve({ stdout, stderr, timedOut: true });
    }, 25000);
    child.on("close", (code) => {
      clearTimeout(timer);
      resolve({ stdout, stderr, code, timedOut: false });
    });
    child.stdin.write(JSON.stringify({ hook_event_name: "stop", loop_count: 0 }));
    child.stdin.end();
  });
}

async function main() {
  const stateDir = path.join(process.env.ARENA_STATE_DIR || "/tmp", "t7-state");
  fs.rmSync(stateDir, { recursive: true, force: true });
  fs.mkdirSync(path.join(stateDir, "outbox"), { recursive: true });

  const writerNkey = lib.nkeyOf(CHANWRITER_CREDS);
  const nc = await lib.connectAs(CHANWRITER_CREDS, writerNkey);
  let lastSeq;
  try {
    const js = nc.jetstream();
    const stream = await js.streams.get(`CHAT_${lib.SPACE}`);
    const sinfo = await stream.info(true);
    lastSeq = sinfo.state.last_seq;
    // Seed the cursor at the current stream tail (steady-state path: the
    // first-ever hook run uses deliver_policy "new" by design — no backfill).
    fs.writeFileSync(
      path.join(stateDir, "channel-cursors.json"),
      JSON.stringify({ [CHANNEL]: lastSeq })
    );
    console.log(`ok: stream tail seq=${lastSeq}, cursor seeded`);

    const payload = {
      id: MSG_ID,
      ts: Date.now(),
      space: lib.SPACE,
      from: { id: writerNkey, name: "chanwriter", role: "agent" },
      parts: [{ kind: "text", text: MSG_TEXT }],
      channel: CHANNEL,
    };
    const h = headers();
    h.set("Nats-Msg-Id", MSG_ID);
    await js.publish(
      `cotal.${lib.SPACE}.chat.local.${writerNkey}.${CHANNEL}`,
      Buffer.from(JSON.stringify(payload), "utf8"),
      { headers: h }
    );
    console.log(`ok: published ${MSG_ID} to #${CHANNEL}`);
  } finally {
    await nc.close().catch(() => {});
  }

  const env = hookEnv(stateDir);
  const res = await invokeHookWith(env);
  if (res.timedOut) throw new Error("hook timed out");
  if (res.code !== 0) throw new Error(`hook exited ${res.code}: ${res.stderr}`);
  const out = JSON.parse(res.stdout);
  assert.strictEqual(typeof out.followup_message, "string", "expected followup_message");
  assert.ok(out.followup_message.includes(MSG_TEXT), "followup missing channel text");
  assert.ok(out.followup_message.includes(MSG_ID), "followup missing message id");
  assert.ok(out.followup_message.includes(`#${CHANNEL}`), "followup missing channel tag");
  console.log("ok: channel post delivered as followup_message with #ops tag + id");

  const cursors = JSON.parse(fs.readFileSync(path.join(stateDir, "channel-cursors.json"), "utf8"));
  assert.ok(cursors[CHANNEL] > lastSeq, `cursor did not advance (${cursors[CHANNEL]} <= ${lastSeq})`);
  console.log(`ok: channel cursor advanced ${lastSeq} -> ${cursors[CHANNEL]}`);

  // Second run: nothing new -> {} (no redelivery).
  const res2 = await invokeHookWith(env);
  const out2 = JSON.parse(res2.stdout);
  assert.ok(!out2.followup_message, `expected no redelivery, got: ${String(out2.followup_message).slice(0, 80)}`);
  console.log("ok: second hook run -> {} (cursor held, no redelivery)");

  console.log("T7 PASS");
}

main().catch((e) => {
  console.error(`T7 FAIL: ${e.message}`);
  process.exit(1);
});
