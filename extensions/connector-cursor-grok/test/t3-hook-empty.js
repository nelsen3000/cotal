"use strict";
// T3: hook with an empty inbox -> returns nothing ({}), turn ends quietly.
const lib = require("./lib");

async function main() {
  // Precondition: inbox is empty (T2 drained it; belt-and-braces drain here).
  const pre = await lib.pullDmUntil(lib.GROK_CREDS, () => true, 2500);
  if (pre) {
    pre.ack();
    console.log(`note: drained a leftover ${pre.payload.id} before the empty test`);
  }

  const res = await lib.invokeHook({
    hook_event_name: "stop",
    conversation_id: "test-conv-t3",
    generation_id: "test-gen-t3",
    loop_count: 0,
  });
  if (res.timedOut) throw new Error("hook timed out");
  if (res.code !== 0) throw new Error(`hook exited ${res.code}: ${res.stderr}`);

  let out;
  try {
    out = JSON.parse(res.stdout);
  } catch {
    throw new Error(`hook stdout is not JSON: ${res.stdout.slice(0, 200)}`);
  }
  if (out.followup_message) {
    throw new Error(`expected no followup_message, got: ${String(out.followup_message).slice(0, 120)}`);
  }
  console.log(`ok: empty inbox -> stdout ${JSON.stringify(out)} (no followup, turn ends)`);
  console.log("T3 PASS");
}

main().catch((e) => {
  console.error(`T3 FAIL: ${e.message}`);
  process.exit(1);
});
