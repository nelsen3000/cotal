"use strict";
// T2: hook with a pending DM -> correctly-shaped followup payload.
const lib = require("./lib");

const DM_ID = `test-dm-t2-${Date.now()}`;
const DM_TEXT = "T2 probe: the quick brown fox jumps over the lazy dog 482917.";

async function main() {
  await lib.sendDm({ id: DM_ID, text: DM_TEXT });
  console.log(`ok: tester sent DM ${DM_ID}`);

  const res = await lib.invokeHook({
    hook_event_name: "stop",
    conversation_id: "test-conv-t2",
    generation_id: "test-gen-t2",
    loop_count: 0,
  });
  if (res.timedOut) throw new Error("hook timed out");
  if (res.code !== 0) throw new Error(`hook exited ${res.code}: ${res.stderr}`);

  // Shape must match the verified Cursor contract: JSON object with a
  // non-empty string followup_message.
  let out;
  try {
    out = JSON.parse(res.stdout);
  } catch {
    throw new Error(`hook stdout is not JSON: ${res.stdout.slice(0, 200)}`);
  }
  if (typeof out.followup_message !== "string" || out.followup_message.length === 0) {
    throw new Error(`missing/empty followup_message: ${res.stdout.slice(0, 200)}`);
  }
  const fm = out.followup_message;
  if (!fm.includes(DM_TEXT)) throw new Error("followup missing the DM text");
  if (!fm.includes(DM_ID)) throw new Error("followup missing the message id");
  if (!fm.includes("tester")) throw new Error("followup missing the sender name");
  if (!fm.includes("outbox")) throw new Error("followup missing the outbox reply instruction");
  console.log(`ok: followup_message present (${fm.length} chars), contains id/text/sender/outbox path`);

  // Ack proof: the DM durable must now be empty (message acked after stdout).
  const hit = await lib.pullDmUntil(lib.GROK_CREDS, () => true, 3000);
  if (hit) throw new Error(`DM durable not drained after hook ack (saw ${hit.payload.id})`);
  console.log("ok: DM durable drained — message was acked post-stdout");

  console.log("T2 PASS");
}

main().catch((e) => {
  console.error(`T2 FAIL: ${e.message}`);
  process.exit(1);
});
