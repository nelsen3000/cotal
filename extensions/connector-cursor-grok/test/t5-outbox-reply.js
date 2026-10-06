"use strict";
// T5: outbound — agent drops outbox/<id>.md, sidecar publishes it to the mesh
// with replyTo set; the original sender receives it.
const fs = require("fs");
const path = require("path");
const lib = require("./lib");

const DM_ID = `test-dm-t5-${Date.now()}`;
const DM_TEXT = "T5 probe: please reply 11235.";
const REPLY_TEXT = "T5 reply: acknowledged, over and out 81321.";

async function main() {
  const stateDir = process.env.ARENA_STATE_DIR;
  if (!stateDir) throw new Error("ARENA_STATE_DIR not set");

  await lib.sendDm({ id: DM_ID, text: DM_TEXT });
  const res = await lib.invokeHook({
    hook_event_name: "stop",
    conversation_id: "test-conv-t5",
    generation_id: "test-gen-t5",
    loop_count: 0,
  });
  const out = JSON.parse(res.stdout);
  if (!out.followup_message || !out.followup_message.includes(DM_ID)) {
    throw new Error("setup: hook did not deliver the T5 DM");
  }
  console.log(`ok: hook delivered ${DM_ID}; pending.json should route the reply`);

  // The agent's move: write the reply file (this is what Cursor rules instruct).
  const replyFile = path.join(stateDir, "outbox", `${DM_ID}.md`);
  fs.writeFileSync(replyFile, REPLY_TEXT + "\n");
  console.log(`ok: wrote ${replyFile}`);

  const child = await lib.startSidecar();
  console.log("ok: sidecar up, outbox watcher polling");
  try {
    // The original sender (tester) should receive the reply on its DM durable.
    const hit = await lib.pullDmUntil(
      lib.TESTER_CREDS,
      (p) => p.replyTo === DM_ID,
      15000
    );
    if (!hit) throw new Error("tester never received the reply");
    const text = (hit.payload.parts || [])
      .filter((p) => p.kind === "text")
      .map((p) => p.text)
      .join("\n");
    if (!text.includes("81321")) throw new Error(`reply text wrong: ${text.slice(0, 80)}`);
    if (hit.payload.id !== `reply-${DM_ID}`) {
      throw new Error(`reply id wrong: ${hit.payload.id}`);
    }
    hit.ack();
    console.log(`ok: tester got reply id=reply-${DM_ID} replyTo=${hit.payload.replyTo}`);

    // The file must have been archived to sent/.
    await lib.sleep(500);
    const sentPath = path.join(stateDir, "sent", `${DM_ID}.md`);
    if (!fs.existsSync(sentPath)) throw new Error("reply file not archived to sent/");
    if (fs.existsSync(replyFile)) throw new Error("reply file still in outbox/");
    console.log("ok: reply file archived to sent/");
  } finally {
    await lib.killSidecar(child, "SIGTERM");
  }
  console.log("T5 PASS");
}

main().catch((e) => {
  console.error(`T5 FAIL: ${e.message}`);
  process.exit(1);
});
