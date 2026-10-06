"use strict";
// T4: offline replay — DM sent while the sidecar is DOWN is delivered on the
// next hook pull. (The hook binds the durable itself; it never needs the sidecar.)
const lib = require("./lib");
const { execSync } = require("child_process");

const DM_ID = `test-dm-t4-${Date.now()}`;
const DM_TEXT = "T4 probe: offline replay 7391 — sent while sidecar down.";

async function main() {
  // Prove the sidecar is really down: no live grok presence entry.
  const presenceKey = lib.presenceKeyOf(lib.GROK_CREDS);
  {
    const e = await lib.watchPresenceOnce(lib.GROK_CREDS, presenceKey, 4000);
    if (e && e.value) {
      const rec = JSON.parse(e.value.toString("utf8"));
      if (rec.status !== "offline") {
        throw new Error("sidecar still up — T4 needs it down");
      }
    }
  }
  // Belt and braces: no sidecar process either ([s] avoids matching pgrep itself).
  try {
    const ps = execSync("pgrep -af '[s]rc/sidecar\\.js' || true").toString();
    if (ps.trim()) throw new Error(`sidecar process still running: ${ps.trim().slice(0, 120)}`);
  } catch (e) {
    if (/sidecar process still running/.test(e.message)) throw e;
  }
  console.log("ok: sidecar confirmed down");

  await lib.sendDm({ id: DM_ID, text: DM_TEXT });
  console.log(`ok: tester sent DM ${DM_ID} while sidecar down`);
  await lib.sleep(2000); // let it sit in the durable with nothing consuming

  const res = await lib.invokeHook({
    hook_event_name: "stop",
    conversation_id: "test-conv-t4",
    generation_id: "test-gen-t4",
    loop_count: 0,
  });
  if (res.timedOut) throw new Error("hook timed out");
  if (res.code !== 0) throw new Error(`hook exited ${res.code}: ${res.stderr}`);
  const out = JSON.parse(res.stdout);
  if (typeof out.followup_message !== "string" || !out.followup_message.includes(DM_TEXT)) {
    throw new Error("offline DM was NOT delivered on next hook pull");
  }
  if (!out.followup_message.includes(DM_ID)) throw new Error("delivered payload missing id");
  console.log("ok: offline DM delivered as followup on next hook pull (durable replay works)");

  console.log("T4 PASS");
}

main().catch((e) => {
  console.error(`T4 FAIL: ${e.message}`);
  process.exit(1);
});
