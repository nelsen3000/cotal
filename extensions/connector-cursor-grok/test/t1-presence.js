"use strict";
// T1: presence visible (self + cross-agent), TTL expiry after kill.
// NOTE: the agent profile grants own-key PUT + bucket WATCH only — direct
// kv.get is denied, so every read here goes through kv.watch (the roster path).
const lib = require("./lib");

async function main() {
  const presenceKey = lib.presenceKeyOf(lib.GROK_CREDS);
  console.log(`presence key: ${presenceKey}`);
  const child = await lib.startSidecar();
  console.log("ok: sidecar started, presence idle visible to self");

  // Self-read via watch.
  {
    const e = await lib.watchPresenceOnce(lib.GROK_CREDS, presenceKey, 8000);
    if (!e || !e.value) throw new Error("self watch saw no entry");
    const rec = JSON.parse(e.value.toString("utf8"));
    if (rec.status !== "idle") throw new Error(`status=${rec.status}, want idle`);
    if (rec.card.name !== "grok") throw new Error(`card.name=${rec.card.name}`);
    if (!rec.lifecycleUid) throw new Error("missing lifecycleUid");
    console.log(
      `ok: self watch: status=idle name=grok uid=${rec.lifecycleUid.slice(0, 8)}…`
    );
  }

  // Cross-agent read: a peer watches the bucket for grok's key.
  {
    const e = await lib.watchPresenceOnce(lib.TESTER_CREDS, presenceKey, 8000);
    if (!e || !e.value) throw new Error("peer watch saw no entry for grok");
    const rec = JSON.parse(e.value.toString("utf8"));
    if (rec.status !== "idle") throw new Error(`peer saw status=${rec.status}`);
    console.log("ok: peer watch sees grok presence status=idle (roster path works)");
  }

  // TTL: SIGKILL (no offline write), then the 6s TTL must reap the key.
  await lib.killSidecar(child, "SIGKILL");
  console.log("ok: sidecar SIGKILLed (no offline write)");
  await lib.sleep(9000);
  {
    const e = await lib.watchPresenceOnce(lib.GROK_CREDS, presenceKey, 5000);
    if (e && e.value) {
      throw new Error("presence key still alive 9s after SIGKILL (TTL 6s broken)");
    }
    console.log("ok: presence key expired after ~9s (6s TTL confirmed)");
  }
  console.log("T1 PASS");
}

main().catch((e) => {
  console.error(`T1 FAIL: ${e.message}`);
  process.exit(1);
});
