// E2E tests for the 1min.ai connector (MOCK API client).
// Proves against an isolated mesh:
//   (1) presence visible in cotal_presence_<space>
//   (2) DM tester->1minai is picked up by the poller and passed to the mock API
//   (3) mock API reply publishes back to the mesh; tester receives it with replyTo
//   (4) offline replay: stop connector, DM, restart -> mock receives the DM
//
// Run from the connector dir:  npm test
// Env (with defaults pointing at the isolated test mesh):
//   E2E_SERVER, E2E_OM_CREDS, E2E_TESTER_CREDS

"use strict";

const { spawn } = require("child_process");
const fs = require("fs");
const os = require("os");
const path = require("path");
const { connect, headers, JSONCodec } = require("nats");
const { parseCredsFile, credsAuthFromFile } = require("../src/creds");

const jc = JSONCodec();
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const results = [];
function verdict(name, ok, detail) {
  results.push({ name, ok, detail });
  console.log(`${ok ? "PASS" : "FAIL"}  ${name}${detail ? ` — ${detail}` : ""}`);
}

const SERVER = process.env.E2E_SERVER || "nats://127.0.0.1:35773";
const OM_CREDS = process.env.E2E_OM_CREDS;
const TESTER_CREDS = process.env.E2E_TESTER_CREDS;
if (!OM_CREDS || !TESTER_CREDS) {
  console.error("E2E needs E2E_OM_CREDS and E2E_TESTER_CREDS");
  process.exit(2);
}

const om = parseCredsFile(OM_CREDS);
const tester = parseCredsFile(TESTER_CREDS);
const SPACE = om.space;
const ROOT = `cotal.${SPACE}`;
const OM_KEY = `${om.owner}.${om.actor}`;
const TESTER_KEY = `${tester.owner}.${tester.actor}`;
const MOCK_LOG = path.join(os.tmpdir(), `onemin-mock-${Date.now()}.jsonl`);

let connectorProc = null;
const CONNECTOR_PID_FILE = path.join(os.tmpdir(), "onemin-e2e-connector.pid");

function killStaleConnector() {
  try {
    if (!fs.existsSync(CONNECTOR_PID_FILE)) return;
    const pid = parseInt(fs.readFileSync(CONNECTOR_PID_FILE, "utf8"), 10);
    if (Number.isFinite(pid)) {
      try { process.kill(pid, "SIGTERM"); console.log(`(killed stale connector pid ${pid})`); } catch {}
    }
  } catch {}
  try { fs.unlinkSync(CONNECTOR_PID_FILE); } catch {}
}
function startConnector() {
  return new Promise((resolve, reject) => {
    const p = spawn("node", [path.join(__dirname, "..", "src", "connector.js")], {
      env: {
        ...process.env,
        COTAL_CREDS: OM_CREDS,
        COTAL_SERVER: SERVER,
        COTAL_CHANNELS: "general",
        ONEMIN_USE_MOCK: "1",
        ONEMIN_MOCK_LOG: MOCK_LOG,
        ONEMIN_MOCK_REPLY: "MOCK-ANSWER: the 1min.ai call is done.",
        ONEMIN_POLL_MS: "2000",
        ONEMIN_PRESENCE_MS: "1000",
      },
      stdio: ["ignore", "pipe", "pipe"],
    });
    connectorProc = p;
    fs.writeFileSync(CONNECTOR_PID_FILE, String(p.pid));
    let out = "";
    p.stdout.on("data", (d) => (out += d));
    p.stderr.on("data", (d) => (out += d));
    p.on("error", reject);
    const timer = setTimeout(() => reject(new Error("connector did not become ready; output:\n" + out)), 20000);
    const check = setInterval(() => {
      if (out.includes("ready.")) { clearTimeout(timer); clearInterval(check); resolve(p); }
    }, 200);
    p.on("exit", (c) => { clearTimeout(timer); clearInterval(check); reject(new Error(`connector exited ${c}; output:\n${out}`)); });
  });
}
async function stopConnector() {
  if (!connectorProc) return;
  const p = connectorProc;
  connectorProc = null;
  p.kill("SIGTERM");
  await new Promise((r) => { p.on("exit", r); setTimeout(r, 8000); });
  try { fs.unlinkSync(CONNECTOR_PID_FILE); } catch {}
}

async function waitFor(fn, { timeoutMs = 30000, everyMs = 500, what = "condition" } = {}) {
  const t0 = Date.now();
  for (;;) {
    const v = await fn();
    if (v) return v;
    if (Date.now() - t0 > timeoutMs) throw new Error(`timed out waiting for ${what}`);
    await sleep(everyMs);
  }
}

function dmMessage(id, text) {
  return {
    id, ts: new Date().toISOString(), space: SPACE,
    from: { id: TESTER_KEY, name: "tester", role: "agent" },
    parts: [{ kind: "text", text }],
    to: { owner: om.owner, actor: om.actor },
  };
}

async function main() {
  killStaleConnector(); // never share the test durable with an orphaned instance
  let nc = null;
  try {
  nc = await connect({
    servers: SERVER,
    authenticator: credsAuthFromFile(TESTER_CREDS),
    inboxPrefix: `_INBOX_${tester.actor}`,
  });
  const js = nc.jetstream();

  // ---- (1) presence visible ----
  await startConnector();
  const kv = await js.views.kv(`cotal_presence_${SPACE}`);
  let presenceEntry = null;
  try {
    presenceEntry = await waitFor(async () => {
      const w = await kv.watch({ key: OM_KEY, initializedFn: () => {} });
      for await (const e of w) { w.stop(); return e.value ? jc.decode(e.value) : null; }
      return null;
    }, { what: "presence entry", timeoutMs: 15000 });
  } catch (e) { /* fall through to verdict */ }
  verdict("1 presence visible in KV", !!(presenceEntry && presenceEntry.status), presenceEntry ? `status=${presenceEntry.status} key=${OM_KEY}` : "no entry");

  // ---- (2)+(3) DM round-trip ----
  // The tester receives DMs the same way the connector does: by binding its
  // pre-provisioned JetStream durable (core subscription to the DM filter is
  // not granted — DMs are JetStream-persisted by design).
  const replies = [];
  const tConsumer = await js.consumers.get(tester.dmStream, tester.durable);
  const tIter = await tConsumer.consume();
  (async () => {
    for await (const m of tIter) {
      try { replies.push(jc.decode(m.data)); m.ack(); } catch {}
    }
  })().catch(() => {});

  const dmId = `e2e-dm-${Date.now()}`;
  const h = headers(); h.append("Nats-Msg-Id", dmId);
  await nc.publish(
    `${ROOT}.inst.${om.owner}.${om.actor}.${tester.owner}.${tester.actor}`,
    jc.encode(dmMessage(dmId, "What is the capital of France?")),
    { headers: h }
  );
  let mockSaw = null;
  try {
    mockSaw = await waitFor(() => {
      if (!fs.existsSync(MOCK_LOG)) return null;
      const lines = fs.readFileSync(MOCK_LOG, "utf8").trim().split("\n").filter(Boolean);
      const rec = lines.map((l) => JSON.parse(l)).find((r) => JSON.stringify(r).includes(dmId));
      return rec || null;
    }, { what: "mock receiving the DM", timeoutMs: 25000 });
  } catch (e) { /* fall through */ }
  verdict("2 poller passed DM to mock API", !!mockSaw, mockSaw ? "mock log contains the DM text+id" : "mock never saw it");

  let reply = null;
  try {
    reply = await waitFor(() => replies.find((r) => r.replyTo === dmId) || null, { what: "reply with replyTo", timeoutMs: 25000 });
  } catch (e) { /* fall through */ }
  verdict("3 reply received with replyTo", !!(reply && /MOCK-ANSWER/.test(JSON.stringify(reply.parts || reply))), reply ? `replyTo=${reply.replyTo}` : "no reply");

  // ---- (4) offline replay ----
  await stopConnector();
  await sleep(1500); // let the durable consumer go away
  const dmId2 = `e2e-dm-offline-${Date.now()}`;
  const h2 = headers(); h2.append("Nats-Msg-Id", dmId2);
  await nc.publish(
    `${ROOT}.inst.${om.owner}.${om.actor}.${tester.owner}.${tester.actor}`,
    jc.encode(dmMessage(dmId2, "Second question while connector is down")),
    { headers: h2 }
  );
  await sleep(1000);
  await startConnector(); // same uid -> same durable -> redelivery
  let mockSaw2 = null;
  try {
    mockSaw2 = await waitFor(() => {
      if (!fs.existsSync(MOCK_LOG)) return null;
      const lines = fs.readFileSync(MOCK_LOG, "utf8").trim().split("\n").filter(Boolean);
      const rec = lines.map((l) => JSON.parse(l)).find((r) => JSON.stringify(r).includes(dmId2));
      return rec || null;
    }, { what: "mock receiving the offline DM after restart", timeoutMs: 45000 });
  } catch (e) { /* fall through */ }
  verdict("4 offline DM replayed after restart", !!mockSaw2, mockSaw2 ? "durable redelivered; mock received it" : "not received");

  } finally {
    await stopConnector();
    if (nc) { try { await nc.drain(); } catch {} }
  }

  const failed = results.filter((r) => !r.ok);
  console.log(`\n${results.length - failed.length}/${results.length} passed`);
  process.exit(failed.length ? 1 : 0);
}

main().catch((e) => { console.error("E2E FATAL:", e.message); process.exit(2); });
