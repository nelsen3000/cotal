#!/usr/bin/env node
/**
 * Integration tests for connector-muse against an ISOLATED mesh.
 *
 * The bash driver (not this script) must first:
 *   1. mkdir -p ~/workspace/ops/cotal-test/mesh-test-muse && cd there
 *   2. cotal up --space test [--detach ...]   (from THAT dir only)
 *   3. cotal mint muse   --space test --out <muse.creds>   --provision   (+ note lifecycle uid)
 *      cotal mint tester --space test --out <tester.creds> --provision   (+ note lifecycle uid)
 *   4. export the TEST_* env vars below and run: node test/run-tests.js
 *
 * Required env:
 *   TEST_SPACE, TEST_SERVER,
 *   MUSE_CREDS, MUSE_UID, MUSE_OWNER, MUSE_ACTOR,
 *   TESTER_CREDS, TESTER_UID, TESTER_OWNER, TESTER_ACTOR,
 *   MUSE_CONNECTOR_DIR (fresh temp dir), CONNECTOR_SRC (src/index.js),
 *   BD_SHIM (optional path to a fake `bd` for the beads test)
 *
 * Proves: (1) presence roster, (2) DM → inbox/<id>.md, (3) outbox file → DM
 * with replyTo + move to sent/, (4) offline replay across restart,
 * (5) malformed outbox → quarantine + log, (6, bonus) /claim → bd → mesh reply.
 */

const { spawn } = require("node:child_process");
const { randomUUID } = require("node:crypto");
const {
  mkdir,
  readdir,
  readFile,
  writeFile,
  stat,
} = require("node:fs/promises");
const { join } = require("node:path");
const { connect, headers } = require("@nats-io/transport-node");
const { credsAuthenticator } = require("@nats-io/nats-core");
const _te = new TextEncoder();
const _td = new TextDecoder();
const StringCodec = () => ({ encode: (s) => _te.encode(s), decode: (b) => _td.decode(b) });
const { jetstream } = require("@nats-io/jetstream");
const { Kvm } = require("@nats-io/kv");

const sc = StringCodec();
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

const SPACE = process.env.TEST_SPACE;
const SERVER = process.env.TEST_SERVER || "nats://127.0.0.1:4222";
const MUSE = {
  creds: process.env.MUSE_CREDS,
  uid: process.env.MUSE_UID,
  owner: process.env.MUSE_OWNER || "test",
  actor: process.env.MUSE_ACTOR || "muse",
};
const TESTER = {
  creds: process.env.TESTER_CREDS,
  uid: process.env.TESTER_UID,
  owner: process.env.TESTER_OWNER || "test",
  actor: process.env.TESTER_ACTOR || "tester",
};
const WORKDIR = process.env.MUSE_CONNECTOR_DIR;
const CONNECTOR_SRC = process.env.CONNECTOR_SRC;
const BD_SHIM = process.env.BD_SHIM || null;

const results = [];
function verdict(name, pass, evidence) {
  results.push({ name, pass, evidence });
  console.log(`${pass ? "PASS" : "FAIL"}  ${name}\n      ${evidence}`);
}

async function waitFor(fn, timeoutMs, label) {
  const start = Date.now();
  let last;
  while (Date.now() - start < timeoutMs) {
    try {
      const v = await fn();
      if (v) return v;
    } catch (err) {
      last = err.message;
    }
    await sleep(500);
  }
  throw new Error(`timeout waiting for: ${label}${last ? ` (last: ${last})` : ""}`);
}

// ---- tester-side mesh helpers (raw wire, independent of the connector) ----

async function testerConnect() {
  const credsBytes = await readFile(TESTER.creds);
  const nc = await connect({
    servers: [SERVER],
    authenticator: credsAuthenticator(credsBytes),
    inboxPrefix: `_INBOX_${TESTER.actor}`,
  });
  return nc;
}

function dmSubject(toOwner, toActor, fromOwner, fromActor) {
  return `cotal.${SPACE}.inst.${toOwner}.${toActor}.${fromOwner}.${fromActor}`;
}

function buildWireDm(id, text) {
  return {
    id,
    ts: Date.now(),
    space: SPACE,
    from: { id: "tester-pubkey", name: `${TESTER.owner}.${TESTER.actor}` },
    parts: [{ kind: "text", text }],
    to: `${MUSE.owner}.${MUSE.actor}`,
  };
}

async function sendDm(nc, text, id = `test-${randomUUID()}`) {
  const h = headers();
  h.set("Nats-Msg-Id", id);
  nc.publish(
    dmSubject(MUSE.owner, MUSE.actor, TESTER.owner, TESTER.actor),
    sc.encode(JSON.stringify(buildWireDm(id, text))),
    { headers: h }
  );
  await nc.flush();
  return id;
}

async function readRoster(nc, timeoutMs = 12000) {
  // NOTE: plain bucket.get() needs $JS.API.STREAM.MSG.GET which the agent
  // JWT does NOT grant — read via watch() (ordered consumer, granted).
  const kvm = new Kvm(nc);
  const bucket = await kvm.open(`cotal_presence_${SPACE}`);
  const w = await bucket.watch();
  const roster = [];
  const seen = new Set();
  const want = `${MUSE.owner}.${MUSE.actor}`;
  const done = new Promise((resolve) => {
    const timer = setTimeout(() => { try { w.stop(); } catch { /* noop */ } resolve(); }, timeoutMs);
    (async () => {
      for await (const e of w) {
        if (e && e.key && !seen.has(e.key) && e.value) {
          seen.add(e.key);
          try { roster.push(JSON.parse(sc.decode(e.value))); } catch { /* skip */ }
        }
        if (roster.some((r) => r?.card?.name === want)) {
          clearTimeout(timer);
          try { w.stop(); } catch { /* noop */ }
          resolve();
          break;
        }
      }
    })().catch(() => resolve());
  });
  await done;
  return roster;
}

async function bindTesterDmConsumer(nc) {
  const js = jetstream(nc);
  return js.consumers.get(`DM_${SPACE}`, `dm_${TESTER.owner}-${TESTER.actor}-${TESTER.uid}`);
}

async function fetchOneDm(consumer, timeoutMs) {
  return waitFor(async () => {
    const iter = await consumer.fetch({ max_messages: 10, expires: 2000 });
    for await (const m of iter) {
      const msg = JSON.parse(sc.decode(m.data));
      m.ack();
      return msg;
    }
    return null;
  }, timeoutMs, "tester DM receipt");
}

// ---- connector process management ----

let connectorProc = null;
function startConnector(extraEnv = {}) {
  return new Promise((resolve, reject) => {
    const proc = spawn("node", [CONNECTOR_SRC], {
      env: {
        ...process.env,
        COTAL_SPACE: SPACE,
        COTAL_OWNER: MUSE.owner,
        COTAL_ACTOR: MUSE.actor,
        COTAL_CREDS: MUSE.creds,
        COTAL_LIFECYCLE_UID: MUSE.uid,
        COTAL_SERVER: SERVER,
        MUSE_CONNECTOR_DIR: WORKDIR,
        POLL_INTERVAL_MS: "1500",
        PRESENCE_INTERVAL_MS: "800",
        ...(BD_SHIM ? { BD_BIN: BD_SHIM } : {}),
        ...extraEnv,
      },
      stdio: ["ignore", "pipe", "pipe"],
    });
    let out = "";
    proc.stdout.on("data", (d) => (out += d));
    proc.stderr.on("data", (d) => (out += d));
    proc.on("error", reject);
    const onRunning = setInterval(() => {
      if (out.includes("running")) {
        clearInterval(onRunning);
        connectorProc = proc;
        resolve(proc);
      }
    }, 200);
    setTimeout(() => {
      clearInterval(onRunning);
      reject(new Error(`connector did not start in time. output:\n${out}`));
    }, 20000);
  });
}

async function stopConnector() {
  if (!connectorProc) return;
  const proc = connectorProc;
  connectorProc = null;
  proc.kill("SIGTERM");
  await new Promise((r) => {
    const t = setTimeout(() => {
      try { proc.kill("SIGKILL"); } catch { /* noop */ }
      r();
    }, 8000);
    proc.on("exit", () => { clearTimeout(t); r(); });
  });
  await sleep(500);
}

// ---- main ----

async function main() {
  for (const [k, v] of Object.entries({
    TEST_SPACE: SPACE, MUSE_CREDS: MUSE.creds, MUSE_UID: MUSE.uid,
    TESTER_CREDS: TESTER.creds, TESTER_UID: TESTER.uid,
    MUSE_CONNECTOR_DIR: WORKDIR, CONNECTOR_SRC,
  })) {
    if (!v) throw new Error(`missing required env: ${k}`);
  }
  await mkdir(WORKDIR, { recursive: true });

  // (1) presence — tester sees muse on the roster
  await startConnector();
  const tnc = await testerConnect();
  try {
    const entry = await waitFor(async () => {
      const roster = await readRoster(tnc);
      return roster.find((r) => r?.card?.name === `${MUSE.owner}.${MUSE.actor}`);
    }, 20000, "muse presence entry");
    verdict(
      "T1 presence — tester sees muse on the roster",
      entry.status === "idle" || entry.status === "working",
      `roster entry card.name=${entry.card.name} status=${entry.status} ` +
        `lifecycleUid=${entry.lifecycleUid} connector=${entry.connector}`
    );
  } catch (err) {
    verdict("T1 presence — tester sees muse on the roster", false, err.message);
  }

  // (2) DM tester→muse appears as inbox/<id>.md with correct headers
  const dmId2 = await sendDm(tnc, "hello muse, this is test two");
  try {
    const file = await waitFor(async () => {
      const files = await readdir(join(WORKDIR, "inbox"));
      return files.find((f) => f === `${dmId2}.md`) || null;
    }, 15000, "inbox file for T2");
    const text = await readFile(join(WORKDIR, "inbox", file), "utf8");
    const okFrom = text.includes(`From: ${TESTER.owner}.${TESTER.actor}`);
    const okId = text.includes(`Id: ${dmId2}`);
    const okBody = text.includes("hello muse, this is test two");
    const okTs = /Ts: \d{4}-\d{2}-\d{2}T/.test(text);
    verdict(
      "T2 inbound — DM becomes inbox/<id>.md with correct headers",
      okFrom && okId && okBody && okTs,
      `file=${file} From✓=${okFrom} Id✓=${okId} body✓=${okBody} Ts✓=${okTs}`
    );
  } catch (err) {
    verdict("T2 inbound — DM becomes inbox/<id>.md with correct headers", false, err.message);
  }

  // (3) outbox file → DM the tester receives, replyTo set, file → sent/
  const testerConsumer = await bindTesterDmConsumer(tnc);
  const outName = "reply-t3.md";
  await writeFile(
    join(WORKDIR, "outbox", outName),
    `To: ${TESTER.owner}.${TESTER.actor}\nReplyTo: ${dmId2}\n\nreplying to test two, over.\n`
  );
  try {
    const received = await fetchOneDm(testerConsumer, 20000);
    const body = (received.parts || []).filter((p) => p.kind === "text").map((p) => p.text).join("\n");
    const okReplyTo = received.replyTo === dmId2;
    const okBody = body.includes("replying to test two, over.");
    const okTo = received.to === `${TESTER.owner}.${TESTER.actor}`;
    // The tester's durable receives the publish before the connector's
    // rename lands — wait for the file instead of racing it.
    const moved = await waitFor(async () => {
      const sentFiles = await readdir(join(WORKDIR, "sent"));
      return sentFiles.some((f) => f === `${received.id}.md`) || null;
    }, 10000, "sent/ file after publish").catch(() => false);
    let gone = true;
    try { await stat(join(WORKDIR, "outbox", outName)); gone = false; } catch { /* gone */ }
    verdict(
      "T3 outbound — outbox file → DM with replyTo, moved to sent/",
      okReplyTo && okBody && okTo && moved && gone,
      `wire id=${received.id} replyTo✓=${okReplyTo} to✓=${okTo} body✓=${okBody} ` +
        `sent/${received.id}.md✓=${moved} outbox cleared✓=${gone}`
    );
  } catch (err) {
    verdict("T3 outbound — outbox file → DM with replyTo, moved to sent/", false, err.message);
  }

  // (4) offline replay — stop, send, restart, file appears
  await stopConnector();
  const dmId4 = await sendDm(tnc, "offline message while connector is down");
  await sleep(2500); // connector is down: nothing should be picked up
  let leaked = false;
  try {
    await stat(join(WORKDIR, "inbox", `${dmId4}.md`));
    leaked = true;
  } catch { /* expected: not there */ }
  await startConnector();
  try {
    await waitFor(async () => {
      try { await stat(join(WORKDIR, "inbox", `${dmId4}.md`)); return true; }
      catch { return false; }
    }, 20000, "offline DM replayed to inbox");
    verdict(
      "T4 offline replay — DM sent while down appears after restart",
      !leaked,
      `sent-while-down id=${dmId4}; no file while stopped✓=${!leaked}; ` +
        `file present after restart✓=true (durable dm_${MUSE.owner}-${MUSE.actor}-${MUSE.uid})`
    );
  } catch (err) {
    verdict("T4 offline replay — DM sent while down appears after restart", false, err.message);
  }

  // (5) malformed outbox file → quarantine + log line
  const badName = "bad-t5.md";
  await writeFile(join(WORKDIR, "outbox", badName), "this file has no headers at all\njust vibes\n");
  try {
    await waitFor(async () => {
      try { await stat(join(WORKDIR, "quarantine", badName)); return true; }
      catch { return false; }
    }, 15000, "quarantine of malformed file");
    const log = await readFile(join(WORKDIR, "connector.log"), "utf8");
    const logged = log.includes(`quarantine: ${badName}`);
    verdict(
      "T5 malformed outbox → quarantine/ + log line (never silent)",
      logged,
      `quarantine/${badName} exists✓=true; connector.log line✓=${logged}`
    );
  } catch (err) {
    verdict("T5 malformed outbox → quarantine/ + log line (never silent)", false, err.message);
  }

  // (6, bonus) /claim → bd shell-out → result posted back to the mesh
  if (BD_SHIM) {
    const dmId6 = await sendDm(tnc, "/claim T-123");
    try {
      const received = await fetchOneDm(testerConsumer, 20000);
      const body = (received.parts || []).filter((p) => p.kind === "text").map((p) => p.text).join("\n");
      const okClaim = body.includes("T-123") && received.replyTo === dmId6;
      verdict(
        "T6 beads — /claim shells out to bd, result posted back",
        okClaim,
        `replyTo✓=${received.replyTo === dmId6}; result body: ${JSON.stringify(body.slice(0, 120))}`
      );
    } catch (err) {
      verdict("T6 beads — /claim shells out to bd, result posted back", false, err.message);
    }
  } else {
    console.log("SKIP  T6 beads — no BD_SHIM provided");
  }

  await stopConnector();
  try { await tnc.close(); } catch { /* noop */ }

  console.log("\n==== summary ====");
  let fails = 0;
  for (const r of results) {
    console.log(`${r.pass ? "PASS" : "FAIL"}  ${r.name}`);
    if (!r.pass) fails++;
  }
  process.exit(fails ? 1 : 0);
}

main().catch((err) => {
  console.error(`FATAL: ${err.stack || err}`);
  stopConnector().finally(() => process.exit(2));
});
