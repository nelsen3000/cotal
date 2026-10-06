"use strict";
// Shared test helpers. Not a test itself.
const { spawn } = require("child_process");
const fs = require("fs");
const path = require("path");
const mesh = require("../src/mesh");
const { connect, credsAuthenticator, headers } = require("nats");

const ROOT = "/home/hatch/workspace/ops/cotal-test/mesh-test-cursor";
const CONNECTOR = "/home/hatch/workspace/ops/cotal-test/connectors/cursor-grok";
const NATS_URL = "nats://127.0.0.1:4222";
const SPACE = "main";

const GROK_CREDS = path.join(ROOT, "creds", "grok.creds");
const TESTER_CREDS = path.join(ROOT, "creds", "tester.creds");

function withEnv(extra) {
  return {
    ARENA_CREDS_PATH: GROK_CREDS,
    ARENA_NATS_URL: NATS_URL,
    ARENA_SPACE: SPACE,
    ARENA_AGENT_NAME: "grok",
    ARENA_STATE_DIR: process.env.ARENA_STATE_DIR,
    PATH: process.env.PATH,
    HOME: process.env.HOME,
    ...extra,
  };
}

async function connectAs(credsPath, nkey) {
  return connect({
    servers: NATS_URL,
    inboxPrefix: `_INBOX_${nkey}`,
    authenticator: credsAuthenticator(fs.readFileSync(credsPath)),
    timeout: 5000,
    maxReconnectAttempts: 2,
  });
}

function nkeyOf(credsPath) {
  const { payload } = mesh.parseCreds(credsPath);
  return payload.sub;
}

function presenceKeyOf(credsPath) {
  const { payload } = mesh.parseCreds(credsPath);
  return mesh.presenceKeyFromJwt(payload);
}

/*
 * Read one presence entry via kv.watch (ordered consumer). Direct kv.get is
 * NOT granted by the agent profile (only own-key put + bucket watch), so
 * watch is the read path for self AND peers. Resolves with the KvEntry or
 * null on timeout.
 */
async function watchPresenceOnce(credsPath, key, timeoutMs = 8000) {
  const nkey = nkeyOf(credsPath);
  const nc = await connectAs(credsPath, nkey);
  try {
    const kv = await nc.jetstream().views.kv(`cotal_presence_${SPACE}`);
    const watcher = await kv.watch({ key });
    // The watch iterator only resolves when an entry arrives; a missing key
    // yields nothing forever, so the timeout must stop the iterator itself.
    const timer = setTimeout(() => {
      try {
        watcher.stop();
      } catch {
        /* ignore */
      }
    }, timeoutMs);
    try {
      for await (const e of watcher) {
        if (e) return e;
      }
    } catch {
      /* stopped by timeout */
    } finally {
      clearTimeout(timer);
    }
    return null;
  } finally {
    await nc.close().catch(() => {});
  }
}

function durableOf(credsPath) {
  const { payload } = mesh.parseCreds(credsPath);
  const m = payload.nats.pub.allow
    .find((s) => s.includes("$JS.API.CONSUMER.INFO.DM_"))
    .match(/\$JS\.API\.CONSUMER\.INFO\.(DM_[^.]+)\.(dm_.+)$/);
  return { stream: m[1], durable: m[2] };
}

/** Tester -> grok DM. Returns the message id. */
async function sendDm({ id, text, fromName = "tester" }) {
  const fromNkey = nkeyOf(TESTER_CREDS);
  const toNkey = nkeyOf(GROK_CREDS);
  const nc = await connectAs(TESTER_CREDS, fromNkey);
  try {
    const js = nc.jetstream();
    const payload = {
      id,
      ts: Date.now(),
      space: SPACE,
      from: { id: fromNkey, name: fromName, role: "agent" },
      parts: [{ kind: "text", text }],
      to: `local.${toNkey}`,
    };
    const h = headers();
    h.set("Nats-Msg-Id", id);
    await js.publish(
      `cotal.${SPACE}.inst.local.${toNkey}.local.${fromNkey}`,
      Buffer.from(JSON.stringify(payload), "utf8"),
      { headers: h }
    );
    return id;
  } finally {
    await nc.close().catch(() => {});
  }
}

/** Pull DMs for an identity until predicate matches or timeout. Returns {msg, ack} or null. */
async function pullDmUntil(credsPath, predicate, timeoutMs = 10000) {
  const nkey = nkeyOf(credsPath);
  const { stream, durable } = durableOf(credsPath);
  const nc = await connectAs(credsPath, nkey);
  try {
    const js = nc.jetstream();
    const consumer = await js.consumers.get(stream, durable);
    const deadline = Date.now() + timeoutMs;
    while (Date.now() < deadline) {
      const iter = await consumer.fetch({ max_messages: 10, expires: 1500 });
      for await (const m of iter) {
        let payload = null;
        try {
          payload = JSON.parse(Buffer.from(m.data).toString("utf8"));
        } catch {
          continue;
        }
        if (predicate(payload, m)) return { payload, ack: () => m.ack() };
      }
    }
    return null;
  } finally {
    await nc.close().catch(() => {});
  }
}

/** Invoke the stop-hook with simulated Cursor stdin. Returns {stdout, stderr, code}. */
function invokeHook(hookInput) {
  return new Promise((resolve) => {
    const child = spawn("node", [path.join(CONNECTOR, "src", "stop-hook.js")], {
      env: withEnv(),
      stdio: ["pipe", "pipe", "pipe"],
    });
    let stdout = "";
    let stderr = "";
    child.stdout.on("data", (d) => (stdout += d));
    child.stderr.on("data", (d) => (stderr += d));
    const timer = setTimeout(() => {
      child.kill("SIGKILL");
      resolve({ stdout, stderr, code: "TIMEOUT", timedOut: true });
    }, 25000);
    child.on("close", (code) => {
      clearTimeout(timer);
      resolve({ stdout, stderr, code, timedOut: false });
    });
    child.stdin.write(JSON.stringify(hookInput));
    child.stdin.end();
  });
}

/** Start the sidecar; resolves when its presence is visible via watch. Returns the child. */
async function startSidecar() {
  const child = spawn("node", [path.join(CONNECTOR, "src", "sidecar.js")], {
    env: withEnv(),
    stdio: ["ignore", "pipe", "pipe"],
  });
  child.stdout.on("data", () => {});
  child.stderr.on("data", () => {});
  const presenceKey = presenceKeyOf(GROK_CREDS);
  const deadline = Date.now() + 15000;
  while (Date.now() < deadline) {
    const e = await watchPresenceOnce(GROK_CREDS, presenceKey, 3000);
    if (e && e.value) {
      try {
        const rec = JSON.parse(e.value.toString("utf8"));
        if (rec.status === "idle") return child;
      } catch {
        /* malformed — keep waiting */
      }
    }
  }
  try {
    child.kill("SIGKILL");
  } catch {
    /* ignore */
  }
  throw new Error("sidecar presence never became idle (child killed)");
}

function killSidecar(child, signal = "SIGKILL") {
  return new Promise((resolve) => {
    child.on("close", () => resolve());
    try {
      child.kill(signal);
    } catch {
      resolve();
    }
    setTimeout(resolve, 3000);
  });
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

module.exports = {
  ROOT,
  CONNECTOR,
  NATS_URL,
  SPACE,
  GROK_CREDS,
  TESTER_CREDS,
  withEnv,
  connectAs,
  nkeyOf,
  presenceKeyOf,
  durableOf,
  sendDm,
  pullDmUntil,
  invokeHook,
  startSidecar,
  killSidecar,
  watchPresenceOnce,
  sleep,
};
