'use strict';
/**
 * test-connector.js — acceptance tests for the Gemini connector (mock API client).
 *
 * Proves against a live isolated mesh:
 *  (1) presence visible on the roster
 *  (2) DM tester→gemini is picked up and passed to the mock API (assert receipt)
 *  (3) mock reply publishes back; tester receives it with replyTo
 *  (4) empty inbox => no API call (quota discipline)
 *  (5) offline replay — stop connector, DM, restart, mock receives it
 *  (6) channel inbound => channel reply (bonus)
 *
 * Env (defaults target the standard isolated test mesh):
 *   TEST_SPACE, TEST_SERVER, TEST_MESH_DIR,
 *   GEMINI_CREDS, TESTER_CREDS, CONNECTOR_DIR
 *
 * Exit 0 = all pass; non-zero = failure (with per-test PASS/FAIL lines).
 */
const fs = require('fs');
const path = require('path');
const os = require('os');
const { spawn, execFile } = require('child_process');
const { connect, credsAuthenticator, StringCodec, headers } = require('nats');
const { parseCreds, derivePrincipal, deriveLifecycleUid } = require('../src/config');

const sc = StringCodec();
const home = os.homedir();
const SPACE = process.env.TEST_SPACE || 'gemini-test';
const SERVER = process.env.TEST_SERVER || 'nats://127.0.0.1:14222';
const MESH_DIR = process.env.TEST_MESH_DIR || path.join(home, 'workspace/ops/cotal-test/mesh-test-gemini');
const CONNECTOR_DIR = process.env.CONNECTOR_DIR || path.join(home, 'workspace/ops/cotal-test/connector-gemini');
const GEMINI_CREDS = process.env.GEMINI_CREDS || path.join(MESH_DIR, 'creds/gemini.creds');
const TESTER_CREDS = process.env.TESTER_CREDS || path.join(MESH_DIR, 'creds/tester.creds');
const LOG_DIR = path.join(MESH_DIR, 'test-logs');
fs.mkdirSync(LOG_DIR, { recursive: true });
const MOCK_LOG = path.join(LOG_DIR, 'mock.jsonl');
const CONN_LOG = path.join(LOG_DIR, 'connector.log');

function ident(credsPath) {
  const text = fs.readFileSync(credsPath, 'utf8');
  const { nkey, payload } = parseCreds(text);
  const grants = (((payload.nats || {}).pub || {}).allow || [])
    .concat((((payload.nats || {}).sub || {}).allow || []));
  const { owner, actor } = derivePrincipal(SPACE, grants);
  return { text, nkey, owner, actor, grants,
    lifecycleUid: deriveLifecycleUid(SPACE, owner, actor, grants) };
}

async function meshConnect(credsText, nkey) {
  return connect({
    servers: SERVER,
    authenticator: credsAuthenticator(Buffer.from(credsText, 'utf8')),
    inboxPrefix: `_INBOX_${nkey}`,
    name: 'gemini-connector-test',
  });
}

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }
function mockCalls() {
  if (!fs.existsSync(MOCK_LOG)) return [];
  return fs.readFileSync(MOCK_LOG, 'utf8').split('\n').filter(Boolean).map(l => JSON.parse(l));
}

function startConnector(extraEnv, clearLog) {
  if (clearLog !== false) { try { fs.unlinkSync(MOCK_LOG); } catch (_e) {} }
  const logFd = fs.openSync(CONN_LOG, 'a');
  const env = Object.assign({}, process.env, {
    COTAL_SPACE: SPACE,
    COTAL_SERVER: SERVER,
    COTAL_CREDS: GEMINI_CREDS,
    COTAL_AGENT_NAME: 'gemini',
    COTAL_SUBSCRIBE: 'testchan',
    POLL_INTERVAL_MS: '3000',
    GEMINI_CLIENT: 'mock',
    MOCK_LOG,
    CONNECTOR_MODE: 'daemon',
  }, extraEnv || {});
  const child = spawn('node', [path.join(CONNECTOR_DIR, 'bin/gemini-connector.js')],
    { env, stdio: ['ignore', logFd, logFd] });
  return child;
}
function stopConnector(child) {
  return new Promise((resolve) => {
    if (!child || child.exitCode !== null) return resolve();
    child.on('exit', () => resolve());
    child.kill('SIGTERM');
    setTimeout(() => { try { child.kill('SIGKILL'); } catch (_e) {} resolve(); }, 5000);
  });
}
function cotalEndpoints() {
  return new Promise((resolve, reject) => {
    execFile(path.join(home, '.local/bin/cotal'), ['endpoints'], { cwd: MESH_DIR, timeout: 20000 },
      (err, stdout, stderr) => err ? reject(new Error(stderr || err.message)) : resolve(stdout));
  });
}

const results = [];
function report(name, pass, detail) {
  results.push({ name, pass });
  console.log(`${pass ? 'PASS' : 'FAIL'} — ${name}${detail ? ' — ' + detail : ''}`);
}

(async () => {
  const gem = ident(GEMINI_CREDS);
  const tst = ident(TESTER_CREDS);
  console.log(`[test] space=${SPACE} server=${SERVER}`);
  console.log(`[test] gemini=${gem.owner}.${gem.actor} tester=${tst.owner}.${tst.actor}`);

  let child = startConnector();
  await sleep(1000);
  if (child.exitCode !== null) {
    report('connector starts', false, `exited immediately (code ${child.exitCode}); see ${CONN_LOG}`);
    process.exit(1);
  }

  // ---- (1) presence visible ----
  let presenceOk = false, presenceDetail = '';
  for (let i = 0; i < 6 && !presenceOk; i++) {
    await sleep(2500);
    try {
      const out = await cotalEndpoints();
      const line = out.split('\n').find(l => l.includes('gemini') && !l.includes('gemini-connector'));
      if (line) { presenceOk = true; presenceDetail = line.trim(); }
    } catch (e) { presenceDetail = e.message; }
  }
  report('1: presence visible on roster', presenceOk, presenceDetail);

  // ---- tester harness ----
  // NOTE: DM receive is via the tester's durable consumer (like a real agent) —
  // a core subscription on inst.> is NOT in the sub grants.
  const nc = await meshConnect(tst.text, tst.nkey);
  const tjs = nc.jetstream();
  const tcons = await tjs.consumers.get(`DM_${SPACE}`, `dm_${tst.owner}-${tst.actor}-${tst.lifecycleUid}`);
  const inbox = [];
  let dmPumpRunning = true;
  const dmPump = (async () => {
    while (dmPumpRunning) {
      try {
        const iter = await tcons.fetch({ max_messages: 50, expires: 1500 });
        for await (const m of iter) {
          inbox.push(m);
          try { m.ack(); } catch (_e) { /* test-side: ack on receipt */ }
        }
      } catch (_e) { await sleep(500); }
    }
  })();
  const chSub = nc.subscribe(`cotal.${SPACE}.chat.*.*.testchan`);
  const pump = async (sub) => { for await (const m of sub) inbox.push(m); };
  pump(chSub).catch(() => {});

  async function sendDM(text) {
    const id = `test-${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 8)}`;
    const h = headers(); h.set('Nats-Msg-Id', id);
    const subject = `cotal.${SPACE}.inst.${gem.owner}.${gem.actor}.${tst.owner}.${tst.actor}`;
    const body = {
      id, ts: Date.now(), space: SPACE,
      from: { id: `${tst.owner}.${tst.actor}`, name: 'tester' },
      to: `${gem.owner}.${gem.actor}`,
      parts: [{ type: 'text', text }],
    };
    await nc.publish(subject, sc.encode(JSON.stringify(body)), { headers: h });
    return id;
  }
  async function waitForInbox(pred, timeoutMs, label) {
    const deadline = Date.now() + timeoutMs;
    while (Date.now() < deadline) {
      const i = inbox.findIndex(pred);
      if (i >= 0) return inbox.splice(i, 1)[0];
      await sleep(300);
    }
    throw new Error(`timeout waiting for ${label}`);
  }
  const parseBody = (m) => JSON.parse(sc.decode(m.data));

  // ---- (2) DM picked up by mock ----
  const ping = `ping-research-${Date.now()}`;
  const dmId = await sendDM(ping);
  let gotCall = null;
  for (let i = 0; i < 20 && !gotCall; i++) {
    await sleep(1000);
    gotCall = mockCalls().find(c => c.digest.includes(ping));
  }
  report('2: DM picked up and passed to mock API', !!gotCall,
    gotCall ? `mock call #${mockCalls().length} contains the DM text` : 'mock never saw the DM');

  // ---- (3) reply back with replyTo ----
  let replyOk = false, replyDetail = '';
  try {
    const m = await waitForInbox(
      (x) => { try { return parseBody(x).replyTo === dmId; } catch (_e) { return false; } },
      20000, 'reply with replyTo');
    const body = parseBody(m);
    replyOk = typeof body.parts?.[0]?.text === 'string' && body.parts[0].text.startsWith('MOCK-REPLY:');
    replyDetail = `replyTo=${body.replyTo} subject=${m.subject}`;
  } catch (e) { replyDetail = e.message; }
  report('3: mock reply received with replyTo', replyOk, replyDetail);

  // ---- (4) empty inbox => no API call ----
  const callsBefore = mockCalls().length;
  await sleep(7500); // >2 poll cycles at 3s
  const callsAfter = mockCalls().length;
  report('4: empty inbox => no API call (quota discipline)',
    callsAfter === callsBefore, `calls before=${callsBefore} after=${callsAfter}`);

  // ---- (5) offline replay ----
  await stopConnector(child); child = null;
  await sleep(1500); // ensure presence flips offline / sockets close
  const offlinePing = `offline-ping-${Date.now()}`;
  const offlineId = await sendDM(offlinePing);
  await sleep(1500); // let the broker store it while nobody is home
  child = startConnector(null, false);
  let replayed = null;
  for (let i = 0; i < 25 && !replayed; i++) {
    await sleep(1000);
    replayed = mockCalls().find(c => c.digest.includes(offlinePing));
  }
  report('5: offline replay (stop, DM, restart)', !!replayed,
    replayed ? 'held DM was pulled from the durable on restart' : 'mock never saw the offline DM');
  // also confirm the reply came back with replyTo
  let replayReplyOk = false;
  try {
    const m = await waitForInbox(
      (x) => { try { return parseBody(x).replyTo === offlineId; } catch (_e) { return false; } },
      20000, 'replay reply');
    replayReplyOk = parseBody(m).parts?.[0]?.text?.startsWith('MOCK-REPLY:');
  } catch (_e) { /* already reported */ }
  report('5b: replayed reply carries replyTo', replayReplyOk);

  // ---- (6) channel inbound => channel reply (bonus) ----
  const chanPing = `chan-ping-${Date.now()}`;
  const chanId = `chantest-${Date.now().toString(36)}`;
  const chh = headers(); chh.set('Nats-Msg-Id', chanId);
  await nc.publish(
    `cotal.${SPACE}.chat.${tst.owner}.${tst.actor}.testchan`,
    sc.encode(JSON.stringify({
      id: chanId, ts: Date.now(), space: SPACE,
      from: { id: `${tst.owner}.${tst.actor}`, name: 'tester' },
      channel: 'testchan',
      parts: [{ type: 'text', text: chanPing }],
    })), { headers: chh });
  let chanCall = null;
  for (let i = 0; i < 20 && !chanCall; i++) {
    await sleep(1000);
    chanCall = mockCalls().find(c => c.digest.includes(chanPing));
  }
  let chanReplyOk = false, chanReplyDetail = '';
  try {
    const m = await waitForInbox(
      (x) => {
        try {
          const b = parseBody(x);
          return b.replyTo === chanId && x.subject.includes('.chat.');
        } catch (_e) { return false; }
      }, 20000, 'channel reply');
    chanReplyOk = parseBody(m).parts?.[0]?.text?.startsWith('MOCK-REPLY:');
    chanReplyDetail = `subject=${m.subject}`;
  } catch (e) { chanReplyDetail = e.message; }
  report('6: channel inbound => mock + channel reply', !!chanCall && chanReplyOk, chanReplyDetail);

  await stopConnector(child);
  dmPumpRunning = false;
  await nc.drain();

  const failed = results.filter(r => !r.pass);
  console.log(`\n${results.length - failed.length}/${results.length} tests passed`);
  process.exit(failed.length ? 1 : 0);
})().catch(err => {
  console.error('TEST HARNESS FATAL:', err);
  process.exit(2);
});
