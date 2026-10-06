/**
 * Muse connector — poll-loop sidecar.
 *
 * Inbound:  poll the mesh DM durable + channel filters → file drop
 *            `inbox/<msgId>.md` (headers + body). This file drop IS the
 *            surfacing mechanism: Muse (the bot) reads these files on its own
 *            schedule. There is NO push to Muse — wake: none (documented).
 * Outbound: poll `outbox/*.md` → publish to the mesh with `replyTo` →
 *            move to `sent/<msgId>.md`. Malformed files → `quarantine/` + log.
 * Presence: KV heartbeat every 2s (TTL 6s); idle/working, where
 *            working = inbox items exist that Muse hasn't acked yet.
 * Beads:    `/claim <id>` / `/release <id>` command lines in inbound messages
 *            shell out to `bd`; the result is posted back to the mesh.
 *            Beads is the source of truth — no parallel claim registry.
 */

const { execFile } = require("node:child_process");
const {
  mkdir,
  readdir,
  readFile,
  writeFile,
  rename,
  appendFile,
  stat,
} = require("node:fs/promises");
const { join, basename } = require("node:path");
const { MeshClient } = require("./mesh.js");
const {
  renderInboxFile,
  parseOutboxFile,
  messageText,
  safeFileName,
} = require("./wire.js");
const { isConcreteChannel } = require("./subjects.js");

const DEFAULTS = {
  space: "main",
  owner: "arena",
  actor: "muse",
  server: "nats://127.0.0.1:4222",
  dir: "./connectors/muse",
  pollIntervalMs: 15000,
  presenceIntervalMs: 2000,
};

function configFromEnv(env = process.env) {
  const num = (v, d) => {
    const n = Number(v);
    return Number.isFinite(n) && n > 0 ? n : d;
  };
  return {
    space: env.COTAL_SPACE || DEFAULTS.space,
    owner: env.COTAL_OWNER || DEFAULTS.owner,
    actor: env.COTAL_ACTOR || DEFAULTS.actor,
    role: env.COTAL_ROLE || null,
    credsFile: env.COTAL_CREDS || null,
    lifecycleUid: env.COTAL_LIFECYCLE_UID || null,
    server: env.COTAL_SERVER || DEFAULTS.server,
    dir: env.MUSE_CONNECTOR_DIR || DEFAULTS.dir,
    channels: (env.COTAL_CHANNELS || "")
      .split(",")
      .map((s) => s.trim())
      .filter(Boolean),
    pollIntervalMs: num(env.POLL_INTERVAL_MS, DEFAULTS.pollIntervalMs),
    presenceIntervalMs: num(env.PRESENCE_INTERVAL_MS, DEFAULTS.presenceIntervalMs),
    bdBin: env.BD_BIN || "bd",
  };
}

function splitPrincipal(name) {
  const parts = String(name || "").split(".");
  if (parts.length !== 2 || !parts[0] || !parts[1]) return null;
  return { owner: parts[0], actor: parts[1] };
}

function runBd(bdBin, args, timeoutMs = 15000) {
  return new Promise((resolve) => {
    execFile(bdBin, args, { timeout: timeoutMs, maxBuffer: 256 * 1024 }, (err, stdout, stderr) => {
      resolve({
        ok: !err,
        code: err?.code ?? 0,
        stdout: String(stdout || "").trim(),
        stderr: String(stderr || err?.message || "").trim(),
      });
    });
  });
}

class MuseConnector {
  constructor(config) {
    this.cfg = config;
    this.dir = config.dir;
    this.inboxDir = join(this.dir, "inbox");
    this.outboxDir = join(this.dir, "outbox");
    this.sentDir = join(this.dir, "sent");
    this.quarantineDir = join(this.dir, "quarantine");
    this.stateFile = join(this.dir, "state.json");
    this.logFile = join(this.dir, "connector.log");
    this.mesh = new MeshClient({
      space: config.space,
      owner: config.owner,
      actor: config.actor,
      role: config.role,
      credsFile: config.credsFile,
      lifecycleUid: config.lifecycleUid,
      server: config.server,
    });
    this.state = { seen: {}, inbox: {} };
    this.running = false;
    this._pollTimer = null;
    this._presenceTimer = null;
  }

  async log(line) {
    const entry = `${new Date().toISOString()} ${line}\n`;
    process.stdout.write(`[muse-connector] ${entry}`);
    try {
      await appendFile(this.logFile, entry);
    } catch { /* logging must never break the loop */ }
  }

  async ensureDirs() {
    for (const d of [this.dir, this.inboxDir, this.outboxDir, this.sentDir, this.quarantineDir]) {
      await mkdir(d, { recursive: true });
    }
  }

  async loadState() {
    try {
      const raw = await readFile(this.stateFile, "utf8");
      const s = JSON.parse(raw);
      if (s && typeof s === "object") {
        this.state.seen = s.seen && typeof s.seen === "object" ? s.seen : {};
        this.state.inbox = s.inbox && typeof s.inbox === "object" ? s.inbox : {};
      }
    } catch { /* first run — start empty */ }
  }

  async saveState() {
    try {
      // Bound the dedup set so state.json can't grow forever.
      const ids = Object.keys(this.state.seen).sort(
        (a, b) => this.state.seen[a] - this.state.seen[b]
      );
      while (ids.length > 2000) delete this.state.seen[ids.shift()];
      await writeFile(this.stateFile, JSON.stringify(this.state, null, 2));
    } catch (err) {
      await this.log(`WARN: could not save state: ${err.message}`);
    }
  }

  async start() {
    await this.ensureDirs();
    await this.loadState();
    await this.log(
      `starting (space=${this.cfg.space} principal=${this.cfg.owner}.${this.cfg.actor} ` +
        `poll=${this.cfg.pollIntervalMs}ms presence=${this.cfg.presenceIntervalMs}ms)`
    );
    // Fail-before-presence: mesh.connect() binds the DM durable BEFORE any
    // presence write happens. A wrong lifecycle uid dies here, ghost-free.
    await this.mesh.connect();
    await this.mesh.subscribeChannels(this.cfg.channels);
    this.running = true;

    // Presence heartbeat — best-effort, never gates delivery.
    const beat = async () => {
      if (!this.running) return;
      await this.mesh.publishPresence(this.currentStatus());
    };
    await beat();
    this._presenceTimer = setInterval(beat, this.cfg.presenceIntervalMs);

    // The poll loop: inbound durable pull + outbox sweep.
    const tick = async () => {
      if (!this.running) return;
      try {
        await this.pollOnce();
      } catch (err) {
        await this.log(`ERROR in poll tick: ${err.stack || err.message}`);
      }
    };
    await tick();
    this._pollTimer = setInterval(tick, this.cfg.pollIntervalMs);
    await this.log("running");
  }

  async stop() {
    this.running = false;
    if (this._pollTimer) clearInterval(this._pollTimer);
    if (this._presenceTimer) clearInterval(this._presenceTimer);
    try {
      await this.mesh.publishPresence("offline");
    } catch { /* noop */ }
    await this.mesh.close();
    await this.saveState();
    await this.log("stopped");
  }

  /** working = inbox items exist that Muse hasn't acked yet; else idle. */
  currentStatus() {
    const unacked = Object.values(this.state.inbox).filter((e) => !e.acked);
    return unacked.length > 0 ? "working" : "idle";
  }

  /** Muse-level ack: an inbox item is acked when Muse proved it read it —
   *  either an explicit `inbox/<id>.ack` sidecar, or a reply (outbox or sent
   *  file) carrying `ReplyTo: <id>`. Scanned every tick; never assumed. */
  async refreshAcks() {
    const replyTos = new Set();
    for (const d of [this.outboxDir, this.sentDir]) {
      let files = [];
      try {
        files = await readdir(d);
      } catch { /* missing dir */ }
      for (const f of files) {
        if (!f.endsWith(".md")) continue;
        try {
          const text = await readFile(join(d, f), "utf8");
          const m = text.match(/^ReplyTo\s*:\s*(\S+)/im);
          if (m) replyTos.add(m[1].trim());
        } catch { /* unreadable file — skip */ }
      }
    }
    for (const [id, entry] of Object.entries(this.state.inbox)) {
      if (entry.acked) continue;
      let sidecar = false;
      try {
        await stat(join(this.inboxDir, `${safeFileName(id)}.ack`));
        sidecar = true;
      } catch { /* no sidecar */ }
      if (sidecar || replyTos.has(id)) {
        entry.acked = true;
        await this.log(`ack: inbox item ${id} acknowledged by Muse (${sidecar ? "ack sidecar" : "reply file"})`);
      }
    }
  }

  async pollOnce() {
    await this.refreshAcks();
    await this.pollInbound();
    await this.pollOutbox();
    await this.saveState();
  }

  // ---- inbound: mesh → inbox/ file drop ---------------------------------

  async pollInbound() {
    const pending = await this.mesh.fetchPending();
    for (const { msg, ack, nak } of pending) {
      if (this.state.seen[msg.id]) {
        ack(); // already surfaced in a previous tick — move the durable on
        continue;
      }
      const fileName = `${safeFileName(msg.id)}.md`;
      const filePath = join(this.inboxDir, fileName);
      try {
        await writeFile(filePath, renderInboxFile(msg), { flag: "wx" });
      } catch (err) {
        if (err.code === "EEXIST") {
          // File already there from an earlier incarnation — still surfaced.
        } else {
          await this.log(`ERROR: could not write inbox file for ${msg.id}: ${err.message}`);
          nak(10000); // leave it for redelivery; file drop is the surfacing proof
          continue;
        }
      }
      // Surfacing proof = the file drop. Ack binds to THIS, never to receipt.
      ack();
      this.state.seen[msg.id] = Date.now();
      this.state.inbox[msg.id] = { file: fileName, acked: false, ts: msg.ts };
      await this.log(
        `inbox: ${msg.id} from ${msg.from?.name} (${msg.channel ? `channel #${msg.channel}` : msg.to ? "dm" : "anycast"}) → ${fileName}`
      );
      // Beads command lines in the body are intents, not parallel state.
      await this.handleBeadsCommand(msg);
    }
  }

  async handleBeadsCommand(msg) {
    const firstLine = messageText(msg).split("\n")[0].trim();
    const m = firstLine.match(/^\/(claim|release)\s+(\S+)\s*$/i);
    if (!m) return;
    const [, verb, taskId] = m;
    const sender = splitPrincipal(msg.from?.name);
    if (!sender) {
      await this.log(`beads: /${verb} ${taskId} ignored — sender principal unparseable`);
      return;
    }
    await this.log(`beads: /${verb} ${taskId} from ${msg.from.name} → shelling out to ${this.cfg.bdBin}`);
    const result = await runBd(
      this.cfg.bdBin,
      verb.toLowerCase() === "claim" ? ["claim", taskId] : ["release", taskId]
    );
    const report = result.ok
      ? `beads ${verb} ${taskId}: OK\n${result.stdout || "(no output)"}`
      : `beads ${verb} ${taskId}: FAILED (exit ${result.code})\n${result.stderr || result.stdout || "(no output)"}\n` +
        `Note: Beads is the source of truth; this connector keeps no claim registry.`;
    try {
      await this.mesh.dm({
        toOwner: sender.owner,
        toActor: sender.actor,
        text: report,
        replyTo: msg.id,
        contextId: msg.contextId,
      });
      await this.log(`beads: result for ${taskId} posted back to ${sender.owner}.${sender.actor}`);
    } catch (err) {
      await this.log(`ERROR: could not post beads result: ${err.message}`);
    }
  }

  // ---- outbound: outbox/ → mesh ------------------------------------------

  async pollOutbox() {
    let files = [];
    try {
      files = (await readdir(this.outboxDir)).filter((f) => f.endsWith(".md")).sort();
    } catch {
      return;
    }
    for (const f of files) {
      const path = join(this.outboxDir, f);
      let text;
      try {
        text = await readFile(path, "utf8");
      } catch (err) {
        await this.log(`WARN: could not read outbox file ${f}: ${err.message}`);
        continue;
      }
      let parsed;
      try {
        parsed = parseOutboxFile(text, f);
      } catch (err) {
        await this.quarantine(f, path, err.message);
        continue;
      }
      try {
        let sent;
        if (parsed.to) {
          const target = splitPrincipal(parsed.to) || splitPrincipal(`${this.cfg.owner}.${parsed.to}`);
          if (!target) throw new Error(`bad "To:" value "${parsed.to}" — want <owner>.<actor>`);
          sent = await this.mesh.dm({
            toOwner: target.owner,
            toActor: target.actor,
            text: parsed.body,
            replyTo: parsed.replyTo,
            contextId: parsed.contextId,
            mentions: parsed.mentions,
          });
        } else {
          if (!isConcreteChannel(parsed.channel)) {
            throw new Error(`bad "Channel:" value "${parsed.channel}" — wildcards are not publishable`);
          }
          sent = await this.mesh.sendChannel({
            channel: parsed.channel,
            text: parsed.body,
            replyTo: parsed.replyTo,
            contextId: parsed.contextId,
            mentions: parsed.mentions,
          });
        }
        const sentName = `${safeFileName(sent.id)}.md`;
        await rename(path, join(this.sentDir, sentName));
        await this.log(
          `sent: ${f} → ${parsed.to ? `dm ${parsed.to}` : `channel #${parsed.channel}`} as ${sent.id}`
        );
        // A reply proves Muse read the inbound — refresh acks immediately.
        await this.refreshAcks();
      } catch (err) {
        await this.quarantine(f, path, `publish failed: ${err.message}`);
      }
    }
  }

  async quarantine(fileName, path, reason) {
    try {
      await rename(path, join(this.quarantineDir, fileName));
    } catch (err) {
      await this.log(`ERROR: could not quarantine ${fileName}: ${err.message}`);
      return;
    }
    // Malformed files are NEVER silently dropped — always a log line.
    await this.log(`quarantine: ${fileName} → quarantine/ (${reason})`);
  }
}

module.exports = { MuseConnector, configFromEnv, DEFAULTS };
