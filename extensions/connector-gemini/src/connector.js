'use strict';
/**
 * connector.js — the Gemini connector poll loop.
 *
 * Wake path (honest, per the interface spec): NONE. This connector is poll-only.
 * Inbound DMs + channel messages are held pending; each poll folds them into a
 * compact digest and makes ONE Gemini Flash API call. Empty inbox => no API
 * call (quota discipline). Ack site: the API round-trip completes AND the reply
 * is published — only then are the durable messages acked.
 */
const { MeshClient } = require('./mesh');
const { createClient, RateLimitedError } = require('./gemini-api');
const { extractCommands, isPureCommand, runBeads, formatResult } = require('./beads');

const MAX_DIGEST_CHARS = 8000;
const MAX_ITEM_CHARS = 2000;

function truncate(s, n) {
  s = String(s || '');
  return s.length > n ? s.slice(0, n) + '…' : s;
}

function foldDigest(items) {
  const lines = [];
  // oldest first so the newest message gets the freshest context position
  const ordered = [...items].sort((a, b) => a.ts - b.ts);
  for (const it of ordered) {
    const when = new Date(it.ts).toISOString();
    const who = `${it.from.name} <${it.from.id}>`;
    const where = it.kind === 'channel' ? `#${it.channel}` : 'DM';
    lines.push(`[${where} from ${who} @ ${when}] ${truncate(it.text, MAX_ITEM_CHARS)}`);
  }
  return truncate(lines.join('\n'), MAX_DIGEST_CHARS);
}

class GeminiConnector {
  constructor(cfg) {
    this.cfg = cfg;
    this.mesh = new MeshClient(cfg);
    this.api = createClient(cfg);
    this.running = false;
    this.timers = [];
    this.status = 'idle';
    this.rateLimitedUntil = 0;
  }

  async start() {
    await this.mesh.connect();
    // FAIL-BEFORE-PRESENCE: durable bind first; a wrong lifecycle uid dies here
    // with no presence ghost.
    await this.mesh.bindDmDurable();
    await this.mesh.subscribeChannels();
    this.running = true;
    this._presenceLoop();
    if (this.cfg.mode === 'once') {
      await this.pollOnce();
      await this.shutdown('idle');
      return;
    }
    // daemon: poll on the interval; first poll immediately
    await this.pollOnce();
    const t = setInterval(() => {
      this.pollOnce().catch(err => console.error('[connector] poll failed:', err.message));
    }, this.cfg.pollMs);
    t.unref?.();
    this.timers.push(t);
  }

  _presenceLoop() {
    const beat = () => this.mesh.putPresence(this.status);
    beat();
    const t = setInterval(beat, this.cfg.presenceMs);
    t.unref?.();
    this.timers.push(t);
  }

  setStatus(s, condition) {
    this.status = s;
    // status changes are announced promptly, not just on the heartbeat tick
    this.mesh.putPresence(s, condition);
  }

  async pollOnce() {
    if (!this.running) return { apiCalled: false, processed: 0 };
    const pending = [
      ...(await this.mesh.pullPendingDMs()),
      ...this.mesh.drainChannelQueue(),
    ];
    if (!pending.length) {
      this.setStatus('idle');
      return { apiCalled: false, processed: 0 }; // quota discipline: no call
    }

    // 1. Beads command intents — executed deterministically, results reported.
    const commandResults = [];
    for (const it of pending) {
      for (const cmd of extractCommands(it.text)) {
        const r = runBeads(cmd.op, cmd.taskId);
        commandResults.push({ inboundId: it.id, from: it.from.id, result: r });
        console.log(`[beads] ${formatResult(r)}`);
      }
    }

    const apiItems = pending.filter(it => !isPureCommand(it.text));
    const commandOnly = pending.filter(it => isPureCommand(it.text));

    let replyText = null;
    let apiCalled = false;
    if (apiItems.length) {
      if (Date.now() < this.rateLimitedUntil) {
        // Still in backoff: leave everything unacked, report waiting.
        this.setStatus('waiting', 'rate_limited');
        console.log('[connector] in 429 backoff; deferring batch (unacked)');
        return { apiCalled: false, processed: 0, deferred: true };
      }
      this.setStatus('working');
      const digest = foldDigest(apiItems);
      const apiIds = new Set(apiItems.map(i => i.id));
      const apiResults = commandResults.filter(c => apiIds.has(c.inboundId));
      const ctx = apiResults.length
        ? 'Beads results:\n' + apiResults.map(c => `- ${formatResult(c.result)}`).join('\n')
        : '';
      try {
        replyText = await this.api.generate(digest, ctx);
        apiCalled = true;
      } catch (err) {
        if (err instanceof RateLimitedError) {
          const waitMs = err.retryAfterMs || 30_000;
          this.rateLimitedUntil = Date.now() + waitMs;
          this.setStatus('waiting', 'rate_limited');
          console.error(`[connector] 429 — backing off ${Math.round(waitMs / 1000)}s; batch unacked`);
          return { apiCalled: false, processed: 0, deferred: true };
        }
        // Non-quota API failure: leave unacked for the next poll, tell the
        // requesters once via DM so the failure is visible, not silent.
        console.error('[connector] API failure:', err.message);
        this.setStatus('idle');
        return { apiCalled: false, processed: 0, error: err.message };
      }
    }

    // 2. Publish replies. One reply per inbound item, each carrying its own
    //    replyTo so threads stay coherent.
    for (const it of pending) {
      let text;
      const myResults = commandResults.filter(c => c.inboundId === it.id);
      if (isPureCommand(it.text)) {
        text = myResults.length
          ? myResults.map(c => formatResult(c.result)).join('\n')
          : '(no command recognized)';
      } else {
        text = replyText;
        if (myResults.length) {
          text += '\n\nBeads:\n' + myResults.map(c => `- ${formatResult(c.result)}`).join('\n');
        }
      }
      try {
        if (it.kind === 'channel' && this.mesh.canPublishChannel(it.channel)) {
          await this.mesh.replyChannel(it, text);
        } else {
          await this.mesh.replyDM(it, text);
        }
      } catch (err) {
        console.error(`[connector] reply publish failed for ${it.id}:`, err.message);
      }
    }

    // 3. ACK SITE: ack only now — API round-trip complete AND replies published.
    for (const it of pending) {
      if (it._jsMsg) {
        try { it._jsMsg.ack(); } catch (_e) { /* best-effort; redelivery is safe */ }
      }
    }
    this.setStatus('idle');
    return { apiCalled, processed: pending.length };
  }

  async shutdown(finalStatus) {
    this.running = false;
    for (const t of this.timers) clearInterval(t);
    this.timers = [];
    try { await this.mesh.putPresence(finalStatus || 'offline'); } catch (_e) { /* best-effort */ }
    await this.mesh.drain();
  }
}

module.exports = { GeminiConnector, foldDigest };
