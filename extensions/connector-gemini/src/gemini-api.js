'use strict';
/**
 * gemini-api.js — Gemini API client abstraction.
 *
 *   GeminiApiClient  — interface: generate(digest, context) -> Promise<string>
 *   MockGeminiClient — test double: records calls to a JSONL log, returns canned text.
 *   RealGeminiClient — Gemini Flash via the public REST API. Reads GEMINI_API_KEY
 *                      from env only. Backs off on 429s and never hammers.
 *
 * Quota discipline (free tier):
 *  - the connector SKIPS the API call entirely when the inbox is empty
 *  - the connector sends a compact DIGEST, never raw history
 *  - on HTTP 429 the client enters backoff (honors Retry-After, else exponential
 *    30s * 2^n capped at 10 min + jitter) and the connector reports `waiting`
 */
const fs = require('fs');

class RateLimitedError extends Error {
  constructor(retryAfterMs) {
    super('Gemini API rate limited (429)');
    this.name = 'RateLimitedError';
    this.retryAfterMs = retryAfterMs;
  }
}

class GeminiApiClient {
  /** @returns {Promise<string>} the model's reply text */
  async generate(_digest, _context) {
    throw new Error('not implemented');
  }
}

/** Test double. Every call appends one JSONL line to MOCK_LOG (if set). */
class MockGeminiClient extends GeminiApiClient {
  constructor(opts) {
    super();
    this.logPath = (opts && opts.logPath) || null;
    this.calls = [];
  }
  async generate(digest, _context) {
    const call = { ts: Date.now(), digest: String(digest).slice(0, 2000) };
    this.calls.push(call);
    if (this.logPath) {
      try {
        fs.appendFileSync(this.logPath, JSON.stringify(call) + '\n');
      } catch (err) {
        console.error('[mock] could not append to MOCK_LOG:', err.message);
      }
    }
    const firstLine = String(digest).split('\n').filter(l => l.trim())[0] || '(empty)';
    return `MOCK-REPLY: received ${String(digest).length} chars; first line: ${firstLine.slice(0, 120)}`;
  }
}

/**
 * Real client. POSTs to the Gemini generateContent endpoint.
 * On 429: throws RateLimitedError (connector catches, backs off, leaves the
 * batch unacked for the next poll — at-least-once, no message loss).
 */
class RealGeminiClient extends GeminiApiClient {
  constructor(opts) {
    super();
    this.apiKey = opts.apiKey; // env only — never logged, never persisted
    this.model = opts.model || 'gemini-2.0-flash';
    this.consecutive429s = 0;
    this.backoffUntil = 0;
    this.baseBackoffMs = 30_000;
    this.maxBackoffMs = 600_000;
  }

  backoffMs(retryAfterMs) {
    if (retryAfterMs && retryAfterMs > 0) return Math.min(retryAfterMs, this.maxBackoffMs);
    const exp = Math.min(this.baseBackoffMs * 2 ** this.consecutive429s, this.maxBackoffMs);
    return exp + Math.floor(Math.random() * 5000); // jitter
  }

  async generate(digest, context) {
    const now = Date.now();
    if (now < this.backoffUntil) {
      throw new RateLimitedError(this.backoffUntil - now);
    }
    const system = [
      'You are the Gemini connector on the Arena Hub agent mesh.',
      'Role: RESEARCH-GRADE traffic only — summaries, triage, small research lookups.',
      'You do NOT write or review code. Keep replies concise (under ~400 words).',
      'Beads command results below were already executed deterministically — report them plainly.',
    ].join(' ');
    const prompt = `${system}\n\n${context || ''}\n\n--- inbound digest ---\n${digest}`;

    const url = `https://generativelanguage.googleapis.com/v1beta/models/${encodeURIComponent(this.model)}:generateContent`;
    const ctl = new AbortController();
    const timer = setTimeout(() => ctl.abort(), 120_000); // never hang a poll forever
    let res;
    try {
      res = await fetch(url, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'x-goog-api-key': this.apiKey },
        body: JSON.stringify({
          contents: [{ parts: [{ text: prompt }] }],
          generationConfig: { maxOutputTokens: 1024, temperature: 0.4 },
        }),
        signal: ctl.signal,
      });
    } catch (err) {
      throw new Error(`Gemini API network failure: ${err.message}`);
    } finally {
      clearTimeout(timer);
    }

    if (res.status === 429) {
      this.consecutive429s += 1;
      let retryAfterMs = 0;
      const ra = res.headers.get('retry-after');
      if (ra) {
        const secs = parseInt(ra, 10);
        if (!Number.isNaN(secs)) retryAfterMs = secs * 1000;
      }
      this.backoffUntil = Date.now() + this.backoffMs(retryAfterMs);
      throw new RateLimitedError(this.backoffUntil - Date.now());
    }
    if (!res.ok) {
      const body = await res.text().catch(() => '');
      throw new Error(`Gemini API error ${res.status}: ${body.slice(0, 300)}`);
    }
    this.consecutive429s = 0;
    const data = await res.json();
    const parts = (((data.candidates || [])[0] || {}).content || {}).parts || [];
    return parts.map(p => p.text || '').join('').trim() || '(empty response from Gemini)';
  }
}

function createClient(cfg) {
  if (cfg.clientKind === 'real') {
    return new RealGeminiClient({ apiKey: cfg.geminiApiKey, model: cfg.geminiModel });
  }
  return new MockGeminiClient({ logPath: cfg.mockLog });
}

module.exports = { GeminiApiClient, MockGeminiClient, RealGeminiClient, RateLimitedError, createClient };
