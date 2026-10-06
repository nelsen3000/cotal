// 1min.ai API client abstraction.
//
// The 1min.ai API is request/response only: POST /api/chat-with-ai with a
// NON-OpenAI-compatible body ({type:"UNIFY_CHAT_WITH_AI", model,
// promptObject:{prompt, isMixed, webSearch}}), authenticated via the `API-KEY`
// request header. There is no push, webhook, streaming-subscription, or
// "wake the client" primitive in 1min.ai's API, so the connector's read path
// is honest polling (see connector.js): pending mesh messages are folded into
// the next scheduled call's prompt.
//
// Request shape verified 2026-10-06 against the live API (via the working
// onemin.py CLI): response text lives at
// aiRecord.aiRecordDetail.resultObject (list of strings, or a single string).
//
// Interface: OneMinClient#chat({ system, messages, model, webSearch, timeoutMs })
//   -> { text, raw }
//
// Implementations:
//   - RealOneMinClient  (reads ONEMIN_API_KEY from env — never in code)
//   - MockOneMinClient  (used by tests; records calls to a JSONL log file)

"use strict";

const https = require("https");

function buildPrompt(system, messages) {
  // messages: [{role, content}] — content already carries the mesh sender
  // prefix ([mesh:owner.actor id:msgid] ...). Mirrors onemin.py's framing.
  const blocks = messages.map((m) => `[User]\n${m.content}`);
  return (system ? `[System instructions]\n${system}\n\n` : "") + blocks.join("\n\n");
}

function extractText(d) {
  // Verified shape: aiRecord.aiRecordDetail.resultObject = ["text", ...]
  try {
    const ro = d.aiRecord.aiRecordDetail.resultObject;
    if (Array.isArray(ro)) {
      const parts = ro.filter((x) => typeof x === "string" && x.trim());
      if (parts.length) return parts.join("\n").trim();
    } else if (typeof ro === "string" && ro.trim()) {
      return ro.trim();
    }
  } catch {
    // fall through to null
  }
  return null;
}

class RealOneMinClient {
  constructor({ apiKey, model = "gpt-4o-mini", baseUrl = "https://api.1min.ai" } = {}) {
    if (!apiKey) throw new Error("RealOneMinClient: ONEMIN_API_KEY is not set");
    this.apiKey = apiKey;
    this.model = model;
    this.baseUrl = baseUrl;
  }

  chat({ system, messages, model, webSearch = false, timeoutMs = 60000 }) {
    const body = JSON.stringify({
      type: "UNIFY_CHAT_WITH_AI",
      model: model || this.model,
      promptObject: {
        prompt: buildPrompt(system, messages),
        isMixed: false,
        webSearch: !!webSearch,
      },
    });
    const url = new URL("/api/chat-with-ai", this.baseUrl);
    return new Promise((resolve, reject) => {
      const req = https.request(
        {
          hostname: url.hostname,
          port: 443,
          path: url.pathname,
          method: "POST",
          headers: {
            "API-KEY": this.apiKey,
            "Content-Type": "application/json",
            "Content-Length": Buffer.byteLength(body),
          },
          timeout: timeoutMs,
        },
        (res) => {
          let data = "";
          res.on("data", (c) => (data += c));
          res.on("end", () => {
            if (res.statusCode < 200 || res.statusCode >= 300) {
              return reject(
                new Error(`1min.ai API HTTP ${res.statusCode}: ${data.slice(0, 500)}`)
              );
            }
            try {
              const json = JSON.parse(data);
              const text = extractText(json) || "[empty response]";
              resolve({ text, raw: json });
            } catch (e) {
              reject(new Error(`1min.ai API: bad JSON: ${e.message}`));
            }
          });
        }
      );
      req.on("timeout", () => req.destroy(new Error("1min.ai API: request timed out")));
      req.on("error", reject);
      req.write(body);
      req.end();
    });
  }
}

class MockOneMinClient {
  // logPath: JSONL file where every call is appended (tests assert on this).
  // reply: function(input) -> string, or a fixed string.
  constructor({ logPath, reply } = {}) {
    this.logPath = logPath;
    this.calls = [];
    this.reply =
      typeof reply === "function"
        ? reply
        : () => reply || "MOCK-REPLY: noted. (1min.ai mock client)";
  }

  async chat(input) {
    const record = { ts: new Date().toISOString(), input };
    this.calls.push(record);
    if (this.logPath) {
      require("fs").appendFileSync(this.logPath, JSON.stringify(record) + "\n");
    }
    return { text: this.reply(input), raw: { mock: true } };
  }
}

function clientFromEnv(env = process.env) {
  if (env.ONEMIN_USE_MOCK === "1") {
    return new MockOneMinClient({
      logPath: env.ONEMIN_MOCK_LOG,
      reply: env.ONEMIN_MOCK_REPLY,
    });
  }
  return new RealOneMinClient({
    apiKey: env.ONEMIN_API_KEY,
    model: env.ONEMIN_MODEL || "gpt-4o-mini",
  });
}

module.exports = {
  RealOneMinClient,
  MockOneMinClient,
  clientFromEnv,
  buildPrompt,
  extractText,
};
