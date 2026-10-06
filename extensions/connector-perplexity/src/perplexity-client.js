// Perplexity API client abstraction.
//
// The Perplexity API is request/response only: POST /chat/completions with an
// OpenAI-compatible body. There is no push, webhook, streaming-subscription, or
// "wake the client" primitive in Perplexity's public API, so the connector's
// read path is honest polling (see connector.js): pending mesh messages are
// folded into the next scheduled call's context.
//
// Interface: PerplexityClient#chat({ system, messages, model, maxTokens, timeoutMs })
//   -> { text, citations[], raw }
//
// Implementations:
//   - RealPerplexityClient  (reads PERPLEXITY_API_KEY from env — never in code)
//   - MockPerplexityClient  (used by tests; records calls to a JSONL log file)

"use strict";

const https = require("https");

class RealPerplexityClient {
  constructor({ apiKey, model = "sonar", baseUrl = "https://api.perplexity.ai" } = {}) {
    if (!apiKey) throw new Error("RealPerplexityClient: PERPLEXITY_API_KEY is not set");
    this.apiKey = apiKey;
    this.model = model;
    this.baseUrl = baseUrl;
  }

  chat({ system, messages, model, maxTokens = 1024, timeoutMs = 60000 }) {
    const body = JSON.stringify({
      model: model || this.model,
      messages: [
        ...(system ? [{ role: "system", content: system }] : []),
        ...messages,
      ],
      max_tokens: maxTokens,
      return_citations: true,
    });
    const url = new URL("/chat/completions", this.baseUrl);
    return new Promise((resolve, reject) => {
      const req = https.request(
        {
          hostname: url.hostname,
          port: 443,
          path: url.pathname,
          method: "POST",
          headers: {
            Authorization: `Bearer ${this.apiKey}`,
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
                new Error(`Perplexity API HTTP ${res.statusCode}: ${data.slice(0, 500)}`)
              );
            }
            try {
              const json = JSON.parse(data);
              const choice = (json.choices && json.choices[0]) || {};
              const text =
                (choice.message && choice.message.content) || "[empty response]";
              resolve({
                text,
                citations: Array.isArray(json.citations) ? json.citations : [],
                raw: json,
              });
            } catch (e) {
              reject(new Error(`Perplexity API: bad JSON: ${e.message}`));
            }
          });
        }
      );
      req.on("timeout", () => req.destroy(new Error("Perplexity API: request timed out")));
      req.on("error", reject);
      req.write(body);
      req.end();
    });
  }
}

class MockPerplexityClient {
  // logPath: JSONL file where every call is appended (tests assert on this).
  // reply: function(input) -> string, or a fixed string.
  constructor({ logPath, reply } = {}) {
    this.logPath = logPath;
    this.calls = [];
    this.reply =
      typeof reply === "function"
        ? reply
        : () => reply || "MOCK-REPLY: research noted. (Perplexity mock client)";
  }

  async chat(input) {
    const record = { ts: new Date().toISOString(), input };
    this.calls.push(record);
    if (this.logPath) {
      require("fs").appendFileSync(this.logPath, JSON.stringify(record) + "\n");
    }
    return { text: this.reply(input), citations: [], raw: { mock: true } };
  }
}

function clientFromEnv(env = process.env) {
  if (env.PERPLEXITY_USE_MOCK === "1") {
    return new MockPerplexityClient({
      logPath: env.PERPLEXITY_MOCK_LOG,
      reply: env.PERPLEXITY_MOCK_REPLY,
    });
  }
  return new RealPerplexityClient({
    apiKey: env.PERPLEXITY_API_KEY,
    model: env.PERPLEXITY_MODEL || "sonar",
  });
}

module.exports = { RealPerplexityClient, MockPerplexityClient, clientFromEnv };
