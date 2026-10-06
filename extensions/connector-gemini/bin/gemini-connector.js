#!/usr/bin/env node
'use strict';
/**
 * gemini-connector — entry point for the Arena Hub Gemini connector (v1).
 *
 * Modes:
 *   CONNECTOR_MODE=daemon  (default) long-running process: presence heartbeat
 *                            + poll loop every POLL_INTERVAL_MS
 *   CONNECTOR_MODE=once    single poll cycle then exit — cron-friendly
 *
 * Required env: COTAL_CREDS (path to 0600 minted creds file).
 * Real API mode: GEMINI_CLIENT=real + GEMINI_API_KEY (env only, never in code).
 */
const { loadConfig } = require('../src/config');
const { GeminiConnector } = require('../src/connector');

async function main() {
  let cfg;
  try {
    cfg = loadConfig();
  } catch (err) {
    console.error(`[gemini-connector] config error: ${err.message}`);
    process.exit(2);
  }

  const conn = new GeminiConnector(cfg);

  const shutdown = (signal) => {
    console.log(`[gemini-connector] ${signal} — shutting down`);
    conn.shutdown('offline').then(() => process.exit(0)).catch(() => process.exit(1));
  };
  process.on('SIGINT', () => shutdown('SIGINT'));
  process.on('SIGTERM', () => shutdown('SIGTERM'));

  try {
    console.log(`[gemini-connector] starting as ${cfg.owner}.${cfg.actor} ` +
      `(agent: ${cfg.agentName}, space: ${cfg.space}, mode: ${cfg.mode}, ` +
      `client: ${cfg.clientKind}, poll: ${cfg.pollMs}ms)`);
    await conn.start();
    if (cfg.mode === 'once') {
      // start() already ran one cycle and shut down
      process.exit(0);
    }
    console.log('[gemini-connector] running (poll-only; wake: none)');
  } catch (err) {
    console.error(`[gemini-connector] fatal: ${err.message}`);
    process.exit(1); // fail-before-presence: no durable bind => no presence
  }
}

main();
