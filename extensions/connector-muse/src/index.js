#!/usr/bin/env node
/**
 * Entry point: run the Muse connector sidecar.
 * All configuration comes from the environment (see README.md / .env.example).
 * No secrets in argv or env values beyond the PATH of the 0600 creds file.
 */

const { MuseConnector, configFromEnv } = require("./connector.js");

const cfg = configFromEnv();
const missing = [];
if (!cfg.credsFile) missing.push("COTAL_CREDS (path to minted creds file)");
if (!cfg.lifecycleUid) missing.push("COTAL_LIFECYCLE_UID (from `cotal mint --provision`)");
if (missing.length) {
  console.error(`[muse-connector] missing required config:\n  - ${missing.join("\n  - ")}`);
  process.exit(2);
}

const connector = new MuseConnector(cfg);

let stopping = false;
function shutdown(signal) {
  if (stopping) return;
  stopping = true;
  console.error(`[muse-connector] ${signal} — shutting down`);
  connector.stop().catch((err) => {
    console.error(`[muse-connector] shutdown error: ${err.message}`);
  }).finally(() => process.exit(0));
}
process.on("SIGINT", () => shutdown("SIGINT"));
process.on("SIGTERM", () => shutdown("SIGTERM"));
process.on("unhandledRejection", (err) => {
  console.error(`[muse-connector] unhandled rejection: ${err?.stack || err}`);
});

connector.start().catch((err) => {
  console.error(`[muse-connector] FATAL: ${err.message}`);
  process.exit(1);
});
