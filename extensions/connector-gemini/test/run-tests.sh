#!/usr/bin/env bash
# run-tests.sh — acceptance run for the Gemini connector (mock API client).
# Prerequisites (isolated mesh root; run FROM that dir, never from ~):
#   cd ~/workspace/ops/cotal-test/mesh-test-gemini
#   cotal up --detach --server nats://127.0.0.1:14222 --space gemini-test
#   cotal mint gemini  --provision --allow-subscribe testchan --allow-publish testchan --out ./creds/gemini.creds
#   cotal mint tester  --provision --allow-subscribe testchan --allow-publish testchan --out ./creds/tester.creds
# Then:  bash /path/to/run-tests.sh   (or: npm test --prefix <connector dir>)
set -u
CONNECTOR_DIR="${CONNECTOR_DIR:-$HOME/workspace/ops/cotal-test/connector-gemini}"
MESH_DIR="${TEST_MESH_DIR:-$HOME/workspace/ops/cotal-test/mesh-test-gemini}"
OUT="$MESH_DIR/test-logs/run-$(date +%Y%m%d-%H%M%S).log"
mkdir -p "$MESH_DIR/test-logs"

{
  echo "=== gemini connector acceptance run: $(date -u +%FT%TZ) ==="
  echo "--- node/npm ---"
  node --version; npm --version
  echo "--- mesh status (cotal endpoints) ---"
  (cd "$MESH_DIR" && "$HOME/.local/bin/cotal" endpoints)
  echo "--- unit checks: config derivation from minted creds ---"
  node -e "
const {loadConfig} = require('$CONNECTOR_DIR/src/config');
const cfg = loadConfig({COTAL_CREDS: '$MESH_DIR/creds/gemini.creds', COTAL_SPACE: 'gemini-test'});
console.log('owner.actor =', cfg.owner + '.' + cfg.actor, '| durable =', cfg.dmDurable, '| bucket =', cfg.presenceBucket);
"
  echo "--- acceptance tests (mock client) ---"
  node "$CONNECTOR_DIR/test/test-connector.js"
  echo "--- exit: $? ---"
} 2>&1 | tee "$OUT"
echo "full log: $OUT"
