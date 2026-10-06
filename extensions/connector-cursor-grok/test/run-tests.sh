#!/usr/bin/env bash
# connector-cursor-grok v1 test suite.
# Runs T1..T5 against the isolated mesh at ~/workspace/ops/cotal-test/mesh-test-cursor.
# Prereqs: mesh up (`cotal up` from mesh-test-cursor/), creds minted for grok + tester.
set -u
CONNECTOR="$(cd "$(dirname "$0")/.." && pwd)"
MESH_ROOT="$HOME/workspace/ops/cotal-test/mesh-test-cursor"
export ARENA_STATE_DIR="${ARENA_STATE_DIR:-$MESH_ROOT/test-state-$$}"
mkdir -p "$ARENA_STATE_DIR"

# Broker sanity check before anything else (auth mode: needs creds).
# Reuses the connector's own mesh.js so the check exercises the real path.
if ! ARENA_CREDS_PATH="$MESH_ROOT/creds/grok.creds" ARENA_NATS_URL="nats://127.0.0.1:4222" node -e "
const mesh = require('$CONNECTOR/src/mesh');
(async () => {
  const nc = await mesh.meshConnect(mesh.loadIdentity());
  await nc.close();
})().then(()=>process.exit(0)).catch(()=>process.exit(1));
"; then
  echo "FATAL: broker nats://127.0.0.1:4222 not reachable. Bring the mesh up first:"
  echo "  cd $MESH_ROOT && cotal up --detach"
  exit 1
fi
for f in "$MESH_ROOT/creds/grok.creds" "$MESH_ROOT/creds/tester.creds"; do
  [ -f "$f" ] || { echo "FATAL: missing $f (cotal mint grok/tester --provision)"; exit 1; }
done

PASS=0; FAIL=0; FAILED=""
# T1 starts/stops its own sidecar; T5 too. Make sure none is lingering.
pkill -f "connector-cursor-grok/src/sidecar" 2>/dev/null || true
sleep 1

for t in t1-presence t2-hook-dm t3-hook-empty t4-offline-replay t5-outbox-reply t6-beads-unit t7-channel; do
  echo "=== $t ==="
  if node "$CONNECTOR/test/$t.js"; then
    PASS=$((PASS+1)); echo "--- $t: PASS"
  else
    FAIL=$((FAIL+1)); FAILED="$FAILED $t"; echo "--- $t: FAIL"
  fi
  echo
done

pkill -f "connector-cursor-grok/src/sidecar" 2>/dev/null || true
echo "================ SUMMARY ================"
echo "PASS: $PASS  FAIL: $FAIL"
[ -n "$FAILED" ] && echo "failed:$FAILED"
echo "state dir kept at: $ARENA_STATE_DIR"
[ "$FAIL" -eq 0 ]
