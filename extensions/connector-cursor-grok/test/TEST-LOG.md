# connector-cursor-grok — test log (2026-10-06)

Isolated mesh: `~/workspace/ops/cotal-test/mesh-test-cursor/` (`cotal up`
from that dir; space `main`, broker `nats://127.0.0.1:4222`, auth mode).
Agents minted: `grok`, `tester` (+ `grokchan`/`chanwriter` for T7),
all `--provision` (pre-created DM durables).

Suite: `bash test/run-tests.sh` → **PASS: 7  FAIL: 0** (final run,
state dir `mesh-test-cursor/test-state-13391`).

| # | Test | Result | Evidence |
|---|------|--------|----------|
| T1 | presence visible (self + peer), 6s TTL expiry | PASS | sidecar up → self watch `status=idle name=grok`; peer (tester) watch sees same record (roster path); after SIGKILL, key gone at ~9s |
| T2 | hook + pending DM → followup payload | PASS | stdout parses as JSON, `followup_message` non-empty string containing DM id/text/sender/outbox path; DM durable drained after (acked post-stdout) |
| T3 | hook + empty inbox → nothing | PASS | stdout `{}` — no `followup_message`, turn ends quietly |
| T4 | offline replay (sidecar down) | PASS | DM sent with sidecar down confirmed (no presence, no process); next hook pull delivered it as followup |
| T5 | outbox reply → mesh | PASS | reply file `outbox/<id>.md` picked up ≤1s; tester received `id=reply-<id>` with `replyTo=<id>`; file archived to `sent/` |
| T6 | beads intent unit tests | PASS | intent validation, bad input, missing-`bd` → graceful `unavailable`, fake-`bd` ok/reject, `[BEADS]` line formats |
| T7 | channel pull (#ops) | PASS | channel post delivered as followup with `#ops` tag; cursor advanced 4→5; second run → `{}` (no redelivery) |

## Bugs found and fixed during testing (all in-connector, none in Cotal)

1. `credsAuthenticator` needs a Buffer, not a utf8 string (`mesh.js`).
2. `inboxPrefix` must be `_INBOX_<nkey>` (matches the minted JWT) — found
   independently, matches the Gemini lane's finding.
3. Presence KV key is the full principal (`local.<nkey>`), derived from the
   JWT's `$KV.cotal_presence_<space>.*` grant — not the bare nkey.
4. Direct `kv.get` is NOT granted to agents (only own-key put + bucket
   watch); all presence reads go through `kv.watch` (ordered consumer).
5. `kv.watch` iterators never resolve for a missing key — the timeout must
   call `watcher.stop()` (hung T1 before the fix).
6. nats.js 2.x: consumer create/delete live on `jetstreamManager()`, not
   `js.consumers`; signature is `add(stream, cfg)` with the name in
   `cfg.name` (not `add(stream, durable, cfg)`).
7. `opt_start_seq` requires `deliver_policy: "by_start_sequence"`.
8. Test-harness self-match: `pgrep`/`pkill -f` patterns must use the `[s]`
   trick or they match the invoking shell.

## Cross-builder findings applied (Gemini lane)

- `fetch({max_messages, expires})` for bounded pulls (was already the design).
- DM receive is durable-consumer-only; no core subscription on the DM filter.
- `nats@2.29.3` pinned (stable API; v3 split packages lack top-level helpers).
- No re-minting mid-test (new lifecycle uid each time).
- Beads: `bd` not installed on this host → shell-out with 15s timeout,
  graceful `UNAVAILABLE` when not on PATH, unit-tested with fake `bd`
  (T6); live round-trip documented as not E2E-verified.

## Mesh-setup gotchas hit (for the record)

- `cotal up` from the empty dir first resolved the mesh root to `~`
  (upward `.cotal` search found the registry `~/.cotal` created by the
  Gemini lane). Fixed: pre-create `<root>/.cotal/` before `cotal up`, tore
  down the stray `~`-rooted `main` mesh and cleaned its trust material,
  keeping the shared registry (`current-mesh`, `meshes/`) intact.
- `cotal mint` follows the registry's current-mesh pointer, not the cwd:
  use `--space main` explicitly when another lane's mesh is current.
