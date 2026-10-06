# connector-gemini — Arena Hub v1 Gemini connector

A poll-only sidecar between the Cotal mesh and the Gemini Flash API.
Internal (Arena Hub v1). Apache-2.0.

## Honest scope

- **Research-grade traffic only** — summaries, triage, small research calls.
  **NOT coding.** The connector sends a compact digest (≤8 KB), never raw history.
- **Poll-only, no push.** Gemini has no push mechanism; the connector polls the
  mesh on `POLL_INTERVAL_MS` (default 60s) and folds pending inbound into one
  API call per cycle. `wake: none` in `connector.json` — the hub must treat this
  agent as poll-only.
- **Ack site:** a durable DM is acked only after the API round-trip completes
  AND the reply is published. Crash before that ⇒ redelivery (at-least-once,
  idempotent by message id).

## How it works

1. **Inbound:** binds the pre-provisioned DM durable
   `dm_<owner>-<actor>-<uid>` on `DM_<space>` (fail-before-presence: a wrong
   lifecycle uid dies with no presence ghost) + core subscriptions on granted
   channel filters (`chat.*.*.<channel>`).
2. **Each poll:** pull pending DMs (unacked hold) + drain channel queue.
   - `/claim <id>` / `/release <id>` lines are executed via the `bd` CLI
     (Beads is the source of truth; rejections are reported, never retried
     silently). Pure-command batches get a deterministic reply with **no API call**.
   - Non-empty inbox → one Gemini Flash API call over the folded digest.
   - **Empty inbox → no API call.** Quota is never burned on nothing.
3. **Outbound:** one reply per inbound item, each carrying its own `replyTo`
   (DM → DM reply; channel → channel reply when the publish grant exists,
   otherwise DM the sender). `Nats-Msg-Id` dedup on publish.
4. **Presence:** KV `cotal_presence_<space>` put every 2s (`idle` / `working` /
   `waiting` during 429 backoff). Best-effort — never gates delivery.

## Quota & backoff (free tier)

- Free-tier Flash quotas are respected: skip-on-empty, small digests,
  `maxOutputTokens: 1024`.
- On HTTP 429 the client honors `Retry-After`, else exponential backoff
  (30s × 2ⁿ, cap 10 min, + jitter). The connector reports `waiting`, leaves the
  batch **unacked** for the next cycle (nothing is lost), and never hammers.
- `GEMINI_CLIENT` defaults to `mock` — real API calls only happen with
  `GEMINI_CLIENT=real` **and** `GEMINI_API_KEY` set.

## Setup

```bash
npm install
# mint + provision (from your mesh root):
cotal mint gemini --provision --allow-subscribe <channels> --allow-publish <channels> \
  --out /secure/path/gemini.creds
chmod 600 /secure/path/gemini.creds
```

## Env vars

| Var | Default | Notes |
|---|---|---|
| `COTAL_CREDS` | *(required)* | Path to the 0600 minted creds file |
| `COTAL_SPACE` | `main` | Mesh space |
| `COTAL_SERVER` | `nats://127.0.0.1:4222` | Broker URL |
| `COTAL_OWNER` / `COTAL_ACTOR` | derived from JWT grants | Override rarely needed |
| `COTAL_LIFECYCLE_UID` | derived from durable grant | Override rarely needed |
| `COTAL_AGENT_NAME` | `gemini` | Display name in `from.name` / roster |
| `COTAL_SUBSCRIBE` | *(none)* | Comma-separated channels; intersected with JWT grants |
| `POLL_INTERVAL_MS` | `60000` | Free-tier friendly |
| `PRESENCE_INTERVAL_MS` | `2000` | Heartbeat (TTL 6000ms per spec) |
| `CONNECTOR_MODE` | `daemon` | `daemon` or `once` (single poll cycle — cron-friendly) |
| `GEMINI_CLIENT` | `mock` | `mock` or `real` |
| `GEMINI_API_KEY` | *(none)* | **Env only.** Required for `real`. Never in code/repo/logs |
| `GEMINI_MODEL` | `gemini-2.0-flash` | Set to the current free-tier Flash model |
| `MOCK_LOG` | *(none)* | JSONL path the mock client appends each call to (tests) |

Run: `node bin/gemini-connector.js` (or `npm start`).
Cron mode: `CONNECTOR_MODE=once node bin/gemini-connector.js` from cron.

## Security

- No API keys in code, env files, argv, or the repo — `GEMINI_API_KEY` rides the
  process environment only, and is never logged.
- Creds ride a 0600 file; only its path is in env.
- `bd` runs with a 15s timeout; task IDs are restricted to `[\w.-]`.
- License: Apache-2.0. Sole runtime dep: `nats` (Apache-2.0).

## Tests

`test/test-connector.js` runs the acceptance suite against an isolated mesh
(mock client): presence, DM pickup, reply with `replyTo`, empty-inbox quota
discipline, offline replay, channel round-trip. See `test/run-tests.sh` for the
recorded command sequence.
