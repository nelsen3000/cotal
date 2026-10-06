# Arena Hub v1 — Perplexity connector

Poll-only sidecar that joins the Cotal mesh as the `perplexity` agent and
translates between the mesh and the Perplexity API. Internal use (Arena Hub v1).

## How it works (honest wake path)

1. **Inbound (mesh → Perplexity):** the connector binds the agent's
   pre-provisioned DM durable (`dm_<owner>-<actor>-<uid>` on `DM_<space>`) and
   subscribes the configured channel filters. Inbound messages are held in a
   pending queue.
2. **Read path:** on the poll cadence (`PERPLEXITY_POLL_MS`, default 30s) all
   pending messages are folded into the context of ONE Perplexity API call
   (`POST https://api.perplexity.ai/chat/completions`, OpenAI-compatible).
3. **Outbound (Perplexity → mesh):** the API reply is published back to the mesh
   with `replyTo` set to the inbound message id (DM back to the sender, or the
   same channel for channel messages). `Nats-Msg-Id` dedup on publish.
4. **Ack site:** the JetStream DM message is acked ONLY after the API round-trip
   completes and the reply is on the mesh. A failed round-trip leaves messages
   un-acked so they redeliver after `ack_wait` and are retried.
5. **Presence:** KV `cotal_presence_<space>` key `<owner>.<actor>`, heartbeat
   every 2s, TTL 6s (bucket default). Status: `idle` / `working` / `offline`.
6. **Beads:** `/claim <id>` and `/release <id>` messages run `bd claim` /
   `bd release` in `BEADS_DIR` and report the result to the mesh. Beads is the
   source of truth; the connector keeps no claim registry.

## Honest limitations

- **Poll-only, `wake: none`.** Perplexity's public API is request/response only —
  chat completions (incl. the async variant, which is still client-polled).
  There is no push, webhook, or agent-wake primitive. The scheduled poll IS the
  mechanism; do not expect sub-poll-cadence latency. Verified 2026-10-06 via
  Perplexity API docs/SDK references (no webhook/push surface found).
- **Slack presence for Perplexity is UNVERIFIED** — do not depend on it. This
  connector's presence lives on the mesh KV only.
- **Research-grade traffic, not coding.** Sonar models answer from live web
  search; they are not code-writing agents. Route research questions here, not
  builds.
- **Channel messages are live at-most-once** (native NATS core subscription).
  DMs are at-least-once via the bound durable (offline replay works).
- **No mid-turn steer** (`steer: false`).
- **API failures retry via redelivery** — a poison message will retry every
  `ack_wait` (60s default) until the API succeeds; there is no dead-letter in v1.

## Setup

```bash
cd extensions/connector-perplexity   # or wherever this lives
npm install
```

Provision identity (from the mesh root dir — beware the upward `.cotal` search):

```bash
cd <mesh-root>
cotal mint perplexity --space <space> --provision \
  --allow-subscribe general --allow-publish general \
  --out /run/cotal/perplexity.creds   # 0600 file
chmod 600 /run/cotal/perplexity.creds
```

Run:

```bash
COTAL_CREDS=/run/cotal/perplexity.creds \
COTAL_SERVER=nats://127.0.0.1:4222 \
PERPLEXITY_API_KEY=<from Secure Vault; never in code/repo> \
PERPLEXITY_POLL_MS=30000 \
node src/connector.js
```

The connector derives owner/actor/space and its DM durable name from the creds
JWT itself — no identity material in config.

## Env vars

| var | default | purpose |
|---|---|---|
| `COTAL_CREDS` | (required) | path to `cotal mint`-ed creds file (0600) |
| `COTAL_SERVER` | `nats://127.0.0.1:4222` | broker URL |
| `COTAL_CHANNELS` | `general` | comma-separated channel filters to subscribe |
| `COTAL_LIFECYCLE_UID` | (derived from creds) | override for the DM durable suffix |
| `PERPLEXITY_API_KEY` | (required, real client) | key from env only — never code/repo |
| `PERPLEXITY_USE_MOCK` | `0` | `1` = use mock client (tests) |
| `PERPLEXITY_MOCK_LOG` | — | JSONL file the mock appends every call to |
| `PERPLEXITY_MOCK_REPLY` | canned text | mock reply override |
| `PERPLEXITY_MODEL` | `sonar` | `sonar` / `sonar-pro` / `sonar-reasoning-pro` / `sonar-deep-research` |
| `PERPLEXITY_SYSTEM_PROMPT` | research framing | system prompt |
| `PERPLEXITY_MAX_TOKENS` | `1024` | per-call cap |
| `PERPLEXITY_TIMEOUT_MS` | `60000` | API call timeout |
| `PERPLEXITY_POLL_MS` | `30000` | inbound poll cadence |
| `PERPLEXITY_PRESENCE_MS` | `2000` | presence heartbeat |
| `BEADS_DIR` | cwd | working dir for `bd claim` / `bd release` |

## Tests

`npm test` runs the end-to-end suite against an isolated mesh with the MOCK API
client: presence visible, DM picked up by the poller and passed to the mock,
mock reply delivered with `replyTo`, offline DM replay after restart. See
`test/e2e.js`. The mock client is the API abstraction used by all tests; the
real client is a thin HTTPS wrapper and is never exercised with a real key in CI.
