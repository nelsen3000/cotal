# Arena Hub v1 — 1min.ai connector

Poll-only sidecar that joins the Cotal mesh as the `1minai` agent and
translates between the mesh and the 1min.ai API. Internal use (Arena Hub v1).

## How it works (honest wake path)

1. **Inbound (mesh → 1min.ai):** the connector binds the agent's
   pre-provisioned DM durable (`dm_<owner>-<actor>-<uid>` on `DM_<space>`) and
   subscribes the configured channel filters. Inbound messages are held in a
   pending queue.
2. **Read path:** on the poll cadence (`ONEMIN_POLL_MS`, default 30s) all
   pending messages are folded into the prompt of ONE 1min.ai API call
   (`POST https://api.1min.ai/api/chat-with-ai`, body
   `{type:"UNIFY_CHAT_WITH_AI", model, promptObject:{prompt, isMixed:false,
   webSearch}}`, auth via the `API-KEY` header — not OpenAI-compatible).
3. **Outbound (1min.ai → mesh):** the API reply is published back to the mesh
   with `replyTo` set to the inbound message id (DM back to the sender, or the
   same channel for channel messages). `Nats-Msg-Id` dedup on publish.
4. **Ack site:** the JetStream DM message is acked ONLY after the API round-trip
   completes and the reply is on the mesh. A failed round-trip leaves messages
   un-acked so they redeliver after `ack_wait` and are retried.
5. **Presence:** KV `cotal_presence_<space>` key `<owner>.<actor>`, heartbeat
   every 2s, TTL 6s (bucket default). Status: `idle` / `working` / `offline`.
6. **Beads:** `/claim <id>` and `/release <id>` messages run `bd claim` /
   `bd release` in `BEADS_DIR` and report the result to the mesh. Beads is the
   source of truth; the connector keeps no claim registry. **E2E-untested:**
   the `bd` CLI is not installed on this host, so the live path cannot be
   exercised here — command parsing is unit-tested (`test/commands.test.js`,
   11/11) and the graceful "bd CLI not installed" degradation follows the same
   pattern as the sibling connectors.

## Honest limitations

- **Poll-only, `wake: none`.** 1min.ai's API is request/response only. There is
  no push, webhook, or agent-wake primitive. The scheduled poll IS the
  mechanism; do not expect sub-poll-cadence latency. Verified 2026-10-06
  against the live API request/response shape (no webhook/push surface).
- **Model is a parameter, not a guarantee.** Default `gpt-4o-mini`
  (`ONEMIN_MODEL`); any model id the 1min.ai account can reach may be set, but
  availability is the account's, not the connector's, to promise.
- **No max-tokens knob.** The verified 1min.ai request shape carries no
  max-output-tokens field, so this connector does not invent one. Keep prompts
  lean instead.
- **No citations surface.** 1min.ai returns plain text; replies carry no source
  list.
- **Channel messages are live at-most-once** (native NATS core subscription).
  DMs are at-least-once via the bound durable (offline replay works).
- **No mid-turn steer** (`steer: false`).
- **API failures retry via redelivery** — a poison message will retry every
  `ack_wait` (60s default) until the API succeeds; there is no dead-letter in v1.

## Setup

```bash
cd extensions/connector-1minai   # or wherever this lives
npm install
```

Provision identity (from the mesh root dir — beware the upward `.cotal` search):

```bash
cd <mesh-root>
cotal mint 1minai --space <space> --provision \
  --allow-subscribe general --allow-publish general \
  --out /run/cotal/1minai.creds   # 0600 file
chmod 600 /run/cotal/1minai.creds
```

Run:

```bash
COTAL_CREDS=/run/cotal/1minai.creds \
COTAL_SERVER=nats://127.0.0.1:4222 \
ONEMIN_API_KEY=<from Secure Vault; never in code/repo> \
ONEMIN_MODEL=gpt-4o-mini \
ONEMIN_POLL_MS=30000 \
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
| `ONEMIN_API_KEY` | (required, real client) | key from env only — never code/repo |
| `ONEMIN_USE_MOCK` | `0` | `1` = use mock client (tests) |
| `ONEMIN_MOCK_LOG` | — | JSONL file the mock appends every call to |
| `ONEMIN_MOCK_REPLY` | canned text | mock reply override |
| `ONEMIN_MODEL` | `gpt-4o-mini` | any model id the account can reach |
| `ONEMIN_WEB_SEARCH` | `0` | `1` = set `webSearch:true` on the API call |
| `ONEMIN_SYSTEM_PROMPT` | 1min.ai framing | system prompt |
| `ONEMIN_TIMEOUT_MS` | `60000` | API call timeout |
| `ONEMIN_POLL_MS` | `30000` | inbound poll cadence |
| `ONEMIN_PRESENCE_MS` | `2000` | presence heartbeat |
| `BEADS_DIR` | cwd | working dir for `bd claim` / `bd release` |

## Tests

`npm test` runs the unit suite (11/11) then the end-to-end suite against an
isolated mesh with the MOCK API client: presence visible, DM picked up by the
poller and passed to the mock, mock reply delivered with `replyTo`, offline DM
replay after restart. See `test/e2e.js`. The mock client is the API
abstraction used by all tests; the real client is a thin HTTPS wrapper and is
never exercised with a real key in CI.
