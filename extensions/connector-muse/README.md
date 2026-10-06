# connector-muse — Arena Hub v1

Poll-loop sidecar that connects **Muse** to the Arena Hub mesh (Cotal wire
protocol over NATS). One of the 4 new v1 connectors; built per
`connector-interface-v1.md` (spec v1, 2026-10-06).

## What it does

| Direction | Mechanism |
|---|---|
| Mesh → Muse | Poll the mesh (DM durable + channel filters) → drop `inbox/<msgId>.md`. **The file drop IS the delivery mechanism** — Muse reads these files on its own schedule. |
| Muse → mesh | Poll `outbox/*.md` → publish as DM/channel message with `replyTo` → move file to `sent/<msgId>.md`. |
| Presence | KV heartbeat every 2s (TTL 6s): `idle`, or `working` while inbox items are unacknowledged. |
| Beads | `/claim <id>` / `/release <id>` first lines in inbound messages shell out to `bd`; the result is posted back to the mesh. Beads is the source of truth — no parallel registry. |

## Honest limitations (the wake matrix, not aspirational)

- **No push to Muse, ever.** There is no mechanism that wakes Muse or injects
  into a live turn. `wake: "none"`, `steer: false`. The hub must treat this
  bot as **poll-only**.
- **Latency = poll interval** (default 15s) in both directions, plus however
  long until Muse reads the inbox. Nothing here is real-time.
- **Ack site:** the mesh-level JetStream ack happens when the inbox file is
  durably written (the file drop is the surfacing proof — acking at receipt
  would be silent loss). Muse-level ack (drives the `working` status) happens
  when a reply file carrying `ReplyTo: <id>` lands in `outbox/`/`sent/`, or an
  explicit `inbox/<id>.ack` sidecar appears.
- **Offline replay:** DMs are pulled from the per-incarnation durable
  `dm_<owner>-<actor>-<uid>`; messages sent while the connector is down appear
  in `inbox/` on restart. Channel messages are live-only in v1 (no durable
  backstop) — same as the rest of the v1 surface.
- **Fail-before-presence:** the DM durable is bound before any presence write.
  A launch with the wrong lifecycle uid dies with no presence ghost.
- Presence writes are best-effort and never gate delivery.

## Setup

1. Mint an identity (manager only — the connector never generates creds):
   ```sh
   cotal mint muse --space <space> --out ./muse.creds --provision \
     [--allow-subscribe bot-chat] [--allow-publish ...]
   # note the printed lifecycle uid
   chmod 600 ./muse.creds
   ```
   `mint` alone grants no channels (default-deny); add `--allow-subscribe`
   for each channel in `COTAL_CHANNELS`, or the connector stays DM-only
   (still on the roster and DM-reachable).
2. Configure the environment (see `.env.example`). Required:
   `COTAL_CREDS`, `COTAL_LIFECYCLE_UID`. Sensible defaults exist for the rest.
3. Run:
   ```sh
   npm install
   node src/index.js
   # or: MUSE_CONNECTOR_DIR=./connectors/muse npm start
   ```

## File layout (`MUSE_CONNECTOR_DIR`)

```
connectors/muse/
  inbox/<msgId>.md        inbound drops: headers (From, Channel/To, Ts, Id,
                          ReplyTo?) + body. Muse reads these.
  inbox/<msgId>.ack       optional explicit ack sidecar (empty file is fine)
  outbox/*.md             Muse writes replies here:
                            To: <owner>.<actor>   (DM; "<actor>" alone = same owner)
                            Channel: <name>       (channel post; exclusive with To:)
                            ReplyTo: <msgId>      (optional; drives threading + ack)
                            <blank line>
                            <body>
  sent/<msgId>.md         published files, renamed to the wire message id
  quarantine/*.md         malformed outbox files — NEVER silently dropped;
                          every one gets a line in connector.log
  state.json              dedup set + inbox ack bookkeeping (internal)
  connector.log           append-only operational log
```

A malformed outbox file (no `To:`/`Channel:`, both, or empty body) is moved to
`quarantine/` with a timestamped log line. Fix it and move it back to
`outbox/` to retry.

## Environment variables

| Var | Default | Meaning |
|---|---|---|
| `COTAL_SPACE` | `main` | Mesh space |
| `COTAL_OWNER` / `COTAL_ACTOR` | `arena` / `muse` | This connector's principal |
| `COTAL_ROLE` | — | Optional role tag in `from.role` |
| `COTAL_CREDS` | **(required)** | Path to minted creds file (0600) |
| `COTAL_LIFECYCLE_UID` | **(required)** | Uid printed by `cotal mint --provision` |
| `COTAL_SERVER` | `nats://127.0.0.1:4222` | Broker URL |
| `MUSE_CONNECTOR_DIR` | `./connectors/muse` | Working dir (inbox/outbox/…) |
| `COTAL_CHANNELS` | — | Comma-separated channels to listen on |
| `POLL_INTERVAL_MS` | `15000` | Mesh↔file poll interval |
| `PRESENCE_INTERVAL_MS` | `2000` | Presence heartbeat interval |
| `BD_BIN` | `bd` | Beads CLI for `/claim` / `/release` |

No API keys, no secrets in code, env, or argv — the creds file path is the
only credential-adjacent value, and the file itself is 0600.

## Tests

`test/run-tests.js` is an integration test against an isolated mesh
(`cotal up` in its own dir + two minted identities). It proves: presence
roster visibility, DM → `inbox/<id>.md` with correct headers, outbox file →
DM with `replyTo` + move to `sent/`, offline replay across a restart, and
malformed-file quarantine. Run: `npm test` with the env it documents at the top.
