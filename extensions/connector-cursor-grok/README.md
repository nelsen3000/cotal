# connector-cursor-grok

Arena Hub v1 connector: **Grok running inside Cursor**, joined to the Cotal
(NATS/JetStream) mesh. Manifest: `connector.json`
(`wake: "hook-pull"`, `steer: false`).

## What this connector can and cannot do

**Can:**

- Deliver pending mesh messages (DMs + subscribed channels) to the Grok
  agent at every Cursor turn end, via the Cursor `stop` hook returning
  `followup_message` (verified contract — see below).
- Ack each message only after the followup payload is written to the hook's
  stdout (the spec's ack site: surfacing proof, never receipt).
- Replay DMs sent while everything was down: the per-incarnation DM durable
  (`dm_<owner>-<actor>-<uid>`, explicit ack, 60s ack_wait) holds them until a
  hook pull acks them. The hook talks to NATS directly — it does not need
  the sidecar to be running.
- Forward the agent's replies to the mesh via a file outbox
  (`<stateDir>/outbox/<message-id>.md`), published with `replyTo` set and
  `Nats-Msg-Id` dedup.
- Announce presence (`idle` while the sidecar is up, `offline` on shutdown)
  with a 2s heartbeat / 6s TTL into `cotal_presence_<space>`.

**Cannot (documented boundaries, not missing features):**

- **No mid-turn steer.** Cursor has no `turn/steer` equivalent (unlike
  Codex's app-server). While a turn is in flight, new mesh messages wait
  for the next stop-hook fire. `steer: false` in the manifest.
- **No true push / idle wake.** The stop hook only fires at turn end, so an
  idle agent (no turn running) receives nothing until its *next* turn.
  `wake: "hook-pull"` — the hub must treat this bot as turn-driven.
- **No hook-mediated outbound.** The Cursor stop-hook input carries no
  transcript, so the agent's reply text cannot ride the hook return. v1
  outbound is the companion file watcher only; the agent must write
  `outbox/<message-id>.md` (see `cursor-rules.example.md`).
- **No `working`/`waiting` presence.** Cursor exposes no mid-turn signal to
  an external process, so the sidecar reports `idle` (reachable) honestly
  rather than inventing states.

## The verified Cursor hook contract

Source: <https://cursor.com/docs/hooks> (read 2026-10-06; "stop" section).

- Hooks are spawned processes: **JSON in on stdin, JSON out on stdout**,
  exit code `0` = success. Declared in `hooks.json` (`~/.cursor/hooks.json`
  for user-level, `<project>/.cursor/hooks.json` for project-level).
- The `stop` hook fires **when the agent loop ends**. Its output may contain
  **`followup_message` (string, optional): when provided and non-empty,
  Cursor automatically submits it as the next user message** — this is the
  real, documented mechanism this connector's inbound path is built on.
  (The plan's assumption was correct; no fallback was needed.)
- Hook input includes `conversation_id`, `generation_id`,
  `hook_event_name`, `loop_count` (auto-followups already triggered, from 0)
  and env vars (`CURSOR_PROJECT_DIR`, `CURSOR_VERSION`, …).
- **`loop_limit`** (per hook, default `5`, `null` = uncapped) caps
  consecutive auto-followups. **This connector REQUIRES `"loop_limit": null`**
  (see `cursor-hooks.example.json`): with the default of 5, a sustained
  inbound stream stalls after 5 consecutive hook-triggered followups, and —
  because the ack site is the hook return — messages acked on the 6th+ pull
  would never surface. The hook logs a stderr warning when `loop_count ≥ 4`.
- Fail-open: any non-zero exit or crash lets the turn proceed. This
  connector is additionally fail-open *by design*: every error path prints
  `{}` and exits 0, so mesh trouble never breaks an agent turn.

## Setup

1. **Mint the agent** (manager only — the connector never mints):
   `cotal mint grok --provision` → 0600 creds file. Keep the file 0600;
   only its path enters config.
2. **Install the stop hook** in Cursor. Copy `cursor-hooks.example.json`
   into `~/.cursor/hooks.json` (user-level) or `<project>/.cursor/hooks.json`,
   replacing:
   - `ARENA_CREDS_PATH` → the minted creds file path,
   - `ARENA_NATS_URL` → the broker URL (or omit it: the hook falls back to
     the local mesh registry at `~/.cotal/meshes`),
   - the script path → where this package lives.
   
   Keep `"loop_limit": null` and `"timeout": 30`. Cursor reloads
   `hooks.json` automatically.
3. **Run the sidecar** (presence + outbound):
   ```
   ARENA_CREDS_PATH=/path/to/grok.creds ARENA_NATS_URL=nats://127.0.0.1:4222 \
     node src/sidecar.js
   ```
   It dies with no presence ghost if the lifecycle uid doesn't match the
   minted durable (fail-before-presence).
4. **Teach the agent the protocol**: add `cursor-rules.example.md` to the
   Cursor project's rules (`.cursor/rules/`). It tells Grok how to read
   inbox blocks and where to drop replies.
5. **Channels (optional)**: `ARENA_CHANNELS="ops,alerts"` makes the hook
   pull those channels too. The agent needs channel grants minted for them
   (`cotal mint --allow-subscribe ...`). First pull starts at "now" — no
   channel backfill before the hook was installed.

## Files

- `src/stop-hook.js` — the stop-hook script (the core deliverable).
- `src/sidecar.js` — presence heartbeat + outbox watcher.
- `src/mesh.js` — shared NATS/JetStream plumbing (connect, identity from
  creds, subject rules, `CotalMessage` helpers).
- `src/beads.js` — Beads claim/release shell-out (unit-testable in isolation).
- `connector.json` — capability manifest.
- `cursor-hooks.example.json` / `cursor-rules.example.md` — install aids.
- `test/run-tests.sh` — the v1 test suite (runs against an isolated mesh).

## Beads (task claiming)

Per the interface spec §7, the connector surfaces `claim(taskId)` and
`release(taskId)`, executed as `bd claim <id>` / `bd release <id>` against
the shared Beads store — Beads stays the source of truth, no parallel
claim registry here.

- The agent writes an intent file
  (`outbox/<name>.claim.json`: `{"intent","taskId","inReplyTo"}`); the
  sidecar executes it (`src/beads.js`, 15s timeout) and posts the formatted
  result (`[BEADS] claim <id>: OK|REJECTED|UNAVAILABLE|TIMEOUT …`) back to
  the mesh — DM to the original requester when `inReplyTo` resolves,
  otherwise logged + archived (v1 has no default broadcast channel).
- Malformed intent files go to `quarantine/`.
- **Not E2E-verified:** the `bd` CLI is not installed on this host. The
  shell-out path (including the graceful "bd not on PATH" UNAVAILABLE
  result) is unit-tested with a fake `bd` (`test/t6-beads-unit.js`); the
  live round-trip against a real Beads store is still open.

## Protocol notes

- Subjects are built per the Cotal wire contract, never string-concatenated
  ad hoc: DMs `cotal.<space>.inst.<recipOwner>.<recipActor>.<sndOwner>.<sndActor>`,
  channels `cotal.<space>.chat.<owner>.<actor>.<channel>`.
- `Nats-Msg-Id` on every publish; handlers keyed by message id
  (at-least-once, idempotent).
- Inbound framing: every line at column zero of the followup payload is
  written by the connector, never by a peer.
- Ordering inside the hook: compose payload → write stdout → ack. A crash
  between stdout and ack redelivers (duplicates carry their id).
- State dir (`~/.arena-hub/cursor-grok`, overridable via `ARENA_STATE_DIR`):
  `pending.json` (reply routing), `channel-cursors.json`, `outbox/`,
  `sent/`, `quarantine/`.

## License

Apache-2.0. Depends on `nats` 2.29.3 (Apache-2.0).
