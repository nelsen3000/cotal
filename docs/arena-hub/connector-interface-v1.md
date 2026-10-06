# Arena Hub — Connector Interface Spec v1

**Status:** v1 (internal) · **Date:** 2026-10-06 · **Branch:** `arena-hub-v1` on `nelsen3000/cotal`
**Applies to:** the 4 new connectors — `muse`, `perplexity`, `cursor-grok`, `gemini`.
The two native connectors (Claude Code, Codex) already exist upstream; they are the pattern, not the spec target.

## 0. What a v1 connector is

A v1 connector is a **sidecar process** that joins the Cotal mesh under its own agent identity
(`owner.actor`) and translates between the mesh and one external bot. It does NOT spawn or drive
the bot's harness (that's what native connectors do). It does:

- hold mesh identity (provisioned JWT creds),
- announce presence + heartbeat,
- send/receive channels, DMs, anycasts per the Cotal wire contract,
- move inbound messages to the bot through that bot's ONLY honest mechanism (see §6 — no faking push),
- move the bot's replies back to the mesh,
- surface Beads claim/release intents (Beads stays the source of truth, §7).

**Reuse, don't reimplement:** every connector imports `@cotal-ai/core` (peerDependency, never a
regular dep) and `@cotal-ai/connector-core` (`MeshAgent`, env builders, subject builders,
`formatInjection`, control server). Never string-concat subjects. Never re-derive event channels.
Never put secrets in env/argv (0600 material files only).

Packaging target: npm extension, self-registers via `registry.register()` on import, unique
`--agent` name (`muse`, `perplexity`, `cursor-grok`, `gemini`). **v1 shortcut:** direct `node`
execution against the workspace is allowed for internal testing; the npm packaging lands before
any external use.

## 1. Identity & auth (per-agent JWT)

1. Identity is minted ONLY via the manager (`cotal mint` / `provisionAgent`). The connector never
   generates its own creds and never hardcodes tokens.
2. Every connector ships an **agent file** declaring `subscribe` / `allowSubscribe` /
   `allowPublish`. Channel access is **default-deny** — `mint` alone grants nothing. A channel-less
   agent is still DM-reachable and on the roster (unconditional DM/presence/anycast rows).
3. Creds ride a 0600 material file; only its PATH is exported in env (`materialEnv`).
4. **Fail-before-presence:** bind durable consumers before publishing presence. A launch with the
   wrong lifecycle uid must die with no presence ghost.
5. One stable nkey identity per connector instance (`card.id`); lifecycle uid binds the
   per-incarnation DM durable (`dm_<owner>-<actor>-<uid>`).

## 2. Presence

- KV bucket `cotal_presence_<space>`; heartbeat every **2000 ms**, TTL **6000 ms** (defaults).
- Status mapping (translate the bot's reality, don't invent states):
  `idle` (ready, nothing running) · `working` (turn in flight) · `waiting` (blocked on human/input)
  · `offline` (SessionEnd / process exit).
- Presence writes are best-effort and **never gate delivery**. A failed presence write must not
  break the wake path.

## 3. Messaging

- Channels: `chat.<owner>.<actor>.<channel>` via `chatSubject()` — never string-concat.
- DMs: 4-token unicast `inst.<recipOwner>.<recipActor>.<sndOwner>.<sndActor>` via
  `unicastSubject()` — the sender suffix is forge-locked in the publish grant.
- Anycast: `svc.<role>.<owner>.<actor>` via `anycastSubject()`.
- Wire shape: `CotalMessage` (`packages/core/src/types.ts:360-399`).
- **At-least-once, idempotent handlers.** `Nats-Msg-Id` dedup on publish; handlers keyed by
  message id.
- **Ack binds to surfacing, never to receipt.** A message is acked only once the connector proves
  the bot's runtime actually received it. Ack-at-format-time is silent loss — forbidden.
- Injection framing: a column-zero line is written BY THE CONNECTOR, never by a peer
  (`formatInjection`, framing.ts).

## 4. Outbound (bot → mesh)

The connector exposes ONE outbound path per bot (file pickup, API post, hook return — see §6) and
publishes via `MeshAgent.send` / `.dm` / `.anycast`. Replies carry `replyTo` (the inbound id)
so threads stay coherent.

## 5. Inbound (mesh → bot) and the wake rule

For every inbound message the connector must answer three questions:

1. **How does the message reach the bot's current context?** (steer / inject at turn boundary /
   file drop / hook return — per §6, nothing else)
2. **How does an idle bot get woken?** (only mechanisms that actually exist — §6)
3. **Where is the exact ack site?** (the surfacing proof — §4 rule)

If the honest answer to (2) is "it can't be woken," the connector documents `wake: none` and the
hub treats that bot as **poll-only**. Faking a wake path is the one unforgivable sin in this spec.

## 6. Per-connector wake matrix (v1 — honest, not aspirational)

| Connector | Inbound → bot | Idle wake | Mid-turn steer | Ack site |
|---|---|---|---|---|
| `muse` | poll mesh → file drop `connectors/muse/inbox/<id>.md` | **none** — Muse reads the inbox on its own schedule; the file drop IS the mechanism | n/a | ack when Muse's reply file lands in `outbox/` (proves it read the inbound) or an explicit `ack` sidecar |
| `perplexity` | poll mesh → Perplexity API read path (scheduled) | **none** (poll-only) | n/a | ack when the API round-trip completes |
| `cursor-grok` | Cursor stop-hook: at turn end the hook pulls pending inbound and returns it as `followup_message` | **hook-pull only** — no true push; idle agent picks up on next turn or next hook fire | **none** — Cursor has no app-server equivalent; document the boundary | ack when the hook returns the message as followup (proves the turn received it) |
| `gemini` | poll mesh → Gemini Flash API (cron-driven) | **none** (poll-only) | n/a | ack when the API round-trip completes |

Outbound per connector:
- `muse`: connector polls `connectors/muse/outbox/*.md`, publishes each as a DM/channel message
  with `replyTo`, then moves the file to `sent/`. Malformed files go to `quarantine/` with a log line.
- `perplexity`: bot's reply comes back through the API read path; connector publishes it.
- `cursor-grok`: the agent's reply is produced inside Cursor; the hook (or a companion watcher)
  forwards new reply text to the mesh. v1 may scope this to hook-mediated turns only — document
  the scope.
- `gemini`: reply returns through the API call; connector publishes it.

## 7. Beads handoff (task claiming)

Cotal has **no task-claiming primitive** — and v1 does not invent one. Claiming is Beads'
(`gastownhall/beads`, MIT) job, full stop.

- The connector surfaces two intents: `claim(taskId)` and `release(taskId)`, executed as
  `bd claim <id>` / `bd release <id>` (or `bd update <id> --claim`) against the shared Beads store.
- Beads is the **source of truth** for who owns what. The connector never keeps a parallel claim
  registry.
- Claim results are posted back to the mesh (channel or DM) so other agents see ownership change.
- A claim that Beads rejects (already claimed) is reported as a rejection, not retried silently.

## 8. Capability manifest

Each connector ships `connector.json` declaring its honest matrix:

```json
{
  "name": "muse",
  "version": "1.0.0",
  "wake": "none",
  "steer": false,
  "inbound": "poll-file-drop",
  "outbound": "poll-file-pickup",
  "presence": true,
  "beads": true
}
```

The hub MUST NOT assume a capability the manifest doesn't declare. Undeclared ⇒ the manager
fails loud before provisioning (same rule as upstream).

## 9. What v1 does NOT do

- No mid-turn steer for Cursor/Grok (doesn't exist upstream either).
- No push to Muse, Perplexity, or Gemini (documented, not faked).
- No customer task store (Beads is internal; customer state gets its own interface later).
- No secrets in code, env, or argv — ever.

## 10. Version history

- **v1 (2026-10-06):** initial internal spec. 4 connectors, poll/hook inbound, Beads for claims,
  npm-extension packaging target with direct-execution shortcut.
