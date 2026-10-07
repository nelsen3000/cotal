# ADOPTION — mvanhorn/agent-tincan → Arena Hub (cotal)

**Source:** [mvanhorn/agent-tincan](https://github.com/mvanhorn/agent-tincan) —
"Let your AI agents ask each other for help, over Tailscale. Wherever they
run." 232 stars, **MIT** (LICENSE verified 2026-10-07), active (commits the
day of adoption).

**Adoption verdict: ADAPT** (Jon pre-approved; checks in
`~/workspace/adoption-2026-10-07/agent-tincan/CHECKS.md`).

## What agent-tincan is

A Go relay on your Tailscale network for agent-to-agent messaging. Every agent
dials OUT (no open ports); the relay queues requests, wakes the target agent
by its per-agent wake method, and carries the reply back. Identity from
Tailscale WhoIs (machine-level). Ships adapters for claude-code, codex,
gemini-cli, grok, perplexity, chatgpt, web agents, plus a "council"
(multi-agent second-opinion) feature and an MCP server.

## Why it does NOT duplicate cotal

Cotal already has messaging, presence, and offline replay — on **NATS**, its
single transport binding. `docs/transport.md` is explicit: the protocol is
transport-agnostic, "there is no transport abstraction layer in code yet,
because there is no second binding." Tincan is a candidate second binding for
peers NATS can't reach (dial-out-only containers, pausing sandboxes, the
laptop, phones). Our existing Tailscale use is SSH-only — that's transport for
a shell session, not agent messaging. Different jobs, no overlap.

## What we adopted — and what we deliberately did not

**Adopted** (`extensions/transport-tincan/`): the adapter spec — a
capability-by-capability mapping of tincan onto Cotal's five-capability
transport contract, typed declarations (`src/capabilities.ts`), the
WhoIs→principal identity mapping rule, and an ops runbook (ADAPTER.md).

**Not adopted:** the tincan Go binary itself (stays upstream), a live binding
implementation (follows after review), anything touching the NATS reference
binding.

## The hard gap (reviewers: scrutinize this)

Tincan's trust model is teammate-grade — joined agents trust each other; no
per-channel ACLs, no default-deny. Cotal's SPEC §9 ACLs must be enforced at
the adapter boundary BEFORE traffic reaches the relay. The relay is never
trusted for isolation. `src/capabilities.ts` declares this as the single
`provision: "gap"` row; the smoke test asserts it stays declared.

## Ops runbook (when this binding goes live)

1. Run one `tincan relay` on the tailnet (the mini is the natural host — it
   already runs Tailscale).
2. Register every fleet agent with its wake method (`tincan onboard`); keep
   the machine→principal registry in the adapter config.
3. Route only peers that can't reach the NATS broker through tincan; NATS
   stays the default.
4. Unknown tailnet machines are rejected at the adapter; never auto-admitted.

## Review status

Draft PR opened against `main` (does not touch `arena-hub-v1` / PR #1).
Verdicts from Grok/Codex recorded in the parent report.
