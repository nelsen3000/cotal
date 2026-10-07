# Tincan transport adapter — contract mapping

How **agent-tincan** ([mvanhorn/agent-tincan](https://github.com/mvanhorn/agent-tincan),
MIT — see NOTICE.md) maps onto Cotal's transport capability contract
(`docs/transport.md`).

## Why a second binding

`docs/transport.md` states the Cotal protocol is transport-agnostic and NATS is
"the reference binding, not the definition of the protocol", adding: "There is
no transport abstraction layer in code yet, because there is no second binding."
Tincan is a candidate second binding for the peers NATS can't reach:

- Agents behind dial-out-only containers/proxies (no inbound, no NATS port).
- Machines where running a NATS server is impractical (the laptop, phones,
  sandboxes that pause) — tincan's design goal is literally
  "your laptop can be off".
- Wake flows: tincan has per-agent wake methods (the Arena Hub connector build
  left live sleeping-agent wake-up as its one unretired risk).

Every agent dials OUT to the tincan relay on the tailnet, so nothing needs an
open port. This is **not** a replacement for NATS — it's the fallback the
spec's transport-agnostic layer anticipates.

## Capability-by-capability (mirrors src/capabilities.ts)

| # | Contract capability | Tincan provides | Cotal supplies above |
|---|---|---|---|
| 1 | Addressed routing | partial (unicast + council fan-out; no channel wildcards) | multicast channels, anycast roles |
| 2 | Durable delivery/history | partial (queued requests, wake retries, reply carry-back) | bookmarks, ack/redelivery, msg-id dedup, durable backstops |
| 3 | Presence/registry | partial (presence + lastseen; no channel KV) | channel registry, membership feed |
| 4 | Identity | partial (Tailscale WhoIs → machine) | machine → owner.actor principal mapping |
| 5 | Authz/isolation | **GAP** (teammate trust, no ACLs) | full ACL enforcement at the adapter boundary (SPEC §9) |

Capability 5 is the hard gap: tincan's trust model ("agents you join trust
each other like teammates") cannot express Cotal's per-channel
allowPublish/allowSubscribe default-deny. The adapter MUST enforce ACLs before
traffic touches the relay — never the reverse.

## Identity mapping

Tailscale WhoIs says *which machine* a request came from; Cotal needs *which
actor* (`owner.actor`). One tailnet machine can host many actors (e.g. the
mini runs several connectors). The adapter keeps an explicit machine→principal
registry; unknown machines are rejected, never auto-admitted.

## What this package is NOT

- Not a Go binary vendor: the tincan relay (`tincan` CLI) stays upstream.
- Not a working binding yet: this PR ships the spec, the capability
  declarations, and the smoke test. The live adapter comes after review.
- It does not touch the NATS reference binding, `arena-hub-v1`, or PR #1.
