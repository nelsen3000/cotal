# transport-tincan

Adapter spec for a **tincan** second transport binding
([mvanhorn/agent-tincan](https://github.com/mvanhorn/agent-tincan), MIT —
see NOTICE.md).

## What this is

Cotal's protocol is transport-agnostic (`docs/transport.md`); NATS/JetStream is
the reference binding and "there is no second binding". This package declares
what a tincan relay binding provides against the five-capability transport
contract, and documents the gaps Cotal must fill above the pipe.

Tincan puts a small relay on your Tailscale network; every agent dials out, so
nothing needs an open port. It queues requests, wakes agents by per-agent
method, and carries replies back. Identity comes from Tailscale WhoIs.

## Layout

- `ADAPTER.md` — full contract mapping, identity mapping, ops guidance
- `src/capabilities.ts` — typed capability declarations (partial/gap per row)
- `smoke/tincan-contract.smoke.ts` — contract-consistency smoke test

## The one thing to get right

Authorization/isolation is a **hard gap**: tincan trusts joined agents as
teammates and has no per-channel ACLs. The adapter must enforce Cotal ACLs
(SPEC §9) at its boundary before traffic reaches the relay.
