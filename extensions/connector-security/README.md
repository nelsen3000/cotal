# connector-security

ADR-adapted security observability for the Arena Hub fleet.

## What this is

A TypeScript port of **uber/ADR**'s Sensor normalized telemetry schema
(`Sensor/adr_sensor/schemas/agent_event_schema.py`, Apache-2.0 — see NOTICE.md):
`AdrAgentEvent`, `AdrChatMessage`, `AdrToolUsage`, plus a mapper from Cotal
journal/run events (`fromCotalRunEvent`) and ADR's noise filter
(`hasMeaningfulContent`).

Cotal's existing `docs/security.md` covers the NATS transport trust model
(profiles, ACLs, consumer confinement). This connector covers the other half:
**what agents actually do** — intent, tool use, execution traces — normalized
the way ADR's two-tier Detector expects.

## What stays external (deliberately)

- **ADR Detector** (dual-agent triage + reasoning) — run upstream; this connector
  produces the telemetry it consumes.
- **ADR-Bench** — research artifact, marked "not for production use" by its own
  README; run it in an isolated environment before admitting fleet connectors.
  `bench/attack-techniques.json` is the evaluation checklist (only categories
  directly documented in uber/ADR's READMEs are named; the full 17 techniques
  are enumerated in the ADR MLSys 2026 paper).

## Layout

- `src/schema.ts` — normalized event schema + Cotal mapper + noise filter
- `bench/attack-techniques.json` — connector evaluation checklist
- `smoke/security-schema.smoke.ts` — schema round-trip + noise-filter smoke test
