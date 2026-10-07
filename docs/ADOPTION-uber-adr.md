# ADOPTION — uber/ADR → Arena Hub (cotal)

**Source:** [uber/ADR](https://github.com/uber/ADR) — "ADR secures enterprise AI
agents through observability, security benchmarking, and threat detection.
Deployed at Uber." 1868 stars, **Apache-2.0** (LICENSE verified 2026-10-07),
active (commits the day of adoption).

**Adoption verdict: ADAPT** (Jon pre-approved; checks in
`~/workspace/adoption-2026-10-07/uber-adr/CHECKS.md`).

## What uber/ADR is

Five capabilities; four open-sourced:

| Component | What it does | Open source? |
|---|---|---|
| Discovery | Inventories AI apps, CLI agents, IDE extensions, model runtimes, MCP servers on endpoints; flags unknown surfaces | Yes (`Discovery/`) |
| Sensor | Collects/normalizes agent telemetry (intent, tool use, execution traces) from 11+ agent platforms into one schema | Yes (`Sensor/`) |
| ADR-Bench | 304 tasks (261 benign, 43 malicious), 134 MCP servers, 17 attack techniques | Yes (`Detection/`, research artifact) |
| Detector | Two-tier detection: high-recall triage + deeper agentic reasoning | Yes (`Detection/guardrail/`) |
| Prevention | Blocks unsafe actions pre-harm | **No** ("Stay tuned") |
| ADR Explorer | Pre-deployment red-teaming engine | **No** |

Paper: MLSys 2026 (docs/adr-paper.pdf in the repo).

## What we adopted — and what we deliberately did not

**Adopted** (`extensions/connector-security/`):

- The Sensor `AgentEvent` schema, ported to TypeScript (`src/schema.ts`):
  `timestamp / source / session_id`, `chat_history` (role, content, tools[] with
  name/type/server/args/result/status/error), `session_context`,
  `token_usage`, plus Cotal addressing extras under `cotal.*`.
- A Cotal journal/run → `AdrAgentEvent` mapper (`fromCotalRunEvent`) and ADR's
  noise filter (`hasMeaningfulContent`, same skip patterns).
- `bench/attack-techniques.json`: connector evaluation checklist. Only
  categories directly documented in uber/ADR's READMEs are named (MCP
  supply-chain impersonation, prompt injection, vulnerable-MCP discovery); the
  full 17 techniques are enumerated in the paper — this file points there
  rather than guessing.
- `NOTICE.md` attribution per Apache-2.0 §4.

**Not adopted:**

- The Detector's Python dual-agent code — stays upstream; this connector feeds
  it normalized telemetry.
- ADR-Bench — its own README says **"Not for production use"** (isolated
  environment only; pinned deps with known CVEs; synthetic credentials and
  prompt-injection payloads). We reference it as a pre-admission evaluation
  step, never as fleet code.
- Discovery / Prevention / Explorer — out of scope; Discovery is a possible
  follow-up for fleet host inventory.

## Why ADAPT and not full vendor

ADR is enterprise Python; cotal is TypeScript. Vendoring the detector would
drag in pinned-vulnerable benchmark deps and duplicate ADR's release cadence.
The schema is the durable interface — porting it keeps us compatible with ADR's
tooling without owning its code.

## Relationship to existing cotal security

- `docs/security.md` (threat model: NATS ACLs, profiles, consumer confinement)
  = **transport** trust. This connector = **agent-behavior** observability.
  Complementary, no overlap.

## Review status

Draft PR opened against `main` (does not touch `arena-hub-v1` / PR #1).
Verdicts from Grok/Codex recorded in the parent report.
