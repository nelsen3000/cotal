/**
 * Smoke test for connector-security — proves the ADR-adapted schema round-trips
 * a Cotal run event and that the noise filter behaves like ADR's
 * has_meaningful_content(): auth failures, rate limits and warmups are dropped,
 * real tool activity is kept. Exercises schema.ts directly; needs no broker.
 * Run: pnpm smoke:security
 */
import { fromCotalRunEvent, hasMeaningfulContent } from "../src/schema.js";

let passed = 0;
let failed = 0;
function ok(label: string, val: unknown): void {
  if (val) {
    console.log(`  ✓ ${label}`);
    passed++;
  } else {
    console.error(`  ✗ FAIL: ${label}`);
    failed++;
  }
}

// 1. Mapping: a Cotal DM run event becomes an ADR AgentEvent.
const mapped = fromCotalRunEvent(
  {
    ts: "2026-10-07T12:00:00.000Z",
    space: "arena-hub",
    from: "ops.muse",
    channel: "dm/ops.muse",
    kind: "dm",
    text: "Deploy the new connector build.",
    toolCalls: [
      { name: "send_message", type: "cotal_endpoint", ok: true },
      { name: "read_file", type: "function_call", ok: false, error: "ENOENT" },
    ],
    connector: "connector-muse",
    model: "muse-spark",
  },
  "session-123",
);
ok("source is cotal", mapped.source === "cotal");
ok("sender principal preserved", mapped.cotal?.sender === "ops.muse");
ok("dm maps to unicast delivery", mapped.cotal?.delivery_mode === "unicast");
ok("tool calls preserved", mapped.chat_history[0].tools.length === 2);
ok("failed tool marked error", mapped.chat_history[0].tools[1].status === "error");
ok("kept: real activity is meaningful", hasMeaningfulContent(mapped) === true);

// 2. Noise filter: matches ADR AgentEvent.has_meaningful_content() skip patterns.
const noisy = fromCotalRunEvent(
  {
    ts: "2026-10-07T12:01:00.000Z",
    space: "arena-hub",
    from: "ops.muse",
    kind: "chat",
    text: "Rate limit exceeded, retrying.",
    connector: "connector-muse",
  },
  "session-124",
);
ok("dropped: rate-limit noise is not meaningful", hasMeaningfulContent(noisy) === false);

const empty = fromCotalRunEvent(
  {
    ts: "2026-10-07T12:02:00.000Z",
    space: "arena-hub",
    from: "ops.muse",
    kind: "chat",
    text: "ok",
    connector: "connector-muse",
  },
  "session-125",
);
ok("dropped: empty content is not meaningful", hasMeaningfulContent(empty) === false);

console.log(`\n${passed} passed, ${failed} failed`);
process.exit(failed === 0 ? 0 : 1);
