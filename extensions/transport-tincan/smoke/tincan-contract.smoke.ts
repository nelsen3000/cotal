/**
 * Smoke test for transport-tincan — proves the capability declarations are
 * internally consistent with Cotal's contract in docs/transport.md:
 * all five contract capabilities are mapped, exactly one hard gap exists
 * (authorization/isolation), and that gap is flagged as Cotal-supplied.
 * Exercises capabilities.ts directly; starts no relay and dials nothing.
 * Run: pnpm smoke:tincan
 */
import { TINCAN_CAPABILITY_MAP, gapsAboveTransport, hardGaps } from "../src/capabilities.js";

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

// All five contract capabilities must be mapped.
const expected = [
  "addressed_routing",
  "durable_delivery_and_history",
  "presence_and_registry",
  "identity",
  "authorization_and_isolation",
] as const;
const mapped = TINCAN_CAPABILITY_MAP.map((m) => m.capability);
ok("all five contract capabilities mapped", expected.every((c) => mapped.includes(c)));

// The authz gap must be declared as a gap with Cotal supplying above.
const authz = TINCAN_CAPABILITY_MAP.find((m) => m.capability === "authorization_and_isolation");
ok("authz declared as a gap", authz?.provision === "gap");
ok("authz flagged Cotal-supplied", authz?.cotal_provides_above === true);

// Exactly one hard gap; helpers agree.
ok("exactly one hard gap", hardGaps().length === 1);
ok(
  "gaps-above list includes authz",
  gapsAboveTransport().includes("authorization_and_isolation"),
);

// Every row must say what Cotal supplies and carry notes for reviewers.
ok(
  "every row documents the split",
  TINCAN_CAPABILITY_MAP.every((m) => m.tincan_realization.length > 0 && m.notes.length > 0),
);

console.log(`\n${passed} passed, ${failed} failed`);
process.exit(failed === 0 ? 0 : 1);
