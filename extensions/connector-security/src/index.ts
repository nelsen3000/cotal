/**
 * connector-security — ADR-adapted security observability for the Arena Hub fleet.
 *
 * Entry point. Re-exports the normalized agent-event schema (ported from
 * uber/ADR's Sensor AgentEvent, Apache-2.0) and the Cotal run-event mapper.
 *
 * Design note: ADR's Detector itself (dual-agent triage + reasoning) and
 * ADR-Bench stay external — the benchmark README marks them "not for
 * production use" (isolated-environment research artifact). This connector
 * produces the telemetry those tools consume; it does not reimplement them.
 */
export {
  AdrAgentEvent,
  AdrChatMessage,
  AdrToolUsage,
  CotalRunEventLike,
  fromCotalRunEvent,
  hasMeaningfulContent,
} from "./schema";
