/**
 * Tincan transport capability declarations — for the tincan second-binding
 * experiment (see extensions/transport-tincan/ADAPTER.md).
 *
 * Cotal's protocol is transport-agnostic; docs/transport.md defines the
 * capability contract a binding must provide (or Cotal must supply above it).
 * NATS/JetStream is the reference binding. This module declares, capability
 * by capability, what a tincan (mvanhorn/agent-tincan, MIT) relay binding
 * provides natively and where Cotal has to fill the gap.
 */

export type TransportContractCapability =
  | "addressed_routing"
  | "durable_delivery_and_history"
  | "presence_and_registry"
  | "identity"
  | "authorization_and_isolation";

export type Provision = "native" | "partial" | "gap";

/**
 * One row of the contract mapping. "cotal_provides_above" is true when Cotal
 * must implement the capability above the tincan pipe (the same split
 * docs/transport.md describes for live-only pipes).
 */
export interface CapabilityMapping {
  capability: TransportContractCapability;
  provision: Provision;
  tincan_realization: string;
  cotal_provides_above: boolean;
  notes: string;
}

/**
 * The mapping, grounded in tincan's documented behavior
 * (mvanhorn/agent-tincan README + docs/protocol.md + docs/trust-model.md)
 * and Cotal's contract (docs/transport.md).
 */
export const TINCAN_CAPABILITY_MAP: CapabilityMapping[] = [
  {
    capability: "addressed_routing",
    provision: "partial",
    tincan_realization:
      "unicast request/reply between named agents (tincan ask <agent>), group fan-out via council; no hierarchical channel wildcards",
    cotal_provides_above: true,
    notes:
      "Cotal multicast channels and anycast roles must be emulated above tincan (fan-out at the adapter); NATS wildcard subscriptions have no tincan equivalent.",
  },
  {
    capability: "durable_delivery_and_history",
    provision: "partial",
    tincan_realization:
      "relay queues requests and retries wake delivery (unanswered-wake follow-ups); replies carried back over the relay",
    cotal_provides_above: true,
    notes:
      "No per-instance bookmarks, durable-channel backstops, at-least-once ack/redelivery, or publish dedup. The adapter must add message-id dedup and the delivery daemon's backstop semantics above the pipe.",
  },
  {
    capability: "presence_and_registry",
    provision: "partial",
    tincan_realization:
      "presence and last-seen tracking exist (internal/presence.go, lastseen); no channel-registry KV",
    cotal_provides_above: true,
    notes:
      "Presence writes can ride tincan presence; the cotal_channels_* registry and membership feed must live above it.",
  },
  {
    capability: "identity",
    provision: "partial",
    tincan_realization:
      "Tailscale WhoIs binds each request to a tailnet machine identity; no owner.actor principal concept",
    cotal_provides_above: true,
    notes:
      "Adapter MUST map machine identity -> cotal principal (owner.actor) explicitly; one machine may host many actors. See docs/ADOPTION-agent-tincan.md.",
  },
  {
    capability: "authorization_and_isolation",
    provision: "gap",
    tincan_realization:
      "trust model is teammate-grade: joined agents trust each other; no per-channel allowPublish/allowSubscribe ACLs, no default-deny",
    cotal_provides_above: true,
    notes:
      "HARD GAP. Cotal must enforce its ACLs (SPEC §9) at the adapter boundary before handing traffic to tincan; the relay itself cannot be trusted for isolation.",
  },
];

/** Convenience: the capabilities Cotal must supply above a tincan pipe. */
export function gapsAboveTransport(): TransportContractCapability[] {
  return TINCAN_CAPABILITY_MAP.filter((m) => m.cotal_provides_above).map(
    (m) => m.capability,
  );
}

/** The one hard gap reviewers should scrutinize: authz is not provided by tincan. */
export function hardGaps(): CapabilityMapping[] {
  return TINCAN_CAPABILITY_MAP.filter((m) => m.provision === "gap");
}
