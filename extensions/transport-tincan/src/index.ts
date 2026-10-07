/**
 * transport-tincan — adapter spec for a tincan second transport binding.
 *
 * Entry point. Re-exports the transport-contract capability mapping
 * (mvanhorn/agent-tincan, MIT — see NOTICE.md) against Cotal's capability
 * contract (docs/transport.md). Spec + declarations only: no live tincan
 * relay is started by this package.
 */
export {
  CapabilityMapping,
  Provision,
  TINCAN_CAPABILITY_MAP,
  TransportContractCapability,
  gapsAboveTransport,
  hardGaps,
} from "./capabilities";
