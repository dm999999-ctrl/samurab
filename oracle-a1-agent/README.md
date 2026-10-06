# Oracle A1 Capacity Agent

This agent continuously polls OCI Compute Capacity Report for Singapore A1 Always Free capacity and launches the Crypto Arbitrage Scanner VM when the exact target becomes available.

## Exact launch target

- Region: `ap-singapore-1`
- Availability Domain: `EmQk:AP-SINGAPORE-1-AD-1`
- Shape: `VM.Standard.A1.Flex`
- OCPU: `1`
- RAM: `6 GB`
- Image: Ubuntu 26.04 ARM64
- Display name: `crypto-arbitrage-scanner`
- Public IP: enabled
- Subnet: existing public subnet in the scanner VCN

## Safety behaviour

The agent is fail-closed. It does not fall back to E5, change the requested size, or create a second scanner VM. Unexpected API errors stop the current run rather than triggering an unsafe launch.

The polling process runs for up to 5h45m per GitHub-hosted job. The workflow is scheduled again every 6 hours so the watcher can continue without requiring another VM.

## GitHub secrets

Configure these repository secrets:

- `OCI_USER_OCID`
- `OCI_FINGERPRINT`
- `OCI_PRIVATE_KEY`

The private key must never be committed to the repository.

## AI gate

The orchestration deliberately keeps the launch decision deterministic rather than allowing an LLM to construct OCI requests. This protects against hallucinated shapes, regions, sizes, or duplicate launches. The "agent" layer can be extended with an LLM later for anomaly interpretation, but the actual launch permission remains governed by the hard safety checks above.
