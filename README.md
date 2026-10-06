# samurab

Standalone home for the Crypto Arbitrage Scanner infrastructure.

## Oracle A1 capacity agent

See [oracle-a1-agent/README.md](oracle-a1-agent/README.md).

The GitHub Actions workflow polls OCI Singapore A1 capacity and launches exactly one `VM.Standard.A1.Flex` instance at 2 OCPU / 12 GB when capacity is available. It is intentionally isolated from the Token Samurai repository.
