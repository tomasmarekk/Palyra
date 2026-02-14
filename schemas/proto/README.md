# Protobuf Schemas (M04)

Canonical protobuf API surfaces live under `schemas/proto/palyra/v1/`:

- `common.proto`: canonical IDs, message/event envelopes, replay protection, `RunStream` primitives.
- `gateway.proto`: gateway control and run-stream RPC surfaces.
- `node.proto`: node registration, device pairing (`PIN`/`QR`), mTLS certificate rotation/revocation,
  capability execution, and event streaming.
- `browser.proto`: browser profile/action/observe RPC surfaces.
- `plugin.proto`: plugin host registration, invocation, and streaming surfaces.

## Compatibility baseline

- Every package is versioned (`*.v1`).
- Messages reserve future field ranges (`reserved ...`) for forward compatibility.
- Breaking changes require a new major package (`v2`) and parallel support window.
