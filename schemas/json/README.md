# JSON Schemas

Canonical JSON schemas are split into:

- `schemas/json/common/`: shared primitives (canonical IDs, replay protection, runtime resource manifests, feature-rollout promotion evidence, and strict runtime-error metadata).
- `schemas/json/envelopes/`: public envelope payloads (`message`, `a2ui`, `config export/import`, `webhook`).

Every public JSON envelope must:

- include `v` (major schema version),
- define hard payload caps via explicit limits,
- set `additionalProperties` to `false` unless explicitly extensible.

Strict shared contracts under `schemas/json/common/` may use `schema_version`
when they are not public envelopes. `runtime-error-envelope.v1.json` is one such
contract: it closes unknown fields, carries only bounded redacted text, and keeps
retry and side-effect uncertainty as typed metadata rather than message-derived
behavior.

`dynamic-tool-artifact.v1.json` and
`dynamic-tool-activation-decision.v1.json` pin the immutable signed artifact,
six-case conformance evidence, host approval generation, catalog epoch, and
rollback pointer for restricted declarative or WASM dynamic tools. Artifact
presence alone never grants catalog or execution authority.

`semantic-memory-candidate.v1.json` and
`consolidated-memory-record.v1.json` pin candidate-only semantic consolidation:
ACL-compatible evidence references, epistemic labels, source citations,
contradiction posture, quality comparison, approval generation, retrieval
feedback, bounded retention, degradation, archive, and rollback history.

`docker-backend-capability-report.v1.json` and
`container-cleanup-attestation.v1.json` pin the daemon/profile qualification
matrix and fail-closed cleanup settlement for production-gated Docker runs.
They expose resource and cleanup posture without container output, environment
values, mount contents, or raw resource-lease identities.

`audio-input-artifact.v1.json`, `transcription-artifact.v1.json`, and
`audio-output-artifact.v1.json` pin connector-neutral media provenance,
untrusted transcript citations, metered usage, bounded retention, and
post-delivery audio descriptors. Raw audio bytes remain outside these durable
contracts.

Managed coding runtime contracts are split by durable or operator-visible
responsibility:

- `process-session-record.v2.json`, `process-output-page.v2.json`,
  `local-resource-registry.v2.json`, and `resource-pressure-snapshot.v1.json`
  pin supervised-process and local-capacity persistence plus bounded redacted
  cursor pages without raw output, environment values, or live handle
  authority.
- `pty-session-descriptor.v1.json` pins native Unix PTY and Windows ConPTY
  metadata while raw terminal bytes and input remain local-only.
- `managed-worktree-registry.v2.json`,
  `worktree-snapshot-descriptor.v1.json`, and
  `worktree-restore-report.v1.json` pin bounded Git isolation and lossless
  snapshot recovery metadata.
- `lsp-registry.v2.json`, `diagnostics-baseline.v2.json`, and
  `diagnostics-delta.v2.json` pin process-backed language-service state and
  generation-aware edit verification. `lsp-diagnostics-snapshot.v2.json` is
  the closed operator projection; it exposes hashed identities and capability
  size evidence instead of raw capability payloads.
- `coding-runtime-capability-report.v2.json`,
  `coding-patch-outcome.v2.json`, `coding-command-status.v2.json`, and
  `coding-task-cleanup-outcome.v2.json` pin integrated capability, mutation,
  command, and cleanup results.
- `managed-coding-diagnostics.v1.json` and
  `managed-coding-recovery.v1.json` pin the redacted operator health projection
  and typed recovery inventory without exposing raw process or workspace
  identities.
- `coding-runtime-soak-report.v1.json` pins bounded cross-platform warm
  language-service performance and cleanup evidence.
