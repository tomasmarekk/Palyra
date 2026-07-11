# JSON Schemas (M04)

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
