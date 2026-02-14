# JSON Schemas (M04)

Canonical JSON schemas are split into:

- `schemas/json/common/`: shared primitives (canonical IDs, replay protection).
- `schemas/json/envelopes/`: public envelope payloads (`message`, `a2ui`, `config export/import`, `webhook`).

Every public JSON envelope must:

- include `v` (major schema version),
- define hard payload caps via explicit limits,
- set `additionalProperties` to `false` unless explicitly extensible.
