# Palyra CLI Docs Bundle

Purpose: provide the tracked operator and release docs that are indexed by `palyra docs` in a
source checkout and bundled into portable installs for offline lookup.

## Included operator docs

- `docs/cli-v1-acp-shim.md`: current CLI and ACP bridge surface, including the preferred command
  families used by release docs and package smoke.
- `docs/cli-mcp-interop-playbook.md`: MCP facade scope, mutation posture, and safe stdio client
  rollout guidance.
- `docs/cli-parity-migration-v1.md`: canonical versus compatibility command names and the migration
  guidance for existing automation.
- `docs/release-engineering-v1.md`: portable archive contract, provenance sidecars, and release
  automation expectations.
- `docs/release-validation-checklist.md`: release handoff checklist for packaging, install, and CI
  validation.

## Included architecture docs

- `docs/architecture/README.md`: architecture subsection index for the packaged docs bundle.
- `docs/architecture/browser-service-v1.md`: browser service runtime contract, security posture,
  and operator surfaces.

## CLI help snapshots

- `docs/help_snapshots/*.txt`: packaged command help output for offline discoverability and parity
  verification.

## Scope boundaries

- This bundle is intentionally small and fully tracked in Git so packaging and CI do not depend on
  ignored local-only `repo-root/docs`.
- The bundle documents current, shipped behavior only. Planning notes and roadmap material stay
  outside the packaged surface.
