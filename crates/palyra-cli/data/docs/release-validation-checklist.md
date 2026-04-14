# Release Validation Checklist

Purpose: provide the operator checklist for release handoff of the packaged CLI and backend parity
surface.

## Preflight

- Run `pwsh -NoLogo -File scripts/release/assert-version-coherence.ps1`.
- Run `bash scripts/check-gh-actions-pinned.sh`.
- Run `bash scripts/check-runtime-artifacts.sh`.
- Run `bash scripts/check-desktop-glib-patch.sh`.

## Local validation

- Run `cargo fmt --all --check`.
- Run `bash scripts/test/run-workflow-regression.sh`.
- Run `pwsh -NoLogo -File scripts/test/run-release-smoke.ps1`.
- Run `pwsh -NoLogo -File scripts/test/run-deterministic-core.ps1`.
- Run `bash scripts/check-high-risk-patterns.sh`.

## Product guardrail review

- Confirm Basic mode still surfaces approvals, access posture, and an explicit path back to the full operator surface.
- Confirm new onboarding or quick-start affordances do not bypass auth, pairing, approval, or trust verification flows.
- Confirm browser handoff and relay defaults still require the existing local mediation and pairing posture.
- Confirm deterministic project context and learned memory remain separate in both UX copy and implementation.
- Confirm UX telemetry stays content-free and records only bounded metadata.
- Confirm cross-surface handoffs carry identifiers and intent only, with no secrets or prompt content.

## Artifact review

- Confirm desktop archives exist for Windows, macOS, and Linux.
- Confirm the headless archive exists.
- Confirm each asset set includes:
  - `*.zip`
  - `*.zip.sha256`
  - `*.release-manifest.json`
  - `*.provenance.json`
- Confirm each archive validates with `scripts/release/validate-portable-archive.ps1`.
- Confirm packaged `RELEASE_NOTES.txt` and `MIGRATION_NOTES.txt` mention:
  - `setup` as the canonical bootstrap command with `init` kept only as a compatibility alias
  - `gateway` as the canonical runtime and admin family with `daemon` kept only as a compatibility alias
  - `onboarding wizard` as the canonical guided flow with `onboard` documented only as shorthand compatibility
  - the current OpenAI and Discord-first posture plus explicit unsupported browser placeholders

## Packaged CLI parity smoke

- Confirm the installed `palyra` command resolves from the managed CLI command root rather than the
  repo checkout.
- Confirm the installed package accepts:
  - `palyra setup --help`
  - `palyra init --help`
  - `palyra onboarding wizard --help`
  - `palyra onboard wizard --help`
  - `palyra gateway --help`
  - `palyra daemon --help`
  - `palyra browser --help`
  - `palyra node --help`
  - `palyra nodes --help`
  - `palyra docs --help`
  - `palyra update --help`
  - `palyra uninstall --help`
  - `palyra support-bundle --help`
- Confirm packaged docs lookups succeed for:
  - `palyra docs search migration`
  - `palyra docs search acp`
  - `palyra docs show release-validation-checklist`
  - `palyra docs show cli-v1-acp-shim`
  - `palyra docs show docs/architecture/browser-service-v1.md`

## Publication gate

- Confirm `.github/workflows/release.yml` keeps draft release creation enabled for tags.
- Confirm GitHub build attestations are generated for uploaded assets.
- Confirm main CI is green before final publication.
