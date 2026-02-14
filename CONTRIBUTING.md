# Contributing to Palyra

## Prerequisites

- Rust toolchain from `rust-toolchain.toml`
- `cargo-audit`
- `cargo-deny`
- `gitleaks` (required by pre-commit hook)
- `protoc` (required for protocol schema checks)
- `just` (optional convenience runner)
- `cargo-cyclonedx` (required for local SBOM generation)

Optional:

- `osv-scanner` (required locally if you want full parity with CI security gate)
- `cargo-fuzz` + nightly Rust (required for fuzz target compilation/runs)

## Local quality checks

Run these commands before opening a pull request:

```bash
just dev
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace --locked
just protocol
just security
just security-artifacts
```

## Local hooks

Use the provided hook path:

```bash
git config core.hooksPath .githooks
```

The pre-commit hook enforces formatting and secret scanning.
The pre-push hook enforces formatting, clippy, tests, and high-risk pattern checks.

## Security SDLC requirements

- Complete the threat review section in `.github/pull_request_template.md`.
- If the change touches high-risk areas (sandbox/policy/crypto/updater/security CI), request security sign-off before merge.
- Exceptions to security gates must be documented and time-bounded in `docs/security/risk-register.md`.

## Commit and provenance expectations

- Keep commits small and milestone-scoped.
- Sign commits when possible (`git commit -S`).
- Do not include credentials, secrets, or private tokens in code, tests, or logs.
