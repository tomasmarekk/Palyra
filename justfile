fmt:
    cargo fmt --all

fmt-check:
    cargo fmt --all --check

doctor:
    @echo "Running strict environment checks via 'palyra doctor --strict'..."
    @echo "Required checks must pass. Optional checks are informational."
    cargo run -p palyra-cli --bin palyra -- doctor --strict
    @echo "Doctor passed. Next step: 'just dev'."

validate-env:
    just doctor

dev:
    just doctor
    cargo build --workspace --locked
    @echo "Bootstrap complete. Run 'just web-bootstrap' for the apps/web clean-room bootstrap."
    @echo "Run 'just test' to verify workspace tests."

lint:
    cargo clippy --workspace --all-targets -- -D warnings

lint-clients:
    bash apps/android/scripts/lint.sh
    bash apps/web/scripts/lint.sh

web-bootstrap:
    npm --prefix apps/web run bootstrap

web-clean:
    npm --prefix apps/web run clean

web-check:
    npm --prefix apps/web run ci:check

web-cleanroom:
    npm --prefix apps/web run cleanroom:check

deterministic-core:
    bash scripts/test/run-deterministic-core.sh

release-smoke:
    pwsh -NoLogo -File scripts/test/run-release-smoke.ps1

push-gate-fast:
    bash scripts/run-pre-push-checks.sh

push-gate-full:
    PALYRA_PRE_PUSH_PROFILE=full bash scripts/run-pre-push-checks.sh

deterministic-soak:
    bash scripts/test/run-deterministic-soak.sh

performance-smoke:
    bash scripts/test/run-performance-smoke.sh

deterministic-fixtures-update:
    bash scripts/test/update-deterministic-fixtures.sh

deterministic-fixtures-check:
    bash scripts/test/check-deterministic-fixtures.sh

protocol-validate:
    bash scripts/protocol/validate-proto.sh

protocol-generate:
    bash scripts/protocol/generate-stubs.sh

protocol:
    just protocol-validate
    just protocol-generate
    bash scripts/protocol/validate-rust-stubs.sh

test:
    cargo test --workspace --locked

build:
    cargo build --workspace --locked

audit:
    cargo audit

deny:
    cargo deny check

osv:
    @if command -v osv-scanner >/dev/null 2>&1; then \
      osv-scanner scan --config osv-scanner.toml -L Cargo.lock; \
    else \
      echo "osv-scanner is not installed. Install it to run this gate locally."; \
      exit 1; \
    fi

security:
    cargo audit
    cargo deny check
    bash scripts/check-runtime-artifacts.sh
    bash scripts/check-desktop-glib-patch.sh
    bash scripts/check-high-risk-patterns.sh

artifact-hygiene:
    bash scripts/check-runtime-artifacts.sh

artifact-clean:
    bash scripts/clean-runtime-artifacts.sh

desktop-glib-patch-check:
    bash scripts/check-desktop-glib-patch.sh

sbom:
    cargo cyclonedx --format json --override-filename sbom

security-artifacts:
    mkdir -p security-artifacts
    just sbom
    bash scripts/generate-attestation-placeholder.sh security-artifacts/attestation-placeholder.json

fuzz-build:
    @if cargo fuzz --help >/dev/null 2>&1; then \
      cd fuzz && \
      cargo fuzz build config_path_parser && \
      cargo fuzz build a2ui_json_parser && \
      cargo fuzz build webhook_payload_parser && \
      cargo fuzz build auth_profile_registry_parser && \
      cargo fuzz build redaction_routines && \
      cargo fuzz build channel_payload_validation && \
      cargo fuzz build webhook_replay_verifier; \
    else \
      echo "cargo-fuzz is not installed. Install it to compile fuzz targets."; \
      exit 1; \
    fi

bench:
    cargo bench -p palyra-policy --no-run
