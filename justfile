fmt:
    cargo fmt --all

fmt-check:
    cargo fmt --all --check

validate-env:
    cargo run -p palyra-cli --bin palyra -- doctor --strict

dev:
    just validate-env
    cargo build --workspace --locked
    @echo "Bootstrap complete. Run 'just test' to verify workspace tests."

lint:
    cargo clippy --workspace --all-targets -- -D warnings

lint-clients:
    bash apps/android/scripts/lint.sh
    bash apps/web/scripts/lint.sh

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
      osv-scanner --config osv-scanner.toml scan -L Cargo.lock; \
    else \
      echo "osv-scanner is not installed. Install it to run this gate locally."; \
      exit 1; \
    fi

security:
    cargo audit
    cargo deny check
    bash scripts/check-high-risk-patterns.sh

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
      cargo fuzz build webhook_payload_parser; \
    else \
      echo "cargo-fuzz is not installed. Install it to compile fuzz targets."; \
      exit 1; \
    fi

bench:
    cargo bench -p palyra-policy --no-run
