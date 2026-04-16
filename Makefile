fmt:
	cargo fmt --all

fmt-check:
	cargo fmt --all --check

doctor:
	@echo "Running strict environment checks via 'palyra doctor --strict'..."
	@echo "Required checks must pass. Optional checks are informational."
	cargo run -p palyra-cli --bin palyra -- doctor --strict
	@echo "Doctor passed. Next step: 'make dev'."

validate-env:
	$(MAKE) doctor

dev: validate-env build
	@echo "Bootstrap complete. Run 'vp install' to materialize the root JS workspace."
	@echo "Run 'make test' to verify workspace tests."

lint:
	cargo clippy --workspace --all-targets -- -D warnings

protocol-validate:
	bash scripts/protocol/validate-proto.sh

protocol-generate:
	bash scripts/protocol/generate-stubs.sh

protocol:
	$(MAKE) protocol-validate
	$(MAKE) protocol-generate
	bash scripts/protocol/validate-rust-stubs.sh

test:
	$(MAKE) desktop-ui-ready
	cargo test --workspace --locked

build:
	$(MAKE) desktop-ui-ready
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
	$(MAKE) sbom
	bash scripts/generate-attestation-placeholder.sh security-artifacts/attestation-placeholder.json

web-bootstrap:
	vp install

web-clean:
	npm run web:clean

web-check:
	npm run web:ci

web-cleanroom:
	rm -rf node_modules
	vp install
	npm run web:ci

desktop-ui-ready:
	bash scripts/test/ensure-desktop-ui.sh

deterministic-core:
	bash scripts/test/run-deterministic-core.sh

cli-test:
	cargo test -p palyra-cli --locked

cli-regression:
	bash scripts/test/run-workflow-regression.sh

cli-install-smoke:
	pwsh -NoLogo -File scripts/test/run-cli-install-smoke.ps1

cli-install-smoke-sh:
	bash scripts/test/run-cli-install-smoke.sh

workflow-regression:
	bash scripts/test/run-workflow-regression.sh

main-preflight:
	cargo test --workspace --locked
	pwsh -NoLogo -File scripts/test/run-workflow-regression.ps1
	pwsh -NoLogo -File scripts/test/run-cli-install-smoke.ps1

release-smoke:
	pwsh -NoLogo -File scripts/test/run-release-smoke.ps1

release-smoke-sh:
	bash scripts/test/run-release-smoke.sh

push-gate-fast:
	bash scripts/run-pre-push-checks.sh

push-gate-full:
	PALYRA_PRE_PUSH_PROFILE=full bash scripts/run-pre-push-checks.sh

deterministic-soak:
	bash scripts/test/run-deterministic-soak.sh

performance-smoke:
	bash scripts/test/run-performance-smoke.sh

surface-release-smoke:
	pwsh -NoLogo -File scripts/test/run-surface-release-smoke.ps1

surface-release-smoke-sh:
	bash scripts/test/run-surface-release-smoke.sh

module-budgets:
	bash scripts/dev/report-module-budgets.sh

module-budgets-strict:
	bash scripts/dev/report-module-budgets.sh --strict

connector-boundaries:
	bash scripts/check-channel-provider-boundaries.sh

deterministic-fixtures-update:
	bash scripts/test/update-deterministic-fixtures.sh

deterministic-fixtures-check:
	bash scripts/test/check-deterministic-fixtures.sh

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
