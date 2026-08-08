#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

resolve_cargo() {
  if command -v cargo >/dev/null 2>&1; then
    command -v cargo
    return 0
  fi
  if command -v cargo.exe >/dev/null 2>&1; then
    command -v cargo.exe
    return 0
  fi
  echo "cargo is required for runtime contract snapshot checks." >&2
  exit 1
}

cd "$ROOT_DIR"
CARGO_BIN="$(resolve_cargo)"

print_snapshot_failure_guidance() {
  cat >&2 <<'EOF'

Runtime contract snapshot gate failed.
Next step for intentional public contract changes:
  1. Inspect the test output above and the git diff for the changed golden snapshot.
  2. Update the matching snapshot_version for the changed public contract surface.
  3. Include a changelog_note or migration note in the same change explaining compatibility.
  4. Never add secrets, tokens, private keys, or local absolute paths to snapshots.

For mechanical local refresh only, run the failing test with:
  PALYRA_UPDATE_CONTRACT_SNAPSHOTS=1 cargo test <package-and-test> --locked
EOF
}

run_contract_check() {
  local label="$1"
  shift
  echo "==> ${label}"
  if ! "$@"; then
    print_snapshot_failure_guidance
    exit 1
  fi
}

run_exact_contract_check() {
  local label="$1"
  local expected_test="$2"
  shift 2
  echo "==> ${label}"

  local output
  if ! output="$("$@" 2>&1)"; then
    printf '%s\n' "$output"
    print_snapshot_failure_guidance
    exit 1
  fi
  printf '%s\n' "$output"

  local expected_result="test ${expected_test} ... ok"
  local executed_count
  executed_count="$(tr -d '\r' <<<"$output" | grep -Fxc "$expected_result" || true)"
  if [[ "$executed_count" -ne 1 ]]; then
    echo "Expected exactly one executed Rust test result for ${expected_test}; observed ${executed_count}." >&2
    print_snapshot_failure_guidance
    exit 1
  fi
}

run_contract_check \
  "public runtime contract snapshot" \
  "$CARGO_BIN" test -p palyra-common public_runtime_contract_snapshot --locked
run_contract_check \
  "plugin SDK public contract snapshot" \
  "$CARGO_BIN" test -p palyra-plugins-sdk plugin_sdk_contract_snapshot_matches_golden --locked
run_contract_check \
  "plugin SDK typed ABI fingerprint" \
  "$CARGO_BIN" test -p palyra-plugins-sdk typed_contract_abi_fingerprint_matches_golden --locked
run_contract_check \
  "plugin executable ABI v2 snapshot" \
  "$CARGO_BIN" test -p palyra-plugins-sdk executable_abi_v2_snapshot_matches_golden --locked
run_contract_check \
  "plugin executable ABI v2 conformance" \
  "$CARGO_BIN" test -p palyra-plugins-runtime --test abi_v2_conformance --locked
run_contract_check \
  "skill manifest public contract snapshot" \
  "$CARGO_BIN" test -p palyra-skills skill_manifest_contract_snapshot_matches_golden --locked
run_contract_check \
  "policy diagnostics safe reason-code contract" \
  "$CARGO_BIN" test -p palyra-policy explain_diagnostics_reports_safe_reason_code_and_hints --locked
run_contract_check \
  "daemon aggregate runtime ABI snapshot" \
  "$CARGO_BIN" test -p palyra-daemon runtime_diagnostics::tests::contract_snapshot_suite_covers_plugin_abi_surfaces --locked
run_exact_contract_check \
  "managed coding compatibility snapshot" \
  "runtime_diagnostics::tests::managed_coding_contract_snapshot_matches_golden" \
  "$CARGO_BIN" test -p palyra-daemon --lib runtime_diagnostics::tests::managed_coding_contract_snapshot_matches_golden --locked -- --exact
run_exact_contract_check \
  "feature rollout promotion manifest contract" \
  "feature_rollout_maturity::manifest::tests::builtin_promotion_manifest_is_valid" \
  "$CARGO_BIN" test -p palyra-daemon feature_rollout_maturity::manifest::tests::builtin_promotion_manifest_is_valid --locked -- --exact
run_exact_contract_check \
  "feature rollout direct hot-path proof" \
  "gateway::tests::session_compaction_safeguard_rolls_back_writes_when_rollout_enforces_failure" \
  "$CARGO_BIN" test -p palyra-daemon gateway::tests::session_compaction_safeguard_rolls_back_writes_when_rollout_enforces_failure --locked -- --exact
run_exact_contract_check \
  "feature rollout no-hidden-fallback proof" \
  "gateway::tests::session_compaction_safeguard_records_explicit_fallback_when_disabled" \
  "$CARGO_BIN" test -p palyra-daemon gateway::tests::session_compaction_safeguard_records_explicit_fallback_when_disabled --locked -- --exact
run_exact_contract_check \
  "Docker rollout direct container-path proof" \
  "gateway::tests::docker_runtime_selects_container_process_path" \
  "$CARGO_BIN" test -p palyra-daemon gateway::tests::docker_runtime_selects_container_process_path --locked -- --exact
run_exact_contract_check \
  "Docker rollout no-hidden-fallback proof" \
  "gateway::tests::docker_runtime_fails_closed_without_host_fallback" \
  "$CARGO_BIN" test -p palyra-daemon gateway::tests::docker_runtime_fails_closed_without_host_fallback --locked -- --exact
run_exact_contract_check \
  "stable core evidence contract" \
  "application::core_stability::stable::tests::builtin_evidence_pack_qualifies" \
  "$CARGO_BIN" test -p palyra-daemon --lib application::core_stability::stable::tests::builtin_evidence_pack_qualifies --locked -- --exact
run_contract_check \
  "stable core maturity, fixture, and redaction conformance" \
  "$CARGO_BIN" test -p palyra-daemon --lib application::core_stability::stable::tests --locked
run_contract_check \
  "release dashboard, runbook drill, and support redaction" \
  pwsh -NoLogo -NoProfile -File scripts/ci/check-release-dashboard.ps1
