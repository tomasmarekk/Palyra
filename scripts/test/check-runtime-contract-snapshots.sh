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
  "skill manifest public contract snapshot" \
  "$CARGO_BIN" test -p palyra-skills skill_manifest_contract_snapshot_matches_golden --locked
run_contract_check \
  "policy diagnostics safe reason-code contract" \
  "$CARGO_BIN" test -p palyra-policy explain_diagnostics_reports_safe_reason_code_and_hints --locked
run_contract_check \
  "daemon aggregate runtime ABI snapshot" \
  "$CARGO_BIN" test -p palyra-daemon runtime_diagnostics::tests::contract_snapshot_suite_covers_phase11_abi_surfaces --locked
