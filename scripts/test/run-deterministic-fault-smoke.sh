#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"

resolve_cargo() {
  if command -v cargo >/dev/null 2>&1; then
    command -v cargo
    return 0
  fi
  if command -v cargo.exe >/dev/null 2>&1; then
    command -v cargo.exe
    return 0
  fi
  echo "cargo is required for deterministic fault smoke checks." >&2
  exit 1
}

cd "$ROOT_DIR"
CARGO_BIN="$(resolve_cargo)"
ARTIFACT_RELATIVE="artifacts/qa-lab/deterministic-fault-smoke"
ARTIFACT_ROOT="$ROOT_DIR/artifacts"
ARTIFACT_PARENT="$ARTIFACT_ROOT/qa-lab"
ARTIFACT_ABSOLUTE="$ARTIFACT_PARENT/deterministic-fault-smoke"

assert_plain_directory_or_missing() {
  local path="$1"
  if [[ -L "$path" ]]; then
    echo "Refusing an indirect deterministic fault artifact directory: $path" >&2
    exit 1
  fi
  if [[ -e "$path" && ! -d "$path" ]]; then
    echo "Deterministic fault artifact path is not a directory: $path" >&2
    exit 1
  fi
}

assert_physical_directory() {
  local path="$1"
  local expected="$2"
  assert_plain_directory_or_missing "$path"
  if [[ ! -d "$path" ]]; then
    echo "Expected deterministic fault artifact directory is missing: $path" >&2
    exit 1
  fi
  local physical
  physical="$(cd "$path" && pwd -P)"
  if [[ "$physical" != "$expected" ]]; then
    echo "Deterministic fault artifact directory resolved unexpectedly: $path" >&2
    exit 1
  fi
}

assert_plain_directory_or_missing "$ARTIFACT_ROOT"
if [[ ! -d "$ARTIFACT_ROOT" ]]; then
  mkdir -- "$ARTIFACT_ROOT"
fi
assert_physical_directory "$ARTIFACT_ROOT" "$ROOT_DIR/artifacts"

assert_plain_directory_or_missing "$ARTIFACT_PARENT"
if [[ ! -d "$ARTIFACT_PARENT" ]]; then
  mkdir -- "$ARTIFACT_PARENT"
fi
assert_physical_directory "$ARTIFACT_PARENT" "$ROOT_DIR/artifacts/qa-lab"

assert_plain_directory_or_missing "$ARTIFACT_ABSOLUTE"
# Campaign checkpoints are durable by design, so a smoke rerun must remove the prior campaign.
rm -rf -- "$ARTIFACT_ABSOLUTE"
mkdir -- "$ARTIFACT_ABSOLUTE"
assert_physical_directory "$ARTIFACT_ABSOLUTE" "$ROOT_DIR/artifacts/qa-lab/deterministic-fault-smoke"
printf '%s\n' 'status=started' >"$ARTIFACT_ABSOLUTE/status.txt"

"$CARGO_BIN" test -p palyra-common --lib qa_fault --locked
bash "$ROOT_DIR/scripts/test/run-continuity-campaign-gate.sh"
"$CARGO_BIN" test -p palyra-cli --lib failure_diagnostics_are_persistable_before_state_root_removal_without_secrets --locked
"$CARGO_BIN" test -p palyra-daemon --lib qa_fault_injection::tests --locked
"$CARGO_BIN" test -p palyra-daemon --lib tools_list_changed_notification_refreshes_future_calls_without_rewriting_in_flight_call --locked
"$CARGO_BIN" test -p palyra-daemon --lib cancel_racing_blocked_final_delivery_produces_one_done_terminal_outcome --locked
"$CARGO_BIN" test -p palyra-daemon --lib qa_fault --features qa-fault-injection --locked
"$CARGO_BIN" test -p palyra-daemon --lib fixture_provider_fault_adapter_runs_through_the_real_provider_path --features qa-fault-injection --locked
"$CARGO_BIN" test -p palyra-daemon --lib managed_process_post_spawn_fault_verifies_cleanup_before_exit --features qa-fault-injection --locked
"$CARGO_BIN" test -p palyra-daemon --lib terminate_cleanup_fault_records_recovery_before_process_exit --features qa-fault-injection --locked
"$CARGO_BIN" test -p palyra-daemon --lib docker_runner_faults_only_after_verified_cleanup_and_records_recovery --features qa-fault-injection --locked
"$CARGO_BIN" test -p palyra-connectors --lib --features qa-fault-injection --locked
"$CARGO_BIN" test -p palyra-workerd --lib --features qa-fault-injection --locked
"$CARGO_BIN" build -p palyra-daemon --bin palyrad --features qa-fault-injection --locked

PALYRAD_BIN="$ROOT_DIR/target/debug/palyrad"
if [[ -f "${PALYRAD_BIN}.exe" ]]; then
  PALYRAD_BIN="${PALYRAD_BIN}.exe"
fi
if command -v cygpath >/dev/null 2>&1; then
  PALYRAD_BIN="$(cygpath -w "$PALYRAD_BIN")"
fi

PALYRA_QA_PALYRAD_BIN="$PALYRAD_BIN" \
  "$CARGO_BIN" run -p palyra-cli --locked -- qa gate \
  --suite qa/suites/fault_smoke.yaml \
  --output-json "$ARTIFACT_RELATIVE/fault-smoke.json" \
  --output-markdown "$ARTIFACT_RELATIVE/fault-smoke.md" \
  --json
printf '%s\n' 'status=passed' >"$ARTIFACT_ABSOLUTE/status.txt"
