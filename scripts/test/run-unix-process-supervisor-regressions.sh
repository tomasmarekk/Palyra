#!/usr/bin/env bash
set -euo pipefail

# Runs the process-supervisor regressions whose signal and process-group semantics are POSIX-only.
# Unsupported hosts fail explicitly so they cannot report false cross-platform coverage.
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"

case "$(uname -s)" in
  Linux|Darwin) ;;
  *)
    echo "Unix process supervisor regressions require Linux or macOS; this host does not validate POSIX behavior." >&2
    exit 1
    ;;
esac

resolve_tool() {
  local name="$1"
  if command -v "$name" >/dev/null 2>&1; then
    command -v "$name"
    return 0
  fi
  echo "$name is required for Unix process supervisor regressions." >&2
  exit 1
}

cd "$ROOT_DIR"
CARGO_BIN="$(resolve_tool cargo)"
RUSTC_BIN="$(resolve_tool rustc)"

if ! "$RUSTC_BIN" --print cfg | awk '$0 == "unix" { found = 1 } END { exit(found ? 0 : 1) }'; then
  echo "The active Rust target does not expose cfg(unix); refusing an empty supervisor test pass." >&2
  exit 1
fi

readonly PREFIX="unix_process_supervisor::tests::unix_process_supervisor_regression_"
readonly EXPECTED_TESTS=(
  "${PREFIX}separates_supervisor_and_target_sessions_and_groups"
  "${PREFIX}pre_start_terminate_acknowledges_without_target_exec"
  "${PREFIX}helper_is_supervisor_owned_armed_for_target_and_reaped_before_acknowledgement"
  "${PREFIX}terminate_kills_target_group_descendants"
  "${PREFIX}natural_leader_exit_cleans_live_descendant_and_preserves_exit_status"
  "${PREFIX}post_start_control_eof_fails_closed_and_cleans_target"
  "${PREFIX}first_cleanup_failure_remains_retryable_and_second_terminate_completes"
  "${PREFIX}natural_exit_retry_success_is_acknowledged"
)

TEST_LIST="$("$CARGO_BIN" test -p palyra-daemon --lib --locked -- --list)"
for test_name in "${EXPECTED_TESTS[@]}"; do
  count="$(printf '%s\n' "$TEST_LIST" | awk -v expected="$test_name: test" '$0 == expected { count += 1 } END { print count + 0 }')"
  if [[ "$count" != "1" ]]; then
    echo "Expected exactly one discovered supervisor regression named '$test_name'; found $count." >&2
    exit 1
  fi
done

prefix_count="$(printf '%s\n' "$TEST_LIST" | awk -v prefix="$PREFIX" 'index($0, prefix) == 1 && $0 ~ /: test$/ { count += 1 } END { print count + 0 }')"
if [[ "$prefix_count" != "${#EXPECTED_TESTS[@]}" ]]; then
  echo "Expected ${#EXPECTED_TESTS[@]} supervisor regressions; discovered $prefix_count." >&2
  exit 1
fi

"$CARGO_BIN" test \
  -p palyra-daemon \
  --lib \
  --locked \
  unix_process_supervisor_regression_ \
  -- \
  --test-threads=1
