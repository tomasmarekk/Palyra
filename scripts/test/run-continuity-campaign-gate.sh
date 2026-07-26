#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"
cd "$ROOT_DIR"

resolve_cargo() {
  if command -v cargo >/dev/null 2>&1; then
    command -v cargo
    return 0
  fi
  if command -v cargo.exe >/dev/null 2>&1; then
    command -v cargo.exe
    return 0
  fi
  echo "cargo is required for the continuity campaign gate." >&2
  exit 1
}

CARGO_BIN="$(resolve_cargo)"

"$CARGO_BIN" test -p palyra-common --test continuity_campaign_contract --locked
"$CARGO_BIN" test -p palyra-daemon --lib recovery_barrier_rejects_input_until_ready --locked
"$CARGO_BIN" test -p palyra-daemon --lib startup_recovery_ --locked
"$CARGO_BIN" test -p palyra-daemon --lib stuck_run_remediation::tests --locked
"$CARGO_BIN" test -p palyra-daemon --lib \
  persisted_session_pin_survives_restart_config_change_and_key_rotation --locked
"$CARGO_BIN" test -p palyra-daemon --lib \
  corrupt_flow_dependencies_fail_closed_after_runtime_restart --locked
"$CARGO_BIN" test -p palyra-daemon --lib \
  persisted_live_process_identity_mismatch_fails_closed --locked
"$CARGO_BIN" run -p palyra-cli --example run_continuity_campaign_gate --locked
