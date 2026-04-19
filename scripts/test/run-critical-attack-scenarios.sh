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

  local candidates=(
    "${HOME:-}/.cargo/bin/cargo"
    "${HOME:-}/.cargo/bin/cargo.exe"
    "${USERPROFILE:-}/.cargo/bin/cargo.exe"
  )
  local candidate
  for candidate in "${candidates[@]}"; do
    if [[ -n "$candidate" && -x "$candidate" ]]; then
      printf '%s\n' "$candidate"
      return 0
    fi
  done

  echo "cargo is required for critical attack scenarios." >&2
  exit 1
}

cd "$ROOT_DIR"

CARGO_BIN="$(resolve_cargo)"

"$CARGO_BIN" test -p palyra-safety --test critical_attack_scenarios --locked
"$CARGO_BIN" test -p palyra-egress-proxy --test critical_attack_scenarios --locked
"$CARGO_BIN" test -p palyra-workerd --test critical_attack_scenarios --locked
"$CARGO_BIN" test -p palyra-daemon --lib --locked webhook_test_integration_surfaces_safety_blocking
"$CARGO_BIN" test -p palyra-daemon --test gateway_grpc --locked grpc_run_stream_policy_decision_journal_includes_execution_gate_report_when_rollout_enabled
"$CARGO_BIN" test -p palyra-daemon --test gateway_grpc --locked grpc_route_message_pending_approval_records_execution_gate_report_when_rollout_enabled
