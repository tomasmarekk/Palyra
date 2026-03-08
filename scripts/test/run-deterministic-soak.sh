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

  echo "cargo is required for deterministic soak checks." >&2
  exit 1
}

cd "$ROOT_DIR"

CARGO_BIN="$(resolve_cargo)"

"$CARGO_BIN" test -p palyra-connectors --lib --locked gateway_envelope_reconnect_resume_cycles_remain_stable_under_soak
"$CARGO_BIN" test -p palyra-connectors --lib --locked repeated_dead_letter_recovery_cycles_keep_queue_accounting_stable
