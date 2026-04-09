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

  echo "cargo is required for workflow regression checks." >&2
  exit 1
}

cd "$ROOT_DIR"

if [[ "${OS:-}" == "Windows_NT" ]] && command -v pwsh.exe >/dev/null 2>&1; then
  pwsh.exe -NoLogo -File "$ROOT_DIR/scripts/test/run-workflow-regression.ps1"
  exit $?
fi

CARGO_BIN="$(resolve_cargo)"

"$CARGO_BIN" build -p palyra-daemon -p palyra-cli --locked
"$CARGO_BIN" build -p palyra-browserd --bin palyra-browserd --locked

"$CARGO_BIN" test -p palyra-daemon --lib --locked compat::tests
"$CARGO_BIN" test -p palyra-daemon --lib --locked session_compaction_apply_persists_durable_writes_and_quality_gates
"$CARGO_BIN" test -p palyra-daemon --lib --locked session_compaction_apply_rolls_back_workspace_writes_on_partial_failure
"$CARGO_BIN" test -p palyra-cli --test wizard_cli --locked -- --test-threads=1
"$CARGO_BIN" test -p palyra-cli --test cli_v1_acp_shim --locked -- --test-threads=1
"$CARGO_BIN" test -p palyra-cli --test workflow_regression_matrix --locked -- --test-threads=1
