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
PROFILE="${PALYRA_WORKFLOW_REGRESSION_PROFILE:-fast}"
REPORT_DIR="${PALYRA_WORKFLOW_REGRESSION_REPORT_DIR:-$ROOT_DIR/target/release-artifacts/workflow-regression/$PROFILE}"

export PALYRA_WORKFLOW_REGRESSION_CARGO_BIN="$CARGO_BIN"

"$CARGO_BIN" run -p palyra-cli --example run_workflow_regression --locked -- \
  --profile "$PROFILE" \
  --report-dir "$REPORT_DIR"
