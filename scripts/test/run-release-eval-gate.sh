#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
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

  echo "cargo is required for release eval gate checks." >&2
  exit 1
}

CARGO_BIN="$(resolve_cargo)"

bash "$ROOT_DIR/scripts/test/run-continuity-campaign-gate.sh"
"$CARGO_BIN" test -p palyra-common --test release_eval_contract --locked
"$CARGO_BIN" run -p palyra-cli --example run_release_eval_gate --locked -- \
  --manifest fixtures/golden/release_eval_inventory.json \
  --report-dir target/release-artifacts/release-evals

"$CARGO_BIN" build -p palyra-daemon --bin palyrad --locked
if [[ -z "${PALYRA_QA_PALYRAD_BIN:-}" ]]; then
  if [[ -x "$ROOT_DIR/target/debug/palyrad.exe" ]]; then
    PALYRA_QA_PALYRAD_BIN="$ROOT_DIR/target/debug/palyrad.exe"
  else
    PALYRA_QA_PALYRAD_BIN="$ROOT_DIR/target/debug/palyrad"
  fi
fi
PALYRA_QA_PALYRAD_BIN="$PALYRA_QA_PALYRAD_BIN" \
  "$CARGO_BIN" run -p palyra-cli --locked -- qa gate \
  --suite qa/suites/runtime_kernel_v2_shadow.yaml \
  --output-json target/qa-lab/runtime-kernel-v2-shadow/report.json \
  --output-markdown target/qa-lab/runtime-kernel-v2-shadow/report.md \
  --json
PALYRA_QA_PALYRAD_BIN="$PALYRA_QA_PALYRAD_BIN" \
  "$CARGO_BIN" run -p palyra-cli --locked -- qa gate \
  --suite qa/suites/runtime_kernel_v2_authoritative.yaml \
  --output-json target/qa-lab/runtime-kernel-v2-authoritative/report.json \
  --output-markdown target/qa-lab/runtime-kernel-v2-authoritative/report.md \
  --json
