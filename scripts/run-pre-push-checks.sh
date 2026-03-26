#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PROFILE="${PALYRA_PRE_PUSH_PROFILE:-fast}"

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

  echo "cargo is required for pre-push checks." >&2
  exit 1
}

CARGO_BIN="$(resolve_cargo)"

cleanup_runtime_artifacts() {
  echo "Cleaning runtime artifacts generated during local validation..."
  bash "$ROOT_DIR/scripts/clean-runtime-artifacts.sh" >/dev/null
}

run_fast_profile() {
  echo "Running rustfmt check..."
  "$CARGO_BIN" fmt --all --check

  echo "Checking runtime artifact hygiene before local validation..."
  bash "$ROOT_DIR/scripts/check-runtime-artifacts.sh"

  echo "Checking local-only tracked paths..."
  bash "$ROOT_DIR/scripts/check-local-only-tracked-files.sh"

  echo "Checking desktop glib patch governance..."
  bash "$ROOT_DIR/scripts/check-desktop-glib-patch.sh"

  echo "Running deterministic pre-push smoke suite..."
  bash "$ROOT_DIR/scripts/test/run-deterministic-core.sh"

  echo "Running high-risk pattern scan..."
  bash "$ROOT_DIR/scripts/check-high-risk-patterns.sh"
}

run_full_profile() {
  echo "Running rustfmt check..."
  "$CARGO_BIN" fmt --all --check

  echo "Checking runtime artifact hygiene before local validation..."
  bash "$ROOT_DIR/scripts/check-runtime-artifacts.sh"

  echo "Checking local-only tracked paths..."
  bash "$ROOT_DIR/scripts/check-local-only-tracked-files.sh"

  echo "Checking desktop glib patch governance..."
  bash "$ROOT_DIR/scripts/check-desktop-glib-patch.sh"

  echo "Running clippy..."
  "$CARGO_BIN" clippy --workspace --all-targets -- -D warnings

  echo "Running unit and integration tests..."
  "$CARGO_BIN" test --workspace --locked

  echo "Running protocol schema checks..."
  bash "$ROOT_DIR/scripts/protocol/validate-proto.sh"
  bash "$ROOT_DIR/scripts/protocol/generate-stubs.sh"
  bash "$ROOT_DIR/scripts/protocol/validate-rust-stubs.sh"

  echo "Running high-risk pattern scan..."
  bash "$ROOT_DIR/scripts/check-high-risk-patterns.sh"
}

trap cleanup_runtime_artifacts EXIT

case "$PROFILE" in
  fast)
    echo "Using pre-push profile: fast"
    run_fast_profile
    ;;
  full)
    echo "Using pre-push profile: full"
    run_full_profile
    ;;
  *)
    echo "Unsupported PALYRA_PRE_PUSH_PROFILE '$PROFILE'. Expected 'fast' or 'full'." >&2
    exit 1
    ;;
esac

echo "Running runtime artifact hygiene guard..."
bash "$ROOT_DIR/scripts/check-runtime-artifacts.sh"
