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
  local scope="${1:-generated during local validation}"
  echo "Cleaning runtime artifacts ${scope}..."
  bash "$ROOT_DIR/scripts/clean-runtime-artifacts.sh" >/dev/null
}

check_runtime_artifact_hygiene() {
  local label="$1"
  echo "$label"
  cleanup_runtime_artifacts "before runtime artifact hygiene check"
  bash "$ROOT_DIR/scripts/check-runtime-artifacts.sh"
}

run_js_workspace_checks() {
  echo "Ensuring JS workspace tooling..."
  bash "$ROOT_DIR/scripts/test/ensure-js-workspace.sh"

  echo "Running Vite+ check..."
  npm run js:check
}

changed_files_for_pre_push() {
  local upstream_ref
  local diff_base

  if upstream_ref="$(git -C "$ROOT_DIR" rev-parse --abbrev-ref --symbolic-full-name '@{u}' 2>/dev/null)" &&
    diff_base="$(git -C "$ROOT_DIR" merge-base HEAD "$upstream_ref" 2>/dev/null)"; then
    git -C "$ROOT_DIR" diff --name-only --diff-filter=ACMR "$diff_base" HEAD
  elif git -C "$ROOT_DIR" rev-parse --verify --quiet HEAD^ >/dev/null; then
    git -C "$ROOT_DIR" diff --name-only --diff-filter=ACMR HEAD^ HEAD
  fi

  git -C "$ROOT_DIR" diff --name-only --diff-filter=ACMR --cached
  git -C "$ROOT_DIR" diff --name-only --diff-filter=ACMR
}

rust_package_dir_for_path() {
  local changed_path="$1"
  local package_dir

  [[ "$changed_path" == crates/* ]] || return 1

  package_dir="$ROOT_DIR/${changed_path%/*}"
  while [[ "$package_dir" == "$ROOT_DIR"/crates* ]]; do
    if [[ -f "$package_dir/Cargo.toml" ]]; then
      printf '%s\n' "$package_dir"
      return 0
    fi
    package_dir="$(dirname "$package_dir")"
  done

  return 1
}

rust_package_name_from_dir() {
  local package_dir="$1"

  sed -n 's/^[[:space:]]*name[[:space:]]*=[[:space:]]*"\([^"]*\)".*/\1/p' "$package_dir/Cargo.toml" | head -n 1
}

changed_rust_package_names() {
  local changed_path
  local package_dir
  local package_name

  changed_files_for_pre_push | sort -u | while IFS= read -r changed_path; do
    [[ -n "$changed_path" ]] || continue
    if package_dir="$(rust_package_dir_for_path "$changed_path")"; then
      package_name="$(rust_package_name_from_dir "$package_dir")"
      if [[ -z "$package_name" ]]; then
        echo "Failed to resolve Rust package name from $package_dir/Cargo.toml." >&2
        exit 1
      fi
      printf '%s\n' "$package_name"
    fi
  done | sort -u
}

change_requires_workspace_rust_tests() {
  local changed_path

  while IFS= read -r changed_path; do
    case "$changed_path" in
      Cargo.toml | Cargo.lock | rust-toolchain.toml | .cargo/config.toml)
        return 0
        ;;
    esac
  done < <(changed_files_for_pre_push | sort -u)

  return 1
}

build_cli_integration_daemon() {
  echo "Building palyrad required by CLI integration tests..."
  "$CARGO_BIN" build -p palyra-daemon --bin palyrad --locked
}

run_changed_rust_package_tests() {
  local package_names
  local package_name

  if change_requires_workspace_rust_tests; then
    echo "Running Rust workspace tests because workspace-level Rust inputs changed..."
    build_cli_integration_daemon
    "$CARGO_BIN" test --workspace --locked
    return
  fi

  package_names="$(changed_rust_package_names)"
  if [[ -z "$package_names" ]]; then
    echo "No changed Rust workspace packages detected; skipping Rust delta tests."
    return
  fi

  echo "Running Rust delta tests for changed workspace packages..."
  while IFS= read -r package_name; do
    [[ -n "$package_name" ]] || continue
    if [[ "$package_name" == "palyra-cli" ]]; then
      build_cli_integration_daemon
    fi
    echo "Running cargo test -p $package_name --locked..."
    "$CARGO_BIN" test -p "$package_name" --locked
  done <<<"$package_names"
}

run_fast_profile() {
  run_js_workspace_checks

  echo "Running rustfmt check..."
  "$CARGO_BIN" fmt --all --check

  echo "Running clippy..."
  "$CARGO_BIN" clippy --workspace --all-targets -- -D warnings

  run_changed_rust_package_tests

  check_runtime_artifact_hygiene "Checking runtime artifact hygiene before local validation..."

  echo "Checking local-only tracked paths..."
  bash "$ROOT_DIR/scripts/check-local-only-tracked-files.sh"

  echo "Running English source scan..."
  bash "$ROOT_DIR/scripts/check-english-source.sh"

  echo "Running module budget and connector boundary ratchet..."
  bash "$ROOT_DIR/scripts/dev/report-module-budgets.sh" --strict

  echo "Checking desktop glib patch governance..."
  bash "$ROOT_DIR/scripts/check-desktop-glib-patch.sh"

  echo "Running deterministic pre-push smoke suite..."
  bash "$ROOT_DIR/scripts/test/run-deterministic-core.sh"

  echo "Running high-risk pattern scan..."
  bash "$ROOT_DIR/scripts/check-high-risk-patterns.sh"
}

run_full_profile() {
  run_js_workspace_checks

  echo "Running rustfmt check..."
  "$CARGO_BIN" fmt --all --check

  check_runtime_artifact_hygiene "Checking runtime artifact hygiene before local validation..."

  echo "Checking local-only tracked paths..."
  bash "$ROOT_DIR/scripts/check-local-only-tracked-files.sh"

  echo "Running English source scan..."
  bash "$ROOT_DIR/scripts/check-english-source.sh"

  echo "Running module budget and connector boundary ratchet..."
  bash "$ROOT_DIR/scripts/dev/report-module-budgets.sh" --strict

  echo "Checking desktop glib patch governance..."
  bash "$ROOT_DIR/scripts/check-desktop-glib-patch.sh"

  echo "Running clippy..."
  "$CARGO_BIN" clippy --workspace --all-targets -- -D warnings

  echo "Running unit and integration tests..."
  build_cli_integration_daemon
  "$CARGO_BIN" test --workspace --locked

  echo "Running workflow regression matrix..."
  bash "$ROOT_DIR/scripts/test/run-workflow-regression.sh"

  echo "Running protocol schema checks..."
  bash "$ROOT_DIR/scripts/protocol/validate-proto.sh"
  bash "$ROOT_DIR/scripts/protocol/generate-stubs.sh"
  bash "$ROOT_DIR/scripts/protocol/validate-rust-stubs.sh"

  echo "Running high-risk pattern scan..."
  bash "$ROOT_DIR/scripts/check-high-risk-patterns.sh"
}

trap cleanup_runtime_artifacts EXIT
cleanup_runtime_artifacts "from prior local validation runs"

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

check_runtime_artifact_hygiene "Running runtime artifact hygiene guard..."
