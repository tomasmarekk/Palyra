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

  echo "cargo is required for replay gate checks." >&2
  exit 1
}

CARGO_BIN="$(resolve_cargo)"
ARTIFACT_BASE_RELATIVE="artifacts/qa-lab/baseline-replay"
ARTIFACT_ROOT="$ROOT_DIR/artifacts"
ARTIFACT_PARENT="$ARTIFACT_ROOT/qa-lab"
ARTIFACT_BASE_ABSOLUTE="$ARTIFACT_PARENT/baseline-replay"

is_mount_point() {
  local path="$1"
  local physical mount_root device parent_device
  [[ -d "$path" ]] || return 1
  physical="$(cd "$path" && pwd -P)" || return 1
  if command -v mountpoint >/dev/null 2>&1 && mountpoint -q -- "$physical"; then
    return 0
  fi
  if mount_root="$(stat -c '%m' -- "$physical" 2>/dev/null)"; then
    [[ "$mount_root" == "$physical" ]]
    return
  fi
  if device="$(stat -f '%d' "$physical" 2>/dev/null)" \
    && parent_device="$(stat -f '%d' "$physical/.." 2>/dev/null)"; then
    [[ "$device" != "$parent_device" ]]
    return
  fi
  return 1
}

assert_plain_directory_or_missing() {
  local path="$1"
  if [[ -L "$path" ]]; then
    echo "Refusing an indirect replay baseline artifact directory: $path" >&2
    exit 1
  fi
  if [[ -e "$path" && ! -d "$path" ]]; then
    echo "Replay baseline artifact path is not a directory: $path" >&2
    exit 1
  fi
}

assert_physical_directory() {
  local path="$1"
  local expected="$2"
  assert_plain_directory_or_missing "$path"
  if [[ ! -d "$path" ]]; then
    echo "Expected replay baseline artifact directory is missing: $path" >&2
    exit 1
  fi
  if is_mount_point "$path"; then
    echo "Refusing a replay baseline artifact directory that is a mountpoint: $path" >&2
    exit 1
  fi
  local physical
  physical="$(cd "$path" && pwd -P)"
  if [[ "$physical" != "$expected" ]]; then
    echo "Replay baseline artifact directory resolved unexpectedly: $path" >&2
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

assert_plain_directory_or_missing "$ARTIFACT_BASE_ABSOLUTE"
if [[ ! -d "$ARTIFACT_BASE_ABSOLUTE" ]]; then
  mkdir -- "$ARTIFACT_BASE_ABSOLUTE"
fi
assert_physical_directory "$ARTIFACT_BASE_ABSOLUTE" "$ROOT_DIR/$ARTIFACT_BASE_RELATIVE"

# A fresh leaf avoids recursive cleanup of an attacker-controlled or mounted
# prior artifact directory. CI may retain the parent as an aggregate artifact.
ARTIFACT_ABSOLUTE="$(mktemp -d "$ARTIFACT_BASE_ABSOLUTE/run.XXXXXXXX")"
case "$ARTIFACT_ABSOLUTE" in
  "$ARTIFACT_BASE_ABSOLUTE"/run.*) ;;
  *)
    echo "Replay baseline per-run artifact path escaped its parent: $ARTIFACT_ABSOLUTE" >&2
    exit 1
    ;;
esac
assert_physical_directory "$ARTIFACT_ABSOLUTE" "$ARTIFACT_ABSOLUTE"
ARTIFACT_RELATIVE="${ARTIFACT_ABSOLUTE#"$ROOT_DIR/"}"
printf '%s\n' 'status=started' >"$ARTIFACT_ABSOLUTE/status.txt"

record_replay_gate_failure() {
  local status=$?
  trap - EXIT
  if ((status != 0)); then
    printf '%s\n' 'status=failed' >"$ARTIFACT_ABSOLUTE/status.txt" || true
  fi
  exit "$status"
}
trap record_replay_gate_failure EXIT

"$CARGO_BIN" test -p palyra-common replay_bundle --locked
"$CARGO_BIN" test -p palyra-common --test release_eval_contract --locked
"$CARGO_BIN" run -p palyra-cli --example run_release_eval_gate --locked -- \
  --manifest fixtures/golden/release_eval_inventory.json \
  --report-dir target/release-artifacts/release-evals
"$CARGO_BIN" test -p palyra-cli support_bundle --locked
"$CARGO_BIN" test -p palyra-daemon replay_capture --locked
"$CARGO_BIN" build -p palyra-daemon --bin palyrad --locked

PALYRAD_BIN="$ROOT_DIR/target/debug/palyrad"
if [[ -f "${PALYRAD_BIN}.exe" ]]; then
  PALYRAD_BIN="${PALYRAD_BIN}.exe"
fi
if [[ ! -f "$PALYRAD_BIN" || -L "$PALYRAD_BIN" ]]; then
  echo "Replay baseline daemon binary is not a regular file: $PALYRAD_BIN" >&2
  exit 1
fi
if command -v cygpath >/dev/null 2>&1; then
  PALYRAD_BIN="$(cygpath -w "$PALYRAD_BIN")"
fi

PALYRA_QA_PALYRAD_BIN="$PALYRAD_BIN" \
  "$CARGO_BIN" run -p palyra-cli --locked -- qa gate \
  --suite qa/suites/baseline_replay.yaml \
  --output-json "$ARTIFACT_RELATIVE/baseline-replay.json" \
  --output-markdown "$ARTIFACT_RELATIVE/baseline-replay.md" \
  --json
printf '%s\n' 'status=passed' >"$ARTIFACT_ABSOLUTE/status.txt"
trap - EXIT
