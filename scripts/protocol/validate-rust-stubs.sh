#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
RUST_STUB="$ROOT_DIR/schemas/generated/rust/protocol_stubs.rs"

to_windows_path() {
  local path="$1"
  if [[ "$path" =~ ^/mnt/([a-zA-Z])/(.*)$ ]]; then
    local drive_letter="${BASH_REMATCH[1]}"
    local rest_path="${BASH_REMATCH[2]}"
    rest_path="${rest_path//\//\\}"
    printf "%s:\\%s" "${drive_letter^^}" "$rest_path"
    return
  fi
  printf "%s" "$path"
}

if command -v rustc >/dev/null 2>&1; then
  rustc_bin="rustc"
elif command -v rustc.exe >/dev/null 2>&1; then
  rustc_bin="rustc.exe"
else
  echo "rustc is required to validate generated Rust stubs." >&2
  exit 1
fi

if [ ! -f "$RUST_STUB" ]; then
  echo "Missing generated Rust stub file: $RUST_STUB" >&2
  exit 1
fi

tmp_dir="$(mktemp -d "$ROOT_DIR/.tmp-rust-stubs.XXXXXX")"
trap 'rm -rf "$tmp_dir"' EXIT

if [[ "$rustc_bin" == *.exe ]]; then
  "$rustc_bin" \
    --edition=2021 \
    --crate-name palyra_protocol_stubs \
    --crate-type lib \
    "$(to_windows_path "$RUST_STUB")" \
    -o "$(to_windows_path "$tmp_dir/libpalyra_protocol_stubs.rlib")"
else
  "$rustc_bin" \
    --edition=2021 \
    --crate-name palyra_protocol_stubs \
    --crate-type lib \
    "$RUST_STUB" \
    -o "$tmp_dir/libpalyra_protocol_stubs.rlib"
fi

echo "Rust protocol stubs compile validation passed."
