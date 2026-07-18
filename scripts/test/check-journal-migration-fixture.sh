#!/usr/bin/env bash
set -euo pipefail

# Validates the immutable pre-V2 journal snapshot before exercising its upgrade path.
# The manifest check prevents a silently rewritten baseline from qualifying a migration.
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
FIXTURE_DIR="$ROOT_DIR/fixtures/golden/journal_migrations"
FIXTURE_NAME="pre_v2_v44.sql"
FIXTURE_PATH="$FIXTURE_DIR/$FIXTURE_NAME"
MANIFEST_PATH="$FIXTURE_DIR/pre_v2_v44.sha256"

resolve_cargo() {
  if command -v cargo >/dev/null 2>&1; then
    command -v cargo
    return 0
  fi
  if command -v cargo.exe >/dev/null 2>&1; then
    command -v cargo.exe
    return 0
  fi
  echo "cargo is required for journal migration fixture validation." >&2
  exit 1
}

sha256_file() {
  local path="$1"
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$path" | awk '{print tolower($1)}'
    return 0
  fi
  if command -v shasum >/dev/null 2>&1; then
    shasum -a 256 "$path" | awk '{print tolower($1)}'
    return 0
  fi
  if command -v openssl >/dev/null 2>&1; then
    openssl dgst -sha256 "$path" | awk '{print tolower($NF)}'
    return 0
  fi
  echo "a SHA256 tool is required for journal migration fixture validation." >&2
  exit 1
}

if [[ ! -f "$FIXTURE_PATH" || -L "$FIXTURE_PATH" ]]; then
  echo "journal migration fixture must be a regular non-symlink file: $FIXTURE_PATH" >&2
  exit 1
fi
if [[ ! -f "$MANIFEST_PATH" || -L "$MANIFEST_PATH" ]]; then
  echo "journal migration fixture manifest must be a regular non-symlink file: $MANIFEST_PATH" >&2
  exit 1
fi

manifest_line="$(tr -d '\r' <"$MANIFEST_PATH")"
if [[ ! "$manifest_line" =~ ^([0-9a-f]{64})[[:space:]][[:space:]]([^[:space:]]+)$ ]]; then
  echo "journal migration fixture manifest has an invalid sha256sum shape." >&2
  exit 1
fi
expected_sha256="${BASH_REMATCH[1]}"
manifest_fixture_name="${BASH_REMATCH[2]}"
if [[ "$manifest_fixture_name" != "$FIXTURE_NAME" ]]; then
  echo "journal migration fixture manifest names an unexpected file: $manifest_fixture_name" >&2
  exit 1
fi

actual_sha256="$(sha256_file "$FIXTURE_PATH")"
if [[ "$actual_sha256" != "$expected_sha256" ]]; then
  echo "journal migration fixture hash mismatch." >&2
  echo "Expected: $expected_sha256" >&2
  echo "Actual:   $actual_sha256" >&2
  exit 1
fi

cd "$ROOT_DIR"
CARGO_BIN="$(resolve_cargo)"
"$CARGO_BIN" test \
  -p palyra-daemon \
  journal::tests::pre_v2_golden_migration_acceptance \
  --locked \
  -- \
  --exact

echo "journal migration fixture validation passed."
