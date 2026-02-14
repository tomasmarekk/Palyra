#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
KOTLIN_STUB="$ROOT_DIR/schemas/generated/kotlin/ProtocolStubs.kt"

if ! command -v kotlinc >/dev/null 2>&1; then
  echo "kotlinc is required to validate generated Kotlin stubs." >&2
  exit 1
fi

if [ ! -f "$KOTLIN_STUB" ]; then
  echo "Missing generated Kotlin stub file: $KOTLIN_STUB" >&2
  exit 1
fi

tmp_dir="$(mktemp -d)"
trap 'rm -rf "$tmp_dir"' EXIT

kotlinc "$KOTLIN_STUB" -d "$tmp_dir/palyra-protocol-stubs.jar"
echo "Kotlin protocol stubs compile validation passed."
