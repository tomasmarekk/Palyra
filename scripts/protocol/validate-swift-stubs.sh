#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
SWIFT_STUB="$ROOT_DIR/schemas/generated/swift/ProtocolStubs.swift"

if ! command -v swiftc >/dev/null 2>&1; then
  echo "swiftc is required to validate generated Swift stubs." >&2
  exit 1
fi

if [ ! -f "$SWIFT_STUB" ]; then
  echo "Missing generated Swift stub file: $SWIFT_STUB" >&2
  exit 1
fi

tmp_dir="$(mktemp -d)"
trap 'rm -rf "$tmp_dir"' EXIT

swiftc -emit-module -module-name PalyraProtocolStubs "$SWIFT_STUB" -o "$tmp_dir/PalyraProtocolStubs.swiftmodule"
echo "Swift protocol stubs compile validation passed."
