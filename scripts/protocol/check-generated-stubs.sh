#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

bash "$ROOT_DIR/scripts/protocol/generate-stubs.sh"

if git -C "$ROOT_DIR" rev-parse --is-inside-work-tree >/dev/null 2>&1; then
  if ! git -C "$ROOT_DIR" diff --quiet -- schemas/generated; then
    echo "Generated stubs are out of date. Re-run scripts/protocol/generate-stubs.sh and commit changes." >&2
    git -C "$ROOT_DIR" diff -- schemas/generated || true
    exit 1
  fi
fi

echo "Generated protocol stubs are up to date."
