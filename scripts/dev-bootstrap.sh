#!/usr/bin/env bash
set -euo pipefail

if command -v just >/dev/null 2>&1; then
  just dev
  exit 0
fi

echo "just is not installed; running fallback bootstrap sequence."
cargo run -p palyra-cli --bin palyra -- doctor --strict
cargo build --workspace --locked
