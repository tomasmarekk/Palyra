#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
FIXTURE_PATH="crates/palyra-connectors/tests/fixtures/channel_simulator_expected.json"
FIXTURE_ABS_PATH="$ROOT_DIR/$FIXTURE_PATH"

before_hash=""
if [[ -f "$FIXTURE_ABS_PATH" ]]; then
  before_hash="$(sha256sum "$FIXTURE_ABS_PATH" | awk '{print $1}')"
fi

bash "$ROOT_DIR/scripts/test/update-deterministic-fixtures.sh"

after_hash=""
if [[ -f "$FIXTURE_ABS_PATH" ]]; then
  after_hash="$(sha256sum "$FIXTURE_ABS_PATH" | awk '{print $1}')"
fi

if [[ "$before_hash" != "$after_hash" ]]; then
  if git -C "$ROOT_DIR" rev-parse --is-inside-work-tree >/dev/null 2>&1; then
    git -C "$ROOT_DIR" diff -- "$FIXTURE_PATH" || true
  fi
  echo "Deterministic fixtures changed during the check. Re-run scripts/test/update-deterministic-fixtures.sh and commit the updated fixture." >&2
  exit 1
fi

echo "Deterministic fixtures are up to date."
