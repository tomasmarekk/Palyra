#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"

exec bash "$ROOT_DIR/scripts/test/run-deterministic-fault-smoke.sh"
