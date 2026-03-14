#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

cd "$ROOT_DIR"

if [[ ! -d "$ROOT_DIR/apps/web/node_modules" ]]; then
  npm --prefix apps/web run bootstrap
else
  npm --prefix apps/web run verify-install
fi

npm --prefix apps/web run build
