#!/usr/bin/env bash
set -euo pipefail

if ! command -v npm >/dev/null 2>&1; then
  echo "Web lint skipped: npm is not installed."
  exit 0
fi

if [[ ! -d "apps/web/node_modules" ]]; then
  echo "Web lint skipped: install dependencies first (cd apps/web && npm install)."
  exit 0
fi

npm --prefix apps/web run lint
