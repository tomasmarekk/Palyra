#!/usr/bin/env bash
set -euo pipefail

if ! command -v npm >/dev/null 2>&1; then
  echo "Web lint failed: npm is not installed." >&2
  exit 1
fi

if [[ ! -d "apps/web/node_modules" ]]; then
  echo "Web lint failed: run 'npm --prefix apps/web run bootstrap' first." >&2
  exit 1
fi

npm --prefix apps/web run verify-install
npm --prefix apps/web run lint
