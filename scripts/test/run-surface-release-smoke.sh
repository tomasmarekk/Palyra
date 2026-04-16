#!/usr/bin/env bash
set -euo pipefail

if ! command -v pwsh >/dev/null 2>&1; then
  echo "pwsh is required for surface release smoke validation." >&2
  exit 1
fi

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
pwsh -NoLogo -File "$repo_root/scripts/test/run-surface-release-smoke.ps1"
