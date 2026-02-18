#!/usr/bin/env bash
set -euo pipefail

if ! command -v git >/dev/null 2>&1; then
  echo "git is required for vendored artifact checks" >&2
  exit 1
fi

tracked_files="$(git ls-files)"
if [[ -z "${tracked_files}" ]]; then
  echo "No tracked files found; skipping vendored artifact check."
  exit 0
fi

forbidden_regex='(^|/)(node_modules|bower_components|\.pnpm-store|vendor)/(.*)?$'

if command -v rg >/dev/null 2>&1; then
  matches="$(printf '%s\n' "${tracked_files}" | rg --line-number --color never "${forbidden_regex}" || true)"
else
  matches="$(printf '%s\n' "${tracked_files}" | grep -En "${forbidden_regex}" || true)"
fi

if [[ -n "${matches//[[:space:]]/}" ]]; then
  echo "Tracked vendored artifacts detected. Remove these paths from git:"
  echo "${matches}"
  exit 1
fi

echo "Vendored artifact guard passed."
