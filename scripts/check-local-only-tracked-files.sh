#!/usr/bin/env bash
set -euo pipefail

if ! command -v git >/dev/null 2>&1; then
  echo "git is required for local-only tracked file checks" >&2
  exit 1
fi

tracked_files="$(git ls-files)"
if [[ -z "${tracked_files}" ]]; then
  echo "No tracked files found; skipping local-only path guard."
  exit 0
fi

local_only_matches="$(printf '%s\n' "${tracked_files}" | grep -E '^roadmap/' || true)"

if [[ -n "${local_only_matches//[[:space:]]/}" ]]; then
  echo "Tracked local-only paths detected. Remove these paths from git history and the index:"
  echo "${local_only_matches}"
  exit 1
fi

echo "Local-only tracked path guard passed."
