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
generated_sbom_regex='(^|/)sbom[^/]*\.json$'
allowlisted_generated_files=(
  "crates/palyra-skills/examples/echo-http/sbom.cdx.json"
)

find_tracked_matches() {
  local regex="$1"
  if command -v rg >/dev/null 2>&1; then
    printf '%s\n' "${tracked_files}" | rg --color never "${regex}" || true
  else
    printf '%s\n' "${tracked_files}" | grep -E "${regex}" || true
  fi
}

vendored_matches="$(find_tracked_matches "${forbidden_regex}")"

if [[ -n "${vendored_matches//[[:space:]]/}" ]]; then
  echo "Tracked vendored artifacts detected. Remove these paths from git:"
  echo "${vendored_matches}"
  exit 1
fi

generated_sbom_matches="$(find_tracked_matches "${generated_sbom_regex}")"
disallowed_generated_matches=""
while IFS= read -r candidate; do
  [[ -z "${candidate}" ]] && continue

  allowlisted=0
  for allowlisted_file in "${allowlisted_generated_files[@]}"; do
    if [[ "${candidate}" == "${allowlisted_file}" ]]; then
      allowlisted=1
      break
    fi
  done

  if [[ "${allowlisted}" -eq 0 ]]; then
    disallowed_generated_matches+="${candidate}"$'\n'
  fi
done <<< "${generated_sbom_matches}"

if [[ -n "${disallowed_generated_matches//[[:space:]]/}" ]]; then
  echo "Tracked generated SBOM artifacts detected. Keep SBOM outputs untracked and use CI artifacts instead:"
  echo "${disallowed_generated_matches}"
  exit 1
fi

echo "Vendored/generated artifact guard passed."
