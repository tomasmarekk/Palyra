#!/usr/bin/env bash
set -euo pipefail

repo_root="$(git rev-parse --show-toplevel 2>/dev/null || pwd)"
cd "${repo_root}"

prune_dirs=(
  "./.git"
  "./node_modules"
  "./target"
  "./apps/web/dist"
  "./apps/web/coverage"
  "./apps/web/.vite"
  "./apps/desktop/ui/dist"
  "./apps/desktop/ui/.vite"
  "./security-artifacts"
)

allowed_paths=(
)

mapfile -t candidates < <(
  find . \
    \( \
      -path "./.git" \
      -o -path "./node_modules" \
      -o -path "./target" \
      -o -path "./apps/web/dist" \
      -o -path "./apps/web/coverage" \
      -o -path "./apps/web/.vite" \
      -o -path "./apps/desktop/ui/dist" \
      -o -path "./apps/desktop/ui/.vite" \
      -o -path "./security-artifacts" \
    \) -prune -o \
    -type f \
    \( \
      -iname "*.sqlite" \
      -o -iname "*.sqlite3" \
      -o -iname "*.sqlite3-*" \
      -o -iname "*.db" \
      -o -iname "*.db-*" \
      -o -iname "*.wal" \
      -o -iname "*.shm" \
      -o -iname "*.log" \
      -o -iname "support-bundle*.json" \
      -o -path "*/browser-profile/*" \
      -o -path "*/browser-profiles/*" \
      -o -path "*/downloads/*" \
    \) -print | sed 's#^\./##'
)

matches=()
for candidate in "${candidates[@]}"; do
  [[ -z "${candidate}" ]] && continue

  allowlisted=0
  for allowed_path in "${allowed_paths[@]}"; do
    if [[ "${candidate}" == "${allowed_path}" ]]; then
      allowlisted=1
      break
    fi
  done

  if [[ "${allowlisted}" -eq 0 ]]; then
    matches+=("${candidate}")
  fi
done

# Inspect the commit independently of the working tree so a local deletion
# cannot hide an artifact that the push would still publish.
if git rev-parse --verify --quiet HEAD >/dev/null; then
  while IFS= read -r -d '' candidate; do
    lower_candidate="${candidate,,}"
    case "${lower_candidate}" in
      node_modules/* | target/* | apps/web/dist/* | apps/web/coverage/* | apps/web/.vite/* | apps/desktop/ui/dist/* | apps/desktop/ui/.vite/* | security-artifacts/*)
        continue
        ;;
      *.sqlite | *.sqlite3 | *.sqlite3-* | *.db | *.db-* | *.wal | *.shm | *.log | support-bundle*.json | */browser-profile/* | */browser-profiles/* | */downloads/*)
        ;;
      *)
        continue
        ;;
    esac

    allowlisted=0
    for allowed_path in "${allowed_paths[@]}"; do
      if [[ "${candidate}" == "${allowed_path}" ]]; then
        allowlisted=1
        break
      fi
    done
    already_matched=0
    for matched_path in "${matches[@]}"; do
      if [[ "${candidate}" == "${matched_path}" ]]; then
        already_matched=1
        break
      fi
    done
    if [[ "${allowlisted}" -eq 0 && "${already_matched}" -eq 0 ]]; then
      matches+=("${candidate}")
    fi
  done < <(git ls-tree -rz --name-only HEAD)
fi

if [[ "${#matches[@]}" -gt 0 ]]; then
  echo "Runtime/package artifacts detected in the working tree or HEAD commit. Remove them or move them under an explicit fixture allowlist before packaging/handoff:" >&2
  printf ' - %s\n' "${matches[@]}" >&2
  exit 1
fi

echo "Runtime artifact hygiene guard passed."
