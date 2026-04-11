#!/usr/bin/env bash
set -euo pipefail

repo_root="$(git rev-parse --show-toplevel 2>/dev/null || pwd)"
cd "${repo_root}"

include_regex='\.(rs|toml|ts|tsx|js|mjs|sh|ps1|md|yml|yaml)$'
exclude_regex='^(schemas/generated/|apps/desktop/src-tauri/third_party/|apps/web/dist/|apps/web/.vite/|apps/desktop/ui/dist/|apps/desktop/ui/.vite/|target/|fuzz/target/|node_modules/|security-artifacts/)'

tracked_source_files() {
  git ls-files | while IFS= read -r path; do
    [[ -z "${path}" ]] && continue
    [[ "${path}" =~ ${exclude_regex} ]] && continue
    [[ "${path}" =~ ${include_regex} ]] || continue
    [[ -f "${path}" ]] || continue
    printf '%s\n' "${path}"
  done
}

is_provider_allowlisted() {
  local path="$1"
  [[ "${path}" == crates/palyra-connector-discord/* ]] && return 0
  [[ "${path}" == crates/palyra-connectors/src/connectors/* ]] && return 0
  [[ "${path}" == crates/palyra-connectors/src/lib.rs ]] && return 0
  [[ "${path}" == crates/palyra-daemon/src/channels/discord.rs ]] && return 0
  [[ "${path}" == crates/palyra-daemon/src/transport/http/handlers/admin/channels/connectors/discord.rs ]] && return 0
  [[ "${path}" == crates/palyra-daemon/src/transport/http/handlers/console/channels/connectors/discord.rs ]] && return 0
  [[ "${path}" == crates/palyra-cli/src/commands/channels/connectors/* ]] && return 0
  [[ "${path}" == apps/web/src/features/channels/connectors/* ]] && return 0
  [[ "${path}" == apps/desktop/src-tauri/src/features/onboarding/connectors/* ]] && return 0
  return 1
}

print_matches_or_none() {
  local title="$1"
  shift
  local patterns=("$@")
  local matches
  matches="$("${patterns[@]}" || true)"
  echo "${title}:"
  if [[ -z "${matches}" ]]; then
    echo "  none"
  else
    while IFS= read -r line; do
      [[ -z "${line}" ]] && continue
      echo "  ${line}"
    done <<<"${matches}"
  fi
  echo
}

total_hits=0
allowlisted_hits=0
leak_hits=0
declare -a allowlisted_rows=()
declare -a leak_rows=()

while IFS= read -r path; do
  [[ -z "${path}" ]] && continue
  if ! rg -q -i 'discord' -- "${path}"; then
    continue
  fi
  hits="$(rg -i -o 'discord' -- "${path}" | wc -l | tr -d '[:space:]')"
  total_hits=$((total_hits + hits))
  if is_provider_allowlisted "${path}"; then
    allowlisted_hits=$((allowlisted_hits + hits))
    allowlisted_rows+=("$(printf '%6d %s' "${hits}" "${path}")")
  else
    leak_hits=$((leak_hits + hits))
    leak_rows+=("$(printf '%6d %s' "${hits}" "${path}")")
  fi
done < <(tracked_source_files)

core_import_violations="$(
  rg -n 'palyra_connector_core' crates apps fuzz \
    --glob '!crates/palyra-connectors/**' \
    --glob '!schemas/generated/**' \
    --glob '!apps/desktop/src-tauri/third_party/**' || true
)"

discord_import_violations="$(
  rg -n 'palyra_connector_discord' crates apps fuzz \
    --glob '!crates/palyra-connectors/**' \
    --glob '!schemas/generated/**' \
    --glob '!apps/desktop/src-tauri/third_party/**' || true
)"

connector_kind_violations="$(
  rg -n 'ConnectorKind::Discord' crates apps fuzz \
    --glob '!crates/palyra-connector-discord/**' \
    --glob '!crates/palyra-connectors/src/connectors/**' \
    --glob '!crates/palyra-daemon/src/channels/discord.rs' \
    --glob '!crates/palyra-daemon/src/transport/http/handlers/admin/channels/connectors/discord.rs' \
    --glob '!crates/palyra-daemon/src/transport/http/handlers/console/channels/connectors/discord.rs' \
    --glob '!crates/palyra-cli/src/commands/channels/connectors/**' \
    --glob '!apps/web/src/features/channels/connectors/**' \
    --glob '!apps/desktop/src-tauri/src/features/onboarding/connectors/**' \
    --glob '!schemas/generated/**' \
    --glob '!apps/desktop/src-tauri/third_party/**' || true
)"

echo "Palyra connector leakage report"
echo "repo=${repo_root}"
echo
echo "Discord keyword scatter:"
printf '  %-28s %s\n' "raw_hits" "${total_hits}"
printf '  %-28s %s\n' "allowlisted_hits" "${allowlisted_hits}"
printf '  %-28s %s\n' "outside_allowlist_hits" "${leak_hits}"
echo

echo "Top allowlisted files:"
if (( ${#allowlisted_rows[@]} == 0 )); then
  echo "  none"
else
  printf '%s\n' "${allowlisted_rows[@]}" | sort -rn | head -n 10 | sed 's/^/  /'
fi
echo

echo "Top files outside provider allowlist:"
if (( ${#leak_rows[@]} == 0 )); then
  echo "  none"
else
  printf '%s\n' "${leak_rows[@]}" | sort -rn | head -n 15 | sed 's/^/  /'
fi
echo

print_matches_or_none \
  "Direct palyra_connector_core import violations (outside crates/palyra-connectors)" \
  bash -lc "printf '%s' \"\$0\"" "${core_import_violations}"

print_matches_or_none \
  "Direct palyra_connector_discord import violations (outside crates/palyra-connectors)" \
  bash -lc "printf '%s' \"\$0\"" "${discord_import_violations}"

print_matches_or_none \
  "ConnectorKind::Discord scatter outside provider/dispatch allowlist" \
  bash -lc "printf '%s' \"\$0\"" "${connector_kind_violations}"

cat <<'EOF'
Milestone acceptance grep commands:
  rg -n 'palyra_connector_core|palyra_connector_discord' crates apps fuzz docs docs-codebase AGENTS.md Cargo.toml
  rg -n 'ConnectorKind::Discord' crates/palyra-daemon crates/palyra-cli crates/palyra-connectors
  rg -n -i 'discord' crates apps docs
EOF
