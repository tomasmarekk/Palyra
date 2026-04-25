#!/usr/bin/env bash
set -euo pipefail

repo_root="$(git rev-parse --show-toplevel 2>/dev/null || pwd)"
cd "${repo_root}"

warn_threshold="${PALYRA_MODULE_BUDGET_WARN:-800}"
critical_threshold="${PALYRA_MODULE_BUDGET_CRITICAL:-1200}"
strict_threshold="${PALYRA_MODULE_BUDGET_STRICT:-${critical_threshold}}"
entrypoint_threshold="${PALYRA_MODULE_BUDGET_ENTRYPOINT:-200}"
entrypoint_strict_threshold="${PALYRA_MODULE_BUDGET_ENTRYPOINT_STRICT:-500}"
allowlist_file="${PALYRA_MODULE_BUDGET_ALLOWLIST:-scripts/dev/module-budget-allowlist.txt}"
strict_mode=false

if [[ "${1-}" == "--strict" ]]; then
  strict_mode=true
fi

if ! command -v git >/dev/null 2>&1; then
  echo "git is required to report module budgets" >&2
  exit 1
fi

include_regex='\.(rs|proto|ts|tsx|js|mjs|sh|ps1|css|html)$'
exclude_regex='^(node_modules/|schemas/generated/|apps/web/dist/|apps/web/.vite/|apps/desktop/ui/dist/|apps/desktop/ui/.vite/|target/|fuzz/target/|security-artifacts/)'

declare -a allowlist_patterns=()
declare -a warn_files=()
declare -a critical_files=()
declare -a large_entrypoints=()
declare -a touched_budget_regressions=()
declare -a allowlisted_touched_budget_regressions=()
declare -a strict_budget_regressions=()
declare -a allowlisted_strict_budget_regressions=()
declare -a touched_entrypoint_regressions=()
declare -a allowlisted_touched_entrypoint_regressions=()
declare -a strict_entrypoint_regressions=()
declare -a allowlisted_strict_entrypoint_regressions=()
declare -a new_oversized_files=()
declare -A discord_counts=(
  ["apps"]=0
  ["crates/palyra-daemon"]=0
  ["crates/palyra-connectors"]=0
  ["crates/palyra-cli"]=0
  ["docs"]=0
)

load_allowlist() {
  [[ -f "${allowlist_file}" ]] || return 0

  while IFS= read -r line || [[ -n "${line}" ]]; do
    line="${line%%$'\r'}"
    line="${line#"${line%%[![:space:]]*}"}"
    line="${line%"${line##*[![:space:]]}"}"
    [[ -z "${line}" || "${line}" == \#* ]] && continue
    allowlist_patterns+=("${line}")
  done < "${allowlist_file}"
}

is_source_path() {
  local path="$1"
  [[ -n "${path}" ]] || return 1
  [[ "${path}" =~ ${exclude_regex} ]] && return 1
  [[ "${path}" =~ ${include_regex} ]] || return 1
  [[ -f "${path}" ]] || return 1
  return 0
}

is_allowlisted() {
  local path="$1"
  local pattern
  for pattern in "${allowlist_patterns[@]}"; do
    [[ "${path}" == ${pattern} ]] && return 0
  done
  return 1
}

count_keyword_hits() {
  local path="$1"
  (git grep -I -i -o 'discord' -- "$path" 2>/dev/null || true) | wc -l
}

current_line_count() {
  local path="$1"
  wc -l < "${path}" | tr -d '[:space:]'
}

previous_line_count() {
  local base_ref="$1"
  local path="$2"
  if [[ -z "${base_ref}" ]]; then
    echo 0
    return 0
  fi
  if ! git cat-file -e "${base_ref}:${path}" 2>/dev/null; then
    echo 0
    return 0
  fi
  git show "${base_ref}:${path}" | wc -l | tr -d '[:space:]'
}

tracked_source_files() {
  git ls-files | while IFS= read -r path; do
    is_source_path "${path}" && printf '%s\n' "${path}"
  done
}

working_tree_dirty() {
  ! git diff --quiet --ignore-submodules HEAD -- 2>/dev/null || [[ -n "$(git ls-files --others --exclude-standard)" ]]
}

discover_diff_base() {
  if git rev-parse --verify HEAD >/dev/null 2>&1; then
    if working_tree_dirty; then
      printf 'HEAD\n'
      return 0
    fi
    if git rev-parse --verify HEAD^ >/dev/null 2>&1; then
      printf 'HEAD^\n'
      return 0
    fi
  fi
  printf '\n'
}

touched_source_files() {
  local base_ref="$1"
  local path
  declare -A seen=()

  if [[ -n "${base_ref}" ]]; then
    while IFS= read -r path; do
      is_source_path "${path}" || continue
      [[ -n "${seen[${path}]+x}" ]] && continue
      seen["${path}"]=1
      printf '%s\n' "${path}"
    done < <(git diff --name-only "${base_ref}" --)
  fi

  while IFS= read -r path; do
    is_source_path "${path}" || continue
    [[ -n "${seen[${path}]+x}" ]] && continue
    seen["${path}"]=1
    printf '%s\n' "${path}"
  done < <(git ls-files --others --exclude-standard)
}

format_regression_row() {
  local delta="$1"
  local previous="$2"
  local current="$3"
  local path="$4"
  printf '%6d (%d -> %d) %s' "${delta}" "${previous}" "${current}" "${path}"
}

load_allowlist

while IFS= read -r path; do
  [[ -z "${path}" ]] && continue

  line_count="$(current_line_count "${path}")"

  if (( line_count >= critical_threshold )); then
    critical_files+=("$(printf '%8d %s' "${line_count}" "${path}")")
  elif (( line_count >= warn_threshold )); then
    warn_files+=("$(printf '%8d %s' "${line_count}" "${path}")")
  fi

  case "${path}" in
    */main.rs|*/lib.rs)
      if (( line_count >= entrypoint_threshold )); then
        large_entrypoints+=("$(printf '%8d %s' "${line_count}" "${path}")")
      fi
      ;;
  esac
done < <(tracked_source_files)

for scope in "${!discord_counts[@]}"; do
  if [[ -d "${scope}" ]]; then
    discord_counts["${scope}"]="$(count_keyword_hits "${scope}" | tr -d '[:space:]')"
  fi
done

diff_base="$(discover_diff_base)"

while IFS= read -r path; do
  [[ -z "${path}" ]] && continue

  current_lines="$(current_line_count "${path}")"
  previous_lines="$(previous_line_count "${diff_base}" "${path}")"
  delta=$((current_lines - previous_lines))

  if (( previous_lines == 0 && current_lines >= warn_threshold )) && ! is_allowlisted "${path}"; then
    new_oversized_files+=("$(printf '%8d %s' "${current_lines}" "${path}")")
  fi

  if (( delta > 0 && current_lines >= warn_threshold )); then
    regression_row="$(format_regression_row "${delta}" "${previous_lines}" "${current_lines}" "${path}")"
    if is_allowlisted "${path}"; then
      allowlisted_touched_budget_regressions+=("${regression_row}")
    else
      touched_budget_regressions+=("${regression_row}")
    fi
  fi

  if (( delta > 0 && current_lines >= strict_threshold )); then
    regression_row="$(format_regression_row "${delta}" "${previous_lines}" "${current_lines}" "${path}")"
    if is_allowlisted "${path}"; then
      allowlisted_strict_budget_regressions+=("${regression_row}")
    else
      strict_budget_regressions+=("${regression_row}")
    fi
  fi

  case "${path}" in
    */main.rs|*/lib.rs)
      if (( delta > 0 && current_lines >= entrypoint_threshold )); then
        regression_row="$(format_regression_row "${delta}" "${previous_lines}" "${current_lines}" "${path}")"
        if is_allowlisted "${path}"; then
          allowlisted_touched_entrypoint_regressions+=("${regression_row}")
        else
          touched_entrypoint_regressions+=("${regression_row}")
        fi
      fi
      if (( delta > 0 && current_lines >= entrypoint_strict_threshold )); then
        regression_row="$(format_regression_row "${delta}" "${previous_lines}" "${current_lines}" "${path}")"
        if is_allowlisted "${path}"; then
          allowlisted_strict_entrypoint_regressions+=("${regression_row}")
        else
          strict_entrypoint_regressions+=("${regression_row}")
        fi
      fi
      ;;
  esac
done < <(touched_source_files "${diff_base}")

echo "Palyra module budget report"
echo "repo=${repo_root}"
echo "warn_threshold=${warn_threshold}"
echo "critical_threshold=${critical_threshold}"
echo "strict_threshold=${strict_threshold}"
echo "entrypoint_threshold=${entrypoint_threshold}"
echo "entrypoint_strict_threshold=${entrypoint_strict_threshold}"
echo "allowlist_file=${allowlist_file}"
echo "diff_base=${diff_base:-none}"
echo

echo "Files at or above critical threshold (${critical_threshold}+ LOC):"
if (( ${#critical_files[@]} == 0 )); then
  echo "  none"
else
  printf '%s\n' "${critical_files[@]}" | sort -nr
fi
echo

echo "Files at or above warning threshold (${warn_threshold}+ LOC, excluding critical):"
if (( ${#warn_files[@]} == 0 )); then
  echo "  none"
else
  printf '%s\n' "${warn_files[@]}" | sort -nr
fi
echo

echo "Large root entrypoints (main.rs/lib.rs at ${entrypoint_threshold}+ LOC):"
if (( ${#large_entrypoints[@]} == 0 )); then
  echo "  none"
else
  printf '%s\n' "${large_entrypoints[@]}" | sort -nr
fi
echo

echo "Touched files that grew at or above warning threshold (not allowlisted, report-only):"
if (( ${#touched_budget_regressions[@]} == 0 )); then
  echo "  none"
else
  printf '%s\n' "${touched_budget_regressions[@]}" | sort -nr | sed 's/^/  /'
fi
echo

echo "Allowlisted touched files that grew at or above warning threshold:"
if (( ${#allowlisted_touched_budget_regressions[@]} == 0 )); then
  echo "  none"
else
  printf '%s\n' "${allowlisted_touched_budget_regressions[@]}" | sort -nr | sed 's/^/  /'
fi
echo

echo "Touched files that grew at or above strict threshold (not allowlisted):"
if (( ${#strict_budget_regressions[@]} == 0 )); then
  echo "  none"
else
  printf '%s\n' "${strict_budget_regressions[@]}" | sort -nr | sed 's/^/  /'
fi
echo

echo "Allowlisted touched files that grew at or above strict threshold:"
if (( ${#allowlisted_strict_budget_regressions[@]} == 0 )); then
  echo "  none"
else
  printf '%s\n' "${allowlisted_strict_budget_regressions[@]}" | sort -nr | sed 's/^/  /'
fi
echo

echo "Touched entrypoints that grew at or above warning threshold (not allowlisted, report-only):"
if (( ${#touched_entrypoint_regressions[@]} == 0 )); then
  echo "  none"
else
  printf '%s\n' "${touched_entrypoint_regressions[@]}" | sort -nr | sed 's/^/  /'
fi
echo

echo "Allowlisted touched entrypoints that grew at or above warning threshold:"
if (( ${#allowlisted_touched_entrypoint_regressions[@]} == 0 )); then
  echo "  none"
else
  printf '%s\n' "${allowlisted_touched_entrypoint_regressions[@]}" | sort -nr | sed 's/^/  /'
fi
echo

echo "Touched entrypoints that grew at or above strict threshold (not allowlisted):"
if (( ${#strict_entrypoint_regressions[@]} == 0 )); then
  echo "  none"
else
  printf '%s\n' "${strict_entrypoint_regressions[@]}" | sort -nr | sed 's/^/  /'
fi
echo

echo "Allowlisted touched entrypoints that grew at or above strict threshold:"
if (( ${#allowlisted_strict_entrypoint_regressions[@]} == 0 )); then
  echo "  none"
else
  printf '%s\n' "${allowlisted_strict_entrypoint_regressions[@]}" | sort -nr | sed 's/^/  /'
fi
echo

echo "New oversized files introduced outside the allowlist:"
if (( ${#new_oversized_files[@]} == 0 )); then
  echo "  none"
else
  printf '%s\n' "${new_oversized_files[@]}" | sort -nr | sed 's/^/  /'
fi
echo

echo "Connector keyword scatter ('discord' raw hits by scope):"
for scope in apps crates/palyra-daemon crates/palyra-connectors crates/palyra-cli docs; do
  printf '  %-26s %s\n' "${scope}" "${discord_counts[$scope]}"
done
echo

if ${strict_mode}; then
  strict_failed=false

  if (( ${#strict_budget_regressions[@]} > 0 )); then
    echo "strict mode: one or more touched files grew at or above strict threshold" >&2
    strict_failed=true
  fi

  if (( ${#strict_entrypoint_regressions[@]} > 0 )); then
    echo "strict mode: one or more touched entrypoints grew at or above strict threshold" >&2
    strict_failed=true
  fi

  if (( ${#new_oversized_files[@]} > 0 )); then
    echo "strict mode: one or more new oversized files were introduced outside the allowlist" >&2
    strict_failed=true
  fi

  if ! bash scripts/check-channel-provider-boundaries.sh; then
    strict_failed=true
  fi

  if ${strict_failed}; then
    exit 1
  fi
fi
