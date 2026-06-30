#!/usr/bin/env bash
set -euo pipefail

find_ripgrep() {
  local candidate

  for name in rg rg.exe; do
    if candidate="$(command -v "$name" 2>/dev/null)" && "$candidate" --version >/dev/null 2>&1; then
      printf '%s\n' "$candidate"
      return 0
    fi
  done

  return 1
}

if rg_bin="$(find_ripgrep)"; then
  set +e
  matches="$("$rg_bin" --line-number --color never \
    --glob '!target/**' \
    --glob '!oc-docs/**' \
    --glob '!roadmap/**' \
    -e 'BEGIN (RSA|OPENSSH|EC|DSA) PRIVATE KEY' \
    -e 'AKIA[0-9A-Z]{16}' \
    -e 'xox[baprs]-[A-Za-z0-9-]+' \
    -e '(?i)aws_secret_access_key\\s*[:=]\\s*(?:[\"'\"'][A-Za-z0-9/+=]{40}[\"'\"']|[A-Za-z0-9/+=]{40})' \
    .)"
  rg_status=$?
  set -e

  if [[ "$rg_status" -gt 1 ]]; then
    echo "High-risk pattern scan failed to run ripgrep: $rg_bin" >&2
    exit "$rg_status"
  fi
else
  common_args=(
    -R -n -E
    --exclude-dir=target
    --exclude-dir=oc-docs
    --exclude-dir=roadmap
  )
  set +e
  main_matches="$(grep "${common_args[@]}" \
    'BEGIN (RSA|OPENSSH|EC|DSA) PRIVATE KEY|AKIA[0-9A-Z]{16}|xox[baprs]-[A-Za-z0-9-]+' \
    .)"
  main_status=$?
  aws_matches="$(grep "${common_args[@]}" -i \
    "aws_secret_access_key[[:space:]]*[:=][[:space:]]*(['\"][A-Za-z0-9/+=]{40}['\"]|[A-Za-z0-9/+=]{40})" \
    .)"
  aws_status=$?
  set -e

  for grep_status in "$main_status" "$aws_status"; do
    if [[ "$grep_status" -gt 1 ]]; then
      echo "High-risk pattern scan failed to run grep fallback." >&2
      exit "$grep_status"
    fi
  done

  matches="${main_matches}"$'\n'"${aws_matches}"
fi

if [[ -n "${matches//[[:space:]]/}" ]]; then
  echo "High-risk credential pattern detected:"
  echo "$matches"
  exit 1
fi

echo "High-risk pattern scan passed."
