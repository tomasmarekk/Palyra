#!/usr/bin/env bash
set -euo pipefail

if command -v rg >/dev/null 2>&1; then
  matches="$(rg --line-number --color never \
    --glob '!target/**' \
    --glob '!oc-docs/**' \
    --glob '!roadmap/**' \
    -e 'BEGIN (RSA|OPENSSH|EC|DSA) PRIVATE KEY' \
    -e 'AKIA[0-9A-Z]{16}' \
    -e 'xox[baprs]-[A-Za-z0-9-]+' \
    -e '(?i)aws_secret_access_key\\s*[:=]\\s*(?:[\"'\"'][A-Za-z0-9/+=]{40}[\"'\"']|[A-Za-z0-9/+=]{40})' \
    . || true)"
else
  common_args=(
    -R -n -E
    --exclude-dir=target
    --exclude-dir=oc-docs
    --exclude-dir=roadmap
  )
  main_matches="$(grep "${common_args[@]}" \
    'BEGIN (RSA|OPENSSH|EC|DSA) PRIVATE KEY|AKIA[0-9A-Z]{16}|xox[baprs]-[A-Za-z0-9-]+' \
    . || true)"
  aws_matches="$(grep "${common_args[@]}" -i \
    "aws_secret_access_key[[:space:]]*[:=][[:space:]]*(['\"][A-Za-z0-9/+=]{40}['\"]|[A-Za-z0-9/+=]{40})" \
    . || true)"
  matches="${main_matches}"$'\n'"${aws_matches}"
fi

if [[ -n "${matches//[[:space:]]/}" ]]; then
  echo "High-risk credential pattern detected:"
  echo "$matches"
  exit 1
fi

echo "High-risk pattern scan passed."
