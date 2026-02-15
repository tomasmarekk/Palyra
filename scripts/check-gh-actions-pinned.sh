#!/usr/bin/env bash
set -euo pipefail

status=0

while IFS=: read -r file line_number raw_line; do
  uses_ref="$(echo "$raw_line" | sed -E "s/^[[:space:]]*uses:[[:space:]]*//; s/[[:space:]]+$//")"
  if [[ -z "$uses_ref" ]]; then
    continue
  fi

  # Local workflow/action references and docker image actions do not use commit SHAs.
  if [[ "$uses_ref" == ./* || "$uses_ref" == docker://* ]]; then
    continue
  fi

  if [[ "$uses_ref" =~ ^[^@[:space:]]+@[0-9a-f]{40}$ ]]; then
    continue
  fi

  echo "ERROR: $file:$line_number uses mutable action reference '$uses_ref'. Pin to a full 40-char commit SHA." >&2
  status=1
done < <(grep -RInE '^[[:space:]]*uses:[[:space:]]*' .github/workflows/*.yml)

exit "$status"
