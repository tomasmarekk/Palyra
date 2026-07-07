#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$repo_root"

patterns=(
  'direct_journal_authority_granted\s*:\s*true'
  'approval_authority_granted\s*:\s*true'
  'tool_executor_authority_granted\s*:\s*true'
  'journal\.write\.direct.*allowed\s*:\s*true'
)

for pattern in "${patterns[@]}"; do
  if rg -n --glob '*.rs' "$pattern" crates/palyra-daemon/src crates/palyra-common/src; then
    echo "host authority bypass pattern matched: $pattern" >&2
    exit 1
  fi
done

cargo test -p palyra-common host_authority_checklist_denies_direct_runtime_authority --locked
