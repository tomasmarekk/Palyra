#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

run_self_test() {
  case "use palyra_daemon::gateway;" in
    *palyra_daemon*) ;;
    *) echo "architecture boundary self-test failed" >&2; exit 1 ;;
  esac
  case "use palyra_common::redaction;" in
    *palyra_daemon*) echo "architecture boundary self-test failed" >&2; exit 1 ;;
    *) ;;
  esac
  case "use palyra_cli::commands;" in
    *palyra_cli*) ;;
    *) echo "architecture boundary self-test failed" >&2; exit 1 ;;
  esac
  echo "architecture boundary self-test passed"
}

filter_non_comment_matches() {
  grep -Ev '^[^:]+:[0-9]+:[[:space:]]*(//|#)' || true
}

check_rule() {
  local rule_name="$1"
  local root="$2"
  local pattern="$3"
  local matches

  [[ -d "$root" ]] || return 0
  matches="$(
    git grep -n -I -E "$pattern" -- "$root" \
      ':(exclude)*.md' \
      ':(exclude)target/**' \
      ':(exclude)node_modules/**' \
      ':(exclude)dist/**' \
      ':(exclude).vite/**' 2>/dev/null | filter_non_comment_matches
  )"
  [[ -z "$matches" ]] && return 0

  echo "architecture boundary violation: $rule_name" >&2
  printf '%s\n' "$matches" | sed 's/^/  /' >&2
  return 1
}

if [[ "${1:-}" == "--self-test" ]]; then
  run_self_test
  exit 0
fi

cd "$ROOT_DIR"

failed=false

check_rule \
  "connectors-stay-provider-neutral" \
  "crates/palyra-connectors" \
  'palyra[_-](daemon|policy|vault)|PolicyDecision|PolicyEvaluation|ApprovalRuntime' \
  || failed=true

check_rule \
  "policy-stays-core-only" \
  "crates/palyra-policy" \
  'palyra[_-](daemon|connectors|vault)|(^|[^[:alnum:]_])(axum|tauri)([^[:alnum:]_]|$)' \
  || failed=true

check_rule \
  "vault-stays-runtime-independent" \
  "crates/palyra-vault" \
  'palyra[_-](daemon|connectors|policy)|(^|[^[:alnum:]_])(axum|tauri)([^[:alnum:]_]|$)' \
  || failed=true

check_rule \
  "web-ui-does-not-import-rust-crates" \
  "apps/web/src" \
  '\.\./\.\./crates/|crates[/\\]palyra' \
  || failed=true

check_rule \
  "desktop-ui-does-not-import-rust-crates" \
  "apps/desktop/ui/src" \
  '\.\./\.\./crates/|crates[/\\]palyra' \
  || failed=true

check_rule \
  "plugin-sdk-stays-host-independent" \
  "crates/palyra-plugins/sdk" \
  'palyra[_-](daemon|cli|vault|connectors|policy)|(^|[^[:alnum:]_])(axum|tauri|rusqlite|reqwest)([^[:alnum:]_]|$)' \
  || failed=true

check_rule \
  "plugin-runtime-stays-host-independent" \
  "crates/palyra-plugins/runtime" \
  'palyra[_-](daemon|cli|vault|connectors|policy)|(^|[^[:alnum:]_])(axum|tauri|rusqlite|reqwest)([^[:alnum:]_]|$)' \
  || failed=true

if [[ "$failed" == true ]]; then
  exit 1
fi

echo "architecture boundary checks passed"
