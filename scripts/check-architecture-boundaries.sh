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
  report_disallowed_matches \
    "self-test-allowlist" \
    '^allowed.rs:[0-9]+:' \
    'allowed.rs:7:use approved::Boundary;' \
    >/dev/null \
    || { echo "architecture boundary self-test failed" >&2; exit 1; }
  if report_disallowed_matches \
    "self-test-rejection" \
    '^allowed.rs:[0-9]+:' \
    'frozen.rs:7:use forbidden::Boundary;' \
    >/dev/null 2>&1; then
    echo "architecture boundary self-test failed" >&2
    exit 1
  fi
  echo "architecture boundary self-test passed"
}

filter_non_comment_matches() {
  grep -Ev '^[^:]+:[0-9]+:[[:space:]]*(//|#)' || true
}

source_matches() {
  local pattern="$1"
  shift

  grep -RInHE \
    --include='*.rs' \
    --exclude-dir=target \
    --exclude-dir=node_modules \
    --exclude-dir=dist \
    --exclude-dir=.vite \
    "$pattern" -- "$@" 2>/dev/null \
    | tr '\\' '/' \
    | filter_non_comment_matches
}

report_disallowed_matches() {
  local rule_name="$1"
  local allowed_path_pattern="$2"
  local matches="$3"
  local disallowed

  disallowed="$(printf '%s\n' "$matches" | grep -Ev "$allowed_path_pattern" || true)"
  [[ -z "$disallowed" ]] && return 0

  echo "architecture boundary violation: $rule_name" >&2
  printf '%s\n' "$disallowed" | sed 's/^/  /' >&2
  return 1
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

check_run_stream_tool_dispatch_boundary() {
  local root="crates/palyra-daemon/src/application/run_stream"
  local allowed="crates/palyra-daemon/src/application/run_stream/tool_flow.rs"
  local symbol="execute_tool_with_runtime_dispatch_with_cancellation_and_progress"
  local call_pattern="${symbol}[[:space:]]*\\("
  local matches
  local disallowed
  local allowed_count

  matches="$(
    source_matches "$call_pattern" "$root"
  )"
  disallowed="$(printf '%s\n' "$matches" | grep -Ev "^${allowed}:[0-9]+:" || true)"
  allowed_count="$(printf '%s\n' "$matches" | grep -Ec "^${allowed}:[0-9]+:" || true)"

  if [[ -n "$disallowed" || "$allowed_count" -ne 1 ]]; then
    echo "architecture boundary violation: run-stream-tool-dispatch-is-canonical" >&2
    echo "  expected exactly one dispatcher call in $allowed; observed $allowed_count" >&2
    [[ -z "$disallowed" ]] || printf '%s\n' "$disallowed" | sed 's/^/  /' >&2
    return 1
  fi
}

check_legacy_run_stream_freeze() {
  local root="crates/palyra-daemon/src/application/run_stream"
  local dispatcher_boundary="$root/orchestration.rs"
  local admission_ingress="$root/admission_ingress.rs"
  local v2_attempt_adapter="$root/embedded_attempt.rs"
  local v2_driver="$root/orchestration/v2_driver.rs"
  local shadow_planning="$root/orchestration/shadow_planning.rs"
  local token_estimation="$root/orchestration/token_estimation.rs"
  local tool_owner_adapter="$root/tool_flow/owner.rs"
  local tool_contract_adapter="$root/tool_flow/stages.rs"
  local allowed_seams
  local runtime_matches
  local decision_matches
  local decision_variant_matches
  local unexpected_decision_variant_matches
  local decision_constructions
  local feature_branch_matches
  local unexpected_feature_branch_matches
  local start_matches
  local legacy_line
  local start_line
  local v2_line

  # INTENTIONAL: every new run-stream file begins frozen. Crossing into V2
  # requires adding one narrowly named adapter seam here during review.
  allowed_seams="^(${dispatcher_boundary}|${admission_ingress}|${v2_attempt_adapter}|${v2_driver}|${shadow_planning}|${token_estimation}|${tool_owner_adapter}|${tool_contract_adapter}):[0-9]+:"
  runtime_matches="$(
    source_matches \
      'runtime_kernel_v2|RuntimeKernel(Profile|Version|Dispatcher|V2)|RuntimeDispatchDecision|RunStreamRuntimeDispatch|RuntimeAuthority::(Legacy|V2)' \
      "$root"
  )"
  report_disallowed_matches \
    "legacy-run-stream-roots-are-frozen" \
    "$allowed_seams" \
    "$runtime_matches" \
    || return 1

  decision_matches="$(
    source_matches 'RuntimeDispatchDecision|RunStreamRuntimeDispatch' "$root"
  )"
  report_disallowed_matches \
    "runtime-profile-branching-has-one-run-stream-boundary" \
    "^${dispatcher_boundary}:[0-9]+:" \
    "$decision_matches" \
    || return 1

  feature_branch_matches="$(
    source_matches 'feature_rollouts\.[[:alnum:]_]+' "$root"
  )"
  unexpected_feature_branch_matches="$(
    printf '%s\n' "$feature_branch_matches" \
      | grep -Ev \
        "^(${dispatcher_boundary}:[0-9]+:.*feature_rollouts\\.(agent_harness_runtime|context_engine|tool_repair)|${root}/tool_flow.rs:[0-9]+:.*feature_rollouts\\.(inline_runtime_hooks|tool_result_middleware))" \
      || true
  )"
  if [[ -n "$unexpected_feature_branch_matches" \
    || "$(printf '%s\n' "$feature_branch_matches" | grep -Ec "^${dispatcher_boundary}:[0-9]+:.*feature_rollouts\\.agent_harness_runtime")" -ne 2 \
    || "$(printf '%s\n' "$feature_branch_matches" | grep -Ec "^${dispatcher_boundary}:[0-9]+:.*feature_rollouts\\.context_engine")" -ne 1 \
    || "$(printf '%s\n' "$feature_branch_matches" | grep -Ec "^${dispatcher_boundary}:[0-9]+:.*feature_rollouts\\.tool_repair")" -ne 2 \
    || "$(printf '%s\n' "$feature_branch_matches" | grep -Ec "^${root}/tool_flow.rs:[0-9]+:.*feature_rollouts\\.inline_runtime_hooks")" -ne 2 \
    || "$(printf '%s\n' "$feature_branch_matches" | grep -Ec "^${root}/tool_flow.rs:[0-9]+:.*feature_rollouts\\.tool_result_middleware")" -ne 1 ]]; then
    echo "architecture boundary violation: legacy-feature-branches-are-frozen" >&2
    printf '%s\n' "$feature_branch_matches" | sed 's/^/  /' >&2
    return 1
  fi

  decision_variant_matches="$(
    source_matches \
      'RuntimeDispatchDecision::(Legacy|LegacyWithShadow|V2|Blocked)' \
      "$dispatcher_boundary"
  )"
  unexpected_decision_variant_matches="$(
    printf '%s\n' "$decision_variant_matches" \
      | grep -Ev \
        "^${dispatcher_boundary}:[0-9]+:[[:space:]]*(RuntimeDispatchDecision::(Legacy|LegacyWithShadow|V2|Blocked)[[:space:]]*\\{[[:space:]]*\\.\\.[[:space:]]*\\}[[:space:]]*=>|if !matches!\\(decision, RuntimeDispatchDecision::V2[[:space:]]*\\{[[:space:]]*\\.\\.[[:space:]]*\\}\\)[[:space:]]*\\{)" \
      || true
  )"
  if [[ -n "$unexpected_decision_variant_matches" ]]; then
    echo "architecture boundary violation: runtime-dispatch-variants-are-match-only" >&2
    printf '%s\n' "$unexpected_decision_variant_matches" | sed 's/^/  /' >&2
    return 1
  fi

  decision_constructions="$(
    source_matches \
      'RuntimeDispatchDecision::(Legacy|LegacyWithShadow|V2|Blocked)[[:space:]]*\{[[:space:]]*authority' \
      "crates/palyra-daemon/src"
  )"
  report_disallowed_matches \
    "runtime-dispatch-decisions-are-dispatcher-owned" \
    '^crates/palyra-daemon/src/application/runtime_kernel_v2/dispatcher.rs:[0-9]+:' \
    "$decision_constructions" \
    || return 1

  start_matches="$(
    source_matches 'start_orchestrator_run[[:space:]]*\(' "$root"
  )"
  report_disallowed_matches \
    "legacy-run-start-stays-at-dispatcher-boundary" \
    "^${dispatcher_boundary}:[0-9]+:" \
    "$start_matches" \
    || return 1

  legacy_line="$(
    grep -nE 'Some\(RuntimeAuthority::Legacy\)[[:space:]]*=>' "$dispatcher_boundary" \
      | cut -d: -f1
  )"
  start_line="$(
    grep -nF '.start_orchestrator_run(OrchestratorRunStartRequest {' "$dispatcher_boundary" \
      | cut -d: -f1
  )"
  v2_line="$(
    grep -nE 'Some\(RuntimeAuthority::V2\)[[:space:]]*=>' "$dispatcher_boundary" \
      | cut -d: -f1
  )"
  if [[ "$(printf '%s\n' "$legacy_line" | grep -c .)" -ne 1 \
    || "$(printf '%s\n' "$start_line" | grep -c .)" -ne 1 \
    || "$(printf '%s\n' "$v2_line" | grep -c .)" -ne 1 \
    || "$legacy_line" -ge "$start_line" \
    || "$start_line" -ge "$v2_line" ]]; then
    echo "architecture boundary violation: v2-selection-cannot-fall-back-to-legacy-start" >&2
    echo "  expected one legacy branch/start followed by one V2 branch in $dispatcher_boundary" >&2
    return 1
  fi
}

check_legacy_retirement_contract() {
  local orchestration="crates/palyra-daemon/src/application/run_stream/orchestration.rs"
  local resolver="crates/palyra-daemon/src/application/runtime_kernel_v2/profile_resolver.rs"
  local resolver_tests="crates/palyra-daemon/src/application/runtime_kernel_v2/profile_resolver/tests.rs"
  local manifest="infra/release/legacy-retirement.json"
  local retirement_module="crates/palyra-daemon/src/application/core_stability/retirement.rs"
  local orchestration_lines
  local guard_count
  local compatibility_test_count

  orchestration_lines="$(wc -l < "$orchestration")"
  if [[ "$orchestration_lines" -gt 11000 ]]; then
    echo "architecture boundary violation: run-stream-orchestration-is-compatibility-adapter" >&2
    echo "  $orchestration has $orchestration_lines lines; retirement budget is 11000" >&2
    return 1
  fi

  for retired in \
    "crates/palyra-daemon/src/application/release_hardening.rs" \
    "crates/palyra-daemon/src/application/runtime_boundary_metrics.rs"; do
    if [[ -e "$retired" ]]; then
      echo "architecture boundary violation: retired-runtime-scaffold-removed" >&2
      echo "  retired module still exists: $retired" >&2
      return 1
    fi
  done

  if [[ ! -f "$manifest" || ! -f "$retirement_module" ]] \
    || ! grep -q '"new_run_admission": false' "$manifest" \
    || ! grep -q '"release_rollback_only": true' "$manifest" \
    || ! grep -q 'struct LegacyRetirementManifest' "$retirement_module" \
    || ! grep -q 'struct ConfigDeprecationNotice' "$retirement_module"; then
    echo "architecture boundary violation: legacy-retirement-contract-is-complete" >&2
    return 1
  fi

  guard_count="$(
    grep -Ec 'ExistingSessionAuthorityBinding::New[[:space:]]*=>[[:space:]]*self\.new_session_profile\(\)\?' \
      "$resolver" \
      || true
  )"
  compatibility_test_count="$(
    grep -Ec '^fn legacy_profile_rejects_new_sessions_but_preserves_existing_session_reads\(\)' \
      "$resolver_tests" \
      || true
  )"
  if [[ "$guard_count" -ne 1 || "$compatibility_test_count" -ne 1 ]]; then
    echo "architecture boundary violation: legacy-new-session-admission-is-retired" >&2
    echo "  expected one production guard and one compatibility regression test" >&2
    return 1
  fi
}

check_v2_tool_authority_boundary() {
  local kernel_root="crates/palyra-daemon/src/application/runtime_kernel_v2"
  local attempt="crates/palyra-daemon/src/application/run_stream/embedded_attempt.rs"
  local live_gateway="$kernel_root/phases/tool_authority/live.rs"
  local run_stream_root="crates/palyra-daemon/src/application/run_stream"
  local tool_flow="$run_stream_root/tool_flow.rs"
  local tool_owner="$run_stream_root/tool_flow/owner.rs"
  local forbidden_matches
  local live_port_matches
  local prepared_execution_matches
  local gateway_impl_matches
  local gateway_impl_disallowed
  local live_gateway_impl_count
  local test_gateway_impl_count

  forbidden_matches="$(
    source_matches \
      'application::tool_runtime|(^|[^[:alnum:]_])(tool_runtime|execution_backends|tool_protocol)([[:space:]]*(::|,|;|as))|execute_tool_call(_with_[[:alnum:]_]*)?[[:space:]]*\(|execute_tool_with_runtime_dispatch[[:alnum:]_]*[[:space:]]*\(|execute_prepared_tool_runtime[[:space:]]*\(|dispatch_remote_tool[[:space:]]*\(|ExecutionBackend[[:alnum:]_]*|(^|[^[:alnum:]_])(ToolExecutionRawResult|ToolExecutionOutcome)([^[:alnum:]_]|$)|RunStreamPreparedToolExecution|PreparedToolRuntimeExecution|prepare_tool_side_effect_fence[[:space:]]*\(|transition_tool_side_effect_fence[[:space:]]*\(' \
      "$kernel_root" \
      "$attempt"
  )"
  if [[ -n "$forbidden_matches" ]]; then
    echo "architecture boundary violation: v2-tools-use-tool-authority-gateway" >&2
    printf '%s\n' "$forbidden_matches" | sed 's/^/  /' >&2
    return 1
  fi

  live_port_matches="$(
    source_matches 'run_stream::tool_flow|LiveToolFlowPort' "$kernel_root" "$attempt"
  )"
  report_disallowed_matches \
    "v2-live-tool-port-is-gateway-owned" \
    "^${live_gateway}:[0-9]+:" \
    "$live_port_matches" \
    || return 1

  prepared_execution_matches="$(
    source_matches 'execute_prepared_tool_runtime[[:space:]]*\(' "$run_stream_root"
  )"
  report_disallowed_matches \
    "prepared-tool-execution-is-adapter-owned" \
    "^(${tool_flow}|${tool_owner}):[0-9]+:" \
    "$prepared_execution_matches" \
    || return 1
  if [[ -z "$prepared_execution_matches" ]]; then
    echo "architecture boundary violation: prepared-tool-execution-is-adapter-owned" >&2
    echo "  canonical prepared execution entry point is missing" >&2
    return 1
  fi

  gateway_impl_matches="$(
    source_matches 'impl[[:space:]]+ToolAuthorityGateway[[:space:]]+for' "$kernel_root"
  )"
  gateway_impl_disallowed="$(
    printf '%s\n' "$gateway_impl_matches" \
      | grep -Ev \
        "^(${live_gateway}:[0-9]+:[[:space:]]*impl ToolAuthorityGateway for LiveToolAuthorityGateway|${kernel_root}/context.rs:[0-9]+:[[:space:]]*impl ToolAuthorityGateway for UnavailableToolGateway)" \
      || true
  )"
  live_gateway_impl_count="$(
    printf '%s\n' "$gateway_impl_matches" \
      | grep -Ec "^${live_gateway}:[0-9]+:[[:space:]]*impl ToolAuthorityGateway for LiveToolAuthorityGateway" \
      || true
  )"
  test_gateway_impl_count="$(
    printf '%s\n' "$gateway_impl_matches" \
      | grep -Ec "^${kernel_root}/context.rs:[0-9]+:[[:space:]]*impl ToolAuthorityGateway for UnavailableToolGateway" \
      || true
  )"
  if [[ -n "$gateway_impl_disallowed" \
    || "$live_gateway_impl_count" -ne 1 \
    || "$test_gateway_impl_count" -ne 1 ]]; then
    echo "architecture boundary violation: tool-authority-gateway-has-one-live-adapter" >&2
    printf '%s\n' "$gateway_impl_matches" | sed 's/^/  /' >&2
    return 1
  fi
}

check_shadow_planner_boundary() {
  local shadow_root="crates/palyra-daemon/src/application/runtime_kernel_v2/shadow"
  local shadow_module="crates/palyra-daemon/src/application/runtime_kernel_v2/shadow.rs"
  local forbidden_matches
  local public_module_matches
  local observer_count

  # Shadow planners are sealed data transforms. Host service dependencies
  # belong in the authoritative dispatcher that supplies sanitized snapshots.
  forbidden_matches="$(
    source_matches \
      '(^|[^[:alnum:]_])(gateway|journal|model_provider|execution_backends|tool_protocol|vault)([[:space:]]*(::|,|;|as))|application::(approvals|delivery_arbitration|outbound_lifecycle|tool_runtime|run_stream)|production_services::|GatewayRuntimeState|JournalStore|RuntimeProviderLaneAuthority|ProviderLeaseExecutionContext|ProductionServiceBundle|LiveToolFlowPort|LiveToolAuthorityGateway|ToolAuthorityGateway|OutboundMessageRequest|ApprovalRuntime|SensitiveBytes|palyra_vault' \
      "$shadow_module" \
      "$shadow_root"
  )"
  if [[ -n "$forbidden_matches" ]]; then
    echo "architecture boundary violation: shadow-planner-is-data-only" >&2
    printf '%s\n' "$forbidden_matches" | sed 's/^/  /' >&2
    return 1
  fi

  public_module_matches="$(
    source_matches 'pub(\([^)]*\))?[[:space:]]+mod[[:space:]]+' "$shadow_module" "$shadow_root"
  )"
  if [[ -n "$public_module_matches" ]]; then
    echo "architecture boundary violation: shadow-planner-modules-are-sealed" >&2
    printf '%s\n' "$public_module_matches" | sed 's/^/  /' >&2
    return 1
  fi

  observer_count="$(grep -Ec '^mod[[:space:]]+observer;' "$shadow_module" || true)"
  if [[ "$observer_count" -ne 1 ]]; then
    echo "architecture boundary violation: shadow-planner-modules-are-sealed" >&2
    echo "  expected one private observer module declaration in $shadow_module" >&2
    return 1
  fi
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

check_run_stream_tool_dispatch_boundary || failed=true
check_legacy_run_stream_freeze || failed=true
check_legacy_retirement_contract || failed=true
check_v2_tool_authority_boundary || failed=true
check_shadow_planner_boundary || failed=true

if [[ "$failed" == true ]]; then
  exit 1
fi

echo "architecture boundary checks passed"
