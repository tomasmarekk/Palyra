//! Host-owned tool governance helpers for the run-stream path.
//!
//! This module keeps Phase 3 governance state machine pieces close to the
//! existing tool execution flow without creating a second executor. The
//! helpers build deterministic reports, signatures, synthetic results, and
//! middleware projections that callers can attach to tape/journal events.

use std::collections::BTreeMap;

use palyra_common::runtime_contracts::{
    validate_tool_result_visibility_downgrade, ToolResultVisibility,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::{
    application::tool_registry::{canonical_json_bytes, stable_hash_bytes},
    tool_protocol::{build_tool_execution_outcome, ToolExecutionOutcome},
};

const TOOL_GOVERNANCE_SCHEMA_VERSION: u32 = 1;
const DEFAULT_REPEATED_FAILURE_LIMIT: u32 = 3;
const MAX_MIDDLEWARE_JSON_DEPTH: usize = 12;
const MAX_MIDDLEWARE_OBJECT_KEYS: usize = 256;
const MAX_MIDDLEWARE_TEXT_CHARS: usize = 64 * 1024;

/// Final decision produced before a normalized tool call may ask for
/// approval or dispatch.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum BeforeToolDecisionKind {
    Allow,
    Block,
    RequireApproval,
    RequireReread,
    RequireSmallerPatch,
    SynthesizeResult,
    FailRun,
}

impl BeforeToolDecisionKind {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Allow => "allow",
            Self::Block => "block",
            Self::RequireApproval => "require_approval",
            Self::RequireReread => "require_reread",
            Self::RequireSmallerPatch => "require_smaller_patch",
            Self::SynthesizeResult => "synthesize_result",
            Self::FailRun => "fail_run",
        }
    }

    pub(crate) const fn allows_dispatch(self) -> bool {
        matches!(self, Self::Allow | Self::RequireApproval)
    }
}

/// Stable identity for one proposed tool call.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ToolCallSignature {
    pub(crate) schema_version: u32,
    pub(crate) tool_name: String,
    pub(crate) normalized_args_hash: String,
    pub(crate) derived_path_scope: Option<String>,
    pub(crate) network_targets: Vec<String>,
    pub(crate) mutability_class: String,
}

/// One stage in the before-tool decision pipeline.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct BeforeToolDecisionStep {
    pub(crate) step_id: String,
    pub(crate) decision: BeforeToolDecisionKind,
    pub(crate) reason_code: String,
    pub(crate) summary: String,
}

/// Full report for the before-tool pipeline.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct BeforeToolDecisionReport {
    pub(crate) schema_version: u32,
    pub(crate) signature: ToolCallSignature,
    pub(crate) final_decision: BeforeToolDecisionKind,
    pub(crate) final_reason_code: String,
    pub(crate) steps: Vec<BeforeToolDecisionStep>,
}

/// Input for a one-shot before-tool pipeline evaluation.
#[derive(Debug, Clone)]
pub(crate) struct BeforeToolDecisionInput<'a> {
    pub(crate) tool_name: &'a str,
    pub(crate) normalized_input_json: &'a [u8],
    pub(crate) hook_decision: Option<BeforeToolDecisionKind>,
    pub(crate) hook_reason: Option<&'a str>,
    pub(crate) guardrail_decision: Option<ToolGuardrailDecision>,
}

/// Builds the canonical signature used by policy, approval, and guardrail reports.
pub(crate) fn build_tool_call_signature(
    tool_name: &str,
    normalized_input_json: &[u8],
) -> ToolCallSignature {
    let canonical_args = serde_json::from_slice::<Value>(normalized_input_json)
        .map(|value| canonical_json_bytes(&value))
        .unwrap_or_else(|_| normalized_input_json.to_vec());
    let value = serde_json::from_slice::<Value>(normalized_input_json).unwrap_or(Value::Null);
    ToolCallSignature {
        schema_version: TOOL_GOVERNANCE_SCHEMA_VERSION,
        tool_name: tool_name.to_owned(),
        normalized_args_hash: stable_hash_bytes(canonical_args.as_slice()),
        derived_path_scope: derived_path_scope(&value),
        network_targets: network_targets(&value),
        mutability_class: mutability_class_for_tool(tool_name).to_owned(),
    }
}

/// Evaluates the host-owned before-tool pipeline up to the existing policy/approval gates.
pub(crate) fn evaluate_before_tool_decision_pipeline(
    input: BeforeToolDecisionInput<'_>,
) -> BeforeToolDecisionReport {
    let signature = build_tool_call_signature(input.tool_name, input.normalized_input_json);
    let mut steps = vec![BeforeToolDecisionStep {
        step_id: "signature".to_owned(),
        decision: BeforeToolDecisionKind::Allow,
        reason_code: "tool.signature.ready".to_owned(),
        summary: "Tool name, normalized args hash, path scope, network targets, and mutability were derived.".to_owned(),
    }];

    if let Some(guardrail_decision) = input.guardrail_decision {
        steps.push(BeforeToolDecisionStep {
            step_id: "guardrail".to_owned(),
            decision: guardrail_decision.kind,
            reason_code: guardrail_decision.reason_code,
            summary: guardrail_decision.summary,
        });
    } else {
        steps.push(BeforeToolDecisionStep {
            step_id: "guardrail".to_owned(),
            decision: BeforeToolDecisionKind::Allow,
            reason_code: "tool.guardrail.clear".to_owned(),
            summary: "No repeated failure dampening was needed for this proposal.".to_owned(),
        });
    }

    if let Some(hook_decision) = input.hook_decision {
        steps.push(BeforeToolDecisionStep {
            step_id: "hook".to_owned(),
            decision: hook_decision,
            reason_code: match hook_decision {
                BeforeToolDecisionKind::RequireApproval => "hook.requested_approval",
                BeforeToolDecisionKind::Block => "hook.blocked",
                BeforeToolDecisionKind::FailRun => "hook.failed_run",
                _ => "hook.decision",
            }
            .to_owned(),
            summary: input
                .hook_reason
                .unwrap_or("Inline hook returned a host-interpreted decision.")
                .to_owned(),
        });
    } else {
        steps.push(BeforeToolDecisionStep {
            step_id: "hook".to_owned(),
            decision: BeforeToolDecisionKind::Allow,
            reason_code: "hook.no_terminal_decision".to_owned(),
            summary: "No inline hook blocked, rewrote, or requested approval for this proposal."
                .to_owned(),
        });
    }

    let terminal = steps
        .iter()
        .rev()
        .find(|step| {
            step.decision == BeforeToolDecisionKind::RequireApproval
                || !step.decision.allows_dispatch()
        })
        .cloned();
    let (final_decision, final_reason_code) = terminal
        .map_or((BeforeToolDecisionKind::Allow, "tool.before_policy.allowed".to_owned()), |step| {
            (step.decision, step.reason_code)
        });

    BeforeToolDecisionReport {
        schema_version: TOOL_GOVERNANCE_SCHEMA_VERSION,
        signature,
        final_decision,
        final_reason_code,
        steps,
    }
}

/// Per-run dampening controller for repeated tool failures.
#[derive(Debug, Clone)]
pub(crate) struct ToolGuardrailController {
    limit: u32,
    failures: BTreeMap<String, ToolGuardrailFailureEntry>,
}

impl Default for ToolGuardrailController {
    fn default() -> Self {
        Self { limit: DEFAULT_REPEATED_FAILURE_LIMIT, failures: BTreeMap::new() }
    }
}

impl ToolGuardrailController {
    pub(crate) fn before_tool(
        &self,
        signature: &ToolCallSignature,
    ) -> Option<ToolGuardrailDecision> {
        let entry = self.failures.get(signature_key(signature).as_str())?;
        (entry.repetitions >= self.limit).then(|| ToolGuardrailDecision {
            kind: BeforeToolDecisionKind::SynthesizeResult,
            reason_code: "tool.guardrail.repeated_failure_limit".to_owned(),
            summary: format!(
                "Identical failed tool call was observed {} times; returning host-generated remediation instead of dispatching again.",
                entry.repetitions
            ),
            synthetic_result: Some(SyntheticToolResult {
                reason_code: "tool.guardrail.repeated_failure_limit".to_owned(),
                message: "Host stopped an identical failing tool call. Re-read current state, repair the arguments, or choose a different tool before retrying.".to_owned(),
                remediation: entry.last_remediation.clone(),
            }),
        })
    }

    pub(crate) fn observe_tool_result(
        &mut self,
        signature: &ToolCallSignature,
        success: bool,
        failure_reason: Option<&str>,
    ) {
        let key = signature_key(signature);
        if success {
            self.failures.remove(key.as_str());
            return;
        }
        let entry = self.failures.entry(key).or_insert_with(|| ToolGuardrailFailureEntry {
            repetitions: 0,
            last_reason: String::new(),
            last_remediation: "Use fresh context and normalized arguments before retrying."
                .to_owned(),
        });
        entry.repetitions = entry.repetitions.saturating_add(1);
        entry.last_reason = failure_reason.unwrap_or("tool failure").to_owned();
        if entry.last_reason.contains("schema") || entry.last_reason.contains("argument") {
            entry.last_remediation =
                "Validate the tool schema and send the minimal corrected JSON arguments."
                    .to_owned();
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ToolGuardrailFailureEntry {
    repetitions: u32,
    last_reason: String,
    last_remediation: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ToolGuardrailDecision {
    pub(crate) kind: BeforeToolDecisionKind,
    pub(crate) reason_code: String,
    pub(crate) summary: String,
    pub(crate) synthetic_result: Option<SyntheticToolResult>,
}

/// Host-authored synthetic result returned instead of dispatching a known-bad proposal.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct SyntheticToolResult {
    pub(crate) reason_code: String,
    pub(crate) message: String,
    pub(crate) remediation: String,
}

pub(crate) fn synthetic_tool_result_outcome(
    proposal_id: &str,
    tool_name: &str,
    input_json: &[u8],
    result: &SyntheticToolResult,
) -> ToolExecutionOutcome {
    let output_json = serde_json::to_vec(&json!({
        "schema_version": TOOL_GOVERNANCE_SCHEMA_VERSION,
        "host_generated": true,
        "kind": "synthetic_tool_result",
        "reason_code": result.reason_code,
        "message": result.message,
        "remediation": result.remediation,
    }))
    .unwrap_or_else(|_| b"{}".to_vec());
    build_tool_execution_outcome(
        proposal_id,
        tool_name,
        input_json,
        false,
        output_json,
        result.message.clone(),
        false,
        "tool_guardrail".to_owned(),
        "host_policy".to_owned(),
    )
}

/// Middleware class controls timeout/failure posture.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ToolResultMiddlewareClass {
    Presentation,
    Redaction,
    MemoryIngest,
    ArtifactPolicy,
    NativeMirror,
}

impl ToolResultMiddlewareClass {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Presentation => "presentation",
            Self::Redaction => "redaction",
            Self::MemoryIngest => "memory_ingest",
            Self::ArtifactPolicy => "artifact_policy",
            Self::NativeMirror => "native_mirror",
        }
    }

    pub(crate) const fn fail_closed(self) -> bool {
        matches!(self, Self::Redaction | Self::ArtifactPolicy)
    }
}

/// One middleware validation/audit step.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ToolResultMiddlewareStep {
    pub(crate) class: ToolResultMiddlewareClass,
    pub(crate) plugin_id: String,
    pub(crate) input_digest: String,
    pub(crate) output_digest: String,
    pub(crate) visibility_before: ToolResultVisibility,
    pub(crate) visibility_after: ToolResultVisibility,
    pub(crate) visibility: ToolResultVisibility,
    pub(crate) failure_posture: String,
    pub(crate) reason_code: String,
}

/// Result of applying middleware before model-visible projection.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ToolResultMiddlewareReport {
    pub(crate) schema_version: u32,
    pub(crate) canonical_output_digest: String,
    pub(crate) model_visible_output_digest: String,
    pub(crate) visibility: ToolResultVisibility,
    pub(crate) steps: Vec<ToolResultMiddlewareStep>,
}

/// Validates and records the host-owned middleware chain.
pub(crate) fn apply_host_tool_result_middleware(
    tool_name: &str,
    output_json: &[u8],
    visibility: ToolResultVisibility,
) -> Result<ToolResultMiddlewareReport, String> {
    let value = serde_json::from_slice::<Value>(output_json)
        .map_err(|error| format!("tool result middleware input must be JSON safe: {error}"))?;
    validate_middleware_shape(&value, 0)?;
    let digest = stable_hash_bytes(canonical_json_bytes(&value).as_slice());
    let mut current_visibility = visibility;
    let mut steps = Vec::new();
    for middleware_class in host_tool_result_middleware_chain() {
        let visibility_before = current_visibility;
        let requested_visibility = visibility_before;
        let visibility_after =
            validate_tool_result_visibility_downgrade(visibility_before, requested_visibility)
                .map_err(|error| format!("{}: {}", error.code, error.message))?;
        current_visibility = visibility_after;
        let failure_posture =
            if middleware_class.fail_closed() { "fail_closed" } else { "fail_open" };
        steps.push(ToolResultMiddlewareStep {
            class: middleware_class,
            plugin_id: format!("host.{}", middleware_class.as_str()),
            input_digest: digest.clone(),
            output_digest: digest.clone(),
            visibility_before,
            visibility_after,
            visibility: visibility_after,
            failure_posture: failure_posture.to_owned(),
            reason_code: format!(
                "tool_result_middleware.host_{}.{failure_posture}.{tool_name}",
                middleware_class.as_str()
            ),
        });
    }
    Ok(ToolResultMiddlewareReport {
        schema_version: TOOL_GOVERNANCE_SCHEMA_VERSION,
        canonical_output_digest: digest.clone(),
        model_visible_output_digest: digest.clone(),
        visibility: current_visibility,
        steps,
    })
}

/// Event passed to a before-finalize gate.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct BeforeFinalizeEvent {
    pub(crate) schema_version: u32,
    pub(crate) last_assistant_message_hash: String,
    pub(crate) pending_tool_count: usize,
    pub(crate) side_effect_summary: String,
    pub(crate) verification_state: String,
    pub(crate) final_answer_contract_status: String,
}

impl BeforeFinalizeEvent {
    pub(crate) fn new(
        last_assistant_message: &str,
        pending_tool_count: usize,
        side_effect_summary: impl Into<String>,
        verification_state: impl Into<String>,
        final_answer_contract_status: impl Into<String>,
    ) -> Self {
        Self {
            schema_version: TOOL_GOVERNANCE_SCHEMA_VERSION,
            last_assistant_message_hash: stable_hash_bytes(last_assistant_message.as_bytes()),
            pending_tool_count,
            side_effect_summary: side_effect_summary.into(),
            verification_state: verification_state.into(),
            final_answer_contract_status: final_answer_contract_status.into(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum BeforeFinalizeDecisionKind {
    Finalize,
    Revise,
    Continue,
    Block,
    FailRun,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct BeforeFinalizeDecision {
    pub(crate) kind: BeforeFinalizeDecisionKind,
    pub(crate) reason_code: String,
    pub(crate) instruction: Option<String>,
    pub(crate) remaining_attempts: u32,
}

/// Per-run budget for before-finalize revise decisions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct BeforeFinalizeBudget {
    max_attempts: u32,
    attempts: BTreeMap<String, u32>,
}

impl BeforeFinalizeBudget {
    pub(crate) fn new(max_attempts: u32) -> Self {
        Self { max_attempts: max_attempts.max(1), attempts: BTreeMap::new() }
    }

    pub(crate) fn decide(
        &mut self,
        event: &BeforeFinalizeEvent,
        instruction_key: &str,
        instruction: &str,
    ) -> BeforeFinalizeDecision {
        if event.pending_tool_count > 0 {
            return BeforeFinalizeDecision {
                kind: BeforeFinalizeDecisionKind::Continue,
                reason_code: "finalize.pending_tools".to_owned(),
                instruction: None,
                remaining_attempts: self.max_attempts,
            };
        }
        if event.final_answer_contract_status == "ok" {
            return BeforeFinalizeDecision {
                kind: BeforeFinalizeDecisionKind::Finalize,
                reason_code: "finalize.contract_ok".to_owned(),
                instruction: None,
                remaining_attempts: self.remaining_attempts(instruction_key),
            };
        }
        let used = self.attempts.entry(instruction_key.to_owned()).or_insert(0);
        if *used >= self.max_attempts {
            return BeforeFinalizeDecision {
                kind: BeforeFinalizeDecisionKind::Block,
                reason_code: "finalize.revise_budget_exhausted".to_owned(),
                instruction: None,
                remaining_attempts: 0,
            };
        }
        *used = used.saturating_add(1);
        BeforeFinalizeDecision {
            kind: BeforeFinalizeDecisionKind::Revise,
            reason_code: "finalize.revise_required".to_owned(),
            instruction: Some(instruction.to_owned()),
            remaining_attempts: self.max_attempts.saturating_sub(*used),
        }
    }

    fn remaining_attempts(&self, instruction_key: &str) -> u32 {
        self.max_attempts.saturating_sub(*self.attempts.get(instruction_key).unwrap_or(&0))
    }
}

/// Runtime context used to adapt the tool surface for a selected harness.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct HarnessToolSurfaceRuntime {
    pub(crate) harness_id: String,
    pub(crate) provider_id: String,
    pub(crate) model_id: String,
    pub(crate) context_budget_tokens: u64,
    pub(crate) runtime_policy: String,
    pub(crate) tool_policy: String,
    pub(crate) sandbox_posture: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum HarnessToolSurfaceMode {
    Direct,
    CompactCatalogBridge,
    Hybrid,
    CodeModeFacade,
    Lean,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct HarnessToolSurfaceProjection {
    pub(crate) schema_version: u32,
    pub(crate) mode: HarnessToolSurfaceMode,
    pub(crate) compatible_tool_names: Vec<String>,
    pub(crate) filtered_tool_names: Vec<String>,
    pub(crate) tool_surface_hash: String,
}

pub(crate) fn project_harness_tool_surface(
    runtime: &HarnessToolSurfaceRuntime,
    tool_names: &[String],
) -> HarnessToolSurfaceProjection {
    let lean = runtime.context_budget_tokens < 4_000 || runtime.model_id.contains("mini");
    let no_mutating = runtime.runtime_policy.contains("no_mutating");
    let mut compatible_tool_names = Vec::new();
    let mut filtered_tool_names = Vec::new();
    for tool_name in tool_names {
        if no_mutating && mutability_class_for_tool(tool_name) != "read_only" {
            filtered_tool_names.push(tool_name.clone());
        } else {
            compatible_tool_names.push(tool_name.clone());
        }
    }
    compatible_tool_names.sort();
    filtered_tool_names.sort();
    let mode = if lean {
        HarnessToolSurfaceMode::Lean
    } else if compatible_tool_names.len() > 16 {
        HarnessToolSurfaceMode::CompactCatalogBridge
    } else {
        HarnessToolSurfaceMode::Direct
    };
    let hash_payload = json!({
        "runtime": runtime,
        "mode": mode,
        "compatible_tool_names": compatible_tool_names,
        "filtered_tool_names": filtered_tool_names,
    });
    let tool_surface_hash = stable_hash_bytes(canonical_json_bytes(&hash_payload).as_slice());
    HarnessToolSurfaceProjection {
        schema_version: TOOL_GOVERNANCE_SCHEMA_VERSION,
        mode,
        compatible_tool_names,
        filtered_tool_names,
        tool_surface_hash,
    }
}

fn signature_key(signature: &ToolCallSignature) -> String {
    format!(
        "{}:{}:{}",
        signature.tool_name, signature.normalized_args_hash, signature.mutability_class
    )
}

fn derived_path_scope(value: &Value) -> Option<String> {
    let mut paths = Vec::new();
    collect_string_field(value.get("path"), &mut paths);
    collect_string_field(value.get("paths"), &mut paths);
    collect_string_field(value.get("cwd"), &mut paths);
    collect_string_field(value.get("workspace_root"), &mut paths);
    paths.sort();
    paths.dedup();
    (!paths.is_empty()).then(|| paths.join("|"))
}

fn network_targets(value: &Value) -> Vec<String> {
    let mut targets = Vec::new();
    collect_string_field(value.get("url"), &mut targets);
    collect_string_field(value.get("urls"), &mut targets);
    collect_string_field(value.get("host"), &mut targets);
    collect_string_field(value.get("hosts"), &mut targets);
    targets.sort();
    targets.dedup();
    targets
}

fn host_tool_result_middleware_chain() -> [ToolResultMiddlewareClass; 5] {
    [
        ToolResultMiddlewareClass::Redaction,
        ToolResultMiddlewareClass::ArtifactPolicy,
        ToolResultMiddlewareClass::Presentation,
        ToolResultMiddlewareClass::MemoryIngest,
        ToolResultMiddlewareClass::NativeMirror,
    ]
}

fn collect_string_field(value: Option<&Value>, values: &mut Vec<String>) {
    match value {
        Some(Value::String(value)) if !value.trim().is_empty() => {
            values.push(value.trim().replace('\\', "/"));
        }
        Some(Value::Array(items)) => {
            for item in items {
                collect_string_field(Some(item), values);
            }
        }
        _ => {}
    }
}

fn mutability_class_for_tool(tool_name: &str) -> &'static str {
    if tool_name.contains("read")
        || tool_name.contains("list")
        || tool_name.contains("search")
        || tool_name == "palyra.echo"
    {
        "read_only"
    } else if tool_name.contains("apply_patch")
        || tool_name.contains("write")
        || tool_name.contains("delete")
        || tool_name.contains("process")
    {
        "mutating"
    } else {
        "unknown"
    }
}

fn validate_middleware_shape(value: &Value, depth: usize) -> Result<(), String> {
    if depth > MAX_MIDDLEWARE_JSON_DEPTH {
        return Err("tool result middleware output exceeds maximum JSON depth".to_owned());
    }
    match value {
        Value::Object(fields) => {
            if fields.len() > MAX_MIDDLEWARE_OBJECT_KEYS {
                return Err("tool result middleware output has too many object keys".to_owned());
            }
            for child in fields.values() {
                validate_middleware_shape(child, depth.saturating_add(1))?;
            }
        }
        Value::Array(values) => {
            for child in values {
                validate_middleware_shape(child, depth.saturating_add(1))?;
            }
        }
        Value::String(value) if value.chars().count() > MAX_MIDDLEWARE_TEXT_CHARS => {
            return Err("tool result middleware output has an oversized text field".to_owned());
        }
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => {}
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tool_call_signature_is_stable_for_reordered_json_args() {
        let left = build_tool_call_signature(
            "palyra.fs.read_file",
            br#"{"path":"src/lib.rs","max_bytes":128}"#,
        );
        let right = build_tool_call_signature(
            "palyra.fs.read_file",
            br#"{"max_bytes":128,"path":"src/lib.rs"}"#,
        );

        assert_eq!(left.normalized_args_hash, right.normalized_args_hash);
        assert_eq!(left.derived_path_scope.as_deref(), Some("src/lib.rs"));
        assert_eq!(left.mutability_class, "read_only");
    }

    #[test]
    fn repeated_failure_guardrail_synthesizes_after_limit() {
        let signature = build_tool_call_signature(
            "palyra.fs.apply_patch",
            br#"{"patch":"*** Begin Patch\n*** Update File: src/lib.rs\n@@\n-old\n+new\n*** End Patch\n"}"#,
        );
        let mut controller = ToolGuardrailController::default();
        assert!(controller.before_tool(&signature).is_none());

        controller.observe_tool_result(&signature, false, Some("schema invalid"));
        controller.observe_tool_result(&signature, false, Some("schema invalid"));
        assert!(controller.before_tool(&signature).is_none());
        controller.observe_tool_result(&signature, false, Some("schema invalid"));
        let decision = controller
            .before_tool(&signature)
            .expect("third identical failure should synthesize guidance");

        assert_eq!(decision.kind, BeforeToolDecisionKind::SynthesizeResult);
        assert_eq!(decision.reason_code, "tool.guardrail.repeated_failure_limit");
        assert!(decision.synthetic_result.is_some());
    }

    #[test]
    fn middleware_shape_validation_bounds_deep_or_oversized_output() {
        let oversized = json!({"message": "x".repeat(MAX_MIDDLEWARE_TEXT_CHARS + 1)});
        let error = apply_host_tool_result_middleware(
            "palyra.fs.read_file",
            serde_json::to_vec(&oversized).expect("payload should serialize").as_slice(),
            ToolResultVisibility::ModelInline,
        )
        .expect_err("oversized strings should fail middleware validation");

        assert!(error.contains("oversized text field"), "{error}");
    }

    #[test]
    fn middleware_chain_records_visibility_downgrade_invariant() {
        let report = apply_host_tool_result_middleware(
            "palyra.fs.read_file",
            br#"{"content":"bounded"}"#,
            ToolResultVisibility::ModelSummary,
        )
        .expect("bounded result should pass middleware");

        assert_eq!(report.steps.len(), 5);
        assert_eq!(report.steps[0].class, ToolResultMiddlewareClass::Redaction);
        assert_eq!(report.steps[1].class, ToolResultMiddlewareClass::ArtifactPolicy);
        assert_eq!(report.steps[2].class, ToolResultMiddlewareClass::Presentation);
        assert!(report.steps.iter().all(|step| {
            step.visibility_after.model_visibility_rank()
                <= step.visibility_before.model_visibility_rank()
        }));
        assert!(report.steps.iter().any(|step| step.failure_posture == "fail_closed"));
    }

    #[test]
    fn before_finalize_budget_prevents_infinite_revisions() {
        let event = BeforeFinalizeEvent::new("", 0, "none", "unverified", "empty");
        let mut budget = BeforeFinalizeBudget::new(1);
        let revise = budget.decide(&event, "empty", "write a final answer");
        let blocked = budget.decide(&event, "empty", "write a final answer");

        assert_eq!(revise.kind, BeforeFinalizeDecisionKind::Revise);
        assert_eq!(blocked.kind, BeforeFinalizeDecisionKind::Block);
        assert_eq!(blocked.reason_code, "finalize.revise_budget_exhausted");
    }

    #[test]
    fn harness_surface_projection_filters_mutating_tools_for_policy() {
        let runtime = HarnessToolSurfaceRuntime {
            harness_id: "embedded_palyra".to_owned(),
            provider_id: "test".to_owned(),
            model_id: "mini-model".to_owned(),
            context_budget_tokens: 2_000,
            runtime_policy: "no_mutating".to_owned(),
            tool_policy: "catalog".to_owned(),
            sandbox_posture: "local".to_owned(),
        };
        let projection = project_harness_tool_surface(
            &runtime,
            &["palyra.fs.read_file".to_owned(), "palyra.fs.apply_patch".to_owned()],
        );

        assert_eq!(projection.mode, HarnessToolSurfaceMode::Lean);
        assert_eq!(projection.compatible_tool_names, vec!["palyra.fs.read_file"]);
        assert_eq!(projection.filtered_tool_names, vec!["palyra.fs.apply_patch"]);
        assert_eq!(projection.tool_surface_hash.len(), 64);
    }
}
