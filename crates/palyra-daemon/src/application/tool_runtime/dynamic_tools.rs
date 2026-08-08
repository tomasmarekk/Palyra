//! Restricted execution for host-approved signed dynamic-tool versions.

use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

use palyra_plugins_runtime::WasmRuntime;
use palyra_plugins_sdk::DEFAULT_RUNTIME_ENTRYPOINT;
use palyra_skills::{
    decide_dynamic_tool_activation, dynamic_tool_capability_grants,
    dynamic_tool_eval_input_fixture, dynamic_tool_malformed_eval_fixture,
    dynamic_tool_output_contains_secret, dynamic_tool_runtime_eval_evidence_sha256,
    parse_dynamic_declarative_plan, render_dynamic_tool_template, validate_dynamic_tool_input,
    verify_dynamic_tool_authority, verify_signed_dynamic_tool_artifact, DeclarativeToolPlanV1,
    DynamicToolHostGate, DynamicToolImplementationType, DynamicToolRuntimeEvalEvidenceV1,
};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

use crate::{
    gateway::{
        execute_tool_with_runtime_dispatch_with_cancellation_and_progress,
        tool_cancellation_requires_execution_drain, GatewayRuntimeState,
        ToolRuntimeDispatchControls, ToolRuntimeExecutionContext,
    },
    tool_protocol::{
        build_tool_execution_outcome, decide_tool_call, tool_requires_approval,
        ToolExecutionOutcome, ToolRequestContext,
    },
};

/// Executes one exact active version with no unsigned or generic fallback.
pub(crate) async fn execute_dynamic_tool(
    runtime_state: &Arc<GatewayRuntimeState>,
    context: ToolRuntimeExecutionContext<'_>,
    proposal_id: &str,
    tool_name: &str,
    input_json: &[u8],
    controls: ToolRuntimeDispatchControls,
) -> ToolExecutionOutcome {
    if !runtime_state.config.feature_rollouts.dynamic_tool_builder.enabled {
        return denied(proposal_id, tool_name, input_json, "dynamic_tool.rollout_disabled", false);
    }
    let Some(expected_provenance) = controls.expected_dynamic_provenance.as_deref() else {
        return denied(
            proposal_id,
            tool_name,
            input_json,
            "dynamic_tool.catalog_binding_missing",
            false,
        );
    };
    let record = match runtime_state.journal_store.active_dynamic_tool(tool_name) {
        Ok(Some(record)) => record,
        Ok(None) => {
            return denied(
                proposal_id,
                tool_name,
                input_json,
                "dynamic_tool.registry_entry_missing",
                false,
            );
        }
        Err(_) => {
            return denied(
                proposal_id,
                tool_name,
                input_json,
                "dynamic_tool.registry_unavailable",
                false,
            );
        }
    };
    if !dynamic_tool_provenance_is_current(
        expected_provenance,
        crate::application::tool_registry::dynamic_tool_record_provenance(&record).as_str(),
    ) {
        return denied(
            proposal_id,
            tool_name,
            input_json,
            "dynamic_tool.catalog_binding_stale",
            false,
        );
    }
    if verify_signed_dynamic_tool_artifact(&record.artifact).is_err()
        || record.artifact.proposal.tool_name != tool_name
        || record.decision.artifact_sha256 != record.artifact.artifact_sha256
    {
        return denied(
            proposal_id,
            tool_name,
            input_json,
            "dynamic_tool.artifact_verification_failed",
            false,
        );
    }
    let input = match serde_json::from_slice::<Value>(input_json) {
        Ok(input) => input,
        Err(_) => {
            return denied(proposal_id, tool_name, input_json, "dynamic_tool.input_invalid", false);
        }
    };
    if validate_dynamic_tool_input(&record.artifact.proposal.input_schema, &input).is_err() {
        return denied(
            proposal_id,
            tool_name,
            input_json,
            "dynamic_tool.input_schema_rejected",
            false,
        );
    }
    let started = Instant::now();
    let result = match record.artifact.proposal.implementation_type {
        DynamicToolImplementationType::DeclarativeComposition => {
            execute_declarative(
                runtime_state,
                context,
                DeclarativeExecutionRequest {
                    proposal_id,
                    artifact: &record.artifact,
                    input: &input,
                    controls,
                    started,
                    mode: DynamicExecutionMode::Production,
                },
            )
            .await
        }
        DynamicToolImplementationType::WasmComponent => {
            execute_wasm(&record.artifact, controls.cancellation_requested.as_ref(), started).await
        }
    };
    let output = match result {
        Ok(output) => output,
        Err(failure) => {
            return denied(
                proposal_id,
                tool_name,
                input_json,
                failure.reason_code,
                failure.timed_out,
            );
        }
    };
    if validate_dynamic_tool_input(&record.artifact.proposal.output_schema, &output).is_err() {
        return denied(
            proposal_id,
            tool_name,
            input_json,
            "dynamic_tool.output_schema_rejected",
            false,
        );
    }
    let output_json = serde_json::to_vec(&output).unwrap_or_else(|_| b"{}".to_vec());
    if dynamic_tool_output_contains_secret(output_json.as_slice()) {
        return denied(
            proposal_id,
            tool_name,
            input_json,
            "dynamic_tool.secret_output_blocked",
            false,
        );
    }
    build_tool_execution_outcome(
        proposal_id,
        tool_name,
        input_json,
        true,
        output_json,
        String::new(),
        false,
        "signed_dynamic_tool".to_owned(),
        "artifact_capability_scoped".to_owned(),
    )
}

struct DeclarativeExecutionRequest<'a> {
    proposal_id: &'a str,
    artifact: &'a palyra_skills::SignedToolArtifact,
    input: &'a Value,
    controls: ToolRuntimeDispatchControls,
    started: Instant,
    mode: DynamicExecutionMode,
}

async fn execute_declarative(
    runtime_state: &Arc<GatewayRuntimeState>,
    context: ToolRuntimeExecutionContext<'_>,
    request: DeclarativeExecutionRequest<'_>,
) -> Result<Value, DynamicFailure> {
    let DeclarativeExecutionRequest { proposal_id, artifact, input, controls, started, mode } =
        request;
    let plan = parse_dynamic_declarative_plan(
        &artifact.proposal,
        artifact.implementation_bytes.as_slice(),
    )
    .map_err(|_| DynamicFailure::new("dynamic_tool.plan_invalid", false))?;
    let mut last_output = json!({});
    for step in plan.steps {
        if step.tool_name.starts_with("dynamic.") || step.tool_name == artifact.proposal.tool_name {
            return Err(DynamicFailure::new("dynamic_tool.dynamic_chaining_denied", false));
        }
        if controls.cancellation_requested.as_ref().is_some_and(|flag| flag.load(Ordering::Acquire))
        {
            return Err(DynamicFailure::new("dynamic_tool.cancelled", false));
        }
        let elapsed_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX);
        if elapsed_ms >= artifact.proposal.semantics.max_execution_ms {
            return Err(DynamicFailure::new("dynamic_tool.timeout", true));
        }
        let child_input = render_dynamic_tool_template(&step.input_template, input)
            .map_err(|_| DynamicFailure::new("dynamic_tool.template_invalid", false))?;
        let child_input_json = serde_json::to_vec(&child_input)
            .map_err(|_| DynamicFailure::new("dynamic_tool.template_invalid", false))?;
        let request_context = ToolRequestContext {
            principal: context.principal.to_owned(),
            device_id: Some(context.device_id.to_owned()),
            channel: context.channel.map(ToOwned::to_owned),
            session_id: Some(context.session_id.to_owned()),
            run_id: Some(context.run_id.to_owned()),
            skill_id: Some(artifact.artifact_sha256.clone()),
        };
        let mut legacy_budget = 0;
        let decision = decide_tool_call(
            &runtime_state.config.tool_call,
            &mut legacy_budget,
            &request_context,
            step.tool_name.as_str(),
            false,
        );
        if !decision.allowed
            || decision.approval_required
            || tool_requires_approval(&step.tool_name)
            || !declarative_child_is_timeout_drop_safe(&step.tool_name)
        {
            return Err(DynamicFailure::new("dynamic_tool.child_policy_or_approval_denied", false));
        }
        match mode {
            DynamicExecutionMode::Production => {
                let child_proposal_id = child_proposal_id(proposal_id, step.step_id.as_str());
                let remaining_ms =
                    artifact.proposal.semantics.max_execution_ms.saturating_sub(elapsed_ms);
                let child = tokio::time::timeout(
                    Duration::from_millis(step.timeout_ms.min(remaining_ms)),
                    Box::pin(execute_tool_with_runtime_dispatch_with_cancellation_and_progress(
                        runtime_state,
                        context,
                        child_proposal_id.as_str(),
                        step.tool_name.as_str(),
                        child_input_json.as_slice(),
                        ToolRuntimeDispatchControls {
                            remaining_tool_budget: controls.remaining_tool_budget.clone(),
                            cancellation_requested: controls.cancellation_requested.clone(),
                            process_progress_sink: None,
                            cancellation_context: controls.cancellation_context.clone(),
                            child_task_parent_context: None,
                            expected_dynamic_provenance: None,
                        },
                    )),
                )
                .await
                .map_err(|_| DynamicFailure::new("dynamic_tool.timeout", true))?;
                if !child.success {
                    return Err(DynamicFailure::new(
                        "dynamic_tool.child_failed",
                        child.attestation.timed_out,
                    ));
                }
                if dynamic_tool_output_contains_secret(child.output_json.as_slice()) {
                    return Err(DynamicFailure::new("dynamic_tool.secret_output_blocked", false));
                }
                last_output = serde_json::from_slice(child.output_json.as_slice())
                    .map_err(|_| DynamicFailure::new("dynamic_tool.child_output_invalid", false))?;
            }
            DynamicExecutionMode::Evaluation => {
                if step.tool_name != "palyra.echo" {
                    return Err(DynamicFailure::new(
                        "dynamic_tool.eval_adapter_unavailable",
                        false,
                    ));
                }
                last_output = child_input;
            }
        }
    }
    Ok(last_output)
}

async fn execute_wasm(
    artifact: &palyra_skills::SignedToolArtifact,
    cancellation: Option<&Arc<std::sync::atomic::AtomicBool>>,
    started: Instant,
) -> Result<Value, DynamicFailure> {
    if cancellation.is_some_and(|flag| flag.load(Ordering::Acquire)) {
        return Err(DynamicFailure::new("dynamic_tool.cancelled", false));
    }
    let bytes = artifact.implementation_bytes.clone();
    let grants = dynamic_tool_capability_grants(&artifact.proposal);
    let timeout = Duration::from_millis(artifact.proposal.semantics.max_execution_ms);
    let execution = tokio::task::spawn_blocking(move || {
        WasmRuntime::new()
            .map_err(|_| DynamicFailure::new("dynamic_tool.wasm_runtime_unavailable", false))?
            .execute_i32_entrypoint_with_timeout(
                bytes.as_slice(),
                DEFAULT_RUNTIME_ENTRYPOINT,
                &grants,
                timeout,
            )
            .map_err(|error| {
                let timed_out = error.to_string().to_ascii_lowercase().contains("timed out");
                DynamicFailure::new(
                    if timed_out {
                        "dynamic_tool.timeout"
                    } else {
                        "dynamic_tool.wasm_execution_failed"
                    },
                    timed_out,
                )
            })
    })
    .await
    .map_err(|_| DynamicFailure::new("dynamic_tool.wasm_worker_failed", false))??;
    if started.elapsed() > timeout {
        return Err(DynamicFailure::new("dynamic_tool.timeout", true));
    }
    Ok(json!({"exit_code": execution.exit_code}))
}

fn declarative_child_is_timeout_drop_safe(tool_name: &str) -> bool {
    // The per-step deadline drops its dispatch future, so process-owning and
    // nested runtimes must stay on the standard approval path instead.
    !tool_cancellation_requires_execution_drain(tool_name)
}

fn child_proposal_id(parent: &str, step_id: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"palyra.dynamic-tool.child.v1\0");
    hasher.update(parent.as_bytes());
    hasher.update([0]);
    hasher.update(step_id.as_bytes());
    format!("dyntool_{}", &hex::encode(hasher.finalize())[..24])
}

fn denied(
    proposal_id: &str,
    tool_name: &str,
    input_json: &[u8],
    reason_code: &str,
    timed_out: bool,
) -> ToolExecutionOutcome {
    build_tool_execution_outcome(
        proposal_id,
        tool_name,
        input_json,
        false,
        json!({"reason_code": reason_code}).to_string().into_bytes(),
        reason_code.to_owned(),
        timed_out,
        "signed_dynamic_tool".to_owned(),
        "artifact_capability_scoped".to_owned(),
    )
}

pub(crate) fn dynamic_tool_provenance_is_current(expected: &str, current: &str) -> bool {
    expected == current
}

struct DynamicFailure {
    reason_code: &'static str,
    timed_out: bool,
}

#[derive(Clone, Copy)]
enum DynamicExecutionMode {
    Production,
    Evaluation,
}

/// Server-observed six-case evidence required before durable activation.
///
/// Declarative candidates use the production parser, policy classifier, and
/// output checks against an isolated fixture adapter so review cannot cause
/// live side effects. WASM candidates use the production restricted runtime.
pub(crate) async fn evaluate_dynamic_tool_candidate(
    runtime_state: &Arc<GatewayRuntimeState>,
    context: ToolRuntimeExecutionContext<'_>,
    artifact: &palyra_skills::SignedToolArtifact,
) -> DynamicToolRuntimeEvalEvidenceV1 {
    evaluate_dynamic_tool_candidate_inner(Some(runtime_state), context, artifact).await
}

async fn evaluate_dynamic_tool_candidate_inner(
    runtime_state: Option<&Arc<GatewayRuntimeState>>,
    context: ToolRuntimeExecutionContext<'_>,
    artifact: &palyra_skills::SignedToolArtifact,
) -> DynamicToolRuntimeEvalEvidenceV1 {
    let mut reasons = Vec::new();
    if verify_signed_dynamic_tool_artifact(artifact).is_err() {
        reasons.push("dynamic_tool.eval.artifact_invalid".to_owned());
        return eval_report(artifact, reasons);
    }
    let input = match dynamic_tool_eval_input_fixture(&artifact.proposal.input_schema) {
        Ok(input) => input,
        Err(_) => {
            reasons.push("dynamic_tool.eval.fixture_invalid".to_owned());
            return eval_report(artifact, reasons);
        }
    };
    let controls = || ToolRuntimeDispatchControls {
        remaining_tool_budget: None,
        cancellation_requested: None,
        process_progress_sink: None,
        cancellation_context: None,
        child_task_parent_context: None,
        expected_dynamic_provenance: None,
    };
    let happy = match artifact.proposal.implementation_type {
        DynamicToolImplementationType::DeclarativeComposition => {
            let Some(runtime_state) = runtime_state else {
                reasons.push("dynamic_tool.eval_adapter_unavailable".to_owned());
                return eval_report(artifact, reasons);
            };
            execute_declarative(
                runtime_state,
                context,
                DeclarativeExecutionRequest {
                    proposal_id: "dynamic_tool_eval_happy",
                    artifact,
                    input: &input,
                    controls: controls(),
                    started: Instant::now(),
                    mode: DynamicExecutionMode::Evaluation,
                },
            )
            .await
        }
        DynamicToolImplementationType::WasmComponent => {
            execute_wasm(artifact, None, Instant::now()).await
        }
    };
    match happy {
        Ok(output) if runtime_eval_output_passes(artifact, &output) => {
            reasons.push("dynamic_tool.eval.happy_path_passed".to_owned());
            reasons.push("dynamic_tool.eval.secret_output_clean".to_owned());
        }
        Ok(_) => {
            reasons.push("dynamic_tool.eval.output_rejected".to_owned());
            return eval_report(artifact, reasons);
        }
        Err(error) => {
            reasons.push(error.reason_code.to_owned());
            return eval_report(artifact, reasons);
        }
    }
    let malformed = match dynamic_tool_malformed_eval_fixture(&artifact.proposal.input_schema) {
        Ok(malformed) => malformed,
        Err(_) => {
            reasons.push("dynamic_tool.eval.malformed_fixture_invalid".to_owned());
            return eval_report(artifact, reasons);
        }
    };
    if validate_dynamic_tool_input(&artifact.proposal.input_schema, &malformed).is_err() {
        reasons.push("dynamic_tool.eval.malformed_input_rejected".to_owned());
    } else {
        reasons.push("dynamic_tool.eval.malformed_input_accepted".to_owned());
        return eval_report(artifact, reasons);
    }
    let cancelled = Arc::new(AtomicBool::new(true));
    let cancelled_controls = || ToolRuntimeDispatchControls {
        remaining_tool_budget: None,
        cancellation_requested: Some(cancelled.clone()),
        process_progress_sink: None,
        cancellation_context: None,
        child_task_parent_context: None,
        expected_dynamic_provenance: None,
    };
    let cancellation_observed = match artifact.proposal.implementation_type {
        DynamicToolImplementationType::DeclarativeComposition => {
            let Some(runtime_state) = runtime_state else {
                reasons.push("dynamic_tool.eval_adapter_unavailable".to_owned());
                return eval_report(artifact, reasons);
            };
            execute_declarative(
                runtime_state,
                context,
                DeclarativeExecutionRequest {
                    proposal_id: "dynamic_tool_eval_cancel",
                    artifact,
                    input: &input,
                    controls: cancelled_controls(),
                    started: Instant::now(),
                    mode: DynamicExecutionMode::Evaluation,
                },
            )
            .await
            .is_err_and(|failure| failure.reason_code == "dynamic_tool.cancelled")
        }
        DynamicToolImplementationType::WasmComponent => {
            execute_wasm(artifact, Some(&cancelled), Instant::now())
                .await
                .is_err_and(|failure| failure.reason_code == "dynamic_tool.cancelled")
        }
    };
    if !cancellation_observed {
        reasons.push("dynamic_tool.eval.cancellation_not_enforced".to_owned());
        return eval_report(artifact, reasons);
    }
    let expired_start = Instant::now()
        .checked_sub(Duration::from_millis(
            artifact.proposal.semantics.max_execution_ms.saturating_add(1),
        ))
        .unwrap_or_else(Instant::now);
    let timeout_observed = match artifact.proposal.implementation_type {
        DynamicToolImplementationType::DeclarativeComposition => {
            let Some(runtime_state) = runtime_state else {
                reasons.push("dynamic_tool.eval_adapter_unavailable".to_owned());
                return eval_report(artifact, reasons);
            };
            execute_declarative(
                runtime_state,
                context,
                DeclarativeExecutionRequest {
                    proposal_id: "dynamic_tool_eval_timeout",
                    artifact,
                    input: &input,
                    controls: controls(),
                    started: expired_start,
                    mode: DynamicExecutionMode::Evaluation,
                },
            )
            .await
            .is_err_and(|failure| failure.timed_out)
        }
        DynamicToolImplementationType::WasmComponent => {
            let loop_module =
                br#"(module (func (export "run") (result i32) (loop (br 0)) i32.const 0))"#;
            let grants = dynamic_tool_capability_grants(&artifact.proposal);
            let result = tokio::task::spawn_blocking(move || {
                WasmRuntime::new().and_then(|runtime| {
                    runtime.execute_i32_entrypoint_with_timeout(
                        loop_module,
                        DEFAULT_RUNTIME_ENTRYPOINT,
                        &grants,
                        Duration::from_millis(1),
                    )
                })
            })
            .await;
            result.is_ok_and(|outcome| {
                outcome.is_err_and(|error| {
                    error.to_string().to_ascii_lowercase().contains("timed out")
                })
            })
        }
    };
    if !timeout_observed {
        reasons.push("dynamic_tool.eval.timeout_not_enforced".to_owned());
        return eval_report(artifact, reasons);
    }
    reasons.push("dynamic_tool.eval.timeout_cancel_fenced".to_owned());
    if verify_dynamic_tool_authority(&artifact.proposal, artifact.implementation_bytes.as_slice())
        .is_ok()
        && authority_escape_probe_is_denied(artifact)
    {
        reasons.push("dynamic_tool.eval.authority_bounded".to_owned());
    } else {
        reasons.push("dynamic_tool.eval.authority_denied".to_owned());
        return eval_report(artifact, reasons);
    }
    if rollback_pointer_probe_is_denied(artifact) {
        reasons.push("dynamic_tool.eval.rollback_pointer_fenced".to_owned());
    } else {
        reasons.push("dynamic_tool.eval.rollback_pointer_unfenced".to_owned());
        return eval_report(artifact, reasons);
    }
    eval_report(artifact, reasons)
}

fn runtime_eval_output_passes(
    artifact: &palyra_skills::SignedToolArtifact,
    output: &Value,
) -> bool {
    let Ok(output_json) = serde_json::to_vec(output) else {
        return false;
    };
    validate_dynamic_tool_input(&artifact.proposal.output_schema, output).is_ok()
        && !dynamic_tool_output_contains_secret(output_json.as_slice())
}

fn authority_escape_probe_is_denied(artifact: &palyra_skills::SignedToolArtifact) -> bool {
    match artifact.proposal.implementation_type {
        DynamicToolImplementationType::DeclarativeComposition => {
            let Ok(mut plan) =
                serde_json::from_slice::<DeclarativeToolPlanV1>(&artifact.implementation_bytes)
            else {
                return false;
            };
            let Some(step) = plan.steps.first_mut() else {
                return false;
            };
            step.tool_name = "dynamic.authority_probe".to_owned();
            let Ok(bytes) = serde_json::to_vec(&plan) else {
                return false;
            };
            verify_dynamic_tool_authority(&artifact.proposal, bytes.as_slice()).is_err()
        }
        DynamicToolImplementationType::WasmComponent => {
            let escape = br#"
                (module
                    (import "wasi_snapshot_preview1" "proc_exit" (func $exit (param i32)))
                    (func (export "run") (result i32) i32.const 0))
            "#;
            verify_dynamic_tool_authority(&artifact.proposal, escape).is_err()
        }
    }
}

fn rollback_pointer_probe_is_denied(artifact: &palyra_skills::SignedToolArtifact) -> bool {
    let zero_digest = "0".repeat(64);
    let mismatched_pointer =
        if artifact.proposal.previous_artifact_sha256.as_deref() == Some(zero_digest.as_str()) {
            "1".repeat(64)
        } else {
            zero_digest
        };
    let gate = DynamicToolHostGate {
        host_validated: true,
        policy_approved: true,
        capability_review_approved: true,
        eval_approved: true,
        expected_catalog_epoch: 1,
        current_catalog_epoch: 1,
        approval_generation: 1,
        trusted_publisher: artifact.signature.publisher.clone(),
        trusted_public_key_base64: artifact.signature.public_key_base64.clone(),
        previous_active_artifact_sha256: Some(mismatched_pointer),
    };
    let decision = decide_dynamic_tool_activation(artifact, &gate);
    !decision.activated && decision.reason_code == "dynamic_tool.rollback_pointer_mismatch"
}

fn eval_report(
    artifact: &palyra_skills::SignedToolArtifact,
    mut reasons: Vec<String>,
) -> DynamicToolRuntimeEvalEvidenceV1 {
    reasons.sort();
    let passed = reasons.len() == 6
        && reasons.iter().all(|reason| {
            reason.ends_with("_passed")
                || reason.ends_with("_clean")
                || reason.ends_with("_rejected")
                || reason.ends_with("_fenced")
                || reason.ends_with("_bounded")
        });
    let evidence_sha256 = dynamic_tool_runtime_eval_evidence_sha256(artifact, reasons.as_slice());
    DynamicToolRuntimeEvalEvidenceV1 { v: 1, passed, evidence_sha256, case_reason_codes: reasons }
}

impl DynamicFailure {
    const fn new(reason_code: &'static str, timed_out: bool) -> Self {
        Self { reason_code, timed_out }
    }
}

#[cfg(test)]
mod tests {
    use palyra_skills::{
        build_signed_dynamic_tool_artifact, DynamicToolBuildRequest, DynamicToolImplementationType,
        DynamicToolProposalV1, DynamicToolSemanticsV1,
    };
    use serde_json::json;

    use super::*;

    fn signed_wasm_with_incompatible_runtime_output() -> palyra_skills::SignedToolArtifact {
        build_signed_dynamic_tool_artifact(DynamicToolBuildRequest {
            proposal: DynamicToolProposalV1 {
                v: 1,
                tool_name: "dynamic.wasm_probe".to_owned(),
                description: "Returns an integer through the restricted runtime.".to_owned(),
                input_schema: json!({
                    "type": "object",
                    "properties": {"value": {"type": "string"}},
                    "required": ["value"],
                    "additionalProperties": false
                }),
                output_schema: json!({
                    "type": "object",
                    "properties": {"value": {"type": "string"}},
                    "required": ["value"],
                    "additionalProperties": false
                }),
                capability_needs: Vec::new(),
                deterministic_constraints: vec!["bounded_output".to_owned()],
                implementation_type: DynamicToolImplementationType::WasmComponent,
                semantics: DynamicToolSemanticsV1 {
                    mutating: false,
                    idempotent: true,
                    requires_approval: false,
                    max_execution_ms: 1_000,
                },
                previous_artifact_sha256: None,
            },
            implementation_bytes: br#"(module (func (export "run") (result i32) i32.const 0))"#
                .to_vec(),
            allowed_capabilities: Vec::new(),
            builder_id: "host-builder".to_owned(),
            publisher: "palyra.local".to_owned(),
            signing_key: [41; 32],
            built_at_unix_ms: 100_000,
        })
        .expect("static preflight should sign the bounded module")
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn signed_wasm_runtime_output_failure_blocks_authoritative_eval() {
        let artifact = signed_wasm_with_incompatible_runtime_output();
        let report = evaluate_dynamic_tool_candidate_inner(
            None,
            ToolRuntimeExecutionContext {
                principal: "user:test",
                device_id: "device:test",
                channel: Some("console"),
                session_id: "session:test",
                run_id: "run:test",
                execution_backend:
                    crate::execution_backends::ExecutionBackendPreference::LocalSandbox,
                backend_reason_code: "dynamic_tool_test",
            },
            &artifact,
        )
        .await;
        assert!(!report.passed);
        assert!(report
            .case_reason_codes
            .iter()
            .any(|reason| reason == "dynamic_tool.eval.output_rejected"));
        assert!(authority_escape_probe_is_denied(&artifact));
        assert!(rollback_pointer_probe_is_denied(&artifact));
    }

    #[test]
    fn declarative_timeout_excludes_child_runtimes_that_require_drain() {
        assert!(declarative_child_is_timeout_drop_safe("palyra.echo"));
        assert!(!declarative_child_is_timeout_drop_safe("palyra.process.run"));
        assert!(!declarative_child_is_timeout_drop_safe("palyra.tool_program.run"));
        assert!(!declarative_child_is_timeout_drop_safe("palyra.plugin.run"));
    }
}
