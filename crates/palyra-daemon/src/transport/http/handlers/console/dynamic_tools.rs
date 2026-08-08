//! Host-reviewed activation endpoints for signed dynamic-tool artifacts.

use axum::{extract::State, http::HeaderMap, response::Response, Json};
use base64::{engine::general_purpose::STANDARD as BASE64_STANDARD, Engine as _};
use palyra_skills::{
    decide_dynamic_tool_activation, decide_dynamic_tool_rollback,
    verify_signed_dynamic_tool_artifact, DynamicToolHostGate, SignedToolArtifact, SkillTrustStore,
};
use serde::Deserialize;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use ulid::Ulid;

use crate::{
    app::state::AppState,
    execution_backends::ExecutionBackendPreference,
    gateway::{current_unix_ms, ToolRuntimeExecutionContext},
    journal::dynamic_tools::{
        dynamic_tool_approval_request, dynamic_tool_approval_subject, DynamicToolReviewAuthority,
    },
    runtime_status_response,
    transport::http::handlers::console::diagnostics::authorize_console_session,
};

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct DynamicToolProposalRequest {
    artifact: SignedToolArtifact,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct DynamicToolActivationRequest {
    artifact: SignedToolArtifact,
    approval_id: String,
}

/// Records an inert proposal and creates the exact durable host approval.
pub(crate) async fn console_dynamic_tool_proposal_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(payload): Json<DynamicToolProposalRequest>,
) -> Result<Json<Value>, Response> {
    let session = authorize_console_session(&state, &headers, true)?;
    require_rollout(&state)?;
    let (_, _, host_policy_sha256) = verify_host_policy(&state, &payload.artifact)?;
    let context = state
        .runtime
        .journal_store
        .dynamic_tool_activation_context(payload.artifact.proposal.tool_name.as_str())
        .map_err(internal)?;
    let active = state
        .runtime
        .journal_store
        .active_dynamic_tool(payload.artifact.proposal.tool_name.as_str())
        .map_err(internal)?;
    let normal_pointer_matches =
        payload.artifact.proposal.previous_artifact_sha256 == context.active_artifact_sha256;
    let is_signed_rollback_target = active.as_ref().is_some_and(|active| {
        active.decision.rollback_artifact_sha256.as_deref()
            == Some(payload.artifact.artifact_sha256.as_str())
    });
    if !normal_pointer_matches && !is_signed_rollback_target {
        return Err(failed_precondition("dynamic_tool.rollback_pointer_mismatch"));
    }
    let authority = DynamicToolReviewAuthority {
        session_id: Ulid::new().to_string(),
        run_id: Ulid::new().to_string(),
        principal: session.context.principal.clone(),
        device_id: session.context.device_id.clone(),
        channel: session.context.channel.clone(),
        host_policy_sha256,
    };
    let runtime_context = ToolRuntimeExecutionContext {
        principal: authority.principal.as_str(),
        device_id: authority.device_id.as_str(),
        channel: authority.channel.as_deref(),
        session_id: authority.session_id.as_str(),
        run_id: authority.run_id.as_str(),
        execution_backend: ExecutionBackendPreference::LocalSandbox,
        backend_reason_code: "dynamic_tool_activation_eval",
    };
    let eval_report =
        crate::application::tool_runtime::dynamic_tools::evaluate_dynamic_tool_candidate(
            &state.runtime,
            runtime_context,
            &payload.artifact,
        )
        .await;
    if !eval_report.passed {
        return Err(failed_precondition("dynamic_tool.runtime_eval_failed"));
    }
    let subject_id = dynamic_tool_approval_subject(
        &payload.artifact,
        &context,
        eval_report.evidence_sha256.as_str(),
    );
    state
        .runtime
        .journal_store
        .record_dynamic_tool_proposal(
            &payload.artifact,
            &eval_report,
            &authority,
            subject_id.as_str(),
            current_unix_ms(),
        )
        .map_err(internal)?;
    let approval_id = Ulid::new().to_string();
    let approval_request = dynamic_tool_approval_request(
        approval_id,
        &payload.artifact,
        &context,
        &eval_report,
        &authority,
    );
    let approval =
        state.runtime.create_approval_record(approval_request).await.map_err(internal_status)?;
    Ok(Json(json!({
        "schema_version": 1,
        "state": "proposed",
        "reason_code": "dynamic_tool.host_approval_required",
        "approval_id": approval.approval_id,
        "approval_subject_id": subject_id,
        "artifact_sha256": payload.artifact.artifact_sha256,
        "catalog_epoch": context.catalog_epoch,
        "approval_generation": context.approval_generation,
        "runtime_eval_sha256": eval_report.evidence_sha256,
    })))
}

/// Activates one proposal only after exact approval and runtime conformance.
pub(crate) async fn console_dynamic_tool_activation_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(payload): Json<DynamicToolActivationRequest>,
) -> Result<Json<Value>, Response> {
    let session = authorize_console_session(&state, &headers, true)?;
    require_rollout(&state)?;
    let (trusted_publisher, trusted_public_key_base64, host_policy_sha256) =
        verify_host_policy(&state, &payload.artifact)?;
    let approval = state
        .runtime
        .approval_record(payload.approval_id.clone())
        .await
        .map_err(internal_status)?
        .ok_or_else(|| failed_precondition("dynamic_tool.host_approval_missing_or_stale"))?;
    if approval.principal != session.context.principal
        || approval.device_id != session.context.device_id
        || approval.channel != session.context.channel
    {
        return Err(failed_precondition("dynamic_tool.host_approval_principal_mismatch"));
    }
    if let Some(active) = state
        .runtime
        .journal_store
        .active_dynamic_tool(payload.artifact.proposal.tool_name.as_str())
        .map_err(internal)?
        .filter(|active| {
            active.artifact.artifact_sha256 == payload.artifact.artifact_sha256
                && active.approval_id == approval.approval_id
        })
    {
        return Ok(Json(json!({
            "schema_version": 1,
            "state": "active",
            "reason_code": "dynamic_tool.activation_replayed",
            "artifact_sha256": active.artifact.artifact_sha256,
            "catalog_epoch": active.decision.catalog_epoch,
            "approval_generation": active.decision.approval_generation,
        })));
    }
    let context = state
        .runtime
        .journal_store
        .dynamic_tool_activation_context(payload.artifact.proposal.tool_name.as_str())
        .map_err(internal)?;
    let authority = DynamicToolReviewAuthority {
        session_id: approval.session_id.clone(),
        run_id: approval.run_id.clone(),
        principal: session.context.principal.clone(),
        device_id: session.context.device_id.clone(),
        channel: session.context.channel.clone(),
        host_policy_sha256,
    };
    let runtime_context = ToolRuntimeExecutionContext {
        principal: authority.principal.as_str(),
        device_id: authority.device_id.as_str(),
        channel: authority.channel.as_deref(),
        session_id: authority.session_id.as_str(),
        run_id: authority.run_id.as_str(),
        execution_backend: ExecutionBackendPreference::LocalSandbox,
        backend_reason_code: "dynamic_tool_activation_eval",
    };
    let eval_report =
        crate::application::tool_runtime::dynamic_tools::evaluate_dynamic_tool_candidate(
            &state.runtime,
            runtime_context,
            &payload.artifact,
        )
        .await;
    if !eval_report.passed {
        return Err(failed_precondition("dynamic_tool.runtime_eval_failed"));
    }
    let expected_subject = dynamic_tool_approval_subject(
        &payload.artifact,
        &context,
        eval_report.evidence_sha256.as_str(),
    );
    if approval.subject_id != expected_subject {
        return Err(failed_precondition("dynamic_tool.host_approval_missing_or_stale"));
    }
    let gate = DynamicToolHostGate {
        host_validated: true,
        policy_approved: true,
        capability_review_approved: true,
        eval_approved: true,
        expected_catalog_epoch: context.catalog_epoch,
        current_catalog_epoch: context.catalog_epoch,
        approval_generation: context.approval_generation,
        trusted_publisher,
        trusted_public_key_base64,
        previous_active_artifact_sha256: context.active_artifact_sha256,
    };
    let current = state
        .runtime
        .journal_store
        .active_dynamic_tool(payload.artifact.proposal.tool_name.as_str())
        .map_err(internal)?;
    let decision = if let Some(current) = current.as_ref().filter(|current| {
        current.decision.rollback_artifact_sha256.as_deref()
            == Some(payload.artifact.artifact_sha256.as_str())
    }) {
        decide_dynamic_tool_rollback(&current.decision, &payload.artifact, &gate)
    } else {
        decide_dynamic_tool_activation(&payload.artifact, &gate)
    };
    if !decision.activated {
        return Err(failed_precondition(decision.reason_code.as_str()));
    }
    let active = state
        .runtime
        .journal_store
        .activate_dynamic_tool(
            &payload.artifact,
            &decision,
            &eval_report,
            &authority,
            approval.approval_id.as_str(),
            current_unix_ms(),
        )
        .map_err(internal)?;
    let _ = state
        .runtime
        .record_console_event(
            &session.context,
            "dynamic_tool.lifecycle.transition",
            json!({
                "schema_version": 1,
                "tool_name": active.artifact.proposal.tool_name,
                "artifact_sha256": active.artifact.artifact_sha256,
                "proposal_sha256": active.artifact.provenance.proposal_sha256,
                "static_preflight_sha256": active.artifact.eval_pack.pack_sha256,
                "runtime_eval_sha256": eval_report.evidence_sha256,
                "catalog_epoch": active.decision.catalog_epoch,
                "approval_generation": active.decision.approval_generation,
                "reason_code": active.decision.reason_code,
            }),
        )
        .await;
    Ok(Json(json!({
        "schema_version": 1,
        "state": "active",
        "reason_code": active.decision.reason_code,
        "artifact_sha256": active.artifact.artifact_sha256,
        "catalog_epoch": active.decision.catalog_epoch,
        "approval_generation": active.decision.approval_generation,
        "runtime_eval": eval_report,
    })))
}

#[allow(clippy::result_large_err)]
fn require_rollout(state: &AppState) -> Result<(), Response> {
    if state.runtime.config.feature_rollouts.dynamic_tool_builder.enabled {
        Ok(())
    } else {
        Err(failed_precondition("dynamic_tool.rollout_disabled"))
    }
}

#[allow(clippy::result_large_err)]
fn verify_host_policy(
    state: &AppState,
    artifact: &SignedToolArtifact,
) -> Result<(String, String, String), Response> {
    verify_signed_dynamic_tool_artifact(artifact)
        .map_err(|_| failed_precondition("dynamic_tool.artifact_verification_failed"))?;
    let trust_store = load_dynamic_trust_store()?;
    let public_key = BASE64_STANDARD
        .decode(artifact.signature.public_key_base64.as_bytes())
        .map_err(|_| failed_precondition("dynamic_tool.publisher_trust_denied"))?;
    let public_key_hex = hex::encode(public_key);
    let trusted = trust_store
        .trusted_publishers
        .get(artifact.signature.publisher.as_str())
        .is_some_and(|keys| keys.iter().any(|key| key == &public_key_hex));
    if !trusted {
        return Err(failed_precondition("dynamic_tool.publisher_trust_denied"));
    }
    if !state
        .runtime
        .config
        .tool_call
        .allowed_tools
        .iter()
        .any(|tool| tool == &artifact.proposal.tool_name)
    {
        return Err(failed_precondition("dynamic_tool.policy_allowlist_denied"));
    }
    if artifact
        .proposal
        .capability_needs
        .iter()
        .any(|capability| !capability_allowed(state, capability))
    {
        return Err(failed_precondition("dynamic_tool.capability_review_denied"));
    }
    Ok((
        artifact.signature.publisher.clone(),
        artifact.signature.public_key_base64.clone(),
        dynamic_tool_host_policy_sha256(state, artifact),
    ))
}

fn dynamic_tool_host_policy_sha256(state: &AppState, artifact: &SignedToolArtifact) -> String {
    let wasm = &state.runtime.config.tool_call.wasm_runtime;
    let mut fields = vec![
        format!("publisher:{}", artifact.signature.publisher),
        format!("public_key:{}", artifact.signature.public_key_base64),
    ];
    fields.extend(
        state.runtime.config.tool_call.allowed_tools.iter().map(|value| format!("tool:{value}")),
    );
    fields.extend(wasm.allowed_http_hosts.iter().map(|value| format!("http_host:{value}")));
    fields.extend(wasm.allowed_secrets.iter().map(|value| format!("secret_lease:{value}")));
    fields.extend(
        wasm.allowed_storage_prefixes.iter().map(|value| format!("storage_prefix:{value}")),
    );
    fields.extend(wasm.allowed_channels.iter().map(|value| format!("channel:{value}")));
    fields.sort();
    fields.dedup();
    let mut hasher = Sha256::new();
    hasher.update(b"palyra.dynamic-tool.host-policy.v1\0");
    hasher.update((fields.len() as u64).to_le_bytes());
    for field in fields {
        hasher.update((field.len() as u64).to_le_bytes());
        hasher.update(field.as_bytes());
    }
    hex::encode(hasher.finalize())
}

fn capability_allowed(state: &AppState, capability: &str) -> bool {
    let wasm = &state.runtime.config.tool_call.wasm_runtime;
    capability.strip_prefix("tool:").is_some_and(|tool| {
        !tool.starts_with("dynamic.")
            && state.runtime.config.tool_call.allowed_tools.iter().any(|allowed| allowed == tool)
    }) || capability
        .strip_prefix("http_host:")
        .is_some_and(|host| wasm.allowed_http_hosts.iter().any(|allowed| allowed == host))
        || capability
            .strip_prefix("secret_lease:")
            .is_some_and(|secret| wasm.allowed_secrets.iter().any(|allowed| allowed == secret))
        || capability.strip_prefix("storage_prefix:").is_some_and(|prefix| {
            wasm.allowed_storage_prefixes.iter().any(|allowed| allowed == prefix)
        })
        || capability
            .strip_prefix("channel:")
            .is_some_and(|channel| wasm.allowed_channels.iter().any(|allowed| allowed == channel))
}

#[allow(clippy::result_large_err)]
fn load_dynamic_trust_store() -> Result<SkillTrustStore, Response> {
    let root = crate::resolve_skills_root()?;
    let path = crate::resolve_skills_trust_store_path(root.as_path());
    crate::load_trust_store(path.as_path())
}

fn failed_precondition(reason_code: &str) -> Response {
    runtime_status_response(tonic::Status::failed_precondition(reason_code.to_owned()))
}

fn internal(_error: impl std::fmt::Display) -> Response {
    runtime_status_response(tonic::Status::internal("dynamic_tool.internal"))
}

fn internal_status(_error: tonic::Status) -> Response {
    runtime_status_response(tonic::Status::internal("dynamic_tool.internal"))
}
