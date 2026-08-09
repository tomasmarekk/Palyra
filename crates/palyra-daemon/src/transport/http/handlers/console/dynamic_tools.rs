//! Host-reviewed activation endpoints for signed dynamic-tool artifacts.

use std::{ffi::OsString, sync::Arc};

use axum::{extract::State, http::HeaderMap, response::Response, Json};
use base64::{engine::general_purpose::STANDARD as BASE64_STANDARD, Engine as _};
use palyra_skills::{
    build_signed_dynamic_tool_artifact, decide_dynamic_tool_activation,
    decide_dynamic_tool_rollback, verify_signed_dynamic_tool_artifact, DynamicToolBuildRequest,
    DynamicToolError, DynamicToolHostGate, DynamicToolProposalV1, SignedToolArtifact,
    SkillTrustStore, MAX_DYNAMIC_TOOL_IMPLEMENTATION_BYTES,
};
use palyra_vault::{Vault, VaultRef};
use serde::Deserialize;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use ulid::Ulid;
use zeroize::Zeroizing;

use crate::{
    app::state::{AppState, ConsoleSession},
    execution_backends::ExecutionBackendPreference,
    gateway::{current_unix_ms, GatewayRuntimeState, ToolRuntimeExecutionContext},
    journal::dynamic_tools::{
        dynamic_tool_approval_request, dynamic_tool_approval_subject, DynamicToolReviewAuthority,
    },
    runtime_status_response,
    transport::http::handlers::console::diagnostics::authorize_console_session,
};

const DYNAMIC_TOOL_PUBLISHER_ID_ENV: &str = "PALYRA_DYNAMIC_TOOL_PUBLISHER_ID";
const DYNAMIC_TOOL_SIGNING_KEY_VAULT_REF_ENV: &str = "PALYRA_DYNAMIC_TOOL_SIGNING_KEY_VAULT_REF";
const DYNAMIC_TOOL_HOST_BUILDER_ID: &str = "palyra.host-publisher";
const DYNAMIC_TOOL_HOST_BUILD_JSON_OVERHEAD_BYTES: usize = 2 * 1024 * 1024;
const MAX_IMPLEMENTATION_BASE64_BYTES: usize =
    MAX_DYNAMIC_TOOL_IMPLEMENTATION_BYTES.div_ceil(3) * 4;
pub(crate) const DYNAMIC_TOOL_HOST_BUILD_MAX_REQUEST_BODY_BYTES: usize =
    MAX_IMPLEMENTATION_BASE64_BYTES + DYNAMIC_TOOL_HOST_BUILD_JSON_OVERHEAD_BYTES;
const DYNAMIC_TOOL_ARTIFACT_JSON_OVERHEAD_BYTES: usize = 2 * 1024 * 1024;
pub(crate) const DYNAMIC_TOOL_ARTIFACT_MAX_REQUEST_BODY_BYTES: usize =
    MAX_DYNAMIC_TOOL_IMPLEMENTATION_BYTES * 4 + DYNAMIC_TOOL_ARTIFACT_JSON_OVERHEAD_BYTES;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct DynamicToolProposalRequest {
    artifact: SignedToolArtifact,
}

/// Unsigned operator submission; signing identity and authority are never wire inputs.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct DynamicToolHostBuildRequest {
    proposal: DynamicToolProposalV1,
    implementation_base64: String,
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
    propose_signed_dynamic_tool(&state, &session, &payload.artifact).await
}

/// Builds an artifact with the configured host signer, then records it as inert.
pub(crate) async fn console_dynamic_tool_host_build_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(payload): Json<DynamicToolHostBuildRequest>,
) -> Result<Json<Value>, Response> {
    let session = authorize_console_session(&state, &headers, true)?;
    // Key material is unreachable while the product rollout is closed.
    require_rollout(&state)?;
    let artifact = build_host_signed_artifact(&state, payload).await?;
    let mut response = propose_signed_dynamic_tool(&state, &session, &artifact).await?;
    response
        .0
        .as_object_mut()
        .ok_or_else(|| internal("proposal response shape"))?
        .insert("artifact".to_owned(), serde_json::to_value(&artifact).map_err(internal)?);
    Ok(response)
}

async fn propose_signed_dynamic_tool(
    state: &AppState,
    session: &ConsoleSession,
    artifact: &SignedToolArtifact,
) -> Result<Json<Value>, Response> {
    let (_, _, host_policy_sha256) = verify_host_policy(state, artifact)?;
    let context = state
        .runtime
        .journal_store
        .dynamic_tool_activation_context(artifact.proposal.tool_name.as_str())
        .map_err(internal)?;
    let active = state
        .runtime
        .journal_store
        .active_dynamic_tool(artifact.proposal.tool_name.as_str())
        .map_err(internal)?;
    let normal_pointer_matches =
        artifact.proposal.previous_artifact_sha256 == context.active_artifact_sha256;
    let is_signed_rollback_target = active.as_ref().is_some_and(|active| {
        active.decision.rollback_artifact_sha256.as_deref()
            == Some(artifact.artifact_sha256.as_str())
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
            artifact,
        )
        .await;
    if !eval_report.passed {
        return Err(failed_precondition("dynamic_tool.runtime_eval_failed"));
    }
    let subject_id =
        dynamic_tool_approval_subject(artifact, &context, eval_report.evidence_sha256.as_str());
    state
        .runtime
        .journal_store
        .record_dynamic_tool_proposal(
            artifact,
            &eval_report,
            &authority,
            subject_id.as_str(),
            current_unix_ms(),
        )
        .map_err(internal)?;
    let approval_id = Ulid::new().to_string();
    let approval_request =
        dynamic_tool_approval_request(approval_id, artifact, &context, &eval_report, &authority);
    let approval =
        state.runtime.create_approval_record(approval_request).await.map_err(internal_status)?;
    Ok(Json(json!({
        "schema_version": 1,
        "state": "proposed",
        "reason_code": "dynamic_tool.host_approval_required",
        "approval_id": approval.approval_id,
        "approval_subject_id": subject_id,
        "artifact_sha256": artifact.artifact_sha256,
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

struct HostPublisherConfig {
    publisher: String,
    signing_key_ref: VaultRef,
}

struct ValidatedHostBuild {
    proposal: DynamicToolProposalV1,
    implementation_bytes: Vec<u8>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum HostPublisherError {
    PublisherUnconfigured,
    PublisherInvalid,
    SigningKeyRefUnconfigured,
    SigningKeyRefInvalid,
    SigningKeyUnavailable,
    SigningKeyInvalid,
    ImplementationInvalid,
    PolicyAllowlistDenied,
    CapabilityReviewDenied,
    ProposalInvalid,
    StaticEvalFailed,
    HostBuildFailed,
    SigningKeyTrustMismatch,
}

impl HostPublisherError {
    const fn reason_code(self) -> &'static str {
        match self {
            Self::PublisherUnconfigured => "dynamic_tool.host_publisher_unconfigured",
            Self::PublisherInvalid => "dynamic_tool.host_publisher_invalid",
            Self::SigningKeyRefUnconfigured => "dynamic_tool.signing_key_ref_unconfigured",
            Self::SigningKeyRefInvalid => "dynamic_tool.signing_key_ref_invalid",
            Self::SigningKeyUnavailable => "dynamic_tool.signing_key_unavailable",
            Self::SigningKeyInvalid => "dynamic_tool.signing_key_invalid",
            Self::ImplementationInvalid => "dynamic_tool.implementation_invalid",
            Self::PolicyAllowlistDenied => "dynamic_tool.policy_allowlist_denied",
            Self::CapabilityReviewDenied => "dynamic_tool.capability_review_denied",
            Self::ProposalInvalid => "dynamic_tool.proposal_invalid",
            Self::StaticEvalFailed => "dynamic_tool.static_eval_failed",
            Self::HostBuildFailed => "dynamic_tool.host_build_failed",
            Self::SigningKeyTrustMismatch => "dynamic_tool.signing_key_trust_mismatch",
        }
    }
}

impl HostPublisherConfig {
    fn from_values(
        publisher: Option<OsString>,
        signing_key_ref: Option<OsString>,
    ) -> Result<Self, HostPublisherError> {
        let publisher = publisher
            .ok_or(HostPublisherError::PublisherUnconfigured)?
            .into_string()
            .map_err(|_| HostPublisherError::PublisherInvalid)?;
        let publisher = publisher.trim();
        if publisher.is_empty()
            || publisher.len() > 128
            || !publisher.bytes().all(|byte| {
                byte.is_ascii_lowercase()
                    || byte.is_ascii_digit()
                    || matches!(byte, b'.' | b'_' | b'-' | b':')
            })
        {
            return Err(HostPublisherError::PublisherInvalid);
        }
        let signing_key_ref = signing_key_ref
            .ok_or(HostPublisherError::SigningKeyRefUnconfigured)?
            .into_string()
            .map_err(|_| HostPublisherError::SigningKeyRefInvalid)?;
        let signing_key_ref = VaultRef::parse(signing_key_ref.as_str())
            .map_err(|_| HostPublisherError::SigningKeyRefInvalid)?;
        Ok(Self { publisher: publisher.to_owned(), signing_key_ref })
    }

    fn from_env() -> Result<Self, HostPublisherError> {
        Self::from_values(
            std::env::var_os(DYNAMIC_TOOL_PUBLISHER_ID_ENV),
            std::env::var_os(DYNAMIC_TOOL_SIGNING_KEY_VAULT_REF_ENV),
        )
    }
}

#[allow(clippy::result_large_err)]
async fn build_host_signed_artifact(
    state: &AppState,
    payload: DynamicToolHostBuildRequest,
) -> Result<SignedToolArtifact, Response> {
    let request = validate_host_build_request(payload).map_err(host_publisher_error)?;
    let allowed_capabilities =
        derive_host_allowed_capabilities(state.runtime.as_ref(), &request.proposal)
            .map_err(host_publisher_error)?;
    let publisher = HostPublisherConfig::from_env().map_err(host_publisher_error)?;
    let signing_seed = load_host_signing_seed(&state.vault, &publisher.signing_key_ref)
        .await
        .map_err(host_publisher_error)?;
    let artifact = build_host_artifact_from_parts(
        request.proposal,
        request.implementation_bytes,
        allowed_capabilities,
        publisher.publisher,
        &signing_seed,
        current_unix_ms(),
    )
    .map_err(|error| host_publisher_error(map_host_build_error(&error)))?;
    verify_host_policy_with_trust_reason(
        state,
        &artifact,
        HostPublisherError::SigningKeyTrustMismatch.reason_code(),
    )?;
    Ok(artifact)
}

fn validate_host_build_request(
    payload: DynamicToolHostBuildRequest,
) -> Result<ValidatedHostBuild, HostPublisherError> {
    let implementation_bytes = decode_host_implementation(payload.implementation_base64.as_str())?;
    Ok(ValidatedHostBuild { proposal: payload.proposal, implementation_bytes })
}

fn decode_host_implementation(encoded: &str) -> Result<Vec<u8>, HostPublisherError> {
    if encoded.is_empty() || encoded.len() > MAX_IMPLEMENTATION_BASE64_BYTES {
        return Err(HostPublisherError::ImplementationInvalid);
    }
    let implementation = BASE64_STANDARD
        .decode(encoded.as_bytes())
        .map_err(|_| HostPublisherError::ImplementationInvalid)?;
    if implementation.is_empty() || implementation.len() > MAX_DYNAMIC_TOOL_IMPLEMENTATION_BYTES {
        return Err(HostPublisherError::ImplementationInvalid);
    }
    Ok(implementation)
}

fn derive_host_allowed_capabilities(
    runtime: &GatewayRuntimeState,
    proposal: &DynamicToolProposalV1,
) -> Result<Vec<String>, HostPublisherError> {
    if !runtime.config.tool_call.allowed_tools.iter().any(|tool| tool == &proposal.tool_name) {
        return Err(HostPublisherError::PolicyAllowlistDenied);
    }
    if proposal.capability_needs.iter().any(|capability| !capability_allowed(runtime, capability)) {
        return Err(HostPublisherError::CapabilityReviewDenied);
    }
    Ok(proposal.capability_needs.clone())
}

async fn load_host_signing_seed(
    vault: &Arc<Vault>,
    signing_key_ref: &VaultRef,
) -> Result<Zeroizing<[u8; 32]>, HostPublisherError> {
    let vault = Arc::clone(vault);
    let scope = signing_key_ref.scope.clone();
    let key = signing_key_ref.key.clone();
    let secret = tokio::task::spawn_blocking(move || {
        vault.get_secret(&scope, key.as_str()).map(Zeroizing::new)
    })
    .await
    .map_err(|_| HostPublisherError::SigningKeyUnavailable)?
    .map_err(|_| HostPublisherError::SigningKeyUnavailable)?;
    let seed = <[u8; 32]>::try_from(secret.as_slice())
        .map_err(|_| HostPublisherError::SigningKeyInvalid)?;
    Ok(Zeroizing::new(seed))
}

fn build_host_artifact_from_parts(
    proposal: DynamicToolProposalV1,
    implementation_bytes: Vec<u8>,
    allowed_capabilities: Vec<String>,
    publisher: String,
    signing_seed: &Zeroizing<[u8; 32]>,
    built_at_unix_ms: i64,
) -> Result<SignedToolArtifact, DynamicToolError> {
    build_signed_dynamic_tool_artifact(DynamicToolBuildRequest {
        proposal,
        implementation_bytes,
        allowed_capabilities,
        builder_id: DYNAMIC_TOOL_HOST_BUILDER_ID.to_owned(),
        publisher,
        signing_key: **signing_seed,
        built_at_unix_ms,
    })
}

fn map_host_build_error(error: &DynamicToolError) -> HostPublisherError {
    match error {
        DynamicToolError::CapabilityEscalation => HostPublisherError::CapabilityReviewDenied,
        DynamicToolError::ProposalInvalid(_) => HostPublisherError::ProposalInvalid,
        DynamicToolError::ImplementationInvalid(_) => HostPublisherError::ImplementationInvalid,
        DynamicToolError::EvalPackInvalid(_) => HostPublisherError::StaticEvalFailed,
        DynamicToolError::SignatureInvalid
        | DynamicToolError::DigestInvalid
        | DynamicToolError::Serialization => HostPublisherError::HostBuildFailed,
    }
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
    verify_host_policy_with_trust_reason(state, artifact, "dynamic_tool.publisher_trust_denied")
}

#[allow(clippy::result_large_err)]
fn verify_host_policy_with_trust_reason(
    state: &AppState,
    artifact: &SignedToolArtifact,
    trust_denial_reason: &'static str,
) -> Result<(String, String, String), Response> {
    verify_signed_dynamic_tool_artifact(artifact)
        .map_err(|_| failed_precondition("dynamic_tool.artifact_verification_failed"))?;
    let trust_store = load_dynamic_trust_store()?;
    let public_key = BASE64_STANDARD
        .decode(artifact.signature.public_key_base64.as_bytes())
        .map_err(|_| failed_precondition(trust_denial_reason))?;
    if public_key.len() != 32
        || !publisher_key_is_trusted(
            &trust_store,
            artifact.signature.publisher.as_str(),
            public_key.as_slice(),
        )
    {
        return Err(failed_precondition(trust_denial_reason));
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
        .any(|capability| !capability_allowed(state.runtime.as_ref(), capability))
    {
        return Err(failed_precondition("dynamic_tool.capability_review_denied"));
    }
    Ok((
        artifact.signature.publisher.clone(),
        artifact.signature.public_key_base64.clone(),
        dynamic_tool_host_policy_sha256(state, artifact),
    ))
}

fn publisher_key_is_trusted(
    trust_store: &SkillTrustStore,
    publisher: &str,
    public_key: &[u8],
) -> bool {
    let public_key_hex = hex::encode(public_key);
    trust_store
        .trusted_publishers
        .get(publisher)
        .is_some_and(|keys| keys.iter().any(|key| key == &public_key_hex))
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

fn capability_allowed(runtime: &GatewayRuntimeState, capability: &str) -> bool {
    let wasm = &runtime.config.tool_call.wasm_runtime;
    capability.strip_prefix("tool:").is_some_and(|tool| {
        !tool.starts_with("dynamic.")
            && runtime.config.tool_call.allowed_tools.iter().any(|allowed| allowed == tool)
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
    let root = crate::resolve_skills_root()
        .map_err(|_| failed_precondition("dynamic_tool.publisher_trust_unavailable"))?;
    let path = crate::resolve_skills_trust_store_path(root.as_path());
    crate::load_trust_store(path.as_path())
        .map_err(|_| failed_precondition("dynamic_tool.publisher_trust_unavailable"))
}

fn host_publisher_error(error: HostPublisherError) -> Response {
    failed_precondition(error.reason_code())
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

#[cfg(test)]
mod tests {
    use palyra_skills::{
        decide_dynamic_tool_activation, decide_dynamic_tool_rollback, DeclarativeToolPlanV1,
        DeclarativeToolStepV1, DynamicToolHostGate, DynamicToolImplementationType,
        DynamicToolSemanticsV1, ToolActivationDecision,
    };
    use palyra_vault::{BackendPreference, VaultConfig, VaultScope};
    use serde_json::json;
    use tempfile::tempdir;

    use super::*;
    use crate::{
        application::{
            tool_registry::{dynamic_tool_record_provenance, dynamic_tool_registry_entry},
            tool_runtime::dynamic_tools::{evaluate_dynamic_tool_candidate, execute_dynamic_tool},
        },
        gateway::{
            tests::build_test_runtime_state_with_runtime_overrides, ToolRuntimeDispatchControls,
        },
        journal::{
            dynamic_tools::DynamicToolActiveRecord, ApprovalDecision, ApprovalDecisionScope,
            ApprovalResolveRequest,
        },
    };

    const PUBLISHER: &str = "palyra.local";
    const SIGNING_SEED: [u8; 32] = [61; 32];

    fn schema(variant: bool) -> Value {
        if variant {
            json!({
                "type": "object",
                "properties": {
                    "value": {"type": "string"},
                    "tag": {"type": "string"}
                },
                "required": ["value"],
                "additionalProperties": false
            })
        } else {
            json!({
                "type": "object",
                "properties": {"value": {"type": "string"}},
                "required": ["value"],
                "additionalProperties": false
            })
        }
    }

    fn output_schema() -> Value {
        json!({
            "type": "object",
            "properties": {
                "text": {"type": "string"},
                "echo": {"type": "string"}
            },
            "additionalProperties": false
        })
    }

    fn proposal(previous_artifact_sha256: Option<String>, variant: bool) -> DynamicToolProposalV1 {
        DynamicToolProposalV1 {
            v: 1,
            tool_name: "dynamic.host_echo".to_owned(),
            description: "Echoes one bounded value through the approved echo tool.".to_owned(),
            input_schema: schema(variant),
            output_schema: output_schema(),
            capability_needs: vec!["tool:palyra.echo".to_owned()],
            deterministic_constraints: vec!["bounded_output".to_owned()],
            implementation_type: DynamicToolImplementationType::DeclarativeComposition,
            semantics: DynamicToolSemanticsV1 {
                mutating: false,
                idempotent: true,
                requires_approval: false,
                max_execution_ms: 1_000,
            },
            previous_artifact_sha256,
        }
    }

    fn implementation() -> Vec<u8> {
        serde_json::to_vec(&DeclarativeToolPlanV1 {
            v: 1,
            steps: vec![DeclarativeToolStepV1 {
                step_id: "echo".to_owned(),
                tool_name: "palyra.echo".to_owned(),
                input_template: json!({"text": "${input.value}"}),
                timeout_ms: 100,
            }],
        })
        .expect("declarative plan should serialize")
    }

    fn test_runtime() -> Arc<GatewayRuntimeState> {
        let mut rollouts = crate::config::FeatureRolloutsConfig::default();
        rollouts.dynamic_tool_builder.enabled = true;
        let mut runtime = build_test_runtime_state_with_runtime_overrides(false, false, rollouts);
        Arc::get_mut(&mut runtime)
            .expect("test runtime must be uniquely owned")
            .config
            .tool_call
            .allowed_tools
            .push("dynamic.host_echo".to_owned());
        runtime
    }

    fn host_artifact(
        previous_artifact_sha256: Option<String>,
        variant: bool,
        built_at_unix_ms: i64,
    ) -> SignedToolArtifact {
        let request = validate_host_build_request(DynamicToolHostBuildRequest {
            proposal: proposal(previous_artifact_sha256, variant),
            implementation_base64: BASE64_STANDARD.encode(implementation()),
        })
        .expect("unsigned host request should validate");
        build_host_artifact_from_parts(
            request.proposal,
            request.implementation_bytes,
            vec!["tool:palyra.echo".to_owned()],
            PUBLISHER.to_owned(),
            &Zeroizing::new(SIGNING_SEED),
            built_at_unix_ms,
        )
        .expect("host artifact should build")
    }

    fn runtime_context<'a>(
        authority: &'a DynamicToolReviewAuthority,
    ) -> ToolRuntimeExecutionContext<'a> {
        ToolRuntimeExecutionContext {
            principal: authority.principal.as_str(),
            device_id: authority.device_id.as_str(),
            channel: authority.channel.as_deref(),
            session_id: authority.session_id.as_str(),
            run_id: authority.run_id.as_str(),
            execution_backend: ExecutionBackendPreference::LocalSandbox,
            backend_reason_code: "dynamic_tool_host_publisher_test",
        }
    }

    async fn activate(
        runtime: &Arc<GatewayRuntimeState>,
        artifact: &SignedToolArtifact,
        rollback_from: Option<&ToolActivationDecision>,
    ) -> DynamicToolActiveRecord {
        let context = runtime
            .journal_store
            .dynamic_tool_activation_context(artifact.proposal.tool_name.as_str())
            .expect("activation context should load");
        let authority = DynamicToolReviewAuthority {
            session_id: Ulid::new().to_string(),
            run_id: Ulid::new().to_string(),
            principal: "user:operator".to_owned(),
            device_id: "device:console".to_owned(),
            channel: Some("console".to_owned()),
            host_policy_sha256: "a".repeat(64),
        };
        let runtime_eval =
            evaluate_dynamic_tool_candidate(runtime, runtime_context(&authority), artifact).await;
        assert!(runtime_eval.passed, "authoritative runtime eval must pass");
        let subject = dynamic_tool_approval_subject(
            artifact,
            &context,
            runtime_eval.evidence_sha256.as_str(),
        );
        runtime
            .journal_store
            .record_dynamic_tool_proposal(
                artifact,
                &runtime_eval,
                &authority,
                subject.as_str(),
                current_unix_ms(),
            )
            .expect("inert proposal should persist");
        let approval_id = Ulid::new().to_string();
        runtime
            .journal_store
            .create_approval(&dynamic_tool_approval_request(
                approval_id.clone(),
                artifact,
                &context,
                &runtime_eval,
                &authority,
            ))
            .expect("approval should persist");
        runtime
            .journal_store
            .resolve_approval(&ApprovalResolveRequest {
                approval_id: approval_id.clone(),
                decision: ApprovalDecision::Allow,
                decision_scope: ApprovalDecisionScope::Once,
                decision_reason: "operator_reviewed_host_build".to_owned(),
                decision_scope_ttl_ms: None,
            })
            .expect("approval should resolve");
        let gate = DynamicToolHostGate {
            host_validated: true,
            policy_approved: true,
            capability_review_approved: true,
            eval_approved: true,
            expected_catalog_epoch: context.catalog_epoch,
            current_catalog_epoch: context.catalog_epoch,
            approval_generation: context.approval_generation,
            trusted_publisher: artifact.signature.publisher.clone(),
            trusted_public_key_base64: artifact.signature.public_key_base64.clone(),
            previous_active_artifact_sha256: context.active_artifact_sha256.clone(),
        };
        let decision = rollback_from.map_or_else(
            || decide_dynamic_tool_activation(artifact, &gate),
            |current| decide_dynamic_tool_rollback(current, artifact, &gate),
        );
        assert!(decision.activated, "host-reviewed decision should activate");
        runtime
            .journal_store
            .activate_dynamic_tool(
                artifact,
                &decision,
                &runtime_eval,
                &authority,
                approval_id.as_str(),
                current_unix_ms(),
            )
            .expect("approval consumption and activation should commit atomically")
    }

    #[test]
    fn host_request_rejects_wire_supplied_authority() {
        let request = json!({
            "proposal": proposal(None, false),
            "implementation_base64": BASE64_STANDARD.encode(implementation()),
            "publisher": "attacker",
            "allowed_capabilities": ["http_host:anywhere"],
            "signing_key": [1, 2, 3],
            "passed": true
        });
        assert!(
            serde_json::from_value::<DynamicToolHostBuildRequest>(request).is_err(),
            "authority-bearing fields must be rejected by the wire DTO"
        );
    }

    #[test]
    fn host_configuration_and_implementation_bounds_fail_closed() {
        assert_eq!(
            HostPublisherConfig::from_values(None, None).err(),
            Some(HostPublisherError::PublisherUnconfigured)
        );
        assert_eq!(
            HostPublisherConfig::from_values(
                Some(OsString::from("Palyra.Invalid")),
                Some(OsString::from("global/dynamic_signer"))
            )
            .err(),
            Some(HostPublisherError::PublisherInvalid)
        );
        assert_eq!(
            HostPublisherConfig::from_values(
                Some(OsString::from(PUBLISHER)),
                Some(OsString::from("not-a-vault-ref"))
            )
            .err(),
            Some(HostPublisherError::SigningKeyRefInvalid)
        );
        assert_eq!(
            decode_host_implementation("not base64"),
            Err(HostPublisherError::ImplementationInvalid)
        );
        let oversized = "A".repeat(MAX_IMPLEMENTATION_BASE64_BYTES + 1);
        assert_eq!(
            decode_host_implementation(oversized.as_str()),
            Err(HostPublisherError::ImplementationInvalid)
        );
    }

    #[tokio::test]
    async fn signing_seed_requires_exact_raw_vault_bytes() {
        let temp = tempdir().expect("temporary root should initialize");
        let vault = Arc::new(
            Vault::open_with_config(VaultConfig {
                root: Some(temp.path().join("vault")),
                identity_store_root: Some(temp.path().join("identity")),
                backend_preference: BackendPreference::EncryptedFile,
                max_secret_bytes: 1024,
            })
            .expect("test vault should open"),
        );
        let missing = VaultRef::parse("global/missing").expect("vault ref should parse");
        assert_eq!(
            load_host_signing_seed(&vault, &missing).await.err(),
            Some(HostPublisherError::SigningKeyUnavailable)
        );
        vault
            .put_secret(&VaultScope::Global, "dynamic_signer", &[7; 31])
            .expect("malformed seed should persist for the negative test");
        let signing_ref = VaultRef::parse("global/dynamic_signer").expect("vault ref should parse");
        assert_eq!(
            load_host_signing_seed(&vault, &signing_ref).await.err(),
            Some(HostPublisherError::SigningKeyInvalid)
        );
        vault
            .put_secret(&VaultScope::Global, "dynamic_signer", &SIGNING_SEED)
            .expect("valid seed should persist");
        let seed =
            load_host_signing_seed(&vault, &signing_ref).await.expect("exact raw seed should load");
        assert_eq!(seed.as_ref(), &SIGNING_SEED);
    }

    #[test]
    fn host_policy_derives_capabilities_and_pins_trusted_key() {
        let runtime = test_runtime();
        let allowed = derive_host_allowed_capabilities(runtime.as_ref(), &proposal(None, false))
            .expect("configured child tool should be allowed");
        assert_eq!(allowed, vec!["tool:palyra.echo"]);
        let mut malicious = proposal(None, false);
        malicious.capability_needs.push("http_host:metadata.internal".to_owned());
        assert_eq!(
            derive_host_allowed_capabilities(runtime.as_ref(), &malicious),
            Err(HostPublisherError::CapabilityReviewDenied)
        );

        let artifact = host_artifact(None, false, 100_000);
        let public_key = BASE64_STANDARD
            .decode(artifact.signature.public_key_base64.as_bytes())
            .expect("public key should decode");
        let mut trust_store = SkillTrustStore::default();
        trust_store
            .trusted_publishers
            .insert(PUBLISHER.to_owned(), vec![hex::encode(public_key.as_slice())]);
        assert!(publisher_key_is_trusted(&trust_store, PUBLISHER, public_key.as_slice()));
        assert!(!publisher_key_is_trusted(&trust_store, PUBLISHER, &[0; 32]));
        assert_eq!(
            HostPublisherError::SigningKeyTrustMismatch.reason_code(),
            "dynamic_tool.signing_key_trust_mismatch"
        );
    }

    #[test]
    fn host_build_maps_static_secret_rejection_to_redacted_reason() {
        let mut plan: DeclarativeToolPlanV1 =
            serde_json::from_slice(implementation().as_slice()).expect("plan should parse");
        plan.steps[0].input_template = json!({"text": "sk-live-sensitive"});
        let error = match build_host_artifact_from_parts(
            proposal(None, false),
            serde_json::to_vec(&plan).expect("plan should serialize"),
            vec!["tool:palyra.echo".to_owned()],
            PUBLISHER.to_owned(),
            &Zeroizing::new(SIGNING_SEED),
            100_000,
        ) {
            Ok(_) => panic!("secret-bearing implementation must fail static evaluation"),
            Err(error) => error,
        };
        let mapped = map_host_build_error(&error);
        assert_eq!(mapped, HostPublisherError::StaticEvalFailed);
        assert_eq!(mapped.reason_code(), "dynamic_tool.static_eval_failed");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn host_build_runs_reviewed_lifecycle_dispatch_and_rollback() {
        let runtime = test_runtime();
        let original = host_artifact(None, false, 100_000);
        let first = activate(&runtime, &original, None).await;
        let registry = dynamic_tool_registry_entry(&first);
        assert_eq!(registry.name, "dynamic.host_echo");
        assert_eq!(registry.provenance, dynamic_tool_record_provenance(&first));

        let authority = DynamicToolReviewAuthority {
            session_id: "session:dispatch".to_owned(),
            run_id: "run:dispatch".to_owned(),
            principal: "user:operator".to_owned(),
            device_id: "device:console".to_owned(),
            channel: Some("console".to_owned()),
            host_policy_sha256: "a".repeat(64),
        };
        let outcome = execute_dynamic_tool(
            &runtime,
            runtime_context(&authority),
            "proposal:dispatch",
            registry.name.as_str(),
            br#"{"value":"hello"}"#,
            ToolRuntimeDispatchControls {
                remaining_tool_budget: None,
                cancellation_requested: None,
                process_progress_sink: None,
                cancellation_context: None,
                child_task_parent_context: None,
                expected_dynamic_provenance: Some(registry.provenance),
            },
        )
        .await;
        assert!(outcome.success, "active catalog entry should dispatch");

        let replacement = host_artifact(Some(original.artifact_sha256.clone()), true, 100_001);
        let second = activate(&runtime, &replacement, None).await;
        assert_eq!(
            second.decision.rollback_artifact_sha256,
            Some(original.artifact_sha256.clone())
        );
        let rolled_back = activate(&runtime, &original, Some(&second.decision)).await;
        assert_eq!(rolled_back.decision.reason_code, "dynamic_tool.rollback_activated");
        assert_eq!(rolled_back.artifact.artifact_sha256, original.artifact_sha256);
    }
}
