//! Host-sealed origin convergence for `RunStream` admission.
//!
//! Internal producers cross the loopback gRPC boundary with a short-lived,
//! one-shot capability. Request payload fields remain descriptive only and
//! cannot promote a caller into cron, internal, or delegation authority.

use std::{
    collections::BTreeMap,
    sync::{LazyLock, Mutex},
    time::{Duration, Instant},
};

use getrandom::fill as fill_random_bytes;
use serde_json::json;
use sha2::{Digest, Sha256};
use tonic::metadata::MetadataMap;

use crate::{
    agents::AgentResolveRequest,
    application::{
        daemon_lifecycle::DrainAdmissionPolicy,
        run_admission::{AdmissionCaller, AdmissionEnvironmentSnapshot, AdmissionQueueIntent},
        runtime_kernel_v2::{
            dispatcher::RuntimeKernelDispatcher, runtime_selection::HostVerifiedRunAdmission,
            selection::ResolvedRuntimeAuthorityIntent,
        },
        tool_registry::canonical_json_bytes,
    },
    gateway::GatewayRuntimeState,
    journal::OrchestratorSessionRecord,
    transport::grpc::{auth::RequestContext, proto::palyra::common::v1 as common_v1},
};

const INGRESS_PROOF_HEADER: &str = "x-palyra-run-ingress-proof";
const INGRESS_PROOF_TTL: Duration = Duration::from_secs(60);
const MAX_PENDING_INGRESS_PROOFS: usize = 1_024;

static PENDING_INGRESS_PROOFS: LazyLock<Mutex<BTreeMap<String, PendingIngressProof>>> =
    LazyLock::new(|| Mutex::new(BTreeMap::new()));

/// A host-established admission origin bound to one exact stream request.
///
/// Construction is intentionally private. Production boundaries use the
/// dedicated registration functions below, and source tests pin those
/// functions to the approved producer modules.
#[derive(Debug)]
pub(crate) struct RunStreamAdmissionIngress {
    kind: RunStreamAdmissionIngressKind,
}

#[derive(Debug)]
enum RunStreamAdmissionIngressKind {
    Console,
    Channel,
    Cron { origin_run_id: Option<String> },
    Internal { origin_run_id: Option<String> },
    Delegation { origin_run_id: String, delegated_admission_json: String },
}

impl RunStreamAdmissionIngress {
    /// Returns whether this host proof carries delegated child authority.
    #[must_use]
    pub(crate) const fn is_delegation(&self) -> bool {
        matches!(&self.kind, RunStreamAdmissionIngressKind::Delegation { .. })
    }

    /// Issues the sole sealed input accepted by the runtime admission controller.
    ///
    /// The returned proof carries host-established provenance; none of its
    /// origin authority is derived from `RunStreamRequest.origin_kind`.
    #[must_use]
    pub(crate) fn issue(
        &self,
        dispatcher: &RuntimeKernelDispatcher,
        caller: AdmissionCaller,
        environment: AdmissionEnvironmentSnapshot,
        authority: ResolvedRuntimeAuthorityIntent,
        queue_intent: Option<AdmissionQueueIntent>,
    ) -> HostVerifiedRunAdmission {
        match &self.kind {
            RunStreamAdmissionIngressKind::Console => {
                dispatcher.issue_console_admission(caller, environment, authority, queue_intent)
            }
            RunStreamAdmissionIngressKind::Channel => {
                dispatcher.issue_channel_admission(caller, environment, authority, queue_intent)
            }
            RunStreamAdmissionIngressKind::Cron { origin_run_id } => dispatcher
                .issue_cron_admission(
                    caller,
                    environment,
                    authority,
                    origin_run_id.clone(),
                    queue_intent,
                ),
            RunStreamAdmissionIngressKind::Internal { origin_run_id } => dispatcher
                .issue_internal_admission(
                    caller,
                    environment,
                    authority,
                    origin_run_id.clone(),
                    queue_intent,
                ),
            RunStreamAdmissionIngressKind::Delegation {
                origin_run_id,
                delegated_admission_json,
            } => dispatcher.issue_delegation_admission(
                caller,
                environment,
                authority,
                origin_run_id.clone(),
                delegated_admission_json.clone(),
                queue_intent,
            ),
        }
    }
}

/// Captures the access, workspace, and queue policy used by every V2 ingress.
///
/// # Errors
/// Returns a transport status when the host cannot resolve the session's
/// agent/workspace binding.
pub(crate) async fn admission_environment(
    runtime_state: &std::sync::Arc<GatewayRuntimeState>,
    request_context: &RequestContext,
    session: &OrchestratorSessionRecord,
) -> Result<AdmissionEnvironmentSnapshot, tonic::Status> {
    let resolved_agent = runtime_state
        .resolve_agent_for_context(AgentResolveRequest {
            principal: request_context.principal.clone(),
            channel: request_context.channel.clone(),
            session_id: Some(session.session_id.clone()),
            preferred_agent_id: None,
            persist_session_binding: false,
        })
        .await?;
    let workspace_binding = canonical_json_bytes(&json!({
        "agent_id": resolved_agent.agent.agent_id,
        "workspace_roots": resolved_agent.agent.workspace_roots,
    }));
    let workspace_sha256 = hex::encode(Sha256::digest(workspace_binding));
    let access_allowed = session.principal == request_context.principal
        && session.device_id == request_context.device_id
        && session.channel == request_context.channel;
    let queue_policy = &runtime_state.config.session_queue_policy;
    let access_policy_json = canonical_policy_json(json!({
        "allow": access_allowed,
        "principal_binding": "authenticated_session_owner",
    }))?;
    let queue_policy_json = canonical_policy_json(json!({
        "mode": queue_policy.mode.as_str(),
        "max_depth": queue_policy.max_depth,
        "merge_window_ms": queue_policy.merge_window_ms,
    }))?;
    let lifecycle = runtime_state
        .daemon_lifecycle_snapshot()
        .map_err(|status| tonic::Status::internal(status.message().to_owned()))?;
    let draining = lifecycle.phase.blocks_admission()
        && lifecycle.admission_policy == DrainAdmissionPolicy::RejectNew;
    let drain_reason = draining.then(|| lifecycle.reason_code.clone());
    Ok(AdmissionEnvironmentSnapshot::host_snapshot(
        workspace_sha256,
        access_policy_json,
        queue_policy_json,
        draining,
        drain_reason,
        u64::try_from(queue_policy.max_depth).unwrap_or(u64::MAX),
    ))
}

fn canonical_policy_json(value: serde_json::Value) -> Result<String, tonic::Status> {
    String::from_utf8(canonical_json_bytes(&value))
        .map_err(|_| tonic::Status::internal("canonical policy serializer emitted invalid UTF-8"))
}

/// A consumed capability awaiting its first-message identity check.
///
/// This type cannot be cloned. A proof therefore establishes at most one
/// stream origin and cannot be replayed onto another request.
#[derive(Debug)]
pub(crate) struct PendingRunStreamAdmissionIngress {
    ingress: RunStreamAdmissionIngress,
    binding: Option<RunStreamIngressBinding>,
}

impl PendingRunStreamAdmissionIngress {
    /// Binds the consumed host proof to authenticated context and exact IDs.
    ///
    /// # Errors
    /// Returns [`RunStreamAdmissionIngressError::BindingMismatch`] when the
    /// authenticated context or first-message identities differ from the
    /// producer-established binding.
    pub(crate) fn bind_first_message(
        self,
        context: &RequestContext,
        message: &common_v1::RunStreamRequest,
    ) -> Result<RunStreamAdmissionIngress, RunStreamAdmissionIngressError> {
        let Some(binding) = self.binding else {
            return Ok(self.ingress);
        };
        let session_id = message.session_id.as_ref().map(|value| value.ulid.as_str());
        let run_id = message.run_id.as_ref().map(|value| value.ulid.as_str());
        let origin_run_id = message.origin_run_id.as_ref().map(|value| value.ulid.as_str());
        if binding.principal != context.principal {
            return Err(RunStreamAdmissionIngressError::BindingMismatch("principal"));
        }
        if binding.device_id != context.device_id {
            return Err(RunStreamAdmissionIngressError::BindingMismatch("device_id"));
        }
        if binding.channel.as_deref() != context.channel.as_deref() {
            return Err(RunStreamAdmissionIngressError::BindingMismatch("channel"));
        }
        if Some(binding.session_id.as_str()) != session_id {
            return Err(RunStreamAdmissionIngressError::BindingMismatch("session_id"));
        }
        if Some(binding.run_id.as_str()) != run_id {
            return Err(RunStreamAdmissionIngressError::BindingMismatch("run_id"));
        }
        if binding.origin_run_id.as_deref() != origin_run_id {
            return Err(RunStreamAdmissionIngressError::BindingMismatch("origin_run_id"));
        }
        if let Some(expected_digest) = binding.delegated_admission_sha256.as_deref() {
            let observed_digest = hex::encode(Sha256::digest(&message.parameter_delta_json));
            if expected_digest != observed_digest {
                return Err(RunStreamAdmissionIngressError::BindingMismatch(
                    "delegated_admission_json",
                ));
            }
        }
        Ok(self.ingress)
    }
}

#[derive(Debug)]
struct PendingIngressProof {
    ingress: RunStreamAdmissionIngress,
    binding: RunStreamIngressBinding,
    expires_at: Instant,
}

#[derive(Debug)]
struct RunStreamIngressBinding {
    principal: String,
    device_id: String,
    channel: Option<String>,
    session_id: String,
    run_id: String,
    origin_run_id: Option<String>,
    delegated_admission_sha256: Option<String>,
}

/// Registers scheduler-owned provenance and writes its opaque one-shot token.
///
/// # Errors
/// Returns [`RunStreamAdmissionIngressError`] when secure random generation,
/// metadata encoding, or bounded registry insertion fails.
pub(crate) fn register_cron_ingress(
    metadata: &mut MetadataMap,
    principal: &str,
    device_id: &str,
    channel: Option<&str>,
    session_id: &str,
    run_id: &str,
    origin_run_id: Option<&str>,
) -> Result<(), RunStreamAdmissionIngressError> {
    register_ingress(
        metadata,
        RunStreamAdmissionIngress {
            kind: RunStreamAdmissionIngressKind::Cron {
                origin_run_id: origin_run_id.map(ToOwned::to_owned),
            },
        },
        ingress_binding(principal, device_id, channel, session_id, run_id, origin_run_id, None),
    )
}

/// Registers daemon-owned continuation provenance and its exact binding.
///
/// # Errors
/// Returns [`RunStreamAdmissionIngressError`] when secure random generation,
/// metadata encoding, or bounded registry insertion fails.
pub(crate) fn register_internal_ingress(
    metadata: &mut MetadataMap,
    principal: &str,
    device_id: &str,
    channel: Option<&str>,
    session_id: &str,
    run_id: &str,
    origin_run_id: Option<&str>,
) -> Result<(), RunStreamAdmissionIngressError> {
    register_ingress(
        metadata,
        RunStreamAdmissionIngress {
            kind: RunStreamAdmissionIngressKind::Internal {
                origin_run_id: origin_run_id.map(ToOwned::to_owned),
            },
        },
        ingress_binding(principal, device_id, channel, session_id, run_id, origin_run_id, None),
    )
}

/// Exact host-authenticated fields needed to seal one delegated ingress.
pub(crate) struct DelegationIngressRegistration<'a> {
    principal: &'a str,
    device_id: &'a str,
    channel: Option<&'a str>,
    session_id: &'a str,
    run_id: &'a str,
    origin_run_id: &'a str,
    delegated_admission_json: String,
}

impl<'a> DelegationIngressRegistration<'a> {
    /// Binds delegated authority to its authenticated stream identity.
    #[must_use]
    pub(crate) fn new(
        principal: &'a str,
        device_id: &'a str,
        channel: Option<&'a str>,
        session_id: &'a str,
        run_id: &'a str,
        origin_run_id: &'a str,
        delegated_admission_json: String,
    ) -> Self {
        Self {
            principal,
            device_id,
            channel,
            session_id,
            run_id,
            origin_run_id,
            delegated_admission_json,
        }
    }
}

/// Registers delegated child provenance with host-validated authority JSON.
///
/// # Errors
/// Returns [`RunStreamAdmissionIngressError`] when secure random generation,
/// metadata encoding, or bounded registry insertion fails.
pub(crate) fn register_delegation_ingress(
    metadata: &mut MetadataMap,
    registration: DelegationIngressRegistration<'_>,
) -> Result<(), RunStreamAdmissionIngressError> {
    let DelegationIngressRegistration {
        principal,
        device_id,
        channel,
        session_id,
        run_id,
        origin_run_id,
        delegated_admission_json,
    } = registration;
    let delegated_admission_sha256 =
        hex::encode(Sha256::digest(delegated_admission_json.as_bytes()));
    register_ingress(
        metadata,
        RunStreamAdmissionIngress {
            kind: RunStreamAdmissionIngressKind::Delegation {
                origin_run_id: origin_run_id.to_owned(),
                delegated_admission_json,
            },
        },
        ingress_binding(
            principal,
            device_id,
            channel,
            session_id,
            run_id,
            Some(origin_run_id),
            Some(delegated_admission_sha256),
        ),
    )
}

/// Establishes channel provenance after connector and route authentication.
#[must_use]
pub(crate) fn channel_ingress() -> RunStreamAdmissionIngress {
    RunStreamAdmissionIngress { kind: RunStreamAdmissionIngressKind::Channel }
}

/// Consumes an internal proof or establishes the external console origin.
///
/// Absence is deliberately mapped to console. A request cannot select
/// another origin through payload or ordinary identity metadata.
///
/// # Errors
/// Returns [`RunStreamAdmissionIngressError`] for duplicate, malformed,
/// expired, unknown, or replayed proof tokens.
pub(crate) fn consume_run_stream_ingress(
    metadata: &MetadataMap,
) -> Result<PendingRunStreamAdmissionIngress, RunStreamAdmissionIngressError> {
    let mut proof_values = metadata.get_all(INGRESS_PROOF_HEADER).iter();
    let Some(proof_value) = proof_values.next() else {
        return Ok(PendingRunStreamAdmissionIngress {
            ingress: RunStreamAdmissionIngress { kind: RunStreamAdmissionIngressKind::Console },
            binding: None,
        });
    };
    if proof_values.next().is_some() {
        return Err(RunStreamAdmissionIngressError::DuplicateProof);
    }
    let token = proof_value.to_str().map_err(|_| RunStreamAdmissionIngressError::MalformedProof)?;
    let now = Instant::now();
    let proof = pending_proofs()
        .remove(token)
        .ok_or(RunStreamAdmissionIngressError::UnknownOrReplayedProof)?;
    if proof.expires_at <= now {
        return Err(RunStreamAdmissionIngressError::ExpiredProof);
    }
    Ok(PendingRunStreamAdmissionIngress { ingress: proof.ingress, binding: Some(proof.binding) })
}

fn register_ingress(
    metadata: &mut MetadataMap,
    ingress: RunStreamAdmissionIngress,
    binding: RunStreamIngressBinding,
) -> Result<(), RunStreamAdmissionIngressError> {
    let mut random = [0_u8; 32];
    fill_random_bytes(&mut random)
        .map_err(|error| RunStreamAdmissionIngressError::Random(error.to_string()))?;
    let token = hex::encode(random);
    let metadata_value =
        token.parse().map_err(|_| RunStreamAdmissionIngressError::MetadataEncoding)?;
    let now = Instant::now();
    let mut proofs = pending_proofs();
    proofs.retain(|_, proof| proof.expires_at > now);
    if proofs.len() >= MAX_PENDING_INGRESS_PROOFS {
        return Err(RunStreamAdmissionIngressError::RegistryCapacity);
    }
    match proofs.entry(token) {
        std::collections::btree_map::Entry::Vacant(entry) => {
            entry.insert(PendingIngressProof {
                ingress,
                binding,
                expires_at: now + INGRESS_PROOF_TTL,
            });
        }
        std::collections::btree_map::Entry::Occupied(_) => {
            return Err(RunStreamAdmissionIngressError::TokenCollision);
        }
    }
    metadata.insert(INGRESS_PROOF_HEADER, metadata_value);
    Ok(())
}

fn ingress_binding(
    principal: &str,
    device_id: &str,
    channel: Option<&str>,
    session_id: &str,
    run_id: &str,
    origin_run_id: Option<&str>,
    delegated_admission_sha256: Option<String>,
) -> RunStreamIngressBinding {
    RunStreamIngressBinding {
        principal: principal.trim().to_owned(),
        device_id: device_id.trim().to_owned(),
        channel: channel.map(str::trim).filter(|value| !value.is_empty()).map(ToOwned::to_owned),
        session_id: session_id.to_owned(),
        run_id: run_id.to_owned(),
        origin_run_id: origin_run_id.map(ToOwned::to_owned),
        delegated_admission_sha256,
    }
}

fn pending_proofs() -> std::sync::MutexGuard<'static, BTreeMap<String, PendingIngressProof>> {
    PENDING_INGRESS_PROOFS.lock().expect("run-stream ingress proof registry mutex is not poisoned")
}

/// Fail-closed ingress proof error.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub(crate) enum RunStreamAdmissionIngressError {
    #[error("failed to generate secure run-stream ingress proof: {0}")]
    Random(String),
    #[error("failed to encode run-stream ingress proof metadata")]
    MetadataEncoding,
    #[error("run-stream ingress proof metadata is duplicated")]
    DuplicateProof,
    #[error("run-stream ingress proof metadata is malformed")]
    MalformedProof,
    #[error("run-stream ingress proof is unknown or was already consumed")]
    UnknownOrReplayedProof,
    #[error("run-stream ingress proof expired before admission")]
    ExpiredProof,
    #[error("run-stream ingress proof registry reached its bounded capacity")]
    RegistryCapacity,
    #[error("secure run-stream ingress proof token collided")]
    TokenCollision,
    #[error("run-stream ingress proof does not match authenticated {0}")]
    BindingMismatch(&'static str),
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, fs, path::PathBuf};

    use super::*;

    fn request(
        session_id: &str,
        run_id: &str,
        origin_run_id: Option<&str>,
    ) -> common_v1::RunStreamRequest {
        common_v1::RunStreamRequest {
            session_id: Some(common_v1::CanonicalId { ulid: session_id.to_owned() }),
            run_id: Some(common_v1::CanonicalId { ulid: run_id.to_owned() }),
            origin_run_id: origin_run_id
                .map(|ulid| common_v1::CanonicalId { ulid: ulid.to_owned() }),
            ..Default::default()
        }
    }

    fn context() -> RequestContext {
        RequestContext {
            principal: "principal:test".to_owned(),
            device_id: "01J00000000000000000000000".to_owned(),
            channel: Some("system:test".to_owned()),
        }
    }

    #[test]
    fn admission_policy_json_uses_the_digest_verifiers_canonical_order() {
        let policy = canonical_policy_json(json!({
            "mode": "queue",
            "max_depth": 8,
            "merge_window_ms": 250,
        }))
        .expect("canonical JSON");

        assert_eq!(policy, r#"{"max_depth":8,"merge_window_ms":250,"mode":"queue"}"#);
    }

    #[test]
    fn absent_proof_is_console_even_when_payload_claims_delegation() {
        let metadata = MetadataMap::new();
        let pending = consume_run_stream_ingress(&metadata).expect("console ingress");
        let mut message = request("session", "run", Some("parent"));
        message.origin_kind = "delegation".to_owned();
        let ingress = pending.bind_first_message(&context(), &message).expect("console binding");
        assert!(matches!(ingress.kind, RunStreamAdmissionIngressKind::Console));
    }

    #[test]
    fn internal_proof_is_one_shot_and_exactly_bound() {
        let mut metadata = MetadataMap::new();
        let context = context();
        register_internal_ingress(
            &mut metadata,
            context.principal.as_str(),
            context.device_id.as_str(),
            context.channel.as_deref(),
            "session",
            "run",
            Some("parent"),
        )
        .expect("internal proof");
        let pending = consume_run_stream_ingress(&metadata).expect("first consume");
        assert!(matches!(
            consume_run_stream_ingress(&metadata),
            Err(RunStreamAdmissionIngressError::UnknownOrReplayedProof)
        ));
        let ingress = pending
            .bind_first_message(&context, &request("session", "run", Some("parent")))
            .expect("matching binding");
        assert!(matches!(ingress.kind, RunStreamAdmissionIngressKind::Internal { .. }));
    }

    #[test]
    fn proof_rejects_authenticated_identity_or_request_mismatch() {
        let cases = [
            (
                "principal",
                "other",
                "01J00000000000000000000000",
                Some("system:test"),
                "session",
                "run",
                Some("parent"),
            ),
            (
                "device_id",
                "principal:test",
                "01J00000000000000000000001",
                Some("system:test"),
                "session",
                "run",
                Some("parent"),
            ),
            (
                "channel",
                "principal:test",
                "01J00000000000000000000000",
                Some("system:other"),
                "session",
                "run",
                Some("parent"),
            ),
            (
                "session_id",
                "principal:test",
                "01J00000000000000000000000",
                Some("system:test"),
                "other",
                "run",
                Some("parent"),
            ),
            (
                "run_id",
                "principal:test",
                "01J00000000000000000000000",
                Some("system:test"),
                "session",
                "other",
                Some("parent"),
            ),
            (
                "origin_run_id",
                "principal:test",
                "01J00000000000000000000000",
                Some("system:test"),
                "session",
                "run",
                Some("other"),
            ),
        ];
        for (expected, principal, device_id, channel, session_id, run_id, origin_run_id) in cases {
            let mut metadata = MetadataMap::new();
            let trusted = context();
            register_internal_ingress(
                &mut metadata,
                trusted.principal.as_str(),
                trusted.device_id.as_str(),
                trusted.channel.as_deref(),
                "session",
                "run",
                Some("parent"),
            )
            .expect("internal proof");
            let pending = consume_run_stream_ingress(&metadata).expect("consume");
            let observed = RequestContext {
                principal: principal.to_owned(),
                device_id: device_id.to_owned(),
                channel: channel.map(ToOwned::to_owned),
            };
            assert!(matches!(
                pending.bind_first_message(
                    &observed,
                    &request(session_id, run_id, origin_run_id),
                ),
                Err(RunStreamAdmissionIngressError::BindingMismatch(field))
                    if field == expected
            ));
        }
    }

    #[test]
    fn delegation_proof_binds_the_exact_authority_document() {
        let mut metadata = MetadataMap::new();
        let context = context();
        register_delegation_ingress(
            &mut metadata,
            DelegationIngressRegistration::new(
                context.principal.as_str(),
                context.device_id.as_str(),
                context.channel.as_deref(),
                "session",
                "run",
                "parent",
                r#"{"delegation":"trusted"}"#.to_owned(),
            ),
        )
        .expect("delegation proof");
        let pending = consume_run_stream_ingress(&metadata).expect("consume");
        let mut message = request("session", "run", Some("parent"));
        message.parameter_delta_json = br#"{"delegation":"mutated"}"#.to_vec();
        assert!(matches!(
            pending.bind_first_message(&context, &message),
            Err(RunStreamAdmissionIngressError::BindingMismatch("delegated_admission_json"))
        ));
    }

    #[test]
    fn host_ingress_entry_points_have_only_approved_production_callers() {
        let source_root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src");
        let allowed = BTreeMap::from([
            ("register_cron_ingress(", &["cron.rs"][..]),
            ("register_internal_ingress(", &["background_queue.rs"][..]),
            ("register_delegation_ingress(", &["background_queue.rs"][..]),
            ("channel_ingress().issue(", &["application/route_message/orchestration.rs"][..]),
            ("consume_run_stream_ingress(", &["transport/grpc/services/gateway/service.rs"][..]),
        ]);
        let this_file = source_root.join("application/run_stream/admission_ingress.rs");
        let mut pending = vec![source_root.clone()];
        let mut violations = Vec::new();
        while let Some(path) = pending.pop() {
            for entry in fs::read_dir(path).expect("source directory should be readable") {
                let path = entry.expect("source entry should be readable").path();
                if path.is_dir() {
                    pending.push(path);
                    continue;
                }
                if path.extension().and_then(|value| value.to_str()) != Some("rs")
                    || path == this_file
                {
                    continue;
                }
                let source = fs::read_to_string(&path).expect("Rust source should be readable");
                let relative = path
                    .strip_prefix(&source_root)
                    .expect("source file should stay below crate src")
                    .to_string_lossy()
                    .replace('\\', "/");
                for (needle, allowed_paths) in &allowed {
                    if source.contains(needle) && !allowed_paths.contains(&relative.as_str()) {
                        violations.push(format!("{needle} in {relative}"));
                    }
                }
            }
        }
        assert!(
            violations.is_empty(),
            "host ingress mint/consume callsites escaped their approved boundaries: {violations:?}"
        );
    }
}
