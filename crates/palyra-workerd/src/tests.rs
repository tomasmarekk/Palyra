//! Unit coverage for worker lifecycle, leases, remote tools, and QA fault invariants.
//!
//! This child module retains access to crate-private implementation contracts.

#[cfg(feature = "qa-fault-injection")]
use std::sync::{Arc, Mutex};

#[cfg(feature = "qa-fault-injection")]
use palyra_common::qa_fault_injection::{
    DeterministicQaFaultController, QaFaultAction, QaFaultActivation, QaFaultActivationDirective,
    QaFaultCheckpoint, QaFaultControllerRecord, QaFaultDirective, QaFaultInjectionPlan,
    QaFaultProbe, QaFaultProbeError, QaFaultProbeHandle, QaFaultRecoveryClass,
    QA_FAULT_INJECTION_PLAN_FORMAT, QA_FAULT_INJECTION_PLAN_SCHEMA_VERSION,
};
use sha2::{Digest, Sha256};

use crate::remote_protocol::RemoteWorkerProtocolV1;

use super::{
    networked_worker_lifecycle_event_id, RuntimeGeneration, TrustedEndpointHealth,
    TrustedEndpointPolicy, TrustedEndpointRecord, TrustedEndpointRegistry,
    TrustedEndpointTransport, TrustedEndpointTrustState, WorkerArtifactTransport,
    WorkerAttestation, WorkerCleanupReport, WorkerFleetManager, WorkerFleetPolicy,
    WorkerLeaseIdentity, WorkerLeaseRequest, WorkerLifecycleError, WorkerLifecycleEvent,
    WorkerLifecycleState, WorkerRemoteIdentity, WorkerRemoteLeaseBinding,
    WorkerRemoteToolContractError, WorkerRemoteToolKind, WorkerRemoteToolRequestEnvelope,
    WorkerRemoteToolResultEnvelope, WorkerRemoteWorkspaceTransfer, WorkerRunGrant,
    WorkerWorkspaceScope, WORKER_REMOTE_TOOL_PROTOCOL, WORKER_REMOTE_TOOL_SCHEMA_VERSION,
};

fn hex_digest(byte: &str) -> String {
    byte.repeat(64)
}

fn sha256_hex(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    hex::encode(hasher.finalize())
}

fn attestation(worker_id: &str) -> WorkerAttestation {
    WorkerAttestation {
        worker_id: worker_id.to_owned(),
        image_digest_sha256: "img".repeat(16),
        build_digest_sha256: "bld".repeat(16),
        artifact_digest_sha256: "art".repeat(16),
        egress_proxy_attested: true,
        supported_capabilities: vec!["tool:palyra.echo".to_owned()],
        capability_authority_sha256: None,
        sdk_protocol_version: 1,
        wit_abi_version: "palyra-worker-abi/v1".to_owned(),
        heartbeat_unix_ms: 2_000,
        issued_at_unix_ms: 1_000,
        expires_at_unix_ms: 10_000,
    }
}

fn trusted_endpoint_record() -> TrustedEndpointRecord {
    TrustedEndpointRecord {
        endpoint_id: "worker-endpoint-a".to_owned(),
        trust_state: TrustedEndpointTrustState::Unknown,
        last_seen_unix_ms: 2_000,
        capabilities: vec!["tool:palyra.fs.read_file".to_owned()],
        transport: TrustedEndpointTransport::Quic,
        identity_digest_sha256: hex_digest("a"),
        policy_bindings: Vec::new(),
        health: TrustedEndpointHealth {
            healthy: true,
            checked_at_unix_ms: 2_000,
            failure_reason: None,
        },
    }
}

#[test]
fn trusted_endpoint_observation_requires_explicit_approval() {
    let mut registry = TrustedEndpointRegistry::default();
    let policy = TrustedEndpointPolicy::default();

    let record = registry
        .observe_endpoint(trusted_endpoint_record(), &policy)
        .expect("endpoint observation should persist");
    let negotiation = registry
        .negotiate_capabilities(
            record.endpoint_id.as_str(),
            &["tool:palyra.fs.read_file".to_owned()],
            &policy,
        )
        .expect("observed endpoint should be negotiable");

    assert_eq!(record.trust_state, TrustedEndpointTrustState::PendingApproval);
    assert!(!negotiation.usable);
    assert_eq!(negotiation.decision_reason, "trust_required");
}

#[test]
fn trusted_endpoint_negotiation_grants_only_policy_capabilities() {
    let mut registry = TrustedEndpointRegistry::default();
    let policy = TrustedEndpointPolicy::default();
    registry
        .observe_endpoint(trusted_endpoint_record(), &policy)
        .expect("endpoint observation should persist");
    registry
        .approve_endpoint("worker-endpoint-a", vec!["operator-approved".to_owned()], 2_100)
        .expect("endpoint approval should succeed");

    let negotiation = registry
        .negotiate_capabilities(
            "worker-endpoint-a",
            &["tool:palyra.fs.read_file".to_owned(), "tool:palyra.untrusted".to_owned()],
            &policy,
        )
        .expect("trusted endpoint should negotiate");

    assert!(!negotiation.usable);
    assert_eq!(negotiation.granted_capabilities, ["tool:palyra.fs.read_file"]);
    assert_eq!(negotiation.denied_capabilities, ["tool:palyra.untrusted"]);
    assert_eq!(negotiation.decision_reason, "capability_denied");
}

#[test]
fn trusted_endpoint_preview_transports_are_disabled_by_default() {
    let mut registry = TrustedEndpointRegistry::default();
    let policy = TrustedEndpointPolicy::default();
    let mut record = trusted_endpoint_record();
    record.transport = TrustedEndpointTransport::LanDiscoveryPreview;
    registry.observe_endpoint(record, &policy).expect("endpoint observation should persist");
    registry
        .approve_endpoint("worker-endpoint-a", vec!["operator-approved".to_owned()], 2_100)
        .expect("endpoint approval should succeed");

    let negotiation = registry
        .negotiate_capabilities(
            "worker-endpoint-a",
            &["tool:palyra.fs.read_file".to_owned()],
            &policy,
        )
        .expect("trusted endpoint should negotiate");

    assert!(!negotiation.usable);
    assert_eq!(negotiation.decision_reason, "preview_transport_disabled");
}

fn lease_request(run_id: &str, ttl_ms: u64) -> WorkerLeaseRequest {
    WorkerLeaseRequest {
        run_id: run_id.to_owned(),
        ttl_ms,
        required_capabilities: Vec::new(),
        workspace_scope: WorkerWorkspaceScope {
            workspace_root: "/workspace".to_owned(),
            allowed_paths: vec!["src".to_owned()],
            read_only: false,
        },
        artifact_transport: WorkerArtifactTransport {
            input_manifest_sha256: "in".repeat(32),
            output_manifest_sha256: "out".repeat(32),
            log_stream_id: "log-stream".to_owned(),
            scratch_directory_id: "scratch".to_owned(),
        },
        grant: WorkerRunGrant {
            grant_id: format!("grant-{run_id}"),
            run_id: run_id.to_owned(),
            tool_name: "palyra.echo".to_owned(),
            expires_at_unix_ms: 9_000,
        },
    }
}

#[cfg(feature = "qa-fault-injection")]
struct RecoveryRejectingProbe {
    activation: QaFaultActivation,
    attempts: Arc<Mutex<Vec<(String, QaFaultRecoveryClass)>>>,
}

#[cfg(feature = "qa-fault-injection")]
impl QaFaultProbe for RecoveryRejectingProbe {
    fn checkpoint(
        &self,
        checkpoint: QaFaultCheckpoint<'_>,
    ) -> Result<QaFaultDirective, QaFaultProbeError> {
        if checkpoint.point_id != self.activation.point_id
            || self.activation.actor.as_deref() != Some(checkpoint.actor)
        {
            return Ok(QaFaultDirective::Continue);
        }
        Ok(QaFaultDirective::Activate(QaFaultActivationDirective {
            activation: self.activation.clone(),
            actor: checkpoint.actor.to_owned(),
            observed_occurrence: 1,
            activation_sequence: 1,
        }))
    }

    fn record_recovery(
        &self,
        activation_id: &str,
        recovery_class: QaFaultRecoveryClass,
    ) -> Result<(), QaFaultProbeError> {
        self.attempts
            .lock()
            .expect("test recovery attempt lock must remain usable")
            .push((activation_id.to_owned(), recovery_class));
        Err(QaFaultProbeError::AdapterFailure("test recovery evidence rejection"))
    }

    fn records(&self) -> Result<Vec<QaFaultControllerRecord>, QaFaultProbeError> {
        Ok(Vec::new())
    }
}

fn policy_for(capability: &str) -> WorkerFleetPolicy {
    WorkerFleetPolicy { trusted_capabilities: vec![capability.into()], ..Default::default() }
}

#[test]
fn lifecycle_event_ids_are_deterministic_and_transition_bound() {
    let event = WorkerLifecycleEvent {
        worker_id: "worker-lifecycle-id".to_owned(),
        state: WorkerLifecycleState::Assigned,
        run_id: Some("run-lifecycle-id".to_owned()),
        lease_id: Some("lease-lifecycle-id".to_owned()),
        reason_code: "worker.assigned".to_owned(),
        timestamp_unix_ms: 1_730_000_000_000,
    };

    let first = networked_worker_lifecycle_event_id("transition-lifecycle-id", &event)
        .expect("valid lifecycle evidence should derive an identity");
    let replay = networked_worker_lifecycle_event_id("transition-lifecycle-id", &event)
        .expect("the same lifecycle evidence should replay deterministically");
    let distinct_transition =
        networked_worker_lifecycle_event_id("transition-lifecycle-id-next", &event)
            .expect("a second valid transition should derive an identity");

    assert_eq!(first, replay);
    assert_ne!(first, distinct_transition);
    assert!(first.starts_with("worker-lifecycle:"));
}

#[test]
fn lifecycle_event_ids_distinguish_optional_identity_boundaries() {
    let event_without_run = WorkerLifecycleEvent {
        worker_id: "worker-lifecycle-id".to_owned(),
        state: WorkerLifecycleState::Registered,
        run_id: None,
        lease_id: None,
        reason_code: "worker.registered".to_owned(),
        timestamp_unix_ms: 1_730_000_000_000,
    };
    let mut event_with_run = event_without_run.clone();
    event_with_run.run_id = Some(String::new());

    let without_run =
        networked_worker_lifecycle_event_id("transition-lifecycle-id", &event_without_run)
            .expect("runless registration evidence should derive an identity");
    let error = networked_worker_lifecycle_event_id("transition-lifecycle-id", &event_with_run)
        .expect_err("an explicitly empty run identity must not alias a missing identity");

    assert!(without_run.starts_with("worker-lifecycle:"));
    assert_eq!(error, WorkerLifecycleError::InvalidLifecycleEvidence);
}

fn remote_identity(worker_id: &str) -> WorkerRemoteIdentity {
    WorkerRemoteIdentity {
        worker_id: worker_id.to_owned(),
        image_digest_sha256: hex_digest("a"),
        build_digest_sha256: hex_digest("b"),
        artifact_digest_sha256: hex_digest("c"),
        capability_authority_sha256: Some(hex_digest("d")),
        sdk_protocol_version: 1,
        wit_abi_version: "palyra-worker-abi/v1".to_owned(),
    }
}

fn remote_request(tool_name: &str) -> WorkerRemoteToolRequestEnvelope {
    let tool_kind = WorkerRemoteToolKind::from_tool_name(tool_name)
        .expect("test tool should be remote-capable");
    let input_json = r#"{"path":"src/lib.rs"}"#.to_owned();
    WorkerRemoteToolRequestEnvelope {
        protocol: WORKER_REMOTE_TOOL_PROTOCOL.to_owned(),
        schema_version: WORKER_REMOTE_TOOL_SCHEMA_VERSION,
        request_id: "remote-request-01".to_owned(),
        proposal_id: "proposal-01".to_owned(),
        tool_name: tool_name.to_owned(),
        tool_kind,
        input_json_sha256: sha256_hex(input_json.as_bytes()),
        input_json,
        lease: WorkerRemoteLeaseBinding {
            lease_id: "lease-01".to_owned(),
            worker_id: "worker-remote-01".to_owned(),
            session_id: "session-01".to_owned(),
            run_id: "run-01".to_owned(),
            run_generation: RuntimeGeneration::new(7).expect("test generation should be valid"),
            grant_id: "grant-01".to_owned(),
            grant_tool_name: tool_name.to_owned(),
            issued_at_unix_ms: 2_000,
            expires_at_unix_ms: 3_000,
            required_capabilities: vec![tool_kind.required_capability()],
            process_executable_allowlist: if matches!(tool_kind, WorkerRemoteToolKind::ProcessRun) {
                vec!["echo".to_owned()]
            } else {
                Vec::new()
            },
            work_graph_claim: None,
            work_graph_posture: Default::default(),
            workspace_scope: WorkerWorkspaceScope {
                workspace_root: "/workspace".to_owned(),
                allowed_paths: vec!["src".to_owned()],
                read_only: true,
            },
            artifact_transport: WorkerArtifactTransport {
                input_manifest_sha256: hex_digest("2"),
                output_manifest_sha256: hex_digest("3"),
                log_stream_id: "logs/run-01/proposal-01".to_owned(),
                scratch_directory_id: "scratch/run-01/proposal-01".to_owned(),
            },
        },
        worker_identity: remote_identity("worker-remote-01"),
        workspace_transfer: WorkerRemoteWorkspaceTransfer::manifest(hex_digest("4")),
        encrypted_secret_artifact: None,
        canonical_protocol: None,
    }
}

fn remote_result(
    request: &WorkerRemoteToolRequestEnvelope,
    output_json: &str,
) -> WorkerRemoteToolResultEnvelope {
    WorkerRemoteToolResultEnvelope {
        protocol: WORKER_REMOTE_TOOL_PROTOCOL.to_owned(),
        schema_version: WORKER_REMOTE_TOOL_SCHEMA_VERSION,
        request_id: request.request_id.clone(),
        proposal_id: request.proposal_id.clone(),
        tool_name: request.tool_name.clone(),
        tool_kind: request.tool_kind,
        worker_id: request.lease.worker_id.clone(),
        lease_id: request.lease.lease_id.clone(),
        run_generation: request.lease.run_generation,
        success: true,
        output_json: output_json.to_owned(),
        output_json_sha256: sha256_hex(output_json.as_bytes()),
        error: None,
        output_manifest_sha256: hex_digest("6"),
        cleanup_report: WorkerCleanupReport {
            removed_workspace_scope: true,
            removed_artifacts: true,
            removed_logs: true,
            failure_reason: None,
        },
        worker_identity: request.worker_identity.clone(),
        completed_at_unix_ms: 2_000,
    }
}

#[test]
fn remote_tool_kind_maps_backend_parity_tools() {
    let cases = [
        ("palyra.fs.read_file", WorkerRemoteToolKind::FsRead),
        ("palyra.fs.list_dir", WorkerRemoteToolKind::FsList),
        ("palyra.fs.search", WorkerRemoteToolKind::FsSearch),
        ("palyra.process.run", WorkerRemoteToolKind::ProcessRun),
        ("palyra.fs.apply_patch", WorkerRemoteToolKind::ApplyPatch),
        ("palyra.artifact.read", WorkerRemoteToolKind::ArtifactRead),
        ("palyra.tool_program.run", WorkerRemoteToolKind::ToolProgramRun),
    ];

    for (tool_name, expected) in cases {
        assert_eq!(WorkerRemoteToolKind::from_tool_name(tool_name), Some(expected));
        assert_eq!(expected.tool_name(), tool_name);
        assert_eq!(expected.required_capability(), format!("tool:{tool_name}"));
    }
    assert_eq!(WorkerRemoteToolKind::from_tool_name("palyra.http.fetch"), None);
}

#[test]
fn remote_request_validates_lease_and_manifest_contract() {
    let request = remote_request("palyra.fs.read_file");
    request.validate(2_000).expect("well-formed request should validate");

    let mut expired = request.clone();
    expired.lease.expires_at_unix_ms = 1_999;
    let error = expired.validate(2_000).expect_err("expired lease must fail closed");
    assert!(matches!(error, WorkerRemoteToolContractError::LeaseExpired { .. }));

    let mut missing_capability = request.clone();
    missing_capability.lease.required_capabilities.clear();
    let error =
        missing_capability.validate(2_000).expect_err("missing tool capability must fail closed");
    assert!(matches!(error, WorkerRemoteToolContractError::MissingRequiredCapability { .. }));
}

#[test]
fn remote_wire_schema_rejects_v1_and_missing_run_generation() {
    let request = remote_request("palyra.fs.read_file");
    assert_eq!(WORKER_REMOTE_TOOL_PROTOCOL, "palyra-worker-rpc/v2");
    let mut legacy_protocol_request = request.clone();
    legacy_protocol_request.protocol = "palyra-worker-rpc/v1".to_owned();
    assert_eq!(
        legacy_protocol_request
            .validate(2_000)
            .expect_err("protocol v1 requests must not bypass generation binding"),
        WorkerRemoteToolContractError::UnsupportedProtocol
    );

    let mut legacy_request = request.clone();
    legacy_request.schema_version = 1;
    assert_eq!(
        legacy_request
            .validate(2_000)
            .expect_err("schema v1 requests must not bypass generation binding"),
        WorkerRemoteToolContractError::UnsupportedProtocol
    );

    let mut request_json = serde_json::to_value(&request).expect("worker request should serialize");
    request_json["lease"]
        .as_object_mut()
        .expect("lease binding should be an object")
        .remove("run_generation");
    let request_error = serde_json::from_value::<WorkerRemoteToolRequestEnvelope>(request_json)
        .expect_err("requests without run generation must fail deserialization");
    assert!(request_error.to_string().contains("run_generation"));

    let result = remote_result(&request, r#"{"content":"ok"}"#);
    let mut legacy_result = result.clone();
    legacy_result.schema_version = 1;
    assert_eq!(
        legacy_result
            .validate_against_request(&request, 2_000)
            .expect_err("schema v1 results must not bypass generation binding"),
        WorkerRemoteToolContractError::UnsupportedProtocol
    );

    let mut result_json = serde_json::to_value(result).expect("worker result should serialize");
    result_json
        .as_object_mut()
        .expect("worker result should be an object")
        .remove("run_generation");
    let result_error = serde_json::from_value::<WorkerRemoteToolResultEnvelope>(result_json)
        .expect_err("results without run generation must fail deserialization");
    assert!(result_error.to_string().contains("run_generation"));
}

#[test]
fn remote_result_requires_cleanup_and_identity_stability() {
    let request = remote_request("palyra.process.run");
    let result = remote_result(&request, r#"{"schema_version":2,"exit_code":0}"#);
    result.validate_against_request(&request, 2_000).expect("matching result should validate");

    let mut cleanup_gap = result.clone();
    cleanup_gap.cleanup_report.removed_logs = false;
    let error = cleanup_gap
        .validate_against_request(&request, 2_000)
        .expect_err("cleanup gaps must fail closed");
    assert!(matches!(error, WorkerRemoteToolContractError::CleanupGap { .. }));

    let mut identity_mismatch = result;
    identity_mismatch.worker_identity.worker_id = "worker-remote-02".to_owned();
    let error = identity_mismatch
        .validate_against_request(&request, 2_000)
        .expect_err("identity drift must fail closed");
    assert!(matches!(error, WorkerRemoteToolContractError::WorkerIdentityMismatch { .. }));
}

#[test]
fn remote_process_lease_requires_exact_digest_bound_executable_authority() {
    let mut request = remote_request("palyra.process.run");
    request.canonical_protocol = Some(RemoteWorkerProtocolV1::from_remote_request(&request));
    request.validate(2_000).expect("exact process executable authority should validate");

    let mut missing = request.clone();
    missing.lease.process_executable_allowlist.clear();
    missing.canonical_protocol = Some(RemoteWorkerProtocolV1::from_remote_request(&missing));
    assert_eq!(
        missing.validate(2_000).expect_err("missing process authority must fail closed"),
        WorkerRemoteToolContractError::ProcessExecutableAuthorityInvalid
    );

    let mut wildcard = request.clone();
    wildcard.lease.process_executable_allowlist = vec!["*".to_owned()];
    wildcard.canonical_protocol = Some(RemoteWorkerProtocolV1::from_remote_request(&wildcard));
    assert_eq!(
        wildcard.validate(2_000).expect_err("wildcard process authority must fail closed"),
        WorkerRemoteToolContractError::ProcessExecutableAuthorityInvalid
    );

    let mut rotated = request.clone();
    rotated.lease.process_executable_allowlist = vec!["printf".to_owned()];
    rotated.canonical_protocol = Some(RemoteWorkerProtocolV1::from_remote_request(&rotated));
    assert_ne!(
        request.canonical_protocol.as_ref().expect("canonical request").task.policy_sha256,
        rotated.canonical_protocol.as_ref().expect("rotated canonical request").task.policy_sha256
    );
}

#[test]
fn remote_result_receipt_digest_is_generation_bound_and_deterministic() {
    let request = remote_request("palyra.process.run");
    let result = remote_result(&request, r#"{"schema_version":2,"exit_code":0}"#);

    let first = result
        .validated_receipt_sha256(&request, 2_000)
        .expect("matching result should produce a receipt digest");
    let replay = result
        .validated_receipt_sha256(&request, 2_000)
        .expect("matching replay should produce the same receipt digest");

    assert_eq!(first, replay);
    assert_eq!(first.len(), 64);
    assert!(first.bytes().all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)));

    let mut stale = result;
    stale.run_generation =
        RuntimeGeneration::new(request.lease.run_generation.get().saturating_add(1))
            .expect("next test generation should be valid");
    assert_eq!(
        stale
            .validated_receipt_sha256(&request, 2_000)
            .expect_err("stale generation must not produce settlement evidence"),
        WorkerRemoteToolContractError::ResultBindingMismatch
    );
}

#[test]
fn remote_result_rejects_post_expiry_host_observation_despite_worker_timestamp() {
    let request = remote_request("palyra.fs.read_file");
    let mut result = remote_result(&request, r#"{"content":"ok"}"#);
    result.completed_at_unix_ms = request.lease.expires_at_unix_ms.saturating_sub(1);

    let error = result
        .validate_against_request(&request, request.lease.expires_at_unix_ms)
        .expect_err("worker time must not authorize a result observed at lease expiry");

    assert!(matches!(error, WorkerRemoteToolContractError::LeaseExpired { .. }));
}

#[test]
fn remote_result_rejects_binding_identity_and_digest_mismatches() {
    let request = remote_request("palyra.fs.read_file");
    let result = remote_result(&request, r#"{"content":"ok"}"#);

    let mut binding_mismatch = result.clone();
    binding_mismatch.proposal_id = "proposal-other".to_owned();
    let error = binding_mismatch
        .validate_against_request(&request, 2_000)
        .expect_err("settlement must reject request binding drift");
    assert_eq!(error, WorkerRemoteToolContractError::ResultBindingMismatch);

    let mut stale_generation = result.clone();
    stale_generation.run_generation =
        RuntimeGeneration::new(request.lease.run_generation.get() + 1)
            .expect("next test generation should be valid");
    let error = stale_generation
        .validate_against_request(&request, 2_000)
        .expect_err("settlement must reject a stale run generation");
    assert_eq!(error, WorkerRemoteToolContractError::ResultBindingMismatch);

    let mut identity_mismatch = result.clone();
    identity_mismatch.worker_identity.build_digest_sha256 = hex_digest("f");
    let error = identity_mismatch
        .validate_against_request(&request, 2_000)
        .expect_err("settlement must reject worker identity drift");
    assert!(matches!(error, WorkerRemoteToolContractError::WorkerIdentityMismatch { .. }));

    let mut digest_mismatch = result;
    digest_mismatch.output_json.push(' ');
    let error = digest_mismatch
        .validate_against_request(&request, 2_000)
        .expect_err("settlement must verify the output payload digest");
    assert_eq!(
        error,
        WorkerRemoteToolContractError::DigestMismatch { field: "output_json_sha256" }
    );
}

#[test]
fn remote_result_rejects_invalid_request_cleanup_and_timestamp_metadata() {
    let request = remote_request("palyra.fs.apply_patch");
    let result = remote_result(&request, r#"{"applied":true}"#);

    let mut invalid_request = request.clone();
    invalid_request.input_json.push(' ');
    let error = result
        .validate_against_request(&invalid_request, 2_000)
        .expect_err("result validation must retain the original request digest binding");
    assert_eq!(error, WorkerRemoteToolContractError::DigestMismatch { field: "input_json_sha256" });

    let mut cleanup_gap = result.clone();
    cleanup_gap.cleanup_report.removed_artifacts = false;
    cleanup_gap.cleanup_report.failure_reason = Some("artifact cleanup failed".to_owned());
    let error = cleanup_gap
        .validate_against_request(&request, 2_000)
        .expect_err("result validation must require verified cleanup");
    assert!(matches!(error, WorkerRemoteToolContractError::CleanupGap { .. }));

    let mut invalid_timestamp = result;
    invalid_timestamp.completed_at_unix_ms = -1;
    let error = invalid_timestamp
        .validate_against_request(&request, 2_000)
        .expect_err("negative worker timestamp metadata must fail closed");
    assert_eq!(error, WorkerRemoteToolContractError::InvalidCompletionTimestamp);
}

#[test]
fn worker_lifecycle_supports_successful_handshake_assignment_and_cleanup() {
    let mut manager = WorkerFleetManager::default();
    let policy = WorkerFleetPolicy::default();

    let register = manager
        .register_worker(attestation("worker-a"), &policy, 2_000)
        .expect("worker should register");
    assert_eq!(register.reason_code, "worker.registered");

    let (lease, assign) = manager
        .assign_work("worker-a", lease_request("run-1", 500), &policy, 2_500)
        .expect("worker should accept a lease");
    assert_eq!(lease.run_id, "run-1");
    assert_eq!(assign.state, WorkerLifecycleState::Assigned);

    let complete = manager
        .complete_work(
            "worker-a",
            &lease.identity(),
            &WorkerCleanupReport {
                removed_workspace_scope: true,
                removed_artifacts: true,
                removed_logs: true,
                failure_reason: None,
            },
            lease.expires_at_unix_ms.saturating_sub(1),
        )
        .expect("cleanup should succeed");
    assert_eq!(complete.state, WorkerLifecycleState::Completed);
    assert_eq!(manager.snapshot().active_leases, 0);
}

#[test]
fn worker_lease_cannot_outlive_its_run_grant() {
    let mut manager = WorkerFleetManager::default();
    let policy = WorkerFleetPolicy::default();
    manager.register_worker(attestation("worker-grant-bound"), &policy, 2_000).unwrap();
    let mut request = lease_request("run-grant-bound", 501);
    request.grant.expires_at_unix_ms = 3_000;

    let error = manager
        .assign_work("worker-grant-bound", request, &policy, 2_500)
        .expect_err("a lease extending past its grant must fail closed");

    assert_eq!(
        error,
        WorkerLifecycleError::InvalidLeaseRequest("lease ttl exceeds grant lifetime".to_owned())
    );
    assert_eq!(manager.snapshot().active_leases, 0);
}

#[test]
fn worker_fault_probe_is_disabled_by_default() {
    let manager = WorkerFleetManager::default();
    assert!(manager.qa_fault_probe.records().unwrap().is_empty());
}

#[cfg(feature = "qa-fault-injection")]
#[test]
fn seeded_claim_barrier_reproduces_one_winner_without_double_assignment() {
    fn run_claim_race(seed: u64) -> String {
        let activation = QaFaultActivation {
            id: "worker-claim-race".to_owned(),
            point_id: "worker.claim.before_effect".to_owned(),
            actor: None,
            occurrence: 1,
            action: QaFaultAction::Barrier { participants: 2 },
        };
        let controller = DeterministicQaFaultController::new(QaFaultInjectionPlan {
            schema_version: QA_FAULT_INJECTION_PLAN_SCHEMA_VERSION,
            format: QA_FAULT_INJECTION_PLAN_FORMAT.to_owned(),
            seed,
            activations: vec![activation.clone()],
        })
        .unwrap();
        let scheduler = controller.scheduler();
        let probe = QaFaultProbeHandle::from_probe(controller);
        let mut manager = WorkerFleetManager::default().with_qa_fault_probe(probe.clone());
        let policy = WorkerFleetPolicy::default();
        manager.register_worker(attestation("worker-race"), &policy, 2_000).unwrap();

        for run_id in ["claim-a", "claim-b"] {
            let error = manager
                .assign_next_work(lease_request(run_id, 500), &policy, 2_100)
                .expect_err("both actors must stop at the barrier before assignment");
            assert!(matches!(
                error,
                WorkerLifecycleError::QaFaultActivated {
                    action: QaFaultAction::Barrier { participants: 2 },
                    ..
                }
            ));
        }
        assert_eq!(manager.snapshot().active_leases, 0);

        let release_order = scheduler
            .release_order(&activation, &["claim-a".to_owned(), "claim-b".to_owned()])
            .unwrap();
        let mut winner = None;
        let mut successful_assignments = 0usize;
        for run_id in release_order {
            match manager.assign_next_work(lease_request(run_id.as_str(), 500), &policy, 2_101) {
                Ok((lease, _)) => {
                    successful_assignments = successful_assignments.saturating_add(1);
                    winner = Some(lease.run_id);
                }
                Err(WorkerLifecycleError::NoAvailableWorker) => {}
                other => panic!("unexpected claim race result: {other:?}"),
            }
        }
        assert_eq!(successful_assignments, 1, "the fresh claim must have exactly one winner");
        manager
            .record_qa_fault_recovery("worker-claim-race", QaFaultRecoveryClass::RetrySucceeded)
            .unwrap();
        let records = probe.records().unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].actors, vec!["claim-a", "claim-b"]);
        assert_eq!(records[0].recovery_class, Some(QaFaultRecoveryClass::RetrySucceeded));
        assert_eq!(manager.snapshot().active_leases, 1);
        winner.expect("one seeded actor must win the claim")
    }

    assert_eq!(run_claim_race(41), run_claim_race(41));
}

#[cfg(feature = "qa-fault-injection")]
#[test]
fn heartbeat_timeout_adapter_is_fail_loud_and_records_recovery() {
    let controller = DeterministicQaFaultController::new(QaFaultInjectionPlan {
        schema_version: QA_FAULT_INJECTION_PLAN_SCHEMA_VERSION,
        format: QA_FAULT_INJECTION_PLAN_FORMAT.to_owned(),
        seed: 17,
        activations: vec![QaFaultActivation {
            id: "worker-heartbeat-timeout".to_owned(),
            point_id: "worker.heartbeat.before_effect".to_owned(),
            actor: Some("worker-heartbeat".to_owned()),
            occurrence: 1,
            action: QaFaultAction::Timeout,
        }],
    })
    .unwrap();
    let probe = QaFaultProbeHandle::from_probe(controller);
    let mut manager = WorkerFleetManager::default().with_qa_fault_probe(probe.clone());
    let policy = WorkerFleetPolicy::default();
    manager.register_worker(attestation("worker-heartbeat"), &policy, 2_000).unwrap();

    let error = manager
        .heartbeat_worker("worker-heartbeat", &policy, 2_100)
        .expect_err("injected heartbeat timeout must be surfaced");
    assert!(matches!(
        error,
        WorkerLifecycleError::QaFaultActivated { action: QaFaultAction::Timeout, .. }
    ));
    assert_eq!(manager.recent_events().len(), 1);

    let event = manager.heartbeat_worker("worker-heartbeat", &policy, 2_101).unwrap();
    assert_eq!(event.timestamp_unix_ms, 2_101);
    manager
        .record_qa_fault_recovery("worker-heartbeat-timeout", QaFaultRecoveryClass::RetrySucceeded)
        .unwrap();
    assert_eq!(
        probe.records().unwrap()[0].recovery_class,
        Some(QaFaultRecoveryClass::RetrySucceeded)
    );
}

#[cfg(feature = "qa-fault-injection")]
#[test]
fn stale_reclaim_adapter_advances_eligibility_without_changing_expiry_evidence() {
    let controller = DeterministicQaFaultController::new(QaFaultInjectionPlan {
        schema_version: QA_FAULT_INJECTION_PLAN_SCHEMA_VERSION,
        format: QA_FAULT_INJECTION_PLAN_FORMAT.to_owned(),
        seed: 23,
        activations: vec![QaFaultActivation {
            id: "worker-stale-clock".to_owned(),
            point_id: "worker.stale_reclaim.before_effect".to_owned(),
            actor: Some("worker-clock".to_owned()),
            occurrence: 1,
            action: QaFaultAction::AdvanceLogicalTime { milliseconds: 101 },
        }],
    })
    .unwrap();
    let probe = QaFaultProbeHandle::from_probe(controller);
    let mut manager = WorkerFleetManager::default().with_qa_fault_probe(probe.clone());
    let policy = WorkerFleetPolicy::default();
    manager.register_worker(attestation("worker-clock"), &policy, 2_000).unwrap();
    let (lease, _) = manager
        .assign_work("worker-clock", lease_request("clock-run", 100), &policy, 2_100)
        .unwrap();

    let events = manager.reap_expired_workers(2_100).unwrap();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].worker_id, "worker-clock");
    assert_eq!(events[0].run_id.as_deref(), Some("clock-run"));
    assert_eq!(events[0].timestamp_unix_ms, lease.expires_at_unix_ms);
    assert_eq!(events[0].state, WorkerLifecycleState::Orphaned);
    let records = probe.records().unwrap();
    assert_eq!(records[0].actors, ["worker-clock"]);
    assert_eq!(records[0].recovery_class, Some(QaFaultRecoveryClass::Reclaimed));
}

#[cfg(feature = "qa-fault-injection")]
#[test]
fn stale_reclaim_barrier_collects_bounded_participants_before_mutation() {
    let controller = DeterministicQaFaultController::new(QaFaultInjectionPlan {
        schema_version: QA_FAULT_INJECTION_PLAN_SCHEMA_VERSION,
        format: QA_FAULT_INJECTION_PLAN_FORMAT.to_owned(),
        seed: 29,
        activations: vec![QaFaultActivation {
            id: "worker-stale-barrier".to_owned(),
            point_id: "worker.stale_reclaim.batch_before_effect".to_owned(),
            actor: None,
            occurrence: 1,
            action: QaFaultAction::Barrier { participants: 2 },
        }],
    })
    .unwrap();
    let probe = QaFaultProbeHandle::from_probe(controller);
    let mut manager = WorkerFleetManager::default().with_qa_fault_probe(probe.clone());
    let policy = WorkerFleetPolicy::default();
    for (worker_id, run_id) in [
        ("worker-barrier-a", "barrier-run-a"),
        ("worker-barrier-b", "barrier-run-b"),
        ("worker-barrier-c", "barrier-run-c"),
    ] {
        manager.register_worker(attestation(worker_id), &policy, 2_000).unwrap();
        manager.assign_work(worker_id, lease_request(run_id, 100), &policy, 2_100).unwrap();
    }

    let error = manager
        .reap_expired_workers(2_201)
        .expect_err("the first scan must join the barrier without reclaiming workers");
    assert!(matches!(
        error,
        WorkerLifecycleError::QaFaultActivated {
            action: QaFaultAction::Barrier { participants: 2 },
            ..
        }
    ));
    assert_eq!(manager.snapshot().active_leases, 3);

    let events = manager
        .reap_expired_workers(2_201)
        .expect("the retry must consume all releases before reclaiming the stale fleet");
    assert_eq!(events.len(), 3);
    assert_eq!(manager.snapshot().active_leases, 0);
    let records = probe.records().unwrap();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].actors, ["worker-barrier-a", "worker-barrier-b"]);
    assert_eq!(records[0].recovery_class, Some(QaFaultRecoveryClass::Reclaimed));
}

#[cfg(feature = "qa-fault-injection")]
#[test]
fn stale_reclaim_barrier_completes_partial_join_when_a_new_candidate_ages_in() {
    let controller = DeterministicQaFaultController::new(QaFaultInjectionPlan {
        schema_version: QA_FAULT_INJECTION_PLAN_SCHEMA_VERSION,
        format: QA_FAULT_INJECTION_PLAN_FORMAT.to_owned(),
        seed: 31,
        activations: vec![QaFaultActivation {
            id: "worker-stale-partial-barrier".to_owned(),
            point_id: "worker.stale_reclaim.batch_before_effect".to_owned(),
            actor: None,
            occurrence: 1,
            action: QaFaultAction::Barrier { participants: 3 },
        }],
    })
    .unwrap();
    let probe = QaFaultProbeHandle::from_probe(controller);
    let mut manager = WorkerFleetManager::default().with_qa_fault_probe(probe.clone());
    let policy = WorkerFleetPolicy::default();
    for (worker_id, run_id, ttl_ms) in [
        ("worker-partial-a", "partial-run-a", 100),
        ("worker-partial-b", "partial-run-b", 100),
        ("worker-partial-c", "partial-run-c", 200),
    ] {
        manager.register_worker(attestation(worker_id), &policy, 2_000).unwrap();
        manager.assign_work(worker_id, lease_request(run_id, ttl_ms), &policy, 2_100).unwrap();
    }

    let first = manager
        .reap_expired_workers(2_201)
        .expect_err("two stale workers must form a partial three-actor barrier");
    assert!(matches!(
        first,
        WorkerLifecycleError::QaFaultActivated {
            action: QaFaultAction::Barrier { participants: 3 },
            ..
        }
    ));
    assert_eq!(manager.snapshot().active_leases, 3);

    let second = manager
        .reap_expired_workers(2_301)
        .expect_err("the newly stale worker must complete the durable join set first");
    assert!(matches!(
        second,
        WorkerLifecycleError::QaFaultActivated {
            action: QaFaultAction::Barrier { participants: 3 },
            ..
        }
    ));
    assert_eq!(manager.snapshot().active_leases, 3);

    let events = manager
        .reap_expired_workers(2_301)
        .expect("the complete barrier retry must reclaim all three workers");
    assert_eq!(events.len(), 3);
    let records = probe.records().unwrap();
    assert_eq!(records[0].actors.len(), 3);
    assert_eq!(records[0].recovery_class, Some(QaFaultRecoveryClass::Reclaimed));
}

#[cfg(feature = "qa-fault-injection")]
#[test]
fn stale_reclaim_does_not_pre_attest_before_exact_lease_revocation() {
    let attempts = Arc::new(Mutex::new(Vec::new()));
    let probe = QaFaultProbeHandle::from_probe(RecoveryRejectingProbe {
        activation: QaFaultActivation {
            id: "worker-stale-recovery".to_owned(),
            point_id: "worker.stale_reclaim.before_effect".to_owned(),
            actor: Some("worker-stale-target".to_owned()),
            occurrence: 1,
            action: QaFaultAction::AdvanceLogicalTime { milliseconds: 101 },
        },
        attempts: Arc::clone(&attempts),
    });
    let mut manager = WorkerFleetManager::default().with_qa_fault_probe(probe);
    let policy = WorkerFleetPolicy::default();
    manager.register_worker(attestation("worker-stale-target"), &policy, 2_000).unwrap();
    manager.register_worker(attestation("worker-stale-control"), &policy, 2_000).unwrap();
    let (target_lease, _) = manager
        .assign_work("worker-stale-target", lease_request("target-run", 100), &policy, 2_100)
        .unwrap();
    let (control_lease, _) = manager
        .assign_work("worker-stale-control", lease_request("control-run", 100), &policy, 2_100)
        .unwrap();
    let event_count_before_reclaim = manager.recent_events().len();

    let error = manager
        .reap_expired_workers(2_100)
        .expect_err("recovery evidence rejection must fail the reclaim operation");

    assert!(matches!(
        error,
        WorkerLifecycleError::QaFaultProbe(QaFaultProbeError::AdapterFailure(
            "test recovery evidence rejection"
        ))
    ));
    assert_eq!(
        *attempts.lock().expect("test recovery attempt lock must remain usable"),
        [("worker-stale-recovery".to_owned(), QaFaultRecoveryClass::Reclaimed)]
    );
    let target = manager.workers.get("worker-stale-target").unwrap();
    assert_eq!(target.state, WorkerLifecycleState::Orphaned);
    assert!(target.lease.is_none());
    let control = manager.workers.get("worker-stale-control").unwrap();
    assert_eq!(control.state, WorkerLifecycleState::Assigned);
    assert_eq!(
        control.lease.as_ref().map(|lease| lease.identity()),
        Some(control_lease.identity())
    );
    assert_eq!(manager.recent_events().len(), event_count_before_reclaim + 1);
    assert_eq!(manager.recent_events()[0].worker_id, "worker-stale-target");
    assert_eq!(manager.recent_events()[0].run_id.as_deref(), Some(target_lease.run_id.as_str()));
    assert_eq!(manager.recent_events()[0].state, WorkerLifecycleState::Orphaned);
}

#[cfg(feature = "qa-fault-injection")]
#[test]
fn stale_heartbeat_does_not_pre_attest_before_exact_state_transition() {
    let attempts = Arc::new(Mutex::new(Vec::new()));
    let probe = QaFaultProbeHandle::from_probe(RecoveryRejectingProbe {
        activation: QaFaultActivation {
            id: "worker-heartbeat-recovery".to_owned(),
            point_id: "worker.stale_reclaim.before_effect".to_owned(),
            actor: Some("worker-heartbeat-target".to_owned()),
            occurrence: 1,
            action: QaFaultAction::AdvanceLogicalTime { milliseconds: 101 },
        },
        attempts: Arc::clone(&attempts),
    });
    let mut manager = WorkerFleetManager::default().with_qa_fault_probe(probe);
    let policy = WorkerFleetPolicy { heartbeat_timeout_ms: 100, ..Default::default() };
    manager.register_worker(attestation("worker-heartbeat-target"), &policy, 2_000).unwrap();
    manager.register_worker(attestation("worker-heartbeat-control"), &policy, 2_000).unwrap();
    let (target_lease, _) = manager
        .assign_work(
            "worker-heartbeat-target",
            lease_request("heartbeat-target-run", 500),
            &policy,
            2_000,
        )
        .unwrap();
    let (control_lease, _) = manager
        .assign_work(
            "worker-heartbeat-control",
            lease_request("heartbeat-control-run", 500),
            &policy,
            2_000,
        )
        .unwrap();
    let event_count_before_reclaim = manager.recent_events().len();

    let error = manager
        .mark_stale_heartbeat_workers(&policy, 2_000)
        .expect_err("recovery evidence rejection must fail the stale-heartbeat operation");

    assert!(matches!(
        error,
        WorkerLifecycleError::QaFaultProbe(QaFaultProbeError::AdapterFailure(
            "test recovery evidence rejection"
        ))
    ));
    assert_eq!(
        *attempts.lock().expect("test recovery attempt lock must remain usable"),
        [("worker-heartbeat-recovery".to_owned(), QaFaultRecoveryClass::Reclaimed)]
    );
    let target = manager.workers.get("worker-heartbeat-target").unwrap();
    assert_eq!(target.state, WorkerLifecycleState::Orphaned);
    assert!(target.lease.is_none());
    let control = manager.workers.get("worker-heartbeat-control").unwrap();
    assert_eq!(control.state, WorkerLifecycleState::Assigned);
    assert_eq!(
        control.lease.as_ref().map(|lease| lease.identity()),
        Some(control_lease.identity())
    );
    assert_eq!(manager.recent_events().len(), event_count_before_reclaim + 1);
    assert_eq!(manager.recent_events()[0].worker_id, "worker-heartbeat-target");
    assert_eq!(manager.recent_events()[0].run_id.as_deref(), Some(target_lease.run_id.as_str()));
    assert_eq!(manager.recent_events()[0].state, WorkerLifecycleState::Orphaned);
}

#[test]
fn completion_at_or_after_lease_expiry_cannot_clear_active_authority() {
    let policy = WorkerFleetPolicy::default();
    let cleanup = WorkerCleanupReport {
        removed_workspace_scope: true,
        removed_artifacts: true,
        removed_logs: true,
        failure_reason: None,
    };

    for (worker_id, completion_offset_ms) in
        [("worker-expiry-boundary", 0), ("worker-expiry-late", 1)]
    {
        let mut manager = WorkerFleetManager::default();
        manager.register_worker(attestation(worker_id), &policy, 2_000).unwrap();
        let (lease, _) = manager
            .assign_work(worker_id, lease_request("run-expiry-boundary", 500), &policy, 2_100)
            .unwrap();
        let completion_time = lease.expires_at_unix_ms.saturating_add(completion_offset_ms);

        let error = manager
            .finalize_work(worker_id, &lease.identity(), cleanup.clone(), completion_time)
            .expect_err("completion at or beyond the exact deadline must fail closed");

        assert!(matches!(error, WorkerLifecycleError::StaleLeaseCompletion { .. }));
        assert_eq!(manager.snapshot().active_leases, 1);
        assert_eq!(manager.snapshot().orphaned_workers, 0);
        assert_eq!(manager.recent_events()[0].state, WorkerLifecycleState::Assigned);
    }
}

#[test]
fn stale_completion_cannot_clear_a_newer_worker_lease() {
    let mut manager = WorkerFleetManager::default();
    let policy = WorkerFleetPolicy::default();
    manager.register_worker(attestation("worker-stale"), &policy, 2_000).unwrap();
    let (first_lease, _) =
        manager.assign_work("worker-stale", lease_request("run-old", 500), &policy, 2_100).unwrap();
    let cleanup = WorkerCleanupReport {
        removed_workspace_scope: true,
        removed_artifacts: true,
        removed_logs: true,
        failure_reason: None,
    };
    manager.finalize_work("worker-stale", &first_lease.identity(), cleanup.clone(), 2_200).unwrap();
    let (new_lease, _) =
        manager.assign_work("worker-stale", lease_request("run-new", 500), &policy, 2_300).unwrap();

    let stale_run_identity = WorkerLeaseIdentity {
        lease_id: new_lease.lease_id.clone(),
        run_id: first_lease.run_id.clone(),
    };
    let error = manager
        .finalize_work("worker-stale", &stale_run_identity, cleanup.clone(), 2_400)
        .expect_err("a stale run id must not finalize the active lease");
    assert!(matches!(error, WorkerLifecycleError::StaleLeaseCompletion { .. }));

    let error = manager
        .finalize_work("worker-stale", &first_lease.identity(), cleanup, 2_500)
        .expect_err("an older lease must not finalize a newer assignment");
    assert!(matches!(error, WorkerLifecycleError::StaleLeaseCompletion { .. }));
    assert_eq!(manager.snapshot().active_leases, 1);
    assert_eq!(manager.recent_events()[0].run_id.as_deref(), Some("run-new"));
    assert_eq!(manager.recent_events()[0].state, WorkerLifecycleState::Assigned);
}

#[test]
fn worker_registration_rejects_missing_egress_proxy_attestation() {
    let mut manager = WorkerFleetManager::default();
    let policy = WorkerFleetPolicy::default();
    let mut worker_attestation = attestation("worker-b");
    worker_attestation.egress_proxy_attested = false;

    let error = manager
        .register_worker(worker_attestation, &policy, 2_000)
        .expect_err("egress proxy binding should be required");
    assert!(matches!(
        error,
        WorkerLifecycleError::Attestation(super::WorkerAttestationError::MissingEgressProxyBinding)
    ));
}

#[test]
fn worker_cleanup_failure_stays_fail_closed() {
    let mut manager = WorkerFleetManager::default();
    let policy = WorkerFleetPolicy::default();
    manager.register_worker(attestation("worker-c"), &policy, 2_000).unwrap();
    let (lease, _) =
        manager.assign_work("worker-c", lease_request("run-2", 500), &policy, 2_500).unwrap();

    let error = manager
        .complete_work(
            "worker-c",
            &lease.identity(),
            &WorkerCleanupReport {
                removed_workspace_scope: false,
                removed_artifacts: true,
                removed_logs: true,
                failure_reason: Some("artifact cleanup failure".to_owned()),
            },
            lease.expires_at_unix_ms.saturating_sub(1),
        )
        .expect_err("cleanup failure should not be ignored");
    assert_eq!(error, WorkerLifecycleError::CleanupFailed);
}

#[test]
fn worker_ttl_reap_marks_orphaned_instances() {
    let mut manager = WorkerFleetManager::default();
    let policy = WorkerFleetPolicy::default();
    manager.register_worker(attestation("worker-d"), &policy, 2_000).unwrap();
    let (lease, _) =
        manager.assign_work("worker-d", lease_request("run-3", 250), &policy, 2_500).unwrap();

    let events = manager.reap_expired_workers(2_751).unwrap();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].state, WorkerLifecycleState::Orphaned);
    assert_eq!(events[0].run_id.as_deref(), Some(lease.run_id.as_str()));
    assert_eq!(events[0].lease_id.as_deref(), Some(lease.lease_id.as_str()));
    assert_eq!(manager.snapshot().orphaned_workers, 1);
}

#[test]
fn bounded_worker_ttl_reap_preserves_remaining_leases() {
    let mut manager = WorkerFleetManager::default();
    let policy = WorkerFleetPolicy::default();
    for index in 0..3 {
        let worker_id = format!("worker-bounded-{index}");
        manager.register_worker(attestation(worker_id.as_str()), &policy, 2_000).unwrap();
        manager
            .assign_work(
                worker_id.as_str(),
                lease_request(format!("run-bounded-{index}").as_str(), 250),
                &policy,
                2_500,
            )
            .unwrap();
    }

    let first = manager.reap_expired_workers_bounded(2_751, 2).unwrap();
    assert_eq!(first.len(), 2);
    assert_eq!(manager.snapshot().active_leases, 1);
    assert_eq!(manager.snapshot().orphaned_workers, 2);

    let second = manager.reap_expired_workers_bounded(2_751, 2).unwrap();
    assert_eq!(second.len(), 1);
    assert_eq!(manager.snapshot().active_leases, 0);
    assert_eq!(manager.snapshot().orphaned_workers, 3);
}

#[test]
fn worker_expiry_plan_rejects_superseded_lease_before_mutation() {
    let mut manager = WorkerFleetManager::default();
    let policy = WorkerFleetPolicy::default();
    manager.register_worker(attestation("worker-plan"), &policy, 2_000).unwrap();
    let (lease, _) =
        manager.assign_work("worker-plan", lease_request("run-plan", 250), &policy, 2_500).unwrap();
    let plan = manager.plan_expired_workers_bounded(2_751, 1).unwrap();
    assert_eq!(plan.events()[0].lease_id.as_deref(), Some(lease.lease_id.as_str()));

    manager
        .finalize_work(
            "worker-plan",
            &lease.identity(),
            WorkerCleanupReport {
                removed_workspace_scope: true,
                removed_artifacts: true,
                removed_logs: true,
                failure_reason: None,
            },
            lease.expires_at_unix_ms.saturating_sub(1),
        )
        .unwrap();
    let error = manager
        .apply_expired_worker_plan(plan)
        .expect_err("a superseded exact lease plan must fail before mutation");
    assert!(matches!(error, WorkerLifecycleError::ExpiryPlanConflict { .. }));
    assert_eq!(manager.snapshot().orphaned_workers, 0);
}

#[test]
fn worker_auto_assignment_matches_required_capabilities() {
    let mut manager = WorkerFleetManager::default();
    let policy = WorkerFleetPolicy::default();
    manager.register_worker(attestation("worker-e"), &policy, 2_000).unwrap();

    let mut request = lease_request("run-4", 500);
    request.required_capabilities = vec!["tool:palyra.echo".to_owned()];
    let (lease, event) = manager
        .assign_next_work(request, &policy, 2_500)
        .expect("matching worker should accept the lease");

    assert_eq!(lease.worker_id, "worker-e");
    assert_eq!(lease.required_capabilities, vec!["tool:palyra.echo"]);
    assert_eq!(event.state, WorkerLifecycleState::Assigned);
    assert_eq!(manager.recent_events().len(), 2);
}

#[test]
fn worker_filtered_assignment_skips_unapproved_candidates_without_mutating_on_empty_set() {
    let mut manager = WorkerFleetManager::default();
    let policy = WorkerFleetPolicy::default();
    manager.register_worker(attestation("worker-filtered-a"), &policy, 2_000).unwrap();
    manager.register_worker(attestation("worker-filtered-b"), &policy, 2_000).unwrap();
    let baseline = manager.snapshot();
    let baseline_events = manager.recent_events();

    let error = manager
        .assign_next_work_from_candidates(
            &std::collections::BTreeSet::new(),
            lease_request("run-filtered-empty", 500),
            &policy,
            2_500,
        )
        .expect_err("an empty compatibility set must not assign a worker");
    assert_eq!(error, WorkerLifecycleError::NoAvailableWorker);
    assert_eq!(manager.snapshot(), baseline);
    assert_eq!(manager.recent_events(), baseline_events);

    let candidates = std::collections::BTreeSet::from(["worker-filtered-b".to_owned()]);
    let (lease, event) = manager
        .assign_next_work_from_candidates(
            &candidates,
            lease_request("run-filtered-selected", 500),
            &policy,
            2_500,
        )
        .expect("the first fleet-eligible approved worker should receive the lease");
    assert_eq!(lease.worker_id, "worker-filtered-b");
    assert_eq!(event.worker_id, "worker-filtered-b");
    assert_eq!(manager.snapshot().active_leases, 1);
}

#[test]
fn worker_auto_assignment_rejects_missing_capability() {
    let mut manager = WorkerFleetManager::default();
    let policy = WorkerFleetPolicy::default();
    manager.register_worker(attestation("worker-f"), &policy, 2_000).unwrap();

    let mut request = lease_request("run-5", 500);
    request.required_capabilities = vec!["tool:palyra.sleep".to_owned()];
    let error = manager
        .assign_next_work(request, &policy, 2_500)
        .expect_err("missing worker capability should fail closed");

    assert_eq!(error, WorkerLifecycleError::NoAvailableWorker);
    assert_eq!(manager.snapshot().active_leases, 0);
}

#[test]
fn worker_cleanup_failure_records_failed_event_for_journal_surfaces() {
    let mut manager = WorkerFleetManager::default();
    let policy = WorkerFleetPolicy::default();
    manager.register_worker(attestation("worker-g"), &policy, 2_000).unwrap();
    let (lease, _) =
        manager.assign_work("worker-g", lease_request("run-6", 500), &policy, 2_500).unwrap();

    let outcome = manager
        .finalize_work(
            "worker-g",
            &lease.identity(),
            WorkerCleanupReport {
                removed_workspace_scope: true,
                removed_artifacts: false,
                removed_logs: true,
                failure_reason: Some("artifact cleanup failure".to_owned()),
            },
            lease.expires_at_unix_ms.saturating_sub(1),
        )
        .expect("cleanup outcome should be returned for journal emission");

    assert!(!outcome.cleanup_succeeded);
    assert_eq!(outcome.event.state, WorkerLifecycleState::Failed);
    assert_eq!(outcome.event.reason_code, "worker.cleanup_failed");
    assert_eq!(manager.snapshot().failed_closed_workers, 1);
    let error = manager
        .assign_work("worker-g", lease_request("run-7", 500), &policy, 3_100)
        .expect_err("failed worker must stay fail closed");
    assert!(matches!(error, WorkerLifecycleError::WorkerFailClosed(_)));
}

#[test]
fn operator_quarantine_and_drain_fail_closed() {
    let mut manager = WorkerFleetManager::default();
    let policy = WorkerFleetPolicy::default();
    manager.register_worker(attestation("worker-h"), &policy, 2_000).unwrap();
    manager.assign_work("worker-h", lease_request("run-8", 500), &policy, 2_500).unwrap();

    let quarantine = manager
        .quarantine_worker("worker-h", "worker.operator.quarantine", 2_750)
        .expect("operator quarantine should be recorded");
    assert_eq!(quarantine.state, WorkerLifecycleState::Failed);
    assert_eq!(quarantine.run_id.as_deref(), Some("run-8"));
    assert_eq!(manager.snapshot().failed_closed_workers, 1);

    manager.register_worker(attestation("worker-i"), &policy, 2_800).unwrap();
    let drain = manager.quarantine_all_workers("worker.operator.drain", 3_000);
    assert_eq!(drain.len(), 1);
    assert_eq!(drain[0].reason_code, "worker.operator.drain");
    assert_eq!(manager.snapshot().failed_closed_workers, 2);
}

#[test]
fn operator_reverify_rejects_fail_closed_workers_and_active_leases() {
    let mut manager = WorkerFleetManager::default();
    let policy = WorkerFleetPolicy::default();
    manager.register_worker(attestation("worker-j"), &policy, 2_000).unwrap();
    manager.assign_work("worker-j", lease_request("run-9", 500), &policy, 2_100).unwrap();

    let active_lease_error = manager
        .reverify_worker("worker-j", &policy, 2_200)
        .expect_err("active lease must not be reverified in place");
    assert!(matches!(active_lease_error, WorkerLifecycleError::LeaseAlreadyActive(_)));

    manager.quarantine_worker("worker-j", "worker.operator.quarantine", 2_300).unwrap();

    let error = manager
        .reverify_worker("worker-j", &policy, 2_400)
        .expect_err("fail-closed worker must not be reverified without fresh registration");
    assert!(matches!(error, WorkerLifecycleError::WorkerFailClosed(_)));

    let error = manager
        .assign_work("worker-j", lease_request("run-9b", 500), &policy, 2_500)
        .expect_err("failed worker must stay unassignable");
    assert!(matches!(error, WorkerLifecycleError::WorkerFailClosed(_)));
}

#[test]
fn force_cleanup_promotes_only_verified_cleanup_reports() {
    let mut manager = WorkerFleetManager::default();
    let policy = WorkerFleetPolicy::default();
    manager.register_worker(attestation("worker-k"), &policy, 2_000).unwrap();
    manager.assign_work("worker-k", lease_request("run-10", 500), &policy, 2_500).unwrap();
    let active_error = manager
        .force_cleanup_worker(
            "worker-k",
            WorkerCleanupReport {
                removed_workspace_scope: true,
                removed_artifacts: true,
                removed_logs: true,
                failure_reason: None,
            },
            2_600,
        )
        .expect_err("operator cleanup must not clear active lease authority");
    assert_eq!(
        active_error,
        WorkerLifecycleError::CleanupRequiresLeaseRevocation("worker-k".to_owned())
    );
    manager
        .quarantine_worker("worker-k", "worker.operator.quarantine", 2_650)
        .expect("quarantine should revoke the active lease");

    let failed = manager
        .force_cleanup_worker(
            "worker-k",
            WorkerCleanupReport {
                removed_workspace_scope: true,
                removed_artifacts: false,
                removed_logs: true,
                failure_reason: Some("operator could not remove artifact".to_owned()),
            },
            2_700,
        )
        .expect("cleanup report should be recorded");
    assert!(!failed.cleanup_succeeded);
    assert_eq!(failed.event.state, WorkerLifecycleState::Failed);

    let recovered = manager
        .force_cleanup_worker(
            "worker-k",
            WorkerCleanupReport {
                removed_workspace_scope: true,
                removed_artifacts: true,
                removed_logs: true,
                failure_reason: None,
            },
            2_900,
        )
        .expect("verified cleanup should be recorded");
    assert!(recovered.cleanup_succeeded);
    assert_eq!(recovered.event.state, WorkerLifecycleState::Failed);
    assert_eq!(recovered.event.reason_code, "worker.cleanup_verified_requires_reattestation");
    assert_eq!(manager.snapshot().failed_closed_workers, 1);
    let error = manager
        .assign_work("worker-k", lease_request("run-10b", 500), &policy, 3_000)
        .expect_err("cleanup verification alone must not make failed workers assignable");
    assert!(matches!(error, WorkerLifecycleError::WorkerFailClosed(_)));
}

#[test]
fn capability_matching_requires_worker_self_report_and_policy_trust() {
    let mut manager = WorkerFleetManager::default();
    let policy = policy_for("tool:palyra.sleep");
    let mut attestation = attestation("worker-l");
    attestation.supported_capabilities = vec!["tool:palyra.sleep".to_owned()];
    manager.register_worker(attestation, &policy, 2_000).unwrap();

    let mut request = lease_request("run-11", 500);
    request.required_capabilities = vec!["tool:palyra.echo".to_owned()];
    let error = manager
        .assign_next_work(request, &policy, 2_500)
        .expect_err("untrusted capability must fail closed even if another tool is trusted");

    assert_eq!(error, WorkerLifecycleError::NoAvailableWorker);
    assert_eq!(manager.snapshot().active_leases, 0);
}

#[test]
fn stale_heartbeat_with_active_lease_orphans_worker_until_remediation() {
    let mut manager = WorkerFleetManager::default();
    let policy = WorkerFleetPolicy { heartbeat_timeout_ms: 100, ..policy_for("tool:palyra.echo") };
    manager.register_worker(attestation("worker-m"), &policy, 2_000).unwrap();
    manager.assign_work("worker-m", lease_request("run-12", 500), &policy, 2_050).unwrap();

    let events = manager.mark_stale_heartbeat_workers(&policy, 2_250).unwrap();

    assert_eq!(events.len(), 1);
    assert_eq!(events[0].state, WorkerLifecycleState::Orphaned);
    assert_eq!(events[0].run_id.as_deref(), Some("run-12"));
    assert_eq!(manager.snapshot().orphaned_workers, 1);
    assert_eq!(manager.snapshot().active_leases, 0);

    let heartbeat = manager
        .heartbeat_worker("worker-m", &policy, 2_260)
        .expect("orphaned worker heartbeat should be recorded without automatic reuse");
    assert_eq!(heartbeat.state, WorkerLifecycleState::Orphaned);
    let error = manager
        .assign_work("worker-m", lease_request("run-12b", 500), &policy, 2_270)
        .expect_err("orphaned stale worker must not receive work before remediation");
    assert!(matches!(error, WorkerLifecycleError::WorkerFailClosed(_)));

    let cleanup = manager
        .force_cleanup_worker(
            "worker-m",
            WorkerCleanupReport {
                removed_workspace_scope: true,
                removed_artifacts: true,
                removed_logs: true,
                failure_reason: None,
            },
            2_280,
        )
        .expect("orphan cleanup verification should be recorded");
    assert!(cleanup.cleanup_succeeded);
    assert_eq!(cleanup.event.state, WorkerLifecycleState::Orphaned);
    assert_eq!(cleanup.event.reason_code, "worker.cleanup_verified_requires_reattestation");
    let error = manager
        .assign_work("worker-m", lease_request("run-12c", 500), &policy, 2_290)
        .expect_err("orphan cleanup verification alone must not make worker assignable");
    assert!(matches!(error, WorkerLifecycleError::WorkerFailClosed(_)));
}

#[test]
fn stale_idle_worker_can_recover_with_fresh_heartbeat() {
    let mut manager = WorkerFleetManager::default();
    let policy = WorkerFleetPolicy { heartbeat_timeout_ms: 100, ..policy_for("tool:palyra.echo") };
    manager.register_worker(attestation("worker-idle"), &policy, 2_000).unwrap();

    let events = manager.mark_stale_heartbeat_workers(&policy, 2_250).unwrap();

    assert_eq!(events.len(), 1);
    assert_eq!(events[0].state, WorkerLifecycleState::Offline);
    assert_eq!(manager.snapshot().offline_workers, 1);

    let heartbeat = manager
        .heartbeat_worker("worker-idle", &policy, 2_260)
        .expect("idle offline worker should accept a fresh heartbeat");
    assert_eq!(heartbeat.state, WorkerLifecycleState::Registered);
    manager
        .assign_work("worker-idle", lease_request("run-idle", 500), &policy, 2_270)
        .expect("fresh idle worker should be reusable");
}

#[test]
fn draining_worker_rejects_new_leases_without_quarantine() {
    let mut manager = WorkerFleetManager::default();
    let policy = WorkerFleetPolicy::default();
    manager.register_worker(attestation("worker-n"), &policy, 2_000).unwrap();

    let drain = manager
        .drain_worker("worker-n", "worker.operator.drain", 2_100)
        .expect("drain should be recorded");
    assert_eq!(drain.state, WorkerLifecycleState::Draining);

    let error = manager
        .assign_work("worker-n", lease_request("run-13", 500), &policy, 2_200)
        .expect_err("draining worker must not accept a new lease");
    assert!(matches!(error, WorkerLifecycleError::WorkerDraining(_)));
    assert_eq!(manager.snapshot().draining_workers, 1);
    assert_eq!(manager.snapshot().failed_closed_workers, 0);
}

#[test]
fn compatibility_matrix_rejects_unversioned_worker_abi() {
    let mut manager = WorkerFleetManager::default();
    let policy = WorkerFleetPolicy {
        required_sdk_protocol_version: Some(2),
        required_wit_abi_version: Some("palyra-worker-abi/v2".to_owned()),
        ..WorkerFleetPolicy::default()
    };

    let error = manager
        .register_worker(attestation("worker-o"), &policy, 2_000)
        .expect_err("worker ABI mismatch must fail closed");

    assert!(matches!(error, WorkerLifecycleError::CompatibilityMismatch(_)));
    assert_eq!(manager.snapshot().registered_workers, 0);
}
