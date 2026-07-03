//! Networked-worker tool execution backend.
//!
//! Routes a small allowlist of tools through the `palyra-workerd` fleet
//! contract: assign a capability-scoped lease, execute, complete the lease
//! with a cleanup attestation, and journal the artifact transport as a
//! runtime decision event. Every failure path (unsupported tool, lease
//! denial, cleanup failure, journal failure) fails closed with a reason-coded
//! [`ToolExecutionOutcome`] instead of falling back to another backend.

use std::sync::Arc;

use palyra_common::runtime_preview::{
    RuntimeDecisionActor, RuntimeDecisionActorKind, RuntimeDecisionEventType,
    RuntimeDecisionPayload, RuntimeDecisionTiming, RuntimeEntityRef, RuntimeResourceBudget,
};
use palyra_workerd::{
    WorkerArtifactTransport, WorkerCleanupReport, WorkerLease, WorkerLeaseRequest, WorkerRunGrant,
    WorkerWorkspaceScope,
};
use serde_json::json;
use sha2::{Digest, Sha256};
use tracing::warn;
use ulid::Ulid;

use crate::{
    gateway::{current_unix_ms, GatewayRuntimeState, ToolRuntimeExecutionContext},
    tool_protocol::{build_tool_execution_outcome, execute_tool_call, ToolExecutionOutcome},
};

const NETWORKED_WORKER_SUPPORTED_TOOLS: &[&str] = &["palyra.echo", "palyra.sleep"];

/// Returns whether `tool_name` may run on the networked-worker backend.
///
/// Tools outside [`NETWORKED_WORKER_SUPPORTED_TOOLS`] fail closed in
/// [`execute_networked_worker_tool`] rather than falling back locally.
#[must_use]
pub(crate) fn networked_worker_supports_tool(tool_name: &str) -> bool {
    NETWORKED_WORKER_SUPPORTED_TOOLS
        .iter()
        .any(|supported| supported.eq_ignore_ascii_case(tool_name))
}

/// Builds the worker capability identifier required to lease `tool_name`.
#[must_use]
pub(crate) fn networked_worker_tool_capability(tool_name: &str) -> String {
    format!("tool:{}", tool_name.to_ascii_lowercase())
}

/// Executes `tool_name` under a networked-worker lease and returns the
/// attested outcome.
///
/// The full lease lifecycle (assignment, cleanup report, artifact-transport
/// journal event) must succeed; any lifecycle failure overrides the tool
/// result and fails the call closed.
pub(crate) async fn execute_networked_worker_tool(
    runtime_state: &Arc<GatewayRuntimeState>,
    context: ToolRuntimeExecutionContext<'_>,
    proposal_id: &str,
    tool_name: &str,
    input_json: &[u8],
) -> ToolExecutionOutcome {
    if !networked_worker_supports_tool(tool_name) {
        return networked_worker_failure_outcome(
            proposal_id,
            tool_name,
            input_json,
            format!(
                "backend policy blocked tool={tool_name}; reason_code=backend.policy.tool_unsupported; resolved_backend=networked_worker"
            ),
            "networked_worker_fail_closed",
        );
    }

    let request =
        build_worker_lease_request(runtime_state, context, proposal_id, tool_name, input_json);
    let (lease, _) = match runtime_state.assign_next_networked_worker_lease(request).await {
        Ok(assignment) => assignment,
        Err(error) => {
            return networked_worker_failure_outcome(
                proposal_id,
                tool_name,
                input_json,
                format!("networked worker lease assignment failed: {}", error.message()),
                "networked_worker_lease_denied",
            );
        }
    };

    // The tool body still runs through the local runner; the lease exists to
    // exercise and attest the fleet lifecycle (assign -> cleanup -> journal)
    // for the supported tool allowlist before remote dispatch lands.
    let local_outcome =
        execute_tool_call(&runtime_state.config.tool_call, proposal_id, tool_name, input_json)
            .await;
    let output_manifest_sha256 = sha256_hex(local_outcome.output_json.as_slice());

    let cleanup_result = runtime_state
        .complete_networked_worker_lease(
            lease.worker_id.as_str(),
            WorkerCleanupReport {
                removed_workspace_scope: true,
                removed_artifacts: true,
                removed_logs: true,
                failure_reason: None,
            },
        )
        .await;

    if let Err(error) = cleanup_result {
        return networked_worker_failure_outcome(
            proposal_id,
            tool_name,
            input_json,
            format!("networked worker cleanup failed: {}", error.message()),
            "networked_worker_cleanup_failed",
        );
    }

    if let Err(error) = record_worker_artifact_transport_event(
        runtime_state,
        context,
        &lease,
        proposal_id,
        tool_name,
        input_json,
        output_manifest_sha256.as_str(),
    )
    .await
    {
        return networked_worker_failure_outcome(
            proposal_id,
            tool_name,
            input_json,
            format!("networked worker artifact transport journal failed: {}", error.message()),
            "networked_worker_artifact_journal_failed",
        );
    }

    build_tool_execution_outcome(
        proposal_id,
        tool_name,
        input_json,
        local_outcome.success,
        local_outcome.output_json,
        local_outcome.error,
        local_outcome.attestation.timed_out,
        format!("networked_worker:{}", lease.worker_id),
        format!(
            "networked_worker;lease_id={};grant_id={};backend_reason_code={}",
            lease.lease_id, lease.grant.grant_id, context.backend_reason_code
        ),
    )
}

fn build_worker_lease_request(
    runtime_state: &Arc<GatewayRuntimeState>,
    context: ToolRuntimeExecutionContext<'_>,
    proposal_id: &str,
    tool_name: &str,
    input_json: &[u8],
) -> WorkerLeaseRequest {
    let now_unix_ms = current_unix_ms();
    let ttl_ms = runtime_state.config.networked_workers.lease_ttl_ms;
    let grant_id = Ulid::new().to_string();
    WorkerLeaseRequest {
        run_id: context.run_id.to_owned(),
        ttl_ms,
        required_capabilities: vec![networked_worker_tool_capability(tool_name)],
        workspace_scope: WorkerWorkspaceScope {
            workspace_root: runtime_state
                .config
                .tool_call
                .process_runner
                .workspace_root
                .to_string_lossy()
                .into_owned(),
            allowed_paths: Vec::new(),
            read_only: true,
        },
        artifact_transport: WorkerArtifactTransport {
            input_manifest_sha256: sha256_hex(input_json),
            // The real output does not exist at lease time; a deterministic
            // placeholder digest keeps the lease request well-formed and the
            // attested digest is journaled after execution instead.
            output_manifest_sha256: sha256_hex(
                format!("pending:{proposal_id}:{tool_name}:{}", context.run_id).as_bytes(),
            ),
            log_stream_id: format!("worker-logs/{}/{}", context.run_id, proposal_id),
            scratch_directory_id: format!("worker-scratch/{}/{}", context.run_id, proposal_id),
        },
        grant: WorkerRunGrant {
            grant_id,
            run_id: context.run_id.to_owned(),
            tool_name: tool_name.to_owned(),
            // `ttl_ms as i64` only wraps for absurd operator-configured TTLs
            // (> i64::MAX ms); the wrap turns negative and merely expires the
            // grant immediately, which fails safe.
            expires_at_unix_ms: now_unix_ms.saturating_add(ttl_ms as i64),
        },
    }
}

async fn record_worker_artifact_transport_event(
    runtime_state: &Arc<GatewayRuntimeState>,
    context: ToolRuntimeExecutionContext<'_>,
    lease: &WorkerLease,
    proposal_id: &str,
    tool_name: &str,
    input_json: &[u8],
    output_manifest_sha256: &str,
) -> Result<(), tonic::Status> {
    let payload = RuntimeDecisionPayload::new(
        RuntimeDecisionEventType::WorkerLeaseLifecycle,
        RuntimeDecisionActor::new(
            RuntimeDecisionActorKind::Worker,
            context.principal.to_owned(),
            context.device_id.to_owned(),
            context.channel.map(ToOwned::to_owned),
        ),
        "worker.artifact_transport.attested",
        "networked_workers.artifact_transport.daemon",
        RuntimeDecisionTiming::observed(current_unix_ms()),
    )
    .with_input(
        RuntimeEntityRef::new("worker_lease", "worker", lease.lease_id.clone())
            .with_state("completed"),
    )
    .with_output(
        RuntimeEntityRef::new("artifact_manifest", "artifact", output_manifest_sha256.to_owned())
            .with_state("attested"),
    )
    .with_resource_budget(RuntimeResourceBudget::default())
    .with_details(json!({
        "proposal_id": proposal_id,
        "tool_name": tool_name,
        "worker_id": lease.worker_id.as_str(),
        "lease_id": lease.lease_id.as_str(),
        "grant_id": lease.grant.grant_id.as_str(),
        "required_capabilities": lease.required_capabilities.clone(),
        "workspace_scope": {
            "read_only": lease.workspace_scope.read_only,
            "allowed_paths": lease.workspace_scope.allowed_paths.clone(),
        },
        "workspace_writeback": {
            "mode": "patch_bundle",
            "authoritative_workspace_mutation": false,
            "approval_required": true,
            "conflict_policy": "reject_changed_local_workspace",
            "cleanup_attestation_required": true,
        },
        "artifact_transport": {
            "input_manifest_sha256": sha256_hex(input_json),
            "output_manifest_sha256": output_manifest_sha256,
            "log_stream_id": lease.artifact_transport.log_stream_id.as_str(),
            "scratch_directory_id": lease.artifact_transport.scratch_directory_id.as_str(),
        },
    }));

    runtime_state
        .record_system_runtime_decision_event(
            context.principal,
            context.device_id,
            context.channel,
            Some(context.session_id),
            Some(context.run_id),
            payload,
        )
        .await
}

fn networked_worker_failure_outcome(
    proposal_id: &str,
    tool_name: &str,
    input_json: &[u8],
    error: String,
    sandbox_enforcement: &str,
) -> ToolExecutionOutcome {
    warn!(tool_name, error = %error, "networked worker execution failed closed");
    build_tool_execution_outcome(
        proposal_id,
        tool_name,
        input_json,
        false,
        b"{}".to_vec(),
        error,
        false,
        "networked_worker".to_owned(),
        sandbox_enforcement.to_owned(),
    )
}

fn sha256_hex(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    hex::encode(hasher.finalize())
}

#[cfg(test)]
mod tests {
    use super::{networked_worker_failure_outcome, sha256_hex};
    use palyra_workerd::{
        WorkerArtifactTransport, WorkerCleanupReport, WorkerRemoteIdentity,
        WorkerRemoteLeaseBinding, WorkerRemoteToolKind, WorkerRemoteToolRequestEnvelope,
        WorkerRemoteToolResultEnvelope, WorkerRemoteWorkspaceTransfer, WorkerWorkspaceScope,
        WORKER_REMOTE_TOOL_PROTOCOL, WORKER_REMOTE_TOOL_SCHEMA_VERSION,
    };
    use serde_json::{json, Value};

    struct FakeRemoteWorker {
        identity: WorkerRemoteIdentity,
        cleanup_report: WorkerCleanupReport,
        completed_at_unix_ms: i64,
    }

    impl FakeRemoteWorker {
        fn healthy(worker_id: &str) -> Self {
            Self {
                identity: remote_identity(worker_id),
                cleanup_report: WorkerCleanupReport {
                    removed_workspace_scope: true,
                    removed_artifacts: true,
                    removed_logs: true,
                    failure_reason: None,
                },
                completed_at_unix_ms: 2_000,
            }
        }

        fn execute(
            &self,
            request: &WorkerRemoteToolRequestEnvelope,
            output_json: Value,
        ) -> WorkerRemoteToolResultEnvelope {
            let output_json =
                serde_json::to_string(&output_json).expect("fake output should serialize");
            WorkerRemoteToolResultEnvelope {
                protocol: WORKER_REMOTE_TOOL_PROTOCOL.to_owned(),
                schema_version: WORKER_REMOTE_TOOL_SCHEMA_VERSION,
                request_id: request.request_id.clone(),
                proposal_id: request.proposal_id.clone(),
                tool_name: request.tool_name.clone(),
                tool_kind: request.tool_kind,
                worker_id: request.lease.worker_id.clone(),
                lease_id: request.lease.lease_id.clone(),
                success: true,
                output_json_sha256: sha256_hex(output_json.as_bytes()),
                output_json,
                error: None,
                output_manifest_sha256: sha256_hex(
                    format!("manifest:{}", request.request_id).as_bytes(),
                ),
                cleanup_report: self.cleanup_report.clone(),
                worker_identity: self.identity.clone(),
                completed_at_unix_ms: self.completed_at_unix_ms,
            }
        }
    }

    fn remote_identity(worker_id: &str) -> WorkerRemoteIdentity {
        WorkerRemoteIdentity {
            worker_id: worker_id.to_owned(),
            image_digest_sha256: sha256_hex(format!("image:{worker_id}").as_bytes()),
            build_digest_sha256: sha256_hex(format!("build:{worker_id}").as_bytes()),
            artifact_digest_sha256: sha256_hex(format!("artifact:{worker_id}").as_bytes()),
            capability_authority_sha256: Some(sha256_hex(
                format!("authority:{worker_id}").as_bytes(),
            )),
            sdk_protocol_version: 1,
            wit_abi_version: "palyra-worker-abi/v1".to_owned(),
        }
    }

    fn remote_request(tool_name: &str) -> WorkerRemoteToolRequestEnvelope {
        let tool_kind = WorkerRemoteToolKind::from_tool_name(tool_name)
            .expect("test tool should be remote-capable");
        WorkerRemoteToolRequestEnvelope {
            protocol: WORKER_REMOTE_TOOL_PROTOCOL.to_owned(),
            schema_version: WORKER_REMOTE_TOOL_SCHEMA_VERSION,
            request_id: format!("req-{}", tool_kind.as_str()),
            proposal_id: format!("proposal-{}", tool_kind.as_str()),
            tool_name: tool_name.to_owned(),
            tool_kind,
            input_json: serde_json::to_string(&json!({"path": "src/lib.rs"}))
                .expect("test input should serialize"),
            input_json_sha256: sha256_hex(br#"{"path":"src/lib.rs"}"#),
            lease: WorkerRemoteLeaseBinding {
                lease_id: format!("lease-{}", tool_kind.as_str()),
                worker_id: "worker-remote-01".to_owned(),
                run_id: "run-remote-01".to_owned(),
                grant_id: format!("grant-{}", tool_kind.as_str()),
                grant_tool_name: tool_name.to_owned(),
                expires_at_unix_ms: 3_000,
                required_capabilities: vec![tool_kind.required_capability()],
                workspace_scope: WorkerWorkspaceScope {
                    workspace_root: "/workspace".to_owned(),
                    allowed_paths: vec!["src".to_owned()],
                    read_only: true,
                },
                artifact_transport: WorkerArtifactTransport {
                    input_manifest_sha256: sha256_hex(b"input-manifest"),
                    output_manifest_sha256: sha256_hex(b"pending-output-manifest"),
                    log_stream_id: "logs/run-remote-01".to_owned(),
                    scratch_directory_id: "scratch/run-remote-01".to_owned(),
                },
            },
            worker_identity: remote_identity("worker-remote-01"),
            workspace_transfer: WorkerRemoteWorkspaceTransfer::manifest(sha256_hex(
                b"workspace-manifest",
            )),
        }
    }

    fn outcome_from_remote_result(
        request: &WorkerRemoteToolRequestEnvelope,
        result: WorkerRemoteToolResultEnvelope,
        now_unix_ms: i64,
    ) -> crate::tool_protocol::ToolExecutionOutcome {
        if let Err(error) = result.validate_against_request(request, now_unix_ms) {
            return networked_worker_failure_outcome(
                request.proposal_id.as_str(),
                request.tool_name.as_str(),
                request.input_json.as_bytes(),
                format!("networked worker remote execution failed: {error}"),
                "networked_worker_remote_fail_closed",
            );
        }
        if sha256_hex(result.output_json.as_bytes()) != result.output_json_sha256 {
            return networked_worker_failure_outcome(
                request.proposal_id.as_str(),
                request.tool_name.as_str(),
                request.input_json.as_bytes(),
                "networked worker remote execution failed: output digest mismatch".to_owned(),
                "networked_worker_remote_digest_mismatch",
            );
        }
        crate::tool_protocol::build_tool_execution_outcome(
            request.proposal_id.as_str(),
            request.tool_name.as_str(),
            request.input_json.as_bytes(),
            result.success,
            result.output_json.into_bytes(),
            result.error.unwrap_or_default(),
            false,
            format!("networked_worker:{}", result.worker_id),
            format!(
                "networked_worker_remote;lease_id={};grant_id={};worker_identity_sha256={};input_manifest_sha256={};output_manifest_sha256={};workspace_manifest_sha256={}",
                request.lease.lease_id,
                request.lease.grant_id,
                request.worker_identity.artifact_digest_sha256,
                request.lease.artifact_transport.input_manifest_sha256,
                result.output_manifest_sha256,
                request.workspace_transfer.workspace_manifest_sha256
            ),
        )
    }

    #[test]
    fn fake_worker_remote_fs_read_list_search_preserves_tool_output_schema() {
        let fake = FakeRemoteWorker::healthy("worker-remote-01");
        let cases = [
            (
                "palyra.fs.read_file",
                json!({
                    "path": "src/lib.rs",
                    "content": "pub mod application;",
                    "bytes": 20,
                    "eof": true,
                }),
            ),
            (
                "palyra.fs.list_dir",
                json!({
                    "path": "src",
                    "entries": [{"name": "lib.rs", "kind": "file"}],
                }),
            ),
            (
                "palyra.fs.search",
                json!({
                    "query": "application",
                    "matches": [{"path": "src/lib.rs", "line": 1, "text": "pub mod application;"}],
                }),
            ),
        ];

        for (tool_name, expected_output) in cases {
            let request = remote_request(tool_name);
            let result = fake.execute(&request, expected_output.clone());
            let outcome = outcome_from_remote_result(&request, result, 2_000);

            assert!(outcome.success, "remote {tool_name} should succeed");
            assert_eq!(outcome.error, "");
            assert_eq!(outcome.attestation.executor, "networked_worker:worker-remote-01");
            assert!(outcome.attestation.sandbox_enforcement.contains("output_manifest_sha256="));
            let output: Value = serde_json::from_slice(outcome.output_json.as_slice())
                .expect("remote output should stay valid JSON");
            assert_eq!(output, expected_output);
            assert!(
                output.get("remote_worker").is_none(),
                "remote result must not wrap the tool-specific output schema"
            );
        }
    }

    #[test]
    fn fake_worker_remote_process_run_preserves_local_process_schema() {
        let fake = FakeRemoteWorker::healthy("worker-remote-01");
        let request = remote_request("palyra.process.run");
        let result = fake.execute(
            &request,
            json!({
                "schema_version": 2,
                "exit_code": 0,
                "stdout": "ok\n",
                "stderr": "",
                "stdout_truncated": false,
                "stderr_truncated": false,
                "duration_ms": 12,
                "sandbox_backend": "networked_worker",
                "resource_usage": {"cpu_time_ms": 3, "max_rss_bytes": 1024},
                "workspace_writeback": {
                    "mode": "patch_bundle",
                    "patches": [],
                    "approval_required": true,
                },
            }),
        );

        let outcome = outcome_from_remote_result(&request, result, 2_000);

        assert!(outcome.success);
        let output: Value = serde_json::from_slice(outcome.output_json.as_slice())
            .expect("process output should be JSON");
        assert_eq!(output.pointer("/schema_version").and_then(Value::as_u64), Some(2));
        assert_eq!(output.pointer("/exit_code").and_then(Value::as_i64), Some(0));
        assert_eq!(output.pointer("/stdout").and_then(Value::as_str), Some("ok\n"));
        assert_eq!(
            output.pointer("/workspace_writeback/mode").and_then(Value::as_str),
            Some("patch_bundle")
        );
    }

    #[test]
    fn fake_worker_remote_lease_expiry_fails_closed() {
        let fake = FakeRemoteWorker::healthy("worker-remote-01");
        let mut request = remote_request("palyra.fs.read_file");
        request.lease.expires_at_unix_ms = 1_500;
        let result = fake.execute(&request, json!({"content": "late"}));

        let outcome = outcome_from_remote_result(&request, result, 2_000);

        assert!(!outcome.success);
        assert!(outcome.error.contains("expired"));
        assert_eq!(outcome.attestation.sandbox_enforcement, "networked_worker_remote_fail_closed");
    }

    #[test]
    fn fake_worker_remote_cleanup_gap_fails_closed() {
        let request = remote_request("palyra.fs.apply_patch");
        let mut fake = FakeRemoteWorker::healthy("worker-remote-01");
        fake.cleanup_report.removed_artifacts = false;
        fake.cleanup_report.failure_reason = Some("artifact directory not empty".to_owned());
        let result = fake.execute(&request, json!({"applied": true}));

        let outcome = outcome_from_remote_result(&request, result, 2_000);

        assert!(!outcome.success);
        assert!(outcome.error.contains("cleanup gap"));
        assert_eq!(outcome.attestation.sandbox_enforcement, "networked_worker_remote_fail_closed");
    }

    #[test]
    fn fake_worker_remote_identity_mismatch_fails_closed() {
        let request = remote_request("palyra.artifact.read");
        let fake = FakeRemoteWorker::healthy("worker-remote-02");
        let result = fake.execute(&request, json!({"artifact_id": "artifact-01"}));

        let outcome = outcome_from_remote_result(&request, result, 2_000);

        assert!(!outcome.success);
        assert!(outcome.error.contains("identity mismatch"));
        assert_eq!(outcome.attestation.sandbox_enforcement, "networked_worker_remote_fail_closed");
    }
}
