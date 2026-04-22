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

#[must_use]
pub(crate) fn networked_worker_supports_tool(tool_name: &str) -> bool {
    NETWORKED_WORKER_SUPPORTED_TOOLS
        .iter()
        .any(|supported| supported.eq_ignore_ascii_case(tool_name))
}

#[must_use]
pub(crate) fn networked_worker_tool_capability(tool_name: &str) -> String {
    format!("tool:{}", tool_name.to_ascii_lowercase())
}

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
