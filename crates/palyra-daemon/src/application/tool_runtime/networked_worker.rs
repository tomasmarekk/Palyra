//! Networked-worker tool execution backend.
//!
//! Routes a small allowlist of tools through the `palyra-workerd` fleet
//! contract: assign a capability-scoped lease, execute, complete the lease
//! with a cleanup attestation, and journal the artifact transport as a
//! runtime decision event. Every failure path (unsupported tool, lease
//! denial, cleanup failure, journal failure) fails closed with a reason-coded
//! [`ToolExecutionOutcome`] instead of falling back to another backend.

use std::{sync::Arc, time::Duration};

use palyra_common::{
    redaction::{redact_auth_error, redact_url_segments_in_text},
    runtime_contracts::REALTIME_DEFAULT_HEARTBEAT_INTERVAL_MS,
    runtime_preview::{
        RuntimeDecisionActor, RuntimeDecisionActorKind, RuntimeDecisionEventType,
        RuntimeDecisionPayload, RuntimeDecisionTiming, RuntimeEntityRef, RuntimeResourceBudget,
    },
};
use palyra_workerd::{
    WorkerArtifactTransport, WorkerAttestation, WorkerCleanupReport, WorkerLease,
    WorkerLeaseRequest, WorkerRemoteIdentity, WorkerRemoteLeaseBinding, WorkerRemoteToolKind,
    WorkerRemoteToolRequestEnvelope, WorkerRemoteToolResultEnvelope, WorkerRemoteWorkspaceTransfer,
    WorkerRunGrant, WorkerWorkspaceScope, WORKER_REMOTE_TOOL_PROTOCOL,
    WORKER_REMOTE_TOOL_SCHEMA_VERSION,
};
use serde_json::json;
use sha2::{Digest, Sha256};
use tracing::warn;
use ulid::Ulid;

use crate::{
    gateway::{current_unix_ms, GatewayRuntimeState, ToolRuntimeExecutionContext},
    node_runtime::{CapabilityExecutionResult, NodeRuntimeState, RegisteredNodeRecord},
    tool_protocol::{build_tool_execution_outcome, ToolExecutionOutcome},
};

const NETWORKED_WORKER_NODE_CAPABILITY_TIMEOUT_MS: u64 = 30_000;
const NETWORKED_WORKER_NODE_CAPABILITY_MAX_PAYLOAD_BYTES: u64 = 512 * 1024;

/// Dispatches a validated remote worker tool envelope to an actual worker transport.
#[async_trait::async_trait]
pub(crate) trait NetworkedWorkerRemoteDispatcher: Send + Sync {
    /// Executes `request` remotely and returns the worker result envelope.
    ///
    /// # Errors
    /// Returns a reason-coded dispatch error when the worker transport is not
    /// configured, the selected worker is unavailable, the request cannot be
    /// queued, or the worker returns a malformed result envelope.
    async fn dispatch_remote_tool(
        &self,
        request: WorkerRemoteToolRequestEnvelope,
    ) -> Result<WorkerRemoteToolResultEnvelope, NetworkedWorkerRemoteDispatchError>;
}

/// Remote dispatch failures surfaced as fail-closed tool outcomes.
#[derive(Debug, thiserror::Error)]
pub(crate) enum NetworkedWorkerRemoteDispatchError {
    #[error("remote worker transport is not configured")]
    Unconfigured,
    #[error("remote worker transport unavailable: {0}")]
    WorkerUnavailable(String),
    #[error("remote worker request was rejected: {0}")]
    RequestRejected(String),
    #[error("remote worker dispatch timed out for request_id={request_id}")]
    Timeout { request_id: String },
    #[error("remote worker result channel closed")]
    ResultChannelClosed,
    #[error("remote worker returned malformed result: {0}")]
    MalformedResult(String),
    #[error("remote worker transport failed: {0}")]
    TransportFailed(String),
}

/// Networked-worker dispatcher backed by the existing node capability queue.
#[derive(Debug)]
pub(crate) struct NodeRuntimeNetworkedWorkerDispatcher {
    node_runtime: Arc<NodeRuntimeState>,
    dispatch_timeout_ms: u64,
    max_payload_bytes: u64,
}

impl NodeRuntimeNetworkedWorkerDispatcher {
    /// Builds a dispatcher over the daemon's paired-node runtime.
    #[must_use]
    pub(crate) fn new(node_runtime: Arc<NodeRuntimeState>) -> Self {
        Self {
            node_runtime,
            dispatch_timeout_ms: NETWORKED_WORKER_NODE_CAPABILITY_TIMEOUT_MS,
            max_payload_bytes: NETWORKED_WORKER_NODE_CAPABILITY_MAX_PAYLOAD_BYTES,
        }
    }

    fn required_capability(request: &WorkerRemoteToolRequestEnvelope) -> String {
        request.tool_kind.required_capability()
    }
}

#[async_trait::async_trait]
impl NetworkedWorkerRemoteDispatcher for NodeRuntimeNetworkedWorkerDispatcher {
    async fn dispatch_remote_tool(
        &self,
        request: WorkerRemoteToolRequestEnvelope,
    ) -> Result<WorkerRemoteToolResultEnvelope, NetworkedWorkerRemoteDispatchError> {
        let worker_id = request.lease.worker_id.as_str();
        let node = self
            .node_runtime
            .node(worker_id)
            .map_err(|error| {
                NetworkedWorkerRemoteDispatchError::WorkerUnavailable(error.to_string())
            })?
            .ok_or_else(|| {
                NetworkedWorkerRemoteDispatchError::WorkerUnavailable(format!(
                    "node runtime has no registered worker node for worker_id={worker_id}"
                ))
            })?;
        ensure_node_is_ready_for_remote_worker(&node, &request)?;

        let payload = serde_json::to_vec(&request).map_err(|error| {
            NetworkedWorkerRemoteDispatchError::MalformedResult(format!(
                "failed to encode remote request envelope: {error}"
            ))
        })?;
        if payload.len() > usize::try_from(self.max_payload_bytes).unwrap_or(usize::MAX) {
            return Err(NetworkedWorkerRemoteDispatchError::RequestRejected(format!(
                "remote request envelope exceeds node payload limit ({} bytes > {} bytes)",
                payload.len(),
                self.max_payload_bytes
            )));
        }

        let capability = Self::required_capability(&request);
        let timeout_ms = bounded_dispatch_timeout_ms(self.dispatch_timeout_ms, &request);
        let (request_id, receiver) = self
            .node_runtime
            .enqueue_capability_request(
                worker_id,
                capability.as_str(),
                payload,
                self.max_payload_bytes,
                Some(timeout_ms),
            )
            .map_err(|error| {
                NetworkedWorkerRemoteDispatchError::RequestRejected(error.to_string())
            })?;

        let result = tokio::time::timeout(Duration::from_millis(timeout_ms), receiver)
            .await
            .map_err(|_| {
                let _ = self.node_runtime.mark_capability_timeout(request_id.as_str());
                NetworkedWorkerRemoteDispatchError::Timeout { request_id: request_id.clone() }
            })?
            .map_err(|_| NetworkedWorkerRemoteDispatchError::ResultChannelClosed)?;
        remote_result_from_node_capability_result(result)
    }
}

/// Returns whether `tool_name` may run on the networked-worker backend.
///
/// Tools outside the remote worker envelope subset fail closed in
/// [`execute_networked_worker_tool`] rather than falling back locally.
#[must_use]
pub(crate) fn networked_worker_supports_tool(tool_name: &str) -> bool {
    WorkerRemoteToolKind::from_tool_name(tool_name).is_some()
}

/// Builds the worker capability identifier required to lease `tool_name`.
#[must_use]
pub(crate) fn networked_worker_tool_capability(tool_name: &str) -> String {
    WorkerRemoteToolKind::from_tool_name(tool_name)
        .map(WorkerRemoteToolKind::required_capability)
        .unwrap_or_else(|| format!("tool:{}", tool_name.to_ascii_lowercase()))
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

    let worker_attestation = match runtime_state
        .networked_worker_attestation(lease.worker_id.as_str())
    {
        Some(attestation) => attestation,
        None => {
            if let Err(error) =
                complete_networked_worker_lease_after_remote_failure(runtime_state, &lease).await
            {
                return networked_worker_failure_outcome(
                    proposal_id,
                    tool_name,
                    input_json,
                    format!("networked worker cleanup failed: {}", error.message()),
                    "networked_worker_cleanup_failed",
                );
            }
            return networked_worker_failure_outcome(
                proposal_id,
                tool_name,
                input_json,
                format!(
                    "networked worker {} has no stored attestation for remote dispatch",
                    lease.worker_id
                ),
                "networked_worker_remote_fail_closed",
            );
        }
    };
    let remote_request = match build_worker_remote_tool_request(
        proposal_id,
        tool_name,
        input_json,
        &lease,
        &worker_attestation,
    ) {
        Ok(request) => request,
        Err(error) => {
            if let Err(cleanup_error) =
                complete_networked_worker_lease_after_remote_failure(runtime_state, &lease).await
            {
                return networked_worker_failure_outcome(
                    proposal_id,
                    tool_name,
                    input_json,
                    format!("networked worker cleanup failed: {}", cleanup_error.message()),
                    "networked_worker_cleanup_failed",
                );
            }
            return networked_worker_failure_outcome(
                proposal_id,
                tool_name,
                input_json,
                error,
                "networked_worker_remote_fail_closed",
            );
        }
    };
    let remote_result = match dispatch_networked_worker_remote_tool(runtime_state, &remote_request)
        .await
    {
        Ok(result) => result,
        Err(error) => {
            if let Err(cleanup_error) =
                complete_networked_worker_lease_after_remote_failure(runtime_state, &lease).await
            {
                return networked_worker_failure_outcome(
                    proposal_id,
                    tool_name,
                    input_json,
                    format!("networked worker cleanup failed: {}", cleanup_error.message()),
                    "networked_worker_cleanup_failed",
                );
            }
            return networked_worker_failure_outcome(
                proposal_id,
                tool_name,
                input_json,
                format!("networked worker remote dispatch failed: {error}"),
                "networked_worker_remote_unavailable",
            );
        }
    };

    if let Err(error) = runtime_state
        .complete_networked_worker_lease(
            lease.worker_id.as_str(),
            remote_result.cleanup_report.clone(),
        )
        .await
    {
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
        remote_result.output_manifest_sha256.as_str(),
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

    networked_worker_outcome_from_remote_result(&remote_request, remote_result, current_unix_ms())
}

fn build_worker_remote_tool_request(
    proposal_id: &str,
    tool_name: &str,
    input_json: &[u8],
    lease: &WorkerLease,
    worker_attestation: &WorkerAttestation,
) -> Result<WorkerRemoteToolRequestEnvelope, String> {
    let tool_kind = WorkerRemoteToolKind::from_tool_name(tool_name).ok_or_else(|| {
        format!(
            "backend policy blocked tool={tool_name}; reason_code=backend.policy.tool_unsupported; resolved_backend=networked_worker"
        )
    })?;
    let input_json_text = std::str::from_utf8(input_json)
        .map_err(|error| format!("networked worker remote input is not UTF-8 JSON: {error}"))?
        .to_owned();
    let workspace_manifest_sha256 = serde_json::to_vec(&lease.workspace_scope)
        .map(|bytes| sha256_hex(bytes.as_slice()))
        .unwrap_or_else(|_| sha256_hex(lease.workspace_scope.workspace_root.as_bytes()));
    let request = WorkerRemoteToolRequestEnvelope {
        protocol: WORKER_REMOTE_TOOL_PROTOCOL.to_owned(),
        schema_version: WORKER_REMOTE_TOOL_SCHEMA_VERSION,
        request_id: Ulid::new().to_string(),
        proposal_id: proposal_id.to_owned(),
        tool_name: tool_name.to_owned(),
        tool_kind,
        input_json: input_json_text,
        input_json_sha256: sha256_hex(input_json),
        lease: WorkerRemoteLeaseBinding::from(lease),
        worker_identity: WorkerRemoteIdentity::from(worker_attestation),
        workspace_transfer: WorkerRemoteWorkspaceTransfer::manifest(workspace_manifest_sha256),
    };
    request
        .validate(current_unix_ms())
        .map_err(|error| format!("networked worker remote request validation failed: {error}"))?;
    Ok(request)
}

async fn dispatch_networked_worker_remote_tool(
    runtime_state: &Arc<GatewayRuntimeState>,
    request: &WorkerRemoteToolRequestEnvelope,
) -> Result<WorkerRemoteToolResultEnvelope, String> {
    let dispatcher = runtime_state
        .networked_worker_remote_dispatcher()
        .ok_or(NetworkedWorkerRemoteDispatchError::Unconfigured)
        .map_err(|error| error.to_string())?;
    dispatcher.dispatch_remote_tool(request.clone()).await.map_err(|error| error.to_string())
}

async fn complete_networked_worker_lease_after_remote_failure(
    runtime_state: &Arc<GatewayRuntimeState>,
    lease: &WorkerLease,
) -> Result<(), tonic::Status> {
    runtime_state
        .complete_networked_worker_lease(
            lease.worker_id.as_str(),
            WorkerCleanupReport {
                removed_workspace_scope: true,
                removed_artifacts: true,
                removed_logs: true,
                failure_reason: None,
            },
        )
        .await
        .map(|_| ())
}

fn networked_worker_outcome_from_remote_result(
    request: &WorkerRemoteToolRequestEnvelope,
    result: WorkerRemoteToolResultEnvelope,
    now_unix_ms: i64,
) -> ToolExecutionOutcome {
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
    build_tool_execution_outcome(
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
    let read_only = WorkerRemoteToolKind::from_tool_name(tool_name)
        .is_none_or(remote_tool_kind_uses_read_only_workspace);
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
            read_only,
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

fn remote_tool_kind_uses_read_only_workspace(tool_kind: WorkerRemoteToolKind) -> bool {
    matches!(
        tool_kind,
        WorkerRemoteToolKind::FsRead
            | WorkerRemoteToolKind::FsList
            | WorkerRemoteToolKind::FsSearch
            | WorkerRemoteToolKind::ArtifactRead
    )
}

fn ensure_node_is_ready_for_remote_worker(
    node: &RegisteredNodeRecord,
    request: &WorkerRemoteToolRequestEnvelope,
) -> Result<(), NetworkedWorkerRemoteDispatchError> {
    let now = current_unix_ms();
    let node_age_ms = now.saturating_sub(node.last_seen_at_unix_ms);
    let freshness_ttl_ms =
        i64::try_from(REALTIME_DEFAULT_HEARTBEAT_INTERVAL_MS.saturating_mul(4)).unwrap_or(i64::MAX);
    if node_age_ms > freshness_ttl_ms {
        return Err(NetworkedWorkerRemoteDispatchError::WorkerUnavailable(format!(
            "worker node {} is stale (last_seen_age_ms={node_age_ms}, ttl_ms={freshness_ttl_ms})",
            node.device_id
        )));
    }

    let required_capability = request.tool_kind.required_capability();
    if !node
        .capabilities
        .iter()
        .any(|capability| capability.available && capability.name == required_capability)
    {
        return Err(NetworkedWorkerRemoteDispatchError::WorkerUnavailable(format!(
            "worker node {} does not advertise required capability {required_capability}",
            node.device_id
        )));
    }
    Ok(())
}

fn bounded_dispatch_timeout_ms(
    configured_timeout_ms: u64,
    request: &WorkerRemoteToolRequestEnvelope,
) -> u64 {
    let remaining_lease_ms = request.lease.expires_at_unix_ms.saturating_sub(current_unix_ms());
    let remaining_lease_ms = u64::try_from(remaining_lease_ms).unwrap_or(1).max(1);
    configured_timeout_ms.min(remaining_lease_ms).max(1)
}

fn remote_result_from_node_capability_result(
    result: CapabilityExecutionResult,
) -> Result<WorkerRemoteToolResultEnvelope, NetworkedWorkerRemoteDispatchError> {
    if !result.success {
        return Err(NetworkedWorkerRemoteDispatchError::TransportFailed(
            sanitize_remote_dispatch_error(result.error.as_str()),
        ));
    }
    serde_json::from_slice::<WorkerRemoteToolResultEnvelope>(result.output_json.as_slice()).map_err(
        |error| {
            NetworkedWorkerRemoteDispatchError::MalformedResult(format!(
                "node capability output was not a remote worker result envelope: {error}"
            ))
        },
    )
}

fn sanitize_remote_dispatch_error(error: &str) -> String {
    let redacted = redact_url_segments_in_text(&redact_auth_error(error));
    let trimmed = redacted.trim();
    if trimmed.is_empty() {
        "remote worker reported a transport failure".to_owned()
    } else {
        trimmed.chars().take(512).collect()
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
    use super::{
        networked_worker_outcome_from_remote_result, networked_worker_supports_tool,
        networked_worker_tool_capability, remote_tool_kind_uses_read_only_workspace, sha256_hex,
    };
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

    #[test]
    fn networked_worker_supports_remote_tool_subset_not_echo_sleep_only() {
        for tool_name in [
            "palyra.fs.read_file",
            "palyra.fs.list_dir",
            "palyra.fs.search",
            "palyra.process.run",
            "palyra.fs.apply_patch",
            "palyra.artifact.read",
            "palyra.tool_program.run",
        ] {
            assert!(
                networked_worker_supports_tool(tool_name),
                "{tool_name} should be remote-capable"
            );
            assert_eq!(
                networked_worker_tool_capability(tool_name),
                WorkerRemoteToolKind::from_tool_name(tool_name)
                    .expect("tool should have a remote kind")
                    .required_capability()
            );
        }

        assert!(!networked_worker_supports_tool("palyra.echo"));
        assert!(!networked_worker_supports_tool("palyra.sleep"));
    }

    #[test]
    fn networked_worker_workspace_scope_tracks_remote_tool_mutability() {
        assert!(remote_tool_kind_uses_read_only_workspace(WorkerRemoteToolKind::FsRead));
        assert!(remote_tool_kind_uses_read_only_workspace(WorkerRemoteToolKind::FsList));
        assert!(remote_tool_kind_uses_read_only_workspace(WorkerRemoteToolKind::FsSearch));
        assert!(remote_tool_kind_uses_read_only_workspace(WorkerRemoteToolKind::ArtifactRead));
        assert!(!remote_tool_kind_uses_read_only_workspace(WorkerRemoteToolKind::ProcessRun));
        assert!(!remote_tool_kind_uses_read_only_workspace(WorkerRemoteToolKind::ApplyPatch));
        assert!(!remote_tool_kind_uses_read_only_workspace(WorkerRemoteToolKind::ToolProgramRun));
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
            let outcome = networked_worker_outcome_from_remote_result(&request, result, 2_000);

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

        let outcome = networked_worker_outcome_from_remote_result(&request, result, 2_000);

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

        let outcome = networked_worker_outcome_from_remote_result(&request, result, 2_000);

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

        let outcome = networked_worker_outcome_from_remote_result(&request, result, 2_000);

        assert!(!outcome.success);
        assert!(outcome.error.contains("cleanup gap"));
        assert_eq!(outcome.attestation.sandbox_enforcement, "networked_worker_remote_fail_closed");
    }

    #[test]
    fn fake_worker_remote_identity_mismatch_fails_closed() {
        let request = remote_request("palyra.artifact.read");
        let fake = FakeRemoteWorker::healthy("worker-remote-02");
        let result = fake.execute(&request, json!({"artifact_id": "artifact-01"}));

        let outcome = networked_worker_outcome_from_remote_result(&request, result, 2_000);

        assert!(!outcome.success);
        assert!(outcome.error.contains("identity mismatch"));
        assert_eq!(outcome.attestation.sandbox_enforcement, "networked_worker_remote_fail_closed");
    }
}
