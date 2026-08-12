//! Networked-worker tool execution backend.
//!
//! Routes a small allowlist of tools through the `palyra-workerd` fleet
//! contract: assign a capability-scoped lease, execute, complete the lease
//! with a cleanup attestation, and journal the artifact transport as a
//! runtime decision event. Every failure path (unsupported tool, lease
//! denial, cleanup failure, journal failure) fails closed with a reason-coded
//! [`ToolExecutionOutcome`] instead of falling back to another backend.

use std::{
    collections::BTreeMap,
    fs,
    path::{Component, Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use base64::{engine::general_purpose::STANDARD as BASE64_STANDARD, Engine as _};
use palyra_common::{
    process_runner_input::{
        interpreter_args_contain_blocked_eval_flag, parse_process_runner_tool_input,
        process_executable_is_interpreter,
    },
    redaction::{redact_auth_error, redact_auth_error_strict, redact_url_segments_in_text},
    runtime_contracts::{
        ArtifactRetentionPolicy, RuntimeGeneration, RuntimeRunId, RuntimeSessionId,
        ToolResultArtifactRef, ToolResultSensitivity, REALTIME_DEFAULT_HEARTBEAT_INTERVAL_MS,
    },
    tool_catalog::{tool_metadata, tool_policy_capability_names, tool_requires_approval},
};
use palyra_workerd::{
    computer_use::{
        ComputerUseAction, ComputerUseApproval, ComputerUseBackendKind,
        ComputerUseCapabilityProfile, ComputerUseRiskClass, ComputerUseTaskContract,
        ComputerUseToolInput, ComputerUseWorkerOutput,
    },
    remote_protocol::{RemoteTaskOutcome, RemoteWorkerProtocolV1},
    WorkerArtifactTransport, WorkerAttestation, WorkerCleanupReport, WorkerLease,
    WorkerLeaseRequest, WorkerRemoteIdentity, WorkerRemoteLeaseBinding,
    WorkerRemoteToolContractError, WorkerRemoteToolKind, WorkerRemoteToolRequestEnvelope,
    WorkerRemoteToolResultEnvelope, WorkerRemoteWorkspaceEntry, WorkerRemoteWorkspaceEntryKind,
    WorkerRemoteWorkspaceTransfer, WorkerRunGrant, WorkerWorkspaceScope,
    WORKER_REMOTE_TOOL_PROTOCOL, WORKER_REMOTE_TOOL_SCHEMA_VERSION,
};
use sha2::{Digest, Sha256};
use tracing::warn;
use ulid::Ulid;

use crate::{
    application::approvals::build_tool_approval_subject_id,
    execution_backends::{ExecutionBackendPreference, WorkspaceStrategyDescriptor},
    gateway::{
        current_unix_ms, GatewayRuntimeState, ManagedRuntimeHealthAuthority,
        ManagedRuntimeHealthFamily, ToolRuntimeExecutionContext, APPROVAL_POLICY_ID,
    },
    journal::{
        ApprovalDecision, ApprovalDecisionScope, ApprovalRecord, ApprovalSubjectType,
        ToolResultArtifactCreateRequest,
    },
    node_runtime::{
        CapabilityDispatchAuthorizer, CapabilityExecutionNotification, CapabilityExecutionReceiver,
        CapabilityExecutionResult, CapabilityRequestStopOutcome, CapabilityRequestTimeoutOutcome,
        NetworkedWorkerHostReceiptContext, NetworkedWorkerResultCommitContext,
        NetworkedWorkerResultCommitDisposition, NodeRuntimeState, RegisteredNodeRecord,
        NETWORKED_WORKER_DELIVERY_FENCE_CAPABILITY,
    },
    tool_protocol::{
        build_tool_execution_outcome, build_tool_execution_outcome_with_manifest,
        ExecutionAttestationManifest, ExecutionCleanupEvidence, ExecutionCleanupResourceEvidence,
        ToolExecutionOutcome,
    },
};

const NETWORKED_WORKER_NODE_CAPABILITY_TIMEOUT_MS: u64 = 30_000;
const NETWORKED_WORKER_GRANT_ADMISSION_SLACK_MS: i64 = 5_000;
const NETWORKED_WORKER_NODE_CAPABILITY_MAX_PAYLOAD_BYTES: u64 = 512 * 1024;
const NETWORKED_WORKER_WORKSPACE_BUNDLE_MAX_BYTES: usize = 384 * 1024;
const NETWORKED_WORKER_WORKSPACE_BUNDLE_MAX_ENTRIES: usize = 128;
const COMPUTER_USE_VIEWPORT_WIDTH: u32 = 320;
const COMPUTER_USE_VIEWPORT_HEIGHT: u32 = 180;
const COMPUTER_USE_MAX_WALL_CLOCK_MS: u64 = 25_000;
const COMPUTER_USE_MAX_WAIT_MS: u64 = 5_000;
const COMPUTER_USE_MAX_SCREENSHOT_BYTES: u64 = 256 * 1024;
const COMPUTER_USE_APPROVAL_JOURNAL_WINDOW: usize = 512;
const NETWORKED_WORKER_SKIPPED_DIRECTORIES: &[&str] =
    &[".git", "node_modules", "target", "dist", "build"];

#[derive(Debug, Clone)]
struct OwnedToolRuntimeExecutionContext {
    principal: String,
    device_id: String,
    channel: Option<String>,
    session_id: String,
    run_id: String,
    execution_backend: ExecutionBackendPreference,
    backend_reason_code: String,
}

impl OwnedToolRuntimeExecutionContext {
    fn from_borrowed(context: ToolRuntimeExecutionContext<'_>) -> Self {
        Self {
            principal: context.principal.to_owned(),
            device_id: context.device_id.to_owned(),
            channel: context.channel.map(ToOwned::to_owned),
            session_id: context.session_id.to_owned(),
            run_id: context.run_id.to_owned(),
            execution_backend: context.execution_backend,
            backend_reason_code: context.backend_reason_code.to_owned(),
        }
    }

    fn as_borrowed(&self) -> ToolRuntimeExecutionContext<'_> {
        ToolRuntimeExecutionContext {
            principal: self.principal.as_str(),
            device_id: self.device_id.as_str(),
            channel: self.channel.as_deref(),
            session_id: self.session_id.as_str(),
            run_id: self.run_id.as_str(),
            execution_backend: self.execution_backend,
            backend_reason_code: self.backend_reason_code.as_str(),
        }
    }
}

struct NetworkedWorkerTaskRequest<'a> {
    proposal_id: &'a str,
    tool_name: &'a str,
    input_json: &'a [u8],
    lease: &'a WorkerLease,
    worker_attestation: &'a WorkerAttestation,
    session_id: &'a str,
    run_generation: RuntimeGeneration,
    process_executable_allowlist: Vec<String>,
    workspace_transfer: WorkerRemoteWorkspaceTransfer,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ComputerUseHostApprovalAuthority {
    approval_id: String,
    expires_at_unix_ms: Option<i64>,
}

/// One remote worker response plus optional node-delivery settlement identity.
#[derive(Debug)]
pub(crate) struct NetworkedWorkerRemoteDispatchResult {
    pub(crate) result: WorkerRemoteToolResultEnvelope,
    pub(crate) delivery_attempt_id: Option<String>,
    pub(crate) observed_at_unix_ms: i64,
}

#[derive(Debug, Clone, Copy)]
enum NetworkedWorkerReleasedDispatchReason {
    Cancelled,
    TimedOut,
}

impl NetworkedWorkerReleasedDispatchReason {
    fn failure_text(self) -> &'static str {
        match self {
            Self::Cancelled => "networked worker remote dispatch cancelled after payload release",
            Self::TimedOut => "networked worker remote dispatch timed out after payload release",
        }
    }
}

#[derive(Debug)]
pub(crate) struct NetworkedWorkerReleasedDispatch {
    node_request_id: String,
    receiver: CapabilityExecutionReceiver,
    reason: NetworkedWorkerReleasedDispatchReason,
}

#[derive(Debug)]
pub(crate) enum NetworkedWorkerRemoteDispatchOutcome {
    Completed(Box<NetworkedWorkerRemoteDispatchResult>),
    Released(NetworkedWorkerReleasedDispatch),
}

/// Dispatches a validated remote worker tool envelope to an actual worker transport.
#[async_trait::async_trait]
pub(crate) trait NetworkedWorkerRemoteDispatcher: Send + Sync {
    /// Verifies that `worker_id` can speak the exact transport contract before a lease is issued.
    ///
    /// # Errors
    /// Returns a fail-closed transport error when the worker is missing, stale, incompatible, or
    /// does not currently advertise every required capability.
    fn preflight_worker(
        &self,
        worker_id: &str,
        required_capabilities: &[String],
    ) -> Result<(), NetworkedWorkerRemoteDispatchError>;

    /// Executes `request` remotely and returns the worker result envelope.
    ///
    /// # Errors
    /// Returns a reason-coded dispatch error when the worker transport is not
    /// configured, the selected worker is unavailable, the request cannot be
    /// queued, or the worker returns a malformed result envelope.
    async fn dispatch_remote_tool(
        &self,
        runtime_state: &Arc<GatewayRuntimeState>,
        host_context: NetworkedWorkerHostReceiptContext,
        request: WorkerRemoteToolRequestEnvelope,
        cancellation_requested: Option<Arc<std::sync::atomic::AtomicBool>>,
    ) -> Result<NetworkedWorkerRemoteDispatchOutcome, NetworkedWorkerRemoteDispatchError>;
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
    #[error(
        "remote worker dispatch timed out for request_id={request_id}; cancelled_before_dispatch={cancelled_before_dispatch}"
    )]
    Timeout { request_id: String, cancelled_before_dispatch: bool },
    #[error(
        "remote worker dispatch cancelled for request_id={request_id}; cancelled_before_dispatch={cancelled_before_dispatch}"
    )]
    Cancelled { request_id: String, cancelled_before_dispatch: bool },
    #[error(
        "remote worker dispatch state transition failed for request_id={request_id}: {message}"
    )]
    StateTransition { request_id: String, message: String },
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
    async fn receive_committed_result(
        receiver: &mut CapabilityExecutionReceiver,
    ) -> Result<CapabilityExecutionNotification, NetworkedWorkerRemoteDispatchError> {
        Ok(receiver.recv().await)
    }

    /// Builds a dispatcher over the daemon's paired-node runtime.
    #[must_use]
    pub(crate) fn new(node_runtime: Arc<NodeRuntimeState>) -> Self {
        Self {
            node_runtime,
            dispatch_timeout_ms: NETWORKED_WORKER_NODE_CAPABILITY_TIMEOUT_MS,
            max_payload_bytes: NETWORKED_WORKER_NODE_CAPABILITY_MAX_PAYLOAD_BYTES,
        }
    }

    /// Builds a dispatcher with a deterministic timeout for bounded-settlement tests.
    #[cfg(test)]
    #[must_use]
    pub(crate) fn with_dispatch_timeout(
        node_runtime: Arc<NodeRuntimeState>,
        dispatch_timeout_ms: u64,
    ) -> Self {
        Self {
            node_runtime,
            dispatch_timeout_ms: dispatch_timeout_ms.max(1),
            max_payload_bytes: NETWORKED_WORKER_NODE_CAPABILITY_MAX_PAYLOAD_BYTES,
        }
    }

    fn required_capability(request: &WorkerRemoteToolRequestEnvelope) -> String {
        request.tool_kind.required_capability()
    }
}

#[async_trait::async_trait]
impl NetworkedWorkerRemoteDispatcher for NodeRuntimeNetworkedWorkerDispatcher {
    fn preflight_worker(
        &self,
        worker_id: &str,
        required_capabilities: &[String],
    ) -> Result<(), NetworkedWorkerRemoteDispatchError> {
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
        ensure_node_is_ready_for_remote_worker(&node, required_capabilities)
    }

    async fn dispatch_remote_tool(
        &self,
        runtime_state: &Arc<GatewayRuntimeState>,
        host_context: NetworkedWorkerHostReceiptContext,
        request: WorkerRemoteToolRequestEnvelope,
        cancellation_requested: Option<Arc<std::sync::atomic::AtomicBool>>,
    ) -> Result<NetworkedWorkerRemoteDispatchOutcome, NetworkedWorkerRemoteDispatchError> {
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
        ensure_node_is_ready_for_remote_worker(&node, &request.lease.required_capabilities)?;

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
        let request_id = Ulid::generate().to_string();
        let claim_request = crate::journal::NetworkedWorkerDispatchClaimCreateRequest {
            remote_request_id: request.request_id.clone(),
            node_request_id: request_id.clone(),
            worker_id: request.lease.worker_id.clone(),
            lease_id: request.lease.lease_id.clone(),
            session_id: request.lease.session_id.clone(),
            run_id: request.lease.run_id.clone(),
            run_generation: request.lease.run_generation,
            lease_expires_at_unix_ms: request.lease.expires_at_unix_ms,
            capability: capability.clone(),
            request_sha256: sha256_hex(payload.as_slice()),
        };
        let claim = runtime_state.create_networked_worker_dispatch_claim(&claim_request).map_err(
            |error| NetworkedWorkerRemoteDispatchError::RequestRejected(error.to_string()),
        )?;
        let receiver = match self.node_runtime.enqueue_claimed_capability_request(
            worker_id,
            capability.as_str(),
            payload,
            self.max_payload_bytes,
            &claim,
            NetworkedWorkerResultCommitContext { request: request.clone(), host: host_context },
        ) {
            Ok(receiver) => receiver,
            Err(error) => {
                runtime_state
                    .cancel_networked_worker_dispatch(
                        claim.remote_request_id.as_str(),
                        claim.node_request_id.as_str(),
                        "worker.dispatch.local_enqueue_failed",
                        current_unix_ms(),
                    )
                    .map_err(|cancel_error| {
                        NetworkedWorkerRemoteDispatchError::StateTransition {
                            request_id: claim.node_request_id.clone(),
                            message: format!(
                                "local enqueue failed and durable cancellation could not be confirmed: {}",
                                cancel_error.message()
                            ),
                        }
                    })?;
                return Err(NetworkedWorkerRemoteDispatchError::RequestRejected(error.to_string()));
            }
        };

        let mut receiver = Some(receiver);
        let wait_for_result = async {
            loop {
                if cancellation_requested
                    .as_ref()
                    .is_some_and(|flag| flag.load(std::sync::atomic::Ordering::Acquire))
                {
                    let stop_outcome = self
                        .node_runtime
                        .stop_capability_request(
                            request_id.as_str(),
                            "networked worker dispatch cancelled before completion",
                            Some(runtime_state.as_ref()),
                        )
                        .map_err(|error| NetworkedWorkerRemoteDispatchError::StateTransition {
                            request_id: request_id.clone(),
                            message: error.message().to_owned(),
                        })?;
                    match stop_outcome {
                        CapabilityRequestStopOutcome::CancelledBeforeRelease => {
                            return Err(NetworkedWorkerRemoteDispatchError::Cancelled {
                                request_id: request_id.clone(),
                                cancelled_before_dispatch: true,
                            });
                        }
                        CapabilityRequestStopOutcome::ReleasedReconciliationOwned => {
                            match self
                                .node_runtime
                                .mark_capability_timeout(request_id.as_str())
                                .map_err(|error| {
                                    NetworkedWorkerRemoteDispatchError::StateTransition {
                                        request_id: request_id.clone(),
                                        message: error.message().to_owned(),
                                    }
                                })? {
                                CapabilityRequestTimeoutOutcome::MarkedTimedOut
                                | CapabilityRequestTimeoutOutcome::AlreadyTerminal => {
                                    return Ok(NetworkedWorkerRemoteDispatchOutcome::Released(
                                        NetworkedWorkerReleasedDispatch {
                                            node_request_id: request_id.clone(),
                                            receiver: receiver.take().ok_or_else(|| {
                                                NetworkedWorkerRemoteDispatchError::StateTransition {
                                                    request_id: request_id.clone(),
                                                    message: "capability result owner was already transferred"
                                                        .to_owned(),
                                                }
                                            })?,
                                            reason:
                                                NetworkedWorkerReleasedDispatchReason::Cancelled,
                                        },
                                    ));
                                }
                                CapabilityRequestTimeoutOutcome::ResultCommitted => {
                                    let notification = Self::receive_committed_result(
                                        receiver.as_mut().ok_or_else(|| {
                                            NetworkedWorkerRemoteDispatchError::StateTransition {
                                                request_id: request_id.clone(),
                                                message: "capability result owner was already transferred"
                                                    .to_owned(),
                                            }
                                        })?,
                                    )
                                    .await?;
                                    return capability_notification_to_dispatch_outcome(
                                        notification,
                                    );
                                }
                                CapabilityRequestTimeoutOutcome::Missing => {
                                    return Err(
                                        NetworkedWorkerRemoteDispatchError::StateTransition {
                                            request_id: request_id.clone(),
                                            message: "capability request disappeared while recording cancellation"
                                                .to_owned(),
                                        },
                                    );
                                }
                            }
                        }
                        CapabilityRequestStopOutcome::AlreadyTerminal => {
                            let notification = Self::receive_committed_result(
                                receiver.as_mut().ok_or_else(|| {
                                    NetworkedWorkerRemoteDispatchError::StateTransition {
                                        request_id: request_id.clone(),
                                        message: "capability result owner was already transferred"
                                            .to_owned(),
                                    }
                                })?,
                            )
                            .await?;
                            return capability_notification_to_dispatch_outcome(notification);
                        }
                        CapabilityRequestStopOutcome::Missing => {
                            return Err(NetworkedWorkerRemoteDispatchError::StateTransition {
                                request_id: request_id.clone(),
                                message: "capability request disappeared during cancellation"
                                    .to_owned(),
                            });
                        }
                    }
                }
                let active_receiver = receiver.as_mut().ok_or_else(|| {
                    NetworkedWorkerRemoteDispatchError::StateTransition {
                        request_id: request_id.clone(),
                        message: "capability result owner was already transferred".to_owned(),
                    }
                })?;
                if let Ok(result) =
                    tokio::time::timeout(Duration::from_millis(50), active_receiver.recv()).await
                {
                    return capability_notification_to_dispatch_outcome(result);
                }
            }
        };
        match tokio::time::timeout(Duration::from_millis(timeout_ms), wait_for_result).await {
            Ok(result) => return result,
            Err(_) => {
                let stop_outcome = self
                    .node_runtime
                    .stop_capability_request(
                        request_id.as_str(),
                        "networked worker dispatch timed out before completion",
                        Some(runtime_state.as_ref()),
                    )
                    .map_err(|error| NetworkedWorkerRemoteDispatchError::StateTransition {
                        request_id: request_id.clone(),
                        message: error.message().to_owned(),
                    })?;
                match stop_outcome {
                    CapabilityRequestStopOutcome::CancelledBeforeRelease => {
                        return Err(NetworkedWorkerRemoteDispatchError::Timeout {
                            request_id,
                            cancelled_before_dispatch: true,
                        });
                    }
                    CapabilityRequestStopOutcome::ReleasedReconciliationOwned => {
                        match self
                            .node_runtime
                            .mark_capability_timeout(request_id.as_str())
                            .map_err(|error| {
                                NetworkedWorkerRemoteDispatchError::StateTransition {
                                    request_id: request_id.clone(),
                                    message: error.message().to_owned(),
                                }
                            })? {
                            CapabilityRequestTimeoutOutcome::MarkedTimedOut
                            | CapabilityRequestTimeoutOutcome::AlreadyTerminal => {
                                return Ok(NetworkedWorkerRemoteDispatchOutcome::Released(
                                    NetworkedWorkerReleasedDispatch {
                                        node_request_id: request_id.clone(),
                                        receiver: receiver.take().ok_or_else(|| {
                                            NetworkedWorkerRemoteDispatchError::StateTransition {
                                                request_id: request_id.clone(),
                                                message: "capability result owner was already transferred"
                                                    .to_owned(),
                                            }
                                        })?,
                                        reason: NetworkedWorkerReleasedDispatchReason::TimedOut,
                                    },
                                ));
                            }
                            CapabilityRequestTimeoutOutcome::ResultCommitted => {
                                return capability_notification_to_dispatch_outcome(
                                    Self::receive_committed_result(
                                        receiver.as_mut().ok_or_else(|| {
                                            NetworkedWorkerRemoteDispatchError::StateTransition {
                                                request_id: request_id.clone(),
                                                message: "capability result owner was already transferred"
                                                    .to_owned(),
                                            }
                                        })?,
                                    )
                                    .await?,
                                );
                            }
                            CapabilityRequestTimeoutOutcome::Missing => {
                                return Err(NetworkedWorkerRemoteDispatchError::StateTransition {
                                    request_id,
                                    message:
                                        "capability request disappeared while recording timeout"
                                            .to_owned(),
                                });
                            }
                        }
                    }
                    CapabilityRequestStopOutcome::AlreadyTerminal => {
                        return capability_notification_to_dispatch_outcome(
                            Self::receive_committed_result(receiver.as_mut().ok_or_else(|| {
                                NetworkedWorkerRemoteDispatchError::StateTransition {
                                    request_id: request_id.clone(),
                                    message: "capability result owner was already transferred"
                                        .to_owned(),
                                }
                            })?)
                            .await?,
                        );
                    }
                    CapabilityRequestStopOutcome::Missing => {
                        return Err(NetworkedWorkerRemoteDispatchError::StateTransition {
                            request_id,
                            message: "capability request disappeared during timeout handling"
                                .to_owned(),
                        });
                    }
                }
            }
        }
    }
}

fn capability_notification_to_dispatch_outcome(
    notification: CapabilityExecutionNotification,
) -> Result<NetworkedWorkerRemoteDispatchOutcome, NetworkedWorkerRemoteDispatchError> {
    let delivery_attempt_id = notification.delivery_attempt_id.ok_or_else(|| {
        NetworkedWorkerRemoteDispatchError::MalformedResult(
            "networked worker result notification missing delivery attempt identity".to_owned(),
        )
    })?;
    let run_generation = notification.run_generation.ok_or_else(|| {
        NetworkedWorkerRemoteDispatchError::MalformedResult(
            "networked worker result notification missing run generation".to_owned(),
        )
    })?;
    let result = remote_result_from_node_capability_result(notification.result)?;
    if result.run_generation != run_generation {
        return Err(NetworkedWorkerRemoteDispatchError::MalformedResult(
            "networked worker result generation does not match the authenticated callback"
                .to_owned(),
        ));
    }
    Ok(NetworkedWorkerRemoteDispatchOutcome::Completed(Box::new(
        NetworkedWorkerRemoteDispatchResult {
            result,
            delivery_attempt_id: Some(delivery_attempt_id),
            observed_at_unix_ms: notification.observed_at_unix_ms,
        },
    )))
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

/// Executes `tool_name` under a runtime-owned networked-worker lifecycle.
///
/// The detached owner continues validation, cleanup, and exact claim settlement if the requesting
/// foreground future disappears. Raw request and result data remain process-local and are dropped
/// after settlement; durable evidence remains bounded and digest-only.
pub(crate) async fn execute_networked_worker_tool(
    runtime_state: &Arc<GatewayRuntimeState>,
    context: ToolRuntimeExecutionContext<'_>,
    proposal_id: &str,
    tool_name: &str,
    input_json: &[u8],
    cancellation_requested: Option<Arc<std::sync::atomic::AtomicBool>>,
) -> ToolExecutionOutcome {
    let runtime_state = Arc::clone(runtime_state);
    let context = OwnedToolRuntimeExecutionContext::from_borrowed(context);
    let proposal_id = proposal_id.to_owned();
    let tool_name = tool_name.to_owned();
    let input_json = input_json.to_vec();
    let failure_proposal_id = proposal_id.clone();
    let failure_tool_name = tool_name.clone();
    let failure_input_json = input_json.clone();
    let (outcome_sender, outcome_receiver) = tokio::sync::oneshot::channel();
    tokio::spawn(async move {
        let outcome = execute_networked_worker_tool_owned(
            &runtime_state,
            context.as_borrowed(),
            proposal_id.as_str(),
            tool_name.as_str(),
            input_json.as_slice(),
            cancellation_requested,
        )
        .await;
        let _ = outcome_sender.send(outcome);
    });
    outcome_receiver.await.unwrap_or_else(|_| {
        networked_worker_failure_outcome(
            failure_proposal_id.as_str(),
            failure_tool_name.as_str(),
            failure_input_json.as_slice(),
            "networked worker lifecycle owner terminated before reporting an outcome".to_owned(),
            "networked_worker_lifecycle_owner_failed",
        )
    })
}

async fn execute_networked_worker_tool_owned(
    runtime_state: &Arc<GatewayRuntimeState>,
    context: ToolRuntimeExecutionContext<'_>,
    proposal_id: &str,
    tool_name: &str,
    input_json: &[u8],
    cancellation_requested: Option<Arc<std::sync::atomic::AtomicBool>>,
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
    let is_computer_use = matches!(
        WorkerRemoteToolKind::from_tool_name(tool_name),
        Some(WorkerRemoteToolKind::ComputerUse)
    );
    if is_computer_use && !runtime_state.config.feature_rollouts.computer_use.enabled {
        return networked_worker_failure_outcome(
            proposal_id,
            tool_name,
            input_json,
            "computer use is disabled; set feature_rollouts.computer_use=true together with the networked-worker rollout"
                .to_owned(),
            "computer_use_rollout_disabled",
        );
    }
    let (generation_session_id, run_generation) =
        match runtime_state.runtime_generation_for_tool_blocking(context.run_id) {
            Ok(Some(authority)) => authority,
            Ok(None) => {
                return networked_worker_failure_outcome(
                    proposal_id,
                    tool_name,
                    input_json,
                    "networked worker dispatch requires an active run generation".to_owned(),
                    "networked_worker_stale_generation",
                );
            }
            Err(error) => {
                return networked_worker_failure_outcome(
                    proposal_id,
                    tool_name,
                    input_json,
                    format!("networked worker generation lookup failed: {}", error.message()),
                    "networked_worker_stale_generation",
                );
            }
        };
    let generation_session_id = match RuntimeSessionId::parse(generation_session_id.as_str()) {
        Ok(session_id) => session_id,
        Err(error) => {
            return networked_worker_failure_outcome(
                proposal_id,
                tool_name,
                input_json,
                format!("networked worker session identity is invalid: {error}"),
                "networked_worker_stale_generation",
            );
        }
    };
    let run_identity = match RuntimeRunId::parse(context.run_id) {
        Ok(run_id) => run_id,
        Err(error) => {
            return networked_worker_failure_outcome(
                proposal_id,
                tool_name,
                input_json,
                format!("networked worker run identity is invalid: {error}"),
                "networked_worker_stale_generation",
            );
        }
    };
    if generation_session_id.as_str() != context.session_id {
        return networked_worker_failure_outcome(
            proposal_id,
            tool_name,
            input_json,
            "networked worker run generation belongs to a different session".to_owned(),
            "networked_worker_stale_generation",
        );
    }

    let computer_use_approval = if is_computer_use {
        match resolve_computer_use_host_approval(runtime_state, context, proposal_id, input_json)
            .await
        {
            Ok(approval) => Some(approval),
            Err(error) => {
                return networked_worker_failure_outcome(
                    proposal_id,
                    tool_name,
                    input_json,
                    error,
                    "computer_use_approval_missing",
                );
            }
        }
    } else {
        None
    };

    let request = match build_worker_lease_request(
        runtime_state,
        context,
        proposal_id,
        tool_name,
        input_json,
    ) {
        Ok(request) => request,
        Err(error) => {
            return networked_worker_failure_outcome(
                proposal_id,
                tool_name,
                input_json,
                error,
                "networked_worker_workspace_scope_invalid",
            );
        }
    };
    let lease = match runtime_state
        .assign_next_networked_worker_lease_for_run(
            request,
            generation_session_id.as_str(),
            run_identity.as_str(),
            run_generation,
        )
        .await
    {
        Ok(crate::gateway::NetworkedWorkerLeaseAssignmentOutcome::Assigned { lease }) => *lease,
        Ok(crate::gateway::NetworkedWorkerLeaseAssignmentOutcome::TransportRejected { reason }) => {
            return networked_worker_failure_outcome(
                proposal_id,
                tool_name,
                input_json,
                format!("networked worker remote preflight failed: {reason}"),
                "networked_worker_remote_unavailable",
            );
        }
        Ok(crate::gateway::NetworkedWorkerLeaseAssignmentOutcome::StaleSuppressed) => {
            return networked_worker_failure_outcome(
                proposal_id,
                tool_name,
                input_json,
                "networked worker lease assignment was suppressed after run supersession"
                    .to_owned(),
                "networked_worker_stale_generation",
            );
        }
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
    let health_authority = match runtime_state
        .admit_managed_runtime_health(ManagedRuntimeHealthFamily::Worker, lease.worker_id.as_str())
    {
        Ok(authority) => authority,
        Err(error) => {
            if let Err(cleanup_error) =
                release_unstarted_networked_worker_lease(runtime_state, &lease).await
            {
                return networked_worker_failure_outcome(
                    proposal_id,
                    tool_name,
                    input_json,
                    format!(
                        "networked worker health admission and cleanup failed: {}; {}",
                        error.message(),
                        cleanup_error.message()
                    ),
                    "networked_worker_cleanup_failed",
                );
            }
            return networked_worker_failure_outcome(
                proposal_id,
                tool_name,
                input_json,
                format!("networked worker health admission failed: {}", error.message()),
                "networked_worker_health_blocked",
            );
        }
    };

    let worker_attestation =
        match runtime_state.networked_worker_attestation(lease.worker_id.as_str()) {
            Some(attestation) => attestation,
            None => {
                runtime_state.record_managed_runtime_health_observation_for_run(
                    &health_authority,
                    &generation_session_id,
                    &run_identity,
                    run_generation,
                    false,
                    "runtime.health.worker_attestation_missing",
                );
                if let Err(error) =
                    record_unverified_networked_worker_cleanup(runtime_state, &lease).await
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
    let remote_input_json = if is_computer_use {
        match build_host_bound_computer_use_task(
            input_json,
            proposal_id,
            &lease,
            run_generation,
            worker_attestation.image_digest_sha256.as_str(),
            computer_use_approval.as_ref(),
        ) {
            Ok(input) => input,
            Err(error) => {
                runtime_state.record_managed_runtime_health_observation_for_run(
                    &health_authority,
                    &generation_session_id,
                    &run_identity,
                    run_generation,
                    false,
                    "runtime.health.computer_use_input_invalid",
                );
                if let Err(cleanup_error) =
                    record_unverified_networked_worker_cleanup(runtime_state, &lease).await
                {
                    return networked_worker_failure_outcome(
                        proposal_id,
                        tool_name,
                        input_json,
                        format!(
                            "computer-use input and cleanup failed: {error}; {}",
                            cleanup_error.message()
                        ),
                        "networked_worker_cleanup_failed",
                    );
                }
                return networked_worker_failure_outcome(
                    proposal_id,
                    tool_name,
                    input_json,
                    error,
                    "computer_use_input_invalid",
                );
            }
        }
    } else {
        input_json.to_vec()
    };
    let process_executable_allowlist = match networked_worker_process_executable_authority(
        tool_name,
        remote_input_json.as_slice(),
        runtime_state.config.tool_call.process_runner.allowed_executables.as_slice(),
        runtime_state.config.tool_call.process_runner.allow_interpreters,
    ) {
        Ok(authority) => authority,
        Err(error) => {
            runtime_state.record_managed_runtime_health_observation_for_run(
                &health_authority,
                &generation_session_id,
                &run_identity,
                run_generation,
                false,
                "runtime.health.worker_process_authority_invalid",
            );
            if let Err(cleanup_error) =
                record_unverified_networked_worker_cleanup(runtime_state, &lease).await
            {
                return networked_worker_failure_outcome(
                    proposal_id,
                    tool_name,
                    input_json,
                    format!(
                        "networked worker process authority and cleanup failed: {error}; {}",
                        cleanup_error.message()
                    ),
                    "networked_worker_cleanup_failed",
                );
            }
            return networked_worker_failure_outcome(
                proposal_id,
                tool_name,
                input_json,
                error,
                "networked_worker_process_authority_invalid",
            );
        }
    };
    let workspace_transfer =
        match prepare_networked_worker_workspace_transfer(&lease, tool_name, input_json).await {
            Ok(transfer) => transfer,
            Err(error) => {
                runtime_state.record_managed_runtime_health_observation_for_run(
                    &health_authority,
                    &generation_session_id,
                    &run_identity,
                    run_generation,
                    false,
                    "runtime.health.worker_workspace_transfer_invalid",
                );
                if let Err(cleanup_error) =
                    record_unverified_networked_worker_cleanup(runtime_state, &lease).await
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
                    "networked_worker_workspace_transfer_failed",
                );
            }
        };
    let remote_request = match build_worker_remote_tool_request(NetworkedWorkerTaskRequest {
        proposal_id,
        tool_name,
        input_json: remote_input_json.as_slice(),
        lease: &lease,
        worker_attestation: &worker_attestation,
        session_id: generation_session_id.as_str(),
        run_generation,
        process_executable_allowlist,
        workspace_transfer,
    }) {
        Ok(request) => request,
        Err(error) => {
            runtime_state.record_managed_runtime_health_observation_for_run(
                &health_authority,
                &generation_session_id,
                &run_identity,
                run_generation,
                false,
                "runtime.health.worker_request_invalid",
            );
            if let Err(cleanup_error) =
                record_unverified_networked_worker_cleanup(runtime_state, &lease).await
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
    let dispatch_outcome = match dispatch_networked_worker_remote_tool(
        runtime_state,
        context,
        &remote_request,
        cancellation_requested,
    )
    .await
    {
        Ok(result) => result,
        Err(error) => {
            runtime_state.record_managed_runtime_health_observation_for_run(
                &health_authority,
                &generation_session_id,
                &run_identity,
                run_generation,
                false,
                "runtime.health.worker_dispatch_unavailable",
            );
            if let Err(cleanup_error) =
                record_unverified_networked_worker_cleanup(runtime_state, &lease).await
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
    let dispatch_result = match dispatch_outcome {
        NetworkedWorkerRemoteDispatchOutcome::Completed(result) => *result,
        NetworkedWorkerRemoteDispatchOutcome::Released(released) => {
            runtime_state.record_managed_runtime_health_observation_for_run(
                &health_authority,
                &generation_session_id,
                &run_identity,
                run_generation,
                false,
                "runtime.health.worker_dispatch_released",
            );
            let failure_text = released.reason.failure_text().to_owned();
            if let Err(cleanup_error) =
                record_unverified_networked_worker_cleanup(runtime_state, &lease).await
            {
                return networked_worker_failure_outcome(
                    proposal_id,
                    tool_name,
                    input_json,
                    format!("networked worker cleanup failed: {}", cleanup_error.message()),
                    "networked_worker_cleanup_failed",
                );
            }
            spawn_networked_worker_late_result_owner(
                Arc::clone(runtime_state),
                remote_request.clone(),
                lease.clone(),
                released,
                health_authority,
            );
            return networked_worker_failure_outcome(
                proposal_id,
                tool_name,
                input_json,
                failure_text,
                "networked_worker_remote_unavailable",
            );
        }
    };
    let NetworkedWorkerRemoteDispatchResult {
        result: remote_result,
        delivery_attempt_id,
        observed_at_unix_ms,
    } = dispatch_result;

    let validation = remote_result.validate_against_request(&remote_request, observed_at_unix_ms);
    if let Err(error) = &validation {
        runtime_state.record_managed_runtime_health_observation_for_run(
            &health_authority,
            &generation_session_id,
            &run_identity,
            run_generation,
            false,
            "runtime.health.worker_result_invalid",
        );
        if matches!(error, WorkerRemoteToolContractError::CleanupGap { .. }) {
            let cleanup_report = remote_result.cleanup_report.clone();
            if let Err(cleanup_error) = runtime_state
                .complete_networked_worker_lease(
                    lease.worker_id.as_str(),
                    lease.identity(),
                    cleanup_report.clone(),
                )
                .await
            {
                if cleanup_error.code() != tonic::Code::FailedPrecondition
                    || cleanup_error.message()
                        != "networked worker cleanup did not remove all scoped data"
                {
                    return networked_worker_failure_outcome(
                        proposal_id,
                        tool_name,
                        input_json,
                        format!("networked worker cleanup failed: {}", cleanup_error.message()),
                        "networked_worker_cleanup_failed",
                    );
                }
            }
            return networked_worker_cleanup_failure_outcome(&remote_request, remote_result);
        }
        if let Err(cleanup_error) =
            record_unverified_networked_worker_cleanup(runtime_state, &lease).await
        {
            return networked_worker_failure_outcome(
                proposal_id,
                tool_name,
                input_json,
                format!("networked worker cleanup failed: {}", cleanup_error.message()),
                "networked_worker_cleanup_failed",
            );
        }
        let sandbox_enforcement = if matches!(
            error,
            WorkerRemoteToolContractError::DigestMismatch { field: "output_json_sha256" }
        ) {
            "networked_worker_remote_digest_mismatch"
        } else {
            "networked_worker_remote_fail_closed"
        };
        return networked_worker_failure_outcome(
            proposal_id,
            tool_name,
            input_json,
            format!("networked worker remote execution failed: {error}"),
            sandbox_enforcement,
        );
    }

    let validated_result_sha256 =
        match remote_result.validated_receipt_sha256(&remote_request, observed_at_unix_ms) {
            Ok(digest) => digest,
            Err(error) => {
                runtime_state.record_managed_runtime_health_observation_for_run(
                    &health_authority,
                    &generation_session_id,
                    &run_identity,
                    run_generation,
                    false,
                    "runtime.health.worker_receipt_invalid",
                );
                if let Err(cleanup_error) =
                    record_unverified_networked_worker_cleanup(runtime_state, &lease).await
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
                    format!("networked worker result receipt failed: {error}"),
                    "networked_worker_result_receipt_failed",
                );
            }
        };
    if let Err(error) = validate_networked_worker_canonical_outcome(&remote_request, &remote_result)
    {
        runtime_state.record_managed_runtime_health_observation_for_run(
            &health_authority,
            &generation_session_id,
            &run_identity,
            run_generation,
            false,
            "runtime.health.worker_canonical_outcome_invalid",
        );
        if let Err(cleanup_error) =
            record_unverified_networked_worker_cleanup(runtime_state, &lease).await
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
            format!("networked worker canonical outcome failed validation: {error}"),
            "networked_worker_canonical_protocol_failed",
        );
    }
    let computer_use_projection =
        if matches!(remote_request.tool_kind, WorkerRemoteToolKind::ComputerUse) {
            match persist_computer_use_evidence(
                runtime_state,
                context,
                proposal_id,
                &remote_request,
                &remote_result,
            )
            .await
            {
                Ok(projection) => Some(projection),
                Err(error) => {
                    runtime_state.record_managed_runtime_health_observation_for_run(
                        &health_authority,
                        &generation_session_id,
                        &run_identity,
                        run_generation,
                        false,
                        "runtime.health.computer_use_evidence_invalid",
                    );
                    if let Err(cleanup_error) =
                        record_unverified_networked_worker_cleanup(runtime_state, &lease).await
                    {
                        return networked_worker_failure_outcome(
                            proposal_id,
                            tool_name,
                            input_json,
                            format!(
                                "computer-use evidence and cleanup failed: {error}; {}",
                                cleanup_error.message()
                            ),
                            "networked_worker_cleanup_failed",
                        );
                    }
                    return networked_worker_failure_outcome(
                        proposal_id,
                        tool_name,
                        input_json,
                        error,
                        "computer_use_evidence_invalid",
                    );
                }
            }
        } else {
            None
        };
    let receipt = crate::gateway::NetworkedWorkerArtifactReceipt {
        request_id: remote_result.request_id.clone(),
        proposal_id: proposal_id.to_owned(),
        tool_name: tool_name.to_owned(),
        principal: context.principal.to_owned(),
        device_id: context.device_id.to_owned(),
        channel: context.channel.map(ToOwned::to_owned),
        session_id: context.session_id.to_owned(),
        run_id: context.run_id.to_owned(),
        input_json_sha256: sha256_hex(input_json),
        output_json_sha256: remote_result.output_json_sha256.clone(),
        output_manifest_sha256: remote_result.output_manifest_sha256.clone(),
        validated_result_sha256,
        grant_id: lease.grant.grant_id.clone(),
        required_capabilities: lease.required_capabilities.clone(),
        workspace_scope: lease.workspace_scope.clone(),
        log_stream_id: lease.artifact_transport.log_stream_id.clone(),
        scratch_directory_id: lease.artifact_transport.scratch_directory_id.clone(),
        observed_at_unix_ms,
    };
    if let Err(error) = runtime_state
        .complete_networked_worker_result(
            lease.worker_id.as_str(),
            lease.identity(),
            remote_result.cleanup_report.clone(),
            receipt,
            Some(crate::gateway::NetworkedWorkerDispatchSettlementIdentity {
                remote_request_id: remote_request.request_id.clone(),
                delivery_attempt_id,
                session_id: remote_request.lease.session_id.clone(),
                run_generation: remote_result.run_generation,
            }),
        )
        .await
    {
        let dispatch_settled = runtime_state
            .journal_store
            .networked_worker_dispatch_claim(remote_request.request_id.as_str())
            .ok()
            .flatten()
            .is_some_and(|claim| {
                claim.state == crate::journal::NetworkedWorkerDispatchClaimState::Settled
            });
        if !dispatch_settled {
            if let Err(cleanup_error) = runtime_state
                .complete_networked_worker_lease(
                    lease.worker_id.as_str(),
                    lease.identity(),
                    remote_result.cleanup_report.clone(),
                )
                .await
            {
                return networked_worker_failure_outcome(
                    proposal_id,
                    tool_name,
                    input_json,
                    format!(
                        "networked worker result commit and cleanup failed: {}; {}",
                        error.message(),
                        cleanup_error.message()
                    ),
                    "networked_worker_cleanup_failed",
                );
            }
        }
        return networked_worker_failure_outcome(
            proposal_id,
            tool_name,
            input_json,
            format!("networked worker result commit failed: {}", error.message()),
            "networked_worker_result_commit_failed",
        );
    }
    runtime_state.record_managed_runtime_health_observation_for_run(
        &health_authority,
        &generation_session_id,
        &run_identity,
        run_generation,
        true,
        if matches!(remote_request.tool_kind, WorkerRemoteToolKind::ComputerUse) {
            "runtime.health.computer_use_dispatch_succeeded"
        } else {
            "runtime.health.worker_dispatch_succeeded"
        },
    );

    networked_worker_outcome_from_validated_remote_result_with_projection(
        &remote_request,
        remote_result,
        computer_use_projection,
        input_json,
    )
}

async fn resolve_computer_use_host_approval(
    runtime_state: &Arc<GatewayRuntimeState>,
    context: ToolRuntimeExecutionContext<'_>,
    proposal_id: &str,
    input_json: &[u8],
) -> Result<ComputerUseHostApprovalAuthority, String> {
    const TOOL_NAME: &str = "palyra.computer.use";
    if tool_metadata(TOOL_NAME).is_none()
        || !tool_requires_approval(TOOL_NAME)
        || tool_policy_capability_names(TOOL_NAME)
            != vec!["filesystem_read", "network", "secrets_read"]
    {
        return Err(
            "computer-use catalog metadata is missing or does not require the host security gate"
                .to_owned(),
        );
    }

    let snapshot = runtime_state
        .journal_snapshot_for_run(context.run_id.to_owned(), COMPUTER_USE_APPROVAL_JOURNAL_WINDOW)
        .await
        .map_err(|error| {
            format!("computer-use approval journal could not be verified: {}", error.message())
        })?;
    let mut candidate_approval_ids = Vec::new();
    for event in snapshot.events.iter().rev() {
        if event.session_id != context.session_id
            || event.run_id != context.run_id
            || event.principal != context.principal
            || event.device_id != context.device_id
            || event.channel.as_deref() != context.channel
        {
            continue;
        }
        let Ok(payload) = serde_json::from_str::<serde_json::Value>(event.payload_json.as_str())
        else {
            continue;
        };
        if payload.get("event").and_then(serde_json::Value::as_str) != Some("approval.resolved")
            || payload.get("proposal_id").and_then(serde_json::Value::as_str) != Some(proposal_id)
            || payload.get("decision").and_then(serde_json::Value::as_str) != Some("allow")
        {
            continue;
        }
        if let Some(approval_id) = payload
            .get("approval_id")
            .and_then(serde_json::Value::as_str)
            .filter(|value| !value.trim().is_empty())
        {
            candidate_approval_ids.push(approval_id.to_owned());
        }
    }

    let expected_subject = build_tool_approval_subject_id(TOOL_NAME, None, input_json);
    let now_unix_ms = current_unix_ms();
    for approval_id in candidate_approval_ids {
        let Some(record) =
            runtime_state.approval_record(approval_id.clone()).await.map_err(|error| {
                format!("computer-use approval record could not be verified: {}", error.message())
            })?
        else {
            continue;
        };
        if let Ok(authority) = validate_computer_use_host_approval_record(
            &record,
            context,
            approval_id.as_str(),
            expected_subject.as_str(),
            now_unix_ms,
        ) {
            return Ok(authority);
        }
    }
    Err("computer use requires a durable allowed host approval for this exact tool proposal"
        .to_owned())
}

fn validate_computer_use_host_approval_record(
    record: &ApprovalRecord,
    context: ToolRuntimeExecutionContext<'_>,
    approval_id: &str,
    expected_subject: &str,
    now_unix_ms: i64,
) -> Result<ComputerUseHostApprovalAuthority, String> {
    const TOOL_NAME: &str = "palyra.computer.use";
    let subject_is_exact_or_skill_scoped = record.subject_id == expected_subject
        || record
            .subject_id
            .strip_prefix(expected_subject)
            .is_some_and(|suffix| suffix.starts_with("|skill:") && suffix.len() > 7);
    let details = serde_json::from_str::<serde_json::Value>(record.prompt.details_json.as_str())
        .map_err(|_| "computer-use approval details are invalid".to_owned())?;
    let deny_is_default = record
        .prompt
        .options
        .iter()
        .any(|option| option.default_selected && option.option_id.starts_with("deny"));
    let allow_is_default = record
        .prompt
        .options
        .iter()
        .any(|option| option.default_selected && option.option_id.starts_with("allow"));
    if record.approval_id != approval_id
        || record.session_id != context.session_id
        || record.run_id != context.run_id
        || record.principal != context.principal
        || record.device_id != context.device_id
        || record.channel.as_deref() != context.channel
        || record.subject_type != ApprovalSubjectType::Tool
        || !subject_is_exact_or_skill_scoped
        || record.prompt.subject_id != record.subject_id
        || record.decision != Some(ApprovalDecision::Allow)
        || record.resolved_at_unix_ms.is_none_or(|resolved| resolved > now_unix_ms)
        || record.requested_at_unix_ms > now_unix_ms
        || details.get("tool_name").and_then(serde_json::Value::as_str) != Some(TOOL_NAME)
        || details.get("subject_id").and_then(serde_json::Value::as_str)
            != Some(record.subject_id.as_str())
        || details
            .pointer("/input_json/permission_request/source")
            .and_then(serde_json::Value::as_str)
            != Some("tool_proposal")
        || details
            .pointer("/input_json/permission_request/tool_name")
            .and_then(serde_json::Value::as_str)
            != Some(TOOL_NAME)
        || details
            .pointer("/input_json/permission_request/subject_id")
            .and_then(serde_json::Value::as_str)
            != Some(record.subject_id.as_str())
        || details
            .pointer("/input_json/permission_request/requested_scope")
            .and_then(serde_json::Value::as_str)
            != Some("single_tool_call")
        || details
            .pointer("/input_json/permission_request/requester/kind")
            .and_then(serde_json::Value::as_str)
            != Some("host_approval_relay")
        || details
            .pointer("/input_json/permission_request/execution_backend/resolved")
            .and_then(serde_json::Value::as_str)
            != Some("networked_worker")
        || details
            .pointer("/input_json/permission_request/execution_backend/approval_required")
            .and_then(serde_json::Value::as_bool)
            != Some(true)
        || record.policy_snapshot.policy_id != APPROVAL_POLICY_ID
        || record.policy_snapshot.evaluation_summary
            != "action=tool.execute resource=tool:palyra.computer.use approval_required=true deny_by_default=true"
        || !deny_is_default
        || allow_is_default
    {
        return Err(
            "computer-use approval record is not bound to the standard sensitive-tool host gate"
                .to_owned(),
        );
    }
    let expires_at_unix_ms = match record.decision_scope {
        Some(ApprovalDecisionScope::Once | ApprovalDecisionScope::Session) => None,
        Some(ApprovalDecisionScope::Timeboxed) => {
            let ttl_ms = record
                .decision_scope_ttl_ms
                .filter(|ttl_ms| *ttl_ms > 0)
                .ok_or_else(|| "computer-use timeboxed approval omitted its TTL".to_owned())?;
            let expiry = record.updated_at_unix_ms.saturating_add(ttl_ms);
            if expiry <= now_unix_ms {
                return Err("computer-use host approval has expired".to_owned());
            }
            Some(expiry)
        }
        None => return Err("computer-use approval has no decision scope".to_owned()),
    };
    Ok(ComputerUseHostApprovalAuthority { approval_id: approval_id.to_owned(), expires_at_unix_ms })
}

fn build_host_bound_computer_use_task(
    input_json: &[u8],
    proposal_id: &str,
    lease: &WorkerLease,
    run_generation: RuntimeGeneration,
    isolation_attestation_sha256: &str,
    approval_authority: Option<&ComputerUseHostApprovalAuthority>,
) -> Result<Vec<u8>, String> {
    let input = serde_json::from_slice::<ComputerUseToolInput>(input_json)
        .map_err(|error| format!("computer-use input must match its strict schema: {error}"))?;
    input.validate().map_err(|error| error.to_string())?;
    let mut filesystem_roots = input
        .actions
        .iter()
        .filter_map(|requested| match &requested.action {
            ComputerUseAction::FileChooser { path } => Some(path.clone()),
            _ => None,
        })
        .collect::<Vec<_>>();
    filesystem_roots.sort();
    filesystem_roots.dedup();
    let approval_authority = approval_authority.ok_or_else(|| {
        "computer-use host approval authority is required before worker dispatch".to_owned()
    })?;
    let approval = ComputerUseApproval {
        approval_id: approval_authority.approval_id.clone(),
        task_id: proposal_id.to_owned(),
        run_generation,
        approved_risks: vec![
            ComputerUseRiskClass::CredentialEntry,
            ComputerUseRiskClass::Payment,
            ComputerUseRiskClass::DestructiveFileOperation,
            ComputerUseRiskClass::PrivilegePrompt,
        ],
        expires_at_unix_ms: approval_authority
            .expires_at_unix_ms
            .map_or(lease.expires_at_unix_ms, |expiry| expiry.min(lease.expires_at_unix_ms)),
    };
    let contract = ComputerUseTaskContract {
        v: input.v,
        initial_ui_text: input.initial_ui_text,
        profile: ComputerUseCapabilityProfile {
            capability: "computer.use".to_owned(),
            backend: ComputerUseBackendKind::IsolatedVirtualDesktop,
            isolation_attestation_sha256: isolation_attestation_sha256.to_owned(),
            host_desktop_access: false,
            filesystem_roots,
            network_hosts: Vec::new(),
            clipboard_read: false,
            clipboard_write: false,
            max_actions: u32::try_from(input.actions.len()).unwrap_or(u32::MAX),
            max_wall_clock_ms: COMPUTER_USE_MAX_WALL_CLOCK_MS,
            max_wait_ms: COMPUTER_USE_MAX_WAIT_MS,
            viewport_width: COMPUTER_USE_VIEWPORT_WIDTH,
            viewport_height: COMPUTER_USE_VIEWPORT_HEIGHT,
            max_screenshot_bytes: COMPUTER_USE_MAX_SCREENSHOT_BYTES,
        },
        actions: input.actions,
        approval: Some(approval),
    };
    contract.profile.validate().map_err(|error| error.to_string())?;
    serde_json::to_vec(&contract)
        .map_err(|error| format!("failed to encode host-bound computer-use task: {error}"))
}

async fn persist_computer_use_evidence(
    runtime_state: &Arc<GatewayRuntimeState>,
    context: ToolRuntimeExecutionContext<'_>,
    proposal_id: &str,
    request: &WorkerRemoteToolRequestEnvelope,
    result: &WorkerRemoteToolResultEnvelope,
) -> Result<Vec<u8>, String> {
    let protocol = request
        .canonical_protocol
        .as_ref()
        .ok_or_else(|| "computer-use result omitted canonical protocol".to_owned())?;
    let contract = serde_json::from_str::<ComputerUseTaskContract>(request.input_json.as_str())
        .map_err(|error| format!("computer-use canonical task is invalid: {error}"))?;
    let output = serde_json::from_str::<ComputerUseWorkerOutput>(result.output_json.as_str())
        .map_err(|error| format!("computer-use worker output is invalid: {error}"))?;
    output
        .validate_against(&protocol.task, &contract.profile)
        .map_err(|error| error.to_string())?;
    if output.succeeded != result.success {
        return Err("computer-use terminal result disagrees with its action evidence".to_owned());
    }

    let mut screenshot_artifacts = Vec::<ToolResultArtifactRef>::with_capacity(2);
    for screenshot in &output.screenshots {
        let bytes = BASE64_STANDARD
            .decode(screenshot.bytes_base64.as_bytes())
            .map_err(|_| "computer-use screenshot base64 is invalid".to_owned())?;
        let artifact = runtime_state
            .create_tool_result_artifact(ToolResultArtifactCreateRequest {
                artifact_id: Ulid::generate().to_string(),
                session_id: context.session_id.to_owned(),
                run_id: context.run_id.to_owned(),
                proposal_id: proposal_id.to_owned(),
                tool_name: request.tool_name.clone(),
                mime_type: screenshot.artifact.media_type.clone(),
                sensitivity: ToolResultSensitivity::ApprovalRiskData,
                retention: ArtifactRetentionPolicy::keep(),
                redacted_preview: format!(
                    "redacted computer-use screenshot generation={} sha256={}",
                    screenshot.artifact.observation_generation, screenshot.artifact.sha256
                ),
                content: bytes,
            })
            .await
            .map_err(|error| {
                format!("failed to persist computer-use screenshot: {}", error.message())
            })?;
        if artifact.digest_sha256 != screenshot.artifact.sha256
            || artifact.size_bytes != screenshot.artifact.size_bytes
        {
            return Err(
                "persisted computer-use screenshot does not match worker integrity metadata"
                    .to_owned(),
            );
        }
        screenshot_artifacts.push(artifact);
    }

    let action_trace_bytes = serde_json::to_vec(&output.action_trace)
        .map_err(|error| format!("failed to encode computer-use action trace: {error}"))?;
    if sha256_hex(action_trace_bytes.as_slice()) != output.action_trace_sha256 {
        return Err("computer-use action trace digest changed before persistence".to_owned());
    }
    let action_trace_artifact = runtime_state
        .create_tool_result_artifact(ToolResultArtifactCreateRequest {
            artifact_id: Ulid::generate().to_string(),
            session_id: context.session_id.to_owned(),
            run_id: context.run_id.to_owned(),
            proposal_id: proposal_id.to_owned(),
            tool_name: request.tool_name.clone(),
            mime_type: "application/vnd.palyra.computer-use-actions+json".to_owned(),
            sensitivity: ToolResultSensitivity::ApprovalRiskData,
            retention: ArtifactRetentionPolicy::keep(),
            redacted_preview: format!(
                "computer-use action trace count={} sha256={}",
                output.action_trace.len(),
                output.action_trace_sha256
            ),
            content: action_trace_bytes,
        })
        .await
        .map_err(|error| {
            format!("failed to persist computer-use action trace: {}", error.message())
        })?;
    if action_trace_artifact.digest_sha256 != output.action_trace_sha256 {
        return Err("persisted computer-use action trace digest is invalid".to_owned());
    }

    serde_json::to_vec(&serde_json::json!({
        "v": output.v,
        "task_id": output.task_id,
        "run_generation": output.run_generation,
        "scope_profile_sha256": output.scope_profile_sha256,
        "initial_observation": output.initial_observation,
        "final_observation": output.final_observation,
        "screenshot_artifacts": screenshot_artifacts,
        "action_trace_artifact": action_trace_artifact,
        "action_trace_sha256": output.action_trace_sha256,
        "action_receipts": output.action_trace,
        "succeeded": output.succeeded,
        "reason_code": output.reason_code,
        "raw_screenshot_bytes_exposed": false,
        "instruction_authority": "none",
    }))
    .map_err(|error| format!("failed to encode computer-use evidence projection: {error}"))
}

async fn prepare_networked_worker_workspace_transfer(
    lease: &WorkerLease,
    tool_name: &str,
    input_json: &[u8],
) -> Result<WorkerRemoteWorkspaceTransfer, String> {
    let workspace_root = PathBuf::from(lease.workspace_scope.workspace_root.as_str());
    let tool_kind = WorkerRemoteToolKind::from_tool_name(tool_name)
        .ok_or_else(|| format!("networked worker tool {tool_name} is unsupported"))?;
    let input_json = input_json.to_vec();
    let allowed_paths = lease.workspace_scope.allowed_paths.clone();
    tokio::task::spawn_blocking(move || {
        build_scoped_networked_worker_workspace(
            workspace_root.as_path(),
            allowed_paths.as_slice(),
            tool_kind,
            &input_json,
        )
    })
    .await
    .map_err(|error| format!("networked worker workspace transfer task failed: {error}"))?
}

fn build_scoped_networked_worker_workspace(
    workspace_root: &Path,
    allowed_paths: &[String],
    tool_kind: WorkerRemoteToolKind,
    input_json: &[u8],
) -> Result<WorkerRemoteWorkspaceTransfer, String> {
    let root = workspace_root
        .canonicalize()
        .map_err(|error| format!("networked worker workspace root is unavailable: {error}"))?;
    if !root.is_dir() {
        return Err("networked worker workspace root is not a directory".to_owned());
    }
    let input = serde_json::from_slice::<serde_json::Value>(input_json)
        .map_err(|error| format!("networked worker tool input is invalid JSON: {error}"))?;
    let object = input
        .as_object()
        .ok_or_else(|| "networked worker tool input must be a JSON object".to_owned())?;
    if object.get("workspace_root").and_then(serde_json::Value::as_str).is_some_and(|value| {
        let value = value.trim();
        !value.is_empty() && value != "/workspace" && value != "workspace"
    }) {
        return Err("networked worker scoped transfer does not accept a secondary workspace_root"
            .to_owned());
    }

    let mut entries = BTreeMap::<String, WorkerRemoteWorkspaceEntry>::new();
    match tool_kind {
        WorkerRemoteToolKind::FsRead => {
            let relative = required_remote_workspace_path(object, "path")?;
            add_remote_workspace_file(&root, relative.as_path(), &mut entries)?;
        }
        WorkerRemoteToolKind::FsList => {
            let relative = optional_remote_workspace_path(object, "path")?;
            add_remote_workspace_directory_listing(&root, relative.as_path(), &mut entries)?;
        }
        WorkerRemoteToolKind::FsSearch => {
            let relative = optional_remote_workspace_path(object, "path")?;
            add_remote_workspace_search_tree(&root, relative.as_path(), &mut entries)?;
        }
        WorkerRemoteToolKind::ApplyPatch => {
            let patch = object
                .get("patch")
                .and_then(serde_json::Value::as_str)
                .filter(|value| !value.trim().is_empty())
                .ok_or_else(|| {
                    "networked worker apply_patch requires non-empty patch".to_owned()
                })?;
            for relative in remote_patch_paths(patch)? {
                let candidate = root.join(relative.as_path());
                match fs::symlink_metadata(candidate.as_path()) {
                    Ok(metadata) if metadata.file_type().is_symlink() => {
                        return Err(format!(
                            "networked worker scoped transfer rejects symlink path {}",
                            relative.display()
                        ));
                    }
                    Ok(metadata) if metadata.is_file() => {
                        add_remote_workspace_file(&root, relative.as_path(), &mut entries)?;
                    }
                    Ok(metadata) if metadata.is_dir() => {
                        add_remote_workspace_directory_entry(relative.as_path(), &mut entries)?;
                    }
                    Ok(_) => {
                        return Err(format!(
                            "networked worker scoped transfer rejects non-file path {}",
                            relative.display()
                        ));
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                    Err(error) => {
                        return Err(format!(
                            "networked worker failed to inspect patch path {}: {error}",
                            relative.display()
                        ));
                    }
                }
            }
        }
        WorkerRemoteToolKind::ComputerUse => {
            let input = serde_json::from_slice::<ComputerUseToolInput>(input_json)
                .map_err(|error| format!("computer-use input is invalid: {error}"))?;
            input.validate().map_err(|error| error.to_string())?;
            for requested in input.actions {
                if let ComputerUseAction::FileChooser { path } = requested.action {
                    add_remote_workspace_file(&root, Path::new(path.as_str()), &mut entries)?;
                }
            }
        }
        _ => {
            return Err(format!(
                "reference network worker does not implement portable workspace transfer for {}",
                tool_kind.tool_name()
            ));
        }
    }
    let entries = entries.into_values().collect::<Vec<_>>();
    for entry in &entries {
        if !remote_workspace_entry_is_allowed(Path::new(entry.path.as_str()), allowed_paths)? {
            return Err(format!(
                "networked worker scoped entry is outside its lease allowlist: {}",
                entry.path
            ));
        }
    }
    let workspace_manifest_sha256 = serde_json::to_vec(&entries)
        .map(|bytes| sha256_hex(bytes.as_slice()))
        .map_err(|error| format!("networked worker workspace manifest failed: {error}"))?;
    WorkerRemoteWorkspaceTransfer::scoped(workspace_manifest_sha256, entries)
        .map_err(|error| format!("networked worker workspace transfer failed validation: {error}"))
}

fn required_remote_workspace_path(
    object: &serde_json::Map<String, serde_json::Value>,
    field: &str,
) -> Result<PathBuf, String> {
    let raw = object
        .get(field)
        .and_then(serde_json::Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| format!("networked worker tool requires non-empty {field}"))?;
    normalize_remote_workspace_path(raw)
}

fn optional_remote_workspace_path(
    object: &serde_json::Map<String, serde_json::Value>,
    field: &str,
) -> Result<PathBuf, String> {
    object
        .get(field)
        .and_then(serde_json::Value::as_str)
        .map(normalize_remote_workspace_path)
        .transpose()
        .map(|path| path.unwrap_or_default())
}

fn normalize_remote_workspace_path(raw: &str) -> Result<PathBuf, String> {
    let replaced = raw.trim().replace('\\', "/");
    let normalized = replaced
        .strip_prefix("/workspace/")
        .or_else(|| replaced.strip_prefix("workspace/"))
        .unwrap_or(replaced.as_str())
        .trim_matches('/');
    if normalized.is_empty() {
        return Ok(PathBuf::new());
    }
    let path = Path::new(normalized);
    if path.is_absolute()
        || path.components().any(|component| {
            matches!(component, Component::ParentDir | Component::RootDir | Component::Prefix(_))
        })
    {
        return Err(format!("networked worker workspace path escapes the scoped root: {raw}"));
    }
    Ok(path.to_path_buf())
}

fn add_remote_workspace_file(
    root: &Path,
    relative: &Path,
    entries: &mut BTreeMap<String, WorkerRemoteWorkspaceEntry>,
) -> Result<(), String> {
    let path = root.join(relative);
    let metadata = fs::symlink_metadata(path.as_path()).map_err(|error| {
        format!("networked worker failed to inspect {}: {error}", path.display())
    })?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err(format!(
            "networked worker scoped file is not a regular file: {}",
            relative.display()
        ));
    }
    if remote_workspace_path_may_contain_secrets(relative) {
        return Err(format!(
            "networked worker scoped transfer blocks secret-bearing path {}",
            relative.display()
        ));
    }
    let canonical = path.canonicalize().map_err(|error| {
        format!("networked worker failed to resolve {}: {error}", path.display())
    })?;
    if !canonical.starts_with(root) {
        return Err(format!(
            "networked worker scoped file escapes the workspace: {}",
            relative.display()
        ));
    }
    let bytes = fs::read(canonical.as_path())
        .map_err(|error| format!("networked worker failed to read {}: {error}", path.display()))?;
    if std::str::from_utf8(bytes.as_slice())
        .is_ok_and(|text| redact_auth_error_strict(text).as_str() != text)
    {
        return Err(format!(
            "networked worker scoped transfer blocks potential secret content in {}",
            relative.display()
        ));
    }
    let current_bytes = entries.values().map(|entry| entry.bytes.len()).sum::<usize>();
    if bytes.len() > NETWORKED_WORKER_WORKSPACE_BUNDLE_MAX_BYTES
        || current_bytes.saturating_add(bytes.len()) > NETWORKED_WORKER_WORKSPACE_BUNDLE_MAX_BYTES
    {
        return Err("networked worker scoped workspace exceeds its byte budget".to_owned());
    }
    let key = remote_workspace_path_string(relative)?;
    entries.insert(
        key.clone(),
        WorkerRemoteWorkspaceEntry {
            path: key,
            kind: WorkerRemoteWorkspaceEntryKind::File,
            sha256: sha256_hex(bytes.as_slice()),
            source_size_bytes: None,
            bytes,
        },
    );
    enforce_remote_workspace_entry_limit(entries)
}

fn add_remote_workspace_file_metadata(
    root: &Path,
    relative: &Path,
    entries: &mut BTreeMap<String, WorkerRemoteWorkspaceEntry>,
) -> Result<(), String> {
    let path = root.join(relative);
    let metadata = fs::symlink_metadata(path.as_path()).map_err(|error| {
        format!("networked worker failed to inspect {}: {error}", path.display())
    })?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err(format!(
            "networked worker scoped metadata path is not a regular file: {}",
            relative.display()
        ));
    }
    if remote_workspace_path_may_contain_secrets(relative) {
        return Err(format!(
            "networked worker scoped transfer blocks secret-bearing path {}",
            relative.display()
        ));
    }
    let canonical = path.canonicalize().map_err(|error| {
        format!("networked worker failed to resolve {}: {error}", path.display())
    })?;
    if !canonical.starts_with(root) {
        return Err(format!(
            "networked worker scoped metadata path escapes the workspace: {}",
            relative.display()
        ));
    }
    let key = remote_workspace_path_string(relative)?;
    let source_size_bytes = metadata.len();
    entries.insert(
        key.clone(),
        WorkerRemoteWorkspaceEntry {
            path: key,
            kind: WorkerRemoteWorkspaceEntryKind::MetadataOnlyFile,
            sha256: sha256_hex(source_size_bytes.to_be_bytes().as_slice()),
            source_size_bytes: Some(source_size_bytes),
            bytes: Vec::new(),
        },
    );
    enforce_remote_workspace_entry_limit(entries)
}

fn add_remote_workspace_directory_listing(
    root: &Path,
    relative: &Path,
    entries: &mut BTreeMap<String, WorkerRemoteWorkspaceEntry>,
) -> Result<(), String> {
    let lexical_directory = root.join(relative);
    let lexical_metadata = fs::symlink_metadata(lexical_directory.as_path()).map_err(|error| {
        format!("networked worker failed to inspect directory {}: {error}", relative.display())
    })?;
    if lexical_metadata.file_type().is_symlink() {
        return Err(format!(
            "networked worker directory listing rejects symlink {}",
            relative.display()
        ));
    }
    let directory = lexical_directory.canonicalize().map_err(|error| {
        format!("networked worker failed to resolve directory {}: {error}", relative.display())
    })?;
    if !directory.starts_with(root) || !directory.is_dir() {
        return Err(format!(
            "networked worker directory escapes the workspace: {}",
            relative.display()
        ));
    }
    if !relative.as_os_str().is_empty() {
        add_remote_workspace_directory_entry(relative, entries)?;
    }
    for child in fs::read_dir(directory.as_path()).map_err(|error| {
        format!("networked worker failed to list directory {}: {error}", relative.display())
    })? {
        let child = child.map_err(|error| {
            format!("networked worker failed to inspect directory {}: {error}", relative.display())
        })?;
        let child_relative = relative.join(child.file_name());
        let file_type = child.file_type().map_err(|error| {
            format!("networked worker failed to inspect {}: {error}", child_relative.display())
        })?;
        if file_type.is_symlink() {
            return Err(format!(
                "networked worker directory listing rejects symlink {}",
                child_relative.display()
            ));
        }
        if file_type.is_dir() {
            add_remote_workspace_directory_entry(child_relative.as_path(), entries)?;
        } else if file_type.is_file() {
            add_remote_workspace_file_metadata(root, child_relative.as_path(), entries)?;
        }
    }
    Ok(())
}

fn add_remote_workspace_search_tree(
    root: &Path,
    relative: &Path,
    entries: &mut BTreeMap<String, WorkerRemoteWorkspaceEntry>,
) -> Result<(), String> {
    let path = root.join(relative);
    let metadata = fs::symlink_metadata(path.as_path()).map_err(|error| {
        format!("networked worker failed to inspect search path {}: {error}", relative.display())
    })?;
    if metadata.file_type().is_symlink() {
        return Err(format!("networked worker search path is a symlink: {}", relative.display()));
    }
    if metadata.is_file() {
        return add_remote_workspace_file(root, relative, entries);
    }
    if !metadata.is_dir() {
        return Err(format!(
            "networked worker search path is not a file or directory: {}",
            relative.display()
        ));
    }
    if !relative.as_os_str().is_empty() {
        add_remote_workspace_directory_entry(relative, entries)?;
    }
    for child in fs::read_dir(path.as_path()).map_err(|error| {
        format!("networked worker failed to read search path {}: {error}", relative.display())
    })? {
        let child = child.map_err(|error| {
            format!(
                "networked worker failed to inspect search path {}: {error}",
                relative.display()
            )
        })?;
        if child
            .file_name()
            .to_str()
            .is_some_and(|name| NETWORKED_WORKER_SKIPPED_DIRECTORIES.contains(&name))
        {
            continue;
        }
        add_remote_workspace_search_tree(
            root,
            relative.join(child.file_name()).as_path(),
            entries,
        )?;
    }
    Ok(())
}

fn add_remote_workspace_directory_entry(
    relative: &Path,
    entries: &mut BTreeMap<String, WorkerRemoteWorkspaceEntry>,
) -> Result<(), String> {
    let key = remote_workspace_path_string(relative)?;
    entries.insert(
        key.clone(),
        WorkerRemoteWorkspaceEntry {
            path: key,
            kind: WorkerRemoteWorkspaceEntryKind::Directory,
            sha256: sha256_hex(&[]),
            source_size_bytes: None,
            bytes: Vec::new(),
        },
    );
    enforce_remote_workspace_entry_limit(entries)
}

fn enforce_remote_workspace_entry_limit(
    entries: &BTreeMap<String, WorkerRemoteWorkspaceEntry>,
) -> Result<(), String> {
    if entries.len() > NETWORKED_WORKER_WORKSPACE_BUNDLE_MAX_ENTRIES {
        Err("networked worker scoped workspace exceeds its entry budget".to_owned())
    } else {
        Ok(())
    }
}

fn remote_workspace_path_string(path: &Path) -> Result<String, String> {
    let rendered = path.to_string_lossy().replace('\\', "/");
    if rendered.is_empty() {
        Err("networked worker workspace entry path must not be empty".to_owned())
    } else {
        Ok(rendered)
    }
}

fn remote_workspace_entry_is_allowed(
    relative: &Path,
    allowed_paths: &[String],
) -> Result<bool, String> {
    if allowed_paths.is_empty() {
        return Err("networked worker lease workspace allowlist is empty".to_owned());
    }
    for raw in allowed_paths {
        let allowed = normalize_remote_workspace_path(raw)?;
        if allowed.as_os_str().is_empty()
            || allowed == Path::new(".")
            || relative == allowed
            || relative.starts_with(allowed.as_path())
        {
            return Ok(true);
        }
    }
    Ok(false)
}

fn remote_workspace_path_may_contain_secrets(path: &Path) -> bool {
    path.components().any(|component| {
        let Component::Normal(name) = component else {
            return false;
        };
        name.to_str().is_some_and(|name| {
            let normalized = name.to_ascii_lowercase();
            normalized == ".env"
                || normalized.starts_with(".env.")
                || normalized.contains("credential")
                || normalized.contains("secret")
                || normalized.ends_with(".pem")
                || normalized.ends_with(".key")
        })
    })
}

fn remote_patch_paths(patch: &str) -> Result<Vec<PathBuf>, String> {
    let mut paths = BTreeMap::<String, PathBuf>::new();
    for line in patch.lines() {
        let trimmed = line.trim();
        let raw = [
            "*** Add File: ",
            "*** Replace File: ",
            "*** Update File: ",
            "*** Delete File: ",
            "*** Replace Line: ",
            "*** Move to: ",
        ]
        .iter()
        .find_map(|prefix| trimmed.strip_prefix(prefix));
        let Some(raw) = raw else {
            continue;
        };
        let path = normalize_remote_workspace_path(raw)?;
        let key = remote_workspace_path_string(path.as_path())?;
        paths.insert(key, path);
    }
    if paths.is_empty() {
        return Err("networked worker patch contains no supported path headers".to_owned());
    }
    Ok(paths.into_values().collect())
}

fn build_worker_remote_tool_request(
    request_context: NetworkedWorkerTaskRequest<'_>,
) -> Result<WorkerRemoteToolRequestEnvelope, String> {
    let NetworkedWorkerTaskRequest {
        proposal_id,
        tool_name,
        input_json,
        lease,
        worker_attestation,
        session_id,
        run_generation,
        process_executable_allowlist,
        workspace_transfer,
    } = request_context;
    let tool_kind = WorkerRemoteToolKind::from_tool_name(tool_name).ok_or_else(|| {
        format!(
            "backend policy blocked tool={tool_name}; reason_code=backend.policy.tool_unsupported; resolved_backend=networked_worker"
        )
    })?;
    let input_json_text = std::str::from_utf8(input_json)
        .map_err(|error| format!("networked worker remote input is not UTF-8 JSON: {error}"))?
        .to_owned();
    let issued_at_unix_ms = current_unix_ms();
    let mut request = WorkerRemoteToolRequestEnvelope {
        protocol: WORKER_REMOTE_TOOL_PROTOCOL.to_owned(),
        schema_version: WORKER_REMOTE_TOOL_SCHEMA_VERSION,
        request_id: Ulid::generate().to_string(),
        proposal_id: proposal_id.to_owned(),
        tool_name: tool_name.to_owned(),
        tool_kind,
        input_json: input_json_text,
        input_json_sha256: sha256_hex(input_json),
        lease: WorkerRemoteLeaseBinding {
            process_executable_allowlist,
            ..WorkerRemoteLeaseBinding::from_lease(
                lease,
                session_id.to_owned(),
                run_generation,
                issued_at_unix_ms,
            )
        },
        worker_identity: WorkerRemoteIdentity::from(worker_attestation),
        workspace_transfer,
        encrypted_secret_artifact: None,
        canonical_protocol: None,
    };
    request.canonical_protocol = Some(RemoteWorkerProtocolV1::from_remote_request(&request));
    request
        .validate(issued_at_unix_ms)
        .map_err(|error| format!("networked worker remote request validation failed: {error}"))?;
    Ok(request)
}

fn networked_worker_process_executable_authority(
    tool_name: &str,
    input_json: &[u8],
    allowed_executables: &[String],
    allow_interpreters: bool,
) -> Result<Vec<String>, String> {
    if tool_name != "palyra.process.run" {
        return Ok(Vec::new());
    }
    let input = parse_process_runner_tool_input(input_json)
        .map_err(|error| format!("networked worker process input is invalid: {error}"))?;
    let command = input.command.as_str();
    if command.trim() != command
        || !networked_worker_process_command_is_unambiguous(command)
        || networked_worker_process_command_is_raw_shell(command)
    {
        return Err(
            "networked worker process command is not an unambiguous executable token".to_owned()
        );
    }
    if process_executable_is_interpreter(command) {
        if !allow_interpreters {
            return Err(
                "networked worker process interpreter requires explicit host policy".to_owned()
            );
        }
        if interpreter_args_contain_blocked_eval_flag(command, input.args.as_slice()) {
            return Err(
                "networked worker process interpreter cannot use inline eval flags".to_owned()
            );
        }
    }
    if !allowed_executables.iter().any(|allowed| {
        let allowed = allowed.trim();
        allowed != "*"
            && networked_worker_process_command_is_unambiguous(allowed)
            && allowed == command
    }) {
        return Err(
            "networked worker process executable is not admitted by exact host policy".to_owned()
        );
    }
    Ok(vec![command.to_owned()])
}

fn networked_worker_process_command_is_unambiguous(command: &str) -> bool {
    if command.is_empty()
        || command.len() > 256
        || command.contains('\\')
        || command.bytes().any(|byte| {
            byte.is_ascii_whitespace() || byte == b'\0' || b"*;&|><`$'\"()".contains(&byte)
        })
    {
        return false;
    }
    if let Some(absolute) = command.strip_prefix('/') {
        return !absolute.is_empty()
            && absolute
                .split('/')
                .all(|segment| !segment.is_empty() && !matches!(segment, "." | ".."));
    }
    !command.contains('/')
        && command
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'+' | b'-'))
}

fn networked_worker_process_command_is_raw_shell(command: &str) -> bool {
    let command_name =
        Path::new(command).file_name().and_then(|name| name.to_str()).unwrap_or(command);
    matches!(
        command_name.to_ascii_lowercase().as_str(),
        "bash"
            | "sh"
            | "zsh"
            | "fish"
            | "cmd"
            | "cmd.exe"
            | "powershell"
            | "powershell.exe"
            | "pwsh"
            | "pwsh.exe"
    )
}

async fn dispatch_networked_worker_remote_tool(
    runtime_state: &Arc<GatewayRuntimeState>,
    context: ToolRuntimeExecutionContext<'_>,
    request: &WorkerRemoteToolRequestEnvelope,
    cancellation_requested: Option<Arc<std::sync::atomic::AtomicBool>>,
) -> Result<NetworkedWorkerRemoteDispatchOutcome, String> {
    let dispatcher = runtime_state
        .networked_worker_remote_dispatcher()
        .ok_or(NetworkedWorkerRemoteDispatchError::Unconfigured)
        .map_err(|error| error.to_string())?;
    dispatcher
        .dispatch_remote_tool(
            runtime_state,
            NetworkedWorkerHostReceiptContext {
                principal: context.principal.to_owned(),
                device_id: context.device_id.to_owned(),
                channel: context.channel.map(ToOwned::to_owned),
            },
            request.clone(),
            cancellation_requested,
        )
        .await
        .map_err(|error| error.to_string())
}

fn spawn_networked_worker_late_result_owner(
    runtime_state: Arc<GatewayRuntimeState>,
    request: WorkerRemoteToolRequestEnvelope,
    lease: WorkerLease,
    mut released: NetworkedWorkerReleasedDispatch,
    health_authority: ManagedRuntimeHealthAuthority,
) {
    let session_id = match RuntimeSessionId::parse(request.lease.session_id.as_str()) {
        Ok(session_id) => session_id,
        Err(error) => {
            warn!(
                request_id = request.request_id.as_str(),
                error = %error,
                reason_code = "worker.late_result_invalid_session_identity",
                "networked worker late result owner failed closed"
            );
            return;
        }
    };
    let run_id = match RuntimeRunId::parse(request.lease.run_id.as_str()) {
        Ok(run_id) => run_id,
        Err(error) => {
            warn!(
                request_id = request.request_id.as_str(),
                error = %error,
                reason_code = "worker.late_result_invalid_run_identity",
                "networked worker late result owner failed closed"
            );
            return;
        }
    };
    let run_generation = request.lease.run_generation;
    tokio::spawn(async move {
        let notification = released.receiver.recv().await;
        let Some(delivery_attempt_id) = notification.delivery_attempt_id.as_deref() else {
            warn!(
                request_id = request.request_id.as_str(),
                node_request_id = released.node_request_id.as_str(),
                reason_code = "worker.late_result_missing_delivery_attempt",
                "networked worker late result failed closed"
            );
            return;
        };
        let Some(callback_run_generation) = notification.run_generation else {
            warn!(
                request_id = request.request_id.as_str(),
                node_request_id = released.node_request_id.as_str(),
                reason_code = "worker.late_result_missing_run_generation",
                "networked worker late result failed closed"
            );
            return;
        };
        let result = match remote_result_from_node_capability_result(notification.result) {
            Ok(result) => result,
            Err(_) => {
                warn!(
                    request_id = request.request_id.as_str(),
                    node_request_id = released.node_request_id.as_str(),
                    reason_code = "worker.late_result_malformed",
                    "networked worker late result failed closed"
                );
                return;
            }
        };
        if result.run_generation != callback_run_generation {
            warn!(
                request_id = request.request_id.as_str(),
                node_request_id = released.node_request_id.as_str(),
                reason_code = "worker.late_result_stale_generation",
                "networked worker late result failed closed"
            );
            return;
        }
        if result.validate_against_request(&request, notification.observed_at_unix_ms).is_err() {
            warn!(
                request_id = request.request_id.as_str(),
                node_request_id = released.node_request_id.as_str(),
                reason_code = "worker.late_result_validation_failed",
                "networked worker late result failed closed"
            );
            return;
        }
        let validated_result_sha256 =
            match result.validated_receipt_sha256(&request, notification.observed_at_unix_ms) {
                Ok(digest) => digest,
                Err(_) => {
                    warn!(
                        request_id = request.request_id.as_str(),
                        node_request_id = released.node_request_id.as_str(),
                        reason_code = "worker.late_result_digest_failed",
                        "networked worker late result failed closed"
                    );
                    return;
                }
            };
        if matches!(
            notification.networked_worker_commit_disposition,
            Some(
                NetworkedWorkerResultCommitDisposition::LateReconciliation
                    | NetworkedWorkerResultCommitDisposition::ExactReplay
            )
        ) {
            runtime_state.record_managed_runtime_health_observation_for_run(
                &health_authority,
                &session_id,
                &run_id,
                run_generation,
                true,
                "runtime.health.worker_late_result_succeeded",
            );
            return;
        }
        let settlement_identity = crate::gateway::NetworkedWorkerDispatchSettlementIdentity {
            remote_request_id: request.request_id.clone(),
            delivery_attempt_id: Some(delivery_attempt_id.to_owned()),
            session_id: request.lease.session_id.clone(),
            run_generation: result.run_generation,
        };
        if let Err(error) = runtime_state.settle_reconciling_networked_worker_dispatch(
            &settlement_identity,
            lease.worker_id.as_str(),
            &lease.identity(),
            validated_result_sha256.as_str(),
            notification.observed_at_unix_ms,
        ) {
            warn!(
                request_id = request.request_id.as_str(),
                node_request_id = released.node_request_id.as_str(),
                reason_code = "worker.late_result_settlement_failed",
                status_code = ?error.code(),
                "networked worker late result settlement failed closed"
            );
            return;
        }
        runtime_state.record_managed_runtime_health_observation_for_run(
            &health_authority,
            &session_id,
            &run_id,
            run_generation,
            true,
            "runtime.health.worker_late_result_succeeded",
        );
    });
}

async fn release_unstarted_networked_worker_lease(
    runtime_state: &Arc<GatewayRuntimeState>,
    lease: &WorkerLease,
) -> Result<(), tonic::Status> {
    runtime_state
        .complete_networked_worker_lease(
            lease.worker_id.as_str(),
            lease.identity(),
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

async fn record_unverified_networked_worker_cleanup(
    runtime_state: &Arc<GatewayRuntimeState>,
    lease: &WorkerLease,
) -> Result<(), tonic::Status> {
    match runtime_state
        .complete_networked_worker_lease(
            lease.worker_id.as_str(),
            lease.identity(),
            WorkerCleanupReport {
                removed_workspace_scope: false,
                removed_artifacts: false,
                removed_logs: false,
                failure_reason: Some("worker.cleanup.incomplete".to_owned()),
            },
        )
        .await
    {
        Ok(_) => Ok(()),
        // The fleet manager has already journaled this expected fail-closed outcome. Preserve the
        // original dispatch error rather than misreporting successful quarantine as a cleanup API
        // failure.
        Err(error)
            if error.code() == tonic::Code::FailedPrecondition
                && error.message() == "networked worker cleanup did not remove all scoped data" =>
        {
            Ok(())
        }
        Err(error) => Err(error),
    }
}

fn networked_worker_execution_manifest(
    request: &WorkerRemoteToolRequestEnvelope,
    result: &WorkerRemoteToolResultEnvelope,
) -> ExecutionAttestationManifest {
    ExecutionAttestationManifest {
        schema_version: 1,
        backend_id: "networked_worker".to_owned(),
        runner_id: "networked_worker_remote_dispatcher".to_owned(),
        runner_version: WORKER_REMOTE_TOOL_PROTOCOL.to_owned(),
        workspace_strategy_digest: WorkspaceStrategyDescriptor::remote_lease_workspace()
            .attestation_digest_sha256(),
        input_manifest_sha256: request.lease.artifact_transport.input_manifest_sha256.clone(),
        output_manifest_sha256: result.output_manifest_sha256.clone(),
        cleanup: networked_worker_cleanup_evidence(&result.cleanup_report),
        egress_posture: "worker_egress_proxy_attested".to_owned(),
        policy_decision_id: Some(request.lease.grant_id.clone()),
        approval_id: None,
    }
}

fn networked_worker_cleanup_evidence(
    cleanup_report: &WorkerCleanupReport,
) -> ExecutionCleanupEvidence {
    let success = cleanup_report.is_verified();
    ExecutionCleanupEvidence {
        strategy: "networked_worker_lease_cleanup".to_owned(),
        success,
        reason_code: if success {
            "worker.cleanup.ok".to_owned()
        } else {
            "worker.cleanup.incomplete".to_owned()
        },
        resources: vec![
            networked_worker_cleanup_resource(
                "remote_workspace",
                cleanup_report.removed_workspace_scope,
            ),
            networked_worker_cleanup_resource("remote_artifacts", cleanup_report.removed_artifacts),
            networked_worker_cleanup_resource("remote_logs", cleanup_report.removed_logs),
        ],
    }
}

fn networked_worker_cleanup_resource(
    kind: &str,
    cleanup_verified: bool,
) -> ExecutionCleanupResourceEvidence {
    ExecutionCleanupResourceEvidence {
        kind: kind.to_owned(),
        status: if cleanup_verified { "removed" } else { "remove_failed" }.to_owned(),
        cleanup_required: true,
        cleanup_verified,
        identifier_sha256: None,
    }
}

#[cfg(test)]
fn networked_worker_outcome_from_remote_result(
    request: &WorkerRemoteToolRequestEnvelope,
    result: WorkerRemoteToolResultEnvelope,
    now_unix_ms: i64,
) -> ToolExecutionOutcome {
    if let Err(error) = result.validate_against_request(request, now_unix_ms) {
        if matches!(&error, WorkerRemoteToolContractError::CleanupGap { .. }) {
            return networked_worker_cleanup_failure_outcome(request, result);
        }
        let sandbox_enforcement = if matches!(
            &error,
            WorkerRemoteToolContractError::DigestMismatch { field: "output_json_sha256" }
        ) {
            "networked_worker_remote_digest_mismatch"
        } else {
            "networked_worker_remote_fail_closed"
        };
        return networked_worker_failure_outcome(
            request.proposal_id.as_str(),
            request.tool_name.as_str(),
            request.input_json.as_bytes(),
            format!("networked worker remote execution failed: {error}"),
            sandbox_enforcement,
        );
    }
    networked_worker_outcome_from_validated_remote_result(request, result)
}

#[cfg(test)]
fn networked_worker_outcome_from_validated_remote_result(
    request: &WorkerRemoteToolRequestEnvelope,
    result: WorkerRemoteToolResultEnvelope,
) -> ToolExecutionOutcome {
    let original_input_json = request.input_json.as_bytes();
    networked_worker_outcome_from_validated_remote_result_with_projection(
        request,
        result,
        None,
        original_input_json,
    )
}

fn networked_worker_outcome_from_validated_remote_result_with_projection(
    request: &WorkerRemoteToolRequestEnvelope,
    result: WorkerRemoteToolResultEnvelope,
    projected_output_json: Option<Vec<u8>>,
    original_input_json: &[u8],
) -> ToolExecutionOutcome {
    if let Err(error) = validate_networked_worker_canonical_outcome(request, &result) {
        return networked_worker_failure_outcome(
            request.proposal_id.as_str(),
            request.tool_name.as_str(),
            original_input_json,
            format!("networked worker canonical outcome failed validation: {error}"),
            "networked_worker_canonical_protocol_failed",
        );
    }
    let manifest = networked_worker_execution_manifest(request, &result);
    let output_json =
        projected_output_json.unwrap_or_else(|| result.output_json.as_bytes().to_vec());
    build_tool_execution_outcome_with_manifest(
        request.proposal_id.as_str(),
        request.tool_name.as_str(),
        original_input_json,
        result.success,
        output_json,
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
        manifest,
    )
}

fn validate_networked_worker_canonical_outcome(
    request: &WorkerRemoteToolRequestEnvelope,
    result: &WorkerRemoteToolResultEnvelope,
) -> Result<(), palyra_workerd::remote_protocol::RemoteWorkerProtocolError> {
    let canonical_task = request
        .canonical_protocol
        .as_ref()
        .map(|protocol| protocol.task.clone())
        .unwrap_or_else(|| RemoteWorkerProtocolV1::from_remote_request(request).task);
    let canonical_outcome = RemoteTaskOutcome::from_remote_result(request, result);
    canonical_outcome.validate_against(&canonical_task, result.completed_at_unix_ms)
}

fn networked_worker_cleanup_failure_outcome(
    request: &WorkerRemoteToolRequestEnvelope,
    result: WorkerRemoteToolResultEnvelope,
) -> ToolExecutionOutcome {
    warn!(
        tool_name = request.tool_name.as_str(),
        reason_code = "worker.cleanup.incomplete",
        "networked worker cleanup failed closed"
    );
    let manifest = networked_worker_execution_manifest(request, &result);
    build_tool_execution_outcome_with_manifest(
        request.proposal_id.as_str(),
        request.tool_name.as_str(),
        request.input_json.as_bytes(),
        false,
        result.output_json.into_bytes(),
        "networked worker remote execution failed: cleanup verification incomplete".to_owned(),
        false,
        format!("networked_worker:{}", result.worker_id),
        "networked_worker_remote_fail_closed".to_owned(),
        manifest,
    )
}

fn build_worker_lease_request(
    runtime_state: &Arc<GatewayRuntimeState>,
    context: ToolRuntimeExecutionContext<'_>,
    proposal_id: &str,
    tool_name: &str,
    input_json: &[u8],
) -> Result<WorkerLeaseRequest, String> {
    let now_unix_ms = current_unix_ms();
    let ttl_ms = runtime_state.config.networked_workers.lease_ttl_ms;
    let ttl_ms_i64 = i64::try_from(ttl_ms)
        .map_err(|_| "networked worker lease ttl exceeds the supported range".to_owned())?;
    let grant_id = Ulid::generate().to_string();
    let read_only = WorkerRemoteToolKind::from_tool_name(tool_name)
        .is_none_or(remote_tool_kind_uses_read_only_workspace);
    let tool_kind = WorkerRemoteToolKind::from_tool_name(tool_name)
        .ok_or_else(|| format!("networked worker tool {tool_name} is unsupported"))?;
    let allowed_paths = networked_worker_allowed_paths(tool_kind, input_json)?;
    Ok(WorkerLeaseRequest {
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
            allowed_paths,
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
            expires_at_unix_ms: now_unix_ms
                .saturating_add(ttl_ms_i64)
                .saturating_add(NETWORKED_WORKER_GRANT_ADMISSION_SLACK_MS),
        },
    })
}

fn networked_worker_allowed_paths(
    tool_kind: WorkerRemoteToolKind,
    input_json: &[u8],
) -> Result<Vec<String>, String> {
    let input = serde_json::from_slice::<serde_json::Value>(input_json)
        .map_err(|error| format!("networked worker tool input is invalid JSON: {error}"))?;
    let object = input
        .as_object()
        .ok_or_else(|| "networked worker tool input must be a JSON object".to_owned())?;
    let paths = match tool_kind {
        WorkerRemoteToolKind::FsRead => {
            vec![required_remote_workspace_path(object, "path")?]
        }
        WorkerRemoteToolKind::FsList | WorkerRemoteToolKind::FsSearch => {
            vec![optional_remote_workspace_path(object, "path")?]
        }
        WorkerRemoteToolKind::ApplyPatch => {
            let patch = object
                .get("patch")
                .and_then(serde_json::Value::as_str)
                .filter(|value| !value.trim().is_empty())
                .ok_or_else(|| {
                    "networked worker apply_patch requires non-empty patch".to_owned()
                })?;
            remote_patch_paths(patch)?
        }
        WorkerRemoteToolKind::ComputerUse => {
            let input = serde_json::from_slice::<ComputerUseToolInput>(input_json)
                .map_err(|error| format!("computer-use input is invalid: {error}"))?;
            input.validate().map_err(|error| error.to_string())?;
            let paths = input
                .actions
                .into_iter()
                .filter_map(|requested| match requested.action {
                    ComputerUseAction::FileChooser { path } => Some(PathBuf::from(path)),
                    _ => None,
                })
                .collect::<Vec<_>>();
            if paths.is_empty() {
                vec![PathBuf::from(".")]
            } else {
                paths
            }
        }
        _ => {
            return Err(format!(
                "reference network worker does not implement portable scope for {}",
                tool_kind.tool_name()
            ));
        }
    };
    paths
        .iter()
        .map(|path| {
            if path.as_os_str().is_empty() {
                Ok(".".to_owned())
            } else {
                remote_workspace_path_string(path.as_path())
            }
        })
        .collect()
}

fn remote_tool_kind_uses_read_only_workspace(tool_kind: WorkerRemoteToolKind) -> bool {
    matches!(
        tool_kind,
        WorkerRemoteToolKind::FsRead
            | WorkerRemoteToolKind::FsList
            | WorkerRemoteToolKind::FsSearch
            | WorkerRemoteToolKind::ArtifactRead
            | WorkerRemoteToolKind::ComputerUse
    )
}

fn ensure_node_is_ready_for_remote_worker(
    node: &RegisteredNodeRecord,
    required_capabilities: &[String],
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

    if !node.capabilities.iter().any(|capability| {
        capability.available && capability.name == NETWORKED_WORKER_DELIVERY_FENCE_CAPABILITY
    }) {
        return Err(NetworkedWorkerRemoteDispatchError::WorkerUnavailable(format!(
            "worker node {} does not advertise required capability {}",
            node.device_id, NETWORKED_WORKER_DELIVERY_FENCE_CAPABILITY
        )));
    }

    if let Some(required_capability) = required_capabilities.iter().find(|required_capability| {
        !node.capabilities.iter().any(|capability| {
            capability.available && capability.name == required_capability.as_str()
        })
    }) {
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
        build_host_bound_computer_use_task, build_scoped_networked_worker_workspace,
        networked_worker_outcome_from_remote_result, networked_worker_process_executable_authority,
        networked_worker_supports_tool, networked_worker_tool_capability,
        remote_tool_kind_uses_read_only_workspace, sha256_hex,
        validate_computer_use_host_approval_record, ComputerUseHostApprovalAuthority,
    };
    use palyra_common::runtime_contracts::RuntimeGeneration;
    use palyra_workerd::{
        computer_use::ComputerUseTaskContract, WorkerArtifactTransport, WorkerCleanupReport,
        WorkerLease, WorkerRemoteIdentity, WorkerRemoteLeaseBinding, WorkerRemoteToolKind,
        WorkerRemoteToolRequestEnvelope, WorkerRemoteToolResultEnvelope,
        WorkerRemoteWorkspaceEntryKind, WorkerRemoteWorkspaceTransfer, WorkerRunGrant,
        WorkerWorkspaceScope, WORKER_REMOTE_TOOL_PROTOCOL, WORKER_REMOTE_TOOL_SCHEMA_VERSION,
    };
    use serde_json::{json, Value};

    use crate::{
        execution_backends::ExecutionBackendPreference,
        gateway::ToolRuntimeExecutionContext,
        journal::{
            ApprovalDecision, ApprovalDecisionScope, ApprovalPolicySnapshot, ApprovalPromptOption,
            ApprovalPromptRecord, ApprovalRecord, ApprovalRiskLevel, ApprovalSubjectType,
        },
    };

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
                run_generation: request.lease.run_generation,
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
                session_id: "session-remote-01".to_owned(),
                run_id: "run-remote-01".to_owned(),
                run_generation: RuntimeGeneration::new(7).expect("test generation should be valid"),
                grant_id: format!("grant-{}", tool_kind.as_str()),
                grant_tool_name: tool_name.to_owned(),
                issued_at_unix_ms: 2_000,
                expires_at_unix_ms: 3_000,
                required_capabilities: vec![tool_kind.required_capability()],
                process_executable_allowlist: if matches!(
                    tool_kind,
                    WorkerRemoteToolKind::ProcessRun
                ) {
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
            encrypted_secret_artifact: None,
            canonical_protocol: None,
        }
    }

    fn computer_use_context() -> ToolRuntimeExecutionContext<'static> {
        ToolRuntimeExecutionContext {
            principal: "operator:test",
            device_id: "device-test",
            channel: Some("test"),
            session_id: "session-test",
            run_id: "run-test",
            execution_backend: ExecutionBackendPreference::NetworkedWorker,
            backend_reason_code: "test.networked_worker",
        }
    }

    fn allowed_computer_use_approval() -> ApprovalRecord {
        ApprovalRecord {
            approval_id: "approval-computer-use".to_owned(),
            session_id: "session-test".to_owned(),
            run_id: "run-test".to_owned(),
            principal: "operator:test".to_owned(),
            device_id: "device-test".to_owned(),
            channel: Some("test".to_owned()),
            requested_at_unix_ms: 1_000,
            resolved_at_unix_ms: Some(1_100),
            subject_type: ApprovalSubjectType::Tool,
            subject_id: "tool:palyra.computer.use".to_owned(),
            request_summary: "computer use approval".to_owned(),
            decision: Some(ApprovalDecision::Allow),
            decision_scope: Some(ApprovalDecisionScope::Once),
            decision_reason: Some("operator_allowed".to_owned()),
            decision_scope_ttl_ms: None,
            policy_snapshot: ApprovalPolicySnapshot {
                policy_id: "tool_call_policy.v1".to_owned(),
                policy_hash: sha256_hex(b"policy"),
                evaluation_summary: "action=tool.execute resource=tool:palyra.computer.use approval_required=true deny_by_default=true".to_owned(),
            },
            prompt: ApprovalPromptRecord {
                title: "Approve palyra.computer.use".to_owned(),
                risk_level: ApprovalRiskLevel::High,
                subject_id: "tool:palyra.computer.use".to_owned(),
                summary: "Computer use requires approval".to_owned(),
                options: vec![
                    ApprovalPromptOption {
                        option_id: "allow_once".to_owned(),
                        label: "Allow once".to_owned(),
                        description: "Allow".to_owned(),
                        default_selected: false,
                        decision_scope: ApprovalDecisionScope::Once,
                        timebox_ttl_ms: None,
                    },
                    ApprovalPromptOption {
                        option_id: "deny_once".to_owned(),
                        label: "Deny".to_owned(),
                        description: "Deny".to_owned(),
                        default_selected: true,
                        decision_scope: ApprovalDecisionScope::Once,
                        timebox_ttl_ms: None,
                    },
                ],
                timeout_seconds: 60,
                details_json: json!({
                    "tool_name": "palyra.computer.use",
                    "subject_id": "tool:palyra.computer.use",
                    "input_json": {
                        "permission_request": {
                            "source": "tool_proposal",
                            "tool_name": "palyra.computer.use",
                            "subject_id": "tool:palyra.computer.use",
                            "requested_scope": "single_tool_call",
                            "requester": {"kind": "host_approval_relay"},
                            "execution_backend": {
                                "resolved": "networked_worker",
                                "approval_required": true
                            }
                        }
                    }
                })
                .to_string(),
                policy_explanation:
                    "Sensitive tool actions are deny-by-default until explicitly approved"
                        .to_owned(),
            },
            created_at_unix_ms: 1_000,
            updated_at_unix_ms: 1_100,
        }
    }

    #[test]
    fn computer_use_approval_requires_standard_durable_host_record() {
        let context = computer_use_context();
        let allowed = allowed_computer_use_approval();
        let authority = validate_computer_use_host_approval_record(
            &allowed,
            context,
            allowed.approval_id.as_str(),
            "tool:palyra.computer.use",
            2_000,
        )
        .expect("standard allowed approval should grant worker authority");
        assert_eq!(
            authority,
            ComputerUseHostApprovalAuthority {
                approval_id: "approval-computer-use".to_owned(),
                expires_at_unix_ms: None,
            }
        );

        let mut missing = allowed.clone();
        missing.decision = None;
        assert!(validate_computer_use_host_approval_record(
            &missing,
            context,
            missing.approval_id.as_str(),
            "tool:palyra.computer.use",
            2_000,
        )
        .is_err());

        let mut bypass = allowed.clone();
        bypass.prompt.details_json = json!({
            "tool_name": "palyra.computer.use",
            "subject_id": "tool:palyra.computer.use",
            "input_json": {}
        })
        .to_string();
        assert!(validate_computer_use_host_approval_record(
            &bypass,
            context,
            bypass.approval_id.as_str(),
            "tool:palyra.computer.use",
            2_000,
        )
        .is_err());

        let mut unknown = allowed.clone();
        unknown.subject_id = "tool:palyra.computer.unknown".to_owned();
        unknown.prompt.subject_id = unknown.subject_id.clone();
        assert!(validate_computer_use_host_approval_record(
            &unknown,
            context,
            unknown.approval_id.as_str(),
            "tool:palyra.computer.use",
            2_000,
        )
        .is_err());
    }

    #[test]
    fn computer_use_task_builder_cannot_self_approve_from_worker_lease() {
        let input = json!({
            "v": 1,
            "initial_ui_text": "untrusted UI",
            "actions": [{
                "expected_observation_generation": 1,
                "action": {"kind": "click", "x": 20, "y": 24}
            }]
        })
        .to_string();
        let lease = WorkerLease {
            lease_id: "lease-computer-use".to_owned(),
            worker_id: "worker-computer-use".to_owned(),
            run_id: "run-test".to_owned(),
            expires_at_unix_ms: 10_000,
            required_capabilities: vec!["tool:palyra.computer.use".to_owned()],
            workspace_scope: WorkerWorkspaceScope {
                workspace_root: "/workspace".to_owned(),
                allowed_paths: vec![".".to_owned()],
                read_only: true,
            },
            artifact_transport: WorkerArtifactTransport {
                input_manifest_sha256: sha256_hex(input.as_bytes()),
                output_manifest_sha256: sha256_hex(b"pending"),
                log_stream_id: "logs/computer-use".to_owned(),
                scratch_directory_id: "scratch/computer-use".to_owned(),
            },
            grant: WorkerRunGrant {
                grant_id: "lease-grant-is-not-approval".to_owned(),
                run_id: "run-test".to_owned(),
                tool_name: "palyra.computer.use".to_owned(),
                expires_at_unix_ms: 10_000,
            },
        };
        let generation = RuntimeGeneration::new(7).expect("generation");
        let missing = build_host_bound_computer_use_task(
            input.as_bytes(),
            "proposal-computer-use",
            &lease,
            generation,
            sha256_hex(b"isolated-image").as_str(),
            None,
        )
        .expect_err("worker lease grant must not self-approve computer use");
        assert!(missing.contains("host approval authority is required"));

        let encoded = build_host_bound_computer_use_task(
            input.as_bytes(),
            "proposal-computer-use",
            &lease,
            generation,
            sha256_hex(b"isolated-image").as_str(),
            Some(&ComputerUseHostApprovalAuthority {
                approval_id: "approval-computer-use".to_owned(),
                expires_at_unix_ms: None,
            }),
        )
        .expect("durable host authority should bind an approval");
        let contract: ComputerUseTaskContract =
            serde_json::from_slice(encoded.as_slice()).expect("computer-use contract");
        assert_eq!(contract.approval.expect("approval").approval_id, "approval-computer-use");
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
    fn networked_worker_process_authority_is_exact_host_policy() {
        let allowed = vec!["cargo".to_owned()];
        let authority = networked_worker_process_executable_authority(
            "palyra.process.run",
            br#"{"command":"cargo","args":["check"]}"#,
            allowed.as_slice(),
            false,
        )
        .expect("exact host policy should yield one task-bound executable");
        assert_eq!(authority, vec!["cargo".to_owned()]);

        for (input, policy) in [
            (br#"{"command":"cargo","args":["check"]}"#.as_slice(), vec!["*".to_owned()]),
            (br#"{"command":"./cargo","args":["check"]}"#.as_slice(), vec!["cargo".to_owned()]),
            (br#"{"command":"sh","args":["-c","cargo check"]}"#.as_slice(), vec!["sh".to_owned()]),
            (br#"{"command":"cargo","unknown":true}"#.as_slice(), vec!["cargo".to_owned()]),
        ] {
            assert!(
                networked_worker_process_executable_authority(
                    "palyra.process.run",
                    input,
                    policy.as_slice(),
                    false,
                )
                .is_err(),
                "wildcard, path alias, shell, and malformed authority must fail closed"
            );
        }
        assert!(networked_worker_process_executable_authority(
            "palyra.fs.read_file",
            br#"{"path":"src/lib.rs"}"#,
            allowed.as_slice(),
            false,
        )
        .expect("non-process tools retain an empty compatibility authority")
        .is_empty());
    }

    #[test]
    fn networked_worker_process_authority_enforces_interpreter_policy() {
        let allowed = vec!["python".to_owned()];

        assert!(networked_worker_process_executable_authority(
            "palyra.process.run",
            br#"{"command":"python","args":["scripts/check.py"]}"#,
            allowed.as_slice(),
            false,
        )
        .is_err());
        assert!(networked_worker_process_executable_authority(
            "palyra.process.run",
            br#"{"command":"python","args":["-c","print('unsafe')"]}"#,
            allowed.as_slice(),
            true,
        )
        .is_err());
        assert_eq!(
            networked_worker_process_executable_authority(
                "palyra.process.run",
                br#"{"command":"python","args":["scripts/check.py"]}"#,
                allowed.as_slice(),
                true,
            )
            .expect("explicit interpreter policy should admit a workspace script"),
            allowed
        );
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
    fn scoped_workspace_transfer_limits_content_to_tools_that_need_it() {
        let workspace = tempfile::tempdir().expect("workspace");
        let listing_contents = b"authorization=PALYRA_TEST_SECRET";
        std::fs::create_dir_all(workspace.path().join("src")).expect("source directory");
        std::fs::write(workspace.path().join("src/lib.rs"), b"pub fn worker() {}\n")
            .expect("source fixture");
        std::fs::write(
            workspace.path().join("src/config.txt"),
            b"authorization=PALYRA_TEST_SECRET",
        )
        .expect("embedded secret fixture");
        std::fs::create_dir_all(workspace.path().join("listing")).expect("listing directory");
        std::fs::write(workspace.path().join("listing/item.txt"), listing_contents)
            .expect("listing fixture");
        std::fs::write(workspace.path().join(".env"), b"PALYRA_TEST_SECRET=blocked")
            .expect("secret fixture");

        let transfer = build_scoped_networked_worker_workspace(
            workspace.path(),
            &["src/lib.rs".to_owned()],
            WorkerRemoteToolKind::FsRead,
            br#"{"path":"src/lib.rs"}"#,
        )
        .expect("scoped transfer");
        assert_eq!(transfer.scoped_entries.len(), 1);
        assert_eq!(transfer.scoped_entries[0].path, "src/lib.rs");
        assert_eq!(transfer.scoped_entries[0].bytes, b"pub fn worker() {}\n");
        assert_eq!(
            transfer.scoped_entries[0].sha256,
            sha256_hex(transfer.scoped_entries[0].bytes.as_slice())
        );
        let outside_allowlist = build_scoped_networked_worker_workspace(
            workspace.path(),
            &["other".to_owned()],
            WorkerRemoteToolKind::FsRead,
            br#"{"path":"src/lib.rs"}"#,
        )
        .expect_err("lease allowlist must fence every transferred entry");
        assert!(outside_allowlist.contains("outside its lease allowlist"));

        let listing = build_scoped_networked_worker_workspace(
            workspace.path(),
            &["listing".to_owned()],
            WorkerRemoteToolKind::FsList,
            br#"{"path":"listing"}"#,
        )
        .expect("listing transfer");
        let listed_file = listing
            .scoped_entries
            .iter()
            .find(|entry| entry.path == "listing/item.txt")
            .expect("listed file entry");
        assert_eq!(listed_file.kind, WorkerRemoteWorkspaceEntryKind::MetadataOnlyFile);
        assert!(listed_file.bytes.is_empty());
        assert_eq!(
            listed_file.source_size_bytes,
            Some(u64::try_from(listing_contents.len()).expect("listing size"))
        );
        assert_eq!(
            listed_file.sha256,
            sha256_hex(
                u64::try_from(listing_contents.len())
                    .expect("listing size")
                    .to_be_bytes()
                    .as_slice()
            )
        );
        assert!(!serde_json::to_string(&listing)
            .expect("listing transfer JSON")
            .contains("PALYRA_TEST_SECRET"));

        let secret = build_scoped_networked_worker_workspace(
            workspace.path(),
            &[".env".to_owned()],
            WorkerRemoteToolKind::FsRead,
            br#"{"path":".env"}"#,
        )
        .expect_err("secret-bearing path must fail closed");
        assert!(secret.contains("blocks secret-bearing path"));

        let embedded_secret = build_scoped_networked_worker_workspace(
            workspace.path(),
            &["src/config.txt".to_owned()],
            WorkerRemoteToolKind::FsRead,
            br#"{"path":"src/config.txt"}"#,
        )
        .expect_err("embedded secret content must fail closed");
        assert!(embedded_secret.contains("blocks potential secret content"));

        let escape = build_scoped_networked_worker_workspace(
            workspace.path(),
            &[".".to_owned()],
            WorkerRemoteToolKind::FsRead,
            br#"{"path":"../outside"}"#,
        )
        .expect_err("workspace escape must fail closed");
        assert!(escape.contains("escapes the scoped root"));
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
            let manifest = outcome
                .attestation
                .execution_manifest
                .as_ref()
                .expect("networked worker outcome should carry an execution manifest");
            assert_eq!(manifest.backend_id, "networked_worker");
            assert_eq!(manifest.runner_id, "networked_worker_remote_dispatcher");
            assert_eq!(
                manifest.input_manifest_sha256,
                request.lease.artifact_transport.input_manifest_sha256
            );
            assert!(manifest.cleanup.success);
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
        fake.cleanup_report.failure_reason =
            Some("artifact directory contains palyra_test_secret_cleanup_123456".to_owned());
        let result = fake.execute(&request, json!({"applied": true}));

        let outcome = networked_worker_outcome_from_remote_result(&request, result, 2_000);

        assert!(!outcome.success);
        assert!(outcome.error.contains("cleanup verification incomplete"));
        assert!(!outcome.error.contains("palyra_test_secret_cleanup_123456"));
        assert_eq!(outcome.attestation.sandbox_enforcement, "networked_worker_remote_fail_closed");
        let manifest = outcome
            .attestation
            .execution_manifest
            .as_ref()
            .expect("networked worker cleanup failure should carry an execution manifest");
        assert_eq!(manifest.backend_id, "networked_worker");
        assert!(!manifest.cleanup.success);
        assert_eq!(manifest.cleanup.reason_code, "worker.cleanup.incomplete");
        let manifest_json = serde_json::to_string(manifest)
            .expect("networked worker cleanup manifest should serialize");
        assert!(!manifest_json.contains("palyra_test_secret_cleanup_123456"));
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

    #[test]
    fn fake_worker_remote_malicious_patch_fails_canonical_validation() {
        let request = remote_request("palyra.fs.apply_patch");
        let fake = FakeRemoteWorker::healthy("worker-remote-01");
        let result = fake.execute(
            &request,
            json!({
                "remote_patch_bundle": {
                    "patch_sha256": "a".repeat(64),
                    "touched_paths": ["../outside"],
                    "review_required": true
                }
            }),
        );

        let outcome = networked_worker_outcome_from_remote_result(&request, result, 2_000);

        assert!(!outcome.success);
        assert_eq!(
            outcome.attestation.sandbox_enforcement,
            "networked_worker_canonical_protocol_failed"
        );
        assert!(outcome.error.contains("patch path escapes"));
    }
}
