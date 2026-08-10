//! Store-error to gRPC `Status` mapping for every gateway storage domain
//! (orchestrator, agents, cron, approvals, memory, skills, canvas), plus the
//! shared wall-clock helper. Status messages here are client-facing contract.

use super::*;

/// Maps an orchestrator/journal store error to the matching gRPC status code;
/// unrecognized errors become `internal` prefixed with `operation`.
pub(crate) fn map_orchestrator_store_error(operation: &str, error: JournalError) -> Status {
    match error {
        JournalError::DuplicateRunId { run_id } => {
            Status::already_exists(format!("orchestrator run already exists: {run_id}"))
        }
        JournalError::SessionRunAlreadyActive { session_id, active_run_id, requested_run_id } => {
            Status::failed_precondition(format!(
                "orchestrator session {session_id} already has active run {active_run_id}; wait for it, cancel it, or start a different session before starting run {requested_run_id}"
            ))
        }
        JournalError::DuplicateTapeSequence { run_id, seq } => Status::already_exists(format!(
            "orchestrator tape already contains seq={seq} for run {run_id}"
        )),
        JournalError::RunNotFound { run_id } => {
            Status::not_found(format!("orchestrator run not found: {run_id}"))
        }
        JournalError::PayloadTooLarge { payload_kind, actual_bytes, max_bytes } => {
            Status::invalid_argument(format!(
                "{payload_kind} payload exceeds maximum size ({actual_bytes} > {max_bytes})"
            ))
        }
        JournalError::JournalCapacityExceeded { current_events, max_events } => {
            Status::resource_exhausted(format!(
                "journal capacity reached ({current_events} >= {max_events})"
            ))
        }
        JournalError::NetworkedWorkerExpiryOutboxCapacityExceeded {
            current_entries,
            max_entries,
        } => Status::resource_exhausted(format!(
            "networked worker expiry outbox capacity reached ({current_entries} >= {max_entries})"
        )),
        JournalError::NetworkedWorkerDispatchClaimCapacityExceeded {
            current_entries,
            max_entries,
        } => Status::resource_exhausted(format!(
            "networked worker dispatch claim capacity reached ({current_entries} >= {max_entries})"
        )),
        JournalError::NetworkedWorkerDispatchClaimConflict { remote_request_id } => {
            Status::already_exists(format!(
                "networked worker dispatch claim conflicts for request {remote_request_id}"
            ))
        }
        JournalError::NetworkedWorkerDispatchAuthorityRejected { remote_request_id } => {
            Status::failed_precondition(format!(
                "networked worker dispatch authority rejected for request {remote_request_id}"
            ))
        }
        JournalError::NetworkedWorkerDispatchSettlementRejected { remote_request_id } => {
            Status::failed_precondition(format!(
                "networked worker dispatch settlement rejected for request {remote_request_id}"
            ))
        }
        JournalError::NetworkedWorkerFleetCapacityExceeded { current_entries, max_entries } => {
            Status::resource_exhausted(format!(
                "networked worker fleet capacity reached ({current_entries} >= {max_entries})"
            ))
        }
        JournalError::NetworkedWorkerFleetGenerationConflict {
            expected_generation,
            actual_generation,
        } => Status::aborted(format!(
            "networked worker fleet generation conflict: expected {expected_generation}, found {actual_generation}"
        )),
        JournalError::SessionIdentityMismatch { session_id } => Status::failed_precondition(
            format!("orchestrator session identity mismatch for session: {session_id}"),
        ),
        JournalError::QueuedInputTransitionConflict {
            queued_input_id,
            expected_state,
            expected_revision,
            actual_state,
            actual_revision,
        } => Status::aborted(format!(
            "queued input transition conflict for {queued_input_id}: expected {expected_state}@{expected_revision}, found {actual_state}@{actual_revision}"
        )),
        JournalError::BackgroundTaskNotFound { task_id } => {
            Status::not_found(format!("background task not found: {task_id}"))
        }
        JournalError::BackgroundTaskRevisionConflict {
            task_id,
            expected_revision,
            actual_revision,
        } => Status::aborted(format!(
            "background task revision conflict for {task_id}: expected {expected_revision}, found {actual_revision}"
        )),
        JournalError::BackgroundTaskExecutionGenerationConflict {
            task_id,
            expected_generation,
            actual_generation,
        } => Status::aborted(format!(
            "background task execution generation conflict for {task_id}: expected {expected_generation}, found {actual_generation}"
        )),
        JournalError::BackgroundTaskClaimRejected { task_id, reason } => {
            Status::failed_precondition(format!(
                "background task claim rejected for {task_id}: {reason}"
            ))
        }
        JournalError::BackgroundTaskWorkerUpdateRejected { task_id, reason } => {
            Status::failed_precondition(format!(
                "background task worker update rejected for {task_id}: {reason}"
            ))
        }
        JournalError::BackgroundTaskChildConflict { reason } => Status::failed_precondition(reason),
        JournalError::ToolSideEffectFenceNotFound { operation_id } => {
            Status::not_found(format!("tool side-effect fence not found: {operation_id}"))
        }
        JournalError::ToolSideEffectFencePrecondition { operation_id, reason } => {
            Status::failed_precondition(format!(
                "tool side-effect fence precondition failed for {operation_id}: {reason}"
            ))
        }
        JournalError::SessionNotFound { selector } => {
            Status::not_found(format!("orchestrator session not found for selector: {selector}"))
        }
        JournalError::SessionWriteLeaseTimeout {
            session_id,
            lease_id,
            owner_process_id,
            owner_label,
            expires_at_unix_ms,
            requested_reason,
        } => Status::aborted(format!(
            "orchestrator session {session_id} write lease timed out while acquiring {requested_reason}; active lease {lease_id} is held by {owner_label} (pid {owner_process_id}) until {expires_at_unix_ms}"
        )),
        JournalError::CheckpointNotFound { checkpoint_kind, checkpoint_id } => {
            Status::not_found(format!("{checkpoint_kind} checkpoint not found: {checkpoint_id}"))
        }
        JournalError::InvalidSessionSelector { reason } => {
            Status::invalid_argument(format!("invalid orchestrator session selector: {reason}"))
        }
        JournalError::FlowNotFound { flow_id } => {
            Status::not_found(format!("flow not found: {flow_id}"))
        }
        JournalError::FlowStepNotFound { flow_id, step_id } => {
            Status::not_found(format!("flow step not found: {flow_id}/{step_id}"))
        }
        JournalError::FlowRevisionConflict { flow_id, expected_revision, actual_revision } => {
            Status::aborted(format!(
                "flow revision conflict for {flow_id}: expected {expected_revision}, found {actual_revision}"
            ))
        }
        JournalError::InvalidFlowDependencies { flow_id, step_id, reason_code } => {
            Status::invalid_argument(format!(
                "invalid flow dependencies for {flow_id}/{step_id}: {reason_code}"
            ))
        }
        JournalError::DuplicateWorkItemId { work_item_id } => {
            Status::already_exists(format!("work item already exists: {work_item_id}"))
        }
        JournalError::WorkItemNotFound { work_item_id } => {
            Status::not_found(format!("work item not found: {work_item_id}"))
        }
        JournalError::InvalidWorkItemTransition { work_item_id, from, to } => {
            Status::failed_precondition(format!(
                "invalid work item transition for {work_item_id}: {from} -> {to}"
            ))
        }
        JournalError::DuplicateCommitmentId { commitment_id } => {
            Status::already_exists(format!("commitment already exists: {commitment_id}"))
        }
        JournalError::CommitmentNotFound { commitment_id } => {
            Status::not_found(format!("commitment not found: {commitment_id}"))
        }
        JournalError::InvalidCommitmentTransition { commitment_id, from, to } => {
            Status::failed_precondition(format!(
                "invalid commitment transition for {commitment_id}: {from} -> {to}"
            ))
        }
        JournalError::InvalidArgument(message) => Status::invalid_argument(message),
        JournalError::DuplicateToolResultArtifactId { artifact_id } => {
            Status::already_exists(format!("tool result artifact already exists: {artifact_id}"))
        }
        JournalError::ToolResultArtifactNotFound { artifact_id } => {
            Status::not_found(format!("tool result artifact not found: {artifact_id}"))
        }
        JournalError::ToolResultArtifactDigestMismatch { artifact_id } => {
            Status::failed_precondition(format!(
                "tool result artifact digest mismatch: {artifact_id}"
            ))
        }
        JournalError::ToolResultArtifactScopeMismatch { artifact_id } => Status::permission_denied(
            format!("tool result artifact is outside the current run/session scope: {artifact_id}"),
        ),
        JournalError::ToolResultArtifactReadDenied { artifact_id, reason } => {
            Status::permission_denied(format!(
                "tool result artifact read denied for {artifact_id}: {reason}"
            ))
        }
        other => Status::internal(format!("{operation} failed: {other}")),
    }
}

/// Maps an agent-registry error to the matching gRPC status code;
/// unrecognized errors become `internal` prefixed with `operation`.
pub(crate) fn map_agent_registry_error(operation: &str, error: AgentRegistryError) -> Status {
    match error {
        AgentRegistryError::AgentNotFound(agent_id) => {
            Status::not_found(format!("agent not found: {agent_id}"))
        }
        AgentRegistryError::DuplicateAgentId(agent_id) => {
            Status::already_exists(format!("agent already exists: {agent_id}"))
        }
        AgentRegistryError::AgentDirCollision(agent_id) => Status::already_exists(format!(
            "agent directory overlaps with existing agent {agent_id}"
        )),
        AgentRegistryError::WorkspaceRootEscape(path)
        | AgentRegistryError::DuplicateWorkspaceRoot(path)
        | AgentRegistryError::InvalidSessionId(path) => Status::invalid_argument(path),
        AgentRegistryError::DefaultAgentNotConfigured => {
            Status::failed_precondition("default agent is not configured")
        }
        AgentRegistryError::InvalidPath { field, message } => {
            Status::invalid_argument(format!("{field}: {message}"))
        }
        AgentRegistryError::RegistryLimitExceeded => {
            Status::resource_exhausted("agent registry limits exceeded")
        }
        other => Status::internal(format!("{operation} failed: {other}")),
    }
}

/// Maps a cron store error to the matching gRPC status code; unrecognized
/// errors become `internal` prefixed with `operation`.
pub(crate) fn map_cron_store_error(operation: &str, error: JournalError) -> Status {
    match error {
        JournalError::CronJobNotFound { job_id } => {
            Status::not_found(format!("cron job not found: {job_id}"))
        }
        JournalError::CronJobHasActiveRuns { job_id } => {
            Status::failed_precondition(format!("cron job has an active run: {job_id}"))
        }
        JournalError::CronRunAlreadyActive { job_id, active_run_id, requested_run_id } => {
            Status::failed_precondition(format!(
                "cron job already has active run: job_id={job_id} active_run_id={active_run_id} requested_run_id={requested_run_id}"
            ))
        }
        JournalError::CronMaxRunsExhausted { job_id, max_runs, reserved_runs } => {
            Status::resource_exhausted(format!(
                "cron max_runs exhausted: job_id={job_id} reserved_runs={reserved_runs} max_runs={max_runs}"
            ))
        }
        JournalError::CronRunNotFound { run_id } => {
            Status::not_found(format!("cron run not found: {run_id}"))
        }
        JournalError::DuplicateCronJobId { job_id } => {
            Status::already_exists(format!("cron job already exists: {job_id}"))
        }
        JournalError::DuplicateCronRunId { run_id } => {
            Status::already_exists(format!("cron run already exists: {run_id}"))
        }
        JournalError::PayloadTooLarge { payload_kind, actual_bytes, max_bytes } => {
            Status::invalid_argument(format!(
                "{payload_kind} payload exceeds maximum size ({actual_bytes} > {max_bytes})"
            ))
        }
        other => Status::internal(format!("{operation} failed: {other}")),
    }
}

/// Maps an approval store error to the matching gRPC status code;
/// unrecognized errors become `internal` prefixed with `operation`.
pub(crate) fn map_approval_store_error(operation: &str, error: JournalError) -> Status {
    match error {
        JournalError::ApprovalNotFound { approval_id } => {
            Status::not_found(format!("approval record not found: {approval_id}"))
        }
        JournalError::DuplicateApprovalId { approval_id } => {
            Status::already_exists(format!("approval record already exists: {approval_id}"))
        }
        JournalError::PayloadTooLarge { payload_kind, actual_bytes, max_bytes } => {
            Status::invalid_argument(format!(
                "{payload_kind} payload exceeds maximum size ({actual_bytes} > {max_bytes})"
            ))
        }
        other => Status::internal(format!("{operation} failed: {other}")),
    }
}

/// Maps a memory/workspace-document store error to the matching gRPC status
/// code; unrecognized errors become `internal` prefixed with `operation`.
pub(crate) fn map_memory_store_error(operation: &str, error: JournalError) -> Status {
    match error {
        JournalError::MemoryNotFound { memory_id } => {
            Status::not_found(format!("memory item not found: {memory_id}"))
        }
        JournalError::DuplicateMemoryId { memory_id } => {
            Status::already_exists(format!("memory item already exists: {memory_id}"))
        }
        JournalError::DuplicateRecallArtifactId { artifact_id } => {
            Status::already_exists(format!("recall artifact already exists: {artifact_id}"))
        }
        JournalError::DuplicateWorkspacePath { path } => {
            Status::already_exists(format!("workspace document already exists for path: {path}"))
        }
        JournalError::WorkspaceDocumentNotFound { path } => {
            Status::not_found(format!("workspace document not found for path: {path}"))
        }
        JournalError::InvalidWorkspacePath { reason } => {
            Status::invalid_argument(format!("invalid workspace path: {reason}"))
        }
        JournalError::InvalidWorkspaceContent { reason } => {
            Status::invalid_argument(format!("invalid workspace content: {reason}"))
        }
        JournalError::PayloadTooLarge { payload_kind, actual_bytes, max_bytes } => {
            Status::invalid_argument(format!(
                "{payload_kind} payload exceeds maximum size ({actual_bytes} > {max_bytes})"
            ))
        }
        other => Status::internal(format!("{operation} failed: {other}")),
    }
}

/// Maps a skill store error to a gRPC status: payload-size violations become
/// `invalid_argument`, everything else `internal` prefixed with `operation`.
pub(crate) fn map_skill_store_error(operation: &str, error: JournalError) -> Status {
    match error {
        JournalError::PayloadTooLarge { payload_kind, actual_bytes, max_bytes } => {
            Status::invalid_argument(format!(
                "{payload_kind} payload exceeds maximum size ({actual_bytes} > {max_bytes})"
            ))
        }
        other => Status::internal(format!("{operation} failed: {other}")),
    }
}

/// Maps a canvas store error to the matching gRPC status code; unrecognized
/// errors become `internal` prefixed with `operation`.
pub(crate) fn map_canvas_store_error(operation: &str, error: JournalError) -> Status {
    match error {
        JournalError::DuplicateCanvasStateVersion { canvas_id, state_version } => {
            Status::already_exists(format!(
                "canvas state already exists for canvas {canvas_id} at version {state_version}"
            ))
        }
        JournalError::CanvasStateNotFound { canvas_id } => {
            Status::not_found(format!("canvas state not found: {canvas_id}"))
        }
        JournalError::InvalidCanvasReplay { canvas_id, reason } => Status::failed_precondition(
            format!("invalid canvas replay state for {canvas_id}: {reason}"),
        ),
        JournalError::PayloadTooLarge { payload_kind, actual_bytes, max_bytes } => {
            Status::invalid_argument(format!(
                "{payload_kind} payload exceeds maximum size ({actual_bytes} > {max_bytes})"
            ))
        }
        other => Status::internal(format!("{operation} failed: {other}")),
    }
}

/// Returns the current unix time in milliseconds for RPC handlers.
///
/// Near-duplicate of [`super::unix_ms_now_for_status`]; this variant predates
/// it and truncates via `as` instead of saturating (identical results until
/// the year ~292278994, so the cast is intentionally left as-is).
///
/// # Errors
/// Returns `Status::internal` when the system clock reads before the epoch.
pub(crate) fn current_unix_ms_status() -> Result<i64, Status> {
    let elapsed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| Status::internal(format!("system time before unix epoch: {error}")))?;
    Ok(elapsed.as_millis() as i64)
}

#[cfg(test)]
mod tests {
    use super::{map_memory_store_error, map_orchestrator_store_error};
    use crate::journal::JournalError;
    use tonic::Code;

    #[test]
    fn map_memory_store_error_maps_invalid_workspace_path_to_invalid_argument() {
        let status = map_memory_store_error(
            "upsert workspace document",
            JournalError::InvalidWorkspacePath { reason: "absolute paths are not allowed".into() },
        );

        assert_eq!(status.code(), Code::InvalidArgument);
        assert!(status.message().contains("invalid workspace path"));
    }

    #[test]
    fn map_memory_store_error_maps_workspace_document_not_found_to_not_found() {
        let status = map_memory_store_error(
            "get workspace document",
            JournalError::WorkspaceDocumentNotFound { path: "docs/missing.md".into() },
        );

        assert_eq!(status.code(), Code::NotFound);
        assert!(status.message().contains("docs/missing.md"));
    }

    #[test]
    fn map_memory_store_error_maps_duplicate_workspace_path_to_already_exists() {
        let status = map_memory_store_error(
            "upsert workspace document",
            JournalError::DuplicateWorkspacePath { path: "docs/guide.md".into() },
        );

        assert_eq!(status.code(), Code::AlreadyExists);
        assert!(status.message().contains("docs/guide.md"));
    }

    #[test]
    fn map_orchestrator_store_error_maps_active_session_run_to_failed_precondition() {
        let status = map_orchestrator_store_error(
            "start orchestrator run",
            JournalError::SessionRunAlreadyActive {
                session_id: "session-1".to_owned(),
                active_run_id: "run-active".to_owned(),
                requested_run_id: "run-next".to_owned(),
            },
        );

        assert_eq!(status.code(), Code::FailedPrecondition);
        assert!(status.message().contains("active run run-active"));
        assert!(status.message().contains("run-next"));
    }

    #[test]
    fn map_orchestrator_store_error_maps_session_write_lease_timeout_to_aborted() {
        let status = map_orchestrator_store_error(
            "start orchestrator run",
            JournalError::SessionWriteLeaseTimeout {
                session_id: "session-1".to_owned(),
                lease_id: "lease-1".to_owned(),
                owner_process_id: 42,
                owner_label: "journal.session_writer".to_owned(),
                expires_at_unix_ms: 1_730_000_030_000,
                requested_reason: "start_orchestrator_run".to_owned(),
            },
        );

        assert_eq!(status.code(), Code::Aborted);
        assert!(status.message().contains("session-1"));
        assert!(status.message().contains("lease-1"));
        assert!(status.message().contains("start_orchestrator_run"));
    }

    #[test]
    fn map_orchestrator_store_error_maps_background_task_authority_failures() {
        let revision = map_orchestrator_store_error(
            "update background task",
            JournalError::BackgroundTaskRevisionConflict {
                task_id: "task-1".to_owned(),
                expected_revision: 2,
                actual_revision: 3,
            },
        );
        assert_eq!(revision.code(), Code::Aborted);
        assert!(revision.message().contains("expected 2, found 3"));

        let generation = map_orchestrator_store_error(
            "settle background task",
            JournalError::BackgroundTaskExecutionGenerationConflict {
                task_id: "task-1".to_owned(),
                expected_generation: 4,
                actual_generation: 5,
            },
        );
        assert_eq!(generation.code(), Code::Aborted);
        assert!(generation.message().contains("expected 4, found 5"));

        let claim = map_orchestrator_store_error(
            "claim background task",
            JournalError::BackgroundTaskClaimRejected {
                task_id: "task-1".to_owned(),
                reason: "task is not queued".to_owned(),
            },
        );
        assert_eq!(claim.code(), Code::FailedPrecondition);

        let callback = map_orchestrator_store_error(
            "settle background task",
            JournalError::BackgroundTaskWorkerUpdateRejected {
                task_id: "task-1".to_owned(),
                reason: "cancel-requested work may settle only as cancelled".to_owned(),
            },
        );
        assert_eq!(callback.code(), Code::FailedPrecondition);
    }

    #[test]
    fn map_orchestrator_store_error_maps_missing_side_effect_fence_to_not_found() {
        let status = map_orchestrator_store_error(
            "resolve side-effect fence",
            JournalError::ToolSideEffectFenceNotFound { operation_id: "operation-1".to_owned() },
        );

        assert_eq!(status.code(), Code::NotFound);
        assert!(status.message().contains("operation-1"));
    }

    #[test]
    fn map_orchestrator_store_error_maps_side_effect_precondition() {
        let status = map_orchestrator_store_error(
            "resolve side-effect fence",
            JournalError::ToolSideEffectFencePrecondition {
                operation_id: "operation-1".to_owned(),
                reason: "intent digest no longer matches".to_owned(),
            },
        );

        assert_eq!(status.code(), Code::FailedPrecondition);
        assert!(status.message().contains("intent digest no longer matches"));
    }

    #[test]
    fn map_orchestrator_store_error_maps_checkpoint_not_found_to_not_found() {
        let status = map_orchestrator_store_error(
            "restore checkpoint",
            JournalError::CheckpointNotFound {
                checkpoint_kind: "workspace",
                checkpoint_id: "checkpoint-1".to_owned(),
            },
        );

        assert_eq!(status.code(), Code::NotFound);
        assert!(status.message().contains("workspace checkpoint not found"));
        assert!(status.message().contains("checkpoint-1"));
    }
}
