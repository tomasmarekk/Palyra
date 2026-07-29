//! Gateway adapter for managed coding lifecycle and durable completion wakes.

use std::path::PathBuf;
use std::sync::{Arc, Weak};

use serde::Deserialize;
use serde_json::json;
use sha2::{Digest as _, Sha256};
use ulid::Ulid;

use super::GatewayRuntimeState;
use crate::application::coding_runtime::{
    CodingCommandOutcomeV2, CodingCommandRequestV2, CodingPatchOutcomeV2,
    CodingPatchVerificationTicketV2, CodingRuntime, CodingRuntimeError, CodingTaskBeginRequestV2,
    CodingTaskCleanupOutcomeV2, CodingTaskHandleV2, CodingTerminalOutputV2,
    CodingWaitBarrierReceiptV2, CodingWakeBridge, CodingWakeReceiptV2, CodingWorkspaceAdmissionV2,
};
use crate::application::lsp_workspace_supervisor::LspLanguageV2;
use crate::application::managed_coding_recovery::{
    ManagedCodingRecoveryInventoryV1, ManagedCodingSnapshotGcOutcomeV1,
    ManagedCodingSnapshotSummaryV1, ManagedCodingWorktreeMutationV1,
    ManagedCodingWorktreeSummaryV1,
};
use crate::application::managed_coding_services::ManagedCodingServicesError;
use crate::application::managed_worktree_snapshots::{
    SnapshotGcDecisionV1, WorktreeRestoreReportV1, WorktreeSnapshotDescriptorV1,
};
use crate::application::process_supervisor::ProcessSessionRecordV2;
use crate::application::pty_backend::{PtyExitOutcomeV1, PtySessionDescriptorV1};
use crate::journal::wait_coordinator::{
    WaitBarrierCreateRequest, WaitBarrierKind, WakeDecision, WakeEventRequest,
};
use crate::journal::{JournalError, OrchestratorRunStartRequest};

const MAX_WAKE_IDENTITY_BYTES: usize = 128;
const MAX_CONTINUATION_PROMPT_BYTES: usize = 16 * 1024;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ManagedCodingAdmission {
    schema_version: u32,
    mode: String,
    language: String,
    #[serde(default)]
    base_ref: Option<String>,
}

#[derive(Deserialize)]
struct ManagedCodingCliContext {
    launch_cwd: Option<String>,
}

pub(crate) struct ManagedCodingProcessExecution {
    pub(crate) outcome: CodingCommandOutcomeV2,
    pub(crate) output: CodingTerminalOutputV2,
}

struct GatewayCodingWakeBridge {
    runtime: Weak<GatewayRuntimeState>,
}

impl GatewayCodingWakeBridge {
    fn runtime(&self) -> Result<Arc<GatewayRuntimeState>, String> {
        self.runtime.upgrade().ok_or_else(|| {
            "gateway runtime stopped before managed coding completion settled".to_owned()
        })
    }
}

impl CodingWakeBridge for GatewayCodingWakeBridge {
    fn register_process_wait(
        &self,
        context: &crate::application::coding_runtime::CodingObjectiveWaitContextV2,
        process: &ProcessSessionRecordV2,
    ) -> Result<CodingWaitBarrierReceiptV2, String> {
        validate_wait_context(context)?;
        let state = self.runtime()?;
        let kind = WaitBarrierKind::ProcessSession;
        let barrier = state
            .journal_store
            .register_wait_barrier(&WaitBarrierCreateRequest {
                barrier_id: Ulid::new().to_string(),
                owner_kind: "coding_objective".to_owned(),
                owner_id: context.objective_attempt_id.clone(),
                session_id: context.session_id.clone(),
                root_run_id: Some(context.root_run_id.clone()),
                barrier_kind: kind,
                source_kind: kind.as_str().to_owned(),
                source_id: process.process_session_id.clone(),
                wake_decision: WakeDecision::Run,
                continuation_prompt: Some(context.continuation_prompt.clone()),
                budget_tokens: context.budget_tokens,
                attempt_generation: context.attempt_generation,
                wake_at_unix_ms: None,
                expires_at_unix_ms: Some(context.expires_at_unix_ms),
                liveness_probe_json: json!({
                    "schema_version": 1,
                    "process_session_id": process.process_session_id,
                    "process_generation": process.process_lease.generation.get(),
                    "lease_expires_at_unix_ms": process.process_lease.expires_at_unix_ms,
                })
                .to_string(),
                active_hours_json: None,
                stale_policy: "cancel".to_owned(),
                reason_code: "coding.process_wait_registered".to_owned(),
            })
            .map_err(|error| error.to_string())?;
        Ok(CodingWaitBarrierReceiptV2 {
            barrier_id: barrier.barrier_id,
            process_session_id: process.process_session_id.clone(),
            reason_code: "coding.process_wait_registered".to_owned(),
        })
    }

    fn emit_process_completion(
        &self,
        barrier: &CodingWaitBarrierReceiptV2,
        process: &ProcessSessionRecordV2,
    ) -> Result<CodingWakeReceiptV2, String> {
        if barrier.process_session_id != process.process_session_id
            || !process.state.is_terminal()
            || !process.outcome.as_ref().is_some_and(|outcome| outcome.cleanup_verified)
        {
            return Err(
                "managed process completion lacks matching terminal cleanup evidence".to_owned()
            );
        }
        let state = self.runtime()?;
        let kind = WaitBarrierKind::ProcessSession;
        let intents = state
            .journal_store
            .emit_wake_event(&WakeEventRequest {
                source_event_id: format!(
                    "wake:coding:process:{}:{}",
                    process.process_session_id,
                    process.process_lease.generation.get()
                ),
                source_kind: kind.as_str().to_owned(),
                source_id: process.process_session_id.clone(),
                source_generation: process.process_lease.generation.get(),
                reason_code: "coding.process_completed".to_owned(),
                evidence_json: json!({
                    "schema_version": 1,
                    "process_session_id": process.process_session_id,
                    "state": process.state,
                    "cleanup_verified": true,
                })
                .to_string(),
                occurred_at_unix_ms: process
                    .outcome
                    .as_ref()
                    .map_or(process.updated_at_unix_ms, |outcome| outcome.completed_at_unix_ms),
            })
            .map_err(|error| error.to_string())?;
        Ok(CodingWakeReceiptV2 {
            barrier_id: barrier.barrier_id.clone(),
            wake_intent_count: intents.len(),
            reason_code: "coding.process_completed".to_owned(),
        })
    }

    fn register_terminal_wait(
        &self,
        context: &crate::application::coding_runtime::CodingObjectiveWaitContextV2,
        terminal: &PtySessionDescriptorV1,
    ) -> Result<CodingWaitBarrierReceiptV2, String> {
        validate_wait_context(context)?;
        if !terminal.pty_active {
            return Err("terminal is not active".to_owned());
        }
        let state = self.runtime()?;
        let kind = WaitBarrierKind::TerminalPid;
        let barrier = state
            .journal_store
            .register_wait_barrier(&WaitBarrierCreateRequest {
                barrier_id: Ulid::new().to_string(),
                owner_kind: "coding_objective".to_owned(),
                owner_id: context.objective_attempt_id.clone(),
                session_id: context.session_id.clone(),
                root_run_id: Some(context.root_run_id.clone()),
                barrier_kind: kind,
                source_kind: kind.as_str().to_owned(),
                source_id: terminal.pty_session_id.clone(),
                wake_decision: WakeDecision::Run,
                continuation_prompt: Some(context.continuation_prompt.clone()),
                budget_tokens: context.budget_tokens,
                attempt_generation: context.attempt_generation,
                wake_at_unix_ms: None,
                expires_at_unix_ms: Some(context.expires_at_unix_ms),
                liveness_probe_json: json!({
                    "schema_version": 1,
                    "pty_session_id": terminal.pty_session_id,
                    "owner_generation": terminal.owner_generation,
                    "backend": terminal.backend,
                })
                .to_string(),
                active_hours_json: None,
                stale_policy: "cancel".to_owned(),
                reason_code: "coding.terminal_wait_registered".to_owned(),
            })
            .map_err(|error| error.to_string())?;
        Ok(CodingWaitBarrierReceiptV2 {
            barrier_id: barrier.barrier_id,
            process_session_id: terminal.pty_session_id.clone(),
            reason_code: "coding.terminal_wait_registered".to_owned(),
        })
    }

    fn emit_terminal_completion(
        &self,
        barrier: &CodingWaitBarrierReceiptV2,
        terminal: &PtySessionDescriptorV1,
        outcome: &PtyExitOutcomeV1,
    ) -> Result<CodingWakeReceiptV2, String> {
        if barrier.process_session_id != terminal.pty_session_id || !outcome.cleanup_verified {
            return Err("managed terminal completion lacks matching cleanup evidence".to_owned());
        }
        let state = self.runtime()?;
        let kind = WaitBarrierKind::TerminalPid;
        let intents = state
            .journal_store
            .emit_wake_event(&WakeEventRequest {
                source_event_id: format!(
                    "wake:coding:terminal:{}:{}",
                    terminal.pty_session_id, terminal.owner_generation
                ),
                source_kind: kind.as_str().to_owned(),
                source_id: terminal.pty_session_id.clone(),
                source_generation: terminal.owner_generation,
                reason_code: "coding.terminal_completed".to_owned(),
                evidence_json: json!({
                    "schema_version": 1,
                    "pty_session_id": terminal.pty_session_id,
                    "exit_code": outcome.exit_code,
                    "cleanup_verified": true,
                })
                .to_string(),
                occurred_at_unix_ms: crate::gateway::util::current_unix_ms(),
            })
            .map_err(|error| error.to_string())?;
        Ok(CodingWakeReceiptV2 {
            barrier_id: barrier.barrier_id.clone(),
            wake_intent_count: intents.len(),
            reason_code: "coding.terminal_completed".to_owned(),
        })
    }
}

impl GatewayRuntimeState {
    pub(super) fn install_managed_coding_wake_bridge(self: &Arc<Self>) -> Result<(), JournalError> {
        let Some(runtime) = self.managed_coding_runtime() else {
            return Ok(());
        };
        runtime
            .install_wake_bridge(Arc::new(GatewayCodingWakeBridge {
                runtime: Arc::downgrade(self),
            }))
            .map_err(|error| JournalError::InvalidArgument(error.to_string()))
    }

    pub(crate) fn managed_coding_runtime(&self) -> Option<Arc<CodingRuntime>> {
        self.managed_coding_services.as_ref().map(|services| Arc::clone(services.runtime()))
    }

    pub(crate) fn managed_coding_diagnostics_snapshot(&self) -> serde_json::Value {
        self.managed_coding_services.as_ref().map_or_else(
            || {
                json!({
                    "schema_version": 1,
                    "status": "unavailable",
                    "reason_code": "coding.runtime_unavailable",
                })
            },
            |services| {
                serde_json::to_value(services.diagnostics_snapshot()).unwrap_or_else(|_| {
                    json!({
                        "schema_version": 1,
                        "status": "unavailable",
                        "reason_code": "coding.diagnostics_serialization_failed",
                    })
                })
            },
        )
    }

    pub(crate) fn managed_coding_workspace_root(&self, run_id: &str) -> Option<PathBuf> {
        self.managed_coding_runtime()?
            .task_handle_for_run(run_id)
            .ok()
            .flatten()
            .map(|handle| handle.workspace_root)
    }

    pub(crate) fn has_managed_coding_run(&self, run_id: &str) -> bool {
        self.managed_coding_runtime()
            .and_then(|runtime| runtime.task_handle_for_run(run_id).ok().flatten())
            .is_some()
    }

    pub(crate) fn prepare_managed_coding_patch(
        &self,
        run_id: &str,
        relative_paths: &[PathBuf],
    ) -> Result<Option<CodingPatchVerificationTicketV2>, CodingRuntimeError> {
        let Some(runtime) = self.managed_coding_runtime() else {
            return Ok(None);
        };
        let Some(task) = runtime.task_handle_for_run(run_id)? else {
            return Ok(None);
        };
        runtime.prepare_patch_verification(task.task_id.as_str(), relative_paths).map(Some)
    }

    pub(crate) fn complete_managed_coding_patch(
        &self,
        ticket_id: &str,
    ) -> Result<Option<CodingPatchOutcomeV2>, CodingRuntimeError> {
        let Some(runtime) = self.managed_coding_runtime() else {
            return Ok(None);
        };
        runtime.complete_patch_verification(ticket_id).map(Some)
    }

    pub(crate) fn cancel_managed_coding_patch(&self, ticket_id: &str) {
        if let Some(runtime) = self.managed_coding_runtime() {
            runtime.cancel_patch_verification(ticket_id);
        }
    }

    pub(super) fn admit_managed_coding_run(
        &self,
        request: &OrchestratorRunStartRequest,
    ) -> Result<Option<CodingTaskHandleV2>, CodingRuntimeError> {
        let selection = match managed_coding_admission(request)? {
            Some(selection) => Some(selection),
            None => managed_coding_policy_admission(
                self.config.code_intel.enabled,
                self.config.code_intel.workspace_root.as_deref(),
            ),
        };
        let Some((admission, source_repo, workspace_admission)) = selection else {
            return Ok(None);
        };
        let runtime = self
            .managed_coding_runtime()
            .ok_or(CodingRuntimeError::WorkspaceIsolationUnavailable)?;
        let language = match admission.language.as_str() {
            "rust" => LspLanguageV2::Rust,
            "typescript" => LspLanguageV2::TypeScript,
            "python" => LspLanguageV2::Python,
            _ => {
                return Err(CodingRuntimeError::InvalidRequest(
                    "managed coding language is unsupported".to_owned(),
                ));
            }
        };
        let task_id = format!("coding_{}", Ulid::new());
        runtime
            .begin_task(CodingTaskBeginRequestV2 {
                task_id,
                session_id: request.session_id.clone(),
                run_id: request.run_id.clone(),
                workspace_admission,
                source_repo,
                base_ref: admission.base_ref.unwrap_or_else(|| "HEAD".to_owned()),
                branch_slug: "managed-run".to_owned(),
                language,
            })
            .map(Some)
    }

    pub(crate) fn cleanup_managed_coding_run(
        &self,
        run_id: &str,
    ) -> Result<Option<CodingTaskCleanupOutcomeV2>, CodingRuntimeError> {
        let Some(runtime) = self.managed_coding_runtime() else {
            return Ok(None);
        };
        let Some(task) = runtime.task_handle_for_run(run_id)? else {
            return Ok(None);
        };
        runtime.cleanup_task(task.task_id.as_str()).map(Some)
    }

    pub(crate) fn run_matching_managed_coding_command(
        &self,
        run_id: &str,
        command: &str,
        args: &[String],
    ) -> Result<Option<ManagedCodingProcessExecution>, CodingRuntimeError> {
        let Some(runtime) = self.managed_coding_runtime() else {
            return Ok(None);
        };
        let Some(task) = runtime.task_handle_for_run(run_id)? else {
            return Ok(None);
        };
        let Some(policy) = runtime.matching_command_policy(command, args) else {
            return Ok(None);
        };
        if let Some(services) = self.managed_coding_services.as_ref() {
            services.relieve_pressure(policy.resource_units).map_err(|error| {
                CodingRuntimeError::Process(format!(
                    "resource pressure coordination failed: {error}"
                ))
            })?;
        }
        let outcome = runtime.run_command(CodingCommandRequestV2 {
            task_id: task.task_id,
            command_id: policy.command_id,
            objective_wait: None,
        })?;
        let output = runtime.command_output(outcome.execution_id.as_str(), None, 512)?;
        Ok(Some(ManagedCodingProcessExecution { outcome, output }))
    }

    pub(crate) fn shutdown_managed_coding_services(
        &self,
    ) -> Result<(), ManagedCodingServicesError> {
        match self.managed_coding_services.as_ref() {
            Some(services) => services.shutdown(),
            None => Ok(()),
        }
    }

    pub(crate) async fn managed_coding_recovery_inventory(
        self: &Arc<Self>,
    ) -> Result<ManagedCodingRecoveryInventoryV1, tonic::Status> {
        let services =
            self.managed_coding_services.as_ref().cloned().ok_or_else(|| {
                tonic::Status::failed_precondition("managed coding is unavailable")
            })?;
        tokio::task::spawn_blocking(move || {
            let worktrees = services
                .worktree_records()
                .map_err(managed_coding_recovery_status)?
                .into_iter()
                .map(|record| ManagedCodingWorktreeSummaryV1 {
                    schema_version: 1,
                    worktree_id: record.worktree_id,
                    generation: record.generation,
                    source_repo_sha256: sha256_path(record.source_repo.as_path()),
                    worktree_path_sha256: sha256_path(record.worktree_path.as_path()),
                    branch: record.branch,
                    base_ref: record.base_ref,
                    lifecycle: record.lifecycle,
                    dirty: record.dirty,
                    locked: record.locked_by_run.is_some(),
                    attached_run_count: record.attached_run_ids.len(),
                    created_at_unix_ms: record.created_at_unix_ms,
                    updated_at_unix_ms: record.updated_at_unix_ms,
                    reason_code: record.reason_code,
                })
                .collect::<Vec<_>>();
            let snapshots = services
                .snapshot_descriptors()
                .map_err(managed_coding_recovery_status)?
                .into_iter()
                .map(|descriptor| ManagedCodingSnapshotSummaryV1 {
                    schema_version: 1,
                    snapshot_id: descriptor.snapshot_id,
                    worktree_id: descriptor.worktree_id,
                    worktree_generation: descriptor.worktree_generation,
                    base_commit: descriptor.base_commit,
                    entry_count: descriptor.entries.len(),
                    total_bytes: descriptor.total_bytes,
                    created_at_unix_ms: descriptor.created_at_unix_ms,
                })
                .collect::<Vec<_>>();
            Ok(ManagedCodingRecoveryInventoryV1 {
                schema_version: 1,
                worktrees,
                snapshots,
                reason_code: "coding.recovery_inventory_loaded".to_owned(),
            })
        })
        .await
        .map_err(|_| tonic::Status::internal("managed coding recovery worker panicked"))?
    }

    pub(crate) async fn managed_coding_snapshot_descriptor(
        self: &Arc<Self>,
        snapshot_id: String,
    ) -> Result<WorktreeSnapshotDescriptorV1, tonic::Status> {
        let services =
            self.managed_coding_services.as_ref().cloned().ok_or_else(|| {
                tonic::Status::failed_precondition("managed coding is unavailable")
            })?;
        tokio::task::spawn_blocking(move || {
            services
                .snapshot_descriptor(snapshot_id.as_str())
                .map_err(managed_coding_recovery_status)
        })
        .await
        .map_err(|_| tonic::Status::internal("managed coding recovery worker panicked"))?
    }

    pub(crate) async fn restore_managed_coding_snapshot(
        self: &Arc<Self>,
        snapshot_id: String,
    ) -> Result<WorktreeRestoreReportV1, tonic::Status> {
        let services =
            self.managed_coding_services.as_ref().cloned().ok_or_else(|| {
                tonic::Status::failed_precondition("managed coding is unavailable")
            })?;
        tokio::task::spawn_blocking(move || {
            services.restore_snapshot(snapshot_id.as_str()).map_err(managed_coding_recovery_status)
        })
        .await
        .map_err(|_| tonic::Status::internal("managed coding recovery worker panicked"))?
    }

    pub(crate) async fn retain_managed_coding_worktree(
        self: &Arc<Self>,
        worktree_id: String,
        generation: u64,
    ) -> Result<ManagedCodingWorktreeMutationV1, tonic::Status> {
        let services =
            self.managed_coding_services.as_ref().cloned().ok_or_else(|| {
                tonic::Status::failed_precondition("managed coding is unavailable")
            })?;
        tokio::task::spawn_blocking(move || {
            let record = services
                .retain_worktree(worktree_id.as_str(), generation)
                .map_err(managed_coding_recovery_status)?;
            Ok(ManagedCodingWorktreeMutationV1 {
                schema_version: 1,
                worktree_id: record.worktree_id,
                generation: record.generation,
                lifecycle: record.lifecycle,
                dirty: record.dirty,
                reason_code: record.reason_code,
            })
        })
        .await
        .map_err(|_| tonic::Status::internal("managed coding recovery worker panicked"))?
    }

    pub(crate) async fn reconcile_managed_coding_worktree(
        self: &Arc<Self>,
        worktree_id: String,
        run_id: String,
    ) -> Result<CodingTaskCleanupOutcomeV2, tonic::Status> {
        let services =
            self.managed_coding_services.as_ref().cloned().ok_or_else(|| {
                tonic::Status::failed_precondition("managed coding is unavailable")
            })?;
        tokio::task::spawn_blocking(move || {
            services
                .reconcile_interrupted_worktree(worktree_id.as_str(), run_id.as_str())
                .map_err(managed_coding_recovery_status)
        })
        .await
        .map_err(|_| tonic::Status::internal("managed coding recovery worker panicked"))?
    }

    pub(crate) async fn gc_managed_coding_snapshot(
        self: &Arc<Self>,
        snapshot_id: String,
        force: bool,
    ) -> Result<ManagedCodingSnapshotGcOutcomeV1, tonic::Status> {
        let services =
            self.managed_coding_services.as_ref().cloned().ok_or_else(|| {
                tonic::Status::failed_precondition("managed coding is unavailable")
            })?;
        tokio::task::spawn_blocking(move || {
            let decision = services
                .gc_snapshot(snapshot_id.as_str(), force)
                .map_err(managed_coding_recovery_status)?;
            Ok(ManagedCodingSnapshotGcOutcomeV1 {
                schema_version: 1,
                snapshot_id,
                decision,
                force_requested: force,
                reason_code: match decision {
                    SnapshotGcDecisionV1::Removed => "coding.snapshot_removed",
                    SnapshotGcDecisionV1::BlockedByActiveLease => "coding.snapshot_active_lease",
                    SnapshotGcDecisionV1::Retained => "coding.snapshot_retained",
                }
                .to_owned(),
            })
        })
        .await
        .map_err(|_| tonic::Status::internal("managed coding recovery worker panicked"))?
    }
}

fn managed_coding_recovery_status(error: ManagedCodingServicesError) -> tonic::Status {
    tonic::Status::failed_precondition(error.to_string())
}

fn sha256_path(path: &std::path::Path) -> String {
    hex::encode(Sha256::digest(path.as_os_str().as_encoded_bytes()))
}

fn managed_coding_admission(
    request: &OrchestratorRunStartRequest,
) -> Result<Option<(ManagedCodingAdmission, PathBuf, CodingWorkspaceAdmissionV2)>, CodingRuntimeError>
{
    let Some(parameter_delta_json) = request.parameter_delta_json.as_deref() else {
        return Ok(None);
    };
    let value = serde_json::from_str::<serde_json::Value>(parameter_delta_json).map_err(|_| {
        CodingRuntimeError::InvalidRequest("run parameter delta is invalid JSON".to_owned())
    })?;
    let Some(raw_admission) = value.get("coding_workspace") else {
        return Ok(None);
    };
    let admission = serde_json::from_value::<ManagedCodingAdmission>(raw_admission.clone())
        .map_err(|_| {
            CodingRuntimeError::InvalidRequest(
                "managed coding workspace request is invalid".to_owned(),
            )
        })?;
    if admission.schema_version != 1 || admission.mode != "managed" {
        return Err(CodingRuntimeError::InvalidRequest(
            "managed coding workspace schema or mode is unsupported".to_owned(),
        ));
    }
    let cli_context = value
        .get("cli_context")
        .cloned()
        .ok_or_else(|| {
            CodingRuntimeError::InvalidRequest(
                "managed coding workspace requires CLI launch context".to_owned(),
            )
        })
        .and_then(|value| {
            serde_json::from_value::<ManagedCodingCliContext>(value).map_err(|_| {
                CodingRuntimeError::InvalidRequest(
                    "managed coding CLI launch context is invalid".to_owned(),
                )
            })
        })?;
    let launch_cwd = cli_context.launch_cwd.ok_or_else(|| {
        CodingRuntimeError::InvalidRequest(
            "managed coding workspace requires launch_cwd".to_owned(),
        )
    })?;
    let source_repo =
        crate::application::tool_runtime::workspace_scope::canonical_launch_workspace_root(
            launch_cwd.as_str(),
        )
        .ok_or_else(|| {
            CodingRuntimeError::InvalidRequest(
                "managed coding launch_cwd is not a safe workspace".to_owned(),
            )
        })?;
    Ok(Some((admission, source_repo, CodingWorkspaceAdmissionV2::Explicit)))
}

fn managed_coding_policy_admission(
    code_intel_enabled: bool,
    workspace_root: Option<&std::path::Path>,
) -> Option<(ManagedCodingAdmission, PathBuf, CodingWorkspaceAdmissionV2)> {
    if !code_intel_enabled {
        return None;
    }
    let workspace_root = workspace_root?.canonicalize().ok()?;
    let language = if workspace_root.join("Cargo.toml").is_file() {
        "rust"
    } else if workspace_root.join("tsconfig.json").is_file()
        || workspace_root.join("package.json").is_file()
    {
        "typescript"
    } else if workspace_root.join("pyproject.toml").is_file()
        || workspace_root.join("setup.py").is_file()
        || workspace_root.join("requirements.txt").is_file()
    {
        "python"
    } else {
        return None;
    };
    Some((
        ManagedCodingAdmission {
            schema_version: 1,
            mode: "managed".to_owned(),
            language: language.to_owned(),
            base_ref: Some("HEAD".to_owned()),
        },
        workspace_root,
        CodingWorkspaceAdmissionV2::Policy,
    ))
}

fn validate_wait_context(
    context: &crate::application::coding_runtime::CodingObjectiveWaitContextV2,
) -> Result<(), String> {
    for (name, value) in [
        ("objective_attempt_id", context.objective_attempt_id.as_str()),
        ("session_id", context.session_id.as_str()),
        ("root_run_id", context.root_run_id.as_str()),
    ] {
        if value.trim().is_empty()
            || value.len() > MAX_WAKE_IDENTITY_BYTES
            || value.chars().any(char::is_control)
        {
            return Err(format!("{name} is invalid for managed coding wake registration"));
        }
    }
    if context.attempt_generation == 0
        || context.budget_tokens == 0
        || context.expires_at_unix_ms <= crate::gateway::util::current_unix_ms()
        || context.continuation_prompt.trim().is_empty()
        || context.continuation_prompt.len() > MAX_CONTINUATION_PROMPT_BYTES
    {
        return Err("managed coding wait policy is invalid".to_owned());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn configured_code_intelligence_workspace_selects_managed_policy_admission() {
        let temp = tempfile::tempdir().expect("temp workspace");
        std::fs::write(temp.path().join("Cargo.toml"), "[package]\nname = \"fixture\"\n")
            .expect("write Cargo manifest");

        let (_, selected_root, admission) =
            managed_coding_policy_admission(true, Some(temp.path())).expect("policy admission");
        assert_eq!(selected_root, temp.path().canonicalize().expect("canonical workspace"));
        assert_eq!(admission, CodingWorkspaceAdmissionV2::Policy);
    }

    #[test]
    fn policy_admission_requires_enabled_code_intelligence_and_known_language() {
        let temp = tempfile::tempdir().expect("temp workspace");
        assert!(managed_coding_policy_admission(false, Some(temp.path())).is_none());
        assert!(managed_coding_policy_admission(true, Some(temp.path())).is_none());
    }
}
