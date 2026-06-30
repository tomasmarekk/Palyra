//! Unified task read model over existing runtime journals.
//!
//! TaskRuntime is intentionally read-mostly: it normalizes background tasks,
//! flows, tool jobs, WorkBoard items, and commitments into one operator-facing
//! timeline while leaving execution ownership in the source subsystem.

use std::sync::Arc;

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tonic::Status;

use crate::{
    acceptance::commitment_acceptance_projection,
    application::plan_state::{
        AgentPlanEvent, AgentPlanItem, AgentPlanQuery, AgentPlanStatus, AgentPlanStore,
        AGENT_PLAN_SCHEMA_VERSION,
    },
    gateway::GatewayRuntimeState,
    journal::{
        self, CommitmentEventRecord, CommitmentListFilter, CommitmentRecord, FlowEventRecord,
        FlowListFilter, FlowRecord, OrchestratorBackgroundTaskListFilter,
        OrchestratorBackgroundTaskRecord, ToolJobRecord, ToolJobsListFilter, WorkItemEventRecord,
        WorkItemListFilter, WorkItemRecord,
    },
};

const TASK_RUNTIME_LIMIT: usize = 200;
const TASK_EVENT_LIMIT: usize = 512;

/// Source store represented by a normalized task id.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum TaskSourceKind {
    BackgroundTask,
    Flow,
    ToolJob,
    WorkItem,
    Commitment,
    AgentPlanItem,
}

impl TaskSourceKind {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::BackgroundTask => "background_task",
            Self::Flow => "flow",
            Self::ToolJob => "tool_job",
            Self::WorkItem => "work_item",
            Self::Commitment => "commitment",
            Self::AgentPlanItem => "agent_plan_item",
        }
    }

    const fn prefix(self) -> &'static str {
        match self {
            Self::BackgroundTask => "background",
            Self::Flow => "flow",
            Self::ToolJob => "tool_job",
            Self::WorkItem => "work_item",
            Self::Commitment => "commitment",
            Self::AgentPlanItem => "agent_plan",
        }
    }
}

/// Principal/device/channel boundary used before returning task data.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct TaskAccessPolicy {
    pub owner_principal: String,
    pub device_id: Option<String>,
    pub channel: Option<String>,
}

impl TaskAccessPolicy {
    pub(crate) fn allows(&self, owner: &str, device_id: &str, channel: Option<&str>) -> bool {
        self.owner_principal == owner
            && self.device_id.as_deref().is_none_or(|expected| expected == device_id)
            && self.channel.as_deref().is_none_or(|expected| Some(expected) == channel)
    }
}

/// Query filter for the normalized read model.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TaskRuntimeFilter {
    pub access: TaskAccessPolicy,
    pub state: Option<String>,
    pub include_terminal: bool,
    pub limit: usize,
}

/// Operator-facing task record, with payload-like fields already redacted.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct TaskRun {
    pub task_id: String,
    pub source_id: String,
    pub source_kind: String,
    pub owner_principal: String,
    pub device_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub channel: Option<String>,
    pub owner: TaskOwner,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub run_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub objective_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub routine_id: Option<String>,
    pub state: String,
    pub title: String,
    pub summary: String,
    pub priority: i64,
    pub steps: Vec<TaskStep>,
    pub artifacts: Vec<TaskArtifact>,
    pub retry_policy: TaskRetryPolicy,
    pub artifact_refs_json: String,
    pub retry_policy_json: String,
    pub access_policy_json: String,
    pub created_at_unix_ms: i64,
    pub updated_at_unix_ms: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub started_at_unix_ms: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub heartbeat_at_unix_ms: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub completed_at_unix_ms: Option<i64>,
}

/// Durable task owner identity and scope.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct TaskOwner {
    pub principal: String,
    pub device_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub channel: Option<String>,
}

/// One task step in the normalized model.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct TaskStep {
    pub step_id: String,
    pub state: String,
    pub title: String,
    pub source_kind: String,
    pub source_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub started_at_unix_ms: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub completed_at_unix_ms: Option<i64>,
}

/// Artifact reference visible from the normalized task model.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct TaskArtifact {
    pub artifact_id: String,
    pub artifact_kind: String,
    pub visibility: String,
}

/// Retry posture for a normalized task.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct TaskRetryPolicy {
    pub attempt_count: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_attempts: Option<u64>,
    pub retry_allowed: bool,
    pub policy_json: String,
}

/// Normalized task lifecycle event.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct TaskTimelineEvent {
    pub event_id: String,
    pub task_id: String,
    pub source_kind: String,
    pub event_type: String,
    pub actor_principal: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub from_state: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub to_state: Option<String>,
    pub summary: String,
    pub payload_json: String,
    pub created_at_unix_ms: i64,
}

/// Summary counters for the current task view.
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
pub(crate) struct TaskRuntimeSummary {
    pub total: usize,
    pub active: usize,
    pub blocked: usize,
    pub failed: usize,
    pub terminal: usize,
}

/// Snapshot returned by `GET /console/v1/tasks`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct TaskRuntimeSnapshot {
    pub tasks: Vec<TaskRun>,
    pub summary: TaskRuntimeSummary,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub plan_progress: Option<TaskPlanProgressCheckpoint>,
}

/// Read-model checkpoint summarizing durable agent-plan progress.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct TaskPlanProgressCheckpoint {
    pub schema_version: u64,
    pub plan_schema_version: u64,
    pub rollout_enabled: bool,
    pub source_kind: String,
    pub reason_code: String,
    pub total: usize,
    pub active: usize,
    pub blocked: usize,
    pub completed: usize,
    pub cancelled: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub latest_update_unix_ms: Option<i64>,
}

/// Stateless facade over the runtime's source stores.
pub(crate) struct TaskRuntime;

impl TaskRuntime {
    /// Builds a normalized task snapshot for the authorized scope.
    ///
    /// # Errors
    /// Propagates runtime storage errors and redaction failures.
    #[allow(clippy::result_large_err)]
    pub(crate) async fn snapshot(
        runtime: &Arc<GatewayRuntimeState>,
        filter: TaskRuntimeFilter,
    ) -> Result<TaskRuntimeSnapshot, Status> {
        let limit = filter.limit.clamp(1, TASK_RUNTIME_LIMIT);
        let owner = Some(filter.access.owner_principal.clone());
        let device_id = filter.access.device_id.clone();
        let channel = filter.access.channel.clone();

        let mut tasks = Vec::new();
        for task in runtime
            .list_orchestrator_background_tasks(OrchestratorBackgroundTaskListFilter {
                owner_principal: owner.clone(),
                device_id: device_id.clone(),
                channel: channel.clone(),
                session_id: None,
                include_completed: filter.include_terminal,
                limit,
            })
            .await?
        {
            push_if_visible(&mut tasks, background_task_run(task)?, &filter);
        }

        for flow in runtime
            .list_flows(FlowListFilter {
                owner_principal: owner.clone(),
                device_id: device_id.clone(),
                channel: channel.clone(),
                state: None,
                include_terminal: filter.include_terminal,
                limit,
            })
            .await?
        {
            push_if_visible(&mut tasks, flow_task_run(flow)?, &filter);
        }

        for job in runtime
            .list_tool_jobs(ToolJobsListFilter {
                owner_principal: owner.clone(),
                include_terminal: filter.include_terminal,
                limit,
                ..ToolJobsListFilter::default()
            })
            .await?
        {
            if filter.access.allows(
                job.owner_principal.as_str(),
                job.device_id.as_str(),
                job.channel.as_deref(),
            ) {
                push_if_visible(&mut tasks, tool_job_task_run(job)?, &filter);
            }
        }

        for item in runtime
            .list_work_items(WorkItemListFilter {
                owner_principal: owner.clone(),
                device_id: device_id.clone(),
                channel: channel.clone(),
                state: None,
                include_terminal: filter.include_terminal,
                limit,
            })
            .await?
        {
            push_if_visible(&mut tasks, work_item_task_run(item)?, &filter);
        }

        for commitment in runtime
            .list_commitments(CommitmentListFilter {
                owner_principal: owner,
                device_id,
                channel,
                status: None,
                due_before_unix_ms: None,
                include_terminal: filter.include_terminal,
                limit,
            })
            .await?
        {
            push_if_visible(&mut tasks, commitment_task_run(commitment)?, &filter);
        }

        let mut plan_progress = None;
        if runtime.config.feature_rollouts.agent_plan_state.enabled {
            let plan_items = list_agent_plan_items(runtime, &filter, limit).await?;
            plan_progress = Some(plan_progress_checkpoint(plan_items.as_slice(), true));
            for item in plan_items {
                push_if_visible(&mut tasks, agent_plan_task_run(item)?, &filter);
            }
        }

        tasks.sort_by(|left, right| {
            right
                .updated_at_unix_ms
                .cmp(&left.updated_at_unix_ms)
                .then_with(|| right.created_at_unix_ms.cmp(&left.created_at_unix_ms))
                .then_with(|| left.task_id.cmp(&right.task_id))
        });
        tasks.truncate(limit);
        let summary = summarize_tasks(tasks.as_slice());
        Ok(TaskRuntimeSnapshot { tasks, summary, plan_progress })
    }

    /// Loads one normalized task by id.
    ///
    /// # Errors
    /// Propagates runtime storage errors and redaction failures.
    #[allow(clippy::result_large_err)]
    pub(crate) async fn get(
        runtime: &Arc<GatewayRuntimeState>,
        access: &TaskAccessPolicy,
        task_id: &str,
    ) -> Result<Option<TaskRun>, Status> {
        let Some(parsed) = ParsedTaskId::parse(task_id) else {
            return Ok(None);
        };
        let task = match parsed.kind {
            TaskSourceKind::BackgroundTask => runtime
                .get_orchestrator_background_task(parsed.source_id.to_owned())
                .await?
                .map(background_task_run)
                .transpose()?,
            TaskSourceKind::Flow => runtime
                .get_flow_bundle(parsed.source_id.to_owned(), 1)
                .await?
                .map(|bundle| flow_task_run(bundle.flow))
                .transpose()?,
            TaskSourceKind::ToolJob => runtime
                .get_tool_job(parsed.source_id.to_owned())
                .await?
                .map(tool_job_task_run)
                .transpose()?,
            TaskSourceKind::WorkItem => runtime
                .get_work_item(parsed.source_id.to_owned())
                .await?
                .map(work_item_task_run)
                .transpose()?,
            TaskSourceKind::Commitment => runtime
                .get_commitment(parsed.source_id.to_owned())
                .await?
                .map(commitment_task_run)
                .transpose()?,
            TaskSourceKind::AgentPlanItem => get_agent_plan_item(runtime, parsed.source_id)
                .await?
                .map(agent_plan_task_run)
                .transpose()?,
        };
        Ok(task.filter(|task| {
            access.allows(
                task.owner_principal.as_str(),
                task.device_id.as_str(),
                task.channel.as_deref(),
            )
        }))
    }

    /// Loads normalized timeline events for one task id.
    ///
    /// # Errors
    /// Propagates runtime storage errors and redaction failures.
    #[allow(clippy::result_large_err)]
    pub(crate) async fn timeline(
        runtime: &Arc<GatewayRuntimeState>,
        access: &TaskAccessPolicy,
        task_id: &str,
    ) -> Result<Vec<TaskTimelineEvent>, Status> {
        let Some(task) = Self::get(runtime, access, task_id).await? else {
            return Ok(Vec::new());
        };
        let source_id = task.source_id.as_str();
        match task.source_kind.as_str() {
            "flow" => {
                let Some(bundle) =
                    runtime.get_flow_bundle(source_id.to_owned(), TASK_EVENT_LIMIT).await?
                else {
                    return Ok(Vec::new());
                };
                bundle.events.into_iter().map(flow_event).collect()
            }
            "work_item" => runtime
                .list_work_item_events(source_id.to_owned(), TASK_EVENT_LIMIT)
                .await?
                .into_iter()
                .map(work_item_event)
                .collect(),
            "commitment" => runtime
                .list_commitment_events(source_id.to_owned(), TASK_EVENT_LIMIT)
                .await?
                .into_iter()
                .map(commitment_event)
                .collect(),
            "agent_plan_item" => list_agent_plan_events(runtime, source_id, TASK_EVENT_LIMIT)
                .await?
                .into_iter()
                .map(agent_plan_event)
                .collect(),
            _ => Ok(vec![synthetic_current_state_event(&task)?]),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ParsedTaskId<'a> {
    kind: TaskSourceKind,
    source_id: &'a str,
}

impl<'a> ParsedTaskId<'a> {
    fn parse(task_id: &'a str) -> Option<Self> {
        let (prefix, source_id) = task_id.split_once(':')?;
        if source_id.trim().is_empty() {
            return None;
        }
        let kind = match prefix {
            "background" => TaskSourceKind::BackgroundTask,
            "flow" => TaskSourceKind::Flow,
            "tool_job" => TaskSourceKind::ToolJob,
            "work_item" => TaskSourceKind::WorkItem,
            "commitment" => TaskSourceKind::Commitment,
            "agent_plan" => TaskSourceKind::AgentPlanItem,
            _ => return None,
        };
        Some(Self { kind, source_id })
    }
}

fn push_if_visible(tasks: &mut Vec<TaskRun>, task: TaskRun, filter: &TaskRuntimeFilter) {
    if !filter.include_terminal && is_terminal_task_state(task.state.as_str()) {
        return;
    }
    if filter.state.as_deref().is_some_and(|state| state != task.state) {
        return;
    }
    tasks.push(task);
}

#[allow(clippy::result_large_err)]
async fn list_agent_plan_items(
    runtime: &Arc<GatewayRuntimeState>,
    filter: &TaskRuntimeFilter,
    limit: usize,
) -> Result<Vec<AgentPlanItem>, Status> {
    let state = Arc::clone(runtime);
    let query = AgentPlanQuery {
        owner_principal: Some(filter.access.owner_principal.clone()),
        device_id: filter.access.device_id.clone(),
        channel: filter.access.channel.clone(),
        session_id: None,
        run_id: None,
        status: None,
        include_terminal: true,
        limit,
    };
    tokio::task::spawn_blocking(move || {
        let store = AgentPlanStore::new(&state.journal_store);
        store.list_items(&query)
    })
    .await
    .map_err(|_| Status::internal("agent plan task list worker panicked"))?
    .map_err(|error| Status::internal(format!("failed to list agent plan tasks: {error}")))
}

#[allow(clippy::result_large_err)]
async fn get_agent_plan_item(
    runtime: &Arc<GatewayRuntimeState>,
    plan_item_id: &str,
) -> Result<Option<AgentPlanItem>, Status> {
    if !runtime.config.feature_rollouts.agent_plan_state.enabled {
        return Ok(None);
    }
    let state = Arc::clone(runtime);
    let plan_item_id = plan_item_id.to_owned();
    tokio::task::spawn_blocking(move || {
        let store = AgentPlanStore::new(&state.journal_store);
        store.get_item(plan_item_id.as_str())
    })
    .await
    .map_err(|_| Status::internal("agent plan task get worker panicked"))?
    .map_err(|error| Status::internal(format!("failed to load agent plan task: {error}")))
}

#[allow(clippy::result_large_err)]
async fn list_agent_plan_events(
    runtime: &Arc<GatewayRuntimeState>,
    plan_item_id: &str,
    limit: usize,
) -> Result<Vec<AgentPlanEvent>, Status> {
    let state = Arc::clone(runtime);
    let plan_item_id = plan_item_id.to_owned();
    tokio::task::spawn_blocking(move || {
        let store = AgentPlanStore::new(&state.journal_store);
        store.list_events(plan_item_id.as_str(), limit)
    })
    .await
    .map_err(|_| Status::internal("agent plan task timeline worker panicked"))?
    .map_err(|error| Status::internal(format!("failed to load agent plan timeline: {error}")))
}

fn background_task_run(task: OrchestratorBackgroundTaskRecord) -> Result<TaskRun, Status> {
    let source_id = task.task_id.clone();
    let state = task.state.clone();
    let title = task.task_kind.clone();
    let retry_policy = json!({
        "attempt_count": task.attempt_count,
        "max_attempts": task.max_attempts,
        "budget_tokens": task.budget_tokens
    });
    let retry_policy_json = retry_policy.to_string();
    Ok(TaskRun {
        task_id: normalized_task_id(TaskSourceKind::BackgroundTask, source_id.as_str()),
        source_id: task.task_id,
        source_kind: TaskSourceKind::BackgroundTask.as_str().to_owned(),
        owner_principal: task.owner_principal.clone(),
        device_id: task.device_id.clone(),
        channel: task.channel.clone(),
        owner: task_owner(&task.owner_principal, &task.device_id, task.channel.clone()),
        session_id: Some(task.session_id),
        run_id: task.parent_run_id.or(task.target_run_id),
        objective_id: None,
        routine_id: None,
        state: task.state,
        title: task.task_kind.clone(),
        summary: task.last_error.clone().unwrap_or(task.task_kind),
        priority: task.priority,
        steps: single_step(
            TaskSourceKind::BackgroundTask,
            source_id.as_str(),
            state.as_str(),
            title.as_str(),
            task.started_at_unix_ms,
            task.completed_at_unix_ms,
        ),
        artifacts: Vec::new(),
        retry_policy: TaskRetryPolicy {
            attempt_count: task.attempt_count,
            max_attempts: Some(task.max_attempts),
            retry_allowed: task.attempt_count < task.max_attempts,
            policy_json: retry_policy_json.clone(),
        },
        artifact_refs_json: redact_optional_json(task.result_json.as_deref())?,
        retry_policy_json,
        access_policy_json: json!({ "owner_only": true }).to_string(),
        created_at_unix_ms: task.created_at_unix_ms,
        updated_at_unix_ms: task.updated_at_unix_ms,
        started_at_unix_ms: task.started_at_unix_ms,
        heartbeat_at_unix_ms: None,
        completed_at_unix_ms: task.completed_at_unix_ms,
    })
}

fn flow_task_run(flow: FlowRecord) -> Result<TaskRun, Status> {
    let source_id = flow.flow_id.clone();
    let state = flow.state.clone();
    let title = flow.title.clone();
    let retry_policy_json = redact_json(flow.retry_policy_json.as_str())?;
    Ok(TaskRun {
        task_id: normalized_task_id(TaskSourceKind::Flow, source_id.as_str()),
        source_id: flow.flow_id,
        source_kind: TaskSourceKind::Flow.as_str().to_owned(),
        owner_principal: flow.owner_principal.clone(),
        device_id: flow.device_id.clone(),
        channel: flow.channel.clone(),
        owner: task_owner(&flow.owner_principal, &flow.device_id, flow.channel.clone()),
        session_id: flow.session_id,
        run_id: flow.origin_run_id,
        objective_id: flow.objective_id,
        routine_id: flow.routine_id,
        state: flow.state,
        title: flow.title,
        summary: flow.summary,
        priority: 0,
        steps: single_step(
            TaskSourceKind::Flow,
            source_id.as_str(),
            state.as_str(),
            title.as_str(),
            None,
            flow.completed_at_unix_ms,
        ),
        artifacts: Vec::new(),
        retry_policy: TaskRetryPolicy {
            attempt_count: 0,
            max_attempts: None,
            retry_allowed: !is_terminal_task_state(state.as_str()),
            policy_json: retry_policy_json.clone(),
        },
        artifact_refs_json: redact_json(flow.metadata_json.as_str())?,
        retry_policy_json,
        access_policy_json: json!({
            "owner_only": true,
            "lock_owner": flow.lock_owner,
            "lock_expires_at_unix_ms": flow.lock_expires_at_unix_ms
        })
        .to_string(),
        created_at_unix_ms: flow.created_at_unix_ms,
        updated_at_unix_ms: flow.updated_at_unix_ms,
        started_at_unix_ms: None,
        heartbeat_at_unix_ms: None,
        completed_at_unix_ms: flow.completed_at_unix_ms,
    })
}

fn tool_job_task_run(job: ToolJobRecord) -> Result<TaskRun, Status> {
    let source_id = job.job_id.clone();
    let state = job.state.as_str().to_owned();
    let title = job.tool_name.clone();
    let retry_policy_json = json!({
        "attempt_count": job.attempt_count,
        "max_attempts": job.max_attempts,
        "retry_allowed": job.retry_allowed
    })
    .to_string();
    Ok(TaskRun {
        task_id: normalized_task_id(TaskSourceKind::ToolJob, source_id.as_str()),
        source_id: job.job_id,
        source_kind: TaskSourceKind::ToolJob.as_str().to_owned(),
        owner_principal: job.owner_principal.clone(),
        device_id: job.device_id.clone(),
        channel: job.channel.clone(),
        owner: task_owner(&job.owner_principal, &job.device_id, job.channel.clone()),
        session_id: Some(job.session_id),
        run_id: Some(job.run_id),
        objective_id: None,
        routine_id: None,
        state: state.clone(),
        title: job.tool_name,
        summary: job.state_reason.or(job.last_error).unwrap_or(job.backend),
        priority: 0,
        steps: single_step(
            TaskSourceKind::ToolJob,
            source_id.as_str(),
            state.as_str(),
            title.as_str(),
            job.started_at_unix_ms,
            job.completed_at_unix_ms,
        ),
        artifacts: Vec::new(),
        retry_policy: TaskRetryPolicy {
            attempt_count: u64::from(job.attempt_count),
            max_attempts: Some(u64::from(job.max_attempts)),
            retry_allowed: job.retry_allowed,
            policy_json: retry_policy_json.clone(),
        },
        artifact_refs_json: redact_optional_json(job.artifact_refs_json.as_deref())?,
        retry_policy_json,
        access_policy_json: json!({
            "owner_only": true,
            "lease_expires_at_unix_ms": job.lease_expires_at_unix_ms
        })
        .to_string(),
        created_at_unix_ms: job.created_at_unix_ms,
        updated_at_unix_ms: job.updated_at_unix_ms,
        started_at_unix_ms: job.started_at_unix_ms,
        heartbeat_at_unix_ms: job.heartbeat_at_unix_ms,
        completed_at_unix_ms: job.completed_at_unix_ms,
    })
}

fn work_item_task_run(item: WorkItemRecord) -> Result<TaskRun, Status> {
    let source_id = item.work_item_id.clone();
    let state = item.state.clone();
    let title = item.title.clone();
    Ok(TaskRun {
        task_id: normalized_task_id(TaskSourceKind::WorkItem, source_id.as_str()),
        source_id: item.work_item_id,
        source_kind: TaskSourceKind::WorkItem.as_str().to_owned(),
        owner_principal: item.owner_principal.clone(),
        device_id: item.device_id.clone(),
        channel: item.channel.clone(),
        owner: task_owner(&item.owner_principal, &item.device_id, item.channel.clone()),
        session_id: item.session_id,
        run_id: item.run_id,
        objective_id: item.objective_id,
        routine_id: item.routine_id,
        state: item.state,
        title: item.title,
        summary: item.summary,
        priority: item.priority,
        steps: single_step(
            TaskSourceKind::WorkItem,
            source_id.as_str(),
            state.as_str(),
            title.as_str(),
            None,
            item.completed_at_unix_ms,
        ),
        artifacts: Vec::new(),
        retry_policy: TaskRetryPolicy {
            attempt_count: 0,
            max_attempts: None,
            retry_allowed: !is_terminal_task_state(state.as_str()),
            policy_json: "{}".to_owned(),
        },
        artifact_refs_json: redact_json(item.artifact_refs_json.as_str())?,
        retry_policy_json: "{}".to_owned(),
        access_policy_json: json!({
            "owner_only": true,
            "assigned_worker": item.assigned_worker,
            "claim_owner": item.claim_owner,
            "claim_expires_at_unix_ms": item.claim_expires_at_unix_ms
        })
        .to_string(),
        created_at_unix_ms: item.created_at_unix_ms,
        updated_at_unix_ms: item.updated_at_unix_ms,
        started_at_unix_ms: None,
        heartbeat_at_unix_ms: item.heartbeat_at_unix_ms,
        completed_at_unix_ms: item.completed_at_unix_ms,
    })
}

fn commitment_task_run(commitment: CommitmentRecord) -> Result<TaskRun, Status> {
    let source_id = commitment.commitment_id.clone();
    let state = commitment.status.clone();
    let title = commitment.normalized_action.clone();
    let recurrence_json = redact_json(commitment.recurrence_json.as_str())?;
    let acceptance = commitment_acceptance_projection(&commitment);
    let artifact_refs_json = redact_json(
        json!({
            "channel_binding": serde_json::from_str::<Value>(commitment.channel_binding_json.as_str())
                .unwrap_or_else(|_| json!({ "raw": commitment.channel_binding_json })),
            "acceptance": acceptance.clone(),
        })
        .to_string()
        .as_str(),
    )?;
    Ok(TaskRun {
        task_id: normalized_task_id(TaskSourceKind::Commitment, source_id.as_str()),
        source_id: commitment.commitment_id,
        source_kind: TaskSourceKind::Commitment.as_str().to_owned(),
        owner_principal: commitment.owner_principal.clone(),
        device_id: commitment.device_id.clone(),
        channel: commitment.channel.clone(),
        owner: task_owner(
            &commitment.owner_principal,
            &commitment.device_id,
            commitment.channel.clone(),
        ),
        session_id: commitment.session_id,
        run_id: commitment.run_id,
        objective_id: None,
        routine_id: None,
        state: commitment.status,
        title: commitment.normalized_action,
        summary: commitment.user_wording,
        priority: 0,
        steps: single_step(
            TaskSourceKind::Commitment,
            source_id.as_str(),
            state.as_str(),
            title.as_str(),
            None,
            commitment.completed_at_unix_ms,
        ),
        artifacts: Vec::new(),
        retry_policy: TaskRetryPolicy {
            attempt_count: 0,
            max_attempts: None,
            retry_allowed: !is_terminal_task_state(state.as_str()),
            policy_json: recurrence_json.clone(),
        },
        artifact_refs_json,
        retry_policy_json: recurrence_json,
        access_policy_json: json!({
            "owner_only": true,
            "approval_requirement": commitment.approval_requirement,
            "privacy_label": commitment.privacy_label,
            "due_at_unix_ms": commitment.due_at_unix_ms,
            "scheduled_at_unix_ms": commitment.scheduled_at_unix_ms,
            "acceptance": acceptance
        })
        .to_string(),
        created_at_unix_ms: commitment.created_at_unix_ms,
        updated_at_unix_ms: commitment.updated_at_unix_ms,
        started_at_unix_ms: None,
        heartbeat_at_unix_ms: None,
        completed_at_unix_ms: commitment.completed_at_unix_ms,
    })
}

fn agent_plan_task_run(item: AgentPlanItem) -> Result<TaskRun, Status> {
    let source_id = item.plan_item_id.clone();
    let state = item.status.as_str().to_owned();
    let title = item.title.clone();
    let reason_code = item.reason_code.clone();
    let redaction_level = item.redaction_level.clone();
    let retry_policy_json = json!({
        "retry_allowed": !item.status.is_terminal(),
        "reason_code": reason_code,
        "redaction_level": redaction_level
    })
    .to_string();
    let artifact_refs_json = redact_json(
        json!({
            "evidence_refs": item.evidence_refs.clone(),
            "details": item.details.clone(),
            "redaction_level": item.redaction_level.as_str()
        })
        .to_string()
        .as_str(),
    )?;
    let summary = item
        .blocked_reason
        .clone()
        .unwrap_or_else(|| format!("agent plan item is {}", item.status.as_str()));
    Ok(TaskRun {
        task_id: normalized_task_id(TaskSourceKind::AgentPlanItem, source_id.as_str()),
        source_id: item.plan_item_id,
        source_kind: TaskSourceKind::AgentPlanItem.as_str().to_owned(),
        owner_principal: item.owner_principal.clone(),
        device_id: item.device_id.clone(),
        channel: item.channel.clone(),
        owner: task_owner(&item.owner_principal, &item.device_id, item.channel.clone()),
        session_id: Some(item.session_id),
        run_id: item.run_id,
        objective_id: None,
        routine_id: None,
        state,
        title: item.title,
        summary,
        priority: item.priority,
        steps: single_step(
            TaskSourceKind::AgentPlanItem,
            source_id.as_str(),
            item.status.as_str(),
            title.as_str(),
            Some(item.created_at_unix_ms),
            item.completed_at_unix_ms.or(item.cancelled_at_unix_ms),
        ),
        artifacts: Vec::new(),
        retry_policy: TaskRetryPolicy {
            attempt_count: 0,
            max_attempts: None,
            retry_allowed: !item.status.is_terminal(),
            policy_json: retry_policy_json.clone(),
        },
        artifact_refs_json,
        retry_policy_json,
        access_policy_json: json!({
            "owner_only": true,
            "plan_schema_version": item.schema_version,
            "reason_code": item.reason_code,
            "redaction_level": item.redaction_level
        })
        .to_string(),
        created_at_unix_ms: item.created_at_unix_ms,
        updated_at_unix_ms: item.updated_at_unix_ms,
        started_at_unix_ms: Some(item.created_at_unix_ms),
        heartbeat_at_unix_ms: Some(item.updated_at_unix_ms),
        completed_at_unix_ms: item.completed_at_unix_ms.or(item.cancelled_at_unix_ms),
    })
}

fn flow_event(event: FlowEventRecord) -> Result<TaskTimelineEvent, Status> {
    Ok(TaskTimelineEvent {
        event_id: event.event_id,
        task_id: normalized_task_id(TaskSourceKind::Flow, event.flow_id.as_str()),
        source_kind: TaskSourceKind::Flow.as_str().to_owned(),
        event_type: event.event_type,
        actor_principal: event.actor_principal,
        from_state: event.from_state,
        to_state: event.to_state,
        summary: event.summary,
        payload_json: redact_json(event.payload_json.as_str())?,
        created_at_unix_ms: event.created_at_unix_ms,
    })
}

fn work_item_event(event: WorkItemEventRecord) -> Result<TaskTimelineEvent, Status> {
    Ok(TaskTimelineEvent {
        event_id: event.event_id,
        task_id: normalized_task_id(TaskSourceKind::WorkItem, event.work_item_id.as_str()),
        source_kind: TaskSourceKind::WorkItem.as_str().to_owned(),
        event_type: event.event_type,
        actor_principal: event.actor_principal,
        from_state: event.from_state,
        to_state: event.to_state,
        summary: event.summary,
        payload_json: redact_json(event.payload_json.as_str())?,
        created_at_unix_ms: event.created_at_unix_ms,
    })
}

fn commitment_event(event: CommitmentEventRecord) -> Result<TaskTimelineEvent, Status> {
    Ok(TaskTimelineEvent {
        event_id: event.event_id,
        task_id: normalized_task_id(TaskSourceKind::Commitment, event.commitment_id.as_str()),
        source_kind: TaskSourceKind::Commitment.as_str().to_owned(),
        event_type: event.event_type,
        actor_principal: event.actor_principal,
        from_state: event.from_status,
        to_state: event.to_status,
        summary: event.summary,
        payload_json: redact_json(event.payload_json.as_str())?,
        created_at_unix_ms: event.created_at_unix_ms,
    })
}

fn agent_plan_event(event: AgentPlanEvent) -> Result<TaskTimelineEvent, Status> {
    Ok(TaskTimelineEvent {
        event_id: event.event_id,
        task_id: normalized_task_id(TaskSourceKind::AgentPlanItem, event.plan_item_id.as_str()),
        source_kind: TaskSourceKind::AgentPlanItem.as_str().to_owned(),
        event_type: event.event_type,
        actor_principal: event.actor_principal,
        from_state: event.from_status.map(|status| status.as_str().to_owned()),
        to_state: event.to_status.map(|status| status.as_str().to_owned()),
        summary: event.summary,
        payload_json: redact_json(
            json!({
                "payload": event.payload,
                "evidence_refs": event.evidence_refs,
                "redaction_level": event.redaction_level,
                "reason_code": event.reason_code
            })
            .to_string()
            .as_str(),
        )?,
        created_at_unix_ms: event.created_at_unix_ms,
    })
}

fn synthetic_current_state_event(task: &TaskRun) -> Result<TaskTimelineEvent, Status> {
    Ok(TaskTimelineEvent {
        event_id: format!("{}:current", task.task_id),
        task_id: task.task_id.clone(),
        source_kind: task.source_kind.clone(),
        event_type: "task.current_state".to_owned(),
        actor_principal: "system:task-runtime".to_owned(),
        from_state: None,
        to_state: Some(task.state.clone()),
        summary: task.summary.clone(),
        payload_json: redact_json(
            json!({
                "source_id": task.source_id,
                "source_kind": task.source_kind,
                "state": task.state
            })
            .to_string()
            .as_str(),
        )?,
        created_at_unix_ms: task.updated_at_unix_ms,
    })
}

fn plan_progress_checkpoint(
    items: &[AgentPlanItem],
    rollout_enabled: bool,
) -> TaskPlanProgressCheckpoint {
    let mut checkpoint = TaskPlanProgressCheckpoint {
        schema_version: 1,
        plan_schema_version: AGENT_PLAN_SCHEMA_VERSION,
        rollout_enabled,
        source_kind: TaskSourceKind::AgentPlanItem.as_str().to_owned(),
        reason_code: "agent_plan_task_runtime_checkpoint".to_owned(),
        total: items.len(),
        active: 0,
        blocked: 0,
        completed: 0,
        cancelled: 0,
        latest_update_unix_ms: items.iter().map(|item| item.updated_at_unix_ms).max(),
    };
    for item in items {
        match item.status {
            AgentPlanStatus::Blocked => {
                checkpoint.blocked += 1;
                checkpoint.active += 1;
            }
            AgentPlanStatus::Completed => checkpoint.completed += 1,
            AgentPlanStatus::Cancelled => checkpoint.cancelled += 1,
            AgentPlanStatus::Pending | AgentPlanStatus::InProgress => checkpoint.active += 1,
        }
    }
    checkpoint
}

fn normalized_task_id(kind: TaskSourceKind, source_id: &str) -> String {
    format!("{}:{source_id}", kind.prefix())
}

fn task_owner(principal: &str, device_id: &str, channel: Option<String>) -> TaskOwner {
    TaskOwner { principal: principal.to_owned(), device_id: device_id.to_owned(), channel }
}

fn single_step(
    kind: TaskSourceKind,
    source_id: &str,
    state: &str,
    title: &str,
    started_at_unix_ms: Option<i64>,
    completed_at_unix_ms: Option<i64>,
) -> Vec<TaskStep> {
    vec![TaskStep {
        step_id: source_id.to_owned(),
        state: state.to_owned(),
        title: title.to_owned(),
        source_kind: kind.as_str().to_owned(),
        source_id: source_id.to_owned(),
        started_at_unix_ms,
        completed_at_unix_ms,
    }]
}

fn redact_optional_json(raw: Option<&str>) -> Result<String, Status> {
    match raw {
        Some(raw) => redact_json(raw),
        None => Ok("{}".to_owned()),
    }
}

fn redact_json(raw: &str) -> Result<String, Status> {
    journal::redact_payload_json(raw.as_bytes())
        .map_err(|error| Status::internal(format!("failed to redact task payload: {error}")))
}

fn summarize_tasks(tasks: &[TaskRun]) -> TaskRuntimeSummary {
    let mut summary = TaskRuntimeSummary { total: tasks.len(), ..TaskRuntimeSummary::default() };
    for task in tasks {
        if is_terminal_task_state(task.state.as_str()) {
            summary.terminal += 1;
        } else {
            summary.active += 1;
        }
        if matches!(task.state.as_str(), "blocked" | "waiting" | "waiting_for_approval") {
            summary.blocked += 1;
        }
        if matches!(task.state.as_str(), "failed" | "expired" | "timed_out" | "orphaned") {
            summary.failed += 1;
        }
    }
    summary
}

fn is_terminal_task_state(state: &str) -> bool {
    matches!(
        state,
        "succeeded"
            | "completed"
            | "failed"
            | "cancelled"
            | "expired"
            | "timed_out"
            | "delivered"
            | "dismissed"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn access(owner: &str) -> TaskAccessPolicy {
        TaskAccessPolicy {
            owner_principal: owner.to_owned(),
            device_id: Some("device".to_owned()),
            channel: Some("cli".to_owned()),
        }
    }

    fn plan_item(id: &str, status: AgentPlanStatus) -> AgentPlanItem {
        AgentPlanItem {
            schema_version: AGENT_PLAN_SCHEMA_VERSION,
            plan_item_id: id.to_owned(),
            session_id: "session-1".to_owned(),
            run_id: Some("run-1".to_owned()),
            parent_run_id: None,
            owner_principal: "user:one".to_owned(),
            device_id: "device".to_owned(),
            channel: Some("cli".to_owned()),
            title: format!("plan item {id}"),
            details: json!({"next":"verify"}),
            status,
            priority: 7,
            blocked_reason: (status == AgentPlanStatus::Blocked)
                .then(|| "waiting on evidence".to_owned()),
            evidence_refs: json!(["journal:event"]),
            redaction_level: "none".to_owned(),
            reason_code: "test".to_owned(),
            created_at_unix_ms: 10,
            updated_at_unix_ms: 20,
            completed_at_unix_ms: (status == AgentPlanStatus::Completed).then_some(30),
            cancelled_at_unix_ms: (status == AgentPlanStatus::Cancelled).then_some(31),
        }
    }

    fn commitment(status: &str) -> CommitmentRecord {
        CommitmentRecord {
            commitment_id: "commitment-1".to_owned(),
            owner_principal: "user:one".to_owned(),
            device_id: "device".to_owned(),
            channel: Some("cli".to_owned()),
            session_id: Some("session".to_owned()),
            run_id: Some("run".to_owned()),
            user_wording: "I will send the report.".to_owned(),
            normalized_action: "send the report".to_owned(),
            due_condition_json: json!({"type":"unspecified"}).to_string(),
            recurrence_json: json!({"type":"none"}).to_string(),
            channel_binding_json: json!({"type":"console_review"}).to_string(),
            approval_requirement: "manual_review".to_owned(),
            privacy_label: "user_visible".to_owned(),
            status: status.to_owned(),
            confidence_bps: 7_500,
            extraction_model: "test".to_owned(),
            review_reason: "explicit commitment language detected".to_owned(),
            scheduler_binding_json: json!({
                "type": "none",
                "acceptance_criteria": {
                    "schema_version": 1,
                    "criteria": [{
                        "description": "Commitment reaches delivery outcome",
                        "required": true,
                        "evidence_refs": ["commitment.delivery"]
                    }],
                    "decision": "pending",
                    "reason_code": "commitment_acceptance_required",
                    "redaction_level": "metadata_only"
                }
            })
            .to_string(),
            due_at_unix_ms: None,
            scheduled_at_unix_ms: None,
            created_at_unix_ms: 1,
            updated_at_unix_ms: 2,
            completed_at_unix_ms: (status == "delivered").then_some(3),
        }
    }

    #[test]
    fn access_policy_rejects_foreign_principal_device_or_channel() {
        let policy = access("user:one");

        assert!(policy.allows("user:one", "device", Some("cli")));
        assert!(!policy.allows("user:two", "device", Some("cli")));
        assert!(!policy.allows("user:one", "other", Some("cli")));
        assert!(!policy.allows("user:one", "device", Some("web")));
    }

    #[test]
    fn parsed_task_id_requires_known_prefix_and_source() {
        let parsed = ParsedTaskId::parse("flow:01H").expect("flow id should parse");

        assert_eq!(parsed.kind, TaskSourceKind::Flow);
        assert_eq!(parsed.source_id, "01H");
        let parsed_plan =
            ParsedTaskId::parse("agent_plan:plan-1").expect("agent plan id should parse");
        assert_eq!(parsed_plan.kind, TaskSourceKind::AgentPlanItem);
        assert_eq!(parsed_plan.source_id, "plan-1");
        assert!(ParsedTaskId::parse("unknown:01H").is_none());
        assert!(ParsedTaskId::parse("flow:").is_none());
    }

    #[test]
    fn agent_plan_item_maps_to_task_run() {
        let task = agent_plan_task_run(plan_item("plan-1", AgentPlanStatus::Blocked))
            .expect("plan item should map to task");

        assert_eq!(task.task_id, "agent_plan:plan-1");
        assert_eq!(task.source_kind, "agent_plan_item");
        assert_eq!(task.state, "blocked");
        assert_eq!(task.summary, "waiting on evidence");
        assert_eq!(task.priority, 7);
        assert!(task.retry_policy.retry_allowed);
        assert_eq!(task.steps[0].source_kind, "agent_plan_item");
        assert!(task.artifact_refs_json.contains("journal:event"));
    }

    #[test]
    fn commitment_task_run_surfaces_acceptance_projection() {
        let task = commitment_task_run(commitment("delivered")).expect("commitment should map");
        let access_policy = serde_json::from_str::<Value>(task.access_policy_json.as_str())
            .expect("access policy should be json");

        assert_eq!(access_policy["acceptance"]["decision"], "satisfied");
        assert_eq!(access_policy["acceptance"]["reason_code"], "commitment_acceptance_required");
        assert!(task.artifact_refs_json.contains("commitment.delivery"));
    }

    #[test]
    fn plan_progress_checkpoint_counts_agent_plan_states() {
        let items = vec![
            plan_item("pending", AgentPlanStatus::Pending),
            plan_item("blocked", AgentPlanStatus::Blocked),
            plan_item("completed", AgentPlanStatus::Completed),
            plan_item("cancelled", AgentPlanStatus::Cancelled),
        ];

        let checkpoint = plan_progress_checkpoint(items.as_slice(), true);

        assert_eq!(checkpoint.total, 4);
        assert_eq!(checkpoint.active, 2);
        assert_eq!(checkpoint.blocked, 1);
        assert_eq!(checkpoint.completed, 1);
        assert_eq!(checkpoint.cancelled, 1);
        assert_eq!(checkpoint.latest_update_unix_ms, Some(20));
        assert_eq!(checkpoint.reason_code, "agent_plan_task_runtime_checkpoint");
    }

    #[test]
    fn summary_counts_active_blocked_failed_and_terminal() {
        let base = TaskRun {
            task_id: "work_item:one".to_owned(),
            source_id: "one".to_owned(),
            source_kind: "work_item".to_owned(),
            owner_principal: "user:one".to_owned(),
            device_id: "device".to_owned(),
            channel: Some("cli".to_owned()),
            owner: task_owner("user:one", "device", Some("cli".to_owned())),
            session_id: None,
            run_id: None,
            objective_id: None,
            routine_id: None,
            state: "running".to_owned(),
            title: "title".to_owned(),
            summary: "summary".to_owned(),
            priority: 0,
            steps: Vec::new(),
            artifacts: Vec::new(),
            retry_policy: TaskRetryPolicy {
                attempt_count: 0,
                max_attempts: None,
                retry_allowed: true,
                policy_json: "{}".to_owned(),
            },
            artifact_refs_json: "{}".to_owned(),
            retry_policy_json: "{}".to_owned(),
            access_policy_json: "{}".to_owned(),
            created_at_unix_ms: 1,
            updated_at_unix_ms: 1,
            started_at_unix_ms: None,
            heartbeat_at_unix_ms: None,
            completed_at_unix_ms: None,
        };
        let mut blocked = base.clone();
        blocked.state = "blocked".to_owned();
        let mut failed = base.clone();
        failed.state = "failed".to_owned();
        let mut delivered = base.clone();
        delivered.state = "delivered".to_owned();

        let summary = summarize_tasks(&[base, blocked, failed, delivered]);

        assert_eq!(summary.total, 4);
        assert_eq!(summary.active, 2);
        assert_eq!(summary.blocked, 1);
        assert_eq!(summary.failed, 1);
        assert_eq!(summary.terminal, 2);
    }
}
