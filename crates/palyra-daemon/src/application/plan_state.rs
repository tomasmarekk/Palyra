//! Durable agent plan state facade for model-visible planning.
//!
//! The store facade keeps tool/runtime code from depending directly on the
//! journal schema while rollout remains conservative.

use std::{str::FromStr, sync::Arc};

use serde::{Deserialize, Serialize};
use serde_json::Value;
use tonic::Status;
use ulid::Ulid;

use crate::{
    gateway::GatewayRuntimeState,
    journal::{
        AgentPlanCreateRequest, AgentPlanEventRecord, AgentPlanItemRecord, AgentPlanListFilter,
        AgentPlanUpdateRequest, JournalError, JournalStore, V2ComplexPlanEnsureOutcome,
        V2ComplexPlanEnsureRequest,
    },
};

pub const AGENT_PLAN_SCHEMA_VERSION: u64 = 1;
pub const AGENT_PLAN_CREATED_EVENT: &str = "agent.plan.created";
pub const AGENT_PLAN_UPDATED_EVENT: &str = "agent.plan.updated";
pub const AGENT_PLAN_COMPLETED_EVENT: &str = "agent.plan.completed";
pub const AGENT_PLAN_BLOCKED_EVENT: &str = "agent.plan.blocked";
pub const AGENT_PLAN_TOOL_INVOKED_EVENT: &str = "agent.plan.tool_invoked";
pub(crate) const V2_COMPLEX_PLAN_REASON: &str = "agent.plan.v2_complex_auto_initialized";
const V2_COMPLEXITY_THRESHOLD: f64 = 0.75;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AgentPlanStatus {
    Pending,
    InProgress,
    Blocked,
    Completed,
    Cancelled,
}

impl AgentPlanStatus {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::InProgress => "in_progress",
            Self::Blocked => "blocked",
            Self::Completed => "completed",
            Self::Cancelled => "cancelled",
        }
    }

    pub const fn is_terminal(self) -> bool {
        matches!(self, Self::Completed | Self::Cancelled)
    }

    pub const fn update_event_type(self) -> &'static str {
        match self {
            Self::Blocked => AGENT_PLAN_BLOCKED_EVENT,
            Self::Completed => AGENT_PLAN_COMPLETED_EVENT,
            Self::Pending | Self::InProgress | Self::Cancelled => AGENT_PLAN_UPDATED_EVENT,
        }
    }
}

impl FromStr for AgentPlanStatus {
    type Err = AgentPlanStatusParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "pending" => Ok(Self::Pending),
            "in_progress" => Ok(Self::InProgress),
            "blocked" => Ok(Self::Blocked),
            "completed" => Ok(Self::Completed),
            "cancelled" => Ok(Self::Cancelled),
            other => Err(AgentPlanStatusParseError { value: other.to_owned() }),
        }
    }
}

#[derive(Debug, Clone, thiserror::Error, PartialEq, Eq)]
#[error("unknown agent plan status: {value}")]
pub struct AgentPlanStatusParseError {
    value: String,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct AgentPlanItem {
    pub schema_version: u64,
    pub plan_item_id: String,
    pub session_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub run_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parent_run_id: Option<String>,
    pub owner_principal: String,
    pub device_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub channel: Option<String>,
    pub title: String,
    pub details: Value,
    pub status: AgentPlanStatus,
    pub priority: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub blocked_reason: Option<String>,
    pub evidence_refs: Value,
    pub redaction_level: String,
    pub reason_code: String,
    pub created_at_unix_ms: i64,
    pub updated_at_unix_ms: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub completed_at_unix_ms: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cancelled_at_unix_ms: Option<i64>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct AgentPlanEvent {
    pub event_id: String,
    pub plan_item_id: String,
    pub session_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub run_id: Option<String>,
    pub event_type: String,
    pub actor_principal: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub from_status: Option<AgentPlanStatus>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub to_status: Option<AgentPlanStatus>,
    pub reason_code: String,
    pub summary: String,
    pub payload: Value,
    pub evidence_refs: Value,
    pub redaction_level: String,
    pub created_at_unix_ms: i64,
}

#[derive(Debug, Clone)]
pub struct AgentPlanCreateCommand {
    pub plan_item_id: Option<String>,
    pub session_id: String,
    pub run_id: Option<String>,
    pub parent_run_id: Option<String>,
    pub owner_principal: String,
    pub device_id: String,
    pub channel: Option<String>,
    pub title: String,
    pub details: Value,
    pub status: AgentPlanStatus,
    pub priority: i64,
    pub blocked_reason: Option<String>,
    pub evidence_refs: Value,
    pub reason_code: String,
    pub actor_principal: String,
    pub payload: Value,
}

#[derive(Debug, Clone, Default)]
pub struct AgentPlanUpdateCommand {
    pub plan_item_id: String,
    pub expected_status: Option<AgentPlanStatus>,
    pub status: Option<AgentPlanStatus>,
    pub title: Option<String>,
    pub details: Option<Value>,
    pub priority: Option<i64>,
    pub blocked_reason: Option<Option<String>>,
    pub evidence_refs: Option<Value>,
    pub reason_code: String,
    pub actor_principal: String,
    pub summary: String,
    pub payload: Value,
}

#[derive(Debug, Clone)]
pub struct AgentPlanQuery {
    pub owner_principal: Option<String>,
    pub device_id: Option<String>,
    pub channel: Option<String>,
    pub session_id: Option<String>,
    pub run_id: Option<String>,
    pub status: Option<AgentPlanStatus>,
    pub include_terminal: bool,
    pub limit: usize,
}

#[derive(Debug, Clone)]
pub(crate) struct AgentPlanStore<'a> {
    journal: &'a JournalStore,
}

impl<'a> AgentPlanStore<'a> {
    pub(crate) fn new(journal: &'a JournalStore) -> Self {
        Self { journal }
    }

    pub(crate) fn create_item(
        &self,
        command: AgentPlanCreateCommand,
    ) -> Result<AgentPlanItem, JournalError> {
        let record = self.journal.create_agent_plan_item(&AgentPlanCreateRequest {
            plan_item_id: command.plan_item_id.unwrap_or_else(|| Ulid::generate().to_string()),
            session_id: command.session_id,
            run_id: command.run_id,
            parent_run_id: command.parent_run_id,
            owner_principal: command.owner_principal,
            device_id: command.device_id,
            channel: command.channel,
            title: command.title,
            details_json: serde_json::to_string(&command.details)?,
            status: command.status.as_str().to_owned(),
            priority: command.priority,
            blocked_reason: command.blocked_reason,
            evidence_refs_json: serde_json::to_string(&command.evidence_refs)?,
            reason_code: command.reason_code,
            actor_principal: command.actor_principal,
            payload_json: serde_json::to_string(&command.payload)?,
        })?;
        item_from_record(record)
    }

    pub(crate) fn update_item(
        &self,
        command: AgentPlanUpdateCommand,
    ) -> Result<AgentPlanItem, JournalError> {
        let record = self.journal.update_agent_plan_item(&AgentPlanUpdateRequest {
            plan_item_id: command.plan_item_id,
            expected_status: command.expected_status.map(|status| status.as_str().to_owned()),
            status: command.status.map(|status| status.as_str().to_owned()),
            title: command.title,
            details_json: command
                .details
                .map(|details| serde_json::to_string(&details))
                .transpose()?,
            priority: command.priority,
            blocked_reason: command.blocked_reason,
            evidence_refs_json: command
                .evidence_refs
                .map(|evidence_refs| serde_json::to_string(&evidence_refs))
                .transpose()?,
            reason_code: command.reason_code,
            actor_principal: command.actor_principal,
            summary: command.summary,
            payload_json: serde_json::to_string(&command.payload)?,
        })?;
        item_from_record(record)
    }

    pub(crate) fn list_items(
        &self,
        query: &AgentPlanQuery,
    ) -> Result<Vec<AgentPlanItem>, JournalError> {
        self.journal
            .list_agent_plan_items(&AgentPlanListFilter {
                owner_principal: query.owner_principal.clone(),
                device_id: query.device_id.clone(),
                channel: query.channel.clone(),
                session_id: query.session_id.clone(),
                run_id: query.run_id.clone(),
                status: query.status.map(|status| status.as_str().to_owned()),
                include_terminal: query.include_terminal,
                limit: query.limit,
            })?
            .into_iter()
            .map(item_from_record)
            .collect()
    }

    pub(crate) fn get_item(
        &self,
        plan_item_id: &str,
    ) -> Result<Option<AgentPlanItem>, JournalError> {
        self.journal.get_agent_plan_item(plan_item_id)?.map(item_from_record).transpose()
    }

    pub(crate) fn list_events(
        &self,
        plan_item_id: &str,
        limit: usize,
    ) -> Result<Vec<AgentPlanEvent>, JournalError> {
        self.journal
            .list_agent_plan_events(plan_item_id, limit)?
            .into_iter()
            .map(event_from_record)
            .collect()
    }
}

/// Host-owned inputs for authoritative-V2 complex plan initialization.
#[derive(Debug, Clone)]
pub(crate) struct V2ComplexPlanContext<'a> {
    pub(crate) authoritative_v2: bool,
    pub(crate) complexity_score: f64,
    pub(crate) session_id: &'a str,
    pub(crate) run_id: &'a str,
    pub(crate) parameter_delta_json: Option<&'a str>,
    pub(crate) owner_principal: &'a str,
    pub(crate) device_id: &'a str,
    pub(crate) channel: Option<&'a str>,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
struct ObjectiveContinuationPlanScope {
    objective_id: String,
    root_run_id: String,
}

/// Atomically initializes the minimal non-authoritative plan for a complex,
/// authoritative RuntimeKernelV2 run.
///
/// # Errors
/// Returns a runtime status when the durable plan/link transaction fails.
pub(crate) async fn ensure_authoritative_v2_complex_plan(
    runtime: &Arc<GatewayRuntimeState>,
    context: V2ComplexPlanContext<'_>,
) -> Result<Option<V2ComplexPlanEnsureOutcome>, Status> {
    let objective_scope = objective_continuation_plan_scope(context.parameter_delta_json);
    if !should_auto_initialize_v2_plan(
        context.authoritative_v2,
        context.complexity_score,
        objective_scope.is_some(),
    ) {
        return Ok(None);
    }
    let root_run_id =
        objective_scope.as_ref().map(|scope| scope.root_run_id.as_str()).unwrap_or(context.run_id);
    let objective_id = objective_scope
        .as_ref()
        .map(|scope| scope.objective_id.clone())
        .unwrap_or_else(|| format!("run:{root_run_id}"));
    let (title, focus) = if let Some(scope) = objective_scope.as_ref() {
        objective_plan_descriptor(runtime, scope.objective_id.as_str()).await?
    } else {
        (
            "Track complex task progress".to_owned(),
            "Maintain a durable, non-authoritative progress checklist.".to_owned(),
        )
    };
    let request = V2ComplexPlanEnsureRequest {
        plan_item_id: Ulid::generate().to_string(),
        objective_id,
        session_id: context.session_id.to_owned(),
        root_run_id: root_run_id.to_owned(),
        source_run_id: context.run_id.to_owned(),
        owner_principal: context.owner_principal.to_owned(),
        device_id: context.device_id.to_owned(),
        channel: context.channel.map(ToOwned::to_owned),
        actor_principal: context.owner_principal.to_owned(),
        title,
        focus,
    };
    let state = Arc::clone(runtime);
    tokio::task::spawn_blocking(move || {
        state
            .journal_store
            .ensure_v2_complex_plan(&request)
            .map(Some)
            .map_err(|error| Status::internal(format!("V2 plan initialization failed: {error}")))
    })
    .await
    .map_err(|_| Status::internal("V2 plan initialization worker panicked"))?
}

async fn objective_plan_descriptor(
    runtime: &Arc<GatewayRuntimeState>,
    objective_id: &str,
) -> Result<(String, String), Status> {
    let registry = Arc::clone(&runtime.routines_runtime_config()?.objectives);
    let objective_id = objective_id.to_owned();
    tokio::task::spawn_blocking(move || {
        let objective = registry
            .get_objective(objective_id.as_str())
            .map_err(|error| Status::internal(format!("objective plan lookup failed: {error}")))?
            .ok_or_else(|| Status::failed_precondition("objective plan scope no longer exists"))?;
        let focus = objective
            .current_focus
            .or(objective.next_recommended_step)
            .or(objective.success_criteria)
            .unwrap_or_else(|| "Verify the objective completion contract.".to_owned());
        Ok(("Track objective progress".to_owned(), focus))
    })
    .await
    .map_err(|_| Status::internal("objective plan lookup worker panicked"))?
}

fn should_auto_initialize_v2_plan(
    authoritative_v2: bool,
    complexity_score: f64,
    objective_continuation: bool,
) -> bool {
    authoritative_v2
        && (objective_continuation
            || (complexity_score.is_finite() && complexity_score >= V2_COMPLEXITY_THRESHOLD))
}

fn objective_continuation_plan_scope(
    parameter_delta_json: Option<&str>,
) -> Option<ObjectiveContinuationPlanScope> {
    let value = serde_json::from_str::<Value>(parameter_delta_json?).ok()?;
    let candidate = value
        .get("objective_continuation")
        .or_else(|| value.get("parameter_delta")?.get("objective_continuation"))?;
    serde_json::from_value(candidate.clone()).ok()
}

fn item_from_record(record: AgentPlanItemRecord) -> Result<AgentPlanItem, JournalError> {
    Ok(AgentPlanItem {
        schema_version: AGENT_PLAN_SCHEMA_VERSION,
        plan_item_id: record.plan_item_id,
        session_id: record.session_id,
        run_id: record.run_id,
        parent_run_id: record.parent_run_id,
        owner_principal: record.owner_principal,
        device_id: record.device_id,
        channel: record.channel,
        title: record.title,
        details: parse_value(record.details_json.as_str(), "agent_plan.details_json")?,
        status: parse_status(record.status.as_str())?,
        priority: record.priority,
        blocked_reason: record.blocked_reason,
        evidence_refs: parse_value(
            record.evidence_refs_json.as_str(),
            "agent_plan.evidence_refs_json",
        )?,
        redaction_level: record.redaction_level,
        reason_code: record.reason_code,
        created_at_unix_ms: record.created_at_unix_ms,
        updated_at_unix_ms: record.updated_at_unix_ms,
        completed_at_unix_ms: record.completed_at_unix_ms,
        cancelled_at_unix_ms: record.cancelled_at_unix_ms,
    })
}

fn event_from_record(record: AgentPlanEventRecord) -> Result<AgentPlanEvent, JournalError> {
    Ok(AgentPlanEvent {
        event_id: record.event_id,
        plan_item_id: record.plan_item_id,
        session_id: record.session_id,
        run_id: record.run_id,
        event_type: record.event_type,
        actor_principal: record.actor_principal,
        from_status: record.from_status.as_deref().map(parse_status).transpose()?,
        to_status: record.to_status.as_deref().map(parse_status).transpose()?,
        reason_code: record.reason_code,
        summary: record.summary,
        payload: parse_value(record.payload_json.as_str(), "agent_plan_event.payload_json")?,
        evidence_refs: parse_value(
            record.evidence_refs_json.as_str(),
            "agent_plan_event.evidence_refs_json",
        )?,
        redaction_level: record.redaction_level,
        created_at_unix_ms: record.created_at_unix_ms,
    })
}

fn parse_status(raw: &str) -> Result<AgentPlanStatus, JournalError> {
    AgentPlanStatus::from_str(raw).map_err(|error| JournalError::InvalidArgument(error.to_string()))
}

fn parse_value(raw: &str, field: &'static str) -> Result<Value, JournalError> {
    serde_json::from_str(raw).map_err(|error| {
        JournalError::InvalidArgument(format!("{field} must be valid JSON: {error}"))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn agent_plan_status_serializes_stable_snake_case_labels() {
        assert_eq!(
            serde_json::to_string(&AgentPlanStatus::InProgress).expect("status should serialize"),
            "\"in_progress\""
        );
        assert_eq!(
            serde_json::from_str::<AgentPlanStatus>("\"blocked\"")
                .expect("status should deserialize"),
            AgentPlanStatus::Blocked
        );
    }

    #[test]
    fn agent_plan_status_maps_update_event_types() {
        assert_eq!(AgentPlanStatus::Blocked.update_event_type(), AGENT_PLAN_BLOCKED_EVENT);
        assert_eq!(AgentPlanStatus::Completed.update_event_type(), AGENT_PLAN_COMPLETED_EVENT);
        assert_eq!(AgentPlanStatus::Cancelled.update_event_type(), AGENT_PLAN_UPDATED_EVENT);
        assert!(AgentPlanStatus::Completed.is_terminal());
    }

    #[test]
    fn v2_complex_plan_heuristic_is_authoritative_and_bounded() {
        assert!(should_auto_initialize_v2_plan(true, 0.75, false));
        assert!(should_auto_initialize_v2_plan(true, 0.10, true));
        assert!(!should_auto_initialize_v2_plan(false, 1.0, true));
        assert!(!should_auto_initialize_v2_plan(true, 0.74, false));
        assert!(!should_auto_initialize_v2_plan(true, f64::NAN, false));
    }

    #[test]
    fn objective_scope_parses_direct_and_wrapped_parameter_delta() {
        let direct = objective_continuation_plan_scope(Some(
            r#"{"objective_continuation":{"objective_id":"objective","root_run_id":"root"}}"#,
        ))
        .expect("direct objective scope should parse");
        let wrapped = objective_continuation_plan_scope(Some(
            r#"{"parameter_delta":{"objective_continuation":{"objective_id":"objective","root_run_id":"root"}}}"#,
        ))
        .expect("wrapped objective scope should parse");

        assert_eq!(direct, wrapped);
    }
}
