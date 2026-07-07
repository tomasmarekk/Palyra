//! Child-session orchestration contracts for delegated agent runs.
//!
//! This module stays pure: queue workers and transports perform persistence,
//! while these helpers validate policy inheritance and build replayable projections.

#![allow(dead_code)]

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tonic::Status;

use super::{
    safe_text, DelegatedRunBudgets, DelegationSnapshot, SubagentSessionRecord,
    SubagentTranscriptStatus,
};

/// Agent primitive that a parent run may request for a child session.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ChildSessionPrimitive {
    Spawn,
    Send,
    History,
    Yield,
    Summarize,
}

impl ChildSessionPrimitive {
    /// Returns the stable wire value for this primitive.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Spawn => "spawn",
            Self::Send => "send",
            Self::History => "history",
            Self::Yield => "yield",
            Self::Summarize => "summarize",
        }
    }
}

/// Sanitized policy envelope inherited by a child run.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ChildSessionPolicyEnvelope {
    pub inheritance: String,
    pub parent_run_id: String,
    pub tool_allowlist: Vec<String>,
    pub skill_allowlist: Vec<String>,
    pub workspace_scope: Value,
    pub cancellation: Value,
    pub budgets: DelegatedRunBudgets,
}

/// Bounded child result projection visible to the parent run.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ChildSessionResultProjection {
    pub status: String,
    pub summary: String,
    pub child_run_id: Option<String>,
    pub evidence_refs: Vec<String>,
    pub bounded: bool,
    pub non_authoritative: bool,
}

/// Request for planning one child-session primitive.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChildSessionPrimitiveRequest {
    pub primitive: ChildSessionPrimitive,
    pub parent_run_id: Option<String>,
    pub child_run_id: Option<String>,
    pub task_id: String,
    pub delegation: DelegationSnapshot,
    pub parent_tool_allowlist: Vec<String>,
    pub parent_skill_allowlist: Vec<String>,
    pub workspace_inheritance_requested: bool,
    pub parent_cancel_requested: bool,
    pub child_terminal_state: Option<String>,
    pub yield_summary: Option<String>,
    pub evidence_refs: Vec<String>,
}

/// Planned primitive outcome, including policy and replay posture.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ChildSessionPrimitivePlan {
    pub primitive: ChildSessionPrimitive,
    pub allowed: bool,
    pub reason_code: String,
    pub policy: ChildSessionPolicyEnvelope,
    pub transcript_tree_replayable: bool,
    pub parent_progress_projection: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result_projection: Option<ChildSessionResultProjection>,
}

/// Replayable transcript tree projection for all child sessions under a parent.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ChildTranscriptTree {
    pub schema_version: u32,
    pub parent_run_id: String,
    pub replayable: bool,
    pub child_count: u64,
    pub nodes: Vec<Value>,
}

/// Plans a child-session primitive without performing runtime side effects.
///
/// # Errors
/// Returns `Status::failed_precondition` when a primitive lacks its required
/// parent or child run identity, and `Status::invalid_argument` when the child
/// would widen a non-empty parent allowlist.
pub fn plan_child_session_primitive(
    request: ChildSessionPrimitiveRequest,
) -> Result<ChildSessionPrimitivePlan, Status> {
    let parent_run_id = request.parent_run_id.clone().ok_or_else(|| {
        Status::failed_precondition("child session primitive requires parent run")
    })?;
    validate_subset(
        "child_session.tool_allowlist",
        request.delegation.tool_allowlist.as_slice(),
        request.parent_tool_allowlist.as_slice(),
    )?;
    validate_subset(
        "child_session.skill_allowlist",
        request.delegation.skill_allowlist.as_slice(),
        request.parent_skill_allowlist.as_slice(),
    )?;
    let policy = child_policy_envelope(
        parent_run_id.as_str(),
        &request.delegation,
        request.workspace_inheritance_requested,
    );
    let child_required = matches!(
        request.primitive,
        ChildSessionPrimitive::Send
            | ChildSessionPrimitive::History
            | ChildSessionPrimitive::Yield
            | ChildSessionPrimitive::Summarize
    );
    if child_required && request.child_run_id.is_none() {
        return Err(Status::failed_precondition(
            "child session primitive requires an attached child run",
        ));
    }

    let reason_code = if request.parent_cancel_requested {
        "child_session.parent_cancel_propagated"
    } else if child_failed(request.child_terminal_state.as_deref()) {
        "child_session.child_failure_bounded"
    } else {
        "child_session.primitive_allowed"
    };
    let allowed = !request.parent_cancel_requested;
    Ok(ChildSessionPrimitivePlan {
        primitive: request.primitive,
        allowed,
        reason_code: reason_code.to_owned(),
        policy,
        transcript_tree_replayable: request.child_run_id.is_some()
            || request.primitive == ChildSessionPrimitive::Spawn,
        parent_progress_projection: "bounded_progress_or_summary".to_owned(),
        result_projection: result_projection(&request, allowed),
    })
}

/// Builds a replayable child transcript tree from redacted subagent records.
#[must_use]
pub fn build_child_transcript_tree(
    parent_run_id: String,
    records: Vec<SubagentSessionRecord>,
) -> ChildTranscriptTree {
    let nodes = records
        .iter()
        .map(|record| {
            json!({
                "record_id": record.record_id,
                "task_id": record.task_id,
                "parent_run_id": record.parent_run_id,
                "child_run_id": record.child_run_id,
                "session_id": record.child_session_id,
                "transcript": {
                    "kind": record.transcript_ref.kind,
                    "status": record.transcript_ref.status,
                    "run_id": record.transcript_ref.run_id,
                },
                "scope": record.scope,
                "status": safe_text(record.status.as_str()),
                "evidence_refs": record.evidence_refs,
            })
        })
        .collect::<Vec<_>>();
    let replayable = records.iter().all(|record| {
        record.transcript_ref.status == SubagentTranscriptStatus::Available
            || record.transcript_ref.status == SubagentTranscriptStatus::Pending
    });
    ChildTranscriptTree {
        schema_version: 1,
        parent_run_id,
        replayable,
        child_count: u64::try_from(records.len()).unwrap_or(u64::MAX),
        nodes,
    }
}

fn child_policy_envelope(
    parent_run_id: &str,
    delegation: &DelegationSnapshot,
    workspace_inheritance_requested: bool,
) -> ChildSessionPolicyEnvelope {
    ChildSessionPolicyEnvelope {
        inheritance: "monotonic_restrictive".to_owned(),
        parent_run_id: parent_run_id.to_owned(),
        tool_allowlist: delegation.tool_allowlist.clone(),
        skill_allowlist: delegation.skill_allowlist.clone(),
        workspace_scope: json!({
            "requested": workspace_inheritance_requested,
            "sandbox_policy": "explicit_parent_sandbox_required",
            "scoped_to_parent_workspace": workspace_inheritance_requested,
        }),
        cancellation: json!({
            "parent_to_child": "propagate_by_policy",
            "child_to_parent": "bounded_result_only",
            "child_failure_cancels_parent": false,
        }),
        budgets: DelegatedRunBudgets::from(delegation),
    }
}

fn result_projection(
    request: &ChildSessionPrimitiveRequest,
    allowed: bool,
) -> Option<ChildSessionResultProjection> {
    let should_project = matches!(
        request.primitive,
        ChildSessionPrimitive::Yield | ChildSessionPrimitive::Summarize
    ) || child_failed(request.child_terminal_state.as_deref());
    if !should_project {
        return None;
    }
    let status = if allowed {
        request.child_terminal_state.as_deref().unwrap_or("partial")
    } else {
        "cancel_requested"
    };
    Some(ChildSessionResultProjection {
        status: safe_text(status),
        summary: safe_text(
            request
                .yield_summary
                .as_deref()
                .unwrap_or("child result is available as a bounded parent summary"),
        ),
        child_run_id: request.child_run_id.clone(),
        evidence_refs: bounded_refs(request.evidence_refs.clone()),
        bounded: true,
        non_authoritative: true,
    })
}

fn child_failed(state: Option<&str>) -> bool {
    matches!(state, Some("failed" | "cancelled" | "canceled" | "timed_out" | "rejected"))
}

fn validate_subset(field: &str, requested: &[String], parent: &[String]) -> Result<(), Status> {
    if parent.is_empty() {
        return Ok(());
    }
    if let Some(disallowed) =
        requested.iter().find(|candidate| !parent.iter().any(|allowed| allowed == *candidate))
    {
        return Err(Status::invalid_argument(format!(
            "{field} entry '{disallowed}' exceeds the parent allowlist"
        )));
    }
    Ok(())
}

fn bounded_refs(values: Vec<String>) -> Vec<String> {
    let mut output = Vec::new();
    for value in values {
        let trimmed = value.trim();
        if trimmed.is_empty() || output.iter().any(|existing| existing == trimmed) {
            continue;
        }
        output.push(trimmed.chars().take(256).collect());
        if output.len() >= 32 {
            break;
        }
    }
    output
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::delegation::{
        build_delegated_scope, build_subagent_session_record, DelegatedReferenceInput,
        DelegatedScopeBuildRequest, DelegationExecutionMode, DelegationMemoryScopeKind,
        DelegationMergeContract, DelegationMergeStrategy, DelegationRole, DelegationRuntimeLimits,
        SubagentSessionRecordBuildRequest,
    };

    fn snapshot() -> DelegationSnapshot {
        DelegationSnapshot {
            profile_id: "research".to_owned(),
            display_name: "Research".to_owned(),
            description: None,
            template_id: None,
            role: DelegationRole::Research,
            execution_mode: DelegationExecutionMode::Parallel,
            group_id: "group-a".to_owned(),
            model_profile: "model".to_owned(),
            tool_allowlist: vec!["palyra.http.fetch".to_owned()],
            skill_allowlist: vec!["repo.read".to_owned()],
            memory_scope: DelegationMemoryScopeKind::ParentSession,
            budget_tokens: 1_000,
            max_attempts: 2,
            merge_contract: DelegationMergeContract {
                strategy: DelegationMergeStrategy::Summarize,
                approval_required: false,
            },
            runtime_limits: DelegationRuntimeLimits::default(),
            agent_id: Some("main".to_owned()),
        }
    }

    fn primitive_request(primitive: ChildSessionPrimitive) -> ChildSessionPrimitiveRequest {
        ChildSessionPrimitiveRequest {
            primitive,
            parent_run_id: Some("parent-run".to_owned()),
            child_run_id: Some("child-run".to_owned()),
            task_id: "task-1".to_owned(),
            delegation: snapshot(),
            parent_tool_allowlist: vec!["palyra.http.fetch".to_owned()],
            parent_skill_allowlist: vec!["repo.read".to_owned()],
            workspace_inheritance_requested: true,
            parent_cancel_requested: false,
            child_terminal_state: None,
            yield_summary: Some("partial evidence".to_owned()),
            evidence_refs: vec!["tool:1".to_owned()],
        }
    }

    #[test]
    fn child_session_spawn_inherits_restrictive_policy() {
        let mut request = primitive_request(ChildSessionPrimitive::Spawn);
        request.child_run_id = None;

        let plan = plan_child_session_primitive(request).expect("spawn should plan");

        assert!(plan.allowed);
        assert_eq!(plan.policy.inheritance, "monotonic_restrictive");
        assert_eq!(
            plan.policy.workspace_scope["sandbox_policy"],
            "explicit_parent_sandbox_required"
        );
        assert_eq!(plan.policy.cancellation["child_failure_cancels_parent"], false);
    }

    #[test]
    fn child_session_rejects_parent_tool_escalation() {
        let mut request = primitive_request(ChildSessionPrimitive::Spawn);
        request.delegation.tool_allowlist.push("palyra.fs.apply_patch".to_owned());

        let error = plan_child_session_primitive(request).expect_err("wider child tools fail");

        assert_eq!(error.code(), tonic::Code::InvalidArgument);
        assert!(error.message().contains("exceeds the parent allowlist"));
    }

    #[test]
    fn parent_cancel_projects_bounded_child_result() {
        let mut request = primitive_request(ChildSessionPrimitive::Yield);
        request.parent_cancel_requested = true;

        let plan = plan_child_session_primitive(request).expect("cancel should plan");

        assert!(!plan.allowed);
        assert_eq!(plan.reason_code, "child_session.parent_cancel_propagated");
        assert_eq!(plan.result_projection.expect("projection").status, "cancel_requested");
    }

    #[test]
    fn transcript_tree_exports_replayable_child_records() {
        let delegation = snapshot();
        let scope = build_delegated_scope(DelegatedScopeBuildRequest {
            objective: "Summarize evidence".to_owned(),
            delegation: delegation.clone(),
            parent_tool_allowlist: vec!["palyra.http.fetch".to_owned()],
            parent_skill_allowlist: vec!["repo.read".to_owned()],
            context_refs: vec![DelegatedReferenceInput {
                ref_id: "parent-run".to_owned(),
                reason: "parent progress".to_owned(),
                sensitivity: "metadata".to_owned(),
            }],
            memory_refs: Vec::new(),
            artifact_refs: Vec::new(),
        })
        .expect("scope should build");
        let record = build_subagent_session_record(SubagentSessionRecordBuildRequest {
            task_id: "task-1".to_owned(),
            parent_run_id: Some("parent-run".to_owned()),
            child_run_id: Some("child-run".to_owned()),
            child_session_id: "session-1".to_owned(),
            scope,
            delegation,
            status: "running".to_owned(),
            child_run_exists: true,
            task_terminal: false,
            artifacts: Vec::new(),
            evidence_refs: vec![json!({"ref": "tool:1"})],
            verification_state: "pending".to_owned(),
            created_at_unix_ms: 1,
            updated_at_unix_ms: 2,
        });

        let tree = build_child_transcript_tree("parent-run".to_owned(), vec![record]);

        assert!(tree.replayable);
        assert_eq!(tree.child_count, 1);
        assert_eq!(tree.nodes[0]["transcript"]["status"], "available");
    }
}
