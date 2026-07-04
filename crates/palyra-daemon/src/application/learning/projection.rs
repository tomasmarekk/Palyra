//! Redacted learning graph and candidate mutation-plan projections.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tonic::Status;

use crate::journal::{LearningCandidateRecord, LearningPreferenceRecord, RecallArtifactRecord};

use super::{
    canonical_learning_text, learning_candidate_open, LEARNING_AUDIT_METADATA_REDACTION_LEVEL,
    LEARNING_GRAPH_PROJECTION_EVENT_COMPLETED, LEARNING_GRAPH_PROJECTION_SCHEMA_VERSION,
    LEARNING_MEMORY_MUTATION_PLAN_EVENT_COMPLETED, LEARNING_MEMORY_MUTATION_PLAN_SCHEMA_VERSION,
    LEARNING_MODEL_CONTEXT_INSTRUCTION_AUTHORITY, LEARNING_MODEL_CONTEXT_TRUST_LABEL,
};

/// Input for building the operator-facing learning graph projection.
pub(crate) struct LearningGraphProjectionInput<'a> {
    pub generated_at_unix_ms: i64,
    pub candidates: &'a [LearningCandidateRecord],
    pub preferences: &'a [LearningPreferenceRecord],
    pub recall_artifacts: &'a [RecallArtifactRecord],
}

/// Kind of node shown in the learning graph projection.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub(crate) enum LearningGraphNodeKind {
    Candidate,
    Preference,
    RecallArtifact,
}

/// Kind of relationship shown in the learning graph projection.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub(crate) enum LearningGraphEdgeKind {
    CandidatePreferenceSource,
    CandidateArtifactEvidence,
    PreferenceArtifactEvidence,
    PreferenceConflict,
    DuplicateCandidate,
}

/// One redacted graph node for candidates, active preferences, and audit artifacts.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct LearningGraphNode {
    pub node_id: String,
    pub node_kind: LearningGraphNodeKind,
    pub label: String,
    pub lifecycle_status: String,
    pub recall_state: String,
    pub recall_included: bool,
    pub scope_kind: Option<String>,
    pub scope_id_hash: Option<String>,
    pub evidence_refs: Vec<String>,
    pub created_at_unix_ms: i64,
    pub updated_at_unix_ms: i64,
    pub redaction_level: String,
}

/// One redacted relationship in the learning graph projection.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct LearningGraphEdge {
    pub edge_id: String,
    pub edge_kind: LearningGraphEdgeKind,
    pub from_node_id: String,
    pub to_node_id: String,
    pub evidence_refs: Vec<String>,
    pub redaction_level: String,
}

/// Recall policy metadata attached to the graph so clients do not treat
/// candidates or artifacts as direct prompt context.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct LearningGraphRecallPolicy {
    pub candidate_policy: String,
    pub preference_policy: String,
    pub artifact_policy: String,
    pub trust_label: String,
    pub instruction_authority: String,
}

/// Operator-facing graph projection over learning candidates, preferences,
/// and recall artifacts. It is observe-only and redacted to metadata.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct LearningGraphProjection {
    pub schema_version: u64,
    pub event_type: String,
    pub generated_at_unix_ms: i64,
    pub node_count: u64,
    pub edge_count: u64,
    pub nodes_by_kind: BTreeMap<LearningGraphNodeKind, u64>,
    pub edges_by_kind: BTreeMap<LearningGraphEdgeKind, u64>,
    pub recall_policy: LearningGraphRecallPolicy,
    pub nodes: Vec<LearningGraphNode>,
    pub edges: Vec<LearningGraphEdge>,
    pub redaction_level: String,
}

/// Request used to build a review-preserving mutation plan for a learning candidate.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub(crate) struct LearningMemoryMutationPlanRequest {
    pub action: String,
    pub reason: String,
    #[serde(default)]
    pub replacement_content: Option<Value>,
    #[serde(default)]
    pub merge_target_id: Option<String>,
}

/// Target descriptor embedded in a learning-memory mutation plan.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct LearningMemoryMutationTarget {
    pub target_kind: String,
    pub target_id: String,
    pub candidate_kind: String,
    pub current_status: String,
    pub scope_kind: String,
    pub scope_id_hash: String,
}

/// Recall impact projected for a proposed learning-memory mutation.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct LearningMemoryMutationRecallEffect {
    pub before: String,
    pub after: String,
    pub direct_recall_write: bool,
    pub model_context_policy: String,
}

/// One concrete operator step needed to execute a proposed mutation.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub(crate) struct LearningMemoryMutationOperatorStep {
    pub method: String,
    pub path: String,
    pub body: Value,
}

/// Audit-safe mutation plan for candidate edit/archive/restore/conflict UX.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub(crate) struct LearningMemoryMutationPlan {
    pub schema_version: u64,
    pub event_type: String,
    pub plan_id: String,
    pub decision: String,
    pub target: LearningMemoryMutationTarget,
    pub action: String,
    pub review_status: String,
    pub action_summary: String,
    pub action_payload: Value,
    pub recall_effect: LearningMemoryMutationRecallEffect,
    pub operator_steps: Vec<LearningMemoryMutationOperatorStep>,
    pub redaction_level: String,
}

/// Builds a redacted graph projection for the console and CLI.
pub(crate) fn learning_graph_projection(
    input: LearningGraphProjectionInput<'_>,
) -> LearningGraphProjection {
    let mut nodes = Vec::new();
    let mut edges = Vec::new();

    for candidate in input.candidates {
        nodes.push(learning_graph_candidate_node(candidate));
    }
    for preference in input.preferences {
        nodes.push(learning_graph_preference_node(preference));
        if let Some(candidate_id) = preference.candidate_id.as_deref() {
            edges.push(learning_graph_edge(
                LearningGraphEdgeKind::CandidatePreferenceSource,
                format!("learning_candidate:{candidate_id}"),
                format!("learning_preference:{}", preference.preference_id),
                vec![
                    format!("learning_candidate:{candidate_id}"),
                    format!("learning_preference:{}", preference.preference_id),
                ],
            ));
        }
    }
    for artifact in input.recall_artifacts {
        nodes.push(learning_graph_recall_artifact_node(artifact));
        edges.extend(learning_graph_artifact_edges(artifact, input.candidates, input.preferences));
    }
    edges.extend(learning_graph_preference_conflict_edges(input.preferences));
    edges.extend(learning_graph_duplicate_candidate_edges(input.candidates));

    let nodes_by_kind = count_graph_nodes_by_kind(nodes.as_slice());
    let edges_by_kind = count_graph_edges_by_kind(edges.as_slice());
    LearningGraphProjection {
        schema_version: LEARNING_GRAPH_PROJECTION_SCHEMA_VERSION,
        event_type: LEARNING_GRAPH_PROJECTION_EVENT_COMPLETED.to_owned(),
        generated_at_unix_ms: input.generated_at_unix_ms,
        node_count: nodes.len() as u64,
        edge_count: edges.len() as u64,
        nodes_by_kind,
        edges_by_kind,
        recall_policy: LearningGraphRecallPolicy {
            candidate_policy: "proposal_only_excluded_until_reviewed_or_applied".to_owned(),
            preference_policy: "active_preferences_are_recall_eligible".to_owned(),
            artifact_policy: "audit_artifacts_are_historical_reference_only".to_owned(),
            trust_label: LEARNING_MODEL_CONTEXT_TRUST_LABEL.to_owned(),
            instruction_authority: LEARNING_MODEL_CONTEXT_INSTRUCTION_AUTHORITY.to_owned(),
        },
        nodes,
        edges,
        redaction_level: LEARNING_AUDIT_METADATA_REDACTION_LEVEL.to_owned(),
    }
}

/// Builds a mutation plan without mutating the candidate or recall index.
///
/// # Errors
/// Returns `invalid_argument` when the action or required fields are invalid.
pub(crate) fn learning_memory_mutation_plan_for_candidate(
    candidate: &LearningCandidateRecord,
    actor_principal: &str,
    now_unix_ms: i64,
    request: LearningMemoryMutationPlanRequest,
) -> Result<LearningMemoryMutationPlan, Status> {
    let reason = request.reason.trim();
    if reason.is_empty() {
        return Err(Status::invalid_argument("reason is required"));
    }
    let action = normalize_learning_memory_mutation_action(request.action.as_str())?;
    if action == "merge" && request.merge_target_id.as_deref().is_none_or(str::is_empty) {
        return Err(Status::invalid_argument("merge_target_id is required for merge"));
    }
    let review_status = review_status_for_learning_memory_mutation(action);
    let before_recall_state = learning_candidate_graph_recall_state(candidate).0.to_owned();
    let after_recall_state = recall_state_after_learning_memory_mutation(action).to_owned();
    let action_payload = json!({
        "schema_version": LEARNING_MEMORY_MUTATION_PLAN_SCHEMA_VERSION,
        "event_type": LEARNING_MEMORY_MUTATION_PLAN_EVENT_COMPLETED,
        "action": action,
        "reason": reason,
        "actor_principal": actor_principal,
        "target": {
            "kind": "learning_candidate",
            "id": candidate.candidate_id,
            "candidate_kind": candidate.candidate_kind,
        },
        "replacement_content": request.replacement_content,
        "merge_target_id": request.merge_target_id,
        "created_at_unix_ms": now_unix_ms,
        "evidence_refs": learning_graph_candidate_evidence_refs(candidate),
        "recall_effect": {
            "before": before_recall_state,
            "after": after_recall_state,
            "direct_recall_write": false,
        },
    });
    let action_summary = format!("{action} learning candidate: {reason}");
    let review_body = json!({
        "status": review_status,
        "action_summary": action_summary,
        "action_payload_json": action_payload.to_string(),
    });
    let plan_id = crate::sha256_hex(
        format!("{}:{}:{}:{}", candidate.candidate_id, action, review_status, action_payload)
            .as_bytes(),
    )
    .chars()
    .take(24)
    .collect::<String>();
    Ok(LearningMemoryMutationPlan {
        schema_version: LEARNING_MEMORY_MUTATION_PLAN_SCHEMA_VERSION,
        event_type: LEARNING_MEMORY_MUTATION_PLAN_EVENT_COMPLETED.to_owned(),
        plan_id,
        decision: "operator_review_required".to_owned(),
        target: LearningMemoryMutationTarget {
            target_kind: "learning_candidate".to_owned(),
            target_id: candidate.candidate_id.clone(),
            candidate_kind: candidate.candidate_kind.clone(),
            current_status: candidate.status.clone(),
            scope_kind: candidate.scope_kind.clone(),
            scope_id_hash: crate::sha256_hex(candidate.scope_id.as_bytes()),
        },
        action: action.to_owned(),
        review_status: review_status.to_owned(),
        action_summary,
        action_payload,
        recall_effect: LearningMemoryMutationRecallEffect {
            before: before_recall_state,
            after: after_recall_state,
            direct_recall_write: false,
            model_context_policy: "candidate_review_record_only".to_owned(),
        },
        operator_steps: vec![LearningMemoryMutationOperatorStep {
            method: "POST".to_owned(),
            path: format!(
                "/console/v1/memory/learning/candidates/{}/review",
                candidate.candidate_id
            ),
            body: review_body,
        }],
        redaction_level: LEARNING_AUDIT_METADATA_REDACTION_LEVEL.to_owned(),
    })
}

fn learning_graph_candidate_node(candidate: &LearningCandidateRecord) -> LearningGraphNode {
    let (recall_state, recall_included) = learning_candidate_graph_recall_state(candidate);
    LearningGraphNode {
        node_id: format!("learning_candidate:{}", candidate.candidate_id),
        node_kind: LearningGraphNodeKind::Candidate,
        label: candidate.title.clone(),
        lifecycle_status: learning_candidate_graph_lifecycle_status(candidate.status.as_str())
            .to_owned(),
        recall_state: recall_state.to_owned(),
        recall_included,
        scope_kind: Some(candidate.scope_kind.clone()),
        scope_id_hash: Some(crate::sha256_hex(candidate.scope_id.as_bytes())),
        evidence_refs: learning_graph_candidate_evidence_refs(candidate),
        created_at_unix_ms: candidate.created_at_unix_ms,
        updated_at_unix_ms: candidate.updated_at_unix_ms,
        redaction_level: LEARNING_AUDIT_METADATA_REDACTION_LEVEL.to_owned(),
    }
}

fn learning_graph_preference_node(preference: &LearningPreferenceRecord) -> LearningGraphNode {
    let recall_included = preference.status == "active";
    LearningGraphNode {
        node_id: format!("learning_preference:{}", preference.preference_id),
        node_kind: LearningGraphNodeKind::Preference,
        label: preference.key.clone(),
        lifecycle_status: preference.status.clone(),
        recall_state: if recall_included {
            "active_preference_recall_eligible".to_owned()
        } else {
            "inactive_preference_excluded".to_owned()
        },
        recall_included,
        scope_kind: Some(preference.scope_kind.clone()),
        scope_id_hash: Some(crate::sha256_hex(preference.scope_id.as_bytes())),
        evidence_refs: learning_graph_preference_evidence_refs(preference),
        created_at_unix_ms: preference.created_at_unix_ms,
        updated_at_unix_ms: preference.updated_at_unix_ms,
        redaction_level: LEARNING_AUDIT_METADATA_REDACTION_LEVEL.to_owned(),
    }
}

fn learning_graph_recall_artifact_node(artifact: &RecallArtifactRecord) -> LearningGraphNode {
    LearningGraphNode {
        node_id: format!("recall_artifact:{}", artifact.artifact_id),
        node_kind: LearningGraphNodeKind::RecallArtifact,
        label: artifact.summary.clone(),
        lifecycle_status: artifact.artifact_kind.clone(),
        recall_state: "audit_artifact_excluded_from_prompt_context".to_owned(),
        recall_included: false,
        scope_kind: artifact.channel.clone().map(|_| "channel".to_owned()),
        scope_id_hash: artifact
            .channel
            .as_deref()
            .map(|channel| crate::sha256_hex(channel.as_bytes())),
        evidence_refs: vec![format!("recall_artifact:{}", artifact.artifact_id)],
        created_at_unix_ms: artifact.created_at_unix_ms,
        updated_at_unix_ms: artifact.created_at_unix_ms,
        redaction_level: LEARNING_AUDIT_METADATA_REDACTION_LEVEL.to_owned(),
    }
}

fn learning_graph_artifact_edges(
    artifact: &RecallArtifactRecord,
    candidates: &[LearningCandidateRecord],
    preferences: &[LearningPreferenceRecord],
) -> Vec<LearningGraphEdge> {
    let artifact_blob = recall_artifact_search_blob(artifact);
    let artifact_node_id = format!("recall_artifact:{}", artifact.artifact_id);
    let mut edges = Vec::new();
    for candidate in candidates {
        if artifact_blob.contains(candidate.candidate_id.as_str()) {
            edges.push(learning_graph_edge(
                LearningGraphEdgeKind::CandidateArtifactEvidence,
                format!("learning_candidate:{}", candidate.candidate_id),
                artifact_node_id.clone(),
                vec![
                    format!("learning_candidate:{}", candidate.candidate_id),
                    format!("recall_artifact:{}", artifact.artifact_id),
                ],
            ));
        }
    }
    for preference in preferences {
        if artifact_blob.contains(preference.preference_id.as_str()) {
            edges.push(learning_graph_edge(
                LearningGraphEdgeKind::PreferenceArtifactEvidence,
                format!("learning_preference:{}", preference.preference_id),
                artifact_node_id.clone(),
                vec![
                    format!("learning_preference:{}", preference.preference_id),
                    format!("recall_artifact:{}", artifact.artifact_id),
                ],
            ));
        }
    }
    edges
}

fn learning_graph_preference_conflict_edges(
    preferences: &[LearningPreferenceRecord],
) -> Vec<LearningGraphEdge> {
    let mut groups = BTreeMap::<String, Vec<&LearningPreferenceRecord>>::new();
    for preference in preferences.iter().filter(|preference| preference.status == "active") {
        let group_key =
            format!("{}:{}:{}", preference.scope_kind, preference.scope_id, preference.key);
        groups.entry(group_key).or_default().push(preference);
    }
    let mut edges = Vec::new();
    for group in groups.into_values() {
        let value_hashes = group
            .iter()
            .map(|preference| {
                crate::sha256_hex(canonical_learning_text(preference.value.as_str()).as_bytes())
            })
            .collect::<BTreeSet<_>>();
        if value_hashes.len() < 2 {
            continue;
        }
        if let Some((first, rest)) = group.split_first() {
            for other in rest {
                edges.push(learning_graph_edge(
                    LearningGraphEdgeKind::PreferenceConflict,
                    format!("learning_preference:{}", first.preference_id),
                    format!("learning_preference:{}", other.preference_id),
                    vec![
                        format!("learning_preference:{}", first.preference_id),
                        format!("learning_preference:{}", other.preference_id),
                    ],
                ));
            }
        }
    }
    edges
}

fn learning_graph_duplicate_candidate_edges(
    candidates: &[LearningCandidateRecord],
) -> Vec<LearningGraphEdge> {
    let mut groups = BTreeMap::<String, Vec<&LearningCandidateRecord>>::new();
    for candidate in candidates.iter().filter(|candidate| learning_candidate_open(candidate)) {
        groups
            .entry(format!(
                "{}:{}:{}:{}",
                candidate.candidate_kind,
                candidate.scope_kind,
                candidate.scope_id,
                candidate.dedupe_key
            ))
            .or_default()
            .push(candidate);
    }
    let mut edges = Vec::new();
    for group in groups.into_values().filter(|group| group.len() > 1) {
        if let Some((first, rest)) = group.split_first() {
            for other in rest {
                edges.push(learning_graph_edge(
                    LearningGraphEdgeKind::DuplicateCandidate,
                    format!("learning_candidate:{}", first.candidate_id),
                    format!("learning_candidate:{}", other.candidate_id),
                    vec![
                        format!("learning_candidate:{}", first.candidate_id),
                        format!("learning_candidate:{}", other.candidate_id),
                    ],
                ));
            }
        }
    }
    edges
}

fn learning_graph_edge(
    edge_kind: LearningGraphEdgeKind,
    from_node_id: String,
    to_node_id: String,
    evidence_refs: Vec<String>,
) -> LearningGraphEdge {
    let edge_id = crate::sha256_hex(
        format!("{edge_kind:?}:{from_node_id}:{to_node_id}:{}", evidence_refs.join(",")).as_bytes(),
    )
    .chars()
    .take(24)
    .collect();
    LearningGraphEdge {
        edge_id,
        edge_kind,
        from_node_id,
        to_node_id,
        evidence_refs,
        redaction_level: LEARNING_AUDIT_METADATA_REDACTION_LEVEL.to_owned(),
    }
}

fn count_graph_nodes_by_kind(nodes: &[LearningGraphNode]) -> BTreeMap<LearningGraphNodeKind, u64> {
    let mut counts = BTreeMap::new();
    for node in nodes {
        *counts.entry(node.node_kind).or_insert(0) += 1;
    }
    counts
}

fn count_graph_edges_by_kind(edges: &[LearningGraphEdge]) -> BTreeMap<LearningGraphEdgeKind, u64> {
    let mut counts = BTreeMap::new();
    for edge in edges {
        *counts.entry(edge.edge_kind).or_insert(0) += 1;
    }
    counts
}

fn learning_graph_candidate_evidence_refs(candidate: &LearningCandidateRecord) -> Vec<String> {
    let mut refs = vec![format!("learning_candidate:{}", candidate.candidate_id)];
    if let Some(source_task_id) = candidate.source_task_id.as_deref() {
        refs.push(format!("background_task:{source_task_id}"));
    }
    refs
}

fn learning_graph_preference_evidence_refs(preference: &LearningPreferenceRecord) -> Vec<String> {
    let mut refs = vec![format!("learning_preference:{}", preference.preference_id)];
    if let Some(candidate_id) = preference.candidate_id.as_deref() {
        refs.push(format!("learning_candidate:{candidate_id}"));
    }
    refs
}

fn recall_artifact_search_blob(artifact: &RecallArtifactRecord) -> String {
    let payload = serde_json::to_string(&artifact.payload).unwrap_or_else(|_| String::new());
    let diagnostics =
        serde_json::to_string(&artifact.diagnostics).unwrap_or_else(|_| String::new());
    let provenance = serde_json::to_string(&artifact.provenance).unwrap_or_else(|_| String::new());
    [
        artifact.query.as_str(),
        artifact.summary.as_str(),
        payload.as_str(),
        diagnostics.as_str(),
        provenance.as_str(),
    ]
    .join("\n")
}

fn learning_candidate_graph_recall_state(
    candidate: &LearningCandidateRecord,
) -> (&'static str, bool) {
    if matches!(
        candidate.status.as_str(),
        "rejected" | "denied" | "suppressed" | "conflicted" | "rolled-back"
    ) {
        return ("retired_candidate_excluded", false);
    }
    if candidate.auto_applied || matches!(candidate.status.as_str(), "applied" | "deployed") {
        return ("applied_candidate_evidence", true);
    }
    ("proposal_only_excluded", false)
}

fn learning_candidate_graph_lifecycle_status(status: &str) -> &'static str {
    match status.trim().to_ascii_lowercase().replace('_', "-").as_str() {
        "" | "queued" | "proposed" | "shadow" => "proposed",
        "needs-review" | "review" | "pending-review" => "needs-review",
        "approved" | "accepted" | "eval-passed" => "approved",
        "applied" | "auto-applied" | "deployed" => "deployed",
        "rollback" | "rolled-back" => "rolled-back",
        "rejected" | "denied" | "suppressed" | "conflicted" => "rejected",
        _ => "proposed",
    }
}

fn normalize_learning_memory_mutation_action(action: &str) -> Result<&'static str, Status> {
    match action.trim().to_ascii_lowercase().replace('_', "-").as_str() {
        "edit" | "update" | "revise" => Ok("edit"),
        "archive" | "suppress" => Ok("archive"),
        "restore" | "unarchive" | "reopen" => Ok("restore"),
        "conflict" | "mark-conflict" | "mark-conflicted" => Ok("mark_conflicted"),
        "merge" | "dedupe" => Ok("merge"),
        "reject" | "deny" => Ok("reject"),
        _ => Err(Status::invalid_argument(
            "action must be edit, archive, restore, mark-conflicted, merge, or reject",
        )),
    }
}

fn review_status_for_learning_memory_mutation(action: &str) -> &'static str {
    match action {
        "archive" | "merge" => "suppressed",
        "restore" | "edit" => "needs-review",
        "mark_conflicted" => "conflicted",
        "reject" => "rejected",
        _ => "needs-review",
    }
}

fn recall_state_after_learning_memory_mutation(action: &str) -> &'static str {
    match action {
        "archive" | "merge" | "reject" => "retired_candidate_excluded",
        "restore" | "edit" => "proposal_only_excluded",
        "mark_conflicted" => "conflict_excluded_until_resolved",
        _ => "proposal_only_excluded",
    }
}
