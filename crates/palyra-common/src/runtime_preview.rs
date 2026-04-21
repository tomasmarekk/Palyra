//! Shared runtime-preview vocabulary for telemetry, rollout catalogues,
//! and acceptance-harness fixtures.
//!
//! Daemon, CLI, and web surfaces should consume these identifiers
//! instead of scattering ad-hoc literals.

use std::fmt;

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::feature_rollouts::FeatureRolloutSource;

pub const RUNTIME_PREVIEW_SCHEMA_VERSION: u32 = 1;

macro_rules! runtime_preview_enum {
    (
        $(#[$meta:meta])*
        pub enum $name:ident {
            $(
                $variant:ident => $canonical:literal $(| $alias:literal )*
            ),+ $(,)?
        }
    ) => {
        $(#[$meta])*
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
        pub enum $name {
            $(
                #[serde(rename = $canonical $(, alias = $alias)*)]
                $variant,
            )+
        }

        impl $name {
            #[must_use]
            pub const fn as_str(self) -> &'static str {
                match self {
                    $(
                        Self::$variant => $canonical,
                    )+
                }
            }

            #[must_use]
            pub fn parse(value: &str) -> Option<Self> {
                let normalized = value.trim().to_ascii_lowercase();
                match normalized.as_str() {
                    $(
                        $canonical $(| $alias )* => Some(Self::$variant),
                    )+
                    _ => None,
                }
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(self.as_str())
            }
        }
    };
}

runtime_preview_enum! {
    /// Rollout-scoped capability identifiers exposed by config,
    /// diagnostics, and workflow regression fixtures.
    pub enum RuntimePreviewCapability {
        SessionQueuePolicy => "session_queue_policy",
        PruningPolicyMatrix => "pruning_policy_matrix",
        RetrievalDualPath => "retrieval_dual_path",
        AuxiliaryExecutor => "auxiliary_executor",
        FlowOrchestration => "flow_orchestration",
        DeliveryArbitration => "delivery_arbitration",
        ReplayCapture => "replay_capture",
        NetworkedWorkers => "networked_workers"
    }
}

pub const ALL_RUNTIME_PREVIEW_CAPABILITIES: [RuntimePreviewCapability; 8] = [
    RuntimePreviewCapability::SessionQueuePolicy,
    RuntimePreviewCapability::PruningPolicyMatrix,
    RuntimePreviewCapability::RetrievalDualPath,
    RuntimePreviewCapability::AuxiliaryExecutor,
    RuntimePreviewCapability::FlowOrchestration,
    RuntimePreviewCapability::DeliveryArbitration,
    RuntimePreviewCapability::ReplayCapture,
    RuntimePreviewCapability::NetworkedWorkers,
];

impl RuntimePreviewCapability {
    #[must_use]
    pub const fn label(self) -> &'static str {
        match self {
            Self::SessionQueuePolicy => "Session queue policy",
            Self::PruningPolicyMatrix => "Pruning policy matrix",
            Self::RetrievalDualPath => "Retrieval dual-path",
            Self::AuxiliaryExecutor => "Auxiliary executor",
            Self::FlowOrchestration => "Flow orchestration",
            Self::DeliveryArbitration => "Delivery arbitration",
            Self::ReplayCapture => "Replay capture",
            Self::NetworkedWorkers => "Networked workers",
        }
    }

    #[must_use]
    pub const fn summary(self) -> &'static str {
        match self {
            Self::SessionQueuePolicy => {
                "Preview queue posture for follow-up enqueue, merge, steer, interrupt, and overflow handling."
            }
            Self::PruningPolicyMatrix => {
                "Preview compaction and pruning posture before pruning decisions become automatic."
            }
            Self::RetrievalDualPath => {
                "Preview split retrieval posture for memory and transcript/workspace recall branches."
            }
            Self::AuxiliaryExecutor => {
                "Preview executor limits and budget posture for background and delegated work."
            }
            Self::FlowOrchestration => {
                "Preview flow-state transitions, retry posture, and cancellation checkpoints."
            }
            Self::DeliveryArbitration => {
                "Preview descendant-aware delivery and stale-parent suppression policy."
            }
            Self::ReplayCapture => {
                "Preview replay bundle capture posture without enabling unrestricted record/replay."
            }
            Self::NetworkedWorkers => {
                "Preview remote worker execution guardrails, lease budgets, and attestation posture."
            }
        }
    }
}

runtime_preview_enum! {
    /// Activation mode for rollout-scoped preview capabilities.
    pub enum RuntimePreviewMode {
        Disabled => "disabled",
        PreviewOnly => "preview_only" | "preview-only" | "preview",
        Enabled => "enabled"
    }
}

impl RuntimePreviewMode {
    #[must_use]
    pub const fn label(self) -> &'static str {
        match self {
            Self::Disabled => "Disabled",
            Self::PreviewOnly => "Preview only",
            Self::Enabled => "Enabled",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimePreviewModeParseError {
    source_name: String,
    value: String,
}

impl fmt::Display for RuntimePreviewModeParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} must be one of: disabled | preview_only | enabled; got '{}'",
            self.source_name, self.value
        )
    }
}

impl std::error::Error for RuntimePreviewModeParseError {}

pub fn parse_runtime_preview_mode(
    raw: &str,
    source_name: &str,
) -> Result<RuntimePreviewMode, RuntimePreviewModeParseError> {
    RuntimePreviewMode::parse(raw).ok_or_else(|| RuntimePreviewModeParseError {
        source_name: source_name.to_owned(),
        value: raw.trim().to_owned(),
    })
}

runtime_preview_enum! {
    /// Effective diagnostics state derived from config mode, rollout posture,
    /// and activation blockers.
    pub enum RuntimePreviewEffectiveState {
        Disabled => "disabled",
        Blocked => "blocked",
        PreviewOnly => "preview_only" | "preview-only" | "preview",
        Enabled => "enabled"
    }
}

runtime_preview_enum! {
    /// Aggregate summary state for all preview capabilities.
    pub enum RuntimePreviewSummaryState {
        Disabled => "disabled",
        Blocked => "blocked",
        PreviewOnly => "preview_only" | "preview-only" | "preview",
        Mixed => "mixed",
        Enabled => "enabled"
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuntimePreviewCapabilityConfigSnapshot {
    pub capability: RuntimePreviewCapability,
    pub label: String,
    pub summary: String,
    pub mode: RuntimePreviewMode,
    pub effective_state: RuntimePreviewEffectiveState,
    pub rollout_enabled: bool,
    pub rollout_source: FeatureRolloutSource,
    pub rollout_env_var: String,
    pub rollout_config_path: String,
    pub config_section: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub activation_blockers: Vec<String>,
    pub settings: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuntimePreviewConfigSnapshot {
    pub schema_version: u32,
    pub state: RuntimePreviewSummaryState,
    pub preview_capabilities: usize,
    pub enabled_capabilities: usize,
    pub blocked_capabilities: usize,
    pub disabled_capabilities: usize,
    pub capabilities: Vec<RuntimePreviewCapabilityConfigSnapshot>,
}

runtime_preview_enum! {
    /// Canonical runtime decision events shared across journal,
    /// diagnostics, and tape surfaces.
    pub enum RuntimeDecisionEventType {
        QueueEnqueue => "queue_enqueue",
        QueueMerge => "queue_merge",
        QueueSteer => "queue_steer",
        QueueInterrupt => "queue_interrupt",
        QueueOverflow => "queue_overflow",
        PruningApply => "pruning_apply",
        RecallSessionSearch => "recall_session_search",
        AuxiliaryTaskLifecycle => "auxiliary_task_lifecycle",
        FlowLifecycle => "flow_lifecycle",
        DeliveryArbitration => "delivery_arbitration",
        WorkerLeaseLifecycle => "worker_lease_lifecycle"
    }
}

pub const ALL_RUNTIME_DECISION_EVENT_TYPES: [RuntimeDecisionEventType; 11] = [
    RuntimeDecisionEventType::QueueEnqueue,
    RuntimeDecisionEventType::QueueMerge,
    RuntimeDecisionEventType::QueueSteer,
    RuntimeDecisionEventType::QueueInterrupt,
    RuntimeDecisionEventType::QueueOverflow,
    RuntimeDecisionEventType::PruningApply,
    RuntimeDecisionEventType::RecallSessionSearch,
    RuntimeDecisionEventType::AuxiliaryTaskLifecycle,
    RuntimeDecisionEventType::FlowLifecycle,
    RuntimeDecisionEventType::DeliveryArbitration,
    RuntimeDecisionEventType::WorkerLeaseLifecycle,
];

impl RuntimeDecisionEventType {
    #[must_use]
    pub const fn journal_event(self) -> &'static str {
        match self {
            Self::QueueEnqueue => "runtime.queue.enqueue",
            Self::QueueMerge => "runtime.queue.merge",
            Self::QueueSteer => "runtime.queue.steer",
            Self::QueueInterrupt => "runtime.queue.interrupt",
            Self::QueueOverflow => "runtime.queue.overflow",
            Self::PruningApply => "runtime.pruning.apply",
            Self::RecallSessionSearch => "runtime.recall.session_search",
            Self::AuxiliaryTaskLifecycle => "runtime.auxiliary_task.lifecycle",
            Self::FlowLifecycle => "runtime.flow.lifecycle",
            Self::DeliveryArbitration => "runtime.delivery.arbitration",
            Self::WorkerLeaseLifecycle => "runtime.worker_lease.lifecycle",
        }
    }

    #[must_use]
    pub const fn label(self) -> &'static str {
        match self {
            Self::QueueEnqueue => "Queue enqueue",
            Self::QueueMerge => "Queue merge",
            Self::QueueSteer => "Queue steer",
            Self::QueueInterrupt => "Queue interrupt",
            Self::QueueOverflow => "Queue overflow",
            Self::PruningApply => "Pruning apply",
            Self::RecallSessionSearch => "Recall session search",
            Self::AuxiliaryTaskLifecycle => "Auxiliary task lifecycle",
            Self::FlowLifecycle => "Flow lifecycle",
            Self::DeliveryArbitration => "Delivery arbitration",
            Self::WorkerLeaseLifecycle => "Worker lease lifecycle",
        }
    }

    #[must_use]
    pub const fn summary(self) -> &'static str {
        match self {
            Self::QueueEnqueue => "Records a follow-up being admitted to the session queue.",
            Self::QueueMerge => "Records queue coalescing or prompt merge behaviour.",
            Self::QueueSteer => "Records queue steering toward a backlog or alternate flow.",
            Self::QueueInterrupt => "Records queue interruption of an existing foreground path.",
            Self::QueueOverflow => "Records queue overflow and fail-closed backpressure decisions.",
            Self::PruningApply => "Records compaction/pruning token savings and write posture.",
            Self::RecallSessionSearch => {
                "Records recall preview or retrieval-branch search decisions."
            }
            Self::AuxiliaryTaskLifecycle => {
                "Records background or delegated task lifecycle updates."
            }
            Self::FlowLifecycle => "Records flow transitions, retries, cancellation, or blocking.",
            Self::DeliveryArbitration => {
                "Records stale-parent suppression or descendant delivery choices."
            }
            Self::WorkerLeaseLifecycle => {
                "Records networked worker registration, lease, completion, and orphaning."
            }
        }
    }
}

runtime_preview_enum! {
    /// Canonical actor kinds attached to runtime decision payloads.
    pub enum RuntimeDecisionActorKind {
        Operator => "operator",
        System => "system",
        Worker => "worker",
        RunStream => "run_stream"
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RuntimeDecisionActor {
    pub kind: RuntimeDecisionActorKind,
    pub principal: String,
    pub device_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub channel: Option<String>,
}

impl RuntimeDecisionActor {
    #[must_use]
    pub fn new(
        kind: RuntimeDecisionActorKind,
        principal: impl Into<String>,
        device_id: impl Into<String>,
        channel: Option<String>,
    ) -> Self {
        Self { kind, principal: principal.into(), device_id: device_id.into(), channel }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RuntimeEntityRef {
    pub role: String,
    pub kind: String,
    pub id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub state: Option<String>,
}

impl RuntimeEntityRef {
    #[must_use]
    pub fn new(role: impl Into<String>, kind: impl Into<String>, id: impl Into<String>) -> Self {
        Self { role: role.into(), kind: kind.into(), id: id.into(), state: None }
    }

    #[must_use]
    pub fn with_state(mut self, state: impl Into<String>) -> Self {
        self.state = Some(state.into());
        self
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RuntimeDecisionTiming {
    pub observed_at_unix_ms: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub duration_ms: Option<u64>,
}

impl RuntimeDecisionTiming {
    #[must_use]
    pub const fn observed(observed_at_unix_ms: i64) -> Self {
        Self { observed_at_unix_ms, duration_ms: None }
    }

    #[must_use]
    pub const fn observed_with_duration(observed_at_unix_ms: i64, duration_ms: u64) -> Self {
        Self { observed_at_unix_ms, duration_ms: Some(duration_ms) }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct RuntimeResourceBudget {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub queue_depth: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub token_budget: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pruning_token_delta: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub retrieval_branch_latency_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub retry_count: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub suppression_count: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RuntimeDecisionPayload {
    pub schema_version: u32,
    pub event_type: RuntimeDecisionEventType,
    pub actor: RuntimeDecisionActor,
    pub reason: String,
    pub policy_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub input: Option<RuntimeEntityRef>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub output: Option<RuntimeEntityRef>,
    pub timing: RuntimeDecisionTiming,
    #[serde(default, skip_serializing_if = "runtime_resource_budget_is_empty")]
    pub resource_budget: RuntimeResourceBudget,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub related_entities: Vec<RuntimeEntityRef>,
    #[serde(default, skip_serializing_if = "Value::is_null")]
    pub details: Value,
}

impl RuntimeDecisionPayload {
    #[must_use]
    pub fn new(
        event_type: RuntimeDecisionEventType,
        actor: RuntimeDecisionActor,
        reason: impl Into<String>,
        policy_id: impl Into<String>,
        timing: RuntimeDecisionTiming,
    ) -> Self {
        Self {
            schema_version: RUNTIME_PREVIEW_SCHEMA_VERSION,
            event_type,
            actor,
            reason: reason.into(),
            policy_id: policy_id.into(),
            input: None,
            output: None,
            timing,
            resource_budget: RuntimeResourceBudget::default(),
            related_entities: Vec::new(),
            details: Value::Null,
        }
    }

    #[must_use]
    pub fn with_input(mut self, input: RuntimeEntityRef) -> Self {
        self.input = Some(input);
        self
    }

    #[must_use]
    pub fn with_output(mut self, output: RuntimeEntityRef) -> Self {
        self.output = Some(output);
        self
    }

    #[must_use]
    pub fn with_resource_budget(mut self, resource_budget: RuntimeResourceBudget) -> Self {
        self.resource_budget = resource_budget;
        self
    }

    #[must_use]
    pub fn with_related_entity(mut self, entity: RuntimeEntityRef) -> Self {
        self.related_entities.push(entity);
        self
    }

    #[must_use]
    pub fn with_details(mut self, details: Value) -> Self {
        self.details = details;
        self
    }
}

fn runtime_resource_budget_is_empty(budget: &RuntimeResourceBudget) -> bool {
    budget.queue_depth.is_none()
        && budget.token_budget.is_none()
        && budget.pruning_token_delta.is_none()
        && budget.retrieval_branch_latency_ms.is_none()
        && budget.retry_count.is_none()
        && budget.suppression_count.is_none()
}

runtime_preview_enum! {
    /// Acceptance scenarios that regression and CI must keep wired into runtime preview coverage.
    pub enum RuntimeAcceptanceScenario {
        QueuedInputLifecycle => "queued_input_lifecycle",
        PruningDecision => "pruning_decision",
        DualPathRetrieval => "dual_path_retrieval",
        PreflightCheckpointPair => "preflight_checkpoint_pair",
        ChildProgressMerge => "child_progress_merge",
        FlowTransitions => "flow_transitions",
        DeliveryArbitration => "delivery_arbitration",
        NetworkedWorkerPreview => "networked_worker_preview"
    }
}

pub const ALL_RUNTIME_ACCEPTANCE_SCENARIOS: [RuntimeAcceptanceScenario; 8] = [
    RuntimeAcceptanceScenario::QueuedInputLifecycle,
    RuntimeAcceptanceScenario::PruningDecision,
    RuntimeAcceptanceScenario::DualPathRetrieval,
    RuntimeAcceptanceScenario::PreflightCheckpointPair,
    RuntimeAcceptanceScenario::ChildProgressMerge,
    RuntimeAcceptanceScenario::FlowTransitions,
    RuntimeAcceptanceScenario::DeliveryArbitration,
    RuntimeAcceptanceScenario::NetworkedWorkerPreview,
];

pub const RUNTIME_ACCEPTANCE_FIXTURE_SESSION_TRANSCRIPT: &str = "session_transcript";
pub const RUNTIME_ACCEPTANCE_FIXTURE_RETRIEVAL_QUERY: &str = "retrieval_query";
pub const RUNTIME_ACCEPTANCE_FIXTURE_WORKSPACE_PATCH: &str = "workspace_patch";
pub const RUNTIME_ACCEPTANCE_FIXTURE_DELEGATED_CHILD_RUN: &str = "delegated_child_run";
pub const RUNTIME_ACCEPTANCE_FIXTURE_REPLAY_BUNDLE: &str = "replay_bundle";
pub const RUNTIME_ACCEPTANCE_FIXTURE_WORKER_LEASE: &str = "worker_lease";

const QUEUED_INPUT_LIFECYCLE_FIXTURES: &[&str] = &[RUNTIME_ACCEPTANCE_FIXTURE_SESSION_TRANSCRIPT];
const PRUNING_DECISION_FIXTURES: &[&str] =
    &[RUNTIME_ACCEPTANCE_FIXTURE_SESSION_TRANSCRIPT, RUNTIME_ACCEPTANCE_FIXTURE_WORKSPACE_PATCH];
const DUAL_PATH_RETRIEVAL_FIXTURES: &[&str] =
    &[RUNTIME_ACCEPTANCE_FIXTURE_SESSION_TRANSCRIPT, RUNTIME_ACCEPTANCE_FIXTURE_RETRIEVAL_QUERY];
const PREFLIGHT_CHECKPOINT_PAIR_FIXTURES: &[&str] =
    &[RUNTIME_ACCEPTANCE_FIXTURE_SESSION_TRANSCRIPT, RUNTIME_ACCEPTANCE_FIXTURE_REPLAY_BUNDLE];
const CHILD_PROGRESS_MERGE_FIXTURES: &[&str] = &[RUNTIME_ACCEPTANCE_FIXTURE_DELEGATED_CHILD_RUN];
const FLOW_TRANSITIONS_FIXTURES: &[&str] = &[
    RUNTIME_ACCEPTANCE_FIXTURE_SESSION_TRANSCRIPT,
    RUNTIME_ACCEPTANCE_FIXTURE_DELEGATED_CHILD_RUN,
];
const DELIVERY_ARBITRATION_FIXTURES: &[&str] = &[
    RUNTIME_ACCEPTANCE_FIXTURE_SESSION_TRANSCRIPT,
    RUNTIME_ACCEPTANCE_FIXTURE_DELEGATED_CHILD_RUN,
];
const NETWORKED_WORKER_PREVIEW_FIXTURES: &[&str] = &[RUNTIME_ACCEPTANCE_FIXTURE_WORKER_LEASE];

impl RuntimeAcceptanceScenario {
    #[must_use]
    pub const fn label(self) -> &'static str {
        match self {
            Self::QueuedInputLifecycle => "Queued input lifecycle",
            Self::PruningDecision => "Pruning decision",
            Self::DualPathRetrieval => "Dual-path retrieval",
            Self::PreflightCheckpointPair => "Preflight checkpoint pair",
            Self::ChildProgressMerge => "Child progress merge",
            Self::FlowTransitions => "Flow transitions",
            Self::DeliveryArbitration => "Delivery arbitration",
            Self::NetworkedWorkerPreview => "Networked worker preview",
        }
    }

    #[must_use]
    pub const fn summary(self) -> &'static str {
        match self {
            Self::QueuedInputLifecycle => {
                "Follow-up queue creation, forwarding, and fail-closed delivery updates."
            }
            Self::PruningDecision => {
                "Compaction preview/apply contract and pruning savings surfaces remain stable."
            }
            Self::DualPathRetrieval => {
                "Recall preview exercises memory and transcript/workspace retrieval branches."
            }
            Self::PreflightCheckpointPair => {
                "Checkpoint + replay preflight pair remains serializable before replay capture expands."
            }
            Self::ChildProgressMerge => {
                "Delegated child progress merge fixtures stay available for descendant delivery work."
            }
            Self::FlowTransitions => {
                "Flow cancellation, retry, and preview transition surfaces remain regression-tested."
            }
            Self::DeliveryArbitration => {
                "Descendant-aware delivery policy and stale-parent suppression stay explicit."
            }
            Self::NetworkedWorkerPreview => {
                "Networked worker preview inventory and lifecycle telemetry stay wired and fail-closed."
            }
        }
    }

    #[must_use]
    pub const fn capability(self) -> RuntimePreviewCapability {
        match self {
            Self::QueuedInputLifecycle => RuntimePreviewCapability::SessionQueuePolicy,
            Self::PruningDecision => RuntimePreviewCapability::PruningPolicyMatrix,
            Self::DualPathRetrieval => RuntimePreviewCapability::RetrievalDualPath,
            Self::PreflightCheckpointPair => RuntimePreviewCapability::ReplayCapture,
            Self::ChildProgressMerge => RuntimePreviewCapability::AuxiliaryExecutor,
            Self::FlowTransitions => RuntimePreviewCapability::FlowOrchestration,
            Self::DeliveryArbitration => RuntimePreviewCapability::DeliveryArbitration,
            Self::NetworkedWorkerPreview => RuntimePreviewCapability::NetworkedWorkers,
        }
    }

    #[must_use]
    pub const fn required_fixture_keys(self) -> &'static [&'static str] {
        match self {
            Self::QueuedInputLifecycle => QUEUED_INPUT_LIFECYCLE_FIXTURES,
            Self::PruningDecision => PRUNING_DECISION_FIXTURES,
            Self::DualPathRetrieval => DUAL_PATH_RETRIEVAL_FIXTURES,
            Self::PreflightCheckpointPair => PREFLIGHT_CHECKPOINT_PAIR_FIXTURES,
            Self::ChildProgressMerge => CHILD_PROGRESS_MERGE_FIXTURES,
            Self::FlowTransitions => FLOW_TRANSITIONS_FIXTURES,
            Self::DeliveryArbitration => DELIVERY_ARBITRATION_FIXTURES,
            Self::NetworkedWorkerPreview => NETWORKED_WORKER_PREVIEW_FIXTURES,
        }
    }
}

#[must_use]
pub fn runtime_acceptance_fixture_catalog() -> Value {
    json!({
        "schema_version": RUNTIME_PREVIEW_SCHEMA_VERSION,
        "session_transcript": {
            "session_id": "01ARZ3NDEKTSV4RRFFQ69G5FAT",
            "run_id": "01ARZ3NDEKTSV4RRFFQ69G5FAU",
            "events": [
                { "seq": 1, "event_type": "status", "message": "queued follow-up accepted" },
                { "seq": 2, "event_type": "runtime.queue.enqueue", "reason": "workflow_regression_fixture" }
            ]
        },
        "retrieval_query": {
            "query": "summarize the last checkpoint decision",
            "memory_top_k": 3,
            "workspace_top_k": 3,
            "max_candidates": 6,
            "prompt_budget_tokens": 1800
        },
        "workspace_patch": {
            "tool_name": "palyra.fs.apply_patch",
            "policy_id": "workspace_patch.preview",
            "paths": ["src/runtime.rs", "src/observability.rs"],
            "attestation_required": true,
            "checkpoint_pair": {
                "mutation_id": "01ARZ3NDEKTSV4RRFFQ69G5WMP",
                "preflight_checkpoint_id": "01ARZ3NDEKTSV4RRFFQ69G5WPF",
                "post_change_checkpoint_id": "01ARZ3NDEKTSV4RRFFQ69G5WPC",
                "stages": ["preflight", "post_change"],
                "review_posture": "review_required",
                "risk_level": "high",
                "compare_summary": {
                    "files_changed": 2,
                    "paths": ["src/runtime.rs", "src/observability.rs"]
                }
            },
            "rollback_smoke": {
                "restore_target": "preflight_checkpoint",
                "restore_attempts": 1,
                "restore_success_rate_bps": 10000,
                "missing_checkpoint_pair_rate_bps": 0,
                "high_risk_mutation_rate_bps": 10000
            }
        },
        "delegated_child_run": {
            "task_kind": "delegation_prompt",
            "parent_run_id": "01ARZ3NDEKTSV4RRFFQ69G5FAV",
            "child_run_id": "01ARZ3NDEKTSV4RRFFQ69G5FAW",
            "delivery_policy": "merge_progress_updates"
        },
        "replay_bundle": {
            "bundle_id": "replay-preview-bundle",
            "preview_only": true,
            "artifact_count": 2,
            "contains_transcript": true,
            "contains_workspace_patch": true
        },
        "worker_lease": {
            "worker_id": "worker-preview-01",
            "attested": true,
            "lease_id": "lease-preview-01",
            "requested_backend": "networked_worker",
            "requested_capability": "networked_worker_preview"
        }
    })
}

#[cfg(test)]
mod tests {
    use super::{
        parse_runtime_preview_mode, runtime_acceptance_fixture_catalog, RuntimeAcceptanceScenario,
        RuntimeDecisionActor, RuntimeDecisionActorKind, RuntimeDecisionEventType,
        RuntimeDecisionPayload, RuntimeDecisionTiming, RuntimeEntityRef, RuntimePreviewCapability,
        RuntimePreviewCapabilityConfigSnapshot, RuntimePreviewConfigSnapshot,
        RuntimePreviewEffectiveState, RuntimePreviewMode, RuntimePreviewSummaryState,
        RuntimeResourceBudget, ALL_RUNTIME_ACCEPTANCE_SCENARIOS, ALL_RUNTIME_PREVIEW_CAPABILITIES,
        RUNTIME_PREVIEW_SCHEMA_VERSION,
    };
    use crate::feature_rollouts::FeatureRolloutSource;
    use serde_json::json;

    #[test]
    fn runtime_preview_capability_round_trips_with_canonical_names() {
        for capability in ALL_RUNTIME_PREVIEW_CAPABILITIES {
            let encoded =
                serde_json::to_string(&capability).expect("capability should serialize cleanly");
            let decoded: RuntimePreviewCapability =
                serde_json::from_str(encoded.as_str()).expect("capability should deserialize");
            assert_eq!(decoded, capability);
            assert!(RuntimePreviewCapability::parse(capability.as_str()).is_some());
            assert!(!capability.label().is_empty());
            assert!(!capability.summary().is_empty());
        }
    }

    #[test]
    fn runtime_decision_payload_keeps_required_fields_and_optional_context() {
        let payload = RuntimeDecisionPayload::new(
            RuntimeDecisionEventType::QueueEnqueue,
            RuntimeDecisionActor::new(
                RuntimeDecisionActorKind::Operator,
                "admin:web-console",
                "device-1",
                Some("web".to_owned()),
            ),
            "queued_followup_submitted",
            "session_queue.preview.followup",
            RuntimeDecisionTiming::observed_with_duration(1_730_000_000_000, 18),
        )
        .with_input(RuntimeEntityRef::new("input", "queued_input", "Q1").with_state("pending"))
        .with_output(RuntimeEntityRef::new("output", "run", "R1").with_state("active"))
        .with_resource_budget(RuntimeResourceBudget {
            queue_depth: Some(1),
            token_budget: Some(256),
            pruning_token_delta: None,
            retrieval_branch_latency_ms: None,
            retry_count: None,
            suppression_count: None,
        })
        .with_related_entity(RuntimeEntityRef::new("session", "session", "S1"))
        .with_details(json!({ "origin_kind": "queued" }));

        let encoded = serde_json::to_value(&payload).expect("payload should serialize");
        assert_eq!(encoded["schema_version"], 1);
        assert_eq!(encoded["event_type"], "queue_enqueue");
        assert_eq!(encoded["reason"], "queued_followup_submitted");
        assert_eq!(encoded["policy_id"], "session_queue.preview.followup");
        assert_eq!(encoded["actor"]["kind"], "operator");
        assert_eq!(encoded["resource_budget"]["queue_depth"], 1);
        assert_eq!(encoded["details"]["origin_kind"], "queued");
    }

    #[test]
    fn runtime_acceptance_scenarios_map_back_to_capabilities() {
        for scenario in ALL_RUNTIME_ACCEPTANCE_SCENARIOS {
            assert!(RuntimeAcceptanceScenario::parse(scenario.as_str()).is_some());
            assert!(!scenario.label().is_empty());
            assert!(!scenario.summary().is_empty());
            assert!(ALL_RUNTIME_PREVIEW_CAPABILITIES.contains(&scenario.capability()));
        }
    }

    #[test]
    fn runtime_acceptance_scenarios_publish_required_fixture_keys() {
        let fixture_catalog = runtime_acceptance_fixture_catalog();
        let fixture_keys = fixture_catalog
            .as_object()
            .expect("fixture catalog should serialize as an object")
            .keys()
            .filter(|key| key.as_str() != "schema_version")
            .cloned()
            .collect::<std::collections::BTreeSet<_>>();
        for scenario in ALL_RUNTIME_ACCEPTANCE_SCENARIOS {
            assert!(
                !scenario.required_fixture_keys().is_empty(),
                "{} should publish required fixtures",
                scenario.as_str()
            );
            for fixture_key in scenario.required_fixture_keys() {
                assert!(
                    fixture_keys.contains(*fixture_key),
                    "{} should reference a declared shared fixture",
                    scenario.as_str()
                );
            }
        }
    }

    #[test]
    fn runtime_acceptance_fixture_catalog_exposes_shared_runtime_fixtures() {
        let fixture = runtime_acceptance_fixture_catalog();
        assert_eq!(fixture["schema_version"], 1);
        assert!(fixture.get("session_transcript").is_some());
        assert!(fixture.get("retrieval_query").is_some());
        assert!(fixture.get("workspace_patch").is_some());
        assert_eq!(
            fixture["workspace_patch"]["checkpoint_pair"]["stages"],
            json!(["preflight", "post_change"])
        );
        assert_eq!(
            fixture["workspace_patch"]["rollback_smoke"]["restore_target"],
            "preflight_checkpoint"
        );
        assert!(fixture.get("delegated_child_run").is_some());
        assert!(fixture.get("replay_bundle").is_some());
        assert!(fixture.get("worker_lease").is_some());
    }

    #[test]
    fn runtime_preview_mode_parser_accepts_canonical_values_and_aliases() {
        assert_eq!(
            parse_runtime_preview_mode("disabled", "runtime.mode").expect("disabled should parse"),
            RuntimePreviewMode::Disabled
        );
        assert_eq!(
            parse_runtime_preview_mode(" preview-only ", "runtime.mode")
                .expect("preview alias should parse"),
            RuntimePreviewMode::PreviewOnly
        );
        assert_eq!(
            parse_runtime_preview_mode("enabled", "runtime.mode").expect("enabled should parse"),
            RuntimePreviewMode::Enabled
        );
    }

    #[test]
    fn runtime_preview_mode_parser_rejects_unknown_values() {
        let error = parse_runtime_preview_mode("pilot", "runtime.mode").expect_err("invalid mode");
        assert!(error.to_string().contains("runtime.mode"));
        assert!(error.to_string().contains("pilot"));
    }

    #[test]
    fn runtime_preview_config_snapshot_serializes_shared_rollout_shapes() {
        let snapshot = RuntimePreviewConfigSnapshot {
            schema_version: RUNTIME_PREVIEW_SCHEMA_VERSION,
            state: RuntimePreviewSummaryState::PreviewOnly,
            preview_capabilities: 1,
            enabled_capabilities: 0,
            blocked_capabilities: 0,
            disabled_capabilities: 7,
            capabilities: vec![RuntimePreviewCapabilityConfigSnapshot {
                capability: RuntimePreviewCapability::SessionQueuePolicy,
                label: RuntimePreviewCapability::SessionQueuePolicy.label().to_owned(),
                summary: RuntimePreviewCapability::SessionQueuePolicy.summary().to_owned(),
                mode: RuntimePreviewMode::PreviewOnly,
                effective_state: RuntimePreviewEffectiveState::PreviewOnly,
                rollout_enabled: false,
                rollout_source: FeatureRolloutSource::Default,
                rollout_env_var: "PALYRA_EXPERIMENTAL_SESSION_QUEUE_POLICY".to_owned(),
                rollout_config_path: "feature_rollouts.session_queue_policy".to_owned(),
                config_section: "session_queue_policy".to_owned(),
                activation_blockers: Vec::new(),
                settings: json!({
                    "max_depth": 8,
                    "merge_window_ms": 1500,
                }),
            }],
        };

        let encoded = serde_json::to_value(&snapshot).expect("snapshot should serialize");
        assert_eq!(encoded["state"], "preview_only");
        assert_eq!(encoded["capabilities"][0]["capability"], "session_queue_policy");
        assert_eq!(encoded["capabilities"][0]["effective_state"], "preview_only");
        assert_eq!(encoded["capabilities"][0]["settings"]["max_depth"], 8);
    }
}
