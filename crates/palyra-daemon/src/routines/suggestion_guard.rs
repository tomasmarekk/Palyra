//! Routine suggestion lifecycle guard contracts.

#![allow(dead_code)]

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};
use serde_json::json;

use super::{RoutineApprovalMode, RoutineTriggerKind};

pub(crate) const ROUTINE_SUGGESTION_SCHEMA_VERSION: u64 = 1;

/// Blueprint family used when proposing a routine without activating it.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum RoutineSuggestionBlueprint {
    PeriodicDigest,
    Reminder,
    Monitor,
    Cleanup,
    Report,
}

impl RoutineSuggestionBlueprint {
    /// Returns the stable wire name for this blueprint.
    #[must_use]
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::PeriodicDigest => "periodic_digest",
            Self::Reminder => "reminder",
            Self::Monitor => "monitor",
            Self::Cleanup => "cleanup",
            Self::Report => "report",
        }
    }
}

/// Input for the routine suggestion lifecycle guard.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct RoutineSuggestionGuardInput {
    pub blueprint: RoutineSuggestionBlueprint,
    pub trigger_kind: RoutineTriggerKind,
    pub approval_mode: RoutineApprovalMode,
    pub max_runs: Option<u32>,
    pub retry_max_attempts: Option<u32>,
    pub destructive_cleanup: bool,
    pub self_scheduling: bool,
    pub owner_principal: String,
    pub scope: String,
}

/// Guard decision for an automation suggestion.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum RoutineSuggestionGuardDecision {
    PendingCandidate,
    ApprovalRequired,
    Rejected,
}

/// Stable reason codes for routine suggestion guard decisions.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum RoutineSuggestionGuardReasonCode {
    #[serde(rename = "routine_suggestion.pending_candidate")]
    PendingCandidate,
    #[serde(rename = "routine_suggestion.owner_scope_recorded")]
    OwnerScopeRecorded,
    #[serde(rename = "routine_suggestion.approval_required")]
    ApprovalRequired,
    #[serde(rename = "routine_suggestion.self_scheduling_rejected")]
    SelfSchedulingRejected,
    #[serde(rename = "routine_suggestion.unbounded_retry_rejected")]
    UnboundedRetryRejected,
    #[serde(rename = "routine_suggestion.destructive_cleanup_requires_approval")]
    DestructiveCleanupRequiresApproval,
    #[serde(rename = "routine_suggestion.missing_owner_rejected")]
    MissingOwnerRejected,
    #[serde(rename = "routine_suggestion.missing_scope_rejected")]
    MissingScopeRejected,
}

impl RoutineSuggestionGuardReasonCode {
    /// Returns the stable wire reason code.
    #[must_use]
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::PendingCandidate => "routine_suggestion.pending_candidate",
            Self::OwnerScopeRecorded => "routine_suggestion.owner_scope_recorded",
            Self::ApprovalRequired => "routine_suggestion.approval_required",
            Self::SelfSchedulingRejected => "routine_suggestion.self_scheduling_rejected",
            Self::UnboundedRetryRejected => "routine_suggestion.unbounded_retry_rejected",
            Self::DestructiveCleanupRequiresApproval => {
                "routine_suggestion.destructive_cleanup_requires_approval"
            }
            Self::MissingOwnerRejected => "routine_suggestion.missing_owner_rejected",
            Self::MissingScopeRejected => "routine_suggestion.missing_scope_rejected",
        }
    }
}

/// Observe-only guard projection; actual routine writes still go through routine control.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct RoutineSuggestionGuardProjection {
    pub schema_version: u64,
    pub event_type: String,
    pub decision: RoutineSuggestionGuardDecision,
    pub reason_codes: Vec<RoutineSuggestionGuardReasonCode>,
    pub blueprint: RoutineSuggestionBlueprint,
    pub trigger_kind: RoutineTriggerKind,
    pub approval_mode: RoutineApprovalMode,
    pub owner_principal_hash: String,
    pub scope_hash: String,
    pub activation_state: String,
    pub trace_json: String,
}

#[must_use]
pub(crate) fn routine_suggestion_guard_projection(
    input: &RoutineSuggestionGuardInput,
) -> RoutineSuggestionGuardProjection {
    let mut reason_codes = BTreeSet::new();
    reason_codes.insert(RoutineSuggestionGuardReasonCode::PendingCandidate);

    let mut rejected = false;
    if input.owner_principal.trim().is_empty() {
        reason_codes.insert(RoutineSuggestionGuardReasonCode::MissingOwnerRejected);
        rejected = true;
    }
    if input.scope.trim().is_empty() {
        reason_codes.insert(RoutineSuggestionGuardReasonCode::MissingScopeRejected);
        rejected = true;
    }
    if input.self_scheduling {
        reason_codes.insert(RoutineSuggestionGuardReasonCode::SelfSchedulingRejected);
        rejected = true;
    }
    if input.retry_max_attempts.is_none_or(|attempts| attempts == 0 || attempts > 16) {
        reason_codes.insert(RoutineSuggestionGuardReasonCode::UnboundedRetryRejected);
        rejected = true;
    }
    if input.approval_mode != RoutineApprovalMode::None {
        reason_codes.insert(RoutineSuggestionGuardReasonCode::ApprovalRequired);
    }
    if !rejected {
        reason_codes.insert(RoutineSuggestionGuardReasonCode::OwnerScopeRecorded);
    }

    let decision = if rejected {
        RoutineSuggestionGuardDecision::Rejected
    } else if reason_codes.contains(&RoutineSuggestionGuardReasonCode::ApprovalRequired) {
        RoutineSuggestionGuardDecision::ApprovalRequired
    } else {
        RoutineSuggestionGuardDecision::PendingCandidate
    };
    let owner_principal_hash = crate::sha256_hex(input.owner_principal.trim().as_bytes());
    let scope_hash = crate::sha256_hex(input.scope.trim().as_bytes());
    let trace = json!({
        "schema_version": ROUTINE_SUGGESTION_SCHEMA_VERSION,
        "event_type": "routine_suggestion.lifecycle_guard",
        "decision": decision,
        "reason_codes": reason_codes.iter().map(|code| code.as_str()).collect::<Vec<_>>(),
        "blueprint": input.blueprint.as_str(),
        "trigger_kind": input.trigger_kind.as_str(),
        "approval_mode": input.approval_mode.as_str(),
        "owner_principal_hash": owner_principal_hash,
        "scope_hash": scope_hash,
        "activation_state": "pending_candidate",
    });

    RoutineSuggestionGuardProjection {
        schema_version: ROUTINE_SUGGESTION_SCHEMA_VERSION,
        event_type: "routine_suggestion.lifecycle_guard".to_owned(),
        decision,
        reason_codes: reason_codes.into_iter().collect(),
        blueprint: input.blueprint,
        trigger_kind: input.trigger_kind,
        approval_mode: input.approval_mode,
        owner_principal_hash,
        scope_hash,
        activation_state: "pending_candidate".to_owned(),
        trace_json: trace.to_string(),
    }
}
