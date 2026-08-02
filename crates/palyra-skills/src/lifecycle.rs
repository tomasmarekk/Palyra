//! Operational lifecycle and usage contracts for installed skill versions.
//!
//! Artifact bytes stay immutable; these records describe which reviewed
//! version the host may execute and retain bounded evidence for curator
//! decisions.

use serde::{Deserialize, Serialize};

/// Current schema for [`SkillLifecycleRecord`] and [`SkillUsageTelemetry`].
pub const SKILL_LIFECYCLE_SCHEMA_VERSION: u32 = 1;
/// Maximum retained lifecycle transitions for one immutable version.
pub const MAX_SKILL_LIFECYCLE_TRANSITIONS: usize = 64;
/// Maximum routine references retained in usage telemetry.
pub const MAX_SKILL_DEPENDENT_ROUTINE_REFS: usize = 64;

/// Host-owned lifecycle state for one immutable skill version.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SkillLifecycleState {
    Observed,
    Staged,
    Evaluated,
    Reviewed,
    Active,
    Stale,
    Archived,
    RolledBack,
}

impl SkillLifecycleState {
    /// Returns the stable serialized state label.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Observed => "observed",
            Self::Staged => "staged",
            Self::Evaluated => "evaluated",
            Self::Reviewed => "reviewed",
            Self::Active => "active",
            Self::Stale => "stale",
            Self::Archived => "archived",
            Self::RolledBack => "rolled_back",
        }
    }

    /// Whether the selected version remains executable.
    #[must_use]
    pub const fn is_executable(self) -> bool {
        matches!(self, Self::Active | Self::Stale)
    }
}

/// Deterministic evaluation posture for an installed version.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SkillEvaluationStatus {
    Pending,
    Passed,
    Failed,
}

/// One bounded lifecycle transition retained with the immutable version.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct SkillLifecycleTransition {
    pub from: Option<SkillLifecycleState>,
    pub to: SkillLifecycleState,
    pub at_unix_ms: i64,
    pub reason_code: String,
}

/// Lifecycle evidence for one installed, immutable skill version.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct SkillLifecycleRecord {
    pub schema_version: u32,
    pub state: SkillLifecycleState,
    pub pinned: bool,
    pub artifact_signed: bool,
    pub artifact_sha256: String,
    pub eval_status: SkillEvaluationStatus,
    pub eval_pack_sha256: Option<String>,
    pub tainted: bool,
    pub reviewed_by_operator: bool,
    pub reviewed_by_policy: bool,
    pub activated_at_unix_ms: Option<i64>,
    pub stale_detected_at_unix_ms: Option<i64>,
    pub archived_at_unix_ms: Option<i64>,
    pub restored_at_unix_ms: Option<i64>,
    pub rollback_count: u64,
    pub transitions: Vec<SkillLifecycleTransition>,
}

impl Default for SkillLifecycleRecord {
    fn default() -> Self {
        Self {
            schema_version: SKILL_LIFECYCLE_SCHEMA_VERSION,
            state: SkillLifecycleState::Observed,
            pinned: false,
            artifact_signed: false,
            artifact_sha256: String::new(),
            eval_status: SkillEvaluationStatus::Pending,
            eval_pack_sha256: None,
            tainted: false,
            reviewed_by_operator: false,
            reviewed_by_policy: false,
            activated_at_unix_ms: None,
            stale_detected_at_unix_ms: None,
            archived_at_unix_ms: None,
            restored_at_unix_ms: None,
            rollback_count: 0,
            transitions: Vec::new(),
        }
    }
}

/// Review authority supplied by the host for activation.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct SkillActivationGate {
    pub operator_approved: bool,
    pub policy_approved: bool,
}

/// Deterministic result of evaluating the activation gate.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct SkillActivationDecision {
    pub allowed: bool,
    pub reason_codes: Vec<String>,
}

/// Durable aggregate used by curators when comparing skill versions.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct SkillUsageTelemetry {
    pub schema_version: u32,
    pub invocation_count: u64,
    pub success_count: u64,
    pub failure_count: u64,
    pub verification_pass_count: u64,
    pub correction_count: u64,
    pub cost_delta_microusd: i64,
    pub last_used_at_unix_ms: Option<i64>,
    pub dependent_routine_refs: Vec<String>,
}

impl Default for SkillUsageTelemetry {
    fn default() -> Self {
        Self {
            schema_version: SKILL_LIFECYCLE_SCHEMA_VERSION,
            invocation_count: 0,
            success_count: 0,
            failure_count: 0,
            verification_pass_count: 0,
            correction_count: 0,
            cost_delta_microusd: 0,
            last_used_at_unix_ms: None,
            dependent_routine_refs: Vec::new(),
        }
    }
}

/// One execution-derived telemetry update.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct SkillUsageUpdate {
    pub succeeded: bool,
    pub verification_passed: bool,
    pub corrected: bool,
    pub cost_delta_microusd: i64,
    pub used_at_unix_ms: i64,
    pub dependent_routine_ref: Option<String>,
}

/// Creates an activation-ready record from signed artifact and evaluation
/// evidence, then applies the host review gate.
///
/// # Errors
/// Returns stable reason codes when signature, evaluation, taint, or review
/// requirements are not satisfied.
pub fn activate_signed_skill_version(
    artifact_sha256: String,
    eval_pack_sha256: String,
    tainted: bool,
    gate: SkillActivationGate,
    now_unix_ms: i64,
) -> Result<SkillLifecycleRecord, SkillActivationDecision> {
    let mut record = SkillLifecycleRecord::default();
    transition(
        &mut record,
        SkillLifecycleState::Staged,
        now_unix_ms,
        "skill.lifecycle.artifact_staged",
    );
    record.artifact_signed = !artifact_sha256.trim().is_empty();
    record.artifact_sha256 = artifact_sha256;
    record.eval_pack_sha256 = (!eval_pack_sha256.trim().is_empty()).then_some(eval_pack_sha256);
    record.eval_status = if record.eval_pack_sha256.is_some() {
        SkillEvaluationStatus::Passed
    } else {
        SkillEvaluationStatus::Failed
    };
    record.tainted = tainted;
    let eval_reason = if record.eval_status == SkillEvaluationStatus::Passed {
        "skill.lifecycle.eval_passed"
    } else {
        "skill.lifecycle.eval_failed"
    };
    transition(&mut record, SkillLifecycleState::Evaluated, now_unix_ms, eval_reason);
    record.reviewed_by_operator = gate.operator_approved;
    record.reviewed_by_policy = gate.policy_approved;
    transition(
        &mut record,
        SkillLifecycleState::Reviewed,
        now_unix_ms,
        "skill.lifecycle.review_recorded",
    );
    activate_existing_skill_version(&mut record, gate, now_unix_ms)?;
    Ok(record)
}

/// Evaluates all activation invariants without mutating the record.
#[must_use]
pub fn evaluate_skill_activation(
    record: &SkillLifecycleRecord,
    gate: SkillActivationGate,
) -> SkillActivationDecision {
    let mut reason_codes = Vec::new();
    if !record.artifact_signed || record.artifact_sha256.trim().is_empty() {
        reason_codes.push("skill.lifecycle.signature_required".to_owned());
    }
    if record.eval_status != SkillEvaluationStatus::Passed
        || record.eval_pack_sha256.as_deref().is_none_or(str::is_empty)
    {
        reason_codes.push("skill.lifecycle.eval_pass_required".to_owned());
    }
    if record.tainted {
        reason_codes.push("skill.lifecycle.taint_denied".to_owned());
    }
    if !(gate.operator_approved || gate.policy_approved) {
        reason_codes.push("skill.lifecycle.review_required".to_owned());
    }
    if record.state == SkillLifecycleState::Archived {
        reason_codes.push("skill.lifecycle.restore_required".to_owned());
    }
    reason_codes.sort();
    reason_codes.dedup();
    SkillActivationDecision { allowed: reason_codes.is_empty(), reason_codes }
}

/// Activates a previously installed version after rechecking every gate.
///
/// # Errors
/// Returns the same stable denial decision as [`evaluate_skill_activation`].
pub fn activate_existing_skill_version(
    record: &mut SkillLifecycleRecord,
    gate: SkillActivationGate,
    now_unix_ms: i64,
) -> Result<(), SkillActivationDecision> {
    let decision = evaluate_skill_activation(record, gate);
    if !decision.allowed {
        return Err(decision);
    }
    record.reviewed_by_operator |= gate.operator_approved;
    record.reviewed_by_policy |= gate.policy_approved;
    record.activated_at_unix_ms = Some(now_unix_ms);
    record.stale_detected_at_unix_ms = None;
    transition(record, SkillLifecycleState::Active, now_unix_ms, "skill.lifecycle.activated");
    Ok(())
}

/// Marks an inactive version stale while retaining all artifact evidence.
pub fn mark_skill_version_stale(record: &mut SkillLifecycleRecord, now_unix_ms: i64) {
    if record.pinned
        || !matches!(record.state, SkillLifecycleState::Active | SkillLifecycleState::Stale)
    {
        return;
    }
    record.stale_detected_at_unix_ms = Some(now_unix_ms);
    transition(record, SkillLifecycleState::Stale, now_unix_ms, "skill.lifecycle.stale_detected");
}

/// Records an explicit pin or unpin operation without changing execution state.
pub fn set_skill_version_pinned(record: &mut SkillLifecycleRecord, pinned: bool, now_unix_ms: i64) {
    if record.pinned == pinned {
        return;
    }
    record.pinned = pinned;
    let reason_code = if pinned { "skill.lifecycle.pinned" } else { "skill.lifecycle.unpinned" };
    transition(record, record.state, now_unix_ms, reason_code);
}

/// Replaces evaluation evidence while preserving archive and rollback posture.
pub fn record_skill_evaluation(
    record: &mut SkillLifecycleRecord,
    passed: bool,
    eval_pack_sha256: Option<String>,
    tainted: bool,
    now_unix_ms: i64,
) {
    record.eval_status =
        if passed { SkillEvaluationStatus::Passed } else { SkillEvaluationStatus::Failed };
    record.eval_pack_sha256 = eval_pack_sha256.filter(|digest| !digest.trim().is_empty());
    record.tainted = tainted;
    let evidence_allows_execution = passed && record.eval_pack_sha256.is_some() && !tainted;
    let next_state = match record.state {
        SkillLifecycleState::Archived | SkillLifecycleState::RolledBack => record.state,
        SkillLifecycleState::Active | SkillLifecycleState::Stale if evidence_allows_execution => {
            record.state
        }
        _ => SkillLifecycleState::Evaluated,
    };
    let reason_code = if evidence_allows_execution {
        "skill.lifecycle.eval_passed"
    } else {
        "skill.lifecycle.eval_failed"
    };
    transition(record, next_state, now_unix_ms, reason_code);
}

/// Archives an inactive, unpinned, unreferenced version without deleting it.
///
/// # Errors
/// Returns a stable reason when current, pinned, referenced, or already
/// archived state prevents the transition.
pub fn archive_skill_version(
    record: &mut SkillLifecycleRecord,
    is_current: bool,
    dependent_routine_count: usize,
    now_unix_ms: i64,
) -> Result<(), &'static str> {
    if is_current {
        return Err("skill.lifecycle.current_archive_denied");
    }
    if record.pinned {
        return Err("skill.lifecycle.pinned_archive_denied");
    }
    if dependent_routine_count > 0 {
        return Err("skill.lifecycle.referenced_archive_denied");
    }
    if record.state == SkillLifecycleState::Archived {
        return Err("skill.lifecycle.already_archived");
    }
    record.archived_at_unix_ms = Some(now_unix_ms);
    transition(record, SkillLifecycleState::Archived, now_unix_ms, "skill.lifecycle.archived");
    Ok(())
}

/// Restores archived evidence to a non-active state; activation remains a
/// separate reviewed operation.
///
/// # Errors
/// Returns a stable reason when the version is not archived.
pub fn restore_skill_version(
    record: &mut SkillLifecycleRecord,
    now_unix_ms: i64,
) -> Result<(), &'static str> {
    if record.state != SkillLifecycleState::Archived {
        return Err("skill.lifecycle.restore_requires_archived");
    }
    record.restored_at_unix_ms = Some(now_unix_ms);
    record.archived_at_unix_ms = None;
    let restored_state = if record.eval_status == SkillEvaluationStatus::Passed {
        SkillLifecycleState::Evaluated
    } else {
        SkillLifecycleState::Staged
    };
    transition(record, restored_state, now_unix_ms, "skill.lifecycle.restored_inactive");
    Ok(())
}

/// Records that the version was replaced by a one-operation rollback.
pub fn mark_skill_version_rolled_back(record: &mut SkillLifecycleRecord, now_unix_ms: i64) {
    record.rollback_count = record.rollback_count.saturating_add(1);
    transition(record, SkillLifecycleState::RolledBack, now_unix_ms, "skill.lifecycle.rolled_back");
}

/// Applies one execution-derived telemetry update with bounded references.
pub fn apply_skill_usage_update(telemetry: &mut SkillUsageTelemetry, update: SkillUsageUpdate) {
    telemetry.schema_version = SKILL_LIFECYCLE_SCHEMA_VERSION;
    telemetry.invocation_count = telemetry.invocation_count.saturating_add(1);
    if update.succeeded {
        telemetry.success_count = telemetry.success_count.saturating_add(1);
    } else {
        telemetry.failure_count = telemetry.failure_count.saturating_add(1);
    }
    if update.verification_passed {
        telemetry.verification_pass_count = telemetry.verification_pass_count.saturating_add(1);
    }
    if update.corrected {
        telemetry.correction_count = telemetry.correction_count.saturating_add(1);
    }
    telemetry.cost_delta_microusd =
        telemetry.cost_delta_microusd.saturating_add(update.cost_delta_microusd);
    telemetry.last_used_at_unix_ms =
        Some(telemetry.last_used_at_unix_ms.unwrap_or(i64::MIN).max(update.used_at_unix_ms));
    if let Some(reference) = update
        .dependent_routine_ref
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
    {
        if !telemetry.dependent_routine_refs.contains(&reference)
            && telemetry.dependent_routine_refs.len() < MAX_SKILL_DEPENDENT_ROUTINE_REFS
        {
            telemetry.dependent_routine_refs.push(reference);
            telemetry.dependent_routine_refs.sort();
        }
    }
}

fn transition(
    record: &mut SkillLifecycleRecord,
    to: SkillLifecycleState,
    at_unix_ms: i64,
    reason_code: &str,
) {
    let from = (!record.transitions.is_empty()).then_some(record.state);
    record.schema_version = SKILL_LIFECYCLE_SCHEMA_VERSION;
    record.state = to;
    record.transitions.push(SkillLifecycleTransition {
        from,
        to,
        at_unix_ms,
        reason_code: reason_code.to_owned(),
    });
    if record.transitions.len() > MAX_SKILL_LIFECYCLE_TRANSITIONS {
        let overflow = record.transitions.len() - MAX_SKILL_LIFECYCLE_TRANSITIONS;
        record.transitions.drain(0..overflow);
    }
}

#[cfg(test)]
mod tests {
    use super::{
        activate_existing_skill_version, activate_signed_skill_version, apply_skill_usage_update,
        archive_skill_version, evaluate_skill_activation, mark_skill_version_stale,
        record_skill_evaluation, restore_skill_version, set_skill_version_pinned,
        SkillActivationGate, SkillEvaluationStatus, SkillLifecycleRecord, SkillLifecycleState,
        SkillUsageTelemetry, SkillUsageUpdate,
    };

    fn operator_gate() -> SkillActivationGate {
        SkillActivationGate { operator_approved: true, policy_approved: false }
    }

    #[test]
    fn signed_evaluated_reviewed_version_activates() {
        let record = activate_signed_skill_version(
            "artifact".to_owned(),
            "eval".to_owned(),
            false,
            operator_gate(),
            10,
        )
        .expect("all activation gates should pass");

        assert_eq!(record.state, SkillLifecycleState::Active);
        assert_eq!(record.eval_status, SkillEvaluationStatus::Passed);
        assert!(record.reviewed_by_operator);
        assert_eq!(record.transitions.len(), 4);
    }

    #[test]
    fn eval_failure_and_taint_deny_activation() {
        let mut record = SkillLifecycleRecord {
            artifact_signed: true,
            artifact_sha256: "artifact".to_owned(),
            eval_status: SkillEvaluationStatus::Failed,
            tainted: true,
            ..SkillLifecycleRecord::default()
        };
        let decision = evaluate_skill_activation(&record, operator_gate());
        assert!(!decision.allowed);
        assert!(decision.reason_codes.contains(&"skill.lifecycle.eval_pass_required".to_owned()));
        assert!(decision.reason_codes.contains(&"skill.lifecycle.taint_denied".to_owned()));
        assert!(activate_existing_skill_version(&mut record, operator_gate(), 10).is_err());
    }

    #[test]
    fn archive_and_restore_preserve_inactive_evidence() {
        let mut record = activate_signed_skill_version(
            "artifact".to_owned(),
            "eval".to_owned(),
            false,
            operator_gate(),
            10,
        )
        .expect("version should activate");
        assert_eq!(
            archive_skill_version(&mut record, true, 0, 20),
            Err("skill.lifecycle.current_archive_denied")
        );
        assert_eq!(
            archive_skill_version(&mut record, false, 1, 20),
            Err("skill.lifecycle.referenced_archive_denied")
        );
        archive_skill_version(&mut record, false, 0, 20).expect("inactive version should archive");
        assert_eq!(record.state, SkillLifecycleState::Archived);

        restore_skill_version(&mut record, 30).expect("archived version should restore");
        assert_eq!(record.state, SkillLifecycleState::Evaluated);
        assert!(record.artifact_signed);
        assert_eq!(record.artifact_sha256, "artifact");
    }

    #[test]
    fn pin_and_failed_evaluation_keep_durable_transition_evidence() {
        let mut record = activate_signed_skill_version(
            "artifact".to_owned(),
            "eval".to_owned(),
            false,
            operator_gate(),
            10,
        )
        .expect("version should activate");

        set_skill_version_pinned(&mut record, true, 20);
        assert!(record.pinned);
        assert_eq!(
            record.transitions.last().map(|transition| transition.reason_code.as_str()),
            Some("skill.lifecycle.pinned")
        );

        record_skill_evaluation(&mut record, false, None, true, 30);
        assert_eq!(record.state, SkillLifecycleState::Evaluated);
        assert_eq!(record.eval_status, SkillEvaluationStatus::Failed);
        assert!(record.tainted);
        assert!(!evaluate_skill_activation(&record, operator_gate()).allowed);
    }

    #[test]
    fn stale_detection_cannot_promote_unreviewed_evidence() {
        let mut record = SkillLifecycleRecord::default();
        mark_skill_version_stale(&mut record, 10);
        assert_eq!(record.state, SkillLifecycleState::Observed);
        assert!(!record.state.is_executable());
        assert!(record.transitions.is_empty());
    }

    #[test]
    fn usage_updates_are_saturating_monotonic_and_bounded() {
        let mut telemetry = SkillUsageTelemetry::default();
        apply_skill_usage_update(
            &mut telemetry,
            SkillUsageUpdate {
                succeeded: true,
                verification_passed: true,
                corrected: false,
                cost_delta_microusd: -5,
                used_at_unix_ms: 20,
                dependent_routine_ref: Some("routine:b".to_owned()),
            },
        );
        apply_skill_usage_update(
            &mut telemetry,
            SkillUsageUpdate {
                succeeded: false,
                verification_passed: false,
                corrected: true,
                cost_delta_microusd: 9,
                used_at_unix_ms: 10,
                dependent_routine_ref: Some("routine:a".to_owned()),
            },
        );

        assert_eq!(telemetry.invocation_count, 2);
        assert_eq!(telemetry.success_count, 1);
        assert_eq!(telemetry.failure_count, 1);
        assert_eq!(telemetry.verification_pass_count, 1);
        assert_eq!(telemetry.correction_count, 1);
        assert_eq!(telemetry.cost_delta_microusd, 4);
        assert_eq!(telemetry.last_used_at_unix_ms, Some(20));
        assert_eq!(telemetry.dependent_routine_refs, ["routine:a", "routine:b"]);
    }
}
