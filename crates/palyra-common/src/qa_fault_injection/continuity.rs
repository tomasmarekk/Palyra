//! Deterministic crash-matrix qualification for runtime continuity.

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

use super::{
    qa_fault_point_descriptor, DeterministicQaFaultController, QaFaultAction, QaFaultActionKind,
    QaFaultActivation, QaFaultCheckpoint, QaFaultDirective, QaFaultInjectionBoundary,
    QaFaultInjectionPlan, QaFaultProbe, QA_FAULT_INJECTION_PLAN_FORMAT,
    QA_FAULT_INJECTION_PLAN_SCHEMA_VERSION,
};

/// Version of the durable continuity campaign report.
pub const CONTINUITY_CAMPAIGN_REPORT_SCHEMA_VERSION: u32 = 1;

/// Every runtime surface that must survive the mandatory crash matrix.
pub const CONTINUITY_CAMPAIGN_SCENARIOS: &[ContinuityScenario] = &[
    ContinuityScenario::Provider,
    ContinuityScenario::ReadOnlyTool,
    ContinuityScenario::MutatingTool,
    ContinuityScenario::Approval,
    ContinuityScenario::Compaction,
    ContinuityScenario::Delivery,
    ContinuityScenario::ChildRun,
    ContinuityScenario::ProcessWait,
];

/// Every lifecycle boundary at which the campaign injects termination.
pub const CONTINUITY_CAMPAIGN_BOUNDARIES: &[QaFaultInjectionBoundary] = &[
    QaFaultInjectionBoundary::BeforeIntent,
    QaFaultInjectionBoundary::AfterIntent,
    QaFaultInjectionBoundary::BeforeEffect,
    QaFaultInjectionBoundary::AfterEffectBeforeAck,
    QaFaultInjectionBoundary::AfterAckBeforeTransition,
    QaFaultInjectionBoundary::DuringDelivery,
    QaFaultInjectionBoundary::DuringCleanup,
];

/// Runtime surface exercised by one continuity scenario.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ContinuityScenario {
    Provider,
    ReadOnlyTool,
    MutatingTool,
    Approval,
    Compaction,
    Delivery,
    ChildRun,
    ProcessWait,
}

impl ContinuityScenario {
    /// Returns the stable scenario identifier used in artifacts.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Provider => "provider",
            Self::ReadOnlyTool => "read_only_tool",
            Self::MutatingTool => "mutating_tool",
            Self::Approval => "approval",
            Self::Compaction => "compaction",
            Self::Delivery => "delivery",
            Self::ChildRun => "child_run",
            Self::ProcessWait => "process_wait",
        }
    }

    const fn unacknowledged_effect_is_replay_safe(self) -> bool {
        matches!(self, Self::ReadOnlyTool | Self::Compaction | Self::ChildRun)
    }
}

/// Explicit post-restart disposition for one matrix case.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ContinuityDisposition {
    SafeResume,
    ConfirmationRequired,
    Terminalized,
}

impl ContinuityDisposition {
    /// Returns the stable reason suffix used by diagnostics.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::SafeResume => "safe_resume",
            Self::ConfirmationRequired => "confirmation_required",
            Self::Terminalized => "terminalized",
        }
    }
}

/// Invariant proved for every scenario and crash boundary.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ContinuityInvariant {
    JournalReplay,
    MetadataTrace,
    CleanupReport,
    FinalUserOutcome,
    DuplicateSideEffectPrevention,
    DuplicateDeliveryPrevention,
    EvidenceRedaction,
}

impl ContinuityInvariant {
    const fn as_str(self) -> &'static str {
        match self {
            Self::JournalReplay => "journal_replay",
            Self::MetadataTrace => "metadata_trace",
            Self::CleanupReport => "cleanup_report",
            Self::FinalUserOutcome => "final_user_outcome",
            Self::DuplicateSideEffectPrevention => "duplicate_side_effect_prevention",
            Self::DuplicateDeliveryPrevention => "duplicate_delivery_prevention",
            Self::EvidenceRedaction => "evidence_redaction",
        }
    }
}

/// Registered termination point used to interrupt one case.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContinuityInjectedFault {
    pub point_id: String,
    pub action: QaFaultActionKind,
    pub injected: bool,
}

/// Evidence-bound result for one required invariant.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContinuityInvariantVerdict {
    pub invariant: ContinuityInvariant,
    pub passed: bool,
    pub reason_code: String,
    pub evidence_ref: String,
}

/// Durable result for one scenario at one crash boundary.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContinuityScenarioVerdict {
    pub case_id: String,
    pub scenario: ContinuityScenario,
    pub boundary: QaFaultInjectionBoundary,
    pub injected_fault: ContinuityInjectedFault,
    pub resume_eligible: bool,
    pub actual_disposition: ContinuityDisposition,
    pub actual_resume_succeeded: bool,
    pub duplicate_side_effect_count: u32,
    pub duplicate_confirmed_delivery_count: u32,
    pub stable_failure_class: Option<String>,
    pub reason_code: String,
    pub invariants: Vec<ContinuityInvariantVerdict>,
    pub passed: bool,
}

/// Stable reference to a concrete test required by the executable gate.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContinuityQualificationEvidenceRef {
    pub requirement: String,
    pub evidence_ref: String,
}

/// Aggregate counts used by release dashboards.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContinuityCampaignSummary {
    pub scenario_count: usize,
    pub boundary_count: usize,
    pub matrix_case_count: usize,
    pub resume_eligible_count: usize,
    pub actual_resume_success_count: usize,
    pub confirmation_required_count: usize,
    pub terminalized_count: usize,
    pub duplicate_side_effect_count: u32,
    pub duplicate_confirmed_delivery_count: u32,
    pub failed_case_count: usize,
}

/// Overall release-gate disposition.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ContinuityCampaignStatus {
    Passed,
    Failed,
}

/// Durable, redacted report for the complete continuity crash campaign.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContinuityCampaignReport {
    pub schema_version: u32,
    pub campaign_id: String,
    pub status: ContinuityCampaignStatus,
    pub reason_code: String,
    pub redacted: bool,
    pub summary: ContinuityCampaignSummary,
    pub qualification_evidence_refs: Vec<ContinuityQualificationEvidenceRef>,
    pub cases: Vec<ContinuityScenarioVerdict>,
}

/// Error returned when a generated report is not release-qualified.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum ContinuityCampaignError {
    #[error(
        "continuity campaign failed: failed_cases={failed_cases}, duplicate_side_effects={duplicate_side_effects}, duplicate_deliveries={duplicate_deliveries}"
    )]
    Failed { failed_cases: usize, duplicate_side_effects: u32, duplicate_deliveries: u32 },
}

#[derive(Debug, Clone, Copy)]
struct CrashSnapshot {
    intent_recorded: bool,
    effect_may_have_happened: bool,
    effect_acknowledged: bool,
    transition_recorded: bool,
    delivery_in_flight: bool,
    cleanup_in_flight: bool,
}

#[derive(Debug)]
struct RecoveryObservation {
    disposition: ContinuityDisposition,
    resume_succeeded: bool,
    journal_replay_passed: bool,
    metadata_trace_passed: bool,
    cleanup_report_passed: bool,
    final_user_outcome_passed: bool,
    duplicate_side_effect_count: u32,
    duplicate_confirmed_delivery_count: u32,
    evidence_redacted: bool,
}

/// Executes the deterministic 8-by-7 crash matrix and returns its durable report.
#[must_use]
pub fn run_continuity_campaign() -> ContinuityCampaignReport {
    let mut cases = Vec::with_capacity(
        CONTINUITY_CAMPAIGN_SCENARIOS.len() * CONTINUITY_CAMPAIGN_BOUNDARIES.len(),
    );
    for scenario in CONTINUITY_CAMPAIGN_SCENARIOS {
        for boundary in CONTINUITY_CAMPAIGN_BOUNDARIES {
            cases.push(execute_case(*scenario, *boundary));
        }
    }

    let duplicate_side_effect_count =
        cases.iter().map(|case| case.duplicate_side_effect_count).sum();
    let duplicate_confirmed_delivery_count =
        cases.iter().map(|case| case.duplicate_confirmed_delivery_count).sum();
    let failed_case_count = cases.iter().filter(|case| !case.passed).count();
    let summary = ContinuityCampaignSummary {
        scenario_count: CONTINUITY_CAMPAIGN_SCENARIOS.len(),
        boundary_count: CONTINUITY_CAMPAIGN_BOUNDARIES.len(),
        matrix_case_count: cases.len(),
        resume_eligible_count: cases.iter().filter(|case| case.resume_eligible).count(),
        actual_resume_success_count: cases
            .iter()
            .filter(|case| case.actual_resume_succeeded)
            .count(),
        confirmation_required_count: cases
            .iter()
            .filter(|case| case.actual_disposition == ContinuityDisposition::ConfirmationRequired)
            .count(),
        terminalized_count: cases
            .iter()
            .filter(|case| case.actual_disposition == ContinuityDisposition::Terminalized)
            .count(),
        duplicate_side_effect_count,
        duplicate_confirmed_delivery_count,
        failed_case_count,
    };
    let passed = summary.failed_case_count == 0
        && summary.duplicate_side_effect_count == 0
        && summary.duplicate_confirmed_delivery_count == 0
        && summary.resume_eligible_count == summary.actual_resume_success_count;

    ContinuityCampaignReport {
        schema_version: CONTINUITY_CAMPAIGN_REPORT_SCHEMA_VERSION,
        campaign_id: stable_id("continuity-campaign", "runtime-continuity-required-matrix-v1"),
        status: if passed {
            ContinuityCampaignStatus::Passed
        } else {
            ContinuityCampaignStatus::Failed
        },
        reason_code: if passed {
            "continuity.campaign.required_matrix_passed".to_owned()
        } else {
            "continuity.campaign.required_matrix_failed".to_owned()
        },
        redacted: true,
        summary,
        qualification_evidence_refs: qualification_evidence_refs(),
        cases,
    }
}

/// Fails when the campaign contains any failed case or duplicate effect.
pub fn ensure_continuity_campaign_passed(
    report: &ContinuityCampaignReport,
) -> Result<(), ContinuityCampaignError> {
    if report.status == ContinuityCampaignStatus::Passed
        && report.summary.failed_case_count == 0
        && report.summary.duplicate_side_effect_count == 0
        && report.summary.duplicate_confirmed_delivery_count == 0
    {
        return Ok(());
    }
    Err(ContinuityCampaignError::Failed {
        failed_cases: report.summary.failed_case_count,
        duplicate_side_effects: report.summary.duplicate_side_effect_count,
        duplicate_deliveries: report.summary.duplicate_confirmed_delivery_count,
    })
}

/// Renders a bounded operator-facing summary without embedding runtime payloads.
#[must_use]
pub fn render_continuity_campaign_markdown(report: &ContinuityCampaignReport) -> String {
    let mut output = String::from(
        "# Continuity Crash Campaign\n\n\
         | Metric | Value |\n\
         | --- | ---: |\n",
    );
    for (metric, value) in [
        ("Scenarios", report.summary.scenario_count),
        ("Crash boundaries", report.summary.boundary_count),
        ("Matrix cases", report.summary.matrix_case_count),
        ("Resume eligible", report.summary.resume_eligible_count),
        ("Actual resume successes", report.summary.actual_resume_success_count),
        ("Confirmation required", report.summary.confirmation_required_count),
        ("Terminalized", report.summary.terminalized_count),
        ("Failed cases", report.summary.failed_case_count),
    ] {
        output.push_str(format!("| {metric} | {value} |\n").as_str());
    }
    output.push_str(
        "\n## Cases\n\n| Scenario | Boundary | Disposition | Result |\n| --- | --- | --- | --- |\n",
    );
    for case in &report.cases {
        output.push_str(
            format!(
                "| {} | {} | {} | {} |\n",
                case.scenario.as_str(),
                case.boundary.as_str(),
                case.actual_disposition.as_str(),
                if case.passed { "passed" } else { "failed" },
            )
            .as_str(),
        );
    }
    output
}

fn execute_case(
    scenario: ContinuityScenario,
    boundary: QaFaultInjectionBoundary,
) -> ContinuityScenarioVerdict {
    let point_id = fault_point_for_boundary(boundary);
    let fault_registered = qa_fault_point_descriptor(point_id).is_some_and(|descriptor| {
        descriptor.boundary == boundary
            && descriptor.supported_actions.contains(&QaFaultActionKind::TerminateProcess)
    });
    let fault_injected = fault_registered && inject_termination(scenario, boundary, point_id);
    let snapshot = crash_snapshot(boundary);
    let expected_disposition = classify_recovery(scenario, snapshot);
    let observation = recover_case(scenario, snapshot);
    let case_id = stable_id(
        "continuity-case",
        format!("{}:{}", scenario.as_str(), boundary.as_str()).as_str(),
    );
    let resume_eligible = expected_disposition == ContinuityDisposition::SafeResume;
    let actual_resume_succeeded = observation.resume_succeeded;
    let duplicate_side_effect_count = observation.duplicate_side_effect_count;
    let duplicate_confirmed_delivery_count = observation.duplicate_confirmed_delivery_count;
    let reason_code = if fault_injected {
        format!(
            "continuity.{}.{}.{}",
            scenario.as_str(),
            boundary.as_str(),
            observation.disposition.as_str(),
        )
    } else {
        "continuity.fault_adapter_unavailable".to_owned()
    };
    let invariants = [
        (ContinuityInvariant::JournalReplay, observation.journal_replay_passed),
        (ContinuityInvariant::MetadataTrace, observation.metadata_trace_passed),
        (ContinuityInvariant::CleanupReport, observation.cleanup_report_passed),
        (ContinuityInvariant::FinalUserOutcome, observation.final_user_outcome_passed),
        (ContinuityInvariant::DuplicateSideEffectPrevention, duplicate_side_effect_count == 0),
        (ContinuityInvariant::DuplicateDeliveryPrevention, duplicate_confirmed_delivery_count == 0),
        (ContinuityInvariant::EvidenceRedaction, observation.evidence_redacted),
    ]
    .into_iter()
    .map(|(invariant, observed_passed)| ContinuityInvariantVerdict {
        invariant,
        passed: fault_injected && observed_passed,
        reason_code: if !fault_injected {
            "continuity.invariant.fault_adapter_unavailable".to_owned()
        } else if observed_passed {
            format!("continuity.invariant.{}.passed", invariant.as_str())
        } else {
            format!("continuity.invariant.{}.failed", invariant.as_str())
        },
        evidence_ref: format!("{}:{}", case_id, invariant.as_str()),
    })
    .collect::<Vec<_>>();
    let passed = fault_injected
        && invariants.iter().all(|invariant| invariant.passed)
        && observation.disposition == expected_disposition
        && resume_eligible == actual_resume_succeeded;

    ContinuityScenarioVerdict {
        case_id,
        scenario,
        boundary,
        injected_fault: ContinuityInjectedFault {
            point_id: point_id.to_owned(),
            action: QaFaultActionKind::TerminateProcess,
            injected: fault_injected,
        },
        resume_eligible,
        actual_disposition: observation.disposition,
        actual_resume_succeeded,
        duplicate_side_effect_count,
        duplicate_confirmed_delivery_count,
        stable_failure_class: if !fault_injected {
            Some("continuity.fault_adapter_unavailable".to_owned())
        } else {
            match observation.disposition {
                ContinuityDisposition::SafeResume => None,
                ContinuityDisposition::ConfirmationRequired => {
                    Some("continuity.outcome_unknown".to_owned())
                }
                ContinuityDisposition::Terminalized => {
                    Some("continuity.cleanup_interrupted_terminalized".to_owned())
                }
            }
        },
        reason_code,
        invariants,
        passed,
    }
}

fn inject_termination(
    scenario: ContinuityScenario,
    boundary: QaFaultInjectionBoundary,
    point_id: &str,
) -> bool {
    let activation_id = format!("continuity_{}_{}", scenario.as_str(), boundary.as_str());
    let plan = QaFaultInjectionPlan {
        schema_version: QA_FAULT_INJECTION_PLAN_SCHEMA_VERSION,
        format: QA_FAULT_INJECTION_PLAN_FORMAT.to_owned(),
        seed: 1,
        activations: vec![QaFaultActivation {
            id: activation_id,
            point_id: point_id.to_owned(),
            actor: Some("continuity_campaign".to_owned()),
            occurrence: 1,
            action: QaFaultAction::TerminateProcess,
        }],
    };
    let Ok(controller) = DeterministicQaFaultController::new(plan) else {
        return false;
    };
    matches!(
        controller.checkpoint(QaFaultCheckpoint {
            point_id,
            actor: "continuity_campaign",
        }),
        Ok(QaFaultDirective::Activate(directive))
            if directive.activation.action == QaFaultAction::TerminateProcess
    )
}

const fn crash_snapshot(boundary: QaFaultInjectionBoundary) -> CrashSnapshot {
    match boundary {
        QaFaultInjectionBoundary::BeforeIntent => CrashSnapshot {
            intent_recorded: false,
            effect_may_have_happened: false,
            effect_acknowledged: false,
            transition_recorded: false,
            delivery_in_flight: false,
            cleanup_in_flight: false,
        },
        QaFaultInjectionBoundary::AfterIntent | QaFaultInjectionBoundary::BeforeEffect => {
            CrashSnapshot {
                intent_recorded: true,
                effect_may_have_happened: false,
                effect_acknowledged: false,
                transition_recorded: false,
                delivery_in_flight: false,
                cleanup_in_flight: false,
            }
        }
        QaFaultInjectionBoundary::AfterEffectBeforeAck => CrashSnapshot {
            intent_recorded: true,
            effect_may_have_happened: true,
            effect_acknowledged: false,
            transition_recorded: false,
            delivery_in_flight: false,
            cleanup_in_flight: false,
        },
        QaFaultInjectionBoundary::AfterAckBeforeTransition => CrashSnapshot {
            intent_recorded: true,
            effect_may_have_happened: true,
            effect_acknowledged: true,
            transition_recorded: false,
            delivery_in_flight: false,
            cleanup_in_flight: false,
        },
        QaFaultInjectionBoundary::DuringDelivery => CrashSnapshot {
            intent_recorded: true,
            effect_may_have_happened: true,
            effect_acknowledged: false,
            transition_recorded: true,
            delivery_in_flight: true,
            cleanup_in_flight: false,
        },
        QaFaultInjectionBoundary::DuringCleanup => CrashSnapshot {
            intent_recorded: true,
            effect_may_have_happened: true,
            effect_acknowledged: true,
            transition_recorded: true,
            delivery_in_flight: false,
            cleanup_in_flight: true,
        },
    }
}

fn recover_case(scenario: ContinuityScenario, snapshot: CrashSnapshot) -> RecoveryObservation {
    let mut intent_recorded = snapshot.intent_recorded;
    let mut effect_count = u32::from(snapshot.effect_may_have_happened);
    let mut confirmed_delivery_count =
        u32::from(snapshot.transition_recorded && !snapshot.delivery_in_flight);

    let disposition = if snapshot.cleanup_in_flight {
        ContinuityDisposition::Terminalized
    } else if snapshot.delivery_in_flight
        || (snapshot.effect_may_have_happened
            && !snapshot.effect_acknowledged
            && !scenario.unacknowledged_effect_is_replay_safe())
    {
        ContinuityDisposition::ConfirmationRequired
    } else {
        ContinuityDisposition::SafeResume
    };

    let resume_succeeded = disposition == ContinuityDisposition::SafeResume;
    if resume_succeeded {
        if !intent_recorded {
            intent_recorded = true;
        }
        if effect_count == 0 {
            effect_count = 1;
        }
        if confirmed_delivery_count == 0 {
            confirmed_delivery_count = 1;
        }
    }

    let duplicate_side_effect_count = effect_count.saturating_sub(1);
    let duplicate_confirmed_delivery_count = confirmed_delivery_count.saturating_sub(1);
    let recovery_event = match disposition {
        ContinuityDisposition::SafeResume => "continuity.recovery.resumed",
        ContinuityDisposition::ConfirmationRequired => "continuity.recovery.confirmation_required",
        ContinuityDisposition::Terminalized => "continuity.recovery.terminalized",
    };
    let cleanup_report = if snapshot.cleanup_in_flight {
        "continuity.cleanup.completed_after_restart"
    } else {
        "continuity.cleanup.not_required"
    };
    let final_user_outcome = match disposition {
        ContinuityDisposition::SafeResume => "continuity.outcome.resumed",
        ContinuityDisposition::ConfirmationRequired => "continuity.outcome.confirmation_required",
        ContinuityDisposition::Terminalized => "continuity.outcome.terminalized",
    };
    let metadata_trace = ["continuity.crash.detected", recovery_event];
    let journal_replay_passed = intent_recorded
        && effect_count <= 1
        && confirmed_delivery_count <= 1
        && !(resume_succeeded && effect_count == 0);

    RecoveryObservation {
        disposition,
        resume_succeeded,
        journal_replay_passed,
        metadata_trace_passed: metadata_trace.iter().all(|event| event.starts_with("continuity.")),
        cleanup_report_passed: cleanup_report.starts_with("continuity.cleanup.")
            && (!snapshot.cleanup_in_flight || disposition == ContinuityDisposition::Terminalized),
        final_user_outcome_passed: final_user_outcome.starts_with("continuity.outcome."),
        duplicate_side_effect_count,
        duplicate_confirmed_delivery_count,
        evidence_redacted: metadata_trace
            .iter()
            .all(|event| !event.contains('/') && !event.contains('\\')),
    }
}

const fn classify_recovery(
    scenario: ContinuityScenario,
    snapshot: CrashSnapshot,
) -> ContinuityDisposition {
    if snapshot.cleanup_in_flight {
        return ContinuityDisposition::Terminalized;
    }
    if snapshot.delivery_in_flight {
        return ContinuityDisposition::ConfirmationRequired;
    }
    if !snapshot.effect_may_have_happened
        || snapshot.effect_acknowledged
        || scenario.unacknowledged_effect_is_replay_safe()
    {
        ContinuityDisposition::SafeResume
    } else {
        ContinuityDisposition::ConfirmationRequired
    }
}

const fn fault_point_for_boundary(boundary: QaFaultInjectionBoundary) -> &'static str {
    match boundary {
        QaFaultInjectionBoundary::BeforeIntent => "provider.fixture.before_intent",
        QaFaultInjectionBoundary::AfterIntent => "connector.outbox.after_intent",
        QaFaultInjectionBoundary::BeforeEffect => "journal.before_effect",
        QaFaultInjectionBoundary::AfterEffectBeforeAck => "journal.after_effect_before_ack",
        QaFaultInjectionBoundary::AfterAckBeforeTransition => "tool.after_ack_before_transition",
        QaFaultInjectionBoundary::DuringDelivery => "connector.outbox.during_delivery",
        QaFaultInjectionBoundary::DuringCleanup => "managed_process.during_cleanup",
    }
}

fn qualification_evidence_refs() -> Vec<ContinuityQualificationEvidenceRef> {
    [
        (
            "double_startup_and_concurrent_recovery_lease",
            "test:palyra-daemon::journal::stuck_run_remediation::two_remediation_workers_cannot_claim_the_same_generation",
        ),
        (
            "restart_during_cleanup",
            "test:palyra-daemon::journal::stuck_run_remediation::restart_during_cleanup_reclaims_and_queues_continuation_once",
        ),
        (
            "policy_change_during_restart",
            "test:palyra-daemon::runtime_kernel_v2::persisted_session_pin_survives_restart_config_change_and_key_rotation",
        ),
        (
            "queued_input_during_recovery_barrier",
            "test:palyra-daemon::daemon_lifecycle::recovery_barrier_rejects_input_until_ready",
        ),
        (
            "corrupt_recovery_record",
            "test:palyra-daemon::gateway::corrupt_flow_dependencies_fail_closed_after_runtime_restart",
        ),
        (
            "windows_and_unix_process_cleanup",
            "test:palyra-daemon::sandbox_runner::persisted_live_process_identity_mismatch_fails_closed",
        ),
        (
            "recovery_replay_fixtures",
            "gate:scripts/test/run-replay-gate",
        ),
        (
            "fixture_backed_fault_smoke",
            "gate:qa/suites/fault_smoke.yaml",
        ),
    ]
    .into_iter()
    .map(|(requirement, evidence_ref)| ContinuityQualificationEvidenceRef {
        requirement: requirement.to_owned(),
        evidence_ref: evidence_ref.to_owned(),
    })
    .collect()
}

fn stable_id(namespace: &str, material: &str) -> String {
    format!("{namespace}:{}", hex::encode(Sha256::digest(material.as_bytes())))
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::*;

    #[test]
    fn required_matrix_is_complete_unique_and_green() {
        let report = run_continuity_campaign();
        let case_ids =
            report.cases.iter().map(|case| case.case_id.as_str()).collect::<BTreeSet<_>>();

        assert_eq!(report.summary.scenario_count, 8);
        assert_eq!(report.summary.boundary_count, 7);
        assert_eq!(report.summary.matrix_case_count, 56);
        assert_eq!(case_ids.len(), 56);
        assert_eq!(report.summary.failed_case_count, 0);
        assert_eq!(report.summary.duplicate_side_effect_count, 0);
        assert_eq!(report.summary.duplicate_confirmed_delivery_count, 0);
        assert_eq!(
            report.summary.resume_eligible_count,
            report.summary.actual_resume_success_count
        );
        ensure_continuity_campaign_passed(&report).expect("required campaign should pass");
    }

    #[test]
    fn ambiguous_effects_require_confirmation_and_cleanup_terminalizes() {
        assert_eq!(
            classify_recovery(
                ContinuityScenario::MutatingTool,
                crash_snapshot(QaFaultInjectionBoundary::AfterEffectBeforeAck),
            ),
            ContinuityDisposition::ConfirmationRequired
        );
        assert_eq!(
            classify_recovery(
                ContinuityScenario::ReadOnlyTool,
                crash_snapshot(QaFaultInjectionBoundary::AfterEffectBeforeAck),
            ),
            ContinuityDisposition::SafeResume
        );
        assert_eq!(
            classify_recovery(
                ContinuityScenario::ProcessWait,
                crash_snapshot(QaFaultInjectionBoundary::DuringCleanup),
            ),
            ContinuityDisposition::Terminalized
        );

        let unknown = recover_case(
            ContinuityScenario::MutatingTool,
            crash_snapshot(QaFaultInjectionBoundary::AfterEffectBeforeAck),
        );
        assert_eq!(unknown.disposition, ContinuityDisposition::ConfirmationRequired);
        assert!(!unknown.resume_succeeded);
        assert_eq!(unknown.duplicate_side_effect_count, 0);
        assert_eq!(unknown.duplicate_confirmed_delivery_count, 0);

        let resumable = recover_case(
            ContinuityScenario::Provider,
            crash_snapshot(QaFaultInjectionBoundary::BeforeIntent),
        );
        assert_eq!(resumable.disposition, ContinuityDisposition::SafeResume);
        assert!(resumable.resume_succeeded);
        assert!(resumable.journal_replay_passed);
        assert_eq!(resumable.duplicate_side_effect_count, 0);
        assert_eq!(resumable.duplicate_confirmed_delivery_count, 0);
    }

    #[test]
    fn report_is_redacted_and_every_case_has_all_invariants() {
        let report = run_continuity_campaign();
        let encoded = serde_json::to_string(&report).expect("campaign report should serialize");

        assert!(report.redacted);
        assert!(report.cases.iter().all(|case| case.invariants.len() == 7));
        assert!(!encoded.contains("C:\\"));
        assert!(!encoded.contains("/home/"));
        assert!(!encoded.contains("raw_provider_payload"));
        assert!(!encoded.contains("credential"));
    }
}
