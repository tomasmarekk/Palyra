//! Durable fault-evidence records and the bounded NDJSON facade.
//!
//! Campaign semantics are validated before records escape this module.

use std::{collections::BTreeMap, error::Error, fmt};

use serde::{Deserialize, Serialize};
use thiserror::Error as ThisError;

use super::{
    evidence_validation::{
        decode_fault_evidence_sidecar_records, validate_fault_evidence_sidecar,
        SidecarCampaignValidationMode,
    },
    launch::QaFaultLaunchDocument,
    plan::{QaFaultAction, QaFaultInjectionPlan, QaFaultRecoveryClass},
    QA_FAULT_EVIDENCE_SIDECAR_MAX_BYTES,
};

/// First sidecar record proving that the daemon loaded an authorized plan.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QaFaultLaunchLoadedRecord {
    /// Sidecar record schema version.
    pub schema_version: u32,
    /// One-based sequence across the persistent campaign.
    pub sequence: u32,
    /// Unique launch identifier loaded at this sequence.
    pub launch_id: String,
    /// Canonical digest shared by every campaign launch.
    pub plan_sha256: String,
    /// Digest of this launch's separate capability file.
    pub capability_sha256: String,
}

/// Durable non-activating checkpoint occurrence needed for restart reproduction.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QaFaultCheckpointObservedRecord {
    /// Sidecar record schema version.
    pub schema_version: u32,
    /// One-based sequence across the persistent campaign.
    pub sequence: u32,
    /// Previously loaded launch that observed this checkpoint.
    pub launch_id: String,
    /// Canonical digest shared by every campaign launch.
    pub plan_sha256: String,
    /// Exact registered fault point reached by the actor.
    pub point_id: String,
    /// Bounded actor whose occurrence counter advanced.
    pub actor: String,
    /// One-based per-point, per-actor occurrence observed by the controller.
    pub occurrence: u32,
}

/// Durable arrival of one actor at a deterministic multi-actor barrier.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QaFaultBarrierJoinedRecord {
    /// Sidecar record schema version.
    pub schema_version: u32,
    /// One-based sequence across the persistent campaign.
    pub sequence: u32,
    /// Previously loaded launch that observed this actor.
    pub launch_id: String,
    /// Canonical digest shared by every campaign launch.
    pub plan_sha256: String,
    /// Unique barrier activation id from the plan.
    pub activation_id: String,
    /// Exact registered barrier point reached by the actor.
    pub point_id: String,
    /// Bounded actor that joined without crossing the protected side effect.
    pub actor: String,
    /// One-based occurrence selected by the plan for this actor.
    pub occurrence: u32,
}

/// Sidecar record written durably before an injected directive is applied.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QaFaultRuleActivatedRecord {
    /// Sidecar record schema version.
    pub schema_version: u32,
    /// One-based sequence across the persistent campaign.
    pub sequence: u32,
    /// Previously loaded launch that activated the rule.
    pub launch_id: String,
    /// Canonical digest shared by every campaign launch.
    pub plan_sha256: String,
    /// Unique activation id from the plan.
    pub activation_id: String,
    /// Exact registered point reached by the adapter.
    pub point_id: String,
    /// Complete bounded actor set participating in this activation.
    pub actors: Vec<String>,
    /// One-based occurrence selected by the plan.
    pub occurrence: u32,
    /// Closed action durably recorded before it is applied.
    pub action: QaFaultAction,
    /// One-based ordering among distinct campaign activations.
    pub activation_sequence: u32,
    /// Seeded release order; non-barrier records contain the single actor.
    pub release_order: Vec<String>,
}

/// Durable consumption of one seeded barrier release before its checkpoint returns.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QaFaultBarrierReleasedRecord {
    /// Sidecar record schema version.
    pub schema_version: u32,
    /// One-based sequence across the persistent campaign.
    pub sequence: u32,
    /// Previously loaded launch that consumed this release.
    pub launch_id: String,
    /// Canonical digest shared by every campaign launch.
    pub plan_sha256: String,
    /// Unique barrier activation id from the plan.
    pub activation_id: String,
    /// Exact registered barrier point retried by the actor.
    pub point_id: String,
    /// Bounded actor whose single release was consumed.
    pub actor: String,
    /// One-based position in the seeded release order.
    pub release_position: u16,
}

/// Sidecar record associating one observed activation with its recovery class.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QaFaultRecoveryRecordedRecord {
    /// Sidecar record schema version.
    pub schema_version: u32,
    /// One-based sequence across the persistent campaign.
    pub sequence: u32,
    /// Previously loaded launch that proved recovery.
    pub launch_id: String,
    /// Canonical digest shared by every campaign launch.
    pub plan_sha256: String,
    /// Earlier campaign activation being recovered.
    pub activation_id: String,
    /// Typed recovery outcome supported by the point registry.
    pub recovery_class: QaFaultRecoveryClass,
    /// Bounded machine-readable recovery reason.
    pub reason_code: String,
}

/// Strict tagged record stored as one JSON object per sidecar line.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "record_type", content = "record", rename_all = "snake_case", deny_unknown_fields)]
pub enum QaFaultEvidenceSidecarRecord {
    /// A process launch accepted its private plan and capability.
    LaunchLoaded(QaFaultLaunchLoadedRecord),
    /// A relevant checkpoint advanced without activating a rule.
    CheckpointObserved(QaFaultCheckpointObservedRecord),
    /// One barrier participant durably joined before any release.
    BarrierJoined(QaFaultBarrierJoinedRecord),
    /// A planned rule reached its durable activation boundary.
    RuleActivated(QaFaultRuleActivatedRecord),
    /// One seeded barrier release was durably consumed before returning.
    BarrierReleased(QaFaultBarrierReleasedRecord),
    /// A loaded launch proved recovery for an earlier activation.
    RecoveryRecorded(QaFaultRecoveryRecordedRecord),
}

/// Validated ordered records from one private NDJSON sidecar.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QaFaultEvidenceSidecar {
    records: Vec<QaFaultEvidenceSidecarRecord>,
}

/// Validated controller offsets reconstructed from a durable evidence campaign.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct QaFaultControllerResumeState {
    pub(super) highest_activation_sequence: u32,
    pub(super) occurrences: BTreeMap<(String, String), u32>,
}

impl QaFaultEvidenceSidecar {
    /// Returns the validated records in durable sequence order.
    #[must_use]
    pub fn records(&self) -> &[QaFaultEvidenceSidecarRecord] {
        self.records.as_slice()
    }

    /// Reconstructs deterministic controller counters from validated durable evidence.
    #[must_use]
    pub fn controller_resume_state(&self) -> QaFaultControllerResumeState {
        let mut resume_state = QaFaultControllerResumeState::default();
        for record in &self.records {
            match record {
                QaFaultEvidenceSidecarRecord::CheckpointObserved(observed) => {
                    update_resume_occurrence(
                        &mut resume_state.occurrences,
                        observed.point_id.as_str(),
                        observed.actor.as_str(),
                        observed.occurrence,
                    );
                }
                QaFaultEvidenceSidecarRecord::BarrierJoined(joined) => {
                    update_resume_occurrence(
                        &mut resume_state.occurrences,
                        joined.point_id.as_str(),
                        joined.actor.as_str(),
                        joined.occurrence,
                    );
                }
                QaFaultEvidenceSidecarRecord::RuleActivated(activated) => {
                    resume_state.highest_activation_sequence =
                        resume_state.highest_activation_sequence.max(activated.activation_sequence);
                    for actor in &activated.actors {
                        update_resume_occurrence(
                            &mut resume_state.occurrences,
                            activated.point_id.as_str(),
                            actor.as_str(),
                            activated.occurrence,
                        );
                    }
                }
                QaFaultEvidenceSidecarRecord::LaunchLoaded(_)
                | QaFaultEvidenceSidecarRecord::BarrierReleased(_)
                | QaFaultEvidenceSidecarRecord::RecoveryRecorded(_) => {}
            }
        }
        resume_state
    }
}

pub(super) fn update_resume_occurrence(
    occurrences: &mut BTreeMap<(String, String), u32>,
    point_id: &str,
    actor: &str,
    occurrence: u32,
) {
    occurrences
        .entry((point_id.to_owned(), actor.to_owned()))
        .and_modify(|observed| *observed = (*observed).max(occurrence))
        .or_insert(occurrence);
}

/// One semantic sidecar validation issue.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct QaFaultEvidenceSidecarIssue {
    /// Stable machine-readable reason code.
    pub code: String,
    /// Zero-based record index, or `None` for campaign-wide issues.
    pub record_index: Option<usize>,
    /// Bounded operator-facing explanation.
    pub message: String,
}

/// Collection of semantic sidecar validation issues.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QaFaultEvidenceSidecarValidationError {
    issues: Vec<QaFaultEvidenceSidecarIssue>,
}

impl QaFaultEvidenceSidecarValidationError {
    fn new(issues: Vec<QaFaultEvidenceSidecarIssue>) -> Self {
        debug_assert!(!issues.is_empty());
        Self { issues }
    }

    /// Returns all collected semantic issues.
    #[must_use]
    pub fn issues(&self) -> &[QaFaultEvidenceSidecarIssue] {
        self.issues.as_slice()
    }
}

impl fmt::Display for QaFaultEvidenceSidecarValidationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(issue) = self.issues.first() {
            if let Some(index) = issue.record_index {
                write!(formatter, "{} at record {index}: {}", issue.code, issue.message)
            } else {
                write!(formatter, "{}: {}", issue.code, issue.message)
            }
        } else {
            formatter.write_str("QA fault evidence sidecar validation failed")
        }
    }
}

impl Error for QaFaultEvidenceSidecarValidationError {}

/// Bounded NDJSON decode or semantic sidecar validation failure.
#[derive(Debug, ThisError)]
pub enum QaFaultEvidenceSidecarError {
    /// Total encoded campaign exceeds its hard byte limit.
    #[error("QA fault evidence sidecar exceeds {QA_FAULT_EVIDENCE_SIDECAR_MAX_BYTES} bytes")]
    SidecarTooLarge,
    /// Final durable record is truncated or lacks its NDJSON delimiter.
    #[error("QA fault evidence sidecar must end at an NDJSON record boundary")]
    UnterminatedRecord,
    /// Campaign contains more records than the schema permits.
    #[error("QA fault evidence sidecar has too many records")]
    TooManyRecords,
    /// One encoded line is empty or exceeds its per-record limit.
    #[error("QA fault evidence sidecar record {record_index} is empty or too large")]
    InvalidRecordSize { record_index: usize },
    /// One line failed strict tagged JSON decoding.
    #[error("failed to parse QA fault evidence sidecar record {record_index}: {source}")]
    MalformedRecord {
        record_index: usize,
        #[source]
        source: serde_json::Error,
    },
    /// Typed records decoded but violated campaign semantics.
    #[error("invalid QA fault evidence sidecar: {0}")]
    Invalid(#[source] QaFaultEvidenceSidecarValidationError),
}

/// Parses bounded NDJSON and validates it against one launch and fault plan.
///
/// The validator treats all records as one restart campaign: each launch must
/// be loaded before its records, while activations remain unique across all
/// launches and recovery may be recorded by a later launch. Missing expected
/// activations are intentionally left to the scenario evidence evaluator.
///
/// # Errors
/// Returns bounded decoding errors or all semantic contract violations.
pub fn parse_qa_fault_evidence_sidecar_ndjson(
    bytes: &[u8],
    launch: &QaFaultLaunchDocument,
    plan: &QaFaultInjectionPlan,
) -> Result<QaFaultEvidenceSidecar, QaFaultEvidenceSidecarError> {
    let records = decode_fault_evidence_sidecar_records(bytes, false)?;
    let issues = validate_fault_evidence_sidecar(
        records.as_slice(),
        launch,
        plan,
        SidecarCampaignValidationMode::CurrentLaunchLoaded,
    );
    if !issues.is_empty() {
        return Err(QaFaultEvidenceSidecarError::Invalid(
            QaFaultEvidenceSidecarValidationError::new(issues),
        ));
    }
    Ok(QaFaultEvidenceSidecar { records })
}

/// Strictly validates a persistent campaign before appending the current launch.
///
/// An empty sidecar is valid for the first launch. A non-empty sidecar must be
/// a complete prior campaign prefix, and the current launch id must not have
/// appeared previously. After this succeeds, the daemon appends and syncs one
/// `launch_loaded` record and then calls
/// [`parse_qa_fault_evidence_sidecar_ndjson`] for the full post-append check.
///
/// # Errors
/// Returns bounded decoding errors, prior campaign violations, or replay of
/// the current launch id.
pub fn validate_qa_fault_evidence_campaign_before_launch(
    bytes: &[u8],
    current_launch: &QaFaultLaunchDocument,
    plan: &QaFaultInjectionPlan,
) -> Result<QaFaultEvidenceSidecar, QaFaultEvidenceSidecarError> {
    let records = decode_fault_evidence_sidecar_records(bytes, true)?;
    let issues = validate_fault_evidence_sidecar(
        records.as_slice(),
        current_launch,
        plan,
        SidecarCampaignValidationMode::BeforeCurrentLaunch,
    );
    if !issues.is_empty() {
        return Err(QaFaultEvidenceSidecarError::Invalid(
            QaFaultEvidenceSidecarValidationError::new(issues),
        ));
    }
    Ok(QaFaultEvidenceSidecar { records })
}
