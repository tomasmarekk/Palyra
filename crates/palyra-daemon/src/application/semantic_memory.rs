//! Candidate-only semantic memory consolidation with evidence and citations.
//!
//! This module derives inert contracts. Durable review and the ordinary-memory
//! projection are committed together by the journal integration.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

const SEMANTIC_MEMORY_SCHEMA_VERSION: u32 = 1;
const MAX_EVIDENCE_REFS: usize = 64;
const MAX_CITATIONS: usize = 64;
const MAX_ROLLBACK_HISTORY: usize = 64;
const MAX_SUMMARY_BYTES: usize = 8 * 1024;
const MAX_SCOPE_BYTES: usize = 256;
const MAX_REASON_CODE_BYTES: usize = 128;
const MIN_QUALITY_EVAL_SAMPLES: u32 = 10;
const MAX_QUALITY_EVAL_CASES: usize = 64;
const MAX_QUALITY_EVAL_HITS_PER_CASE: usize = 16;
const BASIS_POINTS_MAX: u16 = 10_000;

/// Epistemic origin retained through consolidation and retrieval projection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SemanticMemoryEpistemicKind {
    UserFact,
    ObservedFact,
    ModelInference,
    Preference,
}

impl SemanticMemoryEpistemicKind {
    pub(crate) const fn retrieval_label(self) -> &'static str {
        match self {
            Self::UserFact => "user_fact",
            Self::ObservedFact => "observed_fact",
            Self::ModelInference => "model_inference",
            Self::Preference => "preference",
        }
    }
}

/// Sensitivity controls review and bounded retention posture.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SemanticMemorySensitivity {
    Public,
    Internal,
    Sensitive,
    Restricted,
}

/// Contradiction posture for a candidate summary.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SemanticMemoryContradictionStatus {
    None,
    Detected,
    ResolvedByUserCorrection,
}

/// Host-owned lifecycle for one consolidated memory version.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConsolidatedMemoryLifecycle {
    Active,
    Degraded,
    Archived,
    RolledBack,
}

/// Provenance-linked evidence. Raw transcript text is intentionally absent.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SemanticMemoryEvidenceRefV1 {
    pub v: u32,
    pub evidence_id: String,
    pub source_ref: String,
    pub citation_uri: String,
    pub content_sha256: String,
    pub provenance_sha256: String,
    pub claim_key: String,
    pub claim_value_sha256: String,
    pub acl_scope: String,
    pub epistemic_kind: SemanticMemoryEpistemicKind,
    pub sensitivity: SemanticMemorySensitivity,
    pub confidence_basis_points: u16,
    pub observed_at_unix_ms: i64,
    pub expires_at_unix_ms: Option<i64>,
    pub corrects_evidence_ids: Vec<String>,
}

/// Citation projected with consolidated recall without exposing source content.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SemanticMemoryCitationV1 {
    pub evidence_id: String,
    pub source_ref: String,
    pub citation_uri: String,
    pub content_sha256: String,
    pub provenance_sha256: String,
}

/// Baseline comparison required before activation can be considered.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SemanticMemoryQualityEvalV1 {
    pub v: u32,
    pub sample_count: u32,
    pub baseline_precision_basis_points: u16,
    pub consolidated_precision_basis_points: u16,
    pub baseline_usefulness_basis_points: u16,
    pub consolidated_usefulness_basis_points: u16,
    pub baseline_correction_rate_basis_points: u16,
    pub consolidated_correction_rate_basis_points: u16,
    pub evidence_sha256: String,
}

/// One host-labeled retrieval case; outcomes are always observed server-side.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SemanticMemoryQualityEvalCaseV1 {
    pub case_id: String,
    pub query: String,
    pub expected_baseline_memory_ids: Vec<String>,
    pub candidate_relevant: bool,
}

/// Bounded retrieval output captured by the isolated evaluation adapter.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SemanticMemoryQualityEvalObservationV1 {
    pub case_id: String,
    pub baseline_memory_ids: Vec<String>,
    pub consolidated_memory_ids: Vec<String>,
}

impl SemanticMemoryQualityEvalV1 {
    #[must_use]
    pub fn qualifies(&self) -> bool {
        self.v == SEMANTIC_MEMORY_SCHEMA_VERSION
            && self.sample_count >= MIN_QUALITY_EVAL_SAMPLES
            && [
                self.baseline_precision_basis_points,
                self.consolidated_precision_basis_points,
                self.baseline_usefulness_basis_points,
                self.consolidated_usefulness_basis_points,
                self.baseline_correction_rate_basis_points,
                self.consolidated_correction_rate_basis_points,
            ]
            .into_iter()
            .all(|value| value <= BASIS_POINTS_MAX)
            && self.consolidated_precision_basis_points >= self.baseline_precision_basis_points
            && self.consolidated_usefulness_basis_points > self.baseline_usefulness_basis_points
            && self.consolidated_correction_rate_basis_points
                <= self.baseline_correction_rate_basis_points
            && valid_sha256(self.evidence_sha256.as_str())
    }
}

/// Candidate input before the server derives authoritative retrieval evidence.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SemanticMemoryCandidateDraftV1 {
    pub candidate_id: String,
    pub summary_text: String,
    pub evidence_refs: Vec<SemanticMemoryEvidenceRefV1>,
    pub retention_expires_at_unix_ms: Option<i64>,
    pub created_at_unix_ms: i64,
}

/// Inert summary candidate that retains every source citation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SemanticMemoryCandidateV1 {
    pub v: u32,
    pub candidate_id: String,
    pub claim_key: String,
    pub claim_value_sha256: String,
    pub summary_text: String,
    pub summary_sha256: String,
    pub epistemic_kind: SemanticMemoryEpistemicKind,
    pub acl_scope: String,
    pub sensitivity: SemanticMemorySensitivity,
    pub confidence_basis_points: u16,
    pub contradiction_status: SemanticMemoryContradictionStatus,
    pub evidence_refs: Vec<SemanticMemoryEvidenceRefV1>,
    pub citations: Vec<SemanticMemoryCitationV1>,
    pub retention_expires_at_unix_ms: Option<i64>,
    pub review_required: bool,
    pub quality_eval: SemanticMemoryQualityEvalV1,
    pub created_at_unix_ms: i64,
    pub reason_code: String,
}

/// Candidate construction input separated from host policy.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SemanticMemoryConsolidationRequest {
    pub candidate_id: String,
    pub summary_text: String,
    pub evidence_refs: Vec<SemanticMemoryEvidenceRefV1>,
    pub retention_expires_at_unix_ms: Option<i64>,
    pub quality_eval: SemanticMemoryQualityEvalV1,
    pub created_at_unix_ms: i64,
}

/// Optional rollout and review thresholds for candidate construction.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SemanticMemoryConsolidationPolicy {
    pub enabled: bool,
    pub min_corroborating_evidence: usize,
    pub reviewer_confidence_threshold_basis_points: u16,
    pub allow_verbatim_evidence: bool,
    pub max_sensitive_retention_ms: i64,
}

impl Default for SemanticMemoryConsolidationPolicy {
    fn default() -> Self {
        Self {
            enabled: false,
            min_corroborating_evidence: 2,
            reviewer_confidence_threshold_basis_points: 8_500,
            allow_verbatim_evidence: false,
            max_sensitive_retention_ms: 30 * 24 * 60 * 60 * 1_000,
        }
    }
}

/// Host validation authority supplied only at activation or rollback time.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SemanticMemoryHostGate {
    pub host_validated: bool,
    pub policy_approved: bool,
    pub reviewer_approved: bool,
    pub quality_eval_approved: bool,
    pub approval_generation: u64,
    pub activated_at_unix_ms: i64,
}

/// Retrieval telemetry that can degrade an active record but never rewrite it.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SemanticMemoryRetrievalMetricsV1 {
    pub retrieval_count: u64,
    pub useful_count: u64,
    pub not_useful_count: u64,
    pub correction_count: u64,
    pub last_retrieved_at_unix_ms: Option<i64>,
    pub last_corrected_at_unix_ms: Option<i64>,
}

/// One bounded retrieval outcome.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SemanticMemoryRetrievalFeedbackV1 {
    pub useful: bool,
    pub corrected: bool,
    pub retrieved_at_unix_ms: i64,
    pub correction_evidence_ref: Option<SemanticMemoryEvidenceRefV1>,
}

/// Reviewed, active or historical semantic memory version.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConsolidatedMemoryRecord {
    pub v: u32,
    pub memory_id: String,
    pub version: u64,
    pub lifecycle: ConsolidatedMemoryLifecycle,
    pub claim_key: String,
    pub claim_value_sha256: String,
    pub summary_text: String,
    pub summary_sha256: String,
    pub epistemic_kind: SemanticMemoryEpistemicKind,
    pub acl_scope: String,
    pub sensitivity: SemanticMemorySensitivity,
    pub confidence_basis_points: u16,
    pub contradiction_status: SemanticMemoryContradictionStatus,
    pub evidence_refs: Vec<SemanticMemoryEvidenceRefV1>,
    pub citations: Vec<SemanticMemoryCitationV1>,
    pub retention_expires_at_unix_ms: Option<i64>,
    pub quality_eval: SemanticMemoryQualityEvalV1,
    pub approval_generation: u64,
    pub activated_at_unix_ms: i64,
    pub degraded_at_unix_ms: Option<i64>,
    pub archived_at_unix_ms: Option<i64>,
    pub previous_record_sha256: Option<String>,
    pub rollback_history_sha256: Vec<String>,
    pub retrieval_metrics: SemanticMemoryRetrievalMetricsV1,
    pub reason_code: String,
    pub record_sha256: String,
}

/// Model-facing projection that preserves epistemic labeling and citations.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SemanticMemoryRetrievalProjectionV1 {
    pub v: u32,
    pub memory_id: String,
    pub version: u64,
    pub summary_text: String,
    pub epistemic_label: String,
    pub citations: Vec<SemanticMemoryCitationV1>,
    pub confidence_basis_points: u16,
    pub degraded: bool,
    pub instruction_authority: bool,
}

/// Stable fail-closed outcomes for consolidation and lifecycle operations.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum SemanticMemoryError {
    #[error("semantic memory consolidation is disabled")]
    Disabled,
    #[error("semantic memory evidence is invalid: {0}")]
    EvidenceInvalid(&'static str),
    #[error("semantic memory evidence ACL scopes are incompatible")]
    AclMismatch,
    #[error("semantic memory evidence is not sufficiently corroborated")]
    InsufficientCorroboration,
    #[error("semantic memory quality evaluation does not beat baseline")]
    QualityEvalFailed,
    #[error("semantic memory quality evaluation evidence is invalid")]
    QualityEvalEvidenceInvalid,
    #[error("semantic memory raw evidence copy requires explicit policy")]
    VerbatimEvidenceDenied,
    #[error("semantic memory sensitive retention is invalid")]
    SensitiveRetentionInvalid,
    #[error("semantic memory activation requires host validation")]
    HostValidationRequired,
    #[error("semantic memory activation requires policy approval")]
    PolicyApprovalRequired,
    #[error("semantic memory activation requires reviewer approval")]
    ReviewerApprovalRequired,
    #[error("semantic memory activation requires quality-eval approval")]
    QualityEvalApprovalRequired,
    #[error("semantic memory contradiction must be resolved before activation")]
    ContradictionUnresolved,
    #[error("semantic memory approval generation is invalid")]
    ApprovalGenerationInvalid,
    #[error("semantic memory rollback target is invalid")]
    RollbackTargetInvalid,
    #[error("semantic memory record digest is invalid")]
    RecordDigestInvalid,
    #[error("semantic memory serialization failed")]
    Serialization,
}

/// Derives the authoritative quality report from host labels and observed hits.
///
/// # Errors
/// Rejects malformed, duplicate, missing, or caller-reordered observations.
pub fn derive_semantic_memory_quality_eval(
    candidate_memory_id: &str,
    cases: &[SemanticMemoryQualityEvalCaseV1],
    observations: &[SemanticMemoryQualityEvalObservationV1],
) -> Result<SemanticMemoryQualityEvalV1, SemanticMemoryError> {
    if !valid_identifier(candidate_memory_id)
        || cases.len() < MIN_QUALITY_EVAL_SAMPLES as usize
        || cases.len() > MAX_QUALITY_EVAL_CASES
        || cases.len() != observations.len()
    {
        return Err(SemanticMemoryError::QualityEvalEvidenceInvalid);
    }
    let mut case_ids = BTreeSet::new();
    let mut baseline_relevant = 0_u64;
    let mut baseline_retrieved = 0_u64;
    let mut consolidated_relevant = 0_u64;
    let mut consolidated_retrieved = 0_u64;
    let mut baseline_useful = 0_u64;
    let mut consolidated_useful = 0_u64;
    let mut baseline_corrections = 0_u64;
    let mut consolidated_corrections = 0_u64;
    let mut evidence_hasher = Sha256::new();
    evidence_hasher.update(b"palyra.semantic-memory.quality-eval.v1\0");

    for (case, observation) in cases.iter().zip(observations) {
        if !valid_identifier(case.case_id.as_str())
            || !case_ids.insert(case.case_id.as_str())
            || case.query.trim().is_empty()
            || case.query.len() > MAX_SUMMARY_BYTES
            || case.expected_baseline_memory_ids.len() > MAX_QUALITY_EVAL_HITS_PER_CASE
            || case.expected_baseline_memory_ids.iter().any(|id| !valid_identifier(id.as_str()))
            || observation.case_id != case.case_id
            || observation.baseline_memory_ids.len() > MAX_QUALITY_EVAL_HITS_PER_CASE
            || observation.consolidated_memory_ids.len() > MAX_QUALITY_EVAL_HITS_PER_CASE
            || observation
                .baseline_memory_ids
                .iter()
                .chain(&observation.consolidated_memory_ids)
                .any(|id| !valid_identifier(id.as_str()))
        {
            return Err(SemanticMemoryError::QualityEvalEvidenceInvalid);
        }
        let expected_baseline =
            case.expected_baseline_memory_ids.iter().map(String::as_str).collect::<BTreeSet<_>>();
        let baseline_hits =
            observation.baseline_memory_ids.iter().map(String::as_str).collect::<BTreeSet<_>>();
        let consolidated_hits =
            observation.consolidated_memory_ids.iter().map(String::as_str).collect::<BTreeSet<_>>();
        if baseline_hits.len() != observation.baseline_memory_ids.len()
            || consolidated_hits.len() != observation.consolidated_memory_ids.len()
        {
            return Err(SemanticMemoryError::QualityEvalEvidenceInvalid);
        }

        let baseline_case_relevant = baseline_hits.intersection(&expected_baseline).count() as u64;
        let consolidated_baseline_relevant =
            consolidated_hits.intersection(&expected_baseline).count() as u64;
        let consolidated_candidate_relevant =
            u64::from(case.candidate_relevant && consolidated_hits.contains(candidate_memory_id));
        baseline_relevant = baseline_relevant.saturating_add(baseline_case_relevant);
        baseline_retrieved =
            baseline_retrieved.saturating_add(observation.baseline_memory_ids.len() as u64);
        consolidated_relevant = consolidated_relevant
            .saturating_add(consolidated_baseline_relevant)
            .saturating_add(consolidated_candidate_relevant);
        consolidated_retrieved =
            consolidated_retrieved.saturating_add(observation.consolidated_memory_ids.len() as u64);

        let baseline_case_useful = if case.candidate_relevant || !expected_baseline.is_empty() {
            baseline_case_relevant > 0
        } else {
            baseline_hits.is_empty()
        };
        let consolidated_case_useful = if case.candidate_relevant || !expected_baseline.is_empty() {
            consolidated_baseline_relevant > 0 || consolidated_candidate_relevant > 0
        } else {
            consolidated_hits.is_empty()
        };
        baseline_useful = baseline_useful.saturating_add(u64::from(baseline_case_useful));
        consolidated_useful =
            consolidated_useful.saturating_add(u64::from(consolidated_case_useful));
        baseline_corrections = baseline_corrections.saturating_add(
            (observation.baseline_memory_ids.len() as u64).saturating_sub(baseline_case_relevant),
        );
        consolidated_corrections = consolidated_corrections.saturating_add(
            (observation.consolidated_memory_ids.len() as u64)
                .saturating_sub(consolidated_baseline_relevant)
                .saturating_sub(consolidated_candidate_relevant),
        );

        update_hash_field(&mut evidence_hasher, case.case_id.as_bytes());
        update_hash_field(&mut evidence_hasher, sha256_hex(case.query.as_bytes()).as_bytes());
        evidence_hasher.update(u8::from(case.candidate_relevant).to_le_bytes());
        for id in &case.expected_baseline_memory_ids {
            update_hash_field(&mut evidence_hasher, id.as_bytes());
        }
        for id in &observation.baseline_memory_ids {
            update_hash_field(&mut evidence_hasher, id.as_bytes());
        }
        for id in &observation.consolidated_memory_ids {
            update_hash_field(&mut evidence_hasher, id.as_bytes());
        }
    }

    let sample_count =
        u32::try_from(cases.len()).map_err(|_| SemanticMemoryError::QualityEvalEvidenceInvalid)?;
    Ok(SemanticMemoryQualityEvalV1 {
        v: SEMANTIC_MEMORY_SCHEMA_VERSION,
        sample_count,
        baseline_precision_basis_points: ratio_basis_points(
            baseline_relevant,
            baseline_retrieved,
            true,
        ),
        consolidated_precision_basis_points: ratio_basis_points(
            consolidated_relevant,
            consolidated_retrieved,
            true,
        ),
        baseline_usefulness_basis_points: ratio_basis_points(
            baseline_useful,
            cases.len() as u64,
            false,
        ),
        consolidated_usefulness_basis_points: ratio_basis_points(
            consolidated_useful,
            cases.len() as u64,
            false,
        ),
        baseline_correction_rate_basis_points: ratio_basis_points(
            baseline_corrections,
            baseline_retrieved,
            false,
        ),
        consolidated_correction_rate_basis_points: ratio_basis_points(
            consolidated_corrections,
            consolidated_retrieved,
            false,
        ),
        evidence_sha256: hex::encode(evidence_hasher.finalize()),
    })
}

/// Groups compatible evidence into an inert, reviewable candidate.
///
/// # Errors
/// Rejects disabled rollout, raw copies, ACL/provenance mismatches, insufficient
/// corroboration, invalid sensitive retention, or a failed quality comparison.
pub fn build_semantic_memory_candidate(
    mut request: SemanticMemoryConsolidationRequest,
    policy: &SemanticMemoryConsolidationPolicy,
) -> Result<SemanticMemoryCandidateV1, SemanticMemoryError> {
    validate_policy(policy)?;
    if !policy.enabled {
        return Err(SemanticMemoryError::Disabled);
    }
    if !valid_identifier(request.candidate_id.as_str())
        || request.summary_text.trim().is_empty()
        || request.summary_text.len() > MAX_SUMMARY_BYTES
        || request.created_at_unix_ms < 0
    {
        return Err(SemanticMemoryError::EvidenceInvalid("candidate shape"));
    }
    if !request.quality_eval.qualifies() {
        return Err(SemanticMemoryError::QualityEvalFailed);
    }
    if request.evidence_refs.len() < policy.min_corroborating_evidence
        || request.evidence_refs.len() > MAX_EVIDENCE_REFS
    {
        return Err(SemanticMemoryError::InsufficientCorroboration);
    }
    request.evidence_refs.sort_by(|left, right| {
        left.observed_at_unix_ms
            .cmp(&right.observed_at_unix_ms)
            .then(left.evidence_id.cmp(&right.evidence_id))
    });
    validate_evidence(request.evidence_refs.as_slice())?;

    let acl_scopes = request
        .evidence_refs
        .iter()
        .map(|evidence| evidence.acl_scope.as_str())
        .collect::<BTreeSet<_>>();
    if acl_scopes.len() != 1 {
        return Err(SemanticMemoryError::AclMismatch);
    }
    let claim_keys = request
        .evidence_refs
        .iter()
        .map(|evidence| evidence.claim_key.as_str())
        .collect::<BTreeSet<_>>();
    if claim_keys.len() != 1 {
        return Err(SemanticMemoryError::EvidenceInvalid("claim key mismatch"));
    }
    let distinct_sources = request
        .evidence_refs
        .iter()
        .map(|evidence| evidence.source_ref.as_str())
        .collect::<BTreeSet<_>>();
    if distinct_sources.len() < policy.min_corroborating_evidence {
        return Err(SemanticMemoryError::InsufficientCorroboration);
    }

    let summary_text = request.summary_text.trim().to_owned();
    let summary_sha256 = sha256_hex(summary_text.as_bytes());
    if !policy.allow_verbatim_evidence
        && request.evidence_refs.iter().any(|evidence| evidence.content_sha256 == summary_sha256)
    {
        return Err(SemanticMemoryError::VerbatimEvidenceDenied);
    }

    let (claim_value_sha256, contradiction_status, selected_evidence) =
        resolve_claim_value(request.evidence_refs.as_slice());
    let confidence_basis_points = selected_evidence
        .iter()
        .map(|evidence| evidence.confidence_basis_points)
        .min()
        .unwrap_or_default();
    let sensitivity = request
        .evidence_refs
        .iter()
        .map(|evidence| evidence.sensitivity)
        .max()
        .unwrap_or(SemanticMemorySensitivity::Restricted);
    validate_retention(
        sensitivity,
        request.retention_expires_at_unix_ms,
        request.created_at_unix_ms,
        policy.max_sensitive_retention_ms,
    )?;
    let epistemic_kind = safe_epistemic_kind(selected_evidence.as_slice());
    let review_required = sensitivity >= SemanticMemorySensitivity::Sensitive
        || confidence_basis_points < policy.reviewer_confidence_threshold_basis_points
        || contradiction_status == SemanticMemoryContradictionStatus::ResolvedByUserCorrection;
    let citations = request.evidence_refs.iter().map(citation_from_evidence).collect::<Vec<_>>();
    let reason_code = match contradiction_status {
        SemanticMemoryContradictionStatus::None => "semantic_memory.candidate_corroborated",
        SemanticMemoryContradictionStatus::Detected => "semantic_memory.contradiction_detected",
        SemanticMemoryContradictionStatus::ResolvedByUserCorrection => {
            "semantic_memory.user_correction_selected"
        }
    };
    let first_evidence =
        request.evidence_refs.first().ok_or(SemanticMemoryError::InsufficientCorroboration)?;
    Ok(SemanticMemoryCandidateV1 {
        v: SEMANTIC_MEMORY_SCHEMA_VERSION,
        candidate_id: request.candidate_id,
        claim_key: first_evidence.claim_key.clone(),
        claim_value_sha256,
        summary_text,
        summary_sha256,
        epistemic_kind,
        acl_scope: first_evidence.acl_scope.clone(),
        sensitivity,
        confidence_basis_points,
        contradiction_status,
        evidence_refs: request.evidence_refs,
        citations,
        retention_expires_at_unix_ms: request.retention_expires_at_unix_ms,
        review_required,
        quality_eval: request.quality_eval,
        created_at_unix_ms: request.created_at_unix_ms,
        reason_code: reason_code.to_owned(),
    })
}

/// Activates a reviewed candidate as a new immutable-version lineage head.
///
/// # Errors
/// Requires resolved evidence, host and policy validation, applicable review,
/// quality approval, and a monotonically increasing approval generation.
pub fn activate_semantic_memory_candidate(
    memory_id: String,
    candidate: &SemanticMemoryCandidateV1,
    gate: &SemanticMemoryHostGate,
    previous: Option<&ConsolidatedMemoryRecord>,
) -> Result<ConsolidatedMemoryRecord, SemanticMemoryError> {
    validate_candidate(candidate)?;
    validate_gate(candidate, gate)?;
    if !valid_identifier(memory_id.as_str()) {
        return Err(SemanticMemoryError::EvidenceInvalid("memory id"));
    }
    if let Some(previous) = previous {
        validate_record(previous)?;
        if previous.memory_id != memory_id
            || previous.claim_key != candidate.claim_key
            || previous.acl_scope != candidate.acl_scope
            || gate.approval_generation <= previous.approval_generation
        {
            return Err(SemanticMemoryError::ApprovalGenerationInvalid);
        }
    }
    let version = match previous {
        Some(record) => {
            record.version.checked_add(1).ok_or(SemanticMemoryError::ApprovalGenerationInvalid)?
        }
        None => 1,
    };
    let previous_record_sha256 = previous.map(|record| record.record_sha256.clone());
    let mut record = ConsolidatedMemoryRecord {
        v: SEMANTIC_MEMORY_SCHEMA_VERSION,
        memory_id,
        version,
        lifecycle: ConsolidatedMemoryLifecycle::Active,
        claim_key: candidate.claim_key.clone(),
        claim_value_sha256: candidate.claim_value_sha256.clone(),
        summary_text: candidate.summary_text.clone(),
        summary_sha256: candidate.summary_sha256.clone(),
        epistemic_kind: candidate.epistemic_kind,
        acl_scope: candidate.acl_scope.clone(),
        sensitivity: candidate.sensitivity,
        confidence_basis_points: candidate.confidence_basis_points,
        contradiction_status: candidate.contradiction_status,
        evidence_refs: candidate.evidence_refs.clone(),
        citations: candidate.citations.clone(),
        retention_expires_at_unix_ms: candidate.retention_expires_at_unix_ms,
        quality_eval: candidate.quality_eval.clone(),
        approval_generation: gate.approval_generation,
        activated_at_unix_ms: gate.activated_at_unix_ms,
        degraded_at_unix_ms: None,
        archived_at_unix_ms: None,
        previous_record_sha256,
        rollback_history_sha256: previous
            .map(|record| record.rollback_history_sha256.clone())
            .unwrap_or_default(),
        retrieval_metrics: SemanticMemoryRetrievalMetricsV1::default(),
        reason_code: "semantic_memory.activated".to_owned(),
        record_sha256: String::new(),
    };
    refresh_record_digest(&mut record)?;
    Ok(record)
}

/// Applies bounded usefulness/correction feedback and degrades corrected memory.
///
/// # Errors
/// Rejects malformed timestamps, correction evidence, or a corrupt input record.
pub fn apply_semantic_memory_retrieval_feedback(
    record: &mut ConsolidatedMemoryRecord,
    feedback: SemanticMemoryRetrievalFeedbackV1,
) -> Result<(), SemanticMemoryError> {
    validate_record(record)?;
    if !matches!(
        record.lifecycle,
        ConsolidatedMemoryLifecycle::Active | ConsolidatedMemoryLifecycle::Degraded
    ) || feedback.retrieved_at_unix_ms < record.activated_at_unix_ms
    {
        return Err(SemanticMemoryError::EvidenceInvalid("retrieval feedback state"));
    }
    if feedback.corrected {
        let correction = feedback
            .correction_evidence_ref
            .as_ref()
            .ok_or(SemanticMemoryError::EvidenceInvalid("correction evidence missing"))?;
        validate_evidence(std::slice::from_ref(correction))?;
        if correction.epistemic_kind != SemanticMemoryEpistemicKind::UserFact
            || correction.acl_scope != record.acl_scope
            || record.evidence_refs.len() >= MAX_EVIDENCE_REFS
            || record
                .evidence_refs
                .iter()
                .any(|evidence| evidence.evidence_id == correction.evidence_id)
            || !correction.corrects_evidence_ids.iter().any(|evidence_id| {
                record.evidence_refs.iter().any(|evidence| &evidence.evidence_id == evidence_id)
            })
        {
            return Err(SemanticMemoryError::EvidenceInvalid("correction evidence mismatch"));
        }
    }
    advance_record_version(record)?;
    if feedback.corrected {
        let correction = feedback
            .correction_evidence_ref
            .as_ref()
            .ok_or(SemanticMemoryError::EvidenceInvalid("correction evidence missing"))?;
        record.evidence_refs.push(correction.clone());
        record.citations.push(citation_from_evidence(correction));
        record.retrieval_metrics.correction_count =
            record.retrieval_metrics.correction_count.saturating_add(1);
        record.retrieval_metrics.last_corrected_at_unix_ms = Some(feedback.retrieved_at_unix_ms);
        record.lifecycle = ConsolidatedMemoryLifecycle::Degraded;
        record.degraded_at_unix_ms = Some(feedback.retrieved_at_unix_ms);
        record.reason_code = "semantic_memory.degraded_by_correction".to_owned();
    } else if !feedback.useful
        && record.retrieval_metrics.not_useful_count.saturating_add(1)
            >= record.retrieval_metrics.useful_count.saturating_add(2)
        && record.retrieval_metrics.retrieval_count >= 2
    {
        record.lifecycle = ConsolidatedMemoryLifecycle::Degraded;
        record.degraded_at_unix_ms = Some(feedback.retrieved_at_unix_ms);
        record.reason_code = "semantic_memory.degraded_by_feedback".to_owned();
    }
    record.retrieval_metrics.retrieval_count =
        record.retrieval_metrics.retrieval_count.saturating_add(1);
    if feedback.useful {
        record.retrieval_metrics.useful_count =
            record.retrieval_metrics.useful_count.saturating_add(1);
    } else {
        record.retrieval_metrics.not_useful_count =
            record.retrieval_metrics.not_useful_count.saturating_add(1);
    }
    record.retrieval_metrics.last_retrieved_at_unix_ms = Some(feedback.retrieved_at_unix_ms);
    refresh_record_digest(record)
}

/// Degrades memory whose source freshness or retention window has elapsed.
///
/// # Errors
/// Rejects corrupt records or timestamps earlier than activation.
pub fn mark_semantic_memory_stale(
    record: &mut ConsolidatedMemoryRecord,
    observed_at_unix_ms: i64,
    max_age_ms: i64,
) -> Result<bool, SemanticMemoryError> {
    validate_record(record)?;
    if observed_at_unix_ms < record.activated_at_unix_ms || max_age_ms <= 0 {
        return Err(SemanticMemoryError::EvidenceInvalid("staleness timestamp"));
    }
    let age_stale = observed_at_unix_ms.saturating_sub(record.activated_at_unix_ms) > max_age_ms;
    let retention_stale = record
        .retention_expires_at_unix_ms
        .is_some_and(|expires_at| observed_at_unix_ms >= expires_at);
    if !age_stale && !retention_stale {
        return Ok(false);
    }
    advance_record_version(record)?;
    record.lifecycle = ConsolidatedMemoryLifecycle::Degraded;
    record.degraded_at_unix_ms = Some(observed_at_unix_ms);
    record.reason_code = "semantic_memory.stale".to_owned();
    refresh_record_digest(record)?;
    Ok(true)
}

/// Archives a record without deleting citations, evidence, or quality history.
///
/// # Errors
/// Rejects corrupt records or timestamps earlier than activation.
pub fn archive_semantic_memory(
    record: &mut ConsolidatedMemoryRecord,
    archived_at_unix_ms: i64,
) -> Result<(), SemanticMemoryError> {
    validate_record(record)?;
    if archived_at_unix_ms < record.activated_at_unix_ms {
        return Err(SemanticMemoryError::EvidenceInvalid("archive timestamp"));
    }
    advance_record_version(record)?;
    record.lifecycle = ConsolidatedMemoryLifecycle::Archived;
    record.archived_at_unix_ms = Some(archived_at_unix_ms);
    record.reason_code = "semantic_memory.archived".to_owned();
    refresh_record_digest(record)
}

/// Reactivates the exact prior signed-by-digest record as a new lineage version.
///
/// # Errors
/// Requires the current rollback pointer, intact target digest, full host gate,
/// and a strictly newer approval generation.
pub fn rollback_semantic_memory(
    current: &ConsolidatedMemoryRecord,
    target: &ConsolidatedMemoryRecord,
    gate: &SemanticMemoryHostGate,
) -> Result<ConsolidatedMemoryRecord, SemanticMemoryError> {
    validate_record(current)?;
    validate_record(target)?;
    if current.previous_record_sha256.as_deref() != Some(target.record_sha256.as_str())
        || current.memory_id != target.memory_id
        || current.claim_key != target.claim_key
        || current.acl_scope != target.acl_scope
        || gate.approval_generation <= current.approval_generation
        || !gate.host_validated
        || !gate.policy_approved
        || !gate.reviewer_approved
        || !gate.quality_eval_approved
    {
        return Err(SemanticMemoryError::RollbackTargetInvalid);
    }
    let mut rolled_back = target.clone();
    rolled_back.version =
        current.version.checked_add(1).ok_or(SemanticMemoryError::RollbackTargetInvalid)?;
    rolled_back.lifecycle = ConsolidatedMemoryLifecycle::Active;
    rolled_back.approval_generation = gate.approval_generation;
    rolled_back.activated_at_unix_ms = gate.activated_at_unix_ms;
    rolled_back.degraded_at_unix_ms = None;
    rolled_back.archived_at_unix_ms = None;
    rolled_back.previous_record_sha256 = Some(current.record_sha256.clone());
    if rolled_back.rollback_history_sha256.len() >= MAX_ROLLBACK_HISTORY {
        return Err(SemanticMemoryError::RollbackTargetInvalid);
    }
    rolled_back.rollback_history_sha256.push(current.record_sha256.clone());
    rolled_back.reason_code = "semantic_memory.rollback_activated".to_owned();
    refresh_record_digest(&mut rolled_back)?;
    Ok(rolled_back)
}

/// Returns a citation-bearing projection only for recall-eligible lifecycle states.
#[must_use]
pub fn semantic_memory_retrieval_projection(
    record: &ConsolidatedMemoryRecord,
) -> Option<SemanticMemoryRetrievalProjectionV1> {
    if validate_record(record).is_err() || record.lifecycle != ConsolidatedMemoryLifecycle::Active {
        return None;
    }
    Some(SemanticMemoryRetrievalProjectionV1 {
        v: SEMANTIC_MEMORY_SCHEMA_VERSION,
        memory_id: record.memory_id.clone(),
        version: record.version,
        summary_text: record.summary_text.clone(),
        epistemic_label: record.epistemic_kind.retrieval_label().to_owned(),
        citations: record.citations.clone(),
        confidence_basis_points: record.confidence_basis_points,
        degraded: false,
        instruction_authority: false,
    })
}

fn validate_policy(policy: &SemanticMemoryConsolidationPolicy) -> Result<(), SemanticMemoryError> {
    if policy.min_corroborating_evidence < 2
        || policy.min_corroborating_evidence > MAX_EVIDENCE_REFS
        || policy.reviewer_confidence_threshold_basis_points > BASIS_POINTS_MAX
        || policy.max_sensitive_retention_ms <= 0
    {
        return Err(SemanticMemoryError::EvidenceInvalid("consolidation policy"));
    }
    Ok(())
}

fn validate_evidence(evidence: &[SemanticMemoryEvidenceRefV1]) -> Result<(), SemanticMemoryError> {
    if evidence.is_empty() || evidence.len() > MAX_EVIDENCE_REFS {
        return Err(SemanticMemoryError::EvidenceInvalid("evidence count"));
    }
    let mut ids = BTreeSet::new();
    for item in evidence {
        if item.v != SEMANTIC_MEMORY_SCHEMA_VERSION
            || !valid_identifier(item.evidence_id.as_str())
            || !ids.insert(item.evidence_id.clone())
            || !valid_reference(item.source_ref.as_str())
            || !valid_reference(item.citation_uri.as_str())
            || !valid_sha256(item.content_sha256.as_str())
            || !valid_sha256(item.provenance_sha256.as_str())
            || !valid_reference(item.claim_key.as_str())
            || !valid_sha256(item.claim_value_sha256.as_str())
            || item.acl_scope.trim().is_empty()
            || item.acl_scope.len() > MAX_SCOPE_BYTES
            || item.confidence_basis_points > BASIS_POINTS_MAX
            || item.observed_at_unix_ms < 0
            || item
                .expires_at_unix_ms
                .is_some_and(|expires_at| expires_at <= item.observed_at_unix_ms)
            || item.corrects_evidence_ids.len() > MAX_EVIDENCE_REFS
            || item
                .corrects_evidence_ids
                .iter()
                .any(|id| !valid_identifier(id.as_str()) || id == &item.evidence_id)
        {
            return Err(SemanticMemoryError::EvidenceInvalid("evidence shape"));
        }
    }
    Ok(())
}

fn resolve_claim_value(
    evidence: &[SemanticMemoryEvidenceRefV1],
) -> (String, SemanticMemoryContradictionStatus, Vec<&SemanticMemoryEvidenceRefV1>) {
    let by_value = evidence.iter().fold(
        BTreeMap::<&str, Vec<&SemanticMemoryEvidenceRefV1>>::new(),
        |mut grouped, item| {
            grouped.entry(item.claim_value_sha256.as_str()).or_default().push(item);
            grouped
        },
    );
    if by_value.len() == 1 {
        let selected = evidence.iter().collect::<Vec<_>>();
        return (
            evidence
                .first()
                .map(|item| item.claim_value_sha256.clone())
                .unwrap_or_else(|| sha256_hex(b"semantic-memory-missing-evidence")),
            SemanticMemoryContradictionStatus::None,
            selected,
        );
    }
    let corrected = evidence.iter().rev().find(|candidate| {
        candidate.epistemic_kind == SemanticMemoryEpistemicKind::UserFact
            && !candidate.corrects_evidence_ids.is_empty()
            && evidence
                .iter()
                .filter(|item| item.claim_value_sha256 != candidate.claim_value_sha256)
                .all(|item| candidate.corrects_evidence_ids.contains(&item.evidence_id))
    });
    if let Some(corrected) = corrected {
        let selected = evidence
            .iter()
            .filter(|item| item.claim_value_sha256 == corrected.claim_value_sha256)
            .collect();
        return (
            corrected.claim_value_sha256.clone(),
            SemanticMemoryContradictionStatus::ResolvedByUserCorrection,
            selected,
        );
    }
    (
        sha256_hex(b"semantic-memory-unresolved-contradiction"),
        SemanticMemoryContradictionStatus::Detected,
        evidence.iter().collect(),
    )
}

fn safe_epistemic_kind(evidence: &[&SemanticMemoryEvidenceRefV1]) -> SemanticMemoryEpistemicKind {
    let kinds = evidence.iter().map(|item| item.epistemic_kind).collect::<BTreeSet<_>>();
    if kinds.len() == 1 {
        return evidence
            .first()
            .map_or(SemanticMemoryEpistemicKind::ModelInference, |item| item.epistemic_kind);
    }
    // Mixed provenance cannot inherit a stronger source label.
    SemanticMemoryEpistemicKind::ModelInference
}

fn validate_retention(
    sensitivity: SemanticMemorySensitivity,
    expires_at: Option<i64>,
    created_at: i64,
    max_sensitive_retention_ms: i64,
) -> Result<(), SemanticMemoryError> {
    if sensitivity < SemanticMemorySensitivity::Sensitive {
        return Ok(());
    }
    let Some(expires_at) = expires_at else {
        return Err(SemanticMemoryError::SensitiveRetentionInvalid);
    };
    if expires_at <= created_at
        || expires_at.saturating_sub(created_at) > max_sensitive_retention_ms
    {
        return Err(SemanticMemoryError::SensitiveRetentionInvalid);
    }
    Ok(())
}

fn validate_candidate(candidate: &SemanticMemoryCandidateV1) -> Result<(), SemanticMemoryError> {
    if candidate.v != SEMANTIC_MEMORY_SCHEMA_VERSION
        || !valid_identifier(candidate.candidate_id.as_str())
        || candidate.summary_text.trim().is_empty()
        || candidate.summary_text.len() > MAX_SUMMARY_BYTES
        || sha256_hex(candidate.summary_text.as_bytes()) != candidate.summary_sha256
        || !valid_sha256(candidate.claim_value_sha256.as_str())
        || candidate.confidence_basis_points > BASIS_POINTS_MAX
        || candidate.evidence_refs.is_empty()
        || candidate.evidence_refs.len() > MAX_EVIDENCE_REFS
        || candidate.citations.len() != candidate.evidence_refs.len()
        || candidate.citations.len() > MAX_CITATIONS
        || !candidate.quality_eval.qualifies()
        || candidate.created_at_unix_ms < 0
        || !valid_reason_code(candidate.reason_code.as_str())
    {
        return Err(SemanticMemoryError::EvidenceInvalid("candidate contract"));
    }
    validate_evidence(candidate.evidence_refs.as_slice())?;
    let expected_citations =
        candidate.evidence_refs.iter().map(citation_from_evidence).collect::<Vec<_>>();
    if candidate.citations != expected_citations {
        return Err(SemanticMemoryError::EvidenceInvalid("candidate citations"));
    }
    Ok(())
}

fn validate_gate(
    candidate: &SemanticMemoryCandidateV1,
    gate: &SemanticMemoryHostGate,
) -> Result<(), SemanticMemoryError> {
    if candidate.contradiction_status == SemanticMemoryContradictionStatus::Detected {
        return Err(SemanticMemoryError::ContradictionUnresolved);
    }
    if !gate.host_validated {
        return Err(SemanticMemoryError::HostValidationRequired);
    }
    if !gate.policy_approved {
        return Err(SemanticMemoryError::PolicyApprovalRequired);
    }
    if candidate.review_required && !gate.reviewer_approved {
        return Err(SemanticMemoryError::ReviewerApprovalRequired);
    }
    if !gate.quality_eval_approved {
        return Err(SemanticMemoryError::QualityEvalApprovalRequired);
    }
    if gate.approval_generation == 0 || gate.activated_at_unix_ms < candidate.created_at_unix_ms {
        return Err(SemanticMemoryError::ApprovalGenerationInvalid);
    }
    if candidate
        .retention_expires_at_unix_ms
        .is_some_and(|expires_at| expires_at <= gate.activated_at_unix_ms)
    {
        return Err(SemanticMemoryError::SensitiveRetentionInvalid);
    }
    Ok(())
}

fn validate_record(record: &ConsolidatedMemoryRecord) -> Result<(), SemanticMemoryError> {
    if record.v != SEMANTIC_MEMORY_SCHEMA_VERSION
        || !valid_identifier(record.memory_id.as_str())
        || record.version == 0
        || record.approval_generation == 0
        || record.summary_text.trim().is_empty()
        || record.summary_text.len() > MAX_SUMMARY_BYTES
        || sha256_hex(record.summary_text.as_bytes()) != record.summary_sha256
        || !valid_sha256(record.claim_value_sha256.as_str())
        || record.evidence_refs.is_empty()
        || record.evidence_refs.len() > MAX_EVIDENCE_REFS
        || record.citations.len() != record.evidence_refs.len()
        || record.citations.len() > MAX_CITATIONS
        || !record.quality_eval.qualifies()
        || record.activated_at_unix_ms < 0
        || record.previous_record_sha256.as_deref().is_some_and(|digest| !valid_sha256(digest))
        || record.rollback_history_sha256.len() > MAX_ROLLBACK_HISTORY
        || record.rollback_history_sha256.iter().any(|digest| !valid_sha256(digest))
        || !valid_reason_code(record.reason_code.as_str())
        || record.record_sha256 != expected_record_digest(record)?
    {
        return Err(SemanticMemoryError::RecordDigestInvalid);
    }
    validate_evidence(record.evidence_refs.as_slice())?;
    if record.citations
        != record.evidence_refs.iter().map(citation_from_evidence).collect::<Vec<_>>()
    {
        return Err(SemanticMemoryError::RecordDigestInvalid);
    }
    Ok(())
}

fn refresh_record_digest(record: &mut ConsolidatedMemoryRecord) -> Result<(), SemanticMemoryError> {
    record.record_sha256.clear();
    record.record_sha256 = expected_record_digest(record)?;
    Ok(())
}

fn advance_record_version(
    record: &mut ConsolidatedMemoryRecord,
) -> Result<(), SemanticMemoryError> {
    let previous_digest = record.record_sha256.clone();
    record.version =
        record.version.checked_add(1).ok_or(SemanticMemoryError::ApprovalGenerationInvalid)?;
    record.previous_record_sha256 = Some(previous_digest);
    Ok(())
}

fn expected_record_digest(
    record: &ConsolidatedMemoryRecord,
) -> Result<String, SemanticMemoryError> {
    let mut payload = record.clone();
    payload.record_sha256.clear();
    let bytes = serde_json::to_vec(&payload).map_err(|_| SemanticMemoryError::Serialization)?;
    let mut hasher = Sha256::new();
    hasher.update(b"palyra.semantic-memory.record.v1\0");
    hasher.update((bytes.len() as u64).to_le_bytes());
    hasher.update(bytes);
    Ok(hex::encode(hasher.finalize()))
}

fn citation_from_evidence(evidence: &SemanticMemoryEvidenceRefV1) -> SemanticMemoryCitationV1 {
    SemanticMemoryCitationV1 {
        evidence_id: evidence.evidence_id.clone(),
        source_ref: evidence.source_ref.clone(),
        citation_uri: evidence.citation_uri.clone(),
        content_sha256: evidence.content_sha256.clone(),
        provenance_sha256: evidence.provenance_sha256.clone(),
    }
}

fn sha256_hex(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

fn valid_sha256(value: &str) -> bool {
    value.len() == 64
        && value.bytes().all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn ratio_basis_points(numerator: u64, denominator: u64, empty_is_perfect: bool) -> u16 {
    if denominator == 0 {
        return if empty_is_perfect { BASIS_POINTS_MAX } else { 0 };
    }
    let scaled = numerator
        .saturating_mul(u64::from(BASIS_POINTS_MAX))
        .saturating_div(denominator)
        .min(u64::from(BASIS_POINTS_MAX));
    u16::try_from(scaled).unwrap_or(BASIS_POINTS_MAX)
}

fn update_hash_field(hasher: &mut Sha256, field: &[u8]) {
    hasher.update((field.len() as u64).to_le_bytes());
    hasher.update(field);
}

fn valid_identifier(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.'))
}

fn valid_reference(value: &str) -> bool {
    !value.trim().is_empty()
        && value.len() <= MAX_SCOPE_BYTES
        && !value.chars().any(char::is_control)
}

fn valid_reason_code(value: &str) -> bool {
    value.len() <= MAX_REASON_CODE_BYTES && value.contains('.') && valid_identifier(value)
}

#[cfg(test)]
mod tests {
    use super::*;

    const NOW: i64 = 1_000_000;

    fn hash(value: &str) -> String {
        sha256_hex(value.as_bytes())
    }

    fn quality_eval() -> SemanticMemoryQualityEvalV1 {
        SemanticMemoryQualityEvalV1 {
            v: 1,
            sample_count: 20,
            baseline_precision_basis_points: 8_000,
            consolidated_precision_basis_points: 8_500,
            baseline_usefulness_basis_points: 7_000,
            consolidated_usefulness_basis_points: 8_200,
            baseline_correction_rate_basis_points: 800,
            consolidated_correction_rate_basis_points: 500,
            evidence_sha256: hash("quality-eval"),
        }
    }

    fn evidence(
        id: &str,
        source: &str,
        value: &str,
        kind: SemanticMemoryEpistemicKind,
        sensitivity: SemanticMemorySensitivity,
        observed_at: i64,
    ) -> SemanticMemoryEvidenceRefV1 {
        SemanticMemoryEvidenceRefV1 {
            v: 1,
            evidence_id: id.to_owned(),
            source_ref: source.to_owned(),
            citation_uri: format!("memory://evidence/{id}"),
            content_sha256: hash(format!("source-content-{id}").as_str()),
            provenance_sha256: hash(format!("provenance-{id}").as_str()),
            claim_key: "preferred_editor".to_owned(),
            claim_value_sha256: hash(value),
            acl_scope: "principal:user-1".to_owned(),
            epistemic_kind: kind,
            sensitivity,
            confidence_basis_points: 9_000,
            observed_at_unix_ms: observed_at,
            expires_at_unix_ms: None,
            corrects_evidence_ids: Vec::new(),
        }
    }

    fn policy() -> SemanticMemoryConsolidationPolicy {
        SemanticMemoryConsolidationPolicy {
            enabled: true,
            ..SemanticMemoryConsolidationPolicy::default()
        }
    }

    fn request(
        evidence_refs: Vec<SemanticMemoryEvidenceRefV1>,
    ) -> SemanticMemoryConsolidationRequest {
        SemanticMemoryConsolidationRequest {
            candidate_id: "candidate-1".to_owned(),
            summary_text: "The preferred editor is Helix.".to_owned(),
            evidence_refs,
            retention_expires_at_unix_ms: None,
            quality_eval: quality_eval(),
            created_at_unix_ms: NOW,
        }
    }

    fn gate(generation: u64) -> SemanticMemoryHostGate {
        SemanticMemoryHostGate {
            host_validated: true,
            policy_approved: true,
            reviewer_approved: true,
            quality_eval_approved: true,
            approval_generation: generation,
            activated_at_unix_ms: NOW + i64::try_from(generation).unwrap_or(i64::MAX),
        }
    }

    fn candidate() -> SemanticMemoryCandidateV1 {
        build_semantic_memory_candidate(
            request(vec![
                evidence(
                    "evidence-1",
                    "session:one",
                    "helix",
                    SemanticMemoryEpistemicKind::Preference,
                    SemanticMemorySensitivity::Internal,
                    NOW - 20,
                ),
                evidence(
                    "evidence-2",
                    "session:two",
                    "helix",
                    SemanticMemoryEpistemicKind::Preference,
                    SemanticMemorySensitivity::Internal,
                    NOW - 10,
                ),
            ]),
            &policy(),
        )
        .expect("candidate should build")
    }

    #[test]
    fn repeated_corroborating_evidence_activates_with_citations() {
        let candidate = candidate();
        assert_eq!(candidate.contradiction_status, SemanticMemoryContradictionStatus::None);
        assert_eq!(candidate.citations.len(), 2);
        let active =
            activate_semantic_memory_candidate("memory-1".to_owned(), &candidate, &gate(1), None)
                .expect("candidate should activate");
        let projection =
            semantic_memory_retrieval_projection(&active).expect("active memory should project");
        assert_eq!(projection.epistemic_label, "preference");
        assert_eq!(projection.citations.len(), 2);
        assert!(!projection.instruction_authority);
    }

    #[test]
    fn contradiction_is_visible_and_cannot_activate() {
        let candidate = build_semantic_memory_candidate(
            request(vec![
                evidence(
                    "evidence-1",
                    "session:one",
                    "helix",
                    SemanticMemoryEpistemicKind::Preference,
                    SemanticMemorySensitivity::Internal,
                    NOW - 20,
                ),
                evidence(
                    "evidence-2",
                    "session:two",
                    "vim",
                    SemanticMemoryEpistemicKind::Preference,
                    SemanticMemorySensitivity::Internal,
                    NOW - 10,
                ),
            ]),
            &policy(),
        )
        .expect("contradiction should remain a candidate");
        assert_eq!(candidate.contradiction_status, SemanticMemoryContradictionStatus::Detected);
        let error =
            activate_semantic_memory_candidate("memory-1".to_owned(), &candidate, &gate(1), None)
                .expect_err("unresolved contradiction must fail");
        assert_eq!(error, SemanticMemoryError::ContradictionUnresolved);
    }

    #[test]
    fn user_correction_resolves_conflict_without_relabeling_inference() {
        let old = evidence(
            "evidence-old",
            "session:one",
            "vim",
            SemanticMemoryEpistemicKind::ModelInference,
            SemanticMemorySensitivity::Internal,
            NOW - 20,
        );
        let mut correction = evidence(
            "evidence-correction",
            "session:two",
            "helix",
            SemanticMemoryEpistemicKind::UserFact,
            SemanticMemorySensitivity::Internal,
            NOW - 10,
        );
        correction.corrects_evidence_ids = vec![old.evidence_id.clone()];
        let candidate = build_semantic_memory_candidate(request(vec![old, correction]), &policy())
            .expect("user correction should resolve conflict");
        assert_eq!(
            candidate.contradiction_status,
            SemanticMemoryContradictionStatus::ResolvedByUserCorrection
        );
        assert_eq!(candidate.epistemic_kind, SemanticMemoryEpistemicKind::UserFact);
        assert!(candidate.review_required);
    }

    #[test]
    fn stale_memory_degrades_and_leaves_current_retrieval() {
        let candidate = candidate();
        let mut active =
            activate_semantic_memory_candidate("memory-1".to_owned(), &candidate, &gate(1), None)
                .expect("candidate should activate");
        assert!(mark_semantic_memory_stale(&mut active, NOW + 10_000, 100)
            .expect("staleness should evaluate"));
        assert_eq!(active.lifecycle, ConsolidatedMemoryLifecycle::Degraded);
        assert!(
            semantic_memory_retrieval_projection(&active).is_none(),
            "degraded memory must retain evidence without remaining current"
        );
    }

    #[test]
    fn sensitive_candidate_requires_bounded_retention_and_review() {
        let evidence = vec![
            evidence(
                "evidence-1",
                "session:one",
                "helix",
                SemanticMemoryEpistemicKind::UserFact,
                SemanticMemorySensitivity::Sensitive,
                NOW - 20,
            ),
            evidence(
                "evidence-2",
                "session:two",
                "helix",
                SemanticMemoryEpistemicKind::UserFact,
                SemanticMemorySensitivity::Sensitive,
                NOW - 10,
            ),
        ];
        let error = build_semantic_memory_candidate(request(evidence.clone()), &policy())
            .expect_err("unbounded sensitive memory must fail");
        assert_eq!(error, SemanticMemoryError::SensitiveRetentionInvalid);

        let mut request = request(evidence);
        request.retention_expires_at_unix_ms = Some(NOW + 1_000);
        let candidate =
            build_semantic_memory_candidate(request, &policy()).expect("retention is bounded");
        assert!(candidate.review_required);
        let mut gate = gate(1);
        gate.reviewer_approved = false;
        let error =
            activate_semantic_memory_candidate("memory-1".to_owned(), &candidate, &gate, None)
                .expect_err("review must be explicit");
        assert_eq!(error, SemanticMemoryError::ReviewerApprovalRequired);
    }

    #[test]
    fn rollback_creates_new_version_and_preserves_evidence_hashes() {
        let original_candidate = candidate();
        let original = activate_semantic_memory_candidate(
            "memory-1".to_owned(),
            &original_candidate,
            &gate(1),
            None,
        )
        .expect("original should activate");
        let mut replacement_candidate = original_candidate.clone();
        replacement_candidate.candidate_id = "candidate-2".to_owned();
        replacement_candidate.summary_text = "The preferred editor remains Helix.".to_owned();
        replacement_candidate.summary_sha256 =
            sha256_hex(replacement_candidate.summary_text.as_bytes());
        let replacement = activate_semantic_memory_candidate(
            "memory-1".to_owned(),
            &replacement_candidate,
            &gate(2),
            Some(&original),
        )
        .expect("replacement should activate");
        let rolled_back = rollback_semantic_memory(&replacement, &original, &gate(3))
            .expect("rollback should activate");
        assert_eq!(rolled_back.version, 3);
        assert_eq!(rolled_back.lifecycle, ConsolidatedMemoryLifecycle::Active);
        assert_eq!(rolled_back.evidence_refs, original.evidence_refs);
        assert!(semantic_memory_retrieval_projection(&rolled_back).is_some());
        assert!(rolled_back.rollback_history_sha256.contains(&replacement.record_sha256));
    }

    #[test]
    fn retrieval_feedback_tracks_usefulness_and_correction_degrades_record() {
        let candidate = candidate();
        let mut active =
            activate_semantic_memory_candidate("memory-1".to_owned(), &candidate, &gate(1), None)
                .expect("candidate should activate");
        let corrected_id = active.evidence_refs[0].evidence_id.clone();
        let mut correction = evidence(
            "evidence-correction",
            "session:three",
            "zed",
            SemanticMemoryEpistemicKind::UserFact,
            SemanticMemorySensitivity::Internal,
            NOW + 20,
        );
        correction.corrects_evidence_ids = vec![corrected_id];
        apply_semantic_memory_retrieval_feedback(
            &mut active,
            SemanticMemoryRetrievalFeedbackV1 {
                useful: false,
                corrected: true,
                retrieved_at_unix_ms: NOW + 20,
                correction_evidence_ref: Some(correction),
            },
        )
        .expect("feedback should apply");
        assert_eq!(active.retrieval_metrics.retrieval_count, 1);
        assert_eq!(active.retrieval_metrics.correction_count, 1);
        assert_eq!(active.lifecycle, ConsolidatedMemoryLifecycle::Degraded);
    }

    #[test]
    fn durable_contract_top_level_shapes_match_json_schemas() {
        let candidate = candidate();
        let record =
            activate_semantic_memory_candidate("memory-1".to_owned(), &candidate, &gate(1), None)
                .expect("candidate should activate");
        assert_schema_required_fields(
            &serde_json::to_value(candidate).expect("candidate should serialize"),
            include_str!("../../../../schemas/json/common/semantic-memory-candidate.v1.json"),
        );
        assert_schema_required_fields(
            &serde_json::to_value(record).expect("record should serialize"),
            include_str!("../../../../schemas/json/common/consolidated-memory-record.v1.json"),
        );
    }

    #[test]
    fn quality_eval_is_derived_from_observed_rankings_and_rejects_uppercase_digest() {
        let candidate_id = "semantic-eval-candidate";
        let cases = (0..10)
            .map(|index| SemanticMemoryQualityEvalCaseV1 {
                case_id: format!("case-{index}"),
                query: "preferred editor helix".to_owned(),
                expected_baseline_memory_ids: Vec::new(),
                candidate_relevant: true,
            })
            .collect::<Vec<_>>();
        let observations = cases
            .iter()
            .map(|case| SemanticMemoryQualityEvalObservationV1 {
                case_id: case.case_id.clone(),
                baseline_memory_ids: Vec::new(),
                consolidated_memory_ids: vec![candidate_id.to_owned()],
            })
            .collect::<Vec<_>>();
        let derived = derive_semantic_memory_quality_eval(
            candidate_id,
            cases.as_slice(),
            observations.as_slice(),
        )
        .expect("server observations should derive an eval");
        assert!(derived.qualifies());
        assert_eq!(derived.baseline_usefulness_basis_points, 0);
        assert_eq!(derived.consolidated_usefulness_basis_points, 10_000);

        let mut uppercase = derived;
        uppercase.evidence_sha256 = uppercase.evidence_sha256.to_ascii_uppercase();
        assert!(!uppercase.qualifies());
    }

    fn assert_schema_required_fields(value: &serde_json::Value, schema_json: &str) {
        let schema: serde_json::Value =
            serde_json::from_str(schema_json).expect("schema should parse");
        let actual = value
            .as_object()
            .expect("durable contract should be an object")
            .keys()
            .cloned()
            .collect::<BTreeSet<_>>();
        let required = schema
            .get("required")
            .and_then(serde_json::Value::as_array)
            .expect("schema should declare required fields")
            .iter()
            .map(|field| field.as_str().expect("required field should be text").to_owned())
            .collect::<BTreeSet<_>>();
        assert_eq!(actual, required);
    }
}
