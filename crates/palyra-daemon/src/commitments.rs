//! Commitment extraction and scheduling bridge helpers.
//!
//! The extractor is deterministic by design. It turns explicit user-language
//! commitments and opt-in inferred candidates into reviewable ledger rows while
//! keeping delivery/scheduling in a separate audited step.

use std::collections::BTreeSet;

use palyra_common::redaction::redact_diagnostic_text;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use ulid::Ulid;

use crate::acceptance::{commitment_acceptance_criteria, commitment_evidence_json};
use crate::journal::{
    CommitmentCandidateSensitivity, CommitmentCandidateV2, CommitmentCreateRequest,
    CommitmentEvidenceSpanV2, COMMITMENT_CANDIDATE_V2_SCHEMA_VERSION,
};

pub(crate) const HYBRID_INFERRED_COMMITMENTS_SCHEMA_VERSION: u64 = 1;
pub(crate) const HYBRID_INFERRED_COMMITMENTS_EVENT_STARTED: &str =
    "hybridni_inferred_commitments_jako_kandidati.started";
pub(crate) const HYBRID_INFERRED_COMMITMENTS_EVENT_COMPLETED: &str =
    "hybridni_inferred_commitments_jako_kandidati.completed";
pub(crate) const HYBRID_INFERRED_COMMITMENTS_EVENT_FAILED: &str =
    "hybridni_inferred_commitments_jako_kandidati.failed";
pub(crate) const HYBRID_INFERRED_COMMITMENTS_ROLLOUT_OBSERVE_ONLY: &str =
    "operator_opt_in_candidates_only";
pub(crate) const HYBRID_INFERRED_COMMITMENTS_REDACTION_LEVEL: &str = "metadata_only";
pub(crate) const POST_TURN_COMMITMENT_EXTRACTION_SCHEMA_VERSION: u64 = 2;
pub(crate) const POST_TURN_COMMITMENT_EXTRACTION_EVENT: &str = "commitment.extraction.decision";
pub(crate) const POST_TURN_COMMITMENT_EXTRACTION_ROLLOUT: &str = "bounded_candidate_only";

const DEFAULT_EXTRACTION_MODEL: &str = "deterministic.commitment-extractor.v1";
const DEFAULT_INFERENCE_MODEL: &str = "deterministic.commitment-inference.v1";
const MAX_EXTRACTED_COMMITMENTS: usize = 20;
const MAX_INFERRED_COMMITMENTS: usize = 8;
const MAX_POST_TURN_SOURCE_BYTES: usize = 64 * 1024;
const POST_TURN_SAMPLE_BASIS_POINTS: u64 = 1_000;
const POST_TURN_HIGH_VALUE_BASIS_POINTS: u64 = 7_500;

/// Source material and owner scope for commitment extraction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CommitmentExtractionInput {
    pub owner_principal: String,
    pub device_id: String,
    pub channel: Option<String>,
    pub session_id: Option<String>,
    pub run_id: Option<String>,
    pub source_text: String,
    pub extraction_model: Option<String>,
    pub include_inferred: bool,
    pub auxiliary_selection: Option<CommitmentAuxiliarySelection>,
}

/// One extracted commitment candidate awaiting review.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct ExtractedCommitment {
    pub user_wording: String,
    pub normalized_action: String,
    pub due_condition_json: String,
    pub recurrence_json: String,
    pub confidence_bps: u64,
    pub review_reason: String,
    pub evidence_span: CommitmentEvidenceSpanV2,
    pub sensitivity: CommitmentCandidateSensitivity,
}

/// Candidate source class persisted in source evidence.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum CommitmentCandidateKind {
    Explicit,
    Inferred,
}

/// Decision emitted by the hybrid inferred-commitment projection.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum HybridCommitmentInferenceDecision {
    Disabled,
    Candidate,
    NoCandidate,
}

/// Stable reason code for inferred commitment decisions and audit metadata.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) enum HybridCommitmentReasonCode {
    #[serde(rename = "hybrid_commitments.inference_disabled")]
    InferenceDisabled,
    #[serde(rename = "hybrid_commitments.explicit_language")]
    ExplicitLanguage,
    #[serde(rename = "hybrid_commitments.inferred_soft_language")]
    InferredSoftLanguage,
    #[serde(rename = "hybrid_commitments.explicit_overlap")]
    ExplicitOverlap,
    #[serde(rename = "hybrid_commitments.negated_soft_language")]
    NegatedSoftLanguage,
    #[serde(rename = "hybrid_commitments.no_inferred_signal")]
    NoInferredSignal,
}

impl HybridCommitmentReasonCode {
    fn as_str(self) -> &'static str {
        match self {
            Self::InferenceDisabled => "hybrid_commitments.inference_disabled",
            Self::ExplicitLanguage => "hybrid_commitments.explicit_language",
            Self::InferredSoftLanguage => "hybrid_commitments.inferred_soft_language",
            Self::ExplicitOverlap => "hybrid_commitments.explicit_overlap",
            Self::NegatedSoftLanguage => "hybrid_commitments.negated_soft_language",
            Self::NoInferredSignal => "hybrid_commitments.no_inferred_signal",
        }
    }
}

/// One inferred commitment candidate awaiting explicit operator review.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct InferredCommitmentCandidate {
    pub user_wording: String,
    pub normalized_action: String,
    pub due_condition_json: String,
    pub recurrence_json: String,
    pub confidence_bps: u64,
    pub review_reason: String,
    pub reason_code: HybridCommitmentReasonCode,
    pub evidence_span: CommitmentEvidenceSpanV2,
    pub sensitivity: CommitmentCandidateSensitivity,
}

/// Host-owned selection metadata attached to candidates extracted automatically.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct CommitmentAuxiliarySelection {
    pub(crate) reason_code: String,
    pub(crate) value_score_bps: u64,
    pub(crate) sample_bucket_bps: u64,
    pub(crate) source_sha256: String,
}

/// Redacted projection written to the run tape for every post-turn decision.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct PostTurnCommitmentExtractionProjection {
    pub(crate) schema_version: u64,
    pub(crate) rollout_mode: String,
    pub(crate) selected: bool,
    pub(crate) reason_code: String,
    pub(crate) value_score_bps: u64,
    pub(crate) sample_bucket_bps: u64,
    pub(crate) source_bytes: usize,
    pub(crate) source_truncated: bool,
    pub(crate) source_sha256: String,
    pub(crate) candidate_only: bool,
    pub(crate) approval_requirement: String,
    pub(crate) extracted_candidates: usize,
    pub(crate) deduplicated_candidates: usize,
    pub(crate) failed_candidates: usize,
}

/// Selected bounded source plus its metadata-only audit projection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PostTurnCommitmentExtractionDecision {
    pub(crate) source_text: String,
    pub(crate) selection: CommitmentAuxiliarySelection,
    pub(crate) projection: PostTurnCommitmentExtractionProjection,
}

/// Event names attached to the inferred commitment projection contract.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct HybridCommitmentInferenceEventTypes {
    pub started: String,
    pub completed: String,
    pub failed: String,
}

/// Journal/read-model projection for hybrid inferred commitment extraction.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct HybridCommitmentInferenceProjection {
    pub schema_version: u64,
    pub rollout_mode: String,
    pub include_inferred: bool,
    pub decision: HybridCommitmentInferenceDecision,
    pub reason_code: HybridCommitmentReasonCode,
    pub explicit_candidates: usize,
    pub inferred_candidates: usize,
    pub suppressed_candidates: usize,
    pub event_types: HybridCommitmentInferenceEventTypes,
    pub redaction_level: String,
}

/// Commitment create requests plus the audit projection for this extraction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CommitmentExtractionPlan {
    pub requests: Vec<CommitmentCreateRequest>,
    pub inference: HybridCommitmentInferenceProjection,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct HybridCommitmentInferenceOutcome {
    candidates: Vec<InferredCommitmentCandidate>,
    suppressed_candidates: usize,
    reason_code: HybridCommitmentReasonCode,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct CommitmentRequestCandidate {
    kind: CommitmentCandidateKind,
    user_wording: String,
    normalized_action: String,
    due_condition_json: String,
    recurrence_json: String,
    confidence_bps: u64,
    review_reason: String,
    reason_code: HybridCommitmentReasonCode,
    evidence_span: CommitmentEvidenceSpanV2,
    sensitivity: CommitmentCandidateSensitivity,
}

#[derive(Debug, Clone, Copy)]
struct CandidateSentence<'a> {
    text: &'a str,
    start_byte: usize,
    end_byte: usize,
}

/// Applies the fixed sampling and value heuristic used by the post-turn auxiliary stage.
#[must_use]
pub(crate) fn select_post_turn_commitment_extraction(
    run_id: &str,
    source_text: &str,
) -> PostTurnCommitmentExtractionDecision {
    let (bounded_source, source_truncated) = truncate_utf8(source_text, MAX_POST_TURN_SOURCE_BYTES);
    let redacted_source = sanitize_commitment_text(bounded_source.as_str());
    let source_sha256 = sha256_hex(redacted_source.as_bytes());
    let lower = bounded_source.to_ascii_lowercase();
    let has_explicit_signal = looks_like_commitment(lower.as_str());
    let has_inferred_signal = has_inferred_commitment_marker(lower.as_str());
    let recurring = infer_recurrence(bounded_source.as_str()) != json!({ "type": "none" });
    let due_signal = ["tomorrow", "today", "next week"].iter().any(|marker| lower.contains(marker));
    let mut value_score_bps = 0_u64;
    if has_explicit_signal {
        value_score_bps = value_score_bps.saturating_add(6_500);
    } else if has_inferred_signal {
        value_score_bps = value_score_bps.saturating_add(3_500);
    }
    if recurring {
        value_score_bps = value_score_bps.saturating_add(2_500);
    }
    if due_signal {
        value_score_bps = value_score_bps.saturating_add(1_500);
    }
    value_score_bps = value_score_bps.min(10_000);
    let sample_bucket_bps = deterministic_sample_bucket(run_id, source_sha256.as_str());
    let has_signal = has_explicit_signal || has_inferred_signal;
    let (selected, reason_code) = if bounded_source.trim().is_empty() {
        (false, "commitment.extraction.source_empty")
    } else if !has_signal {
        (false, "commitment.extraction.no_signal")
    } else if value_score_bps >= POST_TURN_HIGH_VALUE_BASIS_POINTS {
        (true, "commitment.extraction.high_value")
    } else if sample_bucket_bps < POST_TURN_SAMPLE_BASIS_POINTS {
        (true, "commitment.extraction.sampled")
    } else {
        (false, "commitment.extraction.sample_skipped")
    };
    let selection = CommitmentAuxiliarySelection {
        reason_code: reason_code.to_owned(),
        value_score_bps,
        sample_bucket_bps,
        source_sha256: source_sha256.clone(),
    };
    PostTurnCommitmentExtractionDecision {
        source_text: bounded_source,
        selection,
        projection: PostTurnCommitmentExtractionProjection {
            schema_version: POST_TURN_COMMITMENT_EXTRACTION_SCHEMA_VERSION,
            rollout_mode: POST_TURN_COMMITMENT_EXTRACTION_ROLLOUT.to_owned(),
            selected,
            reason_code: reason_code.to_owned(),
            value_score_bps,
            sample_bucket_bps,
            source_bytes: source_text.len().min(MAX_POST_TURN_SOURCE_BYTES),
            source_truncated,
            source_sha256,
            candidate_only: true,
            approval_requirement: "manual_review".to_owned(),
            extracted_candidates: 0,
            deduplicated_candidates: 0,
            failed_candidates: 0,
        },
    }
}

/// Extracts explicit commitments from user-visible text.
pub(crate) fn extract_commitments_from_text(text: &str) -> Vec<ExtractedCommitment> {
    let mut seen = BTreeSet::new();
    let mut commitments = Vec::new();
    for sentence in split_candidate_sentences(text) {
        let normalized = normalize_sentence(sentence.text);
        if normalized.is_empty() || !looks_like_commitment(normalized.as_str()) {
            continue;
        }
        if !seen.insert(normalized.clone()) {
            continue;
        }
        let user_wording = sanitize_commitment_text(sentence.text);
        if user_wording.is_empty() {
            continue;
        }
        let recurrence_json = infer_recurrence(sentence.text).to_string();
        commitments.push(ExtractedCommitment {
            user_wording,
            normalized_action: normalized,
            due_condition_json: infer_due_condition(sentence.text).to_string(),
            recurrence_json,
            confidence_bps: 7_500,
            review_reason: "explicit commitment language detected".to_owned(),
            evidence_span: commitment_evidence_span(sentence, text),
            sensitivity: classify_candidate_sensitivity(sentence.text),
        });
        if commitments.len() >= MAX_EXTRACTED_COMMITMENTS {
            break;
        }
    }
    commitments
}

/// Converts extracted candidates into ledger create requests and audit metadata.
pub(crate) fn build_commitment_create_plan(
    input: &CommitmentExtractionInput,
    actor_principal: &str,
) -> CommitmentExtractionPlan {
    let explicit_candidates = extract_commitments_from_text(input.source_text.as_str());
    let explicit_actions = explicit_candidates
        .iter()
        .map(|candidate| candidate.normalized_action.clone())
        .collect::<BTreeSet<_>>();
    let model = input.extraction_model.as_deref().unwrap_or(DEFAULT_EXTRACTION_MODEL);
    let mut requests = explicit_candidates
        .iter()
        .map(|candidate| {
            build_commitment_create_request(
                input,
                actor_principal,
                model,
                CommitmentRequestCandidate {
                    kind: CommitmentCandidateKind::Explicit,
                    user_wording: candidate.user_wording.clone(),
                    normalized_action: candidate.normalized_action.clone(),
                    due_condition_json: candidate.due_condition_json.clone(),
                    recurrence_json: candidate.recurrence_json.clone(),
                    confidence_bps: candidate.confidence_bps,
                    review_reason: candidate.review_reason.clone(),
                    reason_code: HybridCommitmentReasonCode::ExplicitLanguage,
                    evidence_span: candidate.evidence_span.clone(),
                    sensitivity: candidate.sensitivity,
                },
            )
        })
        .collect::<Vec<_>>();

    let inference_outcome = if input.include_inferred {
        infer_commitment_candidates_from_text(input.source_text.as_str(), &explicit_actions)
    } else {
        HybridCommitmentInferenceOutcome {
            candidates: Vec::new(),
            suppressed_candidates: 0,
            reason_code: HybridCommitmentReasonCode::InferenceDisabled,
        }
    };
    let inference_model = input.extraction_model.as_deref().unwrap_or(DEFAULT_INFERENCE_MODEL);
    requests.extend(inference_outcome.candidates.iter().map(|candidate| {
        build_commitment_create_request(
            input,
            actor_principal,
            inference_model,
            CommitmentRequestCandidate {
                kind: CommitmentCandidateKind::Inferred,
                user_wording: candidate.user_wording.clone(),
                normalized_action: candidate.normalized_action.clone(),
                due_condition_json: candidate.due_condition_json.clone(),
                recurrence_json: candidate.recurrence_json.clone(),
                confidence_bps: candidate.confidence_bps,
                review_reason: candidate.review_reason.clone(),
                reason_code: candidate.reason_code,
                evidence_span: candidate.evidence_span.clone(),
                sensitivity: candidate.sensitivity,
            },
        )
    }));

    let inference = hybrid_commitment_projection(
        input.include_inferred,
        explicit_candidates.len(),
        inference_outcome.candidates.len(),
        inference_outcome.suppressed_candidates,
        inference_outcome.reason_code,
    );
    CommitmentExtractionPlan { requests, inference }
}

fn infer_commitment_candidates_from_text(
    text: &str,
    explicit_actions: &BTreeSet<String>,
) -> HybridCommitmentInferenceOutcome {
    let mut seen = BTreeSet::new();
    let mut suppressed_candidates = 0;
    let mut reason_code = HybridCommitmentReasonCode::NoInferredSignal;
    let mut candidates = Vec::new();
    for sentence in split_candidate_sentences(text) {
        let normalized = normalize_sentence(sentence.text);
        if normalized.is_empty() || looks_like_commitment(normalized.as_str()) {
            continue;
        }
        if !has_inferred_commitment_marker(normalized.as_str()) {
            continue;
        }
        if has_negated_inferred_marker(normalized.as_str()) {
            suppressed_candidates += 1;
            reason_code = HybridCommitmentReasonCode::NegatedSoftLanguage;
            continue;
        }
        if explicit_actions.contains(normalized.as_str()) || !seen.insert(normalized.clone()) {
            suppressed_candidates += 1;
            reason_code = HybridCommitmentReasonCode::ExplicitOverlap;
            continue;
        }
        let user_wording = sanitize_commitment_text(sentence.text);
        if user_wording.is_empty() {
            continue;
        }
        candidates.push(InferredCommitmentCandidate {
            user_wording,
            normalized_action: normalized,
            due_condition_json: infer_due_condition(sentence.text).to_string(),
            recurrence_json: infer_recurrence(sentence.text).to_string(),
            confidence_bps: 4_500,
            review_reason: "inferred commitment candidate requires manual review".to_owned(),
            reason_code: HybridCommitmentReasonCode::InferredSoftLanguage,
            evidence_span: commitment_evidence_span(sentence, text),
            sensitivity: classify_candidate_sensitivity(sentence.text),
        });
        reason_code = HybridCommitmentReasonCode::InferredSoftLanguage;
        if candidates.len() >= MAX_INFERRED_COMMITMENTS {
            break;
        }
    }

    HybridCommitmentInferenceOutcome { candidates, suppressed_candidates, reason_code }
}

fn build_commitment_create_request(
    input: &CommitmentExtractionInput,
    actor_principal: &str,
    model: &str,
    candidate: CommitmentRequestCandidate,
) -> CommitmentCreateRequest {
    let acceptance = commitment_acceptance_criteria(candidate.normalized_action.as_str());
    let selection = input.auxiliary_selection.clone().unwrap_or_else(|| {
        let redacted_source = sanitize_commitment_text(input.source_text.as_str());
        CommitmentAuxiliarySelection {
            reason_code: "commitment.extraction.operator_requested".to_owned(),
            value_score_bps: 10_000,
            sample_bucket_bps: 0,
            source_sha256: sha256_hex(redacted_source.as_bytes()),
        }
    });
    let dedupe_key = commitment_dedupe_key(
        candidate.normalized_action.as_str(),
        candidate.due_condition_json.as_str(),
        candidate.recurrence_json.as_str(),
    );
    let candidate_v2 = CommitmentCandidateV2 {
        schema_version: COMMITMENT_CANDIDATE_V2_SCHEMA_VERSION,
        evidence_span: candidate.evidence_span.clone(),
        confidence_bps: candidate.confidence_bps,
        recurrence_json: candidate.recurrence_json.clone(),
        sensitivity: candidate.sensitivity,
        dedupe_key,
        extraction_reason_code: candidate.reason_code.as_str().to_owned(),
        selection_reason_code: selection.reason_code.clone(),
        value_score_bps: selection.value_score_bps,
        sample_bucket_bps: selection.sample_bucket_bps,
        source_sha256: selection.source_sha256.clone(),
    };
    CommitmentCreateRequest {
        commitment_id: Ulid::generate().to_string(),
        owner_principal: input.owner_principal.clone(),
        device_id: input.device_id.clone(),
        channel: input.channel.clone(),
        session_id: input.session_id.clone(),
        run_id: input.run_id.clone(),
        user_wording: candidate.user_wording.clone(),
        normalized_action: candidate.normalized_action.clone(),
        due_condition_json: candidate.due_condition_json,
        recurrence_json: candidate.recurrence_json,
        channel_binding_json: json!({
            "type": "console_review",
            "channel": input.channel,
            "candidate_kind": candidate.kind,
        })
        .to_string(),
        approval_requirement: "manual_review".to_owned(),
        privacy_label: "user_visible".to_owned(),
        status: "proposed".to_owned(),
        confidence_bps: candidate.confidence_bps,
        extraction_model: model.to_owned(),
        review_reason: candidate.review_reason.clone(),
        scheduler_binding_json: json!({
            "type": "none",
            "state": "awaiting_review",
            "candidate_kind": candidate.kind,
            "acceptance_criteria": acceptance.clone(),
        })
        .to_string(),
        due_at_unix_ms: None,
        source_kind: source_kind_for_candidate(candidate.kind).to_owned(),
        tape_start_seq: None,
        tape_end_seq: None,
        evidence_json: commitment_evidence_with_hybrid_metadata(
            input,
            candidate.kind,
            candidate.user_wording.as_str(),
            candidate.reason_code,
            acceptance,
            &candidate_v2,
        ),
        candidate_v2: Some(candidate_v2),
        actor_principal: actor_principal.to_owned(),
    }
}

fn commitment_evidence_with_hybrid_metadata(
    input: &CommitmentExtractionInput,
    candidate_kind: CommitmentCandidateKind,
    source_text_preview: &str,
    reason_code: HybridCommitmentReasonCode,
    acceptance: crate::acceptance::AcceptanceCriteria,
    candidate_v2: &CommitmentCandidateV2,
) -> String {
    let base = commitment_evidence_json(
        source_text_preview.to_owned(),
        input.session_id.clone(),
        input.run_id.clone(),
        acceptance,
    );
    let mut evidence =
        serde_json::from_str::<Value>(base.as_str()).expect("commitment evidence is valid JSON");
    if let Some(object) = evidence.as_object_mut() {
        object.insert("candidate_kind".to_owned(), json!(candidate_kind));
        object.insert("commitment_candidate_v2".to_owned(), json!(candidate_v2));
        object.insert(
            "hybrid_inference".to_owned(),
            json!({
                "schema_version": HYBRID_INFERRED_COMMITMENTS_SCHEMA_VERSION,
                "rollout_mode": HYBRID_INFERRED_COMMITMENTS_ROLLOUT_OBSERVE_ONLY,
                "event_types": hybrid_commitment_event_types(),
                "decision": match candidate_kind {
                    CommitmentCandidateKind::Explicit => HybridCommitmentInferenceDecision::NoCandidate,
                    CommitmentCandidateKind::Inferred => HybridCommitmentInferenceDecision::Candidate,
                },
                "reason_code": reason_code.as_str(),
                "redaction_level": HYBRID_INFERRED_COMMITMENTS_REDACTION_LEVEL,
            }),
        );
    }
    evidence.to_string()
}

fn hybrid_commitment_projection(
    include_inferred: bool,
    explicit_candidates: usize,
    inferred_candidates: usize,
    suppressed_candidates: usize,
    reason_code: HybridCommitmentReasonCode,
) -> HybridCommitmentInferenceProjection {
    let decision = if !include_inferred {
        HybridCommitmentInferenceDecision::Disabled
    } else if inferred_candidates > 0 {
        HybridCommitmentInferenceDecision::Candidate
    } else {
        HybridCommitmentInferenceDecision::NoCandidate
    };
    HybridCommitmentInferenceProjection {
        schema_version: HYBRID_INFERRED_COMMITMENTS_SCHEMA_VERSION,
        rollout_mode: HYBRID_INFERRED_COMMITMENTS_ROLLOUT_OBSERVE_ONLY.to_owned(),
        include_inferred,
        decision,
        reason_code,
        explicit_candidates,
        inferred_candidates,
        suppressed_candidates,
        event_types: hybrid_commitment_event_types(),
        redaction_level: HYBRID_INFERRED_COMMITMENTS_REDACTION_LEVEL.to_owned(),
    }
}

fn hybrid_commitment_event_types() -> HybridCommitmentInferenceEventTypes {
    HybridCommitmentInferenceEventTypes {
        started: HYBRID_INFERRED_COMMITMENTS_EVENT_STARTED.to_owned(),
        completed: HYBRID_INFERRED_COMMITMENTS_EVENT_COMPLETED.to_owned(),
        failed: HYBRID_INFERRED_COMMITMENTS_EVENT_FAILED.to_owned(),
    }
}

fn source_kind_for_candidate(kind: CommitmentCandidateKind) -> &'static str {
    match kind {
        CommitmentCandidateKind::Explicit => "post_run_text",
        CommitmentCandidateKind::Inferred => "post_run_inferred_text",
    }
}

fn sanitize_commitment_text(raw: &str) -> String {
    redact_diagnostic_text(raw).split_whitespace().collect::<Vec<_>>().join(" ")
}

fn split_candidate_sentences(text: &str) -> Vec<CandidateSentence<'_>> {
    let mut sentences = Vec::new();
    let mut segment_start = 0;
    for (offset, character) in text.char_indices() {
        if !matches!(character, '\n' | '.' | '!' | '?') {
            continue;
        }
        push_candidate_sentence(text, segment_start, offset, &mut sentences);
        segment_start = offset + character.len_utf8();
    }
    push_candidate_sentence(text, segment_start, text.len(), &mut sentences);
    sentences
}

fn push_candidate_sentence<'a>(
    text: &'a str,
    start: usize,
    end: usize,
    sentences: &mut Vec<CandidateSentence<'a>>,
) {
    let segment = &text[start..end];
    let leading = segment.len().saturating_sub(segment.trim_start().len());
    let trailing = segment.len().saturating_sub(segment.trim_end().len());
    let trimmed_start = start.saturating_add(leading);
    let trimmed_end = end.saturating_sub(trailing);
    if trimmed_start < trimmed_end {
        sentences.push(CandidateSentence {
            text: &text[trimmed_start..trimmed_end],
            start_byte: trimmed_start,
            end_byte: trimmed_end,
        });
    }
}

fn normalize_sentence(sentence: &str) -> String {
    sanitize_commitment_text(sentence).trim_matches(['"', '\'']).to_ascii_lowercase()
}

fn looks_like_commitment(sentence: &str) -> bool {
    const MARKERS: &[&str] = &[
        "i will ",
        "we will ",
        "i'll ",
        "we'll ",
        "remind me",
        "please remind",
        "follow up",
        "send me",
    ];
    MARKERS.iter().any(|marker| sentence.contains(marker))
}

fn has_inferred_commitment_marker(sentence: &str) -> bool {
    const MARKERS: &[&str] =
        &["i should ", "we should ", "i need to ", "we need to ", "let's ", "make sure to "];
    MARKERS.iter().any(|marker| sentence.contains(marker))
}

fn has_negated_inferred_marker(sentence: &str) -> bool {
    const MARKERS: &[&str] = &[
        " should not ",
        " shouldn't ",
        " do not ",
        " don't ",
        " no need to ",
        " not need to ",
        "let's not ",
    ];
    MARKERS.iter().any(|marker| sentence.contains(marker))
}

fn infer_due_condition(sentence: &str) -> serde_json::Value {
    let lower = sentence.to_ascii_lowercase();
    let due_hint = ["tomorrow", "today", "next week"].into_iter().find(|hint| lower.contains(hint));
    match due_hint {
        Some(hint) => json!({ "type": "natural_language", "text": hint }),
        None => json!({ "type": "unspecified" }),
    }
}

fn infer_recurrence(sentence: &str) -> serde_json::Value {
    let lower = sentence.to_ascii_lowercase();
    if lower.contains("every day") || lower.contains("daily") {
        return json!({ "type": "interval", "unit": "day", "every": 1, "review_required": true });
    }
    if lower.contains("every week") || lower.contains("weekly") {
        return json!({ "type": "interval", "unit": "week", "every": 1, "review_required": true });
    }
    if lower.contains("every month") || lower.contains("monthly") {
        return json!({ "type": "interval", "unit": "month", "every": 1, "review_required": true });
    }
    if lower.contains(" every ") {
        return json!({
            "type": "natural_language",
            "text": "recurring",
            "review_required": true,
        });
    }
    json!({ "type": "none" })
}

fn commitment_evidence_span(
    sentence: CandidateSentence<'_>,
    source_text: &str,
) -> CommitmentEvidenceSpanV2 {
    debug_assert!(sentence.end_byte <= source_text.len());
    let redacted = sanitize_commitment_text(sentence.text);
    CommitmentEvidenceSpanV2 {
        start_byte: sentence.start_byte as u64,
        end_byte: sentence.end_byte as u64,
        redacted_text_sha256: sha256_hex(redacted.as_bytes()),
    }
}

fn classify_candidate_sensitivity(sentence: &str) -> CommitmentCandidateSensitivity {
    let redacted = redact_diagnostic_text(sentence);
    if redacted != sentence
        || ["password", "token", "secret", "medical", "health", "bank", "salary"]
            .iter()
            .any(|marker| sentence.to_ascii_lowercase().contains(marker))
    {
        return CommitmentCandidateSensitivity::Sensitive;
    }
    let lower = sentence.to_ascii_lowercase();
    if [" my ", " me ", "family", "home", "address"]
        .iter()
        .any(|marker| format!(" {lower} ").contains(marker))
    {
        CommitmentCandidateSensitivity::Personal
    } else {
        CommitmentCandidateSensitivity::General
    }
}

fn commitment_dedupe_key(
    normalized_action: &str,
    due_condition_json: &str,
    recurrence_json: &str,
) -> String {
    sha256_hex(format!("{normalized_action}\n{due_condition_json}\n{recurrence_json}").as_bytes())
}

fn deterministic_sample_bucket(run_id: &str, source_sha256: &str) -> u64 {
    let digest = Sha256::digest(format!("{run_id}\n{source_sha256}").as_bytes());
    let value = u64::from_be_bytes(
        digest[..8].try_into().expect("SHA-256 prefix always contains eight bytes"),
    );
    value % 10_000
}

fn sha256_hex(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    digest.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn truncate_utf8(text: &str, max_bytes: usize) -> (String, bool) {
    if text.len() <= max_bytes {
        return (text.to_owned(), false);
    }
    let mut end = max_bytes;
    while !text.is_char_boundary(end) {
        end = end.saturating_sub(1);
    }
    (text[..end].to_owned(), true)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extracts_explicit_commitments_and_dedupes() {
        let source =
            "I will send the report tomorrow. I will send the report tomorrow.\nNo action.";

        let extracted = extract_commitments_from_text(source);

        assert_eq!(extracted.len(), 1);
        assert_eq!(extracted[0].normalized_action, "i will send the report tomorrow");
        assert!(extracted[0].due_condition_json.contains("tomorrow"));
        assert_eq!(extracted[0].evidence_span.start_byte, 0);
        assert_eq!(
            extracted[0].evidence_span.end_byte,
            source.find('.').expect("sentence should terminate") as u64
        );
    }

    #[test]
    fn recurring_post_turn_commitment_is_selected_for_review_only() {
        let decision = select_post_turn_commitment_extraction(
            "run-1",
            "I will send the health report every week.",
        );

        assert!(decision.projection.selected);
        assert_eq!(decision.projection.reason_code, "commitment.extraction.high_value");
        let plan = build_commitment_create_plan(
            &CommitmentExtractionInput {
                owner_principal: "user:one".to_owned(),
                device_id: "device".to_owned(),
                channel: Some("cli".to_owned()),
                session_id: Some("session".to_owned()),
                run_id: Some("run-1".to_owned()),
                source_text: decision.source_text,
                extraction_model: None,
                include_inferred: false,
                auxiliary_selection: Some(decision.selection),
            },
            "system:commitment-extractor",
        );

        assert_eq!(plan.requests.len(), 1);
        let request = &plan.requests[0];
        assert_eq!(request.status, "proposed");
        assert_eq!(request.approval_requirement, "manual_review");
        assert_eq!(
            serde_json::from_str::<Value>(request.scheduler_binding_json.as_str())
                .expect("scheduler binding should be JSON")["state"],
            "awaiting_review"
        );
        let candidate = request.candidate_v2.as_ref().expect("V2 candidate should be present");
        assert_eq!(candidate.schema_version, COMMITMENT_CANDIDATE_V2_SCHEMA_VERSION);
        assert_eq!(candidate.sensitivity, CommitmentCandidateSensitivity::Sensitive);
        assert_eq!(
            serde_json::from_str::<Value>(candidate.recurrence_json.as_str())
                .expect("recurrence should be JSON")["type"],
            "interval"
        );
    }

    #[test]
    fn post_turn_selection_and_dedupe_key_are_deterministic() {
        let first = select_post_turn_commitment_extraction("run-stable", "I will send the report.");
        let second =
            select_post_turn_commitment_extraction("run-stable", "I will send the report.");
        assert_eq!(first.projection, second.projection);

        let input = CommitmentExtractionInput {
            owner_principal: "user:one".to_owned(),
            device_id: "device".to_owned(),
            channel: None,
            session_id: None,
            run_id: Some("run-stable".to_owned()),
            source_text: first.source_text.clone(),
            extraction_model: None,
            include_inferred: false,
            auxiliary_selection: Some(first.selection.clone()),
        };
        let first_plan = build_commitment_create_plan(&input, "system:commitment-extractor");
        let second_plan = build_commitment_create_plan(&input, "system:commitment-extractor");
        assert_eq!(
            first_plan.requests[0]
                .candidate_v2
                .as_ref()
                .expect("candidate should be present")
                .dedupe_key,
            second_plan.requests[0]
                .candidate_v2
                .as_ref()
                .expect("candidate should be present")
                .dedupe_key
        );
    }

    #[test]
    fn create_requests_preserve_owner_scope_and_review_status() {
        let input = CommitmentExtractionInput {
            owner_principal: "user:one".to_owned(),
            device_id: "device".to_owned(),
            channel: Some("cli".to_owned()),
            session_id: Some("session".to_owned()),
            run_id: Some("run".to_owned()),
            source_text: "Please remind me next week".to_owned(),
            extraction_model: None,
            include_inferred: false,
            auxiliary_selection: None,
        };

        let requests = build_commitment_create_plan(&input, "user:one").requests;

        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].status, "proposed");
        assert_eq!(requests[0].approval_requirement, "manual_review");
        assert_eq!(requests[0].owner_principal, "user:one");
        let evidence =
            serde_json::from_str::<serde_json::Value>(requests[0].evidence_json.as_str())
                .expect("evidence should be json");
        assert_eq!(
            evidence["acceptance_criteria"]["reason_code"],
            "commitment_acceptance_required"
        );
        assert_eq!(evidence["candidate_kind"], "explicit");
        assert_eq!(
            evidence["acceptance_events"][0],
            "acceptance_criteria_pro_flows_a_commitments.started"
        );
    }

    #[test]
    fn inferred_commitments_are_opt_in_review_candidates() {
        let input = CommitmentExtractionInput {
            owner_principal: "user:one".to_owned(),
            device_id: "device".to_owned(),
            channel: Some("cli".to_owned()),
            session_id: Some("session".to_owned()),
            run_id: Some("run".to_owned()),
            source_text:
                "I will send the report today. We should prepare the release notes tomorrow."
                    .to_owned(),
            extraction_model: None,
            include_inferred: true,
            auxiliary_selection: None,
        };

        let plan = build_commitment_create_plan(&input, "user:one");

        assert_eq!(plan.requests.len(), 2);
        assert_eq!(plan.inference.decision, HybridCommitmentInferenceDecision::Candidate);
        assert_eq!(plan.inference.explicit_candidates, 1);
        assert_eq!(plan.inference.inferred_candidates, 1);
        let inferred = plan
            .requests
            .iter()
            .find(|request| request.source_kind == "post_run_inferred_text")
            .expect("inferred candidate should be present");
        assert_eq!(inferred.status, "proposed");
        assert_eq!(inferred.approval_requirement, "manual_review");
        assert_eq!(inferred.confidence_bps, 4_500);
        assert_eq!(inferred.extraction_model, DEFAULT_INFERENCE_MODEL);
        let evidence = serde_json::from_str::<Value>(inferred.evidence_json.as_str())
            .expect("evidence should be json");
        assert_eq!(evidence["candidate_kind"], "inferred");
        assert_eq!(
            evidence["hybrid_inference"]["event_types"]["started"],
            HYBRID_INFERRED_COMMITMENTS_EVENT_STARTED
        );
        assert_eq!(
            evidence["hybrid_inference"]["reason_code"],
            HybridCommitmentReasonCode::InferredSoftLanguage.as_str()
        );
    }

    #[test]
    fn inferred_commitments_stay_disabled_by_default() {
        let input = CommitmentExtractionInput {
            owner_principal: "user:one".to_owned(),
            device_id: "device".to_owned(),
            channel: None,
            session_id: None,
            run_id: None,
            source_text: "We should prepare the release notes tomorrow.".to_owned(),
            extraction_model: None,
            include_inferred: false,
            auxiliary_selection: None,
        };

        let plan = build_commitment_create_plan(&input, "user:one");

        assert!(plan.requests.is_empty());
        assert_eq!(plan.inference.decision, HybridCommitmentInferenceDecision::Disabled);
        assert_eq!(plan.inference.reason_code, HybridCommitmentReasonCode::InferenceDisabled);
    }

    #[test]
    fn inferred_commitment_projection_round_trips_json() {
        let projection = hybrid_commitment_projection(
            true,
            1,
            2,
            0,
            HybridCommitmentReasonCode::InferredSoftLanguage,
        );

        let encoded = serde_json::to_value(&projection).expect("projection should serialize");
        assert_eq!(encoded["schema_version"], HYBRID_INFERRED_COMMITMENTS_SCHEMA_VERSION);
        assert_eq!(encoded["decision"], "candidate");
        assert_eq!(
            encoded["event_types"]["completed"],
            HYBRID_INFERRED_COMMITMENTS_EVENT_COMPLETED
        );
        assert_eq!(encoded["redaction_level"], HYBRID_INFERRED_COMMITMENTS_REDACTION_LEVEL);
        let decoded: HybridCommitmentInferenceProjection =
            serde_json::from_value(encoded).expect("projection should deserialize");
        assert_eq!(decoded, projection);
    }

    #[test]
    fn inferred_commitments_suppress_overlap_negation_and_secret_text() {
        let explicit_actions = BTreeSet::from(["we should publish the report".to_owned()]);

        let outcome = infer_commitment_candidates_from_text(
            "We should publish the report. We should not email the token. We should rotate token=abc123 tomorrow.",
            &explicit_actions,
        );

        assert_eq!(outcome.candidates.len(), 1);
        assert_eq!(outcome.suppressed_candidates, 2);
        assert_eq!(
            outcome.candidates[0].user_wording,
            "We should rotate token=<redacted> tomorrow"
        );
        assert_eq!(
            outcome.candidates[0].normalized_action,
            "we should rotate token=<redacted> tomorrow"
        );
    }
}
