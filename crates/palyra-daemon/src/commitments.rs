//! Commitment extraction and scheduling bridge helpers.
//!
//! The extractor is deterministic by design. It turns explicit user-language
//! commitments into reviewable ledger rows and keeps delivery/scheduling in a
//! separate audited step.

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};
use serde_json::json;
use ulid::Ulid;

use crate::acceptance::{commitment_acceptance_criteria, commitment_evidence_json};
use crate::journal::CommitmentCreateRequest;

const DEFAULT_EXTRACTION_MODEL: &str = "deterministic.commitment-extractor.v1";
const MAX_EXTRACTED_COMMITMENTS: usize = 20;

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
}

/// Extracts explicit commitments from user-visible text.
pub(crate) fn extract_commitments_from_text(text: &str) -> Vec<ExtractedCommitment> {
    let mut seen = BTreeSet::new();
    let mut commitments = Vec::new();
    for sentence in split_candidate_sentences(text) {
        let normalized = normalize_sentence(sentence);
        if normalized.is_empty() || !looks_like_commitment(normalized.as_str()) {
            continue;
        }
        if !seen.insert(normalized.clone()) {
            continue;
        }
        commitments.push(ExtractedCommitment {
            user_wording: sentence.trim().to_owned(),
            normalized_action: normalized,
            due_condition_json: infer_due_condition(sentence).to_string(),
            recurrence_json: json!({ "type": "none" }).to_string(),
            confidence_bps: 7_500,
            review_reason: "explicit commitment language detected".to_owned(),
        });
        if commitments.len() >= MAX_EXTRACTED_COMMITMENTS {
            break;
        }
    }
    commitments
}

/// Converts extracted candidates into ledger create requests.
pub(crate) fn build_commitment_create_requests(
    input: &CommitmentExtractionInput,
    actor_principal: &str,
) -> Vec<CommitmentCreateRequest> {
    let model =
        input.extraction_model.clone().unwrap_or_else(|| DEFAULT_EXTRACTION_MODEL.to_owned());
    extract_commitments_from_text(input.source_text.as_str())
        .into_iter()
        .map(|candidate| {
            let acceptance = commitment_acceptance_criteria(candidate.normalized_action.as_str());
            CommitmentCreateRequest {
                commitment_id: Ulid::new().to_string(),
                owner_principal: input.owner_principal.clone(),
                device_id: input.device_id.clone(),
                channel: input.channel.clone(),
                session_id: input.session_id.clone(),
                run_id: input.run_id.clone(),
                user_wording: candidate.user_wording.clone(),
                normalized_action: candidate.normalized_action,
                due_condition_json: candidate.due_condition_json,
                recurrence_json: candidate.recurrence_json,
                channel_binding_json: json!({
                    "type": "console_review",
                    "channel": input.channel,
                })
                .to_string(),
                approval_requirement: "manual_review".to_owned(),
                privacy_label: "user_visible".to_owned(),
                status: "proposed".to_owned(),
                confidence_bps: candidate.confidence_bps,
                extraction_model: model.clone(),
                review_reason: candidate.review_reason,
                scheduler_binding_json: json!({
                    "type": "none",
                    "state": "awaiting_review",
                    "acceptance_criteria": acceptance.clone(),
                })
                .to_string(),
                due_at_unix_ms: None,
                source_kind: "post_run_text".to_owned(),
                tape_start_seq: None,
                tape_end_seq: None,
                evidence_json: commitment_evidence_json(
                    candidate.user_wording.clone(),
                    input.session_id.clone(),
                    input.run_id.clone(),
                    acceptance,
                ),
                actor_principal: actor_principal.to_owned(),
            }
        })
        .collect()
}

fn split_candidate_sentences(text: &str) -> impl Iterator<Item = &str> {
    text.split(['\n', '.', '!', '?']).map(str::trim).filter(|sentence| !sentence.is_empty())
}

fn normalize_sentence(sentence: &str) -> String {
    sentence
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .trim_matches(['"', '\''])
        .to_ascii_lowercase()
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

fn infer_due_condition(sentence: &str) -> serde_json::Value {
    let lower = sentence.to_ascii_lowercase();
    let due_hint = ["tomorrow", "today", "next week"].into_iter().find(|hint| lower.contains(hint));
    match due_hint {
        Some(hint) => json!({ "type": "natural_language", "text": hint }),
        None => json!({ "type": "unspecified" }),
    }
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
        };

        let requests = build_commitment_create_requests(&input, "user:one");

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
        assert_eq!(
            evidence["acceptance_events"][0],
            "acceptance_criteria_pro_flows_a_commitments.started"
        );
    }
}
