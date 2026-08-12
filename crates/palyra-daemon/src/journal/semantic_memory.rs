//! Durable semantic-memory candidates, reviewed lifecycle, and recall projection.

use std::collections::BTreeSet;

use rusqlite::{params, OptionalExtension, Transaction, TransactionBehavior};
use serde::Serialize;
use serde_json::json;
use sha2::{Digest, Sha256};
use tempfile::Builder as TempDirBuilder;
use ulid::Ulid;

#[cfg(test)]
use crate::application::semantic_memory::SemanticMemoryRetrievalProjectionV1;
use crate::application::semantic_memory::{
    activate_semantic_memory_candidate, apply_semantic_memory_retrieval_feedback,
    archive_semantic_memory, build_semantic_memory_candidate, derive_semantic_memory_quality_eval,
    mark_semantic_memory_stale, rollback_semantic_memory, semantic_memory_retrieval_projection,
    ConsolidatedMemoryLifecycle, ConsolidatedMemoryRecord, SemanticMemoryCandidateDraftV1,
    SemanticMemoryCandidateV1, SemanticMemoryConsolidationPolicy,
    SemanticMemoryConsolidationRequest, SemanticMemoryError, SemanticMemoryEvidenceRefV1,
    SemanticMemoryHostGate, SemanticMemoryQualityEvalCaseV1,
    SemanticMemoryQualityEvalObservationV1, SemanticMemoryRetrievalFeedbackV1,
};

use super::{
    encode_vector_blob, load_approval_by_id, map_memory_item_row, normalize_embedding_dimensions,
    normalize_memory_tags, sanitize_object_text_field, ApprovalCreateRequest, ApprovalDecision,
    ApprovalDecisionScope, ApprovalPolicySnapshot, ApprovalPromptOption, ApprovalPromptRecord,
    ApprovalRecord, ApprovalRiskLevel, ApprovalSubjectType, JournalConfig, JournalError,
    JournalStore, MemoryItemCreateRequest, MemoryItemRecord, MemorySearchCandidateRecord,
    MemorySearchHit, MemorySearchRequest, MemorySource, CURRENT_MEMORY_EMBEDDING_VERSION,
};

const REVIEW_TIMEOUT_SECONDS: u32 = 900;
const REVIEW_VALIDITY_MS: i64 = 900_000;
const MAX_EVAL_BASELINE_ITEMS: usize = 256;
const EVAL_TOP_K: usize = 5;
const SEMANTIC_MEMORY_TAG: &str = "semantic_memory";

pub(super) const MIGRATION_105_SQL: &str = r#"
    CREATE TABLE semantic_memory_candidates_v1 (
        candidate_id TEXT PRIMARY KEY NOT NULL,
        memory_id TEXT NOT NULL,
        owner_principal TEXT NOT NULL,
        device_id TEXT NOT NULL,
        channel TEXT,
        target_session_id TEXT,
        session_id TEXT NOT NULL,
        run_id TEXT NOT NULL,
        acl_scope TEXT NOT NULL,
        candidate_sha256 TEXT NOT NULL,
        eval_sha256 TEXT NOT NULL,
        candidate_json TEXT NOT NULL,
        lifecycle_state TEXT NOT NULL CHECK (
            lifecycle_state IN ('proposed', 'active', 'superseded', 'rejected')
        ),
        approval_subject_id TEXT NOT NULL,
        approval_policy_sha256 TEXT NOT NULL,
        host_policy_sha256 TEXT NOT NULL,
        expected_previous_record_sha256 TEXT,
        approval_generation INTEGER NOT NULL,
        approval_id TEXT,
        reason_code TEXT NOT NULL,
        created_at_unix_ms INTEGER NOT NULL,
        updated_at_unix_ms INTEGER NOT NULL
    );
    CREATE INDEX idx_semantic_memory_candidates_scope
        ON semantic_memory_candidates_v1(owner_principal, channel, memory_id);

    CREATE TABLE semantic_memory_records_v1 (
        record_sha256 TEXT PRIMARY KEY NOT NULL,
        memory_id TEXT NOT NULL,
        version INTEGER NOT NULL,
        candidate_id TEXT NOT NULL,
        lifecycle_state TEXT NOT NULL CHECK (
            lifecycle_state IN ('active', 'degraded', 'archived', 'rolled_back')
        ),
        record_json TEXT NOT NULL,
        approval_id TEXT,
        projected_memory_ulid TEXT,
        created_at_unix_ms INTEGER NOT NULL,
        UNIQUE(memory_id, version)
    );
    CREATE INDEX idx_semantic_memory_records_lineage
        ON semantic_memory_records_v1(memory_id, version DESC);
    CREATE TRIGGER semantic_memory_records_no_update
        BEFORE UPDATE ON semantic_memory_records_v1
        BEGIN SELECT RAISE(ABORT, 'semantic memory records are immutable'); END;
    CREATE TRIGGER semantic_memory_records_no_delete
        BEFORE DELETE ON semantic_memory_records_v1
        BEGIN SELECT RAISE(ABORT, 'semantic memory records are immutable'); END;

    CREATE TABLE semantic_memory_active_v1 (
        memory_id TEXT PRIMARY KEY NOT NULL,
        record_sha256 TEXT NOT NULL REFERENCES semantic_memory_records_v1(record_sha256),
        projected_memory_ulid TEXT UNIQUE,
        approval_generation INTEGER NOT NULL,
        updated_at_unix_ms INTEGER NOT NULL
    );

    CREATE TABLE semantic_memory_lifecycle_events_v1 (
        event_id INTEGER PRIMARY KEY AUTOINCREMENT,
        memory_id TEXT NOT NULL,
        candidate_id TEXT NOT NULL,
        record_sha256 TEXT NOT NULL,
        previous_record_sha256 TEXT,
        lifecycle_state TEXT NOT NULL,
        approval_id TEXT,
        approval_generation INTEGER,
        eval_sha256 TEXT NOT NULL,
        reason_code TEXT NOT NULL,
        occurred_at_unix_ms INTEGER NOT NULL
    );
    CREATE INDEX idx_semantic_memory_lifecycle
        ON semantic_memory_lifecycle_events_v1(memory_id, event_id DESC);
    CREATE TRIGGER semantic_memory_lifecycle_no_update
        BEFORE UPDATE ON semantic_memory_lifecycle_events_v1
        BEGIN SELECT RAISE(ABORT, 'semantic memory lifecycle is append-only'); END;
    CREATE TRIGGER semantic_memory_lifecycle_no_delete
        BEFORE DELETE ON semantic_memory_lifecycle_events_v1
        BEGIN SELECT RAISE(ABORT, 'semantic memory lifecycle is append-only'); END;

    CREATE TABLE semantic_memory_feedback_events_v1 (
        event_id INTEGER PRIMARY KEY AUTOINCREMENT,
        memory_id TEXT NOT NULL,
        previous_record_sha256 TEXT NOT NULL,
        resulting_record_sha256 TEXT NOT NULL,
        useful INTEGER NOT NULL CHECK (useful IN (0, 1)),
        corrected INTEGER NOT NULL CHECK (corrected IN (0, 1)),
        reason_code TEXT NOT NULL,
        occurred_at_unix_ms INTEGER NOT NULL
    );
    CREATE TRIGGER semantic_memory_feedback_no_update
        BEFORE UPDATE ON semantic_memory_feedback_events_v1
        BEGIN SELECT RAISE(ABORT, 'semantic memory feedback is append-only'); END;
    CREATE TRIGGER semantic_memory_feedback_no_delete
        BEFORE DELETE ON semantic_memory_feedback_events_v1
        BEGIN SELECT RAISE(ABORT, 'semantic memory feedback is append-only'); END;
"#;

/// Exact host identity and policy snapshot bound to one review.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SemanticMemoryReviewAuthority {
    pub(crate) session_id: String,
    pub(crate) run_id: String,
    pub(crate) principal: String,
    pub(crate) device_id: String,
    pub(crate) channel: Option<String>,
    pub(crate) host_policy_sha256: String,
}

/// Recall ACL is independent from the synthetic review session/run.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub(crate) struct SemanticMemoryTargetScope {
    pub(crate) principal: String,
    pub(crate) channel: Option<String>,
    pub(crate) session_id: Option<String>,
}

/// Fenced active pointer observed before proposal or rollback review.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub(crate) struct SemanticMemoryActivationContext {
    pub(crate) approval_generation: u64,
    pub(crate) active_record_sha256: Option<String>,
}

/// Inert candidate plus the exact review fence stored in the journal.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub(crate) struct SemanticMemoryProposedRecord {
    pub(crate) memory_id: String,
    pub(crate) target_scope: SemanticMemoryTargetScope,
    pub(crate) candidate: SemanticMemoryCandidateV1,
    pub(crate) candidate_sha256: String,
    pub(crate) approval_subject_id: String,
    pub(crate) approval_policy_sha256: String,
    pub(crate) context: SemanticMemoryActivationContext,
    pub(crate) reason_code: String,
}

/// Active durable version and its ordinary-memory projection.
#[derive(Debug, Clone, Serialize, PartialEq)]
pub(crate) struct SemanticMemoryActiveRecord {
    pub(crate) candidate_id: String,
    pub(crate) record: ConsolidatedMemoryRecord,
    pub(crate) projected_memory: MemoryItemRecord,
    pub(crate) approval_id: Option<String>,
}

#[derive(Debug)]
struct SemanticMemoryCurrentRecord {
    candidate_id: String,
    target_scope: SemanticMemoryTargetScope,
    record: ConsolidatedMemoryRecord,
    projected_memory: Option<MemoryItemRecord>,
    approval_id: Option<String>,
}

struct LearningRolloutEvent<'a> {
    candidate_id: &'a str,
    principal: &'a str,
    record: &'a ConsolidatedMemoryRecord,
    previous_record_sha256: Option<&'a str>,
    reason_code: &'a str,
    policy_decision: &'a str,
    now_unix_ms: i64,
    rolled_back_at_unix_ms: Option<i64>,
}

struct PreparedSemanticProjection {
    content_text: String,
    content_hash: String,
    tags_json: String,
    confidence: f64,
    ttl_unix_ms: Option<i64>,
    embedding_model: String,
    embedding_dims: i64,
    vector_blob: Vec<u8>,
}

/// Actual hybrid-search result enriched with immutable semantic citations.
#[cfg(test)]
#[derive(Debug, Clone, Serialize, PartialEq)]
pub(crate) struct SemanticMemorySearchHit {
    pub(crate) hit: MemorySearchHit,
    pub(crate) semantic: SemanticMemoryRetrievalProjectionV1,
}

/// Hash-only and low-cardinality operator diagnostics.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub(crate) struct SemanticMemoryDiagnostics {
    pub(crate) proposed_candidates: usize,
    pub(crate) retained_versions: usize,
    pub(crate) active_memories: usize,
    pub(crate) degraded_versions: usize,
    pub(crate) archived_versions: usize,
    pub(crate) latest_reason_code: Option<String>,
    pub(crate) latest_eval_sha256: Option<String>,
}

/// Computes the hashed ACL binding carried by every evidence reference.
#[must_use]
pub(crate) fn semantic_memory_acl_scope(
    principal: &str,
    channel: Option<&str>,
    session_id: Option<&str>,
) -> String {
    format!(
        "acl:v1:p={};c={};s={}",
        digest_text(principal),
        channel.map_or_else(|| "none".to_owned(), digest_text),
        session_id.map_or_else(|| "none".to_owned(), digest_text)
    )
}

/// Binds approval to the exact candidate, eval, pointer, and generation.
#[must_use]
pub(crate) fn semantic_memory_approval_subject(
    memory_id: &str,
    candidate_sha256: &str,
    eval_sha256: &str,
    context: &SemanticMemoryActivationContext,
) -> String {
    format!(
        "semantic-memory:{}:{}:{}:{}:{}",
        digest_text(memory_id),
        candidate_sha256,
        eval_sha256,
        context.active_record_sha256.as_deref().unwrap_or("none"),
        context.approval_generation
    )
}

/// Builds the standard one-shot approval accepted by activation.
#[must_use]
pub(crate) fn semantic_memory_approval_request(
    approval_id: String,
    proposed: &SemanticMemoryProposedRecord,
    authority: &SemanticMemoryReviewAuthority,
) -> ApprovalCreateRequest {
    ApprovalCreateRequest {
        approval_id,
        session_id: authority.session_id.clone(),
        run_id: authority.run_id.clone(),
        principal: authority.principal.clone(),
        device_id: authority.device_id.clone(),
        channel: authority.channel.clone(),
        subject_type: ApprovalSubjectType::Tool,
        subject_id: proposed.approval_subject_id.clone(),
        request_summary: "Review evidence-based semantic memory".to_owned(),
        policy_snapshot: ApprovalPolicySnapshot {
            policy_id: "semantic_memory.host_review.v1".to_owned(),
            policy_hash: proposed.approval_policy_sha256.clone(),
            evaluation_summary:
                "rollout, ACL, provenance, contradiction, retention, and retrieval eval passed"
                    .to_owned(),
        },
        prompt: ApprovalPromptRecord {
            title: "Activate semantic memory".to_owned(),
            risk_level: if proposed.candidate.review_required {
                ApprovalRiskLevel::High
            } else {
                ApprovalRiskLevel::Medium
            },
            subject_id: proposed.approval_subject_id.clone(),
            summary: format!(
                "Activate reviewed memory at generation {}",
                proposed.context.approval_generation
            ),
            options: vec![
                ApprovalPromptOption {
                    option_id: "allow_once".to_owned(),
                    label: "Activate this version".to_owned(),
                    description: "Allow only this evidence and eval digest.".to_owned(),
                    default_selected: false,
                    decision_scope: ApprovalDecisionScope::Once,
                    timebox_ttl_ms: None,
                },
                ApprovalPromptOption {
                    option_id: "deny".to_owned(),
                    label: "Keep candidate inactive".to_owned(),
                    description: "Retain evidence without adding a recall item.".to_owned(),
                    default_selected: true,
                    decision_scope: ApprovalDecisionScope::Once,
                    timebox_ttl_ms: None,
                },
            ],
            timeout_seconds: REVIEW_TIMEOUT_SECONDS,
            details_json: json!({
                "schema_version": 1,
                "memory_id_sha256": digest_text(proposed.memory_id.as_str()),
                "candidate_sha256": proposed.candidate_sha256,
                "eval_sha256": proposed.candidate.quality_eval.evidence_sha256,
                "acl_scope": proposed.candidate.acl_scope,
                "sensitivity": proposed.candidate.sensitivity,
                "confidence_basis_points": proposed.candidate.confidence_basis_points,
                "approval_generation": proposed.context.approval_generation,
                "previous_record_sha256": proposed.context.active_record_sha256,
            })
            .to_string(),
            policy_explanation:
                "Activation requires this exact host policy, principal, eval, and generation."
                    .to_owned(),
        },
    }
}

impl JournalStore {
    /// Evaluates and stores an inert candidate in the semantic and learning views.
    ///
    /// # Errors
    /// Rejects disabled rollout, unsafe content, invalid ACL/provenance, failed
    /// observed retrieval quality, or a stale candidate identifier.
    pub(crate) fn propose_semantic_memory(
        &self,
        memory_id: &str,
        mut draft: SemanticMemoryCandidateDraftV1,
        eval_cases: &[SemanticMemoryQualityEvalCaseV1],
        policy: &SemanticMemoryConsolidationPolicy,
        target_scope: &SemanticMemoryTargetScope,
        authority: &SemanticMemoryReviewAuthority,
    ) -> Result<SemanticMemoryProposedRecord, JournalError> {
        validate_authority(authority)?;
        self.validate_semantic_memory_target_scope(target_scope, authority)?;
        {
            let guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
            validate_review_session_tx(&guard, authority)?;
        }
        let expected_acl = semantic_memory_acl_scope(
            target_scope.principal.as_str(),
            target_scope.channel.as_deref(),
            target_scope.session_id.as_deref(),
        );
        if draft.evidence_refs.iter().any(|evidence| evidence.acl_scope != expected_acl) {
            return Err(reason("semantic_memory.acl_mismatch"));
        }
        draft.summary_text = draft.summary_text.trim().to_owned();
        if sanitize_object_text_field("summary", draft.summary_text.as_str())? != draft.summary_text
        {
            return Err(reason("semantic_memory.summary_redaction_required"));
        }
        validate_reference_redaction(draft.evidence_refs.as_slice())?;
        let quality_eval = self.evaluate_semantic_memory_quality(
            draft.candidate_id.as_str(),
            draft.summary_text.as_str(),
            target_scope,
            eval_cases,
        )?;
        let candidate = build_semantic_memory_candidate(
            SemanticMemoryConsolidationRequest {
                candidate_id: draft.candidate_id,
                summary_text: draft.summary_text,
                evidence_refs: draft.evidence_refs,
                retention_expires_at_unix_ms: draft.retention_expires_at_unix_ms,
                quality_eval,
                created_at_unix_ms: draft.created_at_unix_ms,
            },
            policy,
        )
        .map_err(semantic_error)?;
        if candidate.acl_scope != expected_acl {
            return Err(reason("semantic_memory.acl_mismatch"));
        }
        let candidate_json = serde_json::to_string(&candidate)?;
        let candidate_sha256 =
            domain_digest(b"palyra.semantic-memory.candidate.v1\0", candidate_json.as_bytes());
        let context = self.semantic_memory_activation_context_for_scope(memory_id, target_scope)?;
        let approval_subject_id = semantic_memory_approval_subject(
            memory_id,
            candidate_sha256.as_str(),
            candidate.quality_eval.evidence_sha256.as_str(),
            &context,
        );
        let approval_policy_sha256 = semantic_memory_review_policy_sha256(
            memory_id,
            candidate_sha256.as_str(),
            candidate.quality_eval.evidence_sha256.as_str(),
            &context,
            authority.host_policy_sha256.as_str(),
        );
        let proposed = SemanticMemoryProposedRecord {
            memory_id: memory_id.to_owned(),
            target_scope: target_scope.clone(),
            candidate,
            candidate_sha256,
            approval_subject_id,
            approval_policy_sha256,
            context,
            reason_code: "semantic_memory.host_approval_required".to_owned(),
        };
        self.persist_semantic_memory_proposal(&proposed, authority)?;
        Ok(proposed)
    }

    /// Returns the current generation and digest fence for a memory lineage.
    ///
    /// # Errors
    /// Returns a storage error or rejects an invalid stored generation.
    pub(crate) fn semantic_memory_activation_context_for_scope(
        &self,
        memory_id: &str,
        target_scope: &SemanticMemoryTargetScope,
    ) -> Result<SemanticMemoryActivationContext, JournalError> {
        let guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        let active = guard
            .query_row(
                r#"
                    SELECT active.record_sha256, active.approval_generation,
                           candidates.owner_principal, candidates.channel,
                           candidates.target_session_id
                    FROM semantic_memory_active_v1 AS active
                    INNER JOIN semantic_memory_records_v1 AS records
                        ON records.record_sha256 = active.record_sha256
                    INNER JOIN semantic_memory_candidates_v1 AS candidates
                        ON candidates.candidate_id = records.candidate_id
                    WHERE active.memory_id = ?1
                "#,
                params![memory_id],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, i64>(1)?,
                        SemanticMemoryTargetScope {
                            principal: row.get(2)?,
                            channel: row.get(3)?,
                            session_id: row.get(4)?,
                        },
                    ))
                },
            )
            .optional()?;
        match active {
            Some((digest, generation, stored_scope)) => {
                if stored_scope != *target_scope {
                    return Err(reason("semantic_memory.acl_mismatch"));
                }
                Ok(SemanticMemoryActivationContext {
                    approval_generation: u64::try_from(generation)
                        .ok()
                        .and_then(|value| value.checked_add(1))
                        .ok_or_else(|| reason("semantic_memory.approval_generation_invalid"))?,
                    active_record_sha256: Some(digest),
                })
            }
            None => Ok(SemanticMemoryActivationContext {
                approval_generation: 1,
                active_record_sha256: None,
            }),
        }
    }

    /// Returns the exact sanitized candidate visible to its reviewing principal.
    ///
    /// # Errors
    /// Returns a storage or durable-contract error.
    pub(crate) fn semantic_memory_proposal(
        &self,
        candidate_id: &str,
        target_scope: &SemanticMemoryTargetScope,
    ) -> Result<Option<SemanticMemoryProposedRecord>, JournalError> {
        let guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        let stored = load_proposed_candidate_tx(&guard, candidate_id)?;
        Ok(stored
            .filter(|stored| stored.proposed.target_scope == *target_scope)
            .map(|stored| stored.proposed))
    }

    /// Builds an exact rollback review without changing the current pointer.
    ///
    /// # Errors
    /// Rejects non-current lineages, non-direct targets, or invalid authority.
    pub(crate) fn semantic_memory_rollback_review(
        &self,
        memory_id: &str,
        target_record_sha256: &str,
        target_scope: &SemanticMemoryTargetScope,
        authority: &SemanticMemoryReviewAuthority,
    ) -> Result<SemanticMemoryProposedRecord, JournalError> {
        validate_authority(authority)?;
        self.validate_semantic_memory_target_scope(target_scope, authority)?;
        let now = super::current_unix_ms()?;
        let guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        validate_review_session_tx(&guard, authority)?;
        let current = load_current_semantic_memory_tx(&guard, memory_id, now)?
            .ok_or_else(|| reason("semantic_memory.active_record_missing"))?;
        ensure_current_scope(&current, target_scope)?;
        let (_, target) = guard
            .query_row(
                r#"
                    SELECT candidate_id, record_json
                    FROM semantic_memory_records_v1
                    WHERE record_sha256 = ?1
                "#,
                params![target_record_sha256],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
            )
            .optional()?
            .map(|(candidate_id, record_json)| {
                Ok::<_, JournalError>((
                    candidate_id,
                    serde_json::from_str::<ConsolidatedMemoryRecord>(record_json.as_str())?,
                ))
            })
            .transpose()?
            .ok_or_else(|| reason("semantic_memory.rollback_target_invalid"))?;
        if current.record.previous_record_sha256.as_deref() != Some(target_record_sha256)
            || target.memory_id != memory_id
            || target.acl_scope
                != semantic_memory_acl_scope(
                    target_scope.principal.as_str(),
                    target_scope.channel.as_deref(),
                    target_scope.session_id.as_deref(),
                )
        {
            return Err(reason("semantic_memory.rollback_target_invalid"));
        }
        let context = SemanticMemoryActivationContext {
            approval_generation: current
                .record
                .approval_generation
                .checked_add(1)
                .ok_or_else(|| reason("semantic_memory.approval_generation_invalid"))?,
            active_record_sha256: Some(current.record.record_sha256),
        };
        Ok(rollback_review_projection(memory_id, &target, target_scope, &context, authority))
    }

    /// Atomically consumes exact approval, advances the lineage, and publishes recall.
    ///
    /// # Errors
    /// Rejects missing/stale approval, generation races, or storage failures.
    pub(crate) fn activate_semantic_memory(
        &self,
        candidate_id: &str,
        approval_id: &str,
        target_scope: &SemanticMemoryTargetScope,
        authority: &SemanticMemoryReviewAuthority,
        now_unix_ms: i64,
    ) -> Result<SemanticMemoryActiveRecord, JournalError> {
        validate_authority(authority)?;
        self.validate_semantic_memory_target_scope(target_scope, authority)?;
        if let Some(active) =
            self.active_semantic_memory_by_candidate(candidate_id, target_scope)?
        {
            if active.approval_id.as_deref() == Some(approval_id) {
                return Ok(active);
            }
        }
        let (expected_stored, record) = {
            let guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
            validate_review_session_tx(&guard, authority)?;
            let stored = load_proposed_candidate_tx(&guard, candidate_id)?
                .ok_or_else(|| reason("semantic_memory.proposal_missing_or_stale"))?;
            if stored.authority != *authority {
                return Err(reason("semantic_memory.host_approval_principal_mismatch"));
            }
            if stored.proposed.target_scope != *target_scope {
                return Err(reason("semantic_memory.acl_mismatch"));
            }
            let actual_context =
                active_context_connection(&guard, stored.proposed.memory_id.as_str())?;
            if actual_context != stored.proposed.context {
                return Err(reason("semantic_memory.active_generation_stale"));
            }
            let previous = actual_context
                .active_record_sha256
                .as_deref()
                .map(|digest| load_semantic_record_connection(&guard, digest))
                .transpose()?
                .flatten();
            let gate = SemanticMemoryHostGate {
                host_validated: true,
                policy_approved: true,
                reviewer_approved: true,
                quality_eval_approved: true,
                approval_generation: actual_context.approval_generation,
                activated_at_unix_ms: now_unix_ms,
            };
            let record = activate_semantic_memory_candidate(
                stored.proposed.memory_id.clone(),
                &stored.proposed.candidate,
                &gate,
                previous.as_ref(),
            )
            .map_err(semantic_error)?;
            (stored, record)
        };
        let prepared_projection = prepare_semantic_projection(self, &record)?;
        let mut guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        let transaction = guard.transaction_with_behavior(TransactionBehavior::Immediate)?;
        validate_review_session_tx(&transaction, authority)?;
        let stored = load_proposed_candidate_tx(&transaction, candidate_id)?
            .ok_or_else(|| reason("semantic_memory.proposal_missing_or_stale"))?;
        if stored != expected_stored {
            return Err(reason("semantic_memory.proposal_changed"));
        }
        let actual_context =
            active_context_connection(&transaction, stored.proposed.memory_id.as_str())?;
        if actual_context != stored.proposed.context {
            return Err(reason("semantic_memory.active_generation_stale"));
        }
        let approval = load_approval_by_id(&transaction, approval_id)?
            .ok_or_else(|| reason("semantic_memory.host_approval_missing_or_stale"))?;
        validate_activation_approval(&stored.proposed, authority, &approval, now_unix_ms)?;
        consume_approval_tx(
            &transaction,
            approval_id,
            now_unix_ms,
            "semantic_memory.activation_committed",
        )?;
        let projection_id = semantic_projection_id(record.memory_id.as_str(), record.version);
        remove_active_projection_tx(&transaction, stored.proposed.memory_id.as_str(), now_unix_ms)?;
        insert_semantic_projection_tx(
            &transaction,
            candidate_id,
            projection_id.as_str(),
            &prepared_projection,
            now_unix_ms,
        )?;
        insert_semantic_record_tx(
            &transaction,
            candidate_id,
            &record,
            Some(approval_id),
            Some(projection_id.as_str()),
            now_unix_ms,
        )?;
        transaction.execute(
            r#"
                INSERT INTO semantic_memory_active_v1 (
                    memory_id, record_sha256, projected_memory_ulid,
                    approval_generation, updated_at_unix_ms
                ) VALUES (?1, ?2, ?3, ?4, ?5)
                ON CONFLICT(memory_id) DO UPDATE SET
                    record_sha256 = excluded.record_sha256,
                    projected_memory_ulid = excluded.projected_memory_ulid,
                    approval_generation = excluded.approval_generation,
                    updated_at_unix_ms = excluded.updated_at_unix_ms
            "#,
            params![
                record.memory_id,
                record.record_sha256,
                projection_id,
                i64::try_from(record.approval_generation)
                    .map_err(|_| reason("semantic_memory.approval_generation_invalid"))?,
                now_unix_ms,
            ],
        )?;
        transaction.execute(
            r#"
                UPDATE semantic_memory_candidates_v1
                SET lifecycle_state = 'superseded',
                    reason_code = 'semantic_memory.superseded',
                    updated_at_unix_ms = ?2
                WHERE memory_id = ?1
                  AND lifecycle_state = 'active'
                  AND candidate_id <> ?3
            "#,
            params![record.memory_id, now_unix_ms, candidate_id,],
        )?;
        let activated = transaction.execute(
            r#"
                UPDATE semantic_memory_candidates_v1
                SET lifecycle_state = 'active',
                    approval_id = ?2,
                    reason_code = 'semantic_memory.activated',
                    updated_at_unix_ms = ?3
                WHERE candidate_id = ?1
                  AND lifecycle_state = 'proposed'
            "#,
            params![candidate_id, approval_id, now_unix_ms],
        )?;
        if activated != 1 {
            return Err(reason("semantic_memory.proposal_missing_or_stale"));
        }
        record_lifecycle_tx(
            &transaction,
            candidate_id,
            &record,
            actual_context.active_record_sha256.as_deref(),
            Some(approval_id),
            "semantic_memory.activated",
            now_unix_ms,
        )?;
        accept_learning_candidate_tx(
            &transaction,
            candidate_id,
            authority.principal.as_str(),
            &record,
            actual_context.active_record_sha256.as_deref(),
            now_unix_ms,
        )?;
        transaction.commit()?;
        drop(guard);
        self.active_semantic_memory(stored.proposed.memory_id.as_str(), target_scope)?
            .ok_or_else(|| reason("semantic_memory.activation_projection_missing"))
    }

    /// Returns the active semantic record and ordinary-memory projection.
    ///
    /// # Errors
    /// Returns a storage or durable-contract validation error.
    pub(crate) fn active_semantic_memory(
        &self,
        memory_id: &str,
        target_scope: &SemanticMemoryTargetScope,
    ) -> Result<Option<SemanticMemoryActiveRecord>, JournalError> {
        let now = super::current_unix_ms()?;
        let guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        let Some(current) = load_current_semantic_memory_tx(&guard, memory_id, now)? else {
            return Ok(None);
        };
        ensure_current_scope(&current, target_scope)?;
        if current.record.lifecycle != ConsolidatedMemoryLifecycle::Active {
            return Ok(None);
        }
        let projected_memory = current
            .projected_memory
            .ok_or_else(|| reason("semantic_memory.activation_projection_missing"))?;
        if projected_memory.ttl_unix_ms.is_some_and(|expires_at| expires_at <= now) {
            return Ok(None);
        }
        Ok(Some(SemanticMemoryActiveRecord {
            candidate_id: current.candidate_id,
            record: current.record,
            projected_memory,
            approval_id: current.approval_id,
        }))
    }

    /// Runs actual hybrid retrieval and returns only currently active semantic hits.
    ///
    /// # Errors
    /// Returns a retrieval, storage, or durable-contract error.
    #[cfg(test)]
    pub(crate) fn search_semantic_memory(
        &self,
        request: &MemorySearchRequest,
    ) -> Result<Vec<SemanticMemorySearchHit>, JournalError> {
        let hits = self.search_memory(request)?;
        let guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        let mut semantic_hits = Vec::new();
        for hit in hits {
            let record_json = guard
                .query_row(
                    r#"
                        SELECT records.record_json
                        FROM semantic_memory_active_v1 AS active
                        INNER JOIN semantic_memory_records_v1 AS records
                            ON records.record_sha256 = active.record_sha256
                        WHERE active.projected_memory_ulid = ?1
                          AND records.lifecycle_state = 'active'
                    "#,
                    params![hit.item.memory_id],
                    |row| row.get::<_, String>(0),
                )
                .optional()?;
            let Some(record_json) = record_json else {
                continue;
            };
            let record: ConsolidatedMemoryRecord = serde_json::from_str(record_json.as_str())?;
            let Some(semantic) = semantic_memory_retrieval_projection(&record) else {
                return Err(reason("semantic_memory.active_projection_invalid"));
            };
            semantic_hits.push(SemanticMemorySearchHit { hit, semantic });
        }
        Ok(semantic_hits)
    }

    /// Removes only durable semantic projection IDs before rollout-off scoring.
    ///
    /// User-controlled tags are deliberately ignored because they are not
    /// authoritative provenance.
    ///
    /// # Errors
    /// Returns a storage error.
    pub(crate) fn remove_semantic_memory_candidates(
        &self,
        candidates: &mut Vec<MemorySearchCandidateRecord>,
    ) -> Result<(), JournalError> {
        if candidates.is_empty() {
            return Ok(());
        }
        let guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        let mut statement = guard.prepare(
            r#"
                SELECT EXISTS(
                    SELECT 1
                    FROM semantic_memory_active_v1
                    WHERE projected_memory_ulid = ?1
                )
            "#,
        )?;
        let mut projection_ids = BTreeSet::new();
        for candidate in candidates.iter() {
            if statement.query_row(params![candidate.item.memory_id], |row| row.get::<_, i64>(0))?
                != 0
            {
                projection_ids.insert(candidate.item.memory_id.clone());
            }
        }
        candidates.retain(|candidate| !projection_ids.contains(&candidate.item.memory_id));
        Ok(())
    }

    /// Retains only exact, currently active semantic projection IDs before scoring.
    ///
    /// # Errors
    /// Returns a storage error.
    pub(crate) fn retain_active_semantic_memory_candidates(
        &self,
        candidates: &mut Vec<MemorySearchCandidateRecord>,
    ) -> Result<(), JournalError> {
        if candidates.is_empty() {
            return Ok(());
        }
        let guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        let mut statement = guard.prepare(
            r#"
                SELECT EXISTS(
                    SELECT 1
                    FROM semantic_memory_active_v1 AS active
                    INNER JOIN semantic_memory_records_v1 AS records
                        ON records.record_sha256 = active.record_sha256
                    WHERE active.projected_memory_ulid = ?1
                      AND records.lifecycle_state = 'active'
                )
            "#,
        )?;
        let mut projection_ids = BTreeSet::new();
        for candidate in candidates.iter() {
            if statement.query_row(params![candidate.item.memory_id], |row| row.get::<_, i64>(0))?
                != 0
            {
                projection_ids.insert(candidate.item.memory_id.clone());
            }
        }
        candidates.retain(|candidate| projection_ids.contains(&candidate.item.memory_id));
        Ok(())
    }

    /// Adds exact citation provenance to ordinary memory hits by projection ID.
    ///
    /// # Errors
    /// Returns a storage or durable-contract error.
    pub(crate) fn enrich_semantic_memory_hits(
        &self,
        hits: &mut [MemorySearchHit],
    ) -> Result<(), JournalError> {
        if hits.is_empty() {
            return Ok(());
        }
        let guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        let mut statement = guard.prepare(
            r#"
                SELECT records.record_json
                FROM semantic_memory_active_v1 AS active
                INNER JOIN semantic_memory_records_v1 AS records
                    ON records.record_sha256 = active.record_sha256
                WHERE active.projected_memory_ulid = ?1
                  AND records.lifecycle_state = 'active'
            "#,
        )?;
        for hit in hits {
            let record_json = statement
                .query_row(params![hit.item.memory_id], |row| row.get::<_, String>(0))
                .optional()?;
            let Some(record_json) = record_json else {
                continue;
            };
            let record: ConsolidatedMemoryRecord = serde_json::from_str(record_json.as_str())?;
            hit.semantic = Some(
                semantic_memory_retrieval_projection(&record)
                    .ok_or_else(|| reason("semantic_memory.active_projection_invalid"))?,
            );
        }
        Ok(())
    }

    /// Appends feedback as a new immutable version and removes non-current recall.
    ///
    /// # Errors
    /// Rejects absent active memory, invalid correction evidence, or storage failure.
    pub(crate) fn apply_semantic_memory_feedback(
        &self,
        memory_id: &str,
        target_scope: &SemanticMemoryTargetScope,
        feedback: SemanticMemoryRetrievalFeedbackV1,
    ) -> Result<ConsolidatedMemoryRecord, JournalError> {
        if let Some(correction) = &feedback.correction_evidence_ref {
            validate_reference_redaction(std::slice::from_ref(correction))?;
        }
        let (expected_candidate_id, expected_approval_id, previous_digest, record) = {
            let guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
            let active =
                load_current_semantic_memory_tx(&guard, memory_id, feedback.retrieved_at_unix_ms)?
                    .ok_or_else(|| reason("semantic_memory.active_record_missing"))?;
            ensure_current_scope(&active, target_scope)?;
            if active.record.lifecycle != ConsolidatedMemoryLifecycle::Active
                || active.projected_memory.is_none()
            {
                return Err(reason("semantic_memory.active_record_missing"));
            }
            let previous_digest = active.record.record_sha256.clone();
            let mut record = active.record;
            apply_semantic_memory_retrieval_feedback(&mut record, feedback.clone())
                .map_err(semantic_error)?;
            (active.candidate_id, active.approval_id, previous_digest, record)
        };
        let prepared_projection = if record.lifecycle == ConsolidatedMemoryLifecycle::Active {
            Some(prepare_semantic_projection(self, &record)?)
        } else {
            None
        };
        let mut guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        let transaction = guard.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let active = load_current_semantic_memory_tx(
            &transaction,
            memory_id,
            feedback.retrieved_at_unix_ms,
        )?
        .ok_or_else(|| reason("semantic_memory.active_record_missing"))?;
        ensure_current_scope(&active, target_scope)?;
        if active.record.record_sha256 != previous_digest
            || active.candidate_id != expected_candidate_id
            || active.approval_id != expected_approval_id
            || active.record.lifecycle != ConsolidatedMemoryLifecycle::Active
            || active.projected_memory.is_none()
        {
            return Err(reason("semantic_memory.active_generation_stale"));
        }
        let remains_active = record.lifecycle == ConsolidatedMemoryLifecycle::Active;
        remove_active_projection_tx(&transaction, memory_id, feedback.retrieved_at_unix_ms)?;
        let projection_id = if remains_active {
            let projection_id = semantic_projection_id(memory_id, record.version);
            insert_semantic_projection_tx(
                &transaction,
                active.candidate_id.as_str(),
                projection_id.as_str(),
                prepared_projection
                    .as_ref()
                    .ok_or_else(|| reason("semantic_memory.activation_projection_missing"))?,
                feedback.retrieved_at_unix_ms,
            )?;
            Some(projection_id)
        } else {
            None
        };
        insert_semantic_record_tx(
            &transaction,
            active.candidate_id.as_str(),
            &record,
            active.approval_id.as_deref(),
            projection_id.as_deref(),
            feedback.retrieved_at_unix_ms,
        )?;
        if let Some(projection_id) = &projection_id {
            transaction.execute(
                r#"
                    INSERT INTO semantic_memory_active_v1 (
                        memory_id, record_sha256, projected_memory_ulid,
                        approval_generation, updated_at_unix_ms
                    ) VALUES (?1, ?2, ?3, ?4, ?5)
                    ON CONFLICT(memory_id) DO UPDATE SET
                        record_sha256 = excluded.record_sha256,
                        projected_memory_ulid = excluded.projected_memory_ulid,
                        approval_generation = excluded.approval_generation,
                        updated_at_unix_ms = excluded.updated_at_unix_ms
                "#,
                params![
                    memory_id,
                    record.record_sha256,
                    projection_id,
                    i64::try_from(record.approval_generation)
                        .map_err(|_| reason("semantic_memory.approval_generation_invalid"))?,
                    feedback.retrieved_at_unix_ms,
                ],
            )?;
        } else {
            update_current_pointer_tx(
                &transaction,
                memory_id,
                record.record_sha256.as_str(),
                None,
                record.approval_generation,
                feedback.retrieved_at_unix_ms,
            )?;
        }
        transaction.execute(
            r#"
                INSERT INTO semantic_memory_feedback_events_v1 (
                    memory_id, previous_record_sha256, resulting_record_sha256,
                    useful, corrected, reason_code, occurred_at_unix_ms
                ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
            "#,
            params![
                memory_id,
                previous_digest,
                record.record_sha256,
                i64::from(feedback.useful),
                i64::from(feedback.corrected),
                record.reason_code,
                feedback.retrieved_at_unix_ms,
            ],
        )?;
        record_lifecycle_tx(
            &transaction,
            active.candidate_id.as_str(),
            &record,
            Some(previous_digest.as_str()),
            active.approval_id.as_deref(),
            record.reason_code.as_str(),
            feedback.retrieved_at_unix_ms,
        )?;
        record_learning_rollout_event_tx(
            &transaction,
            LearningRolloutEvent {
                candidate_id: active.candidate_id.as_str(),
                principal: active.target_scope.principal.as_str(),
                record: &record,
                previous_record_sha256: Some(previous_digest.as_str()),
                reason_code: record.reason_code.as_str(),
                policy_decision: "server_feedback",
                now_unix_ms: feedback.retrieved_at_unix_ms,
                rolled_back_at_unix_ms: None,
            },
        )?;
        transaction.commit()?;
        Ok(record)
    }

    /// Evaluates staleness and removes a stale active projection atomically.
    ///
    /// # Errors
    /// Rejects absent active memory, invalid time policy, or storage failure.
    pub(crate) fn mark_semantic_memory_stale_durable(
        &self,
        memory_id: &str,
        target_scope: &SemanticMemoryTargetScope,
        observed_at_unix_ms: i64,
        max_age_ms: i64,
    ) -> Result<bool, JournalError> {
        let mut guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        let transaction = guard.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let active = load_current_semantic_memory_tx(&transaction, memory_id, observed_at_unix_ms)?
            .ok_or_else(|| reason("semantic_memory.active_record_missing"))?;
        ensure_current_scope(&active, target_scope)?;
        if active.record.lifecycle != ConsolidatedMemoryLifecycle::Active
            || active.projected_memory.is_none()
        {
            return Err(reason("semantic_memory.active_record_missing"));
        }
        let mut record = active.record;
        let previous_digest = record.record_sha256.clone();
        if !mark_semantic_memory_stale(&mut record, observed_at_unix_ms, max_age_ms)
            .map_err(semantic_error)?
        {
            return Ok(false);
        }
        remove_active_projection_tx(&transaction, memory_id, observed_at_unix_ms)?;
        insert_semantic_record_tx(
            &transaction,
            active.candidate_id.as_str(),
            &record,
            active.approval_id.as_deref(),
            None,
            observed_at_unix_ms,
        )?;
        update_current_pointer_tx(
            &transaction,
            memory_id,
            record.record_sha256.as_str(),
            None,
            record.approval_generation,
            observed_at_unix_ms,
        )?;
        record_lifecycle_tx(
            &transaction,
            active.candidate_id.as_str(),
            &record,
            Some(previous_digest.as_str()),
            active.approval_id.as_deref(),
            "semantic_memory.stale",
            observed_at_unix_ms,
        )?;
        record_learning_rollout_event_tx(
            &transaction,
            LearningRolloutEvent {
                candidate_id: active.candidate_id.as_str(),
                principal: active.target_scope.principal.as_str(),
                record: &record,
                previous_record_sha256: Some(previous_digest.as_str()),
                reason_code: "semantic_memory.stale",
                policy_decision: "server_staleness_policy",
                now_unix_ms: observed_at_unix_ms,
                rolled_back_at_unix_ms: None,
            },
        )?;
        transaction.commit()?;
        Ok(true)
    }

    /// Archives active semantic memory without deleting lineage or evidence.
    ///
    /// # Errors
    /// Rejects absent active memory, invalid timestamp, or storage failure.
    pub(crate) fn archive_semantic_memory_durable(
        &self,
        memory_id: &str,
        target_scope: &SemanticMemoryTargetScope,
        archived_at_unix_ms: i64,
    ) -> Result<ConsolidatedMemoryRecord, JournalError> {
        let mut guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        let transaction = guard.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let active = load_current_semantic_memory_tx(&transaction, memory_id, archived_at_unix_ms)?
            .ok_or_else(|| reason("semantic_memory.active_record_missing"))?;
        ensure_current_scope(&active, target_scope)?;
        if active.record.lifecycle != ConsolidatedMemoryLifecycle::Active
            || active.projected_memory.is_none()
        {
            return Err(reason("semantic_memory.active_record_missing"));
        }
        let mut record = active.record;
        let previous_digest = record.record_sha256.clone();
        archive_semantic_memory(&mut record, archived_at_unix_ms).map_err(semantic_error)?;
        remove_active_projection_tx(&transaction, memory_id, archived_at_unix_ms)?;
        insert_semantic_record_tx(
            &transaction,
            active.candidate_id.as_str(),
            &record,
            active.approval_id.as_deref(),
            None,
            archived_at_unix_ms,
        )?;
        update_current_pointer_tx(
            &transaction,
            memory_id,
            record.record_sha256.as_str(),
            None,
            record.approval_generation,
            archived_at_unix_ms,
        )?;
        record_lifecycle_tx(
            &transaction,
            active.candidate_id.as_str(),
            &record,
            Some(previous_digest.as_str()),
            active.approval_id.as_deref(),
            "semantic_memory.archived",
            archived_at_unix_ms,
        )?;
        record_learning_rollout_event_tx(
            &transaction,
            LearningRolloutEvent {
                candidate_id: active.candidate_id.as_str(),
                principal: active.target_scope.principal.as_str(),
                record: &record,
                previous_record_sha256: Some(previous_digest.as_str()),
                reason_code: "semantic_memory.archived",
                policy_decision: "host_requested",
                now_unix_ms: archived_at_unix_ms,
                rolled_back_at_unix_ms: None,
            },
        )?;
        transaction.commit()?;
        Ok(record)
    }

    /// Rolls back to the direct prior record with a second exact one-shot approval.
    ///
    /// # Errors
    /// Rejects stale target/current pointers, invalid approval, or storage failure.
    pub(crate) fn rollback_semantic_memory_durable(
        &self,
        memory_id: &str,
        target_record_sha256: &str,
        approval_id: &str,
        target_scope: &SemanticMemoryTargetScope,
        authority: &SemanticMemoryReviewAuthority,
        now_unix_ms: i64,
    ) -> Result<SemanticMemoryActiveRecord, JournalError> {
        validate_authority(authority)?;
        self.validate_semantic_memory_target_scope(target_scope, authority)?;
        let (
            expected_current_digest,
            expected_current_candidate_id,
            target_candidate_id,
            target_digest,
            context,
            proposed,
            record,
        ) = {
            let guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
            validate_review_session_tx(&guard, authority)?;
            let current = load_current_semantic_memory_tx(&guard, memory_id, now_unix_ms)?
                .ok_or_else(|| reason("semantic_memory.active_record_missing"))?;
            ensure_current_scope(&current, target_scope)?;
            let (target_candidate_id, target) =
                load_semantic_record_with_candidate_connection(&guard, target_record_sha256)?
                    .ok_or_else(|| reason("semantic_memory.rollback_target_invalid"))?;
            if target.acl_scope
                != semantic_memory_acl_scope(
                    target_scope.principal.as_str(),
                    target_scope.channel.as_deref(),
                    target_scope.session_id.as_deref(),
                )
            {
                return Err(reason("semantic_memory.acl_mismatch"));
            }
            let context = SemanticMemoryActivationContext {
                approval_generation: current
                    .record
                    .approval_generation
                    .checked_add(1)
                    .ok_or_else(|| reason("semantic_memory.approval_generation_invalid"))?,
                active_record_sha256: Some(current.record.record_sha256.clone()),
            };
            let proposed =
                rollback_review_projection(memory_id, &target, target_scope, &context, authority);
            let gate = SemanticMemoryHostGate {
                host_validated: true,
                policy_approved: true,
                reviewer_approved: true,
                quality_eval_approved: true,
                approval_generation: context.approval_generation,
                activated_at_unix_ms: now_unix_ms,
            };
            let record = rollback_semantic_memory(&current.record, &target, &gate)
                .map_err(semantic_error)?;
            (
                current.record.record_sha256,
                current.candidate_id,
                target_candidate_id,
                target.record_sha256,
                context,
                proposed,
                record,
            )
        };
        let prepared_projection = prepare_semantic_projection(self, &record)?;
        let mut guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        let transaction = guard.transaction_with_behavior(TransactionBehavior::Immediate)?;
        validate_review_session_tx(&transaction, authority)?;
        let current = load_current_semantic_memory_tx(&transaction, memory_id, now_unix_ms)?
            .ok_or_else(|| reason("semantic_memory.active_record_missing"))?;
        ensure_current_scope(&current, target_scope)?;
        if current.record.record_sha256 != expected_current_digest
            || current.candidate_id != expected_current_candidate_id
        {
            return Err(reason("semantic_memory.active_generation_stale"));
        }
        let (observed_target_candidate_id, target) =
            load_semantic_record_with_candidate_connection(&transaction, target_record_sha256)?
                .ok_or_else(|| reason("semantic_memory.rollback_target_invalid"))?;
        if observed_target_candidate_id != target_candidate_id
            || target.record_sha256 != target_digest
        {
            return Err(reason("semantic_memory.rollback_target_invalid"));
        }
        let approval = load_approval_by_id(&transaction, approval_id)?
            .ok_or_else(|| reason("semantic_memory.host_approval_missing_or_stale"))?;
        validate_activation_approval(&proposed, authority, &approval, now_unix_ms)?;
        consume_approval_tx(
            &transaction,
            approval_id,
            now_unix_ms,
            "semantic_memory.rollback_committed",
        )?;
        let gate = SemanticMemoryHostGate {
            host_validated: true,
            policy_approved: true,
            reviewer_approved: true,
            quality_eval_approved: true,
            approval_generation: context.approval_generation,
            activated_at_unix_ms: now_unix_ms,
        };
        let observed_record =
            rollback_semantic_memory(&current.record, &target, &gate).map_err(semantic_error)?;
        if observed_record != record {
            return Err(reason("semantic_memory.active_generation_stale"));
        }
        remove_active_projection_tx(&transaction, memory_id, now_unix_ms)?;
        let projection_id = semantic_projection_id(memory_id, record.version);
        insert_semantic_projection_tx(
            &transaction,
            target_candidate_id.as_str(),
            projection_id.as_str(),
            &prepared_projection,
            now_unix_ms,
        )?;
        insert_semantic_record_tx(
            &transaction,
            target_candidate_id.as_str(),
            &record,
            Some(approval_id),
            Some(projection_id.as_str()),
            now_unix_ms,
        )?;
        transaction.execute(
            r#"
                INSERT INTO semantic_memory_active_v1 (
                    memory_id, record_sha256, projected_memory_ulid,
                    approval_generation, updated_at_unix_ms
                ) VALUES (?1, ?2, ?3, ?4, ?5)
                ON CONFLICT(memory_id) DO UPDATE SET
                    record_sha256 = excluded.record_sha256,
                    projected_memory_ulid = excluded.projected_memory_ulid,
                    approval_generation = excluded.approval_generation,
                    updated_at_unix_ms = excluded.updated_at_unix_ms
            "#,
            params![
                memory_id,
                record.record_sha256,
                projection_id,
                i64::try_from(record.approval_generation)
                    .map_err(|_| reason("semantic_memory.approval_generation_invalid"))?,
                now_unix_ms,
            ],
        )?;
        transaction.execute(
            r#"
                UPDATE semantic_memory_candidates_v1
                SET lifecycle_state = 'superseded',
                    reason_code = 'semantic_memory.rollback_superseded',
                    updated_at_unix_ms = ?2
                WHERE memory_id = ?1
                  AND lifecycle_state = 'active'
                  AND candidate_id <> ?3
            "#,
            params![memory_id, now_unix_ms, target_candidate_id],
        )?;
        transaction.execute(
            r#"
                UPDATE semantic_memory_candidates_v1
                SET lifecycle_state = 'active',
                    approval_id = ?2,
                    reason_code = 'semantic_memory.rollback_activated',
                    updated_at_unix_ms = ?3
                WHERE candidate_id = ?1
            "#,
            params![target_candidate_id, approval_id, now_unix_ms],
        )?;
        record_lifecycle_tx(
            &transaction,
            target_candidate_id.as_str(),
            &record,
            Some(current.record.record_sha256.as_str()),
            Some(approval_id),
            "semantic_memory.rollback_activated",
            now_unix_ms,
        )?;
        record_learning_rollout_event_tx(
            &transaction,
            LearningRolloutEvent {
                candidate_id: target_candidate_id.as_str(),
                principal: target_scope.principal.as_str(),
                record: &record,
                previous_record_sha256: Some(current.record.record_sha256.as_str()),
                reason_code: "semantic_memory.rollback_activated",
                policy_decision: "host_approved",
                now_unix_ms,
                rolled_back_at_unix_ms: Some(now_unix_ms),
            },
        )?;
        transaction.commit()?;
        drop(guard);
        self.active_semantic_memory(memory_id, target_scope)?
            .ok_or_else(|| reason("semantic_memory.activation_projection_missing"))
    }

    /// Returns bounded, redacted diagnostics without candidate or evidence text.
    ///
    /// # Errors
    /// Returns a storage error.
    pub(crate) fn semantic_memory_diagnostics(
        &self,
        target_scope: &SemanticMemoryTargetScope,
    ) -> Result<SemanticMemoryDiagnostics, JournalError> {
        let guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        let proposed_candidates = count_scoped_tx(
            &guard,
            r#"
                SELECT COUNT(*)
                FROM semantic_memory_candidates_v1 AS candidates
                WHERE candidates.lifecycle_state = 'proposed'
                  AND candidates.owner_principal = ?1
                  AND ((?2 IS NULL AND candidates.channel IS NULL)
                    OR candidates.channel = ?2)
                  AND ((?3 IS NULL AND candidates.target_session_id IS NULL)
                    OR candidates.target_session_id = ?3)
            "#,
            target_scope,
        )?;
        let retained_versions = count_scoped_tx(
            &guard,
            r#"
                SELECT COUNT(*)
                FROM semantic_memory_records_v1 AS records
                INNER JOIN semantic_memory_candidates_v1 AS candidates
                    ON candidates.candidate_id = records.candidate_id
                WHERE candidates.owner_principal = ?1
                  AND ((?2 IS NULL AND candidates.channel IS NULL)
                    OR candidates.channel = ?2)
                  AND ((?3 IS NULL AND candidates.target_session_id IS NULL)
                    OR candidates.target_session_id = ?3)
            "#,
            target_scope,
        )?;
        let active_memories = count_scoped_at_tx(
            &guard,
            r#"
                SELECT COUNT(*)
                FROM semantic_memory_active_v1 AS active
                INNER JOIN semantic_memory_records_v1 AS records
                    ON records.record_sha256 = active.record_sha256
                INNER JOIN semantic_memory_candidates_v1 AS candidates
                    ON candidates.candidate_id = records.candidate_id
                INNER JOIN memory_items AS memory
                    ON memory.memory_ulid = active.projected_memory_ulid
                WHERE records.lifecycle_state = 'active'
                  AND active.projected_memory_ulid IS NOT NULL
                  AND (memory.ttl_unix_ms IS NULL OR memory.ttl_unix_ms > ?4)
                  AND candidates.owner_principal = ?1
                  AND ((?2 IS NULL AND candidates.channel IS NULL)
                    OR candidates.channel = ?2)
                  AND ((?3 IS NULL AND candidates.target_session_id IS NULL)
                    OR candidates.target_session_id = ?3)
            "#,
            target_scope,
            super::current_unix_ms()?,
        )?;
        let degraded_versions = count_scoped_tx(
            &guard,
            r#"
                SELECT COUNT(*)
                FROM semantic_memory_records_v1 AS records
                INNER JOIN semantic_memory_candidates_v1 AS candidates
                    ON candidates.candidate_id = records.candidate_id
                WHERE records.lifecycle_state = 'degraded'
                  AND candidates.owner_principal = ?1
                  AND ((?2 IS NULL AND candidates.channel IS NULL)
                    OR candidates.channel = ?2)
                  AND ((?3 IS NULL AND candidates.target_session_id IS NULL)
                    OR candidates.target_session_id = ?3)
            "#,
            target_scope,
        )?;
        let archived_versions = count_scoped_tx(
            &guard,
            r#"
                SELECT COUNT(*)
                FROM semantic_memory_records_v1 AS records
                INNER JOIN semantic_memory_candidates_v1 AS candidates
                    ON candidates.candidate_id = records.candidate_id
                WHERE records.lifecycle_state = 'archived'
                  AND candidates.owner_principal = ?1
                  AND ((?2 IS NULL AND candidates.channel IS NULL)
                    OR candidates.channel = ?2)
                  AND ((?3 IS NULL AND candidates.target_session_id IS NULL)
                    OR candidates.target_session_id = ?3)
            "#,
            target_scope,
        )?;
        let latest = guard
            .query_row(
                r#"
                    SELECT events.reason_code, events.eval_sha256
                    FROM semantic_memory_lifecycle_events_v1 AS events
                    INNER JOIN semantic_memory_candidates_v1 AS candidates
                        ON candidates.candidate_id = events.candidate_id
                    WHERE candidates.owner_principal = ?1
                      AND ((?2 IS NULL AND candidates.channel IS NULL)
                        OR candidates.channel = ?2)
                      AND ((?3 IS NULL AND candidates.target_session_id IS NULL)
                        OR candidates.target_session_id = ?3)
                    ORDER BY events.event_id DESC
                    LIMIT 1
                "#,
                params![target_scope.principal, target_scope.channel, target_scope.session_id],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
            )
            .optional()?;
        Ok(SemanticMemoryDiagnostics {
            proposed_candidates,
            retained_versions,
            active_memories,
            degraded_versions,
            archived_versions,
            latest_reason_code: latest.as_ref().map(|(reason, _)| reason.clone()),
            latest_eval_sha256: latest.map(|(_, eval)| eval),
        })
    }

    fn evaluate_semantic_memory_quality(
        &self,
        candidate_id: &str,
        summary_text: &str,
        target_scope: &SemanticMemoryTargetScope,
        cases: &[SemanticMemoryQualityEvalCaseV1],
    ) -> Result<crate::application::semantic_memory::SemanticMemoryQualityEvalV1, JournalError>
    {
        if cases.len() < 10 || cases.len() > 64 {
            return Err(reason("semantic_memory.quality_eval_evidence_invalid"));
        }
        let candidate_memory_id = semantic_eval_projection_id(candidate_id);
        let baseline_items = self.accessible_memory_items_for_eval(target_scope)?;
        let mut baseline_hits = Vec::with_capacity(cases.len());
        for case in cases {
            baseline_hits.push(self.search_memory(&MemorySearchRequest {
                principal: target_scope.principal.clone(),
                channel: target_scope.channel.clone(),
                session_id: target_scope.session_id.clone(),
                query: case.query.clone(),
                top_k: EVAL_TOP_K,
                min_score: 0.0,
                tags: Vec::new(),
                sources: Vec::new(),
            })?);
        }

        let temp = TempDirBuilder::new()
            .prefix("palyra-semantic-eval-")
            .tempdir()
            .map_err(|_| reason("semantic_memory.quality_eval_runtime_failed"))?;
        let mut config: JournalConfig = self.config.clone();
        config.db_path = temp.path().join("shadow.sqlite3");
        config.hash_chain_enabled = false;
        let shadow = JournalStore::open_with_memory_embedding_runtime(
            config,
            self.memory_embedding_provider.clone(),
            self.memory_embedding_runtime.clone(),
        )?;
        for item in baseline_items {
            shadow.create_memory_item(&MemoryItemCreateRequest {
                memory_id: item.memory_id,
                principal: item.principal,
                channel: item.channel,
                session_id: item.session_id,
                source: item.source,
                content_text: item.content_text,
                tags: item.tags,
                confidence: item.confidence,
                ttl_unix_ms: item.ttl_unix_ms,
            })?;
        }
        shadow.create_memory_item(&MemoryItemCreateRequest {
            memory_id: candidate_memory_id.clone(),
            principal: target_scope.principal.clone(),
            channel: target_scope.channel.clone(),
            session_id: target_scope.session_id.clone(),
            source: MemorySource::Summary,
            content_text: summary_text.to_owned(),
            tags: vec![SEMANTIC_MEMORY_TAG.to_owned()],
            confidence: Some(1.0),
            ttl_unix_ms: None,
        })?;
        let mut observations = Vec::with_capacity(cases.len());
        for (case, baseline) in cases.iter().zip(baseline_hits) {
            let consolidated = shadow.search_memory(&MemorySearchRequest {
                principal: target_scope.principal.clone(),
                channel: target_scope.channel.clone(),
                session_id: target_scope.session_id.clone(),
                query: case.query.clone(),
                top_k: EVAL_TOP_K,
                min_score: 0.0,
                tags: Vec::new(),
                sources: Vec::new(),
            })?;
            observations.push(SemanticMemoryQualityEvalObservationV1 {
                case_id: case.case_id.clone(),
                baseline_memory_ids: baseline.into_iter().map(|hit| hit.item.memory_id).collect(),
                consolidated_memory_ids: consolidated
                    .into_iter()
                    .map(|hit| hit.item.memory_id)
                    .collect(),
            });
        }
        drop(shadow);
        drop(temp);
        let report =
            derive_semantic_memory_quality_eval(&candidate_memory_id, cases, &observations)
                .map_err(semantic_error)?;
        if !report.qualifies() {
            return Err(reason("semantic_memory.quality_eval_failed"));
        }
        Ok(report)
    }

    fn validate_semantic_memory_target_scope(
        &self,
        target_scope: &SemanticMemoryTargetScope,
        authority: &SemanticMemoryReviewAuthority,
    ) -> Result<(), JournalError> {
        if target_scope.principal.trim().is_empty()
            || target_scope.principal != authority.principal
            || target_scope.channel != authority.channel
            || target_scope.principal.len() > 256
            || target_scope
                .channel
                .as_ref()
                .is_some_and(|value| value.trim().is_empty() || value.len() > 128)
            || target_scope
                .session_id
                .as_ref()
                .is_some_and(|value| value.trim().is_empty() || value.len() > 128)
        {
            return Err(reason("semantic_memory.acl_mismatch"));
        }
        let Some(session_id) = target_scope.session_id.as_deref() else {
            return Ok(());
        };
        let guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        let owned = guard.query_row(
            r#"
                SELECT EXISTS(
                    SELECT 1
                    FROM orchestrator_sessions
                    WHERE session_ulid = ?1
                      AND principal = ?2
                      AND device_id = ?3
                      AND ((?4 IS NULL AND channel IS NULL) OR channel = ?4)
                )
            "#,
            params![session_id, target_scope.principal, authority.device_id, target_scope.channel],
            |row| row.get::<_, i64>(0),
        )?;
        if owned == 0 {
            return Err(reason("semantic_memory.acl_mismatch"));
        }
        Ok(())
    }

    fn accessible_memory_items_for_eval(
        &self,
        target_scope: &SemanticMemoryTargetScope,
    ) -> Result<Vec<MemoryItemRecord>, JournalError> {
        let now = super::current_unix_ms()?;
        self.purge_expired_memory_items(now)?;
        let guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        let mut statement = guard.prepare(
            r#"
                SELECT
                    memory_ulid, principal, channel, session_ulid, source,
                    content_text, content_hash, tags_json, confidence, ttl_unix_ms,
                    created_at_unix_ms, updated_at_unix_ms
                FROM memory_items
                WHERE principal = ?1
                  AND (
                    (session_ulid = ?3 AND ((?2 IS NULL AND channel IS NULL)
                        OR (?2 IS NOT NULL AND (channel = ?2 OR channel IS NULL))))
                    OR
                    (session_ulid IS NULL AND ((?2 IS NULL AND channel IS NULL)
                        OR (?2 IS NOT NULL AND (channel = ?2 OR channel IS NULL))))
                  )
                  AND (ttl_unix_ms IS NULL OR ttl_unix_ms > ?4)
                ORDER BY memory_ulid ASC
                LIMIT ?5
            "#,
        )?;
        let mut rows = statement.query(params![
            target_scope.principal,
            target_scope.channel,
            target_scope.session_id,
            now,
            MAX_EVAL_BASELINE_ITEMS as i64,
        ])?;
        let mut items = Vec::new();
        while let Some(row) = rows.next()? {
            items.push(map_memory_item_row(row)?);
        }
        Ok(items)
    }

    fn persist_semantic_memory_proposal(
        &self,
        proposed: &SemanticMemoryProposedRecord,
        authority: &SemanticMemoryReviewAuthority,
    ) -> Result<(), JournalError> {
        let now = super::current_unix_ms()?;
        let candidate_json = serde_json::to_string(&proposed.candidate)?;
        let risk_level = if proposed.candidate.review_required { "high" } else { "medium" };
        let content_json = candidate_json.clone();
        let provenance_json = json!({
            "schema_version": 1,
            "candidate_sha256": proposed.candidate_sha256,
            "eval_sha256": proposed.candidate.quality_eval.evidence_sha256,
            "approval_subject_id": proposed.approval_subject_id,
            "reason_code": proposed.reason_code,
        })
        .to_string();
        let mut guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        let transaction = guard.transaction_with_behavior(TransactionBehavior::Immediate)?;
        validate_review_session_tx(&transaction, authority)?;
        if let Some((digest, subject, state)) = transaction
            .query_row(
                r#"
                    SELECT candidate_sha256, approval_subject_id, lifecycle_state
                    FROM semantic_memory_candidates_v1
                    WHERE candidate_id = ?1
                "#,
                params![proposed.candidate.candidate_id],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                    ))
                },
            )
            .optional()?
        {
            if digest == proposed.candidate_sha256
                && subject == proposed.approval_subject_id
                && state == "proposed"
            {
                return Ok(());
            }
            return Err(reason("semantic_memory.proposal_id_conflict"));
        }
        transaction.execute(
            r#"
                INSERT INTO semantic_memory_candidates_v1 (
                    candidate_id, memory_id, owner_principal, device_id, channel,
                    target_session_id, session_id, run_id, acl_scope,
                    candidate_sha256, eval_sha256,
                    candidate_json, lifecycle_state, approval_subject_id,
                    approval_policy_sha256, host_policy_sha256,
                    expected_previous_record_sha256, approval_generation,
                    approval_id, reason_code, created_at_unix_ms, updated_at_unix_ms
                ) VALUES (
                    ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, 'proposed',
                    ?13, ?14, ?15, ?16, ?17, NULL,
                    'semantic_memory.host_approval_required', ?18, ?18
                )
            "#,
            params![
                proposed.candidate.candidate_id,
                proposed.memory_id,
                authority.principal,
                authority.device_id,
                authority.channel,
                proposed.target_scope.session_id,
                authority.session_id,
                authority.run_id,
                proposed.candidate.acl_scope,
                proposed.candidate_sha256,
                proposed.candidate.quality_eval.evidence_sha256,
                candidate_json,
                proposed.approval_subject_id,
                proposed.approval_policy_sha256,
                authority.host_policy_sha256,
                proposed.context.active_record_sha256,
                i64::try_from(proposed.context.approval_generation)
                    .map_err(|_| reason("semantic_memory.approval_generation_invalid"))?,
                now,
            ],
        )?;
        transaction.execute(
            r#"
                INSERT INTO learning_candidates (
                    candidate_ulid, candidate_kind, session_ulid, run_ulid,
                    owner_principal, device_id, channel, scope_kind, scope_id,
                    status, auto_applied, confidence, risk_level, title, summary,
                    target_path, dedupe_key, content_json, provenance_json,
                    source_task_ulid, created_at_unix_ms, updated_at_unix_ms
                ) VALUES (
                    ?1, 'semantic_memory', ?2, NULL, ?3, ?4, ?5, 'principal', ?3,
                    'review_required', 0, ?6, ?7, 'Semantic memory candidate',
                    'Evidence-linked candidate awaiting host review', NULL, ?8,
                    ?9, ?10, NULL, ?11, ?11
                )
            "#,
            params![
                proposed.candidate.candidate_id,
                authority.session_id,
                authority.principal,
                authority.device_id,
                authority.channel,
                f64::from(proposed.candidate.confidence_basis_points) / 10_000.0,
                risk_level,
                proposed.candidate_sha256,
                content_json,
                provenance_json,
                now,
            ],
        )?;
        transaction.execute(
            r#"
                INSERT INTO learning_candidate_evals (
                    eval_ulid, candidate_ulid, eval_suite, result, threshold,
                    score, decision, actor_principal, policy_decision,
                    evidence_refs_json, created_at_unix_ms
                ) VALUES (
                    ?1, ?2, 'semantic_memory_retrieval_v1', 'passed', 0.0,
                    ?3, 'pass', ?4, 'server_observed',
                    ?5, ?6
                )
            "#,
            params![
                Ulid::generate().to_string(),
                proposed.candidate.candidate_id,
                f64::from(proposed.candidate.quality_eval.consolidated_usefulness_basis_points)
                    / 10_000.0,
                authority.principal,
                json!([{
                    "kind": "semantic_memory_eval",
                    "sha256": proposed.candidate.quality_eval.evidence_sha256,
                    "sample_count": proposed.candidate.quality_eval.sample_count,
                }])
                .to_string(),
                now,
            ],
        )?;
        transaction.commit()?;
        Ok(())
    }

    fn active_semantic_memory_by_candidate(
        &self,
        candidate_id: &str,
        target_scope: &SemanticMemoryTargetScope,
    ) -> Result<Option<SemanticMemoryActiveRecord>, JournalError> {
        let now = super::current_unix_ms()?;
        let guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        let memory_id = guard
            .query_row(
                r#"
                    SELECT active.memory_id
                    FROM semantic_memory_active_v1 AS active
                    INNER JOIN semantic_memory_records_v1 AS records
                        ON records.record_sha256 = active.record_sha256
                    WHERE records.candidate_id = ?1
                "#,
                params![candidate_id],
                |row| row.get::<_, String>(0),
            )
            .optional()?;
        let Some(memory_id) = memory_id else {
            return Ok(None);
        };
        let Some(current) = load_current_semantic_memory_tx(&guard, memory_id.as_str(), now)?
        else {
            return Ok(None);
        };
        ensure_current_scope(&current, target_scope)?;
        if current.record.lifecycle != ConsolidatedMemoryLifecycle::Active {
            return Ok(None);
        }
        let projected_memory = current
            .projected_memory
            .ok_or_else(|| reason("semantic_memory.activation_projection_missing"))?;
        if projected_memory.ttl_unix_ms.is_some_and(|expires_at| expires_at <= now) {
            return Ok(None);
        }
        Ok(Some(SemanticMemoryActiveRecord {
            candidate_id: current.candidate_id,
            record: current.record,
            projected_memory,
            approval_id: current.approval_id,
        }))
    }
}

#[derive(Debug, PartialEq, Eq)]
struct StoredProposal {
    proposed: SemanticMemoryProposedRecord,
    authority: SemanticMemoryReviewAuthority,
}

fn load_proposed_candidate_tx(
    connection: &rusqlite::Connection,
    candidate_id: &str,
) -> Result<Option<StoredProposal>, JournalError> {
    connection
        .query_row(
            r#"
                SELECT memory_id, owner_principal, device_id, channel,
                       target_session_id, session_id, run_id, candidate_sha256,
                       candidate_json, approval_subject_id,
                       approval_policy_sha256, host_policy_sha256,
                       expected_previous_record_sha256, approval_generation, reason_code
                FROM semantic_memory_candidates_v1
                WHERE candidate_id = ?1
                  AND lifecycle_state = 'proposed'
            "#,
            params![candidate_id],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, Option<String>>(3)?,
                    row.get::<_, Option<String>>(4)?,
                    row.get::<_, String>(5)?,
                    row.get::<_, String>(6)?,
                    row.get::<_, String>(7)?,
                    row.get::<_, String>(8)?,
                    row.get::<_, String>(9)?,
                    row.get::<_, String>(10)?,
                    row.get::<_, String>(11)?,
                    row.get::<_, Option<String>>(12)?,
                    row.get::<_, i64>(13)?,
                    row.get::<_, String>(14)?,
                ))
            },
        )
        .optional()?
        .map(
            |(
                memory_id,
                principal,
                device_id,
                channel,
                target_session_id,
                session_id,
                run_id,
                candidate_sha256,
                candidate_json,
                approval_subject_id,
                approval_policy_sha256,
                host_policy_sha256,
                active_record_sha256,
                approval_generation,
                reason_code,
            )| {
                let candidate: SemanticMemoryCandidateV1 =
                    serde_json::from_str(candidate_json.as_str())?;
                let approval_generation = u64::try_from(approval_generation)
                    .map_err(|_| reason("semantic_memory.approval_generation_invalid"))?;
                Ok(StoredProposal {
                    proposed: SemanticMemoryProposedRecord {
                        memory_id,
                        target_scope: SemanticMemoryTargetScope {
                            principal: principal.clone(),
                            channel: channel.clone(),
                            session_id: target_session_id,
                        },
                        candidate,
                        candidate_sha256,
                        approval_subject_id,
                        approval_policy_sha256,
                        context: SemanticMemoryActivationContext {
                            approval_generation,
                            active_record_sha256,
                        },
                        reason_code,
                    },
                    authority: SemanticMemoryReviewAuthority {
                        session_id,
                        run_id,
                        principal,
                        device_id,
                        channel,
                        host_policy_sha256,
                    },
                })
            },
        )
        .transpose()
}

fn active_context_connection(
    connection: &rusqlite::Connection,
    memory_id: &str,
) -> Result<SemanticMemoryActivationContext, JournalError> {
    let active = connection
        .query_row(
            r#"
                SELECT record_sha256, approval_generation
                FROM semantic_memory_active_v1
                WHERE memory_id = ?1
            "#,
            params![memory_id],
            |row| Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?)),
        )
        .optional()?;
    match active {
        Some((digest, generation)) => Ok(SemanticMemoryActivationContext {
            approval_generation: u64::try_from(generation)
                .ok()
                .and_then(|value| value.checked_add(1))
                .ok_or_else(|| reason("semantic_memory.approval_generation_invalid"))?,
            active_record_sha256: Some(digest),
        }),
        None => Ok(SemanticMemoryActivationContext {
            approval_generation: 1,
            active_record_sha256: None,
        }),
    }
}

fn validate_activation_approval(
    proposed: &SemanticMemoryProposedRecord,
    authority: &SemanticMemoryReviewAuthority,
    approval: &ApprovalRecord,
    now_unix_ms: i64,
) -> Result<(), JournalError> {
    let expected =
        semantic_memory_approval_request(approval.approval_id.clone(), proposed, authority);
    let expected_details: serde_json::Value =
        serde_json::from_str(expected.prompt.details_json.as_str())?;
    let observed_details: serde_json::Value =
        serde_json::from_str(approval.prompt.details_json.as_str())?;
    let resolved_at = approval.resolved_at_unix_ms.unwrap_or(i64::MAX);
    if approval.subject_type != ApprovalSubjectType::Tool
        || approval.subject_id != expected.subject_id
        || approval.session_id != expected.session_id
        || approval.run_id != expected.run_id
        || approval.principal != expected.principal
        || approval.device_id != expected.device_id
        || approval.channel != expected.channel
        || approval.request_summary != expected.request_summary
        || approval.decision != Some(ApprovalDecision::Allow)
        || approval.decision_scope != Some(ApprovalDecisionScope::Once)
        || approval.decision_scope_ttl_ms.is_some()
        || resolved_at < approval.requested_at_unix_ms
        || resolved_at > now_unix_ms
        || resolved_at.saturating_sub(approval.requested_at_unix_ms) > REVIEW_VALIDITY_MS
        || now_unix_ms.saturating_sub(resolved_at) > REVIEW_VALIDITY_MS
        || approval.created_at_unix_ms != approval.requested_at_unix_ms
        || approval.updated_at_unix_ms < resolved_at
        || approval.decision_reason.as_deref().is_none_or(str::is_empty)
        || approval.policy_snapshot != expected.policy_snapshot
        || approval.prompt.title != expected.prompt.title
        || approval.prompt.risk_level != expected.prompt.risk_level
        || approval.prompt.subject_id != expected.prompt.subject_id
        || approval.prompt.summary != expected.prompt.summary
        || approval.prompt.options != expected.prompt.options
        || approval.prompt.timeout_seconds != expected.prompt.timeout_seconds
        || observed_details != expected_details
        || approval.prompt.policy_explanation != expected.prompt.policy_explanation
    {
        return Err(reason("semantic_memory.host_approval_missing_or_stale"));
    }
    Ok(())
}

fn consume_approval_tx(
    transaction: &Transaction<'_>,
    approval_id: &str,
    now_unix_ms: i64,
    consume_reason: &str,
) -> Result<(), JournalError> {
    let consumed = transaction.execute(
        r#"
            INSERT INTO approval_consumptions (
                approval_ulid, consumed_at_unix_ms, consume_reason
            )
            SELECT approval_ulid, ?2, ?3
            FROM approvals
            WHERE approval_ulid = ?1
              AND decision = 'allow'
              AND decision_scope = 'once'
              AND NOT EXISTS (
                  SELECT 1 FROM approval_consumptions
                  WHERE approval_consumptions.approval_ulid = approvals.approval_ulid
              )
        "#,
        params![approval_id, now_unix_ms, consume_reason],
    )?;
    if consumed != 1 {
        return Err(reason("semantic_memory.host_approval_consumed"));
    }
    Ok(())
}

fn insert_semantic_projection_tx(
    transaction: &Transaction<'_>,
    candidate_id: &str,
    projection_id: &str,
    prepared: &PreparedSemanticProjection,
    now_unix_ms: i64,
) -> Result<(), JournalError> {
    transaction
        .execute(
            r#"
            INSERT INTO memory_items (
                memory_ulid, principal, channel, session_ulid, source,
                content_text, content_hash, tags_json, confidence, ttl_unix_ms,
                created_at_unix_ms, updated_at_unix_ms
            )
            SELECT ?1, owner_principal, channel, target_session_id, 'summary',
                   ?2, ?3, ?4, ?5, ?6, ?7, ?7
            FROM semantic_memory_candidates_v1
            WHERE candidate_id = ?8
            LIMIT 1
        "#,
            params![
                projection_id,
                prepared.content_text,
                prepared.content_hash,
                prepared.tags_json,
                prepared.confidence,
                prepared.ttl_unix_ms,
                now_unix_ms,
                candidate_id,
            ],
        )
        .and_then(|inserted| {
            if inserted == 1 {
                Ok(inserted)
            } else {
                Err(rusqlite::Error::QueryReturnedNoRows)
            }
        })?;
    transaction.execute(
        r#"
            INSERT INTO memory_vectors (
                memory_ulid, embedding_model, dims, vector_blob,
                created_at_unix_ms, embedding_model_id, embedding_dims,
                embedding_version, embedding_vector, embedded_at_unix_ms
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?2, ?3, ?6, ?4, ?5)
        "#,
        params![
            projection_id,
            prepared.embedding_model,
            prepared.embedding_dims,
            prepared.vector_blob,
            now_unix_ms,
            CURRENT_MEMORY_EMBEDDING_VERSION,
        ],
    )?;
    Ok(())
}

fn prepare_semantic_projection(
    store: &JournalStore,
    record: &ConsolidatedMemoryRecord,
) -> Result<PreparedSemanticProjection, JournalError> {
    let content_text = sanitize_object_text_field("content_text", record.summary_text.as_str())?;
    if content_text != record.summary_text {
        return Err(reason("semantic_memory.summary_redaction_mismatch"));
    }
    let tags = normalize_memory_tags(&[
        SEMANTIC_MEMORY_TAG.to_owned(),
        format!("semantic_{}", record.epistemic_kind.retrieval_label()),
        format!("semantic_v{}", record.version),
    ]);
    let tags_json = serde_json::to_string(&tags)?;
    let embedding_dims = store.memory_embedding_provider.dimensions();
    let vector = normalize_embedding_dimensions(
        store.memory_embedding_provider.embed_text(content_text.as_str()),
        embedding_dims,
    );
    Ok(PreparedSemanticProjection {
        content_hash: digest_text(content_text.as_str()),
        content_text,
        tags_json,
        confidence: f64::from(record.confidence_basis_points) / 10_000.0,
        ttl_unix_ms: record.retention_expires_at_unix_ms,
        embedding_model: store.memory_embedding_provider.model_name().to_owned(),
        embedding_dims: i64::try_from(embedding_dims)
            .map_err(|_| reason("semantic_memory.embedding_dimensions_invalid"))?,
        vector_blob: encode_vector_blob(vector.as_slice()),
    })
}

pub(super) fn reconcile_expired_semantic_memory_tx(
    transaction: &Transaction<'_>,
    now_unix_ms: i64,
) -> Result<usize, JournalError> {
    let mut statement = transaction.prepare(
        r#"
            SELECT records.candidate_id, candidates.owner_principal,
                   records.approval_id, records.record_json
            FROM semantic_memory_active_v1 AS active
            INNER JOIN semantic_memory_records_v1 AS records
                ON records.record_sha256 = active.record_sha256
            INNER JOIN semantic_memory_candidates_v1 AS candidates
                ON candidates.candidate_id = records.candidate_id
            INNER JOIN memory_items AS memory
                ON memory.memory_ulid = active.projected_memory_ulid
            WHERE records.lifecycle_state = 'active'
              AND memory.ttl_unix_ms IS NOT NULL
              AND memory.ttl_unix_ms <= ?1
            ORDER BY active.memory_id ASC
        "#,
    )?;
    let rows = statement.query_map(params![now_unix_ms], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, Option<String>>(2)?,
            row.get::<_, String>(3)?,
        ))
    })?;
    let mut expired = Vec::new();
    for row in rows {
        expired.push(row?);
    }
    drop(statement);

    for (candidate_id, principal, approval_id, record_json) in &expired {
        let mut record: ConsolidatedMemoryRecord = serde_json::from_str(record_json.as_str())?;
        let previous_digest = record.record_sha256.clone();
        if !mark_semantic_memory_stale(&mut record, now_unix_ms, i64::MAX)
            .map_err(semantic_error)?
        {
            return Err(reason("semantic_memory.retention_reconciliation_failed"));
        }
        remove_active_projection_tx(transaction, record.memory_id.as_str(), now_unix_ms)?;
        insert_semantic_record_tx(
            transaction,
            candidate_id.as_str(),
            &record,
            approval_id.as_deref(),
            None,
            now_unix_ms,
        )?;
        update_current_pointer_tx(
            transaction,
            record.memory_id.as_str(),
            record.record_sha256.as_str(),
            None,
            record.approval_generation,
            now_unix_ms,
        )?;
        record_lifecycle_tx(
            transaction,
            candidate_id.as_str(),
            &record,
            Some(previous_digest.as_str()),
            approval_id.as_deref(),
            "semantic_memory.retention_expired",
            now_unix_ms,
        )?;
        record_learning_rollout_event_tx(
            transaction,
            LearningRolloutEvent {
                candidate_id,
                principal,
                record: &record,
                previous_record_sha256: Some(previous_digest.as_str()),
                reason_code: "semantic_memory.retention_expired",
                policy_decision: "server_retention_policy",
                now_unix_ms,
                rolled_back_at_unix_ms: None,
            },
        )?;
    }
    Ok(expired.len())
}

fn remove_active_projection_tx(
    transaction: &Transaction<'_>,
    memory_id: &str,
    _now_unix_ms: i64,
) -> Result<(), JournalError> {
    let projection_id = transaction
        .query_row(
            "SELECT projected_memory_ulid FROM semantic_memory_active_v1 WHERE memory_id = ?1",
            params![memory_id],
            |row| row.get::<_, Option<String>>(0),
        )
        .optional()?
        .flatten();
    transaction.execute(
        r#"
            UPDATE semantic_memory_active_v1
            SET projected_memory_ulid = NULL
            WHERE memory_id = ?1
        "#,
        params![memory_id],
    )?;
    if let Some(projection_id) = projection_id {
        transaction
            .execute("DELETE FROM memory_items WHERE memory_ulid = ?1", params![projection_id])?;
    }
    Ok(())
}

fn insert_semantic_record_tx(
    transaction: &Transaction<'_>,
    candidate_id: &str,
    record: &ConsolidatedMemoryRecord,
    approval_id: Option<&str>,
    projected_memory_id: Option<&str>,
    now_unix_ms: i64,
) -> Result<(), JournalError> {
    transaction.execute(
        r#"
            INSERT INTO semantic_memory_records_v1 (
                record_sha256, memory_id, version, candidate_id, lifecycle_state,
                record_json, approval_id, projected_memory_ulid, created_at_unix_ms
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
        "#,
        params![
            record.record_sha256,
            record.memory_id,
            i64::try_from(record.version)
                .map_err(|_| reason("semantic_memory.record_version_invalid"))?,
            candidate_id,
            lifecycle_str(record.lifecycle),
            serde_json::to_string(record)?,
            approval_id,
            projected_memory_id,
            now_unix_ms,
        ],
    )?;
    Ok(())
}

fn load_semantic_record_connection(
    connection: &rusqlite::Connection,
    record_sha256: &str,
) -> Result<Option<ConsolidatedMemoryRecord>, JournalError> {
    connection
        .query_row(
            "SELECT record_json FROM semantic_memory_records_v1 WHERE record_sha256 = ?1",
            params![record_sha256],
            |row| row.get::<_, String>(0),
        )
        .optional()?
        .map(|json| serde_json::from_str(json.as_str()).map_err(JournalError::from))
        .transpose()
}

fn load_semantic_record_with_candidate_connection(
    connection: &rusqlite::Connection,
    record_sha256: &str,
) -> Result<Option<(String, ConsolidatedMemoryRecord)>, JournalError> {
    connection
        .query_row(
            r#"
                SELECT candidate_id, record_json
                FROM semantic_memory_records_v1
                WHERE record_sha256 = ?1
            "#,
            params![record_sha256],
            |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
        )
        .optional()?
        .map(|(candidate_id, json)| {
            Ok((candidate_id, serde_json::from_str::<ConsolidatedMemoryRecord>(json.as_str())?))
        })
        .transpose()
}

fn load_current_semantic_memory_tx(
    connection: &rusqlite::Connection,
    memory_id: &str,
    _now_unix_ms: i64,
) -> Result<Option<SemanticMemoryCurrentRecord>, JournalError> {
    connection
        .query_row(
            r#"
                SELECT records.candidate_id, records.record_json,
                       records.approval_id, active.projected_memory_ulid,
                       candidates.owner_principal, candidates.channel,
                       candidates.target_session_id
                FROM semantic_memory_active_v1 AS active
                INNER JOIN semantic_memory_records_v1 AS records
                    ON records.record_sha256 = active.record_sha256
                INNER JOIN semantic_memory_candidates_v1 AS candidates
                    ON candidates.candidate_id = records.candidate_id
                WHERE active.memory_id = ?1
            "#,
            params![memory_id],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, Option<String>>(2)?,
                    row.get::<_, Option<String>>(3)?,
                    row.get::<_, String>(4)?,
                    row.get::<_, Option<String>>(5)?,
                    row.get::<_, Option<String>>(6)?,
                ))
            },
        )
        .optional()?
        .map(
            |(
                candidate_id,
                record_json,
                approval_id,
                projection_id,
                principal,
                channel,
                target_session_id,
            )| {
                let record: ConsolidatedMemoryRecord = serde_json::from_str(record_json.as_str())?;
                let projected_memory = projection_id
                    .as_deref()
                    .map(|projection_id| {
                        load_semantic_projection_by_id(connection, projection_id).and_then(|item| {
                            item.ok_or_else(|| {
                                reason("semantic_memory.activation_projection_missing")
                            })
                        })
                    })
                    .transpose()?;
                if record.lifecycle == ConsolidatedMemoryLifecycle::Active
                    && projected_memory.is_none()
                {
                    return Err(reason("semantic_memory.activation_projection_missing"));
                }
                if record.lifecycle != ConsolidatedMemoryLifecycle::Active
                    && projected_memory.is_some()
                {
                    return Err(reason("semantic_memory.nonactive_projection_present"));
                }
                Ok(SemanticMemoryCurrentRecord {
                    candidate_id,
                    target_scope: SemanticMemoryTargetScope {
                        principal,
                        channel,
                        session_id: target_session_id,
                    },
                    record,
                    projected_memory,
                    approval_id,
                })
            },
        )
        .transpose()
}

fn load_semantic_projection_by_id(
    connection: &rusqlite::Connection,
    projection_id: &str,
) -> Result<Option<MemoryItemRecord>, JournalError> {
    let mut statement = connection.prepare(
        r#"
            SELECT
                memory_ulid, principal, channel, session_ulid, source,
                content_text, content_hash, tags_json, confidence, ttl_unix_ms,
                created_at_unix_ms, updated_at_unix_ms
            FROM memory_items
            WHERE memory_ulid = ?1
            LIMIT 1
        "#,
    )?;
    statement.query_row(params![projection_id], map_memory_item_row).optional().map_err(Into::into)
}

fn update_current_pointer_tx(
    transaction: &Transaction<'_>,
    memory_id: &str,
    record_sha256: &str,
    projected_memory_id: Option<&str>,
    approval_generation: u64,
    now_unix_ms: i64,
) -> Result<(), JournalError> {
    let updated = transaction.execute(
        r#"
            UPDATE semantic_memory_active_v1
            SET record_sha256 = ?2,
                projected_memory_ulid = ?3,
                approval_generation = ?4,
                updated_at_unix_ms = ?5
            WHERE memory_id = ?1
        "#,
        params![
            memory_id,
            record_sha256,
            projected_memory_id,
            i64::try_from(approval_generation)
                .map_err(|_| reason("semantic_memory.approval_generation_invalid"))?,
            now_unix_ms,
        ],
    )?;
    if updated != 1 {
        return Err(reason("semantic_memory.active_generation_stale"));
    }
    Ok(())
}

fn record_lifecycle_tx(
    transaction: &Transaction<'_>,
    candidate_id: &str,
    record: &ConsolidatedMemoryRecord,
    previous_record_sha256: Option<&str>,
    approval_id: Option<&str>,
    reason_code: &str,
    now_unix_ms: i64,
) -> Result<(), JournalError> {
    transaction.execute(
        r#"
            INSERT INTO semantic_memory_lifecycle_events_v1 (
                memory_id, candidate_id, record_sha256, previous_record_sha256,
                lifecycle_state, approval_id, approval_generation, eval_sha256,
                reason_code, occurred_at_unix_ms
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)
        "#,
        params![
            record.memory_id,
            candidate_id,
            record.record_sha256,
            previous_record_sha256,
            lifecycle_str(record.lifecycle),
            approval_id,
            i64::try_from(record.approval_generation)
                .map_err(|_| reason("semantic_memory.approval_generation_invalid"))?,
            record.quality_eval.evidence_sha256,
            reason_code,
            now_unix_ms,
        ],
    )?;
    Ok(())
}

fn accept_learning_candidate_tx(
    transaction: &Transaction<'_>,
    candidate_id: &str,
    principal: &str,
    record: &ConsolidatedMemoryRecord,
    previous_record_sha256: Option<&str>,
    now_unix_ms: i64,
) -> Result<(), JournalError> {
    let updated = transaction.execute(
        r#"
            UPDATE learning_candidates
            SET status = 'accepted',
                reviewed_at_unix_ms = ?2,
                reviewed_by_principal = ?3,
                last_action_summary = 'semantic_memory.activated',
                last_action_payload_json = ?4,
                updated_at_unix_ms = ?2
            WHERE candidate_ulid = ?1
              AND status = 'review_required'
        "#,
        params![
            candidate_id,
            now_unix_ms,
            principal,
            json!({
                "record_sha256": record.record_sha256,
                "eval_sha256": record.quality_eval.evidence_sha256,
                "approval_generation": record.approval_generation,
            })
            .to_string(),
        ],
    )?;
    if updated != 1 {
        return Err(reason("semantic_memory.learning_review_stale"));
    }
    transaction.execute(
        r#"
            INSERT INTO learning_candidate_history (
                history_ulid, candidate_ulid, status, reviewed_by_principal,
                action_summary, action_payload_json, created_at_unix_ms
            ) VALUES (?1, ?2, 'accepted', ?3, 'semantic_memory.activated', ?4, ?5)
        "#,
        params![
            Ulid::generate().to_string(),
            candidate_id,
            principal,
            json!({
                "record_sha256": record.record_sha256,
                "reason_code": record.reason_code,
            })
            .to_string(),
            now_unix_ms,
        ],
    )?;
    transaction.execute(
        r#"
            INSERT INTO learning_candidate_rollouts (
                rollout_ulid, candidate_ulid, rollout_kind, state, target_ref,
                previous_version_json, activated_version_json, telemetry_json,
                reason, actor_principal, policy_decision, evidence_refs_json,
                created_at_unix_ms, updated_at_unix_ms, rolled_back_at_unix_ms
            ) VALUES (
                ?1, ?2, 'semantic_memory', 'active', ?3, ?4, ?5, ?6,
                'semantic_memory.activated', ?7, 'host_approved', ?8,
                ?9, ?9, NULL
            )
        "#,
        params![
            Ulid::generate().to_string(),
            candidate_id,
            digest_text(record.memory_id.as_str()),
            json!({"record_sha256": previous_record_sha256}).to_string(),
            json!({
                "record_sha256": record.record_sha256,
                "version": record.version,
            })
            .to_string(),
            json!({
                "reason_code": record.reason_code,
                "approval_generation": record.approval_generation,
            })
            .to_string(),
            principal,
            json!([{
                "kind": "semantic_memory_eval",
                "sha256": record.quality_eval.evidence_sha256,
            }])
            .to_string(),
            now_unix_ms,
        ],
    )?;
    Ok(())
}

fn record_learning_rollout_event_tx(
    transaction: &Transaction<'_>,
    event: LearningRolloutEvent<'_>,
) -> Result<(), JournalError> {
    transaction.execute(
        r#"
            INSERT INTO learning_candidate_rollouts (
                rollout_ulid, candidate_ulid, rollout_kind, state, target_ref,
                previous_version_json, activated_version_json, telemetry_json,
                reason, actor_principal, policy_decision, evidence_refs_json,
                created_at_unix_ms, updated_at_unix_ms, rolled_back_at_unix_ms
            ) VALUES (
                ?1, ?2, 'semantic_memory', ?3, ?4, ?5, ?6, ?7,
                ?8, ?9, ?10, ?11, ?12, ?12, ?13
            )
        "#,
        params![
            Ulid::generate().to_string(),
            event.candidate_id,
            lifecycle_str(event.record.lifecycle),
            digest_text(event.record.memory_id.as_str()),
            json!({"record_sha256": event.previous_record_sha256}).to_string(),
            json!({
                "record_sha256": event.record.record_sha256,
                "version": event.record.version,
            })
            .to_string(),
            json!({
                "reason_code": event.reason_code,
                "approval_generation": event.record.approval_generation,
            })
            .to_string(),
            event.reason_code,
            event.principal,
            event.policy_decision,
            json!([{
                "kind": "semantic_memory_eval",
                "sha256": event.record.quality_eval.evidence_sha256,
            }])
            .to_string(),
            event.now_unix_ms,
            event.rolled_back_at_unix_ms,
        ],
    )?;
    Ok(())
}

fn rollback_review_projection(
    memory_id: &str,
    target: &ConsolidatedMemoryRecord,
    target_scope: &SemanticMemoryTargetScope,
    context: &SemanticMemoryActivationContext,
    authority: &SemanticMemoryReviewAuthority,
) -> SemanticMemoryProposedRecord {
    let candidate = SemanticMemoryCandidateV1 {
        v: target.v,
        candidate_id: format!("rollback-{}", &target.record_sha256[..24]),
        claim_key: target.claim_key.clone(),
        claim_value_sha256: target.claim_value_sha256.clone(),
        summary_text: target.summary_text.clone(),
        summary_sha256: target.summary_sha256.clone(),
        epistemic_kind: target.epistemic_kind,
        acl_scope: target.acl_scope.clone(),
        sensitivity: target.sensitivity,
        confidence_basis_points: target.confidence_basis_points,
        contradiction_status: target.contradiction_status,
        evidence_refs: target.evidence_refs.clone(),
        citations: target.citations.clone(),
        retention_expires_at_unix_ms: target.retention_expires_at_unix_ms,
        review_required: true,
        quality_eval: target.quality_eval.clone(),
        created_at_unix_ms: target.activated_at_unix_ms,
        reason_code: "semantic_memory.rollback_candidate".to_owned(),
    };
    let candidate_sha256 =
        domain_digest(b"palyra.semantic-memory.rollback.v1\0", target.record_sha256.as_bytes());
    let approval_subject_id = semantic_memory_approval_subject(
        memory_id,
        candidate_sha256.as_str(),
        target.quality_eval.evidence_sha256.as_str(),
        context,
    );
    SemanticMemoryProposedRecord {
        memory_id: memory_id.to_owned(),
        target_scope: target_scope.clone(),
        candidate,
        candidate_sha256: candidate_sha256.clone(),
        approval_subject_id,
        approval_policy_sha256: semantic_memory_review_policy_sha256(
            memory_id,
            candidate_sha256.as_str(),
            target.quality_eval.evidence_sha256.as_str(),
            context,
            authority.host_policy_sha256.as_str(),
        ),
        context: context.clone(),
        reason_code: "semantic_memory.rollback_host_approval_required".to_owned(),
    }
}

fn semantic_memory_review_policy_sha256(
    memory_id: &str,
    candidate_sha256: &str,
    eval_sha256: &str,
    context: &SemanticMemoryActivationContext,
    host_policy_sha256: &str,
) -> String {
    let payload = json!({
        "memory_id_sha256": digest_text(memory_id),
        "candidate_sha256": candidate_sha256,
        "eval_sha256": eval_sha256,
        "previous_record_sha256": context.active_record_sha256,
        "approval_generation": context.approval_generation,
        "host_policy_sha256": host_policy_sha256,
    });
    domain_digest(b"palyra.semantic-memory.review-policy.v1\0", payload.to_string().as_bytes())
}

fn validate_authority(authority: &SemanticMemoryReviewAuthority) -> Result<(), JournalError> {
    if authority.session_id.trim().is_empty()
        || authority.run_id.trim().is_empty()
        || authority.principal.trim().is_empty()
        || authority.device_id.trim().is_empty()
        || authority.host_policy_sha256.len() != 64
        || !authority
            .host_policy_sha256
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(reason("semantic_memory.review_authority_invalid"));
    }
    Ok(())
}

fn validate_review_session_tx(
    connection: &rusqlite::Connection,
    authority: &SemanticMemoryReviewAuthority,
) -> Result<(), JournalError> {
    let identity = connection
        .query_row(
            r#"
                SELECT principal, device_id, channel
                FROM orchestrator_sessions
                WHERE session_ulid = ?1
                  AND archived_at_unix_ms IS NULL
            "#,
            params![authority.session_id],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, Option<String>>(2)?,
                ))
            },
        )
        .optional()?;
    if identity
        != Some((
            authority.principal.clone(),
            authority.device_id.clone(),
            authority.channel.clone(),
        ))
    {
        return Err(reason("semantic_memory.review_session_missing_or_stale"));
    }
    Ok(())
}

fn ensure_current_scope(
    current: &SemanticMemoryCurrentRecord,
    expected: &SemanticMemoryTargetScope,
) -> Result<(), JournalError> {
    if current.target_scope != *expected {
        return Err(reason("semantic_memory.acl_mismatch"));
    }
    Ok(())
}

fn validate_reference_redaction(
    evidence_refs: &[SemanticMemoryEvidenceRefV1],
) -> Result<(), JournalError> {
    for evidence in evidence_refs {
        for value in [
            evidence.source_ref.as_str(),
            evidence.citation_uri.as_str(),
            evidence.claim_key.as_str(),
        ] {
            if sanitize_object_text_field("reference", value)? != value {
                return Err(reason("semantic_memory.reference_redaction_required"));
            }
        }
    }
    Ok(())
}

fn semantic_projection_id(memory_id: &str, version: u64) -> String {
    format!("semantic-{}-{version}", &digest_text(memory_id)[..32])
}

fn semantic_eval_projection_id(candidate_id: &str) -> String {
    format!("semantic-eval-{}", &digest_text(candidate_id)[..32])
}

fn lifecycle_str(lifecycle: ConsolidatedMemoryLifecycle) -> &'static str {
    match lifecycle {
        ConsolidatedMemoryLifecycle::Active => "active",
        ConsolidatedMemoryLifecycle::Degraded => "degraded",
        ConsolidatedMemoryLifecycle::Archived => "archived",
        ConsolidatedMemoryLifecycle::RolledBack => "rolled_back",
    }
}

fn count_scoped_tx(
    connection: &rusqlite::Connection,
    sql: &str,
    target_scope: &SemanticMemoryTargetScope,
) -> Result<usize, JournalError> {
    let count = connection.query_row(
        sql,
        params![target_scope.principal, target_scope.channel, target_scope.session_id],
        |row| row.get::<_, i64>(0),
    )?;
    usize::try_from(count).map_err(|_| reason("semantic_memory.diagnostics_invalid"))
}

fn count_scoped_at_tx(
    connection: &rusqlite::Connection,
    sql: &str,
    target_scope: &SemanticMemoryTargetScope,
    now_unix_ms: i64,
) -> Result<usize, JournalError> {
    let count = connection.query_row(
        sql,
        params![target_scope.principal, target_scope.channel, target_scope.session_id, now_unix_ms],
        |row| row.get::<_, i64>(0),
    )?;
    usize::try_from(count).map_err(|_| reason("semantic_memory.diagnostics_invalid"))
}

fn semantic_error(error: SemanticMemoryError) -> JournalError {
    let reason_code = match error {
        SemanticMemoryError::Disabled => "semantic_memory.rollout_disabled",
        SemanticMemoryError::AclMismatch => "semantic_memory.acl_mismatch",
        SemanticMemoryError::InsufficientCorroboration => {
            "semantic_memory.insufficient_corroboration"
        }
        SemanticMemoryError::QualityEvalFailed => "semantic_memory.quality_eval_failed",
        SemanticMemoryError::QualityEvalEvidenceInvalid => {
            "semantic_memory.quality_eval_evidence_invalid"
        }
        SemanticMemoryError::VerbatimEvidenceDenied => "semantic_memory.verbatim_evidence_denied",
        SemanticMemoryError::SensitiveRetentionInvalid => {
            "semantic_memory.sensitive_retention_invalid"
        }
        SemanticMemoryError::HostValidationRequired => "semantic_memory.host_validation_required",
        SemanticMemoryError::PolicyApprovalRequired => "semantic_memory.policy_approval_required",
        SemanticMemoryError::ReviewerApprovalRequired => {
            "semantic_memory.reviewer_approval_required"
        }
        SemanticMemoryError::QualityEvalApprovalRequired => {
            "semantic_memory.quality_eval_approval_required"
        }
        SemanticMemoryError::ContradictionUnresolved => "semantic_memory.contradiction_unresolved",
        SemanticMemoryError::ApprovalGenerationInvalid => {
            "semantic_memory.approval_generation_invalid"
        }
        SemanticMemoryError::RollbackTargetInvalid => "semantic_memory.rollback_target_invalid",
        SemanticMemoryError::RecordDigestInvalid => "semantic_memory.record_digest_invalid",
        SemanticMemoryError::EvidenceInvalid(_) | SemanticMemoryError::Serialization => {
            "semantic_memory.evidence_invalid"
        }
    };
    reason(reason_code)
}

fn reason(reason_code: &str) -> JournalError {
    JournalError::InvalidArgument(reason_code.to_owned())
}

fn digest_text(value: &str) -> String {
    hex::encode(Sha256::digest(value.as_bytes()))
}

fn domain_digest(domain: &[u8], payload: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(domain);
    hasher.update((payload.len() as u64).to_le_bytes());
    hasher.update(payload);
    hex::encode(hasher.finalize())
}

#[cfg(test)]
#[path = "semantic_memory/tests.rs"]
mod tests;
