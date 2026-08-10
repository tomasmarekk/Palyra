//! Append-only bounded handoffs, comments, reviews, and terminal projections.

use crate::domain::work_graph::{
    validate_loaded_graph, validate_transition, WorkGraphCommentCreateRequest,
    WorkGraphCommentRecordV1, WorkGraphListEntryV1, WorkGraphReviewDecision,
    WorkGraphReviewOutcomeV1, WorkGraphReviewRecordV1, WorkGraphReviewRequest, WorkGraphState,
    WorkGraphTerminalSummaryV1, WorkItemHandoffCommitOutcome, WorkItemHandoffCreateRequest,
    WorkItemHandoffEnvelopeV1, WorkItemHandoffSummaryV1, WorkItemState, WorkVerificationState,
    MAX_WORK_GRAPH_COMMENT_BYTES, MAX_WORK_GRAPH_QUERY_RECORDS, MAX_WORK_HANDOFF_REFS,
    MAX_WORK_HANDOFF_REF_BYTES, MAX_WORK_HANDOFF_RESULT_BYTES, MAX_WORK_HANDOFF_SUMMARY_BYTES,
    WORK_GRAPH_SCHEMA_VERSION,
};

use super::{
    storage::{
        insert_event, project_dependency_states, project_graph_state, query_snapshot,
        validation_error, EventInsert,
    },
    *,
};

const HANDOFF_COMMITTED_REASON: &str = "work_graph.handoff.committed";
const COMMENT_COMMITTED_REASON: &str = "work_graph.comment.committed";
const REVIEW_APPROVED_REASON: &str = "work_graph.review.approved";
const REVIEW_REJECTED_REASON: &str = "work_graph.review.rejected";

impl JournalStore {
    /// Appends one current-generation handoff and advances graph/item revisions atomically.
    pub(crate) fn record_work_item_handoff(
        &self,
        request: &WorkItemHandoffCreateRequest,
    ) -> Result<WorkItemHandoffCommitOutcome, JournalError> {
        let normalized = normalize_handoff_request(request)?;
        let now = current_unix_ms()?;
        let mut guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        let transaction = guard.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let snapshot = query_snapshot(&transaction, request.authority.graph_id.as_str())?
            .ok_or_else(|| JournalError::WorkGraphNotFound {
                graph_id: request.authority.graph_id.clone(),
            })?;
        validate_loaded_graph(&snapshot.graph, snapshot.items.as_slice())
            .map_err(validation_error)?;
        let item = snapshot
            .items
            .iter()
            .find(|item| item.work_item_id == request.authority.work_item_id)
            .ok_or_else(|| JournalError::WorkGraphItemNotFound {
                graph_id: request.authority.graph_id.clone(),
                work_item_id: request.authority.work_item_id.clone(),
            })?;
        let claim = item
            .claim
            .as_ref()
            .filter(|claim| {
                claim.worker_id == request.authority.worker_id
                    && claim.worker_principal == request.actor_principal
                    && claim.generation == request.authority.generation
                    && claim.claim_token_sha256 == request.authority.token.sha256_hex()
            })
            .ok_or_else(|| invalid_handoff("current claim authority is required"))?;
        if item.revision != request.expected_item_revision {
            return Err(JournalError::WorkGraphRevisionConflict {
                graph_id: request.authority.graph_id.clone(),
                work_item_id: request.authority.work_item_id.clone(),
                expected_revision: request.expected_item_revision,
                actual_revision: item.revision,
            });
        }
        if !matches!(
            item.state,
            WorkItemState::Claimed | WorkItemState::Running | WorkItemState::Waiting
        ) {
            return Err(invalid_handoff("handoff requires an execution-owned item state"));
        }

        if let Some(existing) = query_handoff_for_generation(
            &transaction,
            request.authority.graph_id.as_str(),
            request.authority.work_item_id.as_str(),
            request.authority.generation,
        )? {
            if existing.provenance_sha256 != normalized.provenance_sha256 {
                return Err(invalid_handoff(
                    "claim generation already committed a different handoff",
                ));
            }
            return Ok(WorkItemHandoffCommitOutcome {
                handoff: existing,
                item_revision: item.revision,
                graph_revision: snapshot.graph.revision,
            });
        }

        let handoff_id = Ulid::new().to_string();
        transaction.execute(
            r#"
                INSERT INTO work_graph_handoffs (
                    handoff_ulid, graph_ulid, work_item_ulid, claim_generation,
                    worker_principal, summary, structured_result_json,
                    context_cost_tokens, evidence_refs_json, artifact_refs_json,
                    verification_state, provenance_sha256, created_at_unix_ms
                ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)
            "#,
            params![
                handoff_id,
                request.authority.graph_id,
                request.authority.work_item_id,
                u64_to_sqlite(request.authority.generation, "claim_generation")?,
                claim.worker_principal,
                normalized.summary,
                normalized.structured_result_json,
                i64::from(normalized.context_cost_tokens),
                serde_json::to_string(&normalized.evidence_refs)?,
                serde_json::to_string(&normalized.artifact_refs)?,
                request.verification_state.as_str(),
                normalized.provenance_sha256,
                now,
            ],
        )?;
        let next_item_revision = item.revision.saturating_add(1);
        let next_graph_revision = snapshot.graph.revision.saturating_add(1);
        transaction.execute(
            r#"
                UPDATE work_graph_items
                SET evidence_refs_json = ?3,
                    artifact_refs_json = ?4,
                    revision = ?5,
                    reason_code = ?6,
                    updated_at_unix_ms = ?7
                WHERE graph_ulid = ?1
                  AND work_item_ulid = ?2
                  AND revision = ?8
                  AND claim_generation = ?9
                  AND claim_token_sha256 = ?10
            "#,
            params![
                request.authority.graph_id,
                request.authority.work_item_id,
                serde_json::to_string(&normalized.evidence_refs)?,
                serde_json::to_string(&normalized.artifact_refs)?,
                u64_to_sqlite(next_item_revision, "work_item_revision")?,
                HANDOFF_COMMITTED_REASON,
                now,
                u64_to_sqlite(item.revision, "expected_item_revision")?,
                u64_to_sqlite(request.authority.generation, "claim_generation")?,
                request.authority.token.sha256_hex(),
            ],
        )?;
        transaction.execute(
            r#"
                UPDATE work_graphs
                SET revision = ?2, reason_code = ?3, updated_at_unix_ms = ?4
                WHERE graph_ulid = ?1 AND revision = ?5
            "#,
            params![
                request.authority.graph_id,
                u64_to_sqlite(next_graph_revision, "graph_revision")?,
                HANDOFF_COMMITTED_REASON,
                now,
                u64_to_sqlite(snapshot.graph.revision, "expected_graph_revision")?,
            ],
        )?;
        insert_event(
            &transaction,
            EventInsert {
                graph_id: request.authority.graph_id.as_str(),
                work_item_id: Some(request.authority.work_item_id.as_str()),
                graph_revision: next_graph_revision,
                item_revision: Some(next_item_revision),
                event_type: "work_graph.item.handoff_committed",
                actor_principal: request.actor_principal.as_str(),
                from_state: Some(item.state.as_str()),
                to_state: Some(item.state.as_str()),
                reason_code: HANDOFF_COMMITTED_REASON,
                payload_json: json!({
                    "handoff_id": handoff_id,
                    "claim_generation": request.authority.generation,
                    "context_cost_tokens": normalized.context_cost_tokens,
                    "evidence_ref_count": normalized.evidence_refs.len(),
                    "artifact_ref_count": normalized.artifact_refs.len(),
                    "provenance_sha256": normalized.provenance_sha256,
                })
                .to_string()
                .as_str(),
                created_at_unix_ms: now,
            },
        )?;
        transaction.commit()?;
        Ok(WorkItemHandoffCommitOutcome {
            handoff: WorkItemHandoffEnvelopeV1 {
                schema_version: WORK_GRAPH_SCHEMA_VERSION,
                handoff_id,
                graph_id: request.authority.graph_id.clone(),
                work_item_id: request.authority.work_item_id.clone(),
                claim_generation: request.authority.generation,
                summary: normalized.summary,
                structured_result: serde_json::from_str(
                    normalized.structured_result_json.as_str(),
                )?,
                context_cost_tokens: normalized.context_cost_tokens,
                evidence_refs: normalized.evidence_refs,
                artifact_refs: normalized.artifact_refs,
                verification_state: request.verification_state,
                provenance_sha256: normalized.provenance_sha256,
                created_at_unix_ms: now,
            },
            item_revision: next_item_revision,
            graph_revision: next_graph_revision,
        })
    }

    /// Appends one comment after enforcing the graph owner/current-worker ACL.
    pub(crate) fn create_work_graph_comment(
        &self,
        request: &WorkGraphCommentCreateRequest,
    ) -> Result<WorkGraphCommentRecordV1, JournalError> {
        ensure_nonempty_field(request.graph_id.as_str(), "graph_id")?;
        ensure_nonempty_field(request.work_item_id.as_str(), "work_item_id")?;
        ensure_nonempty_field(request.actor_principal.as_str(), "actor_principal")?;
        if request.body.trim().is_empty() || request.body.len() > MAX_WORK_GRAPH_COMMENT_BYTES {
            return Err(invalid_handoff("comment body is empty or exceeds the host bound"));
        }
        let body = sanitize_object_text_field("body", request.body.trim())?;
        let now = current_unix_ms()?;
        let mut guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        let transaction = guard.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let snapshot =
            query_snapshot(&transaction, request.graph_id.as_str())?.ok_or_else(|| {
                JournalError::WorkGraphNotFound { graph_id: request.graph_id.clone() }
            })?;
        let item = snapshot
            .items
            .iter()
            .find(|item| item.work_item_id == request.work_item_id)
            .ok_or_else(|| JournalError::WorkGraphItemNotFound {
                graph_id: request.graph_id.clone(),
                work_item_id: request.work_item_id.clone(),
            })?;
        let worker_allowed = item
            .claim
            .as_ref()
            .is_some_and(|claim| claim.worker_principal == request.actor_principal);
        if snapshot.graph.owner.principal != request.actor_principal && !worker_allowed {
            return Err(invalid_handoff("comment principal is outside the graph ACL"));
        }
        let comment_id = Ulid::new().to_string();
        let provenance_sha256 = digest_json(&json!({
            "comment_id": comment_id,
            "graph_id": request.graph_id,
            "work_item_id": request.work_item_id,
            "author_principal": request.actor_principal,
            "body": body,
        }))?;
        transaction.execute(
            r#"
                INSERT INTO work_graph_comments (
                    comment_ulid, graph_ulid, work_item_ulid, author_principal,
                    body, provenance_sha256, created_at_unix_ms
                ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
            "#,
            params![
                comment_id,
                request.graph_id,
                request.work_item_id,
                request.actor_principal,
                body,
                provenance_sha256,
                now,
            ],
        )?;
        let sequence = u64::try_from(transaction.last_insert_rowid()).unwrap_or(u64::MAX);
        insert_event(
            &transaction,
            EventInsert {
                graph_id: request.graph_id.as_str(),
                work_item_id: Some(request.work_item_id.as_str()),
                graph_revision: snapshot.graph.revision,
                item_revision: Some(item.revision),
                event_type: "work_graph.item.comment_committed",
                actor_principal: request.actor_principal.as_str(),
                from_state: Some(item.state.as_str()),
                to_state: Some(item.state.as_str()),
                reason_code: COMMENT_COMMITTED_REASON,
                payload_json: json!({
                    "comment_id": comment_id,
                    "provenance_sha256": provenance_sha256,
                })
                .to_string()
                .as_str(),
                created_at_unix_ms: now,
            },
        )?;
        transaction.commit()?;
        Ok(WorkGraphCommentRecordV1 {
            schema_version: WORK_GRAPH_SCHEMA_VERSION,
            sequence,
            comment_id,
            graph_id: request.graph_id.clone(),
            work_item_id: request.work_item_id.clone(),
            author_principal: request.actor_principal.clone(),
            body,
            provenance_sha256,
            created_at_unix_ms: now,
        })
    }

    /// Records a provenance-bound owner review and atomically approves or reopens the item.
    pub(crate) fn review_work_item_handoff(
        &self,
        request: &WorkGraphReviewRequest,
    ) -> Result<WorkGraphReviewOutcomeV1, JournalError> {
        validate_review_request(request)?;
        let now = current_unix_ms()?;
        let mut guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        let transaction = guard.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let snapshot =
            query_snapshot(&transaction, request.graph_id.as_str())?.ok_or_else(|| {
                JournalError::WorkGraphNotFound { graph_id: request.graph_id.clone() }
            })?;
        validate_loaded_graph(&snapshot.graph, snapshot.items.as_slice())
            .map_err(validation_error)?;
        if snapshot.graph.owner.principal != request.reviewer_principal {
            return Err(invalid_handoff("reviewer principal is outside the graph owner ACL"));
        }
        let item = snapshot
            .items
            .iter()
            .find(|item| item.work_item_id == request.work_item_id)
            .ok_or_else(|| JournalError::WorkGraphItemNotFound {
                graph_id: request.graph_id.clone(),
                work_item_id: request.work_item_id.clone(),
            })?;
        if item.state != WorkItemState::Review {
            return Err(invalid_handoff("review requires an item in review state"));
        }
        let handoff = query_handoff_by_id(&transaction, request.handoff_id.as_str())?
            .filter(|handoff| {
                handoff.graph_id == request.graph_id && handoff.work_item_id == request.work_item_id
            })
            .ok_or_else(|| invalid_handoff("review handoff is missing or outside the item"))?;
        let latest_handoff_id = transaction.query_row(
            r#"
                SELECT handoff_ulid
                FROM work_graph_handoffs
                WHERE graph_ulid = ?1 AND work_item_ulid = ?2
                ORDER BY created_at_unix_ms DESC, handoff_ulid DESC
                LIMIT 1
            "#,
            params![request.graph_id, request.work_item_id],
            |row| row.get::<_, String>(0),
        )?;
        if latest_handoff_id != request.handoff_id {
            return Err(invalid_handoff("review must target the latest immutable handoff"));
        }

        let target_state = match request.decision {
            WorkGraphReviewDecision::Approve => WorkItemState::Succeeded,
            WorkGraphReviewDecision::Reject => WorkItemState::Ready,
        };
        let verification_state = match request.decision {
            WorkGraphReviewDecision::Approve => WorkVerificationState::Verified,
            WorkGraphReviewDecision::Reject => WorkVerificationState::Rejected,
        };
        validate_transition(item.state, item.verification_state, target_state, verification_state)
            .map_err(validation_error)?;
        let stable_reason = match request.decision {
            WorkGraphReviewDecision::Approve => REVIEW_APPROVED_REASON,
            WorkGraphReviewDecision::Reject => REVIEW_REJECTED_REASON,
        };
        let review_id = Ulid::new().to_string();
        let provenance_sha256 = digest_json(&json!({
            "review_id": review_id,
            "handoff_provenance_sha256": handoff.provenance_sha256,
            "reviewer_principal": request.reviewer_principal,
            "decision": request.decision,
            "reason_code": request.reason_code,
        }))?;
        transaction.execute(
            r#"
                INSERT INTO work_graph_reviews (
                    review_ulid, graph_ulid, work_item_ulid, handoff_ulid,
                    reviewer_principal, decision, reason_code, evidence_refs_json,
                    provenance_sha256, created_at_unix_ms
                ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)
            "#,
            params![
                review_id,
                request.graph_id,
                request.work_item_id,
                request.handoff_id,
                request.reviewer_principal,
                request.decision.as_str(),
                request.reason_code,
                serde_json::to_string(&handoff.evidence_refs)?,
                provenance_sha256,
                now,
            ],
        )?;
        let next_item_revision = item.revision.saturating_add(1);
        let next_graph_revision = snapshot.graph.revision.saturating_add(1);
        transaction.execute(
            r#"
                UPDATE work_graph_items
                SET state = ?3,
                    verification_state = ?4,
                    consecutive_failure_count = CASE WHEN ?3 = 'ready' THEN 0 ELSE consecutive_failure_count END,
                    retry_not_before_unix_ms = NULL,
                    circuit_opened_at_unix_ms = NULL,
                    failure_reason_code = NULL,
                    revision = ?5,
                    reason_code = ?6,
                    updated_at_unix_ms = ?7,
                    completed_at_unix_ms = CASE WHEN ?3 = 'succeeded' THEN ?7 ELSE NULL END
                WHERE graph_ulid = ?1 AND work_item_ulid = ?2 AND revision = ?8
            "#,
            params![
                request.graph_id,
                request.work_item_id,
                target_state.as_str(),
                verification_state.as_str(),
                u64_to_sqlite(next_item_revision, "work_item_revision")?,
                stable_reason,
                now,
                u64_to_sqlite(item.revision, "expected_item_revision")?,
            ],
        )?;
        insert_event(
            &transaction,
            EventInsert {
                graph_id: request.graph_id.as_str(),
                work_item_id: Some(request.work_item_id.as_str()),
                graph_revision: next_graph_revision,
                item_revision: Some(next_item_revision),
                event_type: "work_graph.item.reviewed",
                actor_principal: request.reviewer_principal.as_str(),
                from_state: Some(item.state.as_str()),
                to_state: Some(target_state.as_str()),
                reason_code: stable_reason,
                payload_json: json!({
                    "review_id": review_id,
                    "handoff_id": request.handoff_id,
                    "decision": request.decision,
                    "review_reason_code": request.reason_code,
                    "handoff_provenance_sha256": handoff.provenance_sha256,
                    "review_provenance_sha256": provenance_sha256,
                })
                .to_string()
                .as_str(),
                created_at_unix_ms: now,
            },
        )?;
        let _changed = project_dependency_states(
            &transaction,
            request.graph_id.as_str(),
            next_graph_revision,
            request.reviewer_principal.as_str(),
            now,
        )?;
        let graph_state = project_graph_state(&transaction, request.graph_id.as_str())?;
        transaction.execute(
            r#"
                UPDATE work_graphs
                SET state = ?2,
                    revision = ?3,
                    reason_code = ?4,
                    updated_at_unix_ms = ?5,
                    completed_at_unix_ms = CASE WHEN ?6 = 1 THEN ?5 ELSE NULL END
                WHERE graph_ulid = ?1
            "#,
            params![
                request.graph_id,
                graph_state.as_str(),
                u64_to_sqlite(next_graph_revision, "graph_revision")?,
                stable_reason,
                now,
                bool_to_sqlite(graph_state.is_terminal()),
            ],
        )?;
        transaction.commit()?;
        Ok(WorkGraphReviewOutcomeV1 {
            review: WorkGraphReviewRecordV1 {
                schema_version: WORK_GRAPH_SCHEMA_VERSION,
                review_id,
                graph_id: request.graph_id.clone(),
                work_item_id: request.work_item_id.clone(),
                handoff_id: request.handoff_id.clone(),
                reviewer_principal: request.reviewer_principal.clone(),
                decision: request.decision,
                reason_code: request.reason_code.clone(),
                evidence_refs: handoff.evidence_refs,
                provenance_sha256,
                created_at_unix_ms: now,
            },
            item_state: target_state,
            item_revision: next_item_revision,
            graph_revision: next_graph_revision,
        })
    }

    /// Retrieves one owner/session-scoped handoff without exposing a child transcript.
    pub(crate) fn work_item_handoff(
        &self,
        owner_principal: &str,
        owner_device_id: &str,
        owner_session_id: &str,
        graph_id: &str,
        handoff_id: &str,
    ) -> Result<Option<WorkItemHandoffEnvelopeV1>, JournalError> {
        ensure_nonempty_field(owner_principal, "owner_principal")?;
        ensure_nonempty_field(owner_device_id, "owner_device_id")?;
        ensure_nonempty_field(owner_session_id, "owner_session_id")?;
        ensure_nonempty_field(graph_id, "graph_id")?;
        ensure_nonempty_field(handoff_id, "handoff_id")?;
        let guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        let owned_graph = guard
            .query_row(
                r#"
                    SELECT graph_ulid
                    FROM work_graphs
                    WHERE graph_ulid = ?1
                      AND owner_principal = ?2
                      AND device_id = ?3
                      AND session_ulid = ?4
                "#,
                params![graph_id, owner_principal, owner_device_id, owner_session_id],
                |row| row.get::<_, String>(0),
            )
            .optional()?;
        if owned_graph.is_none() {
            return Ok(None);
        }
        Ok(query_handoff_by_id(&guard, handoff_id)?.filter(|handoff| handoff.graph_id == graph_id))
    }

    /// Lists bounded comments visible to the graph owner.
    pub(crate) fn work_graph_comments(
        &self,
        owner_principal: &str,
        graph_id: &str,
        work_item_id: Option<&str>,
        limit: usize,
    ) -> Result<Vec<WorkGraphCommentRecordV1>, JournalError> {
        let guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        let owner = guard
            .query_row(
                "SELECT owner_principal FROM work_graphs WHERE graph_ulid = ?1",
                params![graph_id],
                |row| row.get::<_, String>(0),
            )
            .optional()?;
        if owner.as_deref() != Some(owner_principal) {
            return Ok(Vec::new());
        }
        let limit = i64::try_from(limit.clamp(1, MAX_WORK_GRAPH_QUERY_RECORDS))
            .unwrap_or(MAX_WORK_GRAPH_QUERY_RECORDS as i64);
        let mut statement = guard.prepare(
            r#"
                SELECT seq, comment_ulid, graph_ulid, work_item_ulid,
                       author_principal, body, provenance_sha256, created_at_unix_ms
                FROM work_graph_comments
                WHERE graph_ulid = ?1
                  AND (?2 IS NULL OR work_item_ulid = ?2)
                ORDER BY seq ASC
                LIMIT ?3
            "#,
        )?;
        let rows = statement.query_map(params![graph_id, work_item_id, limit], |row| {
            Ok(WorkGraphCommentRecordV1 {
                schema_version: WORK_GRAPH_SCHEMA_VERSION,
                sequence: row.get::<_, i64>(0)?.max(0) as u64,
                comment_id: row.get(1)?,
                graph_id: row.get(2)?,
                work_item_id: row.get(3)?,
                author_principal: row.get(4)?,
                body: row.get(5)?,
                provenance_sha256: row.get(6)?,
                created_at_unix_ms: row.get(7)?,
            })
        })?;
        rows.collect::<Result<Vec<_>, _>>().map_err(Into::into)
    }

    /// Lists bounded owner-scoped graph headers and state counts.
    pub(crate) fn list_work_graphs_for_owner(
        &self,
        owner_principal: &str,
        session_id: Option<&str>,
        limit: usize,
    ) -> Result<Vec<WorkGraphListEntryV1>, JournalError> {
        ensure_nonempty_field(owner_principal, "owner_principal")?;
        let guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        let mut statement = guard.prepare(
            r#"
                SELECT graph_ulid, objective_id, state, revision, reason_code,
                       updated_at_unix_ms
                FROM work_graphs
                WHERE owner_principal = ?1
                  AND (?2 IS NULL OR session_ulid = ?2)
                ORDER BY updated_at_unix_ms DESC, graph_ulid ASC
                LIMIT ?3
            "#,
        )?;
        let limit = i64::try_from(limit.clamp(1, MAX_WORK_GRAPH_QUERY_RECORDS))
            .unwrap_or(MAX_WORK_GRAPH_QUERY_RECORDS as i64);
        let headers = statement
            .query_map(params![owner_principal, session_id, limit], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, Option<String>>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, i64>(3)?,
                    row.get::<_, String>(4)?,
                    row.get::<_, i64>(5)?,
                ))
            })?
            .collect::<Result<Vec<_>, _>>()?;
        let mut entries = Vec::with_capacity(headers.len());
        for (graph_id, objective_id, state, revision, reason_code, updated_at_unix_ms) in headers {
            let (item_count, ready_count, active_count, terminal_count) = guard.query_row(
                r#"
                    SELECT
                        COUNT(*),
                        COALESCE(SUM(CASE WHEN state = 'ready' THEN 1 ELSE 0 END), 0),
                        COALESCE(SUM(CASE
                            WHEN state IN ('claimed', 'running', 'waiting', 'review') THEN 1
                            ELSE 0 END), 0),
                        COALESCE(SUM(CASE
                            WHEN state IN ('succeeded', 'failed', 'cancelled', 'archived') THEN 1
                            ELSE 0 END), 0)
                    FROM work_graph_items
                    WHERE graph_ulid = ?1
                "#,
                params![graph_id],
                |row| {
                    Ok((
                        row.get::<_, i64>(0)?,
                        row.get::<_, i64>(1)?,
                        row.get::<_, i64>(2)?,
                        row.get::<_, i64>(3)?,
                    ))
                },
            )?;
            entries.push(WorkGraphListEntryV1 {
                schema_version: WORK_GRAPH_SCHEMA_VERSION,
                graph_id,
                objective_id,
                state: WorkGraphState::parse(state.as_str())
                    .ok_or_else(|| invalid_handoff("stored graph state is unknown"))?,
                revision: u64::try_from(revision)
                    .map_err(|_| invalid_handoff("stored graph revision is negative"))?,
                reason_code,
                item_count: count_to_u32(item_count),
                ready_item_count: count_to_u32(ready_count),
                active_item_count: count_to_u32(active_count),
                terminal_item_count: count_to_u32(terminal_count),
                updated_at_unix_ms,
            });
        }
        Ok(entries)
    }

    /// Builds a terminal parent-safe summary from immutable handoff projections.
    pub(crate) fn work_graph_terminal_summary(
        &self,
        owner_principal: &str,
        graph_id: &str,
    ) -> Result<Option<WorkGraphTerminalSummaryV1>, JournalError> {
        let guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        let Some(snapshot) = query_snapshot(&guard, graph_id)? else {
            return Ok(None);
        };
        if snapshot.graph.owner.principal != owner_principal || !snapshot.graph.state.is_terminal()
        {
            return Ok(None);
        }
        let mut statement = guard.prepare(
            r#"
                SELECT h.handoff_ulid, h.work_item_ulid, h.summary,
                       h.context_cost_tokens, h.evidence_refs_json, h.artifact_refs_json,
                       h.verification_state, h.provenance_sha256
                FROM work_graph_handoffs h
                INNER JOIN (
                    SELECT work_item_ulid, MAX(created_at_unix_ms) AS latest_at
                    FROM work_graph_handoffs
                    WHERE graph_ulid = ?1
                    GROUP BY work_item_ulid
                ) latest
                  ON latest.work_item_ulid = h.work_item_ulid
                 AND latest.latest_at = h.created_at_unix_ms
                WHERE h.graph_ulid = ?1
                ORDER BY h.work_item_ulid ASC, h.handoff_ulid DESC
                LIMIT ?2
            "#,
        )?;
        let handoffs = statement
            .query_map(params![graph_id, MAX_WORK_GRAPH_QUERY_RECORDS as i64], map_handoff_summary)?
            .collect::<Result<Vec<_>, _>>()?;
        let succeeded_item_count =
            snapshot.items.iter().filter(|item| item.state == WorkItemState::Succeeded).count();
        Ok(Some(WorkGraphTerminalSummaryV1 {
            schema_version: WORK_GRAPH_SCHEMA_VERSION,
            graph_id: graph_id.to_owned(),
            graph_revision: snapshot.graph.revision,
            state: snapshot.graph.state,
            reason_code: snapshot.graph.reason_code,
            objective_id: snapshot.graph.objective_id,
            flow_id: snapshot.graph.flow_id,
            flow_step_id: snapshot.graph.flow_step_id,
            item_count: u32::try_from(snapshot.items.len()).unwrap_or(u32::MAX),
            succeeded_item_count: u32::try_from(succeeded_item_count).unwrap_or(u32::MAX),
            total_context_cost_tokens: handoffs
                .iter()
                .map(|handoff| u64::from(handoff.context_cost_tokens))
                .sum(),
            handoffs,
        }))
    }
}

struct NormalizedHandoff {
    summary: String,
    structured_result_json: String,
    context_cost_tokens: u32,
    evidence_refs: Vec<String>,
    artifact_refs: Vec<String>,
    provenance_sha256: String,
}

fn normalize_handoff_request(
    request: &WorkItemHandoffCreateRequest,
) -> Result<NormalizedHandoff, JournalError> {
    for (field, value) in [
        ("graph_id", request.authority.graph_id.as_str()),
        ("work_item_id", request.authority.work_item_id.as_str()),
        ("worker_id", request.authority.worker_id.as_str()),
        ("actor_principal", request.actor_principal.as_str()),
    ] {
        ensure_nonempty_field(value, field)?;
    }
    if request.summary.trim().is_empty() || request.summary.len() > MAX_WORK_HANDOFF_SUMMARY_BYTES {
        return Err(invalid_handoff("handoff summary is empty or exceeds the host bound"));
    }
    let summary = sanitize_object_text_field("summary", request.summary.trim())?;
    let raw_result = serde_json::to_vec(&request.structured_result)?;
    if raw_result.len() > MAX_WORK_HANDOFF_RESULT_BYTES {
        return Err(invalid_handoff("structured result exceeds the host bound"));
    }
    let structured_result_json = sanitize_payload(raw_result.as_slice())?.0;
    if structured_result_json.len() > MAX_WORK_HANDOFF_RESULT_BYTES {
        return Err(invalid_handoff("redacted structured result exceeds the host bound"));
    }
    let evidence_refs = normalize_refs(request.evidence_refs.as_slice(), "evidence")?;
    let artifact_refs = normalize_refs(request.artifact_refs.as_slice(), "artifact")?;
    let context_bytes = summary
        .len()
        .saturating_add(structured_result_json.len())
        .saturating_add(evidence_refs.iter().map(String::len).sum::<usize>())
        .saturating_add(artifact_refs.iter().map(String::len).sum::<usize>());
    let context_cost_tokens =
        u32::try_from(context_bytes.saturating_add(3) / 4).unwrap_or(u32::MAX);
    let provenance_sha256 = digest_json(&json!({
        "graph_id": request.authority.graph_id,
        "work_item_id": request.authority.work_item_id,
        "claim_generation": request.authority.generation,
        "summary": summary,
        "structured_result_json": structured_result_json,
        "evidence_refs": evidence_refs,
        "artifact_refs": artifact_refs,
        "verification_state": request.verification_state,
    }))?;
    Ok(NormalizedHandoff {
        summary,
        structured_result_json,
        context_cost_tokens,
        evidence_refs,
        artifact_refs,
        provenance_sha256,
    })
}

fn normalize_refs(values: &[String], kind: &str) -> Result<Vec<String>, JournalError> {
    if values.len() > MAX_WORK_HANDOFF_REFS {
        return Err(invalid_handoff(format!("too many {kind} references").as_str()));
    }
    let mut normalized = Vec::with_capacity(values.len());
    for value in values {
        let value = value.trim();
        if value.is_empty() || value.len() > MAX_WORK_HANDOFF_REF_BYTES {
            return Err(invalid_handoff(
                format!("{kind} reference is empty or exceeds the host bound").as_str(),
            ));
        }
        normalized.push(sanitize_object_text_field("reference", value)?);
    }
    normalized.sort();
    normalized.dedup();
    Ok(normalized)
}

fn validate_review_request(request: &WorkGraphReviewRequest) -> Result<(), JournalError> {
    for (field, value) in [
        ("graph_id", request.graph_id.as_str()),
        ("work_item_id", request.work_item_id.as_str()),
        ("handoff_id", request.handoff_id.as_str()),
        ("reviewer_principal", request.reviewer_principal.as_str()),
        ("reason_code", request.reason_code.as_str()),
    ] {
        ensure_nonempty_field(value, field)?;
    }
    if request.reason_code.len() > 256 {
        return Err(invalid_handoff("review reason code exceeds the host bound"));
    }
    Ok(())
}

fn query_handoff_for_generation(
    connection: &Connection,
    graph_id: &str,
    work_item_id: &str,
    generation: u64,
) -> Result<Option<WorkItemHandoffEnvelopeV1>, JournalError> {
    connection
        .query_row(
            r#"
                SELECT handoff_ulid, graph_ulid, work_item_ulid, claim_generation,
                       summary, structured_result_json, context_cost_tokens,
                       evidence_refs_json, artifact_refs_json, verification_state,
                       provenance_sha256, created_at_unix_ms
                FROM work_graph_handoffs
                WHERE graph_ulid = ?1 AND work_item_ulid = ?2 AND claim_generation = ?3
                ORDER BY created_at_unix_ms DESC, handoff_ulid DESC
                LIMIT 1
            "#,
            params![graph_id, work_item_id, u64_to_sqlite(generation, "claim_generation")?],
            map_handoff,
        )
        .optional()
        .map_err(Into::into)
}

fn query_handoff_by_id(
    connection: &Connection,
    handoff_id: &str,
) -> Result<Option<WorkItemHandoffEnvelopeV1>, JournalError> {
    connection
        .query_row(
            r#"
                SELECT handoff_ulid, graph_ulid, work_item_ulid, claim_generation,
                       summary, structured_result_json, context_cost_tokens,
                       evidence_refs_json, artifact_refs_json, verification_state,
                       provenance_sha256, created_at_unix_ms
                FROM work_graph_handoffs
                WHERE handoff_ulid = ?1
            "#,
            params![handoff_id],
            map_handoff,
        )
        .optional()
        .map_err(Into::into)
}

fn map_handoff(row: &Row<'_>) -> rusqlite::Result<WorkItemHandoffEnvelopeV1> {
    let generation = row.get::<_, i64>(3)?;
    let context_cost_tokens = row.get::<_, i64>(6)?;
    let verification = row.get::<_, String>(9)?;
    Ok(WorkItemHandoffEnvelopeV1 {
        schema_version: WORK_GRAPH_SCHEMA_VERSION,
        handoff_id: row.get(0)?,
        graph_id: row.get(1)?,
        work_item_id: row.get(2)?,
        claim_generation: generation.max(0) as u64,
        summary: row.get(4)?,
        structured_result: serde_json::from_str(row.get::<_, String>(5)?.as_str()).map_err(
            |error| {
                rusqlite::Error::FromSqlConversionFailure(
                    5,
                    rusqlite::types::Type::Text,
                    Box::new(error),
                )
            },
        )?,
        context_cost_tokens: u32::try_from(context_cost_tokens.max(0)).unwrap_or(u32::MAX),
        evidence_refs: decode_json_column(row, 7)?,
        artifact_refs: decode_json_column(row, 8)?,
        verification_state: WorkVerificationState::parse(verification.as_str()).ok_or_else(
            || {
                rusqlite::Error::FromSqlConversionFailure(
                    9,
                    rusqlite::types::Type::Text,
                    format!("unknown work verification state {verification}").into(),
                )
            },
        )?,
        provenance_sha256: row.get(10)?,
        created_at_unix_ms: row.get(11)?,
    })
}

fn map_handoff_summary(row: &Row<'_>) -> rusqlite::Result<WorkItemHandoffSummaryV1> {
    let verification = row.get::<_, String>(6)?;
    Ok(WorkItemHandoffSummaryV1 {
        handoff_id: row.get(0)?,
        work_item_id: row.get(1)?,
        summary: row.get(2)?,
        context_cost_tokens: u32::try_from(row.get::<_, i64>(3)?.max(0)).unwrap_or(u32::MAX),
        evidence_refs: decode_json_column(row, 4)?,
        artifact_refs: decode_json_column(row, 5)?,
        verification_state: WorkVerificationState::parse(verification.as_str()).ok_or_else(
            || {
                rusqlite::Error::FromSqlConversionFailure(
                    6,
                    rusqlite::types::Type::Text,
                    format!("unknown work verification state {verification}").into(),
                )
            },
        )?,
        provenance_sha256: row.get(7)?,
    })
}

fn decode_json_column<T: DeserializeOwned>(row: &Row<'_>, index: usize) -> rusqlite::Result<T> {
    let raw = row.get::<_, String>(index)?;
    serde_json::from_str(raw.as_str()).map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(
            index,
            rusqlite::types::Type::Text,
            Box::new(error),
        )
    })
}

fn digest_json(value: &Value) -> Result<String, JournalError> {
    Ok(hex::encode(Sha256::digest(serde_json::to_vec(value)?)))
}

fn count_to_u32(value: i64) -> u32 {
    u32::try_from(value.max(0)).unwrap_or(u32::MAX)
}

fn invalid_handoff(message: &str) -> JournalError {
    JournalError::InvalidWorkGraph {
        reason_code: "work_graph.handoff.invalid".to_owned(),
        message: message.to_owned(),
    }
}
