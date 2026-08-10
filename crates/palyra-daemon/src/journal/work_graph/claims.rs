//! Atomic work-item claims, generation-fenced heartbeats, reclaim, and settlement.

use crate::domain::work_graph::{
    claim_reason, concurrency_reason, validate_loaded_graph, validate_transition,
    ClaimReadyWorkItemOutcome, ClaimReadyWorkItemRequest, StaleReclaimDecision,
    StaleReclaimRequest, WorkClaimAuthority, WorkClaimSettlementOutcome,
    WorkClaimSettlementRequest, WorkClaimToken, WorkGraphClaimDiagnosticsV1, WorkItemClaimGrant,
    WorkItemClaimV1, WorkItemHeartbeatOutcome, WorkItemHeartbeatRequest, WorkItemRecordV1,
    WorkItemSideEffectFenceOutcome, WorkItemSideEffectFenceRequest, WorkItemState,
    WorkResourceClass, WorkRuntimeLiveness, WorkSideEffectFenceState, MAX_WORK_CLAIM_TTL_MS,
    MIN_WORK_CLAIM_TTL_MS,
};

use super::{
    storage::{
        insert_event, project_dependency_states, project_graph_state, query_snapshot,
        validation_error, EventInsert,
    },
    *,
};

const MAX_ORPHAN_RESULTS_PER_ITEM: i64 = 64;

impl JournalStore {
    /// Claims the highest-priority eligible item with one SQLite compare-and-set transaction.
    pub(crate) fn claim_ready_work_item(
        &self,
        request: &ClaimReadyWorkItemRequest,
    ) -> Result<ClaimReadyWorkItemOutcome, JournalError> {
        validate_claim_request(request)?;
        let now = current_unix_ms()?;
        let mut guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        let transaction = guard.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let snapshot =
            query_snapshot(&transaction, request.graph_id.as_str())?.ok_or_else(|| {
                JournalError::WorkGraphNotFound { graph_id: request.graph_id.clone() }
            })?;
        validate_loaded_graph(&snapshot.graph, snapshot.items.as_slice())
            .map_err(validation_error)?;
        if snapshot.graph.owner.principal != request.authorized_owner_principal {
            return Ok(ClaimReadyWorkItemOutcome::NoEligibleItem {
                reason_code: claim_reason::POLICY_SCOPE_MISMATCH,
            });
        }

        let mut denial_reason = claim_reason::NO_READY_ITEM;
        let mut candidate = None;
        for item in &snapshot.items {
            if item.state != WorkItemState::Ready
                || request
                    .work_item_id
                    .as_ref()
                    .is_some_and(|requested| requested != &item.work_item_id)
                || request.expected_item_revision.is_some_and(|revision| revision != item.revision)
            {
                continue;
            }
            if !request.capability_profiles.contains(item.capability_profile.as_str()) {
                denial_reason = claim_reason::CAPABILITY_MISMATCH;
                continue;
            }
            if let Some(reason_code) = concurrency_denial_reason(
                item,
                snapshot.items.as_slice(),
                &snapshot.graph.concurrency_policy,
                request,
                now,
            ) {
                denial_reason = reason_code;
                continue;
            }
            candidate = Some(item);
            break;
        }
        let Some(candidate) = candidate else {
            return Ok(ClaimReadyWorkItemOutcome::NoEligibleItem { reason_code: denial_reason });
        };

        let (previous_generation, previous_attempt_count) = transaction.query_row(
            r#"
                SELECT claim_generation, attempt_count
                FROM work_graph_items
                WHERE graph_ulid = ?1 AND work_item_ulid = ?2
            "#,
            params![request.graph_id, candidate.work_item_id],
            |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?)),
        )?;
        let generation = u64::try_from(previous_generation)
            .map_err(|_| invalid_claim_data("negative durable claim generation"))?
            .saturating_add(1);
        let attempt_count = u64::try_from(previous_attempt_count)
            .map_err(|_| invalid_claim_data("negative durable attempt count"))?
            .saturating_add(1);
        let token = WorkClaimToken::issue()
            .map_err(|_| invalid_claim_data("operating-system entropy unavailable"))?;
        let token_sha256 = token.sha256_hex();
        let attempt_id = Ulid::new().to_string();
        let expires_at = now.saturating_add(
            i64::try_from(request.lease_ttl_ms)
                .map_err(|_| invalid_claim_data("lease duration exceeds i64"))?,
        );
        let next_item_revision = candidate.revision.saturating_add(1);
        let next_graph_revision = snapshot.graph.revision.saturating_add(1);
        let updated = transaction.execute(
            r#"
                UPDATE work_graph_items
                SET state = ?3,
                    claim_token_sha256 = ?4,
                    claim_worker_id = ?5,
                    claim_worker_principal = ?6,
                    claim_generation = ?7,
                    claim_attempt_ulid = ?8,
                    claim_runtime_instance_id = ?9,
                    claim_process_start_token = ?10,
                    claim_issued_at_unix_ms = ?11,
                    claim_expires_at_unix_ms = ?12,
                    claim_heartbeat_at_unix_ms = ?11,
                    side_effect_fence_state = ?13,
                    attempt_count = ?14,
                    resource_lease_id = ?15,
                    revision = ?16,
                    reason_code = ?17,
                    updated_at_unix_ms = ?11
                WHERE graph_ulid = ?1
                  AND work_item_ulid = ?2
                  AND state = 'ready'
                  AND revision = ?18
            "#,
            params![
                request.graph_id,
                candidate.work_item_id,
                WorkItemState::Claimed.as_str(),
                token_sha256,
                request.worker_id,
                request.worker_principal,
                u64_to_sqlite(generation, "claim_generation")?,
                attempt_id,
                request.runtime_instance_id,
                request.process_start_token,
                now,
                expires_at,
                WorkSideEffectFenceState::Clear.as_str(),
                u64_to_sqlite(attempt_count, "attempt_count")?,
                request.resource_lease_id,
                u64_to_sqlite(next_item_revision, "work_item_revision")?,
                claim_reason::CLAIMED,
                u64_to_sqlite(candidate.revision, "expected_item_revision")?,
            ],
        )?;
        if updated != 1 {
            return Ok(ClaimReadyWorkItemOutcome::NoEligibleItem {
                reason_code: claim_reason::RACE_LOST,
            });
        }
        update_graph_revision(
            &transaction,
            request.graph_id.as_str(),
            next_graph_revision,
            claim_reason::CLAIMED,
            now,
        )?;
        let event_payload = json!({
            "worker_id": request.worker_id,
            "generation": generation,
            "attempt_id": attempt_id,
            "runtime_instance_id": request.runtime_instance_id,
            "expires_at_unix_ms": expires_at,
        })
        .to_string();
        insert_event(
            &transaction,
            EventInsert {
                graph_id: request.graph_id.as_str(),
                work_item_id: Some(candidate.work_item_id.as_str()),
                graph_revision: next_graph_revision,
                item_revision: Some(next_item_revision),
                event_type: "work_graph.item.claimed",
                actor_principal: request.worker_principal.as_str(),
                from_state: Some(candidate.state.as_str()),
                to_state: Some(WorkItemState::Claimed.as_str()),
                reason_code: claim_reason::CLAIMED,
                payload_json: event_payload.as_str(),
                created_at_unix_ms: now,
            },
        )?;
        transaction.commit()?;
        drop(guard);
        let snapshot = self.work_graph_snapshot(request.graph_id.as_str())?.ok_or_else(|| {
            JournalError::WorkGraphNotFound { graph_id: request.graph_id.clone() }
        })?;
        let item = snapshot
            .items
            .into_iter()
            .find(|item| item.work_item_id == candidate.work_item_id)
            .ok_or_else(|| JournalError::WorkGraphItemNotFound {
                graph_id: request.graph_id.clone(),
                work_item_id: candidate.work_item_id.clone(),
            })?;
        let claim =
            item.claim.clone().ok_or_else(|| invalid_claim_data("claim projection lost"))?;
        Ok(ClaimReadyWorkItemOutcome::Granted(Box::new(WorkItemClaimGrant { item, claim, token })))
    }

    /// Renews a claim only when token, worker, and generation still match.
    pub(crate) fn heartbeat_work_item(
        &self,
        request: &WorkItemHeartbeatRequest,
    ) -> Result<WorkItemHeartbeatOutcome, JournalError> {
        validate_authority(&request.authority)?;
        validate_ttl(request.extend_by_ms)?;
        let now = current_unix_ms()?;
        let mut guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        let transaction = guard.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let snapshot = query_snapshot(&transaction, request.authority.graph_id.as_str())?
            .ok_or_else(|| JournalError::WorkGraphNotFound {
                graph_id: request.authority.graph_id.clone(),
            })?;
        let Some(item) =
            snapshot.items.iter().find(|item| item.work_item_id == request.authority.work_item_id)
        else {
            return Err(JournalError::WorkGraphItemNotFound {
                graph_id: request.authority.graph_id.clone(),
                work_item_id: request.authority.work_item_id.clone(),
            });
        };
        let Some(claim) = item.claim.as_ref() else {
            return Ok(WorkItemHeartbeatOutcome::StaleAuthority {
                reason_code: claim_reason::HEARTBEAT_STALE,
            });
        };
        if !authority_matches(&request.authority, claim) {
            return Ok(WorkItemHeartbeatOutcome::StaleAuthority {
                reason_code: claim_reason::HEARTBEAT_STALE,
            });
        }
        if claim.expires_at_unix_ms <= now {
            return Ok(WorkItemHeartbeatOutcome::Expired {
                reason_code: claim_reason::HEARTBEAT_EXPIRED,
            });
        }
        let extension = i64::try_from(request.extend_by_ms)
            .map_err(|_| invalid_claim_data("heartbeat extension exceeds i64"))?;
        let runtime_deadline = claim.issued_at_unix_ms.saturating_add(
            i64::try_from(item.max_runtime_ms)
                .map_err(|_| invalid_claim_data("max runtime exceeds i64"))?,
        );
        let next_expiry =
            claim.expires_at_unix_ms.max(now).saturating_add(extension).min(runtime_deadline);
        if next_expiry <= now {
            return Ok(WorkItemHeartbeatOutcome::Expired {
                reason_code: claim_reason::HEARTBEAT_EXPIRED,
            });
        }
        let next_item_revision = item.revision.saturating_add(1);
        let next_graph_revision = snapshot.graph.revision.saturating_add(1);
        let updated = transaction.execute(
            r#"
                UPDATE work_graph_items
                SET claim_expires_at_unix_ms = ?3,
                    claim_heartbeat_at_unix_ms = ?4,
                    revision = ?5,
                    reason_code = ?6,
                    updated_at_unix_ms = ?4
                WHERE graph_ulid = ?1
                  AND work_item_ulid = ?2
                  AND claim_token_sha256 = ?7
                  AND claim_worker_id = ?8
                  AND claim_generation = ?9
                  AND revision = ?10
            "#,
            params![
                request.authority.graph_id,
                request.authority.work_item_id,
                next_expiry,
                now,
                u64_to_sqlite(next_item_revision, "work_item_revision")?,
                claim_reason::HEARTBEAT_RENEWED,
                request.authority.token.sha256_hex(),
                request.authority.worker_id,
                u64_to_sqlite(request.authority.generation, "claim_generation")?,
                u64_to_sqlite(item.revision, "expected_item_revision")?,
            ],
        )?;
        if updated != 1 {
            return Ok(WorkItemHeartbeatOutcome::StaleAuthority {
                reason_code: claim_reason::HEARTBEAT_STALE,
            });
        }
        update_graph_revision(
            &transaction,
            request.authority.graph_id.as_str(),
            next_graph_revision,
            claim_reason::HEARTBEAT_RENEWED,
            now,
        )?;
        let event_payload = json!({
            "worker_id": claim.worker_id,
            "generation": claim.generation,
            "expires_at_unix_ms": next_expiry,
        })
        .to_string();
        insert_event(
            &transaction,
            EventInsert {
                graph_id: request.authority.graph_id.as_str(),
                work_item_id: Some(request.authority.work_item_id.as_str()),
                graph_revision: next_graph_revision,
                item_revision: Some(next_item_revision),
                event_type: "work_graph.item.heartbeat",
                actor_principal: claim.worker_principal.as_str(),
                from_state: Some(item.state.as_str()),
                to_state: Some(item.state.as_str()),
                reason_code: claim_reason::HEARTBEAT_RENEWED,
                payload_json: event_payload.as_str(),
                created_at_unix_ms: now,
            },
        )?;
        transaction.commit()?;
        drop(guard);
        let updated = self
            .work_graph_snapshot(request.authority.graph_id.as_str())?
            .and_then(|snapshot| {
                snapshot
                    .items
                    .into_iter()
                    .find(|candidate| candidate.work_item_id == request.authority.work_item_id)
            })
            .and_then(|item| item.claim)
            .ok_or_else(|| invalid_claim_data("renewed claim projection lost"))?;
        Ok(WorkItemHeartbeatOutcome::Renewed(updated))
    }

    /// Records generation-fenced side-effect knowledge before a worker mutates external state.
    pub(crate) fn record_work_item_side_effect_fence(
        &self,
        request: &WorkItemSideEffectFenceRequest,
    ) -> Result<WorkItemSideEffectFenceOutcome, JournalError> {
        validate_authority(&request.authority)?;
        ensure_nonempty_field(request.actor_principal.as_str(), "actor_principal")?;
        let now = current_unix_ms()?;
        let mut guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        let transaction = guard.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let snapshot = query_snapshot(&transaction, request.authority.graph_id.as_str())?
            .ok_or_else(|| JournalError::WorkGraphNotFound {
                graph_id: request.authority.graph_id.clone(),
            })?;
        let Some(item) =
            snapshot.items.iter().find(|item| item.work_item_id == request.authority.work_item_id)
        else {
            return Err(JournalError::WorkGraphItemNotFound {
                graph_id: request.authority.graph_id.clone(),
                work_item_id: request.authority.work_item_id.clone(),
            });
        };
        let Some(claim) = item.claim.as_ref() else {
            return Ok(WorkItemSideEffectFenceOutcome::StaleAuthority {
                reason_code: claim_reason::SIDE_EFFECT_FENCE_STALE,
            });
        };
        if item.revision != request.expected_item_revision
            || !authority_matches(&request.authority, claim)
            || claim.expires_at_unix_ms <= now
        {
            return Ok(WorkItemSideEffectFenceOutcome::StaleAuthority {
                reason_code: claim_reason::SIDE_EFFECT_FENCE_STALE,
            });
        }
        if !valid_side_effect_fence_transition(claim.side_effect_fence, request.state) {
            return Err(invalid_claim_data("side-effect fence cannot move to a less safe state"));
        }

        let next_item_revision = item.revision.saturating_add(1);
        let next_graph_revision = snapshot.graph.revision.saturating_add(1);
        let updated = transaction.execute(
            r#"
                UPDATE work_graph_items
                SET side_effect_fence_state = ?3,
                    revision = ?4,
                    reason_code = ?5,
                    updated_at_unix_ms = ?6
                WHERE graph_ulid = ?1
                  AND work_item_ulid = ?2
                  AND revision = ?7
                  AND claim_token_sha256 = ?8
                  AND claim_worker_id = ?9
                  AND claim_generation = ?10
                  AND claim_expires_at_unix_ms > ?11
            "#,
            params![
                request.authority.graph_id,
                request.authority.work_item_id,
                request.state.as_str(),
                u64_to_sqlite(next_item_revision, "work_item_revision")?,
                claim_reason::SIDE_EFFECT_FENCE_UPDATED,
                now,
                u64_to_sqlite(request.expected_item_revision, "expected_item_revision")?,
                request.authority.token.sha256_hex(),
                request.authority.worker_id,
                u64_to_sqlite(request.authority.generation, "claim_generation")?,
                now,
            ],
        )?;
        if updated != 1 {
            return Ok(WorkItemSideEffectFenceOutcome::StaleAuthority {
                reason_code: claim_reason::SIDE_EFFECT_FENCE_STALE,
            });
        }
        update_graph_revision(
            &transaction,
            request.authority.graph_id.as_str(),
            next_graph_revision,
            claim_reason::SIDE_EFFECT_FENCE_UPDATED,
            now,
        )?;
        let event_payload = json!({
            "generation": request.authority.generation,
            "side_effect_fence": request.state,
        })
        .to_string();
        insert_event(
            &transaction,
            EventInsert {
                graph_id: request.authority.graph_id.as_str(),
                work_item_id: Some(request.authority.work_item_id.as_str()),
                graph_revision: next_graph_revision,
                item_revision: Some(next_item_revision),
                event_type: "work_graph.item.side_effect_fence_updated",
                actor_principal: request.actor_principal.as_str(),
                from_state: Some(item.state.as_str()),
                to_state: Some(item.state.as_str()),
                reason_code: claim_reason::SIDE_EFFECT_FENCE_UPDATED,
                payload_json: event_payload.as_str(),
                created_at_unix_ms: now,
            },
        )?;
        transaction.commit()?;
        drop(guard);
        let claim = load_item(
            self,
            request.authority.graph_id.as_str(),
            request.authority.work_item_id.as_str(),
        )?
        .claim
        .ok_or_else(|| invalid_claim_data("updated side-effect fence projection lost"))?;
        Ok(WorkItemSideEffectFenceOutcome::Updated(claim))
    }

    /// Reclaims one expired generation only after liveness and side-effect evidence are known.
    pub(crate) fn reclaim_stale_work_item(
        &self,
        request: &StaleReclaimRequest,
    ) -> Result<StaleReclaimDecision, JournalError> {
        validate_reclaim_request(request)?;
        let now = current_unix_ms()?;
        let mut guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        let transaction = guard.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let snapshot =
            query_snapshot(&transaction, request.graph_id.as_str())?.ok_or_else(|| {
                JournalError::WorkGraphNotFound { graph_id: request.graph_id.clone() }
            })?;
        let Some(item) =
            snapshot.items.iter().find(|item| item.work_item_id == request.work_item_id)
        else {
            return Err(JournalError::WorkGraphItemNotFound {
                graph_id: request.graph_id.clone(),
                work_item_id: request.work_item_id.clone(),
            });
        };
        let Some(claim) = item.claim.as_ref() else {
            return Ok(StaleReclaimDecision::LostRace { reason_code: claim_reason::RACE_LOST });
        };
        if item.revision != request.expected_item_revision
            || claim.generation != request.expected_generation
            || claim.runtime_instance_id != request.runtime_instance_id
            || claim.process_start_token != request.process_start_token
        {
            return Ok(StaleReclaimDecision::LostRace { reason_code: claim_reason::RACE_LOST });
        }
        if claim.expires_at_unix_ms > now {
            return Ok(StaleReclaimDecision::NotExpired {
                reason_code: claim_reason::RECLAIM_NOT_EXPIRED,
            });
        }
        if request.liveness == WorkRuntimeLiveness::Alive {
            return Ok(StaleReclaimDecision::DeferredLive {
                reason_code: claim_reason::RECLAIM_DEFERRED_LIVE,
            });
        }
        let requires_review = request.liveness == WorkRuntimeLiveness::Unknown
            || request.observed_side_effect_fence != WorkSideEffectFenceState::Clear
            || claim.side_effect_fence != WorkSideEffectFenceState::Clear;
        let (target_state, reason_code) = if requires_review {
            (
                WorkItemState::Review,
                if request.liveness == WorkRuntimeLiveness::Unknown {
                    claim_reason::RECLAIM_LIVENESS_UNKNOWN
                } else {
                    claim_reason::RECLAIM_SIDE_EFFECT_UNKNOWN
                },
            )
        } else {
            (
                WorkItemState::Ready,
                if request.liveness == WorkRuntimeLiveness::ProcessIdentityReused {
                    claim_reason::RECLAIMED_PID_REUSE
                } else {
                    claim_reason::RECLAIMED_DEAD
                },
            )
        };
        let next_item_revision = item.revision.saturating_add(1);
        let next_graph_revision = snapshot.graph.revision.saturating_add(1);
        let updated = transaction.execute(
            r#"
                UPDATE work_graph_items
                SET state = ?3,
                    claim_token_sha256 = NULL,
                    claim_worker_id = NULL,
                    claim_worker_principal = NULL,
                    claim_attempt_ulid = NULL,
                    claim_runtime_instance_id = NULL,
                    claim_process_start_token = NULL,
                    claim_issued_at_unix_ms = NULL,
                    claim_expires_at_unix_ms = NULL,
                    claim_heartbeat_at_unix_ms = NULL,
                    resource_lease_id = NULL,
                    side_effect_fence_state = ?4,
                    revision = ?5,
                    reason_code = ?6,
                    updated_at_unix_ms = ?7
                WHERE graph_ulid = ?1
                  AND work_item_ulid = ?2
                  AND revision = ?8
                  AND claim_generation = ?9
            "#,
            params![
                request.graph_id,
                request.work_item_id,
                target_state.as_str(),
                request.observed_side_effect_fence.as_str(),
                u64_to_sqlite(next_item_revision, "work_item_revision")?,
                reason_code,
                now,
                u64_to_sqlite(request.expected_item_revision, "expected_item_revision")?,
                u64_to_sqlite(request.expected_generation, "claim_generation")?,
            ],
        )?;
        if updated != 1 {
            return Ok(StaleReclaimDecision::LostRace { reason_code: claim_reason::RACE_LOST });
        }
        update_graph_revision(
            &transaction,
            request.graph_id.as_str(),
            next_graph_revision,
            reason_code,
            now,
        )?;
        let event_payload = json!({
            "generation": request.expected_generation,
            "liveness": format!("{:?}", request.liveness).to_ascii_lowercase(),
            "side_effect_fence": request.observed_side_effect_fence,
        })
        .to_string();
        insert_event(
            &transaction,
            EventInsert {
                graph_id: request.graph_id.as_str(),
                work_item_id: Some(request.work_item_id.as_str()),
                graph_revision: next_graph_revision,
                item_revision: Some(next_item_revision),
                event_type: "work_graph.item.reclaim_decided",
                actor_principal: request.actor_principal.as_str(),
                from_state: Some(item.state.as_str()),
                to_state: Some(target_state.as_str()),
                reason_code,
                payload_json: event_payload.as_str(),
                created_at_unix_ms: now,
            },
        )?;
        transaction.commit()?;
        drop(guard);
        let item = load_item(self, request.graph_id.as_str(), request.work_item_id.as_str())?;
        if requires_review {
            Ok(StaleReclaimDecision::RequiresReview { item, reason_code })
        } else {
            Ok(StaleReclaimDecision::Reclaimed { item, reason_code })
        }
    }

    /// Applies a current-generation result or records bounded orphan evidence for a stale result.
    pub(crate) fn settle_work_item_claim(
        &self,
        request: &WorkClaimSettlementRequest,
    ) -> Result<WorkClaimSettlementOutcome, JournalError> {
        validate_settlement_request(request)?;
        let now = current_unix_ms()?;
        let mut guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        let transaction = guard.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let snapshot = query_snapshot(&transaction, request.authority.graph_id.as_str())?
            .ok_or_else(|| JournalError::WorkGraphNotFound {
                graph_id: request.authority.graph_id.clone(),
            })?;
        let Some(item) =
            snapshot.items.iter().find(|item| item.work_item_id == request.authority.work_item_id)
        else {
            return Err(JournalError::WorkGraphItemNotFound {
                graph_id: request.authority.graph_id.clone(),
                work_item_id: request.authority.work_item_id.clone(),
            });
        };
        let authority_current = item.claim.as_ref().is_some_and(|claim| {
            authority_matches(&request.authority, claim) && claim.expires_at_unix_ms > now
        });
        if !authority_current {
            record_orphan_result(&transaction, request, item.claim.as_ref(), now)?;
            transaction.commit()?;
            return Ok(WorkClaimSettlementOutcome::Orphaned {
                reason_code: claim_reason::LATE_RESULT_ORPHANED,
            });
        }
        if item.revision != request.expected_item_revision {
            return Err(JournalError::WorkGraphRevisionConflict {
                graph_id: request.authority.graph_id.clone(),
                work_item_id: request.authority.work_item_id.clone(),
                expected_revision: request.expected_item_revision,
                actual_revision: item.revision,
            });
        }
        validate_transition(
            item.state,
            item.verification_state,
            request.target_state,
            request.verification_state,
        )
        .map_err(validation_error)?;
        let failure_count = if request.target_state == WorkItemState::Failed {
            item.failure_circuit.consecutive_failures.saturating_add(1)
        } else {
            0
        };
        let circuit_open = request.target_state == WorkItemState::Failed
            && failure_count >= item.failure_circuit.failure_limit;
        let retry_scheduled = request.target_state == WorkItemState::Failed && !circuit_open;
        let effective_state =
            if retry_scheduled { WorkItemState::Ready } else { request.target_state };
        let effective_reason = if retry_scheduled {
            concurrency_reason::RETRY_BACKOFF
        } else if circuit_open {
            concurrency_reason::CIRCUIT_OPEN
        } else {
            request.reason_code.as_str()
        };
        let retry_not_before = retry_scheduled.then(|| {
            now.saturating_add(retry_backoff_ms(&snapshot.graph.concurrency_policy, failure_count))
        });
        let circuit_opened_at = circuit_open.then_some(now);
        let failure_reason =
            (request.target_state == WorkItemState::Failed).then(|| request.reason_code.clone());
        let next_item_revision = item.revision.saturating_add(1);
        let next_graph_revision = snapshot.graph.revision.saturating_add(1);
        let completed_at = effective_state.is_terminal().then_some(now);
        let updated = transaction.execute(
            r#"
                UPDATE work_graph_items
                SET state = ?3,
                    verification_state = ?4,
                    claim_token_sha256 = NULL,
                    claim_worker_id = NULL,
                    claim_worker_principal = NULL,
                    claim_attempt_ulid = NULL,
                    claim_runtime_instance_id = NULL,
                    claim_process_start_token = NULL,
                    claim_issued_at_unix_ms = NULL,
                    claim_expires_at_unix_ms = NULL,
                    claim_heartbeat_at_unix_ms = NULL,
                    resource_lease_id = NULL,
                    side_effect_fence_state = ?5,
                    consecutive_failure_count = ?6,
                    retry_not_before_unix_ms = ?7,
                    circuit_opened_at_unix_ms = ?8,
                    failure_reason_code = ?9,
                    revision = ?10,
                    reason_code = ?11,
                    updated_at_unix_ms = ?12,
                    completed_at_unix_ms = ?13
                WHERE graph_ulid = ?1
                  AND work_item_ulid = ?2
                  AND revision = ?14
                  AND claim_token_sha256 = ?15
                  AND claim_generation = ?16
                  AND claim_expires_at_unix_ms > ?17
            "#,
            params![
                request.authority.graph_id,
                request.authority.work_item_id,
                effective_state.as_str(),
                request.verification_state.as_str(),
                WorkSideEffectFenceState::Committed.as_str(),
                i64::from(failure_count),
                retry_not_before,
                circuit_opened_at,
                failure_reason,
                u64_to_sqlite(next_item_revision, "work_item_revision")?,
                effective_reason,
                now,
                completed_at,
                u64_to_sqlite(request.expected_item_revision, "expected_item_revision")?,
                request.authority.token.sha256_hex(),
                u64_to_sqlite(request.authority.generation, "claim_generation")?,
                now,
            ],
        )?;
        if updated != 1 {
            record_orphan_result(&transaction, request, item.claim.as_ref(), now)?;
            transaction.commit()?;
            return Ok(WorkClaimSettlementOutcome::Orphaned {
                reason_code: claim_reason::LATE_RESULT_ORPHANED,
            });
        }
        let event_payload = json!({
            "generation": request.authority.generation,
            "result_sha256": request.result_sha256,
            "verification_state": request.verification_state,
            "failure_count": failure_count,
            "retry_scheduled": retry_scheduled,
            "circuit_open": circuit_open,
        })
        .to_string();
        insert_event(
            &transaction,
            EventInsert {
                graph_id: request.authority.graph_id.as_str(),
                work_item_id: Some(request.authority.work_item_id.as_str()),
                graph_revision: next_graph_revision,
                item_revision: Some(next_item_revision),
                event_type: "work_graph.item.claim_settled",
                actor_principal: request.actor_principal.as_str(),
                from_state: Some(item.state.as_str()),
                to_state: Some(effective_state.as_str()),
                reason_code: effective_reason,
                payload_json: event_payload.as_str(),
                created_at_unix_ms: now,
            },
        )?;
        let _dependency_changes = project_dependency_states(
            &transaction,
            request.authority.graph_id.as_str(),
            next_graph_revision,
            request.actor_principal.as_str(),
            now,
        )?;
        let graph_state = project_graph_state(&transaction, request.authority.graph_id.as_str())?;
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
                request.authority.graph_id,
                graph_state.as_str(),
                u64_to_sqlite(next_graph_revision, "graph_revision")?,
                effective_reason,
                now,
                bool_to_sqlite(graph_state.is_terminal()),
            ],
        )?;
        transaction.commit()?;
        drop(guard);
        let item = load_item(
            self,
            request.authority.graph_id.as_str(),
            request.authority.work_item_id.as_str(),
        )?;
        Ok(WorkClaimSettlementOutcome::Applied {
            item: Box::new(item),
            graph_revision: next_graph_revision,
        })
    }

    /// Projects bounded redacted claim metrics and the latest stable decision reason.
    pub(crate) fn work_graph_claim_diagnostics(
        &self,
        graph_id: &str,
    ) -> Result<Option<WorkGraphClaimDiagnosticsV1>, JournalError> {
        ensure_nonempty_field(graph_id, "graph_id")?;
        let now = current_unix_ms()?;
        let guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        let exists = guard.query_row(
            "SELECT EXISTS(SELECT 1 FROM work_graphs WHERE graph_ulid = ?1)",
            params![graph_id],
            |row| row.get::<_, bool>(0),
        )?;
        if !exists {
            return Ok(None);
        }
        let (active, expired, attempts, side_effect_reviews) = guard.query_row(
            r#"
                SELECT
                    COALESCE(SUM(CASE WHEN claim_token_sha256 IS NOT NULL THEN 1 ELSE 0 END), 0),
                    COALESCE(SUM(CASE
                        WHEN claim_token_sha256 IS NOT NULL
                         AND claim_expires_at_unix_ms <= ?2 THEN 1 ELSE 0 END), 0),
                    COALESCE(SUM(attempt_count), 0),
                    COALESCE(SUM(CASE
                        WHEN state = 'review' AND side_effect_fence_state != 'clear'
                        THEN 1 ELSE 0 END), 0)
                FROM work_graph_items
                WHERE graph_ulid = ?1
            "#,
            params![graph_id, now],
            |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, i64>(2)?,
                    row.get::<_, i64>(3)?,
                ))
            },
        )?;
        let orphan_results = guard.query_row(
            "SELECT COUNT(*) FROM work_graph_orphan_results WHERE graph_ulid = ?1",
            params![graph_id],
            |row| row.get::<_, i64>(0),
        )?;
        let last_reason_code = guard
            .query_row(
                r#"
                    SELECT reason_code
                    FROM work_graph_events
                    WHERE graph_ulid = ?1
                      AND (
                        event_type LIKE 'work_graph.item.claim%'
                        OR event_type LIKE 'work_graph.item.heartbeat%'
                        OR event_type LIKE 'work_graph.item.reclaim%'
                        OR event_type LIKE 'work_graph.item.side_effect_fence%'
                      )
                    ORDER BY seq DESC
                    LIMIT 1
                "#,
                params![graph_id],
                |row| row.get::<_, String>(0),
            )
            .optional()?;
        Ok(Some(WorkGraphClaimDiagnosticsV1 {
            graph_id: graph_id.to_owned(),
            active_claim_count: nonnegative_count(active, "active_claim_count")?,
            expired_claim_count: nonnegative_count(expired, "expired_claim_count")?,
            total_attempt_count: nonnegative_count(attempts, "total_attempt_count")?,
            orphan_result_count: nonnegative_count(orphan_results, "orphan_result_count")?,
            side_effect_review_count: nonnegative_count(
                side_effect_reviews,
                "side_effect_review_count",
            )?,
            last_reason_code,
        }))
    }
}

fn validate_claim_request(request: &ClaimReadyWorkItemRequest) -> Result<(), JournalError> {
    for (field, value) in [
        ("graph_id", request.graph_id.as_str()),
        ("worker_id", request.worker_id.as_str()),
        ("worker_principal", request.worker_principal.as_str()),
        ("authorized_owner_principal", request.authorized_owner_principal.as_str()),
        ("runtime_instance_id", request.runtime_instance_id.as_str()),
        ("process_start_token", request.process_start_token.as_str()),
    ] {
        ensure_nonempty_field(value, field)?;
    }
    if request.capability_profiles.is_empty()
        || request.capability_profiles.iter().any(|profile| profile.trim().is_empty())
    {
        return Err(invalid_claim_data("capability profile set cannot be empty"));
    }
    if request.provider_backpressure_profiles.iter().any(|profile| profile.trim().is_empty()) {
        return Err(invalid_claim_data("provider backpressure profiles cannot be empty"));
    }
    if request.resource_lease_id.as_ref().is_some_and(|lease_id| lease_id.trim().is_empty()) {
        return Err(invalid_claim_data("resource lease id cannot be empty"));
    }
    validate_ttl(request.lease_ttl_ms)
}

fn concurrency_denial_reason(
    candidate: &WorkItemRecordV1,
    items: &[WorkItemRecordV1],
    policy: &crate::domain::work_graph::WorkGraphConcurrencyPolicy,
    request: &ClaimReadyWorkItemRequest,
    now: i64,
) -> Option<&'static str> {
    if candidate.failure_circuit.retry_not_before_unix_ms.is_some_and(|not_before| not_before > now)
    {
        return Some(concurrency_reason::RETRY_BACKOFF);
    }
    if candidate.failure_circuit.opened_at_unix_ms.is_some() {
        return Some(concurrency_reason::CIRCUIT_OPEN);
    }
    if request.memory_pressure && candidate.resource_class != WorkResourceClass::Interactive {
        return Some(concurrency_reason::MEMORY_PRESSURE);
    }
    if candidate
        .provider_profile
        .as_ref()
        .is_some_and(|profile| request.provider_backpressure_profiles.contains(profile))
    {
        return Some(concurrency_reason::PROVIDER_RATE_LIMITED);
    }

    let active = items.iter().filter(|item| item.claim.is_some()).collect::<Vec<_>>();
    if active.len() >= policy.max_active_items as usize {
        return Some(concurrency_reason::GLOBAL_LIMIT);
    }
    let profile_limit = policy
        .max_active_per_profile
        .get(candidate.capability_profile.as_str())
        .copied()
        .unwrap_or(policy.max_active_items);
    if active.iter().filter(|item| item.capability_profile == candidate.capability_profile).count()
        >= profile_limit as usize
    {
        return Some(concurrency_reason::PROFILE_LIMIT);
    }
    if let Some(provider) = candidate.provider_profile.as_deref() {
        let provider_limit = policy
            .max_active_per_provider
            .get(provider)
            .copied()
            .unwrap_or(policy.max_active_items);
        if active.iter().filter(|item| item.provider_profile.as_deref() == Some(provider)).count()
            >= provider_limit as usize
        {
            return Some(concurrency_reason::PROVIDER_LIMIT);
        }
    }
    if candidate
        .serialization_key
        .as_ref()
        .is_some_and(|key| active.iter().any(|item| item.serialization_key.as_ref() == Some(key)))
    {
        return Some(concurrency_reason::SERIALIZATION_CONFLICT);
    }
    if let Some(scope) = candidate.workspace_scope.as_deref() {
        let conflicting = active.iter().any(|item| {
            item.workspace_scope.as_deref().is_some_and(|active_scope| {
                workspace_scopes_overlap(scope, active_scope)
                    && (candidate.resource_class == WorkResourceClass::WorkspaceMutation
                        || item.resource_class == WorkResourceClass::WorkspaceMutation)
            })
        });
        if conflicting {
            return Some(concurrency_reason::WORKSPACE_CONFLICT);
        }
        if candidate.resource_class == WorkResourceClass::WorkspaceRead
            && active
                .iter()
                .filter(|item| {
                    item.resource_class == WorkResourceClass::WorkspaceRead
                        && item.workspace_scope.as_deref().is_some_and(|active_scope| {
                            workspace_scopes_overlap(scope, active_scope)
                        })
                })
                .count()
                >= policy.max_workspace_readers_per_scope as usize
        {
            return Some(concurrency_reason::WORKSPACE_READER_LIMIT);
        }
    }
    None
}

fn workspace_scopes_overlap(left: &str, right: &str) -> bool {
    let normalize =
        |value: &str| value.replace('\\', "/").trim_end_matches('/').to_ascii_lowercase();
    let left = normalize(left);
    let right = normalize(right);
    left == right
        || left.strip_prefix(right.as_str()).is_some_and(|remainder| remainder.starts_with('/'))
        || right.strip_prefix(left.as_str()).is_some_and(|remainder| remainder.starts_with('/'))
}

fn retry_backoff_ms(
    policy: &crate::domain::work_graph::WorkGraphConcurrencyPolicy,
    failure_count: u32,
) -> i64 {
    let exponent = failure_count.saturating_sub(1).min(20);
    let delay = policy
        .retry_backoff_base_ms
        .saturating_mul(1_u64 << exponent)
        .min(policy.retry_backoff_max_ms);
    i64::try_from(delay).unwrap_or(i64::MAX)
}

fn validate_ttl(ttl_ms: u64) -> Result<(), JournalError> {
    if !(MIN_WORK_CLAIM_TTL_MS..=MAX_WORK_CLAIM_TTL_MS).contains(&ttl_ms) {
        return Err(invalid_claim_data("claim lease duration is outside host bounds"));
    }
    Ok(())
}

fn validate_authority(authority: &WorkClaimAuthority) -> Result<(), JournalError> {
    for (field, value) in [
        ("graph_id", authority.graph_id.as_str()),
        ("work_item_id", authority.work_item_id.as_str()),
        ("worker_id", authority.worker_id.as_str()),
    ] {
        ensure_nonempty_field(value, field)?;
    }
    if authority.generation == 0 {
        return Err(invalid_claim_data("claim generation must be positive"));
    }
    Ok(())
}

fn validate_reclaim_request(request: &StaleReclaimRequest) -> Result<(), JournalError> {
    for (field, value) in [
        ("graph_id", request.graph_id.as_str()),
        ("work_item_id", request.work_item_id.as_str()),
        ("runtime_instance_id", request.runtime_instance_id.as_str()),
        ("process_start_token", request.process_start_token.as_str()),
        ("actor_principal", request.actor_principal.as_str()),
    ] {
        ensure_nonempty_field(value, field)?;
    }
    if request.expected_generation == 0 {
        return Err(invalid_claim_data("expected generation must be positive"));
    }
    Ok(())
}

fn validate_settlement_request(request: &WorkClaimSettlementRequest) -> Result<(), JournalError> {
    validate_authority(&request.authority)?;
    ensure_nonempty_field(request.actor_principal.as_str(), "actor_principal")?;
    ensure_nonempty_field(request.reason_code.as_str(), "reason_code")?;
    if request.result_sha256.len() != 64
        || !request.result_sha256.bytes().all(|byte| byte.is_ascii_hexdigit())
    {
        return Err(invalid_claim_data("result_sha256 must be a 64-character hex digest"));
    }
    if !matches!(
        request.target_state,
        WorkItemState::Waiting
            | WorkItemState::Review
            | WorkItemState::Succeeded
            | WorkItemState::Failed
            | WorkItemState::Cancelled
    ) {
        return Err(invalid_claim_data("worker settlement target is not permitted"));
    }
    Ok(())
}

fn authority_matches(authority: &WorkClaimAuthority, claim: &WorkItemClaimV1) -> bool {
    claim.worker_id == authority.worker_id
        && claim.generation == authority.generation
        && claim.claim_token_sha256 == authority.token.sha256_hex()
}

fn valid_side_effect_fence_transition(
    current: WorkSideEffectFenceState,
    target: WorkSideEffectFenceState,
) -> bool {
    current == target
        || matches!(
            (current, target),
            (
                WorkSideEffectFenceState::Clear,
                WorkSideEffectFenceState::InFlight | WorkSideEffectFenceState::Unknown
            ) | (
                WorkSideEffectFenceState::InFlight,
                WorkSideEffectFenceState::Committed | WorkSideEffectFenceState::Unknown
            ) | (WorkSideEffectFenceState::Committed, WorkSideEffectFenceState::Unknown)
        )
}

fn nonnegative_count(value: i64, field: &str) -> Result<u64, JournalError> {
    u64::try_from(value)
        .map_err(|_| invalid_claim_data(format!("{field} became negative").as_str()))
}

fn update_graph_revision(
    transaction: &Transaction<'_>,
    graph_id: &str,
    revision: u64,
    reason_code: &str,
    now: i64,
) -> Result<(), JournalError> {
    transaction.execute(
        r#"
            UPDATE work_graphs
            SET revision = ?2, reason_code = ?3, updated_at_unix_ms = ?4
            WHERE graph_ulid = ?1
        "#,
        params![graph_id, u64_to_sqlite(revision, "graph_revision")?, reason_code, now],
    )?;
    Ok(())
}

fn record_orphan_result(
    transaction: &Transaction<'_>,
    request: &WorkClaimSettlementRequest,
    active_claim: Option<&WorkItemClaimV1>,
    now: i64,
) -> Result<(), JournalError> {
    let count = transaction.query_row(
        r#"
            SELECT COUNT(*)
            FROM work_graph_orphan_results
            WHERE graph_ulid = ?1 AND work_item_ulid = ?2
        "#,
        params![request.authority.graph_id, request.authority.work_item_id],
        |row| row.get::<_, i64>(0),
    )?;
    if count >= MAX_ORPHAN_RESULTS_PER_ITEM {
        return Ok(());
    }
    transaction.execute(
        r#"
            INSERT INTO work_graph_orphan_results (
                orphan_ulid, graph_ulid, work_item_ulid, observed_generation,
                active_generation, worker_id, result_sha256, target_state,
                reason_code, created_at_unix_ms
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)
        "#,
        params![
            Ulid::new().to_string(),
            request.authority.graph_id,
            request.authority.work_item_id,
            u64_to_sqlite(request.authority.generation, "observed_generation")?,
            active_claim
                .map(|claim| u64_to_sqlite(claim.generation, "active_generation"))
                .transpose()?,
            request.authority.worker_id,
            request.result_sha256,
            request.target_state.as_str(),
            claim_reason::LATE_RESULT_ORPHANED,
            now,
        ],
    )?;
    Ok(())
}

fn load_item(
    store: &JournalStore,
    graph_id: &str,
    work_item_id: &str,
) -> Result<crate::domain::work_graph::WorkItemRecordV1, JournalError> {
    store
        .work_graph_snapshot(graph_id)?
        .and_then(|snapshot| {
            snapshot.items.into_iter().find(|item| item.work_item_id == work_item_id)
        })
        .ok_or_else(|| JournalError::WorkGraphItemNotFound {
            graph_id: graph_id.to_owned(),
            work_item_id: work_item_id.to_owned(),
        })
}

fn invalid_claim_data(message: &str) -> JournalError {
    JournalError::InvalidWorkGraph {
        reason_code: "work_graph.claim.invalid".to_owned(),
        message: message.to_owned(),
    }
}
