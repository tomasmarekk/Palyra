//! Durable WorkGraph cancellation fanout and concurrency-control mutations.

use crate::domain::work_graph::{
    concurrency_reason, validate_loaded_graph, WorkGraphCancellationPlanV1,
    WorkGraphCancellationTargetV1, WorkGraphState, WorkItemState,
};

use super::{
    storage::{insert_event, query_snapshot, validation_error, EventInsert},
    *,
};

impl JournalStore {
    /// Atomically cancels every non-terminal item and returns prior worker authorities for fanout.
    pub(crate) fn cancel_work_graph(
        &self,
        graph_id: &str,
        expected_graph_revision: u64,
        actor_principal: &str,
    ) -> Result<WorkGraphCancellationPlanV1, JournalError> {
        ensure_nonempty_field(graph_id, "graph_id")?;
        ensure_nonempty_field(actor_principal, "actor_principal")?;
        let now = current_unix_ms()?;
        let mut guard = self.connection.lock().map_err(|_| JournalError::LockPoisoned)?;
        let transaction = guard.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let snapshot = query_snapshot(&transaction, graph_id)?
            .ok_or_else(|| JournalError::WorkGraphNotFound { graph_id: graph_id.to_owned() })?;
        validate_loaded_graph(&snapshot.graph, snapshot.items.as_slice())
            .map_err(validation_error)?;
        if snapshot.graph.revision != expected_graph_revision {
            return Err(JournalError::WorkGraphRevisionConflict {
                graph_id: graph_id.to_owned(),
                work_item_id: "*".to_owned(),
                expected_revision: expected_graph_revision,
                actual_revision: snapshot.graph.revision,
            });
        }
        if snapshot.graph.state.is_terminal() {
            return Ok(WorkGraphCancellationPlanV1 {
                graph_id: graph_id.to_owned(),
                graph_revision: snapshot.graph.revision,
                settle_timeout_ms: snapshot.graph.concurrency_policy.cancel_settle_timeout_ms,
                targets: Vec::new(),
                reason_code: snapshot.graph.reason_code,
            });
        }

        let next_graph_revision = snapshot.graph.revision.saturating_add(1);
        let mut targets = Vec::new();
        for item in snapshot.items.iter().filter(|item| !item.state.is_terminal()) {
            if let Some(claim) = item.claim.as_ref() {
                targets.push(WorkGraphCancellationTargetV1 {
                    work_item_id: item.work_item_id.clone(),
                    worker_id: claim.worker_id.clone(),
                    generation: claim.generation,
                    resource_lease_id: claim.resource_lease_id.clone(),
                });
            }
            let next_item_revision = item.revision.saturating_add(1);
            transaction.execute(
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
                        side_effect_fence_state = CASE
                            WHEN side_effect_fence_state = 'clear' THEN 'clear'
                            ELSE 'unknown'
                        END,
                        retry_not_before_unix_ms = NULL,
                        revision = ?4,
                        reason_code = ?5,
                        updated_at_unix_ms = ?6,
                        completed_at_unix_ms = ?6
                    WHERE graph_ulid = ?1
                      AND work_item_ulid = ?2
                      AND revision = ?7
                "#,
                params![
                    graph_id,
                    item.work_item_id,
                    WorkItemState::Cancelled.as_str(),
                    u64_to_sqlite(next_item_revision, "work_item_revision")?,
                    concurrency_reason::GRAPH_CANCELLED,
                    now,
                    u64_to_sqlite(item.revision, "expected_item_revision")?,
                ],
            )?;
            insert_event(
                &transaction,
                EventInsert {
                    graph_id,
                    work_item_id: Some(item.work_item_id.as_str()),
                    graph_revision: next_graph_revision,
                    item_revision: Some(next_item_revision),
                    event_type: "work_graph.item.cancel_requested",
                    actor_principal,
                    from_state: Some(item.state.as_str()),
                    to_state: Some(WorkItemState::Cancelled.as_str()),
                    reason_code: concurrency_reason::GRAPH_CANCELLED,
                    payload_json: "{}",
                    created_at_unix_ms: now,
                },
            )?;
        }
        let changed = transaction.execute(
            r#"
                UPDATE work_graphs
                SET state = ?2,
                    revision = ?3,
                    reason_code = ?4,
                    updated_at_unix_ms = ?5,
                    completed_at_unix_ms = ?5
                WHERE graph_ulid = ?1 AND revision = ?6
            "#,
            params![
                graph_id,
                WorkGraphState::Cancelled.as_str(),
                u64_to_sqlite(next_graph_revision, "graph_revision")?,
                concurrency_reason::GRAPH_CANCELLED,
                now,
                u64_to_sqlite(expected_graph_revision, "expected_graph_revision")?,
            ],
        )?;
        if changed != 1 {
            return Err(JournalError::WorkGraphRevisionConflict {
                graph_id: graph_id.to_owned(),
                work_item_id: "*".to_owned(),
                expected_revision: expected_graph_revision,
                actual_revision: snapshot.graph.revision,
            });
        }
        insert_event(
            &transaction,
            EventInsert {
                graph_id,
                work_item_id: None,
                graph_revision: next_graph_revision,
                item_revision: None,
                event_type: "work_graph.cancel_requested",
                actor_principal,
                from_state: Some(snapshot.graph.state.as_str()),
                to_state: Some(WorkGraphState::Cancelled.as_str()),
                reason_code: concurrency_reason::GRAPH_CANCELLED,
                payload_json: "{}",
                created_at_unix_ms: now,
            },
        )?;
        transaction.commit()?;
        Ok(WorkGraphCancellationPlanV1 {
            graph_id: graph_id.to_owned(),
            graph_revision: next_graph_revision,
            settle_timeout_ms: snapshot.graph.concurrency_policy.cancel_settle_timeout_ms,
            targets,
            reason_code: concurrency_reason::GRAPH_CANCELLED.to_owned(),
        })
    }
}
