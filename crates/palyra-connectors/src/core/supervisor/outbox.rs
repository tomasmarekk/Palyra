//! Outbox drain loop: claims due entries, dispatches them through adapters,
//! and applies delivered/retry/dead-letter/outcome-unknown transitions.
//!
//! Retry budget: an entry is dead-lettered once its attempt count reaches
//! `min(entry.max_attempts, config.max_retry_attempts)`; delays grow
//! exponentially from `base_retry_delay_ms`, clamped between the configured
//! min and max, with adapter-provided `retry_after_ms` taking precedence.

use serde_json::json;
use std::{sync::Arc, task::Poll};

#[cfg(feature = "qa-fault-injection")]
use palyra_common::qa_fault_injection::QaFaultActiveBarrier;
use palyra_common::qa_fault_injection::{
    QaFaultAction, QaFaultActivationDirective, QaFaultRecoveryClass,
};

use super::super::{
    protocol::{
        ConnectorLiveness, ConnectorReadiness, DeliveryOutcome, DeliveryReceipt, RetryClass,
    },
    storage::{ConnectorInstanceRecord, OutboxEntryRecord},
};
use super::types::{
    classify_permanent_failure, retry_class_label, ConnectorAdapter, DispatchResult,
};
use super::{
    qa_fault_activation_error, unix_ms_now, ConnectorSupervisor, ConnectorSupervisorError,
    DrainOutcome,
};

const QA_FAULT_FAILED_CLOSED_REASON: &str = "outbox.qa_fault_failed_closed_before_effect";
#[cfg(feature = "qa-fault-injection")]
const QA_FAULT_INCOMPLETE_BARRIER_REASON: &str = "outbox.qa_fault_incomplete_barrier";
#[cfg(feature = "qa-fault-injection")]
const QA_FAULT_BATCH_POINT: &str = "connector.outbox.batch_before_effect";
const QA_FAULT_OUTCOME_UNKNOWN_REASON: &str = "outbox.qa_fault_outcome_unknown";
const QA_FAULT_TRANSITION_PENDING_REASON: &str = "outbox.qa_fault_transition_pending";
const QA_FAULT_OUTBOX_ACTOR_PREFIX: &str = "outbox-";

struct PreparedOutboxDispatch {
    entry: OutboxEntryRecord,
    instance: ConnectorInstanceRecord,
    adapter: Arc<dyn ConnectorAdapter>,
}

impl PreparedOutboxDispatch {
    fn qa_fault_actor(&self) -> String {
        qa_fault_actor(self.entry.outbox_id)
    }
}

enum OutboxPreparation {
    Completed(DispatchResult),
    Ready(Box<PreparedOutboxDispatch>),
}

#[cfg(feature = "qa-fault-injection")]
struct QaFaultBarrierBatch {
    activation_id: String,
    point_id: String,
    participants: usize,
    actors: Vec<String>,
    release_order: Option<Vec<String>>,
    released_actors: Vec<String>,
    entries: Vec<OutboxEntryRecord>,
}

#[cfg(feature = "qa-fault-injection")]
struct QaFaultBarrierRelease {
    entries: Vec<OutboxEntryRecord>,
    completion: QaFaultBarrierCompletion,
}

#[cfg(feature = "qa-fault-injection")]
struct QaFaultBarrierCompletion {
    activation_id: String,
    actors: Vec<String>,
    transitioned_claims: Vec<(i64, String)>,
    resolved_outbox_ids: Vec<i64>,
}

impl ConnectorSupervisor {
    /// Reconciles one exact connector outbox actor left claimed by a terminated QA process.
    ///
    /// The caller owns evidence recording after this method returns the proven recovery class.
    ///
    /// # Errors
    /// Returns a validation error for unsupported points, malformed actors, or rows whose
    /// durable fence state cannot prove the requested crash transition.
    #[cfg(feature = "qa-fault-injection")]
    pub fn reconcile_pending_qa_fault_actor(
        &self,
        point_id: &str,
        actor: &str,
    ) -> Result<QaFaultRecoveryClass, ConnectorSupervisorError> {
        let (effect_started, recovery_class, reason_code) = match point_id {
            "connector.outbox.before_intent"
            | "connector.outbox.after_intent"
            | "connector.outbox.before_effect" => {
                (false, QaFaultRecoveryClass::FailedClosed, QA_FAULT_FAILED_CLOSED_REASON)
            }
            "connector.outbox.during_delivery" | "connector.outbox.after_effect_before_ack" => {
                (true, QaFaultRecoveryClass::OutcomeUnknown, QA_FAULT_OUTCOME_UNKNOWN_REASON)
            }
            "connector.outbox.after_ack_before_transition" => {
                (true, QaFaultRecoveryClass::TransitionPending, QA_FAULT_TRANSITION_PENDING_REASON)
            }
            _ => {
                return Err(ConnectorSupervisorError::Validation(format!(
                    "unsupported connector QA crash recovery point {point_id}"
                )))
            }
        };
        let outbox_id = qa_fault_outbox_id(actor)?;
        if !self.store.recover_qa_fault_crash_actor(
            outbox_id,
            effect_started,
            reason_code,
            unix_ms_now()?,
        )? {
            return Err(ConnectorSupervisorError::Validation(format!(
                "connector QA crash actor {actor} has no provable pending outbox state"
            )));
        }
        Ok(recovery_class)
    }

    /// Drains up to `limit` due entries across all unpaused connectors.
    ///
    /// # Errors
    /// Returns clock, store, or supervisor errors raised while dispatching.
    pub async fn drain_due_outbox(
        &self,
        limit: usize,
    ) -> Result<DrainOutcome, ConnectorSupervisorError> {
        let now = unix_ms_now()?;
        let entries = self.store.load_due_outbox(now, limit, None, false)?;
        self.process_due_entries(entries).await
    }

    /// Drains up to `limit` due entries for one connector, honoring its pause
    /// flag.
    ///
    /// # Errors
    /// Returns clock, store, or supervisor errors raised while dispatching.
    pub async fn drain_due_outbox_for_connector(
        &self,
        connector_id: &str,
        limit: usize,
    ) -> Result<DrainOutcome, ConnectorSupervisorError> {
        let now = unix_ms_now()?;
        let entries = self.store.load_due_outbox(now, limit, Some(connector_id), false)?;
        self.process_due_entries(entries).await
    }

    /// Drains one connector even while its queue is paused; operator escape
    /// hatch for flushing a paused backlog.
    ///
    /// # Errors
    /// Returns clock, store, or supervisor errors raised while dispatching.
    pub async fn drain_due_outbox_for_connector_force(
        &self,
        connector_id: &str,
        limit: usize,
    ) -> Result<DrainOutcome, ConnectorSupervisorError> {
        let now = unix_ms_now()?;
        let entries = self.store.load_due_outbox(now, limit, Some(connector_id), true)?;
        self.process_due_entries(entries).await
    }

    async fn process_due_entries(
        &self,
        entries: Vec<OutboxEntryRecord>,
    ) -> Result<DrainOutcome, ConnectorSupervisorError> {
        #[cfg(feature = "qa-fault-injection")]
        {
            return self.process_due_entries_with_fault_injection(entries).await;
        }

        #[cfg(not(feature = "qa-fault-injection"))]
        {
            self.process_due_entries_sequential(entries).await
        }
    }

    async fn process_due_entries_sequential(
        &self,
        entries: Vec<OutboxEntryRecord>,
    ) -> Result<DrainOutcome, ConnectorSupervisorError> {
        let mut outcome = DrainOutcome::default();
        self.dispatch_outbox_entries(entries, &mut outcome).await?;
        Ok(outcome)
    }

    async fn dispatch_outbox_entries(
        &self,
        entries: Vec<OutboxEntryRecord>,
        outcome: &mut DrainOutcome,
    ) -> Result<(), ConnectorSupervisorError> {
        for entry in entries {
            outcome.processed = outcome.processed.saturating_add(1);
            let result = self.dispatch_outbox_entry(entry).await?;
            accumulate_dispatch_result(outcome, result);
        }
        Ok(())
    }

    #[cfg(feature = "qa-fault-injection")]
    async fn process_due_entries_with_fault_injection(
        &self,
        entries: Vec<OutboxEntryRecord>,
    ) -> Result<DrainOutcome, ConnectorSupervisorError> {
        let Some(_) = self.preflight_connector_outbox_barrier(entries.as_slice())? else {
            return self.process_due_entries_sequential(entries).await;
        };
        let _adoption_guard = self.qa_fault_barrier_adoption_lock.lock().await;
        let Some(mut barrier) = self.preflight_connector_outbox_barrier(entries.as_slice())? else {
            return self.process_due_entries_sequential(entries).await;
        };
        let mut overflow = Vec::new();
        for entry in entries {
            if barrier.contains_actor(qa_fault_actor(entry.outbox_id).as_str()) {
                barrier.attach_entry(entry)?;
            } else {
                overflow.push(entry);
            }
        }
        self.refresh_connector_outbox_barrier(&mut barrier)?;
        if !barrier.is_full() || !self.connector_outbox_barrier_is_accounted(&barrier)? {
            self.release_incomplete_qa_fault_barrier(barrier)?;
            return Err(ConnectorSupervisorError::Validation(
                "qa fault barrier participants exceed the claimed outbox batch".to_owned(),
            ));
        }
        let release = self.release_qa_fault_barrier(barrier)?;
        let mut outcome = DrainOutcome::default();
        self.dispatch_outbox_entries(release.entries, &mut outcome).await?;
        self.record_qa_fault_barrier_recovery(&release.completion)?;
        self.dispatch_outbox_entries(overflow, &mut outcome).await?;
        Ok(outcome)
    }

    /// Delivers one claimed entry and applies the resulting state transition.
    async fn dispatch_outbox_entry(
        &self,
        entry: OutboxEntryRecord,
    ) -> Result<DispatchResult, ConnectorSupervisorError> {
        match self.prepare_outbox_entry(entry)? {
            OutboxPreparation::Completed(result) => Ok(result),
            OutboxPreparation::Ready(prepared) => {
                let prepared = *prepared;
                match self.qa_fault_checkpoint(
                    "connector.outbox.before_effect",
                    prepared.qa_fault_actor().as_str(),
                )? {
                    None => self.dispatch_prepared_outbox_entry(prepared).await,
                    Some(activation)
                        if matches!(
                            activation.activation.action,
                            QaFaultAction::Barrier { .. }
                        ) =>
                    {
                        Err(qa_fault_activation_error(activation))
                    }
                    Some(activation) => self.fail_closed_before_effect(prepared, activation),
                }
            }
        }
    }

    fn prepare_outbox_entry(
        &self,
        entry: OutboxEntryRecord,
    ) -> Result<OutboxPreparation, ConnectorSupervisorError> {
        let now = unix_ms_now()?;
        let Some(instance) = self.store.get_instance(entry.connector_id.as_str())? else {
            // The instance was removed after this entry was enqueued; there is
            // no adapter to deliver through, so park the message immediately.
            self.store.move_outbox_to_dead_letter(
                entry.outbox_id,
                entry.claim_token.as_str(),
                "connector instance not found",
                now,
            )?;
            self.store.mark_delivery_intent_dead_lettered_for_outbox(
                entry.connector_id.as_str(),
                entry.envelope_id.as_str(),
                "connector instance not found",
                now,
            )?;
            return Ok(OutboxPreparation::Completed(DispatchResult::DeadLettered));
        };
        if !instance.enabled {
            let retry_at = now.saturating_add(
                i64::try_from(self.config.disabled_poll_delay_ms).unwrap_or(i64::MAX),
            );
            self.store.schedule_outbox_retry(
                entry.outbox_id,
                entry.claim_token.as_str(),
                entry.attempts,
                "connector disabled",
                retry_at,
            )?;
            self.store.mark_delivery_intent_retry_queued_for_outbox(
                instance.connector_id.as_str(),
                entry.envelope_id.as_str(),
                "connector disabled",
                now,
            )?;
            return Ok(OutboxPreparation::Completed(DispatchResult::Retried));
        }

        let Some(adapter) = self.adapters.get(&instance.kind).cloned() else {
            self.store.move_outbox_to_dead_letter(
                entry.outbox_id,
                entry.claim_token.as_str(),
                "connector adapter implementation missing",
                now,
            )?;
            self.store.mark_delivery_intent_dead_lettered_for_outbox(
                instance.connector_id.as_str(),
                entry.envelope_id.as_str(),
                "connector adapter implementation missing",
                now,
            )?;
            self.store.record_event(
                instance.connector_id.as_str(),
                "outbox.dead_letter",
                "error",
                "connector adapter implementation missing",
                Some(&json!({
                    "kind": instance.kind.as_str(),
                    "envelope_id": entry.envelope_id,
                })),
                now,
            )?;
            return Ok(OutboxPreparation::Completed(DispatchResult::DeadLettered));
        };

        let fault_actor = qa_fault_actor(entry.outbox_id);
        if let Some(activation) =
            self.qa_fault_checkpoint("connector.outbox.before_intent", fault_actor.as_str())?
        {
            return self.fail_closed_before_effect(
                PreparedOutboxDispatch { entry, instance, adapter },
                activation,
            );
        }
        self.store.mark_outbox_delivery_intent_started(
            entry.outbox_id,
            entry.claim_token.as_str(),
            now,
        )?;
        if let Some(activation) =
            self.qa_fault_checkpoint("connector.outbox.after_intent", fault_actor.as_str())?
        {
            return self.fail_closed_before_effect(
                PreparedOutboxDispatch { entry, instance, adapter },
                activation,
            );
        }
        Ok(OutboxPreparation::Ready(Box::new(PreparedOutboxDispatch { entry, instance, adapter })))
    }

    async fn dispatch_prepared_outbox_entry(
        &self,
        prepared: PreparedOutboxDispatch,
    ) -> Result<DispatchResult, ConnectorSupervisorError> {
        let PreparedOutboxDispatch { entry, instance, adapter } = prepared;
        let now = unix_ms_now()?;
        let fault_actor = qa_fault_actor(entry.outbox_id);
        self.store.mark_outbox_effect_started(entry.outbox_id, entry.claim_token.as_str(), now)?;

        let mut delivery_future = Box::pin(adapter.send_outbound(&instance, &entry.payload));
        let delivery = match futures::poll!(&mut delivery_future) {
            Poll::Ready(delivery) => delivery,
            Poll::Pending => {
                if let Some(activation) = self
                    .qa_fault_checkpoint("connector.outbox.during_delivery", fault_actor.as_str())?
                {
                    return self.park_outbox_after_qa_fault(
                        &entry,
                        activation,
                        QaFaultRecoveryClass::OutcomeUnknown,
                        QA_FAULT_OUTCOME_UNKNOWN_REASON,
                        now,
                    );
                }
                delivery_future.await
            }
        };
        if let Some(activation) = self
            .qa_fault_checkpoint("connector.outbox.after_effect_before_ack", fault_actor.as_str())?
        {
            return self.park_outbox_after_qa_fault(
                &entry,
                activation,
                QaFaultRecoveryClass::OutcomeUnknown,
                QA_FAULT_OUTCOME_UNKNOWN_REASON,
                now,
            );
        }
        let delivery = match delivery {
            Ok(outcome) => outcome,
            Err(error) => {
                self.store.mark_outbox_outcome_unknown(
                    entry.outbox_id,
                    entry.claim_token.as_str(),
                    "outbox.adapter_transport_error",
                    now,
                )?;
                self.store.record_event(
                    instance.connector_id.as_str(),
                    "outbox.adapter_error",
                    "warn",
                    "adapter delivery call failed; outcome requires reconciliation",
                    Some(&json!({
                        "envelope_id": entry.envelope_id,
                        "error": error.to_string(),
                    })),
                    now,
                )?;
                return Ok(DispatchResult::OutcomeUnknown);
            }
        };
        if matches!(delivery, DeliveryOutcome::Delivered { .. }) {
            if let Some(activation) = self.qa_fault_checkpoint(
                "connector.outbox.after_ack_before_transition",
                fault_actor.as_str(),
            )? {
                return self.park_outbox_after_qa_fault(
                    &entry,
                    activation,
                    QaFaultRecoveryClass::TransitionPending,
                    QA_FAULT_TRANSITION_PENDING_REASON,
                    now,
                );
            }
        }
        self.apply_delivery_outcome(&instance, &entry, delivery, now).await
    }

    fn fail_closed_before_effect<T>(
        &self,
        prepared: PreparedOutboxDispatch,
        activation: QaFaultActivationDirective,
    ) -> Result<T, ConnectorSupervisorError> {
        let now = unix_ms_now()?;
        self.store.schedule_outbox_retry(
            prepared.entry.outbox_id,
            prepared.entry.claim_token.as_str(),
            prepared.entry.attempts,
            QA_FAULT_FAILED_CLOSED_REASON,
            now,
        )?;
        self.store.mark_delivery_intent_retry_queued_for_outbox(
            prepared.instance.connector_id.as_str(),
            prepared.entry.envelope_id.as_str(),
            QA_FAULT_FAILED_CLOSED_REASON,
            now,
        )?;
        self.record_qa_fault_recovery(
            activation.activation.id.as_str(),
            QaFaultRecoveryClass::FailedClosed,
        )?;
        Err(qa_fault_activation_error(activation))
    }

    fn park_outbox_after_qa_fault<T>(
        &self,
        entry: &OutboxEntryRecord,
        activation: QaFaultActivationDirective,
        recovery_class: QaFaultRecoveryClass,
        reason_code: &str,
        now_unix_ms: i64,
    ) -> Result<T, ConnectorSupervisorError> {
        self.store.mark_outbox_outcome_unknown(
            entry.outbox_id,
            entry.claim_token.as_str(),
            reason_code,
            now_unix_ms,
        )?;
        self.record_qa_fault_recovery(activation.activation.id.as_str(), recovery_class)?;
        Err(qa_fault_activation_error(activation))
    }

    #[cfg(feature = "qa-fault-injection")]
    fn preflight_connector_outbox_barrier(
        &self,
        entries: &[OutboxEntryRecord],
    ) -> Result<Option<QaFaultBarrierBatch>, ConnectorSupervisorError> {
        let mut barrier = self.active_connector_outbox_barrier()?;
        for entry in entries {
            let actor = qa_fault_actor(entry.outbox_id);
            if barrier.as_ref().is_some_and(QaFaultBarrierBatch::is_full) {
                break;
            }
            if barrier.as_ref().is_some_and(|batch| batch.contains_actor(actor.as_str())) {
                continue;
            }
            match self.qa_fault_checkpoint(QA_FAULT_BATCH_POINT, actor.as_str())? {
                None => {}
                Some(activation)
                    if matches!(activation.activation.action, QaFaultAction::Barrier { .. }) =>
                {
                    if let Some(batch) = barrier.as_mut() {
                        batch.join(activation)?;
                    } else {
                        barrier = Some(QaFaultBarrierBatch::new(activation)?);
                    }
                }
                Some(activation) => return Err(qa_fault_activation_error(activation)),
            }
        }
        Ok(barrier)
    }

    #[cfg(feature = "qa-fault-injection")]
    fn active_connector_outbox_barrier(
        &self,
    ) -> Result<Option<QaFaultBarrierBatch>, ConnectorSupervisorError> {
        let mut barriers = self
            .qa_fault_probe
            .active_barriers()?
            .into_iter()
            .filter(|barrier| barrier.point_id == QA_FAULT_BATCH_POINT);
        let Some(barrier) = barriers.next() else {
            return Ok(None);
        };
        if barriers.next().is_some() {
            return Err(ConnectorSupervisorError::Validation(
                "multiple active connector outbox barriers share one checkpoint".to_owned(),
            ));
        }
        QaFaultBarrierBatch::from_active(barrier).map(Some)
    }

    #[cfg(feature = "qa-fault-injection")]
    fn refresh_connector_outbox_barrier(
        &self,
        batch: &mut QaFaultBarrierBatch,
    ) -> Result<(), ConnectorSupervisorError> {
        let active = self
            .qa_fault_probe
            .active_barriers()?
            .into_iter()
            .find(|barrier| barrier.activation_id == batch.activation_id)
            .ok_or_else(|| {
                ConnectorSupervisorError::Validation(
                    "connector outbox barrier disappeared before aggregate recovery".to_owned(),
                )
            })?;
        batch.refresh(active)
    }

    #[cfg(feature = "qa-fault-injection")]
    fn connector_outbox_barrier_is_accounted(
        &self,
        batch: &QaFaultBarrierBatch,
    ) -> Result<bool, ConnectorSupervisorError> {
        let missing = batch.missing_entry_actors();
        let outbox_ids = qa_fault_outbox_ids(missing.as_slice())?;
        Ok(self.store.qa_fault_barrier_actors_are_resolved(outbox_ids.as_slice())?)
    }

    #[cfg(feature = "qa-fault-injection")]
    fn release_incomplete_qa_fault_barrier(
        &self,
        batch: QaFaultBarrierBatch,
    ) -> Result<(), ConnectorSupervisorError> {
        let now = unix_ms_now()?;
        let missing = batch.missing_entry_actors();
        let outbox_ids = qa_fault_outbox_ids(missing.as_slice())?;
        self.store.recover_qa_fault_barrier_claims(
            outbox_ids.as_slice(),
            QA_FAULT_INCOMPLETE_BARRIER_REASON,
            now,
        )?;
        for entry in batch.entries {
            self.store.schedule_outbox_retry(
                entry.outbox_id,
                entry.claim_token.as_str(),
                entry.attempts,
                QA_FAULT_INCOMPLETE_BARRIER_REASON,
                now,
            )?;
            self.store.mark_delivery_intent_retry_queued_for_outbox(
                entry.connector_id.as_str(),
                entry.envelope_id.as_str(),
                QA_FAULT_INCOMPLETE_BARRIER_REASON,
                now,
            )?;
        }
        Ok(())
    }

    #[cfg(feature = "qa-fault-injection")]
    fn release_qa_fault_barrier(
        &self,
        batch: QaFaultBarrierBatch,
    ) -> Result<QaFaultBarrierRelease, ConnectorSupervisorError> {
        let release_order = batch.release_order.as_ref().ok_or_else(|| {
            ConnectorSupervisorError::Validation(
                "full connector outbox barrier has no seeded release order".to_owned(),
            )
        })?;
        let resolved_outbox_ids = qa_fault_outbox_ids(batch.missing_entry_actors().as_slice())?;
        let transitioned_claims = batch
            .entries
            .iter()
            .map(|entry| (entry.outbox_id, entry.claim_token.clone()))
            .collect();
        let mut entries = batch.entries.into_iter().map(Some).collect::<Vec<_>>();
        let mut dispatch_order = Vec::with_capacity(entries.len());
        for actor in release_order {
            let entry_position = entries.iter().position(|candidate| {
                candidate.as_ref().is_some_and(|entry| qa_fault_actor(entry.outbox_id) == *actor)
            });
            if batch.released_actors.iter().any(|released| released == actor) {
                if let Some(position) = entry_position {
                    dispatch_order.push(
                        entries[position]
                            .take()
                            .expect("barrier actor position was checked as populated"),
                    );
                }
                continue;
            }
            match self.qa_fault_checkpoint(batch.point_id.as_str(), actor.as_str())? {
                None => {
                    if let Some(position) = entry_position {
                        dispatch_order.push(
                            entries[position]
                                .take()
                                .expect("barrier actor position was checked as populated"),
                        );
                    }
                }
                Some(activation) => return Err(qa_fault_activation_error(activation)),
            }
        }
        if entries.iter().any(Option::is_some) {
            return Err(ConnectorSupervisorError::Validation(
                "connector barrier batch contains an actor outside the seeded release order"
                    .to_owned(),
            ));
        }
        Ok(QaFaultBarrierRelease {
            entries: dispatch_order,
            completion: QaFaultBarrierCompletion {
                activation_id: batch.activation_id,
                actors: batch.actors,
                transitioned_claims,
                resolved_outbox_ids,
            },
        })
    }

    #[cfg(feature = "qa-fault-injection")]
    fn record_qa_fault_barrier_recovery(
        &self,
        completion: &QaFaultBarrierCompletion,
    ) -> Result<(), ConnectorSupervisorError> {
        let active = self
            .qa_fault_probe
            .active_barriers()?
            .into_iter()
            .find(|barrier| barrier.activation_id == completion.activation_id);
        let Some(active) = active else {
            return Ok(());
        };
        if active.actors != completion.actors
            || active.release_order.as_ref() != Some(&active.released_actors)
            || !self.store.qa_fault_barrier_completion_is_durable(
                completion.transitioned_claims.as_slice(),
                completion.resolved_outbox_ids.as_slice(),
            )?
        {
            return Err(ConnectorSupervisorError::Validation(
                "connector barrier recovery lacks complete durable transitions".to_owned(),
            ));
        }
        self.record_qa_fault_recovery(
            completion.activation_id.as_str(),
            QaFaultRecoveryClass::Resumed,
        )
    }

    async fn apply_delivery_outcome(
        &self,
        instance: &super::super::storage::ConnectorInstanceRecord,
        entry: &OutboxEntryRecord,
        delivery: DeliveryOutcome,
        now_unix_ms: i64,
    ) -> Result<DispatchResult, ConnectorSupervisorError> {
        let receipt = DeliveryReceipt::from_outcome(&entry.payload, &delivery);
        match delivery {
            DeliveryOutcome::Delivered { native_message_id } => {
                self.store.mark_outbox_and_delivery_intents_delivered(
                    entry.outbox_id,
                    entry.claim_token.as_str(),
                    native_message_id.as_str(),
                    now_unix_ms,
                )?;
                self.store.record_last_outbound(instance.connector_id.as_str(), now_unix_ms)?;
                self.store.record_event(
                    instance.connector_id.as_str(),
                    "outbox.delivered",
                    "info",
                    "outbound message delivered",
                    Some(&json!({
                        "envelope_id": entry.envelope_id,
                        "native_message_id": native_message_id,
                        "receipt": receipt,
                    })),
                    now_unix_ms,
                )?;
                Ok(DispatchResult::Delivered)
            }
            DeliveryOutcome::Retry { class, reason, retry_after_ms } => {
                let attempts = entry.attempts.saturating_add(1);
                let max_attempts = entry.max_attempts.min(self.config.max_retry_attempts).max(1);
                if attempts >= max_attempts {
                    self.store.move_outbox_to_dead_letter(
                        entry.outbox_id,
                        entry.claim_token.as_str(),
                        reason.as_str(),
                        now_unix_ms,
                    )?;
                    self.store.mark_delivery_intent_dead_lettered_for_outbox(
                        instance.connector_id.as_str(),
                        entry.envelope_id.as_str(),
                        reason.as_str(),
                        now_unix_ms,
                    )?;
                    self.store.record_event(
                        instance.connector_id.as_str(),
                        "outbox.dead_letter",
                        "warn",
                        "retry budget exhausted; moved to dead letter",
                        Some(&json!({
                            "envelope_id": entry.envelope_id,
                            "attempts": attempts,
                            "reason": reason,
                            "retry_class": retry_class_label(class),
                            "receipt": receipt,
                        })),
                        now_unix_ms,
                    )?;
                    return Ok(DispatchResult::DeadLettered);
                }

                let delay_ms = self.retry_delay_ms(attempts, retry_after_ms);
                let next_attempt_unix_ms =
                    now_unix_ms.saturating_add(i64::try_from(delay_ms).unwrap_or(i64::MAX));
                self.store.schedule_outbox_retry(
                    entry.outbox_id,
                    entry.claim_token.as_str(),
                    attempts,
                    reason.as_str(),
                    next_attempt_unix_ms,
                )?;
                self.store.mark_delivery_intent_retry_queued_for_outbox(
                    instance.connector_id.as_str(),
                    entry.envelope_id.as_str(),
                    reason.as_str(),
                    now_unix_ms,
                )?;
                if matches!(class, RetryClass::ConnectorRestarting) {
                    self.store.increment_restart_count(
                        instance.connector_id.as_str(),
                        now_unix_ms,
                        reason.as_str(),
                    )?;
                } else {
                    // INTENTIONAL: a transient retry keeps the connector
                    // reported Ready/Running — only the failure reason is
                    // surfaced via last_error so operators see flapping
                    // deliveries without the connector appearing unhealthy.
                    self.store.set_instance_runtime_state(
                        instance.connector_id.as_str(),
                        ConnectorReadiness::Ready,
                        ConnectorLiveness::Running,
                        Some(reason.as_str()),
                        now_unix_ms,
                    )?;
                }
                self.store.record_event(
                    instance.connector_id.as_str(),
                    "outbox.retry",
                    "warn",
                    "connector delivery requested retry",
                    Some(&json!({
                        "envelope_id": entry.envelope_id,
                        "attempts": attempts,
                        "next_attempt_unix_ms": next_attempt_unix_ms,
                        "reason": reason,
                        "retry_class": retry_class_label(class),
                        "receipt": receipt,
                    })),
                    now_unix_ms,
                )?;
                Ok(DispatchResult::Retried)
            }
            DeliveryOutcome::OutcomeUnknown { reason } => {
                self.store.mark_outbox_outcome_unknown(
                    entry.outbox_id,
                    entry.claim_token.as_str(),
                    "outbox.adapter_reported_outcome_unknown",
                    now_unix_ms,
                )?;
                self.store.record_event(
                    instance.connector_id.as_str(),
                    "outbox.outcome_unknown",
                    "warn",
                    "adapter could not prove the outbound platform outcome",
                    Some(&json!({
                        "envelope_id": entry.envelope_id,
                        "reason": reason,
                        "receipt": receipt,
                    })),
                    now_unix_ms,
                )?;
                Ok(DispatchResult::OutcomeUnknown)
            }
            DeliveryOutcome::PermanentFailure { reason } => {
                self.store.move_outbox_to_dead_letter(
                    entry.outbox_id,
                    entry.claim_token.as_str(),
                    reason.as_str(),
                    now_unix_ms,
                )?;
                self.store.mark_delivery_intent_dead_lettered_for_outbox(
                    instance.connector_id.as_str(),
                    entry.envelope_id.as_str(),
                    reason.as_str(),
                    now_unix_ms,
                )?;
                let readiness = classify_permanent_failure(reason.as_str());
                self.store.set_instance_runtime_state(
                    instance.connector_id.as_str(),
                    readiness,
                    ConnectorLiveness::Running,
                    Some(reason.as_str()),
                    now_unix_ms,
                )?;
                self.store.record_event(
                    instance.connector_id.as_str(),
                    "outbox.dead_letter",
                    "warn",
                    "connector delivery returned permanent failure",
                    Some(&json!({
                        "envelope_id": entry.envelope_id,
                        "reason": reason,
                        "receipt": receipt,
                    })),
                    now_unix_ms,
                )?;
                Ok(DispatchResult::DeadLettered)
            }
        }
    }

    /// Computes the next retry delay: an adapter-requested `retry_after_ms`
    /// wins over the exponential schedule, but both are clamped to the
    /// configured min/max bounds. The exponent is capped so the shift cannot
    /// overflow regardless of the attempt count.
    fn retry_delay_ms(&self, attempts: u32, requested_retry_after_ms: Option<u64>) -> u64 {
        let exponent = attempts.saturating_sub(1).min(10);
        let exponential = self
            .config
            .base_retry_delay_ms
            .saturating_mul(1_u64 << exponent)
            .min(self.config.max_retry_delay_ms);
        requested_retry_after_ms
            .unwrap_or(exponential)
            .max(self.config.min_retry_delay_ms)
            .min(self.config.max_retry_delay_ms)
    }
}

#[cfg(feature = "qa-fault-injection")]
impl QaFaultBarrierBatch {
    fn from_active(active: QaFaultActiveBarrier) -> Result<Self, ConnectorSupervisorError> {
        let participants = usize::from(active.participants);
        if participants == 0 || active.actors.len() > participants {
            return Err(ConnectorSupervisorError::Validation(
                "active connector barrier has an invalid participant set".to_owned(),
            ));
        }
        Ok(Self {
            activation_id: active.activation_id,
            point_id: active.point_id,
            participants,
            actors: active.actors,
            release_order: active.release_order,
            released_actors: active.released_actors,
            entries: Vec::new(),
        })
    }

    fn new(activation: QaFaultActivationDirective) -> Result<Self, ConnectorSupervisorError> {
        let QaFaultAction::Barrier { participants } = activation.activation.action else {
            return Err(ConnectorSupervisorError::Validation(
                "qa fault barrier batch received a non-barrier activation".to_owned(),
            ));
        };
        let participants = usize::from(participants);
        if participants == 0 {
            return Err(ConnectorSupervisorError::Validation(
                "qa fault barrier participant count must be positive".to_owned(),
            ));
        }
        let actor = activation.actor.clone();
        Ok(Self {
            activation_id: activation.activation.id,
            point_id: activation.activation.point_id,
            participants,
            actors: vec![actor],
            release_order: None,
            released_actors: Vec::new(),
            entries: Vec::new(),
        })
    }

    fn join(
        &mut self,
        activation: QaFaultActivationDirective,
    ) -> Result<(), ConnectorSupervisorError> {
        let QaFaultAction::Barrier { participants } = activation.activation.action else {
            return Err(ConnectorSupervisorError::Validation(
                "qa fault barrier batch received a non-barrier activation".to_owned(),
            ));
        };
        if activation.activation.id != self.activation_id
            || activation.activation.point_id != self.point_id
            || usize::from(participants) != self.participants
        {
            return Err(ConnectorSupervisorError::Validation(
                "qa fault barrier contract changed within one outbox batch".to_owned(),
            ));
        }
        if self.is_full() {
            return Err(ConnectorSupervisorError::Validation(
                "qa fault barrier accepted more actors than declared".to_owned(),
            ));
        }
        if self.contains_actor(activation.actor.as_str()) {
            return Err(ConnectorSupervisorError::Validation(
                "qa fault barrier accepted one actor more than once".to_owned(),
            ));
        }
        self.actors.push(activation.actor);
        Ok(())
    }

    fn attach_entry(&mut self, entry: OutboxEntryRecord) -> Result<(), ConnectorSupervisorError> {
        let actor = qa_fault_actor(entry.outbox_id);
        if !self.contains_actor(actor.as_str()) {
            return Err(ConnectorSupervisorError::Validation(
                "claimed connector row does not belong to the active barrier".to_owned(),
            ));
        }
        if self.entries.iter().any(|candidate| qa_fault_actor(candidate.outbox_id) == actor) {
            return Err(ConnectorSupervisorError::Validation(
                "active connector barrier attached one outbox row more than once".to_owned(),
            ));
        }
        self.entries.push(entry);
        Ok(())
    }

    fn refresh(&mut self, active: QaFaultActiveBarrier) -> Result<(), ConnectorSupervisorError> {
        if active.activation_id != self.activation_id
            || active.point_id != self.point_id
            || usize::from(active.participants) != self.participants
        {
            return Err(ConnectorSupervisorError::Validation(
                "active connector barrier contract changed during batch preparation".to_owned(),
            ));
        }
        if self
            .entries
            .iter()
            .any(|entry| !active.actors.contains(&qa_fault_actor(entry.outbox_id)))
        {
            return Err(ConnectorSupervisorError::Validation(
                "prepared connector row is absent from durable barrier membership".to_owned(),
            ));
        }
        self.actors = active.actors;
        self.release_order = active.release_order;
        self.released_actors = active.released_actors;
        Ok(())
    }

    fn contains_actor(&self, actor: &str) -> bool {
        self.actors.iter().any(|candidate| candidate == actor)
    }

    fn missing_entry_actors(&self) -> Vec<String> {
        self.actors
            .iter()
            .filter(|actor| {
                !self
                    .entries
                    .iter()
                    .any(|entry| qa_fault_actor(entry.outbox_id).as_str() == actor.as_str())
            })
            .cloned()
            .collect()
    }

    fn is_full(&self) -> bool {
        self.actors.len() == self.participants
    }
}

// Envelope IDs are connector-scoped, so the outbox primary key prevents durable
// fault recovery from aliasing two connectors that reuse the same envelope ID.
fn qa_fault_actor(outbox_id: i64) -> String {
    format!("{QA_FAULT_OUTBOX_ACTOR_PREFIX}{outbox_id}")
}

#[cfg(feature = "qa-fault-injection")]
fn qa_fault_outbox_id(actor: &str) -> Result<i64, ConnectorSupervisorError> {
    actor
        .strip_prefix(QA_FAULT_OUTBOX_ACTOR_PREFIX)
        .and_then(|value| value.parse::<i64>().ok())
        .filter(|outbox_id| *outbox_id > 0)
        .ok_or_else(|| {
            ConnectorSupervisorError::Validation(format!(
                "invalid connector outbox fault actor {actor}"
            ))
        })
}

#[cfg(feature = "qa-fault-injection")]
fn qa_fault_outbox_ids(actors: &[String]) -> Result<Vec<i64>, ConnectorSupervisorError> {
    actors.iter().map(|actor| qa_fault_outbox_id(actor.as_str())).collect()
}

fn accumulate_dispatch_result(outcome: &mut DrainOutcome, result: DispatchResult) {
    match result {
        DispatchResult::Delivered => {
            outcome.delivered = outcome.delivered.saturating_add(1);
        }
        DispatchResult::Retried => {
            outcome.retried = outcome.retried.saturating_add(1);
        }
        DispatchResult::DeadLettered => {
            outcome.dead_lettered = outcome.dead_lettered.saturating_add(1);
        }
        DispatchResult::OutcomeUnknown => {
            outcome.outcome_unknown = outcome.outcome_unknown.saturating_add(1);
        }
    }
}
