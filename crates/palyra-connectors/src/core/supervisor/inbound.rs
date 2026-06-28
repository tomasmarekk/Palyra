//! Inbound ingestion path: validation, durable ingress enqueueing, claim-based
//! routing, delivery-intent materialization, response enqueueing, and the
//! immediate post-ingest drain.
//!
//! Every accepted inbound event is persisted before routing. Routed outputs
//! are represented as delivery intents before their deterministic outbox rows
//! are drained, so reroutes after a crash are idempotent.

use palyra_safety::sanitize_visible_assistant_text;
use serde_json::json;
use sha2::{Digest, Sha256};

use super::super::{
    protocol::{InboundMessageEvent, OutboundMessageRequest, RouteInboundResult},
    storage::{
        ChannelIngressRecord, ChannelIngressStatus, DeliveryIntentDraft, DeliveryIntentStatus,
        OutboxEnqueueOutcome,
    },
};
use super::{
    unix_ms_now, ConnectorSupervisor, ConnectorSupervisorError, DeliveryPipelineMode,
    InboundIngestOutcome,
};

/// Delivery timeout stamped onto outbound requests derived from routed
/// inbound events.
const ROUTED_OUTBOUND_TIMEOUT_MS: u64 = 30_000;

impl ConnectorSupervisor {
    /// Validates and enqueues an externally constructed outbound request;
    /// duplicates of an already queued envelope are a no-op.
    ///
    /// # Errors
    /// Returns validation errors, [`ConnectorSupervisorError::NotFound`] for
    /// unknown connectors, and store errors when persistence fails.
    pub fn enqueue_outbound(
        &self,
        request: &OutboundMessageRequest,
    ) -> Result<OutboxEnqueueOutcome, ConnectorSupervisorError> {
        request
            .validate(self.config.max_outbound_body_bytes)
            .map_err(|error| ConnectorSupervisorError::Validation(error.to_string()))?;
        let now = unix_ms_now()?;
        let Some(instance) = self.store.get_instance(request.connector_id.as_str())? else {
            return Err(ConnectorSupervisorError::NotFound(request.connector_id.clone()));
        };
        let outcome =
            self.store.enqueue_outbox_if_absent(request, self.config.max_retry_attempts, now)?;
        if outcome.created {
            self.store.record_event(
                instance.connector_id.as_str(),
                "outbox.enqueued",
                "info",
                "outbound message queued by direct enqueue operation",
                Some(&json!({
                    "envelope_id": request.envelope_id,
                    "conversation_id": request.conversation_id,
                    "text_bytes": request.text.len(),
                })),
                now,
            )?;
        }
        Ok(outcome)
    }

    /// Runs the full inbound pipeline for one event: validate, drop if the
    /// connector is disabled, dedupe, route, enqueue routed outputs, then
    /// immediately drain this connector's due outbox once.
    ///
    /// # Errors
    /// Returns validation, not-found, router, adapter, or store errors; soft
    /// rejections (disabled, duplicate, not routed) are reported through the
    /// returned [`super::InboundIngestOutcome`] instead.
    pub async fn ingest_inbound(
        &self,
        event: InboundMessageEvent,
    ) -> Result<InboundIngestOutcome, ConnectorSupervisorError> {
        event
            .validate(self.config.max_inbound_body_bytes)
            .map_err(|error| ConnectorSupervisorError::Validation(error.to_string()))?;
        if matches!(self.config.delivery_pipeline_mode, DeliveryPipelineMode::Off) {
            return self.ingest_inbound_legacy(event).await;
        }
        let now = unix_ms_now()?;
        let Some(instance) = self.store.get_instance(event.connector_id.as_str())? else {
            return Err(ConnectorSupervisorError::NotFound(event.connector_id));
        };
        if !instance.enabled {
            self.store.record_event(
                instance.connector_id.as_str(),
                "inbound.rejected",
                "warn",
                "inbound message dropped because connector is disabled",
                Some(&json!({
                    "envelope_id": event.envelope_id,
                })),
                now,
            )?;
            return Ok(InboundIngestOutcome {
                accepted: false,
                duplicate: false,
                queued_for_retry: false,
                decision_reason: "connector_disabled".to_owned(),
                route_key: None,
                ingress_event_id: None,
                ingress_status: None,
                delivery_intents: 0,
                sanitized_outputs: 0,
                enqueued_outbound: 0,
                immediate_delivery: 0,
            });
        }

        let ingress = self.store.enqueue_channel_ingress_if_absent(
            &event,
            instance.principal.as_str(),
            now,
            self.config.max_ingress_retry_attempts,
            self.config.inbound_dedupe_window_ms,
        )?;
        if !ingress.created {
            let ingress_event_id = ingress.record.ingress_event_id;
            let ingress_status = ingress.record.status.as_str().to_owned();
            let route_key = ingress.record.route_key.clone();
            self.store.record_event(
                instance.connector_id.as_str(),
                "inbound.duplicate",
                "info",
                "inbound duplicate ignored by durable ingress tombstone",
                Some(&json!({
                    "envelope_id": event.envelope_id,
                    "ingress_event_id": ingress.record.ingress_event_id,
                    "ingress_status": ingress.record.status.as_str(),
                    "payload_hash": ingress.record.payload_hash,
                })),
                now,
            )?;
            return Ok(InboundIngestOutcome {
                accepted: true,
                duplicate: true,
                queued_for_retry: false,
                decision_reason: "duplicate_envelope".to_owned(),
                route_key,
                ingress_event_id: Some(ingress_event_id),
                ingress_status: Some(ingress_status),
                delivery_intents: 0,
                sanitized_outputs: 0,
                enqueued_outbound: 0,
                immediate_delivery: 0,
            });
        }
        self.store.record_last_inbound(instance.connector_id.as_str(), now)?;
        self.store.record_event(
            instance.connector_id.as_str(),
            "inbound.received",
            "info",
            "inbound event accepted by supervisor",
            Some(&json!({
                "envelope_id": event.envelope_id,
                "conversation_id": event.conversation_id,
                "is_direct_message": event.is_direct_message,
                "requested_broadcast": event.requested_broadcast,
                "ingress_event_id": ingress.record.ingress_event_id,
                "payload_hash": ingress.record.payload_hash,
                "delivery_pipeline_mode": self.config.delivery_pipeline_mode.as_str(),
            })),
            now,
        )?;

        let mut outcomes = self
            .process_due_ingress_for_connector(
                instance.connector_id.as_str(),
                self.config.immediate_drain_batch_size,
                false,
            )
            .await?;
        if let Some(outcome) = outcomes
            .drain(..)
            .find(|outcome| outcome.ingress_event_id == Some(ingress.record.ingress_event_id))
        {
            return Ok(outcome);
        }

        Ok(InboundIngestOutcome {
            accepted: true,
            duplicate: false,
            queued_for_retry: false,
            decision_reason: "queued_for_ingress_worker".to_owned(),
            route_key: None,
            ingress_event_id: Some(ingress.record.ingress_event_id),
            ingress_status: Some(ChannelIngressStatus::Pending.as_str().to_owned()),
            delivery_intents: 0,
            sanitized_outputs: 0,
            enqueued_outbound: 0,
            immediate_delivery: 0,
        })
    }

    /// Claims and routes due durable ingress work across connectors.
    ///
    /// # Errors
    /// Returns store, clock, router, adapter, or validation errors raised by
    /// the claimed work.
    pub async fn process_due_ingress(
        &self,
        limit: usize,
    ) -> Result<Vec<InboundIngestOutcome>, ConnectorSupervisorError> {
        self.process_due_ingress_for_connector_filter(None, limit, false).await
    }

    /// Claims and routes due durable ingress work for one connector.
    ///
    /// # Errors
    /// Same as [`Self::process_due_ingress`].
    pub async fn process_due_ingress_for_connector(
        &self,
        connector_id: &str,
        limit: usize,
        ignore_queue_pause: bool,
    ) -> Result<Vec<InboundIngestOutcome>, ConnectorSupervisorError> {
        self.process_due_ingress_for_connector_filter(Some(connector_id), limit, ignore_queue_pause)
            .await
    }

    async fn process_due_ingress_for_connector_filter(
        &self,
        connector_filter: Option<&str>,
        limit: usize,
        ignore_queue_pause: bool,
    ) -> Result<Vec<InboundIngestOutcome>, ConnectorSupervisorError> {
        let now = unix_ms_now()?;
        let records = self.store.load_due_channel_ingress(
            now,
            limit,
            connector_filter,
            self.config.ingress_claim_lease_ms,
            ignore_queue_pause,
        )?;
        let mut outcomes = Vec::with_capacity(records.len());
        for record in records {
            outcomes.push(self.route_claimed_ingress(record).await?);
        }
        Ok(outcomes)
    }

    async fn route_claimed_ingress(
        &self,
        record: ChannelIngressRecord,
    ) -> Result<InboundIngestOutcome, ConnectorSupervisorError> {
        let now = unix_ms_now()?;
        let claim_token = record.claim_token.as_deref().ok_or_else(|| {
            ConnectorSupervisorError::Validation(format!(
                "claimed ingress {} has no claim token",
                record.ingress_event_id
            ))
        })?;
        let Some(instance) = self.store.get_instance(record.connector_id.as_str())? else {
            self.store.mark_channel_ingress_failed(
                record.ingress_event_id,
                claim_token,
                "connector_missing",
                "connector instance not found while routing ingress",
                now,
            )?;
            return Ok(ingress_terminal_outcome(
                &record,
                false,
                "connector_missing",
                ChannelIngressStatus::Failed,
            ));
        };
        if !instance.enabled {
            let next_attempt_unix_ms = now.saturating_add(
                i64::try_from(self.config.disabled_poll_delay_ms).unwrap_or(i64::MAX),
            );
            self.store.schedule_channel_ingress_retry(
                record.ingress_event_id,
                claim_token,
                "connector_disabled",
                "connector disabled",
                next_attempt_unix_ms,
            )?;
            return Ok(InboundIngestOutcome {
                accepted: false,
                duplicate: false,
                queued_for_retry: true,
                decision_reason: "connector_disabled".to_owned(),
                route_key: None,
                ingress_event_id: Some(record.ingress_event_id),
                ingress_status: Some(ChannelIngressStatus::Retrying.as_str().to_owned()),
                delivery_intents: 0,
                sanitized_outputs: 0,
                enqueued_outbound: 0,
                immediate_delivery: 0,
            });
        }

        let routed =
            match self.router.route_inbound(instance.principal.as_str(), &record.payload).await {
                Ok(routed) => routed,
                Err(error) => {
                    let message = error.to_string();
                    return self
                        .handle_ingress_route_error(&record, claim_token, message.as_str(), now)
                        .await;
                }
            };
        self.apply_route_result(&record, claim_token, &instance, routed, now).await
    }

    async fn apply_route_result(
        &self,
        record: &ChannelIngressRecord,
        claim_token: &str,
        instance: &super::super::storage::ConnectorInstanceRecord,
        routed: RouteInboundResult,
        now: i64,
    ) -> Result<InboundIngestOutcome, ConnectorSupervisorError> {
        if !routed.accepted {
            self.store.record_event(
                instance.connector_id.as_str(),
                "inbound.not_routed",
                if routed.queued_for_retry { "warn" } else { "info" },
                routed.decision_reason.as_str(),
                Some(&json!({
                    "envelope_id": record.envelope_id,
                    "ingress_event_id": record.ingress_event_id,
                    "queued_for_retry": routed.queued_for_retry,
                    "retry_attempt": routed.retry_attempt,
                    "route_message_latency_ms": routed.route_message_latency_ms,
                })),
                now,
            )?;
            if routed.queued_for_retry {
                return self
                    .retry_or_fail_ingress(
                        record,
                        claim_token,
                        "route_queued_for_retry",
                        routed.decision_reason.as_str(),
                        now,
                    )
                    .await;
            }
            self.store.mark_channel_ingress_completed(
                record.ingress_event_id,
                claim_token,
                routed.route_key.as_deref(),
                routed.session_id.as_deref(),
                routed.run_id.as_deref(),
                now,
            )?;
            return Ok(InboundIngestOutcome {
                accepted: false,
                duplicate: false,
                queued_for_retry: false,
                decision_reason: routed.decision_reason,
                route_key: routed.route_key,
                ingress_event_id: Some(record.ingress_event_id),
                ingress_status: Some(ChannelIngressStatus::Completed.as_str().to_owned()),
                delivery_intents: 0,
                sanitized_outputs: 0,
                enqueued_outbound: 0,
                immediate_delivery: 0,
            });
        }
        self.store.record_event(
            instance.connector_id.as_str(),
            "inbound.routed",
            "info",
            "inbound event routed to gateway",
            Some(&json!({
                "envelope_id": record.envelope_id,
                "ingress_event_id": record.ingress_event_id,
                "route_key": routed.route_key.clone(),
                "outputs": routed.outputs.len(),
                "retry_attempt": routed.retry_attempt,
                "route_message_latency_ms": routed.route_message_latency_ms,
                "session_id_present": routed.session_id.is_some(),
                "run_id_present": routed.run_id.is_some(),
            })),
            now,
        )?;

        let mut enqueued_outbound = 0usize;
        let mut delivery_intents = 0usize;
        let mut sanitized_outputs = 0usize;
        for (output_index, output) in routed.outputs.iter().enumerate() {
            let sanitization = sanitize_visible_assistant_text(output.text.as_str());
            if sanitization.redacted {
                sanitized_outputs = sanitized_outputs.saturating_add(1);
            }
            let base_request = OutboundMessageRequest {
                envelope_id: format!("{}:{output_index}", record.envelope_id),
                connector_id: instance.connector_id.clone(),
                conversation_id: record.payload.conversation_id.clone(),
                reply_thread_id: output.thread_id.clone(),
                in_reply_to_message_id: output.in_reply_to_message_id.clone(),
                text: sanitization.sanitized_text.clone(),
                broadcast: output.broadcast,
                auto_ack_text: output.auto_ack_text.clone(),
                auto_reaction: output.auto_reaction.clone(),
                attachments: output.attachments.clone(),
                structured_json: output.structured_json.clone(),
                a2ui_update: output.a2ui_update.clone(),
                timeout_ms: ROUTED_OUTBOUND_TIMEOUT_MS,
                max_payload_bytes: self.config.max_outbound_body_bytes,
            };
            base_request
                .validate(self.config.max_outbound_body_bytes)
                .map_err(|error| ConnectorSupervisorError::Validation(error.to_string()))?;

            let split_requests = if let Some(adapter) = self.adapters.get(&instance.kind) {
                adapter
                    .split_outbound(instance, &base_request)
                    .map_err(|error| ConnectorSupervisorError::Adapter(error.to_string()))?
            } else {
                vec![base_request]
            };
            if split_requests.is_empty() {
                let draft = delivery_intent_draft(
                    record,
                    &routed,
                    instance.principal.as_str(),
                    output_index,
                    format!("{}:{output_index}:suppressed", record.envelope_id),
                    "",
                    DeliveryIntentStatus::Suppressed,
                    None,
                )?;
                self.store.upsert_delivery_intent(&draft, now)?;
                delivery_intents = delivery_intents.saturating_add(1);
                continue;
            }
            for request in split_requests {
                request
                    .validate(self.config.max_outbound_body_bytes)
                    .map_err(|error| ConnectorSupervisorError::Validation(error.to_string()))?;
                let redaction_summary_json = if sanitization.redacted {
                    Some(serde_json::to_string(&sanitization.summary).map_err(|error| {
                        ConnectorSupervisorError::Validation(format!(
                            "failed to encode delivery redaction summary: {error}"
                        ))
                    })?)
                } else {
                    None
                };
                let draft = delivery_intent_draft(
                    record,
                    &routed,
                    instance.principal.as_str(),
                    delivery_intents,
                    request.envelope_id.clone(),
                    request.text.as_str(),
                    DeliveryIntentStatus::Queued,
                    redaction_summary_json,
                )?;
                self.store.upsert_delivery_intent(&draft, now)?;
                delivery_intents = delivery_intents.saturating_add(1);
                let enqueue = self.store.enqueue_outbox_if_absent(
                    &request,
                    self.config.max_retry_attempts,
                    now,
                )?;
                if enqueue.created {
                    enqueued_outbound = enqueued_outbound.saturating_add(1);
                    self.store.record_event(
                        instance.connector_id.as_str(),
                        "outbox.enqueued",
                        "info",
                        "outbound response queued for connector delivery",
                        Some(&json!({
                            "envelope_id": request.envelope_id,
                            "text_bytes": request.text.len(),
                            "delivery_intent": draft.intent_id,
                            "sanitized": sanitization.redacted,
                        })),
                        now,
                    )?;
                }
            }
        }

        self.store.mark_channel_ingress_completed(
            record.ingress_event_id,
            claim_token,
            routed.route_key.as_deref(),
            routed.session_id.as_deref(),
            routed.run_id.as_deref(),
            now,
        )?;
        let drain = self
            .drain_due_outbox_for_connector(
                instance.connector_id.as_str(),
                self.config.immediate_drain_batch_size,
            )
            .await?;
        Ok(InboundIngestOutcome {
            accepted: true,
            duplicate: false,
            queued_for_retry: false,
            decision_reason: "routed".to_owned(),
            route_key: routed.route_key,
            ingress_event_id: Some(record.ingress_event_id),
            ingress_status: Some(ChannelIngressStatus::Completed.as_str().to_owned()),
            delivery_intents,
            sanitized_outputs,
            enqueued_outbound,
            immediate_delivery: drain.delivered,
        })
    }

    async fn handle_ingress_route_error(
        &self,
        record: &ChannelIngressRecord,
        claim_token: &str,
        message: &str,
        now: i64,
    ) -> Result<InboundIngestOutcome, ConnectorSupervisorError> {
        match classify_ingress_route_error(message) {
            IngressRouteErrorClass::Quarantine => {
                self.store.mark_channel_ingress_quarantined(
                    record.ingress_event_id,
                    claim_token,
                    "route_quarantine",
                    message,
                    now,
                )?;
                Ok(ingress_terminal_outcome(
                    record,
                    false,
                    "route_quarantine",
                    ChannelIngressStatus::Quarantined,
                ))
            }
            IngressRouteErrorClass::HardFailure => {
                self.store.mark_channel_ingress_failed(
                    record.ingress_event_id,
                    claim_token,
                    "route_failed",
                    message,
                    now,
                )?;
                Ok(ingress_terminal_outcome(
                    record,
                    false,
                    "route_failed",
                    ChannelIngressStatus::Failed,
                ))
            }
            IngressRouteErrorClass::Retry => {
                self.retry_or_fail_ingress(record, claim_token, "route_retry", message, now).await
            }
        }
    }

    async fn retry_or_fail_ingress(
        &self,
        record: &ChannelIngressRecord,
        claim_token: &str,
        reason_code: &str,
        message: &str,
        now: i64,
    ) -> Result<InboundIngestOutcome, ConnectorSupervisorError> {
        if record.attempts >= record.max_attempts.min(self.config.max_ingress_retry_attempts).max(1)
        {
            self.store.mark_channel_ingress_failed(
                record.ingress_event_id,
                claim_token,
                reason_code,
                message,
                now,
            )?;
            return Ok(ingress_terminal_outcome(
                record,
                false,
                reason_code,
                ChannelIngressStatus::Failed,
            ));
        }
        let delay_ms = self.ingress_retry_delay_ms(record);
        let next_attempt_unix_ms = now.saturating_add(i64::try_from(delay_ms).unwrap_or(i64::MAX));
        self.store.schedule_channel_ingress_retry(
            record.ingress_event_id,
            claim_token,
            reason_code,
            message,
            next_attempt_unix_ms,
        )?;
        Ok(InboundIngestOutcome {
            accepted: false,
            duplicate: false,
            queued_for_retry: true,
            decision_reason: reason_code.to_owned(),
            route_key: record.route_key.clone(),
            ingress_event_id: Some(record.ingress_event_id),
            ingress_status: Some(ChannelIngressStatus::Retrying.as_str().to_owned()),
            delivery_intents: 0,
            sanitized_outputs: 0,
            enqueued_outbound: 0,
            immediate_delivery: 0,
        })
    }

    async fn ingest_inbound_legacy(
        &self,
        event: InboundMessageEvent,
    ) -> Result<InboundIngestOutcome, ConnectorSupervisorError> {
        let now = unix_ms_now()?;
        let Some(instance) = self.store.get_instance(event.connector_id.as_str())? else {
            return Err(ConnectorSupervisorError::NotFound(event.connector_id));
        };
        if !instance.enabled {
            return Ok(InboundIngestOutcome {
                accepted: false,
                duplicate: false,
                queued_for_retry: false,
                decision_reason: "connector_disabled".to_owned(),
                route_key: None,
                ingress_event_id: None,
                ingress_status: None,
                delivery_intents: 0,
                sanitized_outputs: 0,
                enqueued_outbound: 0,
                immediate_delivery: 0,
            });
        }
        let is_new = self.store.record_inbound_dedupe_if_new(
            instance.connector_id.as_str(),
            event.envelope_id.as_str(),
            now,
            self.config.inbound_dedupe_window_ms,
        )?;
        if !is_new {
            return Ok(InboundIngestOutcome {
                accepted: true,
                duplicate: true,
                queued_for_retry: false,
                decision_reason: "duplicate_envelope".to_owned(),
                route_key: None,
                ingress_event_id: None,
                ingress_status: None,
                delivery_intents: 0,
                sanitized_outputs: 0,
                enqueued_outbound: 0,
                immediate_delivery: 0,
            });
        }
        self.store.record_last_inbound(instance.connector_id.as_str(), now)?;
        let routed = self
            .router
            .route_inbound(instance.principal.as_str(), &event)
            .await
            .map_err(|error| ConnectorSupervisorError::Router(error.to_string()))?;
        if !routed.accepted {
            self.store.record_event(
                instance.connector_id.as_str(),
                "inbound.not_routed",
                if routed.queued_for_retry { "warn" } else { "info" },
                routed.decision_reason.as_str(),
                Some(&json!({
                    "envelope_id": event.envelope_id,
                    "queued_for_retry": routed.queued_for_retry,
                    "retry_attempt": routed.retry_attempt,
                    "route_message_latency_ms": routed.route_message_latency_ms,
                })),
                now,
            )?;
            return Ok(InboundIngestOutcome {
                accepted: false,
                duplicate: false,
                queued_for_retry: routed.queued_for_retry,
                decision_reason: routed.decision_reason,
                route_key: routed.route_key,
                ingress_event_id: None,
                ingress_status: None,
                delivery_intents: 0,
                sanitized_outputs: 0,
                enqueued_outbound: 0,
                immediate_delivery: 0,
            });
        }
        self.store.record_event(
            instance.connector_id.as_str(),
            "inbound.routed",
            "info",
            "inbound event routed to gateway",
            Some(&json!({
                "envelope_id": event.envelope_id,
                "route_key": routed.route_key.clone(),
                "outputs": routed.outputs.len(),
                "retry_attempt": routed.retry_attempt,
                "route_message_latency_ms": routed.route_message_latency_ms,
            })),
            now,
        )?;

        let mut enqueued_outbound = 0usize;
        for (index, output) in routed.outputs.iter().enumerate() {
            let base_request = OutboundMessageRequest {
                // Deriving the outbound envelope id from the inbound one plus
                // the output index keeps re-routing of the same inbound event
                // idempotent against the outbox uniqueness constraint.
                envelope_id: format!("{}:{index}", event.envelope_id),
                connector_id: instance.connector_id.clone(),
                conversation_id: event.conversation_id.clone(),
                reply_thread_id: output.thread_id.clone(),
                in_reply_to_message_id: output.in_reply_to_message_id.clone(),
                text: sanitize_visible_assistant_text(output.text.as_str()).sanitized_text,
                broadcast: output.broadcast,
                auto_ack_text: output.auto_ack_text.clone(),
                auto_reaction: output.auto_reaction.clone(),
                attachments: output.attachments.clone(),
                structured_json: output.structured_json.clone(),
                a2ui_update: output.a2ui_update.clone(),
                timeout_ms: ROUTED_OUTBOUND_TIMEOUT_MS,
                max_payload_bytes: self.config.max_outbound_body_bytes,
            };
            base_request
                .validate(self.config.max_outbound_body_bytes)
                .map_err(|error| ConnectorSupervisorError::Validation(error.to_string()))?;

            let split_requests = if let Some(adapter) = self.adapters.get(&instance.kind) {
                adapter
                    .split_outbound(&instance, &base_request)
                    .map_err(|error| ConnectorSupervisorError::Adapter(error.to_string()))?
            } else {
                vec![base_request]
            };
            if split_requests.is_empty() {
                continue;
            }
            for request in split_requests {
                request
                    .validate(self.config.max_outbound_body_bytes)
                    .map_err(|error| ConnectorSupervisorError::Validation(error.to_string()))?;
                let enqueue = self.store.enqueue_outbox_if_absent(
                    &request,
                    self.config.max_retry_attempts,
                    now,
                )?;
                if enqueue.created {
                    enqueued_outbound = enqueued_outbound.saturating_add(1);
                    self.store.record_event(
                        instance.connector_id.as_str(),
                        "outbox.enqueued",
                        "info",
                        "outbound response queued for connector delivery",
                        Some(&json!({
                            "envelope_id": request.envelope_id,
                            "text_bytes": request.text.len(),
                        })),
                        now,
                    )?;
                }
            }
        }

        let drain = self
            .drain_due_outbox_for_connector(
                instance.connector_id.as_str(),
                self.config.immediate_drain_batch_size,
            )
            .await?;
        Ok(super::InboundIngestOutcome {
            accepted: true,
            duplicate: false,
            queued_for_retry: false,
            decision_reason: "routed".to_owned(),
            route_key: routed.route_key,
            ingress_event_id: None,
            ingress_status: None,
            delivery_intents: 0,
            sanitized_outputs: 0,
            enqueued_outbound,
            immediate_delivery: drain.delivered,
        })
    }

    /// Polls every enabled connector's adapter for inbound events and ingests
    /// them, returning the number of events processed.
    ///
    /// A poll failure on one connector is logged as an event and skipped so
    /// the remaining connectors still make progress.
    ///
    /// # Errors
    /// Returns store/clock errors, or any error from ingesting a polled event.
    pub async fn poll_inbound(
        &self,
        per_connector_limit: usize,
    ) -> Result<usize, ConnectorSupervisorError> {
        let limit = per_connector_limit.max(1);
        let instances = self.store.list_instances()?;
        let mut processed = 0_usize;

        for instance in instances {
            if !instance.enabled {
                continue;
            }
            let Some(adapter) = self.adapters.get(&instance.kind) else {
                continue;
            };
            let inbound = match adapter.poll_inbound(&instance, limit).await {
                Ok(inbound) => inbound,
                Err(error) => {
                    let now = unix_ms_now()?;
                    self.store.record_event(
                        instance.connector_id.as_str(),
                        "inbound.poll_error",
                        "warn",
                        "adapter inbound poll failed; continuing with remaining connectors",
                        Some(&json!({
                            "error": error.to_string(),
                        })),
                        now,
                    )?;
                    continue;
                }
            };
            for event in inbound {
                self.ingest_inbound(event).await?;
                processed = processed.saturating_add(1);
            }
        }

        Ok(processed)
    }

    fn ingress_retry_delay_ms(&self, record: &ChannelIngressRecord) -> u64 {
        let exponent = record.attempts.saturating_sub(1).min(10);
        let base = self
            .config
            .base_retry_delay_ms
            .saturating_mul(1_u64 << exponent)
            .max(self.config.min_retry_delay_ms)
            .min(self.config.max_retry_delay_ms);
        let jitter_span = (base / 4).clamp(1, 1_000);
        let jitter = u64::try_from(record.ingress_event_id)
            .unwrap_or(0)
            .wrapping_add(u64::from(record.attempts))
            % jitter_span;
        base.saturating_add(jitter).min(self.config.max_retry_delay_ms)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum IngressRouteErrorClass {
    Retry,
    HardFailure,
    Quarantine,
}

fn classify_ingress_route_error(message: &str) -> IngressRouteErrorClass {
    let normalized = message.to_ascii_lowercase();
    if [
        "malformed",
        "invalid payload",
        "invalid envelope",
        "schema",
        "deserialize",
        "poison",
        "canonical",
    ]
    .iter()
    .any(|needle| normalized.contains(needle))
    {
        return IngressRouteErrorClass::Quarantine;
    }
    if ["policy denied", "forbidden", "unauthorized", "permission denied"]
        .iter()
        .any(|needle| normalized.contains(needle))
    {
        return IngressRouteErrorClass::HardFailure;
    }
    IngressRouteErrorClass::Retry
}

#[allow(clippy::too_many_arguments)]
fn delivery_intent_draft(
    record: &ChannelIngressRecord,
    routed: &RouteInboundResult,
    principal: &str,
    output_index: usize,
    outbox_envelope_id: String,
    visible_text: &str,
    status: DeliveryIntentStatus,
    redaction_summary_json: Option<String>,
) -> Result<DeliveryIntentDraft, ConnectorSupervisorError> {
    let output_index_u32 = u32::try_from(output_index).map_err(|_| {
        ConnectorSupervisorError::Validation("delivery output index overflow".into())
    })?;
    let payload_hash = sha256_hex(visible_text.as_bytes());
    let visible_text_preview = preview_visible_text(visible_text);
    Ok(DeliveryIntentDraft {
        intent_id: format!(
            "delivery:{}:{}:{}",
            record.connector_id, record.ingress_event_id, outbox_envelope_id
        ),
        connector_id: record.connector_id.clone(),
        ingress_event_id: record.ingress_event_id,
        ingress_envelope_id: record.envelope_id.clone(),
        session_id: routed.session_id.clone(),
        run_id: routed.run_id.clone(),
        principal: principal.to_owned(),
        conversation_id: record.conversation_id.clone(),
        outbox_envelope_id,
        output_index: output_index_u32,
        payload_hash,
        visible_text_preview,
        status,
        redaction_summary_json,
    })
}

fn ingress_terminal_outcome(
    record: &ChannelIngressRecord,
    accepted: bool,
    reason: &str,
    status: ChannelIngressStatus,
) -> InboundIngestOutcome {
    InboundIngestOutcome {
        accepted,
        duplicate: false,
        queued_for_retry: false,
        decision_reason: reason.to_owned(),
        route_key: record.route_key.clone(),
        ingress_event_id: Some(record.ingress_event_id),
        ingress_status: Some(status.as_str().to_owned()),
        delivery_intents: 0,
        sanitized_outputs: 0,
        enqueued_outbound: 0,
        immediate_delivery: 0,
    }
}

fn preview_visible_text(text: &str) -> String {
    let mut preview = text.split_whitespace().collect::<Vec<_>>().join(" ");
    if preview.len() > 160 {
        preview.truncate(160);
    }
    preview
}

fn sha256_hex(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    digest.iter().map(|byte| format!("{byte:02x}")).collect()
}
