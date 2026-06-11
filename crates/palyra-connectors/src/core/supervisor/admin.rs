//! Operator-facing supervisor surface: connector registration and lifecycle,
//! status/runtime snapshots, message read/search/mutation passthrough, queue
//! pause control, and dead-letter administration.
//!
//! Every operation here records an audit event in the connector event log;
//! message operations additionally validate adapter results before returning.

use std::sync::Arc;

use serde_json::{json, Map, Value};

use crate::providers::{provider_availability, provider_capabilities};

use super::super::{
    protocol::{
        ConnectorAvailability, ConnectorCapabilitySet, ConnectorInstanceSpec, ConnectorKind,
        ConnectorMessageDeleteRequest, ConnectorMessageEditRequest, ConnectorMessageMutationResult,
        ConnectorMessageReactionRequest, ConnectorMessageReadRequest, ConnectorMessageReadResult,
        ConnectorMessageSearchRequest, ConnectorMessageSearchResult, ConnectorStatusSnapshot,
    },
    storage::{
        ConnectorEventRecord, ConnectorInstanceRecord, ConnectorQueueSnapshot, DeadLetterRecord,
    },
};
use super::metrics::build_saturation_snapshot;
use super::{unix_ms_now, ConnectorAdapter, ConnectorSupervisor, ConnectorSupervisorError};

impl ConnectorSupervisor {
    /// Registers (or re-registers) a connector instance and records the event.
    ///
    /// # Errors
    /// Returns a store error when the spec is invalid or persistence fails.
    pub fn register_connector(
        &self,
        spec: &ConnectorInstanceSpec,
    ) -> Result<(), ConnectorSupervisorError> {
        let now = unix_ms_now()?;
        self.store.upsert_instance(spec, now)?;
        self.store.record_event(
            spec.connector_id.as_str(),
            "connector.registered",
            "info",
            "connector instance registered",
            Some(&json!({
                "connector_id": spec.connector_id,
                "kind": spec.kind.as_str(),
                "availability": provider_availability(spec.kind).as_str(),
                "enabled": spec.enabled,
            })),
            now,
        )?;
        Ok(())
    }

    /// Enables or disables a connector, stopping its adapter runtime first
    /// when disabling, and returns the resulting status.
    ///
    /// # Errors
    /// Returns [`ConnectorSupervisorError::NotFound`] for unknown ids, and
    /// adapter/store errors when runtime shutdown or persistence fails.
    pub fn set_enabled(
        &self,
        connector_id: &str,
        enabled: bool,
    ) -> Result<ConnectorStatusSnapshot, ConnectorSupervisorError> {
        let now = unix_ms_now()?;
        if !enabled {
            let Some(instance) = self.store.get_instance(connector_id)? else {
                return Err(ConnectorSupervisorError::NotFound(connector_id.to_owned()));
            };
            self.stop_adapter_runtime(&instance)?;
        }
        self.store.set_instance_enabled(connector_id, enabled, now)?;
        self.store.record_event(
            connector_id,
            "connector.enabled_changed",
            "info",
            if enabled { "connector enabled" } else { "connector disabled" },
            Some(&json!({ "enabled": enabled })),
            now,
        )?;
        self.status(connector_id)
    }

    /// Stops the adapter runtime and deletes the instance with its queue rows.
    ///
    /// # Errors
    /// Returns [`ConnectorSupervisorError::NotFound`] for unknown ids, and
    /// adapter/store errors when shutdown or deletion fails.
    pub fn remove_connector(&self, connector_id: &str) -> Result<(), ConnectorSupervisorError> {
        let Some(instance) = self.store.get_instance(connector_id)? else {
            return Err(ConnectorSupervisorError::NotFound(connector_id.to_owned()));
        };
        self.stop_adapter_runtime(&instance)?;
        self.store.delete_instance(connector_id)?;
        Ok(())
    }

    /// Returns the status snapshot for one connector.
    ///
    /// # Errors
    /// Returns [`ConnectorSupervisorError::NotFound`] for unknown ids and a
    /// store error when reads fail.
    pub fn status(
        &self,
        connector_id: &str,
    ) -> Result<ConnectorStatusSnapshot, ConnectorSupervisorError> {
        let Some(instance) = self.store.get_instance(connector_id)? else {
            return Err(ConnectorSupervisorError::NotFound(connector_id.to_owned()));
        };
        self.status_snapshot_for(instance)
    }

    /// Returns status snapshots for all registered connectors.
    ///
    /// # Errors
    /// Returns a store error when reads fail.
    pub fn list_status(&self) -> Result<Vec<ConnectorStatusSnapshot>, ConnectorSupervisorError> {
        let instances = self.store.list_instances()?;
        let mut snapshots = Vec::with_capacity(instances.len());
        for instance in instances {
            snapshots.push(self.status_snapshot_for(instance)?);
        }
        Ok(snapshots)
    }

    fn status_snapshot_for(
        &self,
        instance: ConnectorInstanceRecord,
    ) -> Result<ConnectorStatusSnapshot, ConnectorSupervisorError> {
        let queue_depth = self.store.queue_depth(instance.connector_id.as_str())?;
        Ok(ConnectorStatusSnapshot {
            connector_id: instance.connector_id,
            kind: instance.kind,
            availability: self.connector_availability(instance.kind),
            capabilities: self.connector_capabilities(instance.kind),
            principal: instance.principal,
            enabled: instance.enabled,
            readiness: instance.readiness,
            liveness: instance.liveness,
            restart_count: instance.restart_count,
            queue_depth,
            last_error: instance.last_error,
            last_inbound_unix_ms: instance.last_inbound_unix_ms,
            last_outbound_unix_ms: instance.last_outbound_unix_ms,
            updated_at_unix_ms: instance.updated_at_unix_ms,
        })
    }

    /// Builds the runtime JSON snapshot: adapter state (when provided) merged
    /// with queue, metrics, and saturation views.
    ///
    /// # Errors
    /// Returns [`ConnectorSupervisorError::NotFound`] for unknown ids and a
    /// store error when reads fail.
    pub fn runtime_snapshot(
        &self,
        connector_id: &str,
    ) -> Result<Option<Value>, ConnectorSupervisorError> {
        let Some(instance) = self.store.get_instance(connector_id)? else {
            return Err(ConnectorSupervisorError::NotFound(connector_id.to_owned()));
        };
        let queue = self.queue_snapshot(instance.connector_id.as_str())?;
        let adapter_runtime = self
            .adapters
            .get(&instance.kind)
            .and_then(|adapter| adapter.runtime_snapshot(&instance));
        let metrics = self.build_runtime_metrics(instance.connector_id.as_str())?;
        let mut runtime = match adapter_runtime {
            Some(Value::Object(object)) => Value::Object(object),
            Some(other) => {
                let mut object = Map::new();
                object.insert("adapter".to_owned(), other);
                Value::Object(object)
            }
            None => Value::Object(Map::new()),
        };
        if let Some(object) = runtime.as_object_mut() {
            object.insert("metrics".to_owned(), json!(metrics));
            object.insert("queue".to_owned(), json!(queue));
            object.insert("saturation".to_owned(), json!(build_saturation_snapshot(&queue)));
        }
        Ok(Some(runtime))
    }

    /// Reads messages through the connector's adapter, validating the request
    /// and result and recording an audit event.
    ///
    /// # Errors
    /// Returns validation, not-found, missing-adapter, or adapter errors.
    pub async fn read_messages(
        &self,
        connector_id: &str,
        request: &ConnectorMessageReadRequest,
    ) -> Result<ConnectorMessageReadResult, ConnectorSupervisorError> {
        request
            .validate()
            .map_err(|error| ConnectorSupervisorError::Validation(error.to_string()))?;
        let (instance, adapter) = self.instance_and_adapter(connector_id)?;
        let result = adapter
            .read_messages(&instance, request)
            .await
            .map_err(|error| ConnectorSupervisorError::Adapter(error.to_string()))?;
        result
            .validate()
            .map_err(|error| ConnectorSupervisorError::Validation(error.to_string()))?;
        self.record_message_admin_event(
            instance.connector_id.as_str(),
            result.preflight.allowed,
            result.preflight.audit_event_type.as_str(),
            "connector message read completed",
            Some(&json!({
                "policy_action": result.preflight.policy_action,
                "approval_mode": result.preflight.approval_mode,
                "risk_level": result.preflight.risk_level,
                "reason": result.preflight.reason,
                "conversation_id": result.target.conversation_id,
                "thread_id": result.target.thread_id,
                "exact_message_id": result.exact_message_id,
                "messages_returned": result.messages.len(),
                "next_before_message_id": result.next_before_message_id,
                "next_after_message_id": result.next_after_message_id,
            })),
        )?;
        Ok(result)
    }

    /// Searches messages through the connector's adapter, validating the
    /// request and result and recording an audit event.
    ///
    /// # Errors
    /// Returns validation, not-found, missing-adapter, or adapter errors.
    pub async fn search_messages(
        &self,
        connector_id: &str,
        request: &ConnectorMessageSearchRequest,
    ) -> Result<ConnectorMessageSearchResult, ConnectorSupervisorError> {
        request
            .validate()
            .map_err(|error| ConnectorSupervisorError::Validation(error.to_string()))?;
        let (instance, adapter) = self.instance_and_adapter(connector_id)?;
        let result = adapter
            .search_messages(&instance, request)
            .await
            .map_err(|error| ConnectorSupervisorError::Adapter(error.to_string()))?;
        result
            .validate()
            .map_err(|error| ConnectorSupervisorError::Validation(error.to_string()))?;
        self.record_message_admin_event(
            instance.connector_id.as_str(),
            result.preflight.allowed,
            result.preflight.audit_event_type.as_str(),
            "connector message search completed",
            Some(&json!({
                "policy_action": result.preflight.policy_action,
                "approval_mode": result.preflight.approval_mode,
                "risk_level": result.preflight.risk_level,
                "reason": result.preflight.reason,
                "conversation_id": result.target.conversation_id,
                "thread_id": result.target.thread_id,
                "query": result.query,
                "author_id": result.author_id,
                "has_attachments": result.has_attachments,
                "matches_returned": result.matches.len(),
                "next_before_message_id": result.next_before_message_id,
            })),
        )?;
        Ok(result)
    }

    /// Edits a message through the connector's adapter, validating the
    /// request and result and recording an audit event.
    ///
    /// # Errors
    /// Returns validation, not-found, missing-adapter, or adapter errors.
    pub async fn edit_message(
        &self,
        connector_id: &str,
        request: &ConnectorMessageEditRequest,
    ) -> Result<ConnectorMessageMutationResult, ConnectorSupervisorError> {
        request
            .validate()
            .map_err(|error| ConnectorSupervisorError::Validation(error.to_string()))?;
        let (instance, adapter) = self.instance_and_adapter(connector_id)?;
        let result = adapter
            .edit_message(&instance, request)
            .await
            .map_err(|error| ConnectorSupervisorError::Adapter(error.to_string()))?;
        self.validate_and_record_mutation_result(
            instance.connector_id.as_str(),
            &result,
            "connector message edit completed",
        )?;
        Ok(result)
    }

    /// Deletes a message through the connector's adapter, validating the
    /// request and result and recording an audit event.
    ///
    /// # Errors
    /// Returns validation, not-found, missing-adapter, or adapter errors.
    pub async fn delete_message(
        &self,
        connector_id: &str,
        request: &ConnectorMessageDeleteRequest,
    ) -> Result<ConnectorMessageMutationResult, ConnectorSupervisorError> {
        request
            .validate()
            .map_err(|error| ConnectorSupervisorError::Validation(error.to_string()))?;
        let (instance, adapter) = self.instance_and_adapter(connector_id)?;
        let result = adapter
            .delete_message(&instance, request)
            .await
            .map_err(|error| ConnectorSupervisorError::Adapter(error.to_string()))?;
        self.validate_and_record_mutation_result(
            instance.connector_id.as_str(),
            &result,
            "connector message delete completed",
        )?;
        Ok(result)
    }

    /// Adds a reaction through the connector's adapter, validating the
    /// request and result and recording an audit event.
    ///
    /// # Errors
    /// Returns validation, not-found, missing-adapter, or adapter errors.
    pub async fn add_reaction(
        &self,
        connector_id: &str,
        request: &ConnectorMessageReactionRequest,
    ) -> Result<ConnectorMessageMutationResult, ConnectorSupervisorError> {
        request
            .validate()
            .map_err(|error| ConnectorSupervisorError::Validation(error.to_string()))?;
        let (instance, adapter) = self.instance_and_adapter(connector_id)?;
        let result = adapter
            .add_reaction(&instance, request)
            .await
            .map_err(|error| ConnectorSupervisorError::Adapter(error.to_string()))?;
        self.validate_and_record_mutation_result(
            instance.connector_id.as_str(),
            &result,
            "connector reaction add completed",
        )?;
        Ok(result)
    }

    /// Removes a reaction through the connector's adapter, validating the
    /// request and result and recording an audit event.
    ///
    /// # Errors
    /// Returns validation, not-found, missing-adapter, or adapter errors.
    pub async fn remove_reaction(
        &self,
        connector_id: &str,
        request: &ConnectorMessageReactionRequest,
    ) -> Result<ConnectorMessageMutationResult, ConnectorSupervisorError> {
        request
            .validate()
            .map_err(|error| ConnectorSupervisorError::Validation(error.to_string()))?;
        let (instance, adapter) = self.instance_and_adapter(connector_id)?;
        let result = adapter
            .remove_reaction(&instance, request)
            .await
            .map_err(|error| ConnectorSupervisorError::Adapter(error.to_string()))?;
        self.validate_and_record_mutation_result(
            instance.connector_id.as_str(),
            &result,
            "connector reaction removal completed",
        )?;
        Ok(result)
    }

    /// Lists up to `limit` connector log events, newest first.
    ///
    /// # Errors
    /// Returns a store error when the read fails.
    pub fn list_logs(
        &self,
        connector_id: &str,
        limit: usize,
    ) -> Result<Vec<ConnectorEventRecord>, ConnectorSupervisorError> {
        self.store.list_events(connector_id, limit).map_err(ConnectorSupervisorError::from)
    }

    /// Lists up to `limit` dead letters, newest first.
    ///
    /// # Errors
    /// Returns a store error when the read fails.
    pub fn list_dead_letters(
        &self,
        connector_id: &str,
        limit: usize,
    ) -> Result<Vec<DeadLetterRecord>, ConnectorSupervisorError> {
        self.store.list_dead_letters(connector_id, limit).map_err(ConnectorSupervisorError::from)
    }

    /// Returns the full queue snapshot evaluated at the current time.
    ///
    /// # Errors
    /// Returns clock or store errors when the snapshot cannot be built.
    pub fn queue_snapshot(
        &self,
        connector_id: &str,
    ) -> Result<ConnectorQueueSnapshot, ConnectorSupervisorError> {
        let now = unix_ms_now()?;
        self.store.queue_snapshot(connector_id, now).map_err(ConnectorSupervisorError::from)
    }

    /// Pauses or resumes the connector's outbox, records the event, and
    /// returns the updated queue snapshot.
    ///
    /// # Errors
    /// Returns [`ConnectorSupervisorError::NotFound`] for unknown ids and a
    /// store error when persistence fails.
    pub fn set_queue_paused(
        &self,
        connector_id: &str,
        paused: bool,
        reason: Option<&str>,
    ) -> Result<ConnectorQueueSnapshot, ConnectorSupervisorError> {
        let now = unix_ms_now()?;
        self.store.set_queue_paused(connector_id, paused, reason, now)?;
        self.store.record_event(
            connector_id,
            if paused { "queue.paused" } else { "queue.resumed" },
            "info",
            if paused { "connector outbox queue paused" } else { "connector outbox queue resumed" },
            Some(&json!({
                "paused": paused,
                "reason": reason,
            })),
            now,
        )?;
        self.queue_snapshot(connector_id)
    }

    /// Replays one dead letter into the outbox with a fresh retry budget and
    /// records the event.
    ///
    /// # Errors
    /// Returns a store error when the dead letter is missing or replay fails.
    pub fn replay_dead_letter(
        &self,
        connector_id: &str,
        dead_letter_id: i64,
    ) -> Result<DeadLetterRecord, ConnectorSupervisorError> {
        let now = unix_ms_now()?;
        let replayed = self.store.replay_dead_letter(
            connector_id,
            dead_letter_id,
            self.config.max_retry_attempts,
            now,
        )?;
        self.store.record_event(
            connector_id,
            "dead_letter.replayed",
            "info",
            "dead-letter entry replayed into outbox",
            Some(&json!({
                "dead_letter_id": dead_letter_id,
                "envelope_id": replayed.envelope_id,
            })),
            now,
        )?;
        Ok(replayed)
    }

    /// Discards one dead letter permanently and records the event.
    ///
    /// # Errors
    /// Returns a store error when the dead letter is missing or deletion fails.
    pub fn discard_dead_letter(
        &self,
        connector_id: &str,
        dead_letter_id: i64,
    ) -> Result<DeadLetterRecord, ConnectorSupervisorError> {
        let now = unix_ms_now()?;
        let discarded = self.store.discard_dead_letter(connector_id, dead_letter_id)?;
        self.store.record_event(
            connector_id,
            "dead_letter.discarded",
            "info",
            "dead-letter entry discarded by operator",
            Some(&json!({
                "dead_letter_id": dead_letter_id,
                "envelope_id": discarded.envelope_id,
            })),
            now,
        )?;
        Ok(discarded)
    }

    // Status remains reportable for kinds without a registered adapter by
    // falling back to the static provider registry.
    fn connector_availability(&self, kind: ConnectorKind) -> ConnectorAvailability {
        self.adapters
            .get(&kind)
            .map(|adapter| adapter.availability())
            .unwrap_or_else(|| provider_availability(kind))
    }

    fn connector_capabilities(&self, kind: ConnectorKind) -> ConnectorCapabilitySet {
        self.adapters
            .get(&kind)
            .map(|adapter| adapter.capabilities())
            .unwrap_or_else(|| provider_capabilities(kind))
    }

    fn instance_and_adapter(
        &self,
        connector_id: &str,
    ) -> Result<(ConnectorInstanceRecord, Arc<dyn ConnectorAdapter>), ConnectorSupervisorError>
    {
        let Some(instance) = self.store.get_instance(connector_id)? else {
            return Err(ConnectorSupervisorError::NotFound(connector_id.to_owned()));
        };
        let Some(adapter) = self.adapters.get(&instance.kind).cloned() else {
            return Err(ConnectorSupervisorError::MissingAdapter(instance.kind));
        };
        Ok((instance, adapter))
    }

    fn stop_adapter_runtime(
        &self,
        instance: &ConnectorInstanceRecord,
    ) -> Result<(), ConnectorSupervisorError> {
        if let Some(adapter) = self.adapters.get(&instance.kind) {
            adapter
                .stop_runtime(instance.connector_id.as_str())
                .map_err(|error| ConnectorSupervisorError::Adapter(error.to_string()))?;
        }
        Ok(())
    }

    fn validate_and_record_mutation_result(
        &self,
        connector_id: &str,
        result: &ConnectorMessageMutationResult,
        message: &'static str,
    ) -> Result<(), ConnectorSupervisorError> {
        result
            .validate()
            .map_err(|error| ConnectorSupervisorError::Validation(error.to_string()))?;
        self.record_message_admin_event(
            connector_id,
            result.preflight.allowed,
            result.preflight.audit_event_type.as_str(),
            message,
            Some(&json!({
                "policy_action": result.preflight.policy_action,
                "approval_mode": result.preflight.approval_mode,
                "risk_level": result.preflight.risk_level,
                "preflight_reason": result.preflight.reason,
                "message_id": result.locator.message_id,
                "conversation_id": result.locator.target.conversation_id,
                "thread_id": result.locator.target.thread_id,
                "status": result.status,
                "result_reason": result.reason,
                "has_message": result.message.is_some(),
                "has_diff": result.diff.is_some(),
            })),
        )
    }

    fn record_message_admin_event(
        &self,
        connector_id: &str,
        allowed: bool,
        event_type: &str,
        message: &str,
        details: Option<&Value>,
    ) -> Result<(), ConnectorSupervisorError> {
        let now = unix_ms_now()?;
        self.store.record_event(
            connector_id,
            event_type,
            if allowed { "info" } else { "warn" },
            message,
            details,
            now,
        )?;
        Ok(())
    }
}
