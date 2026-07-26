//! Durable, redacted lifecycle state for the session-bound context engine.
//!
//! The authoritative transcript remains host-owned. This module records only
//! counters, digests, engine identity, health, and projection epochs so a
//! daemon restart can recover context-engine calibration without persisting
//! prompt text, tool output, or custom-engine payloads.

use std::sync::Arc;

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tonic::Status;
use ulid::Ulid;

use crate::{
    application::context_engine::{
        ContextEngineAfterTurnInput, ContextEngineBootstrapInput, ContextEngineDescriptor,
        ContextEngineIngestEvent, ContextEngineRegistry,
    },
    gateway::GatewayRuntimeState,
    journal::{OrchestratorSessionTranscriptRecord, OrchestratorTapeAppendRequest},
    model_provider::ProviderFinishReason,
};

/// Tape event that carries the restart-safe context lifecycle projection.
pub(crate) const CONTEXT_LIFECYCLE_EVENT: &str = "context.lifecycle";
pub(crate) const CONTEXT_ENGINE_BINDING_SCHEMA_VERSION: u32 = 2;
pub(crate) const CONTEXT_ENGINE_STATE_SCHEMA_VERSION: u32 = 1;
pub(crate) const CONTEXT_LIFECYCLE_EVENT_SCHEMA_VERSION: u32 = 1;
const DEFAULT_CONTEXT_ENGINE_ID: &str = "default_context_engine";
const DEFAULT_CONTEXT_ENGINE_VERSION: &str = "context_engine.default.v1";
const CALIBRATION_SCALE_BASIS_POINTS: u64 = 10_000;
const MAX_CALIBRATION_BASIS_POINTS: u64 = 100_000;

fn finish_reason_label(reason: ProviderFinishReason) -> &'static str {
    match reason {
        ProviderFinishReason::Stop => "stop",
        ProviderFinishReason::Length => "length",
        ProviderFinishReason::ToolCalls => "tool_calls",
        ProviderFinishReason::ContentFilter => "content_filter",
        ProviderFinishReason::Cancelled => "cancelled",
        ProviderFinishReason::Error => "error",
        ProviderFinishReason::Unknown => "unknown",
    }
}

/// Health of the engine selected for one session projection epoch.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ContextEngineHealth {
    Healthy,
    Degraded,
    Quarantined,
}

impl ContextEngineHealth {
    #[must_use]
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Healthy => "healthy",
            Self::Degraded => "degraded",
            Self::Quarantined => "quarantined",
        }
    }
}

/// Lifecycle boundary represented by one replay-visible event.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ContextLifecyclePoint {
    BootstrapSession,
    IngestMessage,
    IngestToolExchange,
    BeforePrompt,
    AfterTurn,
    SessionEndUnsupported,
    EngineDegraded,
}

impl ContextLifecyclePoint {
    #[must_use]
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::BootstrapSession => "bootstrap_session",
            Self::IngestMessage => "ingest_message",
            Self::IngestToolExchange => "ingest_tool_exchange",
            Self::BeforePrompt => "before_prompt",
            Self::AfterTurn => "after_turn",
            Self::SessionEndUnsupported => "session_end_unsupported",
            Self::EngineDegraded => "engine_degraded",
        }
    }
}

/// Redacted state sufficient to resume lifecycle counters and token calibration.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct ContextEngineStateSnapshot {
    pub(crate) schema_version: u32,
    pub(crate) lifecycle_sequence: u64,
    pub(crate) ingested_message_count: u64,
    pub(crate) ingested_tool_exchange_count: u64,
    pub(crate) completed_turn_count: u64,
    pub(crate) observed_prompt_tokens: u64,
    pub(crate) observed_completion_tokens: u64,
    pub(crate) estimate_calibration_basis_points: u64,
    pub(crate) persisted_redacted: bool,
    pub(crate) state_sha256: String,
}

impl ContextEngineStateSnapshot {
    fn empty() -> Self {
        let mut state = Self {
            schema_version: CONTEXT_ENGINE_STATE_SCHEMA_VERSION,
            lifecycle_sequence: 0,
            ingested_message_count: 0,
            ingested_tool_exchange_count: 0,
            completed_turn_count: 0,
            observed_prompt_tokens: 0,
            observed_completion_tokens: 0,
            estimate_calibration_basis_points: CALIBRATION_SCALE_BASIS_POINTS,
            persisted_redacted: true,
            state_sha256: String::new(),
        };
        state.refresh_hash();
        state
    }

    fn refresh_hash(&mut self) {
        self.state_sha256 = crate::sha256_hex(
            serde_json::to_vec(&json!({
                "schema_version": self.schema_version,
                "lifecycle_sequence": self.lifecycle_sequence,
                "ingested_message_count": self.ingested_message_count,
                "ingested_tool_exchange_count": self.ingested_tool_exchange_count,
                "completed_turn_count": self.completed_turn_count,
                "observed_prompt_tokens": self.observed_prompt_tokens,
                "observed_completion_tokens": self.observed_completion_tokens,
                "estimate_calibration_basis_points": self.estimate_calibration_basis_points,
                "persisted_redacted": self.persisted_redacted,
            }))
            .unwrap_or_default()
            .as_slice(),
        );
    }

    fn validate(&self) -> Result<(), &'static str> {
        if self.schema_version != CONTEXT_ENGINE_STATE_SCHEMA_VERSION {
            return Err("context.lifecycle.state_schema_unsupported");
        }
        if !self.persisted_redacted {
            return Err("context.lifecycle.state_not_redacted");
        }
        if self.estimate_calibration_basis_points > MAX_CALIBRATION_BASIS_POINTS {
            return Err("context.lifecycle.calibration_out_of_range");
        }
        let mut expected = self.clone();
        expected.refresh_hash();
        if expected.state_sha256 != self.state_sha256 {
            return Err("context.lifecycle.state_digest_mismatch");
        }
        Ok(())
    }
}

/// Durable engine binding for one session projection epoch.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct ContextEngineBindingV2 {
    pub(crate) schema_version: u32,
    pub(crate) binding_id: String,
    pub(crate) engine_id: String,
    pub(crate) engine_version: String,
    pub(crate) projection_epoch: u64,
    pub(crate) health: ContextEngineHealth,
    pub(crate) state: ContextEngineStateSnapshot,
}

impl ContextEngineBindingV2 {
    fn new(descriptor: &ContextEngineDescriptor) -> Self {
        Self {
            schema_version: CONTEXT_ENGINE_BINDING_SCHEMA_VERSION,
            binding_id: Ulid::new().to_string(),
            engine_id: descriptor.engine_id.clone(),
            engine_version: descriptor.version.clone(),
            projection_epoch: 1,
            health: ContextEngineHealth::Healthy,
            state: ContextEngineStateSnapshot::empty(),
        }
    }

    fn safe_builtin(projection_epoch: u64, health: ContextEngineHealth) -> Self {
        Self {
            schema_version: CONTEXT_ENGINE_BINDING_SCHEMA_VERSION,
            binding_id: Ulid::new().to_string(),
            engine_id: DEFAULT_CONTEXT_ENGINE_ID.to_owned(),
            engine_version: DEFAULT_CONTEXT_ENGINE_VERSION.to_owned(),
            projection_epoch: projection_epoch.max(1),
            health,
            state: ContextEngineStateSnapshot::empty(),
        }
    }

    fn validate(&self) -> Result<(), &'static str> {
        if self.schema_version != CONTEXT_ENGINE_BINDING_SCHEMA_VERSION {
            return Err("context.lifecycle.binding_schema_unsupported");
        }
        if Ulid::from_string(self.binding_id.as_str()).is_err() {
            return Err("context.lifecycle.binding_id_invalid");
        }
        if self.engine_id.trim().is_empty()
            || self.engine_version.trim().is_empty()
            || self.projection_epoch == 0
        {
            return Err("context.lifecycle.binding_identity_invalid");
        }
        self.state.validate()
    }

    #[must_use]
    pub(crate) fn diagnostics_json(
        &self,
        point: ContextLifecyclePoint,
        reason_code: &str,
    ) -> Value {
        json!({
            "schema_version": CONTEXT_ENGINE_BINDING_SCHEMA_VERSION,
            "context_lifecycle": {
                "binding_id_sha256": crate::sha256_hex(self.binding_id.as_bytes()),
                "engine_id": self.engine_id,
                "engine_version": self.engine_version,
                "projection_epoch": self.projection_epoch,
                "health": self.health.as_str(),
                "lifecycle_sequence": self.state.lifecycle_sequence,
                "ingested_message_count": self.state.ingested_message_count,
                "ingested_tool_exchange_count": self.state.ingested_tool_exchange_count,
                "completed_turn_count": self.state.completed_turn_count,
                "observed_prompt_tokens": self.state.observed_prompt_tokens,
                "observed_completion_tokens": self.state.observed_completion_tokens,
                "estimate_calibration_basis_points":
                    self.state.estimate_calibration_basis_points,
                "state_sha256": self.state.state_sha256,
                "persisted_redacted": self.state.persisted_redacted,
                "lifecycle_point": point.as_str(),
                "reason_code": reason_code,
            }
        })
    }
}

/// Replay-visible lifecycle transition containing no prompt or tool payload.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct ContextLifecycleEvent {
    pub(crate) schema_version: u32,
    pub(crate) point: ContextLifecyclePoint,
    pub(crate) reason_code: String,
    pub(crate) binding: ContextEngineBindingV2,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) input_sha256: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) input_utf8_bytes: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) finish_reason: Option<String>,
}

impl ContextLifecycleEvent {
    fn validate(&self) -> Result<(), &'static str> {
        if self.schema_version != CONTEXT_LIFECYCLE_EVENT_SCHEMA_VERSION {
            return Err("context.lifecycle.event_schema_unsupported");
        }
        if self.reason_code.trim().is_empty() {
            return Err("context.lifecycle.reason_code_missing");
        }
        if self.input_sha256.as_ref().is_some_and(|value| value.len() != 64) {
            return Err("context.lifecycle.input_digest_invalid");
        }
        self.binding.validate()
    }
}

/// Returns validated metadata for the latest lifecycle projection of one
/// session. The payload is safe for the read-only context inspector because
/// lifecycle events never contain prompt or tool content.
///
/// # Errors
/// Returns a journal error, parse error, or closed-schema validation failure.
#[allow(clippy::result_large_err)]
pub(crate) async fn session_context_lifecycle_diagnostics(
    runtime_state: &Arc<GatewayRuntimeState>,
    session_id: &str,
) -> Result<Value, Status> {
    let latest = runtime_state
        .latest_orchestrator_session_transcript_event(
            session_id.to_owned(),
            CONTEXT_LIFECYCLE_EVENT.to_owned(),
        )
        .await?;
    let Some(latest) = latest else {
        return Ok(json!({
            "available": false,
            "reason_code": "context.lifecycle.session_state_not_recorded",
        }));
    };
    let event = serde_json::from_str::<ContextLifecycleEvent>(latest.payload_json.as_str())
        .map_err(|error| {
            Status::failed_precondition(format!(
                "context lifecycle state could not be decoded: {error}"
            ))
        })?;
    event.validate().map_err(|reason| Status::failed_precondition(reason.to_owned()))?;
    Ok(json!({
        "available": true,
        "event": event.binding.diagnostics_json(event.point, event.reason_code.as_str()),
    }))
}

/// Restores one session binding, or selects the safe built-in engine under a
/// new projection epoch when persisted state or the selected engine is invalid.
fn restore_binding(
    latest: Option<&OrchestratorSessionTranscriptRecord>,
    descriptor: &ContextEngineDescriptor,
) -> (ContextEngineBindingV2, Option<&'static str>) {
    let Some(latest) = latest else {
        return (ContextEngineBindingV2::new(descriptor), None);
    };
    let parsed = serde_json::from_str::<ContextLifecycleEvent>(latest.payload_json.as_str());
    let Ok(event) = parsed else {
        return (
            ContextEngineBindingV2::safe_builtin(1, ContextEngineHealth::Degraded),
            Some("context.lifecycle.malformed_state_fallback"),
        );
    };
    if event.validate().is_err() {
        return (
            ContextEngineBindingV2::safe_builtin(
                event.binding.projection_epoch.saturating_add(1),
                ContextEngineHealth::Degraded,
            ),
            Some("context.lifecycle.invalid_state_fallback"),
        );
    }
    if event.binding.engine_id != descriptor.engine_id
        || event.binding.engine_version != descriptor.version
    {
        return (
            ContextEngineBindingV2::safe_builtin(
                event.binding.projection_epoch.saturating_add(1),
                ContextEngineHealth::Quarantined,
            ),
            Some("context.lifecycle.engine_unavailable_fallback"),
        );
    }
    (event.binding, None)
}

async fn append_event(
    runtime_state: &Arc<GatewayRuntimeState>,
    run_id: &str,
    tape_seq: &mut i64,
    event: &ContextLifecycleEvent,
) -> Result<(), Status> {
    event.validate().map_err(|reason| Status::failed_precondition(reason.to_owned()))?;
    runtime_state
        .append_orchestrator_tape_event(OrchestratorTapeAppendRequest {
            run_id: run_id.to_owned(),
            seq: *tape_seq,
            event_type: CONTEXT_LIFECYCLE_EVENT.to_owned(),
            payload_json: serde_json::to_string(event).map_err(|error| {
                Status::internal(format!("failed to serialize context lifecycle event: {error}"))
            })?,
        })
        .await?;
    runtime_state.record_context_assembly_trace(
        event.binding.diagnostics_json(event.point, event.reason_code.as_str()),
    );
    *tape_seq = tape_seq.saturating_add(1);
    Ok(())
}

fn advance_event(
    mut binding: ContextEngineBindingV2,
    point: ContextLifecyclePoint,
    reason_code: &str,
) -> ContextLifecycleEvent {
    binding.state.lifecycle_sequence = binding.state.lifecycle_sequence.saturating_add(1);
    binding.state.refresh_hash();
    ContextLifecycleEvent {
        schema_version: CONTEXT_LIFECYCLE_EVENT_SCHEMA_VERSION,
        point,
        reason_code: reason_code.to_owned(),
        binding,
        input_sha256: None,
        input_utf8_bytes: None,
        finish_reason: None,
    }
}

/// Restores or bootstraps the binding, then ingests the current user message.
///
/// # Errors
/// Returns a mapped journal error or a validation error when a generated
/// lifecycle projection violates its closed schema.
#[allow(clippy::result_large_err)]
pub(crate) async fn bootstrap_and_ingest_message(
    runtime_state: &Arc<GatewayRuntimeState>,
    run_id: &str,
    session_id: &str,
    tape_seq: &mut i64,
    descriptor: &ContextEngineDescriptor,
    input_text: &str,
) -> Result<ContextEngineBindingV2, Status> {
    let latest = runtime_state
        .latest_orchestrator_session_transcript_event(
            session_id.to_owned(),
            CONTEXT_LIFECYCLE_EVENT.to_owned(),
        )
        .await?;
    let (mut binding, fallback_reason) = restore_binding(latest.as_ref(), descriptor);
    if let Some(reason_code) = fallback_reason {
        let degraded = advance_event(binding, ContextLifecyclePoint::EngineDegraded, reason_code);
        append_event(runtime_state, run_id, tape_seq, &degraded).await?;
        binding = degraded.binding;
    }
    let engine = ContextEngineRegistry::production_default().selected_engine();
    let bootstrap_outcome = engine.bootstrap_session(ContextEngineBootstrapInput {
        binding_id: binding.binding_id.as_str(),
        projection_epoch: binding.projection_epoch,
        restored: latest.is_some() && fallback_reason.is_none(),
    });
    if !bootstrap_outcome.supported {
        return Err(Status::failed_precondition(bootstrap_outcome.reason_code));
    }
    let bootstrap = advance_event(
        binding,
        ContextLifecyclePoint::BootstrapSession,
        bootstrap_outcome.reason_code.as_str(),
    );
    append_event(runtime_state, run_id, tape_seq, &bootstrap).await?;
    binding = bootstrap.binding;

    let input_sha256 = crate::sha256_hex(input_text.as_bytes());
    let input_utf8_bytes = u64::try_from(input_text.len()).unwrap_or(u64::MAX);
    let ingest_outcome = engine.ingest_events(&[ContextEngineIngestEvent::Message {
        input_sha256: input_sha256.as_str(),
        input_utf8_bytes,
    }]);
    if !ingest_outcome.supported {
        return Err(Status::failed_precondition(ingest_outcome.reason_code));
    }
    binding.state.ingested_message_count = binding.state.ingested_message_count.saturating_add(1);
    let mut ingested = advance_event(
        binding,
        ContextLifecyclePoint::IngestMessage,
        ingest_outcome.reason_code.as_str(),
    );
    ingested.input_sha256 = Some(input_sha256);
    ingested.input_utf8_bytes = Some(input_utf8_bytes);
    append_event(runtime_state, run_id, tape_seq, &ingested).await?;

    let before_prompt = advance_event(
        ingested.binding,
        ContextLifecyclePoint::BeforePrompt,
        "context.lifecycle.before_prompt",
    );
    append_event(runtime_state, run_id, tape_seq, &before_prompt).await?;
    Ok(before_prompt.binding)
}

/// Records normalized provider usage and optional tool exchanges after a turn.
///
/// # Errors
/// Returns a mapped journal error, or a validation error when the persisted
/// binding cannot be recovered safely.
#[allow(clippy::too_many_arguments, clippy::result_large_err)]
pub(crate) async fn record_after_turn(
    runtime_state: &Arc<GatewayRuntimeState>,
    run_id: &str,
    session_id: &str,
    tape_seq: &mut i64,
    prompt_tokens: u64,
    completion_tokens: u64,
    tool_exchange_count: u64,
    finish_reason: Option<ProviderFinishReason>,
) -> Result<ContextEngineBindingV2, Status> {
    let descriptor = ContextEngineRegistry::production_default().selected_engine().descriptor();
    let latest = runtime_state
        .latest_orchestrator_session_transcript_event(
            session_id.to_owned(),
            CONTEXT_LIFECYCLE_EVENT.to_owned(),
        )
        .await?;
    let (mut binding, fallback_reason) = restore_binding(latest.as_ref(), &descriptor);
    if let Some(reason_code) = fallback_reason {
        let degraded = advance_event(binding, ContextLifecyclePoint::EngineDegraded, reason_code);
        append_event(runtime_state, run_id, tape_seq, &degraded).await?;
        binding = degraded.binding;
    }

    if tool_exchange_count > 0 {
        let engine = ContextEngineRegistry::production_default().selected_engine();
        let ingest_outcome = engine.ingest_events(&[ContextEngineIngestEvent::ToolExchange {
            exchange_count: tool_exchange_count,
        }]);
        if !ingest_outcome.supported {
            return Err(Status::failed_precondition(ingest_outcome.reason_code));
        }
        binding.state.ingested_tool_exchange_count =
            binding.state.ingested_tool_exchange_count.saturating_add(tool_exchange_count);
        let ingested = advance_event(
            binding,
            ContextLifecyclePoint::IngestToolExchange,
            ingest_outcome.reason_code.as_str(),
        );
        append_event(runtime_state, run_id, tape_seq, &ingested).await?;
        binding = ingested.binding;
    }

    let engine = ContextEngineRegistry::production_default().selected_engine();
    let engine_outcome = engine.after_turn(ContextEngineAfterTurnInput {
        run_id,
        session_id,
        prompt_tokens,
        completion_tokens,
        tool_exchange_count,
    });
    binding.state.completed_turn_count = binding.state.completed_turn_count.saturating_add(1);
    binding.state.observed_prompt_tokens =
        binding.state.observed_prompt_tokens.saturating_add(prompt_tokens);
    binding.state.observed_completion_tokens =
        binding.state.observed_completion_tokens.saturating_add(completion_tokens);
    let observed_total = prompt_tokens.saturating_add(completion_tokens);
    if observed_total > 0 {
        let prior_weight = binding.state.completed_turn_count.saturating_sub(1).min(31);
        let observed_basis_points = prompt_tokens
            .saturating_mul(CALIBRATION_SCALE_BASIS_POINTS)
            .checked_div(observed_total)
            .unwrap_or(CALIBRATION_SCALE_BASIS_POINTS)
            .min(MAX_CALIBRATION_BASIS_POINTS);
        binding.state.estimate_calibration_basis_points = binding
            .state
            .estimate_calibration_basis_points
            .saturating_mul(prior_weight)
            .saturating_add(observed_basis_points)
            .checked_div(prior_weight.saturating_add(1))
            .unwrap_or(observed_basis_points);
    }
    let mut after_turn = advance_event(
        binding,
        ContextLifecyclePoint::AfterTurn,
        engine_outcome.reason_code.as_str(),
    );
    after_turn.finish_reason = finish_reason.map(|reason| finish_reason_label(reason).to_owned());
    append_event(runtime_state, run_id, tape_seq, &after_turn).await?;
    Ok(after_turn.binding)
}

#[cfg(test)]
mod tests {
    use super::{
        advance_event, restore_binding, ContextEngineBindingV2, ContextEngineHealth,
        ContextLifecycleEvent, ContextLifecyclePoint, CONTEXT_LIFECYCLE_EVENT_SCHEMA_VERSION,
    };
    use crate::{
        application::context_engine::ContextEngineRegistry,
        journal::OrchestratorSessionTranscriptRecord,
    };

    fn transcript(payload_json: String) -> OrchestratorSessionTranscriptRecord {
        OrchestratorSessionTranscriptRecord {
            session_id: "session-1".to_owned(),
            run_id: "run-1".to_owned(),
            seq: 1,
            event_type: super::CONTEXT_LIFECYCLE_EVENT.to_owned(),
            payload_json,
            created_at_unix_ms: 1,
            origin_kind: "manual".to_owned(),
            origin_run_id: None,
        }
    }

    #[test]
    fn new_session_bootstrap_is_schema_valid_and_redacted() {
        let descriptor = ContextEngineRegistry::production_default().selected_engine().descriptor();
        let (binding, fallback) = restore_binding(None, &descriptor);
        let event = advance_event(
            binding,
            ContextLifecyclePoint::BootstrapSession,
            "context.lifecycle.session_bootstrapped",
        );

        assert!(fallback.is_none());
        assert_eq!(event.schema_version, CONTEXT_LIFECYCLE_EVENT_SCHEMA_VERSION);
        assert_eq!(event.binding.projection_epoch, 1);
        assert_eq!(event.binding.health, ContextEngineHealth::Healthy);
        assert!(event.binding.state.persisted_redacted);
        assert!(event.validate().is_ok());
        let serialized = serde_json::to_string(&event).expect("event serializes");
        assert!(!serialized.contains("prompt_text"));
        assert!(!serialized.contains("tool_output"));
    }

    #[test]
    fn restart_restores_binding_and_calibration() {
        let descriptor = ContextEngineRegistry::production_default().selected_engine().descriptor();
        let (mut binding, _) = restore_binding(None, &descriptor);
        binding.state.completed_turn_count = 7;
        binding.state.observed_prompt_tokens = 1_200;
        binding.state.estimate_calibration_basis_points = 7_500;
        let event = advance_event(
            binding.clone(),
            ContextLifecyclePoint::AfterTurn,
            "context.lifecycle.default_after_turn_noop",
        );
        let record = transcript(serde_json::to_string(&event).expect("event serializes"));

        let (restored, fallback) = restore_binding(Some(&record), &descriptor);

        assert!(fallback.is_none());
        assert_eq!(restored.binding_id, binding.binding_id);
        assert_eq!(restored.state.completed_turn_count, 7);
        assert_eq!(restored.state.observed_prompt_tokens, 1_200);
        assert_eq!(restored.state.estimate_calibration_basis_points, 7_500);
    }

    #[test]
    fn malformed_state_falls_back_without_payload_reuse() {
        let descriptor = ContextEngineRegistry::production_default().selected_engine().descriptor();
        let record = transcript("{\"raw_tool_output\":\"secret\"}".to_owned());

        let (binding, fallback) = restore_binding(Some(&record), &descriptor);

        assert_eq!(fallback, Some("context.lifecycle.malformed_state_fallback"));
        assert_eq!(binding.engine_id, "default_context_engine");
        assert_eq!(binding.health, ContextEngineHealth::Degraded);
        assert_eq!(binding.state.ingested_tool_exchange_count, 0);
    }

    #[test]
    fn engine_switch_quarantines_old_binding_and_advances_epoch() {
        let descriptor = ContextEngineRegistry::production_default().selected_engine().descriptor();
        let mut old = ContextEngineBindingV2::new(&descriptor);
        old.engine_id = "missing_custom_engine".to_owned();
        old.engine_version = "custom.v7".to_owned();
        old.projection_epoch = 4;
        let event = ContextLifecycleEvent {
            schema_version: CONTEXT_LIFECYCLE_EVENT_SCHEMA_VERSION,
            point: ContextLifecyclePoint::AfterTurn,
            reason_code: "custom.completed".to_owned(),
            binding: old,
            input_sha256: None,
            input_utf8_bytes: None,
            finish_reason: None,
        };
        let record = transcript(serde_json::to_string(&event).expect("event serializes"));

        let (binding, fallback) = restore_binding(Some(&record), &descriptor);

        assert_eq!(fallback, Some("context.lifecycle.engine_unavailable_fallback"));
        assert_eq!(binding.projection_epoch, 5);
        assert_eq!(binding.health, ContextEngineHealth::Quarantined);
        assert_eq!(binding.engine_id, "default_context_engine");
    }

    #[test]
    fn tampered_state_digest_falls_back_to_new_projection_epoch() {
        let descriptor = ContextEngineRegistry::production_default().selected_engine().descriptor();
        let (binding, _) = restore_binding(None, &descriptor);
        let mut event = advance_event(
            binding,
            ContextLifecyclePoint::AfterTurn,
            "context.lifecycle.default_after_turn_noop",
        );
        event.binding.projection_epoch = 8;
        event.binding.state.state_sha256 = "0".repeat(64);
        let record = transcript(serde_json::to_string(&event).expect("event serializes"));

        let (binding, fallback) = restore_binding(Some(&record), &descriptor);

        assert_eq!(fallback, Some("context.lifecycle.invalid_state_fallback"));
        assert_eq!(binding.projection_epoch, 9);
        assert_eq!(binding.health, ContextEngineHealth::Degraded);
    }
}
