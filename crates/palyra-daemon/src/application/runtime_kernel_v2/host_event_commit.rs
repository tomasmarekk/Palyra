//! Canonical host-event construction and atomic journal commit.

use std::collections::BTreeMap;

use palyra_common::runtime_contracts::{
    RuntimeEventEnvelopeV2, RuntimeEventName, RuntimeEventPayloadRef, RuntimeIdentitySetV1,
    RUNTIME_EVENT_ENVELOPE_SCHEMA_VERSION,
};
use serde_json::{json, Value};

use super::{
    super::rollback::VerifiedRuntimeRollbackSafeBoundary, HarnessContractError,
    HostHarnessEventSink, KernelTransition, RuntimeKernelObservationCommitRequest,
};

impl HostHarnessEventSink {
    pub(super) fn begin_finalization(
        &mut self,
        harness_sequence: u64,
        event_name: RuntimeEventName,
        reason_code: &str,
    ) -> Result<(), HarnessContractError> {
        let identities =
            self.identities_for_lane(event_name.descriptor().generation_lane, |identities| {
                identities.attempt_id = Some(self.request.attempt_id().clone());
            })?;
        self.apply_transition(
            event_name,
            KernelTransition::BeginFinalization,
            identities,
            reason_code,
            json!({}),
            harness_sequence,
        )
    }

    pub(super) fn apply_transition(
        &mut self,
        event_name: RuntimeEventName,
        transition: KernelTransition,
        identities: RuntimeIdentitySetV1,
        reason_code: &str,
        metadata: Value,
        harness_sequence: u64,
    ) -> Result<(), HarnessContractError> {
        let descriptor = event_name.descriptor();
        let stamp =
            self.event_authority.issue(descriptor.generation_lane, identities.generation)?;
        let event = RuntimeEventEnvelopeV2 {
            schema_version: RUNTIME_EVENT_ENVELOPE_SCHEMA_VERSION,
            event_id: stamp.event_id,
            identities,
            sequence: 0,
            causal_parent_event_id: None,
            subsystem: descriptor.subsystem,
            phase: descriptor.phase,
            event_name,
            reason_code: reason_code.to_owned(),
            actor_kind: descriptor.actor_kind,
            retryability: descriptor.retryability,
            redaction_class: descriptor.redaction_class,
            terminal: descriptor.terminal,
            payload: RuntimeEventPayloadRef::Inline { metadata },
            occurred_at_unix_ms: stamp.occurred_at_unix_ms,
            extensions: BTreeMap::new(),
        };
        event.validate().map_err(|_| HarnessContractError::HostEventMetadata)?;
        let idempotency_key = format!(
            "harness/{}/{harness_sequence}/{}",
            self.request.attempt_id().as_str(),
            event_name.as_str()
        );
        let request = RuntimeKernelObservationCommitRequest {
            expected_snapshot: self.kernel.snapshot().clone(),
            expected_run_generation: self.request.generation(),
            lane_authority: self.lane_authority.clone(),
            idempotency_key,
            event_template: event,
            transition,
        };
        self.kernel = self
            .journal
            .commit_observation_and_restore(&request)
            .map_err(HarnessContractError::Journal)?;
        if let Some(boundary) = VerifiedRuntimeRollbackSafeBoundary::after_host_event(
            self.kernel.snapshot(),
            event_name,
        ) {
            if let Some(suspended) = self
                .journal
                .apply_pending_rollback_and_restore(&boundary)
                .map_err(HarnessContractError::Journal)?
            {
                self.kernel = suspended;
                return Err(HarnessContractError::RollbackSuspended);
            }
        }
        Ok(())
    }
}
