//! Host-owned single-finalization and durable delivery services for RuntimeKernelV2.
//!
//! The kernel sees opaque projection/evidence references only. Raw outbound content
//! remains behind the projection source and reaches providers solely via the existing outbox.

use std::{
    collections::BTreeMap,
    fmt,
    sync::{Arc, Mutex},
};

use palyra_connectors::OutboundMessageRequest;
use sha2::{Digest, Sha256};
use thiserror::Error;
use ulid::Ulid;

use crate::{
    application::tool_registry::canonical_json_bytes,
    journal::{
        runtime_finalization::{
            runtime_finalization_now, DeliveryArbitrationActionV2, DeliveryArbitrationDecisionV2,
            FinalOutputArtifactDescriptor, FinalizationEvidenceRef, PendingFinalRecoveryOutcome,
            PendingFinalRecoveryState, RuntimeDeliveryIntentDescriptor,
            RuntimeDeliveryLinkObservation, RuntimeDeliverySnapshot, RuntimeDeliveryState,
        },
        JournalError, JournalStore,
    },
};

use super::phases::{
    DeliveryDisposition, DeliveryPhase, DeliveryPhaseInput, DeliveryPhaseOutput, DeliveryRequest,
    DeliveryResult, FinalProjectionRef, FinalizationPhase, FinalizationReceipt,
    FinalizationRequest, KernelPhaseError, KernelPhaseFuture, KernelPhaseInput, KernelPhaseOutput,
    KernelPhaseReason, RedactedEvidenceRef, RuntimePhaseService,
};

/// Metadata and optional delivery request retained behind an opaque final projection.
#[derive(Clone)]
pub(crate) struct RetainedFinalProjection {
    /// Hash of the final content retained by the host.
    pub(crate) content_sha256: String,
    /// Whether this artifact may be delivered to a user-facing destination.
    pub(crate) user_visible: bool,
    /// Hash-only verification evidence.
    pub(crate) verification_evidence: Vec<FinalizationEvidenceRef>,
    /// Expected but unavailable output artifacts.
    pub(crate) missing_artifacts: Vec<FinalizationEvidenceRef>,
    /// Active process and cleanup state at finalization.
    pub(crate) active_process_state: Vec<FinalizationEvidenceRef>,
    /// Existing connector-outbox request retained outside the kernel.
    pub(crate) delivery: Option<RetainedFinalDelivery>,
}

impl fmt::Debug for RetainedFinalProjection {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RetainedFinalProjection")
            .field("content_sha256", &self.content_sha256)
            .field("user_visible", &self.user_visible)
            .field("verification_evidence", &self.verification_evidence)
            .field("missing_artifacts", &self.missing_artifacts)
            .field("active_process_state", &self.active_process_state)
            .field("delivery_present", &self.delivery.is_some())
            .finish()
    }
}

/// Raw delivery request retained by the host, never exposed through phase payloads.
#[derive(Clone)]
pub(crate) struct RetainedFinalDelivery {
    /// Domain-separated destination binding.
    pub(crate) destination_binding_sha256: String,
    /// Request accepted by the existing connector outbox.
    pub(crate) request: OutboundMessageRequest,
}

impl fmt::Debug for RetainedFinalDelivery {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RetainedFinalDelivery")
            .field("destination_binding_sha256", &self.destination_binding_sha256)
            .field("connector_id", &self.request.connector_id)
            .field("envelope_id", &self.request.envelope_id)
            .finish_non_exhaustive()
    }
}

/// Durable host repository resolving an opaque final projection.
pub(crate) trait FinalProjectionSource: Send + Sync {
    /// Loads final metadata and any retained outbox request.
    ///
    /// # Errors
    /// Returns [`FinalizationHostError`] when the projection is absent, corrupt,
    /// or unavailable from its durable host store.
    fn resolve(
        &self,
        projection: &FinalProjectionRef,
    ) -> Result<RetainedFinalProjection, FinalizationHostError>;
}

/// Run-owned projection source for output already persisted in the transcript.
#[derive(Default)]
pub(crate) struct RunFinalProjectionStore {
    projections: Mutex<BTreeMap<String, RetainedFinalProjection>>,
}

impl RunFinalProjectionStore {
    /// Retains one user-visible projection with its complete host-owned delivery request.
    pub(crate) fn retain_visible(
        &self,
        content: &[u8],
        delivery: RetainedFinalDelivery,
        verification_evidence: Vec<FinalizationEvidenceRef>,
        missing_artifacts: Vec<FinalizationEvidenceRef>,
        active_process_state: Vec<FinalizationEvidenceRef>,
    ) -> Result<FinalProjectionRef, FinalizationHostError> {
        let id = palyra_common::runtime_contracts::RuntimeOperationId::parse(
            Ulid::new().to_string().as_str(),
        )
        .map_err(|_| FinalizationHostError::InvalidDeliveryMetadata)?;
        let sha256 = digest_array(content);
        self.projections.lock().map_err(|_| FinalizationHostError::ProjectionNotFound)?.insert(
            id.as_str().to_owned(),
            RetainedFinalProjection {
                content_sha256: hex::encode(sha256),
                user_visible: true,
                verification_evidence,
                missing_artifacts,
                active_process_state,
                delivery: Some(delivery),
            },
        );
        Ok(FinalProjectionRef::from_host(id, sha256))
    }

    /// Retains one hidden projection and returns its content-bound reference.
    pub(crate) fn retain_hidden(
        &self,
        content: &[u8],
        verification_evidence: Vec<FinalizationEvidenceRef>,
        missing_artifacts: Vec<FinalizationEvidenceRef>,
        active_process_state: Vec<FinalizationEvidenceRef>,
    ) -> Result<FinalProjectionRef, FinalizationHostError> {
        let id = palyra_common::runtime_contracts::RuntimeOperationId::parse(
            Ulid::new().to_string().as_str(),
        )
        .map_err(|_| FinalizationHostError::InvalidDeliveryMetadata)?;
        let sha256 = digest_array(content);
        self.projections.lock().map_err(|_| FinalizationHostError::ProjectionNotFound)?.insert(
            id.as_str().to_owned(),
            RetainedFinalProjection {
                content_sha256: hex::encode(sha256),
                user_visible: false,
                verification_evidence,
                missing_artifacts,
                active_process_state,
                delivery: None,
            },
        );
        Ok(FinalProjectionRef::from_host(id, sha256))
    }

    /// Issues hash-only evidence for a host-owned no-delivery decision.
    pub(crate) fn retain_delivery_skip_evidence(
        &self,
        reason_code: &str,
    ) -> Result<RedactedEvidenceRef, FinalizationHostError> {
        let id = palyra_common::runtime_contracts::RuntimeOperationId::parse(
            Ulid::new().to_string().as_str(),
        )
        .map_err(|_| FinalizationHostError::InvalidDeliveryMetadata)?;
        Ok(RedactedEvidenceRef::from_host(id, digest_array(reason_code.as_bytes())))
    }
}

impl FinalProjectionSource for RunFinalProjectionStore {
    fn resolve(
        &self,
        projection: &FinalProjectionRef,
    ) -> Result<RetainedFinalProjection, FinalizationHostError> {
        self.projections
            .lock()
            .map_err(|_| FinalizationHostError::ProjectionNotFound)?
            .get(projection.id().as_str())
            .cloned()
            .ok_or(FinalizationHostError::ProjectionNotFound)
    }
}

/// Connector outbox status visible to the delivery boundary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum DeliveryOutboxState {
    /// No deterministic outbox row exists yet.
    Missing,
    /// The row exists and has not reached a terminal provider outcome.
    Queued,
    /// The existing outbox parked the effect as outcome-unknown.
    OutcomeUnknown,
    /// The existing outbox parked the request for explicit operator retry.
    DeadLetter,
    /// The provider acknowledged delivery.
    Delivered {
        /// Provider-native message identity, hashed before journaling.
        native_message_id: String,
    },
}

/// Host port to the existing connector outbox.
pub(crate) trait DeliveryOutboxPort: Send + Sync {
    /// Reads the current state of one deterministic outbox envelope.
    ///
    /// # Errors
    /// Returns [`FinalizationHostError`] when connector storage is unavailable.
    fn inspect(
        &self,
        connector_id: &str,
        envelope_id: &str,
    ) -> Result<DeliveryOutboxState, FinalizationHostError>;

    /// Idempotently enqueues through the existing connector outbox.
    ///
    /// This method must not call a provider adapter or drain the queue.
    ///
    /// # Errors
    /// Returns [`FinalizationHostError`] when validation or durable enqueue fails.
    fn enqueue(&self, request: &OutboundMessageRequest) -> Result<(), FinalizationHostError>;
}

/// Host failure at the finalization/delivery boundary.
#[derive(Debug, Error)]
pub(crate) enum FinalizationHostError {
    /// Opaque projection is missing.
    #[error("final projection was not found")]
    ProjectionNotFound,
    /// Opaque projection digest does not match retained content.
    #[error("final projection content digest does not match")]
    ProjectionDigestMismatch,
    /// A user-visible artifact lacks a retained delivery request.
    #[error("user-visible final projection has no delivery request")]
    DeliveryRequestMissing,
    /// Delivery was requested for a hidden artifact.
    #[error("hidden final projection cannot be delivered")]
    HiddenOutput,
    /// Retained delivery metadata is invalid.
    #[error("retained delivery metadata is invalid")]
    InvalidDeliveryMetadata,
    /// Daemon journal operation failed.
    #[error("runtime finalization journal operation failed")]
    Journal(#[source] JournalError),
    /// Existing connector outbox operation failed.
    #[error("connector outbox operation failed")]
    Outbox(#[source] crate::channels::ChannelPlatformError),
}

impl From<JournalError> for FinalizationHostError {
    fn from(source: JournalError) -> Self {
        Self::Journal(source)
    }
}

/// Journal-backed implementation of the canonical finalization phase.
pub(crate) struct JournalFinalizationService {
    journal: FinalizationJournal,
    projections: Arc<dyn FinalProjectionSource>,
}

enum FinalizationJournal {
    #[cfg(test)]
    Owned(Arc<JournalStore>),
    Gateway(Arc<crate::gateway::GatewayRuntimeState>),
}

impl FinalizationJournal {
    fn store(&self) -> &JournalStore {
        match self {
            #[cfg(test)]
            Self::Owned(store) => store,
            Self::Gateway(runtime_state) => &runtime_state.journal_store,
        }
    }
}

impl fmt::Debug for JournalFinalizationService {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_struct("JournalFinalizationService").finish_non_exhaustive()
    }
}

impl JournalFinalizationService {
    /// Creates the service over the gateway-owned journal without duplicating it.
    #[must_use]
    pub(crate) fn from_runtime_state(
        runtime_state: Arc<crate::gateway::GatewayRuntimeState>,
        projections: Arc<dyn FinalProjectionSource>,
    ) -> Self {
        Self { journal: FinalizationJournal::Gateway(runtime_state), projections }
    }

    fn execute_sync(
        &self,
        input: KernelPhaseInput<FinalizationPhase, FinalizationRequest>,
    ) -> Result<KernelPhaseOutput<FinalizationPhase, FinalizationReceipt>, KernelPhaseError> {
        let mut retained = self
            .projections
            .resolve(&input.payload().final_projection)
            .map_err(finalization_phase_error)?;
        let projection_sha256 = hex::encode(input.payload().final_projection.sha256());
        if retained.content_sha256 != projection_sha256 {
            return Err(finalization_phase_error(FinalizationHostError::ProjectionDigestMismatch));
        }
        if retained.user_visible && retained.delivery.is_none() {
            return Err(finalization_phase_error(FinalizationHostError::DeliveryRequestMissing));
        }

        let boundary = input.boundary();
        let authority = boundary.execution().lane_authority();
        let decision = match retained.delivery.as_mut() {
            Some(delivery) => {
                let delivery_intent_id = input.payload().final_projection.id().as_str().to_owned();
                delivery.request.envelope_id = deterministic_final_envelope_id(
                    boundary.identities().run_id.as_str(),
                    boundary.generation(),
                    authority.run_lease_id().as_str(),
                    delivery_intent_id.as_str(),
                    input.payload().final_projection.id().as_str(),
                    retained.content_sha256.as_str(),
                    delivery.destination_binding_sha256.as_str(),
                );
                if !valid_sha256(&delivery.destination_binding_sha256)
                    || delivery.destination_binding_sha256
                        != final_delivery_destination_binding(&delivery.request)
                    || !final_delivery_content_matches(
                        retained.content_sha256.as_str(),
                        delivery.request.text.as_str(),
                    )
                {
                    return Err(finalization_phase_error(
                        FinalizationHostError::InvalidDeliveryMetadata,
                    ));
                }
                let request_sha256 = final_delivery_request_sha256(&delivery.request)
                    .map_err(finalization_phase_error)?;
                DeliveryArbitrationDecisionV2 {
                    artifact_id: input.payload().final_projection.id().as_str().to_owned(),
                    session_id: boundary.identities().session_id.as_str().to_owned(),
                    run_id: boundary.identities().run_id.as_str().to_owned(),
                    run_generation: boundary.generation(),
                    parent_run_id: None,
                    descendant_run_ids: Vec::new(),
                    action: DeliveryArbitrationActionV2::Deliver,
                    destination_binding_sha256: Some(delivery.destination_binding_sha256.clone()),
                    delivery_intent_id: Some(delivery_intent_id),
                    connector_id: Some(delivery.request.connector_id.clone()),
                    outbox_envelope_id: Some(delivery.request.envelope_id.clone()),
                    content_sha256: retained.content_sha256.clone(),
                    outbound_request_sha256: Some(request_sha256),
                    dedupe_key: Some(delivery.request.delivery_idempotency_key()),
                    outbound_request: Some(delivery.request.clone()),
                    reason_code: "runtime.delivery.arbitration_deliver".to_owned(),
                    decided_at_unix_ms: runtime_finalization_now().map_err(|error| {
                        finalization_phase_error(FinalizationHostError::Journal(error))
                    })?,
                }
            }
            None => DeliveryArbitrationDecisionV2 {
                artifact_id: input.payload().final_projection.id().as_str().to_owned(),
                session_id: boundary.identities().session_id.as_str().to_owned(),
                run_id: boundary.identities().run_id.as_str().to_owned(),
                run_generation: boundary.generation(),
                parent_run_id: None,
                descendant_run_ids: Vec::new(),
                action: DeliveryArbitrationActionV2::Suppress,
                destination_binding_sha256: None,
                delivery_intent_id: None,
                connector_id: None,
                outbox_envelope_id: None,
                content_sha256: retained.content_sha256.clone(),
                outbound_request_sha256: None,
                dedupe_key: None,
                outbound_request: None,
                reason_code: "runtime.delivery.arbitration_suppress".to_owned(),
                decided_at_unix_ms: runtime_finalization_now().map_err(|error| {
                    finalization_phase_error(FinalizationHostError::Journal(error))
                })?,
            },
        };
        let descriptor = FinalOutputArtifactDescriptor {
            artifact_id: input.payload().final_projection.id().as_str().to_owned(),
            session_id: boundary.identities().session_id.as_str().to_owned(),
            run_id: boundary.identities().run_id.as_str().to_owned(),
            run_generation: boundary.generation(),
            run_lease_id: authority.run_lease_id().as_str().to_owned(),
            terminal_outcome: input.payload().outcome,
            content_sha256: retained.content_sha256,
            projection_sha256,
            user_visible: retained.user_visible,
            verification_evidence: retained.verification_evidence,
            missing_artifacts: retained.missing_artifacts,
            active_process_state: retained.active_process_state,
            reason_code: input.payload().outcome.reason_code().to_owned(),
            committed_at_unix_ms: runtime_finalization_now()
                .map_err(|error| finalization_phase_error(FinalizationHostError::Journal(error)))?,
        };
        self.journal
            .store()
            .commit_runtime_final_output_with_arbitration(&descriptor, &decision)
            .map_err(|error| finalization_phase_error(FinalizationHostError::Journal(error)))?;
        let evidence = RedactedEvidenceRef::from_host(
            input.payload().final_projection.id().clone(),
            digest_array(descriptor.content_sha256.as_bytes()),
        );
        KernelPhaseOutput::from_input(
            &input,
            KernelPhaseReason::FinalizationCommitted,
            FinalizationReceipt { outcome: input.payload().outcome, terminal_evidence: evidence },
        )
        .map_err(KernelPhaseError::from)
    }
}

impl RuntimePhaseService<FinalizationPhase, FinalizationRequest, FinalizationReceipt>
    for JournalFinalizationService
{
    fn execute(
        &self,
        input: KernelPhaseInput<FinalizationPhase, FinalizationRequest>,
    ) -> KernelPhaseFuture<
        '_,
        Result<KernelPhaseOutput<FinalizationPhase, FinalizationReceipt>, KernelPhaseError>,
    > {
        Box::pin(std::future::ready(self.execute_sync(input)))
    }
}

/// Journal-and-outbox implementation of the canonical delivery phase.
pub(crate) struct JournalDeliveryService {
    journal: FinalizationJournal,
    projections: Arc<dyn FinalProjectionSource>,
    outbox: Arc<dyn DeliveryOutboxPort>,
}

impl fmt::Debug for JournalDeliveryService {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_struct("JournalDeliveryService").finish_non_exhaustive()
    }
}

impl JournalDeliveryService {
    /// Creates a service that commits intent before handing off to the existing outbox.
    #[cfg(test)]
    #[must_use]
    pub(crate) fn new(
        journal: Arc<JournalStore>,
        projections: Arc<dyn FinalProjectionSource>,
        outbox: Arc<dyn DeliveryOutboxPort>,
    ) -> Self {
        Self { journal: FinalizationJournal::Owned(journal), projections, outbox }
    }

    /// Creates the service over the gateway-owned journal without duplicating it.
    #[must_use]
    pub(crate) fn from_runtime_state(
        runtime_state: Arc<crate::gateway::GatewayRuntimeState>,
        projections: Arc<dyn FinalProjectionSource>,
        outbox: Arc<dyn DeliveryOutboxPort>,
    ) -> Self {
        Self { journal: FinalizationJournal::Gateway(runtime_state), projections, outbox }
    }

    fn execute_sync(
        &self,
        input: DeliveryPhaseInput,
    ) -> Result<DeliveryPhaseOutput, KernelPhaseError> {
        let retained = self
            .projections
            .resolve(&input.payload().final_projection)
            .map_err(delivery_phase_error)?;
        if !retained.user_visible {
            return Err(delivery_phase_error(FinalizationHostError::HiddenOutput));
        }
        let mut delivery = retained
            .delivery
            .ok_or(FinalizationHostError::DeliveryRequestMissing)
            .map_err(delivery_phase_error)?;
        let projection_sha256 = hex::encode(input.payload().final_projection.sha256());
        if retained.content_sha256 != projection_sha256 {
            return Err(delivery_phase_error(FinalizationHostError::ProjectionDigestMismatch));
        }
        if !valid_sha256(&delivery.destination_binding_sha256)
            || delivery.destination_binding_sha256
                != final_delivery_destination_binding(&delivery.request)
        {
            return Err(delivery_phase_error(FinalizationHostError::InvalidDeliveryMetadata));
        }

        let boundary = input.boundary();
        let authority = boundary.execution().lane_authority();
        delivery.request.envelope_id = deterministic_final_envelope_id(
            boundary.identities().run_id.as_str(),
            boundary.generation(),
            authority.run_lease_id().as_str(),
            input.payload().delivery_intent_id.as_str(),
            input.payload().final_projection.id().as_str(),
            retained.content_sha256.as_str(),
            delivery.destination_binding_sha256.as_str(),
        );
        if !final_delivery_content_matches(
            retained.content_sha256.as_str(),
            delivery.request.text.as_str(),
        ) {
            return Err(delivery_phase_error(FinalizationHostError::InvalidDeliveryMetadata));
        }
        let outbound_request_sha256 =
            final_delivery_request_sha256(&delivery.request).map_err(delivery_phase_error)?;
        let now = runtime_finalization_now()
            .map_err(|error| delivery_phase_error(FinalizationHostError::Journal(error)))?;
        let intent = RuntimeDeliveryIntentDescriptor {
            delivery_intent_id: input.payload().delivery_intent_id.as_str().to_owned(),
            artifact_id: input.payload().final_projection.id().as_str().to_owned(),
            session_id: boundary.identities().session_id.as_str().to_owned(),
            run_id: boundary.identities().run_id.as_str().to_owned(),
            run_generation: boundary.generation(),
            run_lease_id: authority.run_lease_id().as_str().to_owned(),
            delivery_generation: authority.lane_generation(),
            delivery_lease_id: authority.lane_lease_id().as_str().to_owned(),
            destination_binding_sha256: delivery.destination_binding_sha256,
            connector_id: delivery.request.connector_id.clone(),
            outbox_envelope_id: delivery.request.envelope_id.clone(),
            content_sha256: retained.content_sha256,
            outbound_request_sha256: outbound_request_sha256.clone(),
            dedupe_key: delivery.request.delivery_idempotency_key(),
            committed_at_unix_ms: now,
        };
        self.journal
            .store()
            .commit_runtime_delivery_intent(&intent)
            .map_err(|error| delivery_phase_error(FinalizationHostError::Journal(error)))?;
        let durable_intent = self
            .journal
            .store()
            .runtime_delivery_intent(intent.delivery_intent_id.as_str())
            .map_err(|error| delivery_phase_error(FinalizationHostError::Journal(error)))?
            .ok_or_else(|| delivery_phase_error(FinalizationHostError::InvalidDeliveryMetadata))?;
        if durable_intent.outbound_request_sha256 != outbound_request_sha256
            || durable_intent.connector_id != delivery.request.connector_id
            || durable_intent.outbox_envelope_id != delivery.request.envelope_id
        {
            return Err(delivery_phase_error(FinalizationHostError::InvalidDeliveryMetadata));
        }

        let durable_state = self
            .journal
            .store()
            .runtime_delivery_snapshot(intent.delivery_intent_id.as_str())
            .map_err(|error| delivery_phase_error(FinalizationHostError::Journal(error)))?
            .ok_or_else(|| delivery_phase_error(FinalizationHostError::InvalidDeliveryMetadata))?;
        let outbox_state = match reconcile_delivery_outbox(
            self.outbox.as_ref(),
            &durable_state,
            &delivery.request,
        )
        .map_err(delivery_phase_error)?
        {
            DeliveryReconciliation::Replay { state, evidence_sha256 } => {
                return delivery_phase_output(&input, state, evidence_sha256);
            }
            DeliveryReconciliation::Observe(outbox_state) => outbox_state,
        };
        let (state, reason_code, native_message_id_sha256) = match outbox_state {
            DeliveryOutboxState::Missing => {
                return Err(delivery_phase_error(FinalizationHostError::InvalidDeliveryMetadata));
            }
            DeliveryOutboxState::Queued => {
                (RuntimeDeliveryState::Queued, "runtime.delivery.outbox_queued", None)
            }
            DeliveryOutboxState::OutcomeUnknown => {
                (RuntimeDeliveryState::OutcomeUnknown, "runtime.delivery.outcome_unknown", None)
            }
            DeliveryOutboxState::DeadLetter => {
                (RuntimeDeliveryState::DeadLetter, "runtime.delivery.dead_letter", None)
            }
            DeliveryOutboxState::Delivered { native_message_id } => (
                RuntimeDeliveryState::Delivered,
                "runtime.delivery.acknowledged",
                Some(hex_sha256(native_message_id.as_bytes())),
            ),
        };
        let evidence_sha256 = delivery_evidence_sha256(&intent, state, reason_code);
        let state = self
            .journal
            .store()
            .record_runtime_delivery_link(&RuntimeDeliveryLinkObservation {
                delivery_intent_id: intent.delivery_intent_id.clone(),
                state,
                connector_id: intent.connector_id,
                outbox_envelope_id: intent.outbox_envelope_id,
                evidence_sha256: evidence_sha256.clone(),
                reason_code: reason_code.to_owned(),
                native_message_id_sha256,
                observed_at_unix_ms: now,
            })
            .map_err(|error| delivery_phase_error(FinalizationHostError::Journal(error)))?;
        delivery_phase_output(&input, state, evidence_sha256)
    }
}

impl RuntimePhaseService<DeliveryPhase, DeliveryRequest, DeliveryResult>
    for JournalDeliveryService
{
    fn execute(
        &self,
        input: DeliveryPhaseInput,
    ) -> KernelPhaseFuture<'_, Result<DeliveryPhaseOutput, KernelPhaseError>> {
        Box::pin(std::future::ready(self.execute_sync(input)))
    }
}

impl DeliveryOutboxPort for crate::channels::ChannelPlatform {
    fn inspect(
        &self,
        connector_id: &str,
        envelope_id: &str,
    ) -> Result<DeliveryOutboxState, FinalizationHostError> {
        self.runtime_final_delivery_state(connector_id, envelope_id)
            .map_err(FinalizationHostError::Outbox)
    }

    fn enqueue(&self, request: &OutboundMessageRequest) -> Result<(), FinalizationHostError> {
        self.enqueue_runtime_final_delivery(request)
            .map(|_| ())
            .map_err(FinalizationHostError::Outbox)
    }
}

/// Aggregate startup reconciliation result for durable final deliveries.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub(crate) struct PendingFinalRecoveryReport {
    pub(crate) scanned_count: usize,
    pub(crate) artifact_without_intent_count: usize,
    pub(crate) intent_pending_count: usize,
    pub(crate) outcome_unknown_count: usize,
    pub(crate) acknowledged_count: usize,
    pub(crate) dead_letter_count: usize,
    pub(crate) parent_wake_run_ids: Vec<String>,
}

/// Completes only the missing delivery portion of already-finalized outputs.
///
/// The exact request, envelope id, content hash, and dedupe key all come from
/// the durable arbitration record. Confirmed or uncertain effects are never
/// enqueued again.
///
/// # Errors
/// Returns a journal, integrity, or connector-outbox error.
pub(crate) fn recover_pending_final_deliveries(
    journal: &JournalStore,
    outbox: &dyn DeliveryOutboxPort,
) -> Result<PendingFinalRecoveryReport, FinalizationHostError> {
    let decisions = journal.pending_final_delivery_arbitrations()?;
    let mut report = PendingFinalRecoveryReport::default();
    for decision in decisions {
        report.scanned_count = report.scanned_count.saturating_add(1);
        let intent_id = decision
            .delivery_intent_id
            .clone()
            .ok_or(FinalizationHostError::InvalidDeliveryMetadata)?;
        let intent_missing = journal.runtime_delivery_intent(intent_id.as_str())?.is_none();
        if intent_missing {
            report.artifact_without_intent_count =
                report.artifact_without_intent_count.saturating_add(1);
            record_pending_outcome(
                journal,
                &decision,
                PendingFinalRecoveryState::ArtifactWithoutIntent,
                "runtime.delivery.recovery_intent_missing",
                decision.decision_sha256()?,
            )?;
        }
        let intent = journal.ensure_pending_final_delivery_intent(decision.artifact_id.as_str())?;
        let request = decision
            .outbound_request
            .as_ref()
            .ok_or(FinalizationHostError::InvalidDeliveryMetadata)?;
        if request.connector_id != intent.connector_id
            || request.envelope_id != intent.outbox_envelope_id
            || request.delivery_idempotency_key() != intent.dedupe_key
            || final_delivery_request_sha256(request)? != intent.outbound_request_sha256
            || !final_delivery_content_matches(
                intent.content_sha256.as_str(),
                request.text.as_str(),
            )
        {
            return Err(FinalizationHostError::InvalidDeliveryMetadata);
        }

        let durable = journal
            .runtime_delivery_snapshot(intent.delivery_intent_id.as_str())?
            .ok_or(FinalizationHostError::InvalidDeliveryMetadata)?;
        let outbox_state = match durable.state {
            RuntimeDeliveryState::Delivered => DeliveryOutboxState::Delivered {
                native_message_id: "durable-acknowledgement".to_owned(),
            },
            RuntimeDeliveryState::OutcomeUnknown => DeliveryOutboxState::OutcomeUnknown,
            RuntimeDeliveryState::DeadLetter
            | RuntimeDeliveryState::IntentRecorded
            | RuntimeDeliveryState::Queued => {
                match outbox
                    .inspect(intent.connector_id.as_str(), intent.outbox_envelope_id.as_str())?
                {
                    DeliveryOutboxState::Missing
                        if durable.state == RuntimeDeliveryState::DeadLetter =>
                    {
                        DeliveryOutboxState::DeadLetter
                    }
                    DeliveryOutboxState::Missing => {
                        outbox.enqueue(request)?;
                        DeliveryOutboxState::Queued
                    }
                    state => state,
                }
            }
        };
        let now = runtime_finalization_now()?;
        let (recovery_state, delivery_state, reason_code, native_message_id_sha256) =
            match outbox_state {
                DeliveryOutboxState::Missing => {
                    return Err(FinalizationHostError::InvalidDeliveryMetadata);
                }
                DeliveryOutboxState::Queued => {
                    report.intent_pending_count = report.intent_pending_count.saturating_add(1);
                    (
                        PendingFinalRecoveryState::IntentPending,
                        RuntimeDeliveryState::Queued,
                        "runtime.delivery.recovery_intent_pending",
                        None,
                    )
                }
                DeliveryOutboxState::OutcomeUnknown => {
                    report.outcome_unknown_count = report.outcome_unknown_count.saturating_add(1);
                    (
                        PendingFinalRecoveryState::OutcomeUnknown,
                        RuntimeDeliveryState::OutcomeUnknown,
                        "runtime.delivery.recovery_outcome_unknown",
                        None,
                    )
                }
                DeliveryOutboxState::DeadLetter => {
                    report.dead_letter_count = report.dead_letter_count.saturating_add(1);
                    (
                        PendingFinalRecoveryState::DeadLetter,
                        RuntimeDeliveryState::DeadLetter,
                        "runtime.delivery.recovery_dead_letter",
                        None,
                    )
                }
                DeliveryOutboxState::Delivered { native_message_id } => {
                    report.acknowledged_count = report.acknowledged_count.saturating_add(1);
                    (
                        PendingFinalRecoveryState::Acked,
                        RuntimeDeliveryState::Delivered,
                        "runtime.delivery.recovery_acknowledged",
                        Some(hex_sha256(native_message_id.as_bytes())),
                    )
                }
            };
        let evidence_sha256 = durable
            .evidence_sha256
            .unwrap_or_else(|| delivery_evidence_sha256(&intent, delivery_state, reason_code));
        journal.record_runtime_delivery_link(&RuntimeDeliveryLinkObservation {
            delivery_intent_id: intent.delivery_intent_id.clone(),
            state: delivery_state,
            connector_id: intent.connector_id.clone(),
            outbox_envelope_id: intent.outbox_envelope_id.clone(),
            evidence_sha256: evidence_sha256.clone(),
            reason_code: reason_code.to_owned(),
            native_message_id_sha256,
            observed_at_unix_ms: now,
        })?;
        record_pending_outcome(journal, &decision, recovery_state, reason_code, evidence_sha256)?;
        if let Some(parent_run_id) = decision.parent_run_id {
            report.parent_wake_run_ids.push(parent_run_id);
        }
    }
    report.parent_wake_run_ids.sort();
    report.parent_wake_run_ids.dedup();
    Ok(report)
}

fn record_pending_outcome(
    journal: &JournalStore,
    decision: &DeliveryArbitrationDecisionV2,
    state: PendingFinalRecoveryState,
    reason_code: &str,
    evidence_sha256: String,
) -> Result<(), FinalizationHostError> {
    journal.record_pending_final_recovery_outcome(&PendingFinalRecoveryOutcome {
        artifact_id: decision.artifact_id.clone(),
        delivery_intent_id: decision.delivery_intent_id.clone(),
        run_id: decision.run_id.clone(),
        state,
        evidence_sha256,
        reason_code: reason_code.to_owned(),
        parent_run_id: decision.parent_run_id.clone(),
        parent_wake_required: decision.parent_run_id.is_some(),
        observed_at_unix_ms: runtime_finalization_now()?,
    })?;
    Ok(())
}

enum DeliveryReconciliation {
    Replay { state: RuntimeDeliveryState, evidence_sha256: String },
    Observe(DeliveryOutboxState),
}

fn reconcile_delivery_outbox(
    outbox: &dyn DeliveryOutboxPort,
    durable: &RuntimeDeliverySnapshot,
    request: &OutboundMessageRequest,
) -> Result<DeliveryReconciliation, FinalizationHostError> {
    if matches!(
        durable.state,
        RuntimeDeliveryState::Delivered | RuntimeDeliveryState::OutcomeUnknown
    ) {
        let evidence_sha256 = durable
            .evidence_sha256
            .clone()
            .ok_or(FinalizationHostError::InvalidDeliveryMetadata)?;
        return Ok(DeliveryReconciliation::Replay { state: durable.state, evidence_sha256 });
    }

    let state = outbox.inspect(request.connector_id.as_str(), request.envelope_id.as_str())?;
    if state == DeliveryOutboxState::Missing {
        if durable.state == RuntimeDeliveryState::DeadLetter {
            return Ok(DeliveryReconciliation::Observe(DeliveryOutboxState::DeadLetter));
        }
        outbox.enqueue(request)?;
        return Ok(DeliveryReconciliation::Observe(DeliveryOutboxState::Queued));
    }
    Ok(DeliveryReconciliation::Observe(state))
}

fn finalization_phase_error(_error: FinalizationHostError) -> KernelPhaseError {
    KernelPhaseError::HostService {
        phase: palyra_common::runtime_contracts::RuntimeErrorPhase::Finalization,
        reason: KernelPhaseReason::FinalizationBlocked,
        evidence: None,
    }
}

fn delivery_phase_output(
    input: &DeliveryPhaseInput,
    state: RuntimeDeliveryState,
    evidence_sha256: String,
) -> Result<DeliveryPhaseOutput, KernelPhaseError> {
    let disposition = match state {
        RuntimeDeliveryState::IntentRecorded => DeliveryDisposition::IntentRecorded,
        RuntimeDeliveryState::Queued => DeliveryDisposition::Queued,
        RuntimeDeliveryState::OutcomeUnknown => DeliveryDisposition::Unknown,
        RuntimeDeliveryState::DeadLetter => DeliveryDisposition::DeadLetter,
        RuntimeDeliveryState::Delivered => DeliveryDisposition::Delivered,
    };
    let reason = if disposition == DeliveryDisposition::Unknown {
        KernelPhaseReason::DeliveryUnknown
    } else {
        KernelPhaseReason::DeliveryAdvanced
    };
    let evidence = RedactedEvidenceRef::from_host(
        input.payload().final_projection.id().clone(),
        digest_array(evidence_sha256.as_bytes()),
    );
    KernelPhaseOutput::from_input(
        input,
        reason,
        DeliveryResult {
            delivery_intent_id: input.payload().delivery_intent_id.clone(),
            disposition,
            evidence,
        },
    )
    .map_err(KernelPhaseError::from)
}

fn delivery_phase_error(error: FinalizationHostError) -> KernelPhaseError {
    let _ = error;
    KernelPhaseError::HostService {
        phase: palyra_common::runtime_contracts::RuntimeErrorPhase::DeliveryQueue,
        reason: KernelPhaseReason::DeliveryUnknown,
        evidence: None,
    }
}

fn delivery_evidence_sha256(
    intent: &RuntimeDeliveryIntentDescriptor,
    state: RuntimeDeliveryState,
    reason_code: &str,
) -> String {
    let mut digest = Sha256::new();
    digest.update(b"palyra.runtime.delivery.link.v1\0");
    digest.update(intent.delivery_intent_id.as_bytes());
    digest.update([0]);
    digest.update(intent.dedupe_key.as_bytes());
    digest.update([0]);
    digest.update(
        match state {
            RuntimeDeliveryState::IntentRecorded => "intent_recorded",
            RuntimeDeliveryState::Queued => "queued",
            RuntimeDeliveryState::OutcomeUnknown => "outcome_unknown",
            RuntimeDeliveryState::DeadLetter => "dead_letter",
            RuntimeDeliveryState::Delivered => "delivered",
        }
        .as_bytes(),
    );
    digest.update([0]);
    digest.update(reason_code.as_bytes());
    hex::encode(digest.finalize())
}

/// Computes the domain-separated destination binding expected from a retained request.
#[must_use]
pub(crate) fn final_delivery_destination_binding(request: &OutboundMessageRequest) -> String {
    let mut digest = Sha256::new();
    digest.update(b"palyra.runtime.delivery.destination.v1\0");
    digest.update(request.connector_id.as_bytes());
    digest.update([0]);
    digest.update(request.conversation_id.as_bytes());
    digest.update([0]);
    digest.update(request.reply_thread_id.as_deref().unwrap_or_default().as_bytes());
    hex::encode(digest.finalize())
}

fn deterministic_final_envelope_id(
    run_id: &str,
    run_generation: palyra_common::runtime_contracts::RuntimeGeneration,
    run_lease_id: &str,
    delivery_intent_id: &str,
    artifact_id: &str,
    content_sha256: &str,
    destination_binding_sha256: &str,
) -> String {
    let mut digest = Sha256::new();
    digest.update(b"palyra.runtime.delivery.envelope.v1\0");
    for value in [
        run_id,
        run_lease_id,
        delivery_intent_id,
        artifact_id,
        content_sha256,
        destination_binding_sha256,
    ] {
        digest.update(value.as_bytes());
        digest.update([0]);
    }
    digest.update(run_generation.get().to_be_bytes());
    format!("v2-final-{}", hex::encode(digest.finalize()))
}

/// Hashes the canonical complete request that can affect an external delivery.
fn final_delivery_request_sha256(
    request: &OutboundMessageRequest,
) -> Result<String, FinalizationHostError> {
    let value = serde_json::to_value(request)
        .map_err(|_| FinalizationHostError::InvalidDeliveryMetadata)?;
    let mut digest = Sha256::new();
    digest.update(b"palyra.runtime.delivery.outbound-request.v1\0");
    digest.update(canonical_json_bytes(&value));
    Ok(hex::encode(digest.finalize()))
}

fn final_delivery_content_matches(content_sha256: &str, outbound_text: &str) -> bool {
    valid_sha256(content_sha256) && hex_sha256(outbound_text.as_bytes()) == content_sha256
}

fn hex_sha256(bytes: &[u8]) -> String {
    let mut digest = Sha256::new();
    digest.update(bytes);
    hex::encode(digest.finalize())
}

fn digest_array(bytes: &[u8]) -> [u8; 32] {
    Sha256::digest(bytes).into()
}

fn valid_sha256(value: &str) -> bool {
    value.len() == 64
        && value.bytes().all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

#[cfg(test)]
mod tests;
