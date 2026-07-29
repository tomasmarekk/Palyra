//! Production host adapters for context, provider, and compaction phases.
//!
//! Raw prompts, provider responses, failures, and compaction artifacts remain
//! in this host-owned retention store. Kernel contracts carry only typed,
//! digest-bound references into that state.

pub(crate) mod compaction;
pub(crate) mod context_assembly;
pub(crate) mod provider_call;

use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};

use palyra_common::qa_runtime_path::ProviderLaneAttestationEvent;
use palyra_common::runtime_contracts::{
    RuntimeContextProjectionId, RuntimeOperationId, RuntimeToolProposalId,
};
use sha2::{Digest, Sha256};
use ulid::Ulid;

use crate::{
    application::{
        run_stream::embedded_attempt::{
            EmbeddedAttemptHostState, EmbeddedDeliveryPlan, EmbeddedProviderTurn,
        },
        runtime_kernel_v2::{
            harness::{HarnessAttemptRequest, HarnessFuture, HarnessTerminalOutcome},
            host_event_contract::HarnessDeliveryBinding,
            phases::{
                CompactionRequest, CompactionResult, ContextAssemblyRequest, ContextAssemblyResult,
                DeliveryResult, FinalizationReceipt, FinalizationRequest, ProviderCallRequest,
                ProviderCallResult, ToolResultProjection,
            },
        },
    },
    journal::RuntimeProviderLaneAuthority,
    model_provider::{ProviderRequest, ProviderResponse, TerminalOutcomeClassification},
    provider_leases::ProviderLeaseExecutionContext,
};

use compaction::{ProductionCompactionService, RetainedCompactionWork};
use context_assembly::{
    verify_context_binding, PreassembledContextBindingError, PreassembledContextEngineBinding,
    ProductionContextAssemblyService, RetainedContextAssemblyWork,
};
use provider_call::ProductionProviderCallService;

use super::phases::{
    ContextInputRef, ProviderRequestRef, ProviderResponseRef, RedactedEvidenceRef,
};

#[derive(Clone)]
struct RetainedProviderRequest {
    sha256: [u8; 32],
    projection_id: RuntimeContextProjectionId,
    request: ProviderRequest,
    lease: ProviderLeaseExecutionContext,
}

struct RetainedProviderResponse {
    sha256: [u8; 32],
    response: ProviderResponse,
    terminal: TerminalOutcomeClassification,
}

#[derive(Default)]
struct RetainedPayloads {
    context_work: BTreeMap<String, ([u8; 32], Arc<dyn RetainedContextAssemblyWork>)>,
    compaction_work: BTreeMap<String, ([u8; 32], Arc<dyn RetainedCompactionWork>)>,
    provider_requests: BTreeMap<String, RetainedProviderRequest>,
    provider_responses: BTreeMap<String, RetainedProviderResponse>,
    evidence: BTreeMap<String, ([u8; 32], String)>,
    provider_failure_attestations: BTreeMap<String, ProviderLaneAttestationEvent>,
}

/// Run-owned raw payload retention shared by the three production services.
pub(crate) struct ProductionPayloadRetention {
    inner: Mutex<RetainedPayloads>,
    provider_authority: RuntimeProviderLaneAuthority,
}

impl ProductionPayloadRetention {
    fn new(provider_authority: RuntimeProviderLaneAuthority) -> Self {
        Self { inner: Mutex::new(RetainedPayloads::default()), provider_authority }
    }

    /// Registers one real context-assembly invocation behind an opaque manifest.
    pub(crate) fn retain_context_work(
        &self,
        work: Arc<dyn RetainedContextAssemblyWork>,
    ) -> ContextInputRef {
        let (id, digest) = new_ref("context-input", work.evidence_material());
        self.inner
            .lock()
            .expect("production payload retention lock should remain available")
            .context_work
            .insert(id.as_str().to_owned(), (digest, work));
        ContextInputRef::from_host(id, digest)
    }

    /// Registers one real compaction invocation behind an opaque pressure manifest.
    pub(crate) fn retain_compaction_work(
        &self,
        work: Arc<dyn RetainedCompactionWork>,
    ) -> ContextInputRef {
        let (id, digest) = new_ref("compaction-input", work.evidence_material());
        self.inner
            .lock()
            .expect("production payload retention lock should remain available")
            .compaction_work
            .insert(id.as_str().to_owned(), (digest, work));
        ContextInputRef::from_host(id, digest)
    }

    fn context_work(
        &self,
        reference: &ContextInputRef,
    ) -> Option<Arc<dyn RetainedContextAssemblyWork>> {
        let (sha256, work) =
            self.inner.lock().ok()?.context_work.get(reference.id().as_str()).cloned()?;
        (sha256 == *reference.sha256()).then_some(work)
    }

    fn compaction_work(
        &self,
        reference: &ContextInputRef,
    ) -> Option<Arc<dyn RetainedCompactionWork>> {
        let (sha256, work) =
            self.inner.lock().ok()?.compaction_work.get(reference.id().as_str()).cloned()?;
        (sha256 == *reference.sha256()).then_some(work)
    }

    fn retain_provider_request(
        &self,
        projection_id: RuntimeContextProjectionId,
        request: ProviderRequest,
        mut lease: ProviderLeaseExecutionContext,
    ) -> ProviderRequestRef {
        lease.runtime_authority = Some(self.provider_authority.clone());
        let material = serde_json::to_vec(&request).unwrap_or_default();
        let (id, digest) = new_ref("provider-request", material.as_slice());
        self.inner
            .lock()
            .expect("production payload retention lock should remain available")
            .provider_requests
            .insert(
                id.as_str().to_owned(),
                RetainedProviderRequest { sha256: digest, projection_id, request, lease },
            );
        ProviderRequestRef::from_host(id, digest)
    }

    fn provider_request(&self, reference: &ProviderRequestRef) -> Option<RetainedProviderRequest> {
        let retained =
            self.inner.lock().ok()?.provider_requests.get(reference.id().as_str()).cloned()?;
        (retained.sha256 == *reference.sha256()).then_some(retained)
    }

    fn retain_provider_response(
        &self,
        response: ProviderResponse,
        terminal: TerminalOutcomeClassification,
    ) -> ProviderResponseRef {
        let material = serde_json::to_vec(&terminal).unwrap_or_default();
        let (id, digest) = new_ref("provider-response", material.as_slice());
        self.inner
            .lock()
            .expect("production payload retention lock should remain available")
            .provider_responses
            .insert(
                id.as_str().to_owned(),
                RetainedProviderResponse { sha256: digest, response, terminal },
            );
        ProviderResponseRef::from_host(id, digest)
    }

    /// Removes and returns one normalized response for host-side projection.
    pub(crate) fn take_provider_response(
        &self,
        reference: &ProviderResponseRef,
    ) -> Option<(ProviderResponse, TerminalOutcomeClassification)> {
        self.inner
            .lock()
            .ok()?
            .provider_responses
            .remove(reference.id().as_str())
            .filter(|retained| retained.sha256 == *reference.sha256())
            .map(|retained| (retained.response, retained.terminal))
    }

    fn retain_evidence(&self, reason_code: &str) -> RedactedEvidenceRef {
        self.retain_evidence_material(reason_code, reason_code.as_bytes())
    }

    fn retain_evidence_material(&self, reason_code: &str, material: &[u8]) -> RedactedEvidenceRef {
        let (id, digest) = new_ref("redacted-evidence", material);
        self.inner
            .lock()
            .expect("production payload retention lock should remain available")
            .evidence
            .insert(id.as_str().to_owned(), (digest, reason_code.to_owned()));
        RedactedEvidenceRef::from_host(id, digest)
    }

    fn bind_provider_failure_attestation(
        &self,
        evidence: &RedactedEvidenceRef,
        attestation: ProviderLaneAttestationEvent,
    ) {
        self.inner
            .lock()
            .expect("production payload retention lock should remain available")
            .provider_failure_attestations
            .insert(evidence.id().as_str().to_owned(), attestation);
    }

    fn take_provider_failure_attestation(
        &self,
        evidence: &RedactedEvidenceRef,
    ) -> Option<ProviderLaneAttestationEvent> {
        self.inner.lock().ok()?.provider_failure_attestations.remove(evidence.id().as_str())
    }

    /// Resolves a retained low-cardinality failure reason without raw payloads.
    pub(crate) fn take_evidence(&self, reference: &RedactedEvidenceRef) -> Option<String> {
        let (sha256, reason) = self.inner.lock().ok()?.evidence.remove(reference.id().as_str())?;
        (sha256 == *reference.sha256()).then_some(reason)
    }
}

/// Raw provider request prepared by the host after a tool result or compaction.
pub(crate) struct PreparedProductionProviderTurn {
    pub(crate) projection_id: RuntimeContextProjectionId,
    pub(crate) request: ProviderRequest,
    pub(crate) lease: ProviderLeaseExecutionContext,
}

/// Run-specific behavior around the payload-retention state.
///
/// The callbacks own transcript mutation, streaming projection, verification,
/// finalization, and delivery. Raw provider data is supplied only here, never
/// through a kernel phase contract.
pub(crate) trait ProductionAttemptCallbacks: Send + Sync {
    fn provider_effect_started<'a>(
        &'a self,
    ) -> HarnessFuture<'a, Result<(), palyra_common::runtime_contracts::RuntimeErrorEnvelopeV1>>;

    fn prepare_provider_turn<'a>(
        &'a self,
        request: &'a HarnessAttemptRequest,
        context: &'a ContextAssemblyResult,
    ) -> HarnessFuture<
        'a,
        Result<
            Option<PreparedProductionProviderTurn>,
            palyra_common::runtime_contracts::RuntimeErrorEnvelopeV1,
        >,
    >;

    fn project_provider_response<'a>(
        &'a self,
        request: &'a HarnessAttemptRequest,
        response: ProviderResponse,
        terminal: TerminalOutcomeClassification,
    ) -> HarnessFuture<
        'a,
        Result<EmbeddedProviderTurn, palyra_common::runtime_contracts::RuntimeErrorEnvelopeV1>,
    >;

    fn project_provider_failure<'a>(
        &'a self,
        request: &'a HarnessAttemptRequest,
        reason_code: String,
        output_emitted: bool,
        qa_lane_attestation: Option<ProviderLaneAttestationEvent>,
    ) -> HarnessFuture<
        'a,
        Result<EmbeddedProviderTurn, palyra_common::runtime_contracts::RuntimeErrorEnvelopeV1>,
    >;

    fn accept_compaction<'a>(
        &'a self,
        request: &'a HarnessAttemptRequest,
        result: &'a CompactionResult,
    ) -> HarnessFuture<'a, Result<(), palyra_common::runtime_contracts::RuntimeErrorEnvelopeV1>>;

    fn accept_tool_projection<'a>(
        &'a self,
        request: &'a HarnessAttemptRequest,
        projection: ToolResultProjection,
    ) -> HarnessFuture<'a, Result<(), palyra_common::runtime_contracts::RuntimeErrorEnvelopeV1>>;

    fn accept_tool_denial<'a>(
        &'a self,
        request: &'a HarnessAttemptRequest,
        proposal_id: RuntimeToolProposalId,
        reason_code: &'static str,
    ) -> HarnessFuture<'a, Result<(), palyra_common::runtime_contracts::RuntimeErrorEnvelopeV1>>;

    fn verify<'a>(
        &'a self,
        request: &'a HarnessAttemptRequest,
    ) -> HarnessFuture<'a, Result<(), palyra_common::runtime_contracts::RuntimeErrorEnvelopeV1>>;

    fn finalization_request<'a>(
        &'a self,
        request: &'a HarnessAttemptRequest,
        outcome: &'a HarnessTerminalOutcome,
    ) -> HarnessFuture<
        'a,
        Result<FinalizationRequest, palyra_common::runtime_contracts::RuntimeErrorEnvelopeV1>,
    >;

    fn accept_finalization<'a>(
        &'a self,
        request: &'a HarnessAttemptRequest,
        receipt: FinalizationReceipt,
    ) -> HarnessFuture<'a, Result<(), palyra_common::runtime_contracts::RuntimeErrorEnvelopeV1>>;

    fn delivery_plan<'a>(
        &'a self,
        request: &'a HarnessAttemptRequest,
        outcome: &'a HarnessTerminalOutcome,
    ) -> HarnessFuture<
        'a,
        Result<EmbeddedDeliveryPlan, palyra_common::runtime_contracts::RuntimeErrorEnvelopeV1>,
    >;

    fn accept_delivery<'a>(
        &'a self,
        request: &'a HarnessAttemptRequest,
        result: DeliveryResult,
    ) -> HarnessFuture<
        'a,
        Result<HarnessDeliveryBinding, palyra_common::runtime_contracts::RuntimeErrorEnvelopeV1>,
    >;

    fn kernel_failure(
        &self,
        reason_code: &'static str,
    ) -> palyra_common::runtime_contracts::RuntimeErrorEnvelopeV1;
}

#[derive(Default)]
struct AttemptCursor {
    context: Option<ContextAssemblyResult>,
}

/// Concrete payload-retaining implementation of the host-state boundary.
pub(crate) struct ProductionAttemptHostState {
    retention: Arc<ProductionPayloadRetention>,
    context_request: ContextAssemblyRequest,
    compaction_manifest: ContextInputRef,
    callbacks: Arc<dyn ProductionAttemptCallbacks>,
    cursor: Mutex<AttemptCursor>,
}

impl ProductionAttemptHostState {
    pub(crate) fn new(
        retention: Arc<ProductionPayloadRetention>,
        context_request: ContextAssemblyRequest,
        compaction_manifest: ContextInputRef,
        callbacks: Arc<dyn ProductionAttemptCallbacks>,
    ) -> Self {
        Self {
            retention,
            context_request,
            compaction_manifest,
            callbacks,
            cursor: Mutex::new(AttemptCursor::default()),
        }
    }
}

impl EmbeddedAttemptHostState for ProductionAttemptHostState {
    fn context_request<'a>(
        &'a self,
        _request: &'a HarnessAttemptRequest,
    ) -> HarnessFuture<
        'a,
        Result<ContextAssemblyRequest, palyra_common::runtime_contracts::RuntimeErrorEnvelopeV1>,
    > {
        let payload = self.context_request.clone();
        Box::pin(async move { Ok(payload) })
    }

    fn accept_context<'a>(
        &'a self,
        _request: &'a HarnessAttemptRequest,
        result: ContextAssemblyResult,
    ) -> HarnessFuture<'a, Result<(), palyra_common::runtime_contracts::RuntimeErrorEnvelopeV1>>
    {
        Box::pin(async move {
            self.cursor
                .lock()
                .map_err(|_| self.callbacks.kernel_failure("runtime.context.cursor_unavailable"))?
                .context = Some(result);
            Ok(())
        })
    }

    fn provider_request<'a>(
        &'a self,
        request: &'a HarnessAttemptRequest,
    ) -> HarnessFuture<
        'a,
        Result<ProviderCallRequest, palyra_common::runtime_contracts::RuntimeErrorEnvelopeV1>,
    > {
        Box::pin(async move {
            let context = self
                .cursor
                .lock()
                .map_err(|_| self.callbacks.kernel_failure("runtime.context.cursor_unavailable"))?
                .context
                .clone()
                .ok_or_else(|| self.callbacks.kernel_failure("runtime.context.not_assembled"))?;
            let prepared = self.callbacks.prepare_provider_turn(request, &context).await?;
            let (projection_id, provider_request) = match prepared {
                Some(prepared) => {
                    let provider_request = self.retention.retain_provider_request(
                        prepared.projection_id.clone(),
                        prepared.request,
                        prepared.lease,
                    );
                    (prepared.projection_id, provider_request)
                }
                None => (context.projection_id, context.provider_request),
            };
            Ok(ProviderCallRequest { context_projection_id: projection_id, provider_request })
        })
    }

    fn project_provider_result<'a>(
        &'a self,
        request: &'a HarnessAttemptRequest,
        result: ProviderCallResult,
    ) -> HarnessFuture<
        'a,
        Result<EmbeddedProviderTurn, palyra_common::runtime_contracts::RuntimeErrorEnvelopeV1>,
    > {
        Box::pin(async move {
            match result {
                ProviderCallResult::Completed { response, .. } => {
                    let (response, terminal) =
                        self.retention.take_provider_response(&response).ok_or_else(|| {
                            self.callbacks.kernel_failure("runtime.provider.response_missing")
                        })?;
                    self.callbacks.project_provider_response(request, response, terminal).await
                }
                ProviderCallResult::Failed { evidence, output_emitted } => {
                    let qa_lane_attestation =
                        self.retention.take_provider_failure_attestation(&evidence);
                    let reason_code = self.retention.take_evidence(&evidence).ok_or_else(|| {
                        self.callbacks.kernel_failure("runtime.provider.evidence_missing")
                    })?;
                    self.callbacks
                        .project_provider_failure(
                            request,
                            reason_code,
                            output_emitted,
                            qa_lane_attestation,
                        )
                        .await
                }
            }
        })
    }

    fn compaction_request<'a>(
        &'a self,
        _request: &'a HarnessAttemptRequest,
    ) -> HarnessFuture<
        'a,
        Result<CompactionRequest, palyra_common::runtime_contracts::RuntimeErrorEnvelopeV1>,
    > {
        Box::pin(async move {
            let context_projection_id = self
                .cursor
                .lock()
                .map_err(|_| self.callbacks.kernel_failure("runtime.context.cursor_unavailable"))?
                .context
                .as_ref()
                .map(|context| context.projection_id.clone())
                .ok_or_else(|| self.callbacks.kernel_failure("runtime.context.not_assembled"))?;
            Ok(CompactionRequest {
                context_projection_id,
                pressure_manifest: self.compaction_manifest.clone(),
            })
        })
    }

    fn accept_compaction<'a>(
        &'a self,
        request: &'a HarnessAttemptRequest,
        result: CompactionResult,
    ) -> HarnessFuture<'a, Result<(), palyra_common::runtime_contracts::RuntimeErrorEnvelopeV1>>
    {
        Box::pin(async move {
            self.callbacks.accept_compaction(request, &result).await?;
            if let CompactionResult::Applied { context_projection_id, .. } = result {
                if let Some(context) = self
                    .cursor
                    .lock()
                    .map_err(|_| {
                        self.callbacks.kernel_failure("runtime.context.cursor_unavailable")
                    })?
                    .context
                    .as_mut()
                {
                    context.projection_id = context_projection_id;
                }
            }
            Ok(())
        })
    }

    fn accept_tool_projection<'a>(
        &'a self,
        request: &'a HarnessAttemptRequest,
        projection: ToolResultProjection,
    ) -> HarnessFuture<'a, Result<(), palyra_common::runtime_contracts::RuntimeErrorEnvelopeV1>>
    {
        self.callbacks.accept_tool_projection(request, projection)
    }

    fn accept_tool_denial<'a>(
        &'a self,
        request: &'a HarnessAttemptRequest,
        proposal_id: RuntimeToolProposalId,
        reason_code: &'static str,
    ) -> HarnessFuture<'a, Result<(), palyra_common::runtime_contracts::RuntimeErrorEnvelopeV1>>
    {
        self.callbacks.accept_tool_denial(request, proposal_id, reason_code)
    }

    fn verify<'a>(
        &'a self,
        request: &'a HarnessAttemptRequest,
    ) -> HarnessFuture<'a, Result<(), palyra_common::runtime_contracts::RuntimeErrorEnvelopeV1>>
    {
        self.callbacks.verify(request)
    }

    fn finalization_request<'a>(
        &'a self,
        request: &'a HarnessAttemptRequest,
        outcome: &'a HarnessTerminalOutcome,
    ) -> HarnessFuture<
        'a,
        Result<FinalizationRequest, palyra_common::runtime_contracts::RuntimeErrorEnvelopeV1>,
    > {
        self.callbacks.finalization_request(request, outcome)
    }

    fn accept_finalization<'a>(
        &'a self,
        request: &'a HarnessAttemptRequest,
        receipt: FinalizationReceipt,
    ) -> HarnessFuture<'a, Result<(), palyra_common::runtime_contracts::RuntimeErrorEnvelopeV1>>
    {
        self.callbacks.accept_finalization(request, receipt)
    }

    fn delivery_plan<'a>(
        &'a self,
        request: &'a HarnessAttemptRequest,
        outcome: &'a HarnessTerminalOutcome,
    ) -> HarnessFuture<
        'a,
        Result<EmbeddedDeliveryPlan, palyra_common::runtime_contracts::RuntimeErrorEnvelopeV1>,
    > {
        self.callbacks.delivery_plan(request, outcome)
    }

    fn accept_delivery<'a>(
        &'a self,
        request: &'a HarnessAttemptRequest,
        result: DeliveryResult,
    ) -> HarnessFuture<
        'a,
        Result<HarnessDeliveryBinding, palyra_common::runtime_contracts::RuntimeErrorEnvelopeV1>,
    > {
        self.callbacks.accept_delivery(request, result)
    }

    fn kernel_failure(
        &self,
        reason_code: &'static str,
    ) -> palyra_common::runtime_contracts::RuntimeErrorEnvelopeV1 {
        self.callbacks.kernel_failure(reason_code)
    }
}

/// Construction result consumed by the authoritative dispatcher.
pub(crate) struct ProductionServiceBundle {
    pub(crate) context_assembly: Arc<ProductionContextAssemblyService>,
    pub(crate) provider_call: Arc<ProductionProviderCallService>,
    pub(crate) compaction: Arc<ProductionCompactionService>,
    pub(crate) host_state: Arc<ProductionAttemptHostState>,
    // Retain the RAII authority for at least the full lifetime of every
    // service and host-state handle issued from this bundle.
    _provider_authority: RuntimeProviderLaneAuthority,
}

impl ProductionServiceBundle {
    /// Creates one run-owned production service set and its payload-retaining state.
    pub(crate) fn new(
        runtime_state: Arc<crate::gateway::GatewayRuntimeState>,
        provider_authority: RuntimeProviderLaneAuthority,
        context_binding: PreassembledContextEngineBinding,
        context_work: Arc<dyn RetainedContextAssemblyWork>,
        max_input_tokens: u64,
        compaction_work: Arc<dyn RetainedCompactionWork>,
        callbacks: Arc<dyn ProductionAttemptCallbacks>,
    ) -> Result<Self, PreassembledContextBindingError> {
        verify_context_binding(&context_binding, context_work.context_engine_binding())?;
        let retention = Arc::new(ProductionPayloadRetention::new(provider_authority.clone()));
        let context_request = ContextAssemblyRequest {
            input_manifest: retention.retain_context_work(context_work),
            max_input_tokens,
        };
        let compaction_manifest = retention.retain_compaction_work(compaction_work);
        let context_assembly = Arc::new(ProductionContextAssemblyService::new(
            Arc::clone(&retention),
            context_binding,
        ));
        let provider_call = Arc::new(ProductionProviderCallService::new(
            runtime_state,
            Arc::clone(&retention),
            provider_authority.clone(),
            Arc::clone(&callbacks),
        ));
        let compaction = Arc::new(ProductionCompactionService::new(Arc::clone(&retention)));
        let host_state = Arc::new(ProductionAttemptHostState::new(
            retention,
            context_request,
            compaction_manifest,
            callbacks,
        ));
        Ok(Self {
            context_assembly,
            provider_call,
            compaction,
            host_state,
            _provider_authority: provider_authority,
        })
    }
}

fn new_ref(domain: &str, material: &[u8]) -> (RuntimeOperationId, [u8; 32]) {
    let id = RuntimeOperationId::parse(Ulid::new().to_string().as_str())
        .expect("generated ULID must be a valid runtime operation identity");
    let mut hasher = Sha256::new();
    hasher.update(domain.as_bytes());
    hasher.update([0]);
    hasher.update(material);
    (id, hasher.finalize().into())
}

fn new_projection_id() -> RuntimeContextProjectionId {
    RuntimeContextProjectionId::parse(Ulid::new().to_string().as_str())
        .expect("generated ULID must be a valid context projection identity")
}
