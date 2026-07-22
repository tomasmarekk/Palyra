//! RuntimeKernelV2 preassembled-context adapter and production assembly service.

use std::time::Duration;
use std::{future::Future, pin::Pin, sync::Arc};

use palyra_common::{
    qa_runtime_path::{
        ContextEngineBindingEvent, CONTEXT_ENGINE_BINDING_EVENT,
        CONTEXT_ENGINE_BINDING_EVENT_SCHEMA_VERSION,
    },
    runtime_contracts::{
        RuntimeErrorPhase, RUNTIME_KERNEL_V2_PREASSEMBLED_CONTEXT_ENGINE_ID,
        RUNTIME_KERNEL_V2_PREASSEMBLED_CONTEXT_ENGINE_VERSION,
    },
};
use serde::Serialize;
use thiserror::Error;

use crate::{
    application::{
        context_engine::ContextEngineDescriptor,
        runtime_kernel_v2::runtime_selection::RuntimeSelectionV1,
    },
    model_provider::ProviderRequest,
    provider_leases::ProviderLeaseExecutionContext,
};

use super::{new_projection_id, ProductionPayloadRetention};
use crate::application::runtime_kernel_v2::phases::{
    ContextAssemblyPhase, ContextAssemblyRequest, ContextAssemblyResult, KernelPhaseError,
    KernelPhaseFuture, KernelPhaseOutput, KernelPhaseReason, RuntimePhaseService,
};

/// Successful host context assembly retained before the kernel sees its reference.
pub(crate) struct AssembledProviderRequest {
    pub(crate) request: ProviderRequest,
    pub(crate) lease: ProviderLeaseExecutionContext,
    pub(crate) segment_manifest_sha256: [u8; 32],
    pub(crate) retained_token_estimate: u64,
}

/// Immutable identity shared by selection, retention, and the executing adapter.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PreassembledContextEngineBinding {
    engine_id: String,
    engine_version: String,
    projection_epoch: u64,
}

impl PreassembledContextEngineBinding {
    /// Binds the actual adapter to the exact context selection and host epoch.
    ///
    /// # Errors
    /// Returns [`PreassembledContextBindingError`] when selection named another
    /// implementation or when its immutable epoch differs from the host snapshot.
    pub(crate) fn from_selection(
        selection: &RuntimeSelectionV1,
        expected_projection_epoch: u64,
    ) -> Result<Self, PreassembledContextBindingError> {
        Self::from_selected_parts(
            selection.selected_context_engine_id(),
            selection.selected_context_projection_epoch(),
            expected_projection_epoch,
        )
    }

    fn from_selected_parts(
        selected_engine_id: &str,
        selected_projection_epoch: u64,
        expected_projection_epoch: u64,
    ) -> Result<Self, PreassembledContextBindingError> {
        if selected_engine_id != RUNTIME_KERNEL_V2_PREASSEMBLED_CONTEXT_ENGINE_ID {
            return Err(PreassembledContextBindingError::EngineId);
        }
        if selected_projection_epoch == 0 || selected_projection_epoch != expected_projection_epoch
        {
            return Err(PreassembledContextBindingError::ProjectionEpoch);
        }
        Ok(Self {
            engine_id: RUNTIME_KERNEL_V2_PREASSEMBLED_CONTEXT_ENGINE_ID.to_owned(),
            engine_version: RUNTIME_KERNEL_V2_PREASSEMBLED_CONTEXT_ENGINE_VERSION.to_owned(),
            projection_epoch: selected_projection_epoch,
        })
    }

    /// Returns bounded runtime-path evidence for the verified binding.
    #[must_use]
    pub(crate) fn evidence_event(&self) -> ContextEngineBindingEvent {
        ContextEngineBindingEvent {
            schema_version: CONTEXT_ENGINE_BINDING_EVENT_SCHEMA_VERSION,
            event_name: CONTEXT_ENGINE_BINDING_EVENT.to_owned(),
            engine_id: self.engine_id.clone(),
            engine_version: self.engine_version.clone(),
            projection_epoch: self.projection_epoch,
        }
    }
}

/// Fail-closed errors for selected and executable context-engine drift.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub(crate) enum PreassembledContextBindingError {
    #[error("selected context engine does not match the RuntimeKernelV2 adapter")]
    EngineId,
    #[error("selected context projection epoch does not match the host snapshot")]
    ProjectionEpoch,
    #[error("retained context work does not match the selected context engine binding")]
    RetainedWork,
}

/// Returns the exact descriptor registered by authoritative V2 selection.
#[must_use]
pub(crate) fn preassembled_context_engine_descriptor() -> ContextEngineDescriptor {
    ContextEngineDescriptor {
        engine_id: RUNTIME_KERNEL_V2_PREASSEMBLED_CONTEXT_ENGINE_ID.to_owned(),
        label: "RuntimeKernelV2 Preassembled Context Adapter".to_owned(),
        version: RUNTIME_KERNEL_V2_PREASSEMBLED_CONTEXT_ENGINE_VERSION.to_owned(),
        lifecycle_hooks: vec!["assemble_preassembled_context".to_owned()],
    }
}

/// One retained invocation of the selected preassembled-context adapter.
pub(crate) trait RetainedContextAssemblyWork: Send + Sync {
    fn context_engine_binding(&self) -> &PreassembledContextEngineBinding;

    fn evidence_material(&self) -> &[u8];

    fn assemble(
        self: Arc<Self>,
    ) -> Pin<Box<dyn Future<Output = Result<AssembledProviderRequest, &'static str>> + Send>>;
}

/// Already assembled canonical run-stream request retained for the V2 phase.
///
/// RunStream assembles its first context before the runtime branch so the
/// legacy and V2 paths observe the same transcript snapshot. V2 still consumes
/// that request only through the canonical context-assembly service.
pub(crate) struct PreassembledContextAssemblyInput {
    binding: PreassembledContextEngineBinding,
    assembled: AssembledProviderRequest,
    manifest_material: Vec<u8>,
}

impl PreassembledContextAssemblyInput {
    pub(crate) fn new(
        request: ProviderRequest,
        lease: ProviderLeaseExecutionContext,
        binding: PreassembledContextEngineBinding,
    ) -> Self {
        let manifest_material = serde_json::to_vec(&(&binding, &request)).unwrap_or_default();
        let retained_token_estimate = v2_context_retained_token_estimate(&request);
        let segment_manifest_sha256 = sha256_array(manifest_material.as_slice());
        Self {
            binding,
            assembled: AssembledProviderRequest {
                request,
                lease,
                segment_manifest_sha256,
                retained_token_estimate,
            },
            manifest_material,
        }
    }
}

/// Computes the V2 context-engine budget from the exact provider-message manifest.
///
/// This deliberately does not reuse the legacy prompt estimator. Shadow mode
/// and authoritative V2 execution therefore observe the same V2 planning
/// output and can expose drift against the legacy request budget.
#[must_use]
pub(crate) fn v2_context_retained_token_estimate(request: &ProviderRequest) -> u64 {
    let message_bytes = serde_json::to_vec(&request.effective_messages())
        .map(|bytes| u64::try_from(bytes.len()).unwrap_or(u64::MAX))
        .unwrap_or(u64::MAX);
    let message_tokens = message_bytes.saturating_add(3).saturating_div(4).max(1);
    let vision_tokens =
        u64::try_from(request.vision_inputs.len()).unwrap_or(u64::MAX).saturating_mul(256);
    message_tokens.saturating_add(vision_tokens)
}

impl RetainedContextAssemblyWork for PreassembledContextAssemblyInput {
    fn context_engine_binding(&self) -> &PreassembledContextEngineBinding {
        &self.binding
    }

    fn evidence_material(&self) -> &[u8] {
        self.manifest_material.as_slice()
    }

    fn assemble(
        self: Arc<Self>,
    ) -> Pin<Box<dyn Future<Output = Result<AssembledProviderRequest, &'static str>> + Send>> {
        Box::pin(async move {
            Ok(AssembledProviderRequest {
                request: self.assembled.request.clone(),
                lease: self.assembled.lease.clone(),
                segment_manifest_sha256: self.assembled.segment_manifest_sha256,
                retained_token_estimate: self.assembled.retained_token_estimate,
            })
        })
    }
}

pub(crate) struct ProductionContextAssemblyService {
    retention: Arc<ProductionPayloadRetention>,
    binding: PreassembledContextEngineBinding,
}

impl ProductionContextAssemblyService {
    pub(crate) fn new(
        retention: Arc<ProductionPayloadRetention>,
        binding: PreassembledContextEngineBinding,
    ) -> Self {
        Self { retention, binding }
    }
}

impl RuntimePhaseService<ContextAssemblyPhase, ContextAssemblyRequest, ContextAssemblyResult>
    for ProductionContextAssemblyService
{
    fn execute(
        &self,
        input: crate::application::runtime_kernel_v2::phases::ContextAssemblyPhaseInput,
    ) -> KernelPhaseFuture<
        '_,
        Result<
            crate::application::runtime_kernel_v2::phases::ContextAssemblyPhaseOutput,
            KernelPhaseError,
        >,
    > {
        Box::pin(async move {
            if input.boundary().execution().cancellation().signal().current_reason().is_some() {
                return Err(host_error(
                    &self.retention,
                    "runtime.context_assembly.cancelled_before_start",
                ));
            }
            let work =
                self.retention.context_work(&input.payload().input_manifest).ok_or_else(|| {
                    host_error(&self.retention, "runtime.context_assembly.missing_input")
                })?;
            verify_context_binding(&self.binding, work.context_engine_binding()).map_err(|_| {
                host_error(&self.retention, "runtime.context_assembly.binding_mismatch")
            })?;
            let mut assembly = tokio::spawn(work.assemble());
            let deadline = tokio::time::sleep(Duration::from_millis(
                input.boundary().execution().timeout_ms(),
            ));
            tokio::pin!(deadline);
            let assembled = tokio::select! {
                biased;
                _ = input.boundary().execution().cancellation().signal().cancelled() => {
                    await_started_work(&mut assembly).await;
                    return Err(host_error(
                        &self.retention,
                        "runtime.context_assembly.cancelled_after_start",
                    ));
                }
                _ = &mut deadline => {
                    await_started_work(&mut assembly).await;
                    return Err(host_error(
                        &self.retention,
                        "runtime.context_assembly.deadline_after_start",
                    ));
                }
                assembled = &mut assembly => assembled
                    .map_err(|_| host_error(
                        &self.retention,
                        "runtime.context_assembly.host_task_failed",
                    ))?
                    .map_err(|reason| host_error(&self.retention, reason))?,
            };
            if assembled.retained_token_estimate > input.payload().max_input_tokens {
                return Err(host_error(
                    &self.retention,
                    "runtime.context_assembly.budget_exceeded",
                ));
            }
            let projection_id = new_projection_id();
            let provider_request = self.retention.retain_provider_request(
                projection_id.clone(),
                assembled.request,
                assembled.lease,
            );
            let result = ContextAssemblyResult {
                projection_id,
                provider_request,
                segment_manifest_sha256: assembled.segment_manifest_sha256,
                retained_token_estimate: assembled.retained_token_estimate,
            };
            Ok(KernelPhaseOutput::from_input(&input, KernelPhaseReason::ContextAssembled, result)?)
        })
    }
}

pub(super) fn verify_context_binding(
    expected: &PreassembledContextEngineBinding,
    actual: &PreassembledContextEngineBinding,
) -> Result<(), PreassembledContextBindingError> {
    if expected != actual {
        return Err(PreassembledContextBindingError::RetainedWork);
    }
    Ok(())
}

async fn await_started_work<T>(work: &mut tokio::task::JoinHandle<T>) {
    // Once host mutation starts, generation terminalization must wait until the
    // task itself reaches a durable terminal point.
    let _ = work.await;
}

fn host_error(retention: &ProductionPayloadRetention, reason: &'static str) -> KernelPhaseError {
    KernelPhaseError::HostService {
        phase: RuntimeErrorPhase::ContextAssembly,
        reason: KernelPhaseReason::ContextAssemblyBlocked,
        evidence: Some(retention.retain_evidence(reason)),
    }
}

fn sha256_array(value: &[u8]) -> [u8; 32] {
    use sha2::{Digest, Sha256};
    Sha256::digest(value).into()
}

#[cfg(test)]
mod tests {
    use std::{
        sync::{
            atomic::{AtomicUsize, Ordering},
            Arc,
        },
        time::Duration,
    };

    use tokio::sync::Notify;

    use super::{
        await_started_work, v2_context_retained_token_estimate, verify_context_binding,
        PreassembledContextBindingError, PreassembledContextEngineBinding,
    };
    use crate::model_provider::ProviderRequest;

    #[test]
    fn v2_context_budget_is_stable_and_sensitive_to_its_own_message_manifest() {
        let short = ProviderRequest::from_input_text("short".to_owned(), false, Vec::new(), None);
        let long = ProviderRequest::from_input_text(
            "a V2 context message with materially more retained content".repeat(32),
            false,
            Vec::new(),
            None,
        );

        let short_budget = v2_context_retained_token_estimate(&short);
        assert_eq!(short_budget, v2_context_retained_token_estimate(&short));
        assert!(v2_context_retained_token_estimate(&long) > short_budget);
    }

    #[test]
    fn selected_context_binding_rejects_engine_or_epoch_drift() {
        let expected = PreassembledContextEngineBinding::from_selected_parts(
            palyra_common::runtime_contracts::RUNTIME_KERNEL_V2_PREASSEMBLED_CONTEXT_ENGINE_ID,
            7,
            7,
        )
        .expect("canonical selected binding should validate");
        assert_eq!(
            PreassembledContextEngineBinding::from_selected_parts("default_context_engine", 7, 7),
            Err(PreassembledContextBindingError::EngineId)
        );
        assert_eq!(
            PreassembledContextEngineBinding::from_selected_parts(
                palyra_common::runtime_contracts::RUNTIME_KERNEL_V2_PREASSEMBLED_CONTEXT_ENGINE_ID,
                8,
                7,
            ),
            Err(PreassembledContextBindingError::ProjectionEpoch)
        );

        let mut mismatched_work = expected.clone();
        mismatched_work.projection_epoch = 8;
        assert_eq!(
            verify_context_binding(&expected, &mismatched_work),
            Err(PreassembledContextBindingError::RetainedWork)
        );
    }

    #[tokio::test]
    async fn interrupted_phase_waits_for_slow_host_terminal_before_returning() {
        let started = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let writes = Arc::new(AtomicUsize::new(0));
        let mut work = tokio::spawn({
            let started = Arc::clone(&started);
            let release = Arc::clone(&release);
            let writes = Arc::clone(&writes);
            async move {
                started.notify_one();
                release.notified().await;
                writes.fetch_add(1, Ordering::SeqCst);
            }
        });
        started.notified().await;
        let settlement = await_started_work(&mut work);
        tokio::pin!(settlement);
        assert!(
            tokio::time::timeout(Duration::from_millis(10), &mut settlement).await.is_err(),
            "phase must remain non-terminal while durable host work is unresolved"
        );
        release.notify_one();
        settlement.await;
        assert_eq!(writes.load(Ordering::SeqCst), 1);
        tokio::task::yield_now().await;
        assert_eq!(writes.load(Ordering::SeqCst), 1);
    }
}
