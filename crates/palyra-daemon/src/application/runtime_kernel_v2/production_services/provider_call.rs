//! Provider-gateway-backed production provider phase.

use std::{sync::Arc, time::Duration};

use palyra_common::runtime_contracts::{RuntimeErrorPhase, RuntimeGenerationLane};
use palyra_model_providers::{classify_terminal_outcome, provider_events_from_output};

use crate::{gateway::GatewayRuntimeState, journal::RuntimeProviderLaneAuthority};

use super::{ProductionAttemptCallbacks, ProductionPayloadRetention};
use crate::application::runtime_kernel_v2::phases::{
    KernelPhaseError, KernelPhaseFuture, KernelPhaseOutput, KernelPhaseReason, ProviderCallPhase,
    ProviderCallRequest, ProviderCallResult, RuntimePhaseService,
};

const PROVIDER_SETTLEMENT_GRACE_MS: u64 = 2_000;

pub(crate) struct ProductionProviderCallService {
    runtime_state: Arc<GatewayRuntimeState>,
    retention: Arc<ProductionPayloadRetention>,
    provider_authority: RuntimeProviderLaneAuthority,
    callbacks: Arc<dyn ProductionAttemptCallbacks>,
}

impl ProductionProviderCallService {
    pub(crate) fn new(
        runtime_state: Arc<GatewayRuntimeState>,
        retention: Arc<ProductionPayloadRetention>,
        provider_authority: RuntimeProviderLaneAuthority,
        callbacks: Arc<dyn ProductionAttemptCallbacks>,
    ) -> Self {
        Self { runtime_state, retention, provider_authority, callbacks }
    }
}

impl RuntimePhaseService<ProviderCallPhase, ProviderCallRequest, ProviderCallResult>
    for ProductionProviderCallService
{
    fn execute(
        &self,
        input: crate::application::runtime_kernel_v2::phases::ProviderCallPhaseInput,
    ) -> KernelPhaseFuture<
        '_,
        Result<
            crate::application::runtime_kernel_v2::phases::ProviderCallPhaseOutput,
            KernelPhaseError,
        >,
    > {
        Box::pin(async move {
            let lane = input.boundary().execution().lane_authority();
            let run_lease = self.provider_authority.run_lease();
            let provider_lease = self.provider_authority.provider_lease();
            if run_lease.lane != RuntimeGenerationLane::Run
                || run_lease.generation != lane.run_generation()
                || &run_lease.lease_id != lane.run_lease_id()
                || run_lease.session_id.as_str() != lane.session_id().as_str()
                || run_lease.run_id.as_ref().map(|run_id| run_id.as_str())
                    != Some(lane.run_id().as_str())
                || provider_lease.lane != RuntimeGenerationLane::Provider
                || provider_lease.generation != lane.lane_generation()
                || &provider_lease.lease_id != lane.lane_lease_id()
                || provider_lease.session_id.as_str() != lane.session_id().as_str()
                || provider_lease.run_id.as_ref().map(|run_id| run_id.as_str())
                    != Some(lane.run_id().as_str())
            {
                return Err(host_error(
                    &self.retention,
                    "runtime.provider_call.lane_authority_mismatch",
                ));
            }
            let Some(retained) = self.retention.provider_request(&input.payload().provider_request)
            else {
                return Err(host_error(&self.retention, "runtime.provider_call.missing_request"));
            };
            if retained.projection_id != input.payload().context_projection_id {
                return Err(host_error(
                    &self.retention,
                    "runtime.provider_call.context_binding_mismatch",
                ));
            }
            if input.boundary().execution().cancellation().signal().current_reason().is_some() {
                let evidence = self.retention.retain_evidence("runtime.provider_call.cancelled");
                return completed(
                    &input,
                    KernelPhaseReason::ProviderCallFailed,
                    ProviderCallResult::Failed { evidence, output_emitted: false },
                );
            }
            let qa_lane_attestation =
                self.runtime_state.qa_model_provider_lane_attestation(&retained.request);
            let timeout = Duration::from_millis(input.boundary().execution().timeout_ms());
            let runtime_state = Arc::clone(&self.runtime_state);
            let mut provider = tokio::spawn(async move {
                runtime_state
                    .execute_model_provider_with_lease(retained.request, retained.lease)
                    .await
            });
            if self.callbacks.provider_effect_started().await.is_err() {
                let _ = settle_interrupted_provider_call(&mut provider).await;
                return Err(host_error(
                    &self.retention,
                    "runtime.provider_call.start_observation_failed",
                ));
            }
            let deadline = tokio::time::sleep(timeout);
            tokio::pin!(deadline);
            let response = tokio::select! {
                biased;
                _ = input.boundary().execution().cancellation().signal().cancelled() => {
                    let settlement = settle_interrupted_provider_call(&mut provider).await;
                    let evidence = self.retention.retain_evidence_material(
                        "runtime.provider_call.cancelled",
                        settlement.as_bytes(),
                    );
                    if let Some(attestation) = qa_lane_attestation.clone() {
                        self.retention
                            .bind_provider_failure_attestation(&evidence, attestation);
                    }
                    return completed(
                        &input,
                        KernelPhaseReason::ProviderCallFailed,
                        ProviderCallResult::Failed { evidence, output_emitted: false },
                    );
                }
                _ = &mut deadline => {
                    let reason = settle_interrupted_provider_call(&mut provider).await;
                    return Err(host_error(&self.retention, reason));
                }
                response = &mut provider => response,
            };
            let response = match response {
                Ok(Ok(response)) => response,
                Ok(Err(error)) => {
                    let reason = if error.message().contains("class=context_window_exceeded") {
                        "runtime.provider_call.context_window_exceeded"
                    } else {
                        "runtime.provider_call.gateway_failed"
                    };
                    let evidence = self.retention.retain_evidence(reason);
                    if reason == "runtime.provider_call.context_window_exceeded" {
                        if let Some(attestation) = qa_lane_attestation {
                            self.retention
                                .bind_provider_failure_attestation(&evidence, attestation);
                        }
                    }
                    return completed(
                        &input,
                        KernelPhaseReason::ProviderCallFailed,
                        ProviderCallResult::Failed { evidence, output_emitted: false },
                    );
                }
                Err(_) => {
                    return Err(host_error(
                        &self.retention,
                        "runtime.provider_call.gateway_task_failed",
                    ));
                }
            };
            if response.events.len() > input.boundary().execution().backpressure().capacity {
                return Err(host_error(
                    &self.retention,
                    "runtime.provider_call.backpressure_capacity_exceeded",
                ));
            }
            if response.events != provider_events_from_output(&response.output) {
                return Err(host_error(
                    &self.retention,
                    "runtime.provider_call.stream_projection_invalid",
                ));
            }
            let terminal = classify_terminal_outcome(&response.output);
            let output_emitted = terminal.visible_text_bytes > 0;
            let response = self.retention.retain_provider_response(response, terminal);
            completed(
                &input,
                KernelPhaseReason::ProviderCallCompleted,
                ProviderCallResult::Completed { response, output_emitted },
            )
        })
    }
}

async fn settle_interrupted_provider_call(
    provider: &mut tokio::task::JoinHandle<
        Result<crate::model_provider::ProviderResponse, tonic::Status>,
    >,
) -> &'static str {
    match tokio::time::timeout(Duration::from_millis(PROVIDER_SETTLEMENT_GRACE_MS), &mut *provider)
        .await
    {
        Ok(Ok(_)) => "runtime.provider_call.interrupted_after_settlement",
        Ok(Err(_)) => "runtime.provider_call.interrupted_gateway_task_failed",
        Err(_) => {
            provider.abort();
            let _ = provider.await;
            // Aborting drops the gateway's exact attempt-authority guard. The
            // guard synchronously records outcome-unknown or a stale disposition.
            "runtime.provider_call.interrupted_outcome_unknown"
        }
    }
}

fn completed(
    input: &crate::application::runtime_kernel_v2::phases::ProviderCallPhaseInput,
    reason: KernelPhaseReason,
    result: ProviderCallResult,
) -> Result<crate::application::runtime_kernel_v2::phases::ProviderCallPhaseOutput, KernelPhaseError>
{
    Ok(KernelPhaseOutput::from_input(input, reason, result)?)
}

fn host_error(retention: &ProductionPayloadRetention, reason: &'static str) -> KernelPhaseError {
    KernelPhaseError::HostService {
        phase: RuntimeErrorPhase::ProviderCall,
        reason: KernelPhaseReason::ProviderCallFailed,
        evidence: Some(retention.retain_evidence(reason)),
    }
}
