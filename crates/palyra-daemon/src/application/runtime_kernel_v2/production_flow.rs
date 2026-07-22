//! Production flow-control and phase-lazy lane authorities for RuntimeKernelV2.

use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
    time::Duration,
};

use palyra_common::runtime_contracts::{
    BackpressurePolicy, CancellationContextV1, CancellationReason, CancellationScopeKind,
    GenerationLeaseV1, RuntimeErrorPhase, RuntimeGenerationLane, RuntimeIdentitySetV1,
};

use crate::{
    application::run_stream::flow_control::{
        run_stream_response_backpressure_policy, LiveCancellationScope, RunStreamFlowControl,
    },
    gateway::{current_unix_ms, GatewayRuntimeState},
    journal::{runtime_kernel::RuntimeKernelChildLaneAcquireRequest, RuntimeProviderLaneAuthority},
};

use super::{
    context::{
        KernelAuthorityError, KernelBackpressureAuthority, KernelCancellationAuthority,
        KernelDeadlineAuthority, KernelLaneAuthority,
    },
    phases::{
        KernelCancellationScope, KernelCancellationSignal, KernelPhaseFuture, PhaseLaneAuthority,
    },
};

/// Run-owned production implementation of kernel flow and lane capabilities.
pub(crate) struct ProductionKernelFlowAuthorities {
    runtime_state: Arc<GatewayRuntimeState>,
    identities: RuntimeIdentitySetV1,
    run_lease: GenerationLeaseV1,
    provider_authority: RuntimeProviderLaneAuthority,
    flow: RunStreamFlowControl,
    child_leases: Mutex<BTreeMap<RuntimeGenerationLane, GenerationLeaseV1>>,
}

impl ProductionKernelFlowAuthorities {
    /// Binds flow control and all phase-lazy lane acquisition to one exact Run lease.
    #[must_use]
    pub(crate) fn new(
        runtime_state: Arc<GatewayRuntimeState>,
        identities: RuntimeIdentitySetV1,
        run_lease: GenerationLeaseV1,
        provider_authority: RuntimeProviderLaneAuthority,
        flow: RunStreamFlowControl,
    ) -> Self {
        Self {
            runtime_state,
            identities,
            run_lease,
            provider_authority,
            flow,
            child_leases: Mutex::new(BTreeMap::new()),
        }
    }

    fn phase_authority(&self, lease: &GenerationLeaseV1) -> PhaseLaneAuthority {
        PhaseLaneAuthority::from_host_leases(
            self.identities.session_id.clone(),
            self.identities.run_id.clone(),
            self.run_lease.generation,
            self.run_lease.lease_id.clone(),
            lease.lane,
            lease.generation,
            lease.lease_id.clone(),
        )
    }

    /// Returns the exact cached lease used by phase authority for one lane.
    pub(crate) fn lane_lease(
        &self,
        lane: RuntimeGenerationLane,
    ) -> Result<GenerationLeaseV1, KernelAuthorityError> {
        match lane {
            RuntimeGenerationLane::Run => Ok(self.run_lease.clone()),
            RuntimeGenerationLane::Provider => {
                if self.provider_authority.run_lease() != &self.run_lease {
                    return Err(KernelAuthorityError::LaneAuthority);
                }
                Ok(self.provider_authority.provider_lease().clone())
            }
            RuntimeGenerationLane::Harness
            | RuntimeGenerationLane::Tool
            | RuntimeGenerationLane::Delivery => {
                let mut leases =
                    self.child_leases.lock().map_err(|_| KernelAuthorityError::LaneAuthority)?;
                if let Some(lease) = leases.get(&lane) {
                    return Ok(lease.clone());
                }
                let lease = self
                    .runtime_state
                    .journal_store
                    .acquire_runtime_kernel_child_lane(&RuntimeKernelChildLaneAcquireRequest::new(
                        self.identities.clone(),
                        self.run_lease.clone(),
                        lane,
                        format!("phase:{}", lane.as_str()),
                    ))
                    .map_err(|_| KernelAuthorityError::LaneAuthority)?;
                leases.insert(lane, lease.clone());
                Ok(lease)
            }
            _ => Err(KernelAuthorityError::LaneAuthority),
        }
    }
}

impl KernelCancellationAuthority for ProductionKernelFlowAuthorities {
    fn root_context(&self) -> CancellationContextV1 {
        self.flow.root_context().clone()
    }

    fn derive_scope(
        &self,
        scope: CancellationScopeKind,
        timeout_ms: u64,
    ) -> Result<KernelCancellationScope, KernelAuthorityError> {
        let live = if scope == CancellationScopeKind::Run {
            self.flow.live_root()
        } else {
            self.flow
                .live_child(scope, Duration::from_millis(timeout_ms.max(1)))
                .map_err(|_| KernelAuthorityError::Cancellation)?
        };
        let context = live.context().clone();
        KernelCancellationScope::new(context, Arc::new(ProductionCancellationSignal { live }))
            .map_err(|_| KernelAuthorityError::Cancellation)
    }
}

impl KernelDeadlineAuthority for ProductionKernelFlowAuthorities {
    fn timeout_ms(&self, _phase: RuntimeErrorPhase) -> Result<u64, KernelAuthorityError> {
        let root = self.flow.root_context();
        let remaining = root.deadline_unix_ms.map_or(root.hard_abort_after_ms, |deadline| {
            u64::try_from(deadline.saturating_sub(current_unix_ms())).unwrap_or(0)
        });
        (remaining > 0).then_some(remaining).ok_or(KernelAuthorityError::Deadline)
    }
}

impl KernelBackpressureAuthority for ProductionKernelFlowAuthorities {
    fn policy(
        &self,
        _phase: RuntimeErrorPhase,
    ) -> Result<BackpressurePolicy, KernelAuthorityError> {
        run_stream_response_backpressure_policy().map_err(|_| KernelAuthorityError::Backpressure)
    }
}

impl KernelLaneAuthority for ProductionKernelFlowAuthorities {
    fn authority_for(
        &self,
        lane: RuntimeGenerationLane,
    ) -> Result<PhaseLaneAuthority, KernelAuthorityError> {
        let lease = self.lane_lease(lane)?;
        Ok(self.phase_authority(&lease))
    }
}

struct ProductionCancellationSignal {
    live: LiveCancellationScope,
}

impl KernelCancellationSignal for ProductionCancellationSignal {
    fn current_reason(&self) -> Option<CancellationReason> {
        self.live.current_reason()
    }

    fn cancelled(&self) -> KernelPhaseFuture<'_, CancellationReason> {
        let mut live = self.live.clone();
        Box::pin(async move { live.cancelled().await })
    }
}
