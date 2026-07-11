//! Connector supervisor: routes inbound events, drains the durable outbox,
//! and exposes admin/status operations over registered provider adapters.
//!
//! The supervisor owns no background tasks itself; the daemon calls the
//! ingest/drain/poll entry points and all durable state lives in
//! [`ConnectorStore`], so every loop is restart-safe.

use std::{
    collections::HashMap,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use palyra_common::qa_fault_injection::{
    QaFaultActivationDirective, QaFaultCheckpoint, QaFaultDirective, QaFaultProbeHandle,
    QaFaultRecoveryClass,
};

use super::{protocol::ConnectorKind, storage::ConnectorStore};

mod admin;
mod inbound;
mod metrics;
mod outbox;
mod types;

#[cfg(test)]
mod tests;

pub use types::{
    ConnectorAdapter, ConnectorAdapterError, ConnectorAdapterSdkDescriptor,
    ConnectorAdapterSdkOperation, ConnectorRouter, ConnectorRouterError, ConnectorSupervisorConfig,
    ConnectorSupervisorError, DeliveryPipelineMode, DrainOutcome, InboundIngestOutcome,
};

/// Orchestrates connector instances: one router, one adapter per
/// [`ConnectorKind`], and a shared durable store.
pub struct ConnectorSupervisor {
    store: Arc<ConnectorStore>,
    router: Arc<dyn ConnectorRouter>,
    adapters: HashMap<ConnectorKind, Arc<dyn ConnectorAdapter>>,
    config: ConnectorSupervisorConfig,
    qa_fault_probe: QaFaultProbeHandle,
    /// Serializes only an activated barrier's adoption, effects, and recovery proof.
    #[cfg(feature = "qa-fault-injection")]
    qa_fault_barrier_adoption_lock: futures::lock::Mutex<()>,
}

impl ConnectorSupervisor {
    /// Builds a supervisor; when several adapters report the same kind, the
    /// last one in `adapters` wins.
    #[must_use]
    pub fn new(
        store: Arc<ConnectorStore>,
        router: Arc<dyn ConnectorRouter>,
        adapters: Vec<Arc<dyn ConnectorAdapter>>,
        config: ConnectorSupervisorConfig,
    ) -> Self {
        let adapters = adapters
            .into_iter()
            .map(|adapter| (adapter.kind(), adapter))
            .collect::<HashMap<_, _>>();
        Self {
            store,
            router,
            adapters,
            config,
            qa_fault_probe: QaFaultProbeHandle::default(),
            #[cfg(feature = "qa-fault-injection")]
            qa_fault_barrier_adoption_lock: futures::lock::Mutex::new(()),
        }
    }

    /// Replaces the disabled probe with an explicit QA-only fault probe.
    ///
    /// This constructor is unavailable unless the non-default
    /// `qa-fault-injection` crate feature is enabled.
    #[cfg(feature = "qa-fault-injection")]
    #[must_use]
    pub fn with_qa_fault_probe(mut self, probe: QaFaultProbeHandle) -> Self {
        self.qa_fault_probe = probe;
        self
    }

    fn record_qa_fault_recovery(
        &self,
        activation_id: &str,
        recovery_class: QaFaultRecoveryClass,
    ) -> Result<(), ConnectorSupervisorError> {
        self.qa_fault_probe.record_recovery(activation_id, recovery_class)?;
        Ok(())
    }

    /// Returns the shared connector store backing this supervisor.
    #[must_use]
    pub fn store(&self) -> &Arc<ConnectorStore> {
        &self.store
    }

    fn qa_fault_checkpoint(
        &self,
        point_id: &str,
        actor: &str,
    ) -> Result<Option<QaFaultActivationDirective>, ConnectorSupervisorError> {
        match self.qa_fault_probe.checkpoint(QaFaultCheckpoint { point_id, actor })? {
            QaFaultDirective::Continue => Ok(None),
            QaFaultDirective::Activate(activation) => Ok(Some(activation)),
        }
    }
}

fn qa_fault_activation_error(activation: QaFaultActivationDirective) -> ConnectorSupervisorError {
    ConnectorSupervisorError::QaFaultActivated {
        activation_id: activation.activation.id,
        point_id: activation.activation.point_id,
        actor: activation.actor,
        action: activation.activation.action,
    }
}

/// Returns the current unix time in milliseconds, saturating at `i64::MAX`.
pub(super) fn unix_ms_now() -> Result<i64, ConnectorSupervisorError> {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| ConnectorSupervisorError::Clock(error.to_string()))?;
    Ok(now.as_millis().try_into().unwrap_or(i64::MAX))
}
