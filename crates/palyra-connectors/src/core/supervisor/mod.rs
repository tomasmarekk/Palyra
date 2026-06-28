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
        Self { store, router, adapters, config }
    }

    /// Returns the shared connector store backing this supervisor.
    #[must_use]
    pub fn store(&self) -> &Arc<ConnectorStore> {
        &self.store
    }
}

/// Returns the current unix time in milliseconds, saturating at `i64::MAX`.
pub(super) fn unix_ms_now() -> Result<i64, ConnectorSupervisorError> {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| ConnectorSupervisorError::Clock(error.to_string()))?;
    Ok(now.as_millis().try_into().unwrap_or(i64::MAX))
}
