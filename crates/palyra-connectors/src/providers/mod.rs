//! Provider registry and default adapter inventory for `palyra-connectors`.
//!
//! Provider-owned semantics stay in the corresponding submodule while this
//! module remains the generic place that enumerates built-in providers and
//! publishes their runtime descriptors.

use std::sync::{Arc, OnceLock};

use crate::core::{
    ConnectorAdapter, ConnectorAvailability, ConnectorCapabilitySet, ConnectorInstanceSpec,
    ConnectorKind,
};

pub mod discord;
mod echo;
mod slack;
mod telegram;

pub use echo::EchoConnectorAdapter;
pub use slack::SlackConnectorAdapter;
pub use telegram::TelegramConnectorAdapter;

/// Static runtime metadata published by one built-in provider.
#[derive(Debug, Clone)]
pub struct ConnectorProviderDescriptor {
    /// Provider identity used for registry lookups.
    pub kind: ConnectorKind,
    /// Whether the provider is supported, internal-test-only, or deferred.
    pub availability: ConnectorAvailability,
    /// Per-operation capability surface advertised to the console and policy layers.
    pub capabilities: ConnectorCapabilitySet,
    default_instance_spec: Option<fn() -> ConnectorInstanceSpec>,
}

impl ConnectorProviderDescriptor {
    /// Builds the provider's default instance spec, or `None` when the provider has no
    /// out-of-the-box instance (deferred providers).
    #[must_use]
    pub fn default_instance_spec(&self) -> Option<ConnectorInstanceSpec> {
        self.default_instance_spec.map(|build| build())
    }
}

static PROVIDER_DESCRIPTORS: OnceLock<Vec<ConnectorProviderDescriptor>> = OnceLock::new();

/// Returns the descriptors of all built-in providers in stable runtime order.
#[must_use]
pub fn provider_descriptors() -> &'static [ConnectorProviderDescriptor] {
    PROVIDER_DESCRIPTORS.get_or_init(|| {
        vec![
            echo::provider_descriptor(),
            discord::provider_descriptor(),
            slack::provider_descriptor(),
            telegram::provider_descriptor(),
        ]
    })
}

/// Returns the descriptor for `kind`.
///
/// # Panics
/// Panics if no built-in descriptor exists for `kind`; every [`ConnectorKind`] variant must be
/// registered in [`provider_descriptors`].
#[must_use]
pub fn provider_descriptor(kind: ConnectorKind) -> &'static ConnectorProviderDescriptor {
    provider_descriptors()
        .iter()
        .find(|descriptor| descriptor.kind == kind)
        .unwrap_or_else(|| panic!("missing provider descriptor for kind {}", kind.as_str()))
}

/// Returns the runtime availability advertised by the provider for `kind`.
///
/// # Panics
/// Panics if `kind` has no registered descriptor (see [`provider_descriptor`]).
#[must_use]
pub fn provider_availability(kind: ConnectorKind) -> ConnectorAvailability {
    provider_descriptor(kind).availability
}

/// Returns a clone of the capability set advertised by the provider for `kind`.
///
/// # Panics
/// Panics if `kind` has no registered descriptor (see [`provider_descriptor`]).
#[must_use]
pub fn provider_capabilities(kind: ConnectorKind) -> ConnectorCapabilitySet {
    provider_descriptor(kind).capabilities.clone()
}

/// Builds the default connector instance specs for all providers that ship one.
#[must_use]
pub fn default_instance_specs() -> Vec<ConnectorInstanceSpec> {
    provider_descriptors()
        .iter()
        .filter_map(ConnectorProviderDescriptor::default_instance_spec)
        .collect()
}

/// Constructs the adapters wired into the runtime by default (echo and Discord).
#[must_use]
pub fn default_adapters() -> Vec<Arc<dyn ConnectorAdapter>> {
    vec![
        Arc::new(EchoConnectorAdapter::default()),
        Arc::new(discord::DiscordConnectorAdapter::default()),
    ]
}

#[cfg(test)]
mod tests {
    use crate::protocol::{ConnectorAvailability, ConnectorKind};

    use super::{default_adapters, default_instance_specs, provider_descriptor};

    #[test]
    fn provider_registry_contains_supported_provider_adapters() {
        let adapters = default_adapters();
        let runtime_surface = adapters
            .iter()
            .map(|adapter| (adapter.kind(), adapter.availability()))
            .collect::<Vec<_>>();

        assert_eq!(
            runtime_surface,
            vec![
                (ConnectorKind::Echo, ConnectorAvailability::InternalTestOnly),
                (ConnectorKind::Discord, ConnectorAvailability::Supported),
            ]
        );
    }

    #[test]
    fn provider_registry_exposes_default_inventory_specs_in_runtime_order() {
        let inventory = default_instance_specs()
            .into_iter()
            .map(|spec| (spec.connector_id, provider_descriptor(spec.kind).availability))
            .collect::<Vec<_>>();

        assert_eq!(
            inventory,
            vec![
                ("echo:default".to_owned(), ConnectorAvailability::InternalTestOnly,),
                ("discord:default".to_owned(), ConnectorAvailability::Supported),
            ]
        );
    }

    #[test]
    fn deferred_provider_descriptors_publish_deferred_runtime_metadata() {
        assert_eq!(
            provider_descriptor(ConnectorKind::Slack).availability,
            ConnectorAvailability::Deferred
        );
        assert_eq!(
            provider_descriptor(ConnectorKind::Telegram).availability,
            ConnectorAvailability::Deferred
        );
    }
}
