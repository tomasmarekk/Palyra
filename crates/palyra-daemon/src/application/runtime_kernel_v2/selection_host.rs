//! Gateway-owned assembly of sealed production runtime-selection inputs.

use std::collections::BTreeMap;

use palyra_common::runtime_contracts::{RuntimeAuthorityClass, RuntimeInstanceId};
use palyra_model_providers::ProviderRouteCandidateTrace;
use thiserror::Error;

use crate::{
    application::{
        run_admission::PersistedV2AdmissionToken, tool_registry::ModelVisibleToolCatalogSnapshot,
    },
    gateway::GatewayProviderSelectionSnapshot,
    journal::JournalStore,
};

use super::production_services::context_assembly::{
    preassembled_context_engine_descriptor, PreassembledContextBindingError,
    PreassembledContextEngineBinding,
};
use super::runtime_selection::{
    AdmissionSnapshotReferenceV1, AuthCandidatePolicyReferenceV1, AuthSelectionModeV1,
    ContextEngineBindingV1, ContextEngineRegistryCandidateV1, ExecutionProfileBindingV1,
    FallbackPermissionV1, HarnessBindingV1, HarnessRegistryCandidateV1, HostCandidateRegistryProof,
    HostHealthSnapshotProof, HostResidentReadinessProof, HostResidentReadinessV1,
    ImmutableHealthSnapshotV1, MiddlewareChainBindingV1, ProviderRegistryCandidateV1,
    ProviderRouteBindingV1, ProviderRouteClassV1, ResolvedRuntimeSelection,
    RuntimeCapabilityRequirementsV1, RuntimeFallbackPolicyV1, RuntimeSelectionError,
    RuntimeSelectionRequest, RuntimeSelectionService, SafeLabel, SealedRuntimeCandidateRegistryV1,
    SealedToolCatalogSelectionV1, SelectionDigest, SelectionEpochsV1, SessionOverridePolicyV1,
};

/// Exact provider binding selected by the sealed runtime projection.
#[derive(Debug, Clone)]
pub(crate) struct SelectedProductionProviderBinding {
    pub(crate) provider_id: String,
    pub(crate) credential_id: String,
    pub(crate) model_id: String,
}

/// Executable selection plus the raw-free provider binding needed by the host.
pub(crate) struct ProductionRuntimeSelection {
    pub(crate) resolved: ResolvedRuntimeSelection,
    pub(crate) provider: SelectedProductionProviderBinding,
    pub(crate) context: PreassembledContextEngineBinding,
}

/// Fail-closed production selection assembly errors.
#[derive(Debug, Error)]
pub(crate) enum ProductionSelectionError {
    #[error(transparent)]
    Selection(#[from] RuntimeSelectionError),
    #[error("provider routing snapshot has no unique selected chat route")]
    InvalidProviderTopology,
    #[error("embedded harness readiness snapshot is unavailable")]
    EmbeddedHarnessUnavailable,
    #[error("runtime selection chose an unknown provider route")]
    SelectedRouteMissing,
    #[error(transparent)]
    ContextBinding(#[from] PreassembledContextBindingError),
}

/// Consumes persisted admission only after all live host inputs are sealed.
pub(crate) fn select_production_runtime(
    journal: &JournalStore,
    admission: PersistedV2AdmissionToken,
    gateway: &GatewayProviderSelectionSnapshot,
    tool_catalog: &ModelVisibleToolCatalogSnapshot,
) -> Result<ProductionRuntimeSelection, ProductionSelectionError> {
    let epoch = gateway.configuration_epoch.get();
    let authority_ceiling = RuntimeAuthorityClass::PrivilegedMutation;
    let harness_descriptor = gateway
        .embedded_harness_descriptors
        .iter()
        .find(|descriptor| descriptor.embedded_default)
        .ok_or(ProductionSelectionError::EmbeddedHarnessUnavailable)?;
    let context_descriptor = preassembled_context_engine_descriptor();

    let harness_health_id = runtime_instance(format!("host:harness:{}", harness_descriptor.id))?;
    let context_health_id =
        runtime_instance(format!("host:context:{}", context_descriptor.engine_id))?;
    let harness_binding = HarnessBindingV1::from_registry_descriptor(
        harness_descriptor,
        label(gateway.build_version.clone())?,
        RuntimeAuthorityClass::ScopedMutation,
    )?;
    let context_binding = ContextEngineBindingV1::from_registry_descriptor(
        &context_descriptor,
        epoch,
        RuntimeAuthorityClass::ScopedMutation,
    )?;
    let harnesses = vec![HarnessRegistryCandidateV1::new_host_resident(
        harness_binding,
        harness_health_id.clone(),
        Vec::new(),
        0,
    )?];
    let contexts = vec![ContextEngineRegistryCandidateV1::new_host_resident(
        context_binding,
        context_health_id.clone(),
        Vec::new(),
        0,
    )?];

    let selected_count = gateway
        .status
        .route_selection
        .candidates
        .iter()
        .filter(|candidate| candidate.role == "chat" && candidate.selected)
        .count();
    if selected_count != 1 {
        return Err(ProductionSelectionError::InvalidProviderTopology);
    }
    let policy_digest = SelectionDigest::from_domain_bytes(
        b"palyra.runtime_selection.provider_policy.live.v1\0",
        serde_json::to_vec(&gateway.status.route_selection)
            .map_err(|_| RuntimeSelectionError::Serialization)?
            .as_slice(),
    );
    let mut provider_routes = Vec::new();
    let mut route_bindings = BTreeMap::new();
    for (rank, candidate) in gateway
        .status
        .route_selection
        .candidates
        .iter()
        .filter(|candidate| candidate.role == "chat")
        .enumerate()
    {
        let Some(health) = gateway.health_authority_by_provider.get(&candidate.provider_id) else {
            continue;
        };
        let route_id = format!("route:{rank}:{}:{}", candidate.provider_id, candidate.model_id);
        let route_label = label(route_id.clone())?;
        let auth_candidates = SelectionDigest::from_domain_bytes(
            b"palyra.runtime_selection.auth_candidates.live.v1\0",
            candidate.credential_id.as_bytes(),
        );
        let binding = ProviderRouteBindingV1::new(
            route_label,
            label(candidate.provider_id.clone())?,
            label(candidate.model_id.clone())?,
            if candidate.selected {
                ProviderRouteClassV1::Primary
            } else {
                ProviderRouteClassV1::Fallback
            },
            AuthCandidatePolicyReferenceV1::new(
                AuthSelectionModeV1::HostPolicy,
                auth_candidates,
                policy_digest.clone(),
            ),
            RuntimeAuthorityClass::PrivilegedMutation,
        );
        provider_routes.push(ProviderRegistryCandidateV1::new(
            binding,
            health.component_id.clone(),
            Vec::new(),
            u16::try_from(rank).map_err(|_| RuntimeSelectionError::InvalidCandidateRegistry)?,
        )?);
        route_bindings.insert(route_id, candidate.clone());
    }
    if provider_routes.is_empty() {
        return Err(ProductionSelectionError::InvalidProviderTopology);
    }

    let candidates = SealedRuntimeCandidateRegistryV1::seal(
        HostCandidateRegistryProof::from_verified_registries(epoch)?,
        harnesses,
        contexts,
        provider_routes,
    )?;
    let readiness = vec![
        HostResidentReadinessV1::new(
            harness_health_id,
            epoch,
            SelectionDigest::from_domain_bytes(
                b"palyra.runtime_selection.host_harness.live.v1\0",
                harness_descriptor.descriptor_hash.as_bytes(),
            ),
            RuntimeAuthorityClass::ScopedMutation,
            true,
            label("runtime.host_resident.harness_ready".to_owned())?,
            gateway.observed_at_unix_ms,
        )?,
        HostResidentReadinessV1::new(
            context_health_id,
            epoch,
            SelectionDigest::from_domain_bytes(
                b"palyra.runtime_selection.host_context.live.v1\0",
                serde_json::to_vec(&context_descriptor)
                    .map_err(|_| RuntimeSelectionError::Serialization)?
                    .as_slice(),
            ),
            RuntimeAuthorityClass::ScopedMutation,
            true,
            label("runtime.host_resident.context_ready".to_owned())?,
            gateway.observed_at_unix_ms,
        )?,
    ];
    let health = ImmutableHealthSnapshotV1::capture_with_host_resident(
        HostHealthSnapshotProof::from_verified_registry(epoch)?,
        HostResidentReadinessProof::from_gateway_epoch(epoch)?,
        gateway.observed_at_unix_ms,
        gateway.health_records.clone(),
        readiness,
    )?;
    let catalog = SealedToolCatalogSelectionV1::from_registry_snapshot(tool_catalog, epoch)?;
    let admission_reference = AdmissionSnapshotReferenceV1::new(
        label(format!("admission:{}", admission.run_id()))?,
        SelectionDigest::parse(admission.admission_snapshot_sha256().to_owned())?,
        admission.identities().generation,
        authority_ceiling,
    )?;
    let request = RuntimeSelectionRequest::new(
        admission_reference,
        SessionOverridePolicyV1::deny_all(authority_ceiling)?,
        RuntimeCapabilityRequirementsV1::new(Vec::new(), Vec::new(), Vec::new(), Vec::new())?,
        RuntimeFallbackPolicyV1::new(
            FallbackPermissionV1::Forbidden,
            FallbackPermissionV1::BeforeProgress,
        )?,
        candidates,
        health,
        catalog,
        MiddlewareChainBindingV1::new(vec![
            label("safety".to_owned())?,
            label("policy".to_owned())?,
            label("approval".to_owned())?,
            label("sandbox".to_owned())?,
        ])?,
        ExecutionProfileBindingV1::new(label("production".to_owned())?, authority_ceiling)?,
        SelectionEpochsV1::new(epoch, epoch)?,
    );
    let resolved =
        RuntimeSelectionService::select_persisted_admission(journal, admission, &request)?;
    let context = PreassembledContextEngineBinding::from_selection(resolved.projection(), epoch)?;
    let route = route_bindings
        .get(resolved.projection().selected_provider_route_id())
        .ok_or(ProductionSelectionError::SelectedRouteMissing)?;
    Ok(ProductionRuntimeSelection { resolved, provider: provider_binding(route), context })
}

fn provider_binding(candidate: &ProviderRouteCandidateTrace) -> SelectedProductionProviderBinding {
    SelectedProductionProviderBinding {
        provider_id: candidate.provider_id.clone(),
        credential_id: candidate.credential_id.clone(),
        model_id: candidate.model_id.clone(),
    }
}

fn label(value: String) -> Result<SafeLabel, RuntimeSelectionError> {
    SafeLabel::parse(value).map_err(|_| RuntimeSelectionError::InvalidCandidateRegistry)
}

fn runtime_instance(value: String) -> Result<RuntimeInstanceId, RuntimeSelectionError> {
    RuntimeInstanceId::parse(value.as_str())
        .map_err(|_| RuntimeSelectionError::InvalidCandidateRegistry)
}
