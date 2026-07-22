//! Registry-sealed harness, context-engine, and provider-route candidates.

use std::fmt;

use palyra_common::runtime_contracts::{RuntimeAuthorityClass, RuntimeInstanceId};
use serde::{Deserialize, Serialize};

use crate::application::{
    agent_harness::AgentHarnessDescriptor, context_engine::ContextEngineDescriptor,
};

use super::{
    bounded::{BoundedVec, SafeLabel},
    digest::{digest_serializable, SelectionDigest},
    policies::MAX_CAPABILITIES_PER_COMPONENT,
    service::RuntimeSelectionError,
};

const MAX_CANDIDATES_PER_COMPONENT: usize = 32;
const CANDIDATE_DESCRIPTOR_DOMAIN: &[u8] = b"palyra.runtime_selection.candidate_descriptor.v1\0";
const CANDIDATE_REGISTRY_DOMAIN: &[u8] = b"palyra.runtime_selection.candidate_registry.v1\0";
const HARNESS_REGISTRY_DESCRIPTOR_DOMAIN: &[u8] =
    b"palyra.runtime_selection.harness_registry_descriptor.v1\0";
const CONTEXT_REGISTRY_DESCRIPTOR_DOMAIN: &[u8] =
    b"palyra.runtime_selection.context_registry_descriptor.v1\0";

/// Harness execution family.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum HarnessKindV1 {
    Embedded,
    External,
}

/// Host-verified harness binding.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct HarnessBindingV1 {
    harness_id: SafeLabel,
    version: SafeLabel,
    kind: HarnessKindV1,
    registry_descriptor_digest: SelectionDigest,
    authority_class: RuntimeAuthorityClass,
}

impl HarnessBindingV1 {
    pub(crate) fn from_registry_descriptor(
        descriptor: &AgentHarnessDescriptor,
        version: SafeLabel,
        authority_class: RuntimeAuthorityClass,
    ) -> Result<Self, RuntimeSelectionError> {
        let expected = digest_serializable(
            HARNESS_REGISTRY_DESCRIPTOR_DOMAIN,
            &(
                descriptor.id.as_str(),
                descriptor.label.as_str(),
                descriptor.embedded_default,
                descriptor.descriptor_hash.as_str(),
            ),
        )?;
        Ok(Self {
            harness_id: SafeLabel::parse(descriptor.id.clone())
                .map_err(|_| RuntimeSelectionError::InvalidCandidateRegistry)?,
            version,
            kind: if descriptor.embedded_default {
                HarnessKindV1::Embedded
            } else {
                HarnessKindV1::External
            },
            registry_descriptor_digest: expected,
            authority_class,
        })
    }
}

/// Host-verified context engine binding.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ContextEngineBindingV1 {
    engine_id: SafeLabel,
    version: SafeLabel,
    registry_descriptor_digest: SelectionDigest,
    projection_epoch: u64,
    authority_class: RuntimeAuthorityClass,
}

impl ContextEngineBindingV1 {
    pub(crate) fn from_registry_descriptor(
        descriptor: &ContextEngineDescriptor,
        projection_epoch: u64,
        authority_class: RuntimeAuthorityClass,
    ) -> Result<Self, RuntimeSelectionError> {
        if projection_epoch == 0 {
            return Err(RuntimeSelectionError::InvalidCandidateRegistry);
        }
        Ok(Self {
            engine_id: SafeLabel::parse(descriptor.engine_id.clone())
                .map_err(|_| RuntimeSelectionError::InvalidCandidateRegistry)?,
            version: SafeLabel::parse(descriptor.version.clone())
                .map_err(|_| RuntimeSelectionError::InvalidCandidateRegistry)?,
            registry_descriptor_digest: digest_serializable(
                CONTEXT_REGISTRY_DESCRIPTOR_DOMAIN,
                descriptor,
            )?,
            projection_epoch,
            authority_class,
        })
    }
}

/// Credential-selection authority associated with a provider route.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum AuthSelectionModeV1 {
    FixedProfile,
    HostPolicy,
    PerRequestDelegated,
}

impl AuthSelectionModeV1 {
    #[cfg(test)]
    const fn rank(self) -> u8 {
        match self {
            Self::FixedProfile => 0,
            Self::HostPolicy => 1,
            Self::PerRequestDelegated => 2,
        }
    }

    #[cfg(test)]
    pub(super) const fn does_not_widen(self, previous: Self) -> bool {
        self.rank() <= previous.rank()
    }
}

/// Dedicated provider-auth policy references.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct AuthCandidatePolicyReferenceV1 {
    mode: AuthSelectionModeV1,
    candidate_set_digest: SelectionDigest,
    policy_digest: SelectionDigest,
}

impl AuthCandidatePolicyReferenceV1 {
    pub(crate) const fn new(
        mode: AuthSelectionModeV1,
        candidate_set_digest: SelectionDigest,
        policy_digest: SelectionDigest,
    ) -> Self {
        Self { mode, candidate_set_digest, policy_digest }
    }

    #[cfg(test)]
    pub(super) const fn mode(&self) -> AuthSelectionModeV1 {
        self.mode
    }

    #[cfg(test)]
    pub(super) const fn policy_digest(&self) -> &SelectionDigest {
        &self.policy_digest
    }
}

/// Explicit topology class for provider routes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ProviderRouteClassV1 {
    Primary,
    Fallback,
}

/// Origin of readiness authority for a runtime candidate.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum RuntimeHealthAuthoritySourceV1 {
    Managed,
    HostResident,
}

/// Provider route binding without credentials.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ProviderRouteBindingV1 {
    route_id: SafeLabel,
    provider_id: SafeLabel,
    model_id: SafeLabel,
    route_class: ProviderRouteClassV1,
    auth_policy: AuthCandidatePolicyReferenceV1,
    authority_class: RuntimeAuthorityClass,
}

impl ProviderRouteBindingV1 {
    pub(crate) const fn new(
        route_id: SafeLabel,
        provider_id: SafeLabel,
        model_id: SafeLabel,
        route_class: ProviderRouteClassV1,
        auth_policy: AuthCandidatePolicyReferenceV1,
        authority_class: RuntimeAuthorityClass,
    ) -> Self {
        Self { route_id, provider_id, model_id, route_class, auth_policy, authority_class }
    }
}

macro_rules! registry_candidate {
    ($name:ident, $binding:ty, $id:ident) => {
        #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
        #[serde(deny_unknown_fields)]
        pub(crate) struct $name {
            binding: $binding,
            health_component_id: RuntimeInstanceId,
            health_authority_source: RuntimeHealthAuthoritySourceV1,
            required_capabilities: BoundedVec<SafeLabel, MAX_CAPABILITIES_PER_COMPONENT>,
            preference_rank: u16,
            descriptor_digest: SelectionDigest,
        }

        impl $name {
            fn new_with_health_source(
                binding: $binding,
                health_component_id: RuntimeInstanceId,
                health_authority_source: RuntimeHealthAuthoritySourceV1,
                mut required_capabilities: Vec<SafeLabel>,
                preference_rank: u16,
            ) -> Result<Self, RuntimeSelectionError> {
                required_capabilities.sort();
                if required_capabilities.windows(2).any(|window| window[0] == window[1]) {
                    return Err(RuntimeSelectionError::InvalidCandidateRegistry);
                }
                let required_capabilities = BoundedVec::try_new(required_capabilities)
                    .map_err(|_| RuntimeSelectionError::InvalidCandidateRegistry)?;
                let descriptor_digest = digest_serializable(
                    CANDIDATE_DESCRIPTOR_DOMAIN,
                    &(
                        &binding,
                        &health_component_id,
                        health_authority_source,
                        &required_capabilities,
                        preference_rank,
                    ),
                )?;
                Ok(Self {
                    binding,
                    health_component_id,
                    health_authority_source,
                    required_capabilities,
                    preference_rank,
                    descriptor_digest,
                })
            }

            pub(super) const fn health_component_id(&self) -> &RuntimeInstanceId {
                &self.health_component_id
            }

            pub(super) const fn health_authority_source(&self) -> RuntimeHealthAuthoritySourceV1 {
                self.health_authority_source
            }

            pub(super) const fn preference_rank(&self) -> u16 {
                self.preference_rank
            }

            pub(super) fn capabilities(&self) -> &[SafeLabel] {
                &self.required_capabilities
            }

            pub(super) const fn id(&self) -> &SafeLabel {
                &self.binding.$id
            }

            pub(super) const fn descriptor_digest(&self) -> &SelectionDigest {
                &self.descriptor_digest
            }

            fn validate_digest(&self) -> Result<(), RuntimeSelectionError> {
                let expected = digest_serializable(
                    CANDIDATE_DESCRIPTOR_DOMAIN,
                    &(
                        &self.binding,
                        &self.health_component_id,
                        self.health_authority_source,
                        &self.required_capabilities,
                        self.preference_rank,
                    ),
                )?;
                if expected != self.descriptor_digest {
                    return Err(RuntimeSelectionError::DigestMismatch);
                }
                Ok(())
            }
        }
    };
}

registry_candidate!(HarnessRegistryCandidateV1, HarnessBindingV1, harness_id);
registry_candidate!(ContextEngineRegistryCandidateV1, ContextEngineBindingV1, engine_id);
registry_candidate!(ProviderRegistryCandidateV1, ProviderRouteBindingV1, route_id);

impl HarnessRegistryCandidateV1 {
    #[cfg(test)]
    pub(crate) fn new(
        binding: HarnessBindingV1,
        health_component_id: RuntimeInstanceId,
        required_capabilities: Vec<SafeLabel>,
        preference_rank: u16,
    ) -> Result<Self, RuntimeSelectionError> {
        Self::new_with_health_source(
            binding,
            health_component_id,
            RuntimeHealthAuthoritySourceV1::Managed,
            required_capabilities,
            preference_rank,
        )
    }

    pub(crate) fn new_host_resident(
        binding: HarnessBindingV1,
        health_component_id: RuntimeInstanceId,
        required_capabilities: Vec<SafeLabel>,
        preference_rank: u16,
    ) -> Result<Self, RuntimeSelectionError> {
        Self::new_with_health_source(
            binding,
            health_component_id,
            RuntimeHealthAuthoritySourceV1::HostResident,
            required_capabilities,
            preference_rank,
        )
    }

    pub(super) const fn authority_class(&self) -> RuntimeAuthorityClass {
        self.binding.authority_class
    }

    pub(super) const fn kind(&self) -> HarnessKindV1 {
        self.binding.kind
    }
}

impl ContextEngineRegistryCandidateV1 {
    #[cfg(test)]
    pub(crate) fn new(
        binding: ContextEngineBindingV1,
        health_component_id: RuntimeInstanceId,
        required_capabilities: Vec<SafeLabel>,
        preference_rank: u16,
    ) -> Result<Self, RuntimeSelectionError> {
        Self::new_with_health_source(
            binding,
            health_component_id,
            RuntimeHealthAuthoritySourceV1::Managed,
            required_capabilities,
            preference_rank,
        )
    }

    pub(crate) fn new_host_resident(
        binding: ContextEngineBindingV1,
        health_component_id: RuntimeInstanceId,
        required_capabilities: Vec<SafeLabel>,
        preference_rank: u16,
    ) -> Result<Self, RuntimeSelectionError> {
        Self::new_with_health_source(
            binding,
            health_component_id,
            RuntimeHealthAuthoritySourceV1::HostResident,
            required_capabilities,
            preference_rank,
        )
    }

    pub(super) const fn authority_class(&self) -> RuntimeAuthorityClass {
        self.binding.authority_class
    }

    pub(super) const fn projection_epoch(&self) -> u64 {
        self.binding.projection_epoch
    }
}

impl ProviderRegistryCandidateV1 {
    pub(crate) fn new(
        binding: ProviderRouteBindingV1,
        health_component_id: RuntimeInstanceId,
        required_capabilities: Vec<SafeLabel>,
        preference_rank: u16,
    ) -> Result<Self, RuntimeSelectionError> {
        Self::new_with_health_source(
            binding,
            health_component_id,
            RuntimeHealthAuthoritySourceV1::Managed,
            required_capabilities,
            preference_rank,
        )
    }

    pub(super) const fn authority_class(&self) -> RuntimeAuthorityClass {
        self.binding.authority_class
    }

    pub(super) const fn route_class(&self) -> ProviderRouteClassV1 {
        self.binding.route_class
    }

    pub(super) const fn auth_policy(&self) -> &AuthCandidatePolicyReferenceV1 {
        &self.binding.auth_policy
    }
}

/// Single-use evidence that descriptors came from one atomic registry read.
pub(crate) struct HostCandidateRegistryProof {
    registry_epoch: u64,
    _private: (),
}

impl fmt::Debug for HostCandidateRegistryProof {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("HostCandidateRegistryProof")
            .field("registry_epoch", &self.registry_epoch)
            .field("issuer_capability", &"[redacted]")
            .finish()
    }
}

impl HostCandidateRegistryProof {
    pub(in crate::application::runtime_kernel_v2) fn from_verified_registries(
        registry_epoch: u64,
    ) -> Result<Self, RuntimeSelectionError> {
        if registry_epoch == 0 {
            return Err(RuntimeSelectionError::InvalidCandidateRegistry);
        }
        Ok(Self { registry_epoch, _private: () })
    }

    #[cfg(test)]
    pub(crate) fn test_only(registry_epoch: u64) -> Self {
        Self { registry_epoch, _private: () }
    }
}

/// Sealed and canonical registry candidate set.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SealedRuntimeCandidateRegistryV1 {
    registry_epoch: u64,
    harnesses: BoundedVec<HarnessRegistryCandidateV1, MAX_CANDIDATES_PER_COMPONENT>,
    context_engines: BoundedVec<ContextEngineRegistryCandidateV1, MAX_CANDIDATES_PER_COMPONENT>,
    provider_routes: BoundedVec<ProviderRegistryCandidateV1, MAX_CANDIDATES_PER_COMPONENT>,
    registry_digest: SelectionDigest,
}

impl SealedRuntimeCandidateRegistryV1 {
    pub(crate) fn seal(
        proof: HostCandidateRegistryProof,
        mut harnesses: Vec<HarnessRegistryCandidateV1>,
        mut context_engines: Vec<ContextEngineRegistryCandidateV1>,
        mut provider_routes: Vec<ProviderRegistryCandidateV1>,
    ) -> Result<Self, RuntimeSelectionError> {
        harnesses.sort_by(|left, right| left.id().cmp(right.id()));
        context_engines.sort_by(|left, right| left.id().cmp(right.id()));
        provider_routes.sort_by(|left, right| left.id().cmp(right.id()));
        let harnesses = BoundedVec::try_new(harnesses)
            .map_err(|_| RuntimeSelectionError::InvalidCandidateRegistry)?;
        let context_engines = BoundedVec::try_new(context_engines)
            .map_err(|_| RuntimeSelectionError::InvalidCandidateRegistry)?;
        let provider_routes = BoundedVec::try_new(provider_routes)
            .map_err(|_| RuntimeSelectionError::InvalidCandidateRegistry)?;
        validate_candidates(&harnesses, &context_engines, &provider_routes)?;
        let registry_digest = digest_serializable(
            CANDIDATE_REGISTRY_DOMAIN,
            &(proof.registry_epoch, &harnesses, &context_engines, &provider_routes),
        )?;
        Ok(Self {
            registry_epoch: proof.registry_epoch,
            harnesses,
            context_engines,
            provider_routes,
            registry_digest,
        })
    }
    #[must_use]
    pub(crate) const fn digest(&self) -> &SelectionDigest {
        &self.registry_digest
    }

    pub(super) fn harnesses(&self) -> &[HarnessRegistryCandidateV1] {
        &self.harnesses
    }

    pub(super) fn context_engines(&self) -> &[ContextEngineRegistryCandidateV1] {
        &self.context_engines
    }

    pub(super) fn provider_routes(&self) -> &[ProviderRegistryCandidateV1] {
        &self.provider_routes
    }
}

fn validate_candidates(
    harnesses: &[HarnessRegistryCandidateV1],
    contexts: &[ContextEngineRegistryCandidateV1],
    providers: &[ProviderRegistryCandidateV1],
) -> Result<(), RuntimeSelectionError> {
    if harnesses.is_empty() || contexts.is_empty() || providers.is_empty() {
        return Err(RuntimeSelectionError::InvalidCandidateRegistry);
    }
    for candidate in harnesses {
        candidate.validate_digest()?;
    }
    for candidate in contexts {
        candidate.validate_digest()?;
    }
    for candidate in providers {
        candidate.validate_digest()?;
    }
    if has_duplicate_ids(harnesses.iter().map(HarnessRegistryCandidateV1::id))
        || has_duplicate_ids(contexts.iter().map(ContextEngineRegistryCandidateV1::id))
        || has_duplicate_ids(providers.iter().map(ProviderRegistryCandidateV1::id))
    {
        return Err(RuntimeSelectionError::InvalidCandidateRegistry);
    }
    let mut health_ids = harnesses
        .iter()
        .map(HarnessRegistryCandidateV1::health_component_id)
        .chain(contexts.iter().map(ContextEngineRegistryCandidateV1::health_component_id))
        .chain(providers.iter().map(ProviderRegistryCandidateV1::health_component_id))
        .collect::<Vec<_>>();
    health_ids.sort();
    if health_ids.windows(2).any(|window| window[0] == window[1]) {
        return Err(RuntimeSelectionError::InvalidCandidateRegistry);
    }
    let primaries = providers
        .iter()
        .filter(|candidate| candidate.route_class() == ProviderRouteClassV1::Primary)
        .collect::<Vec<_>>();
    if primaries.len() != 1
        || primaries[0].preference_rank() != 0
        || providers.iter().any(|candidate| match candidate.route_class() {
            ProviderRouteClassV1::Primary => candidate.preference_rank() != 0,
            ProviderRouteClassV1::Fallback => candidate.preference_rank() == 0,
        })
    {
        return Err(RuntimeSelectionError::InvalidProviderTopology);
    }
    Ok(())
}

fn has_duplicate_ids<'a>(ids: impl Iterator<Item = &'a SafeLabel>) -> bool {
    let mut ids = ids.collect::<Vec<_>>();
    ids.sort();
    ids.windows(2).any(|window| window[0] == window[1])
}
