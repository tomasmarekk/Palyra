//! Host-owned runtime component selection for one RuntimeKernelV2 generation.
//!
//! Durable projections are content-addressed evidence, not executable authority.
//! Only a consumed host proof for an active persisted admission can issue a grant.

mod authority;
mod bounded;
mod candidates;
mod catalog;
mod digest;
mod health;
mod policies;
mod projection;
mod service;

#[cfg(test)]
pub(crate) use authority::HostRuntimeSelectionAuthorityProof;
#[cfg(test)]
pub(crate) use authority::HostRuntimeSelectionAuthorityProof as TestHostRuntimeSelectionAuthorityProof;
pub(crate) use authority::{
    AuthoritativeRuntimeGrant, HostVerifiedRunAdmission, HostVerifiedSessionAuthorityMigration,
    ResolvedRuntimeSelection,
};
pub(crate) use bounded::SafeLabel;
pub(crate) use candidates::{
    AuthCandidatePolicyReferenceV1, AuthSelectionModeV1, ContextEngineBindingV1,
    ContextEngineRegistryCandidateV1, HarnessBindingV1, HarnessKindV1, HarnessRegistryCandidateV1,
    HostCandidateRegistryProof, ProviderRegistryCandidateV1, ProviderRouteBindingV1,
    ProviderRouteClassV1, SealedRuntimeCandidateRegistryV1,
};
pub(crate) use catalog::SealedToolCatalogSelectionV1;
pub(crate) use digest::SelectionDigest;
pub(crate) use health::{
    HostHealthSnapshotProof, HostResidentReadinessProof, HostResidentReadinessV1,
    ImmutableHealthSnapshotV1,
};
pub(crate) use policies::SelectionEpochsV1;
pub(crate) use policies::{
    FallbackPermissionV1, RuntimeCapabilityRequirementsV1, RuntimeFallbackPolicyV1,
    SessionOverridePolicyV1,
};
pub(crate) use projection::RuntimeSelectionV1;
pub(crate) use service::RuntimeSelectionError;
pub(crate) use service::{
    AdmissionSnapshotReferenceV1, ExecutionProfileBindingV1, MiddlewareChainBindingV1,
    RuntimeSelectionRequest, RuntimeSelectionService,
};

#[cfg(test)]
mod tests;
