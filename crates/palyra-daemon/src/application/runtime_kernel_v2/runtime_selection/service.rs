//! Deterministic runtime selection and fenced one-component recovery.

use palyra_common::runtime_contracts::{
    RuntimeAuthorityClass, RuntimeGeneration, RuntimeHealthState, RuntimeInstanceId,
};
use serde::{Deserialize, Serialize};

use crate::{application::run_admission::PersistedV2AdmissionToken, journal::JournalStore};

use super::{
    authority::{
        AuthoritativeRuntimeGrant, HostRuntimeSelectionAuthorityProof, ResolvedRuntimeSelection,
    },
    bounded::{BoundedVec, SafeLabel},
    candidates::{
        ContextEngineRegistryCandidateV1, HarnessRegistryCandidateV1, ProviderRegistryCandidateV1,
        ProviderRouteClassV1, SealedRuntimeCandidateRegistryV1,
    },
    catalog::{SealedToolCatalogSelectionV1, ToolCatalogSelectionProjectionV1},
    digest::{digest_serializable, SelectionDigest},
    health::ImmutableHealthSnapshotV1,
    policies::{
        FallbackPermissionV1, RuntimeCapabilityRequirementsV1, RuntimeFallbackPolicyV1,
        SelectionEpochsV1, SessionOverridePolicyV1,
    },
    projection::{
        CandidateAvailabilityV1, CandidateCompatibilityV1, CandidateSelectedReasonV1,
        CandidateUnavailableReasonV1, ProviderRequestEpochGuardV1,
        RuntimeFallbackTransitionEvidenceV1, RuntimeSelectionEvidenceV1, RuntimeSelectionV1,
        SelectedContextEngineV1, SelectedHarnessV1, SelectedProviderRouteV1,
        SelectionCandidateReport, SelectionComponentKindV1,
    },
};

#[cfg(test)]
use super::authority::{
    FallbackFailure, FallbackFailureCause, RuntimeFallbackTriggerV1, RuntimeSelectionProgressV1,
};
#[cfg(test)]
use super::candidates::HarnessKindV1;

const MAX_MIDDLEWARE_STAGES: usize = 32;
const ADMISSION_REFERENCE_DOMAIN: &[u8] = b"palyra.runtime_selection.admission_reference.v1\0";
const MIDDLEWARE_CHAIN_DOMAIN: &[u8] = b"palyra.runtime_selection.middleware_chain.v1\0";
const EXECUTION_PROFILE_DOMAIN: &[u8] = b"palyra.runtime_selection.execution_profile.v1\0";

/// Runtime-selection contract failures.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub(crate) enum RuntimeSelectionError {
    #[error("canonical serialization failed")]
    Serialization,
    #[error("digest is not canonical lowercase SHA-256")]
    InvalidDigest,
    #[error("canonical digest mismatch")]
    DigestMismatch,
    #[error("invalid health snapshot")]
    InvalidHealthSnapshot,
    #[error("invalid candidate registry")]
    InvalidCandidateRegistry,
    #[error("provider topology must contain exactly one primary and explicit fallbacks")]
    InvalidProviderTopology,
    #[error("invalid model-visible tool catalog")]
    InvalidToolCatalog,
    #[error("invalid session override policy")]
    InvalidOverridePolicy,
    #[error("invalid capability requirements")]
    InvalidCapabilityRequirements,
    #[error("selection would widen authority")]
    AuthorityEscalation,
    #[error("host authority proof does not match the persisted admission")]
    AuthorityProofMismatch,
    #[error("runtime authority decision did not select an implementation")]
    NoRuntimeAuthority,
    #[error("selection epochs are invalid")]
    InvalidEpochs,
    #[error("component projection epoch is invalid")]
    InvalidEpoch,
    #[error("candidate report is internally inconsistent")]
    InvalidCandidateReport,
    #[error("durable selection projection is invalid")]
    InvalidProjection,
    #[error("recovery reason is illegal for the changed component")]
    IllegalRecoveryReason,
    #[error("required tools are absent from the exact catalog snapshot")]
    RequiredToolMissing,
    #[error("no compatible candidate is available for {0:?}")]
    NoCandidate(SelectionComponentKindV1),
}

/// Durable runtime-admission snapshot reference.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct AdmissionSnapshotReferenceV1 {
    snapshot_id: SafeLabel,
    snapshot_digest: SelectionDigest,
    generation: RuntimeGeneration,
    authority_ceiling: RuntimeAuthorityClass,
    reference_digest: SelectionDigest,
}

#[derive(Serialize)]
struct AdmissionReferencePayload<'a> {
    snapshot_id: &'a SafeLabel,
    snapshot_digest: &'a SelectionDigest,
    generation: RuntimeGeneration,
    authority_ceiling: RuntimeAuthorityClass,
}

impl AdmissionSnapshotReferenceV1 {
    pub(crate) fn new(
        snapshot_id: SafeLabel,
        snapshot_digest: SelectionDigest,
        generation: RuntimeGeneration,
        authority_ceiling: RuntimeAuthorityClass,
    ) -> Result<Self, RuntimeSelectionError> {
        let reference_digest = digest_serializable(
            ADMISSION_REFERENCE_DOMAIN,
            &AdmissionReferencePayload {
                snapshot_id: &snapshot_id,
                snapshot_digest: &snapshot_digest,
                generation,
                authority_ceiling,
            },
        )?;
        Ok(Self { snapshot_id, snapshot_digest, generation, authority_ceiling, reference_digest })
    }

    fn validate(&self) -> Result<(), RuntimeSelectionError> {
        if self.reference_digest
            != digest_serializable(
                ADMISSION_REFERENCE_DOMAIN,
                &AdmissionReferencePayload {
                    snapshot_id: &self.snapshot_id,
                    snapshot_digest: &self.snapshot_digest,
                    generation: self.generation,
                    authority_ceiling: self.authority_ceiling,
                },
            )?
        {
            return Err(RuntimeSelectionError::DigestMismatch);
        }
        Ok(())
    }
}

/// Bounded middleware chain selected from resolved host configuration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct MiddlewareChainBindingV1 {
    stage_ids: BoundedVec<SafeLabel, MAX_MIDDLEWARE_STAGES>,
    chain_digest: SelectionDigest,
}

impl MiddlewareChainBindingV1 {
    pub(crate) fn new(mut stage_ids: Vec<SafeLabel>) -> Result<Self, RuntimeSelectionError> {
        if stage_ids.is_empty() {
            return Err(RuntimeSelectionError::InvalidProjection);
        }
        stage_ids.sort();
        if stage_ids.windows(2).any(|window| window[0] == window[1]) {
            return Err(RuntimeSelectionError::InvalidProjection);
        }
        let stage_ids =
            BoundedVec::try_new(stage_ids).map_err(|_| RuntimeSelectionError::InvalidProjection)?;
        Ok(Self {
            chain_digest: digest_serializable(MIDDLEWARE_CHAIN_DOMAIN, &stage_ids)?,
            stage_ids,
        })
    }

    fn validate(&self) -> Result<(), RuntimeSelectionError> {
        if self.stage_ids.is_empty()
            || self.stage_ids.windows(2).any(|window| window[0] >= window[1])
            || self.chain_digest != digest_serializable(MIDDLEWARE_CHAIN_DOMAIN, &self.stage_ids)?
        {
            return Err(RuntimeSelectionError::DigestMismatch);
        }
        Ok(())
    }
}

/// Resolved execution profile with an explicit authority ceiling.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ExecutionProfileBindingV1 {
    profile_id: SafeLabel,
    authority_class: RuntimeAuthorityClass,
    profile_digest: SelectionDigest,
}

impl ExecutionProfileBindingV1 {
    pub(crate) fn new(
        profile_id: SafeLabel,
        authority_class: RuntimeAuthorityClass,
    ) -> Result<Self, RuntimeSelectionError> {
        Ok(Self {
            profile_digest: digest_serializable(
                EXECUTION_PROFILE_DOMAIN,
                &(&profile_id, authority_class),
            )?,
            profile_id,
            authority_class,
        })
    }

    fn validate(&self) -> Result<(), RuntimeSelectionError> {
        if self.profile_digest
            != digest_serializable(
                EXECUTION_PROFILE_DOMAIN,
                &(&self.profile_id, self.authority_class),
            )?
        {
            return Err(RuntimeSelectionError::DigestMismatch);
        }
        Ok(())
    }
}

/// Complete sealed input set for one deterministic selection.
#[derive(Debug)]
pub(crate) struct RuntimeSelectionRequest {
    pub(crate) admission_snapshot: AdmissionSnapshotReferenceV1,
    pub(crate) override_policy: SessionOverridePolicyV1,
    pub(crate) capability_requirements: RuntimeCapabilityRequirementsV1,
    pub(crate) fallback_policy: RuntimeFallbackPolicyV1,
    pub(crate) candidates: SealedRuntimeCandidateRegistryV1,
    pub(crate) health: ImmutableHealthSnapshotV1,
    pub(crate) tool_catalog: SealedToolCatalogSelectionV1,
    pub(crate) middleware_chain: MiddlewareChainBindingV1,
    pub(crate) execution_profile: ExecutionProfileBindingV1,
    pub(crate) epochs: SelectionEpochsV1,
}

impl RuntimeSelectionRequest {
    /// Collects one complete set of already verified host selection inputs.
    ///
    /// Registry and health capabilities must be consumed before this boundary;
    /// this constructor does not synthesize candidates, component health, or
    /// epochs.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        admission_snapshot: AdmissionSnapshotReferenceV1,
        override_policy: SessionOverridePolicyV1,
        capability_requirements: RuntimeCapabilityRequirementsV1,
        fallback_policy: RuntimeFallbackPolicyV1,
        candidates: SealedRuntimeCandidateRegistryV1,
        health: ImmutableHealthSnapshotV1,
        tool_catalog: SealedToolCatalogSelectionV1,
        middleware_chain: MiddlewareChainBindingV1,
        execution_profile: ExecutionProfileBindingV1,
        epochs: SelectionEpochsV1,
    ) -> Self {
        Self {
            admission_snapshot,
            override_policy,
            capability_requirements,
            fallback_policy,
            candidates,
            health,
            tool_catalog,
            middleware_chain,
            execution_profile,
            epochs,
        }
    }
}

/// Stateless deterministic selector.
#[derive(Debug, Default)]
pub(crate) struct RuntimeSelectionService;

impl RuntimeSelectionService {
    /// Resolves one V2 selection from exact committed admission authority.
    ///
    /// Consuming the non-cloneable admission token rechecks the journaled
    /// session pin and active Run lease before executable authority is issued.
    pub(crate) fn select_persisted_admission(
        journal: &JournalStore,
        admission: PersistedV2AdmissionToken,
        request: &RuntimeSelectionRequest,
    ) -> Result<ResolvedRuntimeSelection, RuntimeSelectionError> {
        let proof = HostRuntimeSelectionAuthorityProof::from_persisted_v2_admission(
            journal,
            admission,
            request.epochs.clone(),
        )?;
        Self::select(proof, request)
    }

    pub(crate) fn select(
        proof: HostRuntimeSelectionAuthorityProof,
        request: &RuntimeSelectionRequest,
    ) -> Result<ResolvedRuntimeSelection, RuntimeSelectionError> {
        validate_request(&proof, request)?;
        let components = select_components(request, None)?;
        let projection = build_projection(&proof, request, components, None)?;
        let grant = AuthoritativeRuntimeGrant::issue(proof, projection.selection_digest().clone());
        Ok(ResolvedRuntimeSelection::new(projection, grant))
    }

    /// Attempts one fenced component replacement and always returns the exact
    /// unchanged prior resolution on failure.
    #[cfg(test)]
    pub(crate) fn select_fallback(
        proof: HostRuntimeSelectionAuthorityProof,
        request: &RuntimeSelectionRequest,
        prior: ResolvedRuntimeSelection,
        trigger: RuntimeFallbackTriggerV1,
        progress: RuntimeSelectionProgressV1,
    ) -> Result<ResolvedRuntimeSelection, Box<FallbackFailure>> {
        let outcome =
            validate_fallback(&proof, request, &prior, &trigger, progress).and_then(|changed| {
                let previous = prior.projection();
                let exclusions = FallbackExclusions {
                    changed,
                    harness_id: (changed == SelectionComponentKindV1::Harness)
                        .then_some(&previous.harness().harness_id),
                    provider_id: (changed == SelectionComponentKindV1::ProviderRoute)
                        .then_some(&previous.provider_route().route_id),
                };
                let components = select_components(request, Some(exclusions))?;
                validate_single_component_change(previous, &components, changed)?;
                let transition = RuntimeFallbackTransitionEvidenceV1::new(
                    previous.selection_digest().clone(),
                    trigger,
                    progress,
                    changed,
                )?;
                build_projection(&proof, request, components, Some(transition))
                    .map_err(FallbackFailureCause::from)
            });

        match outcome {
            Ok(projection) => {
                let grant =
                    AuthoritativeRuntimeGrant::issue(proof, projection.selection_digest().clone());
                Ok(ResolvedRuntimeSelection::new(projection, grant))
            }
            Err(cause) => Err(Box::new(FallbackFailure::new(cause, prior))),
        }
    }
}

#[derive(Clone, Copy)]
struct FallbackExclusions<'a> {
    changed: SelectionComponentKindV1,
    harness_id: Option<&'a SafeLabel>,
    provider_id: Option<&'a SafeLabel>,
}

struct SelectedComponents {
    harness: SelectedHarnessV1,
    context: SelectedContextEngineV1,
    provider: SelectedProviderRouteV1,
    reports: Vec<SelectionCandidateReport>,
}

fn validate_request(
    proof: &HostRuntimeSelectionAuthorityProof,
    request: &RuntimeSelectionRequest,
) -> Result<(), RuntimeSelectionError> {
    request.admission_snapshot.validate()?;
    request.override_policy.validate(request.admission_snapshot.authority_ceiling)?;
    request.capability_requirements.validate()?;
    request.fallback_policy.validate()?;
    request.middleware_chain.validate()?;
    request.execution_profile.validate()?;
    request.epochs.validate()?;
    request.health.validate_for_selection()?;
    if proof.decision().selected_runtime().is_none() {
        return Err(RuntimeSelectionError::NoRuntimeAuthority);
    }
    if request.admission_snapshot.generation != proof.identities().generation
        || request.admission_snapshot.snapshot_digest != *proof.admission_snapshot_digest()
        || request.epochs.digest() != proof.epochs().digest()
        || request.candidates.digest().as_str().is_empty()
        || request.tool_catalog.catalog_epoch() == 0
        || request.health.registry_epoch() == 0
    {
        return Err(RuntimeSelectionError::AuthorityProofMismatch);
    }
    if !request
        .admission_snapshot
        .authority_ceiling
        .permits_fallback(request.execution_profile.authority_class)
    {
        return Err(RuntimeSelectionError::AuthorityEscalation);
    }
    if !request.tool_catalog.satisfies_tools(request.capability_requirements.required_tool_names())
    {
        return Err(RuntimeSelectionError::RequiredToolMissing);
    }
    Ok(())
}

#[cfg(test)]
fn validate_fallback(
    proof: &HostRuntimeSelectionAuthorityProof,
    request: &RuntimeSelectionRequest,
    prior: &ResolvedRuntimeSelection,
    trigger: &RuntimeFallbackTriggerV1,
    progress: RuntimeSelectionProgressV1,
) -> Result<SelectionComponentKindV1, FallbackFailureCause> {
    if progress.blocks_fallback() {
        return Err(FallbackFailureCause::ProgressFence);
    }
    if !prior.grant().matches_proof(proof)
        || prior.grant().selection_digest() != prior.projection().selection_digest()
    {
        return Err(FallbackFailureCause::AuthorityMismatch);
    }
    validate_request(proof, request).map_err(FallbackFailureCause::Selection)?;
    let evidence = prior.projection().evidence();
    if evidence.admission_snapshot_digest != request.admission_snapshot.snapshot_digest
        || evidence.persisted_admission_token_digest != *proof.persisted_token_digest()
        || evidence.fallback_policy_digest != *request.fallback_policy.digest()
        || evidence.override_policy_digest != *request.override_policy.digest()
        || evidence.capability_requirements_digest != *request.capability_requirements.digest()
        || evidence.selection_epochs_digest != *request.epochs.digest()
        || evidence.candidate_registry_digest != *request.candidates.digest()
        || evidence.tool_catalog_snapshot_digest != *request.tool_catalog.snapshot_digest()
        || evidence.tool_catalog_hash != *request.tool_catalog.catalog_hash()
        || evidence.middleware_chain_digest != request.middleware_chain.chain_digest
        || evidence.execution_profile_digest != request.execution_profile.profile_digest
        || prior.projection().selected_profile() != proof.decision().profile()
    {
        return Err(FallbackFailureCause::InputDrift);
    }
    match trigger {
        RuntimeFallbackTriggerV1::HarnessUnavailable { .. } => {
            if request.fallback_policy.external_to_embedded()
                != FallbackPermissionV1::BeforeProgress
                || prior.projection().harness().kind != HarnessKindV1::External
            {
                return Err(FallbackFailureCause::Forbidden);
            }
            Ok(SelectionComponentKindV1::Harness)
        }
        RuntimeFallbackTriggerV1::ProviderRouteUnavailable { .. } => {
            if request.fallback_policy.provider_route() != FallbackPermissionV1::BeforeProgress {
                return Err(FallbackFailureCause::Forbidden);
            }
            Ok(SelectionComponentKindV1::ProviderRoute)
        }
    }
}

fn select_components(
    request: &RuntimeSelectionRequest,
    exclusions: Option<FallbackExclusions<'_>>,
) -> Result<SelectedComponents, RuntimeSelectionError> {
    let harness_recovery =
        exclusions.is_some_and(|value| value.changed == SelectionComponentKindV1::Harness);
    let provider_recovery =
        exclusions.is_some_and(|value| value.changed == SelectionComponentKindV1::ProviderRoute);
    let harness_reason = if harness_recovery {
        CandidateSelectedReasonV1::ExternalHarnessRecovery
    } else if request
        .override_policy
        .requested()
        .and_then(|value| value.harness_id.as_ref())
        .is_some()
    {
        CandidateSelectedReasonV1::SessionOverride
    } else {
        CandidateSelectedReasonV1::PreferredAvailable
    };
    let context_reason = if request
        .override_policy
        .requested()
        .and_then(|value| value.context_engine_id.as_ref())
        .is_some()
    {
        CandidateSelectedReasonV1::SessionOverride
    } else {
        CandidateSelectedReasonV1::PreferredAvailable
    };
    let provider_reason = if provider_recovery {
        CandidateSelectedReasonV1::ProviderRouteRecovery
    } else if request
        .override_policy
        .requested()
        .and_then(|value| value.provider_route_reference_sha256.as_ref())
        .is_some()
    {
        CandidateSelectedReasonV1::SessionOverride
    } else {
        CandidateSelectedReasonV1::PreferredAvailable
    };

    let harness = choose_harness(request, exclusions.and_then(|value| value.harness_id))?;
    let context = choose_context(request)?;
    let provider = choose_provider(request, exclusions.and_then(|value| value.provider_id))?;
    let mut reports = Vec::new();
    reports.extend(harness_reports(request, harness.id(), harness_reason)?);
    reports.extend(context_reports(request, context.id(), context_reason)?);
    reports.extend(provider_reports(request, provider.id(), provider_reason)?);
    Ok(SelectedComponents {
        harness: SelectedHarnessV1 {
            harness_id: harness.id().clone(),
            kind: harness.kind(),
            authority_class: harness.authority_class(),
            reason: harness_reason,
        },
        context: SelectedContextEngineV1 {
            engine_id: context.id().clone(),
            projection_epoch: context.projection_epoch(),
            authority_class: context.authority_class(),
            reason: context_reason,
        },
        provider: SelectedProviderRouteV1 {
            route_id: provider.id().clone(),
            route_class: provider.route_class(),
            authority_class: provider.authority_class(),
            auth_policy: provider.auth_policy().clone(),
            reason: provider_reason,
        },
        reports,
    })
}

fn choose_harness<'a>(
    request: &'a RuntimeSelectionRequest,
    excluded: Option<&SafeLabel>,
) -> Result<&'a HarnessRegistryCandidateV1, RuntimeSelectionError> {
    let requested = excluded
        .is_none()
        .then(|| request.override_policy.requested().and_then(|value| value.harness_id.as_ref()))
        .flatten();
    request
        .candidates
        .harnesses()
        .iter()
        .filter(|candidate| excluded != Some(candidate.id()))
        .filter(|candidate| requested.is_none_or(|id| candidate.id() == id))
        .filter(|candidate| {
            candidate_compatible(
                candidate.capabilities(),
                request.capability_requirements.harness(),
            ) && request
                .health
                .is_available(candidate.health_authority_source(), candidate.health_component_id())
                && request
                    .admission_snapshot
                    .authority_ceiling
                    .permits_fallback(candidate.authority_class())
        })
        .min_by(|left, right| {
            left.preference_rank()
                .cmp(&right.preference_rank())
                .then_with(|| left.id().cmp(right.id()))
        })
        .ok_or(RuntimeSelectionError::NoCandidate(SelectionComponentKindV1::Harness))
}

fn choose_context(
    request: &RuntimeSelectionRequest,
) -> Result<&ContextEngineRegistryCandidateV1, RuntimeSelectionError> {
    let requested =
        request.override_policy.requested().and_then(|value| value.context_engine_id.as_ref());
    request
        .candidates
        .context_engines()
        .iter()
        .filter(|candidate| requested.is_none_or(|id| candidate.id() == id))
        .filter(|candidate| {
            candidate_compatible(
                candidate.capabilities(),
                request.capability_requirements.context_engine(),
            ) && request
                .health
                .is_available(candidate.health_authority_source(), candidate.health_component_id())
                && request
                    .admission_snapshot
                    .authority_ceiling
                    .permits_fallback(candidate.authority_class())
        })
        .min_by(|left, right| {
            left.preference_rank()
                .cmp(&right.preference_rank())
                .then_with(|| left.id().cmp(right.id()))
        })
        .ok_or(RuntimeSelectionError::NoCandidate(SelectionComponentKindV1::ContextEngine))
}

fn choose_provider<'a>(
    request: &'a RuntimeSelectionRequest,
    excluded: Option<&SafeLabel>,
) -> Result<&'a ProviderRegistryCandidateV1, RuntimeSelectionError> {
    let requested = excluded
        .is_none()
        .then(|| {
            request
                .override_policy
                .requested()
                .and_then(|value| value.provider_route_reference_sha256.as_ref())
        })
        .flatten();
    request
        .candidates
        .provider_routes()
        .iter()
        .filter(|candidate| excluded != Some(candidate.id()))
        .filter(|candidate| requested.is_none_or(|digest| candidate.descriptor_digest() == digest))
        .filter(|candidate| {
            candidate.route_class() == ProviderRouteClassV1::Primary
                || request.fallback_policy.provider_route() == FallbackPermissionV1::BeforeProgress
        })
        .filter(|candidate| {
            candidate_compatible(
                candidate.capabilities(),
                request.capability_requirements.provider(),
            ) && request
                .health
                .is_available(candidate.health_authority_source(), candidate.health_component_id())
                && request
                    .admission_snapshot
                    .authority_ceiling
                    .permits_fallback(candidate.authority_class())
        })
        .min_by(|left, right| {
            left.preference_rank()
                .cmp(&right.preference_rank())
                .then_with(|| left.id().cmp(right.id()))
        })
        .ok_or(RuntimeSelectionError::NoCandidate(SelectionComponentKindV1::ProviderRoute))
}

fn candidate_compatible(available: &[SafeLabel], required: &[SafeLabel]) -> bool {
    required.iter().all(|capability| available.contains(capability))
}

fn availability(
    health: &ImmutableHealthSnapshotV1,
    source: super::candidates::RuntimeHealthAuthoritySourceV1,
    component_id: &RuntimeInstanceId,
) -> (CandidateAvailabilityV1, Option<CandidateUnavailableReasonV1>) {
    if source == super::candidates::RuntimeHealthAuthoritySourceV1::HostResident {
        return if health.is_available(source, component_id) {
            (CandidateAvailabilityV1::Available, None)
        } else if health.has_evidence(source, component_id) {
            (CandidateAvailabilityV1::Unavailable, Some(CandidateUnavailableReasonV1::Disabled))
        } else {
            (
                CandidateAvailabilityV1::Unavailable,
                Some(CandidateUnavailableReasonV1::MissingHealthEvidence),
            )
        };
    }
    let Some(record) = health.record(component_id) else {
        return (
            CandidateAvailabilityV1::Unavailable,
            Some(CandidateUnavailableReasonV1::MissingHealthEvidence),
        );
    };
    match record.state {
        RuntimeHealthState::Healthy | RuntimeHealthState::Degraded => {
            (CandidateAvailabilityV1::Available, None)
        }
        RuntimeHealthState::Quarantined => {
            (CandidateAvailabilityV1::Unavailable, Some(CandidateUnavailableReasonV1::Quarantined))
        }
        RuntimeHealthState::Disabled => {
            (CandidateAvailabilityV1::Unavailable, Some(CandidateUnavailableReasonV1::Disabled))
        }
        RuntimeHealthState::Cooldown | RuntimeHealthState::Probing => {
            (CandidateAvailabilityV1::Unavailable, Some(CandidateUnavailableReasonV1::Unhealthy))
        }
    }
}

fn harness_reports(
    request: &RuntimeSelectionRequest,
    selected_id: &SafeLabel,
    selected_reason: CandidateSelectedReasonV1,
) -> Result<Vec<SelectionCandidateReport>, RuntimeSelectionError> {
    request
        .candidates
        .harnesses()
        .iter()
        .map(|candidate| {
            report(
                request,
                SelectionComponentKindV1::Harness,
                candidate.id(),
                candidate.health_component_id(),
                candidate.health_authority_source(),
                candidate.capabilities(),
                request.capability_requirements.harness(),
                candidate.preference_rank(),
                candidate.id() == selected_id,
                selected_reason,
                candidate.descriptor_digest(),
            )
        })
        .collect()
}

fn context_reports(
    request: &RuntimeSelectionRequest,
    selected_id: &SafeLabel,
    selected_reason: CandidateSelectedReasonV1,
) -> Result<Vec<SelectionCandidateReport>, RuntimeSelectionError> {
    request
        .candidates
        .context_engines()
        .iter()
        .map(|candidate| {
            report(
                request,
                SelectionComponentKindV1::ContextEngine,
                candidate.id(),
                candidate.health_component_id(),
                candidate.health_authority_source(),
                candidate.capabilities(),
                request.capability_requirements.context_engine(),
                candidate.preference_rank(),
                candidate.id() == selected_id,
                selected_reason,
                candidate.descriptor_digest(),
            )
        })
        .collect()
}

fn provider_reports(
    request: &RuntimeSelectionRequest,
    selected_id: &SafeLabel,
    selected_reason: CandidateSelectedReasonV1,
) -> Result<Vec<SelectionCandidateReport>, RuntimeSelectionError> {
    request
        .candidates
        .provider_routes()
        .iter()
        .map(|candidate| {
            report(
                request,
                SelectionComponentKindV1::ProviderRoute,
                candidate.id(),
                candidate.health_component_id(),
                candidate.health_authority_source(),
                candidate.capabilities(),
                request.capability_requirements.provider(),
                candidate.preference_rank(),
                candidate.id() == selected_id,
                selected_reason,
                candidate.descriptor_digest(),
            )
        })
        .collect()
}

#[allow(clippy::too_many_arguments)]
fn report(
    request: &RuntimeSelectionRequest,
    kind: SelectionComponentKindV1,
    id: &SafeLabel,
    health_id: &RuntimeInstanceId,
    health_source: super::candidates::RuntimeHealthAuthoritySourceV1,
    capabilities: &[SafeLabel],
    required: &[SafeLabel],
    rank: u16,
    selected: bool,
    selected_reason: CandidateSelectedReasonV1,
    descriptor_digest: &SelectionDigest,
) -> Result<SelectionCandidateReport, RuntimeSelectionError> {
    let (availability, unavailable_reason) =
        availability(&request.health, health_source, health_id);
    let compatibility = if candidate_compatible(capabilities, required) {
        CandidateCompatibilityV1::Compatible
    } else {
        CandidateCompatibilityV1::MissingCapability
    };
    SelectionCandidateReport::new(
        kind,
        id.clone(),
        SafeLabel::parse(health_id.as_str().to_owned())
            .map_err(|_| RuntimeSelectionError::InvalidCandidateReport)?,
        availability,
        unavailable_reason,
        compatibility,
        rank,
        selected,
        selected.then_some(selected_reason),
        descriptor_digest.clone(),
    )
}

fn build_projection(
    proof: &HostRuntimeSelectionAuthorityProof,
    request: &RuntimeSelectionRequest,
    components: SelectedComponents,
    fallback_transition: Option<RuntimeFallbackTransitionEvidenceV1>,
) -> Result<RuntimeSelectionV1, RuntimeSelectionError> {
    let evidence = RuntimeSelectionEvidenceV1 {
        admission_snapshot_digest: request.admission_snapshot.snapshot_digest.clone(),
        persisted_admission_token_digest: proof.persisted_token_digest().clone(),
        fallback_policy_digest: request.fallback_policy.digest().clone(),
        override_policy_digest: request.override_policy.digest().clone(),
        capability_requirements_digest: request.capability_requirements.digest().clone(),
        selection_epochs_digest: request.epochs.digest().clone(),
        candidate_registry_digest: request.candidates.digest().clone(),
        health_snapshot_digest: request.health.digest().clone(),
        tool_catalog_snapshot_digest: request.tool_catalog.snapshot_digest().clone(),
        tool_catalog_hash: request.tool_catalog.catalog_hash().clone(),
        middleware_chain_digest: request.middleware_chain.chain_digest.clone(),
        execution_profile_digest: request.execution_profile.profile_digest.clone(),
    };
    RuntimeSelectionV1::build(
        proof.identities().generation,
        proof.decision().clone(),
        components.harness,
        components.context.clone(),
        components.provider,
        ToolCatalogSelectionProjectionV1::from(&request.tool_catalog),
        ProviderRequestEpochGuardV1::new(
            components.context.projection_epoch,
            request.tool_catalog.catalog_epoch(),
        )?,
        evidence,
        components.reports,
        fallback_transition,
    )
}

#[cfg(test)]
fn validate_single_component_change(
    prior: &RuntimeSelectionV1,
    next: &SelectedComponents,
    changed: SelectionComponentKindV1,
) -> Result<(), RuntimeSelectionError> {
    let harness_changed = prior.harness().harness_id != next.harness.harness_id;
    let context_changed = prior.context_engine().engine_id != next.context.engine_id;
    let provider_changed = prior.provider_route().route_id != next.provider.route_id;
    let exact = match changed {
        SelectionComponentKindV1::Harness => {
            harness_changed && !context_changed && !provider_changed
        }
        SelectionComponentKindV1::ContextEngine => false,
        SelectionComponentKindV1::ProviderRoute => {
            !harness_changed && !context_changed && provider_changed
        }
    };
    if !exact {
        return Err(RuntimeSelectionError::IllegalRecoveryReason);
    }
    if changed == SelectionComponentKindV1::Harness && next.harness.kind != HarnessKindV1::Embedded
        || changed == SelectionComponentKindV1::ProviderRoute
            && next.provider.route_class != ProviderRouteClassV1::Fallback
    {
        return Err(RuntimeSelectionError::IllegalRecoveryReason);
    }
    if changed == SelectionComponentKindV1::ProviderRoute {
        let prior_auth = &prior.provider_route().auth_policy;
        let next_auth = &next.provider.auth_policy;
        if prior_auth.policy_digest() != next_auth.policy_digest()
            || !next_auth.mode().does_not_widen(prior_auth.mode())
        {
            return Err(RuntimeSelectionError::AuthorityEscalation);
        }
    }
    Ok(())
}

#[cfg(test)]
impl From<RuntimeSelectionError> for FallbackFailureCause {
    fn from(value: RuntimeSelectionError) -> Self {
        match value {
            RuntimeSelectionError::NoCandidate(_) => Self::NoCandidate,
            other => Self::Selection(other),
        }
    }
}
