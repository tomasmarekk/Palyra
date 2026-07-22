//! Durable, bounded runtime-selection evidence.
//!
//! These projections are journal evidence only. Journal integrity is the
//! authenticity boundary; deserializing one never recreates executable grant
//! authority.

use palyra_common::runtime_contracts::{RuntimeAuthorityClass, RuntimeGeneration};
use serde::{Deserialize, Deserializer, Serialize};

use super::{
    authority::{RuntimeFallbackTriggerV1, RuntimeSelectionProgressV1},
    bounded::{BoundedVec, SafeLabel},
    candidates::{AuthCandidatePolicyReferenceV1, HarnessKindV1, ProviderRouteClassV1},
    catalog::ToolCatalogSelectionProjectionV1,
    digest::{digest_serializable, SelectionDigest},
    service::RuntimeSelectionError,
};
use crate::application::runtime_kernel_v2::selection::RuntimeAuthorityDecisionV1;

const MAX_CANDIDATE_REPORTS: usize = 96;
const SELECTION_DOMAIN: &[u8] = b"palyra.runtime_selection.projection.v1\0";
const PROGRESS_EVIDENCE_DOMAIN: &[u8] = b"palyra.runtime_selection.progress_evidence.v1\0";

/// Selected component family.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum SelectionComponentKindV1 {
    Harness,
    ContextEngine,
    ProviderRoute,
}

/// Candidate health availability.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum CandidateAvailabilityV1 {
    Available,
    Unavailable,
}

/// Stable reason for unavailability.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum CandidateUnavailableReasonV1 {
    Unhealthy,
    Quarantined,
    Disabled,
    MissingHealthEvidence,
}

/// Candidate capability compatibility.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum CandidateCompatibilityV1 {
    Compatible,
    MissingCapability,
    MissingTool,
}

/// Candidate preference classification.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum CandidatePreferenceV1 {
    Preferred,
    Alternate,
}

/// Why one component was selected.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum CandidateSelectedReasonV1 {
    SessionOverride,
    PreferredAvailable,
    ExternalHarnessRecovery,
    ProviderRouteRecovery,
}

impl CandidateSelectedReasonV1 {
    pub(super) const fn legal_for(self, kind: SelectionComponentKindV1) -> bool {
        match self {
            Self::SessionOverride | Self::PreferredAvailable => true,
            Self::ExternalHarnessRecovery => matches!(kind, SelectionComponentKindV1::Harness),
            Self::ProviderRouteRecovery => matches!(kind, SelectionComponentKindV1::ProviderRoute),
        }
    }
}

/// One bounded deterministic candidate evaluation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct SelectionCandidateReport {
    component_kind: SelectionComponentKindV1,
    candidate_id: SafeLabel,
    health_component_id: SafeLabel,
    availability: CandidateAvailabilityV1,
    unavailable_reason: Option<CandidateUnavailableReasonV1>,
    compatibility: CandidateCompatibilityV1,
    preference: CandidatePreferenceV1,
    preference_rank: u16,
    selected: bool,
    selected_reason: Option<CandidateSelectedReasonV1>,
    descriptor_digest: SelectionDigest,
}

impl SelectionCandidateReport {
    #[allow(clippy::too_many_arguments)]
    pub(super) fn new(
        component_kind: SelectionComponentKindV1,
        candidate_id: SafeLabel,
        health_component_id: SafeLabel,
        availability: CandidateAvailabilityV1,
        unavailable_reason: Option<CandidateUnavailableReasonV1>,
        compatibility: CandidateCompatibilityV1,
        preference_rank: u16,
        selected: bool,
        selected_reason: Option<CandidateSelectedReasonV1>,
        descriptor_digest: SelectionDigest,
    ) -> Result<Self, RuntimeSelectionError> {
        let report = Self {
            component_kind,
            candidate_id,
            health_component_id,
            availability,
            unavailable_reason,
            compatibility,
            preference: if preference_rank == 0 {
                CandidatePreferenceV1::Preferred
            } else {
                CandidatePreferenceV1::Alternate
            },
            preference_rank,
            selected,
            selected_reason,
            descriptor_digest,
        };
        report.validate()?;
        Ok(report)
    }

    fn validate(&self) -> Result<(), RuntimeSelectionError> {
        if (self.availability == CandidateAvailabilityV1::Available)
            == self.unavailable_reason.is_some()
            || self.selected != self.selected_reason.is_some()
            || self.selected_reason.is_some_and(|reason| !reason.legal_for(self.component_kind))
            || (self.preference_rank == 0) != (self.preference == CandidatePreferenceV1::Preferred)
            || (self.selected
                && (self.availability != CandidateAvailabilityV1::Available
                    || self.compatibility != CandidateCompatibilityV1::Compatible))
        {
            return Err(RuntimeSelectionError::InvalidCandidateReport);
        }
        Ok(())
    }
}

/// Selected harness evidence.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct SelectedHarnessV1 {
    pub(super) harness_id: SafeLabel,
    pub(super) kind: HarnessKindV1,
    pub(super) authority_class: RuntimeAuthorityClass,
    pub(super) reason: CandidateSelectedReasonV1,
}

/// Selected context-engine evidence.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct SelectedContextEngineV1 {
    pub(super) engine_id: SafeLabel,
    pub(super) projection_epoch: u64,
    pub(super) authority_class: RuntimeAuthorityClass,
    pub(super) reason: CandidateSelectedReasonV1,
}

/// Selected provider-route evidence.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct SelectedProviderRouteV1 {
    pub(super) route_id: SafeLabel,
    pub(super) route_class: ProviderRouteClassV1,
    pub(super) authority_class: RuntimeAuthorityClass,
    pub(super) auth_policy: AuthCandidatePolicyReferenceV1,
    pub(super) reason: CandidateSelectedReasonV1,
}

/// Provider request must retain the selected context/catalog epochs.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ProviderRequestEpochGuardV1 {
    context_projection_epoch: u64,
    catalog_epoch: u64,
}

impl ProviderRequestEpochGuardV1 {
    pub(super) fn new(
        context_projection_epoch: u64,
        catalog_epoch: u64,
    ) -> Result<Self, RuntimeSelectionError> {
        if context_projection_epoch == 0 || catalog_epoch == 0 {
            return Err(RuntimeSelectionError::InvalidEpoch);
        }
        Ok(Self { context_projection_epoch, catalog_epoch })
    }
}

/// Evidence that a fallback changed exactly one component.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RuntimeFallbackTransitionEvidenceV1 {
    prior_selection_digest: SelectionDigest,
    trigger: RuntimeFallbackTriggerV1,
    progress: RuntimeSelectionProgressV1,
    progress_digest: SelectionDigest,
    changed_component: SelectionComponentKindV1,
}

impl RuntimeFallbackTransitionEvidenceV1 {
    #[cfg(test)]
    pub(super) fn new(
        prior_selection_digest: SelectionDigest,
        trigger: RuntimeFallbackTriggerV1,
        progress: RuntimeSelectionProgressV1,
        changed_component: SelectionComponentKindV1,
    ) -> Result<Self, RuntimeSelectionError> {
        Ok(Self {
            prior_selection_digest,
            trigger,
            progress,
            progress_digest: digest_serializable(PROGRESS_EVIDENCE_DOMAIN, &progress)?,
            changed_component,
        })
    }

    fn validate(&self) -> Result<(), RuntimeSelectionError> {
        if self.progress_digest != digest_serializable(PROGRESS_EVIDENCE_DOMAIN, &self.progress)? {
            return Err(RuntimeSelectionError::DigestMismatch);
        }
        let legal = matches!(
            (&self.trigger, self.changed_component),
            (
                RuntimeFallbackTriggerV1::HarnessUnavailable { .. },
                SelectionComponentKindV1::Harness
            ) | (
                RuntimeFallbackTriggerV1::ProviderRouteUnavailable { .. },
                SelectionComponentKindV1::ProviderRoute
            )
        );
        if !legal {
            return Err(RuntimeSelectionError::IllegalRecoveryReason);
        }
        Ok(())
    }
}

/// All dedicated evidence digests that bind one selection.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RuntimeSelectionEvidenceV1 {
    pub(super) admission_snapshot_digest: SelectionDigest,
    pub(super) persisted_admission_token_digest: SelectionDigest,
    pub(super) fallback_policy_digest: SelectionDigest,
    pub(super) override_policy_digest: SelectionDigest,
    pub(super) capability_requirements_digest: SelectionDigest,
    pub(super) selection_epochs_digest: SelectionDigest,
    pub(super) candidate_registry_digest: SelectionDigest,
    pub(super) health_snapshot_digest: SelectionDigest,
    pub(super) tool_catalog_snapshot_digest: SelectionDigest,
    pub(super) tool_catalog_hash: SelectionDigest,
    pub(super) middleware_chain_digest: SelectionDigest,
    pub(super) execution_profile_digest: SelectionDigest,
}

impl RuntimeSelectionEvidenceV1 {
    #[must_use]
    pub(crate) const fn admission_snapshot_digest(&self) -> &SelectionDigest {
        &self.admission_snapshot_digest
    }

    #[must_use]
    pub(crate) const fn persisted_admission_token_digest(&self) -> &SelectionDigest {
        &self.persisted_admission_token_digest
    }

    #[must_use]
    pub(crate) const fn selection_epochs_digest(&self) -> &SelectionDigest {
        &self.selection_epochs_digest
    }
}

/// Durable selection projection.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RuntimeSelectionV1 {
    schema_version: u32,
    generation: RuntimeGeneration,
    authority_decision: RuntimeAuthorityDecisionV1,
    harness: SelectedHarnessV1,
    context_engine: SelectedContextEngineV1,
    provider_route: SelectedProviderRouteV1,
    tool_catalog: ToolCatalogSelectionProjectionV1,
    provider_epoch_guard: ProviderRequestEpochGuardV1,
    evidence: RuntimeSelectionEvidenceV1,
    candidate_reports: BoundedVec<SelectionCandidateReport, MAX_CANDIDATE_REPORTS>,
    fallback_transition: Option<RuntimeFallbackTransitionEvidenceV1>,
    selection_digest: SelectionDigest,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RuntimeSelectionWireV1 {
    schema_version: u32,
    generation: RuntimeGeneration,
    authority_decision: RuntimeAuthorityDecisionV1,
    harness: SelectedHarnessV1,
    context_engine: SelectedContextEngineV1,
    provider_route: SelectedProviderRouteV1,
    tool_catalog: ToolCatalogSelectionProjectionV1,
    provider_epoch_guard: ProviderRequestEpochGuardV1,
    evidence: RuntimeSelectionEvidenceV1,
    candidate_reports: BoundedVec<SelectionCandidateReport, MAX_CANDIDATE_REPORTS>,
    fallback_transition: Option<RuntimeFallbackTransitionEvidenceV1>,
    selection_digest: SelectionDigest,
}

#[derive(Serialize)]
struct RuntimeSelectionPayload<'a> {
    schema_version: u32,
    generation: RuntimeGeneration,
    authority_decision: &'a RuntimeAuthorityDecisionV1,
    harness: &'a SelectedHarnessV1,
    context_engine: &'a SelectedContextEngineV1,
    provider_route: &'a SelectedProviderRouteV1,
    tool_catalog: &'a ToolCatalogSelectionProjectionV1,
    provider_epoch_guard: &'a ProviderRequestEpochGuardV1,
    evidence: &'a RuntimeSelectionEvidenceV1,
    candidate_reports: &'a [SelectionCandidateReport],
    fallback_transition: &'a Option<RuntimeFallbackTransitionEvidenceV1>,
}

impl<'de> Deserialize<'de> for RuntimeSelectionV1 {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = RuntimeSelectionWireV1::deserialize(deserializer)?;
        let selection = Self {
            schema_version: wire.schema_version,
            generation: wire.generation,
            authority_decision: wire.authority_decision,
            harness: wire.harness,
            context_engine: wire.context_engine,
            provider_route: wire.provider_route,
            tool_catalog: wire.tool_catalog,
            provider_epoch_guard: wire.provider_epoch_guard,
            evidence: wire.evidence,
            candidate_reports: wire.candidate_reports,
            fallback_transition: wire.fallback_transition,
            selection_digest: wire.selection_digest,
        };
        selection.validate().map_err(serde::de::Error::custom)?;
        Ok(selection)
    }
}

impl RuntimeSelectionV1 {
    #[allow(clippy::too_many_arguments)]
    pub(super) fn build(
        generation: RuntimeGeneration,
        authority_decision: RuntimeAuthorityDecisionV1,
        harness: SelectedHarnessV1,
        context_engine: SelectedContextEngineV1,
        provider_route: SelectedProviderRouteV1,
        tool_catalog: ToolCatalogSelectionProjectionV1,
        provider_epoch_guard: ProviderRequestEpochGuardV1,
        evidence: RuntimeSelectionEvidenceV1,
        candidate_reports: Vec<SelectionCandidateReport>,
        fallback_transition: Option<RuntimeFallbackTransitionEvidenceV1>,
    ) -> Result<Self, RuntimeSelectionError> {
        let mut selection = Self {
            schema_version: 1,
            generation,
            authority_decision,
            harness,
            context_engine,
            provider_route,
            tool_catalog,
            provider_epoch_guard,
            evidence,
            candidate_reports: BoundedVec::try_new(candidate_reports)
                .map_err(|_| RuntimeSelectionError::InvalidCandidateReport)?,
            fallback_transition,
            selection_digest: SelectionDigest::from_domain_bytes(SELECTION_DOMAIN, &[]),
        };
        selection.selection_digest = selection.compute_digest()?;
        selection.validate()?;
        Ok(selection)
    }

    #[must_use]
    pub(crate) const fn selection_digest(&self) -> &SelectionDigest {
        &self.selection_digest
    }

    #[must_use]
    pub(crate) const fn selected_profile(
        &self,
    ) -> crate::application::runtime_kernel_v2::RuntimeKernelVersion {
        self.authority_decision.profile()
    }

    /// Returns the registry-pinned harness identifier.
    #[must_use]
    pub(crate) fn selected_harness_id(&self) -> &str {
        self.harness.harness_id.as_str()
    }

    /// Returns the exact registry-pinned context-engine implementation.
    #[must_use]
    pub(crate) fn selected_context_engine_id(&self) -> &str {
        self.context_engine.engine_id.as_str()
    }

    /// Returns the immutable projection epoch of the selected context engine.
    #[must_use]
    pub(crate) const fn selected_context_projection_epoch(&self) -> u64 {
        self.context_engine.projection_epoch
    }

    /// Returns whether the selected harness executes in-process or externally.
    #[must_use]
    pub(crate) const fn selected_harness_kind(&self) -> HarnessKindV1 {
        self.harness.kind
    }

    #[cfg(test)]
    pub(super) const fn harness(&self) -> &SelectedHarnessV1 {
        &self.harness
    }

    #[cfg(test)]
    pub(super) const fn context_engine(&self) -> &SelectedContextEngineV1 {
        &self.context_engine
    }

    #[cfg(test)]
    pub(super) const fn provider_route(&self) -> &SelectedProviderRouteV1 {
        &self.provider_route
    }

    /// Returns the selected host route id without exposing provider credentials.
    #[must_use]
    pub(crate) fn selected_provider_route_id(&self) -> &str {
        self.provider_route.route_id.as_str()
    }

    pub(crate) const fn evidence(&self) -> &RuntimeSelectionEvidenceV1 {
        &self.evidence
    }

    #[must_use]
    pub(crate) const fn authority_decision(&self) -> &RuntimeAuthorityDecisionV1 {
        &self.authority_decision
    }

    fn compute_digest(&self) -> Result<SelectionDigest, RuntimeSelectionError> {
        digest_serializable(
            SELECTION_DOMAIN,
            &RuntimeSelectionPayload {
                schema_version: self.schema_version,
                generation: self.generation,
                authority_decision: &self.authority_decision,
                harness: &self.harness,
                context_engine: &self.context_engine,
                provider_route: &self.provider_route,
                tool_catalog: &self.tool_catalog,
                provider_epoch_guard: &self.provider_epoch_guard,
                evidence: &self.evidence,
                candidate_reports: &self.candidate_reports,
                fallback_transition: &self.fallback_transition,
            },
        )
    }

    pub(crate) fn validate(&self) -> Result<(), RuntimeSelectionError> {
        if self.schema_version != 1
            || self.generation != self.authority_decision.generation()
            || !self.harness.reason.legal_for(SelectionComponentKindV1::Harness)
            || !self.context_engine.reason.legal_for(SelectionComponentKindV1::ContextEngine)
            || !self.provider_route.reason.legal_for(SelectionComponentKindV1::ProviderRoute)
        {
            return Err(RuntimeSelectionError::InvalidProjection);
        }
        for report in self.candidate_reports.iter() {
            report.validate()?;
        }
        if let Some(transition) = &self.fallback_transition {
            transition.validate()?;
        }
        if self.compute_digest()? != self.selection_digest {
            return Err(RuntimeSelectionError::DigestMismatch);
        }
        Ok(())
    }
}
