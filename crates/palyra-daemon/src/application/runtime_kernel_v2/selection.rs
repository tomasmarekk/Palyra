//! Deterministic session-pinned runtime selection for validated atomic profiles.
//!
//! Decisions contain generation and low-cardinality reasons only. Raw session
//! identities, canary hash material, and bucket values never leave the selector.
//! Harness, context, provider, auth, catalog, and middleware selection remain
//! the separate RuntimeSelectionV1 service boundary.

use std::fmt;

use palyra_common::runtime_contracts::{RuntimeGeneration, RuntimeSessionId, SideEffectFenceState};
#[cfg(test)]
use palyra_common::runtime_contracts::{RuntimeIdentityError, RuntimeIdentitySetV1};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

use super::{
    profile::{
        RuntimeKernelProfileConfigV1, RuntimeKernelProfileError, CANARY_BASIS_POINTS_DENOMINATOR,
    },
    RuntimeKernelVersion,
};

const RUNTIME_AUTHORITY_DECISION_SCHEMA_VERSION: u32 = 1;
const CANARY_DOMAIN: &[u8] = b"palyra.runtime_kernel_v2.canary.session.v1\0";
const CANARY_KEY_DOMAIN: &[u8] = b"palyra.runtime_kernel_v2.canary.key.v1\0";
#[cfg(test)]
const MAX_COMPATIBILITY_CANARY_KEY_BYTES: usize = 128;

/// Identity class used for deterministic canary assignment.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CanarySamplingIdentity {
    /// Pin every run in one session to the same cohort.
    Session,
    /// Pin every session owned by one principal to the same cohort.
    Principal,
}

/// Authoritative implementation selected for one run generation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum RuntimeAuthority {
    /// Existing orchestration owns the run.
    Legacy,
    /// RuntimeKernelV2 owns the run.
    V2,
}

/// Stable reason a V2 candidate was not available for selection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum V2UnavailabilityReason {
    /// The executable does not contain a complete V2 implementation.
    NotReady,
    /// Runtime health evidence blocks activation.
    Unhealthy,
    /// The V2 runtime is quarantined.
    Quarantined,
    /// Policy denies V2 for this admitted session.
    PolicyBlocked,
    /// The admitted capability set is incompatible with V2.
    CapabilityMismatch,
}

/// Current availability of the V2 authoritative runtime.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum V2RuntimeAvailability {
    /// V2 may own a newly selected generation.
    Ready,
    /// V2 cannot safely own the generation.
    Unavailable(V2UnavailabilityReason),
}

/// Low-cardinality reason attached to a runtime selection decision.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum RuntimeAuthorityReason {
    /// The closed profile explicitly selected legacy.
    LegacyProfileSelected,
    /// Legacy remains authoritative while V2 computes an observe-only shadow plan.
    V2ShadowLegacyAuthority,
    /// The session was outside the V2 canary allocation.
    V2CanarySessionExcluded,
    /// The session was inside the V2 canary allocation.
    V2CanarySessionSelected,
    /// The closed V2 profile selected V2.
    V2ProfileSelected,
    /// V2 was unavailable and explicit V2 selection forbids legacy fallback.
    V2UnavailableNoLegacyFallback,
    /// V2 became unavailable after output; changing runtimes would duplicate or reorder output.
    V2UnavailableAfterPartialOutput,
    /// V2 became unavailable after a side-effect boundary; legacy cannot inherit authority.
    V2UnavailableAfterSideEffectBoundary,
}

impl RuntimeAuthorityReason {
    /// Returns the stable metadata-trace reason code.
    #[must_use]
    pub(crate) const fn as_reason_code(self) -> &'static str {
        match self {
            Self::LegacyProfileSelected => "runtime.selection.legacy_profile_selected",
            Self::V2ShadowLegacyAuthority => "runtime.selection.v2_shadow_legacy_authority",
            Self::V2CanarySessionExcluded => "runtime.selection.v2_canary_session_excluded",
            Self::V2CanarySessionSelected => "runtime.selection.v2_canary_session_selected",
            Self::V2ProfileSelected => "runtime.selection.v2_profile_selected",
            Self::V2UnavailableNoLegacyFallback => {
                "runtime.selection.v2_unavailable_no_legacy_fallback"
            }
            Self::V2UnavailableAfterPartialOutput => {
                "runtime.selection.v2_unavailable_after_partial_output"
            }
            Self::V2UnavailableAfterSideEffectBoundary => {
                "runtime.selection.v2_unavailable_after_side_effect_boundary"
            }
        }
    }
}

/// Whether a runtime selection admitted an authoritative implementation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum RuntimeAuthorityDisposition {
    /// The selected runtime may own the generation.
    Selected,
    /// Selection stopped without granting legacy or V2 authority.
    Blocked,
}

/// Evidence that constrains recovery after an explicit V2 selection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct RuntimeAuthorityProgressEvidence {
    output_emitted: bool,
    side_effect_state: Option<SideEffectFenceState>,
}

impl RuntimeAuthorityProgressEvidence {
    /// Creates progress evidence from the canonical output and side-effect contracts.
    #[must_use]
    pub(crate) const fn new(
        output_emitted: bool,
        side_effect_state: Option<SideEffectFenceState>,
    ) -> Self {
        Self { output_emitted, side_effect_state }
    }

    /// Creates evidence for a run that has not crossed an externally visible boundary.
    #[must_use]
    pub(crate) const fn pristine() -> Self {
        Self::new(false, None)
    }

    const fn side_effect_boundary_may_have_been_crossed(self) -> bool {
        match self.side_effect_state {
            None | Some(SideEffectFenceState::IntentRecorded) => false,
            Some(
                SideEffectFenceState::EffectStarted
                | SideEffectFenceState::EffectObserved
                | SideEffectFenceState::EffectUnknown
                | SideEffectFenceState::Reconciled
                | SideEffectFenceState::Abandoned,
            ) => true,
        }
    }
}

/// Opaque selector that owns deployment-stable canary key material.
///
/// The key is hashed on construction and redacted from `Debug`; neither the
/// digest nor per-session bucket is exposed by the public selection decision.
pub(crate) struct SessionCanarySelector {
    basis_points: u16,
    identity: CanarySamplingIdentity,
    key_digest: [u8; 32],
}

impl fmt::Debug for SessionCanarySelector {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SessionCanarySelector")
            .field("basis_points", &self.basis_points)
            .field("identity", &self.identity)
            .field("key_digest", &"[redacted]")
            .finish()
    }
}

impl SessionCanarySelector {
    /// Creates a deterministic selector for one validated canary allocation.
    ///
    /// This compatibility constructor accepts existing non-empty host key
    /// material. New config wiring uses [`Self::new_with_identity`] with an
    /// exact 32-byte deployment key.
    ///
    /// # Errors
    /// Returns [`RuntimeAuthorityError::InvalidCanarySelector`] when the
    /// allocation is zero or above 100%, or key material is empty or oversized.
    #[cfg(test)]
    pub(crate) fn new(
        basis_points: u16,
        key_material: &[u8],
    ) -> Result<Self, RuntimeAuthorityError> {
        if key_material.is_empty() || key_material.len() > MAX_COMPATIBILITY_CANARY_KEY_BYTES {
            return Err(RuntimeAuthorityError::InvalidCanarySelector);
        }
        Self::from_key_material(basis_points, key_material, CanarySamplingIdentity::Session)
    }

    /// Creates a selector pinned to the configured identity class and exact
    /// 32-byte deployment key.
    ///
    /// # Errors
    /// Returns [`RuntimeAuthorityError::InvalidCanarySelector`] when the
    /// allocation is not a strict canary percentage.
    pub(crate) fn new_with_identity(
        basis_points: u16,
        key_material: &[u8; 32],
        identity: CanarySamplingIdentity,
    ) -> Result<Self, RuntimeAuthorityError> {
        Self::from_key_material(basis_points, key_material, identity)
    }

    fn from_key_material(
        basis_points: u16,
        key_material: &[u8],
        identity: CanarySamplingIdentity,
    ) -> Result<Self, RuntimeAuthorityError> {
        if !(1..=CANARY_BASIS_POINTS_DENOMINATOR).contains(&basis_points) {
            return Err(RuntimeAuthorityError::InvalidCanarySelector);
        }
        let mut digest = Sha256::new();
        digest.update(CANARY_KEY_DOMAIN);
        digest.update(key_material);
        Ok(Self { basis_points, identity, key_digest: digest.finalize().into() })
    }

    #[cfg(test)]
    fn assignment_value(
        &self,
        identities: &RuntimeIdentitySetV1,
        principal: Option<&str>,
    ) -> Result<u16, RuntimeAuthorityError> {
        self.assignment_value_for_session(&identities.session_id, principal)
    }

    #[cfg(test)]
    fn includes(
        &self,
        identities: &RuntimeIdentitySetV1,
        principal: Option<&str>,
    ) -> Result<bool, RuntimeAuthorityError> {
        Ok(self.assignment_value(identities, principal)? < self.basis_points)
    }

    fn assignment_value_for_session(
        &self,
        session_id: &RuntimeSessionId,
        principal: Option<&str>,
    ) -> Result<u16, RuntimeAuthorityError> {
        let (identity_label, identity_value) = match self.identity {
            CanarySamplingIdentity::Session => ("session_id", session_id.as_str()),
            CanarySamplingIdentity::Principal => {
                let principal = principal
                    .filter(|value| !value.trim().is_empty())
                    .ok_or(RuntimeAuthorityError::MissingCanaryPrincipal)?;
                ("principal", principal)
            }
        };
        let mut digest = Sha256::new();
        digest.update(CANARY_DOMAIN);
        digest.update(self.key_digest);
        digest.update([0]);
        digest.update(identity_label.as_bytes());
        digest.update([0]);
        digest.update(identity_value.as_bytes());
        let bytes: [u8; 32] = digest.finalize().into();
        let sample = u64::from_be_bytes([
            bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
        ]);
        let scaled = (u128::from(sample) * u128::from(CANARY_BASIS_POINTS_DENOMINATOR)) >> 64;
        u16::try_from(scaled).map_err(|_| RuntimeAuthorityError::InvalidCanarySelector)
    }

    fn includes_session_subject(
        &self,
        session_id: &RuntimeSessionId,
        principal: Option<&str>,
    ) -> Result<bool, RuntimeAuthorityError> {
        Ok(self.assignment_value_for_session(session_id, principal)? < self.basis_points)
    }
}

/// Generation-free result of the single host rollout/sampling decision.
///
/// The value carries no identity, bucket, or key material. Runtime admission
/// binds it to the generation allocated inside the admission transaction.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct ResolvedRuntimeAuthorityIntent {
    profile: RuntimeKernelVersion,
    disposition: RuntimeAuthorityDisposition,
    selected_runtime: Option<RuntimeAuthority>,
    shadow_evaluation_enabled: bool,
    reason: RuntimeAuthorityReason,
    v2_unavailability: Option<V2UnavailabilityReason>,
    _private: (),
}

impl ResolvedRuntimeAuthorityIntent {
    fn selected(
        profile: RuntimeKernelVersion,
        selected_runtime: RuntimeAuthority,
        shadow_evaluation_enabled: bool,
        reason: RuntimeAuthorityReason,
    ) -> Self {
        Self {
            profile,
            disposition: RuntimeAuthorityDisposition::Selected,
            selected_runtime: Some(selected_runtime),
            shadow_evaluation_enabled,
            reason,
            v2_unavailability: None,
            _private: (),
        }
    }

    fn blocked(
        profile: RuntimeKernelVersion,
        reason: RuntimeAuthorityReason,
        v2_unavailability: V2UnavailabilityReason,
    ) -> Self {
        Self {
            profile,
            disposition: RuntimeAuthorityDisposition::Blocked,
            selected_runtime: None,
            shadow_evaluation_enabled: false,
            reason,
            v2_unavailability: Some(v2_unavailability),
            _private: (),
        }
    }

    #[must_use]
    pub(crate) const fn profile(&self) -> RuntimeKernelVersion {
        self.profile
    }

    #[must_use]
    pub(crate) const fn selected_runtime(&self) -> Option<RuntimeAuthority> {
        self.selected_runtime
    }

    #[must_use]
    pub(crate) const fn shadow_evaluation_enabled(&self) -> bool {
        self.shadow_evaluation_enabled
    }

    #[must_use]
    pub(crate) const fn reason(&self) -> RuntimeAuthorityReason {
        self.reason
    }

    /// Binds the sampled intent to the generation allocated by admission.
    ///
    /// # Errors
    /// Returns [`RuntimeAuthorityError::InvalidDecision`] if the intent's
    /// profile, authority, shadow posture, and reason are inconsistent.
    pub(crate) fn bind_generation(
        &self,
        generation: RuntimeGeneration,
    ) -> Result<RuntimeAuthorityDecisionV1, RuntimeAuthorityError> {
        let decision = RuntimeAuthorityDecisionV1 {
            schema_version: RUNTIME_AUTHORITY_DECISION_SCHEMA_VERSION,
            profile: self.profile,
            generation,
            disposition: self.disposition,
            selected_runtime: self.selected_runtime,
            shadow_evaluation_enabled: self.shadow_evaluation_enabled,
            reason: self.reason,
            reason_code: self.reason.as_reason_code().to_owned(),
            v2_unavailability: self.v2_unavailability,
        };
        decision.validate()?;
        Ok(decision)
    }

    pub(in crate::application::runtime_kernel_v2) fn from_persisted_pin(
        profile: RuntimeKernelVersion,
        selected_runtime: RuntimeAuthority,
        shadow_evaluation_enabled: bool,
        reason: RuntimeAuthorityReason,
    ) -> Result<Self, RuntimeAuthorityError> {
        let intent = Self::selected(profile, selected_runtime, shadow_evaluation_enabled, reason);
        validate_authority_shape(
            intent.profile,
            intent.disposition,
            intent.selected_runtime,
            intent.shadow_evaluation_enabled,
            intent.reason,
            intent.v2_unavailability,
        )?;
        Ok(intent)
    }
}

/// Validated, identity-free runtime selection projection.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) struct RuntimeAuthorityDecisionV1 {
    schema_version: u32,
    profile: RuntimeKernelVersion,
    generation: RuntimeGeneration,
    disposition: RuntimeAuthorityDisposition,
    #[serde(skip_serializing_if = "Option::is_none")]
    selected_runtime: Option<RuntimeAuthority>,
    shadow_evaluation_enabled: bool,
    reason: RuntimeAuthorityReason,
    reason_code: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    v2_unavailability: Option<V2UnavailabilityReason>,
}

#[derive(Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
struct RuntimeAuthorityDecisionWire {
    schema_version: u32,
    profile: RuntimeKernelVersion,
    generation: RuntimeGeneration,
    disposition: RuntimeAuthorityDisposition,
    #[serde(default)]
    selected_runtime: Option<RuntimeAuthority>,
    shadow_evaluation_enabled: bool,
    reason: RuntimeAuthorityReason,
    reason_code: String,
    #[serde(default)]
    v2_unavailability: Option<V2UnavailabilityReason>,
}

impl<'de> Deserialize<'de> for RuntimeAuthorityDecisionV1 {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let wire = RuntimeAuthorityDecisionWire::deserialize(deserializer)?;
        let decision = Self {
            schema_version: wire.schema_version,
            profile: wire.profile,
            generation: wire.generation,
            disposition: wire.disposition,
            selected_runtime: wire.selected_runtime,
            shadow_evaluation_enabled: wire.shadow_evaluation_enabled,
            reason: wire.reason,
            reason_code: wire.reason_code,
            v2_unavailability: wire.v2_unavailability,
        };
        decision.validate().map_err(serde::de::Error::custom)?;
        Ok(decision)
    }
}

impl RuntimeAuthorityDecisionV1 {
    /// Returns the authoritative runtime, or `None` when selection blocked.
    #[must_use]
    pub(crate) const fn selected_runtime(&self) -> Option<RuntimeAuthority> {
        self.selected_runtime
    }

    /// Returns the closed profile whose policy produced this decision.
    #[must_use]
    pub(crate) const fn profile(&self) -> RuntimeKernelVersion {
        self.profile
    }

    /// Returns the selected run generation.
    #[must_use]
    pub(crate) const fn generation(&self) -> RuntimeGeneration {
        self.generation
    }

    /// Returns whether an observe-only V2 shadow plan may be computed.
    #[must_use]
    pub(crate) const fn shadow_evaluation_enabled(&self) -> bool {
        self.shadow_evaluation_enabled
    }

    /// Returns the low-cardinality selection reason.
    #[must_use]
    pub(crate) const fn reason(&self) -> RuntimeAuthorityReason {
        self.reason
    }

    /// Returns the stable metadata-trace reason code.
    #[must_use]
    #[cfg(test)]
    pub(crate) fn reason_code(&self) -> &str {
        self.reason_code.as_str()
    }

    /// Validates the durable selection projection.
    ///
    /// # Errors
    /// Returns [`RuntimeAuthorityError::InvalidDecision`] when schema, reason,
    /// selected authority, shadow posture, or unavailability evidence conflict.
    pub(crate) fn validate(&self) -> Result<(), RuntimeAuthorityError> {
        if self.schema_version != RUNTIME_AUTHORITY_DECISION_SCHEMA_VERSION
            || self.reason_code != self.reason.as_reason_code()
        {
            return Err(RuntimeAuthorityError::InvalidDecision);
        }
        validate_authority_shape(
            self.profile,
            self.disposition,
            self.selected_runtime,
            self.shadow_evaluation_enabled,
            self.reason,
            self.v2_unavailability,
        )
    }
}

fn validate_authority_shape(
    profile: RuntimeKernelVersion,
    disposition: RuntimeAuthorityDisposition,
    selected_runtime: Option<RuntimeAuthority>,
    shadow_evaluation_enabled: bool,
    reason: RuntimeAuthorityReason,
    v2_unavailability: Option<V2UnavailabilityReason>,
) -> Result<(), RuntimeAuthorityError> {
    let valid = match (
        profile,
        disposition,
        selected_runtime,
        shadow_evaluation_enabled,
        reason,
        v2_unavailability,
    ) {
        (
            RuntimeKernelVersion::Legacy,
            RuntimeAuthorityDisposition::Selected,
            Some(RuntimeAuthority::Legacy),
            false,
            RuntimeAuthorityReason::LegacyProfileSelected,
            None,
        )
        | (
            RuntimeKernelVersion::V2Shadow,
            RuntimeAuthorityDisposition::Selected,
            Some(RuntimeAuthority::Legacy),
            true,
            RuntimeAuthorityReason::V2ShadowLegacyAuthority,
            None,
        )
        | (
            RuntimeKernelVersion::V2Canary,
            RuntimeAuthorityDisposition::Selected,
            Some(RuntimeAuthority::Legacy),
            false,
            RuntimeAuthorityReason::V2CanarySessionExcluded,
            None,
        )
        | (
            RuntimeKernelVersion::V2Canary,
            RuntimeAuthorityDisposition::Selected,
            Some(RuntimeAuthority::V2),
            false,
            RuntimeAuthorityReason::V2CanarySessionSelected,
            None,
        )
        | (
            RuntimeKernelVersion::V2,
            RuntimeAuthorityDisposition::Selected,
            Some(RuntimeAuthority::V2),
            false,
            RuntimeAuthorityReason::V2ProfileSelected,
            None,
        ) => true,
        (
            RuntimeKernelVersion::V2Canary | RuntimeKernelVersion::V2,
            RuntimeAuthorityDisposition::Blocked,
            None,
            false,
            RuntimeAuthorityReason::V2UnavailableNoLegacyFallback
            | RuntimeAuthorityReason::V2UnavailableAfterPartialOutput
            | RuntimeAuthorityReason::V2UnavailableAfterSideEffectBoundary,
            Some(_),
        ) => true,
        (
            RuntimeKernelVersion::Legacy
            | RuntimeKernelVersion::V2Shadow
            | RuntimeKernelVersion::V2Canary
            | RuntimeKernelVersion::V2,
            _,
            _,
            _,
            _,
            _,
        ) => false,
    };
    if valid {
        Ok(())
    } else {
        Err(RuntimeAuthorityError::InvalidDecision)
    }
}

/// Resolves one validated profile to generation-pinned runtime authority.
///
/// Canary inclusion is stable for the session and never appears in the
/// returned projection as a raw identity, digest, or bucket. Explicit V2
/// selection blocks when V2 is unavailable; it never silently returns legacy.
///
/// # Errors
/// Returns [`RuntimeAuthorityError`] when profile or identity input is invalid,
/// or when a canary profile lacks a matching deterministic selector.
#[cfg(test)]
pub(crate) fn resolve_runtime_authority(
    config: &RuntimeKernelProfileConfigV1,
    identities: &RuntimeIdentitySetV1,
    v2_availability: V2RuntimeAvailability,
    progress: RuntimeAuthorityProgressEvidence,
    canary_selector: Option<&SessionCanarySelector>,
) -> Result<RuntimeAuthorityDecisionV1, RuntimeAuthorityError> {
    resolve_runtime_authority_for_principal(
        config,
        identities,
        None,
        v2_availability,
        progress,
        canary_selector,
    )
}

/// Resolves authority with an optional principal for principal-pinned canaries.
///
/// The principal is consumed only by the keyed selector and never enters the
/// returned decision or any diagnostics projection.
///
/// # Errors
/// Returns [`RuntimeAuthorityError`] for invalid config or identity evidence,
/// missing principal input, or mismatched canary configuration.
#[cfg(test)]
pub(crate) fn resolve_runtime_authority_for_principal(
    config: &RuntimeKernelProfileConfigV1,
    identities: &RuntimeIdentitySetV1,
    principal: Option<&str>,
    v2_availability: V2RuntimeAvailability,
    progress: RuntimeAuthorityProgressEvidence,
    canary_selector: Option<&SessionCanarySelector>,
) -> Result<RuntimeAuthorityDecisionV1, RuntimeAuthorityError> {
    identities.validate().map_err(RuntimeAuthorityError::InvalidIdentities)?;
    resolve_runtime_authority_intent_for_principal(
        config,
        &identities.session_id,
        principal,
        v2_availability,
        progress,
        canary_selector,
    )?
    .bind_generation(identities.generation)
}

/// Resolves rollout authority without assuming the next run generation.
///
/// The keyed selector consumes only the configured stable identity. The
/// returned intent contains no identity, bucket, or key material.
///
/// # Errors
/// Returns [`RuntimeAuthorityError`] for invalid config, missing principal
/// input, or a canary selector that does not match the configured allocation.
pub(crate) fn resolve_runtime_authority_intent_for_principal(
    config: &RuntimeKernelProfileConfigV1,
    session_id: &RuntimeSessionId,
    principal: Option<&str>,
    v2_availability: V2RuntimeAvailability,
    progress: RuntimeAuthorityProgressEvidence,
    canary_selector: Option<&SessionCanarySelector>,
) -> Result<ResolvedRuntimeAuthorityIntent, RuntimeAuthorityError> {
    config.validate()?;
    let intent = match config.profile() {
        RuntimeKernelVersion::Legacy => ResolvedRuntimeAuthorityIntent::selected(
            RuntimeKernelVersion::Legacy,
            RuntimeAuthority::Legacy,
            false,
            RuntimeAuthorityReason::LegacyProfileSelected,
        ),
        RuntimeKernelVersion::V2Shadow => ResolvedRuntimeAuthorityIntent::selected(
            RuntimeKernelVersion::V2Shadow,
            RuntimeAuthority::Legacy,
            true,
            RuntimeAuthorityReason::V2ShadowLegacyAuthority,
        ),
        RuntimeKernelVersion::V2Canary => {
            let selector = canary_selector.ok_or(RuntimeAuthorityError::MissingCanarySelector)?;
            if selector.basis_points != config.canary_basis_points() {
                return Err(RuntimeAuthorityError::CanaryAllocationMismatch {
                    profile_basis_points: config.canary_basis_points(),
                    selector_basis_points: selector.basis_points,
                });
            }
            if selector.includes_session_subject(session_id, principal)? {
                select_v2_intent(
                    RuntimeKernelVersion::V2Canary,
                    RuntimeAuthorityReason::V2CanarySessionSelected,
                    v2_availability,
                    progress,
                )
            } else {
                ResolvedRuntimeAuthorityIntent::selected(
                    RuntimeKernelVersion::V2Canary,
                    RuntimeAuthority::Legacy,
                    false,
                    RuntimeAuthorityReason::V2CanarySessionExcluded,
                )
            }
        }
        RuntimeKernelVersion::V2 => select_v2_intent(
            RuntimeKernelVersion::V2,
            RuntimeAuthorityReason::V2ProfileSelected,
            v2_availability,
            progress,
        ),
    };
    validate_authority_shape(
        intent.profile,
        intent.disposition,
        intent.selected_runtime,
        intent.shadow_evaluation_enabled,
        intent.reason,
        intent.v2_unavailability,
    )?;
    Ok(intent)
}

/// Returns an already-persisted authority decision or performs initial selection.
///
/// Runtime authority is immutable for one run generation. Once admission has
/// persisted a decision, restart, config reload, health changes, or canary-key
/// rotation must reuse it rather than recompute a different owner.
///
/// # Errors
/// Returns [`RuntimeAuthorityError`] when the persisted decision is invalid or
/// belongs to another generation, or when initial resolution fails.
#[cfg(test)]
pub(crate) fn resolve_or_reuse_runtime_authority(
    config: &RuntimeKernelProfileConfigV1,
    identities: &RuntimeIdentitySetV1,
    persisted: Option<&RuntimeAuthorityDecisionV1>,
    v2_availability: V2RuntimeAvailability,
    progress: RuntimeAuthorityProgressEvidence,
    canary_selector: Option<&SessionCanarySelector>,
) -> Result<RuntimeAuthorityDecisionV1, RuntimeAuthorityError> {
    resolve_or_reuse_runtime_authority_for_principal(
        config,
        identities,
        None,
        persisted,
        v2_availability,
        progress,
        canary_selector,
    )
}

/// Reuses persisted authority or selects once with optional principal input.
///
/// Config reload never reaches the selection branch when a persisted
/// generation decision exists.
///
/// # Errors
/// Returns [`RuntimeAuthorityError`] when persisted evidence is invalid or
/// initial selection fails.
#[cfg(test)]
pub(crate) fn resolve_or_reuse_runtime_authority_for_principal(
    config: &RuntimeKernelProfileConfigV1,
    identities: &RuntimeIdentitySetV1,
    principal: Option<&str>,
    persisted: Option<&RuntimeAuthorityDecisionV1>,
    v2_availability: V2RuntimeAvailability,
    progress: RuntimeAuthorityProgressEvidence,
    canary_selector: Option<&SessionCanarySelector>,
) -> Result<RuntimeAuthorityDecisionV1, RuntimeAuthorityError> {
    if let Some(decision) = persisted {
        decision.validate()?;
        identities.validate().map_err(RuntimeAuthorityError::InvalidIdentities)?;
        if decision.generation() != identities.generation {
            return Err(RuntimeAuthorityError::PersistedDecisionGenerationMismatch {
                expected: identities.generation,
                observed: decision.generation(),
            });
        }
        return Ok(decision.clone());
    }
    resolve_runtime_authority_for_principal(
        config,
        identities,
        principal,
        v2_availability,
        progress,
        canary_selector,
    )
}

fn select_v2_intent(
    profile: RuntimeKernelVersion,
    selected_reason: RuntimeAuthorityReason,
    availability: V2RuntimeAvailability,
    progress: RuntimeAuthorityProgressEvidence,
) -> ResolvedRuntimeAuthorityIntent {
    match availability {
        V2RuntimeAvailability::Ready => ResolvedRuntimeAuthorityIntent::selected(
            profile,
            RuntimeAuthority::V2,
            false,
            selected_reason,
        ),
        V2RuntimeAvailability::Unavailable(unavailability) => {
            let reason = if progress.side_effect_boundary_may_have_been_crossed() {
                RuntimeAuthorityReason::V2UnavailableAfterSideEffectBoundary
            } else if progress.output_emitted {
                RuntimeAuthorityReason::V2UnavailableAfterPartialOutput
            } else {
                RuntimeAuthorityReason::V2UnavailableNoLegacyFallback
            };
            ResolvedRuntimeAuthorityIntent::blocked(profile, reason, unavailability)
        }
    }
}

/// Fail-closed runtime-authority validation error.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub(crate) enum RuntimeAuthorityError {
    /// The closed profile failed its atomic validation.
    #[error(transparent)]
    InvalidProfile(#[from] RuntimeKernelProfileError),
    /// Runtime identities failed the shared typed-identity contract.
    #[cfg(test)]
    #[error("runtime selection identities are invalid")]
    InvalidIdentities(#[source] RuntimeIdentityError),
    /// Canary key or allocation was invalid.
    #[error("runtime canary selector is invalid")]
    InvalidCanarySelector,
    /// A canary profile was evaluated without a session selector.
    #[error("runtime v2 canary profile requires a session selector")]
    MissingCanarySelector,
    /// Principal-pinned sampling was selected without a principal.
    #[error("runtime principal canary selection requires a non-empty principal")]
    MissingCanaryPrincipal,
    /// Profile and selector allocations differ.
    #[error(
        "runtime canary allocation mismatch: profile has {profile_basis_points}, selector has {selector_basis_points}"
    )]
    CanaryAllocationMismatch {
        /// Allocation validated with the profile.
        profile_basis_points: u16,
        /// Allocation bound into the selector.
        selector_basis_points: u16,
    },
    /// A serialized or in-memory selection projection was inconsistent.
    #[error("runtime selection decision is invalid")]
    InvalidDecision,
    /// A persisted decision was presented for another run generation.
    #[cfg(test)]
    #[error(
        "persisted runtime authority generation mismatch: expected {expected}, observed {observed}"
    )]
    PersistedDecisionGenerationMismatch {
        /// Generation owned by the admitted run.
        expected: RuntimeGeneration,
        /// Generation stored in the persisted decision.
        observed: RuntimeGeneration,
    },
}

#[cfg(test)]
mod tests {
    use palyra_common::runtime_contracts::{
        RuntimeRunId, RuntimeSessionId, RuntimeTraceId, RUNTIME_IDENTITY_SET_SCHEMA_VERSION,
    };
    use serde_json::json;

    use super::*;
    use crate::application::runtime_kernel_v2::profile::{
        RuntimeKernelCompatibilityOverridesV1, RuntimeKernelProfileConfigV1,
    };

    fn identities(session: &str) -> RuntimeIdentitySetV1 {
        RuntimeIdentitySetV1::for_run(
            RuntimeTraceId::parse("trace_01").expect("test trace id is valid"),
            RuntimeSessionId::parse(session).expect("test session id is valid"),
            RuntimeRunId::parse("run_01").expect("test run id is valid"),
            RuntimeGeneration::new(7).expect("test generation is non-zero"),
        )
    }

    fn profile(version: RuntimeKernelVersion, basis_points: u16) -> RuntimeKernelProfileConfigV1 {
        RuntimeKernelProfileConfigV1::new(
            version,
            basis_points,
            RuntimeKernelCompatibilityOverridesV1::none(),
        )
        .expect("test profile should validate")
    }

    #[test]
    fn canary_assignment_is_session_pinned_and_domain_separated() {
        let selector =
            SessionCanarySelector::new(5_000, &[1; 32]).expect("selector should validate");
        let session = identities("session_alpha");

        let first = selector.assignment_value(&session, None).expect("assignment should resolve");
        for _ in 0..32 {
            assert_eq!(
                selector.assignment_value(&session, None).expect("assignment should resolve"),
                first
            );
        }

        let other_key =
            SessionCanarySelector::new(5_000, &[2; 32]).expect("selector should validate");
        assert_ne!(
            other_key.assignment_value(&session, None).expect("assignment should resolve"),
            first
        );
        assert!(!format!("{selector:?}").contains("[1, 1"));
    }

    #[test]
    fn canary_assignment_has_no_sixteen_bit_modulo_bias() {
        let selector =
            SessionCanarySelector::new(5_000, &[3; 32]).expect("selector should validate");
        let selected = (0..40_000)
            .filter(|index| {
                selector
                    .includes(&identities(format!("session_distribution_{index}").as_str()), None)
                    .expect("assignment should resolve")
            })
            .count();

        assert!(
            (19_200..=20_800).contains(&selected),
            "50% allocation selected {selected} of 40000 deterministic sessions"
        );
    }

    #[test]
    fn canary_decision_never_exposes_raw_identity_hash_or_bucket() {
        let selector =
            SessionCanarySelector::new(5_000, &[4; 32]).expect("selector should validate");
        let identities = identities("session_high_cardinality_123");
        let decision = resolve_runtime_authority(
            &profile(RuntimeKernelVersion::V2Canary, 5_000),
            &identities,
            V2RuntimeAvailability::Ready,
            RuntimeAuthorityProgressEvidence::pristine(),
            Some(&selector),
        )
        .expect("selection should succeed");

        let encoded = serde_json::to_string(&decision).expect("decision should serialize");
        assert!(!encoded.contains(identities.session_id.as_str()));
        assert!(!encoded.contains("bucket"));
        assert!(!encoded.contains("sha256"));
        assert_eq!(decision.generation(), identities.generation);
    }

    #[test]
    fn explicit_v2_unavailability_blocks_instead_of_falling_back() {
        let identities = identities("session_01");
        let decision = resolve_runtime_authority(
            &profile(RuntimeKernelVersion::V2, 0),
            &identities,
            V2RuntimeAvailability::Unavailable(V2UnavailabilityReason::Quarantined),
            RuntimeAuthorityProgressEvidence::pristine(),
            None,
        )
        .expect("selection should produce an explicit blocked decision");

        assert_eq!(decision.selected_runtime(), None);
        assert_eq!(decision.reason(), RuntimeAuthorityReason::V2UnavailableNoLegacyFallback);
        assert_eq!(decision.reason_code(), "runtime.selection.v2_unavailable_no_legacy_fallback");
    }

    #[test]
    fn persisted_authority_is_reused_across_config_and_health_changes() {
        let identities = identities("session_pinned");
        let persisted = resolve_runtime_authority(
            &profile(RuntimeKernelVersion::V2, 0),
            &identities,
            V2RuntimeAvailability::Ready,
            RuntimeAuthorityProgressEvidence::pristine(),
            None,
        )
        .expect("initial selection should succeed");

        let reused = resolve_or_reuse_runtime_authority(
            &profile(RuntimeKernelVersion::Legacy, 0),
            &identities,
            Some(&persisted),
            V2RuntimeAvailability::Unavailable(V2UnavailabilityReason::Quarantined),
            RuntimeAuthorityProgressEvidence::new(true, Some(SideEffectFenceState::EffectObserved)),
            None,
        )
        .expect("persisted selection must remain authoritative");

        assert_eq!(reused, persisted);
    }

    #[test]
    fn partial_output_and_side_effects_have_distinct_no_fallback_reasons() {
        let identities = identities("session_01");
        let unavailable = V2RuntimeAvailability::Unavailable(V2UnavailabilityReason::Unhealthy);
        let after_output = resolve_runtime_authority(
            &profile(RuntimeKernelVersion::V2, 0),
            &identities,
            unavailable,
            RuntimeAuthorityProgressEvidence::new(true, None),
            None,
        )
        .expect("selection should block after output");
        assert_eq!(after_output.reason(), RuntimeAuthorityReason::V2UnavailableAfterPartialOutput);

        let after_effect = resolve_runtime_authority(
            &profile(RuntimeKernelVersion::V2, 0),
            &identities,
            unavailable,
            RuntimeAuthorityProgressEvidence::new(true, Some(SideEffectFenceState::EffectUnknown)),
            None,
        )
        .expect("selection should block after a side effect");
        assert_eq!(
            after_effect.reason(),
            RuntimeAuthorityReason::V2UnavailableAfterSideEffectBoundary
        );
    }

    #[test]
    fn shadow_profile_keeps_legacy_authority_with_explicit_reason() {
        let decision = resolve_runtime_authority(
            &profile(RuntimeKernelVersion::V2Shadow, 0),
            &identities("session_01"),
            V2RuntimeAvailability::Unavailable(V2UnavailabilityReason::NotReady),
            RuntimeAuthorityProgressEvidence::pristine(),
            None,
        )
        .expect("shadow profile should select its authoritative path");

        assert_eq!(decision.selected_runtime(), Some(RuntimeAuthority::Legacy));
        assert!(decision.shadow_evaluation_enabled());
        assert_eq!(decision.reason(), RuntimeAuthorityReason::V2ShadowLegacyAuthority);
    }

    #[test]
    fn canary_requires_matching_selector_configuration() {
        let config = profile(RuntimeKernelVersion::V2Canary, 500);
        let missing = resolve_runtime_authority(
            &config,
            &identities("session_01"),
            V2RuntimeAvailability::Ready,
            RuntimeAuthorityProgressEvidence::pristine(),
            None,
        );
        assert_eq!(missing, Err(RuntimeAuthorityError::MissingCanarySelector));

        let selector = SessionCanarySelector::new(501, &[5; 32]).expect("selector should validate");
        let mismatch = resolve_runtime_authority(
            &config,
            &identities("session_01"),
            V2RuntimeAvailability::Ready,
            RuntimeAuthorityProgressEvidence::pristine(),
            Some(&selector),
        );
        assert!(matches!(mismatch, Err(RuntimeAuthorityError::CanaryAllocationMismatch { .. })));
    }

    #[test]
    fn decision_deserialization_rejects_forged_legacy_fallback() {
        let forged = json!({
            "schema_version": 1,
            "profile": "v2",
            "generation": 7,
            "disposition": "selected",
            "selected_runtime": "legacy",
            "shadow_evaluation_enabled": false,
            "reason": "legacy_profile_selected",
            "reason_code": "runtime.selection.legacy_profile_selected"
        });

        assert!(serde_json::from_value::<RuntimeAuthorityDecisionV1>(forged).is_err());
    }

    #[test]
    fn stable_selection_serialization_is_identity_free_and_validated() {
        let decision = resolve_runtime_authority(
            &profile(RuntimeKernelVersion::V2, 0),
            &identities("session_serialize"),
            V2RuntimeAvailability::Ready,
            RuntimeAuthorityProgressEvidence::pristine(),
            None,
        )
        .expect("selection should succeed");

        let encoded = serde_json::to_string(&decision).expect("decision should serialize");
        assert_eq!(
            encoded,
            concat!(
                r#"{"schema_version":1,"profile":"v2","generation":7,"#,
                r#""disposition":"selected","selected_runtime":"v2","#,
                r#""shadow_evaluation_enabled":false,"reason":"v2_profile_selected","#,
                r#""reason_code":"runtime.selection.v2_profile_selected"}"#
            )
        );
        let decoded: RuntimeAuthorityDecisionV1 =
            serde_json::from_str(encoded.as_str()).expect("decision should validate");
        assert_eq!(decoded, decision);
    }

    #[test]
    fn invalid_identity_set_fails_before_hashing_or_selection() {
        let mut invalid = identities("session_01");
        invalid.schema_version = RUNTIME_IDENTITY_SET_SCHEMA_VERSION + 1;

        assert!(matches!(
            resolve_runtime_authority(
                &profile(RuntimeKernelVersion::Legacy, 0),
                &invalid,
                V2RuntimeAvailability::Ready,
                RuntimeAuthorityProgressEvidence::pristine(),
                None,
            ),
            Err(RuntimeAuthorityError::InvalidIdentities(_))
        ));
    }
}
