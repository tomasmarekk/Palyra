//! Host-side resolution of validated runtime config into dispatcher contracts.
//!
//! The resolver binds real compatibility-flag provenance, keyed sampling, and
//! existing-session policy without serializing identities, buckets, or key material.

#[cfg(test)]
use palyra_common::runtime_contracts::RuntimeIdentitySetV1;
use palyra_common::runtime_contracts::RuntimeSessionId;
use serde::Serialize;
use thiserror::Error;

use crate::config::{
    runtime_kernel::{CompatibilityBundleGeneration, CompatibilityBundleSource},
    ExistingSessionMigrationPolicy, FeatureRolloutsConfig, RuntimeKernelConfig,
    RuntimeKernelProfile, RuntimeKernelRollbackPolicy, RuntimeKernelSamplingIdentity,
    RuntimeKernelSamplingKeySource,
};
use crate::journal::run_admission::{
    JournalRuntimeAuthority, JournalRuntimeAuthorityReason, JournalRuntimeProfile,
    JournalSessionAuthorityPin,
};

#[cfg(test)]
use super::selection::{
    resolve_or_reuse_runtime_authority_for_principal, RuntimeAuthorityDecisionV1,
};
use super::{
    profile::{
        RuntimeComponentGeneration, RuntimeKernelCompatibilityOverridesV1,
        RuntimeKernelProfileConfigV1, RuntimeKernelProfileError,
    },
    selection::{
        resolve_runtime_authority_intent_for_principal, CanarySamplingIdentity,
        ResolvedRuntimeAuthorityIntent, RuntimeAuthority, RuntimeAuthorityError,
        RuntimeAuthorityProgressEvidence, SessionCanarySelector, V2RuntimeAvailability,
    },
    RuntimeKernelVersion,
};

const RESOLVED_RUNTIME_PROFILE_SCHEMA_VERSION: u32 = 1;

/// How the real historical compatibility bundle entered effective config.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum RuntimeCompatibilitySource {
    Absent,
    Config,
    Env,
    ConfigAndEnv,
}

/// Identity-free projection suitable for diagnostics and metadata traces.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) struct ResolvedRuntimeProfileV1 {
    schema_version: u32,
    profile: RuntimeKernelVersion,
    canary_basis_points: u16,
    shadow_sample_basis_points: u16,
    sampling_identity: RuntimeKernelSamplingIdentityProjection,
    existing_session_policy: ExistingSessionPolicyProjection,
    rollback_policy: RollbackPolicyProjection,
    compatibility_source: RuntimeCompatibilitySource,
    compatibility_generation: Option<RuntimeComponentGeneration>,
}

/// Redacted rollout identity class.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum RuntimeKernelSamplingIdentityProjection {
    Session,
    Principal,
}

/// Redacted existing-session migration posture.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ExistingSessionPolicyProjection {
    KeepPinned,
    MigrateAtSafeBoundary,
}

/// Redacted active-run rollback posture.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum RollbackPolicyProjection {
    FinishReadOnlySuspendMutating,
    SuspendAllAtSafeBoundary,
}

/// One atomically selected implementation generation for all pipeline components.
#[cfg(test)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) struct AtomicRuntimeComponentBundleV1 {
    context: RuntimeComponentGeneration,
    stream: RuntimeComponentGeneration,
    recovery: RuntimeComponentGeneration,
    queue: RuntimeComponentGeneration,
    hooks: RuntimeComponentGeneration,
    middleware: RuntimeComponentGeneration,
    replay: RuntimeComponentGeneration,
    delivery: RuntimeComponentGeneration,
}

#[cfg(test)]
impl AtomicRuntimeComponentBundleV1 {
    const fn complete(generation: RuntimeComponentGeneration) -> Self {
        Self {
            context: generation,
            stream: generation,
            recovery: generation,
            queue: generation,
            hooks: generation,
            middleware: generation,
            replay: generation,
            delivery: generation,
        }
    }

    /// Returns the single implementation generation shared by every component.
    #[must_use]
    pub(crate) const fn generation(&self) -> RuntimeComponentGeneration {
        self.context
    }
}

/// Existing-session evidence used before selecting a new run generation.
#[cfg(test)]
#[derive(Debug, Clone, Copy)]
pub(crate) enum ExistingSessionBinding<'a> {
    New,
    Existing { pinned_profile: Option<&'a RuntimeKernelProfileConfigV1>, at_safe_boundary: bool },
}

/// Durable session-pin posture used before a run generation exists.
pub(crate) enum ExistingSessionAuthorityBinding<'a> {
    New,
    Existing { pinned: Option<&'a JournalSessionAuthorityPin>, at_safe_boundary: bool },
}

/// Resolver outcome before a session authority migration is durably committed.
pub(crate) enum SessionAuthorityResolution {
    Use(ResolvedRuntimeAuthorityIntent),
    Migrate { expected_revision: u64, target: ResolvedRuntimeAuthorityIntent },
}

/// Fully validated dispatcher input with redacted keyed-sampling state.
pub(crate) struct RuntimeProfileResolver {
    configured_profile: RuntimeKernelProfileConfigV1,
    diagnostics: ResolvedRuntimeProfileV1,
    selector: Option<SessionCanarySelector>,
    existing_session_policy: ExistingSessionMigrationPolicy,
}

impl std::fmt::Debug for RuntimeProfileResolver {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RuntimeProfileResolver")
            .field("diagnostics", &self.diagnostics)
            .field("selector", &self.selector)
            .finish_non_exhaustive()
    }
}

impl RuntimeProfileResolver {
    /// Resolves merged config and any host-resolved secret bytes.
    ///
    /// `resolved_secret_key` is used only when config chose
    /// `sampling_key_secret_ref`; it must contain exactly 32 bytes.
    ///
    /// # Errors
    /// Returns [`RuntimeProfileResolverError`] for invalid profile matrices,
    /// compatibility bundles, or sampling-key resolution.
    pub(crate) fn resolve(
        config: &RuntimeKernelConfig,
        feature_rollouts: &FeatureRolloutsConfig,
        resolved_secret_key: Option<&[u8]>,
    ) -> Result<Self, RuntimeProfileResolverError> {
        let compatibility = crate::config::runtime_kernel::validate(config, feature_rollouts)
            .map_err(RuntimeProfileResolverError::InvalidConfig)?;
        let version = map_profile(config.profile);
        let overrides = match compatibility {
            Some(resolution) => RuntimeKernelCompatibilityOverridesV1::complete(map_generation(
                resolution.generation,
            )),
            None => RuntimeKernelCompatibilityOverridesV1::none(),
        };
        let configured_profile =
            RuntimeKernelProfileConfigV1::new(version, config.canary_basis_points, overrides)?;
        let selector = match config.profile {
            RuntimeKernelProfile::V2Shadow | RuntimeKernelProfile::V2Canary => {
                let key = sampling_key(config, resolved_secret_key)?
                    .ok_or(RuntimeProfileResolverError::SamplingKeyNotResolved)?;
                let basis_points = match config.profile {
                    RuntimeKernelProfile::V2Shadow => config.shadow_sample_basis_points,
                    RuntimeKernelProfile::V2Canary => config.canary_basis_points,
                    RuntimeKernelProfile::Legacy | RuntimeKernelProfile::V2 => {
                        return Err(RuntimeProfileResolverError::ShadowSamplingUnavailable);
                    }
                };
                Some(SessionCanarySelector::new_with_identity(
                    basis_points,
                    &key,
                    map_sampling_identity(config.sampling_identity),
                )?)
            }
            RuntimeKernelProfile::Legacy | RuntimeKernelProfile::V2 => None,
        };
        let diagnostics = ResolvedRuntimeProfileV1 {
            schema_version: RESOLVED_RUNTIME_PROFILE_SCHEMA_VERSION,
            profile: version,
            canary_basis_points: config.canary_basis_points,
            shadow_sample_basis_points: config.shadow_sample_basis_points,
            sampling_identity: map_sampling_identity_projection(config.sampling_identity),
            existing_session_policy: map_session_policy(config.existing_session_policy),
            rollback_policy: map_rollback_policy(config.rollback_policy),
            compatibility_source: compatibility
                .map(|resolution| map_compatibility_source(resolution.source))
                .unwrap_or(RuntimeCompatibilitySource::Absent),
            compatibility_generation: compatibility
                .map(|resolution| map_generation(resolution.generation)),
        };
        Ok(Self {
            configured_profile,
            diagnostics,
            selector,
            existing_session_policy: config.existing_session_policy,
        })
    }

    /// Returns the identity-free resolved-profile projection.
    #[must_use]
    pub(crate) const fn diagnostics(&self) -> &ResolvedRuntimeProfileV1 {
        &self.diagnostics
    }

    /// Chooses the closed profile for a session without changing a persisted pin.
    ///
    /// # Errors
    /// Returns [`RuntimeProfileResolverError`] when a retained pinned profile
    /// is invalid.
    #[cfg(test)]
    pub(crate) fn profile_for_session(
        &self,
        binding: ExistingSessionBinding<'_>,
    ) -> Result<RuntimeKernelProfileConfigV1, RuntimeProfileResolverError> {
        match binding {
            ExistingSessionBinding::New => self.new_session_profile(),
            ExistingSessionBinding::Existing { pinned_profile: Some(profile), .. } => {
                profile.validate()?;
                Ok(profile.clone())
            }
            ExistingSessionBinding::Existing { pinned_profile: None, at_safe_boundary } => {
                match self.existing_session_policy {
                    ExistingSessionMigrationPolicy::KeepPinned => legacy_profile(),
                    ExistingSessionMigrationPolicy::MigrateAtSafeBoundary if at_safe_boundary => {
                        Ok(self.configured_profile.clone())
                    }
                    ExistingSessionMigrationPolicy::MigrateAtSafeBoundary => legacy_profile(),
                }
            }
        }
    }

    /// Selects or reuses authority for one generation.
    ///
    /// A persisted decision wins over config reload, availability, key, and
    /// migration changes.
    ///
    /// # Errors
    /// Returns [`RuntimeProfileResolverError`] when session migration,
    /// identity-pinned sampling, or authority selection fails.
    #[cfg(test)]
    pub(crate) fn resolve_authority(
        &self,
        identities: &RuntimeIdentitySetV1,
        principal: Option<&str>,
        binding: ExistingSessionBinding<'_>,
        persisted: Option<&RuntimeAuthorityDecisionV1>,
        availability: V2RuntimeAvailability,
        progress: RuntimeAuthorityProgressEvidence,
    ) -> Result<RuntimeAuthorityDecisionV1, RuntimeProfileResolverError> {
        let profile = self.profile_for_session(binding)?;
        Ok(resolve_or_reuse_runtime_authority_for_principal(
            &profile,
            identities,
            principal,
            persisted,
            availability,
            progress,
            self.selector.as_ref(),
        )?)
    }

    /// Resolves or reuses a generation-free durable session authority.
    ///
    /// `KeepPinned` always reuses a persisted pin. At a verified safe boundary,
    /// `MigrateAtSafeBoundary` reports an exact CAS target when resolved config
    /// differs from the current durable route.
    ///
    /// # Errors
    /// Returns [`RuntimeProfileResolverError`] when the session identifier,
    /// migration posture, keyed sampling, or authority intent is invalid.
    pub(crate) fn resolve_authority_intent(
        &self,
        session_id: &RuntimeSessionId,
        principal: Option<&str>,
        binding: ExistingSessionAuthorityBinding<'_>,
        availability: V2RuntimeAvailability,
        progress: RuntimeAuthorityProgressEvidence,
    ) -> Result<SessionAuthorityResolution, RuntimeProfileResolverError> {
        if let ExistingSessionAuthorityBinding::Existing {
            pinned: Some(pinned),
            at_safe_boundary,
        } = binding
        {
            let persisted = persisted_pin_intent(pinned)?;
            if self.existing_session_policy != ExistingSessionMigrationPolicy::MigrateAtSafeBoundary
                || !at_safe_boundary
            {
                return Ok(SessionAuthorityResolution::Use(persisted));
            }
            let target_profile = self.new_session_profile()?;
            let target = resolve_runtime_authority_intent_for_principal(
                &target_profile,
                session_id,
                principal,
                availability,
                progress,
                self.selector.as_ref(),
            )?;
            if target.selected_runtime().is_some() && !same_authority_intent(&persisted, &target) {
                return Ok(SessionAuthorityResolution::Migrate {
                    expected_revision: pinned.revision,
                    target,
                });
            }
            return Ok(SessionAuthorityResolution::Use(persisted));
        }
        let profile = match binding {
            ExistingSessionAuthorityBinding::New => self.new_session_profile()?,
            ExistingSessionAuthorityBinding::Existing { pinned: None, at_safe_boundary } => {
                match self.existing_session_policy {
                    ExistingSessionMigrationPolicy::KeepPinned => legacy_profile()?,
                    ExistingSessionMigrationPolicy::MigrateAtSafeBoundary if at_safe_boundary => {
                        self.configured_profile.clone()
                    }
                    ExistingSessionMigrationPolicy::MigrateAtSafeBoundary => legacy_profile()?,
                }
            }
            ExistingSessionAuthorityBinding::Existing { pinned: Some(_), .. } => {
                unreachable!("persisted pin returned above")
            }
        };
        Ok(SessionAuthorityResolution::Use(resolve_runtime_authority_intent_for_principal(
            &profile,
            session_id,
            principal,
            availability,
            progress,
            self.selector.as_ref(),
        )?))
    }

    fn new_session_profile(
        &self,
    ) -> Result<RuntimeKernelProfileConfigV1, RuntimeProfileResolverError> {
        if self.configured_profile.profile() == RuntimeKernelVersion::Legacy {
            return Err(RuntimeProfileResolverError::LegacyNewSessionRetired);
        }
        Ok(self.configured_profile.clone())
    }

    /// Returns the all-legacy or all-V2 component bundle for selected authority.
    ///
    /// # Errors
    /// Returns [`RuntimeProfileResolverError::AuthorityBlocked`] when no
    /// runtime owns the generation.
    #[cfg(test)]
    pub(crate) fn component_bundle(
        &self,
        decision: &RuntimeAuthorityDecisionV1,
    ) -> Result<AtomicRuntimeComponentBundleV1, RuntimeProfileResolverError> {
        match decision.selected_runtime() {
            Some(RuntimeAuthority::Legacy) => {
                Ok(AtomicRuntimeComponentBundleV1::complete(RuntimeComponentGeneration::Legacy))
            }
            Some(RuntimeAuthority::V2) => {
                Ok(AtomicRuntimeComponentBundleV1::complete(RuntimeComponentGeneration::V2))
            }
            None => Err(RuntimeProfileResolverError::AuthorityBlocked),
        }
    }
}

fn same_authority_intent(
    left: &ResolvedRuntimeAuthorityIntent,
    right: &ResolvedRuntimeAuthorityIntent,
) -> bool {
    left.profile() == right.profile()
        && left.selected_runtime() == right.selected_runtime()
        && left.shadow_evaluation_enabled() == right.shadow_evaluation_enabled()
        && left.reason() == right.reason()
}

fn persisted_pin_intent(
    pin: &JournalSessionAuthorityPin,
) -> Result<ResolvedRuntimeAuthorityIntent, RuntimeProfileResolverError> {
    if pin.schema_version != 1
        || pin.revision == 0
        || pin.pin_sha256.len() != 64
        || !pin
            .pin_sha256
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(RuntimeProfileResolverError::InvalidPersistedPin);
    }
    let profile = match pin.configured_profile {
        JournalRuntimeProfile::Legacy => RuntimeKernelVersion::Legacy,
        JournalRuntimeProfile::V2Shadow => RuntimeKernelVersion::V2Shadow,
        JournalRuntimeProfile::V2Canary => RuntimeKernelVersion::V2Canary,
        JournalRuntimeProfile::V2 => RuntimeKernelVersion::V2,
    };
    let selected_runtime = match pin.selected_runtime {
        JournalRuntimeAuthority::Legacy => RuntimeAuthority::Legacy,
        JournalRuntimeAuthority::V2 => RuntimeAuthority::V2,
    };
    let reason = match pin.reason {
        JournalRuntimeAuthorityReason::LegacyProfileSelected => {
            super::selection::RuntimeAuthorityReason::LegacyProfileSelected
        }
        JournalRuntimeAuthorityReason::V2ShadowLegacyAuthority => {
            super::selection::RuntimeAuthorityReason::V2ShadowLegacyAuthority
        }
        JournalRuntimeAuthorityReason::V2CanarySessionExcluded => {
            super::selection::RuntimeAuthorityReason::V2CanarySessionExcluded
        }
        JournalRuntimeAuthorityReason::V2CanarySessionSelected => {
            super::selection::RuntimeAuthorityReason::V2CanarySessionSelected
        }
        JournalRuntimeAuthorityReason::V2ProfileSelected => {
            super::selection::RuntimeAuthorityReason::V2ProfileSelected
        }
    };
    Ok(ResolvedRuntimeAuthorityIntent::from_persisted_pin(
        profile,
        selected_runtime,
        pin.shadow_evaluation_enabled,
        reason,
    )?)
}

fn legacy_profile() -> Result<RuntimeKernelProfileConfigV1, RuntimeProfileResolverError> {
    Ok(RuntimeKernelProfileConfigV1::new(
        RuntimeKernelVersion::Legacy,
        0,
        RuntimeKernelCompatibilityOverridesV1::none(),
    )?)
}

fn sampling_key(
    config: &RuntimeKernelConfig,
    resolved_secret_key: Option<&[u8]>,
) -> Result<Option<[u8; 32]>, RuntimeProfileResolverError> {
    match &config.sampling_key_source {
        None => Ok(None),
        Some(RuntimeKernelSamplingKeySource::Inline(key)) => Ok(Some(*key.expose_bytes())),
        Some(RuntimeKernelSamplingKeySource::SecretRef(_)) => {
            let resolved =
                resolved_secret_key.ok_or(RuntimeProfileResolverError::SamplingKeyNotResolved)?;
            let key = <[u8; 32]>::try_from(resolved)
                .map_err(|_| RuntimeProfileResolverError::InvalidSamplingKeyLength)?;
            Ok(Some(key))
        }
    }
}

const fn map_profile(profile: RuntimeKernelProfile) -> RuntimeKernelVersion {
    match profile {
        RuntimeKernelProfile::Legacy => RuntimeKernelVersion::Legacy,
        RuntimeKernelProfile::V2Shadow => RuntimeKernelVersion::V2Shadow,
        RuntimeKernelProfile::V2Canary => RuntimeKernelVersion::V2Canary,
        RuntimeKernelProfile::V2 => RuntimeKernelVersion::V2,
    }
}

const fn map_generation(generation: CompatibilityBundleGeneration) -> RuntimeComponentGeneration {
    match generation {
        CompatibilityBundleGeneration::Legacy => RuntimeComponentGeneration::Legacy,
        CompatibilityBundleGeneration::V2 => RuntimeComponentGeneration::V2,
    }
}

const fn map_compatibility_source(source: CompatibilityBundleSource) -> RuntimeCompatibilitySource {
    match source {
        CompatibilityBundleSource::Config => RuntimeCompatibilitySource::Config,
        CompatibilityBundleSource::Env => RuntimeCompatibilitySource::Env,
        CompatibilityBundleSource::ConfigAndEnv => RuntimeCompatibilitySource::ConfigAndEnv,
    }
}

const fn map_sampling_identity(identity: RuntimeKernelSamplingIdentity) -> CanarySamplingIdentity {
    match identity {
        RuntimeKernelSamplingIdentity::Session => CanarySamplingIdentity::Session,
        RuntimeKernelSamplingIdentity::Principal => CanarySamplingIdentity::Principal,
    }
}

const fn map_sampling_identity_projection(
    identity: RuntimeKernelSamplingIdentity,
) -> RuntimeKernelSamplingIdentityProjection {
    match identity {
        RuntimeKernelSamplingIdentity::Session => RuntimeKernelSamplingIdentityProjection::Session,
        RuntimeKernelSamplingIdentity::Principal => {
            RuntimeKernelSamplingIdentityProjection::Principal
        }
    }
}

const fn map_session_policy(
    policy: ExistingSessionMigrationPolicy,
) -> ExistingSessionPolicyProjection {
    match policy {
        ExistingSessionMigrationPolicy::KeepPinned => ExistingSessionPolicyProjection::KeepPinned,
        ExistingSessionMigrationPolicy::MigrateAtSafeBoundary => {
            ExistingSessionPolicyProjection::MigrateAtSafeBoundary
        }
    }
}

const fn map_rollback_policy(policy: RuntimeKernelRollbackPolicy) -> RollbackPolicyProjection {
    match policy {
        RuntimeKernelRollbackPolicy::FinishReadOnlySuspendMutating => {
            RollbackPolicyProjection::FinishReadOnlySuspendMutating
        }
        RuntimeKernelRollbackPolicy::SuspendAllAtSafeBoundary => {
            RollbackPolicyProjection::SuspendAllAtSafeBoundary
        }
    }
}

/// Fail-closed profile resolution error.
#[derive(Debug, Error)]
pub(crate) enum RuntimeProfileResolverError {
    /// Merged daemon config is invalid.
    #[error("runtime profile config is invalid")]
    InvalidConfig(#[source] anyhow::Error),
    /// Closed profile validation failed.
    #[error(transparent)]
    InvalidProfile(#[from] RuntimeKernelProfileError),
    /// Authority selection failed.
    #[error(transparent)]
    Authority(#[from] RuntimeAuthorityError),
    /// Secret-backed sampling key was not resolved by the host.
    #[error("runtime sampling key secret was not resolved")]
    SamplingKeyNotResolved,
    /// Resolved sampling key was not exactly 32 bytes.
    #[error("runtime sampling key must resolve to exactly 32 bytes")]
    InvalidSamplingKeyLength,
    /// No implementation owns a blocked generation.
    #[cfg(test)]
    #[error("runtime authority is blocked")]
    AuthorityBlocked,
    /// Shadow sampling was requested outside a configured shadow profile.
    #[error("runtime shadow sampling is unavailable")]
    ShadowSamplingUnavailable,
    /// Persisted session authority pin failed the application contract.
    #[error("persisted session authority pin is invalid")]
    InvalidPersistedPin,
    /// The retired legacy profile may read existing session state, but cannot
    /// acquire authority for a newly admitted session.
    #[error(
        "runtime_kernel.profile=legacy is compatibility-only and cannot admit new sessions; use runtime_kernel.profile=v2 or roll back the release"
    )]
    LegacyNewSessionRetired,
}

#[cfg(test)]
mod tests;
