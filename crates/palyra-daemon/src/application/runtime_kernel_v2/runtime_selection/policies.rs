//! Content-addressed fallback, override, capability, and selector/config epoch policy.
//!
//! Every policy digest is computed from its closed canonical fields. Callers
//! cannot substitute one generic evidence digest for another policy domain.

use palyra_common::runtime_contracts::RuntimeAuthorityClass;
use serde::{Deserialize, Deserializer, Serialize};

use super::{
    bounded::{BoundedVec, SafeLabel},
    digest::{digest_serializable, SelectionDigest},
    service::RuntimeSelectionError,
};

pub(super) const MAX_CAPABILITIES_PER_COMPONENT: usize = 32;
pub(super) const MAX_REQUIRED_TOOLS: usize = 128;
const MAX_OVERRIDE_ITEMS: usize = 32;
const FALLBACK_POLICY_DOMAIN: &[u8] = b"palyra.runtime_selection.fallback_policy.v1\0";
const OVERRIDE_POLICY_DOMAIN: &[u8] = b"palyra.runtime_selection.override_policy.v1\0";
const CAPABILITY_REQUIREMENTS_DOMAIN: &[u8] =
    b"palyra.runtime_selection.capability_requirements.v1\0";
const SELECTION_EPOCHS_DOMAIN: &[u8] = b"palyra.runtime_selection.epochs.v1\0";

/// Explicit permission for one fallback family.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum FallbackPermissionV1 {
    /// No fallback in this family.
    Forbidden,
    /// Fallback is permitted only before partial output or effect start.
    BeforeProgress,
}

/// Closed fallback policy with a dedicated canonical digest.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RuntimeFallbackPolicyV1 {
    external_to_embedded: FallbackPermissionV1,
    provider_route: FallbackPermissionV1,
    same_or_lower_authority_only: bool,
    policy_digest: SelectionDigest,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RuntimeFallbackPolicyWire {
    external_to_embedded: FallbackPermissionV1,
    provider_route: FallbackPermissionV1,
    same_or_lower_authority_only: bool,
    policy_digest: SelectionDigest,
}

#[derive(Serialize)]
struct RuntimeFallbackPolicyPayload {
    external_to_embedded: FallbackPermissionV1,
    provider_route: FallbackPermissionV1,
    same_or_lower_authority_only: bool,
}

impl<'de> Deserialize<'de> for RuntimeFallbackPolicyV1 {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = RuntimeFallbackPolicyWire::deserialize(deserializer)?;
        let policy = Self {
            external_to_embedded: wire.external_to_embedded,
            provider_route: wire.provider_route,
            same_or_lower_authority_only: wire.same_or_lower_authority_only,
            policy_digest: wire.policy_digest,
        };
        policy.validate().map_err(serde::de::Error::custom)?;
        Ok(policy)
    }
}

impl RuntimeFallbackPolicyV1 {
    /// Creates a closed fallback policy and computes its dedicated digest.
    ///
    /// # Errors
    /// Returns [`RuntimeSelectionError::AuthorityEscalation`] unless the
    /// same-or-lower-authority invariant remains enabled.
    pub(crate) fn new(
        external_to_embedded: FallbackPermissionV1,
        provider_route: FallbackPermissionV1,
    ) -> Result<Self, RuntimeSelectionError> {
        let payload = RuntimeFallbackPolicyPayload {
            external_to_embedded,
            provider_route,
            same_or_lower_authority_only: true,
        };
        Ok(Self {
            external_to_embedded,
            provider_route,
            same_or_lower_authority_only: true,
            policy_digest: digest_serializable(FALLBACK_POLICY_DOMAIN, &payload)?,
        })
    }

    /// Returns external-to-embedded permission.
    #[must_use]
    #[cfg(test)]
    pub(crate) const fn external_to_embedded(&self) -> FallbackPermissionV1 {
        self.external_to_embedded
    }

    /// Returns provider-route permission.
    #[must_use]
    pub(crate) const fn provider_route(&self) -> FallbackPermissionV1 {
        self.provider_route
    }

    /// Returns the dedicated policy digest.
    #[must_use]
    pub(crate) const fn digest(&self) -> &SelectionDigest {
        &self.policy_digest
    }

    pub(super) fn validate(&self) -> Result<(), RuntimeSelectionError> {
        if !self.same_or_lower_authority_only {
            return Err(RuntimeSelectionError::AuthorityEscalation);
        }
        let expected = digest_serializable(
            FALLBACK_POLICY_DOMAIN,
            &RuntimeFallbackPolicyPayload {
                external_to_embedded: self.external_to_embedded,
                provider_route: self.provider_route,
                same_or_lower_authority_only: self.same_or_lower_authority_only,
            },
        )?;
        if self.policy_digest != expected {
            return Err(RuntimeSelectionError::DigestMismatch);
        }
        Ok(())
    }
}

/// Optional per-session runtime preferences.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct SessionRuntimeOverridesV1 {
    /// Requested harness id.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) harness_id: Option<SafeLabel>,
    /// Requested context-engine id.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) context_engine_id: Option<SafeLabel>,
    /// Requested provider route reference digest.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) provider_route_reference_sha256: Option<SelectionDigest>,
    /// Requested execution-profile id.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) execution_profile_id: Option<SafeLabel>,
}

impl SessionRuntimeOverridesV1 {
    fn is_empty(&self) -> bool {
        self.harness_id.is_none()
            && self.context_engine_id.is_none()
            && self.provider_route_reference_sha256.is_none()
            && self.execution_profile_id.is_none()
    }
}

/// Whether session overrides are denied or constrained by allowlists.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum SessionOverrideModeV1 {
    /// Reject every non-empty override.
    DenyAll,
    /// Require every requested value to be explicitly allowlisted.
    AllowListed,
}

/// Session override policy with a dedicated canonical digest.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct SessionOverridePolicyV1 {
    mode: SessionOverrideModeV1,
    requested: Option<SessionRuntimeOverridesV1>,
    allowed_harness_ids: BoundedVec<SafeLabel, MAX_OVERRIDE_ITEMS>,
    allowed_context_engine_ids: BoundedVec<SafeLabel, MAX_OVERRIDE_ITEMS>,
    allowed_provider_routes: BoundedVec<SelectionDigest, MAX_OVERRIDE_ITEMS>,
    allowed_execution_profile_ids: BoundedVec<SafeLabel, MAX_OVERRIDE_ITEMS>,
    authority_ceiling: RuntimeAuthorityClass,
    policy_digest: SelectionDigest,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct SessionOverridePolicyPayload {
    mode: SessionOverrideModeV1,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    requested: Option<SessionRuntimeOverridesV1>,
    allowed_harness_ids: BoundedVec<SafeLabel, MAX_OVERRIDE_ITEMS>,
    allowed_context_engine_ids: BoundedVec<SafeLabel, MAX_OVERRIDE_ITEMS>,
    allowed_provider_routes: BoundedVec<SelectionDigest, MAX_OVERRIDE_ITEMS>,
    allowed_execution_profile_ids: BoundedVec<SafeLabel, MAX_OVERRIDE_ITEMS>,
    authority_ceiling: RuntimeAuthorityClass,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SessionOverridePolicyWire {
    mode: SessionOverrideModeV1,
    #[serde(default)]
    requested: Option<SessionRuntimeOverridesV1>,
    allowed_harness_ids: BoundedVec<SafeLabel, MAX_OVERRIDE_ITEMS>,
    allowed_context_engine_ids: BoundedVec<SafeLabel, MAX_OVERRIDE_ITEMS>,
    allowed_provider_routes: BoundedVec<SelectionDigest, MAX_OVERRIDE_ITEMS>,
    allowed_execution_profile_ids: BoundedVec<SafeLabel, MAX_OVERRIDE_ITEMS>,
    authority_ceiling: RuntimeAuthorityClass,
    policy_digest: SelectionDigest,
}

impl<'de> Deserialize<'de> for SessionOverridePolicyV1 {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = SessionOverridePolicyWire::deserialize(deserializer)?;
        let policy = Self {
            mode: wire.mode,
            requested: wire.requested,
            allowed_harness_ids: wire.allowed_harness_ids,
            allowed_context_engine_ids: wire.allowed_context_engine_ids,
            allowed_provider_routes: wire.allowed_provider_routes,
            allowed_execution_profile_ids: wire.allowed_execution_profile_ids,
            authority_ceiling: wire.authority_ceiling,
            policy_digest: wire.policy_digest,
        };
        policy.validate_digest().map_err(serde::de::Error::custom)?;
        Ok(policy)
    }
}

impl SessionOverridePolicyV1 {
    /// Creates a deny-all override policy.
    ///
    /// # Errors
    /// Returns [`RuntimeSelectionError::Serialization`] if canonical hashing fails.
    pub(crate) fn deny_all(
        authority_ceiling: RuntimeAuthorityClass,
    ) -> Result<Self, RuntimeSelectionError> {
        Self::new(
            SessionOverrideModeV1::DenyAll,
            None,
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            authority_ceiling,
        )
    }

    /// Creates and content-addresses an override policy.
    ///
    /// # Errors
    /// Returns [`RuntimeSelectionError::InvalidOverridePolicy`] for oversized
    /// lists, duplicate allowlist entries, or a denied non-empty request.
    pub(crate) fn new(
        mode: SessionOverrideModeV1,
        requested: Option<SessionRuntimeOverridesV1>,
        allowed_harness_ids: Vec<SafeLabel>,
        allowed_context_engine_ids: Vec<SafeLabel>,
        allowed_provider_routes: Vec<SelectionDigest>,
        allowed_execution_profile_ids: Vec<SafeLabel>,
        authority_ceiling: RuntimeAuthorityClass,
    ) -> Result<Self, RuntimeSelectionError> {
        let mut policy = Self {
            mode,
            requested,
            allowed_harness_ids: BoundedVec::try_new(allowed_harness_ids)
                .map_err(|_| RuntimeSelectionError::InvalidOverridePolicy)?,
            allowed_context_engine_ids: BoundedVec::try_new(allowed_context_engine_ids)
                .map_err(|_| RuntimeSelectionError::InvalidOverridePolicy)?,
            allowed_provider_routes: BoundedVec::try_new(allowed_provider_routes)
                .map_err(|_| RuntimeSelectionError::InvalidOverridePolicy)?,
            allowed_execution_profile_ids: BoundedVec::try_new(allowed_execution_profile_ids)
                .map_err(|_| RuntimeSelectionError::InvalidOverridePolicy)?,
            authority_ceiling,
            policy_digest: SelectionDigest::from_domain_bytes(OVERRIDE_POLICY_DOMAIN, b"unsealed"),
        };
        policy.validate_semantics()?;
        policy.policy_digest = policy.computed_digest()?;
        Ok(policy)
    }

    /// Returns requested overrides, when present.
    #[must_use]
    pub(crate) const fn requested(&self) -> Option<&SessionRuntimeOverridesV1> {
        self.requested.as_ref()
    }

    /// Returns the dedicated override-policy digest.
    #[must_use]
    pub(crate) const fn digest(&self) -> &SelectionDigest {
        &self.policy_digest
    }

    pub(super) fn validates_request(&self) -> bool {
        let Some(requested) = self.requested.as_ref() else {
            return true;
        };
        if requested.is_empty() {
            return true;
        }
        self.mode == SessionOverrideModeV1::AllowListed
            && requested
                .harness_id
                .as_ref()
                .is_none_or(|value| self.allowed_harness_ids.contains(value))
            && requested
                .context_engine_id
                .as_ref()
                .is_none_or(|value| self.allowed_context_engine_ids.contains(value))
            && requested
                .provider_route_reference_sha256
                .as_ref()
                .is_none_or(|value| self.allowed_provider_routes.contains(value))
            && requested
                .execution_profile_id
                .as_ref()
                .is_none_or(|value| self.allowed_execution_profile_ids.contains(value))
    }

    pub(super) fn validate(
        &self,
        admission_ceiling: RuntimeAuthorityClass,
    ) -> Result<(), RuntimeSelectionError> {
        self.validate_semantics()?;
        self.validate_digest()?;
        if !admission_ceiling.permits_fallback(self.authority_ceiling) {
            return Err(RuntimeSelectionError::AuthorityEscalation);
        }
        Ok(())
    }

    fn validate_semantics(&self) -> Result<(), RuntimeSelectionError> {
        if !self.validates_request()
            || has_duplicates(&self.allowed_harness_ids)
            || has_duplicates(&self.allowed_context_engine_ids)
            || has_duplicates(&self.allowed_provider_routes)
            || has_duplicates(&self.allowed_execution_profile_ids)
        {
            return Err(RuntimeSelectionError::InvalidOverridePolicy);
        }
        Ok(())
    }

    fn validate_digest(&self) -> Result<(), RuntimeSelectionError> {
        if self.policy_digest != self.computed_digest()? {
            return Err(RuntimeSelectionError::DigestMismatch);
        }
        Ok(())
    }

    fn computed_digest(&self) -> Result<SelectionDigest, RuntimeSelectionError> {
        digest_serializable(
            OVERRIDE_POLICY_DOMAIN,
            &SessionOverridePolicyPayload {
                mode: self.mode,
                requested: self.requested.clone(),
                allowed_harness_ids: self.allowed_harness_ids.clone(),
                allowed_context_engine_ids: self.allowed_context_engine_ids.clone(),
                allowed_provider_routes: self.allowed_provider_routes.clone(),
                allowed_execution_profile_ids: self.allowed_execution_profile_ids.clone(),
                authority_ceiling: self.authority_ceiling,
            },
        )
    }
}

/// Required component capabilities and model-visible tools.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RuntimeCapabilityRequirementsV1 {
    harness: BoundedVec<SafeLabel, MAX_CAPABILITIES_PER_COMPONENT>,
    context_engine: BoundedVec<SafeLabel, MAX_CAPABILITIES_PER_COMPONENT>,
    provider: BoundedVec<SafeLabel, MAX_CAPABILITIES_PER_COMPONENT>,
    required_tool_names: BoundedVec<SafeLabel, MAX_REQUIRED_TOOLS>,
    requirements_digest: SelectionDigest,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RuntimeCapabilityRequirementsPayload {
    harness: BoundedVec<SafeLabel, MAX_CAPABILITIES_PER_COMPONENT>,
    context_engine: BoundedVec<SafeLabel, MAX_CAPABILITIES_PER_COMPONENT>,
    provider: BoundedVec<SafeLabel, MAX_CAPABILITIES_PER_COMPONENT>,
    required_tool_names: BoundedVec<SafeLabel, MAX_REQUIRED_TOOLS>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RuntimeCapabilityRequirementsWire {
    harness: BoundedVec<SafeLabel, MAX_CAPABILITIES_PER_COMPONENT>,
    context_engine: BoundedVec<SafeLabel, MAX_CAPABILITIES_PER_COMPONENT>,
    provider: BoundedVec<SafeLabel, MAX_CAPABILITIES_PER_COMPONENT>,
    required_tool_names: BoundedVec<SafeLabel, MAX_REQUIRED_TOOLS>,
    requirements_digest: SelectionDigest,
}

impl<'de> Deserialize<'de> for RuntimeCapabilityRequirementsV1 {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = RuntimeCapabilityRequirementsWire::deserialize(deserializer)?;
        let requirements = Self {
            harness: wire.harness,
            context_engine: wire.context_engine,
            provider: wire.provider,
            required_tool_names: wire.required_tool_names,
            requirements_digest: wire.requirements_digest,
        };
        requirements.validate().map_err(serde::de::Error::custom)?;
        Ok(requirements)
    }
}

impl RuntimeCapabilityRequirementsV1 {
    /// Creates bounded, sorted capability requirements with a dedicated digest.
    ///
    /// # Errors
    /// Returns [`RuntimeSelectionError::InvalidCapabilityRequirements`] for
    /// oversized or duplicate requirement lists.
    pub(crate) fn new(
        harness: Vec<SafeLabel>,
        context_engine: Vec<SafeLabel>,
        provider: Vec<SafeLabel>,
        required_tool_names: Vec<SafeLabel>,
    ) -> Result<Self, RuntimeSelectionError> {
        let mut requirements = Self {
            harness: bounded(harness)?,
            context_engine: bounded(context_engine)?,
            provider: bounded(provider)?,
            required_tool_names: BoundedVec::try_new(required_tool_names)
                .map_err(|_| RuntimeSelectionError::InvalidCapabilityRequirements)?,
            requirements_digest: SelectionDigest::from_domain_bytes(
                CAPABILITY_REQUIREMENTS_DOMAIN,
                b"unsealed",
            ),
        };
        requirements.sort_and_validate()?;
        requirements.requirements_digest = requirements.computed_digest()?;
        Ok(requirements)
    }

    /// Returns harness requirements.
    #[must_use]
    pub(crate) fn harness(&self) -> &[SafeLabel] {
        &self.harness
    }

    /// Returns context-engine requirements.
    #[must_use]
    pub(crate) fn context_engine(&self) -> &[SafeLabel] {
        &self.context_engine
    }

    /// Returns provider requirements.
    #[must_use]
    pub(crate) fn provider(&self) -> &[SafeLabel] {
        &self.provider
    }

    /// Returns required model-visible tool names.
    #[must_use]
    pub(crate) fn required_tool_names(&self) -> &[SafeLabel] {
        &self.required_tool_names
    }

    /// Returns the dedicated requirements digest.
    #[must_use]
    pub(crate) const fn digest(&self) -> &SelectionDigest {
        &self.requirements_digest
    }

    pub(super) fn validate(&self) -> Result<(), RuntimeSelectionError> {
        if has_duplicates(&self.harness)
            || has_duplicates(&self.context_engine)
            || has_duplicates(&self.provider)
            || has_duplicates(&self.required_tool_names)
            || self.requirements_digest != self.computed_digest()?
        {
            return Err(RuntimeSelectionError::DigestMismatch);
        }
        Ok(())
    }

    fn sort_and_validate(&mut self) -> Result<(), RuntimeSelectionError> {
        self.harness.sort();
        self.context_engine.sort();
        self.provider.sort();
        self.required_tool_names.sort();
        if has_duplicates(&self.harness)
            || has_duplicates(&self.context_engine)
            || has_duplicates(&self.provider)
            || has_duplicates(&self.required_tool_names)
        {
            return Err(RuntimeSelectionError::InvalidCapabilityRequirements);
        }
        Ok(())
    }

    fn computed_digest(&self) -> Result<SelectionDigest, RuntimeSelectionError> {
        digest_serializable(
            CAPABILITY_REQUIREMENTS_DOMAIN,
            &RuntimeCapabilityRequirementsPayload {
                harness: self.harness.clone(),
                context_engine: self.context_engine.clone(),
                provider: self.provider.clone(),
                required_tool_names: self.required_tool_names.clone(),
            },
        )
    }
}

/// Selector-registry and resolved-config epochs with a dedicated digest.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct SelectionEpochsV1 {
    selector_epoch: u64,
    config_epoch: u64,
    epochs_digest: SelectionDigest,
}

#[derive(Serialize)]
struct SelectionEpochsPayload {
    selector_epoch: u64,
    config_epoch: u64,
}

impl SelectionEpochsV1 {
    /// Creates non-zero selector/config epochs and their canonical digest.
    ///
    /// # Errors
    /// Returns [`RuntimeSelectionError::InvalidEpochs`] for a zero epoch.
    pub(crate) fn new(
        selector_epoch: u64,
        config_epoch: u64,
    ) -> Result<Self, RuntimeSelectionError> {
        if selector_epoch == 0 || config_epoch == 0 {
            return Err(RuntimeSelectionError::InvalidEpochs);
        }
        Ok(Self {
            selector_epoch,
            config_epoch,
            epochs_digest: digest_serializable(
                SELECTION_EPOCHS_DOMAIN,
                &SelectionEpochsPayload { selector_epoch, config_epoch },
            )?,
        })
    }

    /// Returns the dedicated epoch digest.
    #[must_use]
    pub(crate) const fn digest(&self) -> &SelectionDigest {
        &self.epochs_digest
    }

    pub(super) fn validate(&self) -> Result<(), RuntimeSelectionError> {
        if self.selector_epoch == 0
            || self.config_epoch == 0
            || self.epochs_digest
                != digest_serializable(
                    SELECTION_EPOCHS_DOMAIN,
                    &SelectionEpochsPayload {
                        selector_epoch: self.selector_epoch,
                        config_epoch: self.config_epoch,
                    },
                )?
        {
            return Err(RuntimeSelectionError::DigestMismatch);
        }
        Ok(())
    }
}

fn bounded<const MAX: usize>(
    values: Vec<SafeLabel>,
) -> Result<BoundedVec<SafeLabel, MAX>, RuntimeSelectionError> {
    BoundedVec::try_new(values).map_err(|_| RuntimeSelectionError::InvalidCapabilityRequirements)
}

fn has_duplicates<T: Ord>(values: &[T]) -> bool {
    values.windows(2).any(|window| window[0] >= window[1])
}
