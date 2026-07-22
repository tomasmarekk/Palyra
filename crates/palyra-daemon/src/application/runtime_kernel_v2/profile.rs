//! Atomic runtime-profile configuration and compatibility-override validation.
//!
//! A validated value proves that every turn-pipeline component moves as one
//! legacy or V2 bundle; mixed historical flags never reach runtime selection.

use serde::{Deserialize, Serialize};
use thiserror::Error;

use super::RuntimeKernelVersion;

/// Schema version for [`RuntimeKernelProfileConfigV1`].
pub(crate) const RUNTIME_KERNEL_PROFILE_SCHEMA_VERSION: u32 = 1;
/// Number of basis points representing the complete population.
pub(crate) const CANARY_BASIS_POINTS_DENOMINATOR: u16 = 10_000;

/// Implementation generation selected for every member of an atomic profile.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum RuntimeComponentGeneration {
    /// Existing orchestration implementation.
    Legacy,
    /// Second-generation runtime-kernel implementation.
    V2,
}

/// Historical per-component settings accepted only as one coherent bundle.
///
/// Every field must be absent, or every field must name the same generation.
/// This preserves a compatibility window without admitting combinations that
/// no runtime profile tests.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) struct RuntimeKernelCompatibilityOverridesV1 {
    #[serde(skip_serializing_if = "Option::is_none")]
    context: Option<RuntimeComponentGeneration>,
    #[serde(skip_serializing_if = "Option::is_none")]
    stream: Option<RuntimeComponentGeneration>,
    #[serde(skip_serializing_if = "Option::is_none")]
    recovery: Option<RuntimeComponentGeneration>,
    #[serde(skip_serializing_if = "Option::is_none")]
    queue: Option<RuntimeComponentGeneration>,
    #[serde(skip_serializing_if = "Option::is_none")]
    hooks: Option<RuntimeComponentGeneration>,
    #[serde(skip_serializing_if = "Option::is_none")]
    middleware: Option<RuntimeComponentGeneration>,
    #[serde(skip_serializing_if = "Option::is_none")]
    replay: Option<RuntimeComponentGeneration>,
    #[serde(skip_serializing_if = "Option::is_none")]
    delivery: Option<RuntimeComponentGeneration>,
}

#[derive(Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
struct RuntimeKernelCompatibilityOverridesWire {
    #[serde(default)]
    context: Option<RuntimeComponentGeneration>,
    #[serde(default)]
    stream: Option<RuntimeComponentGeneration>,
    #[serde(default)]
    recovery: Option<RuntimeComponentGeneration>,
    #[serde(default)]
    queue: Option<RuntimeComponentGeneration>,
    #[serde(default)]
    hooks: Option<RuntimeComponentGeneration>,
    #[serde(default)]
    middleware: Option<RuntimeComponentGeneration>,
    #[serde(default)]
    replay: Option<RuntimeComponentGeneration>,
    #[serde(default)]
    delivery: Option<RuntimeComponentGeneration>,
}

impl<'de> Deserialize<'de> for RuntimeKernelCompatibilityOverridesV1 {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let wire = RuntimeKernelCompatibilityOverridesWire::deserialize(deserializer)?;
        let overrides = Self {
            context: wire.context,
            stream: wire.stream,
            recovery: wire.recovery,
            queue: wire.queue,
            hooks: wire.hooks,
            middleware: wire.middleware,
            replay: wire.replay,
            delivery: wire.delivery,
        };
        overrides.atomic_generation().map_err(serde::de::Error::custom)?;
        Ok(overrides)
    }
}

impl RuntimeKernelCompatibilityOverridesV1 {
    /// Creates an override set that delegates every component to the profile.
    #[must_use]
    pub(crate) const fn none() -> Self {
        Self {
            context: None,
            stream: None,
            recovery: None,
            queue: None,
            hooks: None,
            middleware: None,
            replay: None,
            delivery: None,
        }
    }

    /// Creates a complete compatibility bundle for one implementation generation.
    #[must_use]
    pub(crate) const fn complete(generation: RuntimeComponentGeneration) -> Self {
        Self {
            context: Some(generation),
            stream: Some(generation),
            recovery: Some(generation),
            queue: Some(generation),
            hooks: Some(generation),
            middleware: Some(generation),
            replay: Some(generation),
            delivery: Some(generation),
        }
    }

    /// Returns the override generation, or `None` when the profile owns every component.
    ///
    /// # Errors
    /// Returns [`RuntimeKernelProfileError::PartialCompatibilityOverrides`] when
    /// only some historical flags are present, or
    /// [`RuntimeKernelProfileError::MixedCompatibilityOverrides`] when present
    /// flags select different generations.
    pub(crate) fn atomic_generation(
        &self,
    ) -> Result<Option<RuntimeComponentGeneration>, RuntimeKernelProfileError> {
        let values = [
            self.context,
            self.stream,
            self.recovery,
            self.queue,
            self.hooks,
            self.middleware,
            self.replay,
            self.delivery,
        ];
        let present_count = values.iter().filter(|value| value.is_some()).count();
        if present_count == 0 {
            return Ok(None);
        }
        if present_count != values.len() {
            return Err(RuntimeKernelProfileError::PartialCompatibilityOverrides);
        }
        let Some(first) = values.first().copied().flatten() else {
            return Err(RuntimeKernelProfileError::PartialCompatibilityOverrides);
        };
        if values.iter().copied().flatten().any(|value| value != first) {
            return Err(RuntimeKernelProfileError::MixedCompatibilityOverrides);
        }
        Ok(Some(first))
    }
}

impl Default for RuntimeKernelCompatibilityOverridesV1 {
    fn default() -> Self {
        Self::none()
    }
}

/// Validated atomic runtime-profile configuration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) struct RuntimeKernelProfileConfigV1 {
    schema_version: u32,
    profile: RuntimeKernelVersion,
    canary_basis_points: u16,
    compatibility_overrides: RuntimeKernelCompatibilityOverridesV1,
}

#[derive(Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
struct RuntimeKernelProfileConfigWire {
    schema_version: u32,
    profile: RuntimeKernelVersion,
    canary_basis_points: u16,
    #[serde(default)]
    compatibility_overrides: RuntimeKernelCompatibilityOverridesV1,
}

impl<'de> Deserialize<'de> for RuntimeKernelProfileConfigV1 {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let wire = RuntimeKernelProfileConfigWire::deserialize(deserializer)?;
        Self::from_wire(wire).map_err(serde::de::Error::custom)
    }
}

impl RuntimeKernelProfileConfigV1 {
    /// Creates and validates an atomic runtime profile.
    ///
    /// # Errors
    /// Returns [`RuntimeKernelProfileError`] when the canary allocation is not
    /// valid for the selected profile, or compatibility flags do not form the
    /// profile's complete component bundle.
    pub(crate) fn new(
        profile: RuntimeKernelVersion,
        canary_basis_points: u16,
        compatibility_overrides: RuntimeKernelCompatibilityOverridesV1,
    ) -> Result<Self, RuntimeKernelProfileError> {
        let config = Self {
            schema_version: RUNTIME_KERNEL_PROFILE_SCHEMA_VERSION,
            profile,
            canary_basis_points,
            compatibility_overrides,
        };
        config.validate()?;
        Ok(config)
    }

    fn from_wire(wire: RuntimeKernelProfileConfigWire) -> Result<Self, RuntimeKernelProfileError> {
        let config = Self {
            schema_version: wire.schema_version,
            profile: wire.profile,
            canary_basis_points: wire.canary_basis_points,
            compatibility_overrides: wire.compatibility_overrides,
        };
        config.validate()?;
        Ok(config)
    }

    /// Returns the selected closed runtime profile.
    #[must_use]
    pub(crate) const fn profile(&self) -> RuntimeKernelVersion {
        self.profile
    }

    /// Returns the V2 canary allocation in basis points.
    #[must_use]
    pub(crate) const fn canary_basis_points(&self) -> u16 {
        self.canary_basis_points
    }

    /// Returns the globally uniform component generation, when the profile has one.
    ///
    /// Shadow and canary profiles are intentionally hybrid: at least one run remains
    /// legacy-authoritative while V2 evaluates or owns selected runs. Historical
    /// global component flags therefore cannot represent either profile safely.
    #[must_use]
    pub(crate) const fn component_generation(&self) -> Option<RuntimeComponentGeneration> {
        match self.profile {
            RuntimeKernelVersion::Legacy => Some(RuntimeComponentGeneration::Legacy),
            RuntimeKernelVersion::V2 => Some(RuntimeComponentGeneration::V2),
            RuntimeKernelVersion::V2Shadow | RuntimeKernelVersion::V2Canary => None,
        }
    }

    /// Validates schema, canary, and cross-component atomicity invariants.
    ///
    /// # Errors
    /// Returns [`RuntimeKernelProfileError`] for unsupported schema versions,
    /// invalid canary allocations, partial or mixed overrides, or an override
    /// bundle that conflicts with the selected profile.
    pub(crate) fn validate(&self) -> Result<(), RuntimeKernelProfileError> {
        if self.schema_version != RUNTIME_KERNEL_PROFILE_SCHEMA_VERSION {
            return Err(RuntimeKernelProfileError::UnsupportedSchemaVersion {
                observed: self.schema_version,
            });
        }
        match self.profile {
            RuntimeKernelVersion::V2Canary
                if (1..CANARY_BASIS_POINTS_DENOMINATOR).contains(&self.canary_basis_points) => {}
            RuntimeKernelVersion::V2Canary => {
                return Err(RuntimeKernelProfileError::InvalidCanaryBasisPoints {
                    profile: self.profile,
                    observed: self.canary_basis_points,
                });
            }
            RuntimeKernelVersion::Legacy
            | RuntimeKernelVersion::V2Shadow
            | RuntimeKernelVersion::V2
                if self.canary_basis_points == 0 => {}
            RuntimeKernelVersion::Legacy
            | RuntimeKernelVersion::V2Shadow
            | RuntimeKernelVersion::V2 => {
                return Err(RuntimeKernelProfileError::InvalidCanaryBasisPoints {
                    profile: self.profile,
                    observed: self.canary_basis_points,
                });
            }
        }
        if let Some(observed) = self.compatibility_overrides.atomic_generation()? {
            let Some(expected) = self.component_generation() else {
                return Err(RuntimeKernelProfileError::HybridCompatibilityOverrides {
                    profile: self.profile,
                });
            };
            if observed != expected {
                return Err(RuntimeKernelProfileError::CompatibilityOverrideConflict {
                    profile: self.profile,
                    expected,
                    observed,
                });
            }
        }
        Ok(())
    }
}

/// Fail-closed atomic profile validation error.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub(crate) enum RuntimeKernelProfileError {
    /// The serialized profile uses a schema this runtime cannot interpret.
    #[error("runtime kernel profile schema version {observed} is unsupported")]
    UnsupportedSchemaVersion {
        /// Rejected schema version.
        observed: u32,
    },
    /// The selected profile does not admit the configured canary allocation.
    #[error(
        "runtime kernel profile {profile:?} does not admit canary allocation {observed} basis points"
    )]
    InvalidCanaryBasisPoints {
        /// Selected closed profile.
        profile: RuntimeKernelVersion,
        /// Rejected allocation.
        observed: u16,
    },
    /// Only part of the historical component flag bundle was supplied.
    #[error("runtime kernel compatibility overrides must specify every component or none")]
    PartialCompatibilityOverrides,
    /// Historical component flags selected both legacy and V2.
    #[error("runtime kernel compatibility overrides cannot mix legacy and v2 components")]
    MixedCompatibilityOverrides,
    /// Historical global component flags cannot represent a shadow or canary profile.
    #[error("runtime kernel hybrid profile {profile:?} cannot use global component overrides")]
    HybridCompatibilityOverrides {
        /// Hybrid profile that rejected the override bundle.
        profile: RuntimeKernelVersion,
    },
    /// A complete historical bundle disagreed with the closed profile.
    #[error(
        "runtime kernel profile {profile:?} requires {expected:?} components, observed {observed:?}"
    )]
    CompatibilityOverrideConflict {
        /// Selected closed profile.
        profile: RuntimeKernelVersion,
        /// Component generation required by the profile.
        expected: RuntimeComponentGeneration,
        /// Component generation selected by historical flags.
        observed: RuntimeComponentGeneration,
    },
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn all_closed_profiles_accept_only_their_canary_posture() {
        for (profile, basis_points) in [
            (RuntimeKernelVersion::Legacy, 0),
            (RuntimeKernelVersion::V2Shadow, 0),
            (RuntimeKernelVersion::V2Canary, 1),
            (RuntimeKernelVersion::V2Canary, CANARY_BASIS_POINTS_DENOMINATOR - 1),
            (RuntimeKernelVersion::V2, 0),
        ] {
            RuntimeKernelProfileConfigV1::new(
                profile,
                basis_points,
                RuntimeKernelCompatibilityOverridesV1::none(),
            )
            .expect("closed profile should validate");
        }

        for (profile, basis_points) in [
            (RuntimeKernelVersion::Legacy, 1),
            (RuntimeKernelVersion::V2Shadow, 1),
            (RuntimeKernelVersion::V2Canary, 0),
            (RuntimeKernelVersion::V2Canary, CANARY_BASIS_POINTS_DENOMINATOR),
            (RuntimeKernelVersion::V2, CANARY_BASIS_POINTS_DENOMINATOR),
        ] {
            assert!(matches!(
                RuntimeKernelProfileConfigV1::new(
                    profile,
                    basis_points,
                    RuntimeKernelCompatibilityOverridesV1::none(),
                ),
                Err(RuntimeKernelProfileError::InvalidCanaryBasisPoints { .. })
            ));
        }
    }

    #[test]
    fn partial_and_mixed_historical_flags_fail_closed_during_deserialization() {
        let partial = json!({
            "context": "v2",
            "stream": "v2"
        });
        assert!(serde_json::from_value::<RuntimeKernelCompatibilityOverridesV1>(partial).is_err());

        let mixed = json!({
            "context": "v2",
            "stream": "v2",
            "recovery": "v2",
            "queue": "v2",
            "hooks": "v2",
            "middleware": "legacy",
            "replay": "v2",
            "delivery": "v2"
        });
        assert!(serde_json::from_value::<RuntimeKernelCompatibilityOverridesV1>(mixed).is_err());
    }

    #[test]
    fn complete_compatibility_bundle_must_match_profile() {
        let error = RuntimeKernelProfileConfigV1::new(
            RuntimeKernelVersion::Legacy,
            0,
            RuntimeKernelCompatibilityOverridesV1::complete(RuntimeComponentGeneration::V2),
        )
        .expect_err("legacy profile cannot activate a V2 component bundle");

        assert_eq!(
            error,
            RuntimeKernelProfileError::CompatibilityOverrideConflict {
                profile: RuntimeKernelVersion::Legacy,
                expected: RuntimeComponentGeneration::Legacy,
                observed: RuntimeComponentGeneration::V2,
            }
        );
    }

    #[test]
    fn hybrid_profiles_reject_complete_global_override_bundles() {
        for profile in [RuntimeKernelVersion::V2Shadow, RuntimeKernelVersion::V2Canary] {
            let basis_points = u16::from(matches!(profile, RuntimeKernelVersion::V2Canary));
            for generation in [RuntimeComponentGeneration::Legacy, RuntimeComponentGeneration::V2] {
                assert_eq!(
                    RuntimeKernelProfileConfigV1::new(
                        profile,
                        basis_points,
                        RuntimeKernelCompatibilityOverridesV1::complete(generation),
                    ),
                    Err(RuntimeKernelProfileError::HybridCompatibilityOverrides { profile })
                );
            }
        }
    }

    #[test]
    fn durable_profile_deserialization_validates_schema_and_unknown_fields() {
        let unsupported = json!({
            "schema_version": 2,
            "profile": "legacy",
            "canary_basis_points": 0,
            "compatibility_overrides": {}
        });
        assert!(serde_json::from_value::<RuntimeKernelProfileConfigV1>(unsupported).is_err());

        let unknown = json!({
            "schema_version": 1,
            "profile": "legacy",
            "canary_basis_points": 0,
            "compatibility_overrides": {},
            "independent_runtime_flag": true
        });
        assert!(serde_json::from_value::<RuntimeKernelProfileConfigV1>(unknown).is_err());
    }

    #[test]
    fn stable_profile_serialization_is_snake_case_and_complete() {
        let config = RuntimeKernelProfileConfigV1::new(
            RuntimeKernelVersion::V2,
            0,
            RuntimeKernelCompatibilityOverridesV1::complete(RuntimeComponentGeneration::V2),
        )
        .expect("profile should validate");

        let encoded = serde_json::to_string(&config).expect("profile should serialize");
        assert_eq!(
            encoded,
            concat!(
                r#"{"schema_version":1,"profile":"v2","canary_basis_points":0,"#,
                r#""compatibility_overrides":{"context":"v2","stream":"v2","#,
                r#""recovery":"v2","queue":"v2","hooks":"v2","middleware":"v2","#,
                r#""replay":"v2","delivery":"v2"}}"#
            )
        );
        let decoded: RuntimeKernelProfileConfigV1 =
            serde_json::from_str(encoded.as_str()).expect("serialized profile should validate");
        assert_eq!(decoded, config);
    }
}
