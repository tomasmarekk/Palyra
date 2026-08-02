//! Runtime-kernel config merging and cross-field validation.
//!
//! Historical feature-rollout settings remain the compatibility input. Their
//! explicit provenance is validated as one complete bundle instead of being
//! mirrored into a second set of rollout switches.

use std::env;

use anyhow::{Context, Result};
use palyra_common::{
    daemon_config_schema::FileRuntimeKernelConfig, feature_rollouts::FeatureRolloutSource,
};

use super::schema::{
    ExistingSessionMigrationPolicy, FeatureRolloutsConfig, RuntimeKernelConfig,
    RuntimeKernelProfile, RuntimeKernelRollbackPolicy, RuntimeKernelSamplingIdentity,
    RuntimeKernelSamplingKey, RuntimeKernelSamplingKeySource,
};

const BASIS_POINTS_DENOMINATOR: u16 = 10_000;

/// Generation derived from the real legacy feature-rollout compatibility bundle.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CompatibilityBundleGeneration {
    Legacy,
    V2,
}

/// Provenance of a complete explicit compatibility bundle.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CompatibilityBundleSource {
    Config,
    Env,
    ConfigAndEnv,
}

/// Validated generation and provenance of the real compatibility settings.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct CompatibilityBundleResolution {
    pub(crate) generation: CompatibilityBundleGeneration,
    pub(crate) source: CompatibilityBundleSource,
}

pub(super) fn apply_file(
    config: &mut RuntimeKernelConfig,
    file: FileRuntimeKernelConfig,
) -> Result<()> {
    if let Some(profile) = file.profile {
        config.profile = RuntimeKernelProfile::parse(profile.as_str(), "runtime_kernel.profile")?;
    }
    if let Some(value) = file.canary_basis_points {
        config.canary_basis_points = value;
    }
    if let Some(value) = file.shadow_sample_basis_points {
        config.shadow_sample_basis_points = value;
    }
    if let Some(identity) = file.sampling_identity {
        config.sampling_identity = RuntimeKernelSamplingIdentity::parse(
            identity.as_str(),
            "runtime_kernel.sampling_identity",
        )?;
    }
    match (file.sampling_key_hex, file.sampling_key_secret_ref) {
        (Some(_), Some(_)) => {
            anyhow::bail!(
                "runtime_kernel must not set both sampling_key_hex and sampling_key_secret_ref"
            );
        }
        (Some(key), None) => {
            config.sampling_key_source =
                Some(RuntimeKernelSamplingKeySource::Inline(RuntimeKernelSamplingKey::parse_hex(
                    key.as_str(),
                    "runtime_kernel.sampling_key_hex",
                )?));
        }
        (None, Some(secret_ref)) => {
            secret_ref.validate().context("runtime_kernel.sampling_key_secret_ref is invalid")?;
            config.sampling_key_source =
                Some(RuntimeKernelSamplingKeySource::SecretRef(secret_ref));
        }
        (None, None) => {}
    }
    if let Some(policy) = file.existing_session_policy {
        config.existing_session_policy = ExistingSessionMigrationPolicy::parse(
            policy.as_str(),
            "runtime_kernel.existing_session_policy",
        )?;
    }
    if let Some(policy) = file.rollback_policy {
        config.rollback_policy =
            RuntimeKernelRollbackPolicy::parse(policy.as_str(), "runtime_kernel.rollback_policy")?;
    }
    Ok(())
}

pub(super) fn apply_env(config: &mut RuntimeKernelConfig, source: &mut String) -> Result<()> {
    if let Ok(raw) = env::var("PALYRA_RUNTIME_KERNEL_PROFILE") {
        config.profile =
            RuntimeKernelProfile::parse(raw.as_str(), "PALYRA_RUNTIME_KERNEL_PROFILE")?;
        append_env_source(source, "PALYRA_RUNTIME_KERNEL_PROFILE");
    }
    if let Ok(raw) = env::var("PALYRA_RUNTIME_KERNEL_CANARY_BASIS_POINTS") {
        config.canary_basis_points = raw
            .parse::<u16>()
            .context("PALYRA_RUNTIME_KERNEL_CANARY_BASIS_POINTS must be a valid u16")?;
        append_env_source(source, "PALYRA_RUNTIME_KERNEL_CANARY_BASIS_POINTS");
    }
    if let Ok(raw) = env::var("PALYRA_RUNTIME_KERNEL_SHADOW_SAMPLE_BASIS_POINTS") {
        config.shadow_sample_basis_points = raw
            .parse::<u16>()
            .context("PALYRA_RUNTIME_KERNEL_SHADOW_SAMPLE_BASIS_POINTS must be a valid u16")?;
        append_env_source(source, "PALYRA_RUNTIME_KERNEL_SHADOW_SAMPLE_BASIS_POINTS");
    }
    if let Ok(raw) = env::var("PALYRA_RUNTIME_KERNEL_SAMPLING_IDENTITY") {
        config.sampling_identity = RuntimeKernelSamplingIdentity::parse(
            raw.as_str(),
            "PALYRA_RUNTIME_KERNEL_SAMPLING_IDENTITY",
        )?;
        append_env_source(source, "PALYRA_RUNTIME_KERNEL_SAMPLING_IDENTITY");
    }
    if let Ok(raw) = env::var("PALYRA_RUNTIME_KERNEL_SAMPLING_KEY_HEX") {
        config.sampling_key_source =
            Some(RuntimeKernelSamplingKeySource::Inline(RuntimeKernelSamplingKey::parse_hex(
                raw.as_str(),
                "PALYRA_RUNTIME_KERNEL_SAMPLING_KEY_HEX",
            )?));
        append_env_source(source, "PALYRA_RUNTIME_KERNEL_SAMPLING_KEY_HEX");
    }
    if let Ok(raw) = env::var("PALYRA_RUNTIME_KERNEL_EXISTING_SESSION_POLICY") {
        config.existing_session_policy = ExistingSessionMigrationPolicy::parse(
            raw.as_str(),
            "PALYRA_RUNTIME_KERNEL_EXISTING_SESSION_POLICY",
        )?;
        append_env_source(source, "PALYRA_RUNTIME_KERNEL_EXISTING_SESSION_POLICY");
    }
    if let Ok(raw) = env::var("PALYRA_RUNTIME_KERNEL_ROLLBACK_POLICY") {
        config.rollback_policy = RuntimeKernelRollbackPolicy::parse(
            raw.as_str(),
            "PALYRA_RUNTIME_KERNEL_ROLLBACK_POLICY",
        )?;
        append_env_source(source, "PALYRA_RUNTIME_KERNEL_ROLLBACK_POLICY");
    }
    Ok(())
}

pub(crate) fn validate(
    config: &RuntimeKernelConfig,
    feature_rollouts: &FeatureRolloutsConfig,
) -> Result<Option<CompatibilityBundleResolution>> {
    if let Some(RuntimeKernelSamplingKeySource::SecretRef(secret_ref)) = &config.sampling_key_source
    {
        secret_ref.validate().context("runtime_kernel.sampling_key_secret_ref is invalid")?;
    }
    match config.profile {
        RuntimeKernelProfile::Legacy | RuntimeKernelProfile::V2 => {
            if config.canary_basis_points != 0 || config.shadow_sample_basis_points != 0 {
                anyhow::bail!(
                    "runtime_kernel profile {} requires canary_basis_points=0 and shadow_sample_basis_points=0",
                    config.profile.as_str()
                );
            }
        }
        RuntimeKernelProfile::V2Shadow => {
            if config.canary_basis_points != 0
                || !(1..=BASIS_POINTS_DENOMINATOR).contains(&config.shadow_sample_basis_points)
            {
                anyhow::bail!(
                    "runtime_kernel profile v2_shadow requires canary_basis_points=0 and shadow_sample_basis_points in range 1..=10000"
                );
            }
            require_sampling_key(config)?;
        }
        RuntimeKernelProfile::V2Canary => {
            if !(1..BASIS_POINTS_DENOMINATOR).contains(&config.canary_basis_points)
                || config.shadow_sample_basis_points != 0
            {
                anyhow::bail!(
                    "runtime_kernel profile v2_canary requires canary_basis_points in range 1..10000 and shadow_sample_basis_points=0"
                );
            }
            require_sampling_key(config)?;
        }
    }

    let compatibility = compatibility_bundle_resolution(feature_rollouts)?;
    if let Some(resolution) = compatibility {
        let observed = resolution.generation;
        let expected = match config.profile {
            RuntimeKernelProfile::Legacy => CompatibilityBundleGeneration::Legacy,
            RuntimeKernelProfile::V2 => CompatibilityBundleGeneration::V2,
            RuntimeKernelProfile::V2Shadow | RuntimeKernelProfile::V2Canary => {
                anyhow::bail!(
                    "runtime_kernel hybrid profiles reject explicit legacy feature-rollout bundles"
                );
            }
        };
        if observed != expected {
            anyhow::bail!(
                "runtime_kernel profile {} conflicts with the explicit legacy feature-rollout bundle",
                config.profile.as_str()
            );
        }
    }
    Ok(compatibility)
}

#[cfg(test)]
fn compatibility_bundle_generation(
    feature_rollouts: &FeatureRolloutsConfig,
) -> Result<Option<CompatibilityBundleGeneration>> {
    compatibility_bundle_resolution(feature_rollouts)
        .map(|resolution| resolution.map(|resolution| resolution.generation))
}

pub(crate) fn compatibility_bundle_resolution(
    feature_rollouts: &FeatureRolloutsConfig,
) -> Result<Option<CompatibilityBundleResolution>> {
    let settings = [
        ("context_engine", feature_rollouts.context_engine),
        ("provider_stream_normalizer", feature_rollouts.provider_stream_normalizer),
        ("provider_recovery", feature_rollouts.provider_recovery),
        ("session_queue_policy", feature_rollouts.session_queue_policy),
        ("replay_capture", feature_rollouts.replay_capture),
        ("delivery_arbitration", feature_rollouts.delivery_arbitration),
    ];
    let explicit = settings
        .iter()
        .filter(|(_, setting)| setting.source != FeatureRolloutSource::Default)
        .collect::<Vec<_>>();
    if explicit.is_empty() {
        return Ok(None);
    }
    if explicit.len() != settings.len() {
        let names = explicit.iter().map(|(name, _)| *name).collect::<Vec<_>>().join(", ");
        anyhow::bail!(
            "deprecated runtime-generation compatibility flags must be all explicit or all absent; configure runtime_kernel.profile instead; explicit: {names}"
        );
    }
    let enabled = explicit
        .first()
        .map(|(_, setting)| setting.enabled)
        .ok_or_else(|| anyhow::anyhow!("runtime_kernel compatibility bundle is empty"))?;
    if explicit.iter().any(|(_, setting)| setting.enabled != enabled) {
        anyhow::bail!(
            "deprecated runtime-generation compatibility flags cannot mix legacy and v2 behavior; configure runtime_kernel.profile instead"
        );
    }
    let generation = if enabled {
        CompatibilityBundleGeneration::V2
    } else {
        CompatibilityBundleGeneration::Legacy
    };
    let has_config =
        explicit.iter().any(|(_, setting)| setting.source == FeatureRolloutSource::Config);
    let has_env = explicit.iter().any(|(_, setting)| setting.source == FeatureRolloutSource::Env);
    let source = match (has_config, has_env) {
        (true, true) => CompatibilityBundleSource::ConfigAndEnv,
        (true, false) => CompatibilityBundleSource::Config,
        (false, true) => CompatibilityBundleSource::Env,
        (false, false) => {
            return Err(anyhow::anyhow!(
                "runtime_kernel explicit compatibility bundle has no source"
            ));
        }
    };
    Ok(Some(CompatibilityBundleResolution { generation, source }))
}

fn require_sampling_key(config: &RuntimeKernelConfig) -> Result<()> {
    if config.sampling_key_source.is_none() {
        anyhow::bail!(
            "runtime_kernel profile {} requires a deployment-stable sampling key source",
            config.profile.as_str()
        );
    }
    Ok(())
}

fn append_env_source(source: &mut String, name: &str) {
    source.push_str(" +env(");
    source.push_str(name);
    source.push(')');
}

#[cfg(test)]
mod tests {
    use palyra_common::feature_rollouts::FeatureRolloutSetting;
    use palyra_common::secret_refs::SecretRef;

    use super::*;

    fn set_bundle(config: &mut FeatureRolloutsConfig, setting: FeatureRolloutSetting) {
        config.context_engine = setting;
        config.provider_stream_normalizer = setting;
        config.provider_recovery = setting;
        config.session_queue_policy = setting;
        config.replay_capture = setting;
        config.delivery_arbitration = setting;
    }

    #[test]
    fn real_feature_rollout_sources_form_only_complete_coherent_bundles() {
        let defaults = FeatureRolloutsConfig::default();
        assert_eq!(compatibility_bundle_generation(&defaults).expect("defaults are valid"), None);

        let independent_high_risk = FeatureRolloutsConfig {
            inline_runtime_hooks: FeatureRolloutSetting::from_config(true),
            tool_result_middleware: FeatureRolloutSetting::from_env(true),
            ..FeatureRolloutsConfig::default()
        };
        assert_eq!(
            compatibility_bundle_generation(&independent_high_risk)
                .expect("high-risk capability flags are not generation selectors"),
            None
        );

        let partial = FeatureRolloutsConfig {
            context_engine: FeatureRolloutSetting::from_config(true),
            ..FeatureRolloutsConfig::default()
        };
        let error = compatibility_bundle_generation(&partial)
            .expect_err("partial generation bundle should fail closed");
        let message = error.to_string();
        assert!(message.contains("deprecated runtime-generation compatibility flags"));
        assert!(message.contains("configure runtime_kernel.profile instead"));

        let mut mixed = FeatureRolloutsConfig::default();
        set_bundle(&mut mixed, FeatureRolloutSetting::from_env(true));
        mixed.delivery_arbitration = FeatureRolloutSetting::from_config(false);
        assert!(compatibility_bundle_generation(&mixed).is_err());

        let mut v2 = FeatureRolloutsConfig::default();
        set_bundle(&mut v2, FeatureRolloutSetting::from_config(true));
        assert_eq!(
            v2.session_queue_policy,
            FeatureRolloutSetting::from_config(true),
            "the authoritative V2 bundle must include session_queue_policy"
        );
        assert_eq!(
            compatibility_bundle_generation(&v2).expect("complete V2 bundle is valid"),
            Some(CompatibilityBundleGeneration::V2)
        );
    }

    #[test]
    fn default_runtime_profile_is_v2_and_rejects_a_legacy_bundle() {
        let config = RuntimeKernelConfig::default();
        assert_eq!(config.profile, RuntimeKernelProfile::V2);
        let mut rollouts = FeatureRolloutsConfig::default();
        set_bundle(&mut rollouts, FeatureRolloutSetting::from_config(false));
        assert!(validate(&config, &rollouts).is_err());
    }

    #[test]
    fn hybrid_profiles_require_exact_sampling_configuration() {
        let rollouts = FeatureRolloutsConfig::default();
        let key = RuntimeKernelSamplingKeySource::Inline(
            RuntimeKernelSamplingKey::parse_hex(
                "ab".repeat(32).as_str(),
                "test.runtime_kernel.sampling_key_hex",
            )
            .expect("test sampling key should parse"),
        );

        let shadow = RuntimeKernelConfig {
            profile: RuntimeKernelProfile::V2Shadow,
            shadow_sample_basis_points: BASIS_POINTS_DENOMINATOR,
            sampling_key_source: Some(key.clone()),
            ..RuntimeKernelConfig::default()
        };
        assert!(validate(&shadow, &rollouts).is_ok());

        let invalid_shadow = RuntimeKernelConfig { shadow_sample_basis_points: 0, ..shadow };
        assert!(validate(&invalid_shadow, &rollouts).is_err());

        let canary = RuntimeKernelConfig {
            profile: RuntimeKernelProfile::V2Canary,
            canary_basis_points: BASIS_POINTS_DENOMINATOR - 1,
            sampling_key_source: Some(key),
            ..RuntimeKernelConfig::default()
        };
        assert!(validate(&canary, &rollouts).is_ok());

        let invalid_canary =
            RuntimeKernelConfig { canary_basis_points: BASIS_POINTS_DENOMINATOR, ..canary };
        assert!(validate(&invalid_canary, &rollouts).is_err());
    }

    #[test]
    fn runtime_sampling_key_debug_output_never_exposes_material_or_locator() {
        let inline = RuntimeKernelSamplingKeySource::Inline(
            RuntimeKernelSamplingKey::parse_hex(
                "cd".repeat(32).as_str(),
                "test.runtime_kernel.sampling_key_hex",
            )
            .expect("test sampling key should parse"),
        );
        let inline_debug = format!("{inline:?}");
        assert!(!inline_debug.contains("cdcdcd"));

        let secret_ref = RuntimeKernelSamplingKeySource::SecretRef(
            SecretRef::from_legacy_vault_ref("vault://sensitive/runtime-kernel-key"),
        );
        let secret_ref_debug = format!("{secret_ref:?}");
        assert!(!secret_ref_debug.contains("sensitive"));
        assert!(!secret_ref_debug.contains("vault://"));
    }
}
