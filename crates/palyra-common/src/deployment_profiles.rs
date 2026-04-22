use serde::{Deserialize, Serialize};

pub const DEPLOYMENT_PROFILE_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "kebab-case")]
pub enum DeploymentProfileId {
    #[default]
    Local,
    SingleVm,
    WorkerEnabled,
}

impl DeploymentProfileId {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Local => "local",
            Self::SingleVm => "single-vm",
            Self::WorkerEnabled => "worker-enabled",
        }
    }

    #[must_use]
    pub const fn deployment_mode(self) -> &'static str {
        match self {
            Self::Local => "local_desktop",
            Self::SingleVm | Self::WorkerEnabled => "remote_vps",
        }
    }

    #[must_use]
    pub const fn bind_profile(self) -> &'static str {
        "loopback_only"
    }

    pub fn parse(raw: &str) -> Result<Self, DeploymentProfileError> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "local" | "local_desktop" | "local-desktop" => Ok(Self::Local),
            "single-vm" | "single_vm" | "single" | "remote" | "remote_vps" | "remote-vps"
            | "vps" => Ok(Self::SingleVm),
            "worker-enabled" | "worker_enabled" | "worker" | "workers" => Ok(Self::WorkerEnabled),
            value => Err(DeploymentProfileError::UnknownProfile(value.to_owned())),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum DeploymentProfileError {
    #[error("unknown deployment profile '{0}' (expected local, single-vm, or worker-enabled)")]
    UnknownProfile(String),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeploymentProfileManifest {
    pub schema_version: u32,
    pub profile_id: String,
    pub display_name: String,
    pub deployment_mode: String,
    pub bind_profile: String,
    pub operator_summary: String,
    pub capabilities: Vec<DeploymentProfileCapability>,
    pub defaults: Vec<DeploymentProfileDefault>,
    pub blockers: Vec<DeploymentProfileBlocker>,
    pub health_preflights: Vec<DeploymentProfileHealthPreflight>,
    pub recipe_targets: Vec<DeploymentRecipeTarget>,
    pub next_steps: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeploymentProfileCapability {
    pub id: String,
    pub enabled_by_default: bool,
    pub posture: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeploymentProfileDefault {
    pub config_path: String,
    pub value: DeploymentProfileDefaultValue,
    pub rationale: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
pub enum DeploymentProfileDefaultValue {
    String(String),
    Integer(i64),
    Boolean(bool),
    StringList(Vec<String>),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeploymentProfileBlocker {
    pub code: String,
    pub severity: String,
    pub summary: String,
    pub remediation: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeploymentProfileHealthPreflight {
    pub id: String,
    pub required: bool,
    pub scope: String,
    pub summary: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeploymentRecipeTarget {
    pub kind: String,
    pub path: String,
    pub service: String,
}

#[must_use]
pub fn canonical_deployment_profile_ids() -> [DeploymentProfileId; 3] {
    [DeploymentProfileId::Local, DeploymentProfileId::SingleVm, DeploymentProfileId::WorkerEnabled]
}

#[must_use]
pub fn canonical_deployment_profiles() -> Vec<DeploymentProfileManifest> {
    canonical_deployment_profile_ids().into_iter().map(deployment_profile_manifest).collect()
}

#[must_use]
pub fn deployment_profile_manifest(profile_id: DeploymentProfileId) -> DeploymentProfileManifest {
    match profile_id {
        DeploymentProfileId::Local => local_profile_manifest(),
        DeploymentProfileId::SingleVm => single_vm_profile_manifest(),
        DeploymentProfileId::WorkerEnabled => worker_enabled_profile_manifest(),
    }
}

#[must_use]
pub fn derive_deployment_profile(
    configured_profile: Option<&str>,
    deployment_mode: Option<&str>,
    networked_workers_enabled: bool,
) -> DeploymentProfileId {
    if let Some(profile) =
        configured_profile.and_then(|value| DeploymentProfileId::parse(value).ok())
    {
        return profile;
    }
    if networked_workers_enabled {
        return DeploymentProfileId::WorkerEnabled;
    }
    match deployment_mode.and_then(|value| DeploymentProfileId::parse(value).ok()) {
        Some(DeploymentProfileId::SingleVm | DeploymentProfileId::WorkerEnabled) => {
            DeploymentProfileId::SingleVm
        }
        _ => DeploymentProfileId::Local,
    }
}

fn local_profile_manifest() -> DeploymentProfileManifest {
    manifest(DeploymentProfileManifestSpec {
        profile_id: DeploymentProfileId::Local,
        display_name: "Local operator workstation",
        operator_summary:
            "Loopback-only local runtime with admin auth, local state, and no remote worker execution.",
        capabilities: vec![
            capability("gateway.loopback", true, "required"),
            capability("browserd.local", false, "opt-in"),
            capability("networked_workers", false, "disabled"),
            capability("public_gateway", false, "blocked-by-default"),
        ],
        defaults: vec![
            default_string(
                "deployment.profile",
                "local",
                "records the canonical bootstrap profile",
            ),
            default_string(
                "deployment.mode",
                "local_desktop",
                "keeps runtime behavior compatible with local desktop installs",
            ),
            default_string(
                "gateway.bind_profile",
                "loopback_only",
                "preserves fail-closed local exposure",
            ),
            default_bool("admin.require_auth", true, "keeps admin surfaces authenticated"),
            default_bool(
                "feature_rollouts.networked_workers",
                false,
                "local profile does not route tool execution to workers",
            ),
            default_string(
                "networked_workers.mode",
                "disabled",
                "worker runtime remains unavailable until another profile is selected",
            ),
        ],
        blockers: vec![],
        health_preflights: vec![
            preflight(
                "config_schema",
                true,
                "config",
                "generated config parses against the daemon schema",
            ),
            preflight(
                "loopback_bind",
                true,
                "network",
                "gateway bind profile remains loopback-only",
            ),
            preflight(
                "model_auth",
                false,
                "auth",
                "model-provider credentials may still need operator setup",
            ),
        ],
        recipe_targets: vec![],
        next_steps: vec![
            "palyra doctor --json",
            "palyra gateway status",
            "palyra onboarding status --flow quickstart",
        ],
    })
}

fn single_vm_profile_manifest() -> DeploymentProfileManifest {
    manifest(DeploymentProfileManifestSpec {
        profile_id: DeploymentProfileId::SingleVm,
        display_name: "Single VM service deployment",
        operator_summary:
            "A loopback-first server profile for one host, intended to sit behind SSH tunneling or a hardened reverse proxy.",
        capabilities: vec![
            capability("gateway.loopback", true, "required"),
            capability("systemd.palyrad", true, "recipe"),
            capability("docker.compose", true, "recipe"),
            capability("public_gateway", false, "requires-explicit-public-tls"),
            capability("networked_workers", false, "disabled"),
        ],
        defaults: vec![
            default_string(
                "deployment.profile",
                "single-vm",
                "records the canonical bootstrap profile",
            ),
            default_string(
                "deployment.mode",
                "remote_vps",
                "keeps server-side runtime behavior compatible with remote installs",
            ),
            default_string(
                "gateway.bind_profile",
                "loopback_only",
                "single-VM deploys stay behind a tunnel or reverse proxy by default",
            ),
            default_bool("gateway.allow_insecure_remote", false, "remote exposure must stay fail-closed"),
            default_bool("admin.require_auth", true, "remote-capable installs require admin auth"),
            default_bool(
                "feature_rollouts.networked_workers",
                false,
                "single-VM mode does not lease remote workers",
            ),
            default_string(
                "networked_workers.mode",
                "disabled",
                "worker fleet remains disabled until worker-enabled is selected",
            ),
        ],
        blockers: vec![
            blocker(
                "public_tls_requires_dual_ack",
                "blocking",
                "Public bind still requires TLS, admin auth, config acknowledgement, and runtime environment acknowledgement.",
                "Keep loopback-only or configure public_tls with deployment.dangerous_remote_bind_ack plus PALYRA_GATEWAY_DANGEROUS_REMOTE_BIND_ACK.",
            ),
        ],
        health_preflights: vec![
            preflight("config_schema", true, "config", "profile config parses against the daemon schema"),
            preflight("bind_posture", true, "network", "remote bind is not enabled without public TLS guardrails"),
            preflight("storage_paths", true, "storage", "state, identity, and vault paths are writable by the service user"),
            preflight("systemd_or_compose", false, "service", "operator selects either systemd or Compose service lifecycle"),
        ],
        recipe_targets: vec![
            recipe("dockerfile", "infra/deployment/docker/Dockerfile.palyra", "palyra"),
            recipe("compose", "infra/deployment/compose/single-vm.yml", "palyrad"),
            recipe("systemd", "infra/deployment/systemd/palyrad.service", "palyrad"),
        ],
        next_steps: vec![
            "palyra deployment preflight --deployment-profile single-vm --path ./palyra.toml",
            "palyra deployment recipe --deployment-profile single-vm --output-dir ./artifacts/deploy",
            "palyra gateway status",
        ],
    })
}

fn worker_enabled_profile_manifest() -> DeploymentProfileManifest {
    manifest(DeploymentProfileManifestSpec {
        profile_id: DeploymentProfileId::WorkerEnabled,
        display_name: "Worker-enabled service deployment",
        operator_summary:
            "A server profile that keeps the control plane loopback-first while enabling guarded networked worker execution with attestation.",
        capabilities: vec![
            capability("gateway.loopback", true, "required"),
            capability("networked_workers", true, "preview-with-attestation"),
            capability("worker.attestation", true, "required"),
            capability("artifact_transport", true, "required"),
            capability("public_gateway", false, "requires-explicit-public-tls"),
        ],
        defaults: vec![
            default_string(
                "deployment.profile",
                "worker-enabled",
                "records the canonical bootstrap profile",
            ),
            default_string(
                "deployment.mode",
                "remote_vps",
                "worker-enabled installs run as a service profile",
            ),
            default_string(
                "gateway.bind_profile",
                "loopback_only",
                "control-plane exposure stays fail-closed even when workers are enabled",
            ),
            default_bool("gateway.allow_insecure_remote", false, "remote exposure must stay fail-closed"),
            default_bool("admin.require_auth", true, "worker-enabled installs require admin auth"),
            default_bool(
                "feature_rollouts.networked_workers",
                true,
                "profile selection is the explicit operator opt-in for worker routing",
            ),
            default_string(
                "networked_workers.mode",
                "preview_only",
                "workers begin in preview mode until promotion gates pass",
            ),
            default_bool(
                "networked_workers.require_attestation",
                true,
                "worker leases must remain attested",
            ),
            default_integer(
                "networked_workers.lease_ttl_ms",
                900_000,
                "keeps leases bounded for orphan reaping and rollback",
            ),
        ],
        blockers: vec![
            blocker(
                "worker_attestation_digest_required",
                "blocking",
                "Production worker promotion requires an expected worker image/build/artifact digest.",
                "Set one of networked_workers.expected_*_digest_sha256 before promotion.",
            ),
            blocker(
                "execution_plane_egress_review",
                "blocking",
                "Worker egress policy must be reviewed before broader rollout.",
                "Keep process-runner egress none/preflight or document explicit allowed hosts for the worker pool.",
            ),
        ],
        health_preflights: vec![
            preflight("config_schema", true, "config", "profile config parses against the daemon schema"),
            preflight("worker_rollout", true, "rollout", "networked worker rollout is enabled but starts in preview mode"),
            preflight("attestation", true, "security", "worker leases require attestation"),
            preflight("artifact_transport", true, "storage", "daemon state root can persist worker artifacts"),
            preflight("orphan_reaper", true, "recovery", "lease TTL supports fail-closed cleanup"),
        ],
        recipe_targets: vec![
            recipe("dockerfile", "infra/deployment/docker/Dockerfile.palyra", "palyra"),
            recipe(
                "compose",
                "infra/deployment/compose/worker-enabled.yml",
                "palyrad+palyra-workerd",
            ),
            recipe("systemd", "infra/deployment/systemd/palyrad.service", "palyrad"),
            recipe("systemd", "infra/deployment/systemd/palyra-workerd.service", "palyra-workerd"),
        ],
        next_steps: vec![
            "palyra deployment preflight --deployment-profile worker-enabled --path ./palyra.toml",
            "palyra deployment upgrade-smoke --deployment-profile worker-enabled --path ./palyra.toml",
            "palyra deployment promotion-check --deployment-profile worker-enabled",
            "palyra support-bundle export --output ./artifacts/palyra-support-bundle.json",
        ],
    })
}

struct DeploymentProfileManifestSpec<'a> {
    profile_id: DeploymentProfileId,
    display_name: &'a str,
    operator_summary: &'a str,
    capabilities: Vec<DeploymentProfileCapability>,
    defaults: Vec<DeploymentProfileDefault>,
    blockers: Vec<DeploymentProfileBlocker>,
    health_preflights: Vec<DeploymentProfileHealthPreflight>,
    recipe_targets: Vec<DeploymentRecipeTarget>,
    next_steps: Vec<&'a str>,
}

fn manifest(spec: DeploymentProfileManifestSpec<'_>) -> DeploymentProfileManifest {
    DeploymentProfileManifest {
        schema_version: DEPLOYMENT_PROFILE_SCHEMA_VERSION,
        profile_id: spec.profile_id.as_str().to_owned(),
        display_name: spec.display_name.to_owned(),
        deployment_mode: spec.profile_id.deployment_mode().to_owned(),
        bind_profile: spec.profile_id.bind_profile().to_owned(),
        operator_summary: spec.operator_summary.to_owned(),
        capabilities: spec.capabilities,
        defaults: spec.defaults,
        blockers: spec.blockers,
        health_preflights: spec.health_preflights,
        recipe_targets: spec.recipe_targets,
        next_steps: spec.next_steps.into_iter().map(ToOwned::to_owned).collect(),
    }
}

fn capability(id: &str, enabled_by_default: bool, posture: &str) -> DeploymentProfileCapability {
    DeploymentProfileCapability {
        id: id.to_owned(),
        enabled_by_default,
        posture: posture.to_owned(),
    }
}

fn default_string(config_path: &str, value: &str, rationale: &str) -> DeploymentProfileDefault {
    default(config_path, DeploymentProfileDefaultValue::String(value.to_owned()), rationale)
}

fn default_integer(config_path: &str, value: i64, rationale: &str) -> DeploymentProfileDefault {
    default(config_path, DeploymentProfileDefaultValue::Integer(value), rationale)
}

fn default_bool(config_path: &str, value: bool, rationale: &str) -> DeploymentProfileDefault {
    default(config_path, DeploymentProfileDefaultValue::Boolean(value), rationale)
}

fn default(
    config_path: &str,
    value: DeploymentProfileDefaultValue,
    rationale: &str,
) -> DeploymentProfileDefault {
    DeploymentProfileDefault {
        config_path: config_path.to_owned(),
        value,
        rationale: rationale.to_owned(),
    }
}

fn blocker(
    code: &str,
    severity: &str,
    summary: &str,
    remediation: &str,
) -> DeploymentProfileBlocker {
    DeploymentProfileBlocker {
        code: code.to_owned(),
        severity: severity.to_owned(),
        summary: summary.to_owned(),
        remediation: remediation.to_owned(),
    }
}

fn preflight(
    id: &str,
    required: bool,
    scope: &str,
    summary: &str,
) -> DeploymentProfileHealthPreflight {
    DeploymentProfileHealthPreflight {
        id: id.to_owned(),
        required,
        scope: scope.to_owned(),
        summary: summary.to_owned(),
    }
}

fn recipe(kind: &str, path: &str, service: &str) -> DeploymentRecipeTarget {
    DeploymentRecipeTarget {
        kind: kind.to_owned(),
        path: path.to_owned(),
        service: service.to_owned(),
    }
}

#[cfg(test)]
mod tests {
    use super::{
        canonical_deployment_profiles, deployment_profile_manifest, derive_deployment_profile,
        DeploymentProfileId,
    };

    #[test]
    fn canonical_profiles_are_stable_and_parseable() {
        let profiles = canonical_deployment_profiles();
        let ids = profiles.iter().map(|profile| profile.profile_id.as_str()).collect::<Vec<_>>();
        assert_eq!(ids, vec!["local", "single-vm", "worker-enabled"]);
        for profile in profiles {
            assert_eq!(
                DeploymentProfileId::parse(profile.profile_id.as_str()).ok(),
                Some(match profile.profile_id.as_str() {
                    "local" => DeploymentProfileId::Local,
                    "single-vm" => DeploymentProfileId::SingleVm,
                    "worker-enabled" => DeploymentProfileId::WorkerEnabled,
                    _ => unreachable!("canonical profile id should be exhaustive"),
                })
            );
            assert!(!profile.defaults.is_empty());
            assert!(!profile.health_preflights.is_empty());
        }
    }

    #[test]
    fn worker_enabled_manifest_requires_attestation() {
        let manifest = deployment_profile_manifest(DeploymentProfileId::WorkerEnabled);
        assert_eq!(manifest.deployment_mode, "remote_vps");
        assert!(manifest
            .defaults
            .iter()
            .any(|default| default.config_path == "networked_workers.require_attestation"));
        assert!(manifest
            .blockers
            .iter()
            .any(|blocker| blocker.code == "worker_attestation_digest_required"));
    }

    #[test]
    fn profile_derivation_prefers_explicit_config() {
        assert_eq!(
            derive_deployment_profile(Some("single-vm"), Some("local_desktop"), true),
            DeploymentProfileId::SingleVm
        );
        assert_eq!(
            derive_deployment_profile(None, Some("remote_vps"), false),
            DeploymentProfileId::SingleVm
        );
        assert_eq!(
            derive_deployment_profile(None, Some("local_desktop"), true),
            DeploymentProfileId::WorkerEnabled
        );
    }
}
