//! Canonical deployment profile catalog: local, single-vm, worker-enabled.
//!
//! Each profile carries fail-closed config defaults, promotion blockers,
//! health preflights, and recipe targets. Consumed by `palyra deployment`
//! and onboarding CLI commands and by daemon diagnostics; manifest strings
//! are operator-facing contract data.

use serde::{Deserialize, Serialize};

/// Schema version stamped on emitted deployment profile manifests.
pub const DEPLOYMENT_PROFILE_SCHEMA_VERSION: u32 = 1;
/// Schema version for trusted-proxy feasibility reports.
pub const TRUSTED_PROXY_FEASIBILITY_SCHEMA_VERSION: u32 = 1;
/// Audit event emitted before trusted-proxy feasibility is evaluated.
pub const TRUSTED_PROXY_FEASIBILITY_STARTED_EVENT_TYPE: &str = "trusted_proxy.feasibility.started";
/// Audit event emitted after trusted-proxy feasibility is evaluated.
pub const TRUSTED_PROXY_FEASIBILITY_COMPLETED_EVENT_TYPE: &str =
    "trusted_proxy.feasibility.completed";
/// Audit event emitted when trusted-proxy feasibility evaluation cannot run.
pub const TRUSTED_PROXY_FEASIBILITY_FAILED_EVENT_TYPE: &str = "trusted_proxy.feasibility.failed";

/// Canonical deployment profile selector; defaults to [`Local`](Self::Local).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "kebab-case")]
pub enum DeploymentProfileId {
    #[default]
    Local,
    SingleVm,
    WorkerEnabled,
}

impl DeploymentProfileId {
    /// Canonical kebab-case identifier, matching the serde representation.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Local => "local",
            Self::SingleVm => "single-vm",
            Self::WorkerEnabled => "worker-enabled",
        }
    }

    /// Runtime deployment mode recorded in config for this profile.
    #[must_use]
    pub const fn deployment_mode(self) -> &'static str {
        match self {
            Self::Local => "local_desktop",
            Self::SingleVm | Self::WorkerEnabled => "remote_vps",
        }
    }

    /// Gateway bind posture for this profile.
    ///
    /// INTENTIONAL: every profile is loopback-only. Public exposure is never
    /// a profile default; it requires explicit TLS plus dual acknowledgement.
    #[must_use]
    pub const fn bind_profile(self) -> &'static str {
        "loopback_only"
    }

    /// Parses a profile id or deployment-mode alias (trimmed,
    /// case-insensitive).
    ///
    /// # Errors
    /// Returns [`DeploymentProfileError::UnknownProfile`] for unrecognized
    /// input.
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

/// Errors from parsing deployment profile identifiers.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum DeploymentProfileError {
    #[error("unknown deployment profile '{0}' (expected local, single-vm, or worker-enabled)")]
    UnknownProfile(String),
}

/// Full operator-facing description of one deployment profile.
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

/// A capability surface and its default posture within a profile.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeploymentProfileCapability {
    pub id: String,
    pub enabled_by_default: bool,
    pub posture: String,
}

/// One config default applied by a profile, with operator-facing rationale.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeploymentProfileDefault {
    pub config_path: String,
    pub value: DeploymentProfileDefaultValue,
    pub rationale: String,
}

/// Typed value carried by a profile config default.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
pub enum DeploymentProfileDefaultValue {
    String(String),
    Integer(i64),
    Boolean(bool),
    StringList(Vec<String>),
}

/// A condition that blocks promotion or rollout until remediated.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeploymentProfileBlocker {
    pub code: String,
    pub severity: String,
    pub summary: String,
    pub remediation: String,
}

/// A preflight check the profile expects before going live.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeploymentProfileHealthPreflight {
    pub id: String,
    pub required: bool,
    pub scope: String,
    pub summary: String,
}

/// Deployment recipe artifact (Dockerfile, Compose, systemd) tied to a
/// profile.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeploymentRecipeTarget {
    pub kind: String,
    pub path: String,
    pub service: String,
}

/// All profile ids in canonical order.
#[must_use]
pub fn canonical_deployment_profile_ids() -> [DeploymentProfileId; 3] {
    [DeploymentProfileId::Local, DeploymentProfileId::SingleVm, DeploymentProfileId::WorkerEnabled]
}

/// Manifests for every canonical profile, in canonical order.
#[must_use]
pub fn canonical_deployment_profiles() -> Vec<DeploymentProfileManifest> {
    canonical_deployment_profile_ids().into_iter().map(deployment_profile_manifest).collect()
}

/// Builds the canonical manifest for one profile.
#[must_use]
pub fn deployment_profile_manifest(profile_id: DeploymentProfileId) -> DeploymentProfileManifest {
    match profile_id {
        DeploymentProfileId::Local => local_profile_manifest(),
        DeploymentProfileId::SingleVm => single_vm_profile_manifest(),
        DeploymentProfileId::WorkerEnabled => worker_enabled_profile_manifest(),
    }
}

/// Derives the effective profile from config inputs.
///
/// Precedence: explicit `configured_profile`, then the networked-workers
/// rollout flag, then `deployment_mode` (any remote mode maps to
/// [`DeploymentProfileId::SingleVm`]); everything else falls back to
/// [`DeploymentProfileId::Local`].
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
        // A mode string alone never opts into workers; worker routing
        // requires the explicit rollout flag handled above.
        Some(DeploymentProfileId::SingleVm | DeploymentProfileId::WorkerEnabled) => {
            DeploymentProfileId::SingleVm
        }
        _ => DeploymentProfileId::Local,
    }
}

/// Operator-supplied facts for a trusted reverse-proxy exposure review.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TrustedProxyFeasibilityInput {
    pub deployment_profile: DeploymentProfileId,
    pub public_gateway_requested: bool,
    pub tls_termination_configured: bool,
    pub admin_auth_required: bool,
    pub config_acknowledged: bool,
    pub environment_acknowledged: bool,
    pub strips_untrusted_forwarded_headers: bool,
    pub forwards_authenticated_identity: bool,
    pub allowed_proxy_cidrs: Vec<String>,
}

impl Default for TrustedProxyFeasibilityInput {
    fn default() -> Self {
        Self {
            deployment_profile: DeploymentProfileId::Local,
            public_gateway_requested: false,
            tls_termination_configured: false,
            admin_auth_required: true,
            config_acknowledged: false,
            environment_acknowledged: false,
            strips_untrusted_forwarded_headers: false,
            forwards_authenticated_identity: false,
            allowed_proxy_cidrs: Vec::new(),
        }
    }
}

/// Trusted-proxy gate decision; `FeasiblePreview` still does not enable public bind.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TrustedProxyFeasibilityDecision {
    NotRequired,
    FeasiblePreview,
    Blocked,
}

/// Stable reason code for trusted-proxy feasibility.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TrustedProxyFeasibilityReasonCode {
    LoopbackOnly,
    ReadyForPreview,
    PublicExposureRequiresRemoteProfile,
    PublicExposureRequiresTls,
    AdminAuthRequired,
    DualAcknowledgementRequired,
    ForwardedHeaderSanitizationRequired,
    AuthenticatedProxyIdentityRequired,
    ProxyCidrScopeRequired,
}

/// Pure report for trusted-proxy feasibility.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TrustedProxyFeasibilityReport {
    pub schema_version: u32,
    pub decision: TrustedProxyFeasibilityDecision,
    pub reason_codes: Vec<TrustedProxyFeasibilityReasonCode>,
    pub promotion_blockers: Vec<DeploymentProfileBlocker>,
    pub preview_only: bool,
    pub redaction_level: String,
    pub started_event_type: String,
    pub completed_event_type: String,
    pub failed_event_type: String,
}

/// Evaluate whether a public gateway can sit behind a trusted proxy preview.
///
/// This gate is deliberately stricter than the deployment profile defaults:
/// loopback requires no proxy, while public exposure is only "feasible preview"
/// after TLS, admin auth, dual acknowledgement, forwarded-header hygiene,
/// authenticated identity forwarding, and proxy CIDR scope are all explicit.
#[must_use]
pub fn evaluate_trusted_proxy_feasibility(
    input: &TrustedProxyFeasibilityInput,
) -> TrustedProxyFeasibilityReport {
    if !input.public_gateway_requested {
        return TrustedProxyFeasibilityReport {
            schema_version: TRUSTED_PROXY_FEASIBILITY_SCHEMA_VERSION,
            decision: TrustedProxyFeasibilityDecision::NotRequired,
            reason_codes: vec![TrustedProxyFeasibilityReasonCode::LoopbackOnly],
            promotion_blockers: Vec::new(),
            preview_only: true,
            redaction_level: "metadata_only".to_owned(),
            started_event_type: TRUSTED_PROXY_FEASIBILITY_STARTED_EVENT_TYPE.to_owned(),
            completed_event_type: TRUSTED_PROXY_FEASIBILITY_COMPLETED_EVENT_TYPE.to_owned(),
            failed_event_type: TRUSTED_PROXY_FEASIBILITY_FAILED_EVENT_TYPE.to_owned(),
        };
    }

    let mut blockers = Vec::new();
    let mut reasons = Vec::new();
    if input.deployment_profile == DeploymentProfileId::Local {
        push_trusted_proxy_blocker(
            &mut blockers,
            &mut reasons,
            TrustedProxyFeasibilityReasonCode::PublicExposureRequiresRemoteProfile,
            "public_exposure_requires_remote_profile",
            "Public trusted-proxy exposure is not available for the local workstation profile.",
            "Select single-vm or worker-enabled before reviewing public gateway exposure.",
        );
    }
    if !input.tls_termination_configured {
        push_trusted_proxy_blocker(
            &mut blockers,
            &mut reasons,
            TrustedProxyFeasibilityReasonCode::PublicExposureRequiresTls,
            "public_tls_required",
            "Public trusted-proxy exposure requires TLS termination.",
            "Configure TLS at the trusted proxy before enabling any public gateway route.",
        );
    }
    if !input.admin_auth_required {
        push_trusted_proxy_blocker(
            &mut blockers,
            &mut reasons,
            TrustedProxyFeasibilityReasonCode::AdminAuthRequired,
            "admin_auth_required",
            "Public trusted-proxy exposure requires authenticated admin surfaces.",
            "Keep admin.require_auth=true for every remote-capable deployment profile.",
        );
    }
    if !input.config_acknowledged || !input.environment_acknowledged {
        push_trusted_proxy_blocker(
            &mut blockers,
            &mut reasons,
            TrustedProxyFeasibilityReasonCode::DualAcknowledgementRequired,
            "dual_ack_required",
            "Public trusted-proxy exposure requires config and environment acknowledgement.",
            "Set both deployment config acknowledgement and the matching runtime environment acknowledgement.",
        );
    }
    if !input.strips_untrusted_forwarded_headers {
        push_trusted_proxy_blocker(
            &mut blockers,
            &mut reasons,
            TrustedProxyFeasibilityReasonCode::ForwardedHeaderSanitizationRequired,
            "forwarded_header_sanitization_required",
            "The proxy must strip untrusted forwarded headers before forwarding requests.",
            "Configure the proxy to overwrite X-Forwarded-* headers from clients.",
        );
    }
    if !input.forwards_authenticated_identity {
        push_trusted_proxy_blocker(
            &mut blockers,
            &mut reasons,
            TrustedProxyFeasibilityReasonCode::AuthenticatedProxyIdentityRequired,
            "authenticated_proxy_identity_required",
            "The proxy must forward an authenticated identity boundary to the gateway.",
            "Bind gateway access to the trusted proxy identity instead of anonymous public traffic.",
        );
    }
    if input.allowed_proxy_cidrs.is_empty() {
        push_trusted_proxy_blocker(
            &mut blockers,
            &mut reasons,
            TrustedProxyFeasibilityReasonCode::ProxyCidrScopeRequired,
            "proxy_cidr_scope_required",
            "The gateway must know which proxy CIDR ranges are trusted.",
            "Declare explicit proxy CIDR ranges and keep direct public client IPs untrusted.",
        );
    }

    let decision = if blockers.is_empty() {
        reasons.push(TrustedProxyFeasibilityReasonCode::ReadyForPreview);
        TrustedProxyFeasibilityDecision::FeasiblePreview
    } else {
        TrustedProxyFeasibilityDecision::Blocked
    };

    TrustedProxyFeasibilityReport {
        schema_version: TRUSTED_PROXY_FEASIBILITY_SCHEMA_VERSION,
        decision,
        reason_codes: reasons,
        promotion_blockers: blockers,
        preview_only: true,
        redaction_level: "metadata_only".to_owned(),
        started_event_type: TRUSTED_PROXY_FEASIBILITY_STARTED_EVENT_TYPE.to_owned(),
        completed_event_type: TRUSTED_PROXY_FEASIBILITY_COMPLETED_EVENT_TYPE.to_owned(),
        failed_event_type: TRUSTED_PROXY_FEASIBILITY_FAILED_EVENT_TYPE.to_owned(),
    }
}

fn push_trusted_proxy_blocker(
    blockers: &mut Vec<DeploymentProfileBlocker>,
    reasons: &mut Vec<TrustedProxyFeasibilityReasonCode>,
    reason: TrustedProxyFeasibilityReasonCode,
    code: &str,
    summary: &str,
    remediation: &str,
) {
    reasons.push(reason);
    blockers.push(blocker(code, "blocking", summary, remediation));
}

fn local_profile_manifest() -> DeploymentProfileManifest {
    manifest(DeploymentProfileManifestSpec {
        profile_id: DeploymentProfileId::Local,
        display_name: "Local operator workstation",
        operator_summary:
            "Loopback-only local runtime with admin auth, local state, and no remote worker execution.",
        capabilities: vec![
            capability("gateway.loopback", true, "required"),
            capability("browserd.local", true, "default-on"),
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
        evaluate_trusted_proxy_feasibility, DeploymentProfileId, TrustedProxyFeasibilityDecision,
        TrustedProxyFeasibilityInput, TrustedProxyFeasibilityReasonCode,
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
    fn local_manifest_marks_browserd_enabled_by_default() {
        let manifest = deployment_profile_manifest(DeploymentProfileId::Local);
        let browserd = manifest
            .capabilities
            .iter()
            .find(|capability| capability.id == "browserd.local")
            .expect("local profile should declare browserd capability");

        assert!(browserd.enabled_by_default);
        assert_eq!(browserd.posture, "default-on");
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

    #[test]
    fn trusted_proxy_feasibility_defaults_to_loopback_not_required() {
        let report = evaluate_trusted_proxy_feasibility(&TrustedProxyFeasibilityInput::default());

        assert_eq!(report.decision, TrustedProxyFeasibilityDecision::NotRequired);
        assert_eq!(report.reason_codes, vec![TrustedProxyFeasibilityReasonCode::LoopbackOnly]);
        assert!(report.promotion_blockers.is_empty());
        assert!(report.preview_only);
    }

    #[test]
    fn trusted_proxy_feasibility_blocks_incomplete_public_exposure() {
        let report = evaluate_trusted_proxy_feasibility(&TrustedProxyFeasibilityInput {
            deployment_profile: DeploymentProfileId::SingleVm,
            public_gateway_requested: true,
            admin_auth_required: false,
            ..TrustedProxyFeasibilityInput::default()
        });

        assert_eq!(report.decision, TrustedProxyFeasibilityDecision::Blocked);
        assert!(report
            .reason_codes
            .contains(&TrustedProxyFeasibilityReasonCode::PublicExposureRequiresTls));
        assert!(report
            .reason_codes
            .contains(&TrustedProxyFeasibilityReasonCode::AdminAuthRequired));
        assert!(report
            .reason_codes
            .contains(&TrustedProxyFeasibilityReasonCode::DualAcknowledgementRequired));
        assert!(!report.promotion_blockers.is_empty());
    }

    #[test]
    fn trusted_proxy_feasibility_allows_preview_when_every_guardrail_is_explicit() {
        let report = evaluate_trusted_proxy_feasibility(&TrustedProxyFeasibilityInput {
            deployment_profile: DeploymentProfileId::SingleVm,
            public_gateway_requested: true,
            tls_termination_configured: true,
            admin_auth_required: true,
            config_acknowledged: true,
            environment_acknowledged: true,
            strips_untrusted_forwarded_headers: true,
            forwards_authenticated_identity: true,
            allowed_proxy_cidrs: vec!["203.0.113.0/24".to_owned()],
        });

        assert_eq!(report.decision, TrustedProxyFeasibilityDecision::FeasiblePreview);
        assert_eq!(report.reason_codes, vec![TrustedProxyFeasibilityReasonCode::ReadyForPreview]);
        assert!(report.promotion_blockers.is_empty());
        assert!(report.preview_only);
    }
}
