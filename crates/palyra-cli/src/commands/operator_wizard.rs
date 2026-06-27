//! Operator wizards behind `palyra onboarding`, `palyra setup`, and `palyra configure`.
//!
//! Wizard steps collect a mutation plan that is applied through the shared safe config
//! mutation layer (daemon-schema validation plus backups). API keys land in the vault;
//! configs that carry inline secrets are persisted with owner-only file semantics.

mod model_auth;

use std::{
    collections::BTreeMap,
    io::IsTerminal,
    path::{Path, PathBuf},
    time::Duration,
};

use self::model_auth::{
    api_key_field_label, api_key_prompt_message, auth_method_flow, auth_method_label,
    auth_method_requires_api_key, model_provider_auth_choices, provider_display_name,
    registry_provider_defaults_for_auth_method, AuthMethodFlow, RegistryProviderDefaults,
    DEFAULT_MINIMAX_BASE_URL, DEFAULT_MINIMAX_CN_BASE_URL, GOOGLE_GEMINI_AUTH_PROVIDER_KIND,
    GOOGLE_GEMINI_CLI_AUTH_PROVIDER_KIND, MINIMAX_AUTH_PROVIDER_KIND,
    OPENROUTER_AUTH_PROVIDER_KIND, XAI_AUTH_PROVIDER_KIND,
};
use crate::commands::models::{
    parse_discovered_provider_models, provider_models_endpoint, sanitize_provider_error,
    select_preferred_discovered_model, select_preferred_discovered_model_id,
    DiscoveredProviderModel,
};
use palyra_common::runtime_preview::{
    RuntimePreviewCapability, RuntimePreviewMode, ALL_RUNTIME_PREVIEW_CAPABILITIES,
};
use reqwest::{
    blocking::Client,
    header::{HeaderMap, HeaderValue, ACCEPT, AUTHORIZATION},
};
use serde::Serialize;

use crate::{
    commands::wizard::{
        InteractiveWizardBackend, NonInteractiveWizardBackend, StepChoice, StepKind, WizardBackend,
        WizardError, WizardSession, WizardStep, WizardValue,
    },
    *,
};

const CONFIGURE_BACKUPS: usize = 5;
const INLINE_SECRET_CONFIG_PATHS: &[&str] = &[
    "admin.auth_token",
    "model_provider.openai_api_key",
    "model_provider.anthropic_api_key",
    "tool_call.browser_service.auth_token",
];
const MINIMAX_BASE_URL_ENV: &str = "PALYRA_MODEL_PROVIDER_MINIMAX_BASE_URL";
const TRUSTED_MINIMAX_DISCOVERY_HOSTS: &[&str] = &["api.minimax.io", "api.minimaxi.com"];
const OPENAI_DEFAULT_BASE_URL: &str = "https://api.openai.com/v1";
const ANTHROPIC_DEFAULT_BASE_URL: &str = "https://api.anthropic.com";
const XAI_BASE_URL_ENV: &str = "PALYRA_MODEL_PROVIDER_XAI_BASE_URL";
const GOOGLE_GEMINI_BASE_URL_ENV: &str = "PALYRA_MODEL_PROVIDER_GOOGLE_GEMINI_BASE_URL";
const OPENROUTER_BASE_URL_ENV: &str = "PALYRA_MODEL_PROVIDER_OPENROUTER_BASE_URL";
const PROVIDER_MODEL_DISCOVERY_TIMEOUT: Duration = Duration::from_secs(10);
const HTTP_FETCH_CREDENTIAL_VAULT_REFS_PATH: &str =
    "tool_call.http_fetch.allowed_credential_vault_refs";
#[cfg(test)]
const OPENAI_API_CURATED_DEFAULT_ORDER: &[&str] =
    &["gpt-5.5", "gpt-5.4", "gpt-5.4-mini", "gpt-4.1", "gpt-4o"];

/// Parameters for the onboarding/setup wizard, assembled from CLI arguments.
#[derive(Debug, Clone)]
pub(crate) struct OnboardingWizardRequest {
    pub(crate) path: Option<String>,
    pub(crate) force: bool,
    pub(crate) setup_mode: Option<InitModeArg>,
    pub(crate) setup_tls_scaffold: Option<InitTlsScaffoldArg>,
    pub(crate) options: WizardOverridesArg,
}

/// Parameters for the section-based configure wizard, assembled from CLI arguments.
#[derive(Debug, Clone)]
pub(crate) struct ConfigureWizardRequest {
    pub(crate) path: Option<String>,
    pub(crate) sections: Vec<ConfigureSectionArg>,
    pub(crate) deployment_profile: Option<DeploymentProfileArg>,
    pub(crate) non_interactive: bool,
    pub(crate) accept_risk: bool,
    pub(crate) json: bool,
    pub(crate) workspace_root: Option<String>,
    pub(crate) auth_method: Option<OnboardingAuthMethodArg>,
    pub(crate) api_key_env: Option<String>,
    pub(crate) api_key_stdin: bool,
    pub(crate) api_key_prompt: bool,
    pub(crate) bind_profile: Option<GatewayBindProfileArg>,
    pub(crate) daemon_port: Option<u16>,
    pub(crate) grpc_port: Option<u16>,
    pub(crate) quic_port: Option<u16>,
    pub(crate) tls_scaffold: Option<InitTlsScaffoldArg>,
    pub(crate) tls_cert_path: Option<String>,
    pub(crate) tls_key_path: Option<String>,
    pub(crate) remote_base_url: Option<String>,
    pub(crate) admin_token_env: Option<String>,
    pub(crate) admin_token_stdin: bool,
    pub(crate) admin_token_prompt: bool,
    pub(crate) remote_verification: Option<RemoteVerificationModeArg>,
    pub(crate) pinned_server_cert_sha256: Option<String>,
    pub(crate) pinned_gateway_ca_sha256: Option<String>,
    pub(crate) ssh_target: Option<String>,
    pub(crate) skip_health: bool,
    pub(crate) skip_channels: bool,
    pub(crate) skip_skills: bool,
}

/// Top-level onboarding flow selected by the operator or CLI flags.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WizardFlowKind {
    Quickstart,
    Manual,
    Remote,
}

impl WizardFlowKind {
    fn as_str(self) -> &'static str {
        match self {
            Self::Quickstart => "quickstart",
            Self::Manual => "manual",
            Self::Remote => "remote",
        }
    }

    fn from_arg(value: OnboardingFlowArg) -> Self {
        match value {
            OnboardingFlowArg::Quickstart => Self::Quickstart,
            OnboardingFlowArg::Manual => Self::Manual,
            OnboardingFlowArg::Remote => Self::Remote,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ExistingConfigAction {
    Reuse,
    Overwrite,
    Abort,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RemoteAccessPattern {
    SshTunnel,
    VerifiedHttps,
}

/// Aggregate post-apply health outcome reported in the onboarding summary.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Default)]
#[serde(rename_all = "snake_case")]
enum HealthStatus {
    ConfigReady,
    RemoteVerified,
    RuntimeRestartRequired,
    #[default]
    Skipped,
    ManualFollowUpRequired,
}

/// How the wizard handled background gateway service installation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Default)]
#[serde(rename_all = "snake_case")]
enum ServiceInstallMode {
    #[default]
    NotNow,
    GuidanceOnly,
    InstallNow,
    InstallFailedDeferred,
}

impl ServiceInstallMode {
    fn as_str(self) -> &'static str {
        match self {
            Self::NotNow => "not_now",
            Self::GuidanceOnly => "guidance_only",
            Self::InstallNow => "install_now",
            Self::InstallFailedDeferred => "install_failed_deferred",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SecretInputs {
    api_key: Option<String>,
    admin_token: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
struct HealthCheckSummary {
    check: String,
    status: String,
    detail: String,
}

#[derive(Debug, Clone)]
struct HealthCheckReport {
    status: HealthStatus,
    checks: Vec<HealthCheckSummary>,
}

/// Filesystem locations prepared before the onboarding plan is applied.
#[derive(Debug, Clone)]
struct ApplyContext {
    config_path: PathBuf,
    state_root: PathBuf,
    identity_store_dir: PathBuf,
    vault_dir: PathBuf,
    tls_paths: Option<(PathBuf, PathBuf)>,
}

#[derive(Debug, Clone)]
struct BindProfileConfig {
    bind_profile: String,
    tls_scaffold: Option<InitTlsScaffoldArg>,
    tls_cert_path: Option<String>,
    tls_key_path: Option<String>,
    accept_risk: bool,
}

/// Accumulated wizard decisions, applied to disk in one pass by `apply_onboarding_plan`.
#[derive(Debug, Default, Clone)]
struct OnboardingMutationPlan {
    flow: String,
    deployment_profile: palyra_common::deployment_profiles::DeploymentProfileId,
    deployment_mode: String,
    workspace_root: Option<String>,
    auth_method: String,
    api_key: Option<String>,
    daemon_port: Option<u16>,
    grpc_port: Option<u16>,
    quic_port: Option<u16>,
    bind_profile: String,
    tls_enabled: bool,
    tls_cert_path: Option<String>,
    tls_key_path: Option<String>,
    public_bind_ack: bool,
    admin_token: Option<String>,
    remote_base_url: Option<String>,
    remote_verification: Option<String>,
    pinned_server_cert_sha256: Option<String>,
    pinned_gateway_ca_sha256: Option<String>,
    ssh_target: Option<String>,
    skipped_sections: Vec<String>,
    warnings: Vec<String>,
    risk_events: Vec<String>,
    service_install_mode: ServiceInstallMode,
    existing_config_action: Option<ExistingConfigAction>,
    health_status: HealthStatus,
}

#[derive(Debug, Serialize)]
struct OnboardingSummary {
    status: &'static str,
    flow: String,
    deployment_profile: String,
    deployment_mode: String,
    config_path: String,
    state_root: String,
    workspace_root: Option<String>,
    auth_method: String,
    dashboard_url: String,
    health_status: HealthStatus,
    health_checks: Vec<HealthCheckSummary>,
    skipped_sections: Vec<String>,
    warnings: Vec<String>,
    risk_events: Vec<String>,
    service_install_mode: ServiceInstallMode,
    remote_verification: Option<String>,
    ssh_target: Option<String>,
    recommended_step_id: Option<&'static str>,
    next_step: Option<&'static str>,
    skills: SkillsInventorySnapshot,
}

#[derive(Debug, Clone, Serialize)]
struct ConfigureSectionChange {
    section: String,
    changed: bool,
    before: Vec<String>,
    after: Vec<String>,
    restart_required: bool,
    follow_up_checks: Vec<String>,
}

#[derive(Debug, Serialize)]
struct ConfigureSummary {
    status: &'static str,
    config_path: String,
    changed_sections: Vec<String>,
    unchanged_sections: Vec<String>,
    restart_required: Vec<String>,
    section_changes: Vec<ConfigureSectionChange>,
    follow_up_checks: Vec<String>,
    warnings: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    runtime_reload: Option<crate::commands::runtime_reload::RuntimeConfigReloadOutcome>,
}

/// Runs `palyra setup` by delegating to the onboarding wizard with an explicit init mode.
///
/// # Errors
/// Returns an error when the operator cancels or any onboarding step fails.
pub(crate) fn run_setup_wizard(
    mode: InitModeArg,
    path: Option<String>,
    force: bool,
    tls_scaffold: InitTlsScaffoldArg,
    wizard_options: WizardOverridesArg,
) -> Result<()> {
    run_onboarding_wizard(OnboardingWizardRequest {
        path,
        force,
        setup_mode: Some(mode),
        setup_tls_scaffold: Some(tls_scaffold),
        options: wizard_options,
    })
}

/// Drives the full onboarding flow: collect a mutation plan, apply it to disk,
/// run post-apply health checks, and emit the operator summary.
///
/// # Errors
/// Returns an error when the operator cancels, secret sources are misused,
/// config mutation or validation fails, or the summary cannot be emitted.
pub(crate) fn run_onboarding_wizard(request: OnboardingWizardRequest) -> Result<()> {
    let flow = resolve_onboarding_flow(request.setup_mode, request.options.flow);
    let config_path = match request.setup_mode {
        Some(_) => resolve_init_path(request.path.clone())?,
        None => resolve_onboarding_path(request.path.clone())?,
    };
    let answers = build_onboarding_answers(&request, flow)?;
    let mut backend = build_backend(request.options.non_interactive, answers)?;
    let mut wizard = WizardSession::new(backend.as_mut());

    let mut plan = execute_onboarding_flow(&mut wizard, &request, flow, config_path.as_path())?;
    let apply_context =
        prepare_apply_context(config_path.as_path(), request.force, plan.existing_config_action)?;
    let dashboard_url = apply_onboarding_plan(&apply_context, &mut plan)?;
    let health_report = if request.options.skip_health {
        plan.risk_events.push("health_checks_skipped".to_owned());
        HealthCheckReport {
            status: HealthStatus::Skipped,
            checks: vec![HealthCheckSummary {
                check: "post_apply_health".to_owned(),
                status: "skipped".to_owned(),
                detail: "health checks were skipped by explicit operator choice".to_owned(),
            }],
        }
    } else {
        run_post_apply_health_check(flow, &apply_context, &plan)?
    };
    plan.health_status = health_report.status;
    let (status, recommended_step_id, next_step) = onboarding_summary_next_step(
        flow,
        plan.service_install_mode,
        plan.health_status,
        plan.auth_method.as_str(),
    );
    let summary = OnboardingSummary {
        status,
        flow: plan.flow,
        deployment_profile: plan.deployment_profile.as_str().to_owned(),
        deployment_mode: plan.deployment_mode,
        config_path: apply_context.config_path.display().to_string(),
        state_root: apply_context.state_root.display().to_string(),
        workspace_root: plan.workspace_root,
        auth_method: plan.auth_method,
        dashboard_url,
        health_status: plan.health_status,
        health_checks: health_report.checks,
        skipped_sections: plan.skipped_sections,
        warnings: plan.warnings,
        risk_events: plan.risk_events,
        service_install_mode: plan.service_install_mode,
        remote_verification: plan.remote_verification,
        ssh_target: plan.ssh_target,
        recommended_step_id,
        next_step,
        skills: build_default_skills_inventory_snapshot(),
    };
    emit_onboarding_summary(&summary, output::preferred_json(request.options.json))
}

/// Reconfigures selected sections of an existing config and emits a per-section
/// change summary; the file is rewritten only when a section actually changed.
///
/// # Errors
/// Returns an error when the config is missing or invalid, the operator cancels,
/// or the mutated document fails daemon-schema validation or persistence.
pub(crate) fn run_configure_wizard(request: ConfigureWizardRequest) -> Result<()> {
    let config_path = resolve_config_path(request.path.clone(), true)?;
    let path_ref = Path::new(&config_path);
    let apply_context = prepare_apply_context(path_ref, true, None)?;
    let original_document = load_document_from_existing_path(path_ref)
        .with_context(|| format!("failed to parse {}", path_ref.display()))?
        .0;
    let mut document = original_document.clone();
    let answers = build_configure_answers(&request)?;
    let mut backend = build_backend(request.non_interactive, answers)?;
    let mut wizard = WizardSession::new(backend.as_mut());
    let sections = select_configure_sections(&mut wizard, &request)?;
    let mut changed_sections = Vec::new();
    let mut unchanged_sections = Vec::new();
    let mut restart_required = Vec::new();
    let mut section_changes = Vec::new();
    let mut warnings = Vec::new();
    let mut follow_up_checks =
        vec!["palyra config validate".to_owned(), "palyra security audit".to_owned()];

    for section in sections {
        let before_snapshot = describe_configure_section(&document, section)?;
        let before = document.clone();
        match section {
            ConfigureSectionArg::DeploymentProfile => {
                wizard.note(WizardStep::note(
                    "configure.deployment_profile.note",
                    "Deployment Profile",
                    format!(
                        "Select the canonical profile used for config defaults, deployment recipes, and health preflights. Current state: {}",
                        join_section_state(before_snapshot.as_slice())
                    ),
                ))?;
                let current_profile = get_string_value_at_path(&document, "deployment.profile")?
                    .unwrap_or_else(|| {
                        let worker_enabled =
                            get_bool_value_at_path(&document, "feature_rollouts.networked_workers")
                                .ok()
                                .flatten()
                                .unwrap_or(false);
                        palyra_common::deployment_profiles::derive_deployment_profile(
                            None,
                            get_string_value_at_path(&document, "deployment.mode")
                                .ok()
                                .flatten()
                                .as_deref(),
                            worker_enabled,
                        )
                        .as_str()
                        .to_owned()
                    });
                let deployment_profile = wizard.select(select_step(
                    "deployment_profile",
                    "Deployment Profile",
                    "Choose the profile that should own bootstrap defaults and rollout posture.",
                    vec![
                        choice("local", "Local", Some("loopback-only workstation runtime")),
                        choice(
                            "single-vm",
                            "Single VM",
                            Some("loopback-first service profile for one host"),
                        ),
                        choice(
                            "worker-enabled",
                            "Worker Enabled",
                            Some("service profile with guarded networked worker execution"),
                        ),
                    ],
                    Some(current_profile),
                ))?;
                let deployment_profile =
                    palyra_common::deployment_profiles::DeploymentProfileId::parse(
                        deployment_profile.as_str(),
                    )
                    .context("configure selected an invalid deployment profile")?;
                apply_deployment_profile_defaults(&mut document, deployment_profile)?;
                set_value_at_path(
                    &mut document,
                    "deployment.profile",
                    toml::Value::String(deployment_profile.as_str().to_owned()),
                )?;
                set_value_at_path(
                    &mut document,
                    "deployment.mode",
                    toml::Value::String(deployment_profile.deployment_mode().to_owned()),
                )?;
                set_value_at_path(
                    &mut document,
                    "gateway.bind_profile",
                    toml::Value::String(deployment_profile.bind_profile().to_owned()),
                )?;
                follow_up_checks.push(format!(
                    "palyra deployment preflight --deployment-profile {}",
                    deployment_profile.as_str()
                ));
            }
            ConfigureSectionArg::Workspace => {
                wizard.note(WizardStep::note(
                    "configure.workspace.note",
                    "Workspace",
                    format!(
                        "Update the workspace root used by the process runner. Current state: {}",
                        join_section_state(before_snapshot.as_slice())
                    ),
                ))?;
                let current =
                    get_string_value_at_path(&document, "tool_call.process_runner.workspace_root")?
                        .unwrap_or_else(default_workspace_root);
                let value = wizard.text(
                    text_step(
                        "workspace_root",
                        "Workspace Root",
                        "Select the primary workspace root for local tool execution.",
                        Some(current),
                        None,
                        false,
                    ),
                    |value| validate_non_empty_text(value, "workspace root"),
                )?;
                let normalized = normalize_workspace_root(value.as_str())?;
                ensure_directory_exists(Path::new(&normalized))?;
                set_value_at_path(
                    &mut document,
                    "tool_call.process_runner.workspace_root",
                    toml::Value::String(normalized),
                )?;
            }
            ConfigureSectionArg::AuthModel => {
                wizard.note(WizardStep::note(
                    "configure.auth.note",
                    "Model/Auth",
                    format!(
                        "Configure the OpenAI-compatible provider and credential source. Current state: {}",
                        join_section_state(before_snapshot.as_slice())
                    ),
                ))?;
                let current_auth = current_auth_method(&document);
                let auth_method = wizard.select(select_step(
                    "auth_method",
                    "Auth Method",
                    "Choose how this installation should authenticate to model providers.",
                    model_provider_auth_choices(),
                    Some(current_auth),
                ))?;
                apply_auth_method_choice(
                    &mut wizard,
                    &mut document,
                    auth_method.as_str(),
                    &mut warnings,
                )?;
                if ensure_runtime_defaults(&mut document, &apply_context)? {
                    warnings.push(
                        "runtime path defaults were backfilled so the daemon and CLI share the same local identity and vault state."
                            .to_owned(),
                    );
                }
                if ensure_admin_auth_defaults(&mut document)? {
                    warnings.push(
                        "admin auth defaults were backfilled so the daemon can start after this reconfiguration."
                            .to_owned(),
                    );
                }
                if ensure_missing_deployment_profile_defaults(
                    &mut document,
                    palyra_common::deployment_profiles::DeploymentProfileId::Local,
                )? {
                    warnings.push(
                        "local deployment defaults were backfilled so deployment preflight and runtime startup use the same loopback posture."
                            .to_owned(),
                    );
                }
            }
            ConfigureSectionArg::Gateway => {
                wizard.note(WizardStep::note(
                    "configure.gateway.note",
                    "Gateway",
                    format!(
                        "Review bind posture, remote access, TLS, and dashboard verification pins. Current state: {}",
                        join_section_state(before_snapshot.as_slice())
                    ),
                ))?;
                let current_bind = get_string_value_at_path(&document, "gateway.bind_profile")?
                    .unwrap_or_else(|| "loopback_only".to_owned());
                let bind_profile = wizard.select(select_step(
                    "bind_profile",
                    "Bind Profile",
                    "Choose how the daemon should expose its control-plane endpoints.",
                    vec![
                        choice(
                            "loopback_only",
                            "Loopback Only",
                            Some("safe default for local and tunnel-first use"),
                        ),
                        choice(
                            "public_tls",
                            "Public TLS",
                            Some("requires TLS and explicit dangerous-bind acknowledgement"),
                        ),
                    ],
                    Some(current_bind),
                ))?;
                configure_bind_profile(
                    &mut wizard,
                    &mut document,
                    BindProfileConfig {
                        bind_profile: bind_profile.as_str().to_owned(),
                        tls_scaffold: request.tls_scaffold,
                        tls_cert_path: request.tls_cert_path.clone(),
                        tls_key_path: request.tls_key_path.clone(),
                        accept_risk: request.accept_risk,
                    },
                    &mut warnings,
                )?;
                apply_port_updates(
                    &mut wizard,
                    &mut document,
                    request.daemon_port,
                    request.grpc_port,
                    request.quic_port,
                )?;
                apply_remote_dashboard_settings(
                    &mut wizard,
                    &mut document,
                    request.remote_base_url.clone(),
                    request.remote_verification,
                    request.pinned_server_cert_sha256.clone(),
                    request.pinned_gateway_ca_sha256.clone(),
                    &mut warnings,
                )?;
            }
            ConfigureSectionArg::RuntimeControls => {
                wizard.note(WizardStep::note(
                    "configure.runtime_controls.note",
                    "Runtime Controls",
                    format!(
                        "Review rollout posture, preview modes, and activation blockers for guarded runtime capabilities. Current state: {}",
                        join_section_state(before_snapshot.as_slice())
                    ),
                ))?;
                configure_runtime_controls(&mut wizard, &mut document)?;
            }
            ConfigureSectionArg::DaemonService => {
                wizard.note(WizardStep::note(
                    "configure.service.note",
                    "Daemon / Service",
                    format!(
                        "Gateway service lifecycle is available via `palyra gateway install|start|stop|restart|uninstall`. This section records the current state and next-step guidance. Current state: {}",
                        join_section_state(before_snapshot.as_slice())
                    ),
                ))?;
                wizard.action(WizardStep::action(
                    "configure.service.action",
                    "Service Guidance",
                    "Use `palyra gateway install --start` after applying changes to register the background gateway service, then verify it with `palyra gateway status`.",
                ))?;
                follow_up_checks.push("palyra gateway install --start".to_owned());
                follow_up_checks.push("palyra gateway status".to_owned());
            }
            ConfigureSectionArg::Channels => {
                wizard.note(WizardStep::note(
                    "configure.channels.note",
                    "Channels",
                    format!(
                        "Channel lifecycle is still provider-specific. The configure wizard records the effective state and the next-step guidance here. Current state: {}",
                        join_section_state(before_snapshot.as_slice())
                    ),
                ))?;
                unchanged_sections.push("channels".to_owned());
                warnings.push(
                    "channels section is currently guidance-only; use `palyra channels discord setup` for live connector onboarding."
                        .to_owned(),
                );
                continue;
            }
            ConfigureSectionArg::Skills => {
                let skills_snapshot = build_default_skills_inventory_snapshot();
                wizard.note(WizardStep::note(
                    "configure.skills.note",
                    "Skills",
                    format!(
                        "Review the installed skill inventory and trust posture before changing operators or rollout flow. Current state: {}",
                        join_section_state(before_snapshot.as_slice())
                    ),
                ))?;
                unchanged_sections.push("skills".to_owned());
                warnings.push(format!(
                    "skills inventory snapshot: installed={} eligible={} quarantined={} runtime_unknown={}; use `palyra skills info|check|list` for concrete actions.",
                    skills_snapshot.installed_total,
                    skills_snapshot.eligible_total,
                    skills_snapshot.quarantined_total,
                    skills_snapshot.runtime_unknown_total
                ));
                continue;
            }
            ConfigureSectionArg::HealthSecurity => {
                wizard.note(WizardStep::note(
                    "configure.health.note",
                    "Health / Security",
                    format!(
                        "Review the effective health and security posture before running follow-up checks. Current state: {}",
                        join_section_state(before_snapshot.as_slice())
                    ),
                ))?;
                wizard.progress(
                    WizardStep::progress(
                        "configure.health.progress",
                        "Health / Security",
                        "Validating the resulting config and preparing follow-up checks.",
                    ),
                    || {
                        validate_daemon_compatible_document(&document).map_err(|error| {
                            WizardError::Validation {
                                step_id: "configure.health.progress".to_owned(),
                                message: error.to_string(),
                            }
                        })?;
                        Ok(())
                    },
                )?;
                follow_up_checks.push("palyra doctor".to_owned());
                follow_up_checks.push("palyra gateway status".to_owned());
            }
        }

        let changed = document != before;
        let section_restart_required = section_requires_restart(section, changed);
        if changed {
            changed_sections.push(section.slug().to_owned());
            if section_restart_required {
                restart_required.push(section.slug().to_owned());
            }
        } else {
            unchanged_sections.push(section.slug().to_owned());
        }
        let section_follow_up_checks = section_follow_up_checks(section, &document)?;
        follow_up_checks.extend(section_follow_up_checks.iter().cloned());
        section_changes.push(ConfigureSectionChange {
            section: section.slug().to_owned(),
            changed,
            before: before_snapshot,
            after: describe_configure_section(&document, section)?,
            restart_required: section_restart_required,
            follow_up_checks: section_follow_up_checks,
        });
    }

    validate_daemon_compatible_document(&document).with_context(|| {
        format!("mutated config {} does not match daemon schema", path_ref.display())
    })?;
    if document != original_document {
        write_operator_document_with_backups(path_ref, &document)
            .with_context(|| format!("failed to persist config {}", path_ref.display()))?;
    }
    let runtime_reload = if document != original_document
        && section_changes
            .iter()
            .any(|change| change.section == ConfigureSectionArg::AuthModel.slug() && change.changed)
    {
        Some(crate::commands::runtime_reload::try_apply_active_config_reload_blocking(Some(
            config_path.clone(),
        )))
    } else {
        None
    };
    dedupe_strings(&mut changed_sections);
    dedupe_strings(&mut unchanged_sections);
    dedupe_strings(&mut restart_required);
    dedupe_strings(&mut follow_up_checks);

    let summary = ConfigureSummary {
        status: "complete",
        config_path,
        changed_sections,
        unchanged_sections,
        restart_required,
        section_changes,
        follow_up_checks,
        warnings,
        runtime_reload,
    };
    emit_configure_summary(&summary, output::preferred_json(request.json))
}

/// Persists the config with backups, switching to owner-only secret-file semantics
/// when the document carries an inline secret value.
fn write_operator_document_with_backups(path: &Path, document: &toml::Value) -> Result<()> {
    if document_contains_inline_secret(document)? {
        write_secret_document_with_backups(path, document, CONFIGURE_BACKUPS)?;
    } else {
        write_document_with_backups(path, document, CONFIGURE_BACKUPS)?;
    }
    Ok(())
}

/// Reports whether any known secret-bearing config path holds an inline string value.
fn document_contains_inline_secret(document: &toml::Value) -> Result<bool> {
    for path in INLINE_SECRET_CONFIG_PATHS {
        if get_string_value_at_path(document, path)?.is_some() {
            return Ok(true);
        }
    }
    Ok(false)
}

fn resolve_onboarding_flow(
    setup_mode: Option<InitModeArg>,
    explicit: Option<OnboardingFlowArg>,
) -> WizardFlowKind {
    if let Some(explicit) = explicit {
        return WizardFlowKind::from_arg(explicit);
    }
    match setup_mode {
        Some(InitModeArg::Remote) => WizardFlowKind::Remote,
        _ => WizardFlowKind::Quickstart,
    }
}

fn default_deployment_profile_for_flow(
    flow: WizardFlowKind,
    setup_mode: Option<InitModeArg>,
) -> palyra_common::deployment_profiles::DeploymentProfileId {
    if flow == WizardFlowKind::Remote {
        return palyra_common::deployment_profiles::DeploymentProfileId::SingleVm;
    }
    setup_mode
        .map(InitMode::from_arg)
        .map(default_deployment_profile_for_init)
        .unwrap_or(palyra_common::deployment_profiles::DeploymentProfileId::Local)
}

fn build_backend(
    non_interactive: bool,
    answers: BTreeMap<String, WizardValue>,
) -> Result<Box<dyn WizardBackend>> {
    if non_interactive {
        return Ok(Box::new(NonInteractiveWizardBackend::new(answers)));
    }
    ensure_interactive_terminal()?;
    Ok(Box::new(InteractiveWizardBackend::with_answers(answers)))
}

fn ensure_interactive_terminal() -> Result<()> {
    if !std::io::stdin().is_terminal()
        || !std::io::stdout().is_terminal()
        || !std::io::stderr().is_terminal()
    {
        anyhow::bail!(
            "interactive wizard requires stdin/stdout/stderr TTY; rerun with --non-interactive for scripted execution"
        );
    }
    Ok(())
}

fn build_onboarding_answers(
    request: &OnboardingWizardRequest,
    flow: WizardFlowKind,
) -> Result<BTreeMap<String, WizardValue>> {
    validate_stdin_secret_usage(
        request.options.non_interactive,
        request.options.api_key_stdin,
        request.options.admin_token_stdin,
    )?;
    let secrets = collect_secret_inputs(
        request.options.api_key_env.clone(),
        request.options.api_key_stdin,
        request.options.api_key_prompt,
        request.options.admin_token_env.clone(),
        request.options.admin_token_stdin,
        request.options.admin_token_prompt,
    )?;

    let mut answers = BTreeMap::new();
    answers.insert("flow".to_owned(), WizardValue::Choice(flow.as_str().to_owned()));
    if request.force {
        answers.insert(
            "existing_config_action".to_owned(),
            WizardValue::Choice("overwrite".to_owned()),
        );
    }
    if request.options.accept_risk {
        answers.insert("accept_risk_ack".to_owned(), WizardValue::Bool(true));
        answers.insert("public_bind_ack".to_owned(), WizardValue::Bool(true));
        answers.insert("remote_without_pin_ack".to_owned(), WizardValue::Bool(true));
    }
    if let Some(workspace_root) = request.options.workspace_root.as_ref() {
        answers.insert("workspace_root".to_owned(), WizardValue::Text(workspace_root.clone()));
    }
    let auth_method = request.options.auth_method.map(auth_method_value).or_else(|| {
        (request.options.api_key_env.is_some()
            || request.options.api_key_stdin
            || request.options.api_key_prompt)
            .then(|| "api_key".to_owned())
    });
    validate_api_key_secret_matches_auth_method(auth_method.as_deref(), secrets.api_key.is_some())?;
    if let Some(auth_method) = auth_method {
        answers.insert("auth_method".to_owned(), WizardValue::Choice(auth_method));
    }
    if let Some(deployment_profile) = request.options.deployment_profile {
        answers.insert(
            "deployment_profile".to_owned(),
            WizardValue::Choice(deployment_profile_value(deployment_profile).to_owned()),
        );
    }
    if let Some(api_key) = secrets.api_key {
        answers.insert("model_provider_api_key".to_owned(), WizardValue::SensitiveText(api_key));
    }
    if let Some(bind_profile) = request.options.bind_profile {
        answers.insert(
            "bind_profile".to_owned(),
            WizardValue::Choice(bind_profile_value(bind_profile).to_owned()),
        );
    }
    insert_optional_u16_answer(&mut answers, "daemon_port", request.options.daemon_port);
    insert_optional_u16_answer(&mut answers, "grpc_port", request.options.grpc_port);
    insert_optional_u16_answer(&mut answers, "quic_port", request.options.quic_port);
    if let Some(tls_scaffold) = request.options.tls_scaffold {
        answers.insert(
            "tls_scaffold".to_owned(),
            WizardValue::Choice(tls_scaffold_value(tls_scaffold).to_owned()),
        );
    }
    if let Some(tls_cert_path) = request.options.tls_cert_path.as_ref() {
        answers.insert("tls_cert_path".to_owned(), WizardValue::Text(tls_cert_path.clone()));
    }
    if let Some(tls_key_path) = request.options.tls_key_path.as_ref() {
        answers.insert("tls_key_path".to_owned(), WizardValue::Text(tls_key_path.clone()));
    }
    if let Some(remote_base_url) = request.options.remote_base_url.as_ref() {
        answers.insert("remote_base_url".to_owned(), WizardValue::Text(remote_base_url.clone()));
        answers.insert(
            "remote_access_pattern".to_owned(),
            WizardValue::Choice("verified_https".to_owned()),
        );
    }
    if let Some(admin_token) = secrets.admin_token {
        answers.insert("store_admin_token".to_owned(), WizardValue::Bool(true));
        answers.insert("admin_token".to_owned(), WizardValue::SensitiveText(admin_token));
    }
    if let Some(remote_verification) = request.options.remote_verification {
        answers.insert(
            "remote_verification".to_owned(),
            WizardValue::Choice(remote_verification_value(remote_verification).to_owned()),
        );
    }
    if let Some(value) = request.options.pinned_server_cert_sha256.as_ref() {
        answers.insert("pinned_server_cert_sha256".to_owned(), WizardValue::Text(value.clone()));
    }
    if let Some(value) = request.options.pinned_gateway_ca_sha256.as_ref() {
        answers.insert("pinned_gateway_ca_sha256".to_owned(), WizardValue::Text(value.clone()));
    }
    if let Some(ssh_target) = request.options.ssh_target.as_ref() {
        answers.insert("ssh_target".to_owned(), WizardValue::Text(ssh_target.clone()));
        answers.insert(
            "remote_access_pattern".to_owned(),
            WizardValue::Choice("ssh_tunnel".to_owned()),
        );
    }
    if request.options.skip_health {
        answers.insert("run_health_checks".to_owned(), WizardValue::Bool(false));
    }
    if request.options.skip_channels {
        answers.insert("configure_channels".to_owned(), WizardValue::Bool(false));
    }
    if request.options.skip_skills {
        answers.insert("configure_skills".to_owned(), WizardValue::Bool(false));
    }
    Ok(answers)
}

fn build_configure_answers(
    request: &ConfigureWizardRequest,
) -> Result<BTreeMap<String, WizardValue>> {
    validate_stdin_secret_usage(
        request.non_interactive,
        request.api_key_stdin,
        request.admin_token_stdin,
    )?;
    let secrets = collect_secret_inputs(
        request.api_key_env.clone(),
        request.api_key_stdin,
        request.api_key_prompt,
        request.admin_token_env.clone(),
        request.admin_token_stdin,
        request.admin_token_prompt,
    )?;

    let mut answers = BTreeMap::new();
    if request.accept_risk {
        answers.insert("accept_risk_ack".to_owned(), WizardValue::Bool(true));
        answers.insert("public_bind_ack".to_owned(), WizardValue::Bool(true));
        answers.insert("remote_without_pin_ack".to_owned(), WizardValue::Bool(true));
    }
    if let Some(workspace_root) = request.workspace_root.as_ref() {
        answers.insert("workspace_root".to_owned(), WizardValue::Text(workspace_root.clone()));
    }
    if !request.sections.is_empty() {
        answers.insert(
            "configure_sections".to_owned(),
            WizardValue::Multi(
                request.sections.iter().map(|value| value.slug().to_owned()).collect(),
            ),
        );
    }
    if let Some(deployment_profile) = request.deployment_profile {
        answers.insert(
            "deployment_profile".to_owned(),
            WizardValue::Choice(deployment_profile_value(deployment_profile).to_owned()),
        );
    }
    let auth_method = request.auth_method.map(auth_method_value);
    validate_api_key_secret_matches_auth_method(auth_method.as_deref(), secrets.api_key.is_some())?;
    if let Some(auth_method) = auth_method {
        answers.insert("auth_method".to_owned(), WizardValue::Choice(auth_method));
    }
    if let Some(api_key) = secrets.api_key {
        answers.insert("model_provider_api_key".to_owned(), WizardValue::SensitiveText(api_key));
    }
    if let Some(bind_profile) = request.bind_profile {
        answers.insert(
            "bind_profile".to_owned(),
            WizardValue::Choice(bind_profile_value(bind_profile).to_owned()),
        );
    }
    insert_optional_u16_answer(&mut answers, "daemon_port", request.daemon_port);
    insert_optional_u16_answer(&mut answers, "grpc_port", request.grpc_port);
    insert_optional_u16_answer(&mut answers, "quic_port", request.quic_port);
    if let Some(tls_scaffold) = request.tls_scaffold {
        answers.insert(
            "tls_scaffold".to_owned(),
            WizardValue::Choice(tls_scaffold_value(tls_scaffold).to_owned()),
        );
    }
    if let Some(value) = request.tls_cert_path.as_ref() {
        answers.insert("tls_cert_path".to_owned(), WizardValue::Text(value.clone()));
    }
    if let Some(value) = request.tls_key_path.as_ref() {
        answers.insert("tls_key_path".to_owned(), WizardValue::Text(value.clone()));
    }
    if let Some(value) = request.remote_base_url.as_ref() {
        answers.insert("remote_base_url".to_owned(), WizardValue::Text(value.clone()));
    }
    if let Some(value) = secrets.admin_token {
        answers.insert("store_admin_token".to_owned(), WizardValue::Bool(true));
        answers.insert("admin_token".to_owned(), WizardValue::SensitiveText(value));
    }
    if let Some(remote_verification) = request.remote_verification {
        answers.insert(
            "remote_verification".to_owned(),
            WizardValue::Choice(remote_verification_value(remote_verification).to_owned()),
        );
    }
    if let Some(value) = request.pinned_server_cert_sha256.as_ref() {
        answers.insert("pinned_server_cert_sha256".to_owned(), WizardValue::Text(value.clone()));
    }
    if let Some(value) = request.pinned_gateway_ca_sha256.as_ref() {
        answers.insert("pinned_gateway_ca_sha256".to_owned(), WizardValue::Text(value.clone()));
    }
    if let Some(value) = request.ssh_target.as_ref() {
        answers.insert("ssh_target".to_owned(), WizardValue::Text(value.clone()));
    }
    if request.skip_health {
        answers.insert("run_health_checks".to_owned(), WizardValue::Bool(false));
    }
    if request.skip_channels {
        answers.insert("configure_channels".to_owned(), WizardValue::Bool(false));
    }
    if request.skip_skills {
        answers.insert("configure_skills".to_owned(), WizardValue::Bool(false));
    }
    Ok(answers)
}

/// Runs the wizard step sequence and collects the onboarding mutation plan.
fn execute_onboarding_flow(
    wizard: &mut WizardSession<'_, dyn WizardBackend>,
    request: &OnboardingWizardRequest,
    flow: WizardFlowKind,
    config_path: &Path,
) -> Result<OnboardingMutationPlan> {
    wizard.note(WizardStep::note(
        "intro",
        "Onboarding",
        format!(
            "This guided flow prepares {} and keeps Palyra on safe defaults unless you explicitly opt into risky changes.",
            config_path.display()
        ),
    ))?;
    let accept_risk = wizard.confirm(confirm_step(
        "accept_risk_ack",
        "Risk Acknowledgement",
        "Proceed with a guided onboarding flow that may write config, state roots, and vault-backed credentials?",
        None,
    ))?;
    if !accept_risk {
        return Err(anyhow_from_wizard(WizardError::Cancelled {
            step_id: "accept_risk_ack".to_owned(),
        }));
    }

    let existing_action = resolve_existing_config_action(wizard, request.force, config_path)?;
    if matches!(existing_action, Some(ExistingConfigAction::Abort)) {
        return Err(anyhow_from_wizard(WizardError::Cancelled {
            step_id: "existing_config_action".to_owned(),
        }));
    }
    let default_profile = request
        .options
        .deployment_profile
        .map(deployment_profile_id_from_arg)
        .unwrap_or_else(|| default_deployment_profile_for_flow(flow, request.setup_mode));
    let selected_profile = wizard.select(select_step(
        "deployment_profile",
        "Deployment Profile",
        "Choose the canonical bootstrap profile that should shape config defaults, preflights, and deploy recipes.",
        vec![
            choice("local", "Local", Some("loopback-only workstation runtime")),
            choice("single-vm", "Single VM", Some("loopback-first service profile for one host")),
            choice(
                "worker-enabled",
                "Worker Enabled",
                Some("service profile with guarded networked worker execution"),
            ),
        ],
        Some(default_profile.as_str().to_owned()),
    ))?;
    let deployment_profile =
        palyra_common::deployment_profiles::DeploymentProfileId::parse(selected_profile.as_str())
            .context("wizard selected an invalid deployment profile")?;

    let mut plan = OnboardingMutationPlan {
        flow: flow.as_str().to_owned(),
        deployment_profile,
        deployment_mode: deployment_profile.deployment_mode().to_owned(),
        bind_profile: deployment_profile.bind_profile().to_owned(),
        auth_method: "skip".to_owned(),
        skipped_sections: Vec::new(),
        warnings: Vec::new(),
        risk_events: vec!["wizard_risk_acknowledged".to_owned()],
        service_install_mode: ServiceInstallMode::NotNow,
        existing_config_action: existing_action,
        health_status: HealthStatus::Skipped,
        ..Default::default()
    };

    match flow {
        WizardFlowKind::Quickstart => populate_quickstart_plan(wizard, &mut plan)?,
        WizardFlowKind::Manual => populate_manual_plan(wizard, request, &mut plan)?,
        WizardFlowKind::Remote => populate_remote_plan(wizard, &mut plan)?,
    }
    apply_explicit_port_overrides(request, &mut plan);

    let configure_channels = wizard.confirm(confirm_step(
        "configure_channels",
        "Channels",
        "Do you want this wizard to cover channel setup now? This wizard only records the guidance; live connector provisioning remains under `palyra channels ...`.",
        Some(false),
    ))?;
    if !configure_channels {
        if request.options.skip_channels || !request.options.non_interactive {
            plan.skipped_sections.push("channels".to_owned());
        } else {
            plan.warnings.push(
                "channel setup was left for `palyra channels ...` by the quickstart default; pass --skip-channels to record an explicit skip."
                    .to_owned(),
            );
        }
    } else {
        plan.warnings.push(
            "channels remain guidance-only here; use `palyra channels discord setup` for connector provisioning."
                .to_owned(),
        );
    }

    let configure_skills = wizard.confirm(confirm_step(
        "configure_skills",
        "Skills",
        "Do you want skill lifecycle guidance as part of this flow? This wizard does not change skill trust configuration automatically.",
        Some(false),
    ))?;
    if !configure_skills {
        if request.options.skip_skills || !request.options.non_interactive {
            plan.skipped_sections.push("skills".to_owned());
        } else {
            plan.warnings.push(
                "skill lifecycle setup was left for `palyra skills ...` by the quickstart default; pass --skip-skills to record an explicit skip."
                    .to_owned(),
            );
        }
    } else {
        plan.warnings.push(
            "skills lifecycle remains CLI-driven here; use `palyra skills list|info|check` for concrete actions."
                .to_owned(),
        );
    }

    let service_mode = wizard.select(select_step(
        "service_install_mode",
        "Service Management",
        "Choose how to handle daemon service installation in this flow.",
        vec![
            choice(
                "install_now",
                "Install Now",
                Some("write the config and register the background gateway service immediately"),
            ),
            choice(
                "guidance_only",
                "Show Guidance",
                Some("record the service commands without installing anything yet"),
            ),
            choice("not_now", "Not Now", Some("skip service setup for this run")),
        ],
        Some("not_now".to_owned()),
    ))?;
    plan.service_install_mode = match service_mode.as_str() {
        "install_now" => {
            plan.risk_events.push("service_install_requested".to_owned());
            ServiceInstallMode::InstallNow
        }
        "guidance_only" => {
            plan.warnings.push(
                "service install was deferred; use `palyra gateway install --start` when you are ready to move the runtime into background mode."
                    .to_owned(),
            );
            ServiceInstallMode::GuidanceOnly
        }
        _ => {
            if !matches!(flow, WizardFlowKind::Remote) && plan.auth_method != "existing_config" {
                plan.warnings.push(
                    "local runtime startup was deferred; run `palyra gateway run` now, or `palyra gateway install --start` to register a persistent background service."
                        .to_owned(),
                );
            }
            ServiceInstallMode::NotNow
        }
    };
    dedupe_strings(&mut plan.warnings);

    let run_health_checks = wizard.confirm(confirm_step(
        "run_health_checks",
        "Health Checks",
        "Run the post-apply health or verification checks now?",
        Some(true),
    ))?;
    if !run_health_checks {
        plan.health_status = HealthStatus::Skipped;
        plan.skipped_sections.push("health".to_owned());
    }

    Ok(plan)
}

fn apply_explicit_port_overrides(
    request: &OnboardingWizardRequest,
    plan: &mut OnboardingMutationPlan,
) {
    if let Some(port) = request.options.daemon_port {
        plan.daemon_port = Some(port);
    }
    if let Some(port) = request.options.grpc_port {
        plan.grpc_port = Some(port);
    }
    if let Some(port) = request.options.quic_port {
        plan.quic_port = Some(port);
    }
}

fn populate_quickstart_plan(
    wizard: &mut WizardSession<'_, dyn WizardBackend>,
    plan: &mut OnboardingMutationPlan,
) -> Result<()> {
    wizard.note(WizardStep::note(
        "quickstart.note",
        "QuickStart",
        "QuickStart keeps loopback-only binds, admin auth enabled, safe provider defaults, and a single workspace root for process execution.",
    ))?;
    let workspace_root = wizard.text(
        text_step(
            "workspace_root",
            "Workspace Root",
            "Select the workspace root for local process-runner execution.",
            Some(default_workspace_root()),
            None,
            false,
        ),
        |value| validate_non_empty_text(value, "workspace root"),
    )?;
    let workspace_root = normalize_workspace_root(workspace_root.as_str())?;
    ensure_directory_exists(Path::new(&workspace_root))?;
    plan.workspace_root = Some(workspace_root);

    let auth_method = wizard.select(select_step(
        "auth_method",
        "Model Provider Auth",
        "Choose how QuickStart should configure model-provider access.",
        model_provider_auth_choices(),
        Some("api_key".to_owned()),
    ))?;
    plan.auth_method = auth_method.clone();
    if auth_method_requires_api_key(auth_method.as_str()) {
        let api_key_label = api_key_field_label(auth_method.as_str());
        let api_key = wizard.text(
            text_step(
                "model_provider_api_key",
                api_key_label,
                api_key_prompt_message(auth_method.as_str()),
                None,
                None,
                true,
            ),
            |value| validate_non_empty_text(value, api_key_label),
        )?;
        plan.api_key = Some(api_key);
    } else if auth_method == "skip" {
        plan.risk_events.push("model_auth_skipped".to_owned());
        plan.warnings.push(
            "Model-provider auth was skipped; the resulting config is structurally valid but not ready for remote model calls."
                .to_owned(),
        );
    }
    Ok(())
}

fn populate_manual_plan(
    wizard: &mut WizardSession<'_, dyn WizardBackend>,
    request: &OnboardingWizardRequest,
    plan: &mut OnboardingMutationPlan,
) -> Result<()> {
    wizard.note(WizardStep::note(
        "manual.note",
        "Manual",
        "Manual mode exposes the important deployment and provider-auth posture choices while still applying them through the same safe mutation layer.",
    ))?;
    let workspace_root = wizard.text(
        text_step(
            "workspace_root",
            "Workspace Root",
            "Select the primary workspace root for local process execution.",
            Some(default_workspace_root()),
            None,
            false,
        ),
        |value| validate_non_empty_text(value, "workspace root"),
    )?;
    let workspace_root = normalize_workspace_root(workspace_root.as_str())?;
    ensure_directory_exists(Path::new(&workspace_root))?;
    plan.workspace_root = Some(workspace_root);

    let auth_method = wizard.select(select_step(
        "auth_method",
        "Model Provider Auth",
        "Choose how this installation should authenticate to model providers.",
        model_provider_auth_choices(),
        Some("api_key".to_owned()),
    ))?;
    plan.auth_method = auth_method.clone();
    if auth_method_requires_api_key(auth_method.as_str()) {
        let api_key_label = api_key_field_label(auth_method.as_str());
        let api_key = wizard.text(
            text_step(
                "model_provider_api_key",
                api_key_label,
                api_key_prompt_message(auth_method.as_str()),
                None,
                None,
                true,
            ),
            |value| validate_non_empty_text(value, api_key_label),
        )?;
        plan.api_key = Some(api_key);
    }

    let bind_profile = wizard.select(select_step(
        "bind_profile",
        "Bind Profile",
        "Choose how the daemon should expose its control-plane endpoints.",
        vec![
            choice(
                "loopback_only",
                "Loopback Only",
                Some("safe default for local and tunnel-first access"),
            ),
            choice(
                "public_tls",
                "Public TLS",
                Some("requires TLS and explicit dangerous-bind acknowledgement"),
            ),
        ],
        Some("loopback_only".to_owned()),
    ))?;
    plan.bind_profile = bind_profile.clone();
    if bind_profile == "public_tls" {
        let confirmed = wizard.confirm(confirm_step(
            "public_bind_ack",
            "Dangerous Bind Acknowledgement",
            "Public bind requires TLS, admin auth, and a second environment acknowledgement at runtime. Continue?",
            None,
        ))?;
        if !confirmed {
            return Err(anyhow_from_wizard(WizardError::Cancelled {
                step_id: "public_bind_ack".to_owned(),
            }));
        }
        plan.public_bind_ack = true;
        plan.risk_events.push("public_bind_acknowledged".to_owned());
        plan.deployment_mode = "remote_vps".to_owned();
        configure_tls_inputs(
            wizard,
            plan,
            request.setup_tls_scaffold.or(request.options.tls_scaffold),
        )?;
    }

    plan.daemon_port = Some(prompt_port(
        wizard,
        "daemon_port",
        "Daemon Port",
        "Choose the loopback/admin HTTP port.",
        request.options.daemon_port.unwrap_or(DEFAULT_DAEMON_PORT),
    )?);
    plan.grpc_port = Some(prompt_port(
        wizard,
        "grpc_port",
        "gRPC Port",
        "Choose the gRPC port used by the gateway surface.",
        request.options.grpc_port.unwrap_or(DEFAULT_GATEWAY_GRPC_PORT),
    )?);
    plan.quic_port = Some(prompt_port(
        wizard,
        "quic_port",
        "QUIC Port",
        "Choose the QUIC transport port.",
        request.options.quic_port.unwrap_or(DEFAULT_GATEWAY_QUIC_PORT),
    )?);

    if auth_method == "skip" {
        plan.risk_events.push("model_auth_skipped".to_owned());
        plan.warnings.push(
            "Manual flow left model-provider auth unset; review `palyra auth profiles list` and provider credentials before using remote model calls."
                .to_owned(),
        );
    }
    Ok(())
}

fn populate_remote_plan(
    wizard: &mut WizardSession<'_, dyn WizardBackend>,
    plan: &mut OnboardingMutationPlan,
) -> Result<()> {
    wizard.note(WizardStep::note(
        "remote.note",
        "Remote",
        "Remote onboarding creates a client-side connection profile. It does not provision or mutate the remote host.",
    ))?;

    let access_pattern = wizard.select(select_step(
        "remote_access_pattern",
        "Remote Access Pattern",
        "Choose how operators will reach the remote control plane.",
        vec![
            choice(
                "ssh_tunnel",
                "SSH Tunnel",
                Some("recommended for loopback-only VPS deployments"),
            ),
            choice(
                "verified_https",
                "Verified HTTPS",
                Some("use a public dashboard URL with an explicit verification pin"),
            ),
        ],
        Some("ssh_tunnel".to_owned()),
    ))?;
    let pattern = if access_pattern == "verified_https" {
        RemoteAccessPattern::VerifiedHttps
    } else {
        RemoteAccessPattern::SshTunnel
    };
    plan.auth_method = "remote_admin_token".to_owned();

    if matches!(pattern, RemoteAccessPattern::VerifiedHttps) {
        let remote_base_url = wizard.text(
            text_step(
                "remote_base_url",
                "Remote Dashboard URL",
                "Enter the verified remote dashboard HTTPS URL.",
                None,
                Some("https://dashboard.example.com/".to_owned()),
                false,
            ),
            |value| {
                parse_remote_dashboard_base_url(value, "gateway_access.remote_base_url")
                    .map(|_| ())
                    .map_err(|error| error.to_string())
            },
        )?;
        plan.remote_base_url = Some(parse_remote_dashboard_base_url(
            remote_base_url.as_str(),
            "gateway_access.remote_base_url",
        )?);
        let verification = wizard.select(select_step(
            "remote_verification",
            "Remote Verification",
            "Choose how the wizard should validate the remote HTTPS endpoint.",
            vec![
                choice(
                    "server_cert",
                    "Pinned Server Certificate",
                    Some("pin the remote server certificate SHA-256"),
                ),
                choice("gateway_ca", "Pinned Gateway CA", Some("pin the gateway CA SHA-256")),
                choice("none", "None", Some("skip pin validation and accept a follow-up warning")),
            ],
            Some("server_cert".to_owned()),
        ))?;
        if verification == "none" {
            let confirmed = wizard.confirm(confirm_step(
                "remote_without_pin_ack",
                "Verification Warning",
                "Skipping remote pin verification weakens the connection profile. Continue anyway?",
                None,
            ))?;
            if !confirmed {
                return Err(anyhow_from_wizard(WizardError::Cancelled {
                    step_id: "remote_without_pin_ack".to_owned(),
                }));
            }
            plan.risk_events.push("remote_pin_verification_skipped".to_owned());
            plan.warnings.push(
                "remote HTTPS profile was created without a verification pin; use `palyra configure --section gateway` to add one."
                    .to_owned(),
            );
        } else if verification == "server_cert" {
            let fingerprint = wizard.text(
                text_step(
                    "pinned_server_cert_sha256",
                    "Server Certificate Pin",
                    "Enter the expected remote server certificate SHA-256 fingerprint.",
                    None,
                    None,
                    false,
                ),
                |value| {
                    normalize_sha256_fingerprint(
                        value,
                        "gateway_access.pinned_server_cert_fingerprint_sha256",
                    )
                    .map(|_| ())
                    .map_err(|error| error.to_string())
                },
            )?;
            plan.remote_verification = Some("server_cert".to_owned());
            plan.pinned_server_cert_sha256 = Some(normalize_sha256_fingerprint(
                fingerprint.as_str(),
                "gateway_access.pinned_server_cert_fingerprint_sha256",
            )?);
        } else {
            let fingerprint = wizard.text(
                text_step(
                    "pinned_gateway_ca_sha256",
                    "Gateway CA Pin",
                    "Enter the expected gateway CA SHA-256 fingerprint.",
                    None,
                    None,
                    false,
                ),
                |value| {
                    normalize_sha256_fingerprint(
                        value,
                        "gateway_access.pinned_gateway_ca_fingerprint_sha256",
                    )
                    .map(|_| ())
                    .map_err(|error| error.to_string())
                },
            )?;
            plan.remote_verification = Some("gateway_ca".to_owned());
            plan.pinned_gateway_ca_sha256 = Some(normalize_sha256_fingerprint(
                fingerprint.as_str(),
                "gateway_access.pinned_gateway_ca_fingerprint_sha256",
            )?);
        }
    } else {
        let ssh_target = wizard.text(
            text_step(
                "ssh_target",
                "SSH Tunnel Target",
                "Enter the SSH destination used for `palyra tunnel --ssh ...` guidance.",
                None,
                Some("user@example.com".to_owned()),
                false,
            ),
            |value| validate_non_empty_text(value, "SSH target"),
        )?;
        plan.ssh_target = Some(ssh_target);
        plan.health_status = HealthStatus::ManualFollowUpRequired;
        plan.warnings.push(
            "remote SSH-tunnel profile expects a live `palyra tunnel --ssh ...` session before admin/gateway commands can succeed."
                .to_owned(),
        );
        plan.warnings.push(
            "if the first remote handoff fails, export a support bundle before retrying so trust and handshake diagnostics stay available."
                .to_owned(),
        );
    }

    let store_admin_token = wizard.confirm(confirm_step(
        "store_admin_token",
        "Remote Admin Token",
        "Store the remote admin token in the local config so future admin commands can use it automatically?",
        Some(true),
    ))?;
    if store_admin_token {
        let admin_token = wizard.text(
            text_step(
                "admin_token",
                "Remote Admin Token",
                "Enter the remote admin token.",
                None,
                None,
                true,
            ),
            |value| validate_non_empty_text(value, "remote admin token"),
        )?;
        plan.admin_token = Some(admin_token);
    } else {
        plan.warnings.push(
            "remote admin token was not stored; admin calls will require `--token` or a config update later."
                .to_owned(),
        );
    }

    Ok(())
}

fn resolve_existing_config_action(
    wizard: &mut WizardSession<'_, dyn WizardBackend>,
    force: bool,
    config_path: &Path,
) -> Result<Option<ExistingConfigAction>> {
    if !config_path.exists() {
        return Ok(None);
    }
    // Zero-length files are placeholders, not real configs; skip the reuse/overwrite prompt.
    if config_path.metadata().map(|metadata| metadata.len() == 0).unwrap_or(false) {
        return Ok(None);
    }
    if force {
        return Ok(Some(ExistingConfigAction::Overwrite));
    }
    let selection = wizard.select(select_step(
        "existing_config_action",
        "Existing Config",
        format!("{} already exists. Choose how the wizard should proceed.", config_path.display()),
        vec![
            choice(
                "reuse",
                "Reuse Current",
                Some("load the existing config and only update the selected sections"),
            ),
            choice(
                "overwrite",
                "Overwrite",
                Some("replace the config after taking a backup where applicable"),
            ),
            choice("abort", "Abort", Some("leave the installation untouched")),
        ],
        Some("reuse".to_owned()),
    ))?;
    Ok(Some(match selection.as_str() {
        "overwrite" => ExistingConfigAction::Overwrite,
        "abort" => ExistingConfigAction::Abort,
        _ => ExistingConfigAction::Reuse,
    }))
}

/// Creates the config directory, state root, identity store, and vault directory
/// before any document mutation, bailing out on an operator abort.
fn prepare_apply_context(
    config_path: &Path,
    force: bool,
    existing_action: Option<ExistingConfigAction>,
) -> Result<ApplyContext> {
    if config_path.exists()
        && !force
        && matches!(existing_action, Some(ExistingConfigAction::Abort))
    {
        anyhow::bail!("wizard was cancelled before mutating {}", config_path.display());
    }
    if let Some(parent) = config_path.parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent).with_context(|| {
                format!("failed to create config directory {}", parent.display())
            })?;
        }
    }
    let state_root = resolve_init_state_root()?;
    fs::create_dir_all(state_root.as_path())
        .with_context(|| format!("failed to create state root {}", state_root.display()))?;
    let identity_store_dir = state_root.join("identity");
    let vault_dir = state_root.join("vault");
    fs::create_dir_all(identity_store_dir.as_path()).with_context(|| {
        format!("failed to create identity store directory {}", identity_store_dir.display())
    })?;
    fs::create_dir_all(vault_dir.as_path())
        .with_context(|| format!("failed to create vault directory {}", vault_dir.display()))?;

    let tls_root = config_path
        .parent()
        .filter(|value| !value.as_os_str().is_empty())
        .map(Path::to_path_buf)
        .unwrap_or_else(|| PathBuf::from("."))
        .join("tls");
    let tls_paths = Some((tls_root.join("gateway.crt"), tls_root.join("gateway.key")));

    Ok(ApplyContext {
        config_path: config_path.to_path_buf(),
        state_root,
        identity_store_dir,
        vault_dir,
        tls_paths,
    })
}

/// Applies the collected plan to the config document and filesystem; returns the
/// dashboard URL used in the onboarding summary.
fn apply_onboarding_plan(
    context: &ApplyContext,
    plan: &mut OnboardingMutationPlan,
) -> Result<String> {
    let mut document = if context.config_path.exists()
        && matches!(plan.existing_config_action, Some(ExistingConfigAction::Reuse))
    {
        load_document_from_existing_path(context.config_path.as_path())
            .with_context(|| format!("failed to parse {}", context.config_path.display()))?
            .0
    } else {
        let mode = if plan.deployment_mode == "remote_vps" {
            InitMode::RemoteVps
        } else {
            InitMode::LocalDesktop
        };
        let admin_token = plan.admin_token.clone().unwrap_or_else(generate_admin_token);
        let tls_paths = if plan.tls_enabled { context.tls_paths.as_ref() } else { None };
        build_init_config_document(
            mode,
            plan.deployment_profile,
            context.identity_store_dir.as_path(),
            context.vault_dir.as_path(),
            admin_token.as_str(),
            tls_paths,
        )?
    };

    ensure_onboarding_admin_defaults(&mut document, plan)?;
    if ensure_runtime_defaults(&mut document, context)? {
        plan.warnings.push(
            "Runtime path defaults were backfilled so the daemon and CLI share the same local identity and vault state."
                .to_owned(),
        );
    }

    if let Some(workspace_root) = plan.workspace_root.as_ref() {
        set_value_at_path(
            &mut document,
            "tool_call.process_runner.workspace_root",
            toml::Value::String(workspace_root.clone()),
        )?;
    }
    if plan.auth_method == "skip" {
        clear_model_provider_auth(&mut document)?;
    } else if let Some(api_key) = plan.api_key.as_ref() {
        apply_model_provider_api_key(&mut document, plan.auth_method.as_str(), api_key.as_str())?;
    } else if auth_method_flow(plan.auth_method.as_str())
        == Some(AuthMethodFlow::DeferredAuthProfile)
    {
        apply_deferred_provider_auth_method(
            &mut document,
            plan.auth_method.as_str(),
            &mut plan.warnings,
        )?;
    }

    apply_deployment_profile_defaults(&mut document, plan.deployment_profile)?;
    // Set the profile after applying defaults so a manifest default cannot override it.
    set_value_at_path(
        &mut document,
        "deployment.profile",
        toml::Value::String(plan.deployment_profile.as_str().to_owned()),
    )?;
    set_value_at_path(
        &mut document,
        "deployment.mode",
        toml::Value::String(plan.deployment_mode.clone()),
    )?;
    set_value_at_path(
        &mut document,
        "gateway.bind_profile",
        toml::Value::String(plan.bind_profile.clone()),
    )?;
    set_value_at_path(
        &mut document,
        "deployment.dangerous_remote_bind_ack",
        toml::Value::Boolean(plan.public_bind_ack),
    )?;
    if let Some(port) = plan.daemon_port {
        set_value_at_path(&mut document, "daemon.port", toml::Value::Integer(i64::from(port)))?;
    }
    if let Some(port) = plan.grpc_port {
        set_value_at_path(
            &mut document,
            "gateway.grpc_port",
            toml::Value::Integer(i64::from(port)),
        )?;
    }
    if let Some(port) = plan.quic_port {
        set_value_at_path(
            &mut document,
            "gateway.quic_port",
            toml::Value::Integer(i64::from(port)),
        )?;
    }
    set_value_at_path(
        &mut document,
        "gateway.tls.enabled",
        toml::Value::Boolean(plan.tls_enabled),
    )?;
    if plan.tls_enabled {
        if let Some(cert_path) = plan.tls_cert_path.as_ref() {
            set_value_at_path(
                &mut document,
                "gateway.tls.cert_path",
                toml::Value::String(cert_path.clone()),
            )?;
        }
        if let Some(key_path) = plan.tls_key_path.as_ref() {
            set_value_at_path(
                &mut document,
                "gateway.tls.key_path",
                toml::Value::String(key_path.clone()),
            )?;
        }
    }
    if let Some(remote_base_url) = plan.remote_base_url.as_ref() {
        set_value_at_path(
            &mut document,
            "gateway_access.remote_base_url",
            toml::Value::String(remote_base_url.clone()),
        )?;
    }
    match plan.remote_verification.as_deref() {
        Some("server_cert") => {
            if let Some(value) = plan.pinned_server_cert_sha256.as_ref() {
                set_value_at_path(
                    &mut document,
                    "gateway_access.pinned_server_cert_fingerprint_sha256",
                    toml::Value::String(value.clone()),
                )?;
                unset_value_at_path(
                    &mut document,
                    "gateway_access.pinned_gateway_ca_fingerprint_sha256",
                )?;
            }
        }
        Some("gateway_ca") => {
            if let Some(value) = plan.pinned_gateway_ca_sha256.as_ref() {
                set_value_at_path(
                    &mut document,
                    "gateway_access.pinned_gateway_ca_fingerprint_sha256",
                    toml::Value::String(value.clone()),
                )?;
                unset_value_at_path(
                    &mut document,
                    "gateway_access.pinned_server_cert_fingerprint_sha256",
                )?;
            }
        }
        _ => {
            unset_value_at_path(
                &mut document,
                "gateway_access.pinned_server_cert_fingerprint_sha256",
            )?;
            unset_value_at_path(
                &mut document,
                "gateway_access.pinned_gateway_ca_fingerprint_sha256",
            )?;
        }
    }

    match plan.admin_token.as_ref() {
        Some(admin_token) => {
            set_value_at_path(
                &mut document,
                "admin.auth_token",
                toml::Value::String(admin_token.clone()),
            )?;
        }
        None if plan.flow == "remote" => {
            unset_value_at_path(&mut document, "admin.auth_token")?;
        }
        None => {}
    }

    validate_daemon_compatible_document(&document).with_context(|| {
        format!("generated config {} does not match daemon schema", context.config_path.display())
    })?;
    write_operator_document_with_backups(context.config_path.as_path(), &document)
        .with_context(|| format!("failed to persist config {}", context.config_path.display()))?;

    if plan.deployment_profile == palyra_common::deployment_profiles::DeploymentProfileId::Local {
        app::update_active_profile_paths(
            Some(context.config_path.as_path()),
            Some(context.state_root.as_path()),
        )?;
        super::browser::configure_local_browser_prerequisites(Some(
            context.config_path.display().to_string(),
        ))
        .with_context(|| {
            format!(
                "failed to configure local browser prerequisites for {}",
                context.config_path.display()
            )
        })?;
    }

    if matches!(plan.service_install_mode, ServiceInstallMode::InstallNow) {
        let daemon_bin = super::daemon::resolve_palyrad_binary(None)?;
        let request = support::service::GatewayServiceInstallRequest {
            service_name: None,
            daemon_bin,
            state_root: context.state_root.clone(),
            config_path: Some(context.config_path.clone()),
            log_dir: None,
            start_now: true,
        };
        if let Err(error) = support::service::install_gateway_service(&request)
            .context("failed to install gateway service from onboarding wizard")
        {
            record_service_install_failure(plan, &error);
        }
    }

    let target = resolve_dashboard_access_target(Some(context.config_path.display().to_string()))?;
    Ok(target.url)
}

fn record_service_install_failure(plan: &mut OnboardingMutationPlan, error: &anyhow::Error) {
    plan.service_install_mode = ServiceInstallMode::InstallFailedDeferred;
    plan.risk_events.push("service_install_deferred_after_failure".to_owned());
    plan.warnings.push(format!(
        "`Install Now` was selected, but background gateway service install failed and was deferred: {error:#}. Run `palyra gateway run` for an immediate foreground runtime, or retry `palyra gateway install --start` after fixing service permissions."
    ));
    dedupe_strings(&mut plan.risk_events);
    dedupe_strings(&mut plan.warnings);
}

fn ensure_onboarding_admin_defaults(
    document: &mut toml::Value,
    plan: &OnboardingMutationPlan,
) -> Result<()> {
    ensure_admin_auth_defaults_with_token(document, plan.admin_token.as_deref()).map(|_| ())
}

fn ensure_admin_auth_defaults(document: &mut toml::Value) -> Result<bool> {
    ensure_admin_auth_defaults_with_token(document, None)
}

/// Backfills admin auth defaults and returns `true` when the document changed.
///
/// An existing `admin.auth_token_secret_ref` counts as a configured token source,
/// so no inline token is generated over it.
fn ensure_admin_auth_defaults_with_token(
    document: &mut toml::Value,
    admin_token: Option<&str>,
) -> Result<bool> {
    let before = document.clone();
    if get_bool_value_at_path(document, "admin.require_auth")?.is_none() {
        set_value_at_path(document, "admin.require_auth", toml::Value::Boolean(true))?;
    }
    if !admin_auth_token_source_configured(document)? {
        let admin_token = admin_token.map(str::to_owned).unwrap_or_else(generate_admin_token);
        set_value_at_path(document, "admin.auth_token", toml::Value::String(admin_token))?;
    }
    if get_string_value_at_path(document, "admin.bound_principal")?.is_none() {
        set_value_at_path(
            document,
            "admin.bound_principal",
            toml::Value::String(DEFAULT_ADMIN_BOUND_PRINCIPAL.to_owned()),
        )?;
    }
    Ok(document != &before)
}

fn admin_auth_token_source_configured(document: &toml::Value) -> Result<bool> {
    Ok(get_string_value_at_path(document, "admin.auth_token")?.is_some()
        || get_value_at_path(document, "admin.auth_token_secret_ref")?.is_some())
}

/// Backfills identity-store, vault, and runloop defaults; returns `true` when the
/// document changed.
fn ensure_runtime_defaults(document: &mut toml::Value, context: &ApplyContext) -> Result<bool> {
    let before = document.clone();
    if get_string_value_at_path(document, "gateway.identity_store_dir")?.is_none() {
        set_value_at_path(
            document,
            "gateway.identity_store_dir",
            toml::Value::String(context.identity_store_dir.to_string_lossy().into_owned()),
        )?;
    }
    if get_string_value_at_path(document, "storage.vault_dir")?.is_none() {
        set_value_at_path(
            document,
            "storage.vault_dir",
            toml::Value::String(context.vault_dir.to_string_lossy().into_owned()),
        )?;
    }
    if get_bool_value_at_path(document, "orchestrator.runloop_v1_enabled")?.is_none() {
        set_value_at_path(document, "orchestrator.runloop_v1_enabled", toml::Value::Boolean(true))?;
    }
    Ok(document != &before)
}

/// Applies manifest defaults only for keys that are not already present; returns
/// `true` when the document changed.
fn ensure_missing_deployment_profile_defaults(
    document: &mut toml::Value,
    deployment_profile: palyra_common::deployment_profiles::DeploymentProfileId,
) -> Result<bool> {
    let before = document.clone();
    let manifest =
        palyra_common::deployment_profiles::deployment_profile_manifest(deployment_profile);
    for default in manifest.defaults {
        if get_value_at_path(document, default.config_path.as_str())?.is_some() {
            continue;
        }
        let value = match default.value {
            palyra_common::deployment_profiles::DeploymentProfileDefaultValue::String(value) => {
                toml::Value::String(value)
            }
            palyra_common::deployment_profiles::DeploymentProfileDefaultValue::Integer(value) => {
                toml::Value::Integer(value)
            }
            palyra_common::deployment_profiles::DeploymentProfileDefaultValue::Boolean(value) => {
                toml::Value::Boolean(value)
            }
            palyra_common::deployment_profiles::DeploymentProfileDefaultValue::StringList(
                values,
            ) => toml::Value::Array(values.into_iter().map(toml::Value::String).collect()),
        };
        set_value_at_path(document, default.config_path.as_str(), value)?;
    }
    Ok(document != &before)
}

fn run_post_apply_health_check(
    flow: WizardFlowKind,
    context: &ApplyContext,
    plan: &OnboardingMutationPlan,
) -> Result<HealthCheckReport> {
    match flow {
        WizardFlowKind::Remote
            if plan.remote_base_url.is_some() && plan.remote_verification.is_some() =>
        {
            let target =
                resolve_dashboard_access_target(Some(context.config_path.display().to_string()))?;
            let _ = verify_dashboard_remote_target(&target, None)?;
            Ok(HealthCheckReport {
                status: HealthStatus::RemoteVerified,
                checks: vec![
                    HealthCheckSummary {
                        check: "config_schema".to_owned(),
                        status: "ok".to_owned(),
                        detail: format!(
                            "wizard-generated config {} matches the daemon schema",
                            context.config_path.display()
                        ),
                    },
                    HealthCheckSummary {
                        check: "remote_dashboard_pin_verification".to_owned(),
                        status: "ok".to_owned(),
                        detail: format!("verified remote dashboard target {}", target.url),
                    },
                ],
            })
        }
        WizardFlowKind::Remote => Ok(HealthCheckReport {
            status: HealthStatus::ManualFollowUpRequired,
            checks: vec![HealthCheckSummary {
                check: "remote_connectivity".to_owned(),
                status: "manual_follow_up".to_owned(),
                detail:
                    "remote onboarding requires either a live SSH tunnel session or a verified HTTPS endpoint before runtime probes can succeed"
                        .to_owned(),
            }],
        }),
        _ => {
            let (document, _) = load_document_from_existing_path(context.config_path.as_path())
                .with_context(|| format!("failed to parse {}", context.config_path.display()))?;
            validate_daemon_compatible_document(&document).with_context(|| {
                format!(
                    "generated config {} does not match daemon schema",
                    context.config_path.display()
                )
            })?;
            let bind_profile = get_string_value_at_path(&document, "gateway.bind_profile")?
                .unwrap_or_else(|| "loopback_only".to_owned());
            let admin_auth_required =
                get_bool_value_at_path(&document, "admin.require_auth")?.unwrap_or(false);
            let model_auth_configured = model_auth_configured(&document)?;
            let tls_enabled = get_bool_value_at_path(&document, "gateway.tls.enabled")?
                .unwrap_or(false);
            let public_bind_ack = get_bool_value_at_path(
                &document,
                "deployment.dangerous_remote_bind_ack",
            )?
            .unwrap_or(false);
            let mut checks = vec![HealthCheckSummary {
                check: "config_schema".to_owned(),
                status: "ok".to_owned(),
                detail: format!(
                    "wizard-generated config {} matches the daemon schema",
                    context.config_path.display()
                ),
            }];
            checks.push(HealthCheckSummary {
                check: "admin_auth".to_owned(),
                status: if admin_auth_required { "ok" } else { "warning" }.to_owned(),
                detail: if admin_auth_required {
                    "admin authentication is enabled".to_owned()
                } else {
                    "admin authentication is disabled; review the deployment posture before exposing the daemon".to_owned()
                },
            });
            checks.push(HealthCheckSummary {
                check: "model_auth".to_owned(),
                status: if model_auth_configured { "ok" } else { "warning" }.to_owned(),
                detail: if model_auth_configured {
                    "model provider credentials are configured".to_owned()
                } else {
                    "model provider credentials are still missing; runtime model calls will fail until auth is configured".to_owned()
                },
            });
            checks.push(HealthCheckSummary {
                check: "bind_posture".to_owned(),
                status: if bind_profile == "loopback_only"
                    || (bind_profile == "public_tls" && tls_enabled && public_bind_ack)
                {
                    "ok"
                } else {
                    "warning"
                }
                .to_owned(),
                detail: if bind_profile == "loopback_only" {
                    "loopback-only bind posture is active".to_owned()
                } else if tls_enabled && public_bind_ack {
                    "public TLS bind posture is configured with explicit dangerous-bind acknowledgement".to_owned()
                } else {
                    "public bind posture is incomplete; verify TLS paths and dangerous-bind acknowledgement before exposing the daemon".to_owned()
                },
            });
            if let Some(runtime_check) =
                running_gateway_restart_check(context, &document, admin_auth_required)?
            {
                checks.push(runtime_check);
            }
            let restart_required = checks.iter().any(|check| {
                matches!(check.status.as_str(), "restart_required" | "restart_recommended")
            });
            let needs_follow_up = checks.iter().any(|check| check.status != "ok");
            Ok(HealthCheckReport {
                status: if restart_required {
                    HealthStatus::RuntimeRestartRequired
                } else if needs_follow_up {
                    HealthStatus::ManualFollowUpRequired
                } else {
                    HealthStatus::ConfigReady
                },
                checks,
            })
        }
    }
}

// INTENTIONAL: probe only the unauthenticated /healthz endpoint and never attach admin
// credentials -- whatever process currently owns the port would receive them. Pinned by
// running_gateway_restart_check_does_not_send_admin_token_to_health_responder.
fn running_gateway_restart_check(
    context: &ApplyContext,
    _document: &toml::Value,
    admin_auth_required: bool,
) -> Result<Option<HealthCheckSummary>> {
    let target = resolve_dashboard_access_target(Some(context.config_path.display().to_string()))?;
    let http_client = Client::builder()
        .timeout(std::time::Duration::from_secs(2))
        .build()
        .context("failed to build onboarding health HTTP client")?;
    let status_url = format!("{}/healthz", target.url.trim_end_matches('/'));
    if fetch_health_with_retry(&http_client, status_url.as_str()).is_err() {
        return Ok(None);
    }

    Ok(Some(HealthCheckSummary {
        check: "runtime_config_reload".to_owned(),
        status: if admin_auth_required { "restart_required" } else { "restart_recommended" }
            .to_owned(),
        detail: format!(
            "gateway is already reachable; restart the running gateway so it reloads {}",
            context.config_path.display()
        ),
    }))
}

/// Picks the summary status, recommended step id, and next-step guidance from the
/// flow outcome.
fn onboarding_summary_next_step(
    flow: WizardFlowKind,
    service_install_mode: ServiceInstallMode,
    health_status: HealthStatus,
    auth_method: &str,
) -> (&'static str, Option<&'static str>, Option<&'static str>) {
    if matches!(health_status, HealthStatus::RuntimeRestartRequired) {
        if matches!(service_install_mode, ServiceInstallMode::InstallNow) {
            return (
                "configured_runtime_restart_required",
                Some("gateway_restart"),
                Some("Restart the managed gateway service with `palyra gateway restart`, then rerun `palyra onboarding status`."),
            );
        }
        return (
            "configured_runtime_restart_required",
            Some("foreground_gateway_restart"),
            Some("Stop the current foreground `palyra gateway run` process with Ctrl+C in its terminal, start it again with `palyra gateway run`, then rerun `palyra onboarding status`. Use `palyra gateway install --start` first if you want `palyra gateway restart` to manage a background service."),
        );
    }

    if matches!(health_status, HealthStatus::ManualFollowUpRequired | HealthStatus::Skipped) {
        return (
            "next_step_required",
            Some("onboarding_status"),
            Some("Run `palyra onboarding status --json` to inspect the current blocker before starting the first agent run."),
        );
    }

    if auth_method == "existing_config" {
        return (
            "existing_config_ready",
            Some("onboarding_status"),
            Some("Existing model-provider config was preserved and config defaults were refreshed. Run `palyra onboarding status --json` for live gateway, default-agent, and first-success state."),
        );
    }

    if !matches!(flow, WizardFlowKind::Remote)
        && !matches!(service_install_mode, ServiceInstallMode::InstallNow)
    {
        return (
            "configured_runtime_start_required",
            Some("agent_identity"),
            Some("Start the gateway with `palyra gateway run` or `palyra gateway install --start`, then rerun `palyra onboarding status` and create the default agent."),
        );
    }

    (
        "next_step_required",
        Some("agent_identity"),
        Some("Run `palyra onboarding status --json` and complete the default-agent step before treating onboarding as finished."),
    )
}

fn emit_onboarding_summary(summary: &OnboardingSummary, json_output: bool) -> Result<()> {
    if json_output {
        output::print_json_pretty(summary, "failed to encode onboarding summary as JSON")?;
    } else {
        println!(
            "onboarding.status={} flow={} deployment_mode={} config_path={} state_root={}",
            summary.status,
            summary.flow,
            summary.deployment_mode,
            summary.config_path,
            summary.state_root
        );
        println!("onboarding.deployment_profile={}", summary.deployment_profile);
        println!(
            "onboarding.summary workspace_root_configured={} auth_method={} dashboard_access={} health_status={:?} service_install_mode={}",
            summary.workspace_root.is_some(),
            summary.auth_method,
            if summary.dashboard_url.is_empty() { "none" } else { "configured" },
            summary.health_status,
            summary.service_install_mode.as_str(),
        );
        println!(
            "onboarding.skills installed={} eligible={} quarantined={} runtime_unknown={}",
            summary.skills.installed_total,
            summary.skills.eligible_total,
            summary.skills.quarantined_total,
            summary.skills.runtime_unknown_total
        );
        if let Some(step_id) = summary.recommended_step_id {
            println!("onboarding.next_step={step_id}");
        }
        if let Some(next_step) = summary.next_step {
            println!("onboarding.next_step_hint={next_step}");
        }
        println!(
            "onboarding.risk_events={}",
            if summary.risk_events.is_empty() {
                "none".to_owned()
            } else {
                summary.risk_events.join(",")
            }
        );
        println!(
            "onboarding.skipped sections={}",
            if summary.skipped_sections.is_empty() {
                "none".to_owned()
            } else {
                summary.skipped_sections.join(",")
            }
        );
        if !summary.warnings.is_empty() {
            println!("onboarding.warning_count={}", summary.warnings.len());
            for warning in &summary.warnings {
                println!("onboarding.warning={warning}");
            }
        }
    }
    std::io::stdout().flush().context("stdout flush failed")
}

fn emit_configure_summary(summary: &ConfigureSummary, json_output: bool) -> Result<()> {
    if json_output {
        output::print_json_pretty(summary, "failed to encode configure summary as JSON")?;
    } else {
        println!(
            "configure.status={} config_path={} changed_sections={} unchanged_sections={}",
            summary.status,
            summary.config_path,
            if summary.changed_sections.is_empty() {
                "none".to_owned()
            } else {
                summary.changed_sections.join(",")
            },
            if summary.unchanged_sections.is_empty() {
                "none".to_owned()
            } else {
                summary.unchanged_sections.join(",")
            }
        );
        if !summary.restart_required.is_empty() {
            println!("configure.restart_required={}", summary.restart_required.join(","));
        }
        for change in &summary.section_changes {
            println!(
                "configure.section section={} changed={} before={}",
                change.section,
                change.changed,
                join_section_state(change.before.as_slice())
            );
            println!(
                "configure.section.after section={} values={}",
                change.section,
                join_section_state(change.after.as_slice())
            );
            if !change.follow_up_checks.is_empty() {
                println!(
                    "configure.section.follow_up section={} values={}",
                    change.section,
                    change.follow_up_checks.join(",")
                );
            }
        }
        for follow_up in &summary.follow_up_checks {
            println!("configure.follow_up={follow_up}");
        }
        for warning in &summary.warnings {
            println!("configure.warning={warning}");
        }
        if let Some(runtime_reload) = summary.runtime_reload.as_ref() {
            println!(
                "{}",
                crate::commands::runtime_reload::reload_text_line("configure", runtime_reload)
            );
        }
    }
    std::io::stdout().flush().context("stdout flush failed")
}

fn select_configure_sections(
    wizard: &mut WizardSession<'_, dyn WizardBackend>,
    request: &ConfigureWizardRequest,
) -> Result<Vec<ConfigureSectionArg>> {
    if !request.sections.is_empty() {
        return Ok(request.sections.clone());
    }
    if request.non_interactive {
        anyhow::bail!("non-interactive configure requires at least one --section");
    }
    let selected = wizard.multiselect(multiselect_step(
        "configure_sections",
        "Configure Sections",
        "Choose the sections you want to reconfigure.",
        vec![
            choice("deployment-profile", "Deployment Profile", None),
            choice("workspace", "Workspace", None),
            choice("auth-model", "Auth / Model", None),
            choice("gateway", "Gateway", None),
            choice("runtime-controls", "Runtime Controls", None),
            choice("daemon-service", "Daemon / Service", None),
            choice("channels", "Channels", None),
            choice("skills", "Skills", None),
            choice("health-security", "Health / Security", None),
        ],
        Some("deployment-profile,workspace,auth-model,gateway,health-security".to_owned()),
    ))?;
    selected
        .into_iter()
        .map(|value| match value.as_str() {
            "deployment-profile" => Ok(ConfigureSectionArg::DeploymentProfile),
            "workspace" => Ok(ConfigureSectionArg::Workspace),
            "auth-model" => Ok(ConfigureSectionArg::AuthModel),
            "gateway" => Ok(ConfigureSectionArg::Gateway),
            "runtime-controls" => Ok(ConfigureSectionArg::RuntimeControls),
            "daemon-service" => Ok(ConfigureSectionArg::DaemonService),
            "channels" => Ok(ConfigureSectionArg::Channels),
            "skills" => Ok(ConfigureSectionArg::Skills),
            "health-security" => Ok(ConfigureSectionArg::HealthSecurity),
            _ => anyhow::bail!("unsupported configure section: {value}"),
        })
        .collect()
}

fn apply_auth_method_choice(
    wizard: &mut WizardSession<'_, dyn WizardBackend>,
    document: &mut toml::Value,
    auth_method: &str,
    warnings: &mut Vec<String>,
) -> Result<()> {
    match auth_method {
        "skip" => {
            clear_model_provider_auth(document)?;
            warnings.push(
                "model-provider auth was left unset; review `palyra auth profiles list` and provider credentials before enabling remote model calls."
                    .to_owned(),
            );
        }
        "existing_config" => {}
        _ => {
            if auth_method_requires_api_key(auth_method) {
                let api_key_label = api_key_field_label(auth_method);
                let api_key = wizard.text(
                    text_step(
                        "model_provider_api_key",
                        api_key_label,
                        api_key_prompt_message(auth_method),
                        None,
                        None,
                        true,
                    ),
                    |value| validate_non_empty_text(value, api_key_label),
                )?;
                apply_model_provider_api_key(document, auth_method, api_key.as_str())?;
            } else {
                apply_deferred_provider_auth_method(document, auth_method, warnings)?;
            }
        }
    }
    Ok(())
}

fn apply_port_updates(
    wizard: &mut WizardSession<'_, dyn WizardBackend>,
    document: &mut toml::Value,
    daemon_port_override: Option<u16>,
    grpc_port_override: Option<u16>,
    quic_port_override: Option<u16>,
) -> Result<()> {
    let daemon_port = prompt_port(
        wizard,
        "daemon_port",
        "Daemon Port",
        "Choose the loopback/admin HTTP port.",
        daemon_port_override
            .or_else(|| {
                get_integer_value_at_path(document, "daemon.port")
                    .ok()
                    .flatten()
                    .and_then(|v| u16::try_from(v).ok())
            })
            .unwrap_or(DEFAULT_DAEMON_PORT),
    )?;
    let grpc_port = prompt_port(
        wizard,
        "grpc_port",
        "gRPC Port",
        "Choose the gRPC port used by the gateway surface.",
        grpc_port_override
            .or_else(|| {
                get_integer_value_at_path(document, "gateway.grpc_port")
                    .ok()
                    .flatten()
                    .and_then(|v| u16::try_from(v).ok())
            })
            .unwrap_or(DEFAULT_GATEWAY_GRPC_PORT),
    )?;
    let quic_port = prompt_port(
        wizard,
        "quic_port",
        "QUIC Port",
        "Choose the QUIC transport port.",
        quic_port_override
            .or_else(|| {
                get_integer_value_at_path(document, "gateway.quic_port")
                    .ok()
                    .flatten()
                    .and_then(|v| u16::try_from(v).ok())
            })
            .unwrap_or(DEFAULT_GATEWAY_QUIC_PORT),
    )?;
    set_value_at_path(document, "daemon.port", toml::Value::Integer(i64::from(daemon_port)))?;
    set_value_at_path(document, "gateway.grpc_port", toml::Value::Integer(i64::from(grpc_port)))?;
    set_value_at_path(document, "gateway.quic_port", toml::Value::Integer(i64::from(quic_port)))?;
    Ok(())
}

fn configure_bind_profile(
    wizard: &mut WizardSession<'_, dyn WizardBackend>,
    document: &mut toml::Value,
    config: BindProfileConfig,
    warnings: &mut Vec<String>,
) -> Result<()> {
    set_value_at_path(
        document,
        "gateway.bind_profile",
        toml::Value::String(config.bind_profile.clone()),
    )?;
    if config.bind_profile == "public_tls" {
        if !config.accept_risk {
            let confirmed = wizard.confirm(confirm_step(
                "public_bind_ack",
                "Dangerous Bind Acknowledgement",
                "Public bind requires TLS, admin auth, and an environment acknowledgement at runtime. Continue?",
                None,
            ))?;
            if !confirmed {
                return Err(anyhow_from_wizard(WizardError::Cancelled {
                    step_id: "public_bind_ack".to_owned(),
                }));
            }
        }
        set_value_at_path(
            document,
            "deployment.dangerous_remote_bind_ack",
            toml::Value::Boolean(true),
        )?;
        set_value_at_path(document, "gateway.tls.enabled", toml::Value::Boolean(true))?;
        let cert_path = match config.tls_cert_path {
            Some(path) => path,
            None if matches!(config.tls_scaffold, Some(InitTlsScaffoldArg::SelfSigned | InitTlsScaffoldArg::BringYourOwn)) => wizard.text(
                text_step(
                    "tls_cert_path",
                    "TLS Certificate Path",
                    "Enter the certificate path that the daemon should use when public TLS is enabled.",
                    Some("./tls/gateway.crt".to_owned()),
                    None,
                    false,
                ),
                |value| validate_non_empty_text(value, "TLS certificate path"),
            )?,
            None => "./tls/gateway.crt".to_owned(),
        };
        let key_path = match config.tls_key_path {
            Some(path) => path,
            None if matches!(config.tls_scaffold, Some(InitTlsScaffoldArg::SelfSigned | InitTlsScaffoldArg::BringYourOwn)) => wizard.text(
                text_step(
                    "tls_key_path",
                    "TLS Key Path",
                    "Enter the private key path that the daemon should use when public TLS is enabled.",
                    Some("./tls/gateway.key".to_owned()),
                    None,
                    false,
                ),
                |value| validate_non_empty_text(value, "TLS key path"),
            )?,
            None => "./tls/gateway.key".to_owned(),
        };
        set_value_at_path(document, "gateway.tls.cert_path", toml::Value::String(cert_path))?;
        set_value_at_path(document, "gateway.tls.key_path", toml::Value::String(key_path))?;
        warnings.push(
            "public TLS still requires PALYRA_GATEWAY_DANGEROUS_REMOTE_BIND_ACK=true at runtime before the daemon will accept non-loopback binds."
                .to_owned(),
        );
    } else {
        set_value_at_path(
            document,
            "deployment.dangerous_remote_bind_ack",
            toml::Value::Boolean(false),
        )?;
        set_value_at_path(document, "gateway.tls.enabled", toml::Value::Boolean(false))?;
    }
    Ok(())
}

fn apply_remote_dashboard_settings(
    wizard: &mut WizardSession<'_, dyn WizardBackend>,
    document: &mut toml::Value,
    remote_base_url_override: Option<String>,
    remote_verification: Option<RemoteVerificationModeArg>,
    pinned_server_cert_sha256: Option<String>,
    pinned_gateway_ca_sha256: Option<String>,
    warnings: &mut Vec<String>,
) -> Result<()> {
    let current_remote_url = get_string_value_at_path(document, "gateway_access.remote_base_url")?;
    let remote_base_url = match remote_base_url_override {
        Some(value) => {
            Some(parse_remote_dashboard_base_url(value.as_str(), "gateway_access.remote_base_url")?)
        }
        None => {
            let wants_remote_url = wizard.confirm(confirm_step(
                "configure_remote_url",
                "Remote Dashboard URL",
                "Configure a remote HTTPS dashboard URL for dashboard discovery?",
                Some(current_remote_url.is_some()),
            ))?;
            if wants_remote_url {
                Some(parse_remote_dashboard_base_url(
                    wizard
                        .text(
                            text_step(
                                "remote_base_url",
                                "Remote Dashboard URL",
                                "Enter the remote dashboard HTTPS URL.",
                                current_remote_url.clone(),
                                Some("https://dashboard.example.com/".to_owned()),
                                false,
                            ),
                            |value| {
                                parse_remote_dashboard_base_url(
                                    value,
                                    "gateway_access.remote_base_url",
                                )
                                .map(|_| ())
                                .map_err(|error| error.to_string())
                            },
                        )?
                        .as_str(),
                    "gateway_access.remote_base_url",
                )?)
            } else {
                None
            }
        }
    };

    match remote_base_url {
        Some(remote_base_url) => {
            set_value_at_path(
                document,
                "gateway_access.remote_base_url",
                toml::Value::String(remote_base_url),
            )?;
            let verification_mode = match remote_verification {
                Some(mode) => remote_verification_value(mode).to_owned(),
                None => wizard.select(select_step(
                    "remote_verification",
                    "Remote Verification",
                    "Choose how the CLI should verify the remote HTTPS endpoint.",
                    vec![
                        choice("server_cert", "Pinned Server Certificate", None),
                        choice("gateway_ca", "Pinned Gateway CA", None),
                        choice("none", "None", Some("skip pin verification and accept a warning")),
                    ],
                    Some("server_cert".to_owned()),
                ))?,
            };
            match verification_mode.as_str() {
                "server_cert" => {
                    let value = match pinned_server_cert_sha256 {
                        Some(value) => normalize_sha256_fingerprint(
                            value.as_str(),
                            "gateway_access.pinned_server_cert_fingerprint_sha256",
                        )?,
                        None => normalize_sha256_fingerprint(
                            wizard
                                .text(
                                    text_step(
                                        "pinned_server_cert_sha256",
                                        "Server Certificate Pin",
                                        "Enter the expected remote server certificate SHA-256 fingerprint.",
                                        None,
                                        None,
                                        false,
                                    ),
                                    |value| {
                                        normalize_sha256_fingerprint(
                                            value,
                                            "gateway_access.pinned_server_cert_fingerprint_sha256",
                                        )
                                        .map(|_| ())
                                        .map_err(|error| error.to_string())
                                    },
                                )?
                                .as_str(),
                            "gateway_access.pinned_server_cert_fingerprint_sha256",
                        )?,
                    };
                    set_value_at_path(
                        document,
                        "gateway_access.pinned_server_cert_fingerprint_sha256",
                        toml::Value::String(value),
                    )?;
                    unset_value_at_path(
                        document,
                        "gateway_access.pinned_gateway_ca_fingerprint_sha256",
                    )?;
                }
                "gateway_ca" => {
                    let value = match pinned_gateway_ca_sha256 {
                        Some(value) => normalize_sha256_fingerprint(
                            value.as_str(),
                            "gateway_access.pinned_gateway_ca_fingerprint_sha256",
                        )?,
                        None => normalize_sha256_fingerprint(
                            wizard
                                .text(
                                    text_step(
                                        "pinned_gateway_ca_sha256",
                                        "Gateway CA Pin",
                                        "Enter the expected gateway CA SHA-256 fingerprint.",
                                        None,
                                        None,
                                        false,
                                    ),
                                    |value| {
                                        normalize_sha256_fingerprint(
                                            value,
                                            "gateway_access.pinned_gateway_ca_fingerprint_sha256",
                                        )
                                        .map(|_| ())
                                        .map_err(|error| error.to_string())
                                    },
                                )?
                                .as_str(),
                            "gateway_access.pinned_gateway_ca_fingerprint_sha256",
                        )?,
                    };
                    set_value_at_path(
                        document,
                        "gateway_access.pinned_gateway_ca_fingerprint_sha256",
                        toml::Value::String(value),
                    )?;
                    unset_value_at_path(
                        document,
                        "gateway_access.pinned_server_cert_fingerprint_sha256",
                    )?;
                }
                _ => {
                    unset_value_at_path(
                        document,
                        "gateway_access.pinned_server_cert_fingerprint_sha256",
                    )?;
                    unset_value_at_path(
                        document,
                        "gateway_access.pinned_gateway_ca_fingerprint_sha256",
                    )?;
                    warnings.push(
                        "remote dashboard URL was configured without a verification pin; use `palyra configure --section gateway` to add one later."
                            .to_owned(),
                    );
                }
            }
        }
        None => {
            unset_value_at_path(document, "gateway_access.remote_base_url")?;
            unset_value_at_path(document, "gateway_access.pinned_server_cert_fingerprint_sha256")?;
            unset_value_at_path(document, "gateway_access.pinned_gateway_ca_fingerprint_sha256")?;
        }
    }
    Ok(())
}

fn configure_tls_inputs(
    wizard: &mut WizardSession<'_, dyn WizardBackend>,
    plan: &mut OnboardingMutationPlan,
    tls_scaffold_hint: Option<InitTlsScaffoldArg>,
) -> Result<()> {
    let tls_scaffold = wizard.select(select_step(
        "tls_scaffold",
        "TLS Scaffold",
        "Choose how public TLS paths should be prepared.",
        vec![
            choice(
                "bring-your-own",
                "Bring Your Own",
                Some("reference existing certificate and key files"),
            ),
            choice(
                "self-signed",
                "Self-Signed Paths",
                Some("prepare the default paths for a future self-signed certificate"),
            ),
            choice(
                "none",
                "Skip TLS Paths",
                Some("leave TLS paths unset and rely on later manual configuration"),
            ),
        ],
        Some(
            tls_scaffold_hint
                .map(|value| tls_scaffold_value(value).to_owned())
                .unwrap_or_else(|| "bring-your-own".to_owned()),
        ),
    ))?;
    if tls_scaffold == "none" {
        plan.tls_enabled = true;
        plan.warnings.push(
            "public TLS was selected without concrete cert/key paths; complete those values before the daemon can bind publicly."
                .to_owned(),
        );
        return Ok(());
    }
    plan.tls_enabled = true;
    let cert_path = wizard.text(
        text_step(
            "tls_cert_path",
            "TLS Certificate Path",
            "Enter the certificate path that the daemon should use.",
            Some("./tls/gateway.crt".to_owned()),
            None,
            false,
        ),
        |value| validate_non_empty_text(value, "TLS certificate path"),
    )?;
    let key_path = wizard.text(
        text_step(
            "tls_key_path",
            "TLS Key Path",
            "Enter the private key path that the daemon should use.",
            Some("./tls/gateway.key".to_owned()),
            None,
            false,
        ),
        |value| validate_non_empty_text(value, "TLS key path"),
    )?;
    plan.tls_cert_path = Some(cert_path);
    plan.tls_key_path = Some(key_path);
    Ok(())
}

fn prompt_port(
    wizard: &mut WizardSession<'_, dyn WizardBackend>,
    id: &'static str,
    title: &'static str,
    message: &'static str,
    default_port: u16,
) -> Result<u16> {
    let value = wizard.text(
        text_step(id, title, message, Some(default_port.to_string()), None, false),
        |value| {
            value
                .parse::<u16>()
                .map(|_| ())
                .map_err(|_| format!("{title} must be a valid u16 value"))
        },
    )?;
    value.parse::<u16>().with_context(|| format!("{title} must be a valid u16 value"))
}

fn clear_model_provider_auth(document: &mut toml::Value) -> Result<()> {
    unset_value_at_path(document, "model_provider.openai_api_key")?;
    unset_value_at_path(document, "model_provider.openai_api_key_secret_ref")?;
    unset_value_at_path(document, "model_provider.openai_api_key_vault_ref")?;
    unset_value_at_path(document, "model_provider.anthropic_api_key")?;
    unset_value_at_path(document, "model_provider.anthropic_api_key_secret_ref")?;
    unset_value_at_path(document, "model_provider.anthropic_api_key_vault_ref")?;
    unset_value_at_path(document, "model_provider.auth_profile_id")?;
    unset_value_at_path(document, "model_provider.auth_profile_ref")?;
    unset_value_at_path(document, "model_provider.auth_provider_kind")?;
    unset_value_at_path(document, "model_provider.providers")?;
    unset_value_at_path(document, "model_provider.models")?;
    unset_value_at_path(document, "model_provider.default_chat_model_id")?;
    unset_value_at_path(document, "model_provider.default_embeddings_model_id")?;
    unset_value_at_path(document, "model_provider.default_audio_transcription_model_id")?;
    Ok(())
}

fn apply_model_provider_api_key(
    document: &mut toml::Value,
    auth_method: &str,
    api_key: &str,
) -> Result<()> {
    match auth_method {
        "anthropic_api_key" => {
            let base_url = anthropic_base_url_for_config(document)?;
            clear_model_provider_auth(document)?;
            let vault_ref = store_secret_in_vault("global", "anthropic_api_key", api_key)?;
            ensure_http_fetch_credential_vault_ref(document, vault_ref.as_str())?;
            configure_anthropic_provider_with_base_url(
                document,
                base_url.as_str(),
                None,
                Some(vault_ref),
            )?;
        }
        "minimax_api_key" | "minimax_api_key_global" | "minimax_api_key_cn" => {
            // Discovery must run before clearing auth: it reads the currently configured
            // MiniMax chat model from the document as a fallback for legacy configs.
            let selection = discover_minimax_model_selection(
                document,
                api_key,
                minimax_base_url_override(auth_method),
            )?;
            clear_model_provider_auth(document)?;
            let vault_ref =
                store_secret_in_vault("global", minimax_secret_key(auth_method), api_key)?;
            ensure_http_fetch_credential_vault_ref(document, vault_ref.as_str())?;
            configure_minimax_provider(
                document,
                selection.base_url.as_str(),
                Some(selection.model_id.as_str()),
                Some(vault_ref),
            )?;
        }
        method if registry_provider_defaults_for_auth_method(method).is_some() => {
            let defaults = registry_provider_defaults_for_auth_method(method)
                .expect("registry provider defaults should exist after guard");
            let base_url = registry_provider_base_url(defaults)?;
            let model = discover_openai_compatible_model_selection(
                defaults.display_name,
                base_url.as_str(),
                api_key,
            )?;
            clear_model_provider_auth(document)?;
            let vault_ref = store_secret_in_vault("global", defaults.secret_key, api_key)?;
            ensure_http_fetch_credential_vault_ref(document, vault_ref.as_str())?;
            configure_registry_provider(
                document,
                defaults,
                base_url.as_str(),
                Some(&model),
                Some(vault_ref),
            )?;
        }
        "api_key" => {
            let base_url = openai_base_url_for_config(document)?;
            clear_model_provider_auth(document)?;
            let vault_ref = store_secret_in_vault("global", "openai_api_key", api_key)?;
            ensure_http_fetch_credential_vault_ref(document, vault_ref.as_str())?;
            configure_openai_provider_with_base_url(
                document,
                base_url.as_str(),
                None,
                Some(vault_ref),
            )?;
        }
        _ => anyhow::bail!("unsupported model-provider auth method: {auth_method}"),
    }
    Ok(())
}

fn ensure_http_fetch_credential_vault_ref(
    document: &mut toml::Value,
    vault_ref: &str,
) -> Result<()> {
    let mut refs = match get_value_at_path(document, HTTP_FETCH_CREDENTIAL_VAULT_REFS_PATH)
        .ok()
        .flatten()
    {
        Some(value) => {
            let values = value.as_array().ok_or_else(|| {
                anyhow::anyhow!("{HTTP_FETCH_CREDENTIAL_VAULT_REFS_PATH} must be an array")
            })?;
            let mut refs = Vec::with_capacity(values.len() + 1);
            for value in values {
                let Some(value) = value.as_str() else {
                    anyhow::bail!("{HTTP_FETCH_CREDENTIAL_VAULT_REFS_PATH} must contain strings");
                };
                let trimmed = value.trim();
                if !trimmed.is_empty() && !refs.iter().any(|existing| existing == trimmed) {
                    refs.push(trimmed.to_owned());
                }
            }
            refs
        }
        None => Vec::new(),
    };

    if !refs.iter().any(|existing| existing == vault_ref) {
        refs.push(vault_ref.to_owned());
    }
    set_value_at_path(
        document,
        HTTP_FETCH_CREDENTIAL_VAULT_REFS_PATH,
        toml::Value::Array(refs.into_iter().map(toml::Value::String).collect()),
    )?;
    Ok(())
}

fn apply_deferred_provider_auth_method(
    document: &mut toml::Value,
    auth_method: &str,
    warnings: &mut Vec<String>,
) -> Result<()> {
    clear_model_provider_auth(document)?;
    match auth_method {
        "chatgpt_login" => configure_openai_provider(document, None, None)?,
        "anthropic_oauth" => configure_anthropic_provider(document, None, None)?,
        "minimax_oauth_global" => {
            configure_minimax_provider(document, DEFAULT_MINIMAX_BASE_URL, None, None)?;
        }
        "minimax_oauth_cn" => {
            configure_minimax_provider(document, DEFAULT_MINIMAX_CN_BASE_URL, None, None)?;
        }
        "xai_device_code" | "xai_oauth" => {
            let defaults = registry_provider_defaults_for_auth_method("xai_api_key")
                .expect("xAI registry defaults must exist");
            configure_registry_provider(document, defaults, defaults.base_url, None, None)?;
        }
        "gemini_cli_oauth" => {
            let mut defaults = *registry_provider_defaults_for_auth_method("google_gemini_api_key")
                .expect("Google Gemini registry defaults must exist");
            defaults.auth_provider_kind = GOOGLE_GEMINI_CLI_AUTH_PROVIDER_KIND;
            configure_registry_provider(document, &defaults, defaults.base_url, None, None)?;
        }
        "openrouter_oauth" => {
            let defaults = registry_provider_defaults_for_auth_method("openrouter_api_key")
                .expect("OpenRouter registry defaults must exist");
            configure_registry_provider(document, defaults, defaults.base_url, None, None)?;
        }
        _ => anyhow::bail!("unsupported non-API-key auth method: {auth_method}"),
    }
    if auth_method == "chatgpt_login" {
        warnings.push(
            "ChatGPT Login was selected; after the gateway is running, run `palyra auth openai oauth-start --set-default --open`, sign in at the printed URL, then run `palyra auth openai oauth-state <attempt_id>` until it reports succeeded."
                .to_owned(),
        );
    } else if auth_method == "xai_device_code" {
        warnings.push(
            "xAI device code was selected; after the gateway is running, run `palyra auth xai device-code --set-default --open`, enter the browser code, and wait for authorization to finish."
                .to_owned(),
        );
    } else if auth_method == "xai_oauth" {
        warnings.push(
            "xAI OAuth was selected; after the gateway is running, run `palyra auth xai oauth-start --set-default --open` and finish the browser callback."
                .to_owned(),
        );
    } else {
        warnings.push(format!(
            "{} was selected; finish or select a matching auth profile before enabling remote model calls.",
            auth_method_label(auth_method)
        ));
    }
    Ok(())
}

fn configure_openai_provider(
    document: &mut toml::Value,
    model_id: Option<&str>,
    vault_ref: Option<String>,
) -> Result<()> {
    configure_openai_provider_with_base_url(document, OPENAI_DEFAULT_BASE_URL, model_id, vault_ref)
}

fn configure_openai_provider_with_base_url(
    document: &mut toml::Value,
    base_url: &str,
    model_id: Option<&str>,
    vault_ref: Option<String>,
) -> Result<()> {
    set_value_at_path(
        document,
        "model_provider.kind",
        toml::Value::String("openai_compatible".to_owned()),
    )?;
    set_value_at_path(
        document,
        "model_provider.openai_base_url",
        toml::Value::String(base_url.to_owned()),
    )?;
    if base_url_requires_private_opt_in(base_url) {
        set_value_at_path(
            document,
            "model_provider.allow_private_base_url",
            toml::Value::Boolean(true),
        )?;
    }
    apply_openai_chat_model_selection(document, model_id)?;
    unset_value_at_path(document, "model_provider.openai_embeddings_model")?;
    unset_value_at_path(document, "model_provider.openai_embeddings_dims")?;
    unset_value_at_path(document, "model_provider.anthropic_base_url")?;
    unset_value_at_path(document, "model_provider.anthropic_model")?;
    if let Some(vault_ref) = vault_ref {
        set_value_at_path(
            document,
            "model_provider.openai_api_key_vault_ref",
            toml::Value::String(vault_ref),
        )?;
    }
    if model_id.is_none() {
        write_pending_registry_provider(
            document,
            PendingRegistryProvider {
                provider_id: "openai-primary",
                display_name: "OpenAI",
                kind: "openai_compatible",
                base_url,
                auth_provider_kind: "openai",
            },
            None,
        )?;
    }
    Ok(())
}

fn configure_anthropic_provider(
    document: &mut toml::Value,
    model_id: Option<&str>,
    vault_ref: Option<String>,
) -> Result<()> {
    configure_anthropic_provider_with_base_url(
        document,
        ANTHROPIC_DEFAULT_BASE_URL,
        model_id,
        vault_ref,
    )
}

fn configure_anthropic_provider_with_base_url(
    document: &mut toml::Value,
    base_url: &str,
    model_id: Option<&str>,
    vault_ref: Option<String>,
) -> Result<()> {
    set_value_at_path(
        document,
        "model_provider.kind",
        toml::Value::String("anthropic".to_owned()),
    )?;
    set_value_at_path(
        document,
        "model_provider.anthropic_base_url",
        toml::Value::String(base_url.to_owned()),
    )?;
    if base_url_requires_private_opt_in(base_url) {
        set_value_at_path(
            document,
            "model_provider.allow_private_base_url",
            toml::Value::Boolean(true),
        )?;
    }
    apply_anthropic_chat_model_selection(document, model_id)?;
    unset_value_at_path(document, "model_provider.openai_base_url")?;
    unset_value_at_path(document, "model_provider.openai_model")?;
    unset_value_at_path(document, "model_provider.openai_embeddings_model")?;
    unset_value_at_path(document, "model_provider.openai_embeddings_dims")?;
    if let Some(vault_ref) = vault_ref {
        set_value_at_path(
            document,
            "model_provider.anthropic_api_key_vault_ref",
            toml::Value::String(vault_ref),
        )?;
    }
    if model_id.is_none() {
        write_pending_registry_provider(
            document,
            PendingRegistryProvider {
                provider_id: "anthropic-primary",
                display_name: "Anthropic",
                kind: "anthropic",
                base_url,
                auth_provider_kind: "anthropic",
            },
            None,
        )?;
    }
    Ok(())
}

fn configure_minimax_provider(
    document: &mut toml::Value,
    base_url: &str,
    model_id: Option<&str>,
    vault_ref: Option<String>,
) -> Result<()> {
    set_value_at_path(
        document,
        "model_provider.kind",
        toml::Value::String("anthropic".to_owned()),
    )?;
    set_value_at_path(
        document,
        "model_provider.auth_provider_kind",
        toml::Value::String(MINIMAX_AUTH_PROVIDER_KIND.to_owned()),
    )?;
    set_value_at_path(
        document,
        "model_provider.anthropic_base_url",
        toml::Value::String(base_url.to_owned()),
    )?;
    apply_anthropic_chat_model_selection(document, model_id)?;
    if base_url_requires_private_opt_in(base_url) {
        set_value_at_path(
            document,
            "model_provider.allow_private_base_url",
            toml::Value::Boolean(true),
        )?;
    }
    unset_value_at_path(document, "model_provider.openai_base_url")?;
    unset_value_at_path(document, "model_provider.openai_model")?;
    unset_value_at_path(document, "model_provider.openai_embeddings_model")?;
    unset_value_at_path(document, "model_provider.openai_embeddings_dims")?;
    if let Some(vault_ref) = vault_ref {
        set_value_at_path(
            document,
            "model_provider.anthropic_api_key_vault_ref",
            toml::Value::String(vault_ref),
        )?;
    }
    if model_id.is_none() {
        write_pending_registry_provider(
            document,
            PendingRegistryProvider {
                provider_id: "minimax-primary",
                display_name: "MiniMax",
                kind: "anthropic",
                base_url,
                auth_provider_kind: MINIMAX_AUTH_PROVIDER_KIND,
            },
            None,
        )?;
    }
    Ok(())
}

fn configure_registry_provider(
    document: &mut toml::Value,
    defaults: &RegistryProviderDefaults,
    base_url: &str,
    model: Option<&DiscoveredProviderModel>,
    vault_ref: Option<String>,
) -> Result<()> {
    let model_id = model.map(|model| model.id.as_str());
    set_value_at_path(
        document,
        "model_provider.kind",
        toml::Value::String("openai_compatible".to_owned()),
    )?;
    set_value_at_path(
        document,
        "model_provider.auth_provider_kind",
        toml::Value::String(defaults.auth_provider_kind.to_owned()),
    )?;
    set_value_at_path(
        document,
        "model_provider.openai_base_url",
        toml::Value::String(base_url.to_owned()),
    )?;
    if base_url_requires_private_opt_in(base_url) {
        set_value_at_path(
            document,
            "model_provider.allow_private_base_url",
            toml::Value::Boolean(true),
        )?;
    }
    apply_openai_chat_model_selection(document, model_id)?;
    unset_value_at_path(document, "model_provider.openai_embeddings_model")?;
    unset_value_at_path(document, "model_provider.openai_embeddings_dims")?;
    unset_value_at_path(document, "model_provider.anthropic_base_url")?;
    unset_value_at_path(document, "model_provider.anthropic_model")?;
    set_value_at_path(
        document,
        "model_provider.providers",
        toml::Value::Array(vec![registry_provider_table(defaults, base_url, vault_ref)]),
    )?;
    if let Some(model) = model {
        set_value_at_path(
            document,
            "model_provider.models",
            toml::Value::Array(vec![registry_chat_model_table(defaults, model)]),
        )?;
        set_value_at_path(
            document,
            "model_provider.default_chat_model_id",
            toml::Value::String(model.id.clone()),
        )?;
    } else {
        unset_value_at_path(document, "model_provider.models")?;
        unset_value_at_path(document, "model_provider.default_chat_model_id")?;
    }
    Ok(())
}

fn registry_provider_table(
    defaults: &RegistryProviderDefaults,
    base_url: &str,
    vault_ref: Option<String>,
) -> toml::Value {
    let mut table = toml::map::Map::new();
    table.insert("provider_id".to_owned(), toml::Value::String(defaults.provider_id.to_owned()));
    table.insert("display_name".to_owned(), toml::Value::String(defaults.display_name.to_owned()));
    table.insert("kind".to_owned(), toml::Value::String("openai_compatible".to_owned()));
    table.insert("base_url".to_owned(), toml::Value::String(base_url.to_owned()));
    table.insert(
        "auth_provider_kind".to_owned(),
        toml::Value::String(defaults.auth_provider_kind.to_owned()),
    );
    table.insert("enabled".to_owned(), toml::Value::Boolean(true));
    if let Some(vault_ref) = vault_ref {
        table.insert("api_key_vault_ref".to_owned(), toml::Value::String(vault_ref));
    }
    toml::Value::Table(table)
}

fn registry_chat_model_table(
    defaults: &RegistryProviderDefaults,
    model: &DiscoveredProviderModel,
) -> toml::Value {
    let mut table = toml::map::Map::new();
    table.insert("model_id".to_owned(), toml::Value::String(model.id.clone()));
    table.insert("provider_id".to_owned(), toml::Value::String(defaults.provider_id.to_owned()));
    table.insert("role".to_owned(), toml::Value::String("chat".to_owned()));
    table.insert("enabled".to_owned(), toml::Value::Boolean(true));
    if let Some(supports_tool_calls) = model.supports_tool_calls {
        table.insert("tool_calls".to_owned(), toml::Value::Boolean(supports_tool_calls));
    }
    if let Some(supports_json_mode) = model.supports_json_mode {
        table.insert("json_mode".to_owned(), toml::Value::Boolean(supports_json_mode));
    }
    if let Some(supports_vision) = model.supports_vision {
        table.insert("vision".to_owned(), toml::Value::Boolean(supports_vision));
    }
    toml::Value::Table(table)
}

struct PendingRegistryProvider<'a> {
    provider_id: &'a str,
    display_name: &'a str,
    kind: &'a str,
    base_url: &'a str,
    auth_provider_kind: &'a str,
}

fn write_pending_registry_provider(
    document: &mut toml::Value,
    provider: PendingRegistryProvider<'_>,
    vault_ref: Option<String>,
) -> Result<()> {
    let mut table = toml::map::Map::new();
    table.insert("provider_id".to_owned(), toml::Value::String(provider.provider_id.to_owned()));
    table.insert("display_name".to_owned(), toml::Value::String(provider.display_name.to_owned()));
    table.insert("kind".to_owned(), toml::Value::String(provider.kind.to_owned()));
    table.insert("base_url".to_owned(), toml::Value::String(provider.base_url.to_owned()));
    table.insert(
        "auth_provider_kind".to_owned(),
        toml::Value::String(provider.auth_provider_kind.to_owned()),
    );
    table.insert("enabled".to_owned(), toml::Value::Boolean(true));
    if let Some(vault_ref) = vault_ref {
        table.insert("api_key_vault_ref".to_owned(), toml::Value::String(vault_ref));
    }
    set_value_at_path(
        document,
        "model_provider.providers",
        toml::Value::Array(vec![toml::Value::Table(table)]),
    )?;
    unset_value_at_path(document, "model_provider.models")?;
    unset_value_at_path(document, "model_provider.default_chat_model_id")?;
    Ok(())
}

fn apply_openai_chat_model_selection(
    document: &mut toml::Value,
    model_id: Option<&str>,
) -> Result<()> {
    if let Some(model_id) = model_id {
        set_value_at_path(
            document,
            "model_provider.openai_model",
            toml::Value::String(model_id.to_owned()),
        )?;
    } else {
        unset_value_at_path(document, "model_provider.openai_model")?;
    }
    Ok(())
}

fn apply_anthropic_chat_model_selection(
    document: &mut toml::Value,
    model_id: Option<&str>,
) -> Result<()> {
    if let Some(model_id) = model_id {
        set_value_at_path(
            document,
            "model_provider.anthropic_model",
            toml::Value::String(model_id.to_owned()),
        )?;
    } else {
        unset_value_at_path(document, "model_provider.anthropic_model")?;
    }
    Ok(())
}

#[derive(Debug)]
struct MinimaxModelSelection {
    base_url: String,
    model_id: String,
}

fn discover_openai_compatible_model_selection(
    provider_label: &str,
    base_url: &str,
    api_key: &str,
) -> Result<DiscoveredProviderModel> {
    let models = discover_openai_compatible_models(provider_label, api_key, base_url)?;
    select_preferred_discovered_model(models.as_slice()).cloned().ok_or_else(|| {
        anyhow::anyhow!(
            "{provider_label} model discovery returned no selectable models; no model was written because the wizard does not use hardcoded provider defaults"
        )
    })
}

#[cfg(test)]
fn select_openai_api_preferred_model(
    models: &[DiscoveredProviderModel],
) -> Option<DiscoveredProviderModel> {
    let candidates = models
        .iter()
        .filter(|model| model.can_be_chat_default())
        .filter(|model| !is_openai_dynamic_chat_alias(model.id.as_str()))
        .filter(|model| !is_openai_expensive_or_snapshot_default(model.id.as_str()))
        .collect::<Vec<_>>();
    OPENAI_API_CURATED_DEFAULT_ORDER.iter().find_map(|preferred| {
        candidates
            .iter()
            .copied()
            .find(|model| openai_model_id_matches(model.id.as_str(), preferred))
            .cloned()
    })
}

#[cfg(test)]
fn is_openai_dynamic_chat_alias(model_id: &str) -> bool {
    let normalized = model_id.trim().to_ascii_lowercase();
    normalized == "chat-latest" || normalized.ends_with("/chat-latest")
}

#[cfg(test)]
fn is_openai_expensive_or_snapshot_default(model_id: &str) -> bool {
    let normalized = openai_model_terminal_id(model_id).to_ascii_lowercase();
    is_openai_pro_model(normalized.as_str()) || has_date_snapshot_suffix(normalized.as_str())
}

#[cfg(test)]
fn openai_model_id_matches(model_id: &str, expected: &str) -> bool {
    openai_model_terminal_id(model_id).eq_ignore_ascii_case(expected)
}

#[cfg(test)]
fn openai_model_terminal_id(model_id: &str) -> &str {
    model_id.trim().rsplit('/').next().unwrap_or_default().trim()
}

#[cfg(test)]
fn is_openai_pro_model(model_id: &str) -> bool {
    model_id.split('-').any(|part| part == "pro")
}

#[cfg(test)]
fn has_date_snapshot_suffix(model_id: &str) -> bool {
    let bytes = model_id.as_bytes();
    if bytes.len() < 11 || bytes[bytes.len() - 11] != b'-' {
        return false;
    }
    let suffix = &bytes[bytes.len() - 10..];
    suffix[4] == b'-'
        && suffix[7] == b'-'
        && suffix
            .iter()
            .enumerate()
            .all(|(index, byte)| matches!(index, 4 | 7) || byte.is_ascii_digit())
}

fn discover_openai_compatible_models(
    provider_label: &str,
    api_key: &str,
    base_url: &str,
) -> Result<Vec<DiscoveredProviderModel>> {
    let endpoint = provider_models_endpoint(base_url)?;
    let client = Client::builder()
        .timeout(PROVIDER_MODEL_DISCOVERY_TIMEOUT)
        .build()
        .with_context(|| format!("failed to initialize {provider_label} model discovery client"))?;
    let mut headers = HeaderMap::new();
    headers.insert(ACCEPT, HeaderValue::from_static("application/json"));
    let bearer = format!("Bearer {api_key}");
    headers.insert(
        AUTHORIZATION,
        HeaderValue::from_str(bearer.as_str()).with_context(|| {
            format!("{provider_label} API key cannot be sent as an authorization header")
        })?,
    );

    let response = client
        .get(endpoint)
        .headers(headers)
        .send()
        .with_context(|| format!("failed to call {provider_label} model discovery endpoint"))?;
    let status = response.status();
    let body = response.text().unwrap_or_default();
    if !status.is_success() {
        anyhow::bail!(
            "{provider_label} model discovery failed: {}",
            sanitize_provider_error(body.as_str(), status.as_u16())
        );
    }
    parse_discovered_provider_models(body.as_str())
}

fn openai_base_url_for_config(document: &toml::Value) -> Result<String> {
    openai_base_url_for_config_with_override(document, None)
}

fn openai_base_url_for_config_with_override(
    _document: &toml::Value,
    _env_override: Option<String>,
) -> Result<String> {
    Ok(OPENAI_DEFAULT_BASE_URL.to_owned())
}

fn anthropic_base_url_for_config(document: &toml::Value) -> Result<String> {
    anthropic_base_url_for_config_with_override(document, None)
}

fn anthropic_base_url_for_config_with_override(
    _document: &toml::Value,
    _env_override: Option<String>,
) -> Result<String> {
    Ok(ANTHROPIC_DEFAULT_BASE_URL.to_owned())
}

fn registry_provider_base_url(defaults: &RegistryProviderDefaults) -> Result<String> {
    let env_name = match defaults.auth_provider_kind {
        kind if kind.eq_ignore_ascii_case(XAI_AUTH_PROVIDER_KIND) => Some(XAI_BASE_URL_ENV),
        kind if kind.eq_ignore_ascii_case(GOOGLE_GEMINI_AUTH_PROVIDER_KIND)
            || kind.eq_ignore_ascii_case(GOOGLE_GEMINI_CLI_AUTH_PROVIDER_KIND) =>
        {
            Some(GOOGLE_GEMINI_BASE_URL_ENV)
        }
        kind if kind.eq_ignore_ascii_case(OPENROUTER_AUTH_PROVIDER_KIND) => {
            Some(OPENROUTER_BASE_URL_ENV)
        }
        _ => None,
    };
    if let Some(env_name) = env_name {
        if let Some(base_url) = provider_base_url_from_env(env_name)? {
            return Ok(base_url);
        }
    }
    Ok(defaults.base_url.to_owned())
}

fn provider_base_url_from_env(env_name: &str) -> Result<Option<String>> {
    match env::var(env_name) {
        Ok(raw) => normalize_provider_discovery_base_url(raw.as_str(), env_name).map(Some),
        Err(env::VarError::NotPresent) => Ok(None),
        Err(error) => anyhow::bail!("{env_name} must contain valid Unicode: {error}"),
    }
}

/// Resolves the MiniMax base URL and chat model, preferring live model discovery
/// and falling back to an already-configured MiniMax model when discovery fails
/// or returns nothing selectable.
fn discover_minimax_model_selection(
    document: &toml::Value,
    api_key: &str,
    base_url_override: Option<&str>,
) -> Result<MinimaxModelSelection> {
    let base_url = match base_url_override {
        Some(base_url) => {
            normalize_minimax_discovery_base_url(base_url, "selected MiniMax endpoint")?
        }
        None => minimax_base_url_for_config(document)?,
    };
    let existing_model = existing_minimax_chat_model(document)?;
    match discover_minimax_models(api_key, base_url.as_str()) {
        Ok(models) => {
            if let Some(model_id) = select_preferred_discovered_model_id(models.as_slice()) {
                return Ok(MinimaxModelSelection { base_url, model_id });
            }
            if let Some(model_id) = existing_model {
                return Ok(MinimaxModelSelection { base_url, model_id });
            }
            anyhow::bail!(
                "MiniMax model discovery returned no selectable models; configure a model explicitly or retry after the provider exposes /v1/models"
            );
        }
        Err(error) => {
            if let Some(model_id) = existing_model {
                return Ok(MinimaxModelSelection { base_url, model_id });
            }
            Err(error).context(
                "failed to discover MiniMax models while configuring API-key auth; no model was written because the wizard no longer uses a hardcoded MiniMax default",
            )
        }
    }
}

fn minimax_base_url_override(auth_method: &str) -> Option<&'static str> {
    match auth_method {
        "minimax_api_key_global" => Some(DEFAULT_MINIMAX_BASE_URL),
        "minimax_api_key_cn" => Some(DEFAULT_MINIMAX_CN_BASE_URL),
        _ => None,
    }
}

fn minimax_secret_key(auth_method: &str) -> &'static str {
    match auth_method {
        "minimax_api_key_cn" => "minimax_cn_api_key",
        _ => "minimax_api_key",
    }
}

fn minimax_base_url_for_config(document: &toml::Value) -> Result<String> {
    match env::var(MINIMAX_BASE_URL_ENV) {
        Ok(raw) => return normalize_minimax_discovery_base_url(raw.as_str(), MINIMAX_BASE_URL_ENV),
        Err(env::VarError::NotPresent) => {}
        Err(error) => anyhow::bail!("{MINIMAX_BASE_URL_ENV} must contain valid Unicode: {error}"),
    }

    let configured_for_minimax =
        get_string_value_at_path(document, "model_provider.kind")?.as_deref() == Some("anthropic")
            && get_string_value_at_path(document, "model_provider.auth_provider_kind")?
                .as_deref()
                .is_some_and(|kind| kind.eq_ignore_ascii_case(MINIMAX_AUTH_PROVIDER_KIND));
    if configured_for_minimax {
        if let Some(base_url) =
            get_string_value_at_path(document, "model_provider.anthropic_base_url")?
        {
            return normalize_minimax_discovery_base_url(
                base_url.as_str(),
                "model_provider.anthropic_base_url",
            );
        }
    }

    Ok(DEFAULT_MINIMAX_BASE_URL.to_owned())
}

fn normalize_minimax_discovery_base_url(raw: &str, source: &str) -> Result<String> {
    let base_url = normalize_provider_discovery_base_url(raw, source)?;
    validate_minimax_discovery_base_url(base_url.as_str(), source)?;
    Ok(base_url)
}

fn validate_minimax_discovery_base_url(base_url: &str, source: &str) -> Result<()> {
    let parsed = reqwest::Url::parse(base_url)
        .with_context(|| format!("{source} must be a valid absolute URL"))?;
    let host = parsed.host_str().ok_or_else(|| anyhow::anyhow!("{source} must include a host"))?;
    if TRUSTED_MINIMAX_DISCOVERY_HOSTS
        .iter()
        .any(|trusted_host| host.eq_ignore_ascii_case(trusted_host))
    {
        return Ok(());
    }

    anyhow::bail!(
        "{source} points to unsupported MiniMax discovery host '{host}'; MiniMax API-key onboarding only sends newly supplied keys to official MiniMax endpoints ({})",
        TRUSTED_MINIMAX_DISCOVERY_HOSTS.join(", ")
    );
}

/// Validates and canonicalizes a provider discovery base URL; https is required
/// except for loopback http endpoints used by local test rigs.
fn normalize_provider_discovery_base_url(raw: &str, source: &str) -> Result<String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        anyhow::bail!("{source} cannot be empty");
    }
    let parsed = reqwest::Url::parse(trimmed)
        .with_context(|| format!("{source} must be a valid absolute URL"))?;
    let host = parsed.host_str().ok_or_else(|| anyhow::anyhow!("{source} must include a host"))?;
    if parsed.scheme() != "https" && !(parsed.scheme() == "http" && host_is_loopback(host)) {
        anyhow::bail!("{source} must use https; http is only allowed for loopback hosts");
    }
    if !parsed.username().is_empty() || parsed.password().is_some() {
        anyhow::bail!("{source} must not embed credentials");
    }
    if parsed.query().is_some() || parsed.fragment().is_some() {
        anyhow::bail!("{source} must not include query or fragment");
    }
    Ok(parsed.as_str().trim_end_matches('/').to_owned())
}

fn base_url_requires_private_opt_in(base_url: &str) -> bool {
    reqwest::Url::parse(base_url)
        .is_ok_and(|url| url.scheme() == "http" && url.host_str().is_some_and(host_is_loopback))
}

fn host_is_loopback(host: &str) -> bool {
    host.eq_ignore_ascii_case("localhost")
        || host.parse::<std::net::IpAddr>().is_ok_and(|ip| ip.is_loopback())
}

fn existing_minimax_chat_model(document: &toml::Value) -> Result<Option<String>> {
    let provider_kind =
        get_string_value_at_path(document, "model_provider.kind")?.unwrap_or_default();
    let auth_provider_kind =
        get_string_value_at_path(document, "model_provider.auth_provider_kind")?;
    if provider_kind == "anthropic"
        && auth_provider_kind
            .as_deref()
            .is_some_and(|kind| kind.eq_ignore_ascii_case(MINIMAX_AUTH_PROVIDER_KIND))
    {
        return get_string_value_at_path(document, "model_provider.anthropic_model");
    }
    Ok(None)
}

fn discover_minimax_models(api_key: &str, base_url: &str) -> Result<Vec<DiscoveredProviderModel>> {
    let endpoint = provider_models_endpoint(base_url)?;
    let client = Client::builder()
        .timeout(PROVIDER_MODEL_DISCOVERY_TIMEOUT)
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .context("failed to initialize MiniMax model discovery client")?;
    let mut headers = HeaderMap::new();
    headers.insert(ACCEPT, HeaderValue::from_static("application/json"));
    let bearer = format!("Bearer {api_key}");
    headers.insert(
        AUTHORIZATION,
        HeaderValue::from_str(bearer.as_str())
            .context("MiniMax API key cannot be sent as an authorization header")?,
    );

    let response = client
        .get(endpoint)
        .headers(headers)
        .send()
        .context("failed to call MiniMax model discovery endpoint")?;
    let status = response.status();
    let body = response.text().unwrap_or_default();
    if !status.is_success() {
        anyhow::bail!(
            "MiniMax model discovery failed: {}",
            sanitize_provider_error(body.as_str(), status.as_u16())
        );
    }
    parse_discovered_provider_models(body.as_str())
}

fn store_secret_in_vault(scope_raw: &str, key: &str, value: &str) -> Result<String> {
    let vault = open_cli_vault().context("failed to initialize vault runtime")?;
    let scope = parse_vault_scope(scope_raw)?;
    vault
        .put_secret(&scope, key, value.as_bytes())
        .with_context(|| format!("failed to store secret key={} scope={scope}", key))?;
    Ok(format!("{scope}/{key}"))
}

fn collect_secret_inputs(
    api_key_env: Option<String>,
    api_key_stdin: bool,
    api_key_prompt: bool,
    admin_token_env: Option<String>,
    admin_token_stdin: bool,
    admin_token_prompt: bool,
) -> Result<SecretInputs> {
    Ok(SecretInputs {
        api_key: load_secret_input_optional(
            api_key_env,
            api_key_stdin,
            api_key_prompt,
            "Model provider API key: ",
        )?,
        admin_token: load_secret_input_optional(
            admin_token_env,
            admin_token_stdin,
            admin_token_prompt,
            "Remote admin token: ",
        )?,
    })
}

/// Loads a secret from exactly one of env/stdin/prompt; `Ok(None)` when no source
/// was selected.
fn load_secret_input_optional(
    env_name: Option<String>,
    from_stdin: bool,
    from_prompt: bool,
    prompt: &str,
) -> Result<Option<String>> {
    let selected =
        usize::from(env_name.is_some()) + usize::from(from_stdin) + usize::from(from_prompt);
    if selected == 0 {
        return Ok(None);
    }
    if selected != 1 {
        anyhow::bail!("select exactly one secret source: --*-env, --*-stdin, or --*-prompt");
    }
    if let Some(env_name) = env_name {
        let value = env::var(env_name.as_str())
            .with_context(|| format!("environment variable {env_name} is not set"))?;
        let trimmed = value.trim();
        if trimmed.is_empty() {
            anyhow::bail!("environment variable {env_name} does not contain a usable secret value");
        }
        return Ok(Some(trimmed.to_owned()));
    }
    if from_stdin {
        let mut value = String::new();
        std::io::stdin()
            .read_to_string(&mut value)
            .context("failed to read secret value from stdin")?;
        let trimmed = value.trim();
        if trimmed.is_empty() {
            anyhow::bail!("stdin did not contain a usable secret value");
        }
        return Ok(Some(trimmed.to_owned()));
    }
    let value = rpassword::prompt_password(prompt).context("failed to read secret from prompt")?;
    let trimmed = value.trim();
    if trimmed.is_empty() {
        anyhow::bail!("prompt did not contain a usable secret value");
    }
    Ok(Some(trimmed.to_owned()))
}

fn validate_stdin_secret_usage(
    non_interactive: bool,
    api_key_stdin: bool,
    admin_token_stdin: bool,
) -> Result<()> {
    if api_key_stdin && admin_token_stdin {
        anyhow::bail!(
            "only one secret may be sourced from stdin per invocation; split model-provider API key and admin token configuration into separate runs or use environment/prompt sources"
        );
    }
    if (api_key_stdin || admin_token_stdin) && !non_interactive {
        let flag = if api_key_stdin { "--api-key-stdin" } else { "--admin-token-stdin" };
        anyhow::bail!(
            "{flag} requires --non-interactive so stdin is reserved for the secret value instead of interactive wizard prompts; rerun scripted flows with --non-interactive --accept-risk or use an environment/prompt secret source"
        );
    }
    Ok(())
}

fn validate_api_key_secret_matches_auth_method(
    auth_method: Option<&str>,
    api_key_provided: bool,
) -> Result<()> {
    if !api_key_provided {
        return Ok(());
    }
    let Some(auth_method) = auth_method else {
        return Ok(());
    };
    if auth_method_requires_api_key(auth_method) {
        return Ok(());
    }
    anyhow::bail!(
        "model-provider API-key secret input was provided, but auth method '{}' does not consume an API key; choose an API-key auth method or remove --api-key-env/--api-key-stdin/--api-key-prompt",
        auth_method_label(auth_method)
    );
}

fn validate_non_empty_text(value: &str, field: &str) -> Result<(), String> {
    if value.trim().is_empty() {
        return Err(format!("{field} cannot be empty"));
    }
    Ok(())
}

fn normalize_workspace_root(raw: &str) -> Result<String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        anyhow::bail!("workspace root cannot be empty");
    }
    let path = PathBuf::from(trimmed);
    let absolute = if path.is_absolute() {
        path
    } else {
        env::current_dir()
            .context("failed to resolve current working directory for workspace root")?
            .join(path)
    };
    Ok(absolute.display().to_string())
}

fn ensure_directory_exists(path: &Path) -> Result<()> {
    fs::create_dir_all(path)
        .with_context(|| format!("failed to create directory {}", path.display()))
}

fn default_workspace_root() -> String {
    env::current_dir().map(|path| path.display().to_string()).unwrap_or_else(|_| ".".to_owned())
}

fn get_string_value_at_path(document: &toml::Value, key: &str) -> Result<Option<String>> {
    Ok(get_value_at_path(document, key)?
        .and_then(toml::Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned))
}

fn get_integer_value_at_path(document: &toml::Value, key: &str) -> Result<Option<i64>> {
    Ok(get_value_at_path(document, key)?.and_then(toml::Value::as_integer))
}

fn get_bool_value_at_path(document: &toml::Value, key: &str) -> Result<Option<bool>> {
    Ok(get_value_at_path(document, key)?.and_then(toml::Value::as_bool))
}

fn model_auth_configured(document: &toml::Value) -> Result<bool> {
    Ok(get_string_value_at_path(document, "model_provider.openai_api_key_vault_ref")?.is_some()
        || get_string_value_at_path(document, "model_provider.anthropic_api_key_vault_ref")?
            .is_some()
        || get_string_value_at_path(document, "model_provider.auth_profile_id")?.is_some()
        || registry_auth_configured(document))
}

fn configured_chat_model(document: &toml::Value) -> Result<Option<String>> {
    if let Some(model_id) =
        get_string_value_at_path(document, "model_provider.default_chat_model_id")?
    {
        return Ok(Some(model_id));
    }
    let provider_kind = get_string_value_at_path(document, "model_provider.kind")?
        .unwrap_or_else(|| "openai_compatible".to_owned());
    if provider_kind == "anthropic" {
        get_string_value_at_path(document, "model_provider.anthropic_model")
    } else {
        get_string_value_at_path(document, "model_provider.openai_model")
    }
}

fn configured_embeddings_model(document: &toml::Value) -> Result<Option<String>> {
    if let Some(model_id) =
        get_string_value_at_path(document, "model_provider.default_embeddings_model_id")?
    {
        return Ok(Some(model_id));
    }
    get_string_value_at_path(document, "model_provider.openai_embeddings_model")
}

fn registry_vault_ref_auth_configured(document: &toml::Value) -> bool {
    registry_auth_field_configured(document, "api_key_vault_ref")
}

fn registry_auth_profile_configured(document: &toml::Value) -> bool {
    registry_auth_field_configured(document, "auth_profile_id")
}

fn registry_auth_configured(document: &toml::Value) -> bool {
    registry_vault_ref_auth_configured(document) || registry_auth_profile_configured(document)
}

fn registry_auth_field_configured(document: &toml::Value, field: &str) -> bool {
    let providers = get_value_at_path(document, "model_provider.providers")
        .ok()
        .flatten()
        .and_then(|value| value.as_array());
    providers.is_some_and(|providers| {
        providers.iter().any(|provider| {
            let Some(table) = provider.as_table() else {
                return false;
            };
            table
                .get(field)
                .and_then(toml::Value::as_str)
                .is_some_and(|value| !value.trim().is_empty())
        })
    })
}

fn join_section_state(values: &[String]) -> String {
    if values.is_empty() {
        "none".to_owned()
    } else {
        values.join(" | ")
    }
}

/// Removes duplicates in place while preserving first-occurrence order.
fn dedupe_strings(values: &mut Vec<String>) {
    let mut deduped = Vec::with_capacity(values.len());
    for value in values.drain(..) {
        if !deduped.contains(&value) {
            deduped.push(value);
        }
    }
    *values = deduped;
}

fn describe_configure_section(
    document: &toml::Value,
    section: ConfigureSectionArg,
) -> Result<Vec<String>> {
    match section {
        ConfigureSectionArg::DeploymentProfile => Ok(vec![
            format!(
                "deployment_profile={}",
                get_string_value_at_path(document, "deployment.profile")?
                    .unwrap_or_else(|| "derived".to_owned())
            ),
            format!(
                "deployment_mode={}",
                get_string_value_at_path(document, "deployment.mode")?
                    .unwrap_or_else(|| "unset".to_owned())
            ),
            format!(
                "networked_workers_rollout={}",
                get_bool_value_at_path(document, "feature_rollouts.networked_workers")?
                    .unwrap_or(false)
            ),
        ]),
        ConfigureSectionArg::Workspace => Ok(vec![format!(
            "workspace_root={}",
            get_string_value_at_path(document, "tool_call.process_runner.workspace_root")?
                .unwrap_or_else(|| "unset".to_owned())
        )]),
        ConfigureSectionArg::AuthModel => {
            let provider_kind = get_string_value_at_path(document, "model_provider.kind")?
                .unwrap_or_else(|| "unset".to_owned());
            let auth_provider_kind =
                get_string_value_at_path(document, "model_provider.auth_provider_kind")?;
            let provider_display_name =
                provider_display_name(provider_kind.as_str(), auth_provider_kind.as_deref());
            let protocol_compatibility =
                configure_provider_protocol_compatibility(provider_kind.as_str());
            let auth_source =
                if get_string_value_at_path(document, "model_provider.openai_api_key_vault_ref")?
                    .is_some()
                    || get_string_value_at_path(
                        document,
                        "model_provider.anthropic_api_key_vault_ref",
                    )?
                    .is_some()
                    || registry_vault_ref_auth_configured(document)
                {
                    "vault_ref".to_owned()
                } else if get_string_value_at_path(document, "model_provider.auth_profile_id")?
                    .is_some()
                    || registry_auth_profile_configured(document)
                {
                    "auth_profile".to_owned()
                } else {
                    "unset".to_owned()
                };
            Ok(vec![
                format!("provider_display_name={provider_display_name}"),
                format!("protocol_compatibility={protocol_compatibility}"),
                format!("provider_kind={provider_kind}"),
                format!("auth_source={auth_source}"),
                format!(
                    "chat_model={}",
                    configured_chat_model(document)?.unwrap_or_else(|| "unset".to_owned())
                ),
                format!(
                    "embeddings_model={}",
                    configured_embeddings_model(document)?.unwrap_or_else(|| "unset".to_owned())
                ),
            ])
        }
        ConfigureSectionArg::Gateway => {
            let remote_verification = if get_string_value_at_path(
                document,
                "gateway_access.pinned_server_cert_fingerprint_sha256",
            )?
            .is_some()
            {
                "server_cert"
            } else if get_string_value_at_path(
                document,
                "gateway_access.pinned_gateway_ca_fingerprint_sha256",
            )?
            .is_some()
            {
                "gateway_ca"
            } else {
                "none"
            };
            Ok(vec![
                format!(
                    "bind_profile={}",
                    get_string_value_at_path(document, "gateway.bind_profile")?
                        .unwrap_or_else(|| "unset".to_owned())
                ),
                format!(
                    "daemon_port={}",
                    get_integer_value_at_path(document, "daemon.port")?
                        .map(|value| value.to_string())
                        .unwrap_or_else(|| "unset".to_owned())
                ),
                format!(
                    "grpc_port={}",
                    get_integer_value_at_path(document, "gateway.grpc_port")?
                        .map(|value| value.to_string())
                        .unwrap_or_else(|| "unset".to_owned())
                ),
                format!(
                    "quic_port={}",
                    get_integer_value_at_path(document, "gateway.quic_port")?
                        .map(|value| value.to_string())
                        .unwrap_or_else(|| "unset".to_owned())
                ),
                format!(
                    "tls_enabled={}",
                    get_bool_value_at_path(document, "gateway.tls.enabled")?.unwrap_or(false)
                ),
                format!(
                    "remote_base_url={}",
                    get_string_value_at_path(document, "gateway_access.remote_base_url")?
                        .unwrap_or_else(|| "none".to_owned())
                ),
                format!("remote_verification={remote_verification}"),
            ])
        }
        ConfigureSectionArg::RuntimeControls => ALL_RUNTIME_PREVIEW_CAPABILITIES
            .into_iter()
            .map(|capability| {
                Ok(format!(
                    "{}={}/rollout:{}",
                    capability.as_str(),
                    runtime_preview_mode_for_document(document, capability)?.as_str(),
                    if runtime_preview_rollout_for_document(document, capability)? {
                        "on"
                    } else {
                        "off"
                    }
                ))
            })
            .collect(),
        ConfigureSectionArg::DaemonService => Ok(vec![
            format!(
                "deployment_mode={}",
                get_string_value_at_path(document, "deployment.mode")?
                    .unwrap_or_else(|| "unset".to_owned())
            ),
            "service_install=available_via_gateway_commands".to_owned(),
        ]),
        ConfigureSectionArg::Channels => Ok(vec![
            format!(
                "channel_router_enabled={}",
                get_bool_value_at_path(document, "channel_router.enabled")?.unwrap_or(false)
            ),
            "discord_setup=manual_follow_up".to_owned(),
        ]),
        ConfigureSectionArg::Skills => {
            let snapshot = build_default_skills_inventory_snapshot();
            Ok(vec![
                format!(
                    "skills_trust_store={}",
                    env::var("PALYRA_SKILLS_TRUST_STORE").unwrap_or_else(|_| "default".to_owned())
                ),
                format!("installed_total={}", snapshot.installed_total),
                format!("eligible_total={}", snapshot.eligible_total),
                format!("quarantined_total={}", snapshot.quarantined_total),
            ])
        }
        ConfigureSectionArg::HealthSecurity => Ok(vec![
            format!(
                "admin_auth_required={}",
                get_bool_value_at_path(document, "admin.require_auth")?.unwrap_or(false)
            ),
            format!("model_auth_configured={}", model_auth_configured(document)?),
            format!(
                "dangerous_remote_bind_ack={}",
                get_bool_value_at_path(document, "deployment.dangerous_remote_bind_ack")?
                    .unwrap_or(false)
            ),
        ]),
    }
}

fn configure_provider_protocol_compatibility(provider_kind: &str) -> &'static str {
    match provider_kind {
        "openai_compatible" => "openai_compatible",
        "anthropic" => "anthropic_compatible",
        "deterministic" => "deterministic",
        "unset" => "unset",
        _ => "unknown",
    }
}

fn section_requires_restart(section: ConfigureSectionArg, changed: bool) -> bool {
    changed
        && matches!(
            section,
            ConfigureSectionArg::DeploymentProfile
                | ConfigureSectionArg::Workspace
                | ConfigureSectionArg::Gateway
                | ConfigureSectionArg::RuntimeControls
        )
}

fn section_follow_up_checks(
    section: ConfigureSectionArg,
    document: &toml::Value,
) -> Result<Vec<String>> {
    let mut follow_ups = match section {
        ConfigureSectionArg::DeploymentProfile => {
            let profile = get_string_value_at_path(document, "deployment.profile")?
                .unwrap_or_else(|| "local".to_owned());
            vec![
                format!("palyra deployment preflight --deployment-profile {profile}"),
                format!("palyra deployment recipe --deployment-profile {profile} --output-dir ./artifacts/deploy"),
                gateway_restart_follow_up("deployment profile changes require runtime reload"),
            ]
        }
        ConfigureSectionArg::Workspace => {
            vec![gateway_restart_follow_up(
                "workspace-root changes require runtime reload for existing daemon processes",
            )]
        }
        ConfigureSectionArg::AuthModel => {
            vec!["palyra doctor".to_owned(), "palyra models status".to_owned()]
        }
        ConfigureSectionArg::Gateway => {
            let mut values = vec!["palyra gateway status".to_owned()];
            if get_string_value_at_path(document, "gateway_access.remote_base_url")?.is_some() {
                values.push("palyra dashboard --verify-remote".to_owned());
                values.push("palyra gateway discover --verify-remote".to_owned());
                values.push(
                    "palyra support-bundle export --output ./artifacts/palyra-support-bundle.zip"
                        .to_owned(),
                );
            }
            values
        }
        ConfigureSectionArg::RuntimeControls => vec![
            "palyra doctor".to_owned(),
            "palyra gateway status".to_owned(),
            gateway_restart_follow_up("runtime control changes require runtime reload"),
        ],
        ConfigureSectionArg::DaemonService => {
            vec!["palyra gateway install --start".to_owned(), "palyra gateway status".to_owned()]
        }
        ConfigureSectionArg::Channels => vec!["palyra channels discord setup".to_owned()],
        ConfigureSectionArg::Skills => {
            vec![
                "palyra skills list --eligible-only".to_owned(),
                "palyra skills check".to_owned(),
                "palyra skills info <skill-id>".to_owned(),
            ]
        }
        ConfigureSectionArg::HealthSecurity => {
            vec!["palyra doctor".to_owned(), "palyra security audit".to_owned()]
        }
    };
    dedupe_strings(&mut follow_ups);
    Ok(follow_ups)
}

fn gateway_restart_follow_up(reason: &str) -> String {
    format!(
        "{reason}: use `palyra gateway restart` only for a gateway installed with `palyra gateway install`; for foreground or desktop-managed local runtimes, restart the owning terminal, desktop app, or test harness launcher."
    )
}

fn configure_runtime_controls(
    wizard: &mut WizardSession<'_, dyn WizardBackend>,
    document: &mut toml::Value,
) -> Result<()> {
    for capability in ALL_RUNTIME_PREVIEW_CAPABILITIES {
        let current_mode = runtime_preview_mode_for_document(document, capability)?;
        let current_rollout = runtime_preview_rollout_for_document(document, capability)?;
        let selection = wizard.select(select_step(
            runtime_preview_step_id(capability),
            capability.label(),
            format!(
                "{} Current state: mode={} | rollout={}.",
                capability.summary(),
                current_mode.as_str(),
                if current_rollout { "enabled" } else { "disabled" }
            ),
            vec![
                choice(
                    "keep_current",
                    "Keep Current",
                    Some("leave the current mode and rollout flag unchanged"),
                ),
                choice(
                    "disabled",
                    "Disabled",
                    Some("disable the capability and clear its rollout flag"),
                ),
                choice(
                    "preview_only",
                    "Preview Only",
                    Some("keep preview mode active with rollout disabled"),
                ),
                choice(
                    "preview_only_rollout",
                    "Preview + Rollout",
                    Some("keep preview mode active and arm its rollout flag"),
                ),
                choice(
                    "enabled",
                    "Enabled",
                    Some("enable the capability and set its rollout flag"),
                ),
            ],
            Some("keep_current".to_owned()),
        ))?;
        apply_runtime_control_choice(document, capability, selection.as_str())?;
    }
    Ok(())
}

fn apply_runtime_control_choice(
    document: &mut toml::Value,
    capability: RuntimePreviewCapability,
    choice_value: &str,
) -> Result<()> {
    match choice_value {
        "keep_current" => Ok(()),
        "disabled" => {
            set_runtime_control_state(document, capability, RuntimePreviewMode::Disabled, false)
        }
        "preview_only" => {
            set_runtime_control_state(document, capability, RuntimePreviewMode::PreviewOnly, false)
        }
        "preview_only_rollout" => {
            set_runtime_control_state(document, capability, RuntimePreviewMode::PreviewOnly, true)
        }
        "enabled" => {
            set_runtime_control_state(document, capability, RuntimePreviewMode::Enabled, true)
        }
        _ => anyhow::bail!(
            "unsupported runtime-control selection for {}: {choice_value}",
            capability.as_str()
        ),
    }
}

fn set_runtime_control_state(
    document: &mut toml::Value,
    capability: RuntimePreviewCapability,
    mode: RuntimePreviewMode,
    rollout_enabled: bool,
) -> Result<()> {
    set_value_at_path(
        document,
        runtime_preview_mode_path(capability).as_str(),
        toml::Value::String(mode.as_str().to_owned()),
    )?;
    set_value_at_path(
        document,
        runtime_preview_rollout_path(capability).as_str(),
        toml::Value::Boolean(rollout_enabled),
    )?;
    Ok(())
}

fn runtime_preview_mode_for_document(
    document: &toml::Value,
    capability: RuntimePreviewCapability,
) -> Result<RuntimePreviewMode> {
    let mode_path = runtime_preview_mode_path(capability);
    let Some(value) = get_string_value_at_path(document, mode_path.as_str())? else {
        return Ok(runtime_preview_default_mode(capability));
    };
    RuntimePreviewMode::parse(value.as_str()).ok_or_else(|| {
        anyhow::anyhow!(
            "{} must be one of disabled, preview_only, or enabled; got '{}'",
            mode_path,
            value
        )
    })
}

fn runtime_preview_rollout_for_document(
    document: &toml::Value,
    capability: RuntimePreviewCapability,
) -> Result<bool> {
    Ok(get_bool_value_at_path(document, runtime_preview_rollout_path(capability).as_str())?
        .unwrap_or(false))
}

fn runtime_preview_default_mode(capability: RuntimePreviewCapability) -> RuntimePreviewMode {
    match capability {
        RuntimePreviewCapability::SessionQueuePolicy
        | RuntimePreviewCapability::PruningPolicyMatrix
        | RuntimePreviewCapability::RetrievalDualPath
        | RuntimePreviewCapability::AuxiliaryExecutor
        | RuntimePreviewCapability::FlowOrchestration
        | RuntimePreviewCapability::ReplayCapture => RuntimePreviewMode::PreviewOnly,
        RuntimePreviewCapability::DeliveryArbitration
        | RuntimePreviewCapability::NetworkedWorkers => RuntimePreviewMode::Disabled,
    }
}

fn runtime_preview_mode_path(capability: RuntimePreviewCapability) -> String {
    format!("{}.mode", capability.as_str())
}

fn runtime_preview_rollout_path(capability: RuntimePreviewCapability) -> String {
    format!("feature_rollouts.{}", capability.as_str())
}

fn runtime_preview_step_id(capability: RuntimePreviewCapability) -> &'static str {
    match capability {
        RuntimePreviewCapability::SessionQueuePolicy => "runtime_controls_session_queue_policy",
        RuntimePreviewCapability::PruningPolicyMatrix => "runtime_controls_pruning_policy_matrix",
        RuntimePreviewCapability::RetrievalDualPath => "runtime_controls_retrieval_dual_path",
        RuntimePreviewCapability::AuxiliaryExecutor => "runtime_controls_auxiliary_executor",
        RuntimePreviewCapability::FlowOrchestration => "runtime_controls_flow_orchestration",
        RuntimePreviewCapability::DeliveryArbitration => "runtime_controls_delivery_arbitration",
        RuntimePreviewCapability::ReplayCapture => "runtime_controls_replay_capture",
        RuntimePreviewCapability::NetworkedWorkers => "runtime_controls_networked_workers",
    }
}

/// Derives the wizard's default auth-method choice from the existing provider config.
fn current_auth_method(document: &toml::Value) -> String {
    let provider_kind = get_string_value_at_path(document, "model_provider.kind")
        .ok()
        .flatten()
        .unwrap_or_else(|| "openai_compatible".to_owned());
    if let Some(method) = current_registry_auth_method(document) {
        return method.to_owned();
    }
    if get_string_value_at_path(document, "model_provider.auth_profile_id").ok().flatten().is_some()
    {
        return "existing_config".to_owned();
    }
    let auth_provider_kind =
        get_string_value_at_path(document, "model_provider.auth_provider_kind").ok().flatten();
    if get_string_value_at_path(document, "model_provider.anthropic_api_key_vault_ref")
        .ok()
        .flatten()
        .is_some()
    {
        if provider_kind == "anthropic"
            && auth_provider_kind
                .as_deref()
                .is_some_and(|kind| kind.eq_ignore_ascii_case(MINIMAX_AUTH_PROVIDER_KIND))
        {
            return "minimax_api_key_global".to_owned();
        }
        return if provider_kind == "anthropic" {
            "anthropic_api_key".to_owned()
        } else {
            "existing_config".to_owned()
        };
    }
    if get_string_value_at_path(document, "model_provider.openai_api_key_vault_ref")
        .ok()
        .flatten()
        .is_some()
    {
        return if provider_kind == "openai_compatible" {
            "api_key".to_owned()
        } else {
            "existing_config".to_owned()
        };
    }
    if provider_kind == "anthropic"
        && auth_provider_kind
            .as_deref()
            .is_some_and(|kind| kind.eq_ignore_ascii_case(MINIMAX_AUTH_PROVIDER_KIND))
    {
        return "minimax_api_key_global".to_owned();
    }
    if provider_kind == "anthropic" {
        "anthropic_api_key".to_owned()
    } else {
        "api_key".to_owned()
    }
}

fn current_registry_auth_method(document: &toml::Value) -> Option<&'static str> {
    let providers =
        get_value_at_path(document, "model_provider.providers").ok().flatten()?.as_array()?;
    for provider in providers {
        let Some(table) = provider.as_table() else {
            continue;
        };
        let Some(auth_provider_kind) =
            table.get("auth_provider_kind").and_then(toml::Value::as_str)
        else {
            continue;
        };
        let has_api_key = table
            .get("api_key_vault_ref")
            .and_then(toml::Value::as_str)
            .is_some_and(|value| !value.trim().is_empty());
        if !has_api_key {
            continue;
        }
        if auth_provider_kind.eq_ignore_ascii_case(XAI_AUTH_PROVIDER_KIND) {
            return Some("xai_api_key");
        }
        if auth_provider_kind.eq_ignore_ascii_case(GOOGLE_GEMINI_AUTH_PROVIDER_KIND) {
            return Some("google_gemini_api_key");
        }
        if auth_provider_kind.eq_ignore_ascii_case(OPENROUTER_AUTH_PROVIDER_KIND) {
            return Some("openrouter_api_key");
        }
    }
    None
}

fn auth_method_value(value: OnboardingAuthMethodArg) -> String {
    match value {
        OnboardingAuthMethodArg::ChatgptLogin => "chatgpt_login",
        OnboardingAuthMethodArg::ApiKey => "api_key",
        OnboardingAuthMethodArg::AnthropicApiKey => "anthropic_api_key",
        OnboardingAuthMethodArg::AnthropicOauth => "anthropic_oauth",
        OnboardingAuthMethodArg::MinimaxApiKey => "minimax_api_key",
        OnboardingAuthMethodArg::MinimaxApiKeyGlobal => "minimax_api_key_global",
        OnboardingAuthMethodArg::MinimaxApiKeyCn => "minimax_api_key_cn",
        OnboardingAuthMethodArg::MinimaxOauthGlobal => "minimax_oauth_global",
        OnboardingAuthMethodArg::MinimaxOauthCn => "minimax_oauth_cn",
        OnboardingAuthMethodArg::XaiApiKey => "xai_api_key",
        OnboardingAuthMethodArg::XaiDeviceCode => "xai_device_code",
        OnboardingAuthMethodArg::XaiOauth => "xai_oauth",
        OnboardingAuthMethodArg::GeminiCliOauth => "gemini_cli_oauth",
        OnboardingAuthMethodArg::GoogleGeminiApiKey => "google_gemini_api_key",
        OnboardingAuthMethodArg::OpenrouterApiKey => "openrouter_api_key",
        OnboardingAuthMethodArg::OpenrouterOauth => "openrouter_oauth",
        OnboardingAuthMethodArg::Skip => "skip",
        OnboardingAuthMethodArg::ExistingConfig => "existing_config",
    }
    .to_owned()
}

fn deployment_profile_value(value: DeploymentProfileArg) -> &'static str {
    deployment_profile_id_from_arg(value).as_str()
}

fn bind_profile_value(value: GatewayBindProfileArg) -> &'static str {
    match value {
        GatewayBindProfileArg::LoopbackOnly => "loopback_only",
        GatewayBindProfileArg::PublicTls => "public_tls",
    }
}

fn tls_scaffold_value(value: InitTlsScaffoldArg) -> &'static str {
    match value {
        InitTlsScaffoldArg::None => "none",
        InitTlsScaffoldArg::BringYourOwn => "bring-your-own",
        InitTlsScaffoldArg::SelfSigned => "self-signed",
    }
}

fn remote_verification_value(value: RemoteVerificationModeArg) -> &'static str {
    match value {
        RemoteVerificationModeArg::None => "none",
        RemoteVerificationModeArg::ServerCert => "server_cert",
        RemoteVerificationModeArg::GatewayCa => "gateway_ca",
    }
}

fn insert_optional_u16_answer(
    answers: &mut BTreeMap<String, WizardValue>,
    key: &str,
    value: Option<u16>,
) {
    if let Some(value) = value {
        answers.insert(key.to_owned(), WizardValue::Text(value.to_string()));
    }
}

fn choice(value: &str, label: &str, hint: Option<&str>) -> StepChoice {
    StepChoice {
        value: value.to_owned(),
        label: label.to_owned(),
        hint: hint.map(ToOwned::to_owned),
    }
}

fn text_step(
    id: &'static str,
    title: &'static str,
    message: &'static str,
    default_value: Option<String>,
    placeholder: Option<String>,
    sensitive: bool,
) -> WizardStep {
    WizardStep {
        id,
        kind: StepKind::Text,
        title: Some(title.to_owned()),
        message: message.to_owned(),
        default_value,
        placeholder,
        sensitive,
        allow_empty: false,
        options: Vec::new(),
    }
}

fn confirm_step(
    id: &'static str,
    title: &'static str,
    message: &'static str,
    default_value: Option<bool>,
) -> WizardStep {
    WizardStep {
        id,
        kind: StepKind::Confirm,
        title: Some(title.to_owned()),
        message: message.to_owned(),
        default_value: default_value.map(|value| value.to_string()),
        placeholder: None,
        sensitive: false,
        allow_empty: false,
        options: Vec::new(),
    }
}

fn select_step(
    id: &'static str,
    title: &'static str,
    message: impl Into<String>,
    options: Vec<StepChoice>,
    default_value: Option<String>,
) -> WizardStep {
    WizardStep {
        id,
        kind: StepKind::Select,
        title: Some(title.to_owned()),
        message: message.into(),
        default_value,
        placeholder: None,
        sensitive: false,
        allow_empty: false,
        options,
    }
}

fn multiselect_step(
    id: &'static str,
    title: &'static str,
    message: &'static str,
    options: Vec<StepChoice>,
    default_value: Option<String>,
) -> WizardStep {
    WizardStep {
        id,
        kind: StepKind::MultiSelect,
        title: Some(title.to_owned()),
        message: message.to_owned(),
        default_value,
        placeholder: None,
        sensitive: false,
        allow_empty: false,
        options,
    }
}

fn anyhow_from_wizard(error: WizardError) -> anyhow::Error {
    anyhow::anyhow!(error.to_string())
}

trait ConfigureSectionLabel {
    fn slug(self) -> &'static str;
}

impl ConfigureSectionLabel for ConfigureSectionArg {
    fn slug(self) -> &'static str {
        match self {
            ConfigureSectionArg::DeploymentProfile => "deployment-profile",
            ConfigureSectionArg::Workspace => "workspace",
            ConfigureSectionArg::AuthModel => "auth-model",
            ConfigureSectionArg::Gateway => "gateway",
            ConfigureSectionArg::RuntimeControls => "runtime-controls",
            ConfigureSectionArg::DaemonService => "daemon-service",
            ConfigureSectionArg::Channels => "channels",
            ConfigureSectionArg::Skills => "skills",
            ConfigureSectionArg::HealthSecurity => "health-security",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commands::wizard::{ScriptedWizardBackend, WizardValue};
    use std::{
        collections::VecDeque,
        io::{Read, Write},
        net::TcpListener,
        thread,
        time::{Duration, Instant},
    };

    #[test]
    fn http_fetch_credential_ref_backfill_preserves_existing_refs_without_duplicates() -> Result<()>
    {
        let (mut document, _) = parse_document_with_migration(
            r#"
version = 1

[tool_call.http_fetch]
allowed_credential_vault_refs = ["global/github_token", "global/github_token"]
"#,
        )?;

        ensure_http_fetch_credential_vault_ref(&mut document, "global/minimax_api_key")?;
        ensure_http_fetch_credential_vault_ref(&mut document, "global/minimax_api_key")?;

        let refs = get_value_at_path(&document, HTTP_FETCH_CREDENTIAL_VAULT_REFS_PATH)?
            .and_then(toml::Value::as_array)
            .context("HTTP fetch credential refs should be an array")?
            .iter()
            .map(|value| {
                value
                    .as_str()
                    .map(str::to_owned)
                    .context("HTTP fetch credential ref should be a string")
            })
            .collect::<Result<Vec<_>>>()?;
        assert_eq!(
            refs,
            vec!["global/github_token".to_owned(), "global/minimax_api_key".to_owned()]
        );
        Ok(())
    }

    #[test]
    fn build_onboarding_answers_prefills_skip_flags() {
        let request = OnboardingWizardRequest {
            path: None,
            force: true,
            setup_mode: Some(InitModeArg::Local),
            setup_tls_scaffold: Some(InitTlsScaffoldArg::BringYourOwn),
            options: WizardOverridesArg {
                flow: Some(OnboardingFlowArg::Quickstart),
                non_interactive: true,
                accept_risk: true,
                json: true,
                workspace_root: Some("workspace".to_owned()),
                auth_method: Some(OnboardingAuthMethodArg::ApiKey),
                api_key_env: None,
                api_key_stdin: false,
                api_key_prompt: false,
                deployment_profile: None,
                bind_profile: None,
                daemon_port: None,
                grpc_port: None,
                quic_port: None,
                tls_scaffold: None,
                tls_cert_path: None,
                tls_key_path: None,
                remote_base_url: None,
                admin_token_env: None,
                admin_token_stdin: false,
                admin_token_prompt: false,
                remote_verification: None,
                pinned_server_cert_sha256: None,
                pinned_gateway_ca_sha256: None,
                ssh_target: None,
                skip_health: true,
                skip_channels: true,
                skip_skills: true,
            },
        };
        let answers =
            build_onboarding_answers(&request, WizardFlowKind::Quickstart).expect("answers build");
        assert_eq!(
            answers.get("existing_config_action"),
            Some(&WizardValue::Choice("overwrite".to_owned()))
        );
        assert_eq!(answers.get("accept_risk_ack"), Some(&WizardValue::Bool(true)));
        assert_eq!(answers.get("configure_channels"), Some(&WizardValue::Bool(false)));
        assert_eq!(answers.get("configure_skills"), Some(&WizardValue::Bool(false)));
        assert_eq!(answers.get("run_health_checks"), Some(&WizardValue::Bool(false)));
    }

    #[test]
    fn stdin_secret_sources_require_non_interactive_mode() {
        let error = validate_stdin_secret_usage(false, true, false)
            .expect_err("stdin secret sources should require scripted mode");
        let message = format!("{error:#}");

        assert!(message.contains("--api-key-stdin"), "expected flag context in error: {message}");
        assert!(
            message.contains("--non-interactive"),
            "expected scripted mode guidance in error: {message}"
        );
        assert!(
            message.contains("--accept-risk"),
            "expected complete non-interactive wizard guidance in error: {message}"
        );
    }

    #[test]
    fn api_key_secret_inputs_are_rejected_for_deferred_auth_methods() {
        let error = validate_api_key_secret_matches_auth_method(Some("xai_oauth"), true)
            .expect_err("deferred OAuth methods must not consume API-key inputs");
        let message = format!("{error:#}");

        assert!(message.contains("xAI OAuth"), "expected auth method label in error: {message}");
        assert!(
            message.contains("--api-key-env"),
            "expected API-key source remediation in error: {message}"
        );
    }

    #[test]
    fn resolve_existing_config_action_uses_force_without_prompt() {
        let temp = tempfile::tempdir().expect("tempdir");
        let path = temp.path().join("palyra.toml");
        fs::write(path.as_path(), "version = 1\n").expect("seed config");
        let mut backend = ScriptedWizardBackend::new(BTreeMap::new(), true);
        let backend_ref: &mut dyn WizardBackend = &mut backend;
        let mut wizard = WizardSession::new(backend_ref);
        let action =
            resolve_existing_config_action(&mut wizard, true, path.as_path()).expect("action");
        assert_eq!(action, Some(ExistingConfigAction::Overwrite));
    }

    #[test]
    fn resolve_existing_config_action_ignores_empty_placeholder() {
        let temp = tempfile::tempdir().expect("tempdir");
        let path = temp.path().join("palyra.toml");
        fs::write(path.as_path(), "").expect("seed empty placeholder");
        let mut backend = ScriptedWizardBackend::new(BTreeMap::new(), true);
        let backend_ref: &mut dyn WizardBackend = &mut backend;
        let mut wizard = WizardSession::new(backend_ref);

        let action =
            resolve_existing_config_action(&mut wizard, false, path.as_path()).expect("action");

        assert_eq!(action, None);
    }

    #[test]
    fn apply_onboarding_plan_creates_new_secret_config_securely() {
        let temp = tempfile::tempdir().expect("tempdir");
        let config_path = temp.path().join("nested").join("palyra.toml");
        let context = ApplyContext {
            config_path: config_path.clone(),
            state_root: temp.path().join("state"),
            identity_store_dir: temp.path().join("identity"),
            vault_dir: temp.path().join("vault"),
            tls_paths: None,
        };
        let deployment_profile = palyra_common::deployment_profiles::DeploymentProfileId::SingleVm;
        let mut plan = OnboardingMutationPlan {
            flow: "remote".to_owned(),
            deployment_profile,
            deployment_mode: deployment_profile.deployment_mode().to_owned(),
            bind_profile: deployment_profile.bind_profile().to_owned(),
            auth_method: "remote_admin_token".to_owned(),
            admin_token: Some("admin-secret-test-token".to_owned()),
            remote_base_url: Some("https://dashboard.example.test/".to_owned()),
            remote_verification: Some("server_cert".to_owned()),
            pinned_server_cert_sha256: Some("a".repeat(64)),
            ..Default::default()
        };

        apply_onboarding_plan(&context, &mut plan)
            .expect("new remote onboarding config should be persisted securely");

        let written = fs::read_to_string(config_path.as_path()).expect("config should be readable");
        assert!(written.contains("auth_token = \"admin-secret-test-token\""));
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;

            let mode = fs::metadata(config_path.as_path())
                .expect("config metadata should be readable")
                .permissions()
                .mode()
                & 0o777;
            assert_eq!(mode, 0o600, "new token-bearing config should be owner-only");
        }
    }

    #[test]
    fn document_contains_inline_secret_detects_admin_token() {
        let mut document = toml::Value::Table(Default::default());
        assert!(
            !document_contains_inline_secret(&document).expect("secret scan should succeed"),
            "empty config should not require secret-bearing write mode"
        );

        set_value_at_path(
            &mut document,
            "admin.auth_token",
            toml::Value::String("admin-secret-test-token".to_owned()),
        )
        .expect("admin token should be set");

        assert!(
            document_contains_inline_secret(&document).expect("secret scan should succeed"),
            "inline admin tokens must force owner-only config persistence"
        );
    }

    #[test]
    fn admin_defaults_preserve_existing_auth_token_secret_ref() {
        let mut document: toml::Value = toml::from_str(
            r#"
[admin.auth_token_secret_ref]
kind = "env"
variable = "PALYRA_ADMIN_TOKEN"
"#,
        )
        .expect("secret-ref config should parse");

        ensure_admin_auth_defaults_with_token(&mut document, Some("generated-inline-token"))
            .expect("admin defaults should apply around secret refs");

        assert_eq!(
            get_string_value_at_path(&document, "admin.auth_token")
                .expect("auth token lookup should succeed"),
            None
        );
        assert!(
            get_value_at_path(&document, "admin.auth_token_secret_ref")
                .expect("secret ref lookup should succeed")
                .is_some(),
            "existing admin auth_token_secret_ref should remain configured"
        );
        assert_eq!(
            get_bool_value_at_path(&document, "admin.require_auth")
                .expect("require_auth lookup should succeed"),
            Some(true)
        );
    }

    #[test]
    fn clear_model_provider_auth_removes_structured_secret_refs() {
        let mut document: toml::Value = toml::from_str(
            r#"
[model_provider]
openai_api_key = "sk-old"
openai_api_key_vault_ref = "global/openai_old"
anthropic_api_key = "ant-old"
anthropic_api_key_vault_ref = "global/anthropic_old"
auth_profile_id = "provider.default"
auth_profile_ref = "provider.legacy"
auth_provider_kind = "minimax"
default_chat_model_id = "old-chat"
default_embeddings_model_id = "old-embeddings"
default_audio_transcription_model_id = "old-audio"
[model_provider.openai_api_key_secret_ref]
kind = "env"
variable = "PALYRA_MODEL_PROVIDER_OPENAI_API_KEY"
[model_provider.anthropic_api_key_secret_ref]
kind = "env"
variable = "PALYRA_MODEL_PROVIDER_ANTHROPIC_API_KEY"
[[model_provider.providers]]
provider_id = "old-provider"
kind = "openai_compatible"
api_key_vault_ref = "global/old_provider_key"
[[model_provider.models]]
model_id = "old-chat"
provider_id = "old-provider"
role = "chat"
"#,
        )
        .expect("model provider config should parse");

        clear_model_provider_auth(&mut document).expect("model provider auth should clear");

        for path in [
            "model_provider.openai_api_key",
            "model_provider.openai_api_key_secret_ref",
            "model_provider.openai_api_key_vault_ref",
            "model_provider.anthropic_api_key",
            "model_provider.anthropic_api_key_secret_ref",
            "model_provider.anthropic_api_key_vault_ref",
            "model_provider.auth_profile_id",
            "model_provider.auth_profile_ref",
            "model_provider.auth_provider_kind",
            "model_provider.providers",
            "model_provider.models",
            "model_provider.default_chat_model_id",
            "model_provider.default_embeddings_model_id",
            "model_provider.default_audio_transcription_model_id",
        ] {
            assert!(
                get_value_at_path(&document, path)
                    .expect("config path lookup should succeed")
                    .is_none(),
                "{path} should be removed"
            );
        }
    }

    #[test]
    fn model_provider_auth_choices_include_requested_provider_options() {
        let choices = model_provider_auth_choices();
        for (value, label, hint) in [
            ("chatgpt_login", "ChatGPT Login", "Sign in with your ChatGPT or Codex subscription"),
            ("api_key", "OpenAI API Key", "Use your OpenAI API key directly"),
            ("anthropic_api_key", "Anthropic API key", "Use your Anthropic API key directly"),
            ("anthropic_oauth", "Anthropic OAuth", "Use an Anthropic OAuth auth profile"),
            ("minimax_api_key_cn", "MiniMax API key (CN)", "CN endpoint - api.minimaxi.com"),
            (
                "minimax_api_key_global",
                "MiniMax API key (Global)",
                "Global endpoint - api.minimax.io",
            ),
            ("minimax_oauth_cn", "MiniMax OAuth (CN)", "CN endpoint - api.minimaxi.com"),
            ("minimax_oauth_global", "MiniMax OAuth (Global)", "Global endpoint - api.minimax.io"),
            ("xai_api_key", "xAI API key", "Use your xAI Grok API key directly"),
            ("xai_device_code", "xAI device code", "Use an xAI device-code auth profile"),
            ("xai_oauth", "xAI OAuth", "Use an xAI OAuth auth profile"),
            (
                "gemini_cli_oauth",
                "Gemini CLI OAuth",
                "Google OAuth with project-aware token payload",
            ),
            (
                "google_gemini_api_key",
                "Google Gemini API key",
                "Use your Google Gemini API key directly",
            ),
            ("openrouter_api_key", "OpenRouter API key", "Use your OpenRouter API key directly"),
            ("openrouter_oauth", "OpenRouter OAuth", "Use an OpenRouter OAuth auth profile"),
        ] {
            assert!(
                choices.iter().any(|choice| {
                    choice.value == value
                        && choice.label == label
                        && choice.hint.as_deref() == Some(hint)
                }),
                "missing auth choice value={value} label={label}"
            );
        }
    }

    #[test]
    fn deferred_xai_auth_method_does_not_write_hardcoded_model() {
        let mut document = toml::Value::Table(Default::default());
        let mut warnings = Vec::new();

        apply_deferred_provider_auth_method(&mut document, "xai_oauth", &mut warnings)
            .expect("xAI OAuth defaults should apply");

        assert_eq!(
            get_string_value_at_path(&document, "model_provider.auth_provider_kind")
                .expect("auth provider lookup should succeed")
                .as_deref(),
            Some("xai")
        );
        assert!(
            get_string_value_at_path(&document, "model_provider.default_chat_model_id")
                .expect("default chat lookup should succeed")
                .is_none(),
            "deferred auth must wait for provider discovery before selecting a model"
        );
        assert!(
            get_string_value_at_path(&document, "model_provider.openai_model")
                .expect("OpenAI model lookup should succeed")
                .is_none(),
            "deferred auth must not write a flat OpenAI-compatible model fallback"
        );
        assert!(
            get_value_at_path(&document, "model_provider.models")
                .expect("models lookup should succeed")
                .is_none(),
            "deferred auth must not write a registry model before provider discovery"
        );
        let providers = get_value_at_path(&document, "model_provider.providers")
            .expect("providers lookup should succeed")
            .and_then(toml::Value::as_array)
            .expect("registry providers should be written");
        let provider = providers[0].as_table().expect("provider should be a table");
        assert_eq!(provider.get("provider_id").and_then(toml::Value::as_str), Some("xai-primary"));
        assert!(
            provider.get("api_key_vault_ref").is_none(),
            "deferred auth profile methods must not fabricate an API-key vault ref"
        );
        validate_daemon_compatible_document(&document)
            .expect("deferred xAI auth document should remain daemon-compatible");
        assert!(
            warnings.iter().any(|warning| warning.contains("xAI OAuth")),
            "deferred method should emit an actionable auth-profile warning: {warnings:?}"
        );
    }

    #[test]
    fn openai_api_key_base_url_ignores_config_and_env_overrides() {
        let document: toml::Value = toml::from_str(
            r#"
[model_provider]
kind = "openai_compatible"
auth_provider_kind = "openai"
openai_base_url = "https://chatgpt.com/backend-api/codex"
"#,
        )
        .expect("test config should parse");

        assert_eq!(
            openai_base_url_for_config_with_override(&document, None)
                .expect("OpenAI API key base URL should resolve"),
            OPENAI_DEFAULT_BASE_URL
        );
        assert_eq!(
            openai_base_url_for_config_with_override(
                &document,
                Some("http://127.0.0.1:9876/v1".to_owned())
            )
            .expect("explicit OpenAI API key base URL override should resolve"),
            OPENAI_DEFAULT_BASE_URL
        );
    }

    #[test]
    fn anthropic_api_key_base_url_ignores_config_and_env_overrides() {
        let document: toml::Value = toml::from_str(
            r#"
[model_provider]
kind = "anthropic"
anthropic_base_url = "https://attacker.example/v1"
"#,
        )
        .expect("test config should parse");

        assert_eq!(
            anthropic_base_url_for_config_with_override(&document, None)
                .expect("Anthropic API key base URL should resolve"),
            ANTHROPIC_DEFAULT_BASE_URL
        );
        assert_eq!(
            anthropic_base_url_for_config_with_override(
                &document,
                Some("http://127.0.0.1:9876/v1".to_owned())
            )
            .expect("explicit Anthropic API key base URL override should resolve"),
            ANTHROPIC_DEFAULT_BASE_URL
        );
    }

    #[test]
    fn minimax_discovery_base_url_allows_only_official_hosts() {
        assert_eq!(
            normalize_minimax_discovery_base_url(
                " https://api.minimax.io/anthropic/ ",
                "selected MiniMax endpoint"
            )
            .expect("global MiniMax endpoint should be trusted"),
            DEFAULT_MINIMAX_BASE_URL
        );
        assert_eq!(
            normalize_minimax_discovery_base_url(
                DEFAULT_MINIMAX_CN_BASE_URL,
                "selected MiniMax endpoint"
            )
            .expect("CN MiniMax endpoint should be trusted"),
            DEFAULT_MINIMAX_CN_BASE_URL
        );

        for base_url in ["http://127.0.0.1:9876/anthropic", "https://attacker.example/anthropic"] {
            let error = normalize_minimax_discovery_base_url(base_url, MINIMAX_BASE_URL_ENV)
                .expect_err("custom MiniMax discovery hosts must be rejected");
            let message = format!("{error:#}");
            assert!(
                message.contains("official MiniMax endpoints")
                    && message.contains(MINIMAX_BASE_URL_ENV),
                "unexpected MiniMax discovery rejection: {message}"
            );
        }
    }

    #[test]
    fn openai_api_model_selection_prefers_curated_non_pro_over_newer_snapshot() {
        let models = parse_discovered_provider_models(
            r#"{"data":[{"id":"gpt-5.5-pro-2026-04-23","created":1778012060},{"id":"chat-latest","created":1777704602},{"id":"gpt-4o","created":1777600000},{"id":"gpt-5.5","created":1700000000}]}"#,
        )
        .expect("OpenAI discovery fixture should parse");

        let selected = select_openai_api_preferred_model(models.as_slice())
            .expect("OpenAI curated chat model should be selected");

        assert_eq!(selected.id, "gpt-5.5");
    }

    #[test]
    fn openai_api_model_selection_waits_when_discovery_has_no_chat_signal() {
        let models = parse_discovered_provider_models(
            r#"{"data":[{"id":"gpt-realtime-whisper","created":1778012060},{"id":"gpt-image-2","created":1776399795}]}"#,
        )
        .expect("OpenAI discovery fixture should parse");

        assert!(select_openai_api_preferred_model(models.as_slice()).is_none());
    }

    #[test]
    fn openai_api_model_selection_rejects_pro_and_dated_snapshot_defaults() {
        let models = parse_discovered_provider_models(
            r#"{"data":[{"id":"gpt-5.5-pro","created":1778012060},{"id":"gpt-5.4-pro-2026-04-23","created":1777704602},{"id":"gpt-4.1-2026-01-01","created":1777600000},{"id":"chat-latest","created":1777500000}]}"#,
        )
        .expect("OpenAI discovery fixture should parse");

        assert!(select_openai_api_preferred_model(models.as_slice()).is_none());
    }

    #[test]
    fn deferred_chatgpt_auth_method_points_to_public_oauth_command() {
        let mut document = toml::Value::Table(Default::default());
        let mut warnings = Vec::new();

        apply_deferred_provider_auth_method(&mut document, "chatgpt_login", &mut warnings)
            .expect("ChatGPT Login defaults should apply");

        assert_eq!(
            get_string_value_at_path(&document, "model_provider.openai_base_url")
                .expect("OpenAI base URL lookup should succeed")
                .as_deref(),
            Some("https://api.openai.com/v1")
        );
        assert!(
            get_string_value_at_path(&document, "model_provider.openai_model")
                .expect("OpenAI model lookup should succeed")
                .is_none(),
            "ChatGPT OAuth setup must wait for Codex model discovery before selecting a model"
        );
        assert!(
            get_string_value_at_path(&document, "model_provider.default_chat_model_id")
                .expect("default chat lookup should succeed")
                .is_none(),
            "ChatGPT OAuth setup must not write a registry model fallback"
        );
        validate_daemon_compatible_document(&document)
            .expect("deferred ChatGPT auth document should remain daemon-compatible");
        assert!(
            warnings.iter().any(|warning| {
                warning.contains("palyra auth openai oauth-start --set-default --open")
                    && warning.contains("oauth-state <attempt_id>")
            }),
            "ChatGPT warning should point to the public OAuth command: {warnings:?}"
        );
    }

    #[cfg(unix)]
    #[test]
    fn write_operator_document_with_backups_tightens_existing_secret_config_permissions() {
        use std::os::unix::fs::PermissionsExt;

        let temp = tempfile::tempdir().expect("tempdir");
        let config_path = temp.path().join("palyra.toml");
        fs::write(config_path.as_path(), "version = 1\n").expect("seed config");
        fs::set_permissions(config_path.as_path(), fs::Permissions::from_mode(0o644))
            .expect("seed config permissions");
        let mut document = toml::Value::Table(Default::default());
        set_value_at_path(
            &mut document,
            "admin.auth_token",
            toml::Value::String("admin-secret-test-token".to_owned()),
        )
        .expect("admin token should be set");

        write_operator_document_with_backups(config_path.as_path(), &document)
            .expect("secret-bearing config should be persisted securely");

        let mode = fs::metadata(config_path.as_path())
            .expect("config metadata should be readable")
            .permissions()
            .mode()
            & 0o777;
        let backup_mode = fs::metadata(backup_path(config_path.as_path(), 1))
            .expect("backup metadata should be readable")
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(mode, 0o600, "secret-bearing config should be owner-only");
        assert_eq!(backup_mode, 0o600, "backup should be tightened before rotation");
    }

    #[test]
    fn service_install_failure_is_deferred_instead_of_failing_onboarding() {
        let mut plan = OnboardingMutationPlan {
            service_install_mode: ServiceInstallMode::InstallNow,
            risk_events: vec!["service_install_requested".to_owned()],
            ..Default::default()
        };

        record_service_install_failure(&mut plan, &anyhow::anyhow!("scheduled task denied"));

        assert_eq!(plan.service_install_mode, ServiceInstallMode::InstallFailedDeferred);
        assert!(plan
            .risk_events
            .iter()
            .any(|event| event == "service_install_deferred_after_failure"));
        assert!(
            plan.warnings.iter().any(|warning| {
                warning.contains("background gateway service install failed")
                    && warning.contains("palyra gateway run")
                    && warning.contains("palyra gateway install --start")
            }),
            "expected actionable service install warning: {:?}",
            plan.warnings
        );
    }

    #[test]
    fn onboarding_summary_guides_foreground_restart_when_gateway_is_already_running() {
        let (status, recommended_step_id, next_step) = onboarding_summary_next_step(
            WizardFlowKind::Quickstart,
            ServiceInstallMode::NotNow,
            HealthStatus::RuntimeRestartRequired,
            "minimax-api-key",
        );

        assert_eq!(status, "configured_runtime_restart_required");
        assert_eq!(recommended_step_id, Some("foreground_gateway_restart"));
        let next_step = next_step.expect("restart guidance should be present");
        assert!(
            next_step.contains("Stop the current foreground `palyra gateway run` process"),
            "next step should explain foreground restart path: {next_step}"
        );
        assert!(
            !next_step.starts_with("Restart the already running gateway with `palyra gateway restart`"),
            "foreground guidance should not present managed restart as the primary path: {next_step}"
        );
    }

    #[test]
    fn onboarding_summary_guides_managed_restart_after_service_install() {
        let (status, recommended_step_id, next_step) = onboarding_summary_next_step(
            WizardFlowKind::Quickstart,
            ServiceInstallMode::InstallNow,
            HealthStatus::RuntimeRestartRequired,
            "minimax-api-key",
        );

        assert_eq!(status, "configured_runtime_restart_required");
        assert_eq!(recommended_step_id, Some("gateway_restart"));
        let next_step = next_step.expect("managed restart guidance should be present");
        assert!(
            next_step.contains("palyra gateway restart"),
            "next step should explain managed restart path: {next_step}"
        );
    }

    #[test]
    fn running_gateway_restart_check_does_not_send_admin_token_to_health_responder() {
        let listener = TcpListener::bind(("127.0.0.1", 0)).expect("test listener should bind");
        let port = listener.local_addr().expect("listener address should resolve").port();
        listener.set_nonblocking(true).expect("listener should support nonblocking mode");
        let server = thread::spawn(move || {
            let mut requests = Vec::new();
            let mut deadline = Instant::now() + Duration::from_secs(3);
            while Instant::now() < deadline && requests.len() < 2 {
                match listener.accept() {
                    Ok((mut stream, _)) => {
                        let mut buffer = [0_u8; 4096];
                        let read = stream.read(&mut buffer).unwrap_or(0);
                        if read == 0 {
                            deadline = Instant::now() + Duration::from_millis(300);
                            continue;
                        }
                        let request = String::from_utf8_lossy(&buffer[..read]).to_string();
                        let body = if request.starts_with("GET /healthz ") {
                            r#"{"service":"palyrad","status":"ok","version":"test","git_hash":"test","build_profile":"test","uptime_seconds":1}"#
                        } else {
                            r#"{"ok":true}"#
                        };
                        let response = format!(
                            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                            body.len(),
                            body
                        );
                        stream.write_all(response.as_bytes()).expect("test response should write");
                        requests.push(request);
                        deadline = Instant::now() + Duration::from_millis(300);
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(10));
                    }
                    Err(error) => panic!("test listener failed: {error}"),
                }
            }
            requests
        });

        let temp = tempfile::tempdir().expect("tempdir");
        let config_path = temp.path().join("palyra.toml");
        fs::write(
            config_path.as_path(),
            format!(
                r#"
version = 1
[daemon]
bind_addr = "127.0.0.1"
port = {port}
"#
            ),
        )
        .expect("config should be written");
        let document = toml::from_str::<toml::Value>(
            r#"
version = 1
[admin]
require_auth = true
auth_token = "poc-admin-token-should-not-leave-config"
bound_principal = "operator"
"#,
        )
        .expect("test document should parse");
        let context = ApplyContext {
            config_path,
            state_root: temp.path().join("state"),
            identity_store_dir: temp.path().join("identity"),
            vault_dir: temp.path().join("vault"),
            tls_paths: None,
        };

        let summary = running_gateway_restart_check(&context, &document, true)
            .expect("restart check should complete")
            .expect("health responder should produce restart check");

        assert_eq!(summary.check, "runtime_config_reload");
        assert_eq!(summary.status, "restart_required");
        let requests = server.join().expect("test server should finish");
        assert!(!requests.is_empty(), "restart check must call unauthenticated health");
        for request in &requests {
            assert!(request.starts_with("GET /healthz "), "unexpected request: {request}");
            assert!(
                !request.contains("Authorization: Bearer"),
                "admin token must not be sent to health responder: {request}"
            );
        }
    }

    #[test]
    fn select_configure_sections_prompts_interactively() {
        let mut scripted = BTreeMap::new();
        scripted.insert(
            "configure_sections".to_owned(),
            VecDeque::from([Ok(WizardValue::Multi(vec![
                "workspace".to_owned(),
                "auth-model".to_owned(),
            ]))]),
        );
        let mut backend = ScriptedWizardBackend::new(scripted, true);
        let backend_ref: &mut dyn WizardBackend = &mut backend;
        let mut wizard = WizardSession::new(backend_ref);
        let sections = select_configure_sections(
            &mut wizard,
            &ConfigureWizardRequest {
                path: None,
                sections: Vec::new(),
                deployment_profile: None,
                non_interactive: false,
                accept_risk: false,
                json: false,
                workspace_root: None,
                auth_method: None,
                api_key_env: None,
                api_key_stdin: false,
                api_key_prompt: false,
                bind_profile: None,
                daemon_port: None,
                grpc_port: None,
                quic_port: None,
                tls_scaffold: None,
                tls_cert_path: None,
                tls_key_path: None,
                remote_base_url: None,
                admin_token_env: None,
                admin_token_stdin: false,
                admin_token_prompt: false,
                remote_verification: None,
                pinned_server_cert_sha256: None,
                pinned_gateway_ca_sha256: None,
                ssh_target: None,
                skip_health: false,
                skip_channels: false,
                skip_skills: false,
            },
        )
        .expect("sections");
        assert_eq!(sections, vec![ConfigureSectionArg::Workspace, ConfigureSectionArg::AuthModel]);
    }
}
