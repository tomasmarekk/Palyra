use std::{
    collections::BTreeMap,
    io::IsTerminal,
    path::{Path, PathBuf},
};

use palyra_common::runtime_preview::{
    RuntimePreviewCapability, RuntimePreviewMode, ALL_RUNTIME_PREVIEW_CAPABILITIES,
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
const DEFAULT_TEXT_MODEL: &str = "gpt-4o-mini";
const DEFAULT_EMBEDDINGS_MODEL: &str = "text-embedding-3-small";
const DEFAULT_EMBEDDINGS_DIMS: u32 = 1536;
const DEFAULT_ANTHROPIC_TEXT_MODEL: &str = "claude-3-5-sonnet-latest";

#[derive(Debug, Clone)]
pub(crate) struct OnboardingWizardRequest {
    pub(crate) path: Option<String>,
    pub(crate) force: bool,
    pub(crate) setup_mode: Option<InitModeArg>,
    pub(crate) setup_tls_scaffold: Option<InitTlsScaffoldArg>,
    pub(crate) options: WizardOverridesArg,
}

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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Default)]
#[serde(rename_all = "snake_case")]
enum HealthStatus {
    ConfigReady,
    RemoteVerified,
    #[default]
    Skipped,
    ManualFollowUpRequired,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Default)]
#[serde(rename_all = "snake_case")]
enum ServiceInstallMode {
    #[default]
    NotNow,
    GuidanceOnly,
    InstallNow,
}

impl ServiceInstallMode {
    fn as_str(self) -> &'static str {
        match self {
            Self::NotNow => "not_now",
            Self::GuidanceOnly => "guidance_only",
            Self::InstallNow => "install_now",
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
}

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
    let summary = OnboardingSummary {
        status: "complete",
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
        skills: build_default_skills_inventory_snapshot(),
    };
    emit_onboarding_summary(&summary, output::preferred_json(request.options.json))
}

pub(crate) fn run_configure_wizard(request: ConfigureWizardRequest) -> Result<()> {
    let config_path = resolve_config_path(request.path.clone(), true)?;
    let path_ref = Path::new(&config_path);
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
                    "Choose how this installation should authenticate to OpenAI-compatible APIs.",
                    vec![
                        choice(
                            "existing_config",
                            "Reuse Current",
                            Some("keep the existing credential source"),
                        ),
                        choice(
                            "api_key",
                            "Vault-Backed API Key",
                            Some("store an API key in the vault and update the config"),
                        ),
                        choice(
                            "skip",
                            "Skip",
                            Some("leave auth unset and accept follow-up warnings"),
                        ),
                    ],
                    Some(current_auth),
                ))?;
                apply_auth_method_choice(
                    &mut wizard,
                    &mut document,
                    auth_method.as_str(),
                    request.api_key_env.clone(),
                    request.api_key_stdin,
                    request.api_key_prompt,
                    &mut warnings,
                )?;
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
                warnings.push(
                    format!(
                        "skills inventory snapshot: installed={} eligible={} quarantined={} runtime_unknown={}; use `palyra skills info|check|list` for concrete actions.",
                        skills_snapshot.installed_total,
                        skills_snapshot.eligible_total,
                        skills_snapshot.quarantined_total,
                        skills_snapshot.runtime_unknown_total
                    )
                        .to_owned(),
                );
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
        let mut section_follow_up_checks = section_follow_up_checks(section, &document)?;
        follow_up_checks.extend(section_follow_up_checks.iter().cloned());
        section_changes.push(ConfigureSectionChange {
            section: section.slug().to_owned(),
            changed,
            before: before_snapshot,
            after: describe_configure_section(&document, section)?,
            restart_required: section_restart_required,
            follow_up_checks: std::mem::take(&mut section_follow_up_checks),
        });
    }

    validate_daemon_compatible_document(&document).with_context(|| {
        format!("mutated config {} does not match daemon schema", path_ref.display())
    })?;
    if document != original_document {
        write_document_with_backups(path_ref, &document, CONFIGURE_BACKUPS)
            .with_context(|| format!("failed to persist config {}", path_ref.display()))?;
    }
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
    };
    emit_configure_summary(&summary, output::preferred_json(request.json))
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
    Ok(Box::new(InteractiveWizardBackend::new()))
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
    validate_stdin_secret_usage(request.options.api_key_stdin, request.options.admin_token_stdin)?;
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
    validate_stdin_secret_usage(request.api_key_stdin, request.admin_token_stdin)?;
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
    if let Some(auth_method) = request.auth_method {
        answers
            .insert("auth_method".to_owned(), WizardValue::Choice(auth_method_value(auth_method)));
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

    let configure_channels = wizard.confirm(confirm_step(
        "configure_channels",
        "Channels",
        "Do you want this wizard to cover channel setup now? This wizard only records the guidance; live connector provisioning remains under `palyra channels ...`.",
        Some(false),
    ))?;
    if !configure_channels {
        plan.skipped_sections.push("channels".to_owned());
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
        plan.skipped_sections.push("skills".to_owned());
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
        _ => ServiceInstallMode::NotNow,
    };

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
        vec![
            choice(
                "api_key",
                "OpenAI-Compatible API Key",
                Some("recommended default for a working local instance"),
            ),
            choice(
                "anthropic_api_key",
                "Anthropic API Key",
                Some("use Anthropic as the primary provider for chat workloads"),
            ),
            choice(
                "skip",
                "Skip for Now",
                Some("leave model auth unset and continue with warnings"),
            ),
        ],
        Some("api_key".to_owned()),
    ))?;
    plan.auth_method = auth_method.clone();
    if auth_method == "api_key" || auth_method == "anthropic_api_key" {
        let api_key_label = api_key_field_label(auth_method.as_str());
        let api_key = wizard.text(
            text_step(
                "model_provider_api_key",
                api_key_label,
                if auth_method == "anthropic_api_key" {
                    "Enter the Anthropic API key that should be written to the local vault."
                } else {
                    "Enter the OpenAI-compatible API key that should be written to the local vault."
                },
                None,
                None,
                true,
            ),
            |value| validate_non_empty_text(value, api_key_label),
        )?;
        plan.api_key = Some(api_key);
    } else {
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
        vec![
            choice(
                "api_key",
                "OpenAI-Compatible API Key",
                Some("store the key in the vault and point the config at it"),
            ),
            choice(
                "anthropic_api_key",
                "Anthropic API Key",
                Some("store the Anthropic key in the vault and switch the config to Anthropic"),
            ),
            choice(
                "existing_config",
                "Reuse Current",
                Some("keep the existing credential source if one is already configured"),
            ),
            choice(
                "skip",
                "Skip",
                Some("continue without model auth and accept follow-up warnings"),
            ),
        ],
        Some("api_key".to_owned()),
    ))?;
    plan.auth_method = auth_method.clone();
    if auth_method == "api_key" || auth_method == "anthropic_api_key" {
        let api_key_label = api_key_field_label(auth_method.as_str());
        let api_key = wizard.text(
            text_step(
                "model_provider_api_key",
                api_key_label,
                if auth_method == "anthropic_api_key" {
                    "Enter the Anthropic API key that should be stored in the local vault."
                } else {
                    "Enter the OpenAI-compatible API key that should be stored in the local vault."
                },
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
    }

    set_value_at_path(
        &mut document,
        "deployment.profile",
        toml::Value::String(plan.deployment_profile.as_str().to_owned()),
    )?;
    apply_deployment_profile_defaults(&mut document, plan.deployment_profile)?;
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
    if context.config_path.exists() {
        write_document_with_backups(context.config_path.as_path(), &document, CONFIGURE_BACKUPS)
            .with_context(|| {
                format!("failed to persist config {}", context.config_path.display())
            })?;
    } else {
        let rendered = serialize_document_pretty(&document)
            .context("failed to serialize wizard-generated config document")?;
        fs::write(context.config_path.as_path(), rendered)
            .with_context(|| format!("failed to write {}", context.config_path.display()))?;
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
        support::service::install_gateway_service(&request)
            .context("failed to install gateway service from onboarding wizard")?;
    }

    let target = resolve_dashboard_access_target(Some(context.config_path.display().to_string()))?;
    Ok(target.url)
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
            let needs_follow_up = checks.iter().any(|check| check.status != "ok");
            Ok(HealthCheckReport {
                status: if needs_follow_up {
                    HealthStatus::ManualFollowUpRequired
                } else {
                    HealthStatus::ConfigReady
                },
                checks,
            })
        }
    }
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
    api_key_env: Option<String>,
    api_key_stdin: bool,
    api_key_prompt: bool,
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
            let api_key_label = api_key_field_label(auth_method);
            let explicit_secret = load_secret_input_optional(
                api_key_env,
                api_key_stdin,
                api_key_prompt,
                format!("{api_key_label}: ").as_str(),
            )?;
            let api_key = match explicit_secret {
                Some(value) => value,
                None => wizard.text(
                    text_step(
                        "model_provider_api_key",
                        api_key_label,
                        if auth_method == "anthropic_api_key" {
                            "Enter the Anthropic API key that should be stored in the local vault."
                        } else {
                            "Enter the OpenAI-compatible API key that should be stored in the local vault."
                        },
                        None,
                        None,
                        true,
                    ),
                    |value| validate_non_empty_text(value, api_key_label),
                )?,
            };
            apply_model_provider_api_key(document, auth_method, api_key.as_str())?;
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
    unset_value_at_path(document, "model_provider.openai_api_key_vault_ref")?;
    unset_value_at_path(document, "model_provider.anthropic_api_key")?;
    unset_value_at_path(document, "model_provider.anthropic_api_key_vault_ref")?;
    unset_value_at_path(document, "model_provider.auth_profile_id")?;
    Ok(())
}

fn apply_model_provider_api_key(
    document: &mut toml::Value,
    auth_method: &str,
    api_key: &str,
) -> Result<()> {
    clear_model_provider_auth(document)?;
    match auth_method {
        "anthropic_api_key" => {
            let vault_ref = store_secret_in_vault("global", "anthropic_api_key", api_key)?;
            set_value_at_path(
                document,
                "model_provider.kind",
                toml::Value::String("anthropic".to_owned()),
            )?;
            set_value_at_path(
                document,
                "model_provider.anthropic_base_url",
                toml::Value::String("https://api.anthropic.com".to_owned()),
            )?;
            set_value_at_path(
                document,
                "model_provider.anthropic_model",
                toml::Value::String(DEFAULT_ANTHROPIC_TEXT_MODEL.to_owned()),
            )?;
            unset_value_at_path(document, "model_provider.openai_base_url")?;
            unset_value_at_path(document, "model_provider.openai_model")?;
            unset_value_at_path(document, "model_provider.openai_embeddings_model")?;
            unset_value_at_path(document, "model_provider.openai_embeddings_dims")?;
            set_value_at_path(
                document,
                "model_provider.anthropic_api_key_vault_ref",
                toml::Value::String(vault_ref),
            )?;
        }
        _ => {
            let vault_ref = store_secret_in_vault("global", "openai_api_key", api_key)?;
            set_value_at_path(
                document,
                "model_provider.kind",
                toml::Value::String("openai_compatible".to_owned()),
            )?;
            if get_value_at_path(document, "model_provider.openai_base_url")?
                .and_then(toml::Value::as_str)
                .is_none()
            {
                set_value_at_path(
                    document,
                    "model_provider.openai_base_url",
                    toml::Value::String("https://api.openai.com/v1".to_owned()),
                )?;
            }
            set_value_at_path(
                document,
                "model_provider.openai_model",
                toml::Value::String(DEFAULT_TEXT_MODEL.to_owned()),
            )?;
            set_value_at_path(
                document,
                "model_provider.openai_embeddings_model",
                toml::Value::String(DEFAULT_EMBEDDINGS_MODEL.to_owned()),
            )?;
            set_value_at_path(
                document,
                "model_provider.openai_embeddings_dims",
                toml::Value::Integer(i64::from(DEFAULT_EMBEDDINGS_DIMS)),
            )?;
            unset_value_at_path(document, "model_provider.anthropic_base_url")?;
            unset_value_at_path(document, "model_provider.anthropic_model")?;
            set_value_at_path(
                document,
                "model_provider.openai_api_key_vault_ref",
                toml::Value::String(vault_ref),
            )?;
        }
    }
    Ok(())
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

fn validate_stdin_secret_usage(api_key_stdin: bool, admin_token_stdin: bool) -> Result<()> {
    if api_key_stdin && admin_token_stdin {
        anyhow::bail!(
            "only one secret may be sourced from stdin per invocation; split model-provider API key and admin token configuration into separate runs or use environment/prompt sources"
        );
    }
    Ok(())
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
        || get_string_value_at_path(document, "model_provider.auth_profile_id")?.is_some())
}

fn configured_chat_model(document: &toml::Value) -> Result<Option<String>> {
    let provider_kind = get_string_value_at_path(document, "model_provider.kind")?
        .unwrap_or_else(|| "openai_compatible".to_owned());
    if provider_kind == "anthropic" {
        get_string_value_at_path(document, "model_provider.anthropic_model")
    } else {
        get_string_value_at_path(document, "model_provider.openai_model")
    }
}

fn configured_embeddings_model(document: &toml::Value) -> Result<Option<String>> {
    get_string_value_at_path(document, "model_provider.openai_embeddings_model")
}

fn join_section_state(values: &[String]) -> String {
    if values.is_empty() {
        "none".to_owned()
    } else {
        values.join(" | ")
    }
}

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
            let auth_source =
                if get_string_value_at_path(document, "model_provider.openai_api_key_vault_ref")?
                    .is_some()
                    || get_string_value_at_path(
                        document,
                        "model_provider.anthropic_api_key_vault_ref",
                    )?
                    .is_some()
                {
                    "vault_ref".to_owned()
                } else if get_string_value_at_path(document, "model_provider.auth_profile_id")?
                    .is_some()
                {
                    "auth_profile".to_owned()
                } else {
                    "unset".to_owned()
                };
            Ok(vec![
                format!(
                    "provider_kind={}",
                    get_string_value_at_path(document, "model_provider.kind")?
                        .unwrap_or_else(|| "unset".to_owned())
                ),
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
        ConfigureSectionArg::Skills => Ok(vec![
            format!(
                "skills_trust_store={}",
                env::var("PALYRA_SKILLS_TRUST_STORE").unwrap_or_else(|_| "default".to_owned())
            ),
            format!(
                "installed_total={}",
                build_default_skills_inventory_snapshot().installed_total
            ),
            format!("eligible_total={}", build_default_skills_inventory_snapshot().eligible_total),
            format!(
                "quarantined_total={}",
                build_default_skills_inventory_snapshot().quarantined_total
            ),
        ]),
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

fn section_requires_restart(section: ConfigureSectionArg, changed: bool) -> bool {
    changed
        && matches!(
            section,
            ConfigureSectionArg::DeploymentProfile
                | ConfigureSectionArg::Workspace
                | ConfigureSectionArg::AuthModel
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
                "restart daemon so deployment profile changes take effect".to_owned(),
            ]
        }
        ConfigureSectionArg::Workspace => {
            vec!["restart daemon or new runs to pick up workspace-root changes".to_owned()]
        }
        ConfigureSectionArg::AuthModel => vec![
            "palyra doctor".to_owned(),
            "palyra models status".to_owned(),
            "restart daemon so model-provider auth changes take effect".to_owned(),
        ],
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
            "restart daemon so runtime control changes take effect".to_owned(),
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

fn current_auth_method(document: &toml::Value) -> String {
    let provider_kind = get_string_value_at_path(document, "model_provider.kind")
        .ok()
        .flatten()
        .unwrap_or_else(|| "openai_compatible".to_owned());
    if get_string_value_at_path(document, "model_provider.auth_profile_id").ok().flatten().is_some()
    {
        return "existing_config".to_owned();
    }
    if get_string_value_at_path(document, "model_provider.anthropic_api_key_vault_ref")
        .ok()
        .flatten()
        .is_some()
    {
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
    if provider_kind == "anthropic" {
        "anthropic_api_key".to_owned()
    } else {
        "api_key".to_owned()
    }
}

fn auth_method_value(value: OnboardingAuthMethodArg) -> String {
    match value {
        OnboardingAuthMethodArg::ApiKey => "api_key",
        OnboardingAuthMethodArg::AnthropicApiKey => "anthropic_api_key",
        OnboardingAuthMethodArg::Skip => "skip",
        OnboardingAuthMethodArg::ExistingConfig => "existing_config",
    }
    .to_owned()
}

fn api_key_field_label(auth_method: &str) -> &'static str {
    match auth_method {
        "anthropic_api_key" => "Anthropic API Key",
        _ => "OpenAI API Key",
    }
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
    use std::collections::VecDeque;

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
