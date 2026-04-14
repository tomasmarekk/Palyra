use std::path::Path;

use palyra_control_plane as control_plane;

use crate::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OnboardingVariant {
    Quickstart,
    Manual,
    Remote,
}

impl OnboardingVariant {
    fn as_str(self) -> &'static str {
        match self {
            Self::Quickstart => "quickstart",
            Self::Manual => "manual",
            Self::Remote => "remote",
        }
    }

    fn flow(self) -> control_plane::OnboardingFlow {
        match self {
            Self::Quickstart => control_plane::OnboardingFlow::QuickStart,
            Self::Manual | Self::Remote => control_plane::OnboardingFlow::AdvancedSetup,
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

#[derive(Debug, Clone)]
struct OnboardingSignals {
    config_exists: bool,
    config_path: String,
    workspace_root_configured: bool,
    remote_base_url_configured: bool,
    remote_verification_mode: Option<&'static str>,
    remote_posture_safe: bool,
    deployment_warning: Option<String>,
    provider_auth_configured: bool,
    provider_model_selected: bool,
    provider_health_state: String,
    provider_health_message: String,
    model_discovery_ready: bool,
    model_discovery_message: String,
}

#[derive(Debug)]
struct StepPresentation {
    blocked: Option<control_plane::OnboardingBlockedReason>,
    optional: bool,
    verification_state: Option<String>,
}

impl StepPresentation {
    fn required(verification_state: Option<String>) -> Self {
        Self { blocked: None, optional: false, verification_state }
    }

    fn with_blocked(mut self, blocked: Option<control_plane::OnboardingBlockedReason>) -> Self {
        self.blocked = blocked;
        self
    }
}

pub(crate) fn run_onboarding(command: OnboardingCommand) -> Result<()> {
    match command {
        OnboardingCommand::Wizard { path, force, options } => {
            commands::operator_wizard::run_onboarding_wizard(
                commands::operator_wizard::OnboardingWizardRequest {
                    path,
                    force,
                    setup_mode: None,
                    setup_tls_scaffold: None,
                    options: *options,
                },
            )
        }
        OnboardingCommand::Status { path, flow, json } => run_onboarding_status(path, flow, json),
    }
}

fn run_onboarding_status(
    path: Option<String>,
    flow: Option<OnboardingFlowArg>,
    json: bool,
) -> Result<()> {
    let variant = flow.map(OnboardingVariant::from_arg).unwrap_or(OnboardingVariant::Quickstart);
    let (document, config_path) = load_onboarding_document(path)?;
    let signals = collect_onboarding_signals(&document, config_path, variant)?;
    let steps = build_onboarding_steps(variant, &signals);
    let counts = build_onboarding_counts(&steps);
    let ready_for_first_success = steps
        .iter()
        .filter(|step| !step.optional)
        .all(|step| step.status == control_plane::OnboardingStepStatus::Done);
    let status = derive_posture_status(&steps, ready_for_first_success);
    let payload = control_plane::OnboardingPostureEnvelope {
        contract: cli_contract_descriptor(),
        flow: variant.flow(),
        flow_variant: variant.as_str().to_owned(),
        status,
        config_path: signals.config_path.clone(),
        resume_supported: true,
        ready_for_first_success,
        recommended_step_id: steps
            .iter()
            .find(|step| step.status != control_plane::OnboardingStepStatus::Done)
            .map(|step| step.step_id.clone()),
        first_success_hint: ready_for_first_success.then(|| {
            "Open the dashboard or chat workspace and send a real first request to complete onboarding."
                .to_owned()
        }),
        counts,
        available_flows: vec![
            control_plane::OnboardingFlow::QuickStart,
            control_plane::OnboardingFlow::AdvancedSetup,
        ],
        steps,
    };
    emit_onboarding_status(&payload, output::preferred_json(json))
}

fn load_onboarding_document(path: Option<String>) -> Result<(toml::Value, String)> {
    if let Some(explicit) = path {
        let resolved = resolve_config_path(Some(explicit), false)?;
        let path_ref = Path::new(&resolved);
        if path_ref.exists() {
            let (document, _) = load_document_from_existing_path(path_ref)
                .with_context(|| format!("failed to parse {}", path_ref.display()))?;
            return Ok((document, resolved));
        }
        let (document, _) = parse_document_with_migration("")
            .context("failed to initialize empty config document")?;
        return Ok((document, resolved));
    }

    if let Some(default_path) = find_default_config_path() {
        let path_ref = Path::new(&default_path);
        let (document, _) = load_document_from_existing_path(path_ref)
            .with_context(|| format!("failed to parse {}", path_ref.display()))?;
        return Ok((document, default_path));
    }

    let (document, _) =
        parse_document_with_migration("").context("failed to initialize empty config document")?;
    Ok((document, "defaults".to_owned()))
}

fn collect_onboarding_signals(
    document: &toml::Value,
    config_path: String,
    variant: OnboardingVariant,
) -> Result<OnboardingSignals> {
    let bind_profile = get_string_at_path(document, "gateway.bind_profile")
        .unwrap_or_else(|| "loopback_only".to_owned());
    let tls_enabled = get_bool_at_path(document, "gateway.tls.enabled").unwrap_or(false);
    let admin_auth_required = get_bool_at_path(document, "admin.require_auth").unwrap_or(false);
    let dangerous_remote_bind_ack =
        get_bool_at_path(document, "deployment.dangerous_remote_bind_ack").unwrap_or(false);
    let remote_base_url_configured =
        get_string_at_path(document, "gateway_access.remote_base_url").is_some();
    let remote_verification_mode = remote_verification_mode(document);
    let remote_posture_safe = if bind_profile == "public_tls" {
        tls_enabled
            && admin_auth_required
            && dangerous_remote_bind_ack
            && (variant != OnboardingVariant::Remote
                || (remote_base_url_configured && remote_verification_mode.is_some()))
    } else {
        true
    };
    let deployment_warning = if remote_posture_safe {
        None
    } else if bind_profile == "public_tls" {
        Some(
            "public TLS posture still requires TLS, admin auth, dangerous bind acknowledgement, and verified remote access metadata"
                .to_owned(),
        )
    } else {
        Some("gateway posture still needs configuration review".to_owned())
    };

    let provider_kind = get_string_at_path(document, "model_provider.kind")
        .unwrap_or_else(|| "openai_compatible".to_owned());
    let provider_auth_configured = model_auth_configured(document)?;
    let provider_model_selected = configured_chat_model(document)?.is_some();
    let provider_health_state =
        if provider_auth_configured { "configured".to_owned() } else { "missing_auth".to_owned() };
    let provider_health_message = if provider_auth_configured {
        format!("{provider_kind} credential source is configured in the local daemon config")
    } else {
        "no provider credential is configured yet".to_owned()
    };
    let model_discovery_ready = provider_model_selected;
    let model_discovery_message = if model_discovery_ready {
        "model selection is present in the config; run `palyra models test-connection` for runtime verification"
            .to_owned()
    } else {
        "no chat model is selected in the config yet".to_owned()
    };

    Ok(OnboardingSignals {
        config_exists: config_path != "defaults",
        config_path,
        workspace_root_configured: get_string_at_path(
            document,
            "tool_call.process_runner.workspace_root",
        )
        .is_some(),
        remote_base_url_configured,
        remote_verification_mode,
        remote_posture_safe,
        deployment_warning,
        provider_auth_configured,
        provider_model_selected,
        provider_health_state,
        provider_health_message,
        model_discovery_ready,
        model_discovery_message,
    })
}

fn build_onboarding_steps(
    variant: OnboardingVariant,
    signals: &OnboardingSignals,
) -> Vec<control_plane::OnboardingStepView> {
    let config_step = if signals.config_exists {
        done_step(
            "config",
            "Config ready",
            format!("Daemon config is available at {}.", signals.config_path),
            Some(run_cli_action(
                "Inspect config",
                format!("palyra config inspect --path {}", signals.config_path),
            )),
        )
    } else {
        actionable_step(
            "config",
            "Create config",
            "No daemon config was found yet. Run the canonical setup wizard first.",
            control_plane::OnboardingStepStatus::Todo,
            Some(run_cli_action(
                "Run setup wizard",
                "palyra onboarding wizard --flow quickstart".to_owned(),
            )),
            StepPresentation::required(None),
        )
    };

    let workspace_step = if signals.workspace_root_configured {
        done_step(
            "workspace",
            "Workspace root",
            "The process runner workspace root is configured.",
            Some(run_cli_action(
                "Refine workspace settings",
                "palyra configure --section workspace".to_owned(),
            )),
        )
    } else {
        actionable_step(
            "workspace",
            "Workspace root",
            "Pick the main workspace root before enabling local tool execution.",
            control_plane::OnboardingStepStatus::Todo,
            Some(run_cli_action(
                "Configure workspace",
                "palyra configure --section workspace".to_owned(),
            )),
            StepPresentation::required(None),
        )
    };

    let remote_step = if signals.remote_posture_safe
        && (variant != OnboardingVariant::Remote || signals.remote_base_url_configured)
    {
        done_step(
            "gateway_posture",
            "Gateway posture",
            if variant == OnboardingVariant::Remote {
                format!(
                    "Remote posture is configured with verification mode {}.",
                    signals.remote_verification_mode.unwrap_or("none")
                )
            } else {
                "Gateway posture is safe for local-first onboarding.".to_owned()
            },
            Some(run_cli_action(
                "Review gateway settings",
                "palyra configure --section gateway".to_owned(),
            )),
        )
    } else {
        actionable_step(
            "gateway_posture",
            "Gateway posture",
            if variant == OnboardingVariant::Remote {
                "Remote onboarding requires verified remote access metadata and a safe public posture."
            } else {
                "Deployment posture still needs attention before the operator handoff is safe."
            },
            if signals.config_exists {
                control_plane::OnboardingStepStatus::Blocked
            } else {
                control_plane::OnboardingStepStatus::Todo
            },
            Some(run_cli_action(
                "Configure gateway",
                "palyra configure --section gateway".to_owned(),
            )),
            StepPresentation::required(None).with_blocked(
                signals.deployment_warning.as_deref().map(|detail| {
                    blocked_reason(
                        "deployment_posture",
                        detail,
                        "Resolve the bind/TLS/admin-auth posture before handing onboarding to another surface.",
                    )
                }),
            ),
        )
    };

    let provider_step = if signals.provider_auth_configured && signals.provider_model_selected {
        let status = if signals.provider_health_state == "configured" {
            control_plane::OnboardingStepStatus::Done
        } else {
            control_plane::OnboardingStepStatus::Blocked
        };
        actionable_step(
            "provider_auth",
            "Provider auth",
            "The primary model provider is configured and selected for onboarding.",
            status,
            Some(run_cli_action(
                "Inspect model setup",
                format!("palyra models status --path {}", signals.config_path),
            )),
            StepPresentation::required(Some(signals.provider_health_state.clone())).with_blocked(
                (status == control_plane::OnboardingStepStatus::Blocked).then(|| {
                    blocked_reason(
                        "provider_auth_health",
                        signals.provider_health_message.as_str(),
                        "Repair the configured provider credential before continuing.",
                    )
                }),
            ),
        )
    } else {
        actionable_step(
            "provider_auth",
            "Provider auth",
            "Connect the primary provider and select the model profile used for the first run.",
            control_plane::OnboardingStepStatus::Todo,
            Some(run_cli_action(
                "Open configure wizard",
                "palyra configure --section auth-model".to_owned(),
            )),
            StepPresentation::required(Some("missing_auth".to_owned())),
        )
    };

    let verification_step = if !signals.provider_auth_configured || !signals.provider_model_selected
    {
        actionable_step(
            "model_verification",
            "Model verification",
            "Runtime verification is blocked until provider auth and model selection are complete.",
            control_plane::OnboardingStepStatus::Blocked,
            Some(run_cli_action(
                "Review model status",
                format!("palyra models status --path {}", signals.config_path),
            )),
            StepPresentation::required(Some("blocked".to_owned())).with_blocked(Some(
                blocked_reason(
                    "provider_not_ready",
                    "Provider auth or model selection is incomplete.",
                    "Connect the provider first, then run the model verification commands.",
                ),
            )),
        )
    } else if signals.model_discovery_ready {
        done_step(
            "model_verification",
            "Model verification",
            "Model selection is present; the next verification pass can use the dedicated models command.",
            Some(run_cli_action(
                "Run test connection",
                format!("palyra models test-connection --path {} --json", signals.config_path),
            )),
        )
    } else {
        actionable_step(
            "model_verification",
            "Model verification",
            "Runtime verification still needs an explicit repair pass before the first session.",
            control_plane::OnboardingStepStatus::Blocked,
            Some(run_cli_action(
                "Run test connection",
                format!("palyra models test-connection --path {} --json", signals.config_path),
            )),
            StepPresentation::required(Some(signals.provider_health_state.clone()))
                .with_blocked(Some(blocked_reason(
                "model_verification",
                signals.model_discovery_message.as_str(),
                "Use the model commands to verify connectivity and confirm a selected chat model.",
            ))),
        )
    };

    let first_success_ready =
        [config_step.status, remote_step.status, provider_step.status, verification_step.status]
            .into_iter()
            .all(|status| status == control_plane::OnboardingStepStatus::Done);
    let first_success_step = if first_success_ready {
        actionable_step(
            "first_success",
            "First success",
            "Open the dashboard or chat workspace and send a real request to finish the guided handoff.",
            control_plane::OnboardingStepStatus::InProgress,
            Some(run_cli_action(
                "Open dashboard",
                "palyra dashboard".to_owned(),
            )),
            StepPresentation::required(Some("ready".to_owned())),
        )
    } else {
        actionable_step(
            "first_success",
            "First success",
            "The first-session handoff stays blocked until config, posture, provider auth, and verification are complete.",
            control_plane::OnboardingStepStatus::Blocked,
            Some(run_cli_action(
                "Review blockers",
                format!("palyra onboarding status --path {} --json", signals.config_path),
            )),
            StepPresentation::required(Some("blocked".to_owned())).with_blocked(Some(
                blocked_reason(
                    "first_success_blocked",
                    "Prerequisite onboarding steps are still incomplete.",
                    "Clear the recommended blockers above, then open the dashboard for the first guided success.",
                ),
            )),
        )
    };

    match variant {
        OnboardingVariant::Quickstart => {
            vec![config_step, provider_step, verification_step, first_success_step]
        }
        OnboardingVariant::Manual | OnboardingVariant::Remote => {
            vec![
                config_step,
                workspace_step,
                remote_step,
                provider_step,
                verification_step,
                first_success_step,
            ]
        }
    }
}

fn emit_onboarding_status(
    payload: &control_plane::OnboardingPostureEnvelope,
    json_output: bool,
) -> Result<()> {
    if json_output {
        output::print_json_pretty(payload, "failed to encode onboarding posture as JSON")?;
        return Ok(());
    }

    println!(
        "onboarding.status flow={} variant={} status={} config_path={} ready_for_first_success={}",
        payload.flow_variant,
        payload.flow_variant,
        onboarding_posture_state_label(payload.status),
        payload.config_path,
        payload.ready_for_first_success
    );
    println!(
        "onboarding.counts todo={} in_progress={} blocked={} done={} skipped={}",
        payload.counts.todo,
        payload.counts.in_progress,
        payload.counts.blocked,
        payload.counts.done,
        payload.counts.skipped
    );
    if let Some(step_id) = payload.recommended_step_id.as_deref() {
        println!("onboarding.next_step={step_id}");
    }
    for step in &payload.steps {
        println!(
            "onboarding.step id={} status={} optional={} title={}",
            step.step_id,
            onboarding_step_status_label(step.status),
            step.optional,
            step.title
        );
        println!("onboarding.step.summary id={} {}", step.step_id, step.summary);
        if let Some(blocked) = step.blocked.as_ref() {
            println!(
                "onboarding.step.blocked id={} code={} detail={} repair={}",
                step.step_id, blocked.code, blocked.detail, blocked.repair_hint
            );
        }
        if let Some(action) = step.action.as_ref() {
            println!(
                "onboarding.step.action id={} label={} kind={} target={}",
                step.step_id,
                action.label,
                onboarding_action_kind_label(action.kind),
                action.target
            );
        }
    }
    Ok(())
}

fn build_onboarding_counts(
    steps: &[control_plane::OnboardingStepView],
) -> control_plane::OnboardingStepCounts {
    let mut counts = control_plane::OnboardingStepCounts::default();
    for step in steps {
        match step.status {
            control_plane::OnboardingStepStatus::Todo => counts.todo += 1,
            control_plane::OnboardingStepStatus::InProgress => counts.in_progress += 1,
            control_plane::OnboardingStepStatus::Blocked => counts.blocked += 1,
            control_plane::OnboardingStepStatus::Done => counts.done += 1,
            control_plane::OnboardingStepStatus::Skipped => counts.skipped += 1,
        }
    }
    counts
}

fn derive_posture_status(
    steps: &[control_plane::OnboardingStepView],
    ready_for_first_success: bool,
) -> control_plane::OnboardingPostureState {
    if steps.iter().all(|step| step.status == control_plane::OnboardingStepStatus::Todo) {
        return control_plane::OnboardingPostureState::NotStarted;
    }
    if ready_for_first_success {
        return control_plane::OnboardingPostureState::Ready;
    }
    if steps.iter().any(|step| step.status == control_plane::OnboardingStepStatus::Blocked) {
        return control_plane::OnboardingPostureState::Blocked;
    }
    control_plane::OnboardingPostureState::InProgress
}

fn done_step(
    step_id: &str,
    title: &str,
    summary: impl Into<String>,
    action: Option<control_plane::OnboardingStepAction>,
) -> control_plane::OnboardingStepView {
    actionable_step(
        step_id,
        title,
        summary,
        control_plane::OnboardingStepStatus::Done,
        action,
        StepPresentation::required(Some("ok".to_owned())),
    )
}

fn actionable_step(
    step_id: &str,
    title: &str,
    summary: impl Into<String>,
    status: control_plane::OnboardingStepStatus,
    action: Option<control_plane::OnboardingStepAction>,
    presentation: StepPresentation,
) -> control_plane::OnboardingStepView {
    control_plane::OnboardingStepView {
        step_id: step_id.to_owned(),
        title: title.to_owned(),
        summary: summary.into(),
        status,
        optional: presentation.optional,
        verification_state: presentation.verification_state,
        blocked: presentation.blocked,
        action,
    }
}

fn blocked_reason(
    code: &str,
    detail: &str,
    repair_hint: &str,
) -> control_plane::OnboardingBlockedReason {
    control_plane::OnboardingBlockedReason {
        code: code.to_owned(),
        detail: detail.to_owned(),
        repair_hint: repair_hint.to_owned(),
    }
}

fn run_cli_action(label: &str, command: String) -> control_plane::OnboardingStepAction {
    control_plane::OnboardingStepAction {
        label: label.to_owned(),
        kind: control_plane::OnboardingActionKind::RunCliCommand,
        surface: "cli".to_owned(),
        target: command,
    }
}

fn get_string_at_path(document: &toml::Value, key: &str) -> Option<String> {
    get_value_at_path(document, key)
        .ok()
        .and_then(|value| value.and_then(toml::Value::as_str))
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
}

fn get_bool_at_path(document: &toml::Value, key: &str) -> Option<bool> {
    get_value_at_path(document, key).ok().and_then(|value| value.and_then(toml::Value::as_bool))
}

fn model_auth_configured(document: &toml::Value) -> Result<bool> {
    Ok(get_string_at_path(document, "model_provider.openai_api_key_vault_ref").is_some()
        || get_string_at_path(document, "model_provider.anthropic_api_key_vault_ref").is_some()
        || get_string_at_path(document, "model_provider.auth_profile_id").is_some())
}

fn configured_chat_model(document: &toml::Value) -> Result<Option<String>> {
    let provider_kind = get_string_at_path(document, "model_provider.kind")
        .unwrap_or_else(|| "openai_compatible".to_owned());
    if provider_kind == "anthropic" {
        Ok(get_string_at_path(document, "model_provider.anthropic_model"))
    } else {
        Ok(get_string_at_path(document, "model_provider.openai_model"))
    }
}

fn remote_verification_mode(document: &toml::Value) -> Option<&'static str> {
    if get_string_at_path(document, "gateway_access.pinned_server_cert_fingerprint_sha256")
        .is_some()
    {
        Some("server_cert")
    } else if get_string_at_path(document, "gateway_access.pinned_gateway_ca_fingerprint_sha256")
        .is_some()
    {
        Some("gateway_ca")
    } else {
        None
    }
}

fn onboarding_posture_state_label(state: control_plane::OnboardingPostureState) -> &'static str {
    match state {
        control_plane::OnboardingPostureState::NotStarted => "not_started",
        control_plane::OnboardingPostureState::InProgress => "in_progress",
        control_plane::OnboardingPostureState::Blocked => "blocked",
        control_plane::OnboardingPostureState::Ready => "ready",
        control_plane::OnboardingPostureState::Complete => "complete",
    }
}

fn onboarding_step_status_label(status: control_plane::OnboardingStepStatus) -> &'static str {
    match status {
        control_plane::OnboardingStepStatus::Todo => "todo",
        control_plane::OnboardingStepStatus::InProgress => "in_progress",
        control_plane::OnboardingStepStatus::Blocked => "blocked",
        control_plane::OnboardingStepStatus::Done => "done",
        control_plane::OnboardingStepStatus::Skipped => "skipped",
    }
}

fn onboarding_action_kind_label(kind: control_plane::OnboardingActionKind) -> &'static str {
    match kind {
        control_plane::OnboardingActionKind::OpenConsolePath => "open_console_path",
        control_plane::OnboardingActionKind::RunCliCommand => "run_cli_command",
        control_plane::OnboardingActionKind::OpenDesktopSection => "open_desktop_section",
        control_plane::OnboardingActionKind::ReadDocs => "read_docs",
    }
}

fn cli_contract_descriptor() -> control_plane::ContractDescriptor {
    control_plane::ContractDescriptor {
        contract_version: control_plane::CONTROL_PLANE_CONTRACT_VERSION.to_owned(),
    }
}
