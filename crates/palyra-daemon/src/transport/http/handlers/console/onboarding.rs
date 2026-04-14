use serde::Deserialize;
use serde_json::Value;

use crate::transport::http::handlers::console::diagnostics::{
    build_deployment_posture_summary, load_console_config_snapshot,
};
use crate::*;

#[derive(Debug, Clone, Deserialize, Default)]
pub(crate) struct ConsoleOnboardingQuery {
    #[serde(default)]
    pub flow: Option<String>,
}

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
    discord_enabled: bool,
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

    fn optional(verification_state: Option<String>) -> Self {
        Self { blocked: None, optional: true, verification_state }
    }

    fn with_blocked(mut self, blocked: Option<control_plane::OnboardingBlockedReason>) -> Self {
        self.blocked = blocked;
        self
    }
}

pub(crate) async fn console_onboarding_posture_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<ConsoleOnboardingQuery>,
) -> Result<Json<control_plane::OnboardingPostureEnvelope>, Response> {
    let _session = authorize_console_session(&state, &headers, false)?;
    let configured_path = std::env::var("PALYRA_CONFIG").ok();
    let (document, _, config_path) =
        load_console_config_snapshot(configured_path.as_deref(), true)?;
    let deployment = build_deployment_posture_summary(&state);
    let variant = resolve_onboarding_variant(query.flow.as_deref(), deployment.mode.as_str())?;
    let signals = collect_onboarding_signals(&state, &document, config_path, &deployment)?;
    let steps = build_onboarding_steps(variant, &signals);
    let counts = build_onboarding_counts(&steps);
    let recommended_step_id = steps
        .iter()
        .find(|step| step.status != control_plane::OnboardingStepStatus::Done)
        .map(|step| step.step_id.clone());
    let ready_for_first_success = steps
        .iter()
        .filter(|step| !step.optional)
        .all(|step| matches!(step.status, control_plane::OnboardingStepStatus::Done));
    let status = derive_posture_status(&steps, ready_for_first_success);

    Ok(Json(control_plane::OnboardingPostureEnvelope {
        contract: contract_descriptor(),
        flow: variant.flow(),
        flow_variant: variant.as_str().to_owned(),
        status,
        config_path: signals.config_path,
        resume_supported: true,
        ready_for_first_success,
        recommended_step_id,
        first_success_hint: ready_for_first_success.then(|| {
            "Open the chat workspace and send a first real operator request to complete onboarding."
                .to_owned()
        }),
        counts,
        available_flows: vec![
            control_plane::OnboardingFlow::QuickStart,
            control_plane::OnboardingFlow::AdvancedSetup,
        ],
        steps,
    }))
}

#[allow(clippy::result_large_err)]
fn resolve_onboarding_variant(
    requested: Option<&str>,
    deployment_mode: &str,
) -> Result<OnboardingVariant, Response> {
    let normalized = requested
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(|value| value.to_ascii_lowercase());
    match normalized.as_deref() {
        None => Ok(default_onboarding_variant(deployment_mode)),
        Some("quickstart" | "quick_start") => Ok(OnboardingVariant::Quickstart),
        Some("manual" | "advanced" | "advanced_setup") => Ok(OnboardingVariant::Manual),
        Some("remote") => Ok(OnboardingVariant::Remote),
        Some(other) => Err(runtime_status_response(tonic::Status::invalid_argument(format!(
            "unsupported onboarding flow '{other}'"
        )))),
    }
}

fn default_onboarding_variant(deployment_mode: &str) -> OnboardingVariant {
    if deployment_mode.eq_ignore_ascii_case("remote_vps") {
        OnboardingVariant::Remote
    } else {
        OnboardingVariant::Quickstart
    }
}

#[allow(clippy::result_large_err)]
fn collect_onboarding_signals(
    state: &AppState,
    document: &toml::Value,
    config_path: String,
    deployment: &control_plane::DeploymentPostureSummary,
) -> Result<OnboardingSignals, Response> {
    let provider_snapshot = state.runtime.model_provider_status_snapshot();
    let connectors = state.channels.list().map_err(channel_platform_error_response)?;
    let connectors_value = serde_json::to_value(connectors).map_err(|error| {
        runtime_status_response(tonic::Status::internal(format!(
            "failed to serialize channel inventory for onboarding posture: {error}"
        )))
    })?;

    Ok(OnboardingSignals {
        config_exists: config_path != "defaults",
        config_path,
        workspace_root_configured: get_string_at_path(
            document,
            "tool_call.process_runner.workspace_root",
        )
        .is_some(),
        remote_base_url_configured: get_string_at_path(document, "gateway_access.remote_base_url")
            .is_some(),
        remote_verification_mode: remote_verification_mode(document),
        remote_posture_safe: deployment.warnings.is_empty(),
        deployment_warning: deployment.warnings.first().cloned(),
        provider_auth_configured: provider_snapshot.api_key_configured
            || provider_snapshot.auth_profile_id.is_some(),
        provider_model_selected: provider_snapshot.model_id.is_some()
            || provider_snapshot.openai_model.is_some()
            || provider_snapshot.anthropic_model.is_some(),
        provider_health_state: provider_snapshot.health.state,
        provider_health_message: provider_snapshot.health.message,
        model_discovery_ready: !provider_snapshot.discovery.discovered_model_ids.is_empty(),
        model_discovery_message: provider_snapshot
            .discovery
            .message
            .unwrap_or_else(|| "provider has not published discovered model ids yet".to_owned()),
        discord_enabled: connector_enabled(connectors_value.as_array(), "discord"),
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
            Some(console_path_action("Inspect config", "/#/control/access?panel=config")),
        )
    } else {
        actionable_step(
            "config",
            "Create config",
            "No daemon config was found yet. Run the canonical setup wizard first.",
            control_plane::OnboardingStepStatus::Todo,
            Some(run_cli_action("Run setup wizard", "palyra onboarding wizard --flow quickstart")),
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
                "palyra configure --section workspace",
            )),
        )
    } else {
        actionable_step(
            "workspace",
            "Workspace root",
            "Pick the main workspace root before enabling local tool execution.",
            control_plane::OnboardingStepStatus::Todo,
            Some(run_cli_action("Configure workspace", "palyra configure --section workspace")),
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
            Some(console_path_action("Review access posture", "/#/control/access")),
        )
    } else {
        let summary = if variant == OnboardingVariant::Remote {
            "Remote onboarding requires a verified remote base URL, TLS, and bind posture without active warnings."
        } else {
            "Deployment posture still needs attention before the operator handoff is safe."
        };
        actionable_step(
            "gateway_posture",
            "Gateway posture",
            summary,
            if signals.config_exists {
                control_plane::OnboardingStepStatus::Blocked
            } else {
                control_plane::OnboardingStepStatus::Todo
            },
            Some(run_cli_action(
                "Configure gateway",
                "palyra configure --section gateway",
            )),
            StepPresentation::required(None).with_blocked(
                signals.deployment_warning.as_deref().map(|detail| {
                    blocked_reason(
                        "deployment_posture",
                        detail,
                        "Resolve the bind/TLS/admin-auth warning before handing onboarding to another surface.",
                    )
                }),
            ),
        )
    };

    let provider_step = if signals.provider_auth_configured && signals.provider_model_selected {
        let status = if signals.provider_health_state == "ok" {
            control_plane::OnboardingStepStatus::Done
        } else {
            control_plane::OnboardingStepStatus::Blocked
        };
        actionable_step(
            "provider_auth",
            "Provider auth",
            "The primary model provider is configured and selected for onboarding.",
            status,
            Some(console_path_action("Open provider auth", "/#/control/auth")),
            StepPresentation::required(Some(signals.provider_health_state.clone())).with_blocked(
                (status == control_plane::OnboardingStepStatus::Blocked).then(|| {
                    blocked_reason(
                        "provider_auth_health",
                        signals.provider_health_message.as_str(),
                        "Repair or refresh the configured provider credential before continuing.",
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
            Some(console_path_action("Connect provider", "/#/control/auth")),
            StepPresentation::required(Some("missing_auth".to_owned())),
        )
    };

    let verification_step = if !signals.provider_auth_configured || !signals.provider_model_selected
    {
        actionable_step(
            "model_verification",
            "Model verification",
            "Connection verification is blocked until provider auth and model selection are complete.",
            control_plane::OnboardingStepStatus::Blocked,
            Some(console_path_action(
                "Open model diagnostics",
                "/#/control/access?panel=models",
            )),
            StepPresentation::required(Some("blocked".to_owned())).with_blocked(Some(
                blocked_reason(
                    "provider_not_ready",
                    "Provider auth or model selection is incomplete.",
                    "Connect the provider first, then rerun model verification.",
                ),
            )),
        )
    } else if signals.provider_health_state == "ok" && signals.model_discovery_ready {
        done_step(
            "model_verification",
            "Model verification",
            "Runtime verification sees a healthy provider with discoverable models.",
            Some(console_path_action(
                "Inspect model diagnostics",
                "/#/control/access?panel=models",
            )),
        )
    } else {
        actionable_step(
            "model_verification",
            "Model verification",
            "Runtime verification still needs an explicit repair pass before the first session.",
            control_plane::OnboardingStepStatus::Blocked,
            Some(console_path_action(
                "Run model verification",
                "/#/control/access?panel=models",
            )),
            StepPresentation::required(Some(signals.provider_health_state.clone())).with_blocked(
                Some(blocked_reason(
                    "model_verification",
                    signals.model_discovery_message.as_str(),
                    "Use the model diagnostics action to test the provider and confirm discoverable models.",
                )),
            ),
        )
    };

    let discord_step = if signals.discord_enabled {
        done_step(
            "discord",
            "Discord channel",
            "A Discord connector is already enabled for operator follow-up.",
            Some(console_path_action("Inspect channels", "/#/control/channels")),
        )
    } else {
        actionable_step(
            "discord",
            "Discord channel",
            "Discord onboarding is optional in this phase, but the shared step is ready when you want a first external channel.",
            control_plane::OnboardingStepStatus::Todo,
            Some(console_path_action(
                "Configure Discord",
                "/#/control/channels",
            )),
            StepPresentation::optional(None),
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
            "Open the chat workspace and send a real request to finish the guided handoff.",
            control_plane::OnboardingStepStatus::InProgress,
            Some(console_path_action("Open chat workspace", "/#/chat")),
            StepPresentation::required(Some("ready".to_owned())),
        )
    } else {
        actionable_step(
            "first_success",
            "First success",
            "The first-session handoff stays blocked until config, posture, provider auth, and verification are complete.",
            control_plane::OnboardingStepStatus::Blocked,
            Some(console_path_action(
                "Review blockers",
                "/#/control/overview",
            )),
            StepPresentation::required(Some("blocked".to_owned())).with_blocked(Some(
                blocked_reason(
                    "first_success_blocked",
                    "Prerequisite onboarding steps are still incomplete.",
                    "Clear the recommended blockers above, then open chat for the first guided success.",
                ),
            )),
        )
    };

    match variant {
        OnboardingVariant::Quickstart => {
            vec![config_step, provider_step, verification_step, first_success_step, discord_step]
        }
        OnboardingVariant::Manual | OnboardingVariant::Remote => vec![
            config_step,
            workspace_step,
            remote_step,
            provider_step,
            verification_step,
            first_success_step,
            discord_step,
        ],
    }
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

fn run_cli_action(label: &str, command: &str) -> control_plane::OnboardingStepAction {
    control_plane::OnboardingStepAction {
        label: label.to_owned(),
        kind: control_plane::OnboardingActionKind::RunCliCommand,
        surface: "cli".to_owned(),
        target: command.to_owned(),
    }
}

fn console_path_action(label: &str, path: &str) -> control_plane::OnboardingStepAction {
    control_plane::OnboardingStepAction {
        label: label.to_owned(),
        kind: control_plane::OnboardingActionKind::OpenConsolePath,
        surface: "web".to_owned(),
        target: path.to_owned(),
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

fn connector_enabled(entries: Option<&Vec<Value>>, kind: &str) -> bool {
    entries
        .into_iter()
        .flatten()
        .find(|entry| {
            entry
                .get("kind")
                .and_then(Value::as_str)
                .map(|value| value.eq_ignore_ascii_case(kind))
                .unwrap_or(false)
                || entry
                    .get("connector_id")
                    .and_then(Value::as_str)
                    .map(|value| value.to_ascii_lowercase().starts_with(kind))
                    .unwrap_or(false)
        })
        .and_then(|entry| entry.get("enabled").and_then(Value::as_bool))
        .unwrap_or(false)
}
