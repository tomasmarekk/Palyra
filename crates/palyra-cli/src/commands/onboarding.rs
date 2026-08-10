//! `palyra onboarding` command handlers.
//!
//! Computes the onboarding posture by combining config-file signals with live runtime
//! probes (gateway, browserd, agent registry) and renders it as the shared
//! control-plane `OnboardingPostureEnvelope`, so CLI, desktop, and web surfaces agree
//! on step status and the recommended next action.

use std::{
    fs,
    net::{TcpStream, ToSocketAddrs},
    path::{Path, PathBuf},
    time::Duration,
};

use base64::{engine::general_purpose::STANDARD as BASE64_STANDARD, Engine as _};
use palyra_control_plane as control_plane;
use serde::Deserialize;
use serde_json::{json, Value};

use crate::*;

const CLI_FIRST_SUCCESS_MARKER_RELATIVE_PATH: &str = "onboarding/first-success.json";
// Probe timeouts are deliberately short: `onboarding status` must stay responsive even
// when the gateway and browserd are down, which is the common state during onboarding.
const BROWSER_RUNTIME_CONNECT_TIMEOUT_MS: u64 = 350;
const AUTHENTICATED_RUNTIME_PROBE_TIMEOUT_MS: u64 = 1_000;
const DEFAULT_BROWSER_SERVICE_ENDPOINT: &str = "http://127.0.0.1:7543";
const BROWSER_AUTH_PROBE_PRINCIPAL: &str = "admin:onboarding-browser-probe";

/// Selected onboarding flow; manual and remote share the advanced control-plane flow
/// but differ in which posture signals are required.
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

/// Snapshot of every config and runtime signal the step builder consumes; collected
/// once per status invocation so all steps reason over a consistent view.
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
    gateway_runtime_reachable: bool,
    gateway_runtime_message: String,
    default_agent_configured: bool,
    default_agent_message: String,
    workspace_root: Option<String>,
    chat_model: Option<String>,
    memory_embeddings_configured: bool,
    memory_embeddings_message: String,
    browser_prerequisites_configured: bool,
    browser_prerequisites_message: String,
    browser_runtime_reachable: bool,
    browser_runtime_message: String,
    first_success_completed: bool,
}

#[derive(Debug, Deserialize)]
struct AgentRegistryStatusDocument {
    #[serde(default)]
    default_agent_id: Option<String>,
    #[serde(default)]
    agents: Vec<AgentRegistryStatusRecord>,
}

#[derive(Debug, Deserialize)]
struct AgentRegistryStatusRecord {
    agent_id: String,
}

/// Presentation flags attached to a step view (required/optional, verification state,
/// optional blocked reason).
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

/// Entry point for `palyra onboarding`, dispatching to the wizard or status handler.
///
/// # Errors
/// Returns an error when config loading, signal collection, or output emission fails.
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
    let first_success_completed = onboarding_step_done(&steps, "first_success");
    let ready_for_first_success = onboarding_prerequisites_ready(&steps);
    let status = derive_posture_status(&steps, ready_for_first_success, first_success_completed);
    let recommended_step_id = recommended_onboarding_step_id(&steps);
    let payload = control_plane::OnboardingPostureEnvelope {
        contract: cli_contract_descriptor(),
        flow: variant.flow(),
        flow_variant: variant.as_str().to_owned(),
        status,
        config_path: signals.config_path.clone(),
        resume_supported: true,
        ready_for_first_success,
        recommended_step_id,
        first_success_hint: (ready_for_first_success && !first_success_completed).then(|| {
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

/// Loads the config document for onboarding inspection, falling back to an empty
/// document (with `"defaults"` as the path label) when no config exists yet so the
/// status command works before setup has run.
fn load_onboarding_document(path: Option<String>) -> Result<(toml::Value, String)> {
    if let Some(explicit) = path {
        let resolved = resolve_config_path(Some(explicit), false)?;
        let path_ref = Path::new(&resolved);
        if path_ref.exists() {
            if config_document_has_content(path_ref)? {
                let (document, _) = load_document_from_existing_path(path_ref)
                    .with_context(|| format!("failed to parse {}", path_ref.display()))?;
                return Ok((document, resolved));
            }
            let (document, _) = parse_document_with_migration("")
                .context("failed to initialize empty config document")?;
            return Ok((document, resolved));
        }
        let (document, _) = parse_document_with_migration("")
            .context("failed to initialize empty config document")?;
        return Ok((document, resolved));
    }

    if let Some(active_path) = effective_config_path() {
        let path_ref = Path::new(&active_path);
        if config_document_has_content(path_ref)? {
            let (document, _) = load_document_from_existing_path(path_ref)
                .with_context(|| format!("failed to parse {}", path_ref.display()))?;
            return Ok((document, active_path));
        }
        let (document, _) = parse_document_with_migration("")
            .context("failed to initialize empty config document")?;
        return Ok((document, active_path));
    }

    let (document, _) =
        parse_document_with_migration("").context("failed to initialize empty config document")?;
    Ok((document, "defaults".to_owned()))
}

/// Evaluates all config-derived and probe-derived signals for the given flow variant.
/// Runtime probes (gateway, browserd, agent registry) run here, so this is the only
/// step of `onboarding status` that touches the network.
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
    let chat_model = configured_chat_model(document)?;
    let provider_model_selected = chat_model.is_some();
    let provider_health_state =
        if provider_auth_configured { "configured".to_owned() } else { "missing_auth".to_owned() };
    let provider_health_message = if provider_auth_configured {
        format!("{provider_kind} credential source is configured in the local daemon config")
    } else {
        "no provider credential is configured yet".to_owned()
    };
    let model_discovery_ready = provider_model_selected;
    let model_discovery_message = if model_discovery_ready {
        "model selection is present in the config; run a real agent prompt to verify model usability"
            .to_owned()
    } else {
        "no chat model is selected in the config yet".to_owned()
    };
    let (gateway_runtime_reachable, gateway_runtime_message) = gateway_runtime_status();
    let (default_agent_configured, default_agent_message) = default_agent_status()?;
    let memory_embeddings_configured = memory_embeddings_model_configured_from_document(document)?;
    let memory_embeddings_message = memory_embeddings_onboarding_message(
        document,
        provider_kind.as_str(),
        chat_model.as_deref(),
        memory_embeddings_configured,
    );
    let (browser_prerequisites_configured, browser_prerequisites_message) =
        browser_prerequisites_status(document);
    let (browser_runtime_reachable, browser_runtime_message) = if browser_prerequisites_configured {
        browser_runtime_status(document)
    } else {
        (
            false,
            "browserd runtime reachability is not checked until browser prerequisites are configured"
                .to_owned(),
        )
    };

    Ok(OnboardingSignals {
        config_exists: config_path != "defaults"
            && Path::new(&config_path).exists()
            && config_document_has_content(Path::new(&config_path))?,
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
        gateway_runtime_reachable,
        gateway_runtime_message,
        default_agent_configured,
        default_agent_message,
        workspace_root: get_string_at_path(document, "tool_call.process_runner.workspace_root"),
        chat_model,
        memory_embeddings_configured,
        memory_embeddings_message,
        browser_prerequisites_configured,
        browser_prerequisites_message,
        browser_runtime_reachable,
        browser_runtime_message,
        first_success_completed: cli_first_success_completed()?,
    })
}

/// Maps collected signals to the ordered step views for the requested flow variant.
/// Step ids are a cross-surface contract (desktop and web key on them); ordering
/// drives the recommended-next-step computation.
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
                format!("palyra config list --path {}", signals.config_path),
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

    let memory_embeddings_step = if signals.memory_embeddings_configured {
        done_step(
            "memory_embeddings",
            "Memory embeddings",
            signals.memory_embeddings_message.clone(),
            Some(run_cli_action("Inspect memory status", "palyra memory status".to_owned())),
        )
    } else {
        let status = if signals.first_success_completed {
            control_plane::OnboardingStepStatus::Skipped
        } else {
            control_plane::OnboardingStepStatus::InProgress
        };
        actionable_step(
            "memory_embeddings",
            "Memory embeddings",
            signals.memory_embeddings_message.clone(),
            status,
            Some(run_cli_action(
                "Configure embeddings",
                format!("palyra models status --path {}", signals.config_path),
            )),
            StepPresentation::optional(Some("degraded_hash_fallback".to_owned())),
        )
    };

    let browser_step = if signals.browser_prerequisites_configured
        && signals.browser_runtime_reachable
    {
        done_step(
            "browser_harness",
            "Browser harness",
            format!(
                "{} {}",
                signals.browser_prerequisites_message, signals.browser_runtime_message
            ),
            Some(run_cli_action("Inspect browser status", "palyra browser status".to_owned())),
        )
    } else if signals.browser_prerequisites_configured {
        actionable_step(
            "browser_harness",
            "Browser harness",
            format!(
                "{} {}",
                signals.browser_prerequisites_message, signals.browser_runtime_message
            ),
            control_plane::OnboardingStepStatus::Blocked,
            Some(run_cli_action(
                "Start browserd",
                "palyra browser start --wait-ms 20000 --json".to_owned(),
            )),
            StepPresentation::required(Some("browserd_not_running".to_owned())).with_blocked(
                Some(blocked_reason(
                    "browserd_not_running",
                    signals.browser_runtime_message.as_str(),
                    "Start browserd with `palyra browser start --wait-ms 20000 --json`, then rerun onboarding status before using browser-backed agent workflows.",
                )),
            ),
        )
    } else {
        actionable_step(
            "browser_harness",
            "Browser harness",
            signals.browser_prerequisites_message.clone(),
            control_plane::OnboardingStepStatus::Todo,
            Some(run_cli_action(
                "Configure browser harness",
                format!("palyra browser setup --path {}", signals.config_path),
            )),
            StepPresentation::required(Some("missing_browser_prerequisites".to_owned())),
        )
    };

    let agent_step = if signals.default_agent_configured {
        done_step(
            "agent_identity",
            "Default agent",
            signals.default_agent_message.clone(),
            Some(run_cli_action("Inspect agents", "palyra agents list".to_owned())),
        )
    } else if !signals.gateway_runtime_reachable {
        actionable_step(
            "agent_identity",
            "Default agent",
            format!(
                "{} Start the gateway before creating the default agent; agent creation uses the gRPC runtime API.",
                signals.default_agent_message
            ),
            control_plane::OnboardingStepStatus::Blocked,
            Some(run_cli_action("Start gateway", "palyra gateway run".to_owned())),
            StepPresentation::required(Some("gateway_not_running".to_owned())).with_blocked(
                Some(blocked_reason(
                    "gateway_not_running",
                    signals.gateway_runtime_message.as_str(),
                    "Start the gateway with `palyra gateway run` or `palyra gateway install --start`, then rerun onboarding status and create the default agent.",
                )),
            ),
        )
    } else {
        actionable_step(
            "agent_identity",
            "Default agent",
            signals.default_agent_message.clone(),
            control_plane::OnboardingStepStatus::Todo,
            Some(run_cli_action("Create default agent", default_agent_create_command(signals))),
            StepPresentation::required(Some("missing_default_agent".to_owned())),
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
    } else if signals.first_success_completed {
        done_step(
            "model_verification",
            "Model verification",
            "A real agent prompt completed successfully for the selected model.",
            Some(run_cli_action(
                "Run another prompt",
                "palyra agent run --prompt-stdin".to_owned(),
            )),
        )
    } else if signals.model_discovery_ready {
        actionable_step(
            "model_verification",
            "Model verification",
            "Model selection is present, but live model usability is still pending a real agent prompt.",
            control_plane::OnboardingStepStatus::InProgress,
            Some(run_cli_action(
                "Run smoke prompt",
                "echo Reply exactly PALYRA_ONBOARDING_OK | palyra agent run --session-key onboarding-smoke --reset-session --prompt-stdin"
                    .to_owned(),
            )),
            StepPresentation::required(Some("prompt_required".to_owned())),
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

    // The first-success gate intentionally skips the optional memory-embeddings step
    // (hash fallback keeps memory usable) and the workspace step, which only gates the
    // overall posture via onboarding_prerequisites_ready in manual/remote flows.
    let first_success_ready = [
        config_step.status,
        remote_step.status,
        provider_step.status,
        browser_step.status,
        agent_step.status,
        verification_step.status,
    ]
    .into_iter()
    .all(|status| status == control_plane::OnboardingStepStatus::Done);
    let first_success_step = if first_success_ready && signals.first_success_completed {
        done_step(
            "first_success",
            "First success",
            "A CLI agent run has completed successfully for this installation.",
            Some(run_cli_action(
                "Run another prompt",
                "palyra agent run --prompt-stdin".to_owned(),
            )),
        )
    } else if first_success_ready {
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
            vec![
                config_step,
                provider_step,
                memory_embeddings_step,
                browser_step,
                agent_step,
                verification_step,
                first_success_step,
            ]
        }
        OnboardingVariant::Manual | OnboardingVariant::Remote => {
            vec![
                config_step,
                workspace_step,
                remote_step,
                provider_step,
                memory_embeddings_step,
                browser_step,
                agent_step,
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

    println!("{}", onboarding_status_summary_line(payload));
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

/// Returns the first required step that is not yet done or skipped, in display order.
fn recommended_onboarding_step_id(steps: &[control_plane::OnboardingStepView]) -> Option<String> {
    steps
        .iter()
        .find(|step| {
            !step.optional
                && !matches!(
                    step.status,
                    control_plane::OnboardingStepStatus::Done
                        | control_plane::OnboardingStepStatus::Skipped
                )
        })
        .map(|step| step.step_id.clone())
}

fn derive_posture_status(
    steps: &[control_plane::OnboardingStepView],
    ready_for_first_success: bool,
    first_success_completed: bool,
) -> control_plane::OnboardingPostureState {
    if steps.iter().all(|step| step.status == control_plane::OnboardingStepStatus::Todo) {
        return control_plane::OnboardingPostureState::NotStarted;
    }
    if ready_for_first_success && first_success_completed {
        return control_plane::OnboardingPostureState::Complete;
    }
    if ready_for_first_success {
        return control_plane::OnboardingPostureState::Ready;
    }
    if steps.iter().any(|step| step.status == control_plane::OnboardingStepStatus::Blocked) {
        return control_plane::OnboardingPostureState::Blocked;
    }
    control_plane::OnboardingPostureState::InProgress
}

/// True when every required step except `first_success` itself is done.
fn onboarding_prerequisites_ready(steps: &[control_plane::OnboardingStepView]) -> bool {
    steps
        .iter()
        .filter(|step| !step.optional && step.step_id != "first_success")
        .all(|step| step.status == control_plane::OnboardingStepStatus::Done)
}

fn onboarding_step_done(steps: &[control_plane::OnboardingStepView], step_id: &str) -> bool {
    steps.iter().any(|step| {
        step.step_id == step_id && step.status == control_plane::OnboardingStepStatus::Done
    })
}

/// Persists the first-success marker after a real agent run completes; its presence
/// flips the `first_success` onboarding step to done on later status invocations.
///
/// # Errors
/// Returns an error when the marker directory or file cannot be written.
pub(crate) fn record_cli_first_success(state_root: &Path, run_id: &str) -> Result<()> {
    let marker_path = cli_first_success_marker_path(state_root);
    if let Some(parent) = marker_path.parent() {
        fs::create_dir_all(parent).with_context(|| {
            format!("failed to create onboarding state directory {}", parent.display())
        })?;
    }
    let payload = json!({
        "version": 1,
        "source": "cli_agent_run",
        "run_id": run_id,
        "completed_at_unix_ms": now_unix_ms_i64()?,
    });
    let encoded =
        serde_json::to_vec_pretty(&payload).context("failed to encode first-success marker")?;
    fs::write(marker_path.as_path(), encoded)
        .with_context(|| format!("failed to write first-success marker {}", marker_path.display()))
}

fn cli_first_success_completed() -> Result<bool> {
    let Some(context) = app::current_root_context() else {
        return Ok(false);
    };
    let marker_path = cli_first_success_marker_path(context.state_root());
    if !marker_path.exists() {
        return Ok(false);
    }
    let raw = fs::read(marker_path.as_path()).with_context(|| {
        format!("failed to read first-success marker {}", marker_path.display())
    })?;
    let payload: Value = serde_json::from_slice(raw.as_slice()).with_context(|| {
        format!("failed to parse first-success marker {}", marker_path.display())
    })?;
    Ok(payload.get("completed_at_unix_ms").and_then(Value::as_i64).is_some_and(|value| value > 0))
}

fn cli_first_success_marker_path(state_root: &Path) -> PathBuf {
    state_root.join(CLI_FIRST_SUCCESS_MARKER_RELATIVE_PATH)
}

/// Determines whether a default agent exists, preferring the live gateway registry and
/// falling back to the local `agents.toml`. A gateway probe failure still reports a
/// positive local result so a stopped gateway does not hide an already-created agent.
fn default_agent_status() -> Result<(bool, String)> {
    match default_agent_status_from_gateway_runtime() {
        Ok(Some(status)) => return Ok(status),
        Ok(None) => {}
        Err(error) => {
            let local_status = default_agent_status_from_local_registry()?;
            if local_status.0 {
                return Ok(local_status);
            }
            return Ok((
                false,
                format!(
                    "gateway agent registry probe failed: {}; {}",
                    sanitize_diagnostic_error(error.to_string().as_str()),
                    local_status.1
                ),
            ));
        }
    }
    default_agent_status_from_local_registry()
}

fn default_agent_status_from_gateway_runtime() -> Result<Option<(bool, String)>> {
    let Some(context) = app::current_root_context() else {
        return Ok(None);
    };
    let connection = match context
        .resolve_grpc_connection(app::ConnectionOverrides::default(), app::ConnectionDefaults::USER)
    {
        Ok(connection) => connection,
        Err(_) => return Ok(None),
    };
    run_blocking_probe(gateway_default_agent_status_probe(connection)).map(Some)
}

async fn gateway_default_agent_status_probe(connection: AgentConnection) -> Result<(bool, String)> {
    let mut client = client::runtime::GatewayRuntimeClient::connect(connection).await?;
    let response = client.list_agents(None, Some(500)).await?;
    Ok(default_agent_status_from_agent_ids(
        normalize_optional_text(response.default_agent_id.as_str()),
        response.agents.iter().map(|agent| agent.agent_id.as_str()),
        "gateway runtime",
    ))
}

fn default_agent_status_from_local_registry() -> Result<(bool, String)> {
    let Some(context) = app::current_root_context() else {
        return Ok((false, "CLI state root is unavailable; run setup first".to_owned()));
    };
    let registry_path = context.state_root().join("agents.toml");
    if !registry_path.exists() {
        return Ok((
            false,
            format!(
                "no agent registry exists yet at {}; create a default agent before first chat",
                registry_path.display()
            ),
        ));
    }
    let raw = fs::read_to_string(registry_path.as_path())
        .with_context(|| format!("failed to read agent registry {}", registry_path.display()))?;
    let document: AgentRegistryStatusDocument = toml::from_str(raw.as_str())
        .with_context(|| format!("failed to parse agent registry {}", registry_path.display()))?;
    Ok(default_agent_status_from_agent_ids(
        document.default_agent_id.as_deref().and_then(normalize_optional_text),
        document.agents.iter().map(|agent| agent.agent_id.as_str()),
        "local agent registry",
    ))
}

fn default_agent_status_from_agent_ids<'a>(
    default_agent_id: Option<&str>,
    agent_ids: impl IntoIterator<Item = &'a str>,
    source: &str,
) -> (bool, String) {
    let Some(default_agent_id) = default_agent_id else {
        return (false, format!("{source} does not define a default agent"));
    };
    let found = agent_ids.into_iter().any(|agent_id| agent_id.trim() == default_agent_id);
    if found {
        (true, format!("default agent `{default_agent_id}` is configured in {source}"))
    } else {
        (
            false,
            format!("{source} default `{default_agent_id}` does not match any configured agent"),
        )
    }
}

fn onboarding_status_summary_line(payload: &control_plane::OnboardingPostureEnvelope) -> String {
    format!(
        "onboarding.status flow={} variant={} status={} config_path={} ready_for_first_success={}",
        onboarding_flow_label(payload.flow),
        payload.flow_variant,
        onboarding_posture_state_label(payload.status),
        payload.config_path,
        payload.ready_for_first_success
    )
}

fn memory_embeddings_model_configured_from_document(document: &toml::Value) -> Result<bool> {
    let parsed: palyra_common::daemon_config_schema::RootFileConfig =
        document
            .clone()
            .try_into()
            .context("failed to parse config for memory embeddings onboarding check")?;
    Ok(crate::memory_embeddings_model_configured(&parsed))
}

fn memory_embeddings_onboarding_message(
    document: &toml::Value,
    provider_kind: &str,
    chat_model: Option<&str>,
    configured: bool,
) -> String {
    if configured {
        return "An embeddings-capable provider/model is configured for semantic memory recall."
            .to_owned();
    }
    if minimax_chat_provider_configured(document, provider_kind, chat_model) {
        return "MiniMax chat is configured, but no embeddings-capable provider/model is selected; memory recall remains usable with hash fallback, but semantic recall quality is degraded until you configure an OpenAI-compatible embeddings provider/model, run `palyra models set-embeddings <model>`, restart the gateway, and run `palyra memory index --until-complete`."
            .to_owned();
    }
    "No embeddings-capable provider/model is selected; memory recall remains usable with hash fallback, but semantic recall quality is degraded until you configure embeddings, restart the gateway, and run `palyra memory index --until-complete`."
        .to_owned()
}

fn browser_prerequisites_status(document: &toml::Value) -> (bool, String) {
    let enabled = get_bool_at_path(document, "tool_call.browser_service.enabled").unwrap_or(false);
    let auth_configured = get_string_at_path(document, "tool_call.browser_service.auth_token")
        .is_some()
        || value_present_at_path(document, "tool_call.browser_service.auth_token_secret_ref");
    let state_key_configured =
        get_string_at_path(document, "tool_call.browser_service.state_key_vault_ref").is_some()
            || value_present_at_path(document, "tool_call.browser_service.state_key_secret_ref");

    if enabled && auth_configured && state_key_configured {
        return (
            true,
            "Browser service prerequisites are configured for local browser-backed agent workflows."
                .to_owned(),
        );
    }

    let mut missing = Vec::new();
    if !enabled {
        missing.push("tool_call.browser_service.enabled");
    }
    if !auth_configured {
        missing.push("tool_call.browser_service.auth_token");
    }
    if !state_key_configured {
        missing.push("tool_call.browser_service.state_key_vault_ref");
    }
    (
        false,
        format!(
            "Browser-backed agent workflows need local browser prerequisites before onboarding is complete; missing {}.",
            missing.join(", ")
        ),
    )
}

fn value_present_at_path(document: &toml::Value, key: &str) -> bool {
    get_value_at_path(document, key).ok().flatten().is_some()
}

/// Probes browserd reachability: an authenticated gRPC call when the token is inline
/// in the config, otherwise a plain TCP connect (vault-stored tokens are not resolved
/// by the status command).
fn browser_runtime_status(document: &toml::Value) -> (bool, String) {
    let endpoint = get_string_at_path(document, "tool_call.browser_service.endpoint")
        .unwrap_or_else(|| DEFAULT_BROWSER_SERVICE_ENDPOINT.to_owned());
    let display_endpoint = diagnostic_endpoint_url(endpoint.as_str());
    if let Some(auth_token) = get_string_at_path(document, "tool_call.browser_service.auth_token") {
        return authenticated_browser_runtime_status(endpoint, auth_token, display_endpoint);
    }
    match tcp_url_reachable(
        endpoint.as_str(),
        Duration::from_millis(BROWSER_RUNTIME_CONNECT_TIMEOUT_MS),
        "browser service gRPC",
    ) {
        Ok(()) => (
            true,
            format!(
                "browserd gRPC endpoint {display_endpoint} is reachable; authenticated browser probe was skipped because the browser service token is not available inline to onboarding status."
            ),
        ),
        Err(error) => (
            false,
            format!(
                "browserd gRPC endpoint {display_endpoint} is not reachable: {}",
                sanitize_diagnostic_error(error.to_string().as_str())
            ),
        ),
    }
}

fn authenticated_browser_runtime_status(
    endpoint: String,
    auth_token: String,
    display_endpoint: String,
) -> (bool, String) {
    match run_blocking_auth_probe(browser_runtime_auth_probe(endpoint, auth_token)) {
        Ok(()) => (
            true,
            format!("browserd gRPC endpoint {display_endpoint} accepted the configured browser service token."),
        ),
        Err(error) => (
            false,
            format!(
                "browserd gRPC endpoint {display_endpoint} failed authenticated readiness probe: {}",
                sanitize_diagnostic_error(error.to_string().as_str())
            ),
        ),
    }
}

async fn browser_runtime_auth_probe(endpoint: String, auth_token: String) -> Result<()> {
    let channel = tonic::transport::Endpoint::from_shared(endpoint.clone())
        .with_context(|| format!("invalid browser gRPC URL {endpoint}"))?
        .connect()
        .await
        .with_context(|| format!("failed to connect browser service {endpoint}"))?;
    let mut client = browser_v1::browser_service_client::BrowserServiceClient::new(channel);
    let mut request = tonic::Request::new(browser_v1::ListSessionsRequest {
        v: CANONICAL_PROTOCOL_MAJOR,
        principal: String::new(),
        limit: 1,
    });
    request.metadata_mut().insert(
        "authorization",
        format!("Bearer {auth_token}")
            .parse()
            .context("invalid browser service authorization metadata")?,
    );
    request.metadata_mut().insert(
        "x-palyra-principal",
        BROWSER_AUTH_PROBE_PRINCIPAL
            .parse()
            .context("invalid browser caller principal metadata")?,
    );
    client.list_sessions(request).await.context("failed to call browser ListSessions")?;
    Ok(())
}

/// Detects a MiniMax chat setup, which rides the Anthropic-compatible provider kind
/// and is identified by its auth provider kind or a MiniMax model name; such setups
/// get a dedicated embeddings hint because MiniMax offers no embeddings endpoint here.
fn minimax_chat_provider_configured(
    document: &toml::Value,
    provider_kind: &str,
    chat_model: Option<&str>,
) -> bool {
    let auth_provider_kind = get_string_at_path(document, "model_provider.auth_provider_kind")
        .unwrap_or_default()
        .to_ascii_lowercase();
    provider_kind.eq_ignore_ascii_case("anthropic")
        && (auth_provider_kind == "minimax"
            || chat_model
                .map(|model| model.to_ascii_lowercase().contains("minimax"))
                .unwrap_or(false))
}

fn gateway_runtime_status() -> (bool, String) {
    let Some(context) = app::current_root_context() else {
        return (
            false,
            "CLI root context is unavailable; start the gateway with `palyra gateway run` after setup"
                .to_owned(),
        );
    };
    let connection = match context
        .resolve_grpc_connection(app::ConnectionOverrides::default(), app::ConnectionDefaults::USER)
    {
        Ok(connection) => connection,
        Err(error) => {
            return (
                false,
                format!(
                    "failed to resolve gateway gRPC endpoint: {}",
                    sanitize_diagnostic_error(error.to_string().as_str())
                ),
            );
        }
    };
    let display_grpc_url = diagnostic_endpoint_url(connection.grpc_url.as_str());
    match run_blocking_auth_probe(gateway_runtime_auth_probe(connection)) {
        Ok(()) => (
            true,
            format!(
                "gateway gRPC endpoint {display_grpc_url} accepted the configured authorization token"
            ),
        ),
        Err(error) => (
            false,
            format!(
                "gateway gRPC endpoint {display_grpc_url} failed authenticated readiness probe: {}",
                sanitize_diagnostic_error(error.to_string().as_str())
            ),
        ),
    }
}

async fn gateway_runtime_auth_probe(connection: AgentConnection) -> Result<()> {
    let mut client = client::runtime::GatewayRuntimeClient::connect(connection).await?;
    client.list_agents(None, Some(1)).await.map(|_| ())
}

fn run_blocking_auth_probe<F>(probe: F) -> Result<()>
where
    F: std::future::Future<Output = Result<()>>,
{
    run_blocking_probe(probe)
}

/// Runs an async probe on a fresh runtime with the shared probe timeout so a hung
/// endpoint cannot stall the synchronous status command.
fn run_blocking_probe<F, T>(probe: F) -> Result<T>
where
    F: std::future::Future<Output = Result<T>>,
{
    let runtime = client::grpc::build_runtime()?;
    match runtime.block_on(async {
        tokio::time::timeout(Duration::from_millis(AUTHENTICATED_RUNTIME_PROBE_TIMEOUT_MS), probe)
            .await
    }) {
        Ok(result) => result,
        Err(_) => anyhow::bail!(
            "authenticated readiness probe timed out after {} ms",
            AUTHENTICATED_RUNTIME_PROBE_TIMEOUT_MS
        ),
    }
}

fn diagnostic_endpoint_url(raw_url: &str) -> String {
    redact_url_strict(raw_url)
}

/// Checks whether any resolved socket address of `raw_url` accepts a TCP connection
/// within `timeout`.
///
/// # Errors
/// Returns an error for invalid URLs, DNS failures, or when no address connects.
fn tcp_url_reachable(raw_url: &str, timeout: Duration, endpoint_label: &str) -> Result<()> {
    let url = reqwest::Url::parse(raw_url)
        .with_context(|| format!("{endpoint_label} URL is invalid: {raw_url}"))?;
    let host = url.host_str().ok_or_else(|| anyhow!("{endpoint_label} URL must include a host"))?;
    let port = url
        .port_or_known_default()
        .ok_or_else(|| anyhow!("{endpoint_label} URL must include a port"))?;
    let addresses = (host, port)
        .to_socket_addrs()
        .with_context(|| format!("failed to resolve {endpoint_label} endpoint {host}:{port}"))?;
    let mut last_error = None;
    for address in addresses {
        match TcpStream::connect_timeout(&address, timeout) {
            Ok(stream) => {
                drop(stream);
                return Ok(());
            }
            Err(error) => last_error = Some(error),
        }
    }
    if let Some(error) = last_error {
        return Err(error)
            .with_context(|| format!("failed to connect {endpoint_label} {host}:{port}"));
    }
    anyhow::bail!("{endpoint_label} {host}:{port} resolved no socket addresses")
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

fn default_agent_create_command(signals: &OnboardingSignals) -> String {
    let args = default_agent_create_args(signals);
    render_cli_command(args.as_slice())
}

fn default_agent_create_args(signals: &OnboardingSignals) -> Vec<String> {
    let mut parts = vec![
        "palyra".to_owned(),
        "agents".to_owned(),
        "create".to_owned(),
        "local-default".to_owned(),
        "--display-name".to_owned(),
        "LocalDefaultAgent".to_owned(),
        "--set-default".to_owned(),
    ];
    if let Some(workspace_root) = signals.workspace_root.as_deref() {
        parts.push("--workspace-root".to_owned());
        parts.push(workspace_root.to_owned());
        if looks_absolute_path(workspace_root) {
            parts.push("--allow-absolute-paths".to_owned());
        }
    }
    if let Some(model) = signals.chat_model.as_deref() {
        parts.push("--model-profile".to_owned());
        parts.push(model.to_owned());
    }
    parts
}

fn render_cli_command(args: &[String]) -> String {
    if cfg!(windows) && args.iter().any(|arg| !is_unquoted_cli_arg(arg.as_str())) {
        return powershell_encoded_cli_command(args);
    }
    args.iter().map(|arg| quote_cli_arg(arg.as_str())).collect::<Vec<_>>().join(" ")
}

fn looks_absolute_path(value: &str) -> bool {
    // Recognize Windows drive ("C:...") and UNC ("\\server") forms explicitly so the
    // suggested command stays correct even when this CLI build runs on a non-Windows
    // host inspecting a Windows-style configured workspace root.
    Path::new(value).is_absolute()
        || value.starts_with(r"\\")
        || value.as_bytes().get(1).is_some_and(|byte| *byte == b':')
}

/// Quotes a config-derived value for inclusion in a suggested shell command.
///
/// POSIX single quotes are used so `$()`, backticks, and variable expansion cannot execute if
/// the operator pastes the command into a POSIX shell. Windows commands with unsafe
/// characters are rendered via [`powershell_encoded_cli_command`] instead.
fn quote_cli_arg(value: &str) -> String {
    if is_unquoted_cli_arg(value) {
        value.to_owned()
    } else {
        quote_single_quoted_cli_arg(value)
    }
}

fn quote_single_quoted_cli_arg(value: &str) -> String {
    let mut quoted = String::with_capacity(value.len() + 2);
    quoted.push('\'');
    for ch in value.chars() {
        if ch == '\'' {
            quoted.push_str("'\\''");
        } else {
            quoted.push(ch);
        }
    }
    quoted.push('\'');
    quoted
}

fn is_unquoted_cli_arg(value: &str) -> bool {
    !value.is_empty()
        && value.chars().all(|ch| {
            ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | '.' | '/' | '\\' | ':')
        })
}

fn powershell_encoded_cli_command(args: &[String]) -> String {
    let script = powershell_invocation_script(args);
    let encoded = BASE64_STANDARD.encode(utf16le_bytes(script.as_str()));
    format!("powershell.exe -NoProfile -NonInteractive -EncodedCommand \"{encoded}\"")
}

fn powershell_invocation_script(args: &[String]) -> String {
    let Some((program, rest)) = args.split_first() else {
        return String::new();
    };
    let mut script = format!("& {}", quote_powershell_single_quoted_arg(program));
    for arg in rest {
        script.push(' ');
        script.push_str(quote_powershell_single_quoted_arg(arg).as_str());
    }
    script
}

fn quote_powershell_single_quoted_arg(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

fn utf16le_bytes(value: &str) -> Vec<u8> {
    value.encode_utf16().flat_map(u16::to_le_bytes).collect()
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
    Ok(get_string_at_path(document, "model_provider.openai_api_key").is_some()
        || get_string_at_path(document, "model_provider.openai_api_key_vault_ref").is_some()
        || get_value_at_path(document, "model_provider.openai_api_key_secret_ref")?.is_some()
        || get_string_at_path(document, "model_provider.anthropic_api_key").is_some()
        || get_string_at_path(document, "model_provider.anthropic_api_key_vault_ref").is_some()
        || get_value_at_path(document, "model_provider.anthropic_api_key_secret_ref")?.is_some()
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

fn onboarding_flow_label(flow: control_plane::OnboardingFlow) -> &'static str {
    match flow {
        control_plane::OnboardingFlow::QuickStart => "quick_start",
        control_plane::OnboardingFlow::AdvancedSetup => "advanced_setup",
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

#[cfg(test)]
mod tests {
    use std::{fs, time::Duration};

    use anyhow::Result;
    use palyra_control_plane as control_plane;
    use tempfile::tempdir;

    use super::{
        browser_prerequisites_status, browser_runtime_status, build_onboarding_counts,
        build_onboarding_steps, cli_contract_descriptor, collect_onboarding_signals,
        default_agent_create_command, default_agent_status_from_agent_ids, derive_posture_status,
        diagnostic_endpoint_url, load_onboarding_document, onboarding_prerequisites_ready,
        onboarding_status_summary_line, powershell_invocation_script, quote_cli_arg,
        recommended_onboarding_step_id, record_cli_first_success, tcp_url_reachable,
        OnboardingSignals, OnboardingVariant,
    };
    use crate::{app, args::RootOptions};

    #[test]
    fn default_agent_status_accepts_runtime_registry_default() {
        let (configured, message) = default_agent_status_from_agent_ids(
            Some("local-default"),
            ["local-default", "other-agent"],
            "gateway runtime",
        );

        assert!(configured);
        assert!(message.contains("gateway runtime"));
        assert!(message.contains("local-default"));
    }

    #[test]
    fn onboarding_status_uses_active_root_context_config_path() -> Result<()> {
        let _guard = app::test_env_lock_for_tests().lock().expect("env lock");
        app::clear_root_context_for_tests();

        let temp = tempdir()?;
        let state_root = temp.path().join("state-root");
        fs::create_dir_all(&state_root)?;
        let config_path = temp.path().join("portable").join("palyra.toml");
        fs::create_dir_all(config_path.parent().expect("config parent"))?;
        fs::write(
            &config_path,
            r#"
[model_provider]
kind = "anthropic"
"#,
        )?;

        let _context = app::install_root_context(RootOptions {
            config_path: Some(config_path.display().to_string()),
            state_root: Some(state_root.display().to_string()),
            ..RootOptions::default()
        })?;

        let (_document, resolved_path) = load_onboarding_document(None)?;
        assert_eq!(resolved_path, config_path.display().to_string());

        app::clear_root_context_for_tests();
        Ok(())
    }

    #[test]
    fn onboarding_diagnostic_endpoint_url_redacts_credentials() {
        let redacted = diagnostic_endpoint_url(
            "http://user:supersecret@127.0.0.1:7443?token=LEAKED_TOKEN&mode=ok",
        );

        assert!(redacted.contains("127.0.0.1:7443"));
        assert!(redacted.contains("mode=ok"));
        assert!(!redacted.contains("user:supersecret"));
        assert!(!redacted.contains("LEAKED_TOKEN"));
    }

    #[test]
    fn onboarding_status_summary_line_distinguishes_flow_and_variant() {
        let payload = control_plane::OnboardingPostureEnvelope {
            contract: cli_contract_descriptor(),
            flow: control_plane::OnboardingFlow::AdvancedSetup,
            flow_variant: "remote".to_owned(),
            status: control_plane::OnboardingPostureState::Ready,
            config_path: "C:/portable/palyra.toml".to_owned(),
            resume_supported: true,
            ready_for_first_success: true,
            recommended_step_id: None,
            first_success_hint: None,
            counts: control_plane::OnboardingStepCounts::default(),
            available_flows: Vec::new(),
            steps: Vec::new(),
        };

        let line = onboarding_status_summary_line(&payload);

        assert!(line.contains("flow=advanced_setup"), "{line}");
        assert!(line.contains("variant=remote"), "{line}");
        assert!(!line.contains("flow=remote"), "{line}");
    }

    #[test]
    fn tcp_url_reachable_errors_use_endpoint_label() {
        let error = tcp_url_reachable(
            "http://127.0.0.1:0",
            Duration::from_millis(10),
            "browserd gRPC endpoint",
        )
        .expect_err("port 0 should not accept a TCP connection");
        let message = format!("{error:#}");

        assert!(message.contains("failed to connect browserd gRPC endpoint"), "{message}");
        assert!(!message.contains("failed to connect gateway gRPC endpoint"), "{message}");
    }

    #[test]
    fn config_ready_action_points_to_existing_config_command() {
        let steps = build_onboarding_steps(
            OnboardingVariant::Quickstart,
            &OnboardingSignals {
                config_exists: true,
                config_path: "C:/portable/palyra.toml".to_owned(),
                workspace_root_configured: true,
                remote_base_url_configured: false,
                remote_verification_mode: None,
                remote_posture_safe: true,
                deployment_warning: None,
                provider_auth_configured: true,
                provider_model_selected: true,
                provider_health_state: "configured".to_owned(),
                provider_health_message: "configured".to_owned(),
                model_discovery_ready: true,
                model_discovery_message: "ready".to_owned(),
                gateway_runtime_reachable: true,
                gateway_runtime_message: "reachable".to_owned(),
                default_agent_configured: false,
                default_agent_message: "agent registry does not define a default agent".to_owned(),
                workspace_root: Some("C:/portable".to_owned()),
                chat_model: Some("MiniMax-M2.7".to_owned()),
                memory_embeddings_configured: false,
                memory_embeddings_message: "MiniMax chat is configured; memory uses hash fallback"
                    .to_owned(),
                browser_prerequisites_configured: true,
                browser_prerequisites_message: "browser ready".to_owned(),
                browser_runtime_reachable: true,
                browser_runtime_message: "browserd reachable".to_owned(),
                first_success_completed: false,
            },
        );
        let config_step = steps.iter().find(|step| step.step_id == "config").expect("config step");
        let action = config_step.action.as_ref().expect("config step action");
        assert_eq!(action.target, "palyra config list --path C:/portable/palyra.toml");
    }

    #[test]
    fn default_agent_create_command_uses_shell_stable_display_name() {
        let command = default_agent_create_command(&OnboardingSignals {
            config_exists: true,
            config_path: "C:/portable/palyra.toml".to_owned(),
            workspace_root_configured: true,
            remote_base_url_configured: false,
            remote_verification_mode: None,
            remote_posture_safe: true,
            deployment_warning: None,
            provider_auth_configured: true,
            provider_model_selected: true,
            provider_health_state: "configured".to_owned(),
            provider_health_message: "configured".to_owned(),
            model_discovery_ready: true,
            model_discovery_message: "ready".to_owned(),
            gateway_runtime_reachable: true,
            gateway_runtime_message: "reachable".to_owned(),
            default_agent_configured: false,
            default_agent_message: "agent registry does not define a default agent".to_owned(),
            workspace_root: Some("C:/portable".to_owned()),
            chat_model: Some("MiniMax-M2.7".to_owned()),
            memory_embeddings_configured: false,
            memory_embeddings_message: "MiniMax chat is configured; memory uses hash fallback"
                .to_owned(),
            browser_prerequisites_configured: true,
            browser_prerequisites_message: "browser ready".to_owned(),
            browser_runtime_reachable: true,
            browser_runtime_message: "browserd reachable".to_owned(),
            first_success_completed: false,
        });

        assert!(command.contains("--display-name LocalDefaultAgent"), "{command}");
        assert!(!command.contains("\"Local Default Agent\""), "{command}");
    }

    #[test]
    fn default_agent_create_command_quotes_config_values_without_substitution() {
        let command = default_agent_create_command(&OnboardingSignals {
            config_exists: true,
            config_path: "C:/portable/palyra.toml".to_owned(),
            workspace_root_configured: true,
            remote_base_url_configured: false,
            remote_verification_mode: None,
            remote_posture_safe: true,
            deployment_warning: None,
            provider_auth_configured: true,
            provider_model_selected: true,
            provider_health_state: "configured".to_owned(),
            provider_health_message: "configured".to_owned(),
            model_discovery_ready: true,
            model_discovery_message: "ready".to_owned(),
            gateway_runtime_reachable: true,
            gateway_runtime_message: "reachable".to_owned(),
            default_agent_configured: false,
            default_agent_message: "agent registry does not define a default agent".to_owned(),
            workspace_root: Some("C:/safe & calc & rem".to_owned()),
            chat_model: Some("x'; calc; #".to_owned()),
            memory_embeddings_configured: false,
            memory_embeddings_message: "memory fallback".to_owned(),
            browser_prerequisites_configured: true,
            browser_prerequisites_message: "browser ready".to_owned(),
            browser_runtime_reachable: true,
            browser_runtime_message: "browserd reachable".to_owned(),
            first_success_completed: false,
        });

        if cfg!(windows) {
            assert!(
                command.starts_with("powershell.exe -NoProfile -NonInteractive -EncodedCommand \""),
                "{command}"
            );
            assert!(!command.contains("C:/safe & calc & rem"), "{command}");
            assert!(!command.contains("x'; calc; #"), "{command}");
        } else {
            assert!(command.contains("--workspace-root 'C:/safe & calc & rem'"), "{command}");
            assert!(command.contains("--model-profile 'x'\\''; calc; #'"), "{command}");
        }
    }

    #[test]
    fn powershell_invocation_script_quotes_config_values_as_arguments() {
        let args = vec![
            "palyra".to_owned(),
            "agents".to_owned(),
            "create".to_owned(),
            "local-default".to_owned(),
            "--workspace-root".to_owned(),
            "C:/safe & calc & rem".to_owned(),
            "--model-profile".to_owned(),
            "x'; calc; #".to_owned(),
        ];
        let script = powershell_invocation_script(args.as_slice());

        assert_eq!(
            script,
            "& 'palyra' 'agents' 'create' 'local-default' '--workspace-root' 'C:/safe & calc & rem' '--model-profile' 'x''; calc; #'"
        );
    }

    #[test]
    fn quote_cli_arg_escapes_single_quotes_without_double_quote_substitution() {
        assert_eq!(quote_cli_arg(""), "''");
        assert_eq!(quote_cli_arg("safe/path-1"), "safe/path-1");
        assert_eq!(quote_cli_arg("x$(touch /tmp/pwn)"), "'x$(touch /tmp/pwn)'");
        assert_eq!(quote_cli_arg("x'$(touch /tmp/pwn)"), "'x'\\''$(touch /tmp/pwn)'");
    }

    #[test]
    fn model_selection_requires_real_prompt_before_ready_posture() {
        let steps = build_onboarding_steps(
            OnboardingVariant::Quickstart,
            &OnboardingSignals {
                config_exists: true,
                config_path: "C:/portable/palyra.toml".to_owned(),
                workspace_root_configured: true,
                remote_base_url_configured: false,
                remote_verification_mode: None,
                remote_posture_safe: true,
                deployment_warning: None,
                provider_auth_configured: true,
                provider_model_selected: true,
                provider_health_state: "configured".to_owned(),
                provider_health_message: "configured".to_owned(),
                model_discovery_ready: true,
                model_discovery_message: "model selection is present".to_owned(),
                gateway_runtime_reachable: true,
                gateway_runtime_message: "reachable".to_owned(),
                default_agent_configured: true,
                default_agent_message: "default agent `local-default` is configured".to_owned(),
                workspace_root: Some("C:/portable".to_owned()),
                chat_model: Some("MiniMax-M2.7".to_owned()),
                memory_embeddings_configured: false,
                memory_embeddings_message: "MiniMax chat is configured; memory uses hash fallback"
                    .to_owned(),
                browser_prerequisites_configured: true,
                browser_prerequisites_message: "browser ready".to_owned(),
                browser_runtime_reachable: true,
                browser_runtime_message: "browserd reachable".to_owned(),
                first_success_completed: false,
            },
        );
        let verification = steps
            .iter()
            .find(|step| step.step_id == "model_verification")
            .expect("model verification step");
        let first_success =
            steps.iter().find(|step| step.step_id == "first_success").expect("first_success step");

        assert_eq!(verification.status, control_plane::OnboardingStepStatus::InProgress);
        assert_eq!(verification.verification_state.as_deref(), Some("prompt_required"));
        assert_eq!(
            verification.action.as_ref().map(|action| action.target.as_str()),
            Some(
                "echo Reply exactly PALYRA_ONBOARDING_OK | palyra agent run --session-key onboarding-smoke --reset-session --prompt-stdin"
            )
        );
        assert_eq!(first_success.status, control_plane::OnboardingStepStatus::Blocked);
        assert_eq!(
            derive_posture_status(&steps, onboarding_prerequisites_ready(&steps), false),
            control_plane::OnboardingPostureState::Blocked
        );
    }

    #[test]
    fn default_agent_step_starts_gateway_before_daemon_backed_create() {
        let steps = build_onboarding_steps(
            OnboardingVariant::Quickstart,
            &OnboardingSignals {
                config_exists: true,
                config_path: "C:/portable/palyra.toml".to_owned(),
                workspace_root_configured: true,
                remote_base_url_configured: false,
                remote_verification_mode: None,
                remote_posture_safe: true,
                deployment_warning: None,
                provider_auth_configured: true,
                provider_model_selected: true,
                provider_health_state: "configured".to_owned(),
                provider_health_message: "configured".to_owned(),
                model_discovery_ready: true,
                model_discovery_message: "ready".to_owned(),
                gateway_runtime_reachable: false,
                gateway_runtime_message:
                    "gateway gRPC endpoint http://127.0.0.1:7443 is not reachable".to_owned(),
                default_agent_configured: false,
                default_agent_message: "agent registry does not define a default agent".to_owned(),
                workspace_root: Some("C:/portable".to_owned()),
                chat_model: Some("MiniMax-M2.7".to_owned()),
                memory_embeddings_configured: false,
                memory_embeddings_message: "MiniMax chat is configured; memory uses hash fallback"
                    .to_owned(),
                browser_prerequisites_configured: true,
                browser_prerequisites_message: "browser ready".to_owned(),
                browser_runtime_reachable: true,
                browser_runtime_message: "browserd reachable".to_owned(),
                first_success_completed: false,
            },
        );
        let agent_step =
            steps.iter().find(|step| step.step_id == "agent_identity").expect("agent step");
        let action = agent_step.action.as_ref().expect("agent step action");

        assert_eq!(agent_step.status, control_plane::OnboardingStepStatus::Blocked);
        assert_eq!(action.target, "palyra gateway run");
        assert_eq!(agent_step.verification_state.as_deref(), Some("gateway_not_running"));
        assert_eq!(
            agent_step.blocked.as_ref().map(|blocked| blocked.code.as_str()),
            Some("gateway_not_running")
        );
    }

    #[test]
    fn browser_step_blocks_when_prerequisites_exist_but_browserd_is_down() {
        let steps = build_onboarding_steps(
            OnboardingVariant::Quickstart,
            &OnboardingSignals {
                config_exists: true,
                config_path: "C:/portable/palyra.toml".to_owned(),
                workspace_root_configured: true,
                remote_base_url_configured: false,
                remote_verification_mode: None,
                remote_posture_safe: true,
                deployment_warning: None,
                provider_auth_configured: true,
                provider_model_selected: true,
                provider_health_state: "configured".to_owned(),
                provider_health_message: "configured".to_owned(),
                model_discovery_ready: true,
                model_discovery_message: "ready".to_owned(),
                gateway_runtime_reachable: true,
                gateway_runtime_message: "reachable".to_owned(),
                default_agent_configured: true,
                default_agent_message: "default agent `local-default` is configured".to_owned(),
                workspace_root: Some("C:/portable".to_owned()),
                chat_model: Some("MiniMax-M2.7".to_owned()),
                memory_embeddings_configured: false,
                memory_embeddings_message: "MiniMax chat is configured; memory uses hash fallback"
                    .to_owned(),
                browser_prerequisites_configured: true,
                browser_prerequisites_message: "browser prerequisites configured".to_owned(),
                browser_runtime_reachable: false,
                browser_runtime_message:
                    "browserd gRPC endpoint http://127.0.0.1:7543 is not reachable".to_owned(),
                first_success_completed: false,
            },
        );
        let browser_step =
            steps.iter().find(|step| step.step_id == "browser_harness").expect("browser step");

        assert_eq!(browser_step.status, control_plane::OnboardingStepStatus::Blocked);
        assert_eq!(
            browser_step.action.as_ref().map(|action| action.target.as_str()),
            Some("palyra browser start --wait-ms 20000 --json")
        );
        assert_eq!(browser_step.verification_state.as_deref(), Some("browserd_not_running"));
        assert_eq!(recommended_onboarding_step_id(&steps).as_deref(), Some("browser_harness"));
    }

    #[test]
    fn browser_runtime_status_uses_authenticated_probe_for_inline_token() -> Result<()> {
        let document: toml::Value = toml::from_str(
            r#"
[tool_call.browser_service]
endpoint = "not-a-url"
auth_token = "browser-token"
"#,
        )?;

        let (reachable, message) = browser_runtime_status(&document);

        assert!(!reachable);
        assert!(message.contains("failed authenticated readiness probe"));
        Ok(())
    }

    #[test]
    fn browser_prerequisites_accept_structured_secret_references() -> Result<()> {
        let document: toml::Value = toml::from_str(
            r#"
[tool_call.browser_service]
enabled = true

[tool_call.browser_service.auth_token_secret_ref]
kind = "env"
variable = "PALYRA_BROWSER_SERVICE_AUTH_TOKEN"

[tool_call.browser_service.state_key_secret_ref]
kind = "file"
path = "secrets/browserd.key"
trusted_dirs = ["secrets"]
"#,
        )?;

        let (configured, message) = browser_prerequisites_status(&document);

        assert!(configured, "{message}");
        Ok(())
    }

    #[test]
    fn onboarding_signals_accept_inline_minimax_auth() -> Result<()> {
        let _guard = app::test_env_lock_for_tests().lock().expect("env lock");
        app::clear_root_context_for_tests();

        let temp = tempdir()?;
        let config_path = temp.path().join("config").join("palyra.toml");
        let config = r#"
[model_provider]
kind = "anthropic"
auth_provider_kind = "minimax"
anthropic_model = "MiniMax-M2.7"
anthropic_api_key = "sk-inline-minimax"
"#;
        fs::create_dir_all(config_path.parent().expect("config parent"))?;
        fs::write(config_path.as_path(), config)?;
        let document: toml::Value = toml::from_str(config)?;

        let signals = collect_onboarding_signals(
            &document,
            config_path.display().to_string(),
            OnboardingVariant::Quickstart,
        )?;
        let steps = build_onboarding_steps(OnboardingVariant::Quickstart, &signals);
        let provider_step =
            steps.iter().find(|step| step.step_id == "provider_auth").expect("provider step");
        let memory_step = steps
            .iter()
            .find(|step| step.step_id == "memory_embeddings")
            .expect("memory embeddings step");

        assert!(signals.provider_auth_configured);
        assert_eq!(signals.provider_health_state, "configured");
        assert!(!signals.memory_embeddings_configured);
        assert!(signals.memory_embeddings_message.contains("MiniMax chat"));
        assert!(signals.memory_embeddings_message.contains("hash fallback"));
        assert!(signals.memory_embeddings_message.contains("semantic recall quality is degraded"));
        assert!(signals.memory_embeddings_message.contains("palyra models set-embeddings"));
        assert_eq!(provider_step.status, control_plane::OnboardingStepStatus::Done);
        assert_eq!(provider_step.verification_state.as_deref(), Some("configured"));
        assert_eq!(memory_step.status, control_plane::OnboardingStepStatus::InProgress);
        assert!(memory_step.optional);
        assert_eq!(memory_step.verification_state.as_deref(), Some("degraded_hash_fallback"));
        assert!(memory_step.summary.contains("palyra memory index --until-complete"));

        app::clear_root_context_for_tests();
        Ok(())
    }

    #[test]
    fn cli_first_success_marker_completes_first_success_step() -> Result<()> {
        let _guard = app::test_env_lock_for_tests().lock().expect("env lock");
        app::clear_root_context_for_tests();

        let temp = tempdir()?;
        let state_root = temp.path().join("state-root");
        let config_path = temp.path().join("config").join("palyra.toml");
        let config = r#"
[model_provider]
kind = "anthropic"
anthropic_model = "MiniMax-M2.7"
anthropic_api_key_vault_ref = "global/minimax_api_key"

[tool_call.browser_service]
enabled = true
auth_token = "browser-token"
state_key_vault_ref = "global/browser_state_key"
"#;
        fs::create_dir_all(config_path.parent().expect("config parent"))?;
        fs::write(config_path.as_path(), config)?;
        record_cli_first_success(&state_root, "01ARZ3NDEKTSV4RRFFQ69G5FAV")?;
        fs::write(
            state_root.join("agents.toml"),
            r#"
version = 1
default_agent_id = "local-default"

[[agents]]
agent_id = "local-default"
"#,
        )?;

        let _context = app::install_root_context(RootOptions {
            config_path: Some(config_path.display().to_string()),
            state_root: Some(state_root.display().to_string()),
            ..RootOptions::default()
        })?;
        let document: toml::Value = toml::from_str(config)?;
        let mut signals = collect_onboarding_signals(
            &document,
            config_path.display().to_string(),
            OnboardingVariant::Quickstart,
        )?;
        signals.browser_runtime_reachable = true;
        signals.browser_runtime_message = "browserd gRPC endpoint is reachable".to_owned();
        let steps = build_onboarding_steps(OnboardingVariant::Quickstart, &signals);
        let first_success =
            steps.iter().find(|step| step.step_id == "first_success").expect("first_success step");
        let memory_step = steps
            .iter()
            .find(|step| step.step_id == "memory_embeddings")
            .expect("memory embeddings step");
        let counts = build_onboarding_counts(&steps);

        assert_eq!(first_success.status, control_plane::OnboardingStepStatus::Done);
        assert_eq!(memory_step.status, control_plane::OnboardingStepStatus::Skipped);
        assert!(memory_step.optional);
        assert_eq!(counts.in_progress, 0);
        assert_eq!(recommended_onboarding_step_id(&steps), None);
        assert_eq!(
            derive_posture_status(&steps, onboarding_prerequisites_ready(&steps), true),
            control_plane::OnboardingPostureState::Complete
        );

        app::clear_root_context_for_tests();
        Ok(())
    }

    #[test]
    fn first_success_marker_does_not_complete_without_browser_prerequisites() -> Result<()> {
        let _guard = app::test_env_lock_for_tests().lock().expect("env lock");
        app::clear_root_context_for_tests();

        let temp = tempdir()?;
        let state_root = temp.path().join("state-root");
        let config_path = temp.path().join("config").join("palyra.toml");
        let config = r#"
[model_provider]
kind = "anthropic"
anthropic_model = "MiniMax-M2.7"
anthropic_api_key_vault_ref = "global/minimax_api_key"
"#;
        fs::create_dir_all(config_path.parent().expect("config parent"))?;
        fs::write(config_path.as_path(), config)?;
        record_cli_first_success(&state_root, "01ARZ3NDEKTSV4RRFFQ69G5FAV")?;
        fs::write(
            state_root.join("agents.toml"),
            r#"
version = 1
default_agent_id = "local-default"

[[agents]]
agent_id = "local-default"
"#,
        )?;

        let _context = app::install_root_context(RootOptions {
            config_path: Some(config_path.display().to_string()),
            state_root: Some(state_root.display().to_string()),
            ..RootOptions::default()
        })?;
        let document: toml::Value = toml::from_str(config)?;
        let signals = collect_onboarding_signals(
            &document,
            config_path.display().to_string(),
            OnboardingVariant::Quickstart,
        )?;
        let steps = build_onboarding_steps(OnboardingVariant::Quickstart, &signals);
        let browser_step =
            steps.iter().find(|step| step.step_id == "browser_harness").expect("browser step");

        assert_eq!(browser_step.status, control_plane::OnboardingStepStatus::Todo);
        assert_eq!(recommended_onboarding_step_id(&steps).as_deref(), Some("browser_harness"));
        assert_eq!(
            derive_posture_status(&steps, onboarding_prerequisites_ready(&steps), false),
            control_plane::OnboardingPostureState::Blocked
        );

        app::clear_root_context_for_tests();
        Ok(())
    }

    #[test]
    fn onboarding_signals_require_existing_config_file_for_config_ready() -> Result<()> {
        let temp = tempdir()?;
        let missing_config = temp.path().join("missing").join("palyra.toml");
        let document = toml::Value::Table(Default::default());

        let signals = collect_onboarding_signals(
            &document,
            missing_config.display().to_string(),
            OnboardingVariant::Quickstart,
        )?;

        assert!(!signals.config_exists);
        Ok(())
    }

    #[test]
    fn onboarding_status_treats_empty_config_as_not_ready() -> Result<()> {
        let temp = tempdir()?;
        let config_path = temp.path().join("config").join("palyra.toml");
        fs::create_dir_all(config_path.parent().expect("config parent"))?;
        fs::write(config_path.as_path(), "")?;

        let (document, resolved_path) =
            load_onboarding_document(Some(config_path.display().to_string()))?;
        let signals =
            collect_onboarding_signals(&document, resolved_path, OnboardingVariant::Quickstart)?;
        let steps = build_onboarding_steps(OnboardingVariant::Quickstart, &signals);
        let config_step = steps.iter().find(|step| step.step_id == "config").expect("config step");

        assert!(!signals.config_exists);
        assert_eq!(config_step.status, control_plane::OnboardingStepStatus::Todo);
        assert_eq!(config_step.title, "Create config");
        Ok(())
    }
}
