use std::{
    fs,
    io::Write,
    net::{TcpListener, UdpSocket},
    path::{Path, PathBuf},
};

use anyhow::Result;
use serde::Serialize;

use super::desktop_state::{
    DesktopDiscordOnboardingState, DesktopOnboardingEvent, DesktopOnboardingFailureState,
};
use super::features::onboarding::connectors::discord::{
    derive_discord_onboarding_summary, discord_connect_detail, DesktopDiscordOnboardingSummary,
};
use super::openai_auth::{
    load_openai_auth_status, OpenAiAuthStatusSnapshot, OpenAiControlPlaneInputs,
};
use super::snapshot::{
    build_control_plane_client, build_snapshot_from_inputs, ensure_console_session,
    sanitize_log_line, ControlCenterSnapshot, SnapshotBuildInputs,
};
use super::{
    normalize_optional_text, resolve_binary_path, unix_ms_now, ControlCenter,
    DesktopOnboardingStep, LOOPBACK_HOST,
};

#[derive(Debug, Clone, Serialize)]
pub(crate) struct OnboardingPreflightCheck {
    pub(crate) key: String,
    pub(crate) label: String,
    pub(crate) status: String,
    pub(crate) detail: String,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct OnboardingPreflightSnapshot {
    pub(crate) blocked_count: usize,
    pub(crate) warning_count: usize,
    pub(crate) checks: Vec<OnboardingPreflightCheck>,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct OnboardingStepSnapshot {
    pub(crate) key: DesktopOnboardingStep,
    pub(crate) title: String,
    pub(crate) status: String,
    pub(crate) detail: String,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct OnboardingOperatorAuthSnapshot {
    pub(crate) ready: bool,
    pub(crate) note: String,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct OnboardingRecoverySnapshot {
    pub(crate) step: DesktopOnboardingStep,
    pub(crate) message: String,
    pub(crate) recorded_at_unix_ms: i64,
    pub(crate) suggested_actions: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct OnboardingStepFailureMetric {
    pub(crate) step: String,
    pub(crate) failures: u64,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct OnboardingSupportBundleMetrics {
    pub(crate) attempts: u64,
    pub(crate) successes: u64,
    pub(crate) failures: u64,
    pub(crate) success_rate_bps: u32,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct OnboardingStatusSnapshot {
    pub(crate) flow_id: String,
    pub(crate) phase: String,
    pub(crate) current_step: DesktopOnboardingStep,
    pub(crate) current_step_title: String,
    pub(crate) current_step_detail: String,
    pub(crate) progress_completed: usize,
    pub(crate) progress_total: usize,
    pub(crate) state_root_path: String,
    pub(crate) default_state_root_path: String,
    pub(crate) state_root_confirmed: bool,
    pub(crate) state_root_overridden: bool,
    pub(crate) dashboard_url: String,
    pub(crate) dashboard_access_mode: String,
    pub(crate) dashboard_remote_trust_state: String,
    pub(crate) dashboard_remote_verification_mode: Option<String>,
    pub(crate) dashboard_reachable: bool,
    pub(crate) dashboard_handoff_completed: bool,
    pub(crate) completion_unix_ms: Option<i64>,
    pub(crate) preflight: OnboardingPreflightSnapshot,
    pub(crate) operator_auth: OnboardingOperatorAuthSnapshot,
    pub(crate) openai_ready: bool,
    pub(crate) openai_default_profile_id: Option<String>,
    pub(crate) openai_note: Option<String>,
    pub(crate) discord_ready: bool,
    pub(crate) discord_verified: bool,
    pub(crate) discord_last_verified_target: Option<String>,
    pub(crate) discord_last_verified_at_unix_ms: Option<i64>,
    pub(crate) discord_defaults: DesktopDiscordOnboardingState,
    pub(crate) recovery: Option<OnboardingRecoverySnapshot>,
    pub(crate) failure_step_counts: Vec<OnboardingStepFailureMetric>,
    pub(crate) support_bundle_exports: OnboardingSupportBundleMetrics,
    pub(crate) recent_events: Vec<DesktopOnboardingEvent>,
    pub(crate) steps: Vec<OnboardingStepSnapshot>,
}

#[derive(Debug, Serialize)]
pub(crate) struct DesktopRefreshPayload {
    pub(crate) snapshot: ControlCenterSnapshot,
    pub(crate) onboarding_status: OnboardingStatusSnapshot,
    pub(crate) openai_status: OpenAiAuthStatusSnapshot,
}

#[derive(Debug)]
pub(crate) struct OnboardingStatusInputs {
    pub(crate) snapshot_inputs: SnapshotBuildInputs,
    pub(crate) openai_inputs: OpenAiControlPlaneInputs,
    pub(crate) persisted: super::DesktopStateFile,
    pub(crate) runtime_root: PathBuf,
    pub(crate) default_runtime_root: PathBuf,
    pub(crate) admin_token: String,
    pub(crate) runtime: super::RuntimeConfig,
    pub(crate) http_client: reqwest::Client,
    pub(crate) gateway_bound_ports: Vec<u16>,
    pub(crate) browser_bound_ports: Vec<u16>,
    pub(crate) gateway_running: bool,
    pub(crate) browser_running: bool,
    pub(crate) browser_service_enabled: bool,
}

impl ControlCenter {
    pub(crate) fn capture_onboarding_status_inputs(&mut self) -> OnboardingStatusInputs {
        OnboardingStatusInputs {
            snapshot_inputs: self.capture_snapshot_inputs(),
            openai_inputs: OpenAiControlPlaneInputs::capture(self),
            persisted: self.persisted.clone(),
            runtime_root: self.runtime_root.clone(),
            default_runtime_root: self.default_runtime_root.clone(),
            admin_token: self.admin_token.clone(),
            runtime: self.runtime.clone(),
            http_client: self.http_client.clone(),
            gateway_bound_ports: self.gateway.bound_ports.clone(),
            browser_bound_ports: self.browserd.bound_ports.clone(),
            gateway_running: self.gateway.running(),
            browser_running: self.browserd.running(),
            browser_service_enabled: self.persisted.browser_service_enabled,
        }
    }
}

pub(crate) async fn build_onboarding_status(
    inputs: OnboardingStatusInputs,
) -> Result<OnboardingStatusSnapshot> {
    Ok(build_desktop_refresh_payload(inputs).await?.onboarding_status)
}

pub(crate) async fn build_desktop_refresh_payload(
    inputs: OnboardingStatusInputs,
) -> Result<DesktopRefreshPayload> {
    let OnboardingStatusInputs {
        snapshot_inputs,
        openai_inputs,
        persisted,
        runtime_root,
        default_runtime_root,
        admin_token,
        runtime,
        http_client,
        gateway_bound_ports,
        browser_bound_ports,
        gateway_running,
        browser_running,
        browser_service_enabled,
    } = inputs;

    let (snapshot, openai_status) = tokio::join!(
        build_snapshot_from_inputs(snapshot_inputs),
        load_openai_auth_status(openai_inputs)
    );
    let snapshot = snapshot?;
    let openai_status = match openai_status {
        Ok(status) => status,
        Err(error) => {
            OpenAiAuthStatusSnapshot::unavailable(sanitize_log_line(error.to_string().as_str()))
        }
    };

    let dashboard_reachable = probe_dashboard_reachability(
        &http_client,
        snapshot.quick_facts.dashboard_url.as_str(),
        snapshot.quick_facts.dashboard_access_mode.as_str(),
    )
    .await;
    let preflight = build_preflight_snapshot(
        runtime_root.as_path(),
        gateway_bound_ports.as_slice(),
        browser_bound_ports.as_slice(),
        gateway_running,
        browser_running,
        browser_service_enabled,
        admin_token.as_str(),
        &snapshot,
        dashboard_reachable,
    );
    let operator_auth =
        probe_operator_auth(&http_client, &runtime, admin_token.as_str(), &snapshot).await;
    let openai_ready = is_openai_ready(&openai_status);
    let onboarding = persisted.active_onboarding().clone();
    let discord_summary = derive_discord_onboarding_summary(&snapshot, &onboarding.discord);

    let current_step = derive_current_step(
        &persisted,
        &snapshot,
        &preflight,
        &operator_auth,
        openai_ready,
        discord_summary.verified,
    );
    let recovery = derive_recovery(
        onboarding.last_failure.as_ref(),
        &preflight,
        current_step,
        onboarding.completed_at_unix_ms.is_some(),
    );
    let steps = build_step_snapshots(
        &persisted,
        &snapshot,
        &preflight,
        &operator_auth,
        &openai_status,
        openai_ready,
        &discord_summary,
        dashboard_reachable,
        runtime_root.to_string_lossy().as_ref(),
        current_step,
    );
    let progress_completed = steps.iter().filter(|step| step.status == "complete").count();
    let progress_total = steps.len();
    let current_step_title = step_title(current_step).to_owned();
    let current_step_detail = steps
        .iter()
        .find(|step| step.key == current_step)
        .map(|step| step.detail.clone())
        .unwrap_or_else(|| "Desktop onboarding is waiting for the next step.".to_owned());
    let failure_step_counts = persisted
        .active_onboarding()
        .failure_step_counts
        .iter()
        .map(|(step, failures)| OnboardingStepFailureMetric {
            step: step.clone(),
            failures: *failures,
        })
        .collect::<Vec<_>>();
    let support_bundle_exports = OnboardingSupportBundleMetrics {
        attempts: onboarding.support_bundle_export_attempts,
        successes: onboarding.support_bundle_export_successes,
        failures: onboarding.support_bundle_export_failures,
        success_rate_bps: success_rate_bps(
            onboarding.support_bundle_export_successes,
            onboarding.support_bundle_export_attempts,
        ),
    };

    let phase = if onboarding.completed_at_unix_ms.is_some()
        || current_step == DesktopOnboardingStep::Completion
    {
        "home"
    } else {
        "onboarding"
    }
    .to_owned();

    let onboarding_status = OnboardingStatusSnapshot {
        flow_id: onboarding.flow_id.clone(),
        phase,
        current_step,
        current_step_title,
        current_step_detail,
        progress_completed,
        progress_total,
        state_root_path: runtime_root.to_string_lossy().into_owned(),
        default_state_root_path: default_runtime_root.to_string_lossy().into_owned(),
        state_root_confirmed: onboarding.state_root_confirmed_at_unix_ms.is_some(),
        state_root_overridden: persisted.normalized_runtime_state_root().is_some(),
        dashboard_url: snapshot.quick_facts.dashboard_url.clone(),
        dashboard_access_mode: snapshot.quick_facts.dashboard_access_mode.clone(),
        dashboard_remote_trust_state: snapshot.quick_facts.dashboard_remote_trust_state.clone(),
        dashboard_remote_verification_mode: snapshot
            .quick_facts
            .dashboard_remote_verification_mode
            .clone(),
        dashboard_reachable,
        dashboard_handoff_completed: onboarding.dashboard_handoff_at_unix_ms.is_some(),
        completion_unix_ms: onboarding.completed_at_unix_ms,
        preflight,
        operator_auth,
        openai_ready,
        openai_default_profile_id: openai_status.default_profile_id.clone(),
        openai_note: openai_status.note.clone(),
        discord_ready: discord_summary.ready,
        discord_verified: discord_summary.verified,
        discord_last_verified_target: discord_summary.last_verified_target.clone(),
        discord_last_verified_at_unix_ms: discord_summary.last_verified_at_unix_ms,
        discord_defaults: discord_summary.defaults.clone(),
        recovery,
        failure_step_counts,
        support_bundle_exports,
        recent_events: onboarding.recent_events.clone(),
        steps,
    };

    Ok(DesktopRefreshPayload { snapshot, onboarding_status, openai_status })
}

fn success_rate_bps(successes: u64, attempts: u64) -> u32 {
    if attempts == 0 {
        return 10_000;
    }
    let scaled = successes.saturating_mul(10_000) / attempts;
    u32::try_from(scaled).unwrap_or(u32::MAX)
}

#[allow(clippy::too_many_arguments)]
fn build_preflight_snapshot(
    runtime_root: &Path,
    gateway_bound_ports: &[u16],
    browser_bound_ports: &[u16],
    gateway_running: bool,
    browser_running: bool,
    browser_service_enabled: bool,
    admin_token: &str,
    snapshot: &ControlCenterSnapshot,
    dashboard_reachable: bool,
) -> OnboardingPreflightSnapshot {
    let mut checks = Vec::new();

    checks.push(binary_check(
        "gateway_binary",
        "Gateway binary",
        "palyrad",
        "PALYRA_DESKTOP_PALYRAD_BIN",
        true,
    ));
    checks.push(binary_check(
        "browser_binary",
        "Browser sidecar binary",
        "palyra-browserd",
        "PALYRA_DESKTOP_BROWSERD_BIN",
        browser_service_enabled,
    ));
    checks.push(binary_check(
        "cli_binary",
        "Support CLI",
        "palyra",
        "PALYRA_DESKTOP_PALYRA_BIN",
        false,
    ));
    checks.push(state_root_check(runtime_root));
    checks.push(config_check());
    checks.push(gateway_port_check(
        gateway_bound_ports,
        gateway_running,
        snapshot.quick_facts.gateway_version.is_some(),
    ));
    checks.push(browser_port_check(
        browser_bound_ports,
        browser_running,
        browser_service_enabled,
        snapshot.quick_facts.browser_service.healthy,
    ));
    checks.push(admin_token_check(admin_token));
    checks.push(OnboardingPreflightCheck {
        key: "dashboard_mode".to_owned(),
        label: "Dashboard access mode".to_owned(),
        status: "ok".to_owned(),
        detail: if snapshot.quick_facts.dashboard_access_mode == "remote" {
            match snapshot.quick_facts.dashboard_remote_trust_state.as_str() {
                "verification_configured" => format!(
                    "Remote dashboard access is configured with {} verification. Re-run verify after trust changes.",
                    snapshot
                        .quick_facts
                        .dashboard_remote_verification_mode
                        .as_deref()
                        .unwrap_or("remote")
                ),
                "pin_missing" => {
                    "Remote dashboard access is configured without an explicit pin. Re-run onboarding or update config before first connect."
                        .to_owned()
                }
                _ => "Remote dashboard access is configured. Desktop stays local-first.".to_owned(),
            }
        } else if dashboard_reachable {
            "Dashboard resolves to a local address and responds to HTTP checks.".to_owned()
        } else {
            "Dashboard resolves locally but did not answer yet; start or refresh the runtime."
                .to_owned()
        },
    });

    let blocked_count = checks.iter().filter(|check| check.status == "blocked").count();
    let warning_count = checks.iter().filter(|check| check.status == "warning").count();

    OnboardingPreflightSnapshot { blocked_count, warning_count, checks }
}

fn binary_check(
    key: &str,
    label: &str,
    binary_name: &str,
    env_override: &str,
    required: bool,
) -> OnboardingPreflightCheck {
    match resolve_binary_path(binary_name, env_override) {
        Ok(path) => OnboardingPreflightCheck {
            key: key.to_owned(),
            label: label.to_owned(),
            status: "ok".to_owned(),
            detail: format!("Resolved {} at {}.", binary_name, path.display()),
        },
        Err(error) => OnboardingPreflightCheck {
            key: key.to_owned(),
            label: label.to_owned(),
            status: if required { "blocked" } else { "warning" }.to_owned(),
            detail: sanitize_log_line(error.to_string().as_str()),
        },
    }
}

fn state_root_check(path: &Path) -> OnboardingPreflightCheck {
    let result = (|| -> Result<String> {
        fs::create_dir_all(path).map_err(|error| {
            anyhow::anyhow!("failed to create runtime root {}: {error}", path.display())
        })?;
        let probe = path.join(format!("desktop-write-probe-{}.tmp", unix_ms_now()));
        let mut file = fs::File::create(probe.as_path()).map_err(|error| {
            anyhow::anyhow!("failed to create runtime probe file {}: {error}", probe.display())
        })?;
        file.write_all(b"ok").map_err(|error| {
            anyhow::anyhow!("failed to write runtime probe file {}: {error}", probe.display())
        })?;
        let _ = fs::remove_file(probe.as_path());
        Ok(format!("Runtime root is writable at {}.", path.display()))
    })();

    match result {
        Ok(detail) => OnboardingPreflightCheck {
            key: "state_root".to_owned(),
            label: "Writable state root".to_owned(),
            status: "ok".to_owned(),
            detail,
        },
        Err(error) => OnboardingPreflightCheck {
            key: "state_root".to_owned(),
            label: "Writable state root".to_owned(),
            status: "blocked".to_owned(),
            detail: sanitize_log_line(error.to_string().as_str()),
        },
    }
}

fn config_check() -> OnboardingPreflightCheck {
    match super::snapshot::resolve_dashboard_config_path() {
        Ok(Some(path)) => match super::snapshot::load_dashboard_root_file_config(path.as_path()) {
            Ok(_) => OnboardingPreflightCheck {
                key: "config".to_owned(),
                label: "Config file".to_owned(),
                status: "ok".to_owned(),
                detail: format!("Loaded dashboard config from {}.", path.display()),
            },
            Err(error) => OnboardingPreflightCheck {
                key: "config".to_owned(),
                label: "Config file".to_owned(),
                status: "blocked".to_owned(),
                detail: sanitize_log_line(error.to_string().as_str()),
            },
        },
        Ok(None) => OnboardingPreflightCheck {
            key: "config".to_owned(),
            label: "Config file".to_owned(),
            status: "ok".to_owned(),
            detail: "No persisted config file found. Desktop will use local desktop defaults until the dashboard writes config.".to_owned(),
        },
        Err(error) => OnboardingPreflightCheck {
            key: "config".to_owned(),
            label: "Config file".to_owned(),
            status: "blocked".to_owned(),
            detail: sanitize_log_line(error.to_string().as_str()),
        },
    }
}

#[derive(Debug, Clone, Copy)]
enum PortProtocol {
    Tcp,
    Udp,
}

#[derive(Debug, Clone, Copy)]
struct PortProbe {
    port: u16,
    protocol: PortProtocol,
    label: &'static str,
}

fn gateway_port_check(
    ports: &[u16],
    service_running: bool,
    service_healthy: bool,
) -> OnboardingPreflightCheck {
    let probes = ports
        .iter()
        .enumerate()
        .filter_map(|(index, port)| {
            let (label, protocol) = match index {
                0 => ("admin HTTP", PortProtocol::Tcp),
                1 => ("gRPC", PortProtocol::Tcp),
                2 => ("QUIC", PortProtocol::Udp),
                _ => return None,
            };
            Some(PortProbe { port: *port, protocol, label })
        })
        .collect::<Vec<_>>();
    port_check(
        "gateway_ports",
        "Gateway ports",
        probes.as_slice(),
        service_running,
        service_healthy,
        true,
    )
}

fn browser_port_check(
    ports: &[u16],
    service_running: bool,
    required: bool,
    service_healthy: bool,
) -> OnboardingPreflightCheck {
    let probes = ports
        .iter()
        .enumerate()
        .filter_map(|(index, port)| {
            let label = match index {
                0 => "health",
                1 => "gRPC",
                _ => return None,
            };
            Some(PortProbe { port: *port, protocol: PortProtocol::Tcp, label })
        })
        .collect::<Vec<_>>();
    port_check(
        "browser_ports",
        "Browser sidecar ports",
        probes.as_slice(),
        service_running,
        service_healthy,
        required,
    )
}

fn port_check(
    key: &str,
    label: &str,
    probes: &[PortProbe],
    service_running: bool,
    service_healthy: bool,
    required: bool,
) -> OnboardingPreflightCheck {
    if service_running {
        return OnboardingPreflightCheck {
            key: key.to_owned(),
            label: label.to_owned(),
            status: "ok".to_owned(),
            detail: format!(
                "Desktop-managed service is already using {}.",
                format_probe_list(probes)
            ),
        };
    }
    if service_healthy {
        return OnboardingPreflightCheck {
            key: key.to_owned(),
            label: label.to_owned(),
            status: "ok".to_owned(),
            detail: format!(
                "A Palyra runtime is already responding on {}.",
                format_probe_list(probes)
            ),
        };
    }

    let mut busy = Vec::new();
    for probe in probes {
        let available = match probe.protocol {
            PortProtocol::Tcp => TcpListener::bind((LOOPBACK_HOST, probe.port)).is_ok(),
            PortProtocol::Udp => UdpSocket::bind((LOOPBACK_HOST, probe.port)).is_ok(),
        };
        if !available {
            busy.push(*probe);
        }
    }

    if busy.is_empty() {
        return OnboardingPreflightCheck {
            key: key.to_owned(),
            label: label.to_owned(),
            status: "ok".to_owned(),
            detail: format!("{} are currently available.", format_probe_list(probes)),
        };
    }

    OnboardingPreflightCheck {
        key: key.to_owned(),
        label: label.to_owned(),
        status: if required { "blocked" } else { "warning" }.to_owned(),
        detail: format!("{} are already in use on loopback.", format_probe_list(busy.as_slice())),
    }
}

fn admin_token_check(admin_token: &str) -> OnboardingPreflightCheck {
    OnboardingPreflightCheck {
        key: "operator_auth".to_owned(),
        label: "Operator auth bootstrap".to_owned(),
        status: if normalize_optional_text(admin_token).is_some() { "ok" } else { "blocked" }
            .to_owned(),
        detail: if normalize_optional_text(admin_token).is_some() {
            "Desktop admin token is initialized in the local secret store.".to_owned()
        } else {
            "Desktop admin token is missing from the local secret store.".to_owned()
        },
    }
}

fn format_probe_list(probes: &[PortProbe]) -> String {
    probes
        .iter()
        .map(|probe| {
            let protocol = match probe.protocol {
                PortProtocol::Tcp => "tcp",
                PortProtocol::Udp => "udp",
            };
            format!("{} {}:{}", probe.label, protocol, probe.port)
        })
        .collect::<Vec<_>>()
        .join(", ")
}

async fn probe_operator_auth(
    http_client: &reqwest::Client,
    runtime: &super::RuntimeConfig,
    admin_token: &str,
    snapshot: &ControlCenterSnapshot,
) -> OnboardingOperatorAuthSnapshot {
    if snapshot.quick_facts.gateway_version.is_none() {
        return OnboardingOperatorAuthSnapshot {
            ready: false,
            note: "Start the local runtime before desktop can verify operator auth bootstrap."
                .to_owned(),
        };
    }

    let mut control_plane = match build_control_plane_client(http_client.clone(), runtime) {
        Ok(client) => client,
        Err(error) => {
            return OnboardingOperatorAuthSnapshot {
                ready: false,
                note: sanitize_log_line(error.to_string().as_str()),
            }
        }
    };

    match ensure_console_session(&mut control_plane, admin_token).await {
        Ok(()) => OnboardingOperatorAuthSnapshot {
            ready: true,
            note: "Desktop console session bootstrap succeeded against the local gateway."
                .to_owned(),
        },
        Err(error) => OnboardingOperatorAuthSnapshot {
            ready: false,
            note: sanitize_log_line(error.to_string().as_str()),
        },
    }
}

fn is_openai_ready(status: &OpenAiAuthStatusSnapshot) -> bool {
    status.available
        && status.summary.total > 0
        && status.default_profile_id.is_some()
        && status.profiles.iter().any(|profile| profile.is_default)
}

fn derive_current_step(
    persisted: &super::DesktopStateFile,
    snapshot: &ControlCenterSnapshot,
    preflight: &OnboardingPreflightSnapshot,
    operator_auth: &OnboardingOperatorAuthSnapshot,
    openai_ready: bool,
    discord_verified: bool,
) -> DesktopOnboardingStep {
    let onboarding = persisted.active_onboarding();
    if onboarding.welcome_acknowledged_at_unix_ms.is_none() {
        return DesktopOnboardingStep::Welcome;
    }
    if preflight.blocked_count > 0 {
        return DesktopOnboardingStep::Environment;
    }
    if onboarding.state_root_confirmed_at_unix_ms.is_none() {
        return DesktopOnboardingStep::StateRoot;
    }
    if snapshot.quick_facts.gateway_version.is_none()
        || matches!(snapshot.overall_status, super::snapshot::OverallStatus::Down)
    {
        return DesktopOnboardingStep::GatewayInit;
    }
    if !operator_auth.ready {
        return DesktopOnboardingStep::OperatorAuthBootstrap;
    }
    if !openai_ready {
        return DesktopOnboardingStep::OpenAiConnect;
    }
    if !discord_verified {
        return DesktopOnboardingStep::DiscordConnect;
    }
    if onboarding.dashboard_handoff_at_unix_ms.is_none() {
        return DesktopOnboardingStep::DashboardHandoff;
    }
    DesktopOnboardingStep::Completion
}

fn derive_recovery(
    failure: Option<&DesktopOnboardingFailureState>,
    preflight: &OnboardingPreflightSnapshot,
    current_step: DesktopOnboardingStep,
    was_completed: bool,
) -> Option<OnboardingRecoverySnapshot> {
    if let Some(failure) = failure {
        return Some(OnboardingRecoverySnapshot {
            step: failure.step,
            message: failure.message.clone(),
            recorded_at_unix_ms: failure.recorded_at_unix_ms,
            suggested_actions: suggested_actions(failure.step),
        });
    }

    if preflight.blocked_count > 0 {
        return Some(OnboardingRecoverySnapshot {
            step: DesktopOnboardingStep::Environment,
            message: "Desktop preflight found blocking issues that must be fixed before onboarding can continue.".to_owned(),
            recorded_at_unix_ms: unix_ms_now(),
            suggested_actions: suggested_actions(DesktopOnboardingStep::Environment),
        });
    }

    if was_completed && current_step != DesktopOnboardingStep::Completion {
        return Some(OnboardingRecoverySnapshot {
            step: current_step,
            message: "A previously completed local install needs attention before the desktop home can return to green.".to_owned(),
            recorded_at_unix_ms: unix_ms_now(),
            suggested_actions: suggested_actions(current_step),
        });
    }

    None
}

fn suggested_actions(step: DesktopOnboardingStep) -> Vec<String> {
    match step {
        DesktopOnboardingStep::Welcome => {
            vec!["Start the guided flow from the desktop welcome step.".to_owned()]
        }
        DesktopOnboardingStep::Environment => vec![
            "Resolve missing binaries, invalid config, state-root access, or port conflicts."
                .to_owned(),
            "Refresh desktop preflight after each fix.".to_owned(),
            "Export a support bundle if the environment keeps failing.".to_owned(),
        ],
        DesktopOnboardingStep::StateRoot => {
            vec![
                "Confirm the default runtime root or choose another path inside the desktop state directory."
                    .to_owned(),
            ]
        }
        DesktopOnboardingStep::GatewayInit => vec![
            "Start or restart the local runtime from the desktop action bar.".to_owned(),
            "Check recent sidecar logs and diagnostics for startup failures.".to_owned(),
        ],
        DesktopOnboardingStep::OperatorAuthBootstrap => vec![
            "Refresh after the gateway is healthy so desktop can bootstrap its console session."
                .to_owned(),
            "If the session still fails, restart the runtime and export a support bundle."
                .to_owned(),
        ],
        DesktopOnboardingStep::OpenAiConnect => {
            vec!["Connect OpenAI with API key or OAuth and set a default profile.".to_owned()]
        }
        DesktopOnboardingStep::DiscordConnect => vec![
            "Run Discord preflight, apply the connector, then send a verification message."
                .to_owned(),
            "Re-enter the token if the previous attempt failed; desktop never stores it locally."
                .to_owned(),
        ],
        DesktopOnboardingStep::DashboardHandoff => {
            vec![
                "Open the dashboard from desktop once runtime, OpenAI, and Discord are ready."
                    .to_owned(),
                "If remote trust material rotated, re-run verify-remote before relying on the dashboard handoff."
                    .to_owned(),
                "Export a support bundle if remote verification or handshake keeps failing."
                    .to_owned(),
            ]
        }
        DesktopOnboardingStep::Completion => vec![
            "Desktop home is ready. Use diagnostics or support bundle export if health degrades."
                .to_owned(),
        ],
    }
}

#[allow(clippy::too_many_arguments)]
fn build_step_snapshots(
    persisted: &super::DesktopStateFile,
    snapshot: &ControlCenterSnapshot,
    preflight: &OnboardingPreflightSnapshot,
    operator_auth: &OnboardingOperatorAuthSnapshot,
    openai_status: &OpenAiAuthStatusSnapshot,
    openai_ready: bool,
    discord_summary: &DesktopDiscordOnboardingSummary,
    dashboard_reachable: bool,
    runtime_root_path: &str,
    current_step: DesktopOnboardingStep,
) -> Vec<OnboardingStepSnapshot> {
    let onboarding = persisted.active_onboarding();
    let steps = [
        DesktopOnboardingStep::Welcome,
        DesktopOnboardingStep::Environment,
        DesktopOnboardingStep::StateRoot,
        DesktopOnboardingStep::GatewayInit,
        DesktopOnboardingStep::OperatorAuthBootstrap,
        DesktopOnboardingStep::OpenAiConnect,
        DesktopOnboardingStep::DiscordConnect,
        DesktopOnboardingStep::DashboardHandoff,
        DesktopOnboardingStep::Completion,
    ];

    steps
        .into_iter()
        .map(|step| {
            let (complete, blocked, detail) = match step {
                DesktopOnboardingStep::Welcome => (
                    onboarding.welcome_acknowledged_at_unix_ms.is_some(),
                    false,
                    if onboarding.welcome_acknowledged_at_unix_ms.is_some() {
                        "Desktop onboarding has been started from the welcome screen.".to_owned()
                    } else {
                        "Start the guided first-run flow to begin local setup.".to_owned()
                    },
                ),
                DesktopOnboardingStep::Environment => (
                    preflight.blocked_count == 0,
                    preflight.blocked_count > 0,
                    if preflight.blocked_count > 0 {
                        format!(
                            "{} blocking preflight issue(s) remain before onboarding can continue.",
                            preflight.blocked_count
                        )
                    } else if preflight.warning_count > 0 {
                        format!(
                            "Preflight passed with {} warning(s) that may affect recovery or optional tooling.",
                            preflight.warning_count
                        )
                    } else {
                        "Runtime and install checks look healthy.".to_owned()
                    },
                ),
                DesktopOnboardingStep::StateRoot => (
                    onboarding.state_root_confirmed_at_unix_ms.is_some(),
                    false,
                    if onboarding.state_root_confirmed_at_unix_ms.is_some() {
                        format!(
                            "Desktop will use {} for the local runtime state root.",
                            runtime_root_path
                        )
                    } else {
                        "Confirm the local runtime state root before gateway initialization.".to_owned()
                    },
                ),
                DesktopOnboardingStep::GatewayInit => (
                    snapshot.quick_facts.gateway_version.is_some(),
                    false,
                    if let Some(version) = snapshot.quick_facts.gateway_version.as_deref() {
                        format!("Gateway is healthy and reporting version {version}.")
                    } else {
                        "Start the local runtime from desktop and wait for gateway health.".to_owned()
                    },
                ),
                DesktopOnboardingStep::OperatorAuthBootstrap => (
                    operator_auth.ready,
                    false,
                    operator_auth.note.clone(),
                ),
                DesktopOnboardingStep::OpenAiConnect => (
                    openai_ready,
                    false,
                    if openai_ready {
                        format!(
                            "OpenAI is connected and default profile {} is ready.",
                            openai_status.default_profile_id.as_deref().unwrap_or("unknown")
                        )
                    } else {
                        openai_status.note.clone().unwrap_or_else(|| {
                            "Connect OpenAI with API key or OAuth and set a default profile.".to_owned()
                        })
                    },
                ),
                DesktopOnboardingStep::DiscordConnect => (
                    discord_summary.verified,
                    false,
                    discord_connect_detail(discord_summary),
                ),
                DesktopOnboardingStep::DashboardHandoff => (
                    onboarding.dashboard_handoff_at_unix_ms.is_some(),
                    false,
                    if onboarding.dashboard_handoff_at_unix_ms.is_some() {
                        "Dashboard handoff has been recorded from desktop.".to_owned()
                    } else if snapshot.quick_facts.dashboard_access_mode == "remote"
                        && snapshot.quick_facts.dashboard_remote_trust_state == "pin_missing"
                    {
                        "Remote dashboard is configured but missing an explicit pin. Fix trust config before first-connect handoff."
                            .to_owned()
                    } else if snapshot.quick_facts.dashboard_access_mode == "remote" {
                        format!(
                            "Remote dashboard handoff is ready with {} verification. Re-verify after trust rotation.",
                            snapshot
                                .quick_facts
                                .dashboard_remote_verification_mode
                                .as_deref()
                                .unwrap_or("remote")
                        )
                    } else if dashboard_reachable || snapshot.quick_facts.dashboard_access_mode == "remote" {
                        "Open the dashboard for the full operator surface and finish the handoff.".to_owned()
                    } else {
                        "Dashboard did not answer yet. Refresh after the runtime is healthy or review recovery guidance.".to_owned()
                    },
                ),
                DesktopOnboardingStep::Completion => (
                    onboarding.completed_at_unix_ms.is_some() || current_step == DesktopOnboardingStep::Completion,
                    false,
                    if onboarding.completed_at_unix_ms.is_some() {
                        "Desktop home is active for this local install.".to_owned()
                    } else {
                        "Desktop will switch to home mode after the dashboard handoff is complete.".to_owned()
                    },
                ),
            };

            let status = if complete {
                "complete"
            } else if blocked {
                "blocked"
            } else if step == current_step {
                "current"
            } else {
                "pending"
            }
            .to_owned();

            OnboardingStepSnapshot {
                key: step,
                title: step_title(step).to_owned(),
                status,
                detail,
            }
        })
        .collect()
}

fn step_title(step: DesktopOnboardingStep) -> &'static str {
    match step {
        DesktopOnboardingStep::Welcome => "Welcome",
        DesktopOnboardingStep::Environment => "Environment and binaries",
        DesktopOnboardingStep::StateRoot => "Local state root",
        DesktopOnboardingStep::GatewayInit => "Gateway init",
        DesktopOnboardingStep::OperatorAuthBootstrap => "Operator auth bootstrap",
        DesktopOnboardingStep::OpenAiConnect => "OpenAI connect",
        DesktopOnboardingStep::DiscordConnect => "Discord connect and verify",
        DesktopOnboardingStep::DashboardHandoff => "Dashboard handoff",
        DesktopOnboardingStep::Completion => "Completion",
    }
}

async fn probe_dashboard_reachability(
    http_client: &reqwest::Client,
    raw_url: &str,
    access_mode: &str,
) -> bool {
    super::snapshot::probe_dashboard_reachability(http_client, raw_url, access_mode).await
}
