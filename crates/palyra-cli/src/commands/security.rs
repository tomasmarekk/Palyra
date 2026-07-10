//! `palyra security audit`: aggregates doctor checks, local config inspection,
//! live runtime posture, and the secrets audit into severity-ranked findings.
//! Runtime posture from the daemon admin surface takes precedence over the
//! local doctor snapshot whenever it is reachable.

use crate::*;
use palyra_common::{
    secret_refs::SecretRef,
    security_posture::{
        audit_attack_surface_graph, ApprovalRequirement, AttackSurfaceAudit, AttackSurfaceGraph,
        ChannelExposure, EgressAccess, FilesystemAccess, IngressExposure, IngressSurfaceKind,
        PluginExposure, ProcessAccess, SandboxTier, SecretAccess, SecretExposure, SideEffectLevel,
        ToolExposure, WorkspaceExposure, SECURITY_POSTURE_SCHEMA_VERSION,
    },
};
use palyra_control_plane as control_plane;

use super::{
    models::load_models_status,
    secrets::{build_secrets_audit_payload, SecretAuditFinding, SecretAuditPayload},
};

#[derive(Debug, Serialize)]
struct SecurityAuditPayload {
    generated_at_unix_ms: i64,
    strict: bool,
    used_runtime_posture: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    attack_surface: Option<AttackSurfaceAudit>,
    findings: Vec<SecurityFinding>,
    summary: SecurityAuditSummary,
}

#[derive(Debug, Serialize)]
struct SecurityFinding {
    severity: String,
    code: String,
    component: String,
    message: String,
    remediation: String,
}

#[derive(Debug, Serialize)]
struct SecurityAuditSummary {
    blocking_findings: usize,
    warning_findings: usize,
    info_findings: usize,
}

#[derive(Debug, Deserialize, Default)]
struct SecurityAuthHealthSummary {
    #[serde(default)]
    missing: u64,
    #[serde(default)]
    expired: u64,
    #[serde(default)]
    expiring: u64,
}

struct RuntimeSecuritySnapshot {
    used_runtime_posture: bool,
    deployment: Option<control_plane::DeploymentPostureSummary>,
    auth_summary: Option<SecurityAuthHealthSummary>,
    browser: Option<SecurityBrowserRuntimeSnapshot>,
    error: Option<String>,
}

#[derive(Debug, Clone)]
struct SecurityBrowserRuntimeSnapshot {
    enabled: Option<bool>,
    health_status: Option<String>,
    active_sessions: Option<u64>,
    recent_relay_action_failures: Option<u64>,
    recent_health_failures: Option<u64>,
}

struct LocalSecurityConfigSnapshot {
    path_exists: bool,
    provider_kind: String,
    auth_profile_id: Option<String>,
    openai_api_key_vault_ref: Option<String>,
    openai_api_key_secret_ref_configured: bool,
    openai_inline_api_key: bool,
    anthropic_api_key_vault_ref: Option<String>,
    anthropic_api_key_secret_ref_configured: bool,
    anthropic_inline_api_key: bool,
    browser_service_enabled: bool,
    browser_service_auth_token_configured: bool,
    effective_provider_kind: Option<String>,
    process_runner: LocalProcessRunnerConfigSnapshot,
}

#[derive(Debug, Clone, Default)]
struct LocalProcessRunnerConfigSnapshot {
    enabled: bool,
    tier: String,
    allowed_executables_wildcard: bool,
    egress_enforcement_mode: String,
}

impl LocalProcessRunnerConfigSnapshot {
    fn is_permissive_host_process_profile(&self) -> bool {
        self.enabled
            && matches!(self.tier.as_str(), "b" | "tier_b")
            && self.allowed_executables_wildcard
            && self.egress_enforcement_mode == "none"
    }
}

/// Runs a `palyra security` subcommand.
///
/// # Errors
/// Returns an error when local config or doctor snapshots cannot be built, and
/// in `--strict` mode when the audit reports blocking findings.
pub(crate) fn run_security(command: SecurityCommand) -> Result<()> {
    match command {
        SecurityCommand::Audit { path, offline, strict, json, attack_surface } => {
            let checks = build_doctor_checks();
            let doctor = if offline {
                build_doctor_report_offline(checks.as_slice())?
            } else {
                build_doctor_report(checks.as_slice())?
            };
            let secrets = build_secrets_audit_payload(path.clone(), offline)?;
            let local_config = load_local_security_config_snapshot(path)?;
            let runtime = load_runtime_security_snapshot(offline)?;
            let findings = build_security_findings(&doctor, &local_config, &runtime, &secrets);
            let attack_surface = attack_surface.then(|| {
                let graph = build_attack_surface_graph(&doctor, &local_config, &runtime, &secrets);
                audit_attack_surface_graph(&graph)
            });
            let payload = SecurityAuditPayload {
                generated_at_unix_ms: unix_now_ms(),
                strict,
                used_runtime_posture: runtime.used_runtime_posture,
                attack_surface,
                summary: SecurityAuditSummary {
                    blocking_findings: findings
                        .iter()
                        .filter(|finding| finding.severity == "blocking")
                        .count(),
                    warning_findings: findings
                        .iter()
                        .filter(|finding| finding.severity == "warning")
                        .count(),
                    info_findings: findings
                        .iter()
                        .filter(|finding| finding.severity == "info")
                        .count(),
                },
                findings,
            };
            emit_security_audit(&payload, output::preferred_json(json))?;
            if strict && payload.summary.blocking_findings > 0 {
                anyhow::bail!(
                    "security audit failed with {} blocking findings",
                    payload.summary.blocking_findings
                );
            }
            Ok(())
        }
    }
}

fn emit_security_audit(payload: &SecurityAuditPayload, json_output: bool) -> Result<()> {
    if json_output {
        output::print_json_pretty(payload, "failed to encode security audit payload as JSON")?;
    } else {
        println!(
            "security.audit blocking={} warnings={} info={} runtime_posture={}",
            payload.summary.blocking_findings,
            payload.summary.warning_findings,
            payload.summary.info_findings,
            payload.used_runtime_posture
        );
        for finding in &payload.findings {
            println!(
                "security.finding severity={} code={} component={} message=\"{}\" remediation=\"{}\"",
                finding.severity,
                finding.code,
                finding.component,
                finding.message.replace('"', "'"),
                finding.remediation.replace('"', "'")
            );
        }
        if let Some(attack_surface) = payload.attack_surface.as_ref() {
            println!(
                "security.attack_surface critical={} warnings={} info={} highest_without_approval={} highest_with_one_approval={}",
                attack_surface.summary.critical_findings,
                attack_surface.summary.warning_findings,
                attack_surface.summary.info_findings,
                attack_surface
                    .summary
                    .highest_side_effect_without_human_approval
                    .as_str(),
                attack_surface
                    .summary
                    .highest_side_effect_with_one_approval
                    .as_str()
            );
            for finding in &attack_surface.findings {
                println!(
                    "security.attack_surface.finding severity={} code={} path={} remediation=\"{}\"",
                    finding.severity.as_str(),
                    finding.reason_code.as_str(),
                    finding.affected_path,
                    finding.remediation_hint.replace('"', "'")
                );
            }
        }
    }
    std::io::stdout().flush().context("stdout flush failed")
}

fn build_attack_surface_graph(
    doctor: &DoctorReport,
    local_config: &LocalSecurityConfigSnapshot,
    runtime: &RuntimeSecuritySnapshot,
    secrets: &SecretAuditPayload,
) -> AttackSurfaceGraph {
    let deployment = runtime.deployment.as_ref();
    let remote_bind_detected = deployment
        .map(|value| value.remote_bind_detected)
        .unwrap_or(doctor.deployment.remote_bind_detected);
    let admin_auth_required = deployment
        .map(|value| value.admin_auth_required)
        .unwrap_or(doctor.deployment.admin_auth_required);
    let mut graph = AttackSurfaceGraph {
        schema_version: SECURITY_POSTURE_SCHEMA_VERSION,
        ingress: vec![
            IngressExposure {
                source_id: "cli.operator".to_owned(),
                source: IngressSurfaceKind::Cli,
                principal: "local_operator".to_owned(),
                channel_scope: None,
                channel_exposure: ChannelExposure::Private,
                admin_auth_required: true,
                webhook_signature_required: false,
                approval_requirement: ApprovalRequirement::PolicyOnly,
            },
            IngressExposure {
                source_id: "console.admin".to_owned(),
                source: IngressSurfaceKind::ConsoleApi,
                principal: "admin".to_owned(),
                channel_scope: Some(doctor.deployment.binds.admin.clone()),
                channel_exposure: if remote_bind_detected {
                    ChannelExposure::Public
                } else {
                    ChannelExposure::Private
                },
                admin_auth_required,
                webhook_signature_required: false,
                approval_requirement: if admin_auth_required {
                    ApprovalRequirement::PolicyOnly
                } else {
                    ApprovalRequirement::None
                },
            },
        ],
        tools: build_attack_surface_tool_exposures(local_config),
        secrets: build_attack_surface_secret_exposures(local_config, secrets),
        workspace: build_attack_surface_workspace(local_config),
        plugins: build_attack_surface_plugin_exposures(doctor),
    };
    graph.ingress.sort_by(|left, right| left.source_id.cmp(&right.source_id));
    graph.tools.sort_by(|left, right| left.tool_name.cmp(&right.tool_name));
    graph.secrets.sort_by(|left, right| left.ref_id.cmp(&right.ref_id));
    graph.plugins.sort_by(|left, right| left.plugin_id.cmp(&right.plugin_id));
    graph
}

fn build_attack_surface_tool_exposures(
    local_config: &LocalSecurityConfigSnapshot,
) -> Vec<ToolExposure> {
    let process_access = if !local_config.process_runner.enabled {
        ProcessAccess::None
    } else if local_config.process_runner.allowed_executables_wildcard {
        ProcessAccess::HostWildcard
    } else {
        ProcessAccess::HostAllowlist
    };
    let side_effect = if local_config.process_runner.enabled {
        SideEffectLevel::ProcessExecution
    } else {
        SideEffectLevel::None
    };
    let egress_access =
        process_runner_egress_access(local_config.process_runner.egress_enforcement_mode.as_str());
    let sandbox_tier = match local_config.process_runner.tier.as_str() {
        "tier_c" | "c" => SandboxTier::TierC,
        "tier_a" | "a" => SandboxTier::TierA,
        "tier_b" | "b" => SandboxTier::TierB,
        _ => SandboxTier::None,
    };
    let mut tools = vec![ToolExposure {
        tool_name: "palyra.process_runner".to_owned(),
        target_surfaces: vec![IngressSurfaceKind::Cli, IngressSurfaceKind::ConsoleApi],
        side_effect,
        approval_requirement: if local_config.process_runner.enabled {
            ApprovalRequirement::PolicyOnly
        } else {
            ApprovalRequirement::None
        },
        sandbox_tier,
        process_access,
        egress_access,
    }];
    if local_config.browser_service_enabled {
        tools.push(ToolExposure {
            tool_name: "palyra.browser_service".to_owned(),
            target_surfaces: vec![IngressSurfaceKind::Cli, IngressSurfaceKind::ConsoleApi],
            side_effect: SideEffectLevel::NetworkEgress,
            approval_requirement: ApprovalRequirement::PolicyOnly,
            sandbox_tier: SandboxTier::None,
            process_access: ProcessAccess::None,
            egress_access: EgressAccess::Allowlisted,
        });
    }
    tools
}

fn process_runner_egress_access(mode: &str) -> EgressAccess {
    match mode {
        "none" => EgressAccess::Unrestricted,
        "preflight" | "strict" => EgressAccess::Allowlisted,
        _ => EgressAccess::None,
    }
}

fn build_attack_surface_secret_exposures(
    local_config: &LocalSecurityConfigSnapshot,
    secrets: &SecretAuditPayload,
) -> Vec<SecretExposure> {
    let mut exposures = Vec::new();
    if local_config.openai_inline_api_key {
        exposures.push(raw_secret_exposure("model_provider.openai_api_key"));
    }
    if local_config.anthropic_inline_api_key {
        exposures.push(raw_secret_exposure("model_provider.anthropic_api_key"));
    }
    if local_config.openai_api_key_vault_ref.is_some()
        || local_config.openai_api_key_secret_ref_configured
    {
        exposures.push(vault_ref_exposure("model_provider.openai_api_key"));
    }
    if local_config.anthropic_api_key_vault_ref.is_some()
        || local_config.anthropic_api_key_secret_ref_configured
    {
        exposures.push(vault_ref_exposure("model_provider.anthropic_api_key"));
    }
    if local_config.browser_service_auth_token_configured {
        exposures.push(raw_secret_exposure("tool_call.browser_service.auth_token"));
    }
    for finding in &secrets.findings {
        if finding.severity == "blocking" && finding.component == "vault" {
            exposures.push(SecretExposure {
                ref_id: format!("secrets_audit.{}", finding.code),
                access: SecretAccess::VaultReferenceOnly,
                approval_requirement: ApprovalRequirement::PolicyOnly,
                vault_ref_present: false,
            });
        }
    }
    exposures
}

fn raw_secret_exposure(ref_id: &str) -> SecretExposure {
    SecretExposure {
        ref_id: ref_id.to_owned(),
        access: SecretAccess::RawSecret,
        approval_requirement: ApprovalRequirement::None,
        vault_ref_present: false,
    }
}

fn vault_ref_exposure(ref_id: &str) -> SecretExposure {
    SecretExposure {
        ref_id: ref_id.to_owned(),
        access: SecretAccess::VaultReferenceOnly,
        approval_requirement: ApprovalRequirement::PolicyOnly,
        vault_ref_present: true,
    }
}

fn build_attack_surface_workspace(local_config: &LocalSecurityConfigSnapshot) -> WorkspaceExposure {
    WorkspaceExposure {
        workspace_roots: Vec::new(),
        filesystem_access: if local_config.process_runner.enabled {
            FilesystemAccess::WorkspaceWrite
        } else {
            FilesystemAccess::None
        },
        process_access: if !local_config.process_runner.enabled {
            ProcessAccess::None
        } else if local_config.process_runner.allowed_executables_wildcard {
            ProcessAccess::HostWildcard
        } else {
            ProcessAccess::HostAllowlist
        },
        egress_access: process_runner_egress_access(
            local_config.process_runner.egress_enforcement_mode.as_str(),
        ),
        browser_access_enabled: local_config.browser_service_enabled,
        webhook_access_enabled: false,
    }
}

fn build_attack_surface_plugin_exposures(doctor: &DoctorReport) -> Vec<PluginExposure> {
    let mut plugins = Vec::new();
    if doctor.skills.current_total > 0 {
        plugins.push(PluginExposure {
            plugin_id: "skills.current".to_owned(),
            grants: Vec::new(),
            diagnostics_provider: false,
            approval_requirement: ApprovalRequirement::PolicyOnly,
        });
    }
    if doctor.skills.runtime_unknown_total > 0 || doctor.skills.missing_secrets_total > 0 {
        plugins.push(PluginExposure {
            plugin_id: "skills.diagnostics".to_owned(),
            grants: vec!["diagnostics.redacted".to_owned()],
            diagnostics_provider: true,
            approval_requirement: ApprovalRequirement::PolicyOnly,
        });
    }
    plugins
}

fn build_security_findings(
    doctor: &DoctorReport,
    local_config: &LocalSecurityConfigSnapshot,
    runtime: &RuntimeSecuritySnapshot,
    secrets: &SecretAuditPayload,
) -> Vec<SecurityFinding> {
    let mut findings = Vec::<SecurityFinding>::new();

    if !local_config.path_exists {
        findings.push(SecurityFinding {
            severity: "blocking".to_owned(),
            code: "missing_config".to_owned(),
            component: "config".to_owned(),
            message: "No daemon config file was found for the security audit.".to_owned(),
            remediation: "Create or select a config with `palyra setup`, or pass `--path <config>` to target an explicit file.".to_owned(),
        });
    }

    if !doctor.deployment.admin_auth_required {
        findings.push(SecurityFinding {
            severity: "blocking".to_owned(),
            code: "admin_auth_disabled".to_owned(),
            component: "deployment".to_owned(),
            message: "Admin authentication is disabled.".to_owned(),
            remediation: "Enable `admin.require_auth = true` and configure an admin token before exposing the operator surface.".to_owned(),
        });
    }

    // Prefer live runtime posture over the local doctor snapshot: a reachable
    // daemon reports the binds and TLS state actually in effect.
    let deployment = runtime.deployment.as_ref();
    let remote_bind_detected = deployment
        .map(|value| value.remote_bind_detected)
        .unwrap_or(doctor.deployment.remote_bind_detected);
    let gateway_tls_enabled = deployment
        .map(|value| value.tls.gateway_enabled)
        .unwrap_or(doctor.deployment.gateway_tls_enabled);
    if remote_bind_detected && !gateway_tls_enabled {
        findings.push(SecurityFinding {
            severity: "blocking".to_owned(),
            code: "remote_bind_without_tls".to_owned(),
            component: "deployment".to_owned(),
            message: "Remote bind is detected without gateway TLS.".to_owned(),
            remediation: "Switch to `gateway.bind_profile = \"public_tls\"`, enable gateway TLS, and keep the dual dangerous-bind acknowledgements explicit.".to_owned(),
        });
    }

    let dangerous_ack_config = deployment
        .map(|value| value.dangerous_remote_bind_ack.config)
        .unwrap_or(doctor.deployment.dangerous_remote_bind_ack_config);
    let dangerous_ack_env = deployment
        .map(|value| value.dangerous_remote_bind_ack.env)
        .unwrap_or(doctor.deployment.dangerous_remote_bind_ack_env);
    if dangerous_ack_config || dangerous_ack_env {
        findings.push(SecurityFinding {
            severity: "warning".to_owned(),
            code: "dangerous_remote_bind_ack_present".to_owned(),
            component: "deployment".to_owned(),
            message: "Dangerous remote-bind acknowledgement flags are enabled.".to_owned(),
            remediation: "Keep these acknowledgements enabled only while you intentionally operate a remote-exposed deployment.".to_owned(),
        });
    }

    let deployment_warnings = deployment
        .map(|value| value.warnings.clone())
        .unwrap_or_else(|| doctor.deployment.warnings.clone());
    for warning in deployment_warnings {
        findings.push(SecurityFinding {
            severity: "warning".to_owned(),
            code: "deployment_warning".to_owned(),
            component: "deployment".to_owned(),
            message: warning,
            remediation: "Review the deployment posture and adjust bind profile, TLS, or admin auth settings as indicated.".to_owned(),
        });
    }

    if let Some(provider_kind) = missing_model_auth_kind(local_config) {
        findings.push(SecurityFinding {
            severity: "blocking".to_owned(),
            code: "model_provider_missing_auth".to_owned(),
            component: "model_provider".to_owned(),
            message: missing_model_auth_message(provider_kind),
            remediation: missing_model_auth_remediation(provider_kind),
        });
    }

    if local_config.openai_inline_api_key {
        findings.push(SecurityFinding {
            severity: "warning".to_owned(),
            code: "inline_api_key".to_owned(),
            component: "model_provider".to_owned(),
            message: "The OpenAI API key is configured inline in the daemon config.".to_owned(),
            remediation: "Move the credential into the vault via `palyra auth openai api-key` or `palyra secrets configure openai-api-key`.".to_owned(),
        });
    }
    if local_config.anthropic_inline_api_key {
        findings.push(SecurityFinding {
            severity: "warning".to_owned(),
            code: "inline_api_key".to_owned(),
            component: "model_provider".to_owned(),
            message: "The Anthropic-compatible API key is configured inline in the daemon config."
                .to_owned(),
            remediation:
                "Move the credential into the vault via `palyra configure --section auth-model` before relying on the runtime."
                    .to_owned(),
        });
    }

    if local_config.browser_service_enabled && !local_config.browser_service_auth_token_configured {
        findings.push(SecurityFinding {
            severity: "blocking".to_owned(),
            code: "browser_service_missing_auth_token".to_owned(),
            component: "browser_service".to_owned(),
            message: "Browser service broker is enabled without an auth token.".to_owned(),
            remediation: "Set `tool_call.browser_service.auth_token` or keep the browser broker disabled until it is explicitly secured.".to_owned(),
        });
    }
    if local_config.browser_service_enabled {
        let health_status =
            runtime.browser.as_ref().and_then(|browser| browser.health_status.as_deref());
        if health_status.is_some_and(|status| status != "ok") {
            let active_sessions =
                runtime.browser.as_ref().and_then(|browser| browser.active_sessions).unwrap_or(0);
            findings.push(SecurityFinding {
                severity: "warning".to_owned(),
                code: "browser_service_runtime_degraded".to_owned(),
                component: "browser_service".to_owned(),
                message: format!(
                    "Browser service runtime health is reported as {} (active_sessions={}).",
                    health_status.unwrap_or("unknown"),
                    active_sessions
                ),
                remediation: "Run `palyra browser status` and inspect browserd health, endpoint wiring, and recent failures before relying on browser automation.".to_owned(),
            });
        }

        let recent_health_failures = runtime
            .browser
            .as_ref()
            .and_then(|browser| browser.recent_health_failures)
            .unwrap_or(0);
        if recent_health_failures > 0 {
            findings.push(SecurityFinding {
                severity: "warning".to_owned(),
                code: "browser_service_recent_health_failures".to_owned(),
                component: "browser_service".to_owned(),
                message: format!(
                    "Browser service diagnostics report {} recent health probe failure(s).",
                    recent_health_failures
                ),
                remediation: "Inspect `palyra browser status` and the browserd logs to restore a stable health probe path.".to_owned(),
            });
        }

        let recent_relay_failures = runtime
            .browser
            .as_ref()
            .and_then(|browser| browser.recent_relay_action_failures)
            .unwrap_or(0);
        if recent_relay_failures > 0 {
            findings.push(SecurityFinding {
                severity: "warning".to_owned(),
                code: "browser_service_recent_relay_failures".to_owned(),
                component: "browser_service".to_owned(),
                message: format!(
                    "Browser service diagnostics report {} recent relay/action failure(s).",
                    recent_relay_failures
                ),
                remediation: "Review browser policy, session budgets, and browserd diagnostics before allowing further automation runs.".to_owned(),
            });
        }

        if runtime.used_runtime_posture
            && runtime.browser.as_ref().and_then(|browser| browser.enabled) == Some(false)
        {
            findings.push(SecurityFinding {
                severity: "warning".to_owned(),
                code: "browser_service_runtime_disabled".to_owned(),
                component: "browser_service".to_owned(),
                message: "Browser service is enabled in local config but disabled in the active runtime posture.".to_owned(),
                remediation: "Ensure the intended config is active, then verify browser broker enablement with `palyra browser status`.".to_owned(),
            });
        }
    }

    if let Some(summary) = runtime.auth_summary.as_ref() {
        if summary.missing > 0 {
            findings.push(SecurityFinding {
                severity: "blocking".to_owned(),
                code: "auth_profiles_missing_secrets".to_owned(),
                component: "auth_profiles".to_owned(),
                message: format!("{} auth profile(s) are missing required secret material.", summary.missing),
                remediation: "Run `palyra auth openai status` and repair the affected vault refs or reconnect the profiles.".to_owned(),
            });
        }
        if summary.expired > 0 {
            findings.push(SecurityFinding {
                severity: "blocking".to_owned(),
                code: "auth_profiles_expired".to_owned(),
                component: "auth_profiles".to_owned(),
                message: format!("{} auth profile(s) are expired.", summary.expired),
                remediation:
                    "Refresh or reconnect the expired profiles before relying on the runtime."
                        .to_owned(),
            });
        }
        if summary.expiring > 0 {
            findings.push(SecurityFinding {
                severity: "warning".to_owned(),
                code: "auth_profiles_expiring".to_owned(),
                component: "auth_profiles".to_owned(),
                message: format!("{} auth profile(s) are nearing expiry.", summary.expiring),
                remediation: "Run `palyra auth openai status` and rotate or refresh the expiring profiles proactively.".to_owned(),
            });
        }
    }

    if let Some(error) = runtime.error.as_deref() {
        findings.push(SecurityFinding {
            severity: "warning".to_owned(),
            code: "runtime_posture_unavailable".to_owned(),
            component: "runtime".to_owned(),
            message: format!("Runtime posture checks were degraded: {error}"),
            remediation: "Ensure the daemon admin surface is reachable so `palyra security audit` can verify live deployment posture instead of local-only config snapshots.".to_owned(),
        });
    }

    if doctor.skills.runtime_unknown_total > 0 || doctor.skills.missing_secrets_total > 0 {
        findings.push(SecurityFinding {
            severity: "warning".to_owned(),
            code: "extension_diagnostics_provider_degraded".to_owned(),
            component: "extensions".to_owned(),
            message: format!(
                "Extension diagnostics report {} runtime-unknown skill(s) and {} missing secret reference(s).",
                doctor.skills.runtime_unknown_total,
                doctor.skills.missing_secrets_total
            ),
            remediation:
                "Run `palyra plugins doctor --json` and keep diagnostics provider output redacted before exposing it outside internal operator surfaces."
                    .to_owned(),
        });
    }

    if local_config.process_runner.is_permissive_host_process_profile() {
        findings.push(SecurityFinding {
            severity: "warning".to_owned(),
            code: "process_runner_permissive_host_process_profile".to_owned(),
            component: "sandbox".to_owned(),
            message: "Process runner is enabled as Tier B with wildcard executable selection and egress enforcement disabled.".to_owned(),
            remediation: "Use this only for trusted local desktop automation. For tighter posture, replace `allowed_executables = [\"*\"]` with an explicit allowlist and use `egress_enforcement_mode = \"preflight\"` or `\"strict\"`; `allow_interpreters = false` still permits wildcard non-interpreter host executables, and Tier B is not an OS-level filesystem sandbox.".to_owned(),
        });
    }

    if !doctor.sandbox.tier_b_egress_allowlists_preflight_only {
        findings.push(SecurityFinding {
            severity: "warning".to_owned(),
            code: "tier_b_egress_posture".to_owned(),
            component: "sandbox".to_owned(),
            message: "Tier B process-runner egress posture is not in the expected preflight-only mode.".to_owned(),
            remediation: "Review process-runner allowlists and keep Tier B in the documented preflight-only posture when network egress is enabled.".to_owned(),
        });
    }
    if !doctor.sandbox.tier_c_strict_offline_only {
        findings.push(SecurityFinding {
            severity: "warning".to_owned(),
            code: "tier_c_egress_posture".to_owned(),
            component: "sandbox".to_owned(),
            message: "Tier C process-runner posture is not strict offline-only.".to_owned(),
            remediation: "Keep Tier C fail-closed and offline-only unless a future design explicitly broadens the security contract.".to_owned(),
        });
    }
    if cfg!(windows) && !doctor.sandbox.tier_c_windows_backend_supported {
        findings.push(SecurityFinding {
            severity: "warning".to_owned(),
            code: "tier_c_windows_backend".to_owned(),
            component: "sandbox".to_owned(),
            message: "Tier C Windows backend support is unavailable.".to_owned(),
            remediation: "Avoid depending on Tier C process-runner enforcement on Windows until the required backend support is available.".to_owned(),
        });
    }

    for finding in &secrets.findings {
        findings.push(map_secret_finding_to_security_finding(finding));
    }

    findings
}

fn missing_model_auth_kind(local_config: &LocalSecurityConfigSnapshot) -> Option<&'static str> {
    let effective_provider_kind = local_config
        .effective_provider_kind
        .as_deref()
        .map(normalize_provider_kind)
        .unwrap_or_else(|| normalize_provider_kind(local_config.provider_kind.as_str()));
    let provider_kind = match effective_provider_kind.as_str() {
        "openai_compatible" => "openai_compatible",
        "anthropic" => "anthropic",
        _ => return None,
    };
    if model_provider_auth_configured(local_config, provider_kind) {
        return None;
    }
    Some(provider_kind)
}

fn normalize_provider_kind(kind: &str) -> String {
    kind.trim().to_ascii_lowercase().replace('-', "_")
}

fn model_provider_auth_configured(
    local_config: &LocalSecurityConfigSnapshot,
    provider_kind: &str,
) -> bool {
    if local_config.auth_profile_id.is_some() {
        return true;
    }
    match provider_kind {
        "openai_compatible" => {
            local_config.openai_api_key_vault_ref.is_some()
                || local_config.openai_api_key_secret_ref_configured
                || local_config.openai_inline_api_key
        }
        "anthropic" => {
            local_config.anthropic_api_key_vault_ref.is_some()
                || local_config.anthropic_api_key_secret_ref_configured
                || local_config.anthropic_inline_api_key
        }
        _ => true,
    }
}

fn missing_model_auth_message(provider_kind: &str) -> String {
    match provider_kind {
        "anthropic" => {
            "Anthropic-compatible model provider is configured without any auth source.".to_owned()
        }
        _ => "OpenAI-compatible model provider is configured without any auth source.".to_owned(),
    }
}

fn missing_model_auth_remediation(provider_kind: &str) -> String {
    match provider_kind {
        "anthropic" => {
            "Configure Anthropic-compatible auth with `palyra configure --section auth-model` or select a default auth profile before relying on the runtime."
                .to_owned()
        }
        _ => {
            "Configure OpenAI auth with `palyra auth openai api-key` or select a default auth profile before relying on the runtime."
                .to_owned()
        }
    }
}

fn map_secret_finding_to_security_finding(finding: &SecretAuditFinding) -> SecurityFinding {
    SecurityFinding {
        severity: finding.severity.clone(),
        code: format!("secrets_{}", finding.code),
        component: finding.component.clone(),
        message: finding.message.clone(),
        remediation: finding.remediation.clone(),
    }
}

// Runtime posture failures degrade the audit to local-only signals (recorded
// as a `runtime_posture_unavailable` warning) instead of aborting it, so the
// audit stays usable while the daemon is down. Connection errors pass through
// redact_auth_error before being surfaced.
fn load_runtime_security_snapshot(offline: bool) -> Result<RuntimeSecuritySnapshot> {
    if offline {
        return Ok(RuntimeSecuritySnapshot {
            used_runtime_posture: false,
            deployment: None,
            auth_summary: None,
            browser: None,
            error: None,
        });
    }

    let runtime = build_runtime()?;
    let snapshot = runtime.block_on(async {
        let context =
            match client::control_plane::connect_admin_console(app::ConnectionOverrides::default())
                .await
            {
                Ok(context) => context,
                Err(error) => {
                    return RuntimeSecuritySnapshot {
                        used_runtime_posture: false,
                        deployment: None,
                        auth_summary: None,
                        browser: None,
                        error: Some(redact_auth_error(error.to_string().as_str())),
                    };
                }
            };
        let deployment = context.client.get_deployment_posture().await;
        let auth_health = context.client.get_auth_health(true, None).await;
        let diagnostics = context.client.get_diagnostics().await;
        match (deployment, auth_health, diagnostics) {
            (Ok(deployment), Ok(auth_health), Ok(diagnostics)) => RuntimeSecuritySnapshot {
                used_runtime_posture: true,
                deployment: Some(deployment),
                auth_summary: serde_json::from_value::<SecurityAuthHealthSummary>(
                    auth_health.summary,
                )
                .ok(),
                browser: extract_runtime_browser_security_snapshot(&diagnostics),
                error: None,
            },
            (deployment_result, auth_result, diagnostics_result) => {
                let mut errors = Vec::new();
                if let Err(error) = deployment_result {
                    errors.push(redact_auth_error(error.to_string().as_str()));
                }
                if let Err(error) = auth_result {
                    errors.push(redact_auth_error(error.to_string().as_str()));
                }
                if let Err(error) = diagnostics_result {
                    errors.push(redact_auth_error(error.to_string().as_str()));
                }
                RuntimeSecuritySnapshot {
                    used_runtime_posture: false,
                    deployment: None,
                    auth_summary: None,
                    browser: None,
                    error: Some(errors.join("; ")),
                }
            }
        }
    });
    Ok(snapshot)
}

fn extract_runtime_browser_security_snapshot(
    payload: &Value,
) -> Option<SecurityBrowserRuntimeSnapshot> {
    let browser = payload.get("browserd")?;
    Some(SecurityBrowserRuntimeSnapshot {
        enabled: browser.get("enabled").and_then(Value::as_bool),
        health_status: browser
            .pointer("/health/status")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned),
        active_sessions: browser
            .pointer("/sessions/active")
            .and_then(Value::as_u64)
            .or_else(|| browser.pointer("/health/active_sessions").and_then(Value::as_u64)),
        recent_relay_action_failures: browser
            .pointer("/failures/recent_relay_action_failures")
            .and_then(Value::as_u64),
        recent_health_failures: browser
            .pointer("/failures/recent_health_failures")
            .and_then(Value::as_u64),
    })
}

fn load_local_security_config_snapshot(
    path: Option<String>,
) -> Result<LocalSecurityConfigSnapshot> {
    let resolved = match path {
        Some(path) => resolve_config_path(Some(path), false)?,
        None => match effective_config_path() {
            Some(path) => path,
            None => {
                return Ok(LocalSecurityConfigSnapshot {
                    path_exists: false,
                    provider_kind: "deterministic".to_owned(),
                    auth_profile_id: None,
                    openai_api_key_vault_ref: None,
                    openai_api_key_secret_ref_configured: false,
                    openai_inline_api_key: false,
                    anthropic_api_key_vault_ref: None,
                    anthropic_api_key_secret_ref_configured: false,
                    anthropic_inline_api_key: false,
                    browser_service_enabled: false,
                    browser_service_auth_token_configured: false,
                    effective_provider_kind: None,
                    process_runner: LocalProcessRunnerConfigSnapshot::default(),
                });
            }
        },
    };
    let path_ref = Path::new(&resolved);
    let (document, _) = load_document_from_existing_path(path_ref)
        .with_context(|| format!("failed to parse {resolved}"))?;
    let provider_kind = get_value_at_path(&document, "model_provider.kind")?
        .and_then(toml::Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or("deterministic")
        .to_owned();
    let auth_profile_id = get_value_at_path(&document, "model_provider.auth_profile_id")?
        .and_then(toml::Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
        .or_else(|| {
            get_value_at_path(&document, "model_provider.auth_profile_ref")
                .ok()
                .flatten()
                .and_then(toml::Value::as_str)
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(ToOwned::to_owned)
        });
    let openai_api_key_vault_ref =
        get_value_at_path(&document, "model_provider.openai_api_key_vault_ref")?
            .and_then(toml::Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(ToOwned::to_owned);
    let openai_api_key_secret_ref_configured =
        structured_secret_ref_configured(&document, "model_provider.openai_api_key_secret_ref")?;
    let openai_inline_api_key = get_value_at_path(&document, "model_provider.openai_api_key")?
        .and_then(toml::Value::as_str)
        .map(str::trim)
        .is_some_and(|value| !value.is_empty());
    let anthropic_api_key_vault_ref =
        get_value_at_path(&document, "model_provider.anthropic_api_key_vault_ref")?
            .and_then(toml::Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(ToOwned::to_owned);
    let anthropic_api_key_secret_ref_configured =
        structured_secret_ref_configured(&document, "model_provider.anthropic_api_key_secret_ref")?;
    let anthropic_inline_api_key =
        get_value_at_path(&document, "model_provider.anthropic_api_key")?
            .and_then(toml::Value::as_str)
            .map(str::trim)
            .is_some_and(|value| !value.is_empty());
    let browser_service_enabled =
        get_value_at_path(&document, "tool_call.browser_service.enabled")?
            .and_then(toml::Value::as_bool)
            .unwrap_or(false);
    let browser_service_auth_token_configured =
        get_value_at_path(&document, "tool_call.browser_service.auth_token")?
            .and_then(toml::Value::as_str)
            .map(str::trim)
            .is_some_and(|value| !value.is_empty());
    let effective_provider_kind =
        load_models_status(Some(resolved.clone())).ok().map(|status| status.provider_kind);
    let process_runner = load_local_process_runner_config_snapshot(&document)?;

    Ok(LocalSecurityConfigSnapshot {
        path_exists: true,
        provider_kind,
        auth_profile_id,
        openai_api_key_vault_ref,
        openai_api_key_secret_ref_configured,
        openai_inline_api_key,
        anthropic_api_key_vault_ref,
        anthropic_api_key_secret_ref_configured,
        anthropic_inline_api_key,
        browser_service_enabled,
        browser_service_auth_token_configured,
        effective_provider_kind,
        process_runner,
    })
}

fn structured_secret_ref_configured(document: &toml::Value, path: &str) -> Result<bool> {
    let Some(value) = get_value_at_path(document, path)? else {
        return Ok(false);
    };
    let Ok(secret_ref) = value.clone().try_into::<SecretRef>() else {
        return Ok(false);
    };
    Ok(secret_ref.validate().is_ok())
}

fn load_local_process_runner_config_snapshot(
    document: &toml::Value,
) -> Result<LocalProcessRunnerConfigSnapshot> {
    let enabled = get_value_at_path(document, "tool_call.process_runner.enabled")?
        .and_then(toml::Value::as_bool)
        .unwrap_or(false);
    let tier = normalize_process_runner_token(
        get_value_at_path(document, "tool_call.process_runner.tier")?
            .and_then(toml::Value::as_str)
            .unwrap_or("b"),
    );
    let allowed_executables_wildcard =
        get_value_at_path(document, "tool_call.process_runner.allowed_executables")?
            .and_then(toml::Value::as_array)
            .map(|values| {
                values.iter().filter_map(toml::Value::as_str).any(|value| value.trim() == "*")
            })
            .unwrap_or(false);
    let egress_enforcement_mode = normalize_process_runner_token(
        get_value_at_path(document, "tool_call.process_runner.egress_enforcement_mode")?
            .and_then(toml::Value::as_str)
            .unwrap_or("strict"),
    );

    Ok(LocalProcessRunnerConfigSnapshot {
        enabled,
        tier,
        allowed_executables_wildcard,
        egress_enforcement_mode,
    })
}

fn normalize_process_runner_token(value: &str) -> String {
    value.trim().replace('-', "_").to_ascii_lowercase()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::args::RootOptions;
    use crate::commands::secrets::SecretAuditSummary;

    fn minimal_doctor() -> DoctorReport {
        DoctorReport {
            generated_at_unix_ms: 1,
            profile: None,
            checks: Vec::new(),
            summary: DoctorSummary {
                required_checks_total: 0,
                required_checks_ok: 0,
                required_checks_failed: 0,
                warning_checks_failed: 0,
                info_checks_failed: 0,
            },
            config: DoctorConfigSnapshot {
                path: None,
                exists: true,
                parsed: true,
                migration: None,
                error: None,
            },
            identity: DoctorIdentitySnapshot {
                store_root: None,
                exists: true,
                writable: true,
                error: None,
            },
            connectivity: DoctorConnectivitySnapshot {
                daemon_url: "http://127.0.0.1:7142".to_owned(),
                grpc_url: "http://127.0.0.1:50051".to_owned(),
                http: DoctorConnectivityProbe { ok: true, message: None },
                grpc: DoctorConnectivityProbe { ok: true, message: None },
                admin: DoctorConnectivityProbe { ok: true, message: None },
            },
            provider_auth: DoctorProviderAuthSnapshot {
                fetched: true,
                model_provider: None,
                auth_summary: None,
                error: None,
            },
            browser: DoctorBrowserSnapshot {
                configured_enabled: false,
                auth_token_configured: false,
                endpoint: "http://127.0.0.1:7543".to_owned(),
                health_base_url: "http://127.0.0.1:7143".to_owned(),
                port_diagnostics: Vec::new(),
                connect_timeout_ms: Some(1500),
                request_timeout_ms: Some(15000),
                max_screenshot_bytes: Some(262_144),
                max_title_bytes: Some(4096),
                state_dir_configured: false,
                state_key_vault_ref_configured: false,
                diagnostics_fetched: false,
                health_status: None,
                active_sessions: None,
                recent_relay_action_failures: None,
                recent_health_failures: None,
                error: None,
            },
            feature_rollouts: DoctorFeatureRolloutsSnapshot {
                fetched: true,
                flag_count: 0,
                enabled_flags: 0,
                effective_active_flags: 0,
                not_effectively_active_flags: 0,
                non_authoritative_flags: 0,
                unknown_activation_authority_flags: 0,
                inactive_flags: 0,
                maturity_counts: BTreeMap::new(),
                promotion_state_counts: BTreeMap::new(),
                qualified_hot_path_flags: 0,
                usage: Vec::new(),
                inactive: Vec::new(),
                migration_note: None,
                error: None,
            },
            access: DoctorAccessSnapshot {
                registry_path: Some("state/access_registry.json".to_owned()),
                registry_exists: true,
                parsed: true,
                compat_api_enabled: false,
                api_tokens_enabled: false,
                team_mode_enabled: false,
                rbac_enabled: false,
                staged_rollout_enabled: false,
                backfill_required: false,
                blocking_issues: 0,
                warning_issues: 0,
                external_api_safe_mode: true,
                team_mode_safe_mode: true,
                error: None,
            },
            skills: SkillsInventorySnapshot {
                skills_root: "state/skills".to_owned(),
                installed_total: 0,
                current_total: 0,
                eligible_total: 0,
                quarantined_total: 0,
                disabled_total: 0,
                runtime_unknown_total: 0,
                missing_secrets_total: 0,
                publishers: Vec::new(),
                trust_decisions: BTreeMap::new(),
                error: None,
            },
            sandbox: DoctorSandboxSnapshot {
                tier_b_egress_allowlists_preflight_only: true,
                tier_c_strict_offline_only: true,
                tier_c_windows_backend_supported: true,
            },
            deployment: DoctorDeploymentSnapshot {
                mode: "local_desktop".to_owned(),
                bind_profile: "loopback_only".to_owned(),
                binds: DoctorDeploymentBindSnapshot {
                    admin: "127.0.0.1:7142".to_owned(),
                    grpc: "127.0.0.1:50051".to_owned(),
                    quic: "127.0.0.1:50052".to_owned(),
                },
                gateway_tls_enabled: true,
                admin_auth_required: true,
                admin_token_configured: true,
                dangerous_remote_bind_ack_config: false,
                dangerous_remote_bind_ack_env: false,
                remote_bind_detected: false,
                warnings: Vec::new(),
            },
            config_ref_health: None,
        }
    }

    fn minimal_secrets() -> SecretAuditPayload {
        SecretAuditPayload {
            path: "defaults".to_owned(),
            runtime_profiles_inspected: false,
            runtime_error: None,
            references: Vec::new(),
            findings: Vec::new(),
            summary: SecretAuditSummary {
                total_references: 0,
                resolved_references: 0,
                blocking_findings: 0,
                warning_findings: 0,
                info_findings: 0,
            },
        }
    }

    #[test]
    fn security_audit_flags_missing_model_provider_auth() {
        let doctor = minimal_doctor();
        let local = LocalSecurityConfigSnapshot {
            path_exists: true,
            provider_kind: "openai_compatible".to_owned(),
            auth_profile_id: None,
            openai_api_key_vault_ref: None,
            openai_api_key_secret_ref_configured: false,
            openai_inline_api_key: false,
            anthropic_api_key_vault_ref: None,
            anthropic_api_key_secret_ref_configured: false,
            anthropic_inline_api_key: false,
            browser_service_enabled: false,
            browser_service_auth_token_configured: false,
            effective_provider_kind: None,
            process_runner: LocalProcessRunnerConfigSnapshot::default(),
        };
        let runtime = RuntimeSecuritySnapshot {
            used_runtime_posture: false,
            deployment: None,
            auth_summary: None,
            browser: None,
            error: None,
        };
        let findings = build_security_findings(&doctor, &local, &runtime, &minimal_secrets());
        assert!(
            findings.iter().any(|finding| finding.code == "model_provider_missing_auth"),
            "security audit should flag missing model provider auth for openai_compatible configs"
        );
    }

    #[test]
    fn security_audit_ignores_missing_model_provider_auth_for_effective_deterministic_setup() {
        let doctor = minimal_doctor();
        let local = LocalSecurityConfigSnapshot {
            path_exists: true,
            provider_kind: "openai_compatible".to_owned(),
            auth_profile_id: None,
            openai_api_key_vault_ref: None,
            openai_api_key_secret_ref_configured: false,
            openai_inline_api_key: false,
            anthropic_api_key_vault_ref: None,
            anthropic_api_key_secret_ref_configured: false,
            anthropic_inline_api_key: false,
            browser_service_enabled: false,
            browser_service_auth_token_configured: false,
            effective_provider_kind: Some("deterministic".to_owned()),
            process_runner: LocalProcessRunnerConfigSnapshot::default(),
        };
        let runtime = RuntimeSecuritySnapshot {
            used_runtime_posture: false,
            deployment: None,
            auth_summary: None,
            browser: None,
            error: None,
        };
        let findings = build_security_findings(&doctor, &local, &runtime, &minimal_secrets());
        assert!(
            !findings.iter().any(|finding| finding.code == "model_provider_missing_auth"),
            "security audit should ignore missing OpenAI auth when the effective model status is deterministic"
        );
    }

    #[test]
    fn security_audit_flags_missing_model_provider_auth_when_effective_provider_is_openai() {
        let doctor = minimal_doctor();
        let local = LocalSecurityConfigSnapshot {
            path_exists: true,
            provider_kind: "openai_compatible".to_owned(),
            auth_profile_id: None,
            openai_api_key_vault_ref: None,
            openai_api_key_secret_ref_configured: false,
            openai_inline_api_key: false,
            anthropic_api_key_vault_ref: None,
            anthropic_api_key_secret_ref_configured: false,
            anthropic_inline_api_key: false,
            browser_service_enabled: false,
            browser_service_auth_token_configured: false,
            effective_provider_kind: Some("openai_compatible".to_owned()),
            process_runner: LocalProcessRunnerConfigSnapshot::default(),
        };
        let runtime = RuntimeSecuritySnapshot {
            used_runtime_posture: false,
            deployment: None,
            auth_summary: None,
            browser: None,
            error: None,
        };
        let findings = build_security_findings(&doctor, &local, &runtime, &minimal_secrets());
        assert!(
            findings.iter().any(|finding| finding.code == "model_provider_missing_auth"),
            "security audit should still flag missing OpenAI auth when the effective model status expects OpenAI"
        );
    }

    #[test]
    fn security_audit_ignores_missing_model_provider_auth_for_anthropic_vault_ref() {
        let doctor = minimal_doctor();
        let local = LocalSecurityConfigSnapshot {
            path_exists: true,
            provider_kind: "anthropic".to_owned(),
            auth_profile_id: None,
            openai_api_key_vault_ref: None,
            openai_api_key_secret_ref_configured: false,
            openai_inline_api_key: false,
            anthropic_api_key_vault_ref: Some("global/minimax_api_key".to_owned()),
            anthropic_api_key_secret_ref_configured: false,
            anthropic_inline_api_key: false,
            browser_service_enabled: false,
            browser_service_auth_token_configured: false,
            effective_provider_kind: Some("anthropic".to_owned()),
            process_runner: LocalProcessRunnerConfigSnapshot::default(),
        };
        let runtime = RuntimeSecuritySnapshot {
            used_runtime_posture: false,
            deployment: None,
            auth_summary: None,
            browser: None,
            error: None,
        };
        let findings = build_security_findings(&doctor, &local, &runtime, &minimal_secrets());
        assert!(
            !findings.iter().any(|finding| finding.code == "model_provider_missing_auth"),
            "security audit should not flag missing auth when Anthropic-compatible vault auth is configured"
        );
    }

    #[test]
    fn security_audit_warns_on_permissive_process_runner_profile() {
        let doctor = minimal_doctor();
        let local = LocalSecurityConfigSnapshot {
            path_exists: true,
            provider_kind: "deterministic".to_owned(),
            auth_profile_id: None,
            openai_api_key_vault_ref: None,
            openai_api_key_secret_ref_configured: false,
            openai_inline_api_key: false,
            anthropic_api_key_vault_ref: None,
            anthropic_api_key_secret_ref_configured: false,
            anthropic_inline_api_key: false,
            browser_service_enabled: false,
            browser_service_auth_token_configured: false,
            effective_provider_kind: Some("deterministic".to_owned()),
            process_runner: LocalProcessRunnerConfigSnapshot {
                enabled: true,
                tier: "tier_b".to_owned(),
                allowed_executables_wildcard: true,
                egress_enforcement_mode: "none".to_owned(),
            },
        };
        let runtime = RuntimeSecuritySnapshot {
            used_runtime_posture: false,
            deployment: None,
            auth_summary: None,
            browser: None,
            error: None,
        };

        let findings = build_security_findings(&doctor, &local, &runtime, &minimal_secrets());

        assert!(
            findings.iter().any(|finding| {
                finding.code == "process_runner_permissive_host_process_profile"
                    && finding.severity == "warning"
                    && finding.remediation.contains("explicit allowlist")
                    && finding.remediation.contains("non-interpreter host executables")
            }),
            "security audit should flag permissive local process-runner posture"
        );
    }

    #[test]
    fn security_audit_flags_extension_diagnostics_provider_gaps() {
        let mut doctor = minimal_doctor();
        doctor.skills.runtime_unknown_total = 1;
        doctor.skills.missing_secrets_total = 2;
        let local = LocalSecurityConfigSnapshot {
            path_exists: true,
            provider_kind: "deterministic".to_owned(),
            auth_profile_id: None,
            openai_api_key_vault_ref: None,
            openai_api_key_secret_ref_configured: false,
            openai_inline_api_key: false,
            anthropic_api_key_vault_ref: None,
            anthropic_api_key_secret_ref_configured: false,
            anthropic_inline_api_key: false,
            browser_service_enabled: false,
            browser_service_auth_token_configured: false,
            effective_provider_kind: Some("deterministic".to_owned()),
            process_runner: LocalProcessRunnerConfigSnapshot::default(),
        };
        let runtime = RuntimeSecuritySnapshot {
            used_runtime_posture: false,
            deployment: None,
            auth_summary: None,
            browser: None,
            error: None,
        };

        let findings = build_security_findings(&doctor, &local, &runtime, &minimal_secrets());

        assert!(findings.iter().any(|finding| {
            finding.code == "extension_diagnostics_provider_degraded"
                && finding.component == "extensions"
                && finding.remediation.contains("plugins doctor")
        }));
    }

    #[test]
    fn attack_surface_projection_captures_process_runner_side_effects() {
        let doctor = minimal_doctor();
        let local = LocalSecurityConfigSnapshot {
            path_exists: true,
            provider_kind: "deterministic".to_owned(),
            auth_profile_id: None,
            openai_api_key_vault_ref: None,
            openai_api_key_secret_ref_configured: false,
            openai_inline_api_key: false,
            anthropic_api_key_vault_ref: None,
            anthropic_api_key_secret_ref_configured: false,
            anthropic_inline_api_key: false,
            browser_service_enabled: false,
            browser_service_auth_token_configured: false,
            effective_provider_kind: Some("deterministic".to_owned()),
            process_runner: LocalProcessRunnerConfigSnapshot {
                enabled: true,
                tier: "tier_b".to_owned(),
                allowed_executables_wildcard: true,
                egress_enforcement_mode: "none".to_owned(),
            },
        };
        let runtime = RuntimeSecuritySnapshot {
            used_runtime_posture: false,
            deployment: None,
            auth_summary: None,
            browser: None,
            error: None,
        };

        let graph = build_attack_surface_graph(&doctor, &local, &runtime, &minimal_secrets());
        let audit = audit_attack_surface_graph(&graph);

        assert!(graph.tools.iter().any(|tool| tool.tool_name == "palyra.process_runner"));
        assert_eq!(
            audit.summary.highest_side_effect_without_human_approval,
            SideEffectLevel::ProcessExecution
        );
        assert!(audit.findings.iter().any(|finding| {
            finding.reason_code.as_str() == "unrestricted_egress_without_approval"
        }));
    }

    #[test]
    fn local_security_snapshot_uses_active_root_config_path() -> Result<()> {
        let _guard = app::test_env_lock_for_tests().lock().expect("env lock");
        app::clear_root_context_for_tests();

        let temp = tempfile::tempdir()?;
        let config_path = temp.path().join("harness").join("palyra.toml");
        std::fs::create_dir_all(config_path.parent().expect("config parent"))?;
        std::fs::write(
            config_path.as_path(),
            r#"
version = 1
[model_provider]
kind = "anthropic"
auth_provider_kind = "minimax"
anthropic_base_url = "https://api.minimax.io/anthropic"
anthropic_model = "MiniMax-M2.7"
anthropic_api_key_vault_ref = "global/minimax_api_key"
"#,
        )?;
        let state_root = temp.path().join("state");
        let _context = app::install_root_context(RootOptions {
            config_path: Some(config_path.display().to_string()),
            state_root: Some(state_root.display().to_string()),
            ..RootOptions::default()
        })?;

        let snapshot = load_local_security_config_snapshot(None)?;

        assert_eq!(snapshot.provider_kind, "anthropic");
        assert_eq!(snapshot.anthropic_api_key_vault_ref.as_deref(), Some("global/minimax_api_key"));
        assert_eq!(missing_model_auth_kind(&snapshot), None);

        app::clear_root_context_for_tests();
        Ok(())
    }

    #[test]
    fn local_security_snapshot_accepts_anthropic_structured_secret_ref_auth() -> Result<()> {
        let _guard = app::test_env_lock_for_tests().lock().expect("env lock");
        app::clear_root_context_for_tests();

        let temp = tempfile::tempdir()?;
        let config_path = temp.path().join("harness").join("palyra.toml");
        std::fs::create_dir_all(config_path.parent().expect("config parent"))?;
        std::fs::write(
            config_path.as_path(),
            r#"
version = 1
[model_provider]
kind = "anthropic"
auth_provider_kind = "minimax"
anthropic_base_url = "https://api.minimax.io/anthropic"
anthropic_model = "MiniMax-M2.7"
[model_provider.anthropic_api_key_secret_ref]
kind = "env"
variable = "PALYRA_MODEL_PROVIDER_ANTHROPIC_API_KEY"
"#,
        )?;
        let state_root = temp.path().join("state");
        let _context = app::install_root_context(RootOptions {
            config_path: Some(config_path.display().to_string()),
            state_root: Some(state_root.display().to_string()),
            ..RootOptions::default()
        })?;

        let snapshot = load_local_security_config_snapshot(None)?;

        assert_eq!(snapshot.provider_kind, "anthropic");
        assert!(snapshot.anthropic_api_key_secret_ref_configured);
        assert_eq!(missing_model_auth_kind(&snapshot), None);

        app::clear_root_context_for_tests();
        Ok(())
    }

    #[test]
    fn security_audit_flags_remote_bind_without_tls() {
        let doctor = minimal_doctor();
        let local = LocalSecurityConfigSnapshot {
            path_exists: true,
            provider_kind: "deterministic".to_owned(),
            auth_profile_id: None,
            openai_api_key_vault_ref: None,
            openai_api_key_secret_ref_configured: false,
            openai_inline_api_key: false,
            anthropic_api_key_vault_ref: None,
            anthropic_api_key_secret_ref_configured: false,
            anthropic_inline_api_key: false,
            browser_service_enabled: false,
            browser_service_auth_token_configured: false,
            effective_provider_kind: Some("deterministic".to_owned()),
            process_runner: LocalProcessRunnerConfigSnapshot::default(),
        };
        let runtime = RuntimeSecuritySnapshot {
            used_runtime_posture: true,
            deployment: Some(control_plane::DeploymentPostureSummary {
                contract: control_plane::ContractDescriptor {
                    contract_version: "control-plane.v1".to_owned(),
                },
                profile: "single-vm".to_owned(),
                profile_manifest: serde_json::json!({
                    "schema_version": 1,
                    "profile_id": "single-vm",
                }),
                mode: "remote_vps".to_owned(),
                bind_profile: "public_tls".to_owned(),
                bind_addresses: control_plane::DeploymentBindAddresses {
                    admin: "0.0.0.0:7142".to_owned(),
                    grpc: "0.0.0.0:50051".to_owned(),
                    quic: "0.0.0.0:50052".to_owned(),
                },
                tls: control_plane::DeploymentTlsSummary { gateway_enabled: false },
                admin_auth_required: true,
                dangerous_remote_bind_ack: control_plane::DangerousRemoteBindAckSummary {
                    config: true,
                    env: true,
                    env_name: "PALYRA_GATEWAY_DANGEROUS_REMOTE_BIND_ACK".to_owned(),
                },
                remote_bind_detected: true,
                last_remote_admin_access_attempt: None,
                warnings: Vec::new(),
            }),
            auth_summary: None,
            browser: None,
            error: None,
        };
        let findings = build_security_findings(&doctor, &local, &runtime, &minimal_secrets());
        assert!(
            findings.iter().any(|finding| finding.code == "remote_bind_without_tls"),
            "security audit should flag remote bind without TLS"
        );
    }

    #[test]
    fn security_audit_flags_browser_runtime_failures() {
        let doctor = minimal_doctor();
        let local = LocalSecurityConfigSnapshot {
            path_exists: true,
            provider_kind: "deterministic".to_owned(),
            auth_profile_id: None,
            openai_api_key_vault_ref: None,
            openai_api_key_secret_ref_configured: false,
            openai_inline_api_key: false,
            anthropic_api_key_vault_ref: None,
            anthropic_api_key_secret_ref_configured: false,
            anthropic_inline_api_key: false,
            browser_service_enabled: true,
            browser_service_auth_token_configured: true,
            effective_provider_kind: Some("deterministic".to_owned()),
            process_runner: LocalProcessRunnerConfigSnapshot::default(),
        };
        let runtime = RuntimeSecuritySnapshot {
            used_runtime_posture: true,
            deployment: None,
            auth_summary: None,
            browser: Some(SecurityBrowserRuntimeSnapshot {
                enabled: Some(true),
                health_status: Some("degraded".to_owned()),
                active_sessions: Some(2),
                recent_relay_action_failures: Some(3),
                recent_health_failures: Some(1),
            }),
            error: None,
        };
        let findings = build_security_findings(&doctor, &local, &runtime, &minimal_secrets());
        assert!(
            findings.iter().any(|finding| finding.code == "browser_service_runtime_degraded"),
            "security audit should flag degraded browser runtime health"
        );
        assert!(
            findings.iter().any(|finding| finding.code == "browser_service_recent_relay_failures"),
            "security audit should flag recent browser relay failures"
        );
        assert!(
            findings.iter().any(|finding| finding.code == "browser_service_recent_health_failures"),
            "security audit should flag recent browser health failures"
        );
    }
}
