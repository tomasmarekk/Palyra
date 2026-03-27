use crate::*;
use palyra_control_plane as control_plane;

use super::secrets::{build_secrets_audit_payload, SecretAuditFinding, SecretAuditPayload};

#[derive(Debug, Serialize)]
struct SecurityAuditPayload {
    generated_at_unix_ms: i64,
    strict: bool,
    used_runtime_posture: bool,
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
    api_key_vault_ref: Option<String>,
    inline_api_key: bool,
    browser_service_enabled: bool,
    browser_service_auth_token_configured: bool,
}

pub(crate) fn run_security(command: SecurityCommand) -> Result<()> {
    match command {
        SecurityCommand::Audit { path, offline, strict, json } => {
            let checks = build_doctor_checks();
            let doctor = build_doctor_report(checks.as_slice())?;
            let secrets = build_secrets_audit_payload(path.clone(), offline)?;
            let local_config = load_local_security_config_snapshot(path)?;
            let runtime = load_runtime_security_snapshot(offline)?;
            let findings = build_security_findings(&doctor, &local_config, &runtime, &secrets);
            let payload = SecurityAuditPayload {
                generated_at_unix_ms: unix_now_ms(),
                strict,
                used_runtime_posture: runtime.used_runtime_posture,
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
    }
    std::io::stdout().flush().context("stdout flush failed")
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

    if local_config.provider_kind == "openai_compatible"
        && local_config.auth_profile_id.is_none()
        && local_config.api_key_vault_ref.is_none()
        && !local_config.inline_api_key
    {
        findings.push(SecurityFinding {
            severity: "blocking".to_owned(),
            code: "model_provider_missing_auth".to_owned(),
            component: "model_provider".to_owned(),
            message: "OpenAI-compatible model provider is configured without any auth source.".to_owned(),
            remediation: "Configure OpenAI auth with `palyra auth openai api-key` or select a default auth profile before relying on the runtime.".to_owned(),
        });
    }

    if local_config.inline_api_key {
        findings.push(SecurityFinding {
            severity: "warning".to_owned(),
            code: "inline_api_key".to_owned(),
            component: "model_provider".to_owned(),
            message: "The OpenAI API key is configured inline in the daemon config.".to_owned(),
            remediation: "Move the credential into the vault via `palyra auth openai api-key` or `palyra secrets configure openai-api-key`.".to_owned(),
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

fn map_secret_finding_to_security_finding(finding: &SecretAuditFinding) -> SecurityFinding {
    SecurityFinding {
        severity: finding.severity.clone(),
        code: format!("secrets_{}", finding.code),
        component: finding.component.clone(),
        message: finding.message.clone(),
        remediation: finding.remediation.clone(),
    }
}

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
        None => match find_default_config_path() {
            Some(path) => path,
            None => {
                return Ok(LocalSecurityConfigSnapshot {
                    path_exists: false,
                    provider_kind: "deterministic".to_owned(),
                    auth_profile_id: None,
                    api_key_vault_ref: None,
                    inline_api_key: false,
                    browser_service_enabled: false,
                    browser_service_auth_token_configured: false,
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
    let api_key_vault_ref =
        get_value_at_path(&document, "model_provider.openai_api_key_vault_ref")?
            .and_then(toml::Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(ToOwned::to_owned);
    let inline_api_key = get_value_at_path(&document, "model_provider.openai_api_key")?
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

    Ok(LocalSecurityConfigSnapshot {
        path_exists: true,
        provider_kind,
        auth_profile_id,
        api_key_vault_ref,
        inline_api_key,
        browser_service_enabled,
        browser_service_auth_token_configured,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commands::secrets::SecretAuditSummary;

    fn minimal_doctor() -> DoctorReport {
        DoctorReport {
            generated_at_unix_ms: 1,
            checks: Vec::new(),
            summary: DoctorSummary {
                required_checks_total: 0,
                required_checks_ok: 0,
                required_checks_failed: 0,
                warning_checks_failed: 0,
                info_checks_failed: 0,
            },
            config: DoctorConfigSnapshot { path: None, exists: true, parsed: true, error: None },
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
            api_key_vault_ref: None,
            inline_api_key: false,
            browser_service_enabled: false,
            browser_service_auth_token_configured: false,
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
    fn security_audit_flags_remote_bind_without_tls() {
        let doctor = minimal_doctor();
        let local = LocalSecurityConfigSnapshot {
            path_exists: true,
            provider_kind: "deterministic".to_owned(),
            auth_profile_id: None,
            api_key_vault_ref: None,
            inline_api_key: false,
            browser_service_enabled: false,
            browser_service_auth_token_configured: false,
        };
        let runtime = RuntimeSecuritySnapshot {
            used_runtime_posture: true,
            deployment: Some(control_plane::DeploymentPostureSummary {
                contract: control_plane::ContractDescriptor {
                    contract_version: "control-plane.v1".to_owned(),
                },
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
            api_key_vault_ref: None,
            inline_api_key: false,
            browser_service_enabled: true,
            browser_service_auth_token_configured: true,
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
