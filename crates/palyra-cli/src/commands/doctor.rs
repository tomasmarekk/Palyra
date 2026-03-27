use crate::*;

pub(crate) fn run_doctor(strict: bool, json: bool) -> Result<()> {
    let checks = build_doctor_checks();
    let report = build_doctor_report(checks.as_slice())?;
    let blocking_checks = checks
        .iter()
        .filter(|check| check.severity == DoctorSeverity::Blocking && !check.ok)
        .collect::<Vec<_>>();
    let warning_checks = checks
        .iter()
        .filter(|check| check.severity == DoctorSeverity::Warning && !check.ok)
        .collect::<Vec<_>>();
    let info_checks = checks
        .iter()
        .filter(|check| check.severity == DoctorSeverity::Info && !check.ok)
        .collect::<Vec<_>>();

    if json {
        let encoded = serde_json::to_string_pretty(&report)
            .context("failed to serialize doctor JSON report")?;
        println!("{encoded}");
    } else {
        for check in &checks {
            println!(
                "doctor.check key={} ok={} required={} severity={}",
                check.key,
                check.ok,
                check.required,
                check.severity.as_str()
            );
        }
        println!(
            "doctor.config path={} exists={} parsed={}",
            report.config.path.as_deref().unwrap_or("none"),
            report.config.exists,
            report.config.parsed
        );
        println!(
            "doctor.identity root={} exists={} writable={}",
            report.identity.store_root.as_deref().unwrap_or("unavailable"),
            report.identity.exists,
            report.identity.writable
        );
        println!(
            "doctor.connectivity daemon_url={} http_ok={} grpc_ok={} admin_ok={}",
            report.connectivity.daemon_url,
            report.connectivity.http.ok,
            report.connectivity.grpc.ok,
            report.connectivity.admin.ok
        );
        println!(
            "doctor.browser enabled={} auth_token_configured={} endpoint={} connect_timeout_ms={} request_timeout_ms={} max_screenshot_bytes={} max_title_bytes={} state_dir_configured={} state_key_vault_ref_configured={} diagnostics_fetched={} health_status={} active_sessions={} recent_relay_failures={} recent_health_failures={}",
            report.browser.configured_enabled,
            report.browser.auth_token_configured,
            report.browser.endpoint,
            report
                .browser
                .connect_timeout_ms
                .map(|value| value.to_string())
                .unwrap_or_else(|| "-".to_owned()),
            report
                .browser
                .request_timeout_ms
                .map(|value| value.to_string())
                .unwrap_or_else(|| "-".to_owned()),
            report
                .browser
                .max_screenshot_bytes
                .map(|value| value.to_string())
                .unwrap_or_else(|| "-".to_owned()),
            report
                .browser
                .max_title_bytes
                .map(|value| value.to_string())
                .unwrap_or_else(|| "-".to_owned()),
            report.browser.state_dir_configured,
            report.browser.state_key_vault_ref_configured,
            report.browser.diagnostics_fetched,
            report.browser.health_status.as_deref().unwrap_or("-"),
            report
                .browser
                .active_sessions
                .map(|value| value.to_string())
                .unwrap_or_else(|| "-".to_owned()),
            report
                .browser
                .recent_relay_action_failures
                .map(|value| value.to_string())
                .unwrap_or_else(|| "-".to_owned()),
            report
                .browser
                .recent_health_failures
                .map(|value| value.to_string())
                .unwrap_or_else(|| "-".to_owned()),
        );
        if let Some(error) = report.browser.error.as_deref() {
            println!("doctor.browser_error={error}");
        }
        println!(
            "doctor.sandbox tier_b_preflight_only={} tier_c_strict_offline={} tier_c_windows_backend_supported={}",
            report.sandbox.tier_b_egress_allowlists_preflight_only,
            report.sandbox.tier_c_strict_offline_only,
            report.sandbox.tier_c_windows_backend_supported
        );
        println!(
            "doctor.deployment mode={} bind_profile={} remote_bind_detected={} gateway_tls_enabled={} admin_auth_required={} admin_token_configured={}",
            report.deployment.mode,
            report.deployment.bind_profile,
            report.deployment.remote_bind_detected,
            report.deployment.gateway_tls_enabled,
            report.deployment.admin_auth_required,
            report.deployment.admin_token_configured,
        );
        println!(
            "doctor.summary blocking={} warnings={} info={} required_checks_failed={}",
            blocking_checks.len(),
            warning_checks.len(),
            info_checks.len(),
            report.summary.required_checks_failed
        );
        for check in blocking_checks.as_slice() {
            println!("doctor.finding severity=blocking key={}", check.key);
            for remediation in check.remediation {
                println!("doctor.remediation key={} command={}", check.key, remediation);
            }
        }
        for check in warning_checks.as_slice() {
            println!("doctor.finding severity=warning key={}", check.key);
            for remediation in check.remediation {
                println!("doctor.remediation key={} command={}", check.key, remediation);
            }
        }
        for check in info_checks.as_slice() {
            println!("doctor.finding severity=info key={}", check.key);
            for remediation in check.remediation {
                println!("doctor.remediation key={} command={}", check.key, remediation);
            }
        }
        for warning in report.deployment.warnings.as_slice() {
            println!("doctor.warning={warning}");
        }
        if checks.iter().any(|check| check.key == "memory_embeddings_model_configured" && !check.ok)
        {
            println!(
                "doctor.hint.memory_embeddings_model=configure model_provider.openai_embeddings_model (or PALYRA_MODEL_PROVIDER_OPENAI_EMBEDDINGS_MODEL) for openai-compatible semantic memory embeddings"
            );
        }
        let mut next_steps = Vec::new();
        if !report.connectivity.http.ok
            || !report.connectivity.grpc.ok
            || !blocking_checks.is_empty()
        {
            next_steps.push("palyra health");
            next_steps.push("palyra logs --lines 50");
        }
        if !report.deployment.warnings.is_empty() || !warning_checks.is_empty() {
            next_steps.push("palyra security audit --offline");
        }
        if report.browser.configured_enabled
            && (!report.browser.auth_token_configured
                || report.browser.error.is_some()
                || report.browser.health_status.as_deref().is_some_and(|value| value != "ok")
                || report.browser.recent_relay_action_failures.unwrap_or(0) > 0
                || report.browser.recent_health_failures.unwrap_or(0) > 0)
        {
            next_steps.push("palyra browser status");
        }
        if !blocking_checks.is_empty()
            || !warning_checks.is_empty()
            || !info_checks.is_empty()
            || !report.deployment.warnings.is_empty()
        {
            next_steps.push("palyra support-bundle export --output ./support-bundle.json");
        }
        let mut seen = std::collections::BTreeSet::new();
        for step in next_steps {
            if seen.insert(step) {
                println!("doctor.next_step={step}");
            }
        }
    }

    if strict {
        let failing_required =
            checks.iter().find(|check| check.severity == DoctorSeverity::Blocking && !check.ok);
        if let Some(check) = failing_required {
            anyhow::bail!("strict doctor failed: {}", check.key);
        }
    }

    std::io::stdout().flush().context("stdout flush failed")
}
