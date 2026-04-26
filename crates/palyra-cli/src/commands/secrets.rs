use std::collections::{BTreeMap, BTreeSet};

use crate::{args::SecretsConfigureCommand, *};
use palyra_control_plane as control_plane;

#[derive(Debug, Clone, Serialize)]
pub(crate) struct SecretAuditPayload {
    pub(crate) path: String,
    pub(crate) runtime_profiles_inspected: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) runtime_error: Option<String>,
    pub(crate) references: Vec<SecretReferenceAudit>,
    pub(crate) findings: Vec<SecretAuditFinding>,
    pub(crate) summary: SecretAuditSummary,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct SecretReferenceAudit {
    pub(crate) component: String,
    pub(crate) reference_kind: String,
    pub(crate) reference: String,
    pub(crate) scope: String,
    pub(crate) key: String,
    pub(crate) status: String,
    pub(crate) detail: String,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct SecretAuditFinding {
    pub(crate) severity: String,
    pub(crate) code: String,
    pub(crate) component: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) reference: Option<String>,
    pub(crate) message: String,
    pub(crate) remediation: String,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct SecretAuditSummary {
    pub(crate) total_references: usize,
    pub(crate) resolved_references: usize,
    pub(crate) blocking_findings: usize,
    pub(crate) warning_findings: usize,
    pub(crate) info_findings: usize,
}

#[derive(Debug, Serialize)]
struct SecretsApplyMode {
    apply_mode: String,
    affected_components: usize,
}

#[derive(Debug, Serialize)]
struct VaultSecretExplainEnvelope {
    kind: String,
    reference: String,
    scope: String,
    key: String,
    status: String,
    backend: String,
    value_bytes: u32,
    created_at_unix_ms: i64,
    updated_at_unix_ms: i64,
    configured: bool,
    configured_references: Vec<VaultSecretConfiguredReference>,
    #[serde(skip_serializing_if = "Option::is_none")]
    config_usage_error: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    last_error: Option<String>,
}

#[derive(Debug, Serialize)]
struct VaultSecretConfiguredReference {
    component: String,
    reference_kind: String,
    status: String,
}

#[derive(Debug, Clone)]
struct SecretReferenceCandidate {
    component: String,
    reference_kind: String,
    reference: String,
}

pub(crate) fn run_secrets(command: SecretsCommand) -> Result<()> {
    match command {
        SecretsCommand::Set { scope, key, value_stdin } => {
            let vault = open_cli_vault().context("failed to initialize vault runtime")?;
            let value = read_secret_bytes_from_stdin(value_stdin)?;
            let scope = parse_vault_scope(scope.as_str())?;
            let metadata = vault
                .put_secret(&scope, key.as_str(), value.as_slice())
                .with_context(|| format!("failed to store secret key={} scope={scope}", key))?;
            println!(
                "secrets.set scope={} key={} value_bytes={} backend={}",
                scope,
                metadata.key,
                metadata.value_bytes,
                vault.backend_kind().as_str(),
            );
            std::io::stdout().flush().context("stdout flush failed")
        }
        SecretsCommand::Get { scope, key, reveal } => {
            let vault = open_cli_vault().context("failed to initialize vault runtime")?;
            let scope = parse_vault_scope(scope.as_str())?;
            let value = vault
                .get_secret(&scope, key.as_str())
                .with_context(|| format!("failed to load secret key={} scope={scope}", key))?;
            if reveal {
                eprintln!(
                    "warning: printing secret bytes to stdout can leak via shell history or logs"
                );
                std::io::stdout()
                    .write_all(value.as_slice())
                    .context("failed to write secret value to stdout")?;
            } else {
                println!(
                    "secrets.get scope={} key={} value=<redacted> value_bytes={} reveal=false",
                    scope,
                    key,
                    value.len()
                );
            }
            std::io::stdout().flush().context("stdout flush failed")
        }
        SecretsCommand::List { scope } => {
            let vault = open_cli_vault().context("failed to initialize vault runtime")?;
            let scope = parse_vault_scope(scope.as_str())?;
            let listed_entries = vault
                .list_secrets(&scope)
                .with_context(|| format!("failed to list secrets for scope={scope}"))?
                .into_iter()
                .map(|entry| {
                    (
                        entry.key,
                        entry.created_at_unix_ms,
                        entry.updated_at_unix_ms,
                        entry.value_bytes,
                    )
                })
                .collect::<Vec<_>>();
            let entry_count = listed_entries.len();
            if output::preferred_json(false) {
                return output::print_json_pretty(
                    &json!({
                        "scope": scope.to_string(),
                        "count": entry_count,
                        "backend": vault.backend_kind().as_str(),
                        "entries": listed_entries.iter().map(
                            |(key, created_at_unix_ms, updated_at_unix_ms, value_bytes)| {
                                json!({
                                    "key": key,
                                    "created_at_unix_ms": created_at_unix_ms,
                                    "updated_at_unix_ms": updated_at_unix_ms,
                                    "value_bytes": value_bytes,
                                })
                            }
                        ).collect::<Vec<_>>(),
                    }),
                    "failed to encode secrets list output as JSON",
                );
            }
            if output::preferred_ndjson(false, false) {
                output::print_json_line(
                    &json!({
                        "scope": scope.to_string(),
                        "count": entry_count,
                        "backend": vault.backend_kind().as_str(),
                        "entries": listed_entries.iter().map(
                            |(key, created_at_unix_ms, updated_at_unix_ms, value_bytes)| {
                                json!({
                                    "key": key,
                                    "created_at_unix_ms": created_at_unix_ms,
                                    "updated_at_unix_ms": updated_at_unix_ms,
                                    "value_bytes": value_bytes,
                                })
                            }
                        ).collect::<Vec<_>>(),
                    }),
                    "failed to encode secrets list output as NDJSON",
                )?;
                return std::io::stdout().flush().context("stdout flush failed");
            }
            println!(
                "secrets.list scope={} count={} backend={}",
                scope,
                entry_count,
                vault.backend_kind().as_str()
            );
            for (entry_key, created_at_unix_ms, updated_at_unix_ms, value_byte_count) in
                listed_entries
            {
                println!(
                    "secrets.entry key={} created_at_unix_ms={} updated_at_unix_ms={} value_bytes={}",
                    entry_key,
                    created_at_unix_ms,
                    updated_at_unix_ms,
                    value_byte_count
                );
            }
            std::io::stdout().flush().context("stdout flush failed")
        }
        SecretsCommand::Delete { scope, key } => {
            let vault = open_cli_vault().context("failed to initialize vault runtime")?;
            let scope = parse_vault_scope(scope.as_str())?;
            let deleted = vault
                .delete_secret(&scope, key.as_str())
                .with_context(|| format!("failed to delete secret key={} scope={scope}", key))?;
            println!("secrets.delete scope={} key={} deleted={}", scope, key, deleted);
            std::io::stdout().flush().context("stdout flush failed")
        }
        SecretsCommand::Audit { path, offline, strict, json } => {
            let payload = build_secrets_audit_payload(path, offline)?;
            emit_secrets_audit(
                payload.path.as_str(),
                payload.runtime_profiles_inspected,
                payload.runtime_error.is_some(),
                &payload.summary,
                output::preferred_json(json),
            )?;
            if strict && payload.summary.blocking_findings > 0 {
                anyhow::bail!(
                    "secrets audit failed with {} blocking findings",
                    payload.summary.blocking_findings
                );
            }
            Ok(())
        }
        SecretsCommand::Apply { path, offline, strict, runtime, dry_run, json } => {
            if runtime {
                return run_runtime_secret_apply(path, dry_run, json);
            }
            let audit = build_secrets_audit_payload(path, offline)?;
            if output::preferred_json(json) {
                let action_modes = build_secrets_apply_modes(&audit);
                output::print_json_pretty(
                    &serde_json::json!({
                        "audit": audit.summary,
                        "action_modes": action_modes,
                    }),
                    "failed to encode secrets apply output as JSON",
                )?;
            } else {
                output::print_text_line(
                    "secrets.apply summary=<redacted> use --json for structured output",
                )?;
            }
            std::io::stdout().flush().context("stdout flush failed")?;
            if strict && audit.summary.blocking_findings > 0 {
                anyhow::bail!(
                    "secrets apply blocked by {} unresolved secret findings",
                    audit.summary.blocking_findings
                );
            }
            Ok(())
        }
        SecretsCommand::Inventory { json } => run_configured_secret_inventory(json),
        SecretsCommand::Explain { secret_id, json } => run_secret_explain(secret_id.as_str(), json),
        SecretsCommand::Plan { path, json } => run_runtime_secret_plan(path, json),
        SecretsCommand::Configure { command } => run_secrets_configure(command),
    }
}

pub(crate) fn build_secrets_audit_payload(
    path: Option<String>,
    offline: bool,
) -> Result<SecretAuditPayload> {
    let vault = open_cli_vault().context("failed to initialize vault runtime")?;
    let (path, document) = load_config_document_for_audit(path)?;
    let inline_api_key = get_string_value_at_path(&document, "model_provider.openai_api_key")?;
    let configured_auth_profile =
        get_string_value_at_path(&document, "model_provider.auth_profile_id")?.or_else(|| {
            get_string_value_at_path(&document, "model_provider.auth_profile_ref").ok().flatten()
        });
    let runtime_profiles = load_runtime_auth_profiles(offline)?;
    let runtime_webhooks = load_runtime_webhooks(offline)?;
    let mut candidates = Vec::<SecretReferenceCandidate>::new();
    let mut findings = Vec::<SecretAuditFinding>::new();

    if let Some(reference) =
        get_string_value_at_path(&document, "model_provider.openai_api_key_vault_ref")?
    {
        candidates.push(SecretReferenceCandidate {
            component: "model_provider".to_owned(),
            reference_kind: "model_provider_api_key".to_owned(),
            reference,
        });
    }
    if let Some(reference) =
        get_string_value_at_path(&document, "model_provider.anthropic_api_key_vault_ref")?
    {
        candidates.push(SecretReferenceCandidate {
            component: "model_provider".to_owned(),
            reference_kind: "model_provider_api_key".to_owned(),
            reference,
        });
    }
    if let Some(reference) =
        get_string_value_at_path(&document, "tool_call.browser_service.state_key_vault_ref")?
    {
        candidates.push(SecretReferenceCandidate {
            component: "browser_service".to_owned(),
            reference_kind: "browser_state_key".to_owned(),
            reference,
        });
    }

    if inline_api_key.is_some() {
        findings.push(SecretAuditFinding {
            severity: "warning".to_owned(),
            code: "inline_secret_configured".to_owned(),
            component: "model_provider".to_owned(),
            reference: None,
            message: "model_provider.openai_api_key is set inline instead of using a vault reference or auth profile.".to_owned(),
            remediation: "Prefer `palyra auth openai api-key` or `palyra secrets configure openai-api-key` so the OpenAI credential stays vault-backed.".to_owned(),
        });
    }

    if let Some(profile_id) = configured_auth_profile.as_ref() {
        if runtime_profiles.runtime_profiles_inspected
            && !runtime_profiles
                .profiles
                .iter()
                .any(|profile| profile.profile_id.as_str() == profile_id.as_str())
        {
            findings.push(SecretAuditFinding {
                severity: "blocking".to_owned(),
                code: "missing_auth_profile".to_owned(),
                component: "model_provider".to_owned(),
                reference: None,
                message: format!(
                    "model_provider.auth_profile_id points to missing runtime profile `{profile_id}`."
                ),
                remediation: "Run `palyra auth openai status` to inspect profiles or select a valid default profile with `palyra auth openai use-profile <profile-id>`.".to_owned(),
            });
        }
    }

    if configured_auth_profile.is_some()
        && get_string_value_at_path(&document, "model_provider.openai_api_key_vault_ref")?.is_some()
    {
        findings.push(SecretAuditFinding {
            severity: "info".to_owned(),
            code: "legacy_vault_ref_shadowed".to_owned(),
            component: "model_provider".to_owned(),
            reference: get_string_value_at_path(&document, "model_provider.openai_api_key_vault_ref")?,
            message: "model_provider.auth_profile_id takes precedence over model_provider.openai_api_key_vault_ref.".to_owned(),
            remediation: "Remove the legacy vault reference if it is no longer needed, or unset model_provider.auth_profile_id when you want the direct vault ref to become effective.".to_owned(),
        });
    }

    for profile in &runtime_profiles.profiles {
        match &profile.credential {
            control_plane::AuthCredentialView::ApiKey { api_key_vault_ref } => {
                candidates.push(SecretReferenceCandidate {
                    component: format!("auth_profile:{}", profile.profile_id),
                    reference_kind: "auth_profile_api_key".to_owned(),
                    reference: api_key_vault_ref.clone(),
                });
            }
            control_plane::AuthCredentialView::Oauth {
                access_token_vault_ref,
                refresh_token_vault_ref,
                client_secret_vault_ref,
                ..
            } => {
                candidates.push(SecretReferenceCandidate {
                    component: format!("auth_profile:{}", profile.profile_id),
                    reference_kind: "auth_profile_oauth_access_token".to_owned(),
                    reference: access_token_vault_ref.clone(),
                });
                candidates.push(SecretReferenceCandidate {
                    component: format!("auth_profile:{}", profile.profile_id),
                    reference_kind: "auth_profile_oauth_refresh_token".to_owned(),
                    reference: refresh_token_vault_ref.clone(),
                });
                if let Some(reference) = client_secret_vault_ref
                    .as_ref()
                    .map(|value| value.trim())
                    .filter(|value| !value.is_empty())
                {
                    candidates.push(SecretReferenceCandidate {
                        component: format!("auth_profile:{}", profile.profile_id),
                        reference_kind: "auth_profile_oauth_client_secret".to_owned(),
                        reference: reference.to_owned(),
                    });
                }
            }
        }
    }

    for webhook in &runtime_webhooks.webhooks {
        let reference = webhook.secret_vault_ref.trim();
        if !reference.is_empty() {
            candidates.push(SecretReferenceCandidate {
                component: format!("webhook:{}", webhook.integration_id),
                reference_kind: "webhook_signing_secret".to_owned(),
                reference: reference.to_owned(),
            });
        }
    }

    let mut references = Vec::<SecretReferenceAudit>::new();
    let mut used_refs_by_scope = BTreeMap::<String, BTreeSet<String>>::new();

    for candidate in candidates {
        match VaultRef::parse(candidate.reference.as_str()) {
            Ok(parsed) => {
                let scope = parsed.scope.to_string();
                let key = parsed.key.clone();
                used_refs_by_scope.entry(scope.clone()).or_default().insert(key.clone());
                let status = match vault.get_secret(&parsed.scope, key.as_str()) {
                    Ok(value) if !value.is_empty() => {
                        references.push(SecretReferenceAudit {
                            component: candidate.component.clone(),
                            reference_kind: candidate.reference_kind.clone(),
                            reference: candidate.reference.clone(),
                            scope,
                            key,
                            status: "resolved".to_owned(),
                            detail: "secret value is readable".to_owned(),
                        });
                        continue;
                    }
                    Ok(_) => ("missing", "secret resolved to an empty value".to_owned()),
                    Err(error) => ("missing", sanitize_secret_error(error.to_string().as_str())),
                };
                references.push(SecretReferenceAudit {
                    component: candidate.component.clone(),
                    reference_kind: candidate.reference_kind.clone(),
                    reference: candidate.reference.clone(),
                    scope: scope.clone(),
                    key: key.clone(),
                    status: status.0.to_owned(),
                    detail: status.1.clone(),
                });
                findings.push(SecretAuditFinding {
                    severity: "blocking".to_owned(),
                    code: "unresolved_secret_ref".to_owned(),
                    component: candidate.component,
                    reference: Some(candidate.reference),
                    message: format!("vault reference {scope}/{key} is missing or unreadable."),
                    remediation: "Store the secret in the expected scope/key, or update the referencing config/auth profile to the correct vault ref.".to_owned(),
                });
            }
            Err(error) => {
                references.push(SecretReferenceAudit {
                    component: candidate.component.clone(),
                    reference_kind: candidate.reference_kind.clone(),
                    reference: candidate.reference.clone(),
                    scope: "invalid".to_owned(),
                    key: String::new(),
                    status: "invalid".to_owned(),
                    detail: sanitize_secret_error(error.to_string().as_str()),
                });
                findings.push(SecretAuditFinding {
                    severity: "blocking".to_owned(),
                    code: "invalid_secret_ref".to_owned(),
                    component: candidate.component,
                    reference: Some(candidate.reference),
                    message: "vault reference format is invalid.".to_owned(),
                    remediation:
                        "Use canonical `<scope>/<key>` vault refs such as `global/openai_api_key`."
                            .to_owned(),
                });
            }
        }
    }

    for (scope_raw, used_keys) in used_refs_by_scope {
        let scope = match parse_vault_scope(scope_raw.as_str()) {
            Ok(scope) => scope,
            Err(_) => continue,
        };
        let entries = match vault.list_secrets(&scope) {
            Ok(entries) => entries,
            Err(_) => continue,
        };
        for entry in entries {
            if used_keys.contains(entry.key.as_str()) {
                continue;
            }
            findings.push(SecretAuditFinding {
                severity: "info".to_owned(),
                code: "potentially_unused_secret".to_owned(),
                component: format!("vault_scope:{scope}"),
                reference: Some(format!("{scope}/{}", entry.key)),
                message: format!(
                    "secret `{}/{}` is stored in a referenced scope but is not used by the current config or runtime auth profiles.",
                    scope, entry.key
                ),
                remediation: "Delete the secret if it is obsolete, or keep it if another un-audited subsystem still depends on it.".to_owned(),
            });
        }
    }

    if let Some(error) = runtime_profiles.runtime_error.as_ref() {
        findings.push(SecretAuditFinding {
            severity: "warning".to_owned(),
            code: "runtime_profiles_unavailable".to_owned(),
            component: "auth_profiles".to_owned(),
            reference: None,
            message: format!("runtime auth profile inspection was skipped: {error}"),
            remediation: "Ensure the daemon admin surface is reachable, or rerun with `--offline` when you only want a local vault/config audit.".to_owned(),
        });
    }
    if let Some(error) = runtime_webhooks.runtime_error.as_ref() {
        findings.push(SecretAuditFinding {
            severity: "warning".to_owned(),
            code: "runtime_webhooks_unavailable".to_owned(),
            component: "webhooks".to_owned(),
            reference: None,
            message: format!("runtime webhook inspection was skipped: {error}"),
            remediation: "Ensure the daemon admin surface is reachable, or rerun with `--offline` when you only want a local vault/config audit.".to_owned(),
        });
    }

    let runtime_error =
        match (runtime_profiles.runtime_error.clone(), runtime_webhooks.runtime_error.clone()) {
            (Some(left), Some(right)) => Some(format!("{left}; {right}")),
            (Some(left), None) => Some(left),
            (None, Some(right)) => Some(right),
            (None, None) => None,
        };

    let summary = SecretAuditSummary {
        total_references: references.len(),
        resolved_references: references
            .iter()
            .filter(|reference| reference.status == "resolved")
            .count(),
        blocking_findings: findings.iter().filter(|finding| finding.severity == "blocking").count(),
        warning_findings: findings.iter().filter(|finding| finding.severity == "warning").count(),
        info_findings: findings.iter().filter(|finding| finding.severity == "info").count(),
    };

    Ok(SecretAuditPayload {
        path,
        runtime_profiles_inspected: runtime_profiles.runtime_profiles_inspected,
        runtime_error,
        references,
        findings,
        summary,
    })
}

fn run_secrets_configure(command: SecretsConfigureCommand) -> Result<()> {
    match command {
        SecretsConfigureCommand::OpenaiApiKey { scope, key, value_stdin, path, backups, json } => {
            let path = resolve_config_path(path, false)?;
            configure_secret_backed_setting(
                scope,
                key,
                value_stdin,
                &path,
                backups,
                |document, vault_ref| {
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
                    unset_value_at_path(document, "model_provider.openai_api_key")?;
                    unset_value_at_path(document, "model_provider.auth_profile_id")?;
                    unset_value_at_path(document, "model_provider.auth_profile_ref")?;
                    unset_value_at_path(document, "model_provider.auth_provider_kind")?;
                    set_value_at_path(
                        document,
                        "model_provider.openai_api_key_vault_ref",
                        toml::Value::String(vault_ref.to_owned()),
                    )?;
                    Ok(())
                },
            )?;
            emit_secret_configure_payload(
                "openai_api_key",
                path.as_str(),
                backups,
                output::preferred_json(json),
            )
        }
        SecretsConfigureCommand::BrowserStateKey {
            scope,
            key,
            value_stdin,
            path,
            backups,
            json,
        } => {
            let path = resolve_config_path(path, false)?;
            configure_secret_backed_setting(
                scope,
                key,
                value_stdin,
                &path,
                backups,
                |document, vault_ref| {
                    set_value_at_path(
                        document,
                        "tool_call.browser_service.state_key_vault_ref",
                        toml::Value::String(vault_ref.to_owned()),
                    )?;
                    Ok(())
                },
            )?;
            emit_secret_configure_payload(
                "browser_state_key",
                path.as_str(),
                backups,
                output::preferred_json(json),
            )
        }
    }
}

fn configure_secret_backed_setting<F>(
    scope_raw: String,
    key: String,
    value_stdin: bool,
    path: &str,
    backups: usize,
    mutate_document: F,
) -> Result<()>
where
    F: FnOnce(&mut toml::Value, &str) -> Result<()>,
{
    let vault = open_cli_vault().context("failed to initialize vault runtime")?;
    let value = read_secret_bytes_from_stdin(value_stdin)?;
    let scope = parse_vault_scope(scope_raw.as_str())?;
    vault
        .put_secret(&scope, key.as_str(), value.as_slice())
        .with_context(|| format!("failed to store secret key={} scope={scope}", key))?;

    let path_ref = Path::new(path);
    let (mut document, _) = load_document_for_mutation(path_ref)
        .with_context(|| format!("failed to parse {}", path_ref.display()))?;
    let vault_ref = format!("{scope}/{key}");
    mutate_document(&mut document, vault_ref.as_str())?;
    validate_daemon_compatible_document(&document).with_context(|| {
        format!("mutated config {} does not match daemon schema", path_ref.display())
    })?;
    write_document_with_backups(path_ref, &document, backups)
        .with_context(|| format!("failed to persist config {}", path_ref.display()))?;

    Ok(())
}

fn emit_secret_configure_payload(
    component: &str,
    path: &str,
    backups: usize,
    json_output: bool,
) -> Result<()> {
    if json_output {
        output::print_json_pretty(
            &serde_json::json!({
                "component": component,
                "path": path,
                "backups": backups,
                "vault_ref_configured": true,
            }),
            "failed to encode secrets configure payload as JSON",
        )?;
    } else {
        println!(
            "secrets.configure component={} path={} vault_ref_configured=true backups={}",
            component, path, backups
        );
    }
    std::io::stdout().flush().context("stdout flush failed")
}

fn emit_secrets_audit(
    path: &str,
    runtime_profiles_inspected: bool,
    runtime_error_present: bool,
    summary: &SecretAuditSummary,
    json_output: bool,
) -> Result<()> {
    if json_output {
        output::print_json_pretty(
            &serde_json::json!({
                "path": path,
                "runtime_profiles_inspected": runtime_profiles_inspected,
                "runtime_error_present": runtime_error_present,
                "summary": {
                    "total_references": summary.total_references,
                    "resolved_references": summary.resolved_references,
                    "blocking_findings": summary.blocking_findings,
                    "warning_findings": summary.warning_findings,
                    "info_findings": summary.info_findings,
                }
            }),
            "failed to encode secrets audit payload as JSON",
        )?;
    } else {
        output::print_text_line(
            "secrets.audit summary=<redacted> use --json for structured output",
        )?;
    }
    std::io::stdout().flush().context("stdout flush failed")
}

fn build_secrets_apply_modes(audit: &SecretAuditPayload) -> Vec<SecretsApplyMode> {
    let mut affected_components_by_mode = BTreeMap::<String, BTreeSet<String>>::new();
    for reference in &audit.references {
        let apply_mode = match reference.reference_kind.as_str() {
            "model_provider_api_key" => "daemon_restart_required",
            "browser_state_key" => "browserd_restart_required",
            kind if kind.starts_with("auth_profile_oauth_") => "live_refresh_supported",
            kind if kind.starts_with("auth_profile_") => "live_reference",
            _ => continue,
        };
        affected_components_by_mode
            .entry(apply_mode.to_owned())
            .or_default()
            .insert(reference.component.clone());
    }
    affected_components_by_mode
        .into_iter()
        .map(|(apply_mode, affected_components)| SecretsApplyMode {
            apply_mode,
            affected_components: affected_components.len(),
        })
        .collect()
}

fn read_secret_bytes_from_stdin(value_stdin: bool) -> Result<Vec<u8>> {
    if !value_stdin {
        anyhow::bail!(
            "command requires --value-stdin to avoid exposing raw values in process args"
        );
    }
    let mut value = Vec::new();
    std::io::stdin().read_to_end(&mut value).context("failed to read secret value from stdin")?;
    if value.is_empty() {
        anyhow::bail!("stdin did not contain any secret bytes");
    }
    Ok(value)
}

fn load_config_document_for_audit(path: Option<String>) -> Result<(String, toml::Value)> {
    let resolved = match path {
        Some(path) => resolve_config_path(Some(path), false)?,
        None => match effective_config_path() {
            Some(path) => path,
            None => {
                let (document, _) = parse_document_with_migration("")
                    .context("failed to initialize empty config snapshot for secrets audit")?;
                return Ok(("defaults".to_owned(), document));
            }
        },
    };
    let (document, _) = load_document_from_existing_path(Path::new(&resolved))
        .with_context(|| format!("failed to parse {resolved}"))?;
    Ok((resolved, document))
}

fn get_string_value_at_path(document: &toml::Value, key: &str) -> Result<Option<String>> {
    Ok(get_value_at_path(document, key)
        .with_context(|| format!("invalid config key path: {key}"))?
        .and_then(toml::Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned))
}

struct RuntimeAuthProfiles {
    runtime_profiles_inspected: bool,
    runtime_error: Option<String>,
    profiles: Vec<control_plane::AuthProfileView>,
}

struct RuntimeWebhookIntegrations {
    runtime_error: Option<String>,
    webhooks: Vec<control_plane::WebhookIntegrationView>,
}

fn load_runtime_auth_profiles(offline: bool) -> Result<RuntimeAuthProfiles> {
    if offline {
        return Ok(RuntimeAuthProfiles {
            runtime_profiles_inspected: false,
            runtime_error: None,
            profiles: Vec::new(),
        });
    }

    let runtime = build_runtime()?;
    let result = runtime.block_on(async {
        let context =
            match client::control_plane::connect_admin_console(app::ConnectionOverrides::default())
                .await
            {
                Ok(context) => context,
                Err(error) => {
                    return RuntimeAuthProfiles {
                        runtime_profiles_inspected: false,
                        runtime_error: Some(sanitize_secret_error(error.to_string().as_str())),
                        profiles: Vec::new(),
                    };
                }
            };
        match context.client.list_auth_profiles("limit=200").await {
            Ok(envelope) => RuntimeAuthProfiles {
                runtime_profiles_inspected: true,
                runtime_error: None,
                profiles: envelope.profiles,
            },
            Err(error) => RuntimeAuthProfiles {
                runtime_profiles_inspected: false,
                runtime_error: Some(sanitize_secret_error(error.to_string().as_str())),
                profiles: Vec::new(),
            },
        }
    });
    Ok(result)
}

fn load_runtime_webhooks(offline: bool) -> Result<RuntimeWebhookIntegrations> {
    if offline {
        return Ok(RuntimeWebhookIntegrations { runtime_error: None, webhooks: Vec::new() });
    }

    let runtime = build_runtime()?;
    let result = runtime.block_on(async {
        let context =
            match client::control_plane::connect_admin_console(app::ConnectionOverrides::default())
                .await
            {
                Ok(context) => context,
                Err(error) => {
                    return RuntimeWebhookIntegrations {
                        runtime_error: Some(sanitize_secret_error(error.to_string().as_str())),
                        webhooks: Vec::new(),
                    };
                }
            };
        match context.client.list_webhooks("").await {
            Ok(envelope) => {
                RuntimeWebhookIntegrations { runtime_error: None, webhooks: envelope.integrations }
            }
            Err(error) => RuntimeWebhookIntegrations {
                runtime_error: Some(sanitize_secret_error(error.to_string().as_str())),
                webhooks: Vec::new(),
            },
        }
    });
    Ok(result)
}

fn run_configured_secret_inventory(json: bool) -> Result<()> {
    let runtime = build_runtime()?;
    let envelope = runtime.block_on(async {
        let context =
            client::control_plane::connect_admin_console(app::ConnectionOverrides::default())
                .await?;
        context.client.list_configured_secrets().await.map_err(anyhow::Error::from)
    })?;
    if output::preferred_json(json) {
        output::print_json_pretty(&envelope, "failed to encode configured secret inventory")?;
        return Ok(());
    }
    output::print_text_line(
        "Sensitive configuration details are redacted in text output; use --json for structured output",
    )?;
    Ok(())
}

fn run_secret_explain(secret_id: &str, json: bool) -> Result<()> {
    if secret_id.contains('/') {
        let vault_ref = VaultRef::parse(secret_id)
            .with_context(|| format!("invalid vault secret reference `{secret_id}`"))?;
        return run_vault_secret_explain(&vault_ref, json);
    }
    run_configured_secret_explain(secret_id, json)
}

fn run_vault_secret_explain(vault_ref: &VaultRef, json: bool) -> Result<()> {
    let vault = open_cli_vault().context("failed to initialize vault runtime")?;
    let reference = format!("{}/{}", vault_ref.scope, vault_ref.key);
    let metadata = vault
        .list_secrets(&vault_ref.scope)
        .with_context(|| format!("failed to list secrets for scope={}", vault_ref.scope))?
        .into_iter()
        .find(|entry| entry.key == vault_ref.key)
        .with_context(|| {
            format!("secret reference {reference} was not found in the local vault")
        })?;
    let (status, value_bytes, last_error) = match vault.get_secret(&vault_ref.scope, &vault_ref.key)
    {
        Ok(value) if value.is_empty() => ("empty".to_owned(), 0, None),
        Ok(value) => ("stored".to_owned(), u32::try_from(value.len()).unwrap_or(u32::MAX), None),
        Err(error) => (
            "unreadable".to_owned(),
            u32::try_from(metadata.value_bytes).unwrap_or(u32::MAX),
            Some(sanitize_secret_error(error.to_string().as_str())),
        ),
    };
    let (configured_references, config_usage_error) = match build_secrets_audit_payload(None, true)
    {
        Ok(audit) => (
            audit
                .references
                .into_iter()
                .filter(|entry| entry.scope == vault_ref.scope.to_string())
                .filter(|entry| entry.key == vault_ref.key)
                .map(|entry| VaultSecretConfiguredReference {
                    component: entry.component,
                    reference_kind: entry.reference_kind,
                    status: entry.status,
                })
                .collect::<Vec<_>>(),
            None,
        ),
        Err(error) => (Vec::new(), Some(sanitize_secret_error(error.to_string().as_str()))),
    };
    let envelope = VaultSecretExplainEnvelope {
        kind: "vault_secret".to_owned(),
        reference: reference.clone(),
        scope: vault_ref.scope.to_string(),
        key: vault_ref.key.clone(),
        status,
        backend: vault.backend_kind().as_str().to_owned(),
        value_bytes,
        created_at_unix_ms: metadata.created_at_unix_ms,
        updated_at_unix_ms: metadata.updated_at_unix_ms,
        configured: !configured_references.is_empty(),
        configured_references,
        config_usage_error,
        last_error,
    };
    if output::preferred_json(json) {
        output::print_json_pretty(&envelope, "failed to encode vault secret detail")?;
        return Ok(());
    }
    output::print_text_line(
        format!(
            "secret.explain reference={} status={} configured={} backend={} value_bytes={}",
            envelope.reference,
            envelope.status,
            envelope.configured,
            envelope.backend,
            envelope.value_bytes
        )
        .as_str(),
    )?;
    if envelope.config_usage_error.is_some() {
        output::print_text_line(
            "secret.explain config_usage=unknown use --json for sanitized diagnostic detail",
        )?;
    }
    Ok(())
}

fn run_configured_secret_explain(secret_id: &str, json: bool) -> Result<()> {
    let runtime = build_runtime()?;
    let envelope = runtime.block_on(async {
        let context =
            client::control_plane::connect_admin_console(app::ConnectionOverrides::default())
                .await?;
        context.client.get_configured_secret(secret_id).await.map_err(anyhow::Error::from)
    })?;
    if output::preferred_json(json) {
        output::print_json_pretty(&envelope, "failed to encode configured secret detail")?;
        return Ok(());
    }
    output::print_text_line(
        "Sensitive configuration details are redacted in text output; use --json for structured output",
    )?;
    Ok(())
}

fn run_runtime_secret_plan(path: Option<String>, json: bool) -> Result<()> {
    let runtime = build_runtime()?;
    let envelope = runtime.block_on(async {
        let context =
            client::control_plane::connect_admin_console(app::ConnectionOverrides::default())
                .await?;
        context
            .client
            .plan_config_reload(&control_plane::ConfigReloadPlanRequest { path })
            .await
            .map_err(anyhow::Error::from)
    })?;
    if output::preferred_json(json) {
        output::print_json_pretty(&envelope, "failed to encode reload plan")?;
        return Ok(());
    }
    emit_reload_plan_summary(&envelope)
}

fn run_runtime_secret_apply(path: Option<String>, dry_run: bool, json: bool) -> Result<()> {
    let runtime = build_runtime()?;
    let envelope = runtime.block_on(async {
        let context =
            client::control_plane::connect_admin_console(app::ConnectionOverrides::default())
                .await?;
        context
            .client
            .apply_config_reload(&control_plane::ConfigReloadApplyRequest {
                path,
                plan_id: None,
                idempotency_key: None,
                dry_run,
                force: false,
            })
            .await
            .map_err(anyhow::Error::from)
    })?;
    if output::preferred_json(json) {
        output::print_json_pretty(&envelope, "failed to encode reload apply result")?;
        return Ok(());
    }
    let result_line =
        format!("secrets.reload outcome={} message={}", envelope.outcome, envelope.message);
    output::print_text_line(result_line.as_str())?;
    emit_reload_plan_summary(&envelope.plan)?;
    Ok(())
}

fn emit_reload_plan_summary(plan: &control_plane::ConfigReloadPlanEnvelope) -> Result<()> {
    let plan_line = format!(
        "reload.plan id={} active_runs={} hot_safe={} restart_required={} blocked={} manual_review={}",
        plan.plan_id,
        plan.active_runs,
        plan.summary.hot_safe,
        plan.summary.restart_required,
        plan.summary.blocked_while_runs_active,
        plan.summary.manual_review
    );
    output::print_text_line(plan_line.as_str())?;
    for step in &plan.steps {
        let step_line = format!(
            "reload.step component={} path={} category={} reason={}",
            step.component, step.config_path, step.category, step.reason
        );
        output::print_text_line(step_line.as_str())?;
    }
    Ok(())
}

fn sanitize_secret_error(raw: &str) -> String {
    redact_auth_error(redact_url_segments_in_text(raw).as_str())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_secret_audit_payload() -> SecretAuditPayload {
        SecretAuditPayload {
            path: "palyra.toml".to_owned(),
            runtime_profiles_inspected: false,
            runtime_error: Some("token refresh failed for vault://global/openai".to_owned()),
            references: vec![SecretReferenceAudit {
                component: "model_provider".to_owned(),
                reference_kind: "model_provider_api_key".to_owned(),
                reference: "vault://global/openai".to_owned(),
                scope: "global".to_owned(),
                key: "openai".to_owned(),
                status: "resolved".to_owned(),
                detail: "resolved from vault".to_owned(),
            }],
            findings: vec![SecretAuditFinding {
                severity: "warning".to_owned(),
                code: "secret.reference.present".to_owned(),
                component: "model_provider".to_owned(),
                reference: Some("vault://global/openai".to_owned()),
                message: "secret reference present".to_owned(),
                remediation: "rotate vault://global/openai".to_owned(),
            }],
            summary: SecretAuditSummary {
                total_references: 1,
                resolved_references: 1,
                blocking_findings: 0,
                warning_findings: 1,
                info_findings: 0,
            },
        }
    }

    #[test]
    fn secret_audit_output_redacts_sensitive_fields() {
        let payload = sample_secret_audit_payload();
        let output = serde_json::to_string(&serde_json::json!({
            "path": payload.path,
            "runtime_profiles_inspected": payload.runtime_profiles_inspected,
            "runtime_error_present": payload.runtime_error.is_some(),
            "summary": payload.summary,
        }))
        .expect("audit output should serialize");
        assert!(output.contains("\"runtime_error_present\":true"));
        assert!(!output.contains("vault://global/openai"));
        assert!(!output.contains("resolved from vault"));
        assert!(!output.contains("rotate vault"));
    }

    #[test]
    fn secrets_apply_output_redacts_action_text() {
        let payload = sample_secret_audit_payload();
        let output = serde_json::to_string(&serde_json::json!({
            "audit": payload.summary,
            "action_modes": build_secrets_apply_modes(&payload),
        }))
        .expect("apply output");
        assert!(output.contains("\"apply_mode\":\"daemon_restart_required\""));
        assert!(!output.contains("restart palyrad"));
        assert!(!output.contains("vault://global/openai"));
    }

    #[test]
    fn secret_configure_output_hides_vault_reference() {
        let output = serde_json::to_string(&serde_json::json!({
            "component": "model_provider",
            "path": "palyra.toml",
            "backups": 2,
            "vault_ref_configured": true,
        }))
        .expect("serialize");
        assert!(output.contains("\"vault_ref_configured\":true"));
        assert!(!output.contains("global/openai"));
    }
}
