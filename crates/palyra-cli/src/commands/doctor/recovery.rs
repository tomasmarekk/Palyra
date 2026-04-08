use crate::*;
use anyhow::{Context, Result};
use base64::Engine;
use ring::{
    aead::{Aad, LessSafeKey, Nonce, UnboundKey, CHACHA20_POLY1305},
    rand::{SecureRandom, SystemRandom},
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value as JsonValue};
use std::{
    collections::{BTreeMap, BTreeSet},
    env, fs,
    io::Write,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::{Duration, SystemTime},
};
use toml::Value as TomlValue;
use ulid::Ulid;

const DOCTOR_EXECUTION_SCHEMA_VERSION: u32 = 1;
const DOCTOR_RECOVERY_MANIFEST_SCHEMA_VERSION: u32 = 1;
const DOCTOR_RECOVERY_RUNS_RELATIVE_DIR: &str = "recovery/runs";
const DOCTOR_RECOVERY_MANIFEST_FILE_NAME: &str = "manifest.json";
const DOCTOR_CLI_PROFILES_RELATIVE_PATH: &str = "cli/profiles.toml";
const DOCTOR_AUTH_REGISTRY_FILE_NAME: &str = "auth_profiles.toml";
const DOCTOR_ROUTINES_DEFINITIONS_RELATIVE_PATH: &str = "routines/definitions.json";
const DOCTOR_ROUTINES_RUN_METADATA_RELATIVE_PATH: &str = "routines/run_metadata.json";
const DOCTOR_NODE_RUNTIME_FILE_NAME: &str = "node-runtime.v1.json";
const DOCTOR_ACCESS_REGISTRY_FILE_NAME: &str = "access_registry.json";
const DOCTOR_BROWSER_STATE_DIR_RELATIVE_PATH: &str = "browserd";
const DOCTOR_BROWSER_PROFILE_REGISTRY_FILE_NAME: &str = "profiles.enc";
const DOCTOR_BROWSER_STATE_KEY_ENV: &str = "PALYRA_BROWSERD_STATE_ENCRYPTION_KEY";
const DOCTOR_BROWSER_STATE_DIR_ENV: &str = "PALYRA_BROWSERD_STATE_DIR";
const DOCTOR_AUTH_REGISTRY_PATH_ENV: &str = "PALYRA_AUTH_PROFILES_PATH";
const DOCTOR_BROWSER_PROFILE_REGISTRY_VERSION: u32 = 1;
const DOCTOR_BROWSER_PROFILE_RECORD_VERSION: u32 = 2;
const DOCTOR_BROWSER_STATE_FILE_MAGIC: &[u8; 4] = b"PBS1";
const DOCTOR_BROWSER_STATE_NONCE_LEN: usize = 12;
const DOCTOR_BROWSER_STATE_KEY_LEN: usize = 32;
const DOCTOR_STALE_TMP_MAX_AGE: Duration = Duration::from_secs(5 * 60);
const DOCTOR_STALE_RUNTIME_MAX_AGE: Duration = Duration::from_secs(60 * 60);
const DOCTOR_ACCESS_REGISTRY_VERSION: u32 = 1;
const DOCTOR_ROUTINE_REGISTRY_VERSION: u32 = 1;
const DOCTOR_SUPPORT_BUNDLE_MANIFEST_LIMIT: usize = 5;

#[derive(Clone, Debug)]
struct DoctorCommandRequest {
    strict: bool,
    json: bool,
    repair: bool,
    dry_run: bool,
    force: bool,
    only: Vec<String>,
    skip: Vec<String>,
    rollback_run: Option<String>,
}

#[derive(Clone, Debug)]
struct DoctorEnvironment {
    state_root: PathBuf,
    config_path: Option<PathBuf>,
    generated_at_unix_ms: i64,
}

#[derive(Clone, Copy, Debug, Serialize)]
#[serde(rename_all = "snake_case")]
enum DoctorExecutionMode {
    Diagnostics,
    RepairPreview,
    RepairApply,
    RollbackPreview,
    RollbackApply,
}

#[derive(Debug, Serialize)]
pub(crate) struct DoctorExecutionReport {
    schema_version: u32,
    generated_at_unix_ms: i64,
    mode: DoctorExecutionMode,
    diagnostics: DoctorReport,
    recovery: DoctorRecoveryReport,
}

#[derive(Clone, Debug, Default, Serialize)]
struct DoctorRecoveryReport {
    requested: bool,
    dry_run: bool,
    force: bool,
    selected_only: Vec<String>,
    skipped: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    rollback_run: Option<String>,
    planned_steps: Vec<DoctorRepairStep>,
    applied_steps: Vec<DoctorAppliedStep>,
    #[serde(skip_serializing_if = "Option::is_none")]
    run_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    backup_manifest_path: Option<String>,
    available_runs: Vec<DoctorRecoveryRunSummary>,
    next_steps: Vec<String>,
}

#[derive(Clone, Debug, Serialize)]
struct DoctorRepairStep {
    id: String,
    kind: String,
    severity: DoctorSeverity,
    title: String,
    description: String,
    impact: String,
    local_only: bool,
    security_sensitive: bool,
    requires_force: bool,
    apply_supported: bool,
    changed_objects: Vec<String>,
    warnings: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct DoctorAppliedStep {
    id: String,
    outcome: String,
    message: String,
    changed_objects: Vec<String>,
    warnings: Vec<String>,
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct DoctorRecoveryRunSummary {
    run_id: String,
    created_at_unix_ms: i64,
    manifest_path: String,
    step_ids: Vec<String>,
    file_count: usize,
    completed: bool,
    rollback_command: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct DoctorRecoveryManifest {
    schema_version: u32,
    run_id: String,
    created_at_unix_ms: i64,
    state_root: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    config_path: Option<String>,
    completed: bool,
    steps: Vec<String>,
    #[serde(default)]
    applied_steps: Vec<DoctorAppliedStep>,
    #[serde(default)]
    next_steps: Vec<String>,
    entries: Vec<DoctorRecoveryManifestEntry>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct DoctorRecoveryManifestEntry {
    step_id: String,
    target_path: String,
    change_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    backup_path: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    before_sha256: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    after_sha256: Option<String>,
    secret_aware: bool,
}

#[derive(Clone, Debug)]
struct DoctorRepairPlan {
    step: DoctorRepairStep,
    kind: DoctorRepairKind,
}

#[derive(Clone, Debug)]
enum DoctorRepairKind {
    InitializeMissingConfig {
        path: PathBuf,
    },
    MigrateConfigVersion {
        path: PathBuf,
    },
    RestoreConfigBackup {
        path: PathBuf,
        backup_path: PathBuf,
    },
    ReinitializeConfig {
        path: PathBuf,
    },
    GenerateBrowserAuthToken {
        path: PathBuf,
    },
    NormalizeAuthRegistry {
        path: PathBuf,
        quarantine_unknown_root_keys: Vec<(String, TomlValue)>,
    },
    RestoreAuthRegistryBackup {
        path: PathBuf,
        backup_path: PathBuf,
    },
    ReinitializeAuthRegistry {
        path: PathBuf,
    },
    GatewayRemoteVerificationManual {
        path: PathBuf,
    },
    NormalizeCliProfiles {
        path: PathBuf,
        quarantine_unknown_root_keys: Vec<(String, TomlValue)>,
    },
    RestoreCliProfilesBackup {
        path: PathBuf,
        backup_path: PathBuf,
    },
    ReinitializeCliProfiles {
        path: PathBuf,
    },
    NormalizeRoutineRegistry {
        path: PathBuf,
        top_level_array_key: &'static str,
    },
    NormalizeNodeRuntime {
        path: PathBuf,
    },
    RestoreNodeRuntimeBackup {
        path: PathBuf,
        backup_path: PathBuf,
    },
    ReinitializeNodeRuntime {
        path: PathBuf,
    },
    BackfillAccessRegistry {
        path: PathBuf,
    },
    RestoreAccessRegistryBackup {
        path: PathBuf,
        backup_path: PathBuf,
    },
    ReinitializeAccessRegistry {
        path: PathBuf,
    },
    NormalizeBrowserProfileRegistry {
        registry_path: PathBuf,
        master_key: [u8; DOCTOR_BROWSER_STATE_KEY_LEN],
    },
    ReinitializeBrowserProfileRegistry {
        registry_path: PathBuf,
        master_key: [u8; DOCTOR_BROWSER_STATE_KEY_LEN],
    },
    CleanupArtifacts {
        paths: Vec<PathBuf>,
    },
}

pub(crate) fn run_doctor(
    strict: bool,
    json: bool,
    repair: bool,
    dry_run: bool,
    force: bool,
    only: Vec<String>,
    skip: Vec<String>,
    rollback_run: Option<String>,
) -> Result<()> {
    let request =
        DoctorCommandRequest { strict, json, repair, dry_run, force, only, skip, rollback_run };
    let execution = build_doctor_execution(&request)?;
    if request.json {
        let encoded = serde_json::to_string_pretty(&execution)
            .context("failed to serialize doctor execution report")?;
        println!("{encoded}");
    } else {
        render_doctor_text(&execution);
    }
    if request.strict {
        let failing_required = execution
            .diagnostics
            .checks
            .iter()
            .find(|check| check.severity == DoctorSeverity::Blocking && !check.ok);
        if let Some(check) = failing_required {
            anyhow::bail!("strict doctor failed: {}", check.key);
        }
    }
    std::io::stdout().flush().context("stdout flush failed")
}

pub(crate) fn build_doctor_execution_preview_value() -> Result<JsonValue> {
    let execution = build_doctor_execution(&DoctorCommandRequest {
        strict: false,
        json: true,
        repair: true,
        dry_run: true,
        force: false,
        only: Vec::new(),
        skip: Vec::new(),
        rollback_run: None,
    })?;
    serde_json::to_value(execution).context("failed to encode doctor execution preview")
}

pub(crate) fn build_doctor_support_bundle_value() -> Result<JsonValue> {
    let preview = build_doctor_execution_preview_value()?;
    let environment = resolve_doctor_environment()?;
    let available_runs = collect_recovery_runs(environment.state_root.as_path());
    let recent_manifests = collect_recovery_manifest_values(
        environment.state_root.as_path(),
        DOCTOR_SUPPORT_BUNDLE_MANIFEST_LIMIT,
    );
    Ok(json!({
        "preview": preview,
        "available_runs": available_runs,
        "recent_manifests": recent_manifests,
    }))
}

fn build_doctor_execution(request: &DoctorCommandRequest) -> Result<DoctorExecutionReport> {
    if request.rollback_run.is_some() && request.repair {
        anyhow::bail!(
            "doctor repair apply and rollback cannot be requested in the same invocation"
        );
    }
    if request.rollback_run.is_some() && !request.only.is_empty() {
        anyhow::bail!("doctor rollback does not support --only filters");
    }
    let environment = resolve_doctor_environment()?;
    let checks = build_doctor_checks();
    let diagnostics = build_doctor_report(checks.as_slice())?;
    let mut recovery = DoctorRecoveryReport {
        requested: request.repair || request.rollback_run.is_some(),
        dry_run: request.dry_run,
        force: request.force,
        selected_only: request.only.clone(),
        skipped: request.skip.clone(),
        rollback_run: request.rollback_run.clone(),
        available_runs: collect_recovery_runs(environment.state_root.as_path()),
        ..Default::default()
    };

    let mode = if let Some(run_id) = request.rollback_run.as_deref() {
        let rollback = execute_rollback(
            environment.state_root.as_path(),
            run_id,
            request.force,
            request.dry_run,
        )?;
        recovery.applied_steps = rollback.applied_steps;
        recovery.backup_manifest_path = rollback.manifest_path;
        recovery.next_steps = rollback.next_steps;
        if request.dry_run {
            DoctorExecutionMode::RollbackPreview
        } else {
            DoctorExecutionMode::RollbackApply
        }
    } else if request.repair {
        let plans = evaluate_repair_plans(&environment)?;
        let plans = plans
            .into_iter()
            .filter(|entry| {
                step_selected(
                    entry.step.id.as_str(),
                    request.only.as_slice(),
                    request.skip.as_slice(),
                )
            })
            .collect::<Vec<_>>();
        recovery.planned_steps = plans.iter().map(|entry| entry.step.clone()).collect();
        if request.dry_run {
            recovery.next_steps =
                build_repair_next_steps_preview(recovery.planned_steps.as_slice());
            DoctorExecutionMode::RepairPreview
        } else {
            let apply_result = apply_repair_plans(&environment, plans.as_slice(), request.force)?;
            recovery.applied_steps = apply_result.applied_steps;
            recovery.run_id = apply_result.run_id;
            recovery.backup_manifest_path = apply_result.manifest_path;
            recovery.next_steps = apply_result.next_steps;
            DoctorExecutionMode::RepairApply
        }
    } else {
        recovery.next_steps = build_default_next_steps(&diagnostics);
        DoctorExecutionMode::Diagnostics
    };

    Ok(DoctorExecutionReport {
        schema_version: DOCTOR_EXECUTION_SCHEMA_VERSION,
        generated_at_unix_ms: environment.generated_at_unix_ms,
        mode,
        diagnostics,
        recovery,
    })
}

fn resolve_doctor_environment() -> Result<DoctorEnvironment> {
    let generated_at_unix_ms = now_unix_ms_i64()?;
    let current_context = app::current_root_context();
    let state_root = current_context
        .as_ref()
        .map(|context| context.state_root().to_path_buf())
        .map(Ok)
        .unwrap_or_else(|| app::resolve_cli_state_root(None))?;
    let config_path = current_context
        .as_ref()
        .and_then(|context| context.config_path().map(|path| path.to_path_buf()))
        .or_else(doctor_config_path);
    Ok(DoctorEnvironment { state_root, config_path, generated_at_unix_ms })
}

fn evaluate_repair_plans(environment: &DoctorEnvironment) -> Result<Vec<DoctorRepairPlan>> {
    let mut plans = Vec::new();
    plans.extend(evaluate_config_repairs(environment)?);
    plans.extend(evaluate_auth_registry_repairs(environment)?);
    plans.extend(evaluate_cli_profiles_repairs(environment)?);
    plans.extend(evaluate_routine_registry_repairs(environment)?);
    plans.extend(evaluate_node_runtime_repairs(environment)?);
    plans.extend(evaluate_access_registry_repairs(environment)?);
    plans.extend(evaluate_browser_profile_registry_repairs(environment)?);
    if let Some(step) = evaluate_stale_artifact_cleanup(environment)? {
        plans.push(step);
    }
    Ok(plans)
}

fn evaluate_config_repairs(environment: &DoctorEnvironment) -> Result<Vec<DoctorRepairPlan>> {
    let Some(path) = environment.config_path.clone() else {
        return Ok(Vec::new());
    };
    if !path.exists() {
        return Ok(vec![DoctorRepairPlan {
            step: DoctorRepairStep {
                id: "config.initialize".to_owned(),
                kind: "config_initialize".to_owned(),
                severity: DoctorSeverity::Warning,
                title: "Initialize missing config".to_owned(),
                description:
                    "Create a minimal versioned config document so the install can parse again."
                        .to_owned(),
                impact: "Writes a new versioned config file at the configured path.".to_owned(),
                local_only: true,
                security_sensitive: false,
                requires_force: false,
                apply_supported: true,
                changed_objects: vec![display_path(path.as_path())],
                warnings: Vec::new(),
            },
            kind: DoctorRepairKind::InitializeMissingConfig { path },
        }]);
    }

    let raw = fs::read_to_string(path.as_path())
        .with_context(|| format!("failed to read config {}", path.display()))?;
    let mut plans = Vec::new();
    match parse_document_with_migration(raw.as_str()) {
        Ok((_document, migration)) => {
            if migration.migrated {
                plans.push(DoctorRepairPlan {
                    step: DoctorRepairStep {
                        id: "config.schema_version".to_owned(),
                        kind: "config_version_migration".to_owned(),
                        severity: DoctorSeverity::Warning,
                        title: "Migrate config schema version".to_owned(),
                        description: format!(
                            "Update config schema metadata from version {} to {}.",
                            migration.source_version, migration.target_version
                        ),
                        impact: "Rewrites the config document without changing operator values."
                            .to_owned(),
                        local_only: true,
                        security_sensitive: false,
                        requires_force: false,
                        apply_supported: true,
                        changed_objects: vec![display_path(path.as_path())],
                        warnings: Vec::new(),
                    },
                    kind: DoctorRepairKind::MigrateConfigVersion { path: path.clone() },
                });
            }
        }
        Err(error) => {
            let latest_backup = backup_path(path.as_path(), 1);
            plans.push(if latest_backup.exists() {
                DoctorRepairPlan {
                    step: DoctorRepairStep {
                        id: "config.restore_latest_backup".to_owned(),
                        kind: "config_restore_backup".to_owned(),
                        severity: DoctorSeverity::Blocking,
                        title: "Restore latest config backup".to_owned(),
                        description: sanitize_diagnostic_error(error.to_string().as_str()),
                        impact: "Restores the unreadable config from the latest rotated backup.".to_owned(),
                        local_only: true,
                        security_sensitive: false,
                        requires_force: false,
                        apply_supported: true,
                        changed_objects: vec![
                            display_path(path.as_path()),
                            display_path(latest_backup.as_path()),
                        ],
                        warnings: Vec::new(),
                    },
                    kind: DoctorRepairKind::RestoreConfigBackup { path: path.clone(), backup_path: latest_backup },
                }
            } else {
                DoctorRepairPlan {
                    step: DoctorRepairStep {
                        id: "config.reinitialize".to_owned(),
                        kind: "config_reinitialize".to_owned(),
                        severity: DoctorSeverity::Blocking,
                        title: "Reinitialize unreadable config".to_owned(),
                        description: sanitize_diagnostic_error(error.to_string().as_str()),
                        impact: "Quarantines the unreadable config bytes and writes a minimal versioned config.".to_owned(),
                        local_only: true,
                        security_sensitive: false,
                        requires_force: true,
                        apply_supported: true,
                        changed_objects: vec![display_path(path.as_path())],
                        warnings: vec!["This path requires --force because no rotated backup exists.".to_owned()],
                    },
                    kind: DoctorRepairKind::ReinitializeConfig { path: path.clone() },
                }
            });
        }
    }

    if let Some(step) = evaluate_browser_auth_token_repair(path.as_path())? {
        plans.push(step);
    }
    if let Some(step) = evaluate_gateway_remote_verification_repair(path.as_path())? {
        plans.push(step);
    }
    Ok(plans)
}

fn evaluate_auth_registry_repairs(
    environment: &DoctorEnvironment,
) -> Result<Vec<DoctorRepairPlan>> {
    let path = resolve_auth_registry_path(environment);
    if !path.exists() {
        return Ok(Vec::new());
    }
    let raw = fs::read_to_string(path.as_path())
        .with_context(|| format!("failed to read auth registry {}", path.display()))?;
    let value = match toml::from_str::<TomlValue>(raw.as_str()) {
        Ok(value) => value,
        Err(error) => {
            let latest_backup = backup_path(path.as_path(), 1);
            return Ok(vec![if latest_backup.exists() {
                DoctorRepairPlan {
                    step: DoctorRepairStep {
                        id: "auth_registry.restore_latest_backup".to_owned(),
                        kind: "auth_registry_restore_backup".to_owned(),
                        severity: DoctorSeverity::Blocking,
                        title: "Restore latest auth registry backup".to_owned(),
                        description: sanitize_diagnostic_error(error.to_string().as_str()),
                        impact: "Restores the last readable auth registry document so provider auth health can evaluate again.".to_owned(),
                        local_only: true,
                        security_sensitive: false,
                        requires_force: false,
                        apply_supported: true,
                        changed_objects: vec![
                            display_path(path.as_path()),
                            display_path(latest_backup.as_path()),
                        ],
                        warnings: Vec::new(),
                    },
                    kind: DoctorRepairKind::RestoreAuthRegistryBackup { path, backup_path: latest_backup },
                }
            } else {
                DoctorRepairPlan {
                    step: DoctorRepairStep {
                        id: "auth_registry.reinitialize".to_owned(),
                        kind: "auth_registry_reinitialize".to_owned(),
                        severity: DoctorSeverity::Blocking,
                        title: "Reinitialize unreadable auth registry".to_owned(),
                        description: sanitize_diagnostic_error(error.to_string().as_str()),
                        impact: "Quarantines the unreadable auth registry and writes an empty versioned registry document.".to_owned(),
                        local_only: true,
                        security_sensitive: false,
                        requires_force: true,
                        apply_supported: true,
                        changed_objects: vec![display_path(path.as_path())],
                        warnings: vec!["This path requires --force because no rotated backup exists.".to_owned()],
                    },
                    kind: DoctorRepairKind::ReinitializeAuthRegistry { path },
                }
            }]);
        }
    };
    let mut root = match value {
        TomlValue::Table(root) => root,
        _ => {
            return Ok(vec![DoctorRepairPlan {
                step: DoctorRepairStep {
                    id: "auth_registry.reinitialize".to_owned(),
                    kind: "auth_registry_reinitialize".to_owned(),
                    severity: DoctorSeverity::Blocking,
                    title: "Reinitialize malformed auth registry".to_owned(),
                    description: "The auth profile registry must be a TOML table.".to_owned(),
                    impact: "Quarantines the malformed auth registry and writes an empty versioned registry document.".to_owned(),
                    local_only: true,
                    security_sensitive: false,
                    requires_force: true,
                    apply_supported: true,
                    changed_objects: vec![display_path(path.as_path())],
                    warnings: vec!["This path requires --force.".to_owned()],
                },
                kind: DoctorRepairKind::ReinitializeAuthRegistry { path },
            }]);
        }
    };

    let allowed_keys = BTreeSet::from(["version".to_owned(), "profiles".to_owned()]);
    let unknown_root_keys = root
        .keys()
        .filter(|key| !allowed_keys.contains((*key).to_owned().as_str()))
        .cloned()
        .collect::<Vec<_>>();
    let quarantine_unknown_root_keys = unknown_root_keys
        .into_iter()
        .filter_map(|key| root.remove(key.as_str()).map(|value| (key, value)))
        .collect::<Vec<_>>();
    let version_missing =
        !matches!(root.get("version").and_then(TomlValue::as_integer), Some(value) if value == 1);
    if !version_missing && quarantine_unknown_root_keys.is_empty() {
        return Ok(Vec::new());
    }
    Ok(vec![DoctorRepairPlan {
        step: DoctorRepairStep {
            id: "auth_registry.schema_version".to_owned(),
            kind: "auth_registry_normalize".to_owned(),
            severity: DoctorSeverity::Warning,
            title: "Normalize auth registry schema".to_owned(),
            description: "Add explicit schema metadata and quarantine unknown root keys so provider auth registry parsing stays deterministic.".to_owned(),
            impact: "Rewrites the auth registry and optionally writes a sidecar quarantine file for unknown keys.".to_owned(),
            local_only: true,
            security_sensitive: false,
            requires_force: false,
            apply_supported: true,
            changed_objects: {
                let mut changed = vec![display_path(path.as_path())];
                if !quarantine_unknown_root_keys.is_empty() {
                    changed.push(display_path(auth_registry_quarantine_path(path.as_path()).as_path()));
                }
                changed
            },
            warnings: if quarantine_unknown_root_keys.is_empty() {
                Vec::new()
            } else {
                vec!["Unknown auth registry root keys are moved into a sidecar quarantine document for manual review.".to_owned()]
            },
        },
        kind: DoctorRepairKind::NormalizeAuthRegistry { path, quarantine_unknown_root_keys },
    }])
}

fn evaluate_browser_auth_token_repair(path: &Path) -> Result<Option<DoctorRepairPlan>> {
    if !path.exists() {
        return Ok(None);
    }
    let (document, _) = load_document_from_existing_path(path)
        .with_context(|| format!("failed to parse {}", path.display()))?;
    let enabled = get_value_at_path(&document, "tool_call.browser_service.enabled")?
        .and_then(TomlValue::as_bool)
        .unwrap_or(false);
    let auth_token_present = get_value_at_path(&document, "tool_call.browser_service.auth_token")?
        .and_then(TomlValue::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .is_some()
        || env::var("PALYRA_BROWSER_SERVICE_AUTH_TOKEN")
            .ok()
            .map(|value| value.trim().to_owned())
            .filter(|value| !value.is_empty())
            .is_some();
    if !enabled || auth_token_present {
        return Ok(None);
    }
    Ok(Some(DoctorRepairPlan {
        step: DoctorRepairStep {
            id: "browser.auth_token.configure".to_owned(),
            kind: "browser_auth_token_config".to_owned(),
            severity: DoctorSeverity::Warning,
            title: "Generate browser relay auth token".to_owned(),
            description: "The browser broker is enabled but no auth token is configured in config or environment.".to_owned(),
            impact: "Writes a new local token into the config so browser relay and daemon can authenticate again.".to_owned(),
            local_only: true,
            security_sensitive: true,
            requires_force: false,
            apply_supported: true,
            changed_objects: vec![display_path(path)],
            warnings: vec![
                "The generated token is stored locally in the config file; support bundle exports keep it redacted."
                    .to_owned(),
            ],
        },
            kind: DoctorRepairKind::GenerateBrowserAuthToken { path: path.to_path_buf() },
    }))
}

fn evaluate_gateway_remote_verification_repair(path: &Path) -> Result<Option<DoctorRepairPlan>> {
    if !path.exists() {
        return Ok(None);
    }
    let (document, _) = load_document_from_existing_path(path)
        .with_context(|| format!("failed to parse {}", path.display()))?;
    let remote_base_url = get_value_at_path(&document, "gateway_access.remote_base_url")?
        .and_then(TomlValue::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty());
    let Some(_remote_base_url) = remote_base_url else {
        return Ok(None);
    };
    let pinned_server =
        get_value_at_path(&document, "gateway_access.pinned_server_cert_fingerprint_sha256")?
            .and_then(TomlValue::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty());
    let pinned_gateway_ca =
        get_value_at_path(&document, "gateway_access.pinned_gateway_ca_fingerprint_sha256")?
            .and_then(TomlValue::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty());

    let (title, description, warnings) = if pinned_server.is_some() && pinned_gateway_ca.is_some() {
        (
            "Resolve ambiguous gateway pinning".to_owned(),
            "The remote dashboard base URL is configured with both server-cert and gateway-CA pins. Operator review must choose the intended trust anchor before onboarding continues.".to_owned(),
            vec!["This step is intentionally manual; automatic repair would weaken the trust model.".to_owned()],
        )
    } else if pinned_server.is_none() && pinned_gateway_ca.is_none() {
        (
            "Review remote gateway verification".to_owned(),
            "The remote dashboard base URL is configured without an explicit server-cert or gateway-CA pin. Re-run onboarding or update the config so remote verification stays explicit.".to_owned(),
            vec!["This step is intentionally manual because trust material must be confirmed out-of-band.".to_owned()],
        )
    } else {
        return Ok(None);
    };

    Ok(Some(DoctorRepairPlan {
        step: DoctorRepairStep {
            id: "gateway_access.remote_verification".to_owned(),
            kind: "gateway_remote_verification_manual".to_owned(),
            severity: DoctorSeverity::Warning,
            title,
            description,
            impact: "Prevents support workflows from masking a remote onboarding trust mismatch."
                .to_owned(),
            local_only: true,
            security_sensitive: true,
            requires_force: false,
            apply_supported: false,
            changed_objects: vec![display_path(path)],
            warnings,
        },
        kind: DoctorRepairKind::GatewayRemoteVerificationManual { path: path.to_path_buf() },
    }))
}

fn evaluate_cli_profiles_repairs(environment: &DoctorEnvironment) -> Result<Vec<DoctorRepairPlan>> {
    let path = environment.state_root.join(DOCTOR_CLI_PROFILES_RELATIVE_PATH);
    if !path.exists() {
        return Ok(Vec::new());
    }
    let raw = fs::read_to_string(path.as_path())
        .with_context(|| format!("failed to read CLI profiles {}", path.display()))?;
    let value = match toml::from_str::<TomlValue>(raw.as_str()) {
        Ok(value) => value,
        Err(error) => {
            let latest_backup = backup_path(path.as_path(), 1);
            return Ok(vec![if latest_backup.exists() {
                DoctorRepairPlan {
                    step: DoctorRepairStep {
                        id: "cli_profiles.restore_latest_backup".to_owned(),
                        kind: "cli_profiles_restore_backup".to_owned(),
                        severity: DoctorSeverity::Blocking,
                        title: "Restore latest CLI profiles backup".to_owned(),
                        description: sanitize_diagnostic_error(error.to_string().as_str()),
                        impact: "Restores the last good CLI profiles document.".to_owned(),
                        local_only: true,
                        security_sensitive: false,
                        requires_force: false,
                        apply_supported: true,
                        changed_objects: vec![
                            display_path(path.as_path()),
                            display_path(latest_backup.as_path()),
                        ],
                        warnings: Vec::new(),
                    },
                    kind: DoctorRepairKind::RestoreCliProfilesBackup {
                        path,
                        backup_path: latest_backup,
                    },
                }
            } else {
                DoctorRepairPlan {
                    step: DoctorRepairStep {
                        id: "cli_profiles.reinitialize".to_owned(),
                        kind: "cli_profiles_reinitialize".to_owned(),
                        severity: DoctorSeverity::Blocking,
                        title: "Reinitialize unreadable CLI profiles".to_owned(),
                        description: sanitize_diagnostic_error(error.to_string().as_str()),
                        impact: "Quarantines the unreadable CLI profiles file and writes an empty versioned document.".to_owned(),
                        local_only: true,
                        security_sensitive: false,
                        requires_force: true,
                        apply_supported: true,
                        changed_objects: vec![display_path(path.as_path())],
                        warnings: vec!["This path requires --force because no rotated backup exists.".to_owned()],
                    },
                    kind: DoctorRepairKind::ReinitializeCliProfiles { path },
                }
            }]);
        }
    };
    let mut root = match value {
        TomlValue::Table(root) => root,
        _ => {
            return Ok(vec![DoctorRepairPlan {
                step: DoctorRepairStep {
                    id: "cli_profiles.reinitialize".to_owned(),
                    kind: "cli_profiles_reinitialize".to_owned(),
                    severity: DoctorSeverity::Blocking,
                    title: "Reinitialize malformed CLI profiles".to_owned(),
                    description: "The CLI profiles file must be a TOML table.".to_owned(),
                    impact: "Quarantines the malformed CLI profiles file and writes an empty versioned document.".to_owned(),
                    local_only: true,
                    security_sensitive: false,
                    requires_force: true,
                    apply_supported: true,
                    changed_objects: vec![display_path(path.as_path())],
                    warnings: vec!["This path requires --force.".to_owned()],
                },
                kind: DoctorRepairKind::ReinitializeCliProfiles { path },
            }]);
        }
    };
    let allowed_keys =
        BTreeSet::from(["version".to_owned(), "default_profile".to_owned(), "profiles".to_owned()]);
    let unknown_root_keys = root
        .keys()
        .filter(|key| !allowed_keys.contains((*key).to_owned().as_str()))
        .cloned()
        .collect::<Vec<_>>();
    let quarantine_unknown_root_keys = unknown_root_keys
        .into_iter()
        .filter_map(|key| root.remove(key.as_str()).map(|value| (key, value)))
        .collect::<Vec<_>>();
    let version_missing =
        !matches!(root.get("version").and_then(TomlValue::as_integer), Some(value) if value == 1);
    if !version_missing && quarantine_unknown_root_keys.is_empty() {
        return Ok(Vec::new());
    }
    Ok(vec![DoctorRepairPlan {
        step: DoctorRepairStep {
            id: "cli_profiles.schema_version".to_owned(),
            kind: "cli_profiles_normalize".to_owned(),
            severity: DoctorSeverity::Warning,
            title: "Normalize CLI profiles schema".to_owned(),
            description: "Add explicit schema metadata and quarantine unknown root keys so the CLI can parse profiles deterministically again.".to_owned(),
            impact: "Rewrites the CLI profiles document and optionally writes a sidecar quarantine file for unknown keys.".to_owned(),
            local_only: true,
            security_sensitive: false,
            requires_force: false,
            apply_supported: true,
            changed_objects: {
                let mut changed = vec![display_path(path.as_path())];
                if !quarantine_unknown_root_keys.is_empty() {
                    changed.push(display_path(cli_profiles_quarantine_path(path.as_path()).as_path()));
                }
                changed
            },
            warnings: if quarantine_unknown_root_keys.is_empty() {
                Vec::new()
            } else {
                vec!["Unknown root keys are moved into a sidecar quarantine document for manual review.".to_owned()]
            },
        },
        kind: DoctorRepairKind::NormalizeCliProfiles { path, quarantine_unknown_root_keys },
    }])
}

fn evaluate_routine_registry_repairs(
    environment: &DoctorEnvironment,
) -> Result<Vec<DoctorRepairPlan>> {
    let mut plans = Vec::new();
    for (path, id, description, top_level_array_key) in [
        (
            environment.state_root.join(DOCTOR_ROUTINES_DEFINITIONS_RELATIVE_PATH),
            "routines.definition_schema",
            "Routine definitions are missing schema metadata.",
            "routines",
        ),
        (
            environment.state_root.join(DOCTOR_ROUTINES_RUN_METADATA_RELATIVE_PATH),
            "routines.run_metadata_schema",
            "Routine run metadata is missing schema metadata.",
            "runs",
        ),
    ] {
        if !path.exists() {
            continue;
        }
        let raw = fs::read_to_string(path.as_path())
            .with_context(|| format!("failed to read routine registry {}", path.display()))?;
        let needs_rewrite = match serde_json::from_str::<JsonValue>(raw.as_str()) {
            Ok(JsonValue::Object(ref object)) => !matches!(
                object.get("schema_version").and_then(JsonValue::as_u64),
                Some(value) if value == DOCTOR_ROUTINE_REGISTRY_VERSION as u64
            ),
            _ => true,
        };
        if needs_rewrite {
            plans.push(DoctorRepairPlan {
                step: DoctorRepairStep {
                    id: id.to_owned(),
                    kind: "routine_registry_normalize".to_owned(),
                    severity: DoctorSeverity::Warning,
                    title: "Normalize routine registry schema".to_owned(),
                    description: description.to_owned(),
                    impact: "Rewrites the JSON document with the current schema version and preserves existing arrays.".to_owned(),
                    local_only: true,
                    security_sensitive: false,
                    requires_force: false,
                    apply_supported: true,
                    changed_objects: vec![display_path(path.as_path())],
                    warnings: Vec::new(),
                },
                kind: DoctorRepairKind::NormalizeRoutineRegistry { path, top_level_array_key },
            });
        }
    }
    Ok(plans)
}

fn evaluate_node_runtime_repairs(environment: &DoctorEnvironment) -> Result<Vec<DoctorRepairPlan>> {
    let path = environment.state_root.join(DOCTOR_NODE_RUNTIME_FILE_NAME);
    if !path.exists() {
        return Ok(Vec::new());
    }
    let raw = fs::read_to_string(path.as_path())
        .with_context(|| format!("failed to read node runtime state {}", path.display()))?;
    let value = match serde_json::from_str::<JsonValue>(raw.as_str()) {
        Ok(value) => value,
        Err(error) => {
            let latest_backup = backup_path(path.as_path(), 1);
            return Ok(vec![if latest_backup.exists() {
                DoctorRepairPlan {
                    step: DoctorRepairStep {
                        id: "node_runtime.restore_latest_backup".to_owned(),
                        kind: "node_runtime_restore_backup".to_owned(),
                        severity: DoctorSeverity::Blocking,
                        title: "Restore latest node runtime backup".to_owned(),
                        description: sanitize_diagnostic_error(error.to_string().as_str()),
                        impact: "Restores the node runtime state from the latest rotated backup."
                            .to_owned(),
                        local_only: true,
                        security_sensitive: false,
                        requires_force: false,
                        apply_supported: true,
                        changed_objects: vec![
                            display_path(path.as_path()),
                            display_path(latest_backup.as_path()),
                        ],
                        warnings: Vec::new(),
                    },
                    kind: DoctorRepairKind::RestoreNodeRuntimeBackup {
                        path,
                        backup_path: latest_backup,
                    },
                }
            } else {
                DoctorRepairPlan {
                    step: DoctorRepairStep {
                        id: "node_runtime.reinitialize".to_owned(),
                        kind: "node_runtime_reinitialize".to_owned(),
                        severity: DoctorSeverity::Blocking,
                        title: "Reinitialize unreadable node runtime state".to_owned(),
                        description: sanitize_diagnostic_error(error.to_string().as_str()),
                        impact: "Quarantines the unreadable node runtime document and writes an empty state shell.".to_owned(),
                        local_only: true,
                        security_sensitive: false,
                        requires_force: true,
                        apply_supported: true,
                        changed_objects: vec![display_path(path.as_path())],
                        warnings: vec!["This path requires --force because no rotated backup exists.".to_owned()],
                    },
                    kind: DoctorRepairKind::ReinitializeNodeRuntime { path },
                }
            }]);
        }
    };
    let normalized = normalize_node_runtime_json(value, environment.generated_at_unix_ms);
    if !normalized.changed {
        return Ok(Vec::new());
    }
    Ok(vec![DoctorRepairPlan {
        step: DoctorRepairStep {
            id: "node_runtime.normalize".to_owned(),
            kind: "node_runtime_normalize".to_owned(),
            severity: DoctorSeverity::Warning,
            title: "Normalize node runtime state".to_owned(),
            description: format!(
                "Expire stale pairing state, drop expired pairing codes, and stamp schema metadata (expired codes: {}, expired requests: {}).",
                normalized.expired_codes, normalized.expired_requests
            ),
            impact: "Rewrites the node runtime JSON document in-place.".to_owned(),
            local_only: true,
            security_sensitive: false,
            requires_force: false,
            apply_supported: true,
            changed_objects: vec![display_path(path.as_path())],
            warnings: Vec::new(),
        },
        kind: DoctorRepairKind::NormalizeNodeRuntime { path },
    }])
}

fn evaluate_access_registry_repairs(
    environment: &DoctorEnvironment,
) -> Result<Vec<DoctorRepairPlan>> {
    let path = environment.state_root.join(DOCTOR_ACCESS_REGISTRY_FILE_NAME);
    if !path.exists() {
        return Ok(Vec::new());
    }
    let raw = fs::read_to_string(path.as_path())
        .with_context(|| format!("failed to read access registry {}", path.display()))?;
    let value = match serde_json::from_str::<JsonValue>(raw.as_str()) {
        Ok(value) => value,
        Err(error) => {
            let latest_backup = backup_path(path.as_path(), 1);
            return Ok(vec![if latest_backup.exists() {
                DoctorRepairPlan {
                    step: DoctorRepairStep {
                        id: "access_registry.restore_latest_backup".to_owned(),
                        kind: "access_registry_restore_backup".to_owned(),
                        severity: DoctorSeverity::Blocking,
                        title: "Restore latest access registry backup".to_owned(),
                        description: sanitize_diagnostic_error(error.to_string().as_str()),
                        impact: "Restores the access registry from the latest rotated backup."
                            .to_owned(),
                        local_only: true,
                        security_sensitive: false,
                        requires_force: false,
                        apply_supported: true,
                        changed_objects: vec![
                            display_path(path.as_path()),
                            display_path(latest_backup.as_path()),
                        ],
                        warnings: Vec::new(),
                    },
                    kind: DoctorRepairKind::RestoreAccessRegistryBackup {
                        path,
                        backup_path: latest_backup,
                    },
                }
            } else {
                DoctorRepairPlan {
                    step: DoctorRepairStep {
                        id: "access_registry.reinitialize".to_owned(),
                        kind: "access_registry_reinitialize".to_owned(),
                        severity: DoctorSeverity::Blocking,
                        title: "Reinitialize unreadable access registry".to_owned(),
                        description: sanitize_diagnostic_error(error.to_string().as_str()),
                        impact: "Quarantines the unreadable access registry and writes a default fail-closed registry shell.".to_owned(),
                        local_only: true,
                        security_sensitive: true,
                        requires_force: true,
                        apply_supported: true,
                        changed_objects: vec![display_path(path.as_path())],
                        warnings: vec!["This path requires --force because no rotated backup exists.".to_owned()],
                    },
                    kind: DoctorRepairKind::ReinitializeAccessRegistry { path },
                }
            }]);
        }
    };
    let normalized = normalize_access_registry_json(value, environment.generated_at_unix_ms);
    if !normalized.changed {
        return Ok(Vec::new());
    }
    Ok(vec![DoctorRepairPlan {
        step: DoctorRepairStep {
            id: "access_registry.backfill".to_owned(),
            kind: "access_registry_backfill".to_owned(),
            severity: DoctorSeverity::Warning,
            title: "Backfill access registry rollout defaults".to_owned(),
            description: format!(
                "Restore missing access registry schema/defaults and feature flags (missing flags: {}).",
                if normalized.missing_feature_flags.is_empty() {
                    "none".to_owned()
                } else {
                    normalized.missing_feature_flags.join(", ")
                }
            ),
            impact: "Rewrites the access registry JSON while preserving existing principals, memberships, and shares.".to_owned(),
            local_only: true,
            security_sensitive: true,
            requires_force: false,
            apply_supported: true,
            changed_objects: vec![display_path(path.as_path())],
            warnings: Vec::new(),
        },
        kind: DoctorRepairKind::BackfillAccessRegistry { path },
    }])
}

fn evaluate_browser_profile_registry_repairs(
    environment: &DoctorEnvironment,
) -> Result<Vec<DoctorRepairPlan>> {
    let Some(master_key) = resolve_browser_state_key()? else {
        return Ok(Vec::new());
    };
    let state_dir = resolve_browser_state_dir(environment.state_root.as_path())?;
    let registry_path = state_dir.join(DOCTOR_BROWSER_PROFILE_REGISTRY_FILE_NAME);
    if !registry_path.exists() {
        return Ok(Vec::new());
    }
    let encrypted = fs::read(registry_path.as_path()).with_context(|| {
        format!("failed to read browser profile registry {}", registry_path.display())
    })?;
    let decrypted = decrypt_browser_state_blob(&master_key, encrypted.as_slice());
    let decrypted = match decrypted {
        Ok(bytes) => bytes,
        Err(error) => {
            return Ok(vec![DoctorRepairPlan {
                step: DoctorRepairStep {
                    id: "browser.profile_registry.reinitialize".to_owned(),
                    kind: "browser_profile_registry_reinitialize".to_owned(),
                    severity: DoctorSeverity::Blocking,
                    title: "Reinitialize unreadable browser profile registry".to_owned(),
                    description: sanitize_diagnostic_error(error.to_string().as_str()),
                    impact: "Quarantines the unreadable encrypted registry and writes a fresh empty registry.".to_owned(),
                    local_only: true,
                    security_sensitive: true,
                    requires_force: true,
                    apply_supported: true,
                    changed_objects: vec![display_path(registry_path.as_path())],
                    warnings: vec!["This path requires --force because the encrypted registry could not be decoded.".to_owned()],
                },
                kind: DoctorRepairKind::ReinitializeBrowserProfileRegistry { registry_path, master_key },
            }]);
        }
    };
    let value = match serde_json::from_slice::<JsonValue>(decrypted.as_slice()) {
        Ok(value) => value,
        Err(error) => {
            return Ok(vec![DoctorRepairPlan {
                step: DoctorRepairStep {
                    id: "browser.profile_registry.reinitialize".to_owned(),
                    kind: "browser_profile_registry_reinitialize".to_owned(),
                    severity: DoctorSeverity::Blocking,
                    title: "Reinitialize malformed browser profile registry".to_owned(),
                    description: sanitize_diagnostic_error(error.to_string().as_str()),
                    impact: "Quarantines the unreadable encrypted registry and writes a fresh empty registry.".to_owned(),
                    local_only: true,
                    security_sensitive: true,
                    requires_force: true,
                    apply_supported: true,
                    changed_objects: vec![display_path(registry_path.as_path())],
                    warnings: vec!["This path requires --force.".to_owned()],
                },
                kind: DoctorRepairKind::ReinitializeBrowserProfileRegistry { registry_path, master_key },
            }]);
        }
    };
    let normalized = normalize_browser_profile_registry_json(value);
    if !normalized.changed {
        return Ok(Vec::new());
    }
    Ok(vec![DoctorRepairPlan {
        step: DoctorRepairStep {
            id: "browser.profile_registry.normalize".to_owned(),
            kind: "browser_profile_registry_normalize".to_owned(),
            severity: DoctorSeverity::Warning,
            title: "Normalize browser profile registry".to_owned(),
            description: format!(
                "Repair browser profile schema/version metadata and drop invalid active profile pointers (repaired profiles: {}, removed active pointers: {}).",
                normalized.repaired_profile_records, normalized.removed_active_pointers
            ),
            impact: "Rewrites the encrypted browser profile registry with a normalized active-profile map.".to_owned(),
            local_only: true,
            security_sensitive: true,
            requires_force: false,
            apply_supported: true,
            changed_objects: vec![display_path(registry_path.as_path())],
            warnings: Vec::new(),
        },
        kind: DoctorRepairKind::NormalizeBrowserProfileRegistry { registry_path, master_key },
    }])
}

fn evaluate_stale_artifact_cleanup(
    environment: &DoctorEnvironment,
) -> Result<Option<DoctorRepairPlan>> {
    let mut candidates = scan_stale_artifacts(environment.state_root.as_path())?;
    let browser_state_dir = resolve_browser_state_dir(environment.state_root.as_path())?;
    if browser_state_dir != environment.state_root {
        candidates.extend(scan_stale_artifacts(browser_state_dir.as_path())?);
    }
    candidates.sort();
    candidates.dedup();
    if candidates.is_empty() {
        return Ok(None);
    }
    Ok(Some(DoctorRepairPlan {
        step: DoctorRepairStep {
            id: "stale_runtime.cleanup".to_owned(),
            kind: "stale_runtime_cleanup".to_owned(),
            severity: DoctorSeverity::Warning,
            title: "Clean stale runtime artifacts".to_owned(),
            description: format!(
                "Remove stale lock/tmp/socket/pid artifacts left behind by interrupted runtime flows ({} paths).",
                candidates.len()
            ),
            impact: "Deletes stale local-only artifacts that are old enough to be considered abandoned.".to_owned(),
            local_only: true,
            security_sensitive: false,
            requires_force: false,
            apply_supported: true,
            changed_objects: candidates.iter().map(|path| display_path(path.as_path())).collect(),
            warnings: vec![
                "Cleanup only targets files older than the configured stale threshold to avoid touching active runtime paths."
                    .to_owned(),
            ],
        },
        kind: DoctorRepairKind::CleanupArtifacts { paths: candidates },
    }))
}

struct DoctorApplyResult {
    applied_steps: Vec<DoctorAppliedStep>,
    run_id: Option<String>,
    manifest_path: Option<String>,
    next_steps: Vec<String>,
}

struct DoctorRollbackResult {
    applied_steps: Vec<DoctorAppliedStep>,
    manifest_path: Option<String>,
    next_steps: Vec<String>,
}

fn apply_repair_plans(
    environment: &DoctorEnvironment,
    plans: &[DoctorRepairPlan],
    force: bool,
) -> Result<DoctorApplyResult> {
    let actionable = plans.iter().filter(|entry| entry.step.apply_supported).count();
    if actionable == 0 {
        return Ok(DoctorApplyResult {
            applied_steps: plans
                .iter()
                .map(|entry| DoctorAppliedStep {
                    id: entry.step.id.clone(),
                    outcome: if entry.step.apply_supported {
                        "no_change".to_owned()
                    } else {
                        "manual".to_owned()
                    },
                    message: if entry.step.apply_supported {
                        "No repair action was required.".to_owned()
                    } else {
                        "Manual follow-up required; no automatic apply path exists.".to_owned()
                    },
                    changed_objects: Vec::new(),
                    warnings: entry.step.warnings.clone(),
                })
                .collect(),
            run_id: None,
            manifest_path: None,
            next_steps: build_repair_next_steps_preview(&[]),
        });
    }

    let run_id = Ulid::new().to_string();
    let manager = DoctorRecoveryRunWriter::new(environment, run_id.as_str())?;
    let mut applied_steps = Vec::new();
    for plan in plans {
        if !plan.step.apply_supported {
            applied_steps.push(DoctorAppliedStep {
                id: plan.step.id.clone(),
                outcome: "manual".to_owned(),
                message: "Manual follow-up required; no automatic apply path exists.".to_owned(),
                changed_objects: Vec::new(),
                warnings: plan.step.warnings.clone(),
            });
            continue;
        }
        if plan.step.requires_force && !force {
            applied_steps.push(DoctorAppliedStep {
                id: plan.step.id.clone(),
                outcome: "blocked".to_owned(),
                message: "Step requires --force.".to_owned(),
                changed_objects: Vec::new(),
                warnings: plan.step.warnings.clone(),
            });
            continue;
        }
        applied_steps.push(apply_repair_plan(manager.as_ref(), plan)?);
    }
    let next_steps = vec![
        format!("palyra doctor --rollback-run {}", manager.run_id()),
        "palyra support-bundle export --output ./support-bundle.json".to_owned(),
    ];
    manager.complete(applied_steps.as_slice(), next_steps.as_slice())?;
    Ok(DoctorApplyResult {
        applied_steps,
        run_id: Some(manager.run_id().to_owned()),
        manifest_path: Some(display_path(manager.manifest_path())),
        next_steps,
    })
}

fn apply_repair_plan(
    manager: &DoctorRecoveryRunWriter,
    plan: &DoctorRepairPlan,
) -> Result<DoctorAppliedStep> {
    match &plan.kind {
        DoctorRepairKind::InitializeMissingConfig { path } => {
            let document = empty_versioned_config_document()?;
            let encoded = serialize_document_pretty(&document)?;
            manager.write_string(plan.step.id.as_str(), path.as_path(), encoded.as_str(), true)?;
            Ok(applied_step_ok(plan, "Initialized versioned config document.".to_owned()))
        }
        DoctorRepairKind::MigrateConfigVersion { path } => {
            let raw = fs::read_to_string(path.as_path())
                .with_context(|| format!("failed to read config {}", path.display()))?;
            let (document, _) = parse_document_with_migration(raw.as_str())?;
            let encoded = serialize_document_pretty(&document)?;
            manager.write_string(plan.step.id.as_str(), path.as_path(), encoded.as_str(), true)?;
            Ok(applied_step_ok(plan, "Config schema metadata updated.".to_owned()))
        }
        DoctorRepairKind::RestoreConfigBackup { path, backup_path } => {
            restore_from_backup_entry(manager, plan, path.as_path(), backup_path.as_path(), true)?;
            Ok(applied_step_ok(plan, "Restored config from the latest backup.".to_owned()))
        }
        DoctorRepairKind::ReinitializeConfig { path } => {
            if path.exists() {
                manager.move_existing_file(
                    plan.step.id.as_str(),
                    path.as_path(),
                    quarantine_path(path.as_path()).as_path(),
                    true,
                )?;
            }
            let document = empty_versioned_config_document()?;
            let encoded = serialize_document_pretty(&document)?;
            manager.write_string(plan.step.id.as_str(), path.as_path(), encoded.as_str(), true)?;
            Ok(applied_step_ok(
                plan,
                "Quarantined the unreadable config and wrote a minimal versioned config."
                    .to_owned(),
            ))
        }
        DoctorRepairKind::GenerateBrowserAuthToken { path } => {
            let (mut document, _) = load_or_init_document(path.as_path())?;
            set_value_at_path(
                &mut document,
                "tool_call.browser_service.auth_token",
                TomlValue::String(format!("browser-{}", Ulid::new())),
            )?;
            let encoded = serialize_document_pretty(&document)?;
            manager.write_string(plan.step.id.as_str(), path.as_path(), encoded.as_str(), true)?;
            Ok(applied_step_ok(
                plan,
                "Generated and persisted a browser relay auth token.".to_owned(),
            ))
        }
        DoctorRepairKind::NormalizeAuthRegistry { path, quarantine_unknown_root_keys } => {
            let raw = fs::read_to_string(path.as_path())
                .with_context(|| format!("failed to read auth registry {}", path.display()))?;
            let mut document = toml::from_str::<TomlValue>(raw.as_str())
                .context("failed to parse auth registry for normalization")?;
            let root = document.as_table_mut().context("auth registry root must be a table")?;
            root.insert("version".to_owned(), TomlValue::Integer(1));
            for (key, _) in quarantine_unknown_root_keys {
                root.remove(key.as_str());
            }
            if !root.contains_key("profiles") {
                root.insert("profiles".to_owned(), TomlValue::Array(Vec::new()));
            }
            let encoded = toml::to_string_pretty(&document)
                .context("failed to serialize auth registry document")?;
            manager.write_string(plan.step.id.as_str(), path.as_path(), encoded.as_str(), false)?;
            if !quarantine_unknown_root_keys.is_empty() {
                let quarantine_document = TomlValue::Table(
                    quarantine_unknown_root_keys
                        .iter()
                        .map(|(key, value)| (key.clone(), value.clone()))
                        .collect(),
                );
                let quarantine_encoded = toml::to_string_pretty(&quarantine_document)
                    .context("failed to serialize auth registry quarantine document")?;
                manager.write_string(
                    plan.step.id.as_str(),
                    auth_registry_quarantine_path(path.as_path()).as_path(),
                    quarantine_encoded.as_str(),
                    false,
                )?;
            }
            Ok(applied_step_ok(plan, "Normalized auth registry schema metadata.".to_owned()))
        }
        DoctorRepairKind::RestoreAuthRegistryBackup { path, backup_path } => {
            restore_from_backup_entry(manager, plan, path.as_path(), backup_path.as_path(), false)?;
            Ok(applied_step_ok(plan, "Restored auth registry from the latest backup.".to_owned()))
        }
        DoctorRepairKind::ReinitializeAuthRegistry { path } => {
            if path.exists() {
                manager.move_existing_file(
                    plan.step.id.as_str(),
                    path.as_path(),
                    quarantine_path(path.as_path()).as_path(),
                    false,
                )?;
            }
            let encoded = empty_auth_registry_document()?;
            manager.write_string(plan.step.id.as_str(), path.as_path(), encoded.as_str(), false)?;
            Ok(applied_step_ok(
                plan,
                "Quarantined the unreadable auth registry and wrote an empty versioned registry."
                    .to_owned(),
            ))
        }
        DoctorRepairKind::GatewayRemoteVerificationManual { path } => Ok(DoctorAppliedStep {
            id: plan.step.id.clone(),
            outcome: "manual".to_owned(),
            message: format!(
                "Manual review required for gateway access trust material in {}.",
                display_path(path.as_path())
            ),
            changed_objects: Vec::new(),
            warnings: plan.step.warnings.clone(),
        }),
        DoctorRepairKind::NormalizeCliProfiles { path, quarantine_unknown_root_keys } => {
            let raw = fs::read_to_string(path.as_path())
                .with_context(|| format!("failed to read CLI profiles {}", path.display()))?;
            let mut document = toml::from_str::<TomlValue>(raw.as_str())
                .context("failed to parse CLI profiles for normalization")?;
            let root = document.as_table_mut().context("CLI profiles root must be a table")?;
            root.insert("version".to_owned(), TomlValue::Integer(1));
            for (key, _) in quarantine_unknown_root_keys {
                root.remove(key.as_str());
            }
            let encoded = toml::to_string_pretty(&document)
                .context("failed to serialize CLI profiles document")?;
            manager.write_string(plan.step.id.as_str(), path.as_path(), encoded.as_str(), false)?;
            if !quarantine_unknown_root_keys.is_empty() {
                let quarantine_document = TomlValue::Table(
                    quarantine_unknown_root_keys
                        .iter()
                        .map(|(key, value)| (key.clone(), value.clone()))
                        .collect(),
                );
                let quarantine_encoded = toml::to_string_pretty(&quarantine_document)
                    .context("failed to serialize CLI profiles quarantine document")?;
                manager.write_string(
                    plan.step.id.as_str(),
                    cli_profiles_quarantine_path(path.as_path()).as_path(),
                    quarantine_encoded.as_str(),
                    false,
                )?;
            }
            Ok(applied_step_ok(plan, "Normalized CLI profiles schema metadata.".to_owned()))
        }
        DoctorRepairKind::RestoreCliProfilesBackup { path, backup_path } => {
            restore_from_backup_entry(manager, plan, path.as_path(), backup_path.as_path(), false)?;
            Ok(applied_step_ok(plan, "Restored CLI profiles from the latest backup.".to_owned()))
        }
        DoctorRepairKind::ReinitializeCliProfiles { path } => {
            if path.exists() {
                manager.move_existing_file(
                    plan.step.id.as_str(),
                    path.as_path(),
                    quarantine_path(path.as_path()).as_path(),
                    false,
                )?;
            }
            let encoded = toml_json_to_string(json!({ "version": 1, "profiles": {} }))?;
            manager.write_string(plan.step.id.as_str(), path.as_path(), encoded.as_str(), false)?;
            Ok(applied_step_ok(plan, "Reinitialized CLI profiles document.".to_owned()))
        }
        DoctorRepairKind::NormalizeRoutineRegistry { path, top_level_array_key } => {
            let raw = fs::read_to_string(path.as_path())
                .with_context(|| format!("failed to read routine registry {}", path.display()))?;
            let value = serde_json::from_str::<JsonValue>(raw.as_str()).unwrap_or(JsonValue::Null);
            let normalized = normalize_routine_registry_json(value, top_level_array_key);
            let encoded = serde_json::to_vec_pretty(&normalized)
                .context("failed to serialize routine registry")?;
            manager.write_bytes(
                plan.step.id.as_str(),
                path.as_path(),
                encoded.as_slice(),
                false,
            )?;
            Ok(applied_step_ok(plan, "Normalized routine registry schema.".to_owned()))
        }
        DoctorRepairKind::NormalizeNodeRuntime { path } => {
            let raw = fs::read_to_string(path.as_path())
                .with_context(|| format!("failed to read node runtime state {}", path.display()))?;
            let value = serde_json::from_str::<JsonValue>(raw.as_str())
                .context("failed to parse node runtime state")?;
            let normalized = normalize_node_runtime_json(value, now_unix_ms_i64()?);
            let encoded = serde_json::to_vec_pretty(&normalized.value)
                .context("failed to encode node runtime state")?;
            manager.write_bytes(
                plan.step.id.as_str(),
                path.as_path(),
                encoded.as_slice(),
                false,
            )?;
            Ok(applied_step_ok(plan, "Normalized node runtime state.".to_owned()))
        }
        DoctorRepairKind::RestoreNodeRuntimeBackup { path, backup_path } => {
            restore_from_backup_entry(manager, plan, path.as_path(), backup_path.as_path(), false)?;
            Ok(applied_step_ok(
                plan,
                "Restored node runtime state from the latest backup.".to_owned(),
            ))
        }
        DoctorRepairKind::ReinitializeNodeRuntime { path } => {
            if path.exists() {
                manager.move_existing_file(
                    plan.step.id.as_str(),
                    path.as_path(),
                    quarantine_path(path.as_path()).as_path(),
                    false,
                )?;
            }
            let encoded = serde_json::to_vec_pretty(&json!({
                "version": 1,
                "active_pairing_codes": {},
                "pairing_requests": {},
                "nodes": {}
            }))
            .context("failed to encode empty node runtime state")?;
            manager.write_bytes(
                plan.step.id.as_str(),
                path.as_path(),
                encoded.as_slice(),
                false,
            )?;
            Ok(applied_step_ok(plan, "Reinitialized node runtime state.".to_owned()))
        }
        DoctorRepairKind::BackfillAccessRegistry { path } => {
            let raw = fs::read_to_string(path.as_path())
                .with_context(|| format!("failed to read access registry {}", path.display()))?;
            let value = serde_json::from_str::<JsonValue>(raw.as_str())
                .context("failed to parse access registry")?;
            let normalized = normalize_access_registry_json(value, now_unix_ms_i64()?);
            let encoded = serde_json::to_vec_pretty(&normalized.value)
                .context("failed to encode access registry")?;
            manager.write_bytes(
                plan.step.id.as_str(),
                path.as_path(),
                encoded.as_slice(),
                false,
            )?;
            Ok(applied_step_ok(plan, "Backfilled access registry defaults.".to_owned()))
        }
        DoctorRepairKind::RestoreAccessRegistryBackup { path, backup_path } => {
            restore_from_backup_entry(manager, plan, path.as_path(), backup_path.as_path(), false)?;
            Ok(applied_step_ok(plan, "Restored access registry from the latest backup.".to_owned()))
        }
        DoctorRepairKind::ReinitializeAccessRegistry { path } => {
            if path.exists() {
                manager.move_existing_file(
                    plan.step.id.as_str(),
                    path.as_path(),
                    quarantine_path(path.as_path()).as_path(),
                    false,
                )?;
            }
            let encoded = serde_json::to_vec_pretty(&default_access_registry_json())
                .context("failed to encode default access registry")?;
            manager.write_bytes(
                plan.step.id.as_str(),
                path.as_path(),
                encoded.as_slice(),
                false,
            )?;
            Ok(applied_step_ok(
                plan,
                "Reinitialized access registry with fail-closed defaults.".to_owned(),
            ))
        }
        DoctorRepairKind::NormalizeBrowserProfileRegistry { registry_path, master_key } => {
            let encrypted = fs::read(registry_path.as_path()).with_context(|| {
                format!("failed to read browser profile registry {}", registry_path.display())
            })?;
            let decrypted = decrypt_browser_state_blob(master_key, encrypted.as_slice())?;
            let value = serde_json::from_slice::<JsonValue>(decrypted.as_slice())
                .context("failed to parse browser profile registry")?;
            let normalized = normalize_browser_profile_registry_json(value);
            let encoded = serde_json::to_vec(&normalized.value)
                .context("failed to encode browser profile registry")?;
            let encrypted = encrypt_browser_state_blob(master_key, encoded.as_slice())?;
            manager.write_bytes(
                plan.step.id.as_str(),
                registry_path.as_path(),
                encrypted.as_slice(),
                true,
            )?;
            Ok(applied_step_ok(plan, "Normalized browser profile registry.".to_owned()))
        }
        DoctorRepairKind::ReinitializeBrowserProfileRegistry { registry_path, master_key } => {
            if registry_path.exists() {
                manager.move_existing_file(
                    plan.step.id.as_str(),
                    registry_path.as_path(),
                    quarantine_path(registry_path.as_path()).as_path(),
                    true,
                )?;
            }
            let encoded = serde_json::to_vec(&json!({
                "v": DOCTOR_BROWSER_PROFILE_REGISTRY_VERSION,
                "profiles": [],
                "active_profile_by_principal": {}
            }))
            .context("failed to encode empty browser profile registry")?;
            let encrypted = encrypt_browser_state_blob(master_key, encoded.as_slice())?;
            manager.write_bytes(
                plan.step.id.as_str(),
                registry_path.as_path(),
                encrypted.as_slice(),
                true,
            )?;
            Ok(applied_step_ok(plan, "Reinitialized browser profile registry.".to_owned()))
        }
        DoctorRepairKind::CleanupArtifacts { paths } => {
            for path in paths {
                manager.remove_file(plan.step.id.as_str(), path.as_path(), false)?;
            }
            Ok(applied_step_ok(plan, format!("Removed {} stale runtime artifacts.", paths.len())))
        }
    }
}

fn restore_from_backup_entry(
    manager: &DoctorRecoveryRunWriter,
    plan: &DoctorRepairPlan,
    target_path: &Path,
    backup_path: &Path,
    secret_aware: bool,
) -> Result<()> {
    let restored = fs::read(backup_path)
        .with_context(|| format!("failed to read backup {}", backup_path.display()))?;
    manager.write_bytes(plan.step.id.as_str(), target_path, restored.as_slice(), secret_aware)
}

fn applied_step_ok(plan: &DoctorRepairPlan, message: String) -> DoctorAppliedStep {
    DoctorAppliedStep {
        id: plan.step.id.clone(),
        outcome: "applied".to_owned(),
        message,
        changed_objects: plan.step.changed_objects.clone(),
        warnings: plan.step.warnings.clone(),
    }
}

fn execute_rollback(
    state_root: &Path,
    run_id: &str,
    force: bool,
    dry_run: bool,
) -> Result<DoctorRollbackResult> {
    let manifest_path = state_root
        .join(DOCTOR_RECOVERY_RUNS_RELATIVE_DIR)
        .join(run_id)
        .join(DOCTOR_RECOVERY_MANIFEST_FILE_NAME);
    let raw = fs::read_to_string(manifest_path.as_path())
        .with_context(|| format!("failed to read recovery manifest {}", manifest_path.display()))?;
    let manifest =
        serde_json::from_str::<DoctorRecoveryManifest>(raw.as_str()).with_context(|| {
            format!("failed to parse recovery manifest {}", manifest_path.display())
        })?;
    let mut applied_steps = Vec::new();
    if dry_run {
        for entry in manifest.entries {
            applied_steps.push(DoctorAppliedStep {
                id: format!("rollback:{}", entry.step_id),
                outcome: "planned".to_owned(),
                message: format!("Would restore '{}'.", entry.target_path),
                changed_objects: vec![entry.target_path],
                warnings: Vec::new(),
            });
        }
        return Ok(DoctorRollbackResult {
            applied_steps,
            manifest_path: Some(display_path(manifest_path.as_path())),
            next_steps: vec![format!("palyra doctor --rollback-run {run_id} --force")],
        });
    }

    for entry in manifest.entries {
        let target = PathBuf::from(entry.target_path.as_str());
        let current_hash = if target.exists() { Some(hash_file(target.as_path())?) } else { None };
        if !force && current_hash != entry.after_sha256 {
            applied_steps.push(DoctorAppliedStep {
                id: format!("rollback:{}", entry.step_id),
                outcome: "blocked".to_owned(),
                message: format!(
                    "Current file hash differs from the recorded post-repair hash for '{}'.",
                    display_path(target.as_path())
                ),
                changed_objects: vec![display_path(target.as_path())],
                warnings: vec![
                    "Re-run with --force only after confirming there are no newer manual changes."
                        .to_owned(),
                ],
            });
            continue;
        }
        match entry.change_type.as_str() {
            "created" | "deleted" => {
                if target.exists() {
                    fs::remove_file(target.as_path()).with_context(|| {
                        format!("failed to remove rollback target {}", target.display())
                    })?;
                }
            }
            _ => {
                let backup_path = entry
                    .backup_path
                    .as_deref()
                    .map(PathBuf::from)
                    .ok_or_else(|| anyhow::anyhow!("rollback entry missing backup_path"))?;
                let bytes = fs::read(backup_path.as_path()).with_context(|| {
                    format!("failed to read rollback backup {}", backup_path.display())
                })?;
                write_bytes_atomic(target.as_path(), bytes.as_slice())?;
            }
        }
        applied_steps.push(DoctorAppliedStep {
            id: format!("rollback:{}", entry.step_id),
            outcome: "applied".to_owned(),
            message: format!("Restored '{}'.", display_path(target.as_path())),
            changed_objects: vec![display_path(target.as_path())],
            warnings: Vec::new(),
        });
    }
    Ok(DoctorRollbackResult {
        applied_steps,
        manifest_path: Some(display_path(manifest_path.as_path())),
        next_steps: vec!["palyra doctor --repair --dry-run --json".to_owned()],
    })
}

struct DoctorRecoveryRunWriter {
    manifest_path: PathBuf,
    manifest: Mutex<DoctorRecoveryManifest>,
}

impl DoctorRecoveryRunWriter {
    fn new(environment: &DoctorEnvironment, run_id: &str) -> Result<Arc<Self>> {
        let run_dir = environment.state_root.join(DOCTOR_RECOVERY_RUNS_RELATIVE_DIR).join(run_id);
        fs::create_dir_all(run_dir.join("backups")).with_context(|| {
            format!("failed to create recovery run directory {}", run_dir.display())
        })?;
        let writer = Arc::new(Self {
            manifest_path: run_dir.join(DOCTOR_RECOVERY_MANIFEST_FILE_NAME),
            manifest: Mutex::new(DoctorRecoveryManifest {
                schema_version: DOCTOR_RECOVERY_MANIFEST_SCHEMA_VERSION,
                run_id: run_id.to_owned(),
                created_at_unix_ms: environment.generated_at_unix_ms,
                state_root: display_path(environment.state_root.as_path()),
                config_path: environment
                    .config_path
                    .as_ref()
                    .map(|path| display_path(path.as_path())),
                completed: false,
                steps: Vec::new(),
                applied_steps: Vec::new(),
                next_steps: Vec::new(),
                entries: Vec::new(),
            }),
        });
        writer.persist()?;
        Ok(writer)
    }

    fn run_id(&self) -> String {
        self.manifest.lock().expect("manifest lock").run_id.clone()
    }

    fn manifest_path(&self) -> &Path {
        self.manifest_path.as_path()
    }

    fn complete(&self, applied_steps: &[DoctorAppliedStep], next_steps: &[String]) -> Result<()> {
        {
            let mut guard = self
                .manifest
                .lock()
                .map_err(|_| anyhow::anyhow!("recovery manifest lock poisoned"))?;
            guard.completed = true;
            guard.applied_steps = applied_steps.to_vec();
            guard.next_steps = next_steps.to_vec();
        }
        self.persist()
    }

    fn write_string(
        &self,
        step_id: &str,
        path: &Path,
        content: &str,
        secret_aware: bool,
    ) -> Result<()> {
        self.write_bytes(step_id, path, content.as_bytes(), secret_aware)
    }

    fn write_bytes(
        &self,
        step_id: &str,
        path: &Path,
        content: &[u8],
        secret_aware: bool,
    ) -> Result<()> {
        let before = snapshot_file(path)?;
        let backup_path = self.persist_backup(step_id, path, before.as_ref())?;
        write_bytes_atomic(path, content)?;
        self.record_entry(
            step_id,
            path,
            backup_path,
            before,
            Some(sha256_hex(content)),
            secret_aware,
        )
    }

    fn remove_file(&self, step_id: &str, path: &Path, secret_aware: bool) -> Result<()> {
        let before = snapshot_file(path)?;
        let backup_path = self.persist_backup(step_id, path, before.as_ref())?;
        if path.exists() {
            fs::remove_file(path)
                .with_context(|| format!("failed to remove stale artifact {}", path.display()))?;
        }
        self.record_entry(step_id, path, backup_path, before, None, secret_aware)
    }

    fn move_existing_file(
        &self,
        step_id: &str,
        path: &Path,
        destination: &Path,
        secret_aware: bool,
    ) -> Result<()> {
        if !path.exists() {
            return Ok(());
        }
        let before = snapshot_file(path)?;
        let backup_path = self.persist_backup(step_id, path, before.as_ref())?;
        let bytes = fs::read(path)
            .with_context(|| format!("failed to read quarantine source {}", path.display()))?;
        write_bytes_atomic(destination, bytes.as_slice())?;
        fs::remove_file(path).with_context(|| {
            format!("failed to remove original quarantined file {}", path.display())
        })?;
        self.record_entry(step_id, path, backup_path, before, None, secret_aware)
    }

    fn persist_backup(
        &self,
        step_id: &str,
        path: &Path,
        snapshot: Option<&FileSnapshot>,
    ) -> Result<Option<PathBuf>> {
        let Some(snapshot) = snapshot else {
            return Ok(None);
        };
        let backup_dir = self.manifest_path.parent().expect("manifest path parent").join("backups");
        fs::create_dir_all(backup_dir.as_path())
            .with_context(|| format!("failed to create backup dir {}", backup_dir.display()))?;
        let backup_path = backup_dir.join(format!(
            "{}-{}-{}",
            sanitize_file_component(step_id),
            Ulid::new(),
            path.file_name().and_then(|value| value.to_str()).unwrap_or("backup.bin")
        ));
        write_bytes_atomic(backup_path.as_path(), snapshot.bytes.as_slice())?;
        Ok(Some(backup_path))
    }

    fn record_entry(
        &self,
        step_id: &str,
        path: &Path,
        backup_path: Option<PathBuf>,
        before: Option<FileSnapshot>,
        after_sha256: Option<String>,
        secret_aware: bool,
    ) -> Result<()> {
        {
            let mut manifest = self
                .manifest
                .lock()
                .map_err(|_| anyhow::anyhow!("recovery manifest lock poisoned"))?;
            if !manifest.steps.iter().any(|existing| existing == step_id) {
                manifest.steps.push(step_id.to_owned());
            }
            manifest.entries.push(DoctorRecoveryManifestEntry {
                step_id: step_id.to_owned(),
                target_path: display_path(path),
                change_type: match (before.as_ref(), after_sha256.as_ref()) {
                    (Some(_), Some(_)) => "modified".to_owned(),
                    (None, Some(_)) => "created".to_owned(),
                    (Some(_), None) => "deleted".to_owned(),
                    (None, None) => "noop".to_owned(),
                },
                backup_path: backup_path.map(|value| display_path(value.as_path())),
                before_sha256: before.as_ref().map(|snapshot| snapshot.sha256.clone()),
                after_sha256,
                secret_aware,
            });
        }
        self.persist()
    }

    fn persist(&self) -> Result<()> {
        let manifest = self
            .manifest
            .lock()
            .map_err(|_| anyhow::anyhow!("recovery manifest lock poisoned"))?
            .clone();
        let encoded =
            serde_json::to_vec_pretty(&manifest).context("failed to encode recovery manifest")?;
        write_bytes_atomic(self.manifest_path.as_path(), encoded.as_slice())
    }
}

#[derive(Clone)]
struct FileSnapshot {
    bytes: Vec<u8>,
    sha256: String,
}

fn snapshot_file(path: &Path) -> Result<Option<FileSnapshot>> {
    if !path.exists() {
        return Ok(None);
    }
    let bytes = fs::read(path)
        .with_context(|| format!("failed to read snapshot source {}", path.display()))?;
    Ok(Some(FileSnapshot { sha256: sha256_hex(bytes.as_slice()), bytes }))
}

fn hash_file(path: &Path) -> Result<String> {
    let bytes =
        fs::read(path).with_context(|| format!("failed to read hash source {}", path.display()))?;
    Ok(sha256_hex(bytes.as_slice()))
}

fn write_bytes_atomic(path: &Path, content: &[u8]) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create parent dir {}", parent.display()))?;
    }
    let tmp_path = path.with_extension(format!("{}.tmp", Ulid::new()));
    fs::write(tmp_path.as_path(), content)
        .with_context(|| format!("failed to write tmp file {}", tmp_path.display()))?;
    if path.exists() {
        let rollback_path = path.with_extension(format!("{}.rollback", Ulid::new()));
        fs::rename(path, rollback_path.as_path()).with_context(|| {
            format!("failed to stage existing file {} for atomic write", path.display())
        })?;
        if let Err(error) = fs::rename(tmp_path.as_path(), path) {
            let _ = fs::rename(rollback_path.as_path(), path);
            return Err(error)
                .with_context(|| format!("failed to install atomic file {}", path.display()));
        }
        let _ = fs::remove_file(rollback_path.as_path());
    } else {
        fs::rename(tmp_path.as_path(), path)
            .with_context(|| format!("failed to install atomic file {}", path.display()))?;
    }
    Ok(())
}

fn empty_versioned_config_document() -> Result<TomlValue> {
    let (document, _) =
        parse_document_with_migration("").context("failed to create empty versioned config")?;
    Ok(document)
}

fn load_or_init_document(path: &Path) -> Result<(TomlValue, ConfigMigrationInfo)> {
    if path.exists() {
        return load_document_from_existing_path(path);
    }
    let (document, migration) =
        parse_document_with_migration("").context("failed to create empty config document")?;
    Ok((document, migration))
}

fn normalize_routine_registry_json(value: JsonValue, top_level_array_key: &str) -> JsonValue {
    let mut object = value.as_object().cloned().unwrap_or_default();
    object.insert(
        "schema_version".to_owned(),
        JsonValue::Number(serde_json::Number::from(DOCTOR_ROUTINE_REGISTRY_VERSION)),
    );
    if !matches!(object.get(top_level_array_key), Some(JsonValue::Array(_))) {
        object.insert(top_level_array_key.to_owned(), JsonValue::Array(Vec::new()));
    }
    JsonValue::Object(object)
}

struct NormalizedNodeRuntime {
    value: JsonValue,
    changed: bool,
    expired_codes: usize,
    expired_requests: usize,
}

fn normalize_node_runtime_json(value: JsonValue, now_unix_ms: i64) -> NormalizedNodeRuntime {
    let mut changed = false;
    let mut expired_codes = 0;
    let mut expired_requests = 0;
    let mut object = value.as_object().cloned().unwrap_or_default();
    if !matches!(object.get("version").and_then(JsonValue::as_u64), Some(1)) {
        object.insert("version".to_owned(), JsonValue::Number(serde_json::Number::from(1)));
        changed = true;
    }
    if !matches!(object.get("active_pairing_codes"), Some(JsonValue::Object(_))) {
        object.insert("active_pairing_codes".to_owned(), JsonValue::Object(Default::default()));
        changed = true;
    }
    if let Some(codes) = object.get_mut("active_pairing_codes").and_then(JsonValue::as_object_mut) {
        let original_len = codes.len();
        codes.retain(|_, record| {
            record.get("expires_at_unix_ms").and_then(JsonValue::as_i64).unwrap_or(i64::MAX)
                > now_unix_ms
        });
        expired_codes = original_len.saturating_sub(codes.len());
        if expired_codes > 0 {
            changed = true;
        }
    }
    if !matches!(object.get("pairing_requests"), Some(JsonValue::Object(_))) {
        object.insert("pairing_requests".to_owned(), JsonValue::Object(Default::default()));
        changed = true;
    }
    if let Some(requests) = object.get_mut("pairing_requests").and_then(JsonValue::as_object_mut) {
        for request in requests.values_mut() {
            let expired = request
                .get("expires_at_unix_ms")
                .and_then(JsonValue::as_i64)
                .is_some_and(|value| value <= now_unix_ms);
            let state = request.get("state").and_then(JsonValue::as_str).unwrap_or_default();
            if expired && matches!(state, "pending_approval" | "approved") {
                request["state"] = JsonValue::String("expired".to_owned());
                if request.get("decision_reason").is_none() || request["decision_reason"].is_null()
                {
                    request["decision_reason"] =
                        JsonValue::String("pairing request expired".to_owned());
                }
                expired_requests += 1;
                changed = true;
            }
        }
    }
    if !matches!(object.get("nodes"), Some(JsonValue::Object(_))) {
        object.insert("nodes".to_owned(), JsonValue::Object(Default::default()));
        changed = true;
    }
    NormalizedNodeRuntime {
        value: JsonValue::Object(object),
        changed,
        expired_codes,
        expired_requests,
    }
}

struct NormalizedAccessRegistry {
    value: JsonValue,
    changed: bool,
    missing_feature_flags: Vec<String>,
}

fn normalize_access_registry_json(value: JsonValue, now_unix_ms: i64) -> NormalizedAccessRegistry {
    let mut changed = false;
    let mut object = value.as_object().cloned().unwrap_or_default();
    if !matches!(object.get("version").and_then(JsonValue::as_u64), Some(value) if value == DOCTOR_ACCESS_REGISTRY_VERSION as u64)
    {
        object.insert(
            "version".to_owned(),
            JsonValue::Number(serde_json::Number::from(DOCTOR_ACCESS_REGISTRY_VERSION)),
        );
        changed = true;
    }
    for key in
        ["api_tokens", "teams", "workspaces", "memberships", "invitations", "shares", "telemetry"]
    {
        if !matches!(object.get(key), Some(JsonValue::Array(_))) {
            object.insert(key.to_owned(), JsonValue::Array(Vec::new()));
            changed = true;
        }
    }
    let default_flags = default_access_feature_flags();
    let mut current_flags =
        object.get("feature_flags").and_then(JsonValue::as_array).cloned().unwrap_or_default();
    let existing_keys = current_flags
        .iter()
        .filter_map(|record| record.get("key").and_then(JsonValue::as_str))
        .map(ToOwned::to_owned)
        .collect::<BTreeSet<_>>();
    let mut missing_feature_flags = Vec::new();
    for default_flag in default_flags {
        let key =
            default_flag.get("key").and_then(JsonValue::as_str).unwrap_or_default().to_owned();
        if !existing_keys.contains(key.as_str()) {
            current_flags.push(default_flag);
            missing_feature_flags.push(key);
            changed = true;
        }
    }
    object.insert("feature_flags".to_owned(), JsonValue::Array(current_flags));
    object.insert(
        "last_backfill_at_unix_ms".to_owned(),
        JsonValue::Number(serde_json::Number::from(now_unix_ms)),
    );
    NormalizedAccessRegistry { value: JsonValue::Object(object), changed, missing_feature_flags }
}

fn default_access_registry_json() -> JsonValue {
    json!({
        "version": DOCTOR_ACCESS_REGISTRY_VERSION,
        "last_backfill_at_unix_ms": null,
        "feature_flags": default_access_feature_flags(),
        "api_tokens": [],
        "teams": [],
        "workspaces": [],
        "memberships": [],
        "invitations": [],
        "shares": [],
        "telemetry": [],
    })
}

fn default_access_feature_flags() -> Vec<JsonValue> {
    vec![
        json!({"key":"compat_api","label":"OpenAI-compatible API","description":"Expose the minimal OpenAI-compatible facade for models, chat completions, and responses.","enabled":false,"stage":"admin_only","depends_on":[],"updated_at_unix_ms":0,"updated_by_principal":"system"}),
        json!({"key":"api_tokens","label":"External API tokens","description":"Allow operators to issue scoped external API tokens with rotation and lifecycle control.","enabled":false,"stage":"admin_only","depends_on":["compat_api"],"updated_at_unix_ms":0,"updated_by_principal":"system"}),
        json!({"key":"team_mode","label":"Team and workspace mode","description":"Enable shared workspaces, invitations, and workspace-bound runtime principals.","enabled":false,"stage":"pilot","depends_on":[],"updated_at_unix_ms":0,"updated_by_principal":"system"}),
        json!({"key":"rbac","label":"RBAC and sharing","description":"Enforce role-based access checks and explicit workspace sharing metadata.","enabled":false,"stage":"pilot","depends_on":["team_mode"],"updated_at_unix_ms":0,"updated_by_principal":"system"}),
        json!({"key":"staged_rollout","label":"Staged rollout controls","description":"Expose rollout stages, kill switches, and privacy-aware feature telemetry.","enabled":false,"stage":"internal","depends_on":[],"updated_at_unix_ms":0,"updated_by_principal":"system"}),
    ]
}

struct NormalizedBrowserProfileRegistry {
    value: JsonValue,
    changed: bool,
    repaired_profile_records: usize,
    removed_active_pointers: usize,
}

fn normalize_browser_profile_registry_json(value: JsonValue) -> NormalizedBrowserProfileRegistry {
    let mut changed = false;
    let mut repaired_profile_records = 0;
    let mut removed_active_pointers = 0;
    let mut object = value.as_object().cloned().unwrap_or_default();
    if !matches!(object.get("v").and_then(JsonValue::as_u64), Some(value) if value == DOCTOR_BROWSER_PROFILE_REGISTRY_VERSION as u64)
    {
        object.insert(
            "v".to_owned(),
            JsonValue::Number(serde_json::Number::from(DOCTOR_BROWSER_PROFILE_REGISTRY_VERSION)),
        );
        changed = true;
    }
    if !matches!(object.get("profiles"), Some(JsonValue::Array(_))) {
        object.insert("profiles".to_owned(), JsonValue::Array(Vec::new()));
        changed = true;
    }
    let mut valid_profiles = BTreeMap::<String, String>::new();
    if let Some(profiles) = object.get_mut("profiles").and_then(JsonValue::as_array_mut) {
        for profile in profiles.iter_mut() {
            let Some(record) = profile.as_object_mut() else {
                continue;
            };
            let Some(profile_id) =
                record.get("profile_id").and_then(JsonValue::as_str).map(str::to_owned)
            else {
                continue;
            };
            let principal = record
                .get("principal")
                .and_then(JsonValue::as_str)
                .unwrap_or_default()
                .trim()
                .to_owned();
            if principal.is_empty() {
                continue;
            }
            if record.get("state_schema_version").and_then(JsonValue::as_u64).unwrap_or(0)
                < DOCTOR_BROWSER_PROFILE_RECORD_VERSION as u64
            {
                record.insert(
                    "state_schema_version".to_owned(),
                    JsonValue::Number(serde_json::Number::from(
                        DOCTOR_BROWSER_PROFILE_RECORD_VERSION,
                    )),
                );
                repaired_profile_records += 1;
                changed = true;
            }
            valid_profiles.insert(profile_id, principal);
        }
    }
    if !matches!(object.get("active_profile_by_principal"), Some(JsonValue::Object(_))) {
        object.insert(
            "active_profile_by_principal".to_owned(),
            JsonValue::Object(Default::default()),
        );
        changed = true;
    }
    if let Some(active) =
        object.get_mut("active_profile_by_principal").and_then(JsonValue::as_object_mut)
    {
        active.retain(|principal, profile_id| {
            let keep = profile_id
                .as_str()
                .and_then(|profile_id| valid_profiles.get(profile_id))
                .is_some_and(|record_principal| record_principal == principal);
            if !keep {
                removed_active_pointers += 1;
                changed = true;
            }
            keep
        });
    }
    NormalizedBrowserProfileRegistry {
        value: JsonValue::Object(object),
        changed,
        repaired_profile_records,
        removed_active_pointers,
    }
}

fn resolve_browser_state_key() -> Result<Option<[u8; DOCTOR_BROWSER_STATE_KEY_LEN]>> {
    let raw = match env::var(DOCTOR_BROWSER_STATE_KEY_ENV) {
        Ok(value) => value,
        Err(_) => return Ok(None),
    };
    let raw = raw.trim();
    if raw.is_empty() {
        return Ok(None);
    }
    let decoded = base64::engine::general_purpose::STANDARD
        .decode(raw)
        .context("failed to decode PALYRA_BROWSERD_STATE_ENCRYPTION_KEY as base64")?;
    if decoded.len() != DOCTOR_BROWSER_STATE_KEY_LEN {
        anyhow::bail!(
            "PALYRA_BROWSERD_STATE_ENCRYPTION_KEY must decode to exactly {} bytes",
            DOCTOR_BROWSER_STATE_KEY_LEN
        );
    }
    let mut key = [0_u8; DOCTOR_BROWSER_STATE_KEY_LEN];
    key.copy_from_slice(decoded.as_slice());
    Ok(Some(key))
}

fn resolve_browser_state_dir(state_root: &Path) -> Result<PathBuf> {
    if let Ok(raw) = env::var(DOCTOR_BROWSER_STATE_DIR_ENV) {
        let trimmed = raw.trim();
        if !trimmed.is_empty() {
            return Ok(PathBuf::from(trimmed));
        }
    }
    Ok(state_root.join(DOCTOR_BROWSER_STATE_DIR_RELATIVE_PATH))
}

fn encrypt_browser_state_blob(
    key: &[u8; DOCTOR_BROWSER_STATE_KEY_LEN],
    plaintext: &[u8],
) -> Result<Vec<u8>> {
    let unbound_key = UnboundKey::new(&CHACHA20_POLY1305, key)
        .map_err(|_| anyhow::anyhow!("failed to initialize browser state cipher"))?;
    let key = LessSafeKey::new(unbound_key);
    let mut nonce_bytes = [0_u8; DOCTOR_BROWSER_STATE_NONCE_LEN];
    SystemRandom::new()
        .fill(&mut nonce_bytes)
        .map_err(|_| anyhow::anyhow!("failed to generate browser state nonce"))?;
    let nonce = Nonce::assume_unique_for_key(nonce_bytes);
    let mut in_out = plaintext.to_vec();
    key.seal_in_place_append_tag(nonce, Aad::empty(), &mut in_out)
        .map_err(|_| anyhow::anyhow!("failed to encrypt browser state"))?;
    let mut output = Vec::with_capacity(
        DOCTOR_BROWSER_STATE_FILE_MAGIC.len() + DOCTOR_BROWSER_STATE_NONCE_LEN + in_out.len(),
    );
    output.extend_from_slice(DOCTOR_BROWSER_STATE_FILE_MAGIC);
    output.extend_from_slice(&nonce_bytes);
    output.extend_from_slice(in_out.as_slice());
    Ok(output)
}

fn decrypt_browser_state_blob(
    key: &[u8; DOCTOR_BROWSER_STATE_KEY_LEN],
    encrypted: &[u8],
) -> Result<Vec<u8>> {
    if encrypted.len() < DOCTOR_BROWSER_STATE_FILE_MAGIC.len() + DOCTOR_BROWSER_STATE_NONCE_LEN {
        anyhow::bail!("browser profile registry payload is too short");
    }
    if &encrypted[..DOCTOR_BROWSER_STATE_FILE_MAGIC.len()] != DOCTOR_BROWSER_STATE_FILE_MAGIC {
        anyhow::bail!("browser profile registry magic header is invalid");
    }
    let mut nonce_bytes = [0_u8; DOCTOR_BROWSER_STATE_NONCE_LEN];
    nonce_bytes.copy_from_slice(
        &encrypted[DOCTOR_BROWSER_STATE_FILE_MAGIC.len()
            ..DOCTOR_BROWSER_STATE_FILE_MAGIC.len() + DOCTOR_BROWSER_STATE_NONCE_LEN],
    );
    let mut in_out = encrypted
        [DOCTOR_BROWSER_STATE_FILE_MAGIC.len() + DOCTOR_BROWSER_STATE_NONCE_LEN..]
        .to_vec();
    let unbound_key = UnboundKey::new(&CHACHA20_POLY1305, key)
        .map_err(|_| anyhow::anyhow!("failed to initialize browser state cipher"))?;
    let key = LessSafeKey::new(unbound_key);
    let plaintext = key
        .open_in_place(Nonce::assume_unique_for_key(nonce_bytes), Aad::empty(), &mut in_out)
        .map_err(|_| anyhow::anyhow!("failed to decrypt browser profile registry"))?;
    Ok(plaintext.to_vec())
}

fn scan_stale_artifacts(root: &Path) -> Result<Vec<PathBuf>> {
    let mut stale = Vec::new();
    if !root.exists() || !root.is_dir() {
        return Ok(stale);
    }
    let entries = fs::read_dir(root)
        .with_context(|| format!("failed to enumerate runtime dir {}", root.display()))?;
    for entry in entries {
        let entry =
            entry.with_context(|| format!("failed to inspect entry in {}", root.display()))?;
        let path = entry.path();
        let metadata = entry
            .metadata()
            .with_context(|| format!("failed to stat runtime entry {}", path.display()))?;
        if metadata.is_dir() {
            stale.extend(scan_nested_stale_artifacts(path.as_path(), 1)?);
        } else if is_stale_artifact(path.as_path(), &metadata) {
            stale.push(path);
        }
    }
    Ok(stale)
}

fn scan_nested_stale_artifacts(root: &Path, depth: usize) -> Result<Vec<PathBuf>> {
    if depth > 2 || !root.is_dir() {
        return Ok(Vec::new());
    }
    let mut stale = Vec::new();
    let entries = match fs::read_dir(root) {
        Ok(entries) => entries,
        Err(_) => return Ok(stale),
    };
    for entry in entries {
        let entry =
            entry.with_context(|| format!("failed to inspect entry in {}", root.display()))?;
        let path = entry.path();
        let metadata = entry
            .metadata()
            .with_context(|| format!("failed to stat runtime entry {}", path.display()))?;
        if metadata.is_dir() {
            stale.extend(scan_nested_stale_artifacts(path.as_path(), depth + 1)?);
        } else if is_stale_artifact(path.as_path(), &metadata) {
            stale.push(path);
        }
    }
    Ok(stale)
}

fn is_stale_artifact(path: &Path, metadata: &fs::Metadata) -> bool {
    let Some(name) = path.file_name().and_then(|value| value.to_str()) else {
        return false;
    };
    let now = SystemTime::now();
    let modified = metadata.modified().ok();
    let age = modified.and_then(|value| now.duration_since(value).ok());
    if name.ends_with(".tmp") {
        return age.is_some_and(|value| value >= DOCTOR_STALE_TMP_MAX_AGE);
    }
    if name.ends_with(".lock")
        || name.ends_with(".pid")
        || name.ends_with(".sock")
        || name.ends_with(".socket")
    {
        return age.is_some_and(|value| value >= DOCTOR_STALE_RUNTIME_MAX_AGE);
    }
    false
}

fn collect_recovery_runs(state_root: &Path) -> Vec<DoctorRecoveryRunSummary> {
    let root = state_root.join(DOCTOR_RECOVERY_RUNS_RELATIVE_DIR);
    let entries = match fs::read_dir(root.as_path()) {
        Ok(entries) => entries,
        Err(_) => return Vec::new(),
    };
    let mut runs = Vec::new();
    for entry in entries.flatten() {
        let path = entry.path().join(DOCTOR_RECOVERY_MANIFEST_FILE_NAME);
        let Ok(raw) = fs::read_to_string(path.as_path()) else {
            continue;
        };
        let Ok(manifest) = serde_json::from_str::<DoctorRecoveryManifest>(raw.as_str()) else {
            continue;
        };
        runs.push(DoctorRecoveryRunSummary {
            rollback_command: format!("palyra doctor --rollback-run {}", manifest.run_id),
            run_id: manifest.run_id,
            created_at_unix_ms: manifest.created_at_unix_ms,
            manifest_path: display_path(path.as_path()),
            step_ids: manifest.steps,
            file_count: manifest.entries.len(),
            completed: manifest.completed,
        });
    }
    runs.sort_by(|left, right| right.created_at_unix_ms.cmp(&left.created_at_unix_ms));
    runs
}

fn collect_recovery_manifest_values(state_root: &Path, limit: usize) -> Vec<JsonValue> {
    collect_recovery_runs(state_root)
        .into_iter()
        .take(limit)
        .filter_map(|run| {
            let path = PathBuf::from(run.manifest_path.clone());
            let raw = fs::read_to_string(path.as_path()).ok()?;
            serde_json::from_str::<JsonValue>(raw.as_str()).ok()
        })
        .collect()
}

fn resolve_auth_registry_path(environment: &DoctorEnvironment) -> PathBuf {
    let configured = env::var(DOCTOR_AUTH_REGISTRY_PATH_ENV)
        .ok()
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
        .map(PathBuf::from);
    match configured {
        Some(path) if path.is_absolute() => path,
        Some(path) => environment.state_root.join(path),
        None => environment.state_root.join(DOCTOR_AUTH_REGISTRY_FILE_NAME),
    }
}

fn step_selected(id: &str, only: &[String], skip: &[String]) -> bool {
    let matches_filter = |candidate: &str, filter: &str| {
        candidate == filter || candidate.starts_with(filter) || candidate.contains(filter)
    };
    if !only.is_empty() && !only.iter().any(|filter| matches_filter(id, filter)) {
        return false;
    }
    !skip.iter().any(|filter| matches_filter(id, filter))
}

fn build_repair_next_steps_preview(planned_steps: &[DoctorRepairStep]) -> Vec<String> {
    if planned_steps.is_empty() {
        return vec!["No repair steps are currently required.".to_owned()];
    }
    vec![
        "palyra doctor --repair --dry-run --json".to_owned(),
        "palyra doctor --repair --json".to_owned(),
    ]
}

fn build_default_next_steps(report: &DoctorReport) -> Vec<String> {
    let mut next_steps = Vec::new();
    let blocking_checks = report
        .checks
        .iter()
        .filter(|check| check.severity == DoctorSeverity::Blocking && !check.ok)
        .count();
    if blocking_checks > 0 || !report.connectivity.http.ok || !report.connectivity.grpc.ok {
        next_steps.push("palyra health".to_owned());
        next_steps.push("palyra logs --lines 50".to_owned());
    }
    if report.access.backfill_required || report.access.blocking_issues > 0 {
        next_steps.push("palyra auth access backfill --dry-run".to_owned());
    }
    if report.browser.configured_enabled
        && (!report.browser.auth_token_configured
            || report.browser.error.is_some()
            || report.browser.health_status.as_deref().is_some_and(|value| value != "ok"))
    {
        next_steps.push("palyra browser status".to_owned());
    }
    next_steps.push("palyra doctor --repair --dry-run --json".to_owned());
    dedupe_strings(next_steps)
}

fn render_doctor_text(execution: &DoctorExecutionReport) {
    let checks = &execution.diagnostics.checks;
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
    for check in checks {
        println!(
            "doctor.check key={} ok={} required={} severity={}",
            check.key,
            check.ok,
            check.required,
            check.severity.as_str()
        );
    }
    println!(
        "doctor.summary blocking={} warnings={} info={} required_checks_failed={}",
        blocking_checks.len(),
        warning_checks.len(),
        info_checks.len(),
        execution.diagnostics.summary.required_checks_failed
    );
    if execution.recovery.requested {
        println!(
            "doctor.recovery requested=true mode={:?} dry_run={} force={}",
            execution.mode, execution.recovery.dry_run, execution.recovery.force
        );
        for step in execution.recovery.planned_steps.as_slice() {
            println!(
                "doctor.repair_step id={} severity={} apply_supported={} requires_force={} kind={}",
                step.id,
                step.severity.as_str(),
                step.apply_supported,
                step.requires_force,
                step.kind
            );
        }
        for step in execution.recovery.applied_steps.as_slice() {
            println!(
                "doctor.repair_result id={} outcome={} message={}",
                step.id, step.outcome, step.message
            );
        }
        if let Some(run_id) = execution.recovery.run_id.as_deref() {
            println!("doctor.recovery_run_id={run_id}");
        }
        if let Some(path) = execution.recovery.backup_manifest_path.as_deref() {
            println!("doctor.recovery_manifest={path}");
        }
    }
    for next_step in execution.recovery.next_steps.as_slice() {
        println!("doctor.next_step={next_step}");
    }
}

fn cli_profiles_quarantine_path(path: &Path) -> PathBuf {
    path.with_extension("unknown.toml")
}

fn auth_registry_quarantine_path(path: &Path) -> PathBuf {
    path.with_extension("unknown.toml")
}

fn quarantine_path(path: &Path) -> PathBuf {
    let file_name = path.file_name().and_then(|value| value.to_str()).unwrap_or("quarantine");
    path.with_file_name(format!("{file_name}.quarantine-{}", Ulid::new()))
}

fn empty_auth_registry_document() -> Result<String> {
    let mut table = toml::map::Map::new();
    table.insert("version".to_owned(), TomlValue::Integer(1));
    table.insert("profiles".to_owned(), TomlValue::Array(Vec::new()));
    toml::to_string_pretty(&TomlValue::Table(table))
        .context("failed to serialize empty auth registry document")
}

fn toml_json_to_string(value: JsonValue) -> Result<String> {
    let value = TomlValue::try_from(value).context("failed to convert JSON into TOML value")?;
    toml::to_string_pretty(&value).context("failed to serialize TOML document")
}

fn dedupe_strings(values: Vec<String>) -> Vec<String> {
    let mut seen = BTreeSet::new();
    values.into_iter().filter(|value| seen.insert(value.clone())).collect::<Vec<_>>()
}

fn display_path(path: &Path) -> String {
    path.to_string_lossy().into_owned()
}

fn sanitize_file_component(value: &str) -> String {
    value
        .chars()
        .map(|character| if character.is_ascii_alphanumeric() { character } else { '_' })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn normalize_node_runtime_adds_version_and_expires_entries() {
        let normalized = normalize_node_runtime_json(
            json!({
                "active_pairing_codes": {
                    "expired": { "expires_at_unix_ms": 10 },
                    "fresh": { "expires_at_unix_ms": 30 }
                },
                "pairing_requests": {
                    "request": { "expires_at_unix_ms": 10, "state": "approved" }
                }
            }),
            20,
        );
        assert!(normalized.changed);
        assert_eq!(normalized.expired_codes, 1);
        assert_eq!(normalized.expired_requests, 1);
        assert_eq!(normalized.value.get("version").and_then(JsonValue::as_u64), Some(1));
    }

    #[test]
    fn normalize_access_registry_restores_missing_feature_flags() {
        let normalized = normalize_access_registry_json(json!({ "version": 1 }), 123);
        assert!(normalized.changed);
        assert!(!normalized.missing_feature_flags.is_empty());
        assert_eq!(
            normalized.value.get("feature_flags").and_then(JsonValue::as_array).map(Vec::len),
            Some(default_access_feature_flags().len())
        );
    }

    #[test]
    fn browser_registry_normalization_drops_invalid_active_pointer() {
        let normalized = normalize_browser_profile_registry_json(json!({
            "profiles": [
                { "profile_id": "profile-1", "principal": "user:ops", "state_schema_version": 1 }
            ],
            "active_profile_by_principal": { "user:ops": "missing" }
        }));
        assert!(normalized.changed);
        assert_eq!(normalized.repaired_profile_records, 1);
        assert_eq!(normalized.removed_active_pointers, 1);
    }

    #[test]
    fn auth_registry_normalization_quarantines_unknown_root_keys() -> Result<()> {
        let temp = tempdir()?;
        let state_root = temp.path().join("state");
        fs::create_dir_all(&state_root)?;
        fs::write(
            state_root.join(DOCTOR_AUTH_REGISTRY_FILE_NAME),
            "profiles = []\nlegacy = \"value\"\n",
        )?;
        let environment =
            DoctorEnvironment { state_root, config_path: None, generated_at_unix_ms: 100 };
        let plans = evaluate_auth_registry_repairs(&environment)?;
        assert_eq!(plans.len(), 1);
        assert_eq!(plans[0].step.id, "auth_registry.schema_version");
        assert!(plans[0]
            .step
            .changed_objects
            .iter()
            .any(|value| value.ends_with("auth_profiles.unknown.toml")));
        Ok(())
    }

    #[test]
    fn gateway_remote_verification_stays_manual_for_ambiguous_pins() -> Result<()> {
        let temp = tempdir()?;
        let config_path = temp.path().join("palyra.toml");
        fs::write(
            &config_path,
            r#"
version = 1
[gateway_access]
remote_base_url = "https://dashboard.example.test/"
pinned_server_cert_fingerprint_sha256 = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
pinned_gateway_ca_fingerprint_sha256 = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
"#,
        )?;
        let plan = evaluate_gateway_remote_verification_repair(config_path.as_path())?
            .expect("gateway mismatch should create a manual review step");
        assert_eq!(plan.step.id, "gateway_access.remote_verification");
        assert!(!plan.step.apply_supported);
        assert!(plan.step.security_sensitive);
        Ok(())
    }

    #[test]
    fn repair_plan_matrix_covers_phase_one_broken_states() -> Result<()> {
        let temp = tempdir()?;
        let state_root = temp.path().join("state");
        fs::create_dir_all(&state_root)?;
        let config_path = temp.path().join("palyra.toml");
        fs::write(
            &config_path,
            r#"
version = 1
[tool_call.browser_service]
enabled = true

[gateway_access]
remote_base_url = "https://dashboard.example.test/"
"#,
        )?;
        fs::write(
            state_root.join(DOCTOR_NODE_RUNTIME_FILE_NAME),
            serde_json::to_string(&json!({
                "active_pairing_codes": {
                    "expired": { "expires_at_unix_ms": 10 }
                },
                "pairing_requests": {
                    "request": { "expires_at_unix_ms": 10, "state": "pending_approval" }
                }
            }))?,
        )?;
        fs::write(state_root.join(DOCTOR_ACCESS_REGISTRY_FILE_NAME), "{ invalid json")?;

        let environment = DoctorEnvironment {
            state_root,
            config_path: Some(config_path),
            generated_at_unix_ms: 20,
        };
        let plans = evaluate_repair_plans(&environment)?;
        let ids = plans
            .iter()
            .map(|plan| plan.step.id.as_str())
            .collect::<std::collections::BTreeSet<_>>();
        assert!(ids.contains("browser.auth_token.configure"));
        assert!(ids.contains("gateway_access.remote_verification"));
        assert!(ids.contains("node_runtime.normalize"));
        assert!(ids.contains("access_registry.reinitialize"));
        Ok(())
    }

    #[test]
    fn recovery_run_writer_captures_backup_manifest() -> Result<()> {
        let temp = tempdir()?;
        let state_root = temp.path().join("state");
        let target = state_root.join("sample.txt");
        fs::create_dir_all(&state_root)?;
        fs::write(&target, b"before")?;
        let environment =
            DoctorEnvironment { state_root, config_path: None, generated_at_unix_ms: 100 };
        let writer = DoctorRecoveryRunWriter::new(&environment, "01TESTRUN")?;
        writer.write_string("sample.step", target.as_path(), "after", false)?;
        writer.complete(
            &[DoctorAppliedStep {
                id: "sample.step".to_owned(),
                outcome: "ok".to_owned(),
                message: "updated".to_owned(),
                changed_objects: vec![display_path(target.as_path())],
                warnings: Vec::new(),
            }],
            &["palyra doctor --rollback-run 01TESTRUN".to_owned()],
        )?;
        let manifest = serde_json::from_str::<DoctorRecoveryManifest>(&fs::read_to_string(
            writer.manifest_path(),
        )?)?;
        assert!(manifest.completed);
        assert_eq!(manifest.entries.len(), 1);
        assert_eq!(manifest.applied_steps.len(), 1);
        assert_eq!(manifest.next_steps.len(), 1);
        let before_hash = sha256_hex(b"before");
        let after_hash = sha256_hex(b"after");
        assert_eq!(manifest.entries[0].before_sha256.as_deref(), Some(before_hash.as_str()));
        assert_eq!(manifest.entries[0].after_sha256.as_deref(), Some(after_hash.as_str()));
        Ok(())
    }
}
