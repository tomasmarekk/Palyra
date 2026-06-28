//! Workspace patch application with rollback and redacted previews.
//!
//! Patches are confined to resolved workspace roots and applied through the
//! shared `palyra-common` workspace-patch engine; failures report a
//! redacted preview and a validation exit code instead of partial writes.

use crate::*;
use serde::{Deserialize, Serialize};
use std::{
    path::Path,
    time::{SystemTime, UNIX_EPOCH},
};

/// Runs `palyra patch apply`, reading a patch from stdin and applying it
/// (or validating it in dry-run mode) inside the resolved workspace roots.
///
/// # Errors
/// Fails when stdin is missing or empty, workspace-root resolution fails,
/// or the patch is invalid; invalid patches exit with the validation code
/// after emitting the failure payload.
pub(crate) fn run_patch(command: PatchCommand) -> Result<()> {
    match command {
        PatchCommand::Apply { workspace_root, stdin, dry_run, json } => {
            let json = output::preferred_json(json);
            if !stdin {
                anyhow::bail!("patch apply requires --stdin");
            }
            let mut patch = String::new();
            std::io::stdin()
                .read_to_string(&mut patch)
                .context("failed to read patch from stdin")?;
            if patch.trim().is_empty() {
                anyhow::bail!("patch from stdin is empty");
            }

            let workspace_roots =
                resolve_patch_workspace_roots(workspace_root.as_deref()).with_context(|| {
                    "failed to resolve workspace root for patch apply; pass --workspace-root to select one explicitly"
                })?;
            let workspace_root_labels =
                workspace_roots.iter().map(|root| root.display().to_string()).collect::<Vec<_>>();
            let limits = WorkspacePatchLimits::default();
            let redaction_policy = WorkspacePatchRedactionPolicy::default();
            let request = WorkspacePatchRequest {
                patch: patch.clone(),
                dry_run,
                redaction_policy: redaction_policy.clone(),
            };

            match apply_workspace_patch(workspace_roots.as_slice(), &request, &limits) {
                Ok(outcome) => {
                    if json {
                        let payload = json!({
                            "workspace_roots": workspace_root_labels,
                            "dry_run": outcome.dry_run,
                            "files_touched": outcome.files_touched,
                            "patch_sha256": outcome.patch_sha256,
                            "rollback_performed": outcome.rollback_performed,
                            "redacted_preview": outcome.redacted_preview,
                        });
                        let rendered = serde_json::to_string_pretty(&payload)
                            .context("failed to serialize patch apply output")?;
                        println!("{rendered}");
                    } else {
                        println!(
                            "patch.apply success=true dry_run={} files_touched={} patch_sha256={} workspace_root={}",
                            outcome.dry_run,
                            outcome.files_touched.len(),
                            outcome.patch_sha256,
                            workspace_root_labels.first().map(String::as_str).unwrap_or("none")
                        );
                    }
                    std::io::stdout().flush().context("stdout flush failed")
                }
                Err(error) => {
                    let parse_error = error
                        .parse_location()
                        .map(|(line, column)| json!({ "line": line, "column": column }));
                    let payload = json!({
                        "success": false,
                        "error_kind": "validation_error",
                        "exit_code": output::CliExitCode::Validation as u8,
                        "patch_sha256": compute_patch_sha256(patch.as_str()),
                        "dry_run": dry_run,
                        "rollback_performed": error.rollback_performed(),
                        "workspace_roots": workspace_root_labels,
                        "redacted_preview": redact_patch_preview(
                            patch.as_str(),
                            &redaction_policy,
                            limits.max_preview_bytes,
                        ),
                        "parse_error": parse_error,
                        "error": error.to_string(),
                    });
                    if json {
                        let rendered = serde_json::to_string_pretty(&payload)
                            .context("failed to serialize patch apply failure output")?;
                        println!("{rendered}");
                        std::io::stdout().flush().context("stdout flush failed")?;
                        return Err(output::already_emitted_error(output::CliExitCode::Validation));
                    } else {
                        println!(
                            "patch.apply success=false dry_run={} rollback_performed={} error={}",
                            dry_run,
                            error.rollback_performed(),
                            error
                        );
                    }
                    std::io::stdout().flush().context("stdout flush failed")?;
                    anyhow::bail!("invalid patch: {error}");
                }
            }
        }
        PatchCommand::Bundles { command } => run_patch_bundle_command(command),
    }
}

fn run_patch_bundle_command(command: PatchBundleCommand) -> Result<()> {
    match command {
        PatchBundleCommand::List { store, json } => {
            let json = output::preferred_json(json);
            let store = resolve_patch_bundle_store(store.as_deref())?;
            let bundles = list_patch_bundles(store.as_path())?;
            if json {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&json!({
                        "store": store.display().to_string(),
                        "bundles": bundles,
                    }))
                    .context("failed to serialize patch bundle list")?
                );
            } else {
                println!("patch.bundles count={} store={}", bundles.len(), store.display());
                for bundle in bundles {
                    println!(
                        "{} status={} touched={} risk={} source_run={}",
                        bundle.bundle_id,
                        bundle.status,
                        bundle.touched_count,
                        bundle.risk,
                        bundle.source_run_id.as_deref().unwrap_or("unknown")
                    );
                }
            }
            std::io::stdout().flush().context("stdout flush failed")
        }
        PatchBundleCommand::Show { id, store, json } => {
            let json = output::preferred_json(json);
            let store = resolve_patch_bundle_store(store.as_deref())?;
            let bundle = load_patch_bundle_by_id(store.as_path(), id.as_str())?;
            emit_patch_bundle_detail(store.as_path(), &bundle, json)
        }
        PatchBundleCommand::Approve { id, store, json } => {
            let json = output::preferred_json(json);
            let store = resolve_patch_bundle_store(store.as_deref())?;
            let mut bundle = load_patch_bundle_by_id(store.as_path(), id.as_str())?;
            bundle.status = PatchBundleStatus::Approved;
            bundle.approval.approved = true;
            bundle.approval.approved_at_unix_ms = Some(current_unix_ms()?);
            write_patch_bundle(store.as_path(), &bundle)?;
            emit_patch_bundle_action(store.as_path(), &bundle, "approve", json)
        }
        PatchBundleCommand::Apply { id, workspace_root, store, dry_run, json } => {
            let json = output::preferred_json(json);
            let store = resolve_patch_bundle_store(store.as_deref())?;
            let mut bundle = load_patch_bundle_by_id(store.as_path(), id.as_str())?;
            apply_patch_bundle(
                store.as_path(),
                &mut bundle,
                workspace_root.as_deref(),
                dry_run,
                json,
            )
        }
        PatchBundleCommand::Discard { id, store, json } => {
            let json = output::preferred_json(json);
            let store = resolve_patch_bundle_store(store.as_deref())?;
            let mut bundle = load_patch_bundle_by_id(store.as_path(), id.as_str())?;
            bundle.status = PatchBundleStatus::Discarded;
            write_patch_bundle(store.as_path(), &bundle)?;
            emit_patch_bundle_action(store.as_path(), &bundle, "discard", json)
        }
    }
}

// Resolution order: explicit --workspace-root, then the configured process
// runner workspace, then <state_root>/workspace, then the current directory.
fn resolve_patch_workspace_roots(explicit_workspace_root: Option<&str>) -> Result<Vec<PathBuf>> {
    if let Some(explicit_workspace_root) =
        explicit_workspace_root.map(str::trim).filter(|value| !value.is_empty())
    {
        return Ok(vec![resolve_explicit_patch_workspace_root(explicit_workspace_root)?]);
    }

    if let Some(configured_workspace_root) = configured_process_runner_workspace_root()? {
        return Ok(vec![configured_workspace_root]);
    }

    if let Some(context) = app::current_root_context() {
        let workspace = context.state_root().join("workspace");
        if workspace.is_dir() {
            return Ok(vec![workspace]);
        }
    }

    let workspace_root =
        std::env::current_dir().context("failed to resolve current working directory")?;
    Ok(vec![workspace_root])
}

fn resolve_explicit_patch_workspace_root(raw: &str) -> Result<PathBuf> {
    if raw.contains('\0') {
        anyhow::bail!("workspace root cannot contain embedded NUL byte");
    }
    let path = PathBuf::from(raw);
    if path.is_absolute() {
        Ok(path)
    } else {
        Ok(std::env::current_dir()
            .context("failed to resolve current working directory")?
            .join(path))
    }
}

fn configured_process_runner_workspace_root() -> Result<Option<PathBuf>> {
    let Some(config_path) = effective_config_path() else {
        return Ok(None);
    };
    let config_path = PathBuf::from(config_path);
    let (document, _) =
        load_document_from_existing_path(config_path.as_path()).with_context(|| {
            format!(
                "failed to parse {} while resolving patch workspace root",
                config_path.display()
            )
        })?;
    let content =
        toml::to_string(&document).context("failed to serialize daemon config document")?;
    let parsed: RootFileConfig =
        toml::from_str(content.as_str()).context("invalid daemon config schema")?;
    let Some(raw_workspace_root) = parsed
        .tool_call
        .and_then(|tool_call| tool_call.process_runner)
        .and_then(|process_runner| process_runner.workspace_root)
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
    else {
        return Ok(None);
    };

    Ok(Some(resolve_config_relative_path(config_path.as_path(), raw_workspace_root.as_str())))
}

#[derive(Debug, Clone, Serialize)]
struct PatchBundleListEntry {
    bundle_id: String,
    status: PatchBundleStatus,
    touched_count: usize,
    risk: String,
    source_run_id: Option<String>,
    patch_sha256: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct WorkspacePatchBundle {
    #[serde(default = "patch_bundle_schema_version")]
    schema_version: u32,
    bundle_id: String,
    #[serde(default)]
    source_run_id: Option<String>,
    #[serde(default)]
    lease_id: Option<String>,
    patch: String,
    #[serde(default)]
    patch_sha256: Option<String>,
    #[serde(default)]
    touched_files: Vec<PatchBundleTouchedFile>,
    #[serde(default)]
    artifacts: Vec<PatchBundleArtifact>,
    #[serde(default)]
    cleanup_attestation: PatchBundleCleanupAttestation,
    #[serde(default)]
    risk: PatchBundleRisk,
    #[serde(default)]
    approval: PatchBundleApproval,
    #[serde(default)]
    status: PatchBundleStatus,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct PatchBundleTouchedFile {
    path: String,
    #[serde(default)]
    before_sha256: Option<String>,
    #[serde(default)]
    after_sha256: Option<String>,
    #[serde(default)]
    operation: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct PatchBundleArtifact {
    artifact_id: String,
    #[serde(default)]
    path: Option<String>,
    #[serde(default)]
    sha256: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct PatchBundleCleanupAttestation {
    #[serde(default)]
    removed_workspace_scope: bool,
    #[serde(default)]
    removed_artifacts: bool,
    #[serde(default)]
    removed_logs: bool,
    #[serde(default)]
    failure_reason: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct PatchBundleRisk {
    #[serde(default)]
    level: Option<String>,
    #[serde(default)]
    sensitive: bool,
    #[serde(default)]
    reasons: Vec<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct PatchBundleApproval {
    #[serde(default)]
    required: bool,
    #[serde(default)]
    approved: bool,
    #[serde(default)]
    approved_at_unix_ms: Option<i64>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum PatchBundleStatus {
    #[default]
    Pending,
    Approved,
    Applied,
    Discarded,
    Conflict,
}

impl std::fmt::Display for PatchBundleStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Pending => "pending",
            Self::Approved => "approved",
            Self::Applied => "applied",
            Self::Discarded => "discarded",
            Self::Conflict => "conflict",
        })
    }
}

fn patch_bundle_schema_version() -> u32 {
    1
}

fn resolve_patch_bundle_store(explicit_store: Option<&str>) -> Result<PathBuf> {
    if let Some(explicit_store) = explicit_store.map(str::trim).filter(|value| !value.is_empty()) {
        return resolve_store_path(explicit_store);
    }
    if let Some(context) = app::current_root_context() {
        return Ok(context.state_root().join("patch-bundles"));
    }
    Ok(std::env::current_dir()
        .context("failed to resolve current working directory")?
        .join(".palyra")
        .join("patch-bundles"))
}

fn resolve_store_path(raw: &str) -> Result<PathBuf> {
    if raw.contains('\0') {
        anyhow::bail!("patch bundle store cannot contain embedded NUL byte");
    }
    let path = PathBuf::from(raw);
    if path.is_absolute() {
        Ok(path)
    } else {
        Ok(std::env::current_dir()
            .context("failed to resolve current working directory")?
            .join(path))
    }
}

fn list_patch_bundles(store: &Path) -> Result<Vec<PatchBundleListEntry>> {
    if !store.exists() {
        return Ok(Vec::new());
    }
    let mut bundles = Vec::new();
    for entry in std::fs::read_dir(store)
        .with_context(|| format!("failed to read patch bundle store {}", store.display()))?
    {
        let entry = entry.context("failed to read patch bundle directory entry")?;
        let path = entry.path();
        if path.extension().and_then(|value| value.to_str()) != Some("json") {
            continue;
        }
        let bundle = load_patch_bundle_file(path.as_path())?;
        bundles.push(PatchBundleListEntry {
            bundle_id: bundle.bundle_id.clone(),
            status: bundle.status,
            touched_count: bundle.touched_files.len(),
            risk: bundle_risk_label(&bundle),
            source_run_id: bundle.source_run_id.clone(),
            patch_sha256: bundle_patch_sha256(&bundle),
        });
    }
    bundles.sort_by(|left, right| left.bundle_id.cmp(&right.bundle_id));
    Ok(bundles)
}

fn load_patch_bundle_by_id(store: &Path, id: &str) -> Result<WorkspacePatchBundle> {
    let id = normalize_patch_bundle_id(id)?;
    let path = patch_bundle_path(store, id.as_str());
    load_patch_bundle_file(path.as_path())
}

fn load_patch_bundle_file(path: &Path) -> Result<WorkspacePatchBundle> {
    let content = std::fs::read_to_string(path)
        .with_context(|| format!("failed to read patch bundle {}", path.display()))?;
    let bundle: WorkspacePatchBundle = serde_json::from_str(content.as_str())
        .with_context(|| format!("invalid patch bundle JSON in {}", path.display()))?;
    validate_patch_bundle(&bundle)?;
    Ok(bundle)
}

fn validate_patch_bundle(bundle: &WorkspacePatchBundle) -> Result<()> {
    normalize_patch_bundle_id(bundle.bundle_id.as_str())?;
    if bundle.patch.trim().is_empty() {
        anyhow::bail!("patch bundle '{}' has an empty patch", bundle.bundle_id);
    }
    if let Some(expected) = bundle.patch_sha256.as_deref() {
        let actual = compute_patch_sha256(bundle.patch.as_str());
        if expected != actual {
            anyhow::bail!(
                "patch bundle '{}' patch_sha256 mismatch: expected {} actual {}",
                bundle.bundle_id,
                expected,
                actual
            );
        }
    }
    for file in &bundle.touched_files {
        validate_bundle_relative_path(file.path.as_str())?;
    }
    Ok(())
}

fn write_patch_bundle(store: &Path, bundle: &WorkspacePatchBundle) -> Result<()> {
    std::fs::create_dir_all(store)
        .with_context(|| format!("failed to create patch bundle store {}", store.display()))?;
    let path = patch_bundle_path(store, bundle.bundle_id.as_str());
    let rendered =
        serde_json::to_string_pretty(bundle).context("failed to serialize patch bundle")?;
    std::fs::write(path.as_path(), rendered)
        .with_context(|| format!("failed to write patch bundle {}", path.display()))
}

fn patch_bundle_path(store: &Path, id: &str) -> PathBuf {
    store.join(format!("{id}.json"))
}

fn normalize_patch_bundle_id(raw: &str) -> Result<String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        anyhow::bail!("patch bundle id cannot be empty");
    }
    if trimmed.contains('\0')
        || trimmed.contains('/')
        || trimmed.contains('\\')
        || trimmed.contains("..")
    {
        anyhow::bail!("patch bundle id contains invalid path characters");
    }
    Ok(trimmed.to_owned())
}

fn emit_patch_bundle_detail(store: &Path, bundle: &WorkspacePatchBundle, json: bool) -> Result<()> {
    if json {
        println!(
            "{}",
            serde_json::to_string_pretty(&json!({
                "store": store.display().to_string(),
                "bundle": bundle,
                "summary": patch_bundle_summary(bundle),
            }))
            .context("failed to serialize patch bundle detail")?
        );
    } else {
        let summary = patch_bundle_summary(bundle);
        println!(
            "patch.bundle id={} status={} touched={} risk={} source_run={} patch_sha256={}",
            bundle.bundle_id,
            bundle.status,
            summary["touched_count"].as_u64().unwrap_or_default(),
            bundle_risk_label(bundle),
            bundle.source_run_id.as_deref().unwrap_or("unknown"),
            bundle_patch_sha256(bundle)
        );
    }
    std::io::stdout().flush().context("stdout flush failed")
}

fn emit_patch_bundle_action(
    store: &Path,
    bundle: &WorkspacePatchBundle,
    action: &str,
    json: bool,
) -> Result<()> {
    if json {
        println!(
            "{}",
            serde_json::to_string_pretty(&json!({
                "store": store.display().to_string(),
                "action": action,
                "bundle_id": bundle.bundle_id.as_str(),
                "status": bundle.status,
                "approval": &bundle.approval,
            }))
            .context("failed to serialize patch bundle action")?
        );
    } else {
        println!("patch.bundle action={} id={} status={}", action, bundle.bundle_id, bundle.status);
    }
    std::io::stdout().flush().context("stdout flush failed")
}

fn apply_patch_bundle(
    store: &Path,
    bundle: &mut WorkspacePatchBundle,
    workspace_root: Option<&str>,
    dry_run: bool,
    json: bool,
) -> Result<()> {
    if patch_bundle_requires_approval(bundle) && !bundle.approval.approved {
        let payload = json!({
            "success": false,
            "bundle_id": bundle.bundle_id.as_str(),
            "status": bundle.status,
            "approval_required": true,
            "risk": &bundle.risk,
            "error": "patch bundle requires approval before apply",
        });
        if json {
            println!(
                "{}",
                serde_json::to_string_pretty(&payload)
                    .context("failed to serialize approval-required payload")?
            );
            std::io::stdout().flush().context("stdout flush failed")?;
            return Err(output::already_emitted_error(output::CliExitCode::Validation));
        }
        anyhow::bail!("patch bundle '{}' requires approval before apply", bundle.bundle_id);
    }

    let workspace_roots = resolve_patch_workspace_roots(workspace_root)?;
    let conflicts = detect_patch_bundle_conflicts(bundle, workspace_roots.as_slice())?;
    if !conflicts.is_empty() {
        bundle.status = PatchBundleStatus::Conflict;
        write_patch_bundle(store, bundle)?;
        let payload = json!({
            "success": false,
            "bundle_id": bundle.bundle_id.as_str(),
            "status": bundle.status,
            "conflicts": conflicts,
            "error": "patch bundle conflicts with current workspace",
        });
        if json {
            println!(
                "{}",
                serde_json::to_string_pretty(&payload)
                    .context("failed to serialize patch bundle conflict payload")?
            );
            std::io::stdout().flush().context("stdout flush failed")?;
            return Err(output::already_emitted_error(output::CliExitCode::Validation));
        }
        anyhow::bail!("patch bundle '{}' conflicts with current workspace", bundle.bundle_id);
    }

    let limits = WorkspacePatchLimits::default();
    let request = WorkspacePatchRequest {
        patch: bundle.patch.clone(),
        dry_run,
        redaction_policy: WorkspacePatchRedactionPolicy::default(),
    };
    let outcome = apply_workspace_patch(workspace_roots.as_slice(), &request, &limits)
        .with_context(|| format!("failed to apply patch bundle '{}'", bundle.bundle_id))?;
    if !dry_run {
        bundle.status = PatchBundleStatus::Applied;
        write_patch_bundle(store, bundle)?;
    }
    if json {
        println!(
            "{}",
            serde_json::to_string_pretty(&json!({
                "success": true,
                "bundle_id": bundle.bundle_id.as_str(),
                "dry_run": dry_run,
                "status": bundle.status,
                "files_touched": outcome.files_touched,
                "patch_sha256": outcome.patch_sha256,
                "rollback_performed": outcome.rollback_performed,
            }))
            .context("failed to serialize patch bundle apply output")?
        );
    } else {
        println!(
            "patch.bundle.apply success=true id={} dry_run={} files_touched={} status={}",
            bundle.bundle_id,
            dry_run,
            outcome.files_touched.len(),
            bundle.status
        );
    }
    std::io::stdout().flush().context("stdout flush failed")
}

fn patch_bundle_summary(bundle: &WorkspacePatchBundle) -> Value {
    json!({
        "bundle_id": bundle.bundle_id.as_str(),
        "status": bundle.status,
        "risk": bundle_risk_label(bundle),
        "source_run_id": bundle.source_run_id.as_deref(),
        "lease_id": bundle.lease_id.as_deref(),
        "patch_sha256": bundle_patch_sha256(bundle),
        "touched_count": bundle.touched_files.len(),
        "touched_files": &bundle.touched_files,
        "artifact_count": bundle.artifacts.len(),
        "cleanup_attestation": &bundle.cleanup_attestation,
        "approval_required": patch_bundle_requires_approval(bundle),
        "approved": bundle.approval.approved,
    })
}

fn patch_bundle_requires_approval(bundle: &WorkspacePatchBundle) -> bool {
    bundle.approval.required
        || bundle.risk.sensitive
        || bundle
            .risk
            .level
            .as_deref()
            .is_some_and(|level| matches!(level, "high" | "sensitive" | "critical"))
}

fn bundle_risk_label(bundle: &WorkspacePatchBundle) -> String {
    if bundle.risk.sensitive {
        return "sensitive".to_owned();
    }
    bundle.risk.level.clone().unwrap_or_else(|| "unknown".to_owned())
}

fn bundle_patch_sha256(bundle: &WorkspacePatchBundle) -> String {
    bundle.patch_sha256.clone().unwrap_or_else(|| compute_patch_sha256(bundle.patch.as_str()))
}

fn detect_patch_bundle_conflicts(
    bundle: &WorkspacePatchBundle,
    workspace_roots: &[PathBuf],
) -> Result<Vec<Value>> {
    let mut conflicts = Vec::new();
    for file in &bundle.touched_files {
        let Some(expected_sha) = file.before_sha256.as_deref() else {
            continue;
        };
        let path = resolve_touched_file_path(workspace_roots, file.path.as_str())?;
        let actual_sha =
            if path.is_file() { Some(compute_file_sha256(path.as_path())?) } else { None };
        if actual_sha.as_deref() != Some(expected_sha) {
            conflicts.push(json!({
                "path": file.path,
                "expected_before_sha256": expected_sha,
                "actual_sha256": actual_sha,
                "state": if path.exists() { "changed" } else { "missing" },
            }));
        }
    }
    Ok(conflicts)
}

fn resolve_touched_file_path(workspace_roots: &[PathBuf], relative_path: &str) -> Result<PathBuf> {
    validate_bundle_relative_path(relative_path)?;
    let root = workspace_roots
        .first()
        .ok_or_else(|| anyhow::anyhow!("workspace roots cannot be empty"))?;
    Ok(root.join(relative_path))
}

fn validate_bundle_relative_path(path: &str) -> Result<()> {
    let raw = path.trim();
    if raw.is_empty() || raw.contains('\0') {
        anyhow::bail!("patch bundle touched file path cannot be empty or contain NUL");
    }
    let path = PathBuf::from(raw);
    if path.is_absolute()
        || path.components().any(|component| {
            !matches!(component, std::path::Component::Normal(_) | std::path::Component::CurDir)
        })
    {
        anyhow::bail!("patch bundle touched file path must be workspace-relative");
    }
    Ok(())
}

fn compute_file_sha256(path: &Path) -> Result<String> {
    let bytes =
        std::fs::read(path).with_context(|| format!("failed to read {}", path.display()))?;
    Ok(crate::sha256_hex(bytes.as_slice()))
}

fn current_unix_ms() -> Result<i64> {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system clock is before unix epoch")?;
    Ok(i64::try_from(duration.as_millis()).unwrap_or(i64::MAX))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_bundle() -> WorkspacePatchBundle {
        WorkspacePatchBundle {
            schema_version: 1,
            bundle_id: "bundle-01".to_owned(),
            source_run_id: Some("run-01".to_owned()),
            lease_id: Some("lease-01".to_owned()),
            patch: "diff --git a/app.rs b/app.rs\n".to_owned(),
            patch_sha256: None,
            touched_files: Vec::new(),
            artifacts: Vec::new(),
            cleanup_attestation: PatchBundleCleanupAttestation {
                removed_workspace_scope: true,
                removed_artifacts: true,
                removed_logs: true,
                failure_reason: None,
            },
            risk: PatchBundleRisk::default(),
            approval: PatchBundleApproval::default(),
            status: PatchBundleStatus::Pending,
        }
    }

    #[test]
    fn patch_bundle_conflict_detection_rejects_changed_workspace_file() {
        let tempdir = tempfile::tempdir().expect("tempdir should be created");
        let root = tempdir.path();
        std::fs::write(root.join("app.rs"), "changed").expect("workspace file should be written");

        let mut bundle = test_bundle();
        bundle.touched_files.push(PatchBundleTouchedFile {
            path: "app.rs".to_owned(),
            before_sha256: Some(crate::sha256_hex(b"original")),
            after_sha256: None,
            operation: Some("modify".to_owned()),
        });

        let conflicts = detect_patch_bundle_conflicts(&bundle, &[root.to_path_buf()])
            .expect("conflict detection should complete");

        assert_eq!(conflicts.len(), 1);
        assert_eq!(conflicts[0]["path"].as_str(), Some("app.rs"));
        assert_eq!(conflicts[0]["state"].as_str(), Some("changed"));
        assert_eq!(
            conflicts[0]["expected_before_sha256"].as_str(),
            Some(crate::sha256_hex(b"original").as_str())
        );
    }

    #[test]
    fn sensitive_patch_bundle_requires_approval_before_apply() {
        let tempdir = tempfile::tempdir().expect("tempdir should be created");
        let store = tempdir.path().join("store");
        let root = tempdir.path().join("workspace");
        std::fs::create_dir_all(root.as_path()).expect("workspace root should be created");
        let mut bundle = test_bundle();
        bundle.risk.sensitive = true;

        let root_string = root.display().to_string();
        let error = apply_patch_bundle(
            store.as_path(),
            &mut bundle,
            Some(root_string.as_str()),
            true,
            false,
        )
        .expect_err("sensitive bundle should require approval");

        assert!(error.to_string().contains("requires approval before apply"));
        assert_eq!(bundle.status, PatchBundleStatus::Pending);
    }
}
