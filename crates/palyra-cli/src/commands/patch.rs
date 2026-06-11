//! Workspace patch application with rollback and redacted previews.
//!
//! Patches are confined to resolved workspace roots and applied through the
//! shared `palyra-common` workspace-patch engine; failures report a
//! redacted preview and a validation exit code instead of partial writes.

use crate::*;

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
