use std::{
    fs,
    path::{Path, PathBuf},
    process::{Command, Stdio},
};

use anyhow::{Context, Result};

use crate::app::{self, ConnectionDefaults, ConnectionOverrides};

use super::{
    handoff::{build_console_handoff_path, TuiCrossSurfaceHandoff},
    percent_encode_component, read_json_string, sanitize_terminal_text, shorten_id, truncate_text,
    App, EntryKind,
};

#[derive(Debug, Clone, PartialEq, Eq)]
enum WorkspaceCommandAction {
    Summary,
    Changed,
    Show { artifact_ref: String },
    Open { artifact_ref: String },
    Handoff { open_browser: bool },
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct WorkspaceCommandRequest {
    action: WorkspaceCommandAction,
    run_id: Option<String>,
}

pub(super) async fn handle_workspace_command(app: &mut App, arguments: Vec<String>) -> Result<()> {
    let request = parse_workspace_command(arguments.as_slice())?;
    let run_id = resolve_workspace_run_id(app, request.run_id.clone())
        .context("/workspace requires a previous run or an explicit run id")?;
    match request.action {
        WorkspaceCommandAction::Summary => {
            show_workspace_summary(app, run_id.as_str(), false).await
        }
        WorkspaceCommandAction::Changed => show_workspace_summary(app, run_id.as_str(), true).await,
        WorkspaceCommandAction::Show { artifact_ref } => {
            show_workspace_artifact(app, run_id.as_str(), artifact_ref.as_str()).await
        }
        WorkspaceCommandAction::Open { artifact_ref } => {
            open_workspace_artifact(app, run_id.as_str(), artifact_ref.as_str()).await
        }
        WorkspaceCommandAction::Handoff { open_browser } => {
            workspace_handoff(app, run_id.as_str(), open_browser).await
        }
    }
}

fn parse_workspace_command(arguments: &[String]) -> Result<WorkspaceCommandRequest> {
    let mut run_id = None;
    let mut open_browser = false;
    let mut positionals = Vec::new();
    let mut index = 0usize;
    while index < arguments.len() {
        match arguments[index].as_str() {
            "--run" => {
                let Some(value) = arguments.get(index + 1) else {
                    anyhow::bail!(
                        "Usage: /workspace [list|changed|show|open|handoff] [--run <run_id>]"
                    );
                };
                run_id = Some(value.clone());
                index += 2;
            }
            "--open" => {
                open_browser = true;
                index += 1;
            }
            value => {
                positionals.push(value.to_owned());
                index += 1;
            }
        }
    }

    let Some(first) = positionals.first().map(|value| value.to_ascii_lowercase()) else {
        return Ok(WorkspaceCommandRequest { action: WorkspaceCommandAction::Summary, run_id });
    };

    let action = match first.as_str() {
        "list" | "ls" | "summary" => WorkspaceCommandAction::Summary,
        "changed" => WorkspaceCommandAction::Changed,
        "show" => WorkspaceCommandAction::Show {
            artifact_ref: positionals
                .get(1)
                .cloned()
                .context("Usage: /workspace show <index|artifact-id> [--run <run_id>]")?,
        },
        "open" => WorkspaceCommandAction::Open {
            artifact_ref: positionals
                .get(1)
                .cloned()
                .context("Usage: /workspace open <index|artifact-id> [--run <run_id>]")?,
        },
        "handoff" => WorkspaceCommandAction::Handoff {
            open_browser: open_browser
                || positionals
                    .get(1)
                    .map(|value| value.eq_ignore_ascii_case("open"))
                    .unwrap_or(false),
        },
        _ => {
            if run_id.is_none() && positionals.len() == 1 {
                run_id = Some(positionals[0].clone());
                WorkspaceCommandAction::Summary
            } else {
                anyhow::bail!(
                    "Usage: /workspace [run-id] | /workspace changed | /workspace show <index|artifact-id> | /workspace open <index|artifact-id> | /workspace handoff [open]"
                );
            }
        }
    };

    Ok(WorkspaceCommandRequest { action, run_id })
}

fn resolve_workspace_run_id(app: &App, explicit: Option<String>) -> Option<String> {
    explicit
        .and_then(|value| (!value.trim().is_empty()).then_some(value))
        .or_else(|| app.last_run_id.clone())
        .or_else(|| {
            app.current_session_catalog.as_ref().and_then(|session| session.last_run_id.clone())
        })
        .or_else(|| app.session.last_run_id.as_ref().map(|value| value.ulid.clone()))
}

async fn show_workspace_summary(app: &mut App, run_id: &str, changed_only: bool) -> Result<()> {
    let context = app.connect_admin_console().await?;
    let payload = context
        .client
        .get_json_value(format!(
            "console/v1/chat/runs/{}/workspace?limit=24",
            percent_encode_component(run_id)
        ))
        .await?;
    let artifacts = payload
        .pointer("/workspace/artifacts")
        .and_then(serde_json::Value::as_array)
        .cloned()
        .unwrap_or_default();
    let checkpoints = payload
        .pointer("/workspace/workspace_checkpoints")
        .and_then(serde_json::Value::as_array)
        .cloned()
        .unwrap_or_default();
    let background_tasks = payload
        .pointer("/workspace/background_tasks")
        .and_then(serde_json::Value::as_array)
        .map(Vec::len)
        .unwrap_or_default();

    let mut lines = vec![format!(
        "Run {} exposes {} artifact(s), {} workspace checkpoint(s), and {} background task(s).",
        shorten_id(run_id),
        artifacts.len(),
        checkpoints.len(),
        background_tasks
    )];
    if artifacts.is_empty() {
        lines.push("No workspace artifacts were published for this run.".to_owned());
    } else {
        lines.push(if changed_only {
            "Changed workspace paths:".to_owned()
        } else {
            "Workspace artifacts:".to_owned()
        });
        lines.extend(artifacts.iter().take(10).enumerate().map(|(index, artifact)| {
            let deleted =
                artifact.pointer("/deleted").and_then(serde_json::Value::as_bool).unwrap_or(false);
            format!(
                "{}. {} · {}{}",
                index + 1,
                artifact
                    .pointer("/display_path")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("unknown"),
                artifact
                    .pointer("/change_kind")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("changed"),
                if deleted { " · deleted" } else { "" }
            )
        }));
        if artifacts.len() > 10 {
            lines.push(format!("… and {} more artifact(s).", artifacts.len() - 10));
        }
    }

    if !changed_only {
        if checkpoints.is_empty() {
            lines.push("Rollback checkpoints: none".to_owned());
        } else {
            lines.push("Rollback checkpoints:".to_owned());
            lines.extend(checkpoints.iter().take(6).map(|checkpoint| {
                let stage = checkpoint
                    .pointer("/checkpoint_stage")
                    .and_then(serde_json::Value::as_str)
                    .map(workspace_checkpoint_stage_label)
                    .unwrap_or("checkpoint");
                let risk = checkpoint
                    .pointer("/risk_level")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("risk unknown");
                let pair = checkpoint
                    .pointer("/paired_checkpoint_id")
                    .and_then(serde_json::Value::as_str)
                    .map(shorten_id)
                    .unwrap_or_else(|| "unpaired".to_owned());
                format!(
                    "  {} · {} · {} · {} · paired {} · restores {}",
                    checkpoint
                        .pointer("/checkpoint_id")
                        .and_then(serde_json::Value::as_str)
                        .map(shorten_id)
                        .unwrap_or_else(|| "unknown".to_owned()),
                    checkpoint
                        .pointer("/source_label")
                        .and_then(serde_json::Value::as_str)
                        .unwrap_or("workspace checkpoint"),
                    stage,
                    risk,
                    pair,
                    checkpoint
                        .pointer("/restore_count")
                        .and_then(serde_json::Value::as_u64)
                        .unwrap_or_default()
                )
            }));
        }
        lines.push(
            "Use `/workspace show <index|artifact-id>` for preview, `/workspace open <index|artifact-id>` to open locally, `/workspace handoff` for the web inspector, and `/rollback diff <checkpoint-id>` before restore.".to_owned(),
        );
    }

    app.push_entry(
        EntryKind::System,
        if changed_only { "Workspace changes" } else { "Workspace" },
        lines.join("\n"),
    );
    app.status_line = if changed_only {
        "Workspace changed-file view loaded".to_owned()
    } else {
        "Workspace summary loaded".to_owned()
    };
    Ok(())
}

fn workspace_checkpoint_stage_label(stage: &str) -> &'static str {
    match stage {
        "preflight" => "preflight",
        "post_change" => "post-change",
        _ => "checkpoint",
    }
}

async fn show_workspace_artifact(app: &mut App, run_id: &str, artifact_ref: &str) -> Result<()> {
    let artifact_id = resolve_workspace_artifact_id(app, run_id, artifact_ref).await?;
    let context = app.connect_admin_console().await?;
    let payload = context
        .client
        .get_json_value(format!(
            "console/v1/chat/runs/{}/workspace/artifacts/{}?include_content=true",
            percent_encode_component(run_id),
            percent_encode_component(artifact_id.as_str())
        ))
        .await?;
    let artifact = payload
        .pointer("/detail/artifact")
        .cloned()
        .context("workspace artifact payload is missing /detail/artifact")?;
    let checkpoint_id = read_json_string(&payload, "/detail/checkpoint/checkpoint_id");
    let content_available = payload
        .pointer("/detail/content_available")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false);
    let content_text = payload
        .pointer("/detail/text_content")
        .and_then(serde_json::Value::as_str)
        .map(sanitize_terminal_text);
    let preview_text = artifact
        .pointer("/preview_text")
        .and_then(serde_json::Value::as_str)
        .map(sanitize_terminal_text);
    let mut lines = vec![
        format!(
            "{} · {} · {}",
            artifact
                .pointer("/display_path")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("unknown"),
            artifact
                .pointer("/change_kind")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("changed"),
            artifact
                .pointer("/content_type")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("application/octet-stream")
        ),
        format!(
            "artifact={} · checkpoint={} · preview={} · versions={} · deleted={}",
            shorten_id(artifact_id.as_str()),
            if checkpoint_id.is_empty() {
                "none".to_owned()
            } else {
                shorten_id(checkpoint_id.as_str())
            },
            artifact
                .pointer("/preview_kind")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("metadata"),
            artifact
                .pointer("/version_count")
                .and_then(serde_json::Value::as_u64)
                .unwrap_or_default(),
            artifact.pointer("/deleted").and_then(serde_json::Value::as_bool).unwrap_or(false),
        ),
    ];
    if let Some(moved_from) =
        artifact.pointer("/moved_from_path").and_then(serde_json::Value::as_str)
    {
        if !moved_from.trim().is_empty() {
            lines.push(format!("Moved from: {moved_from}"));
        }
    }
    lines.push(if content_available {
        "Inline preview:".to_owned()
    } else {
        "Inline preview unavailable; using bounded artifact preview:".to_owned()
    });
    if let Some(text) = content_text.or(preview_text) {
        lines.push(truncate_workspace_preview(text.as_str()));
    } else if artifact.pointer("/deleted").and_then(serde_json::Value::as_bool).unwrap_or(false) {
        lines.push(
            "This artifact records a deletion, so there is no current file payload to preview."
                .to_owned(),
        );
    } else {
        lines.push("No inline content is available for this artifact.".to_owned());
    }
    lines.push(format!(
        "Open locally with `/workspace open {}` or preview the rollback surface with `/rollback diff {}`.",
        artifact_id,
        if checkpoint_id.is_empty() { run_id.to_owned() } else { checkpoint_id.clone() }
    ));
    app.push_entry(EntryKind::System, "Workspace artifact", lines.join("\n"));
    app.status_line = "Workspace artifact detail loaded".to_owned();
    Ok(())
}

async fn open_workspace_artifact(app: &mut App, run_id: &str, artifact_ref: &str) -> Result<()> {
    let artifact = resolve_workspace_artifact(app, run_id, artifact_ref).await?;
    if artifact.pointer("/deleted").and_then(serde_json::Value::as_bool).unwrap_or(false) {
        app.push_entry(
            EntryKind::System,
            "Workspace artifact",
            "The selected artifact represents a deleted path, so there is no local file to open.",
        );
        app.status_line = "Deleted workspace artifact cannot be opened locally".to_owned();
        return Ok(());
    }
    let resolved = resolve_workspace_artifact_path(app, &artifact)?;
    open_path_in_default_app(resolved.as_path())?;
    app.push_entry(
        EntryKind::System,
        "Workspace artifact",
        format!("Opened {} in the platform default application.", resolved.display()),
    );
    app.status_line = "Workspace artifact opened externally".to_owned();
    Ok(())
}

async fn workspace_handoff(app: &mut App, run_id: &str, open_browser: bool) -> Result<()> {
    let handoff_path = build_console_handoff_path(&TuiCrossSurfaceHandoff {
        section: "chat".to_owned(),
        session_id: app.active_session_id().ok(),
        run_id: Some(run_id.to_owned()),
        objective_id: app.selected_objective_id.clone(),
        intent: Some("inspect_workspace".to_owned()),
        source: Some("tui".to_owned()),
        ..TuiCrossSurfaceHandoff::default()
    });
    let absolute_url = build_console_handoff_url(handoff_path.as_str())?;
    if open_browser {
        webbrowser::open(absolute_url.as_str())
            .context("failed to open workspace handoff in the default browser")?;
        app.push_entry(EntryKind::System, "Workspace handoff", format!("Opened {absolute_url}"));
        app.status_line = "Workspace handoff opened in browser".to_owned();
        return Ok(());
    }
    app.push_entry(
        EntryKind::System,
        "Workspace handoff",
        format!(
            "Fragment: {handoff_path}\nBrowser URL: {absolute_url}\nUse `/workspace handoff open` to open it directly."
        ),
    );
    app.status_line = "Workspace handoff path ready".to_owned();
    Ok(())
}

async fn resolve_workspace_artifact(
    app: &App,
    run_id: &str,
    artifact_ref: &str,
) -> Result<serde_json::Value> {
    let context = app.connect_admin_console().await?;
    let payload = context
        .client
        .get_json_value(format!(
            "console/v1/chat/runs/{}/workspace?limit=24",
            percent_encode_component(run_id)
        ))
        .await?;
    let artifacts = payload
        .pointer("/workspace/artifacts")
        .and_then(serde_json::Value::as_array)
        .cloned()
        .unwrap_or_default();
    resolve_workspace_artifact_value(artifacts.as_slice(), artifact_ref).with_context(|| {
        format!("workspace artifact `{artifact_ref}` was not found for run {}", shorten_id(run_id))
    })
}

async fn resolve_workspace_artifact_id(
    app: &App,
    run_id: &str,
    artifact_ref: &str,
) -> Result<String> {
    let artifact = resolve_workspace_artifact(app, run_id, artifact_ref).await?;
    Ok(read_json_string(&artifact, "/artifact_id"))
}

fn resolve_workspace_artifact_value(
    artifacts: &[serde_json::Value],
    artifact_ref: &str,
) -> Option<serde_json::Value> {
    let trimmed = artifact_ref.trim();
    if trimmed.is_empty() {
        return None;
    }
    if let Ok(index) = trimmed.parse::<usize>() {
        return index.checked_sub(1).and_then(|offset| artifacts.get(offset).cloned());
    }
    let normalized = trimmed.to_ascii_lowercase();
    artifacts
        .iter()
        .find(|artifact| {
            artifact
                .pointer("/artifact_id")
                .and_then(serde_json::Value::as_str)
                .map(|value| value.eq_ignore_ascii_case(trimmed))
                .unwrap_or(false)
                || artifact
                    .pointer("/display_path")
                    .and_then(serde_json::Value::as_str)
                    .map(|value| value.to_ascii_lowercase() == normalized)
                    .unwrap_or(false)
                || artifact
                    .pointer("/path")
                    .and_then(serde_json::Value::as_str)
                    .map(|value| value.to_ascii_lowercase() == normalized)
                    .unwrap_or(false)
        })
        .cloned()
}

fn resolve_workspace_artifact_path(app: &App, artifact: &serde_json::Value) -> Result<PathBuf> {
    let workspace_root_index = artifact
        .pointer("/workspace_root_index")
        .and_then(serde_json::Value::as_u64)
        .context("workspace artifact is missing workspace_root_index")?
        as usize;
    let relative_path = artifact
        .pointer("/path")
        .and_then(serde_json::Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .context("workspace artifact is missing path")?;
    let workspace_root = app
        .current_agent
        .as_ref()
        .and_then(|agent| agent.workspace_roots.get(workspace_root_index))
        .cloned()
        .context(
            "current agent workspace roots are unavailable; use `/workspace handoff` instead",
        )?;
    let canonical_root = fs::canonicalize(Path::new(workspace_root.as_str()))
        .with_context(|| format!("failed to resolve workspace root {}", workspace_root))?;
    let candidate = canonical_root.join(relative_path);
    let canonical_candidate = fs::canonicalize(candidate.as_path()).with_context(|| {
        format!("workspace path {} does not exist locally", candidate.display())
    })?;
    if !canonical_candidate.starts_with(canonical_root.as_path()) {
        anyhow::bail!(
            "workspace artifact path escaped the configured workspace root: {}",
            canonical_candidate.display()
        );
    }
    Ok(canonical_candidate)
}

fn open_path_in_default_app(path: &Path) -> Result<()> {
    #[cfg(target_os = "windows")]
    let mut commands = vec![{
        let mut command = Command::new("explorer");
        command.arg(path.as_os_str());
        command
    }];
    #[cfg(target_os = "macos")]
    let mut commands = vec![{
        let mut command = Command::new("open");
        command.arg(path.as_os_str());
        command
    }];
    #[cfg(all(unix, not(target_os = "macos")))]
    let mut commands = vec![{
        let mut command = Command::new("xdg-open");
        command.arg(path.as_os_str());
        command
    }];

    let mut failures = Vec::new();
    for command in &mut commands {
        match command.stdin(Stdio::null()).stdout(Stdio::null()).stderr(Stdio::null()).status() {
            Ok(status) if status.success() => return Ok(()),
            Ok(status) => failures.push(format!("launcher exited with {}", status)),
            Err(error) => failures.push(error.to_string()),
        }
    }
    anyhow::bail!("failed to open workspace path {}: {}", path.display(), failures.join("; "))
}

fn build_console_handoff_url(path: &str) -> Result<String> {
    let root_context = app::current_root_context().context("CLI root context is unavailable")?;
    let http = root_context
        .resolve_http_connection(ConnectionOverrides::default(), ConnectionDefaults::ADMIN)?;
    Ok(format!("{}{}", http.base_url.trim_end_matches('/'), path))
}

fn truncate_workspace_preview(value: &str) -> String {
    let bounded = value
        .lines()
        .take(28)
        .map(|line| truncate_text(line.to_owned(), 160))
        .collect::<Vec<_>>()
        .join("\n");
    if bounded.trim().is_empty() {
        "(preview empty)".to_owned()
    } else {
        bounded
    }
}

#[cfg(test)]
mod tests {
    use super::{parse_workspace_command, WorkspaceCommandAction, WorkspaceCommandRequest};

    #[test]
    fn workspace_parser_accepts_run_only_summary() {
        assert_eq!(
            parse_workspace_command(&["01ARZ3NDEKTSV4RRFFQ69G5FAV".to_owned()]).unwrap(),
            WorkspaceCommandRequest {
                action: WorkspaceCommandAction::Summary,
                run_id: Some("01ARZ3NDEKTSV4RRFFQ69G5FAV".to_owned()),
            }
        );
    }

    #[test]
    fn workspace_parser_accepts_show_with_explicit_run() {
        assert_eq!(
            parse_workspace_command(&[
                "show".to_owned(),
                "2".to_owned(),
                "--run".to_owned(),
                "run-2".to_owned(),
            ])
            .unwrap(),
            WorkspaceCommandRequest {
                action: WorkspaceCommandAction::Show { artifact_ref: "2".to_owned() },
                run_id: Some("run-2".to_owned()),
            }
        );
    }

    #[test]
    fn workspace_parser_accepts_handoff_open_flag() {
        assert_eq!(
            parse_workspace_command(&["handoff".to_owned(), "--open".to_owned()]).unwrap(),
            WorkspaceCommandRequest {
                action: WorkspaceCommandAction::Handoff { open_browser: true },
                run_id: None,
            }
        );
    }
}
