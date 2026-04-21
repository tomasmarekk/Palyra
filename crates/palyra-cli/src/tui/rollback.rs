use anyhow::{anyhow, Context, Result};

use super::{
    percent_encode_component, read_json_string, shorten_id, App, EntryKind, TuiUxMetricKey,
};

pub(super) async fn handle_rollback_command(app: &mut App, arguments: Vec<String>) -> Result<()> {
    let context = app.connect_admin_console().await?;
    let Some(first) = arguments.first().map(String::as_str) else {
        let run_id = app
            .last_run_id
            .clone()
            .context("/rollback requires a previous run or an explicit run id")?;
        let payload = context
            .client
            .get_json_value(format!(
                "console/v1/chat/runs/{}/workspace?limit=24",
                percent_encode_component(run_id.as_str())
            ))
            .await?;
        let artifact_count = payload
            .pointer("/workspace/artifacts")
            .and_then(serde_json::Value::as_array)
            .map(Vec::len)
            .unwrap_or(0);
        let checkpoints = payload
            .pointer("/workspace/workspace_checkpoints")
            .and_then(serde_json::Value::as_array)
            .cloned()
            .unwrap_or_default();
        let mut lines = vec![format!(
            "Run {} exposes {} artifact(s) and {} workspace checkpoint(s).",
            shorten_id(run_id.as_str()),
            artifact_count,
            checkpoints.len()
        )];
        if checkpoints.is_empty() {
            lines.push(
                "No workspace checkpoints are available yet. Mutating tool calls create rollback points.".to_owned(),
            );
        } else {
            lines.push("Recent workspace checkpoints:".to_owned());
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
                        .unwrap_or(0)
                )
            }));
            lines.push(
                "Use `/rollback <checkpoint-id>` to inspect one checkpoint, `/rollback diff <checkpoint-id-or-run-id>` for a diff preview, or `/rollback restore <checkpoint-id> --confirm` to restore the tracked workspace.".to_owned(),
            );
        }
        app.push_entry(EntryKind::System, "Workspace rollback", lines.join("\n"));
        app.status_line = "Workspace rollback checkpoints refreshed".to_owned();
        return Ok(());
    };

    if first.eq_ignore_ascii_case("restore") {
        let request = parse_restore_request(arguments.as_slice())?;
        let payload = context
            .client
            .get_json_value(format!(
                "console/v1/chat/workspace-checkpoints/{}",
                percent_encode_component(request.checkpoint_id.as_str())
            ))
            .await?;
        if !request.confirmed {
            let source_label = read_json_string(&payload, "/checkpoint/source_label");
            let summary = read_json_string(&payload, "/checkpoint/summary_text");
            let stage = payload
                .pointer("/checkpoint/checkpoint_stage")
                .and_then(serde_json::Value::as_str)
                .map(workspace_checkpoint_stage_label)
                .unwrap_or("checkpoint");
            let risk = read_json_string(&payload, "/checkpoint/risk_level");
            let review_posture = read_json_string(&payload, "/checkpoint/review_posture");
            let restore_count = payload
                .pointer("/checkpoint/restore_count")
                .and_then(serde_json::Value::as_u64)
                .unwrap_or_default();
            app.push_entry(
                EntryKind::System,
                "Workspace restore confirmation",
                format!(
                    "{} · {} · {} · {} · {}\n{}\nrestore_count={}\nRestore stays branch-safe by default.\nRun `/rollback restore {} --confirm{}` to proceed.",
                    shorten_id(request.checkpoint_id.as_str()),
                    source_label,
                    stage,
                    if risk.is_empty() { "risk unknown" } else { risk.as_str() },
                    if review_posture.is_empty() {
                        "review unknown"
                    } else {
                        review_posture.as_str()
                    },
                    summary,
                    restore_count,
                    request.checkpoint_id,
                    if request.branch_session { "" } else { " --in-place" }
                ),
            );
            app.status_line = "Workspace restore requires --confirm".to_owned();
            return Ok(());
        }

        let restore_payload = context
            .client
            .post_json_value(
                format!(
                    "console/v1/chat/workspace-checkpoints/{}/restore",
                    percent_encode_component(request.checkpoint_id.as_str())
                ),
                &serde_json::json!({
                    "branch_session": request.branch_session,
                    "scope_kind": "workspace",
                }),
            )
            .await?;
        let next_session_id = restore_payload
            .pointer("/session/session_id")
            .and_then(serde_json::Value::as_str)
            .map(ToOwned::to_owned)
            .or_else(|| app.active_session_id().ok())
            .context("workspace restore response is missing session_id")?;
        let changed_session =
            app.active_session_id().ok().as_deref() != Some(next_session_id.as_str());
        if changed_session {
            app.switch_session(next_session_id.clone()).await?;
        } else {
            let _ = app.refresh_slash_entity_catalogs().await;
            let _ = app.refresh_session_runtime_snapshot().await;
        }
        let branch_summary = if restore_payload
            .pointer("/restore/report/branched_session_id")
            .and_then(serde_json::Value::as_str)
            .is_some()
        {
            format!("Restored into branched session {}.", next_session_id)
        } else {
            format!("Restored in session {}.", next_session_id)
        };
        let suggested_summary = restore_payload
            .pointer("/suggested_session_label")
            .and_then(serde_json::Value::as_str)
            .map(|value| format!("Suggested title: {value}."))
            .unwrap_or_default();
        let failed_path_count = restore_payload
            .pointer("/restore/failed_paths")
            .and_then(serde_json::Value::as_array)
            .map(Vec::len)
            .unwrap_or_default();
        app.push_entry(
            EntryKind::System,
            "Workspace restore",
            format!(
                "{}\n{} {}{}\nUse `/workspace` to inspect the restored files or `/workspace handoff open` for the richer web preview.",
                read_json_string(&restore_payload, "/restore/report/reconciliation_summary"),
                branch_summary,
                suggested_summary,
                if failed_path_count > 0 {
                    format!(" Failed paths: {}.", failed_path_count)
                } else {
                    String::new()
                }
            ),
        );
        app.status_line = "Workspace restore completed".to_owned();
        return Ok(());
    }

    if first.eq_ignore_ascii_case("diff") {
        let Some(target) = arguments.get(1).map(String::as_str) else {
            app.status_line = "Usage: /rollback diff <checkpoint_id|run_id>".to_owned();
            app.ux_metrics.record(TuiUxMetricKey::Errors);
            return Ok(());
        };
        let left_run_id = app
            .last_run_id
            .clone()
            .context("/rollback diff requires a previous run in this session")?;
        let response = match context
            .client
            .post_json_value(
                "console/v1/chat/workspace/compare".to_owned(),
                &serde_json::json!({
                    "left_run_id": left_run_id,
                    "right_run_id": target,
                    "limit": 24,
                }),
            )
            .await
        {
            Ok(payload) => payload,
            Err(_) => {
                context
                    .client
                    .post_json_value(
                        "console/v1/chat/workspace/compare".to_owned(),
                        &serde_json::json!({
                            "left_run_id": left_run_id,
                            "right_checkpoint_id": target,
                            "limit": 24,
                        }),
                    )
                    .await?
            }
        };
        let diff = response
            .pointer("/diff")
            .cloned()
            .ok_or_else(|| anyhow!("workspace compare payload is missing /diff"))?;
        let files = diff
            .pointer("/files")
            .and_then(serde_json::Value::as_array)
            .cloned()
            .unwrap_or_default();
        let mut lines = vec![format!(
            "{} -> {}",
            read_json_string(&diff, "/left_anchor/label"),
            read_json_string(&diff, "/right_anchor/label")
        )];
        if files.is_empty() {
            lines.push(
                "No changed workspace paths were found between the selected anchors.".to_owned(),
            );
        } else {
            lines.push(format!("Changed files: {}", files.len()));
            lines.extend(files.iter().take(12).map(|file| {
                let path = file
                    .pointer("/display_path")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("unknown");
                let diff_kind = file
                    .pointer("/diff_kind")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("changed");
                format!("  {path} · {diff_kind}")
            }));
        }
        app.push_entry(EntryKind::System, "Workspace rollback diff", lines.join("\n"));
        app.status_line = "Workspace rollback diff loaded".to_owned();
        return Ok(());
    }

    let target = first;
    match context
        .client
        .get_json_value(format!(
            "console/v1/chat/workspace-checkpoints/{}",
            percent_encode_component(target)
        ))
        .await
    {
        Ok(payload) => {
            let checkpoint_id = read_json_string(&payload, "/checkpoint/checkpoint_id");
            let source_label = read_json_string(&payload, "/checkpoint/source_label");
            let summary = read_json_string(&payload, "/checkpoint/summary_text");
            let stage = payload
                .pointer("/checkpoint/checkpoint_stage")
                .and_then(serde_json::Value::as_str)
                .map(workspace_checkpoint_stage_label)
                .unwrap_or("checkpoint");
            let risk = read_json_string(&payload, "/checkpoint/risk_level");
            let review_posture = read_json_string(&payload, "/checkpoint/review_posture");
            let pair = payload
                .pointer("/checkpoint/paired_checkpoint_id")
                .and_then(serde_json::Value::as_str)
                .map(shorten_id)
                .unwrap_or_else(|| "unpaired".to_owned());
            let restore_reports = payload
                .pointer("/restore_reports")
                .and_then(serde_json::Value::as_array)
                .map(Vec::len)
                .unwrap_or(0);
            app.push_entry(
                EntryKind::System,
                "Workspace checkpoint",
                format!(
                    "{} · {} · {} · {} · {} · paired {}\n{}\nrestore_reports={}\nUse `/rollback diff {}` before restoring, then `/rollback restore {} --confirm` when you're ready.",
                    shorten_id(checkpoint_id.as_str()),
                    source_label,
                    stage,
                    if risk.is_empty() { "risk unknown" } else { risk.as_str() },
                    if review_posture.is_empty() {
                        "review unknown"
                    } else {
                        review_posture.as_str()
                    },
                    pair,
                    summary,
                    restore_reports,
                    checkpoint_id,
                    checkpoint_id
                ),
            );
            app.status_line = "Workspace checkpoint detail loaded".to_owned();
        }
        Err(_) => {
            let payload = context
                .client
                .get_json_value(format!(
                    "console/v1/chat/runs/{}/workspace?limit=24",
                    percent_encode_component(target)
                ))
                .await?;
            let artifact_count = payload
                .pointer("/workspace/artifacts")
                .and_then(serde_json::Value::as_array)
                .map(Vec::len)
                .unwrap_or(0);
            let checkpoint_count = payload
                .pointer("/workspace/workspace_checkpoints")
                .and_then(serde_json::Value::as_array)
                .map(Vec::len)
                .unwrap_or(0);
            app.push_entry(
                EntryKind::System,
                "Workspace rollback",
                format!(
                    "Run {} exposes {} artifact(s) and {} workspace checkpoint(s).\nUse `/rollback diff <checkpoint-id-or-run-id>` to preview a rollback or `/rollback restore <checkpoint-id> --confirm` to apply one.",
                    shorten_id(target),
                    artifact_count,
                    checkpoint_count
                ),
            );
            app.status_line = "Workspace rollback run detail loaded".to_owned();
        }
    }
    Ok(())
}

fn workspace_checkpoint_stage_label(stage: &str) -> &'static str {
    match stage {
        "preflight" => "preflight",
        "post_change" => "post-change",
        _ => "checkpoint",
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RestoreRequest {
    checkpoint_id: String,
    confirmed: bool,
    branch_session: bool,
}

fn parse_restore_request(arguments: &[String]) -> Result<RestoreRequest> {
    let mut checkpoint_id = None;
    let mut confirmed = false;
    let mut branch_session = true;

    for argument in arguments.iter().skip(1) {
        match argument.as_str() {
            "--confirm" | "--yes" => confirmed = true,
            "--in-place" | "--no-branch" => branch_session = false,
            "--branch" => branch_session = true,
            value if value.starts_with("--") => {
                anyhow::bail!("Usage: /rollback restore <checkpoint_id> [--confirm] [--in-place]");
            }
            value => {
                if checkpoint_id.is_none() {
                    checkpoint_id = Some(value.to_owned());
                } else {
                    anyhow::bail!(
                        "Usage: /rollback restore <checkpoint_id> [--confirm] [--in-place]"
                    );
                }
            }
        }
    }

    Ok(RestoreRequest {
        checkpoint_id: checkpoint_id
            .context("Usage: /rollback restore <checkpoint_id> [--confirm] [--in-place]")?,
        confirmed,
        branch_session,
    })
}

#[cfg(test)]
mod tests {
    use super::{parse_restore_request, RestoreRequest};

    #[test]
    fn restore_parser_defaults_to_branch_safe_confirmation() {
        assert_eq!(
            parse_restore_request(&["restore".to_owned(), "checkpoint-1".to_owned()]).unwrap(),
            RestoreRequest {
                checkpoint_id: "checkpoint-1".to_owned(),
                confirmed: false,
                branch_session: true,
            }
        );
    }

    #[test]
    fn restore_parser_accepts_confirm_and_in_place_flags() {
        assert_eq!(
            parse_restore_request(&[
                "restore".to_owned(),
                "checkpoint-2".to_owned(),
                "--confirm".to_owned(),
                "--in-place".to_owned(),
            ])
            .unwrap(),
            RestoreRequest {
                checkpoint_id: "checkpoint-2".to_owned(),
                confirmed: true,
                branch_session: false,
            }
        );
    }
}
