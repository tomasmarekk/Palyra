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
                format!(
                    "  {} · {} · restores {}",
                    checkpoint
                        .pointer("/checkpoint_id")
                        .and_then(serde_json::Value::as_str)
                        .map(shorten_id)
                        .unwrap_or_else(|| "unknown".to_owned()),
                    checkpoint
                        .pointer("/source_label")
                        .and_then(serde_json::Value::as_str)
                        .unwrap_or("workspace checkpoint"),
                    checkpoint
                        .pointer("/restore_count")
                        .and_then(serde_json::Value::as_u64)
                        .unwrap_or(0)
                )
            }));
            lines.push(
                "Use `/rollback <checkpoint-id>` to inspect one checkpoint or `/rollback diff <checkpoint-id-or-run-id>` for a diff preview.".to_owned(),
            );
        }
        app.push_entry(EntryKind::System, "Workspace rollback", lines.join("\n"));
        app.status_line = "Workspace rollback checkpoints refreshed".to_owned();
        return Ok(());
    };

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
            let restore_reports = payload
                .pointer("/restore_reports")
                .and_then(serde_json::Value::as_array)
                .map(Vec::len)
                .unwrap_or(0);
            app.push_entry(
                EntryKind::System,
                "Workspace checkpoint",
                format!(
                    "{} · {}\n{}\nrestore_reports={}\nUse `/rollback diff {}` before restoring in the web workspace inspector.",
                    shorten_id(checkpoint_id.as_str()),
                    source_label,
                    summary,
                    restore_reports,
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
                    "Run {} exposes {} artifact(s) and {} workspace checkpoint(s).\nUse `/rollback diff <checkpoint-id-or-run-id>` to preview a rollback.",
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
