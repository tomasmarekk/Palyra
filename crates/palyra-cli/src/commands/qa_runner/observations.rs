//! Runtime observation collection for fixture-backed QA scenarios.

use std::{collections::BTreeMap, time::Duration};

use anyhow::{Context, Result};
use palyra_common::{
    qa_evidence::{
        QaArtifactEvidence, QaEvidenceBuildInput, QaPublicEventEvidence, QaRunTapeEvent,
        QaToolCallEvidence, QaTranscriptMessage,
    },
    qa_scenarios::{
        QaScenarioApprovalDecision, QaScenarioManifest, QaScenarioStep, QaScenarioStepAction,
    },
};
use palyra_control_plane::{
    ApprovalDecisionRequest, ConsoleLoginRequest, ControlPlaneClient, ControlPlaneClientConfig,
    NdjsonStreamLimits,
};
use serde_json::{json, Value};

use super::process::QaDaemonSandbox;

const MAX_STREAM_BUFFER_BYTES: usize = 1024 * 1024;
const MAX_ERROR_BODY_BYTES: usize = 64 * 1024;
const MAX_STREAM_EVENTS: usize = 1_024;
const MAX_PUBLIC_EVENTS: usize = 512;
const MAX_TAPE_EVENTS: usize = 1_024;
const MAX_TRANSCRIPT_ROWS: usize = 128;
const MAX_FINAL_ANSWER_CHARS: usize = 64 * 1024;
const MAX_WORKSPACE_ARTIFACTS: usize = 256;
const TAPE_PAGE_SIZE: usize = 256;

pub(super) struct QaScenarioObservations {
    pub(super) run_id: String,
    pub(super) session_id: String,
    pub(super) terminal_state: String,
    pub(super) terminal_observed: bool,
    pub(super) evidence: QaEvidenceBuildInput,
}

#[derive(Default)]
struct StreamObservations {
    run_id: Option<String>,
    session_id: Option<String>,
    complete_status: Option<String>,
    final_answer: String,
    public_events: Vec<QaPublicEventEvidence>,
    tool_names_by_proposal: BTreeMap<String, String>,
    tool_calls: Vec<QaToolCallEvidence>,
    approval_cursor: usize,
    event_count: usize,
}

/// Runs the scenario's user prompt and captures real stream, journal, and workspace evidence.
pub(super) async fn collect_scenario_observations(
    manifest: &QaScenarioManifest,
    sandbox: &mut QaDaemonSandbox,
) -> Result<QaScenarioObservations> {
    let prompt = scenario_user_prompt(manifest)?;
    let mut client = authenticated_client(manifest, sandbox).await?;
    let session_id = create_session(&client, manifest).await?;
    sandbox.record_session_id(session_id.as_str());
    let stream_path = format!("console/v1/chat/sessions/{session_id}/messages/stream");
    let message = json!({
        "text": prompt,
        "session_label": format!("QA: {}", manifest.id),
        "allow_sensitive_tools": false,
        "origin_kind": "qa_fixture",
        "parameter_delta": {
            "cli_context": {
                "launch_cwd": sandbox.workspace().to_string_lossy(),
                "workspace_roots": [],
                "env": {},
            }
        },
        "attachments": [],
    });
    let limits = NdjsonStreamLimits::new(MAX_STREAM_BUFFER_BYTES, MAX_ERROR_BODY_BYTES);
    let mut stream = client
        .post_ndjson_stream(stream_path, &message, limits)
        .await
        .context("qa.runner.stream_open_failed")?;
    let mut observed = StreamObservations::default();
    let approval_steps = manifest
        .steps
        .iter()
        .filter(|step| step.action == QaScenarioStepAction::ApprovalDecision)
        .collect::<Vec<_>>();
    let run_timeout = Duration::from_millis(manifest.timeout.run_ms);
    tokio::time::timeout(run_timeout, async {
        while let Some(line) =
            stream.next_value().await.context("qa.runner.stream_decode_failed")?
        {
            observed.event_count = observed.event_count.saturating_add(1);
            if observed.event_count > MAX_STREAM_EVENTS {
                anyhow::bail!("qa.runner.stream_event_limit_exceeded");
            }
            process_stream_line(&mut client, &line, approval_steps.as_slice(), &mut observed)
                .await?;
            if let Some(run_id) = observed.run_id.as_deref() {
                sandbox.record_run_id(run_id);
            }
        }
        Result::<()>::Ok(())
    })
    .await
    .map_err(|_| anyhow::anyhow!("qa.runner.run_timeout"))??;

    let run_id = observed.run_id.clone().context("qa.runner.missing_run_id")?;
    if observed.session_id.as_deref() != Some(session_id.as_str()) {
        anyhow::bail!("qa.runner.session_identity_mismatch");
    }
    if observed.approval_cursor != approval_steps.len() {
        anyhow::bail!("qa.runner.approval_step_not_observed");
    }

    let status = load_run_status(&client, run_id.as_str()).await?;
    let terminal_state = canonical_terminal_state(&status, observed.complete_status.as_deref());
    let terminal_observed = is_terminal_state(terminal_state.as_str());
    let tape_events = load_run_tape(&client, run_id.as_str()).await?;
    let mut transcript = transcript_from_tape(tape_events.as_slice());
    if !observed.final_answer.is_empty() && transcript.len() < MAX_TRANSCRIPT_ROWS {
        transcript.push(QaTranscriptMessage {
            role: "assistant".to_owned(),
            content: observed.final_answer.clone(),
        });
    }
    // The session view is queried even though the tape is the safer text
    // source; this verifies that the normal transcript projection is healthy.
    verify_session_transcript(&client, session_id.as_str()).await?;
    let artifacts = load_workspace_artifacts(&client, run_id.as_str()).await?;
    let evidence = QaEvidenceBuildInput {
        run_id: Some(run_id.clone()),
        session_id: Some(session_id.clone()),
        terminal_state: Some(terminal_state.clone()),
        final_answer: (!observed.final_answer.is_empty()).then_some(observed.final_answer),
        transcript,
        tape_events,
        public_events: observed.public_events,
        tool_calls: observed.tool_calls,
        artifacts,
    };
    Ok(QaScenarioObservations { run_id, session_id, terminal_state, terminal_observed, evidence })
}

async fn authenticated_client(
    manifest: &QaScenarioManifest,
    sandbox: &QaDaemonSandbox,
) -> Result<ControlPlaneClient> {
    let timeout = Duration::from_millis(manifest.timeout.run_ms.saturating_add(5_000));
    let mut config = ControlPlaneClientConfig::new(sandbox.admin_url());
    config.request_timeout = timeout;
    config.safe_read_retries = 0;
    let mut client = ControlPlaneClient::new(config).context("qa.runner.client_init_failed")?;
    client
        .login(&ConsoleLoginRequest {
            admin_token: Some(sandbox.admin_token().to_owned()),
            principal: sandbox.principal().to_owned(),
            device_id: sandbox.device_id().to_owned(),
            channel: Some("qa".to_owned()),
        })
        .await
        .context("qa.runner.console_login_failed")?;
    Ok(client)
}

async fn create_session(
    client: &ControlPlaneClient,
    manifest: &QaScenarioManifest,
) -> Result<String> {
    let response = client
        .post_json_value(
            "console/v1/chat/sessions",
            &json!({
                "session_label": format!("QA: {}", manifest.id),
                "require_existing": false,
                "reset_session": false,
            }),
        )
        .await
        .context("qa.runner.session_create_failed")?;
    required_string(&response, "/session/session_id", "qa.runner.session_id_missing")
}

async fn process_stream_line(
    client: &mut ControlPlaneClient,
    line: &Value,
    approval_steps: &[&QaScenarioStep],
    observed: &mut StreamObservations,
) -> Result<()> {
    match line.get("type").and_then(Value::as_str) {
        Some("meta") => {
            set_consistent_identity(
                &mut observed.run_id,
                required_string(line, "/run_id", "qa.runner.stream_run_id_missing")?,
                "qa.runner.stream_run_id_changed",
            )?;
            set_consistent_identity(
                &mut observed.session_id,
                required_string(line, "/session_id", "qa.runner.stream_session_id_missing")?,
                "qa.runner.stream_session_id_changed",
            )?;
        }
        Some("event") => process_runtime_event(client, line, approval_steps, observed).await?,
        Some("complete") => {
            observed.complete_status =
                Some(line.get("status").and_then(Value::as_str).unwrap_or("unknown").to_owned());
        }
        Some("error") => anyhow::bail!("qa.runner.runtime_stream_error"),
        _ => anyhow::bail!("qa.runner.unknown_stream_line"),
    }
    Ok(())
}

async fn process_runtime_event(
    client: &mut ControlPlaneClient,
    line: &Value,
    approval_steps: &[&QaScenarioStep],
    observed: &mut StreamObservations,
) -> Result<()> {
    let event = line.get("event").context("qa.runner.stream_event_missing")?;
    if let Some(public_event) = event.get("public_event") {
        if observed.public_events.len() >= MAX_PUBLIC_EVENTS {
            anyhow::bail!("qa.runner.public_event_limit_exceeded");
        }
        observed.public_events.push(QaPublicEventEvidence {
            event_type: required_string(
                public_event,
                "/event",
                "qa.runner.public_event_name_missing",
            )?,
            payload: public_event.clone(),
        });
    }
    match event.get("event_type").and_then(Value::as_str) {
        Some("model_token") => {
            if let Some(token) = event.pointer("/model_token/token").and_then(Value::as_str) {
                push_bounded_text(&mut observed.final_answer, token)?;
            }
        }
        Some("tool_proposal") => {
            let proposal_id = required_string(
                event,
                "/tool_proposal/proposal_id",
                "qa.runner.tool_proposal_id_missing",
            )?;
            let tool_name =
                required_string(event, "/tool_proposal/tool_name", "qa.runner.tool_name_missing")?;
            observed.tool_names_by_proposal.insert(proposal_id, tool_name);
        }
        Some("tool_result") => {
            let proposal_id = required_string(
                event,
                "/tool_result/proposal_id",
                "qa.runner.tool_result_proposal_id_missing",
            )?;
            let name = observed
                .tool_names_by_proposal
                .get(proposal_id.as_str())
                .cloned()
                .unwrap_or_else(|| "<unknown>".to_owned());
            observed.tool_calls.push(QaToolCallEvidence {
                name,
                proposal_id: Some(proposal_id),
                success: event
                    .pointer("/tool_result/success")
                    .and_then(Value::as_bool)
                    .unwrap_or(false),
            });
        }
        Some("tool_approval_request") => {
            let step = approval_steps
                .get(observed.approval_cursor)
                .copied()
                .context("qa.runner.unexpected_approval_request")?;
            decide_approval(client, event, step).await?;
            observed.approval_cursor = observed.approval_cursor.saturating_add(1);
        }
        _ => {}
    }
    Ok(())
}

async fn decide_approval(
    client: &ControlPlaneClient,
    event: &Value,
    step: &QaScenarioStep,
) -> Result<()> {
    let approval_id = required_string(
        event,
        "/tool_approval_request/approval_id",
        "qa.runner.approval_id_missing",
    )?;
    let proposal_id = required_string(
        event,
        "/tool_approval_request/proposal_id",
        "qa.runner.approval_proposal_id_missing",
    )?;
    if step.proposal_id.as_deref().is_some_and(|expected| expected != proposal_id) {
        anyhow::bail!("qa.runner.approval_proposal_mismatch");
    }
    let approved = match step.decision.as_ref() {
        Some(QaScenarioApprovalDecision::Allow) => true,
        Some(QaScenarioApprovalDecision::Deny) => false,
        Some(QaScenarioApprovalDecision::Legacy(_)) | None => {
            anyhow::bail!("qa.runner.invalid_approval_decision")
        }
    };
    client
        .decide_approval(
            approval_id.as_str(),
            &ApprovalDecisionRequest {
                approved,
                reason: Some(if approved {
                    "qa_fixture_allow".to_owned()
                } else {
                    "qa_fixture_deny".to_owned()
                }),
                decision_scope: Some("once".to_owned()),
                decision_scope_ttl_ms: None,
            },
        )
        .await
        .context("qa.runner.approval_decision_failed")?;
    Ok(())
}

async fn load_run_status(client: &ControlPlaneClient, run_id: &str) -> Result<Value> {
    client
        .get_json_value(format!("console/v1/chat/runs/{run_id}/status"))
        .await
        .context("qa.runner.run_status_failed")
}

async fn load_run_tape(client: &ControlPlaneClient, run_id: &str) -> Result<Vec<QaRunTapeEvent>> {
    let mut events = Vec::new();
    let mut after_seq = None;
    loop {
        let mut path = format!("console/v1/chat/runs/{run_id}/events?limit={TAPE_PAGE_SIZE}");
        if let Some(cursor) = after_seq {
            path.push_str(format!("&after_seq={cursor}").as_str());
        }
        let response = client.get_json_value(path).await.context("qa.runner.run_tape_failed")?;
        let page = response
            .pointer("/tape/events")
            .and_then(Value::as_array)
            .context("qa.runner.run_tape_events_missing")?;
        for event in page {
            if events.len() >= MAX_TAPE_EVENTS {
                anyhow::bail!("qa.runner.tape_event_limit_exceeded");
            }
            events.push(QaRunTapeEvent {
                seq: event
                    .get("seq")
                    .and_then(Value::as_i64)
                    .context("qa.runner.tape_seq_missing")?,
                event_type: required_string(
                    event,
                    "/event_type",
                    "qa.runner.tape_event_type_missing",
                )?,
                payload: parse_payload_json(event.get("payload_json"))?,
            });
        }
        let next = response.pointer("/tape/next_after_seq").and_then(Value::as_i64);
        match next {
            Some(next) if after_seq.is_none_or(|previous| next > previous) => {
                after_seq = Some(next);
            }
            Some(_) => anyhow::bail!("qa.runner.tape_cursor_stalled"),
            None => break,
        }
    }
    Ok(events)
}

fn transcript_from_tape(tape: &[QaRunTapeEvent]) -> Vec<QaTranscriptMessage> {
    tape.iter()
        .filter_map(|event| match event.event_type.as_str() {
            "message.received" => {
                event.payload.get("text").and_then(Value::as_str).map(|content| {
                    QaTranscriptMessage { role: "user".to_owned(), content: content.to_owned() }
                })
            }
            _ => None,
        })
        .take(MAX_TRANSCRIPT_ROWS.saturating_sub(1))
        .collect()
}

async fn verify_session_transcript(client: &ControlPlaneClient, session_id: &str) -> Result<()> {
    let response = client
        .get_json_value(format!("console/v1/chat/sessions/{session_id}/transcript"))
        .await
        .context("qa.runner.session_transcript_failed")?;
    response
        .get("records")
        .and_then(Value::as_array)
        .context("qa.runner.session_transcript_missing")?;
    Ok(())
}

async fn load_workspace_artifacts(
    client: &ControlPlaneClient,
    run_id: &str,
) -> Result<Vec<QaArtifactEvidence>> {
    let response = client
        .get_json_value(format!(
            "console/v1/chat/runs/{run_id}/workspace?limit={MAX_WORKSPACE_ARTIFACTS}"
        ))
        .await
        .context("qa.runner.workspace_observation_failed")?;
    parse_workspace_artifacts_response(&response)
}

fn parse_workspace_artifacts_response(response: &Value) -> Result<Vec<QaArtifactEvidence>> {
    let artifacts_complete = response
        .pointer("/workspace/artifacts_complete")
        .and_then(Value::as_bool)
        .context("qa.runner.workspace_artifacts_completeness_missing")?;
    if !artifacts_complete {
        anyhow::bail!("qa.runner.workspace_artifacts_truncated");
    }
    let artifacts = response
        .pointer("/workspace/artifacts")
        .and_then(Value::as_array)
        .context("qa.runner.workspace_artifacts_missing")?;
    let artifact_count = response
        .pointer("/workspace/artifact_count")
        .and_then(Value::as_u64)
        .and_then(|count| usize::try_from(count).ok())
        .context("qa.runner.workspace_artifact_count_missing")?;
    if artifact_count != artifacts.len() || artifact_count > MAX_WORKSPACE_ARTIFACTS {
        anyhow::bail!("qa.runner.workspace_artifacts_incomplete");
    }
    artifacts
        .iter()
        .map(|artifact| {
            Ok(QaArtifactEvidence {
                path: required_string(artifact, "/path", "qa.runner.artifact_path_missing")?,
                kind: artifact
                    .get("change_kind")
                    .and_then(Value::as_str)
                    .unwrap_or("workspace")
                    .to_owned(),
                present: !artifact.get("deleted").and_then(Value::as_bool).unwrap_or(false),
                sha256: artifact.get("content_sha256").and_then(Value::as_str).map(str::to_owned),
                size_bytes: artifact.get("size_bytes").and_then(Value::as_u64),
            })
        })
        .collect()
}

fn scenario_user_prompt(manifest: &QaScenarioManifest) -> Result<&str> {
    let mut prompts =
        manifest.steps.iter().filter(|step| step.action == QaScenarioStepAction::UserPrompt);
    let prompt = prompts
        .next()
        .and_then(|step| step.prompt.as_deref())
        .context("qa.runner.user_prompt_missing")?;
    if prompts.next().is_some() {
        anyhow::bail!("qa.runner.multiple_user_prompts_unsupported");
    }
    Ok(prompt)
}

fn canonical_terminal_state(status: &Value, stream_status: Option<&str>) -> String {
    let state = status.pointer("/run/state").and_then(Value::as_str);
    match state.or(stream_status) {
        Some("completed" | "done") => "completed",
        Some("failed") => "failed",
        Some("cancelled" | "canceled") => "cancelled",
        Some("waiting_approval" | "approval_required") => "approval_required",
        Some(other) => other,
        None => "unknown",
    }
    .to_owned()
}

fn is_terminal_state(state: &str) -> bool {
    matches!(state, "completed" | "failed" | "cancelled" | "approval_required")
}

fn required_string(value: &Value, pointer: &str, code: &'static str) -> Result<String> {
    value
        .pointer(pointer)
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .map(str::to_owned)
        .ok_or_else(|| anyhow::anyhow!(code))
}

fn set_consistent_identity(
    slot: &mut Option<String>,
    value: String,
    code: &'static str,
) -> Result<()> {
    if slot.as_deref().is_some_and(|existing| existing != value) {
        anyhow::bail!(code);
    }
    *slot = Some(value);
    Ok(())
}

fn push_bounded_text(target: &mut String, fragment: &str) -> Result<()> {
    let next_chars = target.chars().count().saturating_add(fragment.chars().count());
    if next_chars > MAX_FINAL_ANSWER_CHARS {
        anyhow::bail!("qa.runner.final_answer_limit_exceeded");
    }
    target.push_str(fragment);
    Ok(())
}

fn parse_payload_json(value: Option<&Value>) -> Result<Value> {
    let raw = value.and_then(Value::as_str).context("qa.runner.tape_payload_missing")?;
    serde_json::from_str(raw).context("qa.runner.tape_payload_invalid")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn terminal_state_prefers_durable_run_snapshot() {
        let status = json!({"run": {"state": "completed"}});

        assert_eq!(canonical_terminal_state(&status, Some("failed")), "completed");
    }

    #[test]
    fn transcript_projection_uses_real_received_messages_only() {
        let tape = vec![
            QaRunTapeEvent {
                seq: 0,
                event_type: "message.received".to_owned(),
                payload: json!({"text": "hello"}),
            },
            QaRunTapeEvent {
                seq: 1,
                event_type: "message.replied".to_owned(),
                payload: json!({"reply_text": "<redacted>"}),
            },
        ];

        assert_eq!(
            transcript_from_tape(tape.as_slice()),
            vec![QaTranscriptMessage { role: "user".to_owned(), content: "hello".to_owned() }]
        );
    }

    #[test]
    fn identity_rejects_a_midstream_change() {
        let mut value = Some("run-a".to_owned());

        let error =
            set_consistent_identity(&mut value, "run-b".to_owned(), "qa.runner.identity_changed")
                .expect_err("runtime identities must remain stable");

        assert_eq!(error.to_string(), "qa.runner.identity_changed");
    }

    #[test]
    fn workspace_artifact_projection_fails_closed_when_server_truncates() {
        let response = json!({
            "workspace": {
                "artifacts": (0..MAX_WORKSPACE_ARTIFACTS)
                    .map(|index| json!({"path": format!("artifact-{index}.txt")}))
                    .collect::<Vec<_>>(),
                "artifact_count": MAX_WORKSPACE_ARTIFACTS + 1,
                "artifacts_complete": false,
            }
        });

        let error = parse_workspace_artifacts_response(&response)
            .expect_err("a partial artifact set must never satisfy QA evidence");

        assert_eq!(error.to_string(), "qa.runner.workspace_artifacts_truncated");
    }

    #[test]
    fn workspace_artifact_projection_accepts_a_complete_bounded_set() {
        let response = json!({
            "workspace": {
                "artifacts": [{
                    "path": "src/output.txt",
                    "change_kind": "modified",
                    "deleted": false,
                    "content_sha256": "f".repeat(64),
                    "size_bytes": 12,
                }],
                "artifact_count": 1,
                "artifacts_complete": true,
            }
        });

        let artifacts = parse_workspace_artifacts_response(&response)
            .expect("a complete bounded artifact set should parse");

        assert_eq!(artifacts.len(), 1);
        assert_eq!(artifacts[0].path, "src/output.txt");
    }
}
