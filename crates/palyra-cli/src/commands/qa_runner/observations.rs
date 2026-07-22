//! Runtime observation collection for fixture-backed QA scenarios.

use std::{
    collections::BTreeMap,
    future::Future,
    io,
    time::{Duration, Instant},
};

use anyhow::{Context, Result};
use palyra_common::{
    metadata_trace::MetadataTraceV1,
    qa_evidence::{
        QaArtifactEvidence, QaEvidenceBuildInput, QaPublicEventEvidence, QaRunTapeEvent,
        QaToolCallEvidence, QaTranscriptMessage,
    },
    qa_scenarios::{
        QaScenarioApprovalDecision, QaScenarioManifest, QaScenarioStep, QaScenarioStepAction,
    },
    redaction::redact_diagnostic_text,
    runtime_contracts::RUNTIME_KERNEL_V2_PROVIDER_EFFECT_STARTED_MESSAGE,
};
use palyra_control_plane::{
    ApprovalDecisionRequest, ConsoleLoginRequest, ControlPlaneClient, ControlPlaneClientConfig,
    NdjsonStreamLimits,
};
use serde::Serialize;
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
const MAX_JSON_RESPONSE_BYTES: usize = 4 * 1024 * 1024;
const MAX_OBSERVATION_BYTES: usize = 32 * 1024 * 1024;
const TAPE_PAGE_SIZE: usize = 256;
const AUTHORITATIVE_COMPACTION_SEED_TURNS: usize = 5;
const CONTROL_PLANE_TIMEOUT_GRACE: Duration = Duration::from_secs(5);

pub(super) struct QaScenarioObservations {
    pub(super) run_id: String,
    pub(super) session_id: String,
    pub(super) terminal_state: String,
    pub(super) terminal_observed: bool,
    pub(super) evidence: QaEvidenceBuildInput,
}

struct QaObservationBudget {
    consumed_bytes: usize,
    max_bytes: usize,
}

impl QaObservationBudget {
    fn new(max_bytes: usize) -> Self {
        Self { consumed_bytes: 0, max_bytes }
    }

    fn consume_serialized<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<()> {
        let mut counter = SerializedByteCounter::default();
        serde_json::to_writer(&mut counter, value)
            .context("qa.runner.observation_size_encode_failed")?;
        self.consume(counter.bytes)
    }

    fn consume(&mut self, bytes: usize) -> Result<()> {
        let next = self
            .consumed_bytes
            .checked_add(bytes)
            .ok_or_else(|| anyhow::anyhow!("qa.runner.evidence_byte_limit_exceeded"))?;
        if next > self.max_bytes {
            anyhow::bail!("qa.runner.evidence_byte_limit_exceeded");
        }
        self.consumed_bytes = next;
        Ok(())
    }
}

impl Default for QaObservationBudget {
    fn default() -> Self {
        Self::new(MAX_OBSERVATION_BYTES)
    }
}

#[derive(Default)]
struct SerializedByteCounter {
    bytes: usize,
}

impl io::Write for SerializedByteCounter {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        self.bytes = self.bytes.saturating_add(buffer.len());
        Ok(buffer.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

/// One absolute run deadline with a narrower cap for each external step.
#[derive(Debug, Clone, Copy)]
pub(super) struct QaRunDeadline {
    expires_at: Instant,
    step_timeout: Duration,
}

impl QaRunDeadline {
    pub(super) fn new(manifest: &QaScenarioManifest) -> Result<Self> {
        Self::from_timeouts(
            Duration::from_millis(manifest.timeout.run_ms),
            Duration::from_millis(manifest.timeout.step_ms.unwrap_or(manifest.timeout.run_ms)),
        )
    }

    fn from_timeouts(run_timeout: Duration, step_timeout: Duration) -> Result<Self> {
        let expires_at = Instant::now()
            .checked_add(run_timeout)
            .ok_or_else(|| anyhow::anyhow!("qa.runner.run_deadline_overflow"))?;
        Ok(Self { expires_at, step_timeout })
    }

    pub(super) fn step_budget(self) -> Result<Duration> {
        let remaining = self.remaining_budget()?;
        Ok(remaining.min(self.step_timeout))
    }

    pub(super) fn remaining_budget(self) -> Result<Duration> {
        self.expires_at
            .checked_duration_since(Instant::now())
            .filter(|remaining| !remaining.is_zero())
            .ok_or_else(|| anyhow::anyhow!("qa.runner.run_timeout"))
    }

    pub(super) fn normalize_sync_result<T>(self, result: Result<T>) -> Result<T> {
        match result {
            Err(_) if self.remaining_budget().is_err() => {
                Err(anyhow::anyhow!("qa.runner.run_timeout"))
            }
            outcome => outcome,
        }
    }

    async fn run_step<T, F>(self, future: F) -> Result<T>
    where
        F: Future<Output = Result<T>>,
    {
        let now = Instant::now();
        let remaining = self
            .expires_at
            .checked_duration_since(now)
            .filter(|remaining| !remaining.is_zero())
            .ok_or_else(|| anyhow::anyhow!("qa.runner.run_timeout"))?;
        let limited_by_run = remaining <= self.step_timeout;
        let budget = remaining.min(self.step_timeout);
        let deadline = tokio::time::Instant::from_std(
            now.checked_add(budget)
                .ok_or_else(|| anyhow::anyhow!("qa.runner.step_deadline_overflow"))?,
        );
        match tokio::time::timeout_at(deadline, future).await {
            Ok(result) => result,
            Err(_) if limited_by_run => Err(anyhow::anyhow!("qa.runner.run_timeout")),
            Err(_) => Err(anyhow::anyhow!("qa.runner.step_timeout")),
        }
    }
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
    cancellation_requested: bool,
    event_count: usize,
}

/// Runs the scenario's user prompt and captures real stream, journal, and workspace evidence.
pub(super) async fn collect_scenario_observations(
    manifest: &QaScenarioManifest,
    sandbox: &mut QaDaemonSandbox,
    deadline: QaRunDeadline,
) -> Result<QaScenarioObservations> {
    let mut observation_budget = QaObservationBudget::default();
    let prompt = scenario_user_prompt(manifest)?;
    let mut client = authenticated_client(sandbox, deadline).await?;
    let session_id = deadline.run_step(create_session(&client, manifest)).await?;
    sandbox.record_session_id(session_id.as_str());
    let stream_path = format!("console/v1/chat/sessions/{session_id}/messages/stream");
    if uses_authoritative_v2_compaction_profile(manifest) {
        seed_authoritative_v2_compaction_history(
            &mut client,
            sandbox,
            session_id.as_str(),
            stream_path.as_str(),
            deadline,
        )
        .await?;
    }
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
    let mut stream = deadline
        .run_step(async {
            client
                .post_ndjson_stream(stream_path, &message, limits)
                .await
                .context("qa.runner.stream_open_failed")
        })
        .await?;
    let mut observed = StreamObservations::default();
    let approval_steps = manifest
        .steps
        .iter()
        .filter(|step| step.action == QaScenarioStepAction::ApprovalDecision)
        .collect::<Vec<_>>();
    let cancel_after_admission =
        manifest.runner.as_ref().and_then(|runner| runner.policy_profile())
            == Some("runtime_kernel_v2_authoritative_cancel");
    deadline
        .run_step(async {
            while let Some(line) =
                stream.next_value().await.context("qa.runner.stream_decode_failed")?
            {
                observed.event_count = observed.event_count.saturating_add(1);
                if observed.event_count > MAX_STREAM_EVENTS {
                    anyhow::bail!("qa.runner.stream_event_limit_exceeded");
                }
                process_stream_line(
                    &mut client,
                    &line,
                    approval_steps.as_slice(),
                    cancel_after_admission,
                    &mut observed,
                    &mut observation_budget,
                    deadline,
                )
                .await?;
                if let Some(run_id) = observed.run_id.as_deref() {
                    sandbox.record_run_id(run_id);
                }
            }
            Result::<()>::Ok(())
        })
        .await?;

    let run_id = observed.run_id.clone().context("qa.runner.missing_run_id")?;
    if observed.session_id.as_deref() != Some(session_id.as_str()) {
        anyhow::bail!("qa.runner.session_identity_mismatch");
    }
    if observed.approval_cursor != approval_steps.len() {
        anyhow::bail!("qa.runner.approval_step_not_observed");
    }

    let status = deadline.run_step(load_run_status(&client, run_id.as_str())).await?;
    let terminal_state = canonical_terminal_state(&status, observed.complete_status.as_deref());
    let terminal_observed = is_terminal_state(terminal_state.as_str());
    let metadata_trace = deadline
        .run_step(load_run_metadata_trace(&client, run_id.as_str(), &mut observation_budget))
        .await?;
    let tape_events =
        deadline.run_step(load_run_tape(&client, run_id.as_str(), &mut observation_budget)).await?;
    let mut transcript = transcript_from_tape(tape_events.as_slice(), &mut observation_budget)?;
    if !observed.final_answer.is_empty() {
        observation_budget.consume_serialized(&observed.final_answer)?;
        if transcript.len() < MAX_TRANSCRIPT_ROWS {
            let final_message = QaTranscriptMessage {
                role: "assistant".to_owned(),
                content: observed.final_answer.clone(),
            };
            observation_budget.consume_serialized(&final_message)?;
            transcript.push(final_message);
        }
    }
    // The session view is queried even though the tape is the safer text
    // source; this verifies that the normal transcript projection is healthy.
    deadline.run_step(verify_session_transcript(&client, session_id.as_str())).await?;
    let artifacts = deadline
        .run_step(load_workspace_artifacts(&client, run_id.as_str(), &mut observation_budget))
        .await?;
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
        metadata_trace: Some(metadata_trace),
        ..QaEvidenceBuildInput::default()
    };
    Ok(QaScenarioObservations { run_id, session_id, terminal_state, terminal_observed, evidence })
}

fn uses_authoritative_v2_compaction_profile(manifest: &QaScenarioManifest) -> bool {
    manifest.runner.as_ref().and_then(|runner| runner.policy_profile())
        == Some("runtime_kernel_v2_authoritative_compaction")
}

async fn seed_authoritative_v2_compaction_history(
    client: &mut ControlPlaneClient,
    sandbox: &mut QaDaemonSandbox,
    session_id: &str,
    stream_path: &str,
    deadline: QaRunDeadline,
) -> Result<()> {
    let launch_cwd = sandbox.workspace().to_string_lossy().into_owned();
    for turn in 1..=AUTHORITATIVE_COMPACTION_SEED_TURNS {
        let message = json!({
            "text": format!(
                "Seed authoritative V2 compaction history turn {turn}: preserve deterministic continuity."
            ),
            "session_label": "QA: runtime_kernel_v2.authoritative_compaction",
            "allow_sensitive_tools": false,
            "origin_kind": "qa_fixture",
            "parameter_delta": {
                "cli_context": {
                    "launch_cwd": launch_cwd,
                    "workspace_roots": [],
                    "env": {},
                }
            },
            "attachments": [],
        });
        let limits = NdjsonStreamLimits::new(MAX_STREAM_BUFFER_BYTES, MAX_ERROR_BODY_BYTES);
        let mut stream = deadline
            .run_step(async {
                client
                    .post_ndjson_stream(stream_path.to_owned(), &message, limits)
                    .await
                    .context("qa.runner.compaction_seed_stream_open_failed")
            })
            .await?;
        let mut run_id = None;
        let mut complete_status = None;
        let mut event_count = 0usize;
        deadline
            .run_step(async {
                while let Some(line) = stream
                    .next_value()
                    .await
                    .context("qa.runner.compaction_seed_stream_decode_failed")?
                {
                    event_count = event_count.saturating_add(1);
                    if event_count > MAX_STREAM_EVENTS {
                        anyhow::bail!("qa.runner.compaction_seed_event_limit_exceeded");
                    }
                    match line.get("type").and_then(Value::as_str) {
                        Some("meta") => {
                            set_consistent_identity(
                                &mut run_id,
                                required_string(
                                    &line,
                                    "/run_id",
                                    "qa.runner.compaction_seed_run_id_missing",
                                )?,
                                "qa.runner.compaction_seed_run_id_changed",
                            )?;
                            if required_string(
                                &line,
                                "/session_id",
                                "qa.runner.compaction_seed_session_id_missing",
                            )? != session_id
                            {
                                anyhow::bail!("qa.runner.compaction_seed_session_id_changed");
                            }
                        }
                        Some("event") => {}
                        Some("complete") => {
                            complete_status = Some(
                                line.get("status")
                                    .and_then(Value::as_str)
                                    .unwrap_or("unknown")
                                    .to_owned(),
                            );
                        }
                        Some("error") => {
                            let diagnostic = line
                                .get("error")
                                .and_then(Value::as_str)
                                .map(redact_diagnostic_text)
                                .unwrap_or_else(|| "unstructured runtime stream error".to_owned());
                            anyhow::bail!(
                                "qa.runner.compaction_seed_runtime_stream_error: {diagnostic}"
                            )
                        }
                        _ => anyhow::bail!("qa.runner.compaction_seed_unknown_stream_line"),
                    }
                }
                Result::<()>::Ok(())
            })
            .await?;
        let run_id = run_id.context("qa.runner.compaction_seed_run_id_missing")?;
        let status = deadline.run_step(load_run_status(client, run_id.as_str())).await?;
        if canonical_terminal_state(&status, complete_status.as_deref()) != "completed" {
            anyhow::bail!("qa.runner.compaction_seed_not_completed");
        }
        sandbox.record_run_id(run_id.as_str());
    }
    Ok(())
}

/// Reloads durable run evidence after an expected daemon restart.
pub(super) async fn collect_recovered_scenario_observations(
    sandbox: &QaDaemonSandbox,
    deadline: QaRunDeadline,
) -> Result<QaScenarioObservations> {
    let mut observation_budget = QaObservationBudget::default();
    let run_id = sandbox.active_run_id().context("qa.runner.recovery_run_id_missing")?.to_owned();
    let session_id =
        sandbox.active_session_id().context("qa.runner.recovery_session_id_missing")?.to_owned();
    let client = authenticated_client(sandbox, deadline).await?;
    let status = deadline.run_step(load_run_status(&client, run_id.as_str())).await?;
    let terminal_state = canonical_terminal_state(&status, None);
    let terminal_observed = is_terminal_state(terminal_state.as_str());
    if !terminal_observed {
        anyhow::bail!("qa.runner.recovery_run_not_terminal");
    }
    let metadata_trace = deadline
        .run_step(load_run_metadata_trace(&client, run_id.as_str(), &mut observation_budget))
        .await?;
    let tape_events =
        deadline.run_step(load_run_tape(&client, run_id.as_str(), &mut observation_budget)).await?;
    let transcript = transcript_from_tape(tape_events.as_slice(), &mut observation_budget)?;
    deadline.run_step(verify_session_transcript(&client, session_id.as_str())).await?;
    let artifacts = deadline
        .run_step(load_workspace_artifacts(&client, run_id.as_str(), &mut observation_budget))
        .await?;
    let evidence = QaEvidenceBuildInput {
        run_id: Some(run_id.clone()),
        session_id: Some(session_id.clone()),
        terminal_state: Some(terminal_state.clone()),
        transcript,
        tape_events,
        artifacts,
        metadata_trace: Some(metadata_trace),
        ..QaEvidenceBuildInput::default()
    };
    Ok(QaScenarioObservations { run_id, session_id, terminal_state, terminal_observed, evidence })
}

async fn authenticated_client(
    sandbox: &QaDaemonSandbox,
    deadline: QaRunDeadline,
) -> Result<ControlPlaneClient> {
    let mut config = ControlPlaneClientConfig::new(sandbox.admin_url());
    // The QA deadline owns timeout classification. Keep the transport timeout
    // behind it as a fail-safe for a client that does not react to cancellation.
    config.request_timeout =
        deadline.remaining_budget()?.saturating_add(CONTROL_PLANE_TIMEOUT_GRACE);
    config.safe_read_retries = 0;
    config.max_json_response_bytes = MAX_JSON_RESPONSE_BYTES;
    let mut client = ControlPlaneClient::new(config).context("qa.runner.client_init_failed")?;
    deadline
        .run_step(async {
            client
                .login(&ConsoleLoginRequest {
                    admin_token: Some(sandbox.admin_token().to_owned()),
                    principal: sandbox.principal().to_owned(),
                    device_id: sandbox.device_id().to_owned(),
                    channel: Some("qa".to_owned()),
                })
                .await
                .context("qa.runner.console_login_failed")
        })
        .await?;
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
    cancel_after_admission: bool,
    observed: &mut StreamObservations,
    observation_budget: &mut QaObservationBudget,
    deadline: QaRunDeadline,
) -> Result<()> {
    match line.get("type").and_then(Value::as_str) {
        Some("meta") => {
            observation_budget.consume_serialized(line)?;
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
        Some("event") => {
            process_runtime_event(
                client,
                line,
                approval_steps,
                cancel_after_admission,
                observed,
                observation_budget,
                deadline,
            )
            .await?
        }
        Some("complete") => {
            observation_budget.consume_serialized(line)?;
            observed.complete_status =
                Some(line.get("status").and_then(Value::as_str).unwrap_or("unknown").to_owned());
        }
        Some("error") => {
            observation_budget.consume_serialized(line)?;
            let diagnostic = line
                .get("error")
                .and_then(Value::as_str)
                .unwrap_or("runtime returned an unstructured stream error");
            anyhow::bail!("qa.runner.runtime_stream_error: {diagnostic}")
        }
        _ => anyhow::bail!("qa.runner.unknown_stream_line"),
    }
    Ok(())
}

async fn request_run_cancellation(client: &ControlPlaneClient, run_id: &str) -> Result<()> {
    let response = client
        .post_json_value(
            format!("console/v1/sessions/runs/{run_id}/abort"),
            &json!({"reason": "qa_authoritative_v2_cancel_after_admission"}),
        )
        .await
        .context("qa.runner.run_cancel_failed")?;
    if required_string(&response, "/run_id", "qa.runner.run_cancel_id_missing")? != run_id {
        anyhow::bail!("qa.runner.run_cancel_id_mismatch");
    }
    Ok(())
}

async fn process_runtime_event(
    client: &mut ControlPlaneClient,
    line: &Value,
    approval_steps: &[&QaScenarioStep],
    cancel_after_admission: bool,
    observed: &mut StreamObservations,
    observation_budget: &mut QaObservationBudget,
    deadline: QaRunDeadline,
) -> Result<()> {
    let event = line.get("event").context("qa.runner.stream_event_missing")?;
    if let Some(public_event) = event.get("public_event") {
        if observed.public_events.len() >= MAX_PUBLIC_EVENTS {
            anyhow::bail!("qa.runner.public_event_limit_exceeded");
        }
        observation_budget.consume_serialized(public_event)?;
        observed.public_events.push(QaPublicEventEvidence {
            event_type: required_string(
                public_event,
                "/event",
                "qa.runner.public_event_name_missing",
            )?,
            payload: public_event.clone(),
        });
    }
    let event_type = event.get("event_type").and_then(Value::as_str);
    if should_request_authoritative_v2_cancellation(
        event,
        cancel_after_admission,
        observed.cancellation_requested,
    ) {
        let run_id = observed.run_id.as_deref().context("qa.runner.stream_run_id_missing")?;
        deadline.run_step(request_run_cancellation(client, run_id)).await?;
        observed.cancellation_requested = true;
    }
    match event_type {
        Some("model_token") => {
            if let Some(token) = event.pointer("/model_token/token").and_then(Value::as_str) {
                push_bounded_text(&mut observed.final_answer, token)?;
            }
        }
        Some("tool_proposal") => {
            let proposal = event.get("tool_proposal").context("qa.runner.tool_proposal_missing")?;
            observation_budget.consume_serialized(proposal)?;
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
            let result = event.get("tool_result").context("qa.runner.tool_result_missing")?;
            observation_budget.consume_serialized(result)?;
            let proposal_id = required_string(
                result,
                "/proposal_id",
                "qa.runner.tool_result_proposal_id_missing",
            )?;
            let success =
                required_bool(result, "/success", "qa.runner.tool_result_success_missing")?;
            let name = observed
                .tool_names_by_proposal
                .get(proposal_id.as_str())
                .cloned()
                .context("qa.runner.tool_result_proposal_unknown")?;
            let tool_call =
                QaToolCallEvidence { name, proposal_id: Some(proposal_id), success: Some(success) };
            observation_budget.consume_serialized(&tool_call)?;
            observed.tool_calls.push(tool_call);
        }
        Some("tool_approval_request") => {
            let step = approval_steps
                .get(observed.approval_cursor)
                .copied()
                .context("qa.runner.unexpected_approval_request")?;
            deadline.run_step(decide_approval(client, event, step)).await?;
            observed.approval_cursor = observed.approval_cursor.saturating_add(1);
        }
        _ => {}
    }
    Ok(())
}

fn should_request_authoritative_v2_cancellation(
    event: &Value,
    cancel_after_admission: bool,
    cancellation_requested: bool,
) -> bool {
    cancel_after_admission
        && !cancellation_requested
        && event.get("event_type").and_then(Value::as_str) == Some("status")
        && event.pointer("/status/message").and_then(Value::as_str)
            == Some(RUNTIME_KERNEL_V2_PROVIDER_EFFECT_STARTED_MESSAGE)
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

async fn load_run_metadata_trace(
    client: &ControlPlaneClient,
    run_id: &str,
    observation_budget: &mut QaObservationBudget,
) -> Result<MetadataTraceV1> {
    let response = client
        .get_json_value(format!("console/v1/chat/runs/{run_id}/metadata-trace"))
        .await
        .context("qa.runner.metadata_trace_failed")?;
    let trace = serde_json::from_value::<MetadataTraceV1>(
        response.get("metadata_trace").cloned().context("qa.runner.metadata_trace_missing")?,
    )
    .context("qa.runner.metadata_trace_invalid")?;
    trace.validate_shape().context("qa.runner.metadata_trace_invalid")?;
    observation_budget.consume_serialized(&trace)?;
    Ok(trace)
}

async fn load_run_tape(
    client: &ControlPlaneClient,
    run_id: &str,
    observation_budget: &mut QaObservationBudget,
) -> Result<Vec<QaRunTapeEvent>> {
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
            observation_budget.consume_serialized(event)?;
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

fn transcript_from_tape(
    tape: &[QaRunTapeEvent],
    observation_budget: &mut QaObservationBudget,
) -> Result<Vec<QaTranscriptMessage>> {
    let mut transcript = Vec::new();
    for event in tape {
        if transcript.len() >= MAX_TRANSCRIPT_ROWS.saturating_sub(1) {
            break;
        }
        if event.event_type != "message.received" {
            continue;
        }
        let Some(content) = event.payload.get("text").and_then(Value::as_str) else {
            continue;
        };
        let message = QaTranscriptMessage { role: "user".to_owned(), content: content.to_owned() };
        observation_budget.consume_serialized(&message)?;
        transcript.push(message);
    }
    Ok(transcript)
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
    observation_budget: &mut QaObservationBudget,
) -> Result<Vec<QaArtifactEvidence>> {
    let response = client
        .get_json_value(format!(
            "console/v1/chat/runs/{run_id}/workspace?limit={MAX_WORKSPACE_ARTIFACTS}"
        ))
        .await
        .context("qa.runner.workspace_observation_failed")?;
    parse_workspace_artifacts_response(&response, observation_budget)
}

fn parse_workspace_artifacts_response(
    response: &Value,
    observation_budget: &mut QaObservationBudget,
) -> Result<Vec<QaArtifactEvidence>> {
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
    let mut projected = Vec::with_capacity(artifacts.len());
    for artifact in artifacts {
        observation_budget.consume_serialized(artifact)?;
        projected.push(QaArtifactEvidence {
            path: required_string(artifact, "/path", "qa.runner.artifact_path_missing")?,
            // `change_kind` describes the mutation operation; the artifact
            // category remains workspace so manifest path+kind checks are exact.
            kind: "workspace".to_owned(),
            present: !artifact.get("deleted").and_then(Value::as_bool).unwrap_or(false),
            sha256: artifact.get("content_sha256").and_then(Value::as_str).map(str::to_owned),
            size_bytes: artifact.get("size_bytes").and_then(Value::as_u64),
        });
    }
    Ok(projected)
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

fn required_bool(value: &Value, pointer: &str, code: &'static str) -> Result<bool> {
    value.pointer(pointer).and_then(Value::as_bool).ok_or_else(|| anyhow::anyhow!(code))
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
    fn run_deadline_caps_each_step_to_the_manifest_budget() {
        let deadline =
            QaRunDeadline::from_timeouts(Duration::from_secs(1), Duration::from_millis(50))
                .expect("deadline should be constructible");

        let budget = deadline.step_budget().expect("run should retain time");

        assert!(budget > Duration::ZERO);
        assert!(budget <= Duration::from_millis(50));
    }

    #[test]
    fn exhausted_run_deadline_fails_before_starting_another_step() {
        let deadline = QaRunDeadline::from_timeouts(Duration::ZERO, Duration::from_secs(1))
            .expect("zero-duration deadline should still have a stable representation");

        let error = deadline
            .step_budget()
            .expect_err("an exhausted run must reject another external operation");

        assert_eq!(error.to_string(), "qa.runner.run_timeout");
    }

    #[tokio::test]
    async fn step_deadline_cancels_a_pending_external_operation() {
        let deadline =
            QaRunDeadline::from_timeouts(Duration::from_secs(1), Duration::from_millis(10))
                .expect("deadline should be constructible");
        let started = Instant::now();

        let error = deadline
            .run_step(std::future::pending::<Result<()>>())
            .await
            .expect_err("a pending operation must consume only its step budget");

        assert_eq!(error.to_string(), "qa.runner.step_timeout");
        assert!(started.elapsed() < Duration::from_millis(500));
    }

    #[test]
    fn observation_budget_rejects_before_the_count_limit_without_committing_bytes() {
        let row = json!({"event": "x".repeat(64)});
        let mut counter = SerializedByteCounter::default();
        serde_json::to_writer(&mut counter, &row).expect("row should serialize compactly");
        let mut budget = QaObservationBudget::new(counter.bytes.saturating_mul(2) - 1);

        budget.consume_serialized(&row).expect("first retained row should fit");
        let consumed_before_rejection = budget.consumed_bytes;
        let error = budget
            .consume_serialized(&row)
            .expect_err("aggregate bytes must fail before a count cap is reached");

        assert_eq!(error.to_string(), "qa.runner.evidence_byte_limit_exceeded");
        assert_eq!(budget.consumed_bytes, consumed_before_rejection);
    }

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

        let mut budget = QaObservationBudget::default();
        assert_eq!(
            transcript_from_tape(tape.as_slice(), &mut budget)
                .expect("bounded transcript should project"),
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
    fn tool_result_success_must_be_an_explicit_boolean() {
        for result in [json!({}), json!({"success": "false"}), json!({"success": null})] {
            let error = required_bool(&result, "/success", "qa.runner.tool_result_success_missing")
                .expect_err("missing and non-boolean outcomes must remain unknown");
            assert_eq!(error.to_string(), "qa.runner.tool_result_success_missing");
        }
        assert!(!required_bool(
            &json!({"success": false}),
            "/success",
            "qa.runner.tool_result_success_missing",
        )
        .expect("an explicit false result should be retained"));
    }

    #[tokio::test]
    async fn tool_result_payload_is_budgeted_before_projection_validation() {
        let mut client =
            ControlPlaneClient::new(ControlPlaneClientConfig::new("http://127.0.0.1:1"))
                .expect("local control-plane client should construct without connecting");
        let mut observed = StreamObservations::default();
        observed.tool_names_by_proposal.insert("proposal-1".to_owned(), "tool.test".to_owned());
        let mut budget = QaObservationBudget::new(1_024);
        let deadline = QaRunDeadline::from_timeouts(Duration::from_secs(1), Duration::from_secs(1))
            .expect("deadline should be constructible");
        let line = json!({
            "event": {
                "event_type": "tool_result",
                "tool_result": {"proposal_id": "proposal-1"},
            }
        });

        let error = process_runtime_event(
            &mut client,
            &line,
            &[],
            false,
            &mut observed,
            &mut budget,
            deadline,
        )
        .await
        .expect_err("missing success must fail after the payload is accounted");

        assert_eq!(error.to_string(), "qa.runner.tool_result_success_missing");
        assert!(budget.consumed_bytes > 0);
        assert!(observed.tool_calls.is_empty());
    }

    #[tokio::test]
    async fn tool_result_requires_a_known_proposal_mapping() {
        let mut client =
            ControlPlaneClient::new(ControlPlaneClientConfig::new("http://127.0.0.1:1"))
                .expect("local control-plane client should construct without connecting");
        let mut observed = StreamObservations::default();
        let mut budget = QaObservationBudget::new(1_024);
        let deadline = QaRunDeadline::from_timeouts(Duration::from_secs(1), Duration::from_secs(1))
            .expect("deadline should be constructible");
        let line = json!({
            "event": {
                "event_type": "tool_result",
                "tool_result": {"proposal_id": "proposal-unknown", "success": false},
            }
        });

        let error = process_runtime_event(
            &mut client,
            &line,
            &[],
            false,
            &mut observed,
            &mut budget,
            deadline,
        )
        .await
        .expect_err("an uncorrelated tool result must not acquire a synthetic tool name");

        assert_eq!(error.to_string(), "qa.runner.tool_result_proposal_unknown");
        assert!(budget.consumed_bytes > 0);
        assert!(observed.tool_calls.is_empty());
    }

    #[test]
    fn authoritative_v2_cancellation_uses_the_provider_effect_boundary() {
        let provider_started = json!({
            "event_type": "status",
            "status": {"message": RUNTIME_KERNEL_V2_PROVIDER_EFFECT_STARTED_MESSAGE},
        });
        let tape_only_selection = json!({"event_type": "runtime.authority.selected"});

        assert!(should_request_authoritative_v2_cancellation(&provider_started, true, false));
        assert!(!should_request_authoritative_v2_cancellation(&provider_started, false, false));
        assert!(!should_request_authoritative_v2_cancellation(&provider_started, true, true));
        assert!(!should_request_authoritative_v2_cancellation(&tape_only_selection, true, false));
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

        let mut budget = QaObservationBudget::default();
        let error = parse_workspace_artifacts_response(&response, &mut budget)
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

        let mut budget = QaObservationBudget::default();
        let artifacts = parse_workspace_artifacts_response(&response, &mut budget)
            .expect("a complete bounded artifact set should parse");

        assert_eq!(artifacts.len(), 1);
        assert_eq!(artifacts[0].path, "src/output.txt");
    }
}
