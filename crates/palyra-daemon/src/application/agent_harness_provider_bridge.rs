//! Provider-response projection for authoritative external agent harnesses.
//!
//! This adapter keeps model-visible output on the existing run-stream path while
//! every host callback remains capability-scoped, bounded, and fail-closed.

use std::{
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;
use palyra_model_providers::provider_output_from_text_and_tools;
use serde_json::{json, Value};
use tonic::Status;

use super::{
    agent_harness_host::{
        GuardedHarnessHost, HarnessCancellationContext, HarnessCapabilityStore, HarnessHostBackend,
        HarnessHostError, HarnessHostOperation,
    },
    agent_harness_v2::{
        execute_agent_harness_v2, AgentHarnessAcceptedV2, AgentHarnessAttemptRequestV2,
        AgentHarnessEventKindV2, AgentHarnessEventSinkV2, AgentHarnessEventV2,
        AgentHarnessTerminalOutcomeV2, AgentHarnessTerminalReceiptV2, AgentHarnessTerminalV2,
        AgentHarnessV2,
    },
};
use crate::model_provider::{
    provider_events_from_output, ProviderAttemptSummary, ProviderEvent, ProviderFinishReason,
    ProviderRawProviderRefs, ProviderRequest, ProviderResponse, ProviderTurnOutput, ProviderUsage,
};

/// Owned inputs required to execute one external harness as a provider turn.
pub(crate) struct ExternalHarnessProviderTurn {
    pub(crate) harness: Arc<dyn AgentHarnessV2>,
    pub(crate) run_id: String,
    pub(crate) session_id: String,
    pub(crate) generation: u64,
    pub(crate) provider_id: String,
    pub(crate) model_id: String,
    pub(crate) trace_context: String,
    pub(crate) provider_request: ProviderRequest,
    pub(crate) cancellation: HarnessCancellationContext,
    pub(crate) deadline_unix_ms: i64,
}

/// Executes an external harness and projects its terminal output as a normal provider response.
#[allow(clippy::result_large_err)]
pub(crate) async fn execute_external_harness_provider_turn(
    turn: ExternalHarnessProviderTurn,
) -> Result<ProviderResponse, Status> {
    if turn.generation == 0 || turn.deadline_unix_ms <= now_unix_ms() {
        return Err(Status::deadline_exceeded(
            "external agent harness provider turn deadline expired",
        ));
    }
    let harness_id = turn.harness.descriptor().id.clone();
    let capabilities = Arc::new(HarnessCapabilityStore::default());
    let capability = capabilities
        .issue(
            harness_id.as_str(),
            turn.generation,
            vec![
                HarnessHostOperation::GetRuntimeContext,
                HarnessHostOperation::RequestModelTurn,
                HarnessHostOperation::ProposeToolCall,
                HarnessHostOperation::AwaitToolOutcome,
                HarnessHostOperation::EmitTextDelta,
                HarnessHostOperation::EmitProgress,
                HarnessHostOperation::RequestCompaction,
                HarnessHostOperation::SideQuestion,
                HarnessHostOperation::CreateArtifact,
                HarnessHostOperation::Checkpoint,
                HarnessHostOperation::Heartbeat,
            ],
            turn.deadline_unix_ms,
        )
        .map_err(|error| {
            Status::failed_precondition(format!(
                "failed to issue external harness host capability: {}",
                error.reason_code()
            ))
        })?;
    let tool_surface =
        turn.provider_request.tool_catalog_snapshot.clone().unwrap_or_else(|| json!({"tools": []}));
    let sanitized_transcript = serde_json::to_value(turn.provider_request.effective_messages())
        .ok()
        .and_then(|value| value.as_array().cloned())
        .ok_or_else(|| Status::internal("failed to project external harness transcript"))?;
    let context_token_budget =
        turn.provider_request.max_output_tokens.unwrap_or(8_192).saturating_mul(4).max(8_192);
    let workspace_root =
        std::env::current_dir().ok().map(|path| path.to_string_lossy().into_owned());
    let request = AgentHarnessAttemptRequestV2 {
        run_id: turn.run_id,
        session_id: turn.session_id,
        generation: turn.generation,
        deadline_unix_ms: turn.deadline_unix_ms,
        provider_id: turn.provider_id.clone(),
        model_id: turn.model_id.clone(),
        context_token_budget,
        reasoning_policy: turn
            .provider_request
            .reasoning_effort
            .as_ref()
            .and_then(|effort| serde_json::to_value(effort).ok())
            .and_then(|value| value.as_str().map(str::to_owned)),
        sanitized_transcript,
        tool_surface: tool_surface.clone(),
        tool_catalog_epoch: turn.generation,
        workspace_root,
        sandbox: "host_owned".to_owned(),
        trace_context: turn.trace_context,
        host_capability: capability,
        cancellation: turn.cancellation.clone(),
    };
    let backend = Arc::new(RunStreamHarnessHostBackend {
        runtime_context: json!({
            "run_id_sha256": crate::sha256_hex(request.run_id.as_bytes()),
            "session_id_sha256": crate::sha256_hex(request.session_id.as_bytes()),
            "generation": request.generation,
            "provider_id": request.provider_id,
            "model_id": request.model_id,
            "sandbox": request.sandbox,
            "tool_catalog_epoch": request.tool_catalog_epoch,
        }),
        tool_surface,
    });
    let host =
        GuardedHarnessHost::new(backend, capabilities, turn.cancellation, Duration::from_secs(30));
    let (_, sink) = execute_agent_harness_v2(
        turn.harness.as_ref(),
        &request,
        &host,
        ProviderProjectionSink::default(),
    )
    .await
    .map_err(|error| {
        Status::unavailable(format!("external agent harness provider turn failed: {error}"))
    })?;
    sink.into_provider_response(turn.provider_id, turn.model_id)
}

#[derive(Debug)]
struct RunStreamHarnessHostBackend {
    runtime_context: Value,
    tool_surface: Value,
}

#[async_trait]
impl HarnessHostBackend for RunStreamHarnessHostBackend {
    async fn invoke(
        &self,
        operation: HarnessHostOperation,
        payload: Value,
        _cancellation: HarnessCancellationContext,
    ) -> Result<Value, HarnessHostError> {
        Ok(match operation {
            HarnessHostOperation::GetRuntimeContext => self.runtime_context.clone(),
            HarnessHostOperation::ProposeToolCall => {
                let tool_name =
                    payload.get("tool_name").and_then(Value::as_str).unwrap_or_default();
                if tool_visible_in_surface(&self.tool_surface, tool_name) {
                    // The provider callback cannot borrow the run-owned stream
                    // and approval state. Projecting the proposal keeps the
                    // existing run-stream tool loop as the sole executor.
                    json!({
                        "ok": true,
                        "deferred_to_run_stream": true,
                        "safe_message": "Palyra accepted this proposal for its canonical host-owned tool loop; the authoritative result will be projected in the next turn.",
                        "reason_code": "harness.tool.deferred_to_run_stream",
                    })
                } else {
                    json!({
                        "ok": false,
                        "safe_message": "The requested tool is not visible in the pinned catalog.",
                        "reason_code": "harness.tool.not_visible",
                    })
                }
            }
            HarnessHostOperation::AwaitToolOutcome => json!({
                "ok": false,
                "safe_message": "No host-authorized inline tool outcome is available.",
            }),
            HarnessHostOperation::RequestModelTurn => {
                json!({"available": false, "reason_code": "harness.model_turn.host_owned"})
            }
            HarnessHostOperation::RequestCompaction => {
                json!({"compacted": false, "reason_code": "harness.compaction.deferred"})
            }
            HarnessHostOperation::SideQuestion => json!({"answer": ""}),
            HarnessHostOperation::CreateArtifact => {
                json!({"created": false, "reason_code": "harness.artifact.host_owned"})
            }
            HarnessHostOperation::EmitTextDelta
            | HarnessHostOperation::EmitProgress
            | HarnessHostOperation::Checkpoint
            | HarnessHostOperation::Heartbeat => Value::Null,
        })
    }
}

#[derive(Debug, Default)]
struct ProviderProjectionSink {
    accepted: bool,
    event_count: usize,
    text: String,
    tool_proposals: Vec<ProviderEvent>,
    prompt_tokens: u64,
    completion_tokens: u64,
    terminal: Option<AgentHarnessTerminalV2>,
}

#[async_trait]
impl AgentHarnessEventSinkV2 for ProviderProjectionSink {
    async fn accepted(
        &mut self,
        _accepted: AgentHarnessAcceptedV2,
    ) -> Result<(), super::agent_harness_v2::AgentHarnessV2Error> {
        self.accepted = true;
        Ok(())
    }

    async fn event(
        &mut self,
        event: AgentHarnessEventV2,
    ) -> Result<(), super::agent_harness_v2::AgentHarnessV2Error> {
        self.event_count = self.event_count.saturating_add(1);
        match event.event {
            AgentHarnessEventKindV2::TextDelta { text } => self.text.push_str(text.as_str()),
            AgentHarnessEventKindV2::ToolProposed { call_id, tool_name, input_json } => {
                let input_json = serde_json::to_vec(&input_json)
                    .map_err(|_| super::agent_harness_v2::AgentHarnessV2Error::InvalidEvent)?;
                self.tool_proposals.push(ProviderEvent::ToolProposal {
                    proposal_id: call_id,
                    tool_name,
                    input_json,
                });
            }
            AgentHarnessEventKindV2::Usage { prompt_tokens, completion_tokens } => {
                self.prompt_tokens = self.prompt_tokens.saturating_add(prompt_tokens);
                self.completion_tokens = self.completion_tokens.saturating_add(completion_tokens);
            }
            _ => {}
        }
        Ok(())
    }

    async fn terminal(
        &mut self,
        terminal: AgentHarnessTerminalV2,
    ) -> Result<AgentHarnessTerminalReceiptV2, super::agent_harness_v2::AgentHarnessV2Error> {
        self.terminal = Some(terminal.clone());
        Ok(AgentHarnessTerminalReceiptV2 {
            generation: terminal.generation,
            terminal_sequence: terminal.sequence,
            event_count: self.event_count,
        })
    }
}

impl ProviderProjectionSink {
    #[allow(clippy::result_large_err)]
    fn into_provider_response(
        self,
        provider_id: String,
        model_id: String,
    ) -> Result<ProviderResponse, Status> {
        if !self.accepted {
            return Err(Status::unavailable(
                "external agent harness did not accept the provider turn",
            ));
        }
        let terminal = self
            .terminal
            .ok_or_else(|| Status::unavailable("external agent harness omitted its terminal"))?;
        let final_text = match terminal.outcome {
            AgentHarnessTerminalOutcomeV2::Completed { final_message } => {
                final_message.unwrap_or(self.text)
            }
            AgentHarnessTerminalOutcomeV2::Blocked { reason_code } => {
                return Err(Status::failed_precondition(format!(
                    "external agent harness blocked the provider turn: {reason_code}"
                )));
            }
            AgentHarnessTerminalOutcomeV2::Failed { reason_code, safe_message } => {
                return Err(Status::unavailable(format!(
                    "external agent harness failed the provider turn ({reason_code}): {safe_message}"
                )));
            }
            AgentHarnessTerminalOutcomeV2::Cancelled { reason_code } => {
                return Err(Status::cancelled(format!(
                    "external agent harness cancelled the provider turn: {reason_code}"
                )));
            }
            AgentHarnessTerminalOutcomeV2::TimedOut { reason_code } => {
                return Err(Status::deadline_exceeded(format!(
                    "external agent harness timed out: {reason_code}"
                )));
            }
        };
        let usage =
            ProviderUsage::new(self.prompt_tokens, self.completion_tokens, "agent_harness_v2");
        let output = if self.tool_proposals.is_empty() {
            ProviderTurnOutput::text(
                final_text,
                ProviderFinishReason::Stop,
                usage,
                ProviderRawProviderRefs::default(),
            )
        } else {
            provider_output_from_text_and_tools(
                final_text,
                self.tool_proposals,
                ProviderFinishReason::ToolCalls,
                usage,
                ProviderRawProviderRefs::default(),
            )
        };
        let events = provider_events_from_output(&output);
        Ok(ProviderResponse {
            output,
            events,
            prompt_tokens: self.prompt_tokens,
            completion_tokens: self.completion_tokens,
            retry_count: 0,
            provider_id: provider_id.clone(),
            model_id: model_id.clone(),
            served_from_cache: false,
            failover_count: 0,
            attempts: vec![ProviderAttemptSummary {
                provider_id,
                model_id,
                outcome: "completed".to_owned(),
                retryable: false,
                served_from_cache: false,
                reason_code: None,
                state: None,
            }],
            qa_lane_attestation: None,
        })
    }
}

fn tool_visible_in_surface(surface: &Value, tool_name: &str) -> bool {
    if tool_name.trim().is_empty() {
        return false;
    }
    let tools = surface.get("tools").and_then(Value::as_array).or_else(|| surface.as_array());
    tools.is_some_and(|tools| {
        tools.iter().any(|tool| {
            tool.get("name").and_then(Value::as_str) == Some(tool_name)
                || tool.pointer("/function/name").and_then(Value::as_str) == Some(tool_name)
        })
    })
}

fn now_unix_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| i64::try_from(duration.as_millis()).unwrap_or(i64::MAX))
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model_provider::ProviderOutputContentPart;

    #[tokio::test]
    async fn visible_tool_proposals_are_deferred_to_the_canonical_run_stream_loop() {
        let backend = RunStreamHarnessHostBackend {
            runtime_context: json!({}),
            tool_surface: json!({
                "tools": [{
                    "name": "palyra.fixture",
                    "inputSchema": {"type": "object"}
                }]
            }),
        };
        let (_cancel, cancellation) = HarnessCancellationContext::channel();

        let outcome = backend
            .invoke(
                HarnessHostOperation::ProposeToolCall,
                json!({"tool_name": "palyra.fixture"}),
                cancellation,
            )
            .await
            .expect("visible proposal decision");

        assert_eq!(outcome["ok"], true);
        assert_eq!(outcome["deferred_to_run_stream"], true);
        assert_eq!(outcome["reason_code"], "harness.tool.deferred_to_run_stream");
    }

    #[tokio::test]
    async fn projected_tool_proposal_preserves_pinned_arguments_for_host_execution() {
        let mut sink = ProviderProjectionSink::default();
        sink.accepted(AgentHarnessAcceptedV2 { generation: 7, sequence: 1 })
            .await
            .expect("accepted projection");
        sink.event(AgentHarnessEventV2 {
            generation: 7,
            sequence: 2,
            event: AgentHarnessEventKindV2::ToolProposed {
                call_id: "call-7".to_owned(),
                tool_name: "palyra.fixture".to_owned(),
                input_json: json!({"value": 7}),
            },
        })
        .await
        .expect("tool projection");
        sink.terminal(AgentHarnessTerminalV2 {
            generation: 7,
            sequence: 3,
            outcome: AgentHarnessTerminalOutcomeV2::Completed { final_message: None },
        })
        .await
        .expect("terminal projection");

        let response = sink
            .into_provider_response("fixture-provider".to_owned(), "fixture-model".to_owned())
            .expect("provider response");

        assert_eq!(response.output.finish_reason, ProviderFinishReason::ToolCalls);
        assert!(response.output.content_parts.iter().any(|part| matches!(
            part,
            ProviderOutputContentPart::ToolCall {
                proposal_id,
                tool_name,
                input_json,
            } if proposal_id == "call-7"
                && tool_name == "palyra.fixture"
                && input_json == &json!({"value": 7})
        )));
    }
}
