//! AgentHarnessV2 adapter for the shared managed-runtime transport.
//!
//! Child observations are projected through HarnessHost before they reach the
//! ordered sink, so external execution cannot bypass tool or delivery authority.

use std::{
    sync::{Arc, Mutex},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;
use serde_json::{json, Value};

use super::{
    agent_harness::AgentHarnessDescriptor,
    agent_harness_host::{HarnessCallId, HarnessHost, HarnessHostCallContext},
    agent_harness_v2::{
        AgentHarnessAcceptedV2, AgentHarnessAttemptRequestV2, AgentHarnessEventKindV2,
        AgentHarnessEventSinkV2, AgentHarnessEventV2, AgentHarnessHealthV2,
        AgentHarnessSteerOutcomeV2, AgentHarnessTerminalOutcomeV2, AgentHarnessTerminalReceiptV2,
        AgentHarnessTerminalV2, AgentHarnessV2, AgentHarnessV2Error,
    },
    managed_runtime::{
        ManagedRuntimeHealthState, ManagedRuntimeStartRequest, RuntimeTransport,
        RuntimeTransportCommand, RuntimeTransportError, RuntimeTransportEvent,
    },
};

/// Process-backed external harness using a reusable transport implementation.
pub struct ManagedExternalAgentHarness<Transport> {
    descriptor: AgentHarnessDescriptor,
    transport: Arc<Transport>,
    resume_metadata_json: Mutex<Option<String>>,
}

enum EventProjectionOutcome {
    Projected(Result<(), AgentHarnessV2Error>),
    Cancelled,
    TimedOut,
}

struct RuntimeEventProjection {
    command_id: String,
    generation: u64,
    sequence: u64,
    method: String,
    payload: Value,
}

impl<Transport> ManagedExternalAgentHarness<Transport>
where
    Transport: RuntimeTransport,
{
    /// Constructs a process-backed harness.
    #[must_use]
    pub fn new(descriptor: AgentHarnessDescriptor, transport: Arc<Transport>) -> Self {
        Self { descriptor, transport, resume_metadata_json: Mutex::new(None) }
    }

    fn call_context(
        &self,
        request: &AgentHarnessAttemptRequestV2,
        sequence: u64,
    ) -> Result<HarnessHostCallContext, AgentHarnessV2Error> {
        Ok(HarnessHostCallContext {
            call_id: HarnessCallId::parse(format!("{}-{}", request.run_id, sequence))?,
            harness_id: self.descriptor.id.clone(),
            generation: request.generation,
            deadline_unix_ms: request.deadline_unix_ms.min(now_unix_ms().saturating_add(30_000)),
            capability: request.host_capability.clone(),
        })
    }

    async fn project_event(
        &self,
        request: &AgentHarnessAttemptRequestV2,
        host: &dyn HarnessHost,
        sink: &mut dyn AgentHarnessEventSinkV2,
        projection: RuntimeEventProjection,
    ) -> Result<(), AgentHarnessV2Error> {
        let RuntimeEventProjection { command_id, generation, sequence, method, payload } =
            projection;
        let event = match method.as_str() {
            "text_delta" => {
                let text = payload
                    .get("text")
                    .and_then(Value::as_str)
                    .ok_or(AgentHarnessV2Error::Transport {
                        reason_code: "harness.transport.invalid_text_delta".to_owned(),
                    })?
                    .to_owned();
                host.emit_text_delta(
                    self.call_context(request, sequence)?,
                    json!({"command_id": command_id, "text": text}),
                )
                .await?;
                AgentHarnessEventKindV2::TextDelta { text }
            }
            "progress" => {
                let completed_units = payload
                    .get("completed_units")
                    .and_then(Value::as_u64)
                    .ok_or(AgentHarnessV2Error::Transport {
                        reason_code: "harness.transport.invalid_progress".to_owned(),
                    })?;
                let total_units = payload.get("total_units").and_then(Value::as_u64).ok_or(
                    AgentHarnessV2Error::Transport {
                        reason_code: "harness.transport.invalid_progress".to_owned(),
                    },
                )?;
                let label = payload
                    .get("label")
                    .and_then(Value::as_str)
                    .unwrap_or("external_runtime")
                    .to_owned();
                host.emit_progress(
                    self.call_context(request, sequence)?,
                    json!({
                        "command_id": command_id,
                        "completed_units": completed_units,
                        "total_units": total_units,
                        "label": label,
                    }),
                )
                .await?;
                AgentHarnessEventKindV2::Progress { completed_units, total_units, label }
            }
            "tool_proposed" => {
                let call_id = payload
                    .get("call_id")
                    .and_then(Value::as_str)
                    .ok_or(AgentHarnessV2Error::Transport {
                        reason_code: "harness.transport.invalid_tool_proposal".to_owned(),
                    })?
                    .to_owned();
                let tool_name = payload
                    .get("tool_name")
                    .and_then(Value::as_str)
                    .ok_or(AgentHarnessV2Error::Transport {
                        reason_code: "harness.transport.invalid_tool_proposal".to_owned(),
                    })?
                    .to_owned();
                let input_json = payload.get("arguments").cloned().unwrap_or_else(|| json!({}));
                let host_result = host
                    .propose_tool_call(self.call_context(request, sequence)?, payload.clone())
                    .await?;
                self.forward_host_response(
                    request,
                    sequence,
                    &payload,
                    normalize_dynamic_tool_result(&host_result),
                )
                .await?;
                AgentHarnessEventKindV2::ToolProposed { call_id, tool_name, input_json }
            }
            "tool_outcome" => {
                let call_id = required_string(&payload, "call_id")?;
                let outcome = required_string(&payload, "outcome")?;
                AgentHarnessEventKindV2::ToolOutcome { call_id, outcome }
            }
            "approval_required" => {
                let call_id = required_string(&payload, "call_id")?;
                let approval_id = required_string(&payload, "approval_id")?;
                let host_result = host
                    .await_tool_outcome(self.call_context(request, sequence)?, payload.clone())
                    .await?;
                self.forward_host_response(
                    request,
                    sequence,
                    &payload,
                    normalize_approval_result(&host_result),
                )
                .await?;
                AgentHarnessEventKindV2::ApprovalRequired { call_id, approval_id }
            }
            "approval_resolved" => {
                let approval_id = required_string(&payload, "approval_id")?;
                let outcome = required_string(&payload, "outcome")?;
                AgentHarnessEventKindV2::ApprovalResolved { approval_id, outcome }
            }
            "side_question" => {
                let question_id = required_string(&payload, "question_id")?;
                let host_result = host
                    .side_question(self.call_context(request, sequence)?, payload.clone())
                    .await?;
                self.forward_host_response(
                    request,
                    sequence,
                    &payload,
                    normalize_side_question_result(question_id.as_str(), &host_result),
                )
                .await?;
                AgentHarnessEventKindV2::SideQuestionRequested { question_id }
            }
            "reasoning_metadata" => {
                let summary = required_string(&payload, "summary")?;
                AgentHarnessEventKindV2::ReasoningMetadata { summary }
            }
            "usage" => {
                let prompt_tokens =
                    payload.get("prompt_tokens").and_then(Value::as_u64).unwrap_or_default();
                let completion_tokens =
                    payload.get("completion_tokens").and_then(Value::as_u64).unwrap_or_default();
                AgentHarnessEventKindV2::Usage { prompt_tokens, completion_tokens }
            }
            "compaction_requested" => {
                host.request_compaction(self.call_context(request, sequence)?, payload).await?;
                AgentHarnessEventKindV2::CompactionRequested
            }
            "compaction_completed" => AgentHarnessEventKindV2::CompactionCompleted,
            "heartbeat" => {
                let ordinal = payload.get("ordinal").and_then(Value::as_u64).unwrap_or(sequence);
                host.heartbeat(self.call_context(request, sequence)?, payload).await?;
                AgentHarnessEventKindV2::Heartbeat { ordinal }
            }
            _ => {
                return Err(AgentHarnessV2Error::Transport {
                    reason_code: "harness.transport.unknown_event".to_owned(),
                });
            }
        };
        sink.event(AgentHarnessEventV2 { generation, sequence, event }).await
    }

    async fn forward_host_response(
        &self,
        request: &AgentHarnessAttemptRequestV2,
        sequence: u64,
        source: &Value,
        result: Value,
    ) -> Result<(), AgentHarnessV2Error> {
        let request_id =
            source.get("request_id").cloned().ok_or(AgentHarnessV2Error::Transport {
                reason_code: "harness.transport.missing_host_request_id".to_owned(),
            })?;
        self.transport
            .send_command(RuntimeTransportCommand {
                command_id: format!("host-response-{sequence}"),
                generation: request.generation,
                method: "host_response".to_owned(),
                payload: json!({
                    "request_id": request_id,
                    "request_kind": source.get("request_kind").cloned().unwrap_or(Value::Null),
                    "call_id": source.get("call_id").cloned().unwrap_or(Value::Null),
                    "approval_id": source.get("approval_id").cloned().unwrap_or(Value::Null),
                    "result": result,
                }),
                deadline_unix_ms: request
                    .deadline_unix_ms
                    .min(now_unix_ms().saturating_add(30_000)),
            })
            .await?;
        Ok(())
    }

    async fn terminalize_cancelled(
        &self,
        request: &AgentHarnessAttemptRequestV2,
        sink: &mut dyn AgentHarnessEventSinkV2,
        last_sink_sequence: u64,
    ) -> Result<AgentHarnessTerminalReceiptV2, AgentHarnessV2Error> {
        let _ = self.transport.cancel(request.run_id.as_str(), request.generation).await;
        let event_sequence =
            last_sink_sequence.checked_add(1).ok_or(AgentHarnessV2Error::InvalidEvent)?;
        sink.event(AgentHarnessEventV2 {
            generation: request.generation,
            sequence: event_sequence,
            event: AgentHarnessEventKindV2::CancellationObserved,
        })
        .await?;
        let terminal_sequence =
            event_sequence.checked_add(1).ok_or(AgentHarnessV2Error::InvalidTerminal)?;
        sink.terminal(AgentHarnessTerminalV2 {
            generation: request.generation,
            sequence: terminal_sequence,
            outcome: AgentHarnessTerminalOutcomeV2::Cancelled {
                reason_code: "harness.external.cancelled".to_owned(),
            },
        })
        .await
    }

    async fn terminalize_timed_out(
        &self,
        request: &AgentHarnessAttemptRequestV2,
        sink: &mut dyn AgentHarnessEventSinkV2,
        last_sink_sequence: u64,
    ) -> Result<AgentHarnessTerminalReceiptV2, AgentHarnessV2Error> {
        let _ = self.transport.cancel(request.run_id.as_str(), request.generation).await;
        let terminal_sequence =
            last_sink_sequence.checked_add(1).ok_or(AgentHarnessV2Error::InvalidTerminal)?;
        sink.terminal(AgentHarnessTerminalV2 {
            generation: request.generation,
            sequence: terminal_sequence,
            outcome: AgentHarnessTerminalOutcomeV2::TimedOut {
                reason_code: "harness.external.deadline_exceeded".to_owned(),
            },
        })
        .await
    }

    async fn settle_transport(&self) {
        let _ = self.transport.close().await;
    }
}

#[async_trait]
impl<Transport> AgentHarnessV2 for ManagedExternalAgentHarness<Transport>
where
    Transport: RuntimeTransport + 'static,
{
    fn descriptor(&self) -> &AgentHarnessDescriptor {
        &self.descriptor
    }

    async fn run_attempt(
        &self,
        request: &AgentHarnessAttemptRequestV2,
        host: &dyn HarnessHost,
        sink: &mut dyn AgentHarnessEventSinkV2,
    ) -> Result<AgentHarnessTerminalReceiptV2, AgentHarnessV2Error> {
        let resume_metadata_json = {
            self.resume_metadata_json
                .lock()
                .map_err(|_| AgentHarnessV2Error::Transport {
                    reason_code: "harness.transport.resume_state_unavailable".to_owned(),
                })?
                .clone()
                .or_else(|| {
                    Some(
                        json!({
                            "harness_id": self.descriptor.id,
                            "tool_catalog_epoch": request.tool_catalog_epoch,
                        })
                        .to_string(),
                    )
                })
        };
        self.transport
            .start(ManagedRuntimeStartRequest {
                session_id: request.session_id.clone(),
                generation: request.generation,
                resume_metadata_json,
            })
            .await?;
        let mut events = self.transport.event_stream()?;
        sink.accepted(AgentHarnessAcceptedV2 { generation: request.generation, sequence: 1 })
            .await?;
        let command_result = self
            .transport
            .send_command(RuntimeTransportCommand {
                command_id: request.run_id.clone(),
                generation: request.generation,
                method: "run_attempt".to_owned(),
                payload: json!({
                    "run_id": request.run_id,
                    "session_id": request.session_id,
                    "provider_id": request.provider_id,
                    "model_id": request.model_id,
                    "context_token_budget": request.context_token_budget,
                    "reasoning_policy": request.reasoning_policy,
                    "sanitized_transcript": request.sanitized_transcript,
                    "tool_surface": request.tool_surface,
                    "tool_catalog_epoch": request.tool_catalog_epoch,
                    "workspace_root": request.workspace_root,
                    "sandbox": request.sandbox,
                    "trace_context": request.trace_context,
                }),
                deadline_unix_ms: request.deadline_unix_ms,
            })
            .await;
        if let Err(error) = command_result {
            // Runtime startup is part of the attempt budget, so an exhausted
            // deadline terminalizes the accepted attempt instead of leaking a
            // transport validation error.
            if request.deadline_unix_ms <= now_unix_ms() {
                let receipt = sink
                    .terminal(AgentHarnessTerminalV2 {
                        generation: request.generation,
                        sequence: 2,
                        outcome: AgentHarnessTerminalOutcomeV2::TimedOut {
                            reason_code: "harness.external.deadline_exceeded".to_owned(),
                        },
                    })
                    .await?;
                self.settle_transport().await;
                return Ok(receipt);
            }
            self.settle_transport().await;
            return Err(error.into());
        }
        let mut cancellation = request.cancellation.clone();
        let mut last_sink_sequence = 1_u64;
        let deadline_ms = request.deadline_unix_ms.saturating_sub(now_unix_ms());
        let deadline =
            tokio::time::sleep(Duration::from_millis(u64::try_from(deadline_ms).unwrap_or(0)));
        tokio::pin!(deadline);
        let outcome = async {
            loop {
                tokio::select! {
                _ = cancellation.cancelled() => {
                    return self
                        .terminalize_cancelled(request, sink, last_sink_sequence)
                        .await;
                }
                _ = &mut deadline => {
                    return self
                        .terminalize_timed_out(request, sink, last_sink_sequence)
                        .await;
                }
                event = events.recv() => {
                    let event = event.map_err(|_| AgentHarnessV2Error::Transport {
                        reason_code: "harness.transport.event_stream_closed".to_owned(),
                    })?;
                    match event {
                        RuntimeTransportEvent::Accepted { generation, .. } => {
                            if generation != request.generation {
                                self.settle_transport().await;
                                return Err(AgentHarnessV2Error::StaleGeneration {
                                    active: request.generation,
                                    observed: generation,
                                });
                            }
                        }
                        RuntimeTransportEvent::Event {
                            command_id,
                            generation,
                            sequence,
                            method,
                            payload,
                        } => {
                            let projection_outcome = {
                                let projection = self.project_event(
                                    request,
                                    host,
                                    sink,
                                    RuntimeEventProjection {
                                        command_id,
                                        generation,
                                        sequence,
                                        method,
                                        payload,
                                    },
                                );
                                tokio::pin!(projection);
                                tokio::select! {
                                    biased;
                                    _ = cancellation.cancelled() => {
                                        EventProjectionOutcome::Cancelled
                                    }
                                    _ = &mut deadline => {
                                        EventProjectionOutcome::TimedOut
                                    }
                                    result = &mut projection => {
                                        EventProjectionOutcome::Projected(result)
                                    }
                                }
                            };
                            match projection_outcome {
                                EventProjectionOutcome::Projected(result) => result?,
                                EventProjectionOutcome::Cancelled => {
                                    return self
                                        .terminalize_cancelled(request, sink, last_sink_sequence)
                                        .await;
                                }
                                EventProjectionOutcome::TimedOut => {
                                    return self
                                        .terminalize_timed_out(request, sink, last_sink_sequence)
                                        .await;
                                }
                            }
                            last_sink_sequence = sequence;
                        }
                        RuntimeTransportEvent::Terminal {
                            generation,
                            sequence,
                            outcome,
                            payload,
                            ..
                        } => {
                            let terminal_outcome = terminal_outcome(outcome.as_str(), &payload)?;
                            if let Some(binding) = self.transport.binding()? {
                                *self.resume_metadata_json.lock().map_err(|_| {
                                    AgentHarnessV2Error::Transport {
                                        reason_code:
                                            "harness.transport.resume_state_unavailable".to_owned(),
                                    }
                                })? = binding.resume_metadata_json.clone();
                                host.checkpoint(
                                    self.call_context(request, sequence)?,
                                    json!({
                                        "runtime_id": binding.runtime_id,
                                        "generation": binding.generation,
                                        "resume_metadata_json": binding.resume_metadata_json,
                                        "last_acknowledged_sequence":
                                            binding.last_acknowledged_sequence,
                                    }),
                                )
                                .await?;
                            }
                            let receipt = sink.terminal(AgentHarnessTerminalV2 {
                                generation,
                                sequence,
                                outcome: terminal_outcome,
                            }).await?;
                            self.settle_transport().await;
                            return Ok(receipt);
                        }
                        RuntimeTransportEvent::ChildExited { .. }
                        | RuntimeTransportEvent::ProtocolError { .. } => {
                            self.settle_transport().await;
                            return Err(AgentHarnessV2Error::Transport {
                                reason_code: "harness.transport.runtime_failed".to_owned(),
                            });
                        }
                        RuntimeTransportEvent::Cleanup { .. } => {
                            return Err(AgentHarnessV2Error::Transport {
                                reason_code: "harness.transport.closed_before_terminal".to_owned(),
                            });
                        }
                    }
                }
                }
            }
        }
        .await;
        self.settle_transport().await;
        outcome
    }

    async fn dispose(&self) -> Result<(), AgentHarnessV2Error> {
        match self.transport.close().await {
            Ok(_) | Err(RuntimeTransportError::NotStarted) => Ok(()),
            Err(error) => Err(error.into()),
        }
    }

    async fn steer(
        &self,
        generation: u64,
        input: &str,
    ) -> Result<AgentHarnessSteerOutcomeV2, AgentHarnessV2Error> {
        if !self.descriptor.capabilities.steering {
            return Err(AgentHarnessV2Error::UnsupportedCapability { capability: "steering" });
        }
        if generation == 0 || input.trim().is_empty() || input.len() > 256 * 1024 {
            return Ok(AgentHarnessSteerOutcomeV2::Rejected {
                reason_code: "harness.steering.invalid_input".to_owned(),
            });
        }
        self.transport
            .send_command(RuntimeTransportCommand {
                command_id: format!("steer-{generation}"),
                generation,
                method: "steer".to_owned(),
                payload: json!({"input": input}),
                deadline_unix_ms: now_unix_ms().saturating_add(30_000),
            })
            .await?;
        Ok(AgentHarnessSteerOutcomeV2::Accepted { generation })
    }

    async fn health_probe(&self) -> Result<AgentHarnessHealthV2, AgentHarnessV2Error> {
        let health = self.transport.health();
        Ok(AgentHarnessHealthV2 {
            ready: health.state == ManagedRuntimeHealthState::Ready,
            reason_code: health.last_reason_code,
        })
    }
}

impl From<RuntimeTransportError> for AgentHarnessV2Error {
    fn from(error: RuntimeTransportError) -> Self {
        let reason_code = match error {
            RuntimeTransportError::InvalidDescriptor => "harness.transport.invalid_descriptor",
            RuntimeTransportError::InvalidStartRequest => "harness.transport.invalid_start",
            RuntimeTransportError::InvalidCommand => "harness.transport.invalid_command",
            RuntimeTransportError::AlreadyStarted => "harness.transport.already_started",
            RuntimeTransportError::NotStarted => "harness.transport.not_started",
            RuntimeTransportError::StaleGeneration { .. } => "harness.transport.stale_generation",
            RuntimeTransportError::SpawnFailed { .. } => "harness.transport.spawn_failed",
            RuntimeTransportError::StdioUnavailable => "harness.transport.stdio_unavailable",
            RuntimeTransportError::HandshakeTimedOut => "harness.transport.handshake_timeout",
            RuntimeTransportError::HandshakeMismatch => "harness.transport.handshake_mismatch",
            RuntimeTransportError::MalformedFrame => "harness.transport.malformed_frame",
            RuntimeTransportError::ReadFailed => "harness.transport.read_failed",
            RuntimeTransportError::WriteFailed => "harness.transport.write_failed",
            RuntimeTransportError::CommandTimedOut => "harness.transport.command_timeout",
            RuntimeTransportError::Backpressure => "harness.transport.backpressure",
            RuntimeTransportError::Unavailable => "harness.transport.unavailable",
        };
        Self::Transport { reason_code: reason_code.to_owned() }
    }
}

fn required_string(payload: &Value, key: &str) -> Result<String, AgentHarnessV2Error> {
    payload
        .get(key)
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty() && value.len() <= 256)
        .map(ToOwned::to_owned)
        .ok_or_else(|| AgentHarnessV2Error::Transport {
            reason_code: "harness.transport.invalid_event".to_owned(),
        })
}

fn normalize_dynamic_tool_result(result: &Value) -> Value {
    if result.get("success").and_then(Value::as_bool).is_some()
        && result.get("contentItems").and_then(Value::as_array).is_some()
    {
        return result.clone();
    }
    let success = result
        .get("ok")
        .and_then(Value::as_bool)
        .or_else(|| result.get("success").and_then(Value::as_bool))
        .unwrap_or(true);
    let text = result
        .get("text")
        .or_else(|| result.get("safe_message"))
        .and_then(Value::as_str)
        .map(str::to_owned)
        .unwrap_or_else(|| {
            serde_json::to_string(result)
                .unwrap_or_else(|_| "Host tool outcome unavailable".to_owned())
        });
    json!({
        "success": success,
        "contentItems": [{"type": "inputText", "text": text}],
    })
}

fn normalize_approval_result(result: &Value) -> Value {
    let decision = result
        .get("decision")
        .or_else(|| result.get("outcome"))
        .and_then(Value::as_str)
        .unwrap_or("decline");
    let decision = match decision {
        "accept" | "accepted" | "allow" | "allowed" | "approve" | "approved" => "accept",
        _ => "decline",
    };
    json!({"decision": decision})
}

fn normalize_side_question_result(question_id: &str, result: &Value) -> Value {
    if result.get("answers").and_then(Value::as_object).is_some() {
        return result.clone();
    }
    let answer = result
        .get("answer")
        .or_else(|| result.get("text"))
        .and_then(Value::as_str)
        .unwrap_or_default();
    json!({"answers": {question_id: {"answers": [answer]}}})
}

fn terminal_outcome(
    outcome: &str,
    payload: &Value,
) -> Result<AgentHarnessTerminalOutcomeV2, AgentHarnessV2Error> {
    match outcome {
        "completed" => Ok(AgentHarnessTerminalOutcomeV2::Completed {
            final_message: payload
                .get("final_message")
                .and_then(Value::as_str)
                .map(ToOwned::to_owned),
        }),
        "blocked" => Ok(AgentHarnessTerminalOutcomeV2::Blocked {
            reason_code: required_string(payload, "reason_code")?,
        }),
        "failed" => Ok(AgentHarnessTerminalOutcomeV2::Failed {
            reason_code: required_string(payload, "reason_code")?,
            safe_message: payload
                .get("safe_message")
                .and_then(Value::as_str)
                .unwrap_or("External runtime failed.")
                .to_owned(),
        }),
        "cancelled" => Ok(AgentHarnessTerminalOutcomeV2::Cancelled {
            reason_code: required_string(payload, "reason_code")?,
        }),
        "timed_out" => Ok(AgentHarnessTerminalOutcomeV2::TimedOut {
            reason_code: required_string(payload, "reason_code")?,
        }),
        _ => Err(AgentHarnessV2Error::Transport {
            reason_code: "harness.transport.invalid_terminal".to_owned(),
        }),
    }
}

fn now_unix_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| i64::try_from(duration.as_millis()).unwrap_or(i64::MAX))
        .unwrap_or(0)
}
