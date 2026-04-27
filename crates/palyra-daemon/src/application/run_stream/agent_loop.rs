use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::{
    gateway::current_unix_ms,
    model_provider::{ProviderMessage, ProviderResponse, ProviderTurnOutput},
};

pub(crate) const DEFAULT_AGENT_LOOP_MAX_MODEL_TURNS: u32 = 8;
pub(crate) const DEFAULT_AGENT_LOOP_WALL_CLOCK_BUDGET_MS: u64 = 120_000;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum AgentLoopTerminationReason {
    FinalAnswer,
    MaxTurns,
    MaxToolCalls,
    WallClock,
    Cancellation,
    ApprovalDenied,
    ProviderError,
    ContextBudgetExhausted,
}

impl AgentLoopTerminationReason {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::FinalAnswer => "final_answer",
            Self::MaxTurns => "max_turns",
            Self::MaxToolCalls => "max_tool_calls",
            Self::WallClock => "wall_clock",
            Self::Cancellation => "cancellation",
            Self::ApprovalDenied => "approval_denied",
            Self::ProviderError => "provider_error",
            Self::ContextBudgetExhausted => "context_budget_exhausted",
        }
    }

    pub(crate) const fn is_success(self) -> bool {
        matches!(self, Self::FinalAnswer)
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct AgentLoopUsageSnapshot {
    pub(crate) prompt_tokens: u64,
    pub(crate) completion_tokens: u64,
    pub(crate) total_tokens: u64,
}

impl AgentLoopUsageSnapshot {
    fn add(&mut self, prompt_tokens: u64, completion_tokens: u64) {
        self.prompt_tokens = self.prompt_tokens.saturating_add(prompt_tokens);
        self.completion_tokens = self.completion_tokens.saturating_add(completion_tokens);
        self.total_tokens = self.prompt_tokens.saturating_add(self.completion_tokens);
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct AgentLoopSnapshot {
    pub(crate) schema_version: u32,
    pub(crate) run_id: String,
    pub(crate) current_turn: u32,
    pub(crate) remaining_model_turns: u32,
    pub(crate) remaining_tool_calls: u32,
    pub(crate) completed_tool_calls: u32,
    pub(crate) message_count: usize,
    pub(crate) wall_clock_budget_ms: u64,
    pub(crate) elapsed_ms: u64,
    pub(crate) usage: AgentLoopUsageSnapshot,
    pub(crate) termination_reason: Option<AgentLoopTerminationReason>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct AgentLoopFinalizationEnvelope {
    pub(crate) schema_version: u32,
    pub(crate) termination_reason: AgentLoopTerminationReason,
    pub(crate) status: String,
    pub(crate) user_visible_message: String,
    pub(crate) usage: AgentLoopUsageSnapshot,
    pub(crate) tool_count: u32,
    pub(crate) artifact_refs: Vec<String>,
    pub(crate) provider_trace_ref: Option<String>,
}

#[derive(Debug, Clone)]
pub(crate) struct AgentRunLoopState {
    messages: Vec<ProviderMessage>,
    max_model_turns: u32,
    remaining_model_turns: u32,
    max_tool_calls: u32,
    remaining_tool_calls: u32,
    wall_clock_budget_ms: u64,
    started_at_unix_ms: i64,
    started_at: Instant,
    current_turn: u32,
    completed_tool_calls: u32,
    usage: AgentLoopUsageSnapshot,
}

impl AgentRunLoopState {
    pub(crate) fn new(
        messages: Vec<ProviderMessage>,
        max_model_turns: u32,
        max_tool_calls: u32,
        wall_clock_budget_ms: u64,
    ) -> Self {
        let bounded_model_turns = max_model_turns.clamp(1, DEFAULT_AGENT_LOOP_MAX_MODEL_TURNS);
        Self {
            messages,
            max_model_turns: bounded_model_turns,
            remaining_model_turns: bounded_model_turns,
            max_tool_calls,
            remaining_tool_calls: max_tool_calls,
            wall_clock_budget_ms: wall_clock_budget_ms.max(1),
            started_at_unix_ms: current_unix_ms(),
            started_at: Instant::now(),
            current_turn: 0,
            completed_tool_calls: 0,
            usage: AgentLoopUsageSnapshot::default(),
        }
    }

    pub(crate) fn default_model_turn_budget(max_tool_calls: u32) -> u32 {
        max_tool_calls.saturating_add(1).clamp(1, DEFAULT_AGENT_LOOP_MAX_MODEL_TURNS)
    }

    pub(crate) fn start_model_turn(&mut self) -> Result<u32, AgentLoopTerminationReason> {
        if self.elapsed() > Duration::from_millis(self.wall_clock_budget_ms) {
            return Err(AgentLoopTerminationReason::WallClock);
        }
        if self.remaining_model_turns == 0 {
            return Err(AgentLoopTerminationReason::MaxTurns);
        }
        self.remaining_model_turns = self.remaining_model_turns.saturating_sub(1);
        self.current_turn = self.current_turn.saturating_add(1);
        Ok(self.current_turn)
    }

    pub(crate) fn record_provider_response(&mut self, response: &ProviderResponse) {
        self.usage.add(response.prompt_tokens, response.completion_tokens);
    }

    pub(crate) fn append_assistant_turn(&mut self, output: &ProviderTurnOutput) {
        self.messages.push(ProviderMessage::assistant_from_output(output));
    }

    pub(crate) fn append_tool_result_messages(&mut self, messages: Vec<ProviderMessage>) {
        let added = messages.len().try_into().unwrap_or(u32::MAX);
        self.completed_tool_calls = self.completed_tool_calls.saturating_add(added);
        self.messages.extend(messages);
    }

    pub(crate) fn messages(&self) -> Vec<ProviderMessage> {
        self.messages.clone()
    }

    pub(crate) fn remaining_tool_calls(&self) -> u32 {
        self.remaining_tool_calls
    }

    pub(crate) fn sync_remaining_tool_calls(&mut self, remaining_tool_calls: u32) {
        self.remaining_tool_calls = remaining_tool_calls.min(self.max_tool_calls);
    }

    pub(crate) fn snapshot(
        &self,
        run_id: &str,
        termination_reason: Option<AgentLoopTerminationReason>,
    ) -> AgentLoopSnapshot {
        AgentLoopSnapshot {
            schema_version: 1,
            run_id: run_id.to_owned(),
            current_turn: self.current_turn,
            remaining_model_turns: self.remaining_model_turns,
            remaining_tool_calls: self.remaining_tool_calls,
            completed_tool_calls: self.completed_tool_calls,
            message_count: self.messages.len(),
            wall_clock_budget_ms: self.wall_clock_budget_ms,
            elapsed_ms: self.elapsed().as_millis().try_into().unwrap_or(u64::MAX),
            usage: self.usage.clone(),
            termination_reason,
        }
    }

    pub(crate) fn finalization_envelope(
        &self,
        reason: AgentLoopTerminationReason,
        user_visible_message: impl Into<String>,
        provider_trace_ref: Option<String>,
    ) -> AgentLoopFinalizationEnvelope {
        AgentLoopFinalizationEnvelope {
            schema_version: 1,
            termination_reason: reason,
            status: if reason.is_success() { "completed" } else { "failed" }.to_owned(),
            user_visible_message: user_visible_message.into(),
            usage: self.usage.clone(),
            tool_count: self.completed_tool_calls,
            artifact_refs: Vec::new(),
            provider_trace_ref,
        }
    }

    pub(crate) fn start_payload(&self, run_id: &str) -> String {
        serde_json::to_string(&json!({
            "event": "agent_loop.started",
            "started_at_unix_ms": self.started_at_unix_ms,
            "max_model_turns": self.max_model_turns,
            "max_tool_calls": self.max_tool_calls,
            "state": self.snapshot(run_id, None),
        }))
        .unwrap_or_else(|_| "{}".to_owned())
    }

    pub(crate) fn turn_payload(&self, run_id: &str, event: &str) -> String {
        serde_json::to_string(&json!({
            "event": event,
            "state": self.snapshot(run_id, None),
        }))
        .unwrap_or_else(|_| "{}".to_owned())
    }

    pub(crate) fn termination_payload(
        &self,
        run_id: &str,
        reason: AgentLoopTerminationReason,
        user_visible_message: &str,
        provider_trace_ref: Option<String>,
    ) -> String {
        serde_json::to_string(&json!({
            "event": "agent_loop.terminated",
            "termination_reason": reason.as_str(),
            "state": self.snapshot(run_id, Some(reason)),
            "finalization": self.finalization_envelope(
                reason,
                user_visible_message.to_owned(),
                provider_trace_ref,
            ),
        }))
        .unwrap_or_else(|_| "{}".to_owned())
    }

    fn elapsed(&self) -> Duration {
        Instant::now().saturating_duration_since(self.started_at)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model_provider::{ProviderFinishReason, ProviderRawProviderRefs, ProviderUsage};

    #[test]
    fn loop_state_enforces_turn_budget_and_serializes_termination() {
        let mut state =
            AgentRunLoopState::new(vec![ProviderMessage::user_text("hello")], 1, 2, 10_000);
        assert_eq!(state.start_model_turn(), Ok(1));
        assert_eq!(state.start_model_turn(), Err(AgentLoopTerminationReason::MaxTurns));

        let payload = state.termination_payload(
            "run-01",
            AgentLoopTerminationReason::MaxTurns,
            "maximum model turns reached",
            Some("provider-trace".to_owned()),
        );
        let parsed: serde_json::Value =
            serde_json::from_str(payload.as_str()).expect("termination payload should be JSON");
        assert_eq!(parsed["event"], "agent_loop.terminated");
        assert_eq!(parsed["state"]["termination_reason"], "max_turns");
        assert_eq!(parsed["finalization"]["status"], "failed");
        assert_eq!(parsed["finalization"]["provider_trace_ref"], "provider-trace");
    }

    #[test]
    fn assistant_and_tool_messages_preserve_native_tool_ids() {
        let output = ProviderTurnOutput {
            full_text: String::new(),
            content_parts: vec![crate::model_provider::ProviderOutputContentPart::ToolCall {
                proposal_id: "call-01".to_owned(),
                tool_name: "palyra.echo".to_owned(),
                input_json: serde_json::json!({"text":"hello"}),
            }],
            finish_reason: ProviderFinishReason::ToolCalls,
            usage: ProviderUsage::new(3, 1, "provider"),
            raw_provider_refs: ProviderRawProviderRefs::default(),
            redaction_state: Default::default(),
        };
        let assistant = ProviderMessage::assistant_from_output(&output);
        assert_eq!(assistant.tool_calls[0].proposal_id, "call-01");

        let tool = ProviderMessage::tool_result("call-01", r#"{"echo":"hello"}"#);
        assert_eq!(tool.tool_call_id.as_deref(), Some("call-01"));
        assert!(tool.tool_calls.is_empty());
    }
}
