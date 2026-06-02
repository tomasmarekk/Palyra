use std::{
    collections::{BTreeMap, BTreeSet},
    time::{Duration, Instant},
};

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::{
    gateway::current_unix_ms,
    model_provider::{ProviderMessage, ProviderResponse, ProviderTurnOutput},
};

// The local setup configures a larger tool budget for app/browser workflows.
// Keep extra model-turn headroom for recovery, final verification, and concise
// partial summaries instead of failing immediately after the last tool batch.
pub(crate) const DEFAULT_AGENT_LOOP_MAX_MODEL_TURNS: u32 = 128;
pub(crate) const DEFAULT_AGENT_LOOP_WALL_CLOCK_BUDGET_MS: u64 = 900_000;

const BROWSER_SESSION_CREATE_TOOL_NAME: &str = "palyra.browser.session.create";
const BROWSER_SESSION_CLOSE_TOOL_NAME: &str = "palyra.browser.session.close";
const ROUTINES_QUERY_TOOL_NAME: &str = "palyra.routines.query";
const ROUTINES_CONTROL_TOOL_NAME: &str = "palyra.routines.control";

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
    IncompleteFinalAnswer,
    RepeatedToolFailure,
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
            Self::IncompleteFinalAnswer => "incomplete_final_answer",
            Self::RepeatedToolFailure => "repeated_tool_failure",
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
        max_tool_calls.saturating_add(8).clamp(1, DEFAULT_AGENT_LOOP_MAX_MODEL_TURNS)
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

    pub(crate) fn append_user_guidance(&mut self, text: impl Into<String>) {
        self.messages.push(ProviderMessage::user_text(text.into()));
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

    pub(crate) fn remaining_model_turns(&self) -> u32 {
        self.remaining_model_turns
    }

    pub(crate) fn completed_tool_calls(&self) -> u32 {
        self.completed_tool_calls
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

    pub(crate) fn message_with_cleanup_guidance(&self, message: &str) -> String {
        let cleanup = self.cleanup_instructions();
        if cleanup.is_empty() {
            return message.to_owned();
        }
        format!("{message} Cleanup required: {}", cleanup.join(" "))
    }

    fn cleanup_instructions(&self) -> Vec<String> {
        let mut cleanup = pending_browser_session_ids(self.messages.as_slice())
            .into_iter()
            .map(|session_id| {
                format!(
                    "browser session {session_id} may still be open; close it with `palyra browser session close {session_id} --json` or stop browserd with `palyra browser stop --json`."
                )
            })
            .collect::<Vec<_>>();
        cleanup.extend(pending_routine_cleanup_instructions(self.messages.as_slice()));
        cleanup
    }

    fn elapsed(&self) -> Duration {
        Instant::now().saturating_duration_since(self.started_at)
    }
}

#[derive(Debug, Clone)]
struct BrowserToolCallRef {
    tool_name: String,
    input_json: Value,
}

#[derive(Debug, Clone)]
struct RoutineToolCallRef {
    tool_name: String,
    input_json: Value,
}

fn pending_browser_session_ids(messages: &[ProviderMessage]) -> BTreeSet<String> {
    let mut tool_calls_by_id = BTreeMap::<String, BrowserToolCallRef>::new();
    let mut open_session_ids = BTreeSet::<String>::new();

    for message in messages {
        for tool_call in &message.tool_calls {
            if matches!(
                tool_call.tool_name.as_str(),
                BROWSER_SESSION_CREATE_TOOL_NAME | BROWSER_SESSION_CLOSE_TOOL_NAME
            ) {
                tool_calls_by_id.insert(
                    tool_call.proposal_id.clone(),
                    BrowserToolCallRef {
                        tool_name: tool_call.tool_name.clone(),
                        input_json: tool_call.input_json.clone(),
                    },
                );
            }
        }

        if message.role != crate::model_provider::ProviderMessageRole::Tool {
            continue;
        }
        let Some(tool_call_id) = message.tool_call_id.as_deref() else {
            continue;
        };
        let Some(tool_call) = tool_calls_by_id.get(tool_call_id) else {
            continue;
        };
        let Ok(output) = serde_json::from_str::<Value>(message.text_content().as_str()) else {
            continue;
        };

        match tool_call.tool_name.as_str() {
            BROWSER_SESSION_CREATE_TOOL_NAME => {
                if let Some(session_id) = output.get("session_id").and_then(Value::as_str) {
                    open_session_ids.insert(session_id.to_owned());
                }
            }
            BROWSER_SESSION_CLOSE_TOOL_NAME => {
                if browser_session_close_confirmed(&output) {
                    if let Some(session_id) =
                        tool_call.input_json.get("session_id").and_then(Value::as_str)
                    {
                        open_session_ids.remove(session_id);
                    }
                }
            }
            _ => {}
        }
    }

    open_session_ids
}

fn pending_routine_cleanup_instructions(messages: &[ProviderMessage]) -> Vec<String> {
    let mut tool_calls_by_id = BTreeMap::<String, RoutineToolCallRef>::new();
    let mut created_routine_ids = BTreeSet::<String>::new();
    let mut latest_routine_views = BTreeMap::<String, Value>::new();

    for message in messages {
        for tool_call in &message.tool_calls {
            if matches!(
                tool_call.tool_name.as_str(),
                ROUTINES_QUERY_TOOL_NAME | ROUTINES_CONTROL_TOOL_NAME
            ) {
                tool_calls_by_id.insert(
                    tool_call.proposal_id.clone(),
                    RoutineToolCallRef {
                        tool_name: tool_call.tool_name.clone(),
                        input_json: tool_call.input_json.clone(),
                    },
                );
            }
        }

        if message.role != crate::model_provider::ProviderMessageRole::Tool {
            continue;
        }
        let Some(tool_call_id) = message.tool_call_id.as_deref() else {
            continue;
        };
        let Some(tool_call) = tool_calls_by_id.get(tool_call_id) else {
            continue;
        };
        let Ok(raw_output) = serde_json::from_str::<Value>(message.text_content().as_str()) else {
            continue;
        };
        let output = routine_tool_output_payload(&raw_output);
        let operation = routine_tool_operation(&tool_call.input_json, output);

        if tool_call.tool_name == ROUTINES_CONTROL_TOOL_NAME
            && operation == Some("delete")
            && output.get("deleted").and_then(Value::as_bool).unwrap_or(false)
        {
            if let Some(routine_id) = routine_id_from_routine_payload(output) {
                created_routine_ids.remove(routine_id);
                latest_routine_views.remove(routine_id);
            }
            continue;
        }

        if tool_call.tool_name == ROUTINES_CONTROL_TOOL_NAME && operation == Some("upsert") {
            let upsert_created_routine =
                tool_call.input_json.get("routine_id").and_then(Value::as_str).is_none();
            if upsert_created_routine {
                if let Some(routine) = output.get("routine").and_then(Value::as_object) {
                    if let Some(routine_id) = routine.get("routine_id").and_then(Value::as_str) {
                        created_routine_ids.insert(routine_id.to_owned());
                        latest_routine_views
                            .insert(routine_id.to_owned(), Value::Object(routine.clone()));
                    }
                }
            }
            continue;
        }

        if let Some(routine) = output.get("routine").and_then(Value::as_object) {
            if let Some(routine_id) = routine.get("routine_id").and_then(Value::as_str) {
                if created_routine_ids.contains(routine_id) {
                    latest_routine_views
                        .insert(routine_id.to_owned(), Value::Object(routine.clone()));
                }
            }
        }
    }

    created_routine_ids
        .into_iter()
        .map(|routine_id| {
            routine_cleanup_instruction(
                routine_id.as_str(),
                latest_routine_views.get(routine_id.as_str()),
            )
        })
        .collect()
}

fn routine_tool_output_payload(output: &Value) -> &Value {
    output
        .get("output")
        .filter(|_| output.get("tool_name").and_then(Value::as_str).is_some())
        .unwrap_or(output)
}

fn routine_tool_operation<'a>(input: &'a Value, output: &'a Value) -> Option<&'a str> {
    output
        .get("operation")
        .and_then(Value::as_str)
        .or_else(|| input.get("operation").and_then(Value::as_str))
}

fn routine_id_from_routine_payload(output: &Value) -> Option<&str> {
    output
        .get("routine_id")
        .and_then(Value::as_str)
        .or_else(|| output.get("routine")?.get("routine_id")?.as_str())
}

fn routine_cleanup_instruction(routine_id: &str, latest_view: Option<&Value>) -> String {
    let enabled = latest_view
        .and_then(|view| view.get("enabled"))
        .map(json_value_label)
        .unwrap_or_else(|| "unknown".to_owned());
    let next_run_at_unix_ms = latest_view
        .and_then(|view| view.get("next_run_at_unix_ms"))
        .map(json_value_label)
        .unwrap_or_else(|| "unknown".to_owned());
    let last_outcome_kind = latest_view
        .and_then(|view| view.get("last_outcome_kind"))
        .map(json_value_label)
        .unwrap_or_else(|| "unknown".to_owned());
    format!(
        "routine {routine_id} may still exist (enabled={enabled}, next_run_at_unix_ms={next_run_at_unix_ms}, last_outcome_kind={last_outcome_kind}); inspect it with `palyra routines show --id {routine_id} --json` or delete it with `palyra routines delete --id {routine_id} --json`."
    )
}

fn json_value_label(value: &Value) -> String {
    match value {
        Value::Null => "null".to_owned(),
        Value::Bool(value) => value.to_string(),
        Value::Number(value) => value.to_string(),
        Value::String(value) => value.clone(),
        _ => "structured".to_owned(),
    }
}

fn browser_session_close_confirmed(output: &Value) -> bool {
    output.get("closed").and_then(Value::as_bool).unwrap_or(false)
        || output.get("reason").and_then(Value::as_str).is_some_and(browser_session_absent_reason)
        || output.get("error").and_then(Value::as_str).is_some_and(browser_session_absent_reason)
        || output.get("output").is_some_and(browser_session_close_confirmed)
}

fn browser_session_absent_reason(raw: &str) -> bool {
    raw.contains("session_not_found") || raw.contains("chromium_session_not_found")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model_provider::{
        ProviderFinishReason, ProviderMessageContentPart, ProviderMessageRole,
        ProviderMessageToolCall, ProviderRawProviderRefs, ProviderUsage,
    };

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
    fn default_turn_budget_preserves_recovery_headroom() {
        assert_eq!(AgentRunLoopState::default_model_turn_budget(64), 72);

        let state =
            AgentRunLoopState::new(vec![ProviderMessage::user_text("hello")], 192, 2, 10_000);
        let snapshot = state.snapshot("run-01", None);

        assert_eq!(snapshot.remaining_model_turns, DEFAULT_AGENT_LOOP_MAX_MODEL_TURNS);
    }

    #[test]
    fn default_turn_budget_tracks_local_app_workflow_tool_budget() {
        assert_eq!(AgentRunLoopState::default_model_turn_budget(96), 104);

        let mut state =
            AgentRunLoopState::new(vec![ProviderMessage::user_text("hello")], 104, 96, 10_000);
        for expected_turn in 1..=104 {
            assert_eq!(state.start_model_turn(), Ok(expected_turn));
        }
        assert_eq!(state.start_model_turn(), Err(AgentLoopTerminationReason::MaxTurns));
    }

    #[test]
    fn wall_clock_budget_allows_longer_browser_verification_workflows() {
        assert_eq!(DEFAULT_AGENT_LOOP_WALL_CLOCK_BUDGET_MS, 15 * 60 * 1_000);
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

    #[test]
    fn loop_state_reports_browser_session_cleanup_guidance_on_failure() {
        let session_id = "01ARZ3NDEKTSV4RRFFQ69G5FAV";
        let messages = vec![
            ProviderMessage {
                role: ProviderMessageRole::Assistant,
                content: Vec::new(),
                name: None,
                tool_call_id: None,
                tool_calls: vec![ProviderMessageToolCall {
                    proposal_id: "call-create".to_owned(),
                    tool_name: BROWSER_SESSION_CREATE_TOOL_NAME.to_owned(),
                    input_json: serde_json::json!({"allow_private_targets": true}),
                }],
            },
            ProviderMessage::tool_result(
                "call-create",
                serde_json::json!({"session_id": session_id}).to_string(),
            ),
        ];
        let state = AgentRunLoopState::new(messages, 2, 4, 10_000);

        let message = state.message_with_cleanup_guidance("agent loop wall-clock budget exhausted");

        assert!(message.contains("agent loop wall-clock budget exhausted"));
        assert!(message.contains("browser session 01ARZ3NDEKTSV4RRFFQ69G5FAV"));
        assert!(message.contains("palyra browser session close 01ARZ3NDEKTSV4RRFFQ69G5FAV --json"));
        assert!(message.contains("palyra browser stop --json"));
    }

    #[test]
    fn loop_state_omits_cleanup_guidance_for_closed_browser_sessions() {
        let session_id = "01ARZ3NDEKTSV4RRFFQ69G5FAV";
        let messages = vec![
            ProviderMessage {
                role: ProviderMessageRole::Assistant,
                content: vec![ProviderMessageContentPart::text("creating browser")],
                name: None,
                tool_call_id: None,
                tool_calls: vec![ProviderMessageToolCall {
                    proposal_id: "call-create".to_owned(),
                    tool_name: BROWSER_SESSION_CREATE_TOOL_NAME.to_owned(),
                    input_json: serde_json::json!({}),
                }],
            },
            ProviderMessage::tool_result(
                "call-create",
                serde_json::json!({"session_id": session_id}).to_string(),
            ),
            ProviderMessage {
                role: ProviderMessageRole::Assistant,
                content: Vec::new(),
                name: None,
                tool_call_id: None,
                tool_calls: vec![ProviderMessageToolCall {
                    proposal_id: "call-close".to_owned(),
                    tool_name: BROWSER_SESSION_CLOSE_TOOL_NAME.to_owned(),
                    input_json: serde_json::json!({"session_id": session_id}),
                }],
            },
            ProviderMessage::tool_result(
                "call-close",
                serde_json::json!({"closed": true}).to_string(),
            ),
        ];
        let state = AgentRunLoopState::new(messages, 2, 4, 10_000);

        let message = state.message_with_cleanup_guidance("agent loop wall-clock budget exhausted");

        assert_eq!(message, "agent loop wall-clock budget exhausted");
    }

    #[test]
    fn loop_state_omits_cleanup_guidance_when_browser_session_is_already_absent() {
        let session_id = "01ARZ3NDEKTSV4RRFFQ69G5FAV";
        let messages = vec![
            ProviderMessage {
                role: ProviderMessageRole::Assistant,
                content: vec![ProviderMessageContentPart::text("creating browser")],
                name: None,
                tool_call_id: None,
                tool_calls: vec![ProviderMessageToolCall {
                    proposal_id: "call-create".to_owned(),
                    tool_name: BROWSER_SESSION_CREATE_TOOL_NAME.to_owned(),
                    input_json: serde_json::json!({}),
                }],
            },
            ProviderMessage::tool_result(
                "call-create",
                serde_json::json!({"session_id": session_id}).to_string(),
            ),
            ProviderMessage {
                role: ProviderMessageRole::Assistant,
                content: Vec::new(),
                name: None,
                tool_call_id: None,
                tool_calls: vec![ProviderMessageToolCall {
                    proposal_id: "call-close".to_owned(),
                    tool_name: BROWSER_SESSION_CLOSE_TOOL_NAME.to_owned(),
                    input_json: serde_json::json!({"session_id": session_id}),
                }],
            },
            ProviderMessage::tool_result(
                "call-close",
                serde_json::json!({
                    "success": false,
                    "tool_name": BROWSER_SESSION_CLOSE_TOOL_NAME,
                    "error": "palyra.browser.session.close failed: session_not_found",
                    "output": {
                        "closed": false,
                        "reason": "session_not_found"
                    }
                })
                .to_string(),
            ),
        ];
        let state = AgentRunLoopState::new(messages, 2, 4, 10_000);

        let message = state.message_with_cleanup_guidance("agent loop wall-clock budget exhausted");

        assert_eq!(message, "agent loop wall-clock budget exhausted");
    }

    #[test]
    fn loop_state_reports_created_routine_cleanup_guidance_on_failure() {
        let routine_id = "01ARZ3NDEKTSV4RRFFQ69G5FAV";
        let messages = vec![
            ProviderMessage {
                role: ProviderMessageRole::Assistant,
                content: Vec::new(),
                name: None,
                tool_call_id: None,
                tool_calls: vec![ProviderMessageToolCall {
                    proposal_id: "call-upsert".to_owned(),
                    tool_name: ROUTINES_CONTROL_TOOL_NAME.to_owned(),
                    input_json: serde_json::json!({
                        "operation": "upsert",
                        "trigger_kind": "schedule",
                        "name": "heartbeat",
                        "prompt": "append heartbeat",
                        "schedule_type": "every",
                        "every_interval_ms": 60_000,
                        "max_runs": 3,
                    }),
                }],
            },
            ProviderMessage::tool_result(
                "call-upsert",
                serde_json::json!({
                    "operation": "upsert",
                    "routine": {
                        "routine_id": routine_id,
                        "enabled": false,
                        "next_run_at_unix_ms": null,
                        "last_outcome_kind": "success_with_output"
                    }
                })
                .to_string(),
            ),
        ];
        let state = AgentRunLoopState::new(messages, 2, 4, 10_000);

        let message = state.message_with_cleanup_guidance("agent loop wall-clock budget exhausted");

        assert!(message.contains("agent loop wall-clock budget exhausted"));
        assert!(message.contains("routine 01ARZ3NDEKTSV4RRFFQ69G5FAV"));
        assert!(message.contains("enabled=false"));
        assert!(message.contains("next_run_at_unix_ms=null"));
        assert!(message.contains("last_outcome_kind=success_with_output"));
        assert!(message.contains("palyra routines delete --id 01ARZ3NDEKTSV4RRFFQ69G5FAV --json"));
    }

    #[test]
    fn loop_state_omits_created_routine_cleanup_after_delete() {
        let routine_id = "01ARZ3NDEKTSV4RRFFQ69G5FAV";
        let messages = vec![
            ProviderMessage {
                role: ProviderMessageRole::Assistant,
                content: Vec::new(),
                name: None,
                tool_call_id: None,
                tool_calls: vec![ProviderMessageToolCall {
                    proposal_id: "call-upsert".to_owned(),
                    tool_name: ROUTINES_CONTROL_TOOL_NAME.to_owned(),
                    input_json: serde_json::json!({
                        "operation": "upsert",
                        "trigger_kind": "schedule",
                        "name": "heartbeat",
                        "prompt": "append heartbeat",
                        "schedule_type": "every",
                        "every_interval_ms": 60_000,
                    }),
                }],
            },
            ProviderMessage::tool_result(
                "call-upsert",
                serde_json::json!({
                    "operation": "upsert",
                    "routine": {
                        "routine_id": routine_id,
                        "enabled": true,
                        "next_run_at_unix_ms": 42,
                    }
                })
                .to_string(),
            ),
            ProviderMessage {
                role: ProviderMessageRole::Assistant,
                content: Vec::new(),
                name: None,
                tool_call_id: None,
                tool_calls: vec![ProviderMessageToolCall {
                    proposal_id: "call-delete".to_owned(),
                    tool_name: ROUTINES_CONTROL_TOOL_NAME.to_owned(),
                    input_json: serde_json::json!({
                        "operation": "delete",
                        "routine_id": routine_id,
                    }),
                }],
            },
            ProviderMessage::tool_result(
                "call-delete",
                serde_json::json!({
                    "operation": "delete",
                    "deleted": true,
                    "routine_id": routine_id,
                })
                .to_string(),
            ),
        ];
        let state = AgentRunLoopState::new(messages, 2, 4, 10_000);

        let message = state.message_with_cleanup_guidance("agent loop wall-clock budget exhausted");

        assert_eq!(message, "agent loop wall-clock budget exhausted");
    }
}
