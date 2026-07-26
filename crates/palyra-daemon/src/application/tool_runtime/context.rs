//! Read-only context-engine tools executed behind the tool authority gateway.
//!
//! The provider-visible catalog, policy, and approval checks happen before
//! this module is dispatched. The executor exposes only validated, redacted
//! lifecycle metadata for the calling session; prompt and tool payloads remain
//! exclusively in the host-owned transcript.

use std::sync::Arc;

use serde_json::{json, Value};

use crate::{
    application::{
        context_compaction::CONTEXT_INSPECT_TOOL_NAME,
        context_engine::{
            ContextEngineRegistry, ContextEngineToolCall, ContextEngineToolCallOutcome,
        },
        context_lifecycle::session_context_lifecycle_diagnostics,
    },
    gateway::{GatewayRuntimeState, ToolRuntimeExecutionContext},
    tool_protocol::{build_tool_execution_outcome, ToolExecutionOutcome},
};

const CONTEXT_RUNTIME_EXECUTOR: &str = "context_runtime";

fn outcome(
    proposal_id: &str,
    input_json: &[u8],
    success: bool,
    output_json: Vec<u8>,
    error: String,
) -> ToolExecutionOutcome {
    build_tool_execution_outcome(
        proposal_id,
        CONTEXT_INSPECT_TOOL_NAME,
        input_json,
        success,
        output_json,
        error,
        false,
        CONTEXT_RUNTIME_EXECUTOR.to_owned(),
        "none".to_owned(),
    )
}

fn validate_call(input_json: &[u8]) -> Result<ContextEngineToolCallOutcome, String> {
    let arguments = serde_json::from_slice::<Value>(input_json)
        .map_err(|error| format!("palyra.context.inspect invalid JSON input: {error}"))?;
    if !arguments.is_object() {
        return Err("palyra.context.inspect requires JSON object input".to_owned());
    }
    let engine = ContextEngineRegistry::production_default().selected_engine();
    let result = engine.handle_context_tool_call(ContextEngineToolCall {
        name: CONTEXT_INSPECT_TOOL_NAME.to_owned(),
        arguments,
    });
    if !result.handled {
        return Err(result.reason_code);
    }
    Ok(result)
}

/// Executes the session-scoped inspector after the shared gateway has admitted
/// the exact catalog entry. Only registry identity and redacted lifecycle
/// counters are returned.
pub(crate) async fn execute_context_inspect_tool(
    runtime_state: &Arc<GatewayRuntimeState>,
    context: ToolRuntimeExecutionContext<'_>,
    proposal_id: &str,
    input_json: &[u8],
) -> ToolExecutionOutcome {
    let call = match validate_call(input_json) {
        Ok(call) => call,
        Err(error) => return outcome(proposal_id, input_json, false, b"{}".to_vec(), error),
    };
    let lifecycle =
        match session_context_lifecycle_diagnostics(runtime_state, context.session_id).await {
            Ok(lifecycle) => lifecycle,
            Err(error) => {
                return outcome(
                    proposal_id,
                    input_json,
                    false,
                    b"{}".to_vec(),
                    format!("palyra.context.inspect failed: {}", error.message()),
                );
            }
        };
    let payload = json!({
        "schema_version": 1,
        "scope": "session_context_metadata_only",
        "registry": ContextEngineRegistry::production_default().snapshot(),
        "lifecycle": lifecycle,
        "handled_reason_code": call.reason_code,
        "raw_context_included": false,
    });
    match serde_json::to_vec(&payload) {
        Ok(output_json) => outcome(proposal_id, input_json, true, output_json, String::new()),
        Err(error) => outcome(
            proposal_id,
            input_json,
            false,
            b"{}".to_vec(),
            format!("palyra.context.inspect failed to serialize output: {error}"),
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::validate_call;

    #[test]
    fn context_inspect_accepts_only_empty_object_input() {
        let accepted = validate_call(br#"{}"#).expect("empty object is accepted");
        assert!(accepted.handled);
        assert_eq!(accepted.reason_code, "context.tool.inspect_handled");

        assert!(validate_call(br#"{"raw":true}"#).is_err());
        assert!(validate_call(br#"[]"#).is_err());
    }
}
