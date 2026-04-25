use palyra_common::runtime_contracts::{AcpCapability, AcpCommand, AcpScope};
use serde_json::{json, Value};

use super::acp_runtime_response;
use crate::*;

pub(crate) async fn console_acp_status_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<Value>, Response> {
    let _session = authorize_console_session(&state, &headers, false)?;
    let snapshot = state.acp_runtime.snapshot().map_err(acp_runtime_response)?;
    Ok(Json(json!({
        "protocol": state.acp_runtime.protocol_range(),
        "root": state.acp_runtime.root().display().to_string(),
        "counts": {
            "session_bindings": snapshot.session_bindings.len(),
            "conversation_bindings": snapshot.conversation_bindings.len(),
            "pending_prompts": snapshot.pending_prompts.len(),
        },
        "methods": acp_method_descriptors(),
    })))
}

fn acp_method_descriptors() -> Vec<Value> {
    [
        (AcpCommand::SessionList, AcpScope::SessionsRead, AcpCapability::SessionList, false),
        (AcpCommand::SessionLoad, AcpScope::SessionsRead, AcpCapability::SessionLoad, false),
        (AcpCommand::SessionNew, AcpScope::SessionsWrite, AcpCapability::SessionNew, true),
        (AcpCommand::SessionReplay, AcpScope::SessionsRead, AcpCapability::SessionReplay, false),
        (AcpCommand::SessionFork, AcpScope::SessionsWrite, AcpCapability::SessionFork, true),
        (
            AcpCommand::SessionCompactPreview,
            AcpScope::SessionsRead,
            AcpCapability::SessionCompact,
            false,
        ),
        (
            AcpCommand::SessionCompactApply,
            AcpScope::SessionsWrite,
            AcpCapability::SessionCompact,
            true,
        ),
        (AcpCommand::SessionExplain, AcpScope::SessionsRead, AcpCapability::SessionExplain, false),
        (AcpCommand::ApprovalDecide, AcpScope::ApprovalsWrite, AcpCapability::ApprovalBridge, true),
        (
            AcpCommand::BindingList,
            AcpScope::BindingsRead,
            AcpCapability::ConversationBindings,
            false,
        ),
        (
            AcpCommand::BindingUpsert,
            AcpScope::BindingsWrite,
            AcpCapability::ConversationBindings,
            true,
        ),
        (
            AcpCommand::BindingRepairApply,
            AcpScope::BindingsWrite,
            AcpCapability::BindingRepair,
            true,
        ),
    ]
    .into_iter()
    .map(|(command, scope, capability, side_effecting)| {
        json!({
            "command": command.as_str(),
            "version": 1,
            "required_scopes": [scope.as_str()],
            "required_capabilities": [capability.as_str()],
            "side_effecting": side_effecting,
            "rate_limit_bucket": "acp.command",
        })
    })
    .collect()
}
