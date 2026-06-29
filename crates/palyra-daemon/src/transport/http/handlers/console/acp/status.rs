//! ACP status payload helpers for console and command dispatch.

use palyra_common::runtime_contracts::{AcpCapability, AcpCommand, AcpScope};
use serde_json::{json, Value};

use super::acp_runtime_response;
use crate::acp::{MAX_EVENT_LEDGER_EVENTS, MAX_EVENT_LEDGER_EVENTS_PER_SESSION};
use crate::*;

/// Returns the ACP runtime status visible to the current console session.
///
/// # Errors
/// Returns an error response when session authorization or ACP runtime snapshot
/// loading fails.
pub(crate) async fn console_acp_status_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<Value>, Response> {
    let session = authorize_console_session(&state, &headers, false)?;
    build_acp_status_payload(&state, Some(session.context.principal.as_str()))
        .map(Json)
        .map_err(acp_runtime_response)
}

pub(super) fn build_acp_status_payload(
    state: &AppState,
    owner_principal: Option<&str>,
) -> Result<Value, crate::acp::AcpRuntimeError> {
    let snapshot = state.acp_runtime.snapshot()?;
    let session_bindings = snapshot
        .session_bindings
        .iter()
        .filter(|entry| owner_principal.is_none_or(|owner| entry.owner_principal == owner))
        .count();
    let conversation_bindings = snapshot
        .conversation_bindings
        .iter()
        .filter(|entry| owner_principal.is_none_or(|owner| entry.owner_principal == owner))
        .count();
    let pending_prompts = snapshot
        .pending_prompts
        .iter()
        .filter(|entry| {
            owner_principal.is_none_or(|owner| {
                snapshot.session_bindings.iter().any(|binding| {
                    binding.owner_principal == owner
                        && binding.acp_client_id == entry.acp_client_id
                        && binding.acp_session_id == entry.acp_session_id
                })
            })
        })
        .count();
    let visible_session_keys = snapshot
        .session_bindings
        .iter()
        .filter(|entry| owner_principal.is_none_or(|owner| entry.owner_principal == owner))
        .map(|entry| {
            (
                entry.acp_client_id.as_str(),
                entry.acp_session_id.as_str(),
                entry.palyra_session_id.as_str(),
            )
        })
        .collect::<std::collections::BTreeSet<_>>();
    let visible_ledger_events = snapshot
        .event_ledger
        .iter()
        .filter(|entry| {
            visible_session_keys.contains(&(
                entry.acp_client_id.as_str(),
                entry.acp_session_id.as_str(),
                entry.palyra_session_id.as_str(),
            ))
        })
        .collect::<Vec<_>>();
    let oldest_sequence = visible_ledger_events.iter().map(|entry| entry.sequence).min();
    let newest_sequence = visible_ledger_events.iter().map(|entry| entry.sequence).max();
    Ok(json!({
        "protocol": state.acp_runtime.protocol_range(),
        "root": state.acp_runtime.root().display().to_string(),
        "counts": {
            "session_bindings": session_bindings,
            "conversation_bindings": conversation_bindings,
            "pending_prompts": pending_prompts,
            "event_ledger": visible_ledger_events.len(),
        },
        "event_ledger": {
            "retained_events": visible_ledger_events.len(),
            "retention_max_events": MAX_EVENT_LEDGER_EVENTS,
            "retention_max_events_per_session": MAX_EVENT_LEDGER_EVENTS_PER_SESSION,
            "oldest_sequence": oldest_sequence,
            "newest_sequence": newest_sequence,
            "sessions_with_events": visible_session_keys.len(),
        },
        "methods": acp_method_descriptors(),
    }))
}

pub(super) fn acp_method_descriptors() -> Vec<Value> {
    [
        (AcpCommand::Status, AcpScope::SessionsRead, AcpCapability::RuntimeStatus, false),
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
        (AcpCommand::ApprovalList, AcpScope::ApprovalsRead, AcpCapability::ApprovalBridge, false),
        (
            AcpCommand::ApprovalRequest,
            AcpScope::ApprovalsWrite,
            AcpCapability::ApprovalBridge,
            true,
        ),
        (AcpCommand::ApprovalDecide, AcpScope::ApprovalsWrite, AcpCapability::ApprovalBridge, true),
        (AcpCommand::RunCreate, AcpScope::RunsWrite, AcpCapability::RunControl, true),
        (AcpCommand::RunAbort, AcpScope::RunsWrite, AcpCapability::RunControl, true),
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
            AcpCommand::BindingGet,
            AcpScope::BindingsRead,
            AcpCapability::ConversationBindings,
            false,
        ),
        (
            AcpCommand::BindingDetach,
            AcpScope::BindingsWrite,
            AcpCapability::ConversationBindings,
            true,
        ),
        (
            AcpCommand::BindingRepairPlan,
            AcpScope::BindingsWrite,
            AcpCapability::BindingRepair,
            false,
        ),
        (
            AcpCommand::BindingRepairApply,
            AcpScope::BindingsWrite,
            AcpCapability::BindingRepair,
            true,
        ),
        (
            AcpCommand::BindingExplain,
            AcpScope::BindingsRead,
            AcpCapability::ConversationBindings,
            false,
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
