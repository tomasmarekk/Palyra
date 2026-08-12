//! Console node inventory and capability invocation handlers.
//!
//! Node capability execution can require local mediation before dispatch; this
//! file owns the HTTP boundary and the approval prompt used for those cases.

use std::time::{Duration, Instant};

use crate::journal::{
    ApprovalCreateRequest, ApprovalPolicySnapshot, ApprovalPromptOption, ApprovalPromptRecord,
    ApprovalRecord, ApprovalRiskLevel,
};
use crate::node_runtime::{
    CapabilityExecutionResult, CapabilityRequestTimeoutOutcome, RegisteredNodeRecord,
};
use crate::*;
use palyra_common::runtime_contracts::REALTIME_DEFAULT_HEARTBEAT_INTERVAL_MS;
use sha2::{Digest, Sha256};

const NODE_CAPABILITY_LOCAL_MEDIATION_POLL_MS: u64 = 250;

/// Classifies how a node capability may be executed from the console.
fn capability_execution_mode(name: &str) -> &'static str {
    match name {
        "desktop.open_url" | "desktop.open_path" => "local_mediation",
        _ => "automatic",
    }
}

fn capability_requires_local_mediation(name: &str) -> bool {
    capability_execution_mode(name.trim()) == "local_mediation"
}

/// Query parameters for pending node pairing requests.
#[derive(Debug, Default, Deserialize)]
pub(crate) struct ConsoleNodesPendingQuery {
    #[serde(default, alias = "status")]
    state: Option<String>,
}

/// Request body for invoking a node capability.
#[derive(Debug, Deserialize)]
pub(crate) struct ConsoleNodeInvokeRequest {
    capability: String,
    #[serde(default)]
    input_json: Value,
    #[serde(default)]
    max_payload_bytes: Option<u64>,
    #[serde(default)]
    timeout_ms: Option<u64>,
}

/// Lists currently registered nodes.
///
/// # Errors
/// Returns an error response when console authorization or node-runtime lookup
/// fails.
pub(crate) async fn console_nodes_list_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<control_plane::NodeListEnvelope>, Response> {
    let _session = authorize_console_session(&state, &headers, false)?;
    let nodes = collect_nodes(&state).map_err(runtime_status_response)?;
    let page = build_page_info(nodes.len().max(1), nodes.len(), None);
    Ok(Json(control_plane::NodeListEnvelope { contract: contract_descriptor(), nodes, page }))
}

/// Lists node pairing requests that are waiting for operator action.
///
/// # Errors
/// Returns an error response when console authorization or node-runtime lookup
/// fails.
pub(crate) async fn console_nodes_pending_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<ConsoleNodesPendingQuery>,
) -> Result<Json<control_plane::NodePairingListEnvelope>, Response> {
    let _session = authorize_console_session(&state, &headers, false)?;
    let mut requests = state
        .node_runtime
        .pairing_requests()
        .map_err(runtime_status_response)?
        .into_iter()
        .filter(|record| record.client_kind == palyra_identity::PairingClientKind::Node)
        .collect::<Vec<_>>();
    if let Some(state_filter) =
        query.state.as_deref().map(str::trim).filter(|value| !value.is_empty())
    {
        requests.retain(|record| record.state.as_str() == state_filter);
    }
    Ok(Json(control_plane::NodePairingListEnvelope {
        contract: contract_descriptor(),
        codes: Vec::new(),
        requests: requests
            .iter()
            .map(super::pairing::control_plane_node_pairing_request_view)
            .collect(),
        page: build_page_info(requests.len().max(1), requests.len(), None),
    }))
}

/// Returns one registered node by device id.
///
/// # Errors
/// Returns an error response when console authorization, device-id validation,
/// or node lookup fails.
pub(crate) async fn console_node_get_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(device_id): Path<String>,
) -> Result<Json<control_plane::NodeEnvelope>, Response> {
    let _session = authorize_console_session(&state, &headers, false)?;
    validate_canonical_id(device_id.as_str()).map_err(|_| {
        runtime_status_response(tonic::Status::invalid_argument(
            "device_id must be a canonical ULID",
        ))
    })?;
    let node =
        state.node_runtime.node(device_id.as_str()).map_err(runtime_status_response)?.ok_or_else(
            || runtime_status_response(tonic::Status::not_found("node was not found")),
        )?;
    Ok(Json(control_plane::NodeEnvelope {
        contract: contract_descriptor(),
        node: node_record_json(&node),
    }))
}

/// Invokes a node capability, requiring local mediation for desktop openers.
///
/// # Errors
/// Returns an error response when console authorization, device-id validation,
/// node freshness checks, input encoding, mediation approval, queueing, timeout
/// handling, or result delivery fails.
pub(crate) async fn console_node_invoke_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(device_id): Path<String>,
    Json(payload): Json<ConsoleNodeInvokeRequest>,
) -> Result<Json<control_plane::NodeInvokeEnvelope>, Response> {
    let session = authorize_console_session(&state, &headers, true)?;
    validate_canonical_id(device_id.as_str()).map_err(|_| {
        runtime_status_response(tonic::Status::invalid_argument(
            "device_id must be a canonical ULID",
        ))
    })?;
    let node =
        state.node_runtime.node(device_id.as_str()).map_err(runtime_status_response)?.ok_or_else(
            || runtime_status_response(tonic::Status::not_found("node was not found")),
        )?;
    ensure_node_fresh_for_work(&node).map_err(runtime_status_response)?;
    let input_json = serde_json::to_vec(&payload.input_json).map_err(|error| {
        runtime_status_response(tonic::Status::invalid_argument(error.to_string()))
    })?;
    let timeout_ms = payload.timeout_ms.unwrap_or(30_000).clamp(1_000, 120_000);
    let max_payload_bytes = payload.max_payload_bytes.unwrap_or(64 * 1024);
    let capability = payload.capability.trim().to_owned();
    if capability_requires_local_mediation(capability.as_str()) {
        require_node_capability_local_mediation(
            &state,
            &session.context,
            &node,
            capability.as_str(),
            &payload.input_json,
            timeout_ms,
        )
        .await?;
    }
    let (request_id, mut receiver) = state
        .node_runtime
        .enqueue_capability_request(
            device_id.as_str(),
            capability.as_str(),
            input_json,
            max_payload_bytes,
            Some(timeout_ms),
        )
        .map_err(runtime_status_response)?;
    let result =
        match tokio::time::timeout(Duration::from_millis(timeout_ms), receiver.recv()).await {
            Ok(result) => result,
            Err(_) => match state
                .node_runtime
                .mark_capability_timeout(request_id.as_str())
                .map_err(runtime_status_response)?
            {
                CapabilityRequestTimeoutOutcome::MarkedTimedOut
                | CapabilityRequestTimeoutOutcome::AlreadyTerminal => {
                    return Err(runtime_status_response(tonic::Status::deadline_exceeded(
                        "timed out waiting for node capability result",
                    )));
                }
                CapabilityRequestTimeoutOutcome::ResultCommitted => receiver.recv().await,
                CapabilityRequestTimeoutOutcome::Missing => {
                    return Err(runtime_status_response(tonic::Status::internal(
                        "node capability request disappeared during timeout handling",
                    )));
                }
            },
        };
    Ok(Json(node_capability_result_json(device_id.as_str(), capability.as_str(), result.result)))
}

async fn require_node_capability_local_mediation(
    state: &AppState,
    context: &gateway::RequestContext,
    node: &RegisteredNodeRecord,
    capability: &str,
    input_json: &Value,
    timeout_ms: u64,
) -> Result<(), Response> {
    let approval = state
        .runtime
        .create_approval_record(build_node_capability_approval_request(
            context, node, capability, input_json, timeout_ms,
        ))
        .await
        .map_err(runtime_status_response)?;
    let resolved = wait_for_node_capability_local_mediation_decision(
        state,
        approval.approval_id.as_str(),
        timeout_ms,
    )
    .await?;
    match resolved.decision {
        Some(ApprovalDecision::Allow) => Ok(()),
        Some(ApprovalDecision::Deny) => Err(runtime_status_response(
            tonic::Status::permission_denied("node capability local mediation denied the request"),
        )),
        Some(ApprovalDecision::Timeout) => {
            Err(runtime_status_response(tonic::Status::deadline_exceeded(
                "timed out waiting for node capability local mediation",
            )))
        }
        Some(ApprovalDecision::Error) | None => {
            Err(runtime_status_response(tonic::Status::failed_precondition(
                "node capability local mediation did not allow the request",
            )))
        }
    }
}

async fn wait_for_node_capability_local_mediation_decision(
    state: &AppState,
    approval_id: &str,
    timeout_ms: u64,
) -> Result<ApprovalRecord, Response> {
    let started = Instant::now();
    let timeout = Duration::from_millis(timeout_ms);
    loop {
        let Some(record) = state
            .runtime
            .approval_record(approval_id.to_owned())
            .await
            .map_err(runtime_status_response)?
        else {
            return Err(runtime_status_response(tonic::Status::not_found(format!(
                "node capability approval record not found: {approval_id}"
            ))));
        };
        if record.decision.is_some() {
            return Ok(record);
        }
        if started.elapsed() >= timeout {
            return state
                .runtime
                .resolve_approval_record(journal::ApprovalResolveRequest {
                    approval_id: approval_id.to_owned(),
                    decision: ApprovalDecision::Timeout,
                    decision_scope: ApprovalDecisionScope::Once,
                    decision_reason: "node capability local mediation timed out".to_owned(),
                    decision_scope_ttl_ms: None,
                })
                .await
                .map_err(runtime_status_response);
        }
        tokio::time::sleep(Duration::from_millis(NODE_CAPABILITY_LOCAL_MEDIATION_POLL_MS)).await;
    }
}

fn build_node_capability_approval_request(
    context: &gateway::RequestContext,
    node: &RegisteredNodeRecord,
    capability: &str,
    input_json: &Value,
    timeout_ms: u64,
) -> ApprovalCreateRequest {
    let approval_id = Ulid::generate().to_string();
    let details_json = json!({
        "device_id": node.device_id,
        "platform": node.platform,
        "capability": capability,
        "execution_mode": "local_mediation",
        "input_json": input_json,
    })
    .to_string();
    let prompt = ApprovalPromptRecord {
        title: format!("Approve local node capability {capability}"),
        risk_level: if capability == "desktop.open_path" {
            ApprovalRiskLevel::High
        } else {
            ApprovalRiskLevel::Medium
        },
        subject_id: format!("node_capability:{}:{capability}", node.device_id),
        summary: format!(
            "Node {} wants to execute local capability {capability}.",
            node.device_id
        ),
        options: vec![
            ApprovalPromptOption {
                option_id: "allow_once".to_owned(),
                label: "Allow once".to_owned(),
                description: "Run this local desktop capability one time.".to_owned(),
                default_selected: false,
                decision_scope: ApprovalDecisionScope::Once,
                timebox_ttl_ms: None,
            },
            ApprovalPromptOption {
                option_id: "deny".to_owned(),
                label: "Deny".to_owned(),
                description: "Do not run this local desktop capability.".to_owned(),
                default_selected: true,
                decision_scope: ApprovalDecisionScope::Once,
                timebox_ttl_ms: None,
            },
        ],
        timeout_seconds: u32::try_from(timeout_ms.saturating_add(999) / 1_000).unwrap_or(u32::MAX),
        details_json,
        policy_explanation:
            "Desktop node capabilities that can open local URLs or paths require explicit local mediation before dispatch."
                .to_owned(),
    };
    let policy_hash = hex::encode(Sha256::digest(prompt.details_json.as_bytes()));
    ApprovalCreateRequest {
        approval_id: approval_id.clone(),
        session_id: context.device_id.clone(),
        run_id: approval_id,
        principal: context.principal.clone(),
        device_id: node.device_id.clone(),
        channel: context.channel.clone(),
        subject_type: ApprovalSubjectType::NodeCapability,
        subject_id: prompt.subject_id.clone(),
        request_summary: format!(
            "device_id={} capability={capability} approval_required=true local_mediation=true",
            node.device_id
        ),
        policy_snapshot: ApprovalPolicySnapshot {
            policy_id: "node_capability.local_mediation.v1".to_owned(),
            policy_hash,
            evaluation_summary:
                "action=node.capability.execute approval_required=true local_mediation=true"
                    .to_owned(),
        },
        prompt,
    }
}

fn ensure_node_fresh_for_work(node: &RegisteredNodeRecord) -> Result<(), tonic::Status> {
    let now = unix_ms_now().map_err(|error| {
        tonic::Status::internal(format!("failed to read system clock: {error}"))
    })?;
    let ttl_ms =
        i64::try_from(REALTIME_DEFAULT_HEARTBEAT_INTERVAL_MS.saturating_mul(4)).unwrap_or(i64::MAX);
    if now.saturating_sub(node.last_seen_at_unix_ms) > ttl_ms {
        return Err(tonic::Status::failed_precondition(
            "stale node cannot receive new capability work",
        ));
    }
    Ok(())
}

fn collect_nodes(state: &AppState) -> Result<Vec<control_plane::NodeRecord>, tonic::Status> {
    state.node_runtime.nodes()?.iter().map(|node| Ok(node_record_json(node))).collect()
}

/// Converts a registered node into the control-plane node view.
pub(crate) fn node_record_json(node: &RegisteredNodeRecord) -> control_plane::NodeRecord {
    control_plane::NodeRecord {
        device_id: node.device_id.clone(),
        platform: node.platform.clone(),
        capabilities: node
            .capabilities
            .iter()
            .map(|capability| control_plane::NodeCapabilityView {
                name: capability.name.clone(),
                available: capability.available,
                execution_mode: capability_execution_mode(capability.name.as_str()).to_owned(),
            })
            .collect(),
        registered_at_unix_ms: node.registered_at_unix_ms,
        last_seen_at_unix_ms: node.last_seen_at_unix_ms,
        last_event_name: node.last_event_name.clone(),
        last_event_at_unix_ms: node.last_event_at_unix_ms,
    }
}

fn node_capability_result_json(
    device_id: &str,
    capability: &str,
    result: CapabilityExecutionResult,
) -> control_plane::NodeInvokeEnvelope {
    let output_json = if result.output_json.is_empty() {
        None
    } else {
        Some(serde_json::from_slice(&result.output_json).unwrap_or_else(
            |_| json!({ "raw_utf8": String::from_utf8_lossy(&result.output_json) }),
        ))
    };
    control_plane::NodeInvokeEnvelope {
        contract: contract_descriptor(),
        device_id: device_id.to_owned(),
        capability: capability.to_owned(),
        success: result.success,
        output_json,
        error: result.error,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::gateway::RequestContext;
    use crate::node_runtime::DeviceCapabilityView;

    #[test]
    fn node_capability_local_mediation_matches_desktop_openers_only() {
        assert!(capability_requires_local_mediation("desktop.open_url"));
        assert!(capability_requires_local_mediation("desktop.open_path"));
        assert!(capability_requires_local_mediation(" desktop.open_url "));
        assert!(!capability_requires_local_mediation("system.health"));
        assert!(!capability_requires_local_mediation("echo"));
    }

    #[test]
    fn node_capability_approval_request_captures_local_mediation_policy() {
        let context = RequestContext {
            principal: "admin:desktop".to_owned(),
            device_id: "01ARZ3NDEKTSV4RRFFQ69G5FAV".to_owned(),
            channel: Some("desktop".to_owned()),
        };
        let node = RegisteredNodeRecord {
            device_id: "01ARZ3NDEKTSV4RRFFQ69G5FAZ".to_owned(),
            platform: "windows-x86_64".to_owned(),
            capabilities: vec![DeviceCapabilityView {
                name: "desktop.open_path".to_owned(),
                available: true,
            }],
            registered_at_unix_ms: 1_700_000_000_000,
            last_seen_at_unix_ms: 1_700_000_001_000,
            last_event_name: None,
            last_event_at_unix_ms: None,
        };

        let request = build_node_capability_approval_request(
            &context,
            &node,
            "desktop.open_path",
            &json!({ "path": "C:\\Users\\palo\\Desktop\\report.txt" }),
            30_000,
        );

        assert_eq!(request.principal, "admin:desktop");
        assert_eq!(request.device_id, node.device_id);
        assert_eq!(request.channel.as_deref(), Some("desktop"));
        assert_eq!(request.subject_type, ApprovalSubjectType::NodeCapability);
        assert_eq!(request.policy_snapshot.policy_id, "node_capability.local_mediation.v1");
        assert!(request.policy_snapshot.evaluation_summary.contains("local_mediation=true"));
        assert_eq!(request.prompt.risk_level, ApprovalRiskLevel::High);
        assert_eq!(request.prompt.timeout_seconds, 30);
        assert!(request.prompt.details_json.contains("desktop.open_path"));
        assert!(request.prompt.details_json.contains("report.txt"));
        assert!(request.prompt.options.iter().any(|option| option.option_id == "allow_once"));
    }
}
