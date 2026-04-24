use std::time::Duration;

use crate::node_runtime::{CapabilityExecutionResult, RegisteredNodeRecord};
use crate::*;
use palyra_common::runtime_contracts::REALTIME_DEFAULT_HEARTBEAT_INTERVAL_MS;

fn capability_execution_mode(name: &str) -> &'static str {
    match name {
        "desktop.open_url" | "desktop.open_path" => "local_mediation",
        _ => "automatic",
    }
}

#[derive(Debug, Default, Deserialize)]
pub(crate) struct ConsoleNodesPendingQuery {
    #[serde(default, alias = "status")]
    state: Option<String>,
}

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

pub(crate) async fn console_nodes_list_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<control_plane::NodeListEnvelope>, Response> {
    let _session = authorize_console_session(&state, &headers, false)?;
    let nodes = collect_nodes(&state).map_err(runtime_status_response)?;
    let page = build_page_info(nodes.len().max(1), nodes.len(), None);
    Ok(Json(control_plane::NodeListEnvelope { contract: contract_descriptor(), nodes, page }))
}

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

pub(crate) async fn console_node_invoke_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(device_id): Path<String>,
    Json(payload): Json<ConsoleNodeInvokeRequest>,
) -> Result<Json<control_plane::NodeInvokeEnvelope>, Response> {
    let _session = authorize_console_session(&state, &headers, true)?;
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
    let (request_id, receiver) = state
        .node_runtime
        .enqueue_capability_request(
            device_id.as_str(),
            payload.capability.trim(),
            input_json,
            max_payload_bytes,
            Some(timeout_ms),
        )
        .map_err(runtime_status_response)?;
    let result = tokio::time::timeout(Duration::from_millis(timeout_ms), receiver)
        .await
        .map_err(|_| {
            let _ = state.node_runtime.mark_capability_timeout(request_id.as_str());
            runtime_status_response(tonic::Status::deadline_exceeded(
                "timed out waiting for node capability result",
            ))
        })?
        .map_err(|_| {
            runtime_status_response(tonic::Status::internal(
                "node capability result channel closed",
            ))
        })?;
    Ok(Json(node_capability_result_json(device_id.as_str(), payload.capability.as_str(), result)))
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
