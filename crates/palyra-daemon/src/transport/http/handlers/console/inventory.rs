use std::cmp::Reverse;
use std::collections::HashMap;

use super::diagnostics::{
    authorize_console_session, build_connector_observability, build_page_info,
    collect_console_browser_diagnostics,
};
use crate::gateway::current_unix_ms;
use crate::*;

const NODE_STALE_AFTER_MS: i64 = 5 * 60 * 1000;
const NODE_OFFLINE_AFTER_MS: i64 = 30 * 60 * 1000;

pub(crate) async fn console_inventory_list_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<control_plane::InventoryListEnvelope>, Response> {
    let session = authorize_console_session(&state, &headers, false)?;
    let generated_at_unix_ms = current_unix_ms();
    let all_pairings = collect_inventory_pairings(&state).map_err(runtime_status_response)?;
    let active_pairings = active_inventory_pairings(all_pairings.as_slice());
    let devices = build_inventory_devices(&state, all_pairings.as_slice(), generated_at_unix_ms)
        .map_err(runtime_status_response)?;
    let instances = build_inventory_instances(&state, &session, generated_at_unix_ms).await?;
    let summary = build_inventory_summary(
        devices.as_slice(),
        active_pairings.as_slice(),
        instances.as_slice(),
    );

    Ok(Json(control_plane::InventoryListEnvelope {
        contract: contract_descriptor(),
        generated_at_unix_ms,
        summary,
        page: build_page_info(devices.len().max(1), devices.len(), None),
        devices,
        pending_pairings: active_pairings,
        instances,
    }))
}

pub(crate) async fn console_inventory_device_detail_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(device_id): Path<String>,
) -> Result<Json<control_plane::InventoryDeviceDetailEnvelope>, Response> {
    let _session = authorize_console_session(&state, &headers, false)?;
    validate_canonical_id(device_id.as_str()).map_err(|_| {
        runtime_status_response(tonic::Status::invalid_argument(
            "device_id must be a canonical ULID",
        ))
    })?;
    let generated_at_unix_ms = current_unix_ms();
    let all_pairings = collect_inventory_pairings(&state).map_err(runtime_status_response)?;
    let devices = build_inventory_devices(&state, all_pairings.as_slice(), generated_at_unix_ms)
        .map_err(runtime_status_response)?;
    let device = devices
        .into_iter()
        .find(|record| record.device_id == device_id)
        .ok_or_else(|| runtime_status_response(tonic::Status::not_found("device was not found")))?;
    let mut pairings = all_pairings
        .into_iter()
        .filter(|record| record.device_id == device.device_id)
        .collect::<Vec<_>>();
    pairings.sort_by_key(|record| Reverse(record.requested_at_unix_ms));
    let capability_requests = state
        .node_runtime
        .capability_requests(Some(device.device_id.as_str()))
        .map_err(runtime_status_response)?
        .into_iter()
        .map(capability_request_view)
        .collect::<Vec<_>>();
    let workspace_activity =
        crate::application::workspace_observability::load_workspace_activity_snapshot(
            &state.runtime,
            crate::application::workspace_observability::WorkspaceActivityQuery {
                session_id: None,
                run_id: None,
                device_id: Some(device.device_id.as_str()),
                limit: 6,
            },
        )
        .await
        .map_err(runtime_status_response)?;

    Ok(Json(control_plane::InventoryDeviceDetailEnvelope {
        contract: contract_descriptor(),
        generated_at_unix_ms,
        device,
        pairings,
        capability_requests,
        workspace_activity: Some(control_plane::InventoryWorkspaceActivity {
            summary: control_plane::InventoryWorkspaceRestoreSummary {
                checkpoint_count: workspace_activity.summary.checkpoint_count,
                checkpoint_restore_total: workspace_activity.summary.checkpoint_restore_total,
                restore_report_count: workspace_activity.summary.restore_report_count,
                succeeded_restore_count: workspace_activity.summary.succeeded_restore_count,
                partial_failure_restore_count: workspace_activity
                    .summary
                    .partial_failure_restore_count,
                failed_restore_count: workspace_activity.summary.failed_restore_count,
            },
            recent_checkpoints: workspace_activity
                .recent_checkpoints
                .into_iter()
                .map(inventory_workspace_checkpoint_record)
                .collect(),
            recent_restore_reports: workspace_activity
                .recent_restore_reports
                .into_iter()
                .map(inventory_workspace_restore_report_record)
                .collect(),
        }),
    }))
}

fn capability_request_view(
    record: crate::node_runtime::CapabilityRequestRecord,
) -> control_plane::NodeCapabilityRequestView {
    control_plane::NodeCapabilityRequestView {
        request_id: record.request_id,
        device_id: record.device_id,
        capability: record.capability,
        state: match record.state {
            crate::node_runtime::CapabilityRequestState::Queued => {
                control_plane::NodeCapabilityRequestState::Queued
            }
            crate::node_runtime::CapabilityRequestState::Dispatched => {
                control_plane::NodeCapabilityRequestState::Dispatched
            }
            crate::node_runtime::CapabilityRequestState::AwaitingLocalMediation => {
                control_plane::NodeCapabilityRequestState::AwaitingLocalMediation
            }
            crate::node_runtime::CapabilityRequestState::Succeeded => {
                control_plane::NodeCapabilityRequestState::Succeeded
            }
            crate::node_runtime::CapabilityRequestState::Failed => {
                control_plane::NodeCapabilityRequestState::Failed
            }
            crate::node_runtime::CapabilityRequestState::TimedOut => {
                control_plane::NodeCapabilityRequestState::TimedOut
            }
            crate::node_runtime::CapabilityRequestState::Rejected => {
                control_plane::NodeCapabilityRequestState::Rejected
            }
        },
        created_at_unix_ms: record.created_at_unix_ms,
        updated_at_unix_ms: record.updated_at_unix_ms,
        dispatched_at_unix_ms: record.dispatched_at_unix_ms,
        completed_at_unix_ms: record.completed_at_unix_ms,
        max_payload_bytes: record.max_payload_bytes,
        input_summary: record.input_summary,
        output_summary: record.output_summary,
        error: record.error,
    }
}

fn inventory_workspace_checkpoint_record(
    checkpoint: crate::application::workspace_observability::WorkspaceCheckpointSummary,
) -> control_plane::InventoryWorkspaceCheckpointRecord {
    control_plane::InventoryWorkspaceCheckpointRecord {
        checkpoint_id: checkpoint.checkpoint_id,
        session_id: checkpoint.session_id,
        run_id: checkpoint.run_id,
        source_kind: checkpoint.source_kind,
        source_label: checkpoint.source_label,
        tool_name: checkpoint.tool_name,
        proposal_id: checkpoint.proposal_id,
        actor_principal: checkpoint.actor_principal,
        device_id: checkpoint.device_id,
        channel: checkpoint.channel,
        summary_text: checkpoint.summary_text,
        created_at_unix_ms: checkpoint.created_at_unix_ms,
        restore_count: checkpoint.restore_count,
        last_restored_at_unix_ms: checkpoint.last_restored_at_unix_ms,
        latest_restore_report_id: checkpoint.latest_restore_report_id,
    }
}

fn inventory_workspace_restore_report_record(
    report: crate::application::workspace_observability::WorkspaceRestoreReportSummary,
) -> control_plane::InventoryWorkspaceRestoreReportRecord {
    control_plane::InventoryWorkspaceRestoreReportRecord {
        report_id: report.report_id,
        checkpoint_id: report.checkpoint_id,
        session_id: report.session_id,
        run_id: report.run_id,
        actor_principal: report.actor_principal,
        device_id: report.device_id,
        channel: report.channel,
        scope_kind: report.scope_kind,
        target_path: report.target_path,
        reconciliation_summary: report.reconciliation_summary,
        branched_session_id: report.branched_session_id,
        result_state: report.result_state,
        created_at_unix_ms: report.created_at_unix_ms,
    }
}

fn build_inventory_devices(
    state: &AppState,
    pairings: &[control_plane::NodePairingRequestView],
    now_unix_ms: i64,
) -> Result<Vec<control_plane::InventoryDeviceRecord>, tonic::Status> {
    let devices = super::devices::collect_device_records(state)?;
    let nodes = state.node_runtime.nodes()?;
    let node_lookup = nodes
        .iter()
        .map(|record| (record.device_id.clone(), super::nodes::node_record_json(record)))
        .collect::<HashMap<_, _>>();
    let mut pairings_by_device =
        HashMap::<String, Vec<control_plane::NodePairingRequestView>>::new();
    for pairing in pairings {
        pairings_by_device.entry(pairing.device_id.clone()).or_default().push(pairing.clone());
    }

    let mut records = devices
        .iter()
        .map(|device| {
            build_inventory_device_record(
                device,
                node_lookup.get(device.device_id.as_str()),
                pairings_by_device.get(device.device_id.as_str()),
                now_unix_ms,
            )
        })
        .collect::<Vec<_>>();

    records.sort_by(|left, right| {
        inventory_presence_rank(left.presence_state)
            .cmp(&inventory_presence_rank(right.presence_state))
            .then_with(|| left.device_id.cmp(&right.device_id))
    });
    Ok(records)
}

fn build_inventory_device_record(
    device: &control_plane::DeviceRecord,
    node: Option<&control_plane::NodeRecord>,
    pairings: Option<&Vec<control_plane::NodePairingRequestView>>,
    now_unix_ms: i64,
) -> control_plane::InventoryDeviceRecord {
    let capability_summary = inventory_capability_summary(
        node.map(|value| value.capabilities.as_slice()).unwrap_or(&[]),
    );
    let unavailable_capabilities = capability_summary.unavailable;
    let can_invoke = node.is_some() && capability_summary.available > 0;
    let presence_state =
        inventory_device_presence_state(device, node, unavailable_capabilities, now_unix_ms);
    let trust_state = inventory_device_trust_state(device);
    let heartbeat_age_ms =
        node.map(|record| now_unix_ms.saturating_sub(record.last_seen_at_unix_ms.max(0)));
    let latest_session_id = pairings
        .and_then(|records| {
            records
                .iter()
                .max_by_key(|record| record.requested_at_unix_ms)
                .map(|record| record.session_id.clone())
        })
        .filter(|value| !value.trim().is_empty());
    let pending_pairings = pairings
        .map(|records| {
            records
                .iter()
                .filter(|record| {
                    matches!(
                        record.state,
                        control_plane::NodePairingRequestState::PendingApproval
                            | control_plane::NodePairingRequestState::Approved
                    )
                })
                .count()
        })
        .unwrap_or(0);

    control_plane::InventoryDeviceRecord {
        device_id: device.device_id.clone(),
        client_kind: device.client_kind.clone(),
        device_status: device.status.clone(),
        trust_state,
        presence_state,
        paired_at_unix_ms: device.paired_at_unix_ms,
        updated_at_unix_ms: device.updated_at_unix_ms,
        registered_at_unix_ms: node.map(|record| record.registered_at_unix_ms),
        last_seen_at_unix_ms: node.map(|record| record.last_seen_at_unix_ms),
        heartbeat_age_ms,
        latest_session_id,
        pending_pairings,
        issued_by: device.issued_by.clone(),
        approval_id: device.approval_id.clone(),
        identity_fingerprint: device.identity_fingerprint.clone(),
        transcript_hash_hex: device.transcript_hash_hex.clone(),
        current_certificate_fingerprint: device.current_certificate_fingerprint.clone(),
        certificate_fingerprint_history: device.certificate_fingerprint_history.clone(),
        platform: node.map(|record| record.platform.clone()),
        capabilities: node.map(|record| record.capabilities.clone()).unwrap_or_default(),
        capability_summary,
        last_event_name: node.and_then(|record| record.last_event_name.clone()),
        last_event_at_unix_ms: node.and_then(|record| record.last_event_at_unix_ms),
        current_certificate_expires_at_unix_ms: device.current_certificate_expires_at_unix_ms,
        revoked_reason: device.revoked_reason.clone(),
        warnings: inventory_device_warnings(
            device,
            node,
            presence_state,
            unavailable_capabilities,
            pending_pairings,
        ),
        actions: control_plane::InventoryActionAvailability {
            can_rotate: device.status == "paired",
            can_revoke: device.status == "paired",
            can_remove: matches!(device.status.as_str(), "paired" | "revoked"),
            can_invoke,
        },
    }
}

async fn build_inventory_instances(
    state: &AppState,
    session: &ConsoleSession,
    now_unix_ms: i64,
) -> Result<Vec<control_plane::InventoryInstanceRecord>, Response> {
    let status_snapshot = state
        .runtime
        .status_snapshot_async(session.context.clone(), state.auth.clone())
        .await
        .map_err(runtime_status_response)?;
    let browser_payload = collect_console_browser_diagnostics(state).await;
    let media_payload = state.channels.media_snapshot().map_err(channel_platform_error_response)?;
    let connector_payload =
        build_connector_observability(state, &media_payload).map_err(|error| *error)?;

    let browser_enabled = browser_payload.get("enabled").and_then(Value::as_bool).unwrap_or(false);
    let browser_health_failures = browser_payload
        .pointer("/failures/recent_health_failures")
        .and_then(Value::as_u64)
        .unwrap_or(0);
    let browser_relay_failures = browser_payload
        .pointer("/failures/recent_relay_action_failures")
        .and_then(Value::as_u64)
        .unwrap_or(0);
    let browser_status_label = if browser_enabled {
        browser_payload.pointer("/health/status").and_then(Value::as_str).unwrap_or({
            if browser_health_failures > 0 || browser_relay_failures > 0 {
                "degraded"
            } else {
                "ok"
            }
        })
    } else {
        "disabled"
    };
    let browser_presence_label = if browser_enabled { browser_status_label } else { "offline" };

    Ok(vec![
        control_plane::InventoryInstanceRecord {
            instance_id: "palyrad".to_owned(),
            label: "palyrad".to_owned(),
            kind: "gateway".to_owned(),
            presence_state: inventory_presence_from_runtime_label(status_snapshot.status),
            observed_at_unix_ms: now_unix_ms,
            state_label: status_snapshot.status.to_owned(),
            detail: Some(format!(
                "{} uptime={}s",
                status_snapshot.service, status_snapshot.uptime_seconds
            )),
            capability_summary: control_plane::InventoryCapabilitySummary {
                total: 1,
                available: usize::from(matches!(
                    inventory_presence_from_runtime_label(status_snapshot.status),
                    control_plane::InventoryPresenceState::Ok
                )),
                unavailable: usize::from(!matches!(
                    inventory_presence_from_runtime_label(status_snapshot.status),
                    control_plane::InventoryPresenceState::Ok
                )),
            },
        },
        control_plane::InventoryInstanceRecord {
            instance_id: "browserd".to_owned(),
            label: "Browser service".to_owned(),
            kind: "browserd".to_owned(),
            presence_state: inventory_presence_from_runtime_label(browser_presence_label),
            observed_at_unix_ms: now_unix_ms,
            state_label: browser_status_label.to_owned(),
            detail: Some(format!(
                "{} active sessions",
                browser_payload.pointer("/sessions/active").and_then(Value::as_u64).unwrap_or(0)
            )),
            capability_summary: control_plane::InventoryCapabilitySummary {
                total: 1,
                available: usize::from(browser_enabled && browser_health_failures == 0),
                unavailable: usize::from(!browser_enabled || browser_health_failures > 0),
            },
        },
        control_plane::InventoryInstanceRecord {
            instance_id: "channels".to_owned(),
            label: "Channels runtime".to_owned(),
            kind: "channels".to_owned(),
            presence_state: if connector_payload
                .get("degraded_connectors")
                .and_then(Value::as_u64)
                .unwrap_or(0)
                > 0
                || connector_payload.get("dead_letters").and_then(Value::as_u64).unwrap_or(0) > 0
            {
                control_plane::InventoryPresenceState::Degraded
            } else {
                control_plane::InventoryPresenceState::Ok
            },
            observed_at_unix_ms: now_unix_ms,
            state_label: if connector_payload
                .get("degraded_connectors")
                .and_then(Value::as_u64)
                .unwrap_or(0)
                > 0
                || connector_payload.get("dead_letters").and_then(Value::as_u64).unwrap_or(0) > 0
            {
                "degraded".to_owned()
            } else {
                "ok".to_owned()
            },
            detail: Some(format!(
                "{} connectors, {} degraded, {} dead letters",
                connector_payload.get("connectors").and_then(Value::as_u64).unwrap_or(0),
                connector_payload.get("degraded_connectors").and_then(Value::as_u64).unwrap_or(0),
                connector_payload.get("dead_letters").and_then(Value::as_u64).unwrap_or(0)
            )),
            capability_summary: {
                let total = connector_payload.get("connectors").and_then(Value::as_u64).unwrap_or(0)
                    as usize;
                let unavailable = connector_payload
                    .get("degraded_connectors")
                    .and_then(Value::as_u64)
                    .unwrap_or(0) as usize;
                control_plane::InventoryCapabilitySummary {
                    total,
                    available: total.saturating_sub(unavailable),
                    unavailable,
                }
            },
        },
    ])
}

fn collect_inventory_pairings(
    state: &AppState,
) -> Result<Vec<control_plane::NodePairingRequestView>, tonic::Status> {
    let mut records = state
        .node_runtime
        .pairing_requests()?
        .into_iter()
        .filter(|record| record.client_kind == palyra_identity::PairingClientKind::Node)
        .map(|record| super::pairing::control_plane_node_pairing_request_view(&record))
        .collect::<Vec<_>>();
    records.sort_by_key(|record| Reverse(record.requested_at_unix_ms));
    Ok(records)
}

fn active_inventory_pairings(
    pairings: &[control_plane::NodePairingRequestView],
) -> Vec<control_plane::NodePairingRequestView> {
    pairings
        .iter()
        .filter(|record| {
            matches!(
                record.state,
                control_plane::NodePairingRequestState::PendingApproval
                    | control_plane::NodePairingRequestState::Approved
            )
        })
        .cloned()
        .collect()
}

fn build_inventory_summary(
    devices: &[control_plane::InventoryDeviceRecord],
    pending_pairings: &[control_plane::NodePairingRequestView],
    instances: &[control_plane::InventoryInstanceRecord],
) -> control_plane::InventorySummary {
    let mut summary = control_plane::InventorySummary {
        devices: devices.len(),
        trusted_devices: 0,
        pending_pairings: pending_pairings.len(),
        ok_devices: 0,
        stale_devices: 0,
        degraded_devices: 0,
        offline_devices: 0,
        ok_instances: 0,
        stale_instances: 0,
        degraded_instances: 0,
        offline_instances: 0,
    };

    for device in devices {
        if matches!(device.trust_state, control_plane::InventoryTrustState::Trusted) {
            summary.trusted_devices = summary.trusted_devices.saturating_add(1);
        }
        increment_presence_counts(
            device.presence_state,
            &mut summary.ok_devices,
            &mut summary.stale_devices,
            &mut summary.degraded_devices,
            &mut summary.offline_devices,
        );
    }
    for instance in instances {
        increment_presence_counts(
            instance.presence_state,
            &mut summary.ok_instances,
            &mut summary.stale_instances,
            &mut summary.degraded_instances,
            &mut summary.offline_instances,
        );
    }

    summary
}

fn increment_presence_counts(
    state: control_plane::InventoryPresenceState,
    ok: &mut usize,
    stale: &mut usize,
    degraded: &mut usize,
    offline: &mut usize,
) {
    match state {
        control_plane::InventoryPresenceState::Ok => *ok = ok.saturating_add(1),
        control_plane::InventoryPresenceState::Stale => *stale = stale.saturating_add(1),
        control_plane::InventoryPresenceState::Degraded => *degraded = degraded.saturating_add(1),
        control_plane::InventoryPresenceState::Offline => *offline = offline.saturating_add(1),
    }
}

fn inventory_capability_summary(
    capabilities: &[control_plane::NodeCapabilityView],
) -> control_plane::InventoryCapabilitySummary {
    control_plane::InventoryCapabilitySummary {
        total: capabilities.len(),
        available: capabilities.iter().filter(|capability| capability.available).count(),
        unavailable: capabilities.iter().filter(|capability| !capability.available).count(),
    }
}

fn inventory_device_presence_state(
    device: &control_plane::DeviceRecord,
    node: Option<&control_plane::NodeRecord>,
    unavailable_capabilities: usize,
    now_unix_ms: i64,
) -> control_plane::InventoryPresenceState {
    if device.status != "paired" {
        return control_plane::InventoryPresenceState::Offline;
    }
    let Some(node) = node else {
        return control_plane::InventoryPresenceState::Offline;
    };
    let age_ms = now_unix_ms.saturating_sub(node.last_seen_at_unix_ms.max(0));
    if age_ms >= NODE_OFFLINE_AFTER_MS {
        control_plane::InventoryPresenceState::Offline
    } else if age_ms >= NODE_STALE_AFTER_MS {
        control_plane::InventoryPresenceState::Stale
    } else if unavailable_capabilities > 0 {
        control_plane::InventoryPresenceState::Degraded
    } else {
        control_plane::InventoryPresenceState::Ok
    }
}

fn inventory_device_trust_state(
    device: &control_plane::DeviceRecord,
) -> control_plane::InventoryTrustState {
    match device.status.as_str() {
        "revoked" => control_plane::InventoryTrustState::Revoked,
        "removed" => control_plane::InventoryTrustState::Removed,
        "paired"
            if !device.approval_id.trim().is_empty()
                && !device.identity_fingerprint.trim().is_empty() =>
        {
            control_plane::InventoryTrustState::Trusted
        }
        "paired" => control_plane::InventoryTrustState::Legacy,
        _ => control_plane::InventoryTrustState::Unknown,
    }
}

fn inventory_device_warnings(
    device: &control_plane::DeviceRecord,
    node: Option<&control_plane::NodeRecord>,
    presence_state: control_plane::InventoryPresenceState,
    unavailable_capabilities: usize,
    pending_pairings: usize,
) -> Vec<String> {
    let mut warnings = Vec::new();
    if pending_pairings > 0 {
        warnings.push(format!("{pending_pairings} pairing requests still require completion"));
    }
    match presence_state {
        control_plane::InventoryPresenceState::Stale => {
            warnings.push("last node heartbeat is stale".to_owned());
        }
        control_plane::InventoryPresenceState::Offline if device.status == "paired" => {
            warnings.push("no active node heartbeat is currently visible".to_owned());
        }
        _ => {}
    }
    if unavailable_capabilities > 0 {
        warnings.push(format!("{unavailable_capabilities} capabilities are currently unavailable"));
    }
    if let Some(reason) = device.revoked_reason.as_deref().filter(|value| !value.trim().is_empty())
    {
        warnings.push(format!("device revoked: {reason}"));
    }
    if node.is_none() && device.status == "paired" {
        warnings.push("node runtime has not registered this paired device".to_owned());
    }
    warnings
}

fn inventory_presence_from_runtime_label(raw: &str) -> control_plane::InventoryPresenceState {
    match raw.trim().to_ascii_lowercase().as_str() {
        "ok" | "ready" | "healthy" | "running" | "active" | "connected" => {
            control_plane::InventoryPresenceState::Ok
        }
        "stale" => control_plane::InventoryPresenceState::Stale,
        "degraded" | "warning" => control_plane::InventoryPresenceState::Degraded,
        "disabled" | "offline" | "down" | "failed" | "stopped" | "missing" => {
            control_plane::InventoryPresenceState::Offline
        }
        _ => control_plane::InventoryPresenceState::Degraded,
    }
}

fn inventory_presence_rank(state: control_plane::InventoryPresenceState) -> usize {
    match state {
        control_plane::InventoryPresenceState::Degraded => 0,
        control_plane::InventoryPresenceState::Stale => 1,
        control_plane::InventoryPresenceState::Offline => 2,
        control_plane::InventoryPresenceState::Ok => 3,
    }
}

#[cfg(test)]
mod tests {
    use super::{inventory_device_presence_state, inventory_device_trust_state};
    use palyra_control_plane as control_plane;

    #[test]
    fn paired_node_transitions_between_ok_stale_and_offline() {
        let device = control_plane::DeviceRecord {
            device_id: "01ARZ3NDEKTSV4RRFFQ69G5FAZ".to_owned(),
            client_kind: "node".to_owned(),
            status: "paired".to_owned(),
            paired_at_unix_ms: 1_000,
            updated_at_unix_ms: 1_000,
            issued_by: "admin:test".to_owned(),
            approval_id: "approval-1".to_owned(),
            identity_fingerprint: "fingerprint-1".to_owned(),
            transcript_hash_hex: "hash-1".to_owned(),
            current_certificate_fingerprint: None,
            certificate_fingerprint_history: Vec::new(),
            current_certificate_expires_at_unix_ms: None,
            revoked_reason: None,
            revoked_at_unix_ms: None,
            removed_at_unix_ms: None,
        };
        let node = control_plane::NodeRecord {
            device_id: device.device_id.clone(),
            platform: "windows".to_owned(),
            capabilities: vec![control_plane::NodeCapabilityView {
                name: "ping".to_owned(),
                available: true,
                execution_mode: "automatic".to_owned(),
            }],
            registered_at_unix_ms: 1_000,
            last_seen_at_unix_ms: 1_000,
            last_event_name: None,
            last_event_at_unix_ms: None,
        };

        assert_eq!(
            inventory_device_presence_state(&device, Some(&node), 0, 1_001),
            control_plane::InventoryPresenceState::Ok
        );
        assert_eq!(
            inventory_device_presence_state(
                &device,
                Some(&node),
                0,
                1_000 + super::NODE_STALE_AFTER_MS
            ),
            control_plane::InventoryPresenceState::Stale
        );
        assert_eq!(
            inventory_device_presence_state(
                &device,
                Some(&node),
                0,
                1_000 + super::NODE_OFFLINE_AFTER_MS
            ),
            control_plane::InventoryPresenceState::Offline
        );
    }

    #[test]
    fn trust_state_distinguishes_trusted_legacy_and_revoked_devices() {
        let mut trusted = control_plane::DeviceRecord {
            device_id: "01ARZ3NDEKTSV4RRFFQ69G5FAZ".to_owned(),
            client_kind: "node".to_owned(),
            status: "paired".to_owned(),
            paired_at_unix_ms: 1_000,
            updated_at_unix_ms: 1_000,
            issued_by: "admin:test".to_owned(),
            approval_id: "approval-1".to_owned(),
            identity_fingerprint: "fingerprint-1".to_owned(),
            transcript_hash_hex: "hash-1".to_owned(),
            current_certificate_fingerprint: None,
            certificate_fingerprint_history: Vec::new(),
            current_certificate_expires_at_unix_ms: None,
            revoked_reason: None,
            revoked_at_unix_ms: None,
            removed_at_unix_ms: None,
        };

        assert_eq!(
            inventory_device_trust_state(&trusted),
            control_plane::InventoryTrustState::Trusted
        );

        trusted.approval_id.clear();
        assert_eq!(
            inventory_device_trust_state(&trusted),
            control_plane::InventoryTrustState::Legacy
        );

        trusted.status = "revoked".to_owned();
        assert_eq!(
            inventory_device_trust_state(&trusted),
            control_plane::InventoryTrustState::Revoked
        );
    }
}
