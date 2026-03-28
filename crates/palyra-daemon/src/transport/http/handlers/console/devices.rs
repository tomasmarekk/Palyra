use std::{
    collections::HashMap,
    time::{SystemTime, UNIX_EPOCH},
};

use crate::node_runtime::DevicePairingRequestRecord;
use crate::*;

type DeviceHandlerStatus<T> = Result<T, tonic::Status>;

pub(crate) async fn console_devices_list_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<control_plane::DeviceListEnvelope>, Response> {
    let _session = authorize_console_session(&state, &headers, false)?;
    let devices = collect_device_records(&state).map_err(runtime_status_response)?;
    let page = build_page_info(devices.len().max(1), devices.len(), None);
    Ok(Json(control_plane::DeviceListEnvelope { contract: contract_descriptor(), devices, page }))
}

pub(crate) async fn console_device_get_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(device_id): Path<String>,
) -> Result<Json<control_plane::DeviceEnvelope>, Response> {
    let _session = authorize_console_session(&state, &headers, false)?;
    validate_device_id(device_id.as_str()).map_err(runtime_status_response)?;
    Ok(Json(control_plane::DeviceEnvelope {
        contract: contract_descriptor(),
        device: resolve_device_record(&state, device_id.as_str())
            .map_err(runtime_status_response)?,
    }))
}

pub(crate) async fn console_device_rotate_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(device_id): Path<String>,
) -> Result<Json<control_plane::DeviceEnvelope>, Response> {
    let _session = authorize_console_session(&state, &headers, true)?;
    validate_device_id(device_id.as_str()).map_err(runtime_status_response)?;
    {
        let mut identity = lock_identity_manager(&state).map_err(runtime_status_response)?;
        identity.force_rotate_device_certificate(device_id.as_str()).map_err(|error| {
            runtime_status_response(tonic::Status::failed_precondition(error.to_string()))
        })?;
    }
    let updated_at_unix_ms = current_unix_ms().map_err(runtime_status_response)?;
    Ok(Json(control_plane::DeviceEnvelope {
        contract: contract_descriptor(),
        device: resolve_device_record_with_updated_at(
            &state,
            device_id.as_str(),
            updated_at_unix_ms,
        )
        .map_err(runtime_status_response)?,
    }))
}

pub(crate) async fn console_device_revoke_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(device_id): Path<String>,
    Json(payload): Json<control_plane::DeviceActionRequest>,
) -> Result<Json<control_plane::DeviceEnvelope>, Response> {
    let _session = authorize_console_session(&state, &headers, true)?;
    validate_device_id(device_id.as_str()).map_err(runtime_status_response)?;
    {
        let mut identity = lock_identity_manager(&state).map_err(runtime_status_response)?;
        identity
            .revoke_device(
                device_id.as_str(),
                payload.reason.as_deref().unwrap_or("revoked_by_operator"),
                SystemTime::now(),
            )
            .map_err(|error| {
                runtime_status_response(tonic::Status::failed_precondition(error.to_string()))
            })?;
    }
    let _ = state.node_runtime.remove_node(device_id.as_str());
    Ok(Json(control_plane::DeviceEnvelope {
        contract: contract_descriptor(),
        device: resolve_device_record(&state, device_id.as_str())
            .map_err(runtime_status_response)?,
    }))
}

pub(crate) async fn console_device_remove_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(device_id): Path<String>,
    Json(payload): Json<control_plane::DeviceActionRequest>,
) -> Result<Json<control_plane::DeviceEnvelope>, Response> {
    let _session = authorize_console_session(&state, &headers, true)?;
    validate_device_id(device_id.as_str()).map_err(runtime_status_response)?;
    let existing =
        resolve_device_record(&state, device_id.as_str()).map_err(runtime_status_response)?;
    let removed = {
        let mut identity = lock_identity_manager(&state).map_err(runtime_status_response)?;
        let removed_paired =
            identity.remove_paired_device(device_id.as_str()).map_err(|error| {
                runtime_status_response(tonic::Status::failed_precondition(error.to_string()))
            })?;
        if removed_paired {
            true
        } else {
            identity.clear_revoked_device(device_id.as_str()).map_err(|error| {
                runtime_status_response(tonic::Status::failed_precondition(error.to_string()))
            })?
        }
    };
    if !removed {
        return Err(runtime_status_response(tonic::Status::not_found("device was not found")));
    }
    let _ = state.node_runtime.remove_node(device_id.as_str());
    let removed_at_unix_ms = current_unix_ms().map_err(runtime_status_response)?;
    Ok(Json(control_plane::DeviceEnvelope {
        contract: contract_descriptor(),
        device: control_plane::DeviceRecord {
            status: "removed".to_owned(),
            updated_at_unix_ms: removed_at_unix_ms,
            removed_at_unix_ms: Some(removed_at_unix_ms),
            revoked_reason: payload.reason.or_else(|| existing.revoked_reason.clone()),
            ..existing
        },
    }))
}

pub(crate) async fn console_devices_clear_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(_payload): Json<control_plane::DeviceClearRequest>,
) -> Result<Json<control_plane::DeviceClearEnvelope>, Response> {
    let _session = authorize_console_session(&state, &headers, true)?;
    let revoked_ids = {
        let identity = lock_identity_manager(&state).map_err(runtime_status_response)?;
        identity
            .revoked_device_records()
            .into_iter()
            .map(|record| record.device_id)
            .collect::<Vec<_>>()
    };
    let mut cleared = 0_usize;
    {
        let mut identity = lock_identity_manager(&state).map_err(runtime_status_response)?;
        for device_id in &revoked_ids {
            if identity.clear_revoked_device(device_id.as_str()).map_err(|error| {
                runtime_status_response(tonic::Status::failed_precondition(error.to_string()))
            })? {
                cleared += 1;
            }
        }
    }
    Ok(Json(control_plane::DeviceClearEnvelope {
        contract: contract_descriptor(),
        deleted: cleared,
    }))
}

fn collect_device_records(
    state: &AppState,
) -> DeviceHandlerStatus<Vec<control_plane::DeviceRecord>> {
    let (paired_devices, revoked_devices) = {
        let identity = lock_identity_manager(state)?;
        (identity.paired_devices(), identity.revoked_device_records())
    };
    let pairing_requests = latest_pairing_requests_by_device(state)?;

    let mut records = paired_devices
        .into_iter()
        .map(|paired| {
            build_paired_device_record(&paired, pairing_requests.get(&paired.device_id), None)
        })
        .collect::<DeviceHandlerStatus<Vec<_>>>()?;

    for revoked in revoked_devices {
        records
            .push(build_revoked_device_record(&revoked, pairing_requests.get(&revoked.device_id)));
    }

    records.sort_by(|left, right| left.device_id.cmp(&right.device_id));
    Ok(records)
}

fn resolve_device_record(
    state: &AppState,
    device_id: &str,
) -> DeviceHandlerStatus<control_plane::DeviceRecord> {
    resolve_device_record_with_updated_at(state, device_id, 0)
}

fn resolve_device_record_with_updated_at(
    state: &AppState,
    device_id: &str,
    updated_at_override: i64,
) -> DeviceHandlerStatus<control_plane::DeviceRecord> {
    let pairing_requests = latest_pairing_requests_by_device(state)?;
    let identity = lock_identity_manager(state)?;
    if let Some(paired) = identity.paired_device(device_id) {
        return build_paired_device_record(
            paired,
            pairing_requests.get(device_id),
            (updated_at_override > 0).then_some(updated_at_override),
        );
    }
    if let Some(revoked) = identity.revoked_device_record(device_id) {
        return Ok(build_revoked_device_record(revoked, pairing_requests.get(device_id)));
    }
    Err(tonic::Status::not_found("device was not found"))
}

fn latest_pairing_requests_by_device(
    state: &AppState,
) -> DeviceHandlerStatus<HashMap<String, DevicePairingRequestRecord>> {
    let mut records = HashMap::new();
    for record in state.node_runtime.pairing_requests()? {
        records.entry(record.device_id.clone()).or_insert(record);
    }
    Ok(records)
}

fn build_paired_device_record(
    paired: &palyra_identity::PairedDevice,
    metadata: Option<&DevicePairingRequestRecord>,
    updated_at_override: Option<i64>,
) -> DeviceHandlerStatus<control_plane::DeviceRecord> {
    let fallback_timestamp = current_unix_ms()?;
    let certificate_expires_at_unix_ms =
        i64::try_from(paired.current_certificate.expires_at_unix_ms)
            .map_err(|_| tonic::Status::internal("device certificate expiry overflowed i64"))?;
    let client_kind = metadata
        .map(|record| record.client_kind.as_str().to_owned())
        .unwrap_or_else(|| paired.client_kind.as_str().to_owned());
    let paired_at_unix_ms =
        metadata.map(|record| record.requested_at_unix_ms).unwrap_or(fallback_timestamp);
    let updated_at_unix_ms = updated_at_override.unwrap_or(paired_at_unix_ms);
    Ok(control_plane::DeviceRecord {
        device_id: paired.device_id.clone(),
        client_kind,
        status: "paired".to_owned(),
        paired_at_unix_ms,
        updated_at_unix_ms,
        issued_by: metadata
            .map(|record| record.code_issued_by.clone())
            .unwrap_or_else(|| "legacy".to_owned()),
        approval_id: metadata.map(|record| record.approval_id.clone()).unwrap_or_default(),
        identity_fingerprint: metadata
            .map(|record| record.verified_pairing.identity_fingerprint.clone())
            .unwrap_or_default(),
        transcript_hash_hex: metadata
            .map(|record| record.verified_pairing.transcript_hash_hex.clone())
            .unwrap_or_default(),
        current_certificate_fingerprint: paired.certificate_fingerprints.last().cloned(),
        current_certificate_expires_at_unix_ms: Some(certificate_expires_at_unix_ms),
        revoked_reason: None,
        revoked_at_unix_ms: None,
        removed_at_unix_ms: None,
    })
}

fn build_revoked_device_record(
    revoked: &palyra_identity::RevokedDevice,
    metadata: Option<&DevicePairingRequestRecord>,
) -> control_plane::DeviceRecord {
    control_plane::DeviceRecord {
        device_id: revoked.device_id.clone(),
        client_kind: metadata
            .map(|record| record.client_kind.as_str().to_owned())
            .unwrap_or_else(|| "unknown".to_owned()),
        status: "revoked".to_owned(),
        paired_at_unix_ms: metadata
            .map(|record| record.requested_at_unix_ms)
            .unwrap_or_else(|| i64::try_from(revoked.revoked_at_unix_ms).unwrap_or(i64::MAX)),
        updated_at_unix_ms: i64::try_from(revoked.revoked_at_unix_ms).unwrap_or(i64::MAX),
        issued_by: metadata
            .map(|record| record.code_issued_by.clone())
            .unwrap_or_else(|| "legacy".to_owned()),
        approval_id: metadata.map(|record| record.approval_id.clone()).unwrap_or_default(),
        identity_fingerprint: metadata
            .map(|record| record.verified_pairing.identity_fingerprint.clone())
            .unwrap_or_default(),
        transcript_hash_hex: metadata
            .map(|record| record.verified_pairing.transcript_hash_hex.clone())
            .unwrap_or_default(),
        current_certificate_fingerprint: None,
        current_certificate_expires_at_unix_ms: None,
        revoked_reason: Some(revoked.reason.clone()),
        revoked_at_unix_ms: Some(i64::try_from(revoked.revoked_at_unix_ms).unwrap_or(i64::MAX)),
        removed_at_unix_ms: None,
    }
}

fn current_unix_ms() -> DeviceHandlerStatus<i64> {
    let duration = SystemTime::now().duration_since(UNIX_EPOCH).map_err(|error| {
        tonic::Status::internal(format!("system clock was before UNIX epoch: {error}"))
    })?;
    i64::try_from(duration.as_millis()).map_err(|_| {
        tonic::Status::internal("system time overflowed i64 while building device response")
    })
}

fn validate_device_id(device_id: &str) -> DeviceHandlerStatus<()> {
    validate_canonical_id(device_id)
        .map_err(|_| tonic::Status::invalid_argument("device_id must be a canonical ULID"))
}

fn lock_identity_manager(
    state: &AppState,
) -> DeviceHandlerStatus<std::sync::MutexGuard<'_, palyra_identity::IdentityManager>> {
    state.identity_manager.lock().map_err(|_| {
        tonic::Status::internal("identity manager lock poisoned while handling device request")
    })
}
