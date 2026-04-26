use crate::maintenance::{
    collect_doctor_health_graph, collect_maintenance_status, publish_maintenance_realtime_event,
    DoctorHealthGraphSnapshot, MaintenanceStatusFilter, MaintenanceStatusSnapshot,
};
use crate::*;
use serde::Serialize;

#[derive(Debug, Deserialize)]
pub(crate) struct ConsoleMaintenanceStatusQuery {
    #[serde(default)]
    component: Option<String>,
    #[serde(default)]
    severity: Option<String>,
}

pub(crate) async fn console_maintenance_status_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<ConsoleMaintenanceStatusQuery>,
) -> Result<Json<Value>, Response> {
    let session = authorize_console_session(&state, &headers, false)?;
    let component_filter = query.component.clone();
    let severity_filter = query.severity.clone();
    publish_maintenance_realtime_event(
        &state,
        Some(session.context.principal.clone()),
        json!({
            "event": "maintenance.status.started",
            "component_filter": component_filter,
            "severity_filter": severity_filter,
        }),
    );
    let filter = MaintenanceStatusFilter { component: query.component, severity: query.severity };
    match collect_maintenance_status(&state, &session.context, filter).await {
        Ok(snapshot) => {
            publish_maintenance_realtime_event(
                &state,
                Some(session.context.principal),
                json!({
                    "event": "maintenance.status.completed",
                    "overall_state": snapshot.summary.overall_state.as_str(),
                    "highest_severity": snapshot.summary.highest_severity.as_str(),
                    "task_count": snapshot.summary.total_tasks,
                    "fix_count": snapshot.summary.fix_count,
                    "error_count": snapshot.summary.error_count,
                }),
            );
            Ok(Json(maintenance_payload(snapshot)?))
        }
        Err(error) => {
            publish_maintenance_realtime_event(
                &state,
                Some(session.context.principal),
                json!({
                    "event": "maintenance.status.failed",
                    "error": sanitize_http_error_message(error.message()),
                }),
            );
            Err(runtime_status_response(error))
        }
    }
}

pub(crate) async fn console_doctor_health_graph_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<Value>, Response> {
    let session = authorize_console_session(&state, &headers, false)?;
    let graph = collect_doctor_health_graph(&state, &session.context)
        .await
        .map_err(runtime_status_response)?;
    Ok(Json(doctor_health_graph_payload(graph)?))
}

fn maintenance_payload(snapshot: MaintenanceStatusSnapshot) -> Result<Value, Response> {
    payload_with_contract(snapshot, "maintenance status")
}

fn doctor_health_graph_payload(snapshot: DoctorHealthGraphSnapshot) -> Result<Value, Response> {
    payload_with_contract(snapshot, "doctor health graph")
}

fn payload_with_contract<T>(snapshot: T, label: &str) -> Result<Value, Response>
where
    T: Serialize,
{
    let mut payload = serde_json::to_value(snapshot).map_err(|error| {
        runtime_status_response(tonic::Status::internal(format!(
            "failed to serialize {label} payload: {error}"
        )))
    })?;
    if let Some(object) = payload.as_object_mut() {
        object.insert(
            "contract".to_owned(),
            serde_json::to_value(contract_descriptor()).map_err(|error| {
                runtime_status_response(tonic::Status::internal(format!(
                    "failed to serialize {label} contract descriptor: {error}"
                )))
            })?,
        );
    }
    Ok(payload)
}
