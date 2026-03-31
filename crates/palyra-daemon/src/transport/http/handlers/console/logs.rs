use std::cmp::Reverse;
use std::collections::BTreeSet;

use super::diagnostics::{
    authorize_console_session, build_page_info, redact_console_diagnostics_value,
};
use crate::*;

const DEFAULT_LOG_LIMIT: usize = 200;
const MAX_LOG_LIMIT: usize = 2_000;
const LOG_SCAN_MULTIPLIER: usize = 4;

#[derive(Debug, Deserialize)]
pub(crate) struct ConsoleLogsQuery {
    limit: Option<usize>,
    cursor: Option<String>,
    direction: Option<String>,
    format: Option<String>,
    source: Option<String>,
    severity: Option<String>,
    contains: Option<String>,
    start_at_unix_ms: Option<i64>,
    end_at_unix_ms: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct LogSortKey {
    timestamp_unix_ms: i64,
    tie_breaker: String,
}

pub(crate) async fn console_logs_list_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<ConsoleLogsQuery>,
) -> Result<Json<control_plane::LogListEnvelope>, Response> {
    let _session = authorize_console_session(&state, &headers, false)?;
    let limit = query.limit.unwrap_or(DEFAULT_LOG_LIMIT).clamp(1, MAX_LOG_LIMIT);
    let direction =
        normalize_log_direction(query.direction.as_deref()).map_err(runtime_status_response)?;
    let cursor = query.cursor.as_deref().and_then(parse_log_cursor);
    let source_filter = normalize_optional_query_text(query.source.as_deref());
    let severity_filter = normalize_optional_query_text(query.severity.as_deref());
    let contains_filter = normalize_optional_query_text(query.contains.as_deref());

    let mut records =
        collect_log_records(&state, limit.saturating_mul(LOG_SCAN_MULTIPLIER)).await?;
    records.retain(|record| {
        record.timestamp_unix_ms >= query.start_at_unix_ms.unwrap_or(i64::MIN)
            && record.timestamp_unix_ms <= query.end_at_unix_ms.unwrap_or(i64::MAX)
    });
    if let Some(filter) = source_filter.as_deref() {
        records.retain(|record| log_source_matches(record.source.as_str(), filter));
    }
    if let Some(filter) = severity_filter.as_deref() {
        records.retain(|record| record.severity.eq_ignore_ascii_case(filter));
    }
    if let Some(filter) = contains_filter.as_deref() {
        records.retain(|record| log_contains(record, filter));
    }
    if let Some(cursor) = cursor.as_ref() {
        records.retain(|record| match direction {
            "after" => log_record_sort_key(record) > *cursor,
            _ => log_record_sort_key(record) < *cursor,
        });
    }
    records.sort_by_key(|record| Reverse(log_record_sort_key(record)));

    let available_sources = records
        .iter()
        .map(|record| record.source.clone())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    let has_more = records.len() > limit;
    records.truncate(limit);
    let returned = records.len();

    let newest_cursor = records.first().map(|record| record.cursor.clone());
    let next_cursor =
        has_more.then(|| records.last().map(|record| record.cursor.clone())).flatten();
    Ok(Json(control_plane::LogListEnvelope {
        contract: contract_descriptor(),
        query: control_plane::LogQueryEcho {
            limit,
            direction: direction.to_owned(),
            cursor: query.cursor,
            source: source_filter,
            severity: severity_filter,
            contains: contains_filter,
            start_at_unix_ms: query.start_at_unix_ms,
            end_at_unix_ms: query.end_at_unix_ms,
        },
        records,
        page: build_page_info(limit, returned, next_cursor),
        newest_cursor,
        available_sources,
    }))
}

pub(crate) async fn console_logs_export_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<ConsoleLogsQuery>,
) -> Result<impl IntoResponse, Response> {
    let _session = authorize_console_session(&state, &headers, false)?;
    let limit = query.limit.unwrap_or(DEFAULT_LOG_LIMIT).clamp(1, MAX_LOG_LIMIT);
    let format =
        normalize_log_export_format(query.format.as_deref()).map_err(runtime_status_response)?;
    let mut records =
        collect_log_records(&state, limit.saturating_mul(LOG_SCAN_MULTIPLIER)).await?;
    let source_filter = normalize_optional_query_text(query.source.as_deref());
    let severity_filter = normalize_optional_query_text(query.severity.as_deref());
    let contains_filter = normalize_optional_query_text(query.contains.as_deref());
    records.retain(|record| {
        record.timestamp_unix_ms >= query.start_at_unix_ms.unwrap_or(i64::MIN)
            && record.timestamp_unix_ms <= query.end_at_unix_ms.unwrap_or(i64::MAX)
    });
    if let Some(filter) = source_filter.as_deref() {
        records.retain(|record| log_source_matches(record.source.as_str(), filter));
    }
    if let Some(filter) = severity_filter.as_deref() {
        records.retain(|record| record.severity.eq_ignore_ascii_case(filter));
    }
    if let Some(filter) = contains_filter.as_deref() {
        records.retain(|record| log_contains(record, filter));
    }
    records.sort_by_key(|record| Reverse(log_record_sort_key(record)));
    let available_sources = records
        .iter()
        .map(|record| record.source.clone())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    let has_more = records.len() > limit;
    records.truncate(limit);
    let returned = records.len();
    let newest_cursor = records.first().map(|record| record.cursor.clone());
    let next_cursor =
        has_more.then(|| records.last().map(|record| record.cursor.clone())).flatten();

    let envelope = control_plane::LogListEnvelope {
        contract: contract_descriptor(),
        query: control_plane::LogQueryEcho {
            limit,
            direction: "before".to_owned(),
            cursor: None,
            source: source_filter,
            severity: severity_filter,
            contains: contains_filter,
            start_at_unix_ms: query.start_at_unix_ms,
            end_at_unix_ms: query.end_at_unix_ms,
        },
        records,
        page: build_page_info(limit, returned, next_cursor),
        newest_cursor,
        available_sources,
    };

    match format {
        "json" => {
            let body = serde_json::to_vec_pretty(&envelope).map_err(|error| {
                runtime_status_response(tonic::Status::internal(format!(
                    "failed to encode logs export as JSON: {error}"
                )))
            })?;
            let mut response = axum::response::Response::new(axum::body::Body::from(body));
            response.headers_mut().insert(
                axum::http::header::CONTENT_TYPE,
                HeaderValue::from_static("application/json; charset=utf-8"),
            );
            Ok(response)
        }
        _ => {
            let mut csv = String::from(
                "timestamp_unix_ms,source,source_kind,severity,message,session_id,run_id,device_id,connector_id,event_name,structured_payload_json\n",
            );
            for record in envelope.records {
                let structured_payload_json = record
                    .structured_payload
                    .as_ref()
                    .and_then(|payload| serde_json::to_string(payload).ok())
                    .unwrap_or_default();
                csv.push_str(
                    format!(
                        "{},{},{},{},{},{},{},{},{},{},{}\n",
                        record.timestamp_unix_ms,
                        csv_escape(record.source.as_str()),
                        csv_escape(record.source_kind.as_str()),
                        csv_escape(record.severity.as_str()),
                        csv_escape(record.message.as_str()),
                        csv_escape(record.session_id.as_deref().unwrap_or_default()),
                        csv_escape(record.run_id.as_deref().unwrap_or_default()),
                        csv_escape(record.device_id.as_deref().unwrap_or_default()),
                        csv_escape(record.connector_id.as_deref().unwrap_or_default()),
                        csv_escape(record.event_name.as_deref().unwrap_or_default()),
                        csv_escape(structured_payload_json.as_str()),
                    )
                    .as_str(),
                );
            }

            let mut response = axum::response::Response::new(axum::body::Body::from(csv));
            response.headers_mut().insert(
                axum::http::header::CONTENT_TYPE,
                HeaderValue::from_static("text/csv; charset=utf-8"),
            );
            Ok(response)
        }
    }
}

async fn collect_log_records(
    state: &AppState,
    limit_per_source: usize,
) -> Result<Vec<control_plane::LogRecord>, Response> {
    let mut records = Vec::new();
    let journal_snapshot = state
        .runtime
        .recent_journal_snapshot(limit_per_source)
        .await
        .map_err(runtime_status_response)?;
    for event in journal_snapshot.events {
        records.push(map_journal_log_record(event));
    }

    let connectors = state.channels.list().map_err(channel_platform_error_response)?;
    for connector in connectors {
        let connector_id = connector.connector_id;
        for event in state
            .channels
            .logs(connector_id.as_str(), Some(limit_per_source))
            .map_err(channel_platform_error_response)?
        {
            records.push(map_connector_event_log_record(connector_id.as_str(), event));
        }
        for dead_letter in state
            .channels
            .dead_letters(connector_id.as_str(), Some(limit_per_source))
            .map_err(channel_platform_error_response)?
        {
            records.push(map_dead_letter_log_record(connector_id.as_str(), dead_letter));
        }
    }

    Ok(records)
}

fn normalize_log_direction(raw: Option<&str>) -> Result<&'static str, tonic::Status> {
    match normalize_optional_query_text(raw).as_deref() {
        None | Some("before") => Ok("before"),
        Some("after") => Ok("after"),
        Some(_) => {
            Err(tonic::Status::invalid_argument("log direction must be one of before|after"))
        }
    }
}

fn normalize_log_export_format(raw: Option<&str>) -> Result<&'static str, tonic::Status> {
    match normalize_optional_query_text(raw).as_deref() {
        None | Some("csv") => Ok("csv"),
        Some("json") => Ok("json"),
        Some(_) => {
            Err(tonic::Status::invalid_argument("log export format must be one of csv|json"))
        }
    }
}

fn normalize_optional_query_text(raw: Option<&str>) -> Option<String> {
    raw.map(str::trim).filter(|value| !value.is_empty()).map(|value| value.to_ascii_lowercase())
}

fn parse_log_cursor(raw: &str) -> Option<LogSortKey> {
    let (timestamp, tie_breaker) = raw.split_once(':')?;
    Some(LogSortKey {
        timestamp_unix_ms: timestamp.parse().ok()?,
        tie_breaker: tie_breaker.to_owned(),
    })
}

fn log_record_sort_key(record: &control_plane::LogRecord) -> LogSortKey {
    parse_log_cursor(record.cursor.as_str()).unwrap_or(LogSortKey {
        timestamp_unix_ms: record.timestamp_unix_ms,
        tie_breaker: record.cursor.clone(),
    })
}

fn log_source_matches(source: &str, filter: &str) -> bool {
    if filter == "channels" {
        source.starts_with("channel:")
    } else {
        source.eq_ignore_ascii_case(filter)
    }
}

fn log_contains(record: &control_plane::LogRecord, filter: &str) -> bool {
    if record.message.to_ascii_lowercase().contains(filter) {
        return true;
    }
    record
        .structured_payload
        .as_ref()
        .and_then(|payload| serde_json::to_string(payload).ok())
        .is_some_and(|payload| payload.to_ascii_lowercase().contains(filter))
}

fn map_journal_log_record(event: journal::JournalEventRecord) -> control_plane::LogRecord {
    let payload = decode_redacted_log_payload(event.payload_json.as_str());
    let event_name = payload
        .as_ref()
        .and_then(|value| value.get("event").and_then(Value::as_str))
        .map(str::to_owned);
    let (source, source_kind) = classify_journal_log_source(event_name.as_deref());
    let tie_breaker = format!("{source_kind}:{}", event.event_id);
    control_plane::LogRecord {
        cursor: format!("{}:{tie_breaker}", event.timestamp_unix_ms),
        source: source.to_owned(),
        source_kind: source_kind.to_owned(),
        severity: classify_journal_log_severity(
            event.kind,
            event_name.as_deref(),
            payload.as_ref(),
        ),
        message: summarize_journal_log_message(event.kind, event_name.as_deref(), payload.as_ref()),
        timestamp_unix_ms: event.timestamp_unix_ms,
        session_id: non_empty_string_option(event.session_id),
        run_id: non_empty_string_option(event.run_id),
        device_id: non_empty_string_option(event.device_id),
        connector_id: None,
        event_name,
        structured_payload: payload,
    }
}

fn map_connector_event_log_record(
    connector_id: &str,
    event: palyra_connectors::ConnectorEventRecord,
) -> control_plane::LogRecord {
    let payload = redact_optional_value(event.details);
    control_plane::LogRecord {
        cursor: format!(
            "{}:channel-event:{connector_id}:{}",
            event.created_at_unix_ms, event.event_id
        ),
        source: format!("channel:{connector_id}"),
        source_kind: "channel".to_owned(),
        severity: normalize_connector_severity(event.level.as_str()),
        message: redact_log_text(event.message.as_str()),
        timestamp_unix_ms: event.created_at_unix_ms,
        session_id: None,
        run_id: None,
        device_id: None,
        connector_id: Some(connector_id.to_owned()),
        event_name: Some(event.event_type),
        structured_payload: payload,
    }
}

fn map_dead_letter_log_record(
    connector_id: &str,
    record: palyra_connectors::DeadLetterRecord,
) -> control_plane::LogRecord {
    let payload = redact_optional_value(Some(record.payload));
    control_plane::LogRecord {
        cursor: format!(
            "{}:dead-letter:{connector_id}:{}",
            record.created_at_unix_ms, record.dead_letter_id
        ),
        source: format!("channel:{connector_id}"),
        source_kind: "channel".to_owned(),
        severity: "error".to_owned(),
        message: redact_log_text(record.reason.as_str()),
        timestamp_unix_ms: record.created_at_unix_ms,
        session_id: None,
        run_id: None,
        device_id: None,
        connector_id: Some(connector_id.to_owned()),
        event_name: Some("dead_letter".to_owned()),
        structured_payload: payload,
    }
}

fn decode_redacted_log_payload(raw: &str) -> Option<Value> {
    let mut payload = serde_json::from_str::<Value>(raw).ok()?;
    redact_console_diagnostics_value(&mut payload, None);
    Some(payload)
}

fn redact_optional_value(value: Option<Value>) -> Option<Value> {
    let mut value = value?;
    redact_console_diagnostics_value(&mut value, None);
    Some(value)
}

fn classify_journal_log_source(event_name: Option<&str>) -> (&'static str, &'static str) {
    if event_name.is_some_and(|value| value.starts_with("browser.")) {
        ("browserd", "browserd")
    } else {
        ("palyrad", "palyrad")
    }
}

fn classify_journal_log_severity(
    kind: i32,
    event_name: Option<&str>,
    payload: Option<&Value>,
) -> String {
    let event_name = event_name.unwrap_or_default().to_ascii_lowercase();
    if payload
        .and_then(|value| value.get("success").and_then(Value::as_bool))
        .is_some_and(|success| !success)
        || payload
            .and_then(|value| value.get("error").and_then(Value::as_str))
            .is_some_and(|error| !error.trim().is_empty())
        || event_name.contains("failed")
        || event_name.contains("error")
        || event_name.contains("rejected")
        || kind == common_v1::journal_event::EventKind::RunFailed as i32
    {
        "error".to_owned()
    } else if event_name.contains("warning")
        || event_name.contains("stale")
        || event_name.contains("expired")
        || event_name.contains("degraded")
    {
        "warning".to_owned()
    } else {
        "info".to_owned()
    }
}

fn summarize_journal_log_message(
    kind: i32,
    event_name: Option<&str>,
    payload: Option<&Value>,
) -> String {
    for key in ["message", "summary", "reason", "error"] {
        if let Some(value) = payload.and_then(|payload| payload.get(key).and_then(Value::as_str)) {
            let redacted = redact_log_text(value);
            if !redacted.trim().is_empty() {
                return redacted;
            }
        }
    }
    if let Some(event_name) = event_name {
        let candidate = event_name.trim();
        if !candidate.is_empty() {
            return candidate.to_owned();
        }
    }
    match common_v1::journal_event::EventKind::try_from(kind)
        .unwrap_or(common_v1::journal_event::EventKind::Unspecified)
    {
        common_v1::journal_event::EventKind::MessageReceived => "message received".to_owned(),
        common_v1::journal_event::EventKind::ModelToken => "model token".to_owned(),
        common_v1::journal_event::EventKind::ToolProposed => "tool proposed".to_owned(),
        common_v1::journal_event::EventKind::ToolExecuted => "tool executed".to_owned(),
        common_v1::journal_event::EventKind::A2uiUpdated => "a2ui updated".to_owned(),
        common_v1::journal_event::EventKind::RunCompleted => "run completed".to_owned(),
        common_v1::journal_event::EventKind::RunFailed => "run failed".to_owned(),
        common_v1::journal_event::EventKind::Unspecified => "journal event".to_owned(),
    }
}

fn normalize_connector_severity(raw: &str) -> String {
    match raw.trim().to_ascii_lowercase().as_str() {
        "error" | "warn" | "warning" | "info" | "debug" => raw.trim().to_ascii_lowercase(),
        _ => "info".to_owned(),
    }
}

fn redact_log_text(raw: &str) -> String {
    let sanitized = sanitize_http_error_message(raw);
    palyra_common::redaction::redact_url_segments_in_text(&sanitized)
}

fn non_empty_string_option(value: String) -> Option<String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_owned())
    }
}

fn csv_escape(raw: &str) -> String {
    let escaped = raw.replace('"', "\"\"");
    format!("\"{escaped}\"")
}

#[cfg(test)]
mod tests {
    use super::{
        classify_journal_log_source, decode_redacted_log_payload, log_record_sort_key,
        map_connector_event_log_record, map_journal_log_record, normalize_log_export_format,
        parse_log_cursor, LogSortKey,
    };
    use crate::common_v1::journal_event::{EventActor, EventKind};
    use crate::journal::JournalEventRecord;
    use serde_json::json;

    #[test]
    fn decode_redacted_log_payload_masks_secret_values() {
        let payload = decode_redacted_log_payload(
            r#"{"authorization":"Bearer secret-token","details":{"refresh_token":"secret-refresh","state":"ok"}}"#,
        )
        .expect("payload should decode");

        assert_eq!(
            payload.get("authorization").and_then(serde_json::Value::as_str),
            Some("<redacted>"),
            "top-level sensitive fields should be redacted"
        );
        assert_eq!(
            payload
                .get("details")
                .and_then(|value| value.get("refresh_token"))
                .and_then(serde_json::Value::as_str),
            Some("<redacted>"),
            "nested sensitive fields should stay redacted"
        );
    }

    #[test]
    fn browser_journal_events_are_classified_as_browserd() {
        let record = map_journal_log_record(JournalEventRecord {
            seq: 11,
            event_id: "01ARZ3NDEKTSV4RRFFQ69G5FAZ".to_owned(),
            session_id: String::new(),
            run_id: String::new(),
            kind: EventKind::ToolExecuted as i32,
            actor: EventActor::System as i32,
            timestamp_unix_ms: 1_700_000_000_000,
            payload_json: json!({
                "event": "browser.relay.action",
                "success": false,
                "error": "token=secret"
            })
            .to_string(),
            redacted: true,
            hash: None,
            prev_hash: None,
            principal: "admin:test".to_owned(),
            device_id: String::new(),
            channel: None,
            created_at_unix_ms: 1_700_000_000_000,
        });

        assert_eq!(record.source, "browserd");
        assert_eq!(record.source_kind, "browserd");
        assert_eq!(record.severity, "error");
        assert!(
            record.message.contains("<redacted>"),
            "browser error summary should keep redaction marker"
        );
    }

    #[test]
    fn connector_logs_build_stable_cursor_sort_keys() {
        let record = map_connector_event_log_record(
            "discord:default",
            palyra_connectors::ConnectorEventRecord {
                event_id: 42,
                connector_id: "discord:default".to_owned(),
                event_type: "delivery.failed".to_owned(),
                level: "warning".to_owned(),
                message: "refresh_token=secret".to_owned(),
                details: Some(json!({ "session": "sensitive" })),
                created_at_unix_ms: 1_700_000_000_100,
            },
        );

        let cursor = parse_log_cursor(record.cursor.as_str()).expect("cursor should parse");
        assert_eq!(
            cursor,
            LogSortKey {
                timestamp_unix_ms: 1_700_000_000_100,
                tie_breaker: "channel-event:discord:default:42".to_owned(),
            }
        );
        assert_eq!(log_record_sort_key(&record), cursor);
        assert_eq!(
            classify_journal_log_source(Some("browser.action.click")),
            ("browserd", "browserd")
        );
    }

    #[test]
    fn log_export_defaults_to_csv_and_accepts_json() {
        assert_eq!(normalize_log_export_format(None).expect("default format"), "csv");
        assert_eq!(normalize_log_export_format(Some("json")).expect("json format"), "json");
        assert!(
            normalize_log_export_format(Some("xml")).is_err(),
            "unsupported formats must fail closed"
        );
    }
}
