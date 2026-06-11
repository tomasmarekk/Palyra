//! Console audit event listing handler.
//!
//! Audit reads use the journal snapshot and preserve existing event payloads;
//! filtering is applied only at the transport boundary.

use crate::*;

/// Lists recent audit events from the daemon journal.
///
/// # Errors
/// Returns an error response when console authorization or journal lookup
/// fails.
pub(crate) async fn console_audit_events_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<ConsoleAuditEventsQuery>,
) -> Result<Json<Value>, Response> {
    let _session = authorize_console_session(&state, &headers, false)?;
    let limit = query.limit.unwrap_or(200).clamp(1, 2_000);
    let snapshot =
        state.runtime.recent_journal_snapshot(limit).await.map_err(runtime_status_response)?;
    let contains = query
        .contains
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(|value| value.to_ascii_lowercase());
    let events = snapshot
        .events
        .into_iter()
        .filter(|event| query.kind.is_none_or(|kind| event.kind == kind))
        .filter(|event| {
            query
                .principal
                .as_deref()
                .is_none_or(|principal| event.principal.eq_ignore_ascii_case(principal.trim()))
        })
        .filter(|event| {
            query.channel.as_deref().is_none_or(|channel| {
                event
                    .channel
                    .as_deref()
                    .is_some_and(|value| value.eq_ignore_ascii_case(channel.trim()))
            })
        })
        .filter(|event| {
            contains.as_ref().is_none_or(|needle| {
                event.payload_json.to_ascii_lowercase().contains(needle.as_str())
            })
        })
        .collect::<Vec<_>>();
    Ok(Json(json!({
        "hash_chain_enabled": snapshot.hash_chain_enabled,
        "total_events": snapshot.total_events,
        "returned_events": events.len(),
        "events": events,
        "page": build_page_info(limit, events.len(), None),
    })))
}
