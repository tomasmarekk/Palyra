//! Discord connector console handlers.
//!
//! These console routes wrap Discord onboarding and account lifecycle
//! operations behind authenticated console mutations.

use std::path::{Path as FsPath, PathBuf};

use crate::application::channels::providers::discord::{
    apply_discord_onboarding, build_discord_onboarding_preflight, perform_discord_account_logout,
    perform_discord_account_remove,
};
use crate::*;

/// Runs a Discord onboarding preflight without persisting connector changes.
///
/// # Errors
/// Returns an error response when console authorization or onboarding
/// preflight validation fails.
pub(crate) async fn console_discord_onboarding_probe_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(payload): Json<DiscordOnboardingRequest>,
) -> Result<Json<DiscordOnboardingPreflightResponse>, Response> {
    let _session = authorize_console_session(&state, &headers, true)?;
    let response = build_discord_onboarding_preflight(&state, payload).await?;
    Ok(Json(response))
}

/// Applies Discord onboarding changes for a connector.
///
/// # Errors
/// Returns an error response when console authorization or onboarding
/// application fails.
pub(crate) async fn console_discord_onboarding_apply_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(payload): Json<DiscordOnboardingRequest>,
) -> Result<Json<Value>, Response> {
    let _session = authorize_console_session(&state, &headers, true)?;
    let response = apply_discord_onboarding(&state, payload).await?;
    Ok(Json(response))
}

/// Logs out a Discord account selected by path parameter.
///
/// # Errors
/// Returns an error response when console authorization or account logout
/// fails.
pub(crate) async fn console_discord_account_logout_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(account_id): Path<String>,
    Json(payload): Json<DiscordAccountLifecycleRequest>,
) -> Result<Json<Value>, Response> {
    let _session = authorize_console_session(&state, &headers, true)?;
    let response = perform_discord_account_logout(&state, account_id, &payload)?;
    Ok(Json(response))
}

/// Removes a Discord account selected by path parameter.
///
/// # Errors
/// Returns an error response when console authorization or account removal
/// fails.
pub(crate) async fn console_discord_account_remove_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(account_id): Path<String>,
    Json(payload): Json<DiscordAccountLifecycleRequest>,
) -> Result<Json<Value>, Response> {
    let _session = authorize_console_session(&state, &headers, true)?;
    let response = perform_discord_account_remove(&state, account_id, &payload)?;
    Ok(Json(response))
}

/// Parses comma-separated console query values into trimmed entries.
pub(crate) fn parse_csv_values(raw: Option<&str>) -> Vec<String> {
    raw.map(|value| {
        value
            .split(',')
            .map(str::trim)
            .filter(|entry| !entry.is_empty())
            .map(ToOwned::to_owned)
            .collect::<Vec<_>>()
    })
    .unwrap_or_default()
}

/// Derives the connector database path colocated with a journal database.
pub(crate) fn connector_db_path_from_journal_path(journal_db_path: &FsPath) -> PathBuf {
    let Some(parent) = journal_db_path.parent().filter(|path| !path.as_os_str().is_empty()) else {
        return PathBuf::from("data").join("connectors.sqlite3");
    };
    let Some(stem) = journal_db_path
        .file_stem()
        .and_then(|value| value.to_str())
        .map(str::trim)
        .filter(|value| !value.is_empty())
    else {
        return parent.join("connectors.sqlite3");
    };
    parent.join(format!("{stem}.connectors.sqlite3"))
}

/// Parses memory-source filters from comma-separated console query text.
///
/// # Errors
/// Returns an error response when any memory source value is unsupported.
#[expect(
    clippy::result_large_err,
    reason = "console parsing helpers return Response directly to preserve HTTP contracts"
)]
pub(crate) fn parse_memory_sources_csv(
    raw: Option<&str>,
) -> Result<Vec<journal::MemorySource>, Response> {
    let mut parsed = Vec::new();
    for value in parse_csv_values(raw) {
        let source = journal::MemorySource::from_str(value.as_str()).ok_or_else(|| {
            runtime_status_response(tonic::Status::invalid_argument(format!(
                "unsupported memory source value: {value}"
            )))
        })?;
        parsed.push(source);
    }
    Ok(parsed)
}
