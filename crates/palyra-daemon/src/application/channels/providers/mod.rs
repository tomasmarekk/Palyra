//! Central provider dispatch for daemon channel application flows.
//!
//! Provider-specific behavior is delegated into submodules so generic handlers
//! do not accumulate scattered Discord branching over time.

use serde_json::{json, Value};

use crate::{app::state::AppState, journal::ApprovalRiskLevel, *};

pub(crate) mod discord;

/// Builds the provider-specific block of the channel operations snapshot.
///
/// Returns [`Value::Null`] for connector kinds without provider-owned
/// operations data so the generic payload shape stays stable.
pub(crate) fn build_channel_provider_operations_payload(
    connector_id: &str,
    connector: &palyra_connectors::ConnectorStatusSnapshot,
    runtime: Option<&Value>,
    recent_dead_letters: &[palyra_connectors::DeadLetterRecord],
) -> Value {
    match connector.kind {
        palyra_connectors::ConnectorKind::Discord => {
            discord::build_discord_channel_operations_payload(
                connector_id,
                connector,
                runtime,
                recent_dead_letters,
            )
        }
        _ => Value::Null,
    }
}

/// Runs the provider's static (no-network) credential check and returns the
/// failure message when credentials cannot be resolved.
///
/// `None` means either the credentials resolved or the provider has no static
/// check; callers use the message to overlay a fail-closed auth surface on
/// otherwise healthy-looking status payloads.
pub(crate) fn channel_provider_static_auth_failure(
    state: &AppState,
    connector_id: &str,
    connector: &palyra_connectors::ConnectorStatusSnapshot,
) -> Option<String> {
    match connector.kind {
        palyra_connectors::ConnectorKind::Discord => {
            discord::resolve_discord_connector_token(state, connector_id).err()
        }
        _ => None,
    }
}

/// Runs the provider's live health-refresh probe for the connector.
///
/// Unsupported connector kinds return a stable `supported: false` payload
/// instead of an error so the console can render the gap explicitly.
///
/// # Errors
/// Returns a platform error response when the connector is unknown or the
/// provider probe rejects its inputs.
#[allow(clippy::result_large_err)]
pub(crate) async fn build_channel_provider_health_refresh_payload(
    state: &AppState,
    connector_id: &str,
    verify_channel_id: Option<String>,
) -> Result<Value, Response> {
    let connector = state.channels.status(connector_id).map_err(channel_platform_error_response)?;
    match connector.kind {
        palyra_connectors::ConnectorKind::Discord => {
            discord::build_discord_channel_health_refresh_payload(
                state,
                connector_id,
                verify_channel_id,
            )
            .await
        }
        _ => Ok(json!({
            "supported": false,
            "message": "health refresh is currently implemented for Discord connectors only",
        })),
    }
}

/// Classifies the approval/risk governance for a channel message mutation
/// (edit, delete, reaction changes) against the previewed message.
///
/// Non-Discord connectors fail closed: every mutation is classified high risk
/// with approval required until a provider implements its own governance.
///
/// # Errors
/// Returns a platform error response when the connector or its instance
/// cannot be loaded, or when the wall clock is unavailable.
#[allow(clippy::result_large_err)]
pub(crate) fn classify_channel_message_mutation_governance(
    state: &AppState,
    connector_id: &str,
    preview: &palyra_connectors::ConnectorMessageRecord,
    operation: channels::DiscordMessageMutationKind,
) -> Result<channels::DiscordMessageMutationGovernance, Response> {
    let connector = state.channels.status(connector_id).map_err(channel_platform_error_response)?;
    match connector.kind {
        palyra_connectors::ConnectorKind::Discord => {
            let instance = state
                .channels
                .connector_instance(connector_id)
                .map_err(channel_platform_error_response)?;
            Ok(channels::classify_discord_message_mutation_governance(
                &instance,
                preview,
                operation,
                unix_ms_now().map_err(|error| {
                    runtime_status_response(tonic::Status::internal(sanitize_http_error_message(
                        error.to_string().as_str(),
                    )))
                })?,
            ))
        }
        _ => Ok(channels::DiscordMessageMutationGovernance {
            risk_level: ApprovalRiskLevel::High,
            approval_required: true,
            reason: "non-Discord connector mutation defaults to explicit approval".to_owned(),
        }),
    }
}

/// Maps a message mutation to its policy action name.
///
/// Currently delegates to Discord because the mutation kinds themselves are
/// Discord-shaped; a second provider will need a provider-neutral kind first.
pub(crate) fn channel_message_policy_action(
    operation: channels::DiscordMessageMutationKind,
) -> &'static str {
    discord::channel_message_policy_action(operation)
}

/// Maps a message mutation to the provider permission labels it requires.
pub(crate) fn channel_message_required_permissions(
    operation: channels::DiscordMessageMutationKind,
) -> Vec<String> {
    discord::channel_message_required_permissions(operation)
}

/// Returns the first non-empty message containing any needle
/// (case-insensitive), sanitized for safe surfacing to operators.
pub(crate) fn find_matching_message<'a, I>(messages: I, needles: &[&str]) -> Option<String>
where
    I: IntoIterator<Item = Option<&'a str>>,
{
    messages.into_iter().flatten().find_map(|message| {
        let normalized = message.trim().to_ascii_lowercase();
        if normalized.is_empty() {
            return None;
        }
        if needles.iter().any(|needle| normalized.contains(needle)) {
            Some(sanitize_http_error_message(message.trim()))
        } else {
            None
        }
    })
}
