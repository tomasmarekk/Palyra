//! Discord onboarding: preflight evaluation and the apply flow.
//!
//! One evaluation path backs both surfaces: preflight validates the token
//! against the Discord API and previews routing, policy, and security
//! defaults without persisting anything, while apply re-runs the same
//! evaluation (with the open-guild-scope confirmation enforced) and then
//! persists the token to the vault, merges routing rules into the config
//! file, and enables the connector. Permission and intent checks are
//! best-effort warnings, never hard failures, so onboarding can complete
//! under degraded Discord API connectivity.

use std::{
    fs,
    path::PathBuf,
    time::{Duration, Instant},
};

use reqwest::{Client as ReqwestClient, Url};
use serde_json::{json, Value};

use crate::{
    app::state::AppState,
    transport::http::contracts::channels::discord::{
        DiscordApplicationSummary, DiscordBotIdentitySummary, DiscordChannelPermissionCheck,
        DiscordChannelPermissionCheckStatus, DiscordInboundMonitorSummary, DiscordOnboardingMode,
        DiscordOnboardingPreflightResponse, DiscordOnboardingRequest, DiscordOnboardingScope,
        DiscordRoutingPreview,
    },
    *,
};

/// Normalized, validated onboarding decisions derived from one request.
///
/// Built once by [`build_discord_onboarding_plan`] and shared by preflight
/// preview, config persistence, and policy-warning evaluation so all three
/// surfaces describe the same effective configuration.
#[derive(Debug, Clone)]
pub(crate) struct DiscordOnboardingPlan {
    pub(crate) connector_id: String,
    pub(crate) account_id: String,
    pub(crate) mode: DiscordOnboardingMode,
    pub(crate) inbound_scope: DiscordOnboardingScope,
    pub(crate) require_mention: bool,
    pub(crate) mention_patterns: Vec<String>,
    pub(crate) allow_from: Vec<String>,
    pub(crate) deny_from: Vec<String>,
    pub(crate) allow_direct_messages: bool,
    pub(crate) direct_message_policy: channel_router::DirectMessagePolicy,
    pub(crate) broadcast_strategy: channel_router::BroadcastStrategy,
    pub(crate) concurrency_limit: u64,
    pub(crate) confirm_open_guild_channels: bool,
}

#[derive(Debug, Clone)]
struct DiscordOnboardingEvaluation {
    token: String,
    plan: DiscordOnboardingPlan,
    preflight: DiscordOnboardingPreflightResponse,
}

#[derive(Debug, Clone)]
struct DiscordOnboardingConfigWrite {
    path: PathBuf,
    created: bool,
    rollback: DiscordOnboardingConfigRollback,
}

#[derive(Debug, Clone)]
struct DiscordOnboardingConfigRollback {
    path: PathBuf,
    previous_content: Option<String>,
}

impl DiscordOnboardingConfigRollback {
    fn restore(&self) -> Result<(), String> {
        match &self.previous_content {
            Some(content) => palyra_common::config_system::write_content_with_backups(
                self.path.as_path(),
                content,
                DISCORD_ONBOARDING_CONFIG_BACKUPS,
            )
            .map_err(|error| {
                format!(
                    "failed to restore discord onboarding config '{}': {error}",
                    self.path.display()
                )
            }),
            None => {
                if self.path.exists() {
                    fs::remove_file(self.path.as_path()).map_err(|error| {
                        format!(
                            "failed to remove created discord onboarding config '{}': {error}",
                            self.path.display()
                        )
                    })?;
                }
                Ok(())
            }
        }
    }
}

/// Evaluates an onboarding request without persisting anything: validates
/// the token live against Discord and returns the full preflight preview.
///
/// # Errors
/// Returns an error response when the token is empty, request fields fail
/// validation, or the Discord identity probe fails.
pub(crate) async fn build_discord_onboarding_preflight(
    state: &AppState,
    payload: DiscordOnboardingRequest,
) -> Result<DiscordOnboardingPreflightResponse, Response> {
    let evaluation = evaluate_discord_onboarding_request(state, &payload, false).await?;
    Ok(evaluation.preflight)
}

/// Applies an onboarding request end to end and returns the combined
/// preflight, applied-state, status, and inbound-monitor payload.
///
/// Step ordering is deliberate: the request is fully re-evaluated (including
/// the live token probe and the open-guild-scope confirmation gate) before
/// any persistence; the token reaches the vault before the connector is
/// enabled so the runtime can authenticate on first start; and the routing
/// rules land in the config file before enablement, even though they only
/// take effect after a daemon restart (`restart_required_for_routing_rules`).
///
/// If a later persistence step fails after the token is stored, apply rolls
/// back the token and restores the config snapshot so a failed enablement
/// cannot leave an unused bot token or stale routing rule behind.
///
/// # Errors
/// Returns an error response when evaluation fails, the open-guild scope is
/// not confirmed, or any vault, config, or connector persistence step fails.
pub(crate) async fn apply_discord_onboarding(
    state: &AppState,
    payload: DiscordOnboardingRequest,
) -> Result<Value, Response> {
    let evaluation = evaluate_discord_onboarding_request(state, &payload, true).await?;
    let connector_existed = state.channels.status(evaluation.plan.connector_id.as_str()).is_ok();
    state
        .channels
        .ensure_discord_connector(evaluation.plan.account_id.as_str())
        .map_err(channel_platform_error_response)?;

    let token_vault_ref = channels::discord_token_vault_ref(evaluation.plan.account_id.as_str());
    let parsed_ref = VaultRef::parse(token_vault_ref.as_str()).map_err(|error| {
        runtime_status_response(tonic::Status::internal(format!(
            "failed to parse discord token vault ref: {error}"
        )))
    })?;
    state
        .vault
        .put_secret(&parsed_ref.scope, parsed_ref.key.as_str(), evaluation.token.as_bytes())
        .map_err(|error| {
            runtime_status_response(tonic::Status::internal(format!(
                "failed to store discord token in vault: {error}"
            )))
        })?;

    let config_write = match persist_discord_onboarding_config(&evaluation.plan) {
        Ok(write) => write,
        Err(error) => {
            rollback_discord_onboarding_after_failure(
                state,
                &parsed_ref,
                None,
                evaluation.plan.connector_id.as_str(),
                !connector_existed,
                "config persistence",
            )?;
            return Err(error);
        }
    };
    let status = match state.channels.set_enabled(evaluation.plan.connector_id.as_str(), true) {
        Ok(status) => status,
        Err(error) => {
            let response = channel_platform_error_response(error);
            rollback_discord_onboarding_after_failure(
                state,
                &parsed_ref,
                Some(&config_write.rollback),
                evaluation.plan.connector_id.as_str(),
                !connector_existed,
                "connector enablement",
            )?;
            return Err(response);
        }
    };
    let runtime = state
        .channels
        .runtime_snapshot(evaluation.plan.connector_id.as_str())
        .map_err(channel_platform_error_response)?;
    let inbound_monitor =
        wait_for_discord_inbound_monitor_summary(state, evaluation.plan.connector_id.as_str())
            .await;
    let inbound_alive = discord_inbound_monitor_is_alive(&inbound_monitor);
    let inbound_monitor_warnings = build_discord_inbound_monitor_warnings(&inbound_monitor);

    Ok(json!({
        "preflight": evaluation.preflight,
        "applied": {
            "token_vault_ref": token_vault_ref,
            "connector_id": evaluation.plan.connector_id,
            "config_path": config_write.path.display().to_string(),
            "config_created": config_write.created,
            "config_backups": DISCORD_ONBOARDING_CONFIG_BACKUPS,
            "connector_enabled": true,
            "restart_required_for_routing_rules": true,
        },
        "status": status,
        "runtime": runtime,
        "inbound_monitor": inbound_monitor,
        "inbound_alive": inbound_alive,
        "inbound_monitor_warnings": inbound_monitor_warnings,
    }))
}

#[allow(clippy::result_large_err)]
fn rollback_discord_onboarding_after_failure(
    state: &AppState,
    parsed_ref: &VaultRef,
    config_rollback: Option<&DiscordOnboardingConfigRollback>,
    connector_id: &str,
    remove_created_connector: bool,
    failed_stage: &str,
) -> Result<(), Response> {
    if let Some(rollback) = config_rollback {
        rollback.restore().map_err(|error| {
            discord_onboarding_rollback_error_response(failed_stage, error.as_str())
        })?;
    }
    rollback_discord_onboarding_vault_secret(state.vault.as_ref(), parsed_ref).map_err(
        |error| discord_onboarding_rollback_error_response(failed_stage, error.as_str()),
    )?;
    if remove_created_connector {
        state.channels.remove_connector(connector_id).map_err(|error| {
            let message = error.to_string();
            discord_onboarding_rollback_error_response(failed_stage, message.as_str())
        })?;
    }
    Ok(())
}

fn rollback_discord_onboarding_vault_secret(
    vault: &palyra_vault::Vault,
    parsed_ref: &VaultRef,
) -> Result<bool, String> {
    vault
        .delete_secret(&parsed_ref.scope, parsed_ref.key.as_str())
        .map_err(|error| format!("failed to delete discord onboarding token from vault: {error}"))
}

fn discord_onboarding_rollback_error_response(
    failed_stage: &str,
    rollback_error: &str,
) -> Response {
    runtime_status_response(tonic::Status::internal(format!(
        "discord onboarding failed during {failed_stage} and rollback failed: {rollback_error}"
    )))
}

/// Shared evaluation behind preflight and apply: normalize the request into
/// a plan, probe Discord with the token, and assemble the preview payload.
///
/// `require_open_scope_confirmation` is `true` only on apply so preflight can
/// preview an open-guild plan (with a warning) without the confirmation flag.
async fn evaluate_discord_onboarding_request(
    state: &AppState,
    payload: &DiscordOnboardingRequest,
    require_open_scope_confirmation: bool,
) -> Result<DiscordOnboardingEvaluation, Response> {
    let token = normalize_discord_token(payload.token.as_str()).ok_or_else(|| {
        runtime_status_response(tonic::Status::invalid_argument("discord token cannot be empty"))
    })?;
    let mut plan = build_discord_onboarding_plan(payload)?;
    if require_open_scope_confirmation
        && matches!(plan.inbound_scope, DiscordOnboardingScope::OpenGuildChannels)
        && !plan.confirm_open_guild_channels
    {
        return Err(runtime_status_response(tonic::Status::invalid_argument(
            "open guild channels require explicit confirm_open_guild_channels=true",
        )));
    }

    let verify_channel_id =
        normalize_optional_discord_channel_id(payload.verify_channel_id.as_deref())?;
    let (bot, application, channel_permission_check) =
        probe_discord_bot_identity(token.as_str(), verify_channel_id.as_deref())
            .await
            .map_err(runtime_status_response)?;
    // The identity probe must precede plan finalization: default mention
    // patterns are derived from the bot id/username returned by Discord.
    plan = finalize_discord_onboarding_plan(plan, &bot);
    let inbound_monitor = load_discord_inbound_monitor_summary(state, plan.connector_id.as_str());
    let mut warnings = build_discord_onboarding_warnings(
        &plan,
        application.as_ref(),
        require_open_scope_confirmation,
    );
    warnings.extend(build_discord_channel_permission_warnings(channel_permission_check.as_ref()));
    warnings.extend(build_discord_inbound_monitor_warnings(&inbound_monitor));
    let policy_warnings = evaluate_discord_policy_warnings(state, &plan);
    let invite_client_id = application
        .as_ref()
        .and_then(|summary| summary.id.clone())
        .unwrap_or_else(|| bot.id.clone());
    let invite_url_template = format!(
        "https://discord.com/oauth2/authorize?client_id={invite_client_id}&scope=bot&permissions={}",
        discord_min_invite_permissions()
    );
    let preflight = DiscordOnboardingPreflightResponse {
        connector_id: plan.connector_id.clone(),
        account_id: plan.account_id.clone(),
        mode: plan.mode,
        inbound_scope: plan.inbound_scope,
        bot,
        application,
        invite_url_template,
        required_permissions: discord_required_permission_labels(),
        egress_allowlist: channels::discord_default_egress_allowlist(),
        security_defaults: build_discord_onboarding_security_defaults(&plan),
        routing_preview: build_discord_routing_preview(&plan),
        channel_permission_check,
        inbound_alive: discord_inbound_monitor_is_alive(&inbound_monitor),
        inbound_monitor,
        warnings,
        policy_warnings,
    };
    Ok(DiscordOnboardingEvaluation { token, plan, preflight })
}

/// Normalizes and validates a raw onboarding request into a
/// [`DiscordOnboardingPlan`], applying conservative defaults (DM-only scope,
/// pairing DM policy, deny broadcast, mention gating outside open scope).
///
/// # Errors
/// Returns an invalid-argument response when an explicitly provided mode,
/// scope, policy, or filter value fails validation.
#[allow(clippy::result_large_err)]
pub(crate) fn build_discord_onboarding_plan(
    payload: &DiscordOnboardingRequest,
) -> Result<DiscordOnboardingPlan, Response> {
    let account_id =
        channels::normalize_discord_account_id(payload.account_id.as_deref().unwrap_or("default"))
            .map_err(channel_platform_error_response)?;
    // `parse` returns None for both absent and invalid input, so defaulting
    // and rejecting an explicit-but-invalid value need separate checks.
    let mode = DiscordOnboardingMode::parse(payload.mode.as_deref())
        .unwrap_or(DiscordOnboardingMode::Local);
    if payload.mode.is_some() && DiscordOnboardingMode::parse(payload.mode.as_deref()).is_none() {
        return Err(runtime_status_response(tonic::Status::invalid_argument(
            "mode must be one of: local, remote_vps",
        )));
    }
    let inbound_scope = DiscordOnboardingScope::parse(payload.inbound_scope.as_deref())
        .unwrap_or(DiscordOnboardingScope::DmOnly);
    if payload.inbound_scope.is_some()
        && DiscordOnboardingScope::parse(payload.inbound_scope.as_deref()).is_none()
    {
        return Err(runtime_status_response(tonic::Status::invalid_argument(
            "inbound_scope must be one of: dm_only, allowlisted_guild_channels, open_guild_channels",
        )));
    }
    let require_mention = payload
        .require_mention
        .unwrap_or(!matches!(inbound_scope, DiscordOnboardingScope::OpenGuildChannels));
    let mention_patterns = normalize_discord_mention_patterns(payload.mention_patterns.as_deref())?;
    let allow_from = normalize_discord_sender_filters(payload.allow_from.as_deref(), "allow_from")?;
    let deny_from = normalize_discord_sender_filters(payload.deny_from.as_deref(), "deny_from")?;
    let direct_message_policy = if let Some(value) = payload.direct_message_policy.as_deref() {
        channel_router::DirectMessagePolicy::parse(value).ok_or_else(|| {
            runtime_status_response(tonic::Status::invalid_argument(
                "direct_message_policy must be one of: deny, pairing, allow",
            ))
        })?
    } else {
        channel_router::DirectMessagePolicy::Pairing
    };
    let broadcast_strategy = if let Some(value) = payload.broadcast_strategy.as_deref() {
        channel_router::BroadcastStrategy::parse(value).ok_or_else(|| {
            runtime_status_response(tonic::Status::invalid_argument(
                "broadcast_strategy must be one of: deny, mention_only, allow",
            ))
        })?
    } else {
        channel_router::BroadcastStrategy::Deny
    };
    let concurrency_limit = payload.concurrency_limit.unwrap_or(2).clamp(1, 32);
    Ok(DiscordOnboardingPlan {
        connector_id: channels::discord_connector_id(account_id.as_str()),
        account_id,
        mode,
        inbound_scope,
        require_mention,
        mention_patterns,
        allow_from,
        deny_from,
        allow_direct_messages: true,
        direct_message_policy,
        broadcast_strategy,
        concurrency_limit,
        confirm_open_guild_channels: payload.confirm_open_guild_channels.unwrap_or(false),
    })
}

/// Normalizes an optional channel id to a canonical Discord snowflake,
/// treating absent or blank input as "no channel to verify".
///
/// # Errors
/// Returns an invalid-argument response when a non-blank value contains
/// non-digit characters or has a non-snowflake length.
#[allow(clippy::result_large_err)]
pub(crate) fn normalize_optional_discord_channel_id(
    raw: Option<&str>,
) -> Result<Option<String>, Response> {
    let Some(value) = raw else {
        return Ok(None);
    };
    let normalized = value.trim();
    if normalized.is_empty() {
        return Ok(None);
    }
    if !normalized.chars().all(|character| character.is_ascii_digit()) {
        return Err(runtime_status_response(tonic::Status::invalid_argument(
            "verify_channel_id must contain only decimal digits",
        )));
    }
    if !(16..=24).contains(&normalized.len()) {
        return Err(runtime_status_response(tonic::Status::invalid_argument(
            "verify_channel_id must be a canonical Discord snowflake id",
        )));
    }
    Ok(Some(normalized.to_owned()))
}

/// Completes a plan with identity-dependent defaults: when mention gating is
/// on and the request supplied no patterns, derive them from the probed bot
/// id and username.
pub(crate) fn finalize_discord_onboarding_plan(
    mut plan: DiscordOnboardingPlan,
    bot: &DiscordBotIdentitySummary,
) -> DiscordOnboardingPlan {
    if plan.require_mention && plan.mention_patterns.is_empty() {
        plan.mention_patterns =
            default_discord_mention_patterns(bot.id.as_str(), bot.username.as_str());
    }
    plan
}

fn build_discord_routing_preview(plan: &DiscordOnboardingPlan) -> DiscordRoutingPreview {
    DiscordRoutingPreview {
        connector_id: plan.connector_id.clone(),
        mode: plan.mode,
        inbound_scope: plan.inbound_scope,
        require_mention: plan.require_mention,
        mention_patterns: plan.mention_patterns.clone(),
        allow_from: plan.allow_from.clone(),
        deny_from: plan.deny_from.clone(),
        allow_direct_messages: plan.allow_direct_messages,
        direct_message_policy: plan.direct_message_policy.as_str().to_owned(),
        broadcast_strategy: plan.broadcast_strategy.as_str().to_owned(),
        concurrency_limit: plan.concurrency_limit,
    }
}

fn build_discord_onboarding_warnings(
    plan: &DiscordOnboardingPlan,
    application: Option<&DiscordApplicationSummary>,
    require_open_scope_confirmation: bool,
) -> Vec<String> {
    let mut warnings = Vec::new();
    match plan.inbound_scope {
        DiscordOnboardingScope::DmOnly => warnings.push(
            "DM-only onboarding keeps guild replies mention-gated and routes DMs through explicit pairing/allowlist policy."
                .to_owned(),
        ),
        DiscordOnboardingScope::AllowlistedGuildChannels => {
            if plan.allow_from.is_empty() {
                warnings.push(
                    "Allowlisted guild scope is selected but allow_from is empty. Add sender allowlist entries to avoid broad guild routing."
                        .to_owned(),
                );
            } else {
                warnings.push(
                    "Allowlisted guild scope uses sender + mention gates. Keep rules narrowly scoped to trusted operators."
                        .to_owned(),
                );
            }
        }
        DiscordOnboardingScope::OpenGuildChannels => {
            warnings.push(
                "Open guild channels can trigger unsolicited responses. Keep this mode temporary and move to scoped allowlists."
                    .to_owned(),
            );
            if !require_open_scope_confirmation && !plan.confirm_open_guild_channels {
                warnings.push(
                    "Open guild channels will require explicit confirmation on apply."
                        .to_owned(),
                );
            }
        }
    }
    if matches!(plan.broadcast_strategy, channel_router::BroadcastStrategy::Allow) {
        warnings.push(
            "Broadcast strategy is set to allow. This enables broad outbound fan-out; keep deny unless explicitly required."
                .to_owned(),
        );
    }
    if matches!(plan.direct_message_policy, channel_router::DirectMessagePolicy::Allow) {
        warnings.push(
            "Direct message policy is set to allow. This bypasses pairing/allowlist safeguards for DMs."
                .to_owned(),
        );
    }
    if let Some(intents) = application.and_then(|summary| summary.intents.as_ref()) {
        if !matches!(intents.message_content, DiscordPrivilegedIntentStatus::Enabled) {
            warnings.push(
                "Discord Message Content intent is not fully enabled. Inbound command quality may be degraded."
                    .to_owned(),
            );
        }
        if !matches!(intents.guild_members, DiscordPrivilegedIntentStatus::Enabled) {
            warnings.push(
                "Guild Members intent is not fully enabled. Mention and membership resolution may be limited."
                    .to_owned(),
            );
        }
    } else {
        warnings.push(
            "Unable to read Discord application flags; intents checks are best-effort and were not confirmed."
                .to_owned(),
        );
    }
    warnings
}

/// Converts a channel permission check into operator-facing warnings, one
/// per failed check status and one per missing required permission.
pub(crate) fn build_discord_channel_permission_warnings(
    check: Option<&DiscordChannelPermissionCheck>,
) -> Vec<String> {
    let Some(check) = check else {
        return Vec::new();
    };
    let mut warnings = Vec::new();
    match check.status {
        DiscordChannelPermissionCheckStatus::Ok => {}
        DiscordChannelPermissionCheckStatus::Forbidden => warnings.push(format!(
            "Discord verify_channel_id preflight failed: bot cannot access channel '{}'. Verify channel visibility and permission overrides.",
            check.channel_id
        )),
        DiscordChannelPermissionCheckStatus::NotFound => warnings.push(format!(
            "Discord verify_channel_id preflight failed: channel '{}' was not found for this bot token.",
            check.channel_id
        )),
        DiscordChannelPermissionCheckStatus::Unavailable => warnings.push(format!(
            "Discord verify_channel_id preflight check for '{}' is unavailable right now. Retry probe/apply after Discord API connectivity stabilizes.",
            check.channel_id
        )),
        DiscordChannelPermissionCheckStatus::ParseError => warnings.push(format!(
            "Discord verify_channel_id preflight returned channel '{}', but permission bitset could not be parsed.",
            check.channel_id
        )),
    }
    if !check.can_view_channel {
        warnings.push(format!(
            "Discord verify_channel_id '{}' is missing 'View Channels' permission.",
            check.channel_id
        ));
    }
    if !check.can_send_messages {
        warnings.push(format!(
            "Discord verify_channel_id '{}' is missing 'Send Messages' permission.",
            check.channel_id
        ));
    }
    if !check.can_read_message_history {
        warnings.push(format!(
            "Discord verify_channel_id '{}' is missing 'Read Message History' permission.",
            check.channel_id
        ));
    }
    if !check.can_embed_links {
        warnings.push(format!(
            "Discord verify_channel_id '{}' is missing 'Embed Links' permission.",
            check.channel_id
        ));
    }
    if !check.can_attach_files {
        warnings.push(format!(
            "Discord verify_channel_id '{}' is missing 'Attach Files' permission.",
            check.channel_id
        ));
    }
    if !check.can_send_messages_in_threads {
        warnings.push(format!(
            "Discord verify_channel_id '{}' is missing 'Send Messages in Threads' permission.",
            check.channel_id
        ));
    }
    warnings
}

/// Describes the security posture this plan ships with, as human-readable
/// statements surfaced in the preflight preview.
pub(crate) fn build_discord_onboarding_security_defaults(
    plan: &DiscordOnboardingPlan,
) -> Vec<String> {
    let mut defaults = vec![
        "Connector ingress auth can be scoped with admin.connector_token / PALYRA_CONNECTOR_TOKEN instead of reusing admin token."
            .to_owned(),
        "Discord attachment downloads are deny-by-default in current scope; connector forwards metadata only."
            .to_owned(),
        "Discord egress is restricted to explicit allowlist domains (API, gateway, CDN variants)."
            .to_owned(),
    ];
    defaults.push(if plan.require_mention {
        "Guild routing is mention-gated by default using canonical <@bot_id>/<@!bot_id> patterns; @everyone/@here are deny-by-default triggers unless explicitly configured."
            .to_owned()
    } else {
        "Guild routing mention gate is disabled for this plan; keep scope narrow and explicitly approved."
            .to_owned()
    });
    defaults.push(format!(
        "DM policy default is '{}'; pairing keeps direct-message senders explicit and auditable.",
        plan.direct_message_policy.as_str()
    ));
    defaults
}

/// Summarizes the connector's inbound gateway-monitor state from the current
/// runtime snapshot; lookup failures degrade to a not-registered summary.
pub(crate) fn load_discord_inbound_monitor_summary(
    state: &AppState,
    connector_id: &str,
) -> DiscordInboundMonitorSummary {
    let connector_registered = state.channels.status(connector_id).is_ok();
    let runtime = state.channels.runtime_snapshot(connector_id).ok().flatten();
    summarize_discord_inbound_monitor(connector_registered, runtime.as_ref())
}

/// Polls the inbound monitor summary until it reports alive or the bounded
/// wait expires, returning the last summary either way.
///
/// The gateway monitor exposes no readiness signal to await, so a short
/// bounded poll of the runtime snapshot is the only way to give apply
/// responses a meaningful `inbound_alive` without blocking indefinitely.
async fn wait_for_discord_inbound_monitor_summary(
    state: &AppState,
    connector_id: &str,
) -> DiscordInboundMonitorSummary {
    let started = Instant::now();
    let timeout = Duration::from_millis(DISCORD_ONBOARDING_MONITOR_WAIT_TIMEOUT_MS);
    loop {
        let summary = load_discord_inbound_monitor_summary(state, connector_id);
        if discord_inbound_monitor_is_alive(&summary) || started.elapsed() >= timeout {
            return summary;
        }
        tokio::time::sleep(Duration::from_millis(DISCORD_ONBOARDING_MONITOR_WAIT_POLL_MS)).await;
    }
}

/// Builds the inbound-monitor summary from a runtime snapshot's `inbound`
/// section; missing fields degrade to disconnected/never-observed values.
pub(crate) fn summarize_discord_inbound_monitor(
    connector_registered: bool,
    runtime: Option<&Value>,
) -> DiscordInboundMonitorSummary {
    let inbound = runtime.and_then(|value| value.get("inbound"));
    let gateway_connected = inbound
        .and_then(|value| value.get("gateway_connected"))
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let last_inbound_unix_ms =
        inbound.and_then(|value| value.get("last_inbound_unix_ms")).and_then(Value::as_i64);
    let last_connect_unix_ms =
        inbound.and_then(|value| value.get("last_connect_unix_ms")).and_then(Value::as_i64);
    let last_disconnect_unix_ms =
        inbound.and_then(|value| value.get("last_disconnect_unix_ms")).and_then(Value::as_i64);
    let last_event_type = inbound
        .and_then(|value| value.get("last_event_type"))
        .and_then(Value::as_str)
        .map(ToOwned::to_owned);
    let now_unix_ms = unix_ms_now().unwrap_or_default();
    let recent_inbound = last_inbound_unix_ms.is_some_and(|observed_at| {
        observed_at > 0
            && now_unix_ms.saturating_sub(observed_at)
                <= DISCORD_ONBOARDING_INBOUND_RECENT_WINDOW_MS
    });
    DiscordInboundMonitorSummary {
        connector_registered,
        gateway_connected,
        recent_inbound,
        last_inbound_unix_ms,
        last_connect_unix_ms,
        last_disconnect_unix_ms,
        last_event_type,
    }
}

/// Emits the single most actionable inbound-monitor warning for the
/// summary's state: registration, then connectivity, then first ingest, then
/// staleness; later checks are meaningless until earlier ones pass.
pub(crate) fn build_discord_inbound_monitor_warnings(
    summary: &DiscordInboundMonitorSummary,
) -> Vec<String> {
    let mut warnings = Vec::new();
    if !summary.connector_registered {
        warnings.push(
            "Inbound monitor check: connector is not registered yet. Apply onboarding to start gateway monitor."
                .to_owned(),
        );
        return warnings;
    }
    if !summary.gateway_connected {
        warnings.push(
            "Inbound monitor check: gateway monitor is not connected yet. Verify token, intents, and Discord egress allowlist."
                .to_owned(),
        );
        return warnings;
    }
    if summary.last_inbound_unix_ms.is_none() {
        warnings.push(
            "Inbound monitor check: gateway monitor is connected but no inbound messages were observed yet. Send a DM or <@bot_id> mention to confirm ingest."
                .to_owned(),
        );
        return warnings;
    }
    if !summary.recent_inbound {
        warnings.push(
            "Inbound monitor check: last inbound event is stale. Send a fresh DM or mention and verify last_inbound_unix_ms updates."
                .to_owned(),
        );
    }
    warnings
}

/// Returns whether the inbound monitor is fully alive: registered, gateway
/// connected, and with a recent inbound event.
pub(crate) fn discord_inbound_monitor_is_alive(summary: &DiscordInboundMonitorSummary) -> bool {
    summary.connector_registered && summary.gateway_connected && summary.recent_inbound
}

/// Dry-runs the deny-by-default policy engine for the message and tool
/// actions this plan will need, so operators learn about missing policy
/// grants during preflight instead of at first live message.
fn evaluate_discord_policy_warnings(state: &AppState, plan: &DiscordOnboardingPlan) -> Vec<String> {
    let mut warnings = Vec::new();
    let principal = channels::discord_principal(plan.account_id.as_str());
    let resource = format!("channel:{}", plan.connector_id);
    for action in [
        "message.reply",
        "channel.send",
        "message.broadcast",
        "attachment.metadata.accept",
        "attachment.download",
        "attachment.vision",
        "attachment.upload",
    ] {
        match evaluate_with_config(
            &PolicyRequest {
                principal: principal.clone(),
                action: action.to_owned(),
                resource: resource.clone(),
            },
            &PolicyEvaluationConfig::default(),
        ) {
            Ok(outcome) => {
                if let PolicyDecision::DenyByDefault { reason } = outcome.decision {
                    warnings.push(format!(
                        "Policy warning: action '{action}' for '{}' is denied by default ({reason}).",
                        plan.connector_id
                    ));
                }
            }
            Err(error) => warnings.push(format!(
                "Policy warning: failed to evaluate '{action}' for '{}': {}",
                plan.connector_id, error
            )),
        }
    }

    let tool_policy = PolicyEvaluationConfig {
        allowlisted_tools: state.tool_allowed_tools.clone(),
        allow_sensitive_tools: false,
        sensitive_tool_names: state.tool_allowed_tools.clone(),
        sensitive_capability_names: vec![
            "process_exec".to_owned(),
            "network".to_owned(),
            "secrets_read".to_owned(),
            "filesystem_write".to_owned(),
        ],
        ..PolicyEvaluationConfig::default()
    };
    match evaluate_with_context(
        &PolicyRequest {
            principal: principal.clone(),
            action: "tool.execute".to_owned(),
            resource: "tool:palyra.process.run".to_owned(),
        },
        &PolicyRequestContext {
            channel: Some(plan.connector_id.clone()),
            tool_name: Some("palyra.process.run".to_owned()),
            capabilities: vec!["process_exec".to_owned()],
            ..PolicyRequestContext::default()
        },
        &tool_policy,
    ) {
        Ok(outcome) => {
            if let PolicyDecision::DenyByDefault { reason } = outcome.decision {
                warnings.push(format!(
                    "Policy warning: tool execution for '{}' is currently denied ({reason}).",
                    plan.connector_id
                ));
            }
        }
        Err(error) => warnings.push(format!(
            "Policy warning: failed to evaluate tool execution policy for '{}': {}",
            plan.connector_id, error
        )),
    }
    warnings
}

/// Validates the bot token against the Discord API and returns the bot
/// identity plus best-effort application and channel-permission summaries.
///
/// Only the identity lookup is fatal; the application and channel checks
/// degrade to `None`/check-status values so transient Discord issues cannot
/// block onboarding.
///
/// # Errors
/// Returns `invalid_argument` when Discord rejects the token (401/403) and
/// `unavailable` when the identity endpoint is unreachable or returns an
/// unusable response.
pub(crate) async fn probe_discord_bot_identity(
    token: &str,
    verify_channel_id: Option<&str>,
) -> Result<
    (
        DiscordBotIdentitySummary,
        Option<DiscordApplicationSummary>,
        Option<DiscordChannelPermissionCheck>,
    ),
    tonic::Status,
> {
    let client = ReqwestClient::builder()
        .timeout(Duration::from_millis(DISCORD_ONBOARDING_HTTP_TIMEOUT_MS))
        .build()
        .map_err(|error| {
            tonic::Status::internal(format!(
                "failed to initialize discord preflight HTTP client: {error}"
            ))
        })?;
    let me_url = Url::parse(format!("{DISCORD_API_BASE}/users/@me").as_str()).map_err(|error| {
        tonic::Status::internal(format!("failed to construct discord API URL: {error}"))
    })?;
    let me_response = client
        .get(me_url)
        .header("Authorization", format!("Bot {token}"))
        .header("User-Agent", "palyra-discord-onboarding/1.0")
        .send()
        .await
        .map_err(|error| {
            tonic::Status::unavailable(format!(
                "failed to reach discord identity endpoint: {error}"
            ))
        })?;
    let me_status = me_response.status();
    let me_body = me_response.text().await.unwrap_or_default();
    if me_status.as_u16() == 401 || me_status.as_u16() == 403 {
        let summary = parse_discord_error_summary(me_body.as_str())
            .unwrap_or_else(|| "unauthorized".to_owned());
        return Err(tonic::Status::invalid_argument(format!(
            "discord token validation failed (status={}): {}",
            me_status.as_u16(),
            summary
        )));
    }
    if !me_status.is_success() {
        let summary = parse_discord_error_summary(me_body.as_str())
            .unwrap_or_else(|| "unexpected response".to_owned());
        return Err(tonic::Status::unavailable(format!(
            "discord identity lookup failed (status={}): {}",
            me_status.as_u16(),
            summary
        )));
    }
    let me_json = serde_json::from_str::<Value>(me_body.as_str()).map_err(|error| {
        tonic::Status::unavailable(format!(
            "discord identity endpoint returned invalid JSON: {error}"
        ))
    })?;
    let bot_id = me_json
        .get("id")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| tonic::Status::unavailable("discord identity response is missing bot id"))?
        .to_owned();
    let bot_username = me_json
        .get("username")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or("discord-bot")
        .to_owned();
    let bot = DiscordBotIdentitySummary { id: bot_id, username: bot_username };
    let application = fetch_discord_application_summary(&client, token).await;
    let channel_permission_check =
        probe_discord_channel_permission_check(&client, token, verify_channel_id).await;
    Ok((bot, application, channel_permission_check))
}

/// Builds the all-permissions-false check result used for every non-Ok
/// outcome. `can_view_channel` is `true` only for [`ParseError`] outcomes:
/// the channel GET itself succeeded, proving visibility, even though the
/// permission bitset could not be read.
///
/// [`ParseError`]: DiscordChannelPermissionCheckStatus::ParseError
fn discord_channel_permission_check_failure(
    channel_id: &str,
    status: DiscordChannelPermissionCheckStatus,
) -> DiscordChannelPermissionCheck {
    DiscordChannelPermissionCheck {
        channel_id: channel_id.to_owned(),
        status,
        can_view_channel: matches!(status, DiscordChannelPermissionCheckStatus::ParseError),
        can_send_messages: false,
        can_read_message_history: false,
        can_embed_links: false,
        can_attach_files: false,
        can_send_messages_in_threads: false,
    }
}

async fn probe_discord_channel_permission_check(
    client: &ReqwestClient,
    token: &str,
    verify_channel_id: Option<&str>,
) -> Option<DiscordChannelPermissionCheck> {
    let channel_id = verify_channel_id?.trim();
    if channel_id.is_empty() {
        return None;
    }
    let channel_url = build_discord_api_url(format!("/channels/{channel_id}").as_str()).ok()?;
    let response = match client
        .get(channel_url)
        .header("Authorization", format!("Bot {token}"))
        .header("User-Agent", "palyra-discord-onboarding/1.0")
        .send()
        .await
    {
        Ok(value) => value,
        Err(_) => {
            return Some(discord_channel_permission_check_failure(
                channel_id,
                DiscordChannelPermissionCheckStatus::Unavailable,
            ));
        }
    };
    let status_code = response.status().as_u16();
    let status_success = response.status().is_success();
    let body = response.text().await.unwrap_or_default();
    if status_code == 403 {
        return Some(discord_channel_permission_check_failure(
            channel_id,
            DiscordChannelPermissionCheckStatus::Forbidden,
        ));
    }
    if status_code == 404 {
        return Some(discord_channel_permission_check_failure(
            channel_id,
            DiscordChannelPermissionCheckStatus::NotFound,
        ));
    }
    if !status_success {
        return Some(discord_channel_permission_check_failure(
            channel_id,
            DiscordChannelPermissionCheckStatus::Unavailable,
        ));
    }
    let payload = match serde_json::from_str::<Value>(body.as_str()) {
        Ok(value) => value,
        Err(_) => {
            return Some(discord_channel_permission_check_failure(
                channel_id,
                DiscordChannelPermissionCheckStatus::ParseError,
            ));
        }
    };
    let Some(raw_permissions) = payload
        .get("permissions")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
    else {
        return Some(discord_channel_permission_check_failure(
            channel_id,
            DiscordChannelPermissionCheckStatus::ParseError,
        ));
    };
    let permissions_mask = match raw_permissions.parse::<u64>() {
        Ok(value) => value,
        Err(_) => {
            return Some(discord_channel_permission_check_failure(
                channel_id,
                DiscordChannelPermissionCheckStatus::ParseError,
            ));
        }
    };
    Some(DiscordChannelPermissionCheck {
        channel_id: channel_id.to_owned(),
        status: DiscordChannelPermissionCheckStatus::Ok,
        can_view_channel: (permissions_mask & DISCORD_PERMISSION_VIEW_CHANNEL) != 0,
        can_send_messages: (permissions_mask & DISCORD_PERMISSION_SEND_MESSAGES) != 0,
        can_read_message_history: (permissions_mask & DISCORD_PERMISSION_READ_MESSAGE_HISTORY) != 0,
        can_embed_links: (permissions_mask & DISCORD_PERMISSION_EMBED_LINKS) != 0,
        can_attach_files: (permissions_mask & DISCORD_PERMISSION_ATTACH_FILES) != 0,
        can_send_messages_in_threads: (permissions_mask
            & DISCORD_PERMISSION_SEND_MESSAGES_IN_THREADS)
            != 0,
    })
}

/// Best-effort lookup of the bot's application id, flags, and privileged
/// intent status; any failure returns `None` rather than blocking onboarding.
async fn fetch_discord_application_summary(
    client: &ReqwestClient,
    token: &str,
) -> Option<DiscordApplicationSummary> {
    let url = build_discord_api_url("/oauth2/applications/@me").ok()?;
    let response = client
        .get(url)
        .header("Authorization", format!("Bot {token}"))
        .header("User-Agent", "palyra-discord-onboarding/1.0")
        .send()
        .await
        .ok()?;
    if !response.status().is_success() {
        return None;
    }
    let body = response.text().await.ok()?;
    let payload = serde_json::from_str::<Value>(body.as_str()).ok()?;
    let id = payload.get("id").and_then(Value::as_str).map(str::to_owned);
    let flags = payload.get("flags").and_then(Value::as_u64);
    let intents = flags.map(resolve_discord_intents_from_flags);
    Some(DiscordApplicationSummary { id, flags, intents })
}

#[allow(clippy::result_large_err)]
fn build_discord_api_url(path: &str) -> Result<Url, Response> {
    Url::parse(format!("{DISCORD_API_BASE}{path}").as_str()).map_err(|error| {
        runtime_status_response(tonic::Status::internal(format!(
            "failed to construct discord API URL: {error}"
        )))
    })
}

/// Extracts a short, sanitized error summary from a Discord error body.
///
/// Prefers the structured `message` field; otherwise falls back to the raw
/// body capped at 200 chars, since the body is untrusted remote input that
/// ends up in operator-facing error strings.
fn parse_discord_error_summary(raw: &str) -> Option<String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return None;
    }
    if let Ok(parsed) = serde_json::from_str::<Value>(trimmed) {
        if let Some(message) = parsed.get("message").and_then(Value::as_str) {
            let sanitized = sanitize_http_error_message(message);
            let sanitized = sanitized.trim();
            if !sanitized.is_empty() {
                return Some(sanitized.to_owned());
            }
        }
    }
    let sanitized = sanitize_http_error_message(trimmed);
    let sanitized = sanitized.trim();
    if sanitized.is_empty() {
        None
    } else {
        Some(sanitized.chars().take(200).collect())
    }
}

/// Normalizes a pasted bot token: trims whitespace and strips an optional
/// authorization-scheme prefix users commonly copy from API examples.
/// Returns `None` when nothing usable remains.
pub(crate) fn normalize_discord_token(raw: &str) -> Option<String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return None;
    }
    let normalized = trimmed
        .strip_prefix("Bot ")
        .or_else(|| trimmed.strip_prefix("bot "))
        .unwrap_or(trimmed)
        .trim();
    if normalized.is_empty() {
        None
    } else {
        Some(normalized.to_owned())
    }
}

/// Lowercases, dedupes, and validates sender filter entries; the character
/// allowlist keeps the values safe to embed in TOML routing rules.
#[allow(clippy::result_large_err)]
fn normalize_discord_sender_filters(
    raw: Option<&[String]>,
    field_name: &'static str,
) -> Result<Vec<String>, Response> {
    let mut values = Vec::new();
    for candidate in raw.unwrap_or_default().iter().map(String::as_str).map(str::trim) {
        if candidate.is_empty() {
            continue;
        }
        if !candidate.chars().all(|ch| {
            ch.is_ascii_alphanumeric() || matches!(ch, '.' | '_' | '-' | '@' | ':' | '/' | '#')
        }) {
            return Err(runtime_status_response(tonic::Status::invalid_argument(format!(
                "{field_name} contains invalid sender identifier '{candidate}'"
            ))));
        }
        let normalized = candidate.to_ascii_lowercase();
        if !values.iter().any(|existing| existing == &normalized) {
            values.push(normalized);
        }
    }
    Ok(values)
}

/// Lowercases, dedupes, and length-bounds mention patterns; an empty result
/// lets `finalize_discord_onboarding_plan` substitute bot-derived defaults.
#[allow(clippy::result_large_err)]
fn normalize_discord_mention_patterns(raw: Option<&[String]>) -> Result<Vec<String>, Response> {
    let mut patterns = Vec::new();
    for candidate in raw.unwrap_or_default().iter().map(String::as_str) {
        let trimmed = candidate.trim();
        if trimmed.is_empty() {
            continue;
        }
        if trimmed.len() > 128 {
            return Err(runtime_status_response(tonic::Status::invalid_argument(
                "mention_patterns entries must be at most 128 bytes",
            )));
        }
        let normalized = trimmed.to_ascii_lowercase();
        if !patterns.iter().any(|existing| existing == &normalized) {
            patterns.push(normalized);
        }
    }
    Ok(patterns)
}

fn default_discord_mention_patterns(bot_id: &str, bot_username: &str) -> Vec<String> {
    let mut patterns = Vec::new();
    let mut defaults = vec![format!("<@{bot_id}>"), format!("<@!{bot_id}>"), "@palyra".to_owned()];
    let normalized_username = bot_username.trim().to_ascii_lowercase();
    if !normalized_username.is_empty() {
        defaults.push(format!("@{normalized_username}"));
    }
    for value in defaults {
        let normalized = value.trim().to_ascii_lowercase();
        if !normalized.is_empty() && !patterns.iter().any(|existing| existing == &normalized) {
            patterns.push(normalized);
        }
    }
    patterns
}

/// Merges the plan's routing rule into the config file (replacing any
/// existing rule for the same connector), enables the channel router, and
/// writes the document with rotating backups.
///
/// The merged document is validated against the full daemon config schema
/// before writing so a bad merge can never persist an unloadable config.
/// Returns the config path and whether the file was newly created.
#[allow(clippy::result_large_err)]
fn persist_discord_onboarding_config(
    plan: &DiscordOnboardingPlan,
) -> Result<DiscordOnboardingConfigWrite, Response> {
    let config_path = resolve_discord_onboarding_config_path()?;
    let config_exists = config_path.exists();
    let content = if config_exists {
        fs::read_to_string(config_path.as_path()).map_err(|error| {
            runtime_status_response(tonic::Status::internal(format!(
                "failed to read config for discord onboarding update: {error}"
            )))
        })?
    } else {
        String::new()
    };
    let (mut document, _) = parse_document_with_migration(content.as_str()).map_err(|error| {
        runtime_status_response(tonic::Status::internal(format!(
            "failed to parse config document for discord onboarding update: {error}"
        )))
    })?;
    let mut merged_rules = document
        .get("channel_router")
        .and_then(|value| value.get("routing"))
        .and_then(|value| value.get("channels"))
        .and_then(toml::Value::as_array)
        .cloned()
        .unwrap_or_default()
        .into_iter()
        .filter(|rule| {
            rule.get("channel")
                .and_then(toml::Value::as_str)
                .is_none_or(|channel| !channel.eq_ignore_ascii_case(plan.connector_id.as_str()))
        })
        .collect::<Vec<_>>();
    merged_rules.push(build_discord_onboarding_rule(plan));

    set_value_at_path(&mut document, "channel_router.enabled", toml::Value::Boolean(true))
        .map_err(|error| {
            runtime_status_response(tonic::Status::internal(format!(
                "failed to set channel_router.enabled during onboarding update: {error}"
            )))
        })?;
    set_value_at_path(
        &mut document,
        "channel_router.routing.channels",
        toml::Value::Array(merged_rules),
    )
    .map_err(|error| {
        runtime_status_response(tonic::Status::internal(format!(
            "failed to set channel_router routing rules during onboarding update: {error}"
        )))
    })?;
    validate_discord_onboarding_document(&document)?;

    if let Some(parent) = config_path.parent().filter(|path| !path.as_os_str().is_empty()) {
        fs::create_dir_all(parent).map_err(|error| {
            runtime_status_response(tonic::Status::internal(format!(
                "failed to create config directory during onboarding update: {error}"
            )))
        })?;
    }
    write_document_with_backups(
        config_path.as_path(),
        &document,
        DISCORD_ONBOARDING_CONFIG_BACKUPS,
    )
    .map_err(|error| {
        runtime_status_response(tonic::Status::internal(format!(
            "failed to persist config during discord onboarding update: {error}"
        )))
    })?;
    Ok(DiscordOnboardingConfigWrite {
        path: config_path.clone(),
        created: !config_exists,
        rollback: DiscordOnboardingConfigRollback {
            path: config_path,
            previous_content: config_exists.then_some(content),
        },
    })
}

fn build_discord_onboarding_rule(plan: &DiscordOnboardingPlan) -> toml::Value {
    let mut map = toml::map::Map::new();
    map.insert("channel".to_owned(), toml::Value::String(plan.connector_id.clone()));
    map.insert("enabled".to_owned(), toml::Value::Boolean(true));
    map.insert(
        "mention_patterns".to_owned(),
        toml::Value::Array(
            plan.mention_patterns.iter().map(|value| toml::Value::String(value.clone())).collect(),
        ),
    );
    map.insert(
        "allow_from".to_owned(),
        toml::Value::Array(
            plan.allow_from.iter().map(|value| toml::Value::String(value.clone())).collect(),
        ),
    );
    map.insert(
        "deny_from".to_owned(),
        toml::Value::Array(
            plan.deny_from.iter().map(|value| toml::Value::String(value.clone())).collect(),
        ),
    );
    map.insert(
        "allow_direct_messages".to_owned(),
        toml::Value::Boolean(plan.allow_direct_messages),
    );
    map.insert(
        "direct_message_policy".to_owned(),
        toml::Value::String(plan.direct_message_policy.as_str().to_owned()),
    );
    map.insert(
        "broadcast_strategy".to_owned(),
        toml::Value::String(plan.broadcast_strategy.as_str().to_owned()),
    );
    map.insert(
        "concurrency_limit".to_owned(),
        // Clamped to 1..=32 in build_discord_onboarding_plan, so this
        // conversion cannot actually saturate.
        toml::Value::Integer(i64::try_from(plan.concurrency_limit).unwrap_or(i64::MAX)),
    );
    toml::Value::Table(map)
}

/// Round-trips the merged TOML document through the daemon config schema to
/// reject any onboarding edit that would leave the config unloadable.
#[allow(clippy::result_large_err)]
fn validate_discord_onboarding_document(document: &toml::Value) -> Result<(), Response> {
    let serialized = toml::to_string(document).map_err(|error| {
        runtime_status_response(tonic::Status::internal(format!(
            "failed to serialize onboarding config document: {error}"
        )))
    })?;
    let _: RootFileConfig = toml::from_str(serialized.as_str()).map_err(|error| {
        runtime_status_response(tonic::Status::internal(format!(
            "onboarding config update produced invalid daemon schema: {error}"
        )))
    })?;
    Ok(())
}

/// Resolves where onboarding reads and writes the daemon config:
/// `PALYRA_CONFIG` when set, otherwise the first existing default search
/// path, otherwise the first default path (which will be created on write).
///
/// # Errors
/// Returns an error response when `PALYRA_CONFIG` holds an invalid path or
/// the platform exposes no default config location.
#[allow(clippy::result_large_err)]
pub(super) fn resolve_discord_onboarding_config_path() -> Result<PathBuf, Response> {
    if let Ok(path_raw) = std::env::var("PALYRA_CONFIG") {
        let parsed = parse_config_path(path_raw.as_str()).map_err(|error| {
            runtime_status_response(tonic::Status::invalid_argument(format!(
                "PALYRA_CONFIG contains an invalid config path: {error}"
            )))
        })?;
        return Ok(parsed);
    }
    let candidates = default_config_search_paths();
    if candidates.is_empty() {
        return Err(runtime_status_response(tonic::Status::failed_precondition(
            "no default config path is available on this platform; set PALYRA_CONFIG first",
        )));
    }
    for candidate in candidates.iter() {
        if candidate.exists() {
            return Ok(candidate.clone());
        }
    }
    Ok(candidates[0].clone())
}

/// Lifecycle-facing alias for the schema validation so `lifecycle.rs` shares
/// the exact same guarantee when rewriting the config during account removal.
///
/// # Errors
/// Returns an error response when the document no longer satisfies the
/// daemon config schema.
#[allow(clippy::result_large_err)]
pub(super) fn validate_discord_onboarding_document_for_lifecycle(
    document: &toml::Value,
) -> Result<(), Response> {
    validate_discord_onboarding_document(document)
}

#[cfg(test)]
mod tests {
    use palyra_vault::{BackendPreference, Vault, VaultConfig, VaultError, VaultRef};
    use tempfile::tempdir;

    use super::{rollback_discord_onboarding_vault_secret, DiscordOnboardingConfigRollback};

    #[test]
    fn discord_onboarding_config_rollback_restores_previous_content() {
        let temp = tempdir().expect("tempdir should be created");
        let config_path = temp.path().join("palyra.toml");
        std::fs::write(config_path.as_path(), "channel_router.enabled = false\n")
            .expect("initial config should be written");
        let rollback = DiscordOnboardingConfigRollback {
            path: config_path.clone(),
            previous_content: Some("channel_router.enabled = false\n".to_owned()),
        };
        std::fs::write(config_path.as_path(), "channel_router.enabled = true\n")
            .expect("updated config should be written");

        rollback.restore().expect("rollback should restore prior config");

        let restored =
            std::fs::read_to_string(config_path.as_path()).expect("config should be readable");
        assert_eq!(restored, "channel_router.enabled = false\n");
    }

    #[test]
    fn discord_onboarding_vault_rollback_deletes_stored_token() {
        let temp = tempdir().expect("tempdir should be created");
        let vault = Vault::open_with_config(VaultConfig {
            root: Some(temp.path().join("vault")),
            identity_store_root: Some(temp.path().join("identity")),
            backend_preference: BackendPreference::EncryptedFile,
            max_secret_bytes: 1024,
        })
        .expect("vault should open");
        let parsed_ref =
            VaultRef::parse("channel:discord:default/bot_token").expect("vault ref should parse");
        vault
            .put_secret(&parsed_ref.scope, parsed_ref.key.as_str(), b"discord-token")
            .expect("token should be stored");

        let deleted = rollback_discord_onboarding_vault_secret(&vault, &parsed_ref)
            .expect("rollback should delete token");

        assert!(deleted);
        assert!(matches!(
            vault.get_secret(&parsed_ref.scope, parsed_ref.key.as_str()),
            Err(VaultError::NotFound)
        ));
    }
}
