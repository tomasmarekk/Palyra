pub(crate) mod connectors;

use crate::journal::{
    ApprovalCreateRequest, ApprovalDecision, ApprovalPolicySnapshot, ApprovalPromptOption,
    ApprovalPromptRecord, ApprovalRecord, ApprovalRiskLevel, ApprovalSubjectType,
};
use crate::transport::http::handlers::console::channels::build_channel_router_preview_input;
use crate::transport::http::handlers::console::channels::connectors::discord::{
    build_discord_channel_permission_warnings, build_discord_inbound_monitor_warnings,
    discord_inbound_monitor_is_alive, load_discord_inbound_monitor_summary,
    normalize_optional_discord_channel_id, probe_discord_bot_identity,
};
use crate::*;
use palyra_connector_discord::{
    discord_permission_labels_for_operation, discord_policy_action_for_operation,
    DiscordMessageOperation,
};
use palyra_connectors::ConnectorMessageRecord;

const CHANNEL_MESSAGE_APPROVAL_TIMEOUT_SECONDS: u32 = 15 * 60;

pub(crate) async fn admin_channels_list_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<Value>, Response> {
    authorize_headers(&headers, &state.auth).map_err(|error| {
        state.runtime.record_denied();
        auth_error_response(error)
    })?;
    let _context = request_context_from_headers(&headers).map_err(|error| {
        state.runtime.record_denied();
        auth_error_response(error)
    })?;
    state.runtime.record_admin_status_request();
    let connectors = state.channels.list().map_err(channel_platform_error_response)?;
    Ok(Json(json!({ "connectors": connectors })))
}

#[allow(clippy::result_large_err)]
pub(crate) fn build_channel_status_payload(
    state: &AppState,
    connector_id: &str,
) -> Result<Value, Response> {
    let connector = state.channels.status(connector_id).map_err(channel_platform_error_response)?;
    let runtime =
        state.channels.runtime_snapshot(connector_id).map_err(channel_platform_error_response)?;
    let queue =
        state.channels.queue_snapshot(connector_id).map_err(channel_platform_error_response)?;
    let recent_dead_letters = state
        .channels
        .dead_letters(connector_id, Some(5))
        .map_err(channel_platform_error_response)?;
    Ok(json!({
        "connector": connector,
        "runtime": runtime,
        "operations": build_channel_operations_snapshot(
            connector_id,
            &connector,
            runtime.as_ref(),
            &queue,
            recent_dead_letters.as_slice(),
        ),
    }))
}

fn build_channel_operations_snapshot(
    connector_id: &str,
    connector: &palyra_connectors::ConnectorStatusSnapshot,
    runtime: Option<&Value>,
    queue: &palyra_connectors::ConnectorQueueSnapshot,
    recent_dead_letters: &[palyra_connectors::DeadLetterRecord],
) -> Value {
    let last_runtime_error = runtime
        .and_then(|payload| payload.get("last_error"))
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned);
    let runtime_global_retry_after_ms = runtime
        .and_then(|payload| payload.get("global_retry_after_ms"))
        .and_then(Value::as_i64)
        .filter(|value| *value > 0);
    let active_route_limits = runtime
        .and_then(|payload| payload.get("route_rate_limits"))
        .and_then(Value::as_array)
        .map(|entries| {
            entries
                .iter()
                .filter(|entry| {
                    entry
                        .get("retry_after_ms")
                        .and_then(Value::as_i64)
                        .is_some_and(|value| value > 0)
                })
                .count()
        })
        .unwrap_or(0);
    let last_permission_failure = find_matching_message(
        [
            connector.last_error.as_deref(),
            last_runtime_error.as_deref(),
            recent_dead_letters.first().map(|entry| entry.reason.as_str()),
        ],
        &[
            "missing permissions",
            "permission",
            "forbidden",
            "view channels",
            "send messages",
            "read message history",
            "embed links",
            "attach files",
            "send messages in threads",
        ],
    );
    let last_auth_failure = find_matching_message(
        [
            connector.last_error.as_deref(),
            last_runtime_error.as_deref(),
            recent_dead_letters.first().map(|entry| entry.reason.as_str()),
        ],
        &["auth", "token", "unauthorized", "credential missing", "missing credential"],
    );
    let mut saturation_reasons = Vec::new();
    let saturation_state = if !connector.enabled {
        saturation_reasons.push("connector_disabled".to_owned());
        "paused"
    } else if queue.paused {
        saturation_reasons.push("queue_paused".to_owned());
        if let Some(reason) = queue.pause_reason.as_deref() {
            saturation_reasons.push(format!("pause_reason={reason}"));
        }
        "paused"
    } else if queue.dead_letters > 0 {
        saturation_reasons.push(format!("dead_letters={}", queue.dead_letters));
        "dead_lettered"
    } else if runtime_global_retry_after_ms.is_some() || active_route_limits > 0 {
        if let Some(wait_ms) = runtime_global_retry_after_ms {
            saturation_reasons.push(format!("global_retry_after_ms={wait_ms}"));
        }
        if active_route_limits > 0 {
            saturation_reasons.push(format!("active_route_limits={active_route_limits}"));
        }
        "rate_limited"
    } else if queue.claimed_outbox > 0 || queue.due_outbox > 0 {
        if queue.claimed_outbox > 0 {
            saturation_reasons.push(format!("claimed_outbox={}", queue.claimed_outbox));
        }
        if queue.due_outbox > 0 {
            saturation_reasons.push(format!("due_outbox={}", queue.due_outbox));
        }
        "backpressure"
    } else if queue.pending_outbox > 0 {
        saturation_reasons.push(format!("pending_outbox={}", queue.pending_outbox));
        "retrying"
    } else {
        "healthy"
    };
    if let Some(error) = &connector.last_error {
        saturation_reasons.push(format!("last_error={error}"));
    } else if let Some(error) = &last_runtime_error {
        saturation_reasons.push(format!("runtime_error={error}"));
    }
    let discord = if connector.kind == palyra_connectors::ConnectorKind::Discord {
        json!({
            "required_permissions": discord_required_permission_labels(),
            "last_permission_failure": last_permission_failure,
            "exact_gap_check_available": true,
            "health_refresh_hint": format!(
                "Run channel health refresh for '{}' with verify_channel_id to confirm channel-specific Discord permission gaps.",
                connector_id
            ),
        })
    } else {
        Value::Null
    };
    json!({
        "queue": {
            "pending_outbox": queue.pending_outbox,
            "due_outbox": queue.due_outbox,
            "claimed_outbox": queue.claimed_outbox,
            "dead_letters": queue.dead_letters,
            "paused": queue.paused,
            "pause_reason": queue.pause_reason,
            "pause_updated_at_unix_ms": queue.pause_updated_at_unix_ms,
            "next_attempt_unix_ms": queue.next_attempt_unix_ms,
            "oldest_pending_created_at_unix_ms": queue.oldest_pending_created_at_unix_ms,
            "latest_dead_letter_unix_ms": queue.latest_dead_letter_unix_ms,
        },
        "saturation": {
            "state": saturation_state,
            "reasons": saturation_reasons,
        },
        "last_auth_failure": last_auth_failure,
        "rate_limits": {
            "global_retry_after_ms": runtime_global_retry_after_ms,
            "active_route_limits": active_route_limits,
            "routes": runtime.and_then(|payload| payload.get("route_rate_limits")).cloned(),
        },
        "discord": discord,
    })
}

fn find_matching_message<'a, I>(messages: I, needles: &[&str]) -> Option<String>
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

fn discord_account_id_from_connector_id(connector_id: &str) -> Option<&str> {
    connector_id.trim().strip_prefix("discord:").map(str::trim).filter(|value| !value.is_empty())
}

fn resolve_discord_connector_token(state: &AppState, connector_id: &str) -> Result<String, String> {
    let instance = state.channels.connector_instance(connector_id).map_err(|error| {
        format!(
            "failed to load connector instance '{}' for Discord token lookup: {error}",
            connector_id.trim()
        )
    })?;
    let vault_ref_raw = if let Some(vault_ref) =
        instance.token_vault_ref.as_deref().map(str::trim).filter(|value| !value.is_empty())
    {
        vault_ref.to_owned()
    } else {
        let Some(account_id) = discord_account_id_from_connector_id(connector_id) else {
            return Err(format!("connector '{}' is not a Discord connector", connector_id.trim()));
        };
        channels::discord_token_vault_ref(account_id)
    };
    let vault_ref = VaultRef::parse(vault_ref_raw.as_str()).map_err(|error| {
        format!("failed to parse Discord token vault ref '{}': {error}", vault_ref_raw)
    })?;
    let value =
        state.vault.get_secret(&vault_ref.scope, vault_ref.key.as_str()).map_err(|error| {
            format!("failed to load Discord token from vault ref '{}': {error}", vault_ref_raw)
        })?;
    let decoded = String::from_utf8(value).map_err(|error| {
        format!("Discord token from vault ref '{}' was not valid UTF-8: {error}", vault_ref_raw)
    })?;
    let token = decoded.trim().to_owned();
    if token.is_empty() {
        return Err(format!(
            "Discord token vault ref '{}' resolved to an empty secret",
            vault_ref_raw
        ));
    }
    Ok(token)
}

pub(crate) async fn build_channel_health_refresh_payload(
    state: &AppState,
    connector_id: &str,
    verify_channel_id: Option<String>,
) -> Result<Value, Response> {
    let mut payload = build_channel_status_payload(state, connector_id)?;
    if !connector_id.trim().starts_with("discord:") {
        payload["health_refresh"] = json!({
            "supported": false,
            "message": "health refresh is currently implemented for Discord connectors only",
        });
        return Ok(payload);
    }

    let token = match resolve_discord_connector_token(state, connector_id) {
        Ok(token) => token,
        Err(message) => {
            payload["health_refresh"] = json!({
                "supported": true,
                "refreshed": false,
                "message": message,
                "required_permissions": discord_required_permission_labels(),
            });
            return Ok(payload);
        }
    };

    let verify_channel_id = normalize_optional_discord_channel_id(verify_channel_id.as_deref())?;
    let inbound_monitor = load_discord_inbound_monitor_summary(state, connector_id);
    let inbound_alive = discord_inbound_monitor_is_alive(&inbound_monitor);
    let mut warnings = build_discord_inbound_monitor_warnings(&inbound_monitor);
    match probe_discord_bot_identity(token.as_str(), verify_channel_id.as_deref()).await {
        Ok((bot, application, channel_permission_check)) => {
            let permission_warnings =
                build_discord_channel_permission_warnings(channel_permission_check.as_ref());
            warnings.extend(permission_warnings.clone());
            payload["health_refresh"] = json!({
                "supported": true,
                "refreshed": true,
                "bot": bot,
                "application": application,
                "required_permissions": discord_required_permission_labels(),
                "channel_permission_check": channel_permission_check,
                "permission_warnings": permission_warnings,
                "inbound_monitor": inbound_monitor,
                "inbound_alive": inbound_alive,
                "warnings": warnings,
            });
        }
        Err(error) => {
            let message = sanitize_http_error_message(error.message());
            payload["health_refresh"] = json!({
                "supported": true,
                "refreshed": false,
                "message": message,
                "required_permissions": discord_required_permission_labels(),
                "inbound_monitor": inbound_monitor,
                "inbound_alive": inbound_alive,
                "warnings": warnings,
            });
        }
    }
    Ok(payload)
}

pub(crate) async fn channel_message_read_response(
    state: &AppState,
    context: &RequestContext,
    connector_id: String,
    request: ConnectorMessageReadRequest,
) -> Result<Json<Value>, Response> {
    let connector_id = normalize_non_empty_field(connector_id, "connector_id")?;
    let result = state
        .channels
        .read_messages(connector_id.as_str(), channels::ChannelMessageReadOperation { request })
        .await
        .map_err(channel_platform_error_response)?;
    record_channel_message_console_event(
        state,
        context,
        "channel.message.read",
        json!({
            "connector_id": connector_id,
            "preflight": result.preflight,
            "target": result.target,
            "exact_message_id": result.exact_message_id,
            "message_count": result.messages.len(),
            "next_before_message_id": result.next_before_message_id,
            "next_after_message_id": result.next_after_message_id,
        }),
    )
    .await?;
    Ok(Json(json!({ "result": result })))
}

pub(crate) async fn channel_message_search_response(
    state: &AppState,
    context: &RequestContext,
    connector_id: String,
    request: ConnectorMessageSearchRequest,
) -> Result<Json<Value>, Response> {
    let connector_id = normalize_non_empty_field(connector_id, "connector_id")?;
    let result = state
        .channels
        .search_messages(connector_id.as_str(), channels::ChannelMessageSearchOperation { request })
        .await
        .map_err(channel_platform_error_response)?;
    record_channel_message_console_event(
        state,
        context,
        "channel.message.search",
        json!({
            "connector_id": connector_id,
            "preflight": result.preflight,
            "target": result.target,
            "query": result.query,
            "author_id": result.author_id,
            "has_attachments": result.has_attachments,
            "match_count": result.matches.len(),
            "next_before_message_id": result.next_before_message_id,
        }),
    )
    .await?;
    Ok(Json(json!({ "result": result })))
}

pub(crate) async fn channel_message_edit_response(
    state: &AppState,
    context: &RequestContext,
    connector_id: String,
    request: ConnectorMessageEditRequest,
    approval_id: Option<String>,
) -> Result<Json<Value>, Response> {
    let connector_id = normalize_non_empty_field(connector_id, "connector_id")?;
    let preview = state
        .channels
        .fetch_message_preview(connector_id.as_str(), &request.locator)
        .await
        .map_err(channel_platform_error_response)?;
    let authorization = resolve_channel_message_mutation_authorization(
        state,
        context,
        ChannelMessageMutationAuthorizationInput {
            connector_id: connector_id.as_str(),
            operation: channels::DiscordMessageMutationKind::Edit,
            locator: &request.locator,
            preview: preview.as_ref(),
            approval_id: approval_id.as_deref(),
            mutation_details: json!({
                "body": request.body,
                "preview_diff": {
                    "before_body": preview.as_ref().map(|message| message.body.clone()),
                    "after_body": request.body,
                },
            }),
        },
    )
    .await?;
    if let Some(response) = authorization.pending_response {
        return Ok(Json(response));
    }
    let result = state
        .channels
        .edit_message(connector_id.as_str(), channels::ChannelMessageEditOperation { request })
        .await
        .map_err(channel_platform_error_response)?;
    record_channel_message_console_event(
        state,
        context,
        "channel.message.edit",
        json!({
            "connector_id": connector_id,
            "approval_id": authorization.approval_id,
            "governance": authorization.governance.map(|value| json!({
                "risk_level": value.risk_level.as_str(),
                "approval_required": value.approval_required,
                "reason": value.reason,
            })),
            "result": result,
        }),
    )
    .await?;
    Ok(Json(json!({ "result": result })))
}

pub(crate) async fn channel_message_delete_response(
    state: &AppState,
    context: &RequestContext,
    connector_id: String,
    request: ConnectorMessageDeleteRequest,
    approval_id: Option<String>,
) -> Result<Json<Value>, Response> {
    let connector_id = normalize_non_empty_field(connector_id, "connector_id")?;
    let preview = state
        .channels
        .fetch_message_preview(connector_id.as_str(), &request.locator)
        .await
        .map_err(channel_platform_error_response)?;
    let authorization = resolve_channel_message_mutation_authorization(
        state,
        context,
        ChannelMessageMutationAuthorizationInput {
            connector_id: connector_id.as_str(),
            operation: channels::DiscordMessageMutationKind::Delete,
            locator: &request.locator,
            preview: preview.as_ref(),
            approval_id: approval_id.as_deref(),
            mutation_details: json!({
                "reason": request.reason,
                "preview_diff": {
                    "before_body": preview.as_ref().map(|message| message.body.clone()),
                    "after_body": Value::Null,
                },
            }),
        },
    )
    .await?;
    if let Some(response) = authorization.pending_response {
        return Ok(Json(response));
    }
    let result = state
        .channels
        .delete_message(connector_id.as_str(), channels::ChannelMessageDeleteOperation { request })
        .await
        .map_err(channel_platform_error_response)?;
    record_channel_message_console_event(
        state,
        context,
        "channel.message.delete",
        json!({
            "connector_id": connector_id,
            "approval_id": authorization.approval_id,
            "governance": authorization.governance.map(|value| json!({
                "risk_level": value.risk_level.as_str(),
                "approval_required": value.approval_required,
                "reason": value.reason,
            })),
            "result": result,
        }),
    )
    .await?;
    Ok(Json(json!({ "result": result })))
}

pub(crate) async fn channel_message_reaction_response(
    state: &AppState,
    context: &RequestContext,
    connector_id: String,
    request: ConnectorMessageReactionRequest,
    approval_id: Option<String>,
    operation: channels::DiscordMessageMutationKind,
) -> Result<Json<Value>, Response> {
    let connector_id = normalize_non_empty_field(connector_id, "connector_id")?;
    let preview = state
        .channels
        .fetch_message_preview(connector_id.as_str(), &request.locator)
        .await
        .map_err(channel_platform_error_response)?;
    let authorization = resolve_channel_message_mutation_authorization(
        state,
        context,
        ChannelMessageMutationAuthorizationInput {
            connector_id: connector_id.as_str(),
            operation,
            locator: &request.locator,
            preview: preview.as_ref(),
            approval_id: approval_id.as_deref(),
            mutation_details: json!({
                "emoji": request.emoji,
                "existing_reactions": preview.as_ref().map(|message| message.reactions.clone()),
            }),
        },
    )
    .await?;
    if let Some(response) = authorization.pending_response {
        return Ok(Json(response));
    }
    let result = match operation {
        channels::DiscordMessageMutationKind::ReactAdd => {
            state
                .channels
                .add_reaction(
                    connector_id.as_str(),
                    channels::ChannelMessageReactionOperation { request },
                )
                .await
        }
        channels::DiscordMessageMutationKind::ReactRemove => {
            state
                .channels
                .remove_reaction(
                    connector_id.as_str(),
                    channels::ChannelMessageReactionOperation { request },
                )
                .await
        }
        channels::DiscordMessageMutationKind::Edit
        | channels::DiscordMessageMutationKind::Delete => {
            return Err(runtime_status_response(tonic::Status::internal(
                "invalid reaction mutation dispatch",
            )));
        }
    }
    .map_err(channel_platform_error_response)?;
    record_channel_message_console_event(
        state,
        context,
        match operation {
            channels::DiscordMessageMutationKind::ReactAdd => "channel.message.react_add",
            channels::DiscordMessageMutationKind::ReactRemove => "channel.message.react_remove",
            channels::DiscordMessageMutationKind::Edit
            | channels::DiscordMessageMutationKind::Delete => unreachable!(),
        },
        json!({
            "connector_id": connector_id,
            "approval_id": authorization.approval_id,
            "governance": authorization.governance.map(|value| json!({
                "risk_level": value.risk_level.as_str(),
                "approval_required": value.approval_required,
                "reason": value.reason,
            })),
            "result": result,
        }),
    )
    .await?;
    Ok(Json(json!({ "result": result })))
}

#[derive(Debug, Clone)]
struct ChannelMessageMutationAuthorization {
    approval_id: Option<String>,
    governance: Option<channels::DiscordMessageMutationGovernance>,
    pending_response: Option<Value>,
}

struct ChannelMessageMutationAuthorizationInput<'a> {
    connector_id: &'a str,
    operation: channels::DiscordMessageMutationKind,
    locator: &'a ConnectorMessageLocator,
    preview: Option<&'a ConnectorMessageRecord>,
    approval_id: Option<&'a str>,
    mutation_details: Value,
}

struct ChannelMessageApprovalInput<'a> {
    connector_id: &'a str,
    operation: channels::DiscordMessageMutationKind,
    locator: &'a ConnectorMessageLocator,
    preview: &'a ConnectorMessageRecord,
    governance: &'a channels::DiscordMessageMutationGovernance,
    mutation_details: Value,
}

async fn resolve_channel_message_mutation_authorization(
    state: &AppState,
    context: &RequestContext,
    input: ChannelMessageMutationAuthorizationInput<'_>,
) -> Result<ChannelMessageMutationAuthorization, Response> {
    let preview = input.preview.ok_or_else(|| {
        runtime_status_response(tonic::Status::not_found(
            "message preview is unavailable for the requested mutation",
        ))
    })?;
    let connector =
        state.channels.status(input.connector_id).map_err(channel_platform_error_response)?;
    let governance = if connector.kind == palyra_connectors::ConnectorKind::Discord {
        let instance = state
            .channels
            .connector_instance(input.connector_id)
            .map_err(channel_platform_error_response)?;
        Some(channels::classify_discord_message_mutation_governance(
            &instance,
            preview,
            input.operation,
            unix_ms_now().map_err(|error| {
                runtime_status_response(tonic::Status::internal(sanitize_http_error_message(
                    error.to_string().as_str(),
                )))
            })?,
        ))
    } else {
        Some(channels::DiscordMessageMutationGovernance {
            risk_level: ApprovalRiskLevel::High,
            approval_required: true,
            reason: "non-Discord connector mutation defaults to explicit approval".to_owned(),
        })
    };
    let policy_action = channel_message_policy_action(input.operation);
    let subject_id =
        build_channel_message_subject_id(input.connector_id, input.operation, input.locator);
    let resource = build_channel_message_resource(input.connector_id, input.locator);
    let mut policy_config = PolicyEvaluationConfig::default();
    if governance.as_ref().is_some_and(|value| value.approval_required) {
        policy_config.sensitive_actions.push(policy_action.to_owned());
    }
    let resolved_approval = if governance.as_ref().is_some_and(|value| value.approval_required) {
        load_channel_message_approval(
            state,
            input.approval_id,
            subject_id.as_str(),
            context.principal.as_str(),
        )
        .await?
    } else {
        None
    };
    if resolved_approval.is_some() {
        policy_config.allow_sensitive_tools = true;
    }
    let evaluation = evaluate_with_context(
        &PolicyRequest {
            principal: context.principal.clone(),
            action: policy_action.to_owned(),
            resource: resource.clone(),
        },
        &PolicyRequestContext {
            device_id: Some(context.device_id.clone()),
            channel: context.channel.clone().or_else(|| Some(input.connector_id.to_owned())),
            ..PolicyRequestContext::default()
        },
        &policy_config,
    )
    .map_err(|error| {
        runtime_status_response(tonic::Status::internal(format!(
            "failed to evaluate channel message mutation policy: {error}"
        )))
    })?;
    match evaluation.decision {
        PolicyDecision::Allow => Ok(ChannelMessageMutationAuthorization {
            approval_id: resolved_approval.as_ref().map(|record| record.approval_id.clone()),
            governance,
            pending_response: None,
        }),
        PolicyDecision::DenyByDefault { reason } => {
            if governance.as_ref().is_some_and(|value| value.approval_required)
                && evaluation.explanation.is_sensitive_action
            {
                let approval = ensure_channel_message_approval(
                    state,
                    context,
                    ChannelMessageApprovalInput {
                        connector_id: input.connector_id,
                        operation: input.operation,
                        locator: input.locator,
                        preview,
                        governance: governance
                            .as_ref()
                            .expect("governance should exist for approval"),
                        mutation_details: input.mutation_details,
                    },
                )
                .await?;
                record_channel_message_console_event(
                    state,
                    context,
                    "channel.message.approval_requested",
                    json!({
                        "connector_id": input.connector_id,
                        "subject_id": subject_id,
                        "policy_action": policy_action,
                        "policy_reason": reason,
                        "approval_id": approval.approval_id,
                    }),
                )
                .await?;
                return Ok(ChannelMessageMutationAuthorization {
                    approval_id: Some(approval.approval_id.clone()),
                    governance,
                    pending_response: Some(json!({
                        "approval_required": true,
                        "approval": approval,
                        "policy": {
                            "action": policy_action,
                            "resource": resource,
                            "reason": reason,
                            "explanation": evaluation.explanation.reason,
                        },
                        "preview": channels::ChannelMessageMutationPreview {
                            locator: input.locator.clone(),
                            message: Some(preview.clone()),
                            approved: false,
                            approval_id: Some(approval.approval_id.clone()),
                        },
                    })),
                });
            }
            Err(runtime_status_response(tonic::Status::permission_denied(format!(
                "policy denied action '{policy_action}' on '{resource}': {reason}"
            ))))
        }
    }
}

async fn load_channel_message_approval(
    state: &AppState,
    approval_id: Option<&str>,
    subject_id: &str,
    principal: &str,
) -> Result<Option<ApprovalRecord>, Response> {
    let Some(approval_id) = approval_id.map(str::trim).filter(|value| !value.is_empty()) else {
        return Ok(None);
    };
    let approval = state
        .runtime
        .approval_record(approval_id.to_owned())
        .await
        .map_err(runtime_status_response)?
        .ok_or_else(|| {
            runtime_status_response(tonic::Status::permission_denied(format!(
                "approval '{}' does not exist for this message mutation",
                approval_id
            )))
        })?;
    if approval.subject_id != subject_id || approval.principal != principal {
        return Err(runtime_status_response(tonic::Status::permission_denied(
            "approval subject does not match the requested message mutation",
        )));
    }
    match approval.decision {
        Some(ApprovalDecision::Allow) => Ok(Some(approval)),
        Some(ApprovalDecision::Deny) => Err(runtime_status_response(
            tonic::Status::permission_denied("message mutation approval was explicitly denied"),
        )),
        Some(ApprovalDecision::Timeout) => Err(runtime_status_response(
            tonic::Status::permission_denied("message mutation approval has expired"),
        )),
        Some(ApprovalDecision::Error) => Err(runtime_status_response(
            tonic::Status::permission_denied("message mutation approval is in an error state"),
        )),
        None => Err(runtime_status_response(tonic::Status::permission_denied(
            "message mutation approval is still pending",
        ))),
    }
}

async fn ensure_channel_message_approval(
    state: &AppState,
    context: &RequestContext,
    input: ChannelMessageApprovalInput<'_>,
) -> Result<ApprovalRecord, Response> {
    let subject_id =
        build_channel_message_subject_id(input.connector_id, input.operation, input.locator);
    let policy_action = channel_message_policy_action(input.operation);
    let details_json = json!({
        "connector_id": input.connector_id,
        "operation": input.operation.as_str(),
        "policy_action": policy_action,
        "locator": input.locator,
        "preview_message": input.preview,
        "governance": {
            "risk_level": input.governance.risk_level.as_str(),
            "approval_required": input.governance.approval_required,
            "reason": input.governance.reason,
        },
        "mutation": input.mutation_details,
        "required_permissions": channel_message_required_permissions(input.operation),
    })
    .to_string();
    let policy_hash = hex::encode(Sha256::digest(details_json.as_bytes()));
    state
        .runtime
        .create_approval_record(ApprovalCreateRequest {
            approval_id: Ulid::new().to_string(),
            session_id: Ulid::new().to_string(),
            run_id: Ulid::new().to_string(),
            principal: context.principal.clone(),
            device_id: context.device_id.clone(),
            channel: context
                .channel
                .clone()
                .or_else(|| Some(input.connector_id.to_owned())),
            subject_type: ApprovalSubjectType::Tool,
            subject_id: subject_id.clone(),
            request_summary: format!(
                "connector={} operation={} conversation_id={} thread_id={} message_id={}",
                input.connector_id,
                input.operation.as_str(),
                input.locator.target.conversation_id,
                input.locator.target.thread_id.as_deref().unwrap_or("-"),
                input.locator.message_id
            ),
            policy_snapshot: ApprovalPolicySnapshot {
                policy_id: "discord.message.mutation.approval.v1".to_owned(),
                policy_hash,
                evaluation_summary: format!(
                    "action={} approval_required={} risk_level={} {}",
                    policy_action,
                    input.governance.approval_required,
                    input.governance.risk_level.as_str(),
                    input.governance.reason
                ),
            },
            prompt: ApprovalPromptRecord {
                title: format!("Approve Discord message {}", input.operation.as_str()),
                risk_level: input.governance.risk_level,
                subject_id,
                summary: format!(
                    "Connector '{}' wants to {} Discord message '{}'",
                    input.connector_id,
                    input.operation.as_str(),
                    input.locator.message_id
                ),
                options: channel_message_approval_options(),
                timeout_seconds: CHANNEL_MESSAGE_APPROVAL_TIMEOUT_SECONDS,
                details_json,
                policy_explanation: format!(
                    "Discord message mutations stay deny-by-default for higher-risk channel, age, and connector-profile combinations. {}",
                    input.governance.reason
                ),
            },
        })
        .await
        .map_err(runtime_status_response)
}

fn channel_message_approval_options() -> Vec<ApprovalPromptOption> {
    vec![
        ApprovalPromptOption {
            option_id: "allow_once".to_owned(),
            label: "Approve once".to_owned(),
            description: "Allow this exact Discord message mutation one time.".to_owned(),
            default_selected: true,
            decision_scope: ApprovalDecisionScope::Once,
            timebox_ttl_ms: None,
        },
        ApprovalPromptOption {
            option_id: "deny_once".to_owned(),
            label: "Keep blocked".to_owned(),
            description: "Keep the Discord mutation blocked until an operator explicitly retries."
                .to_owned(),
            default_selected: false,
            decision_scope: ApprovalDecisionScope::Once,
            timebox_ttl_ms: None,
        },
    ]
}

fn build_channel_message_subject_id(
    connector_id: &str,
    operation: channels::DiscordMessageMutationKind,
    locator: &ConnectorMessageLocator,
) -> String {
    format!(
        "channel-message:{}:{}:{}:{}",
        connector_id,
        operation.as_str(),
        locator.target.conversation_id,
        locator.message_id
    )
}

fn build_channel_message_resource(connector_id: &str, locator: &ConnectorMessageLocator) -> String {
    format!(
        "channel:{}:message:{}:{}",
        connector_id, locator.target.conversation_id, locator.message_id
    )
}

fn channel_message_policy_action(operation: channels::DiscordMessageMutationKind) -> &'static str {
    let discord_operation = match operation {
        channels::DiscordMessageMutationKind::Edit => DiscordMessageOperation::Edit,
        channels::DiscordMessageMutationKind::Delete => DiscordMessageOperation::Delete,
        channels::DiscordMessageMutationKind::ReactAdd => DiscordMessageOperation::ReactAdd,
        channels::DiscordMessageMutationKind::ReactRemove => DiscordMessageOperation::ReactRemove,
    };
    discord_policy_action_for_operation(discord_operation)
}

fn channel_message_required_permissions(
    operation: channels::DiscordMessageMutationKind,
) -> Vec<String> {
    let discord_operation = match operation {
        channels::DiscordMessageMutationKind::Edit => DiscordMessageOperation::Edit,
        channels::DiscordMessageMutationKind::Delete => DiscordMessageOperation::Delete,
        channels::DiscordMessageMutationKind::ReactAdd => DiscordMessageOperation::ReactAdd,
        channels::DiscordMessageMutationKind::ReactRemove => DiscordMessageOperation::ReactRemove,
    };
    discord_permission_labels_for_operation(discord_operation)
        .iter()
        .map(|value| (*value).to_owned())
        .collect()
}

async fn record_channel_message_console_event(
    state: &AppState,
    context: &RequestContext,
    event: &str,
    details: Value,
) -> Result<(), Response> {
    state
        .runtime
        .record_console_event(context, event, details)
        .await
        .map_err(runtime_status_response)
}

pub(crate) async fn admin_channel_status_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(connector_id): Path<String>,
) -> Result<Json<Value>, Response> {
    authorize_headers(&headers, &state.auth).map_err(|error| {
        state.runtime.record_denied();
        auth_error_response(error)
    })?;
    let _context = request_context_from_headers(&headers).map_err(|error| {
        state.runtime.record_denied();
        auth_error_response(error)
    })?;
    state.runtime.record_admin_status_request();
    let connector_id = normalize_non_empty_field(connector_id, "connector_id")?;
    Ok(Json(build_channel_status_payload(&state, connector_id.as_str())?))
}

pub(crate) async fn admin_channel_set_enabled_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(connector_id): Path<String>,
    Json(payload): Json<ChannelEnabledRequest>,
) -> Result<Json<Value>, Response> {
    authorize_headers(&headers, &state.auth).map_err(|error| {
        state.runtime.record_denied();
        auth_error_response(error)
    })?;
    let _context = request_context_from_headers(&headers).map_err(|error| {
        state.runtime.record_denied();
        auth_error_response(error)
    })?;
    state.runtime.record_admin_status_request();
    let connector_id = normalize_non_empty_field(connector_id, "connector_id")?;
    let connector = state
        .channels
        .set_enabled(connector_id.as_str(), payload.enabled)
        .map_err(channel_platform_error_response)?;
    Ok(Json(json!({ "connector": connector })))
}

pub(crate) async fn admin_channel_logs_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(connector_id): Path<String>,
    Query(query): Query<ChannelLogsQuery>,
) -> Result<Json<Value>, Response> {
    authorize_headers(&headers, &state.auth).map_err(|error| {
        state.runtime.record_denied();
        auth_error_response(error)
    })?;
    let _context = request_context_from_headers(&headers).map_err(|error| {
        state.runtime.record_denied();
        auth_error_response(error)
    })?;
    state.runtime.record_admin_status_request();
    admin_channel_logs_response(&state, connector_id, query.limit)
}

pub(crate) async fn admin_channel_message_read_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(connector_id): Path<String>,
    Json(payload): Json<ChannelMessageReadBody>,
) -> Result<Json<Value>, Response> {
    authorize_headers(&headers, &state.auth).map_err(|error| {
        state.runtime.record_denied();
        auth_error_response(error)
    })?;
    let context = request_context_from_headers(&headers).map_err(|error| {
        state.runtime.record_denied();
        auth_error_response(error)
    })?;
    state.runtime.record_admin_status_request();
    channel_message_read_response(&state, &context, connector_id, payload.request).await
}

pub(crate) async fn admin_channel_message_search_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(connector_id): Path<String>,
    Json(payload): Json<ChannelMessageSearchBody>,
) -> Result<Json<Value>, Response> {
    authorize_headers(&headers, &state.auth).map_err(|error| {
        state.runtime.record_denied();
        auth_error_response(error)
    })?;
    let context = request_context_from_headers(&headers).map_err(|error| {
        state.runtime.record_denied();
        auth_error_response(error)
    })?;
    state.runtime.record_admin_status_request();
    channel_message_search_response(&state, &context, connector_id, payload.request).await
}

pub(crate) async fn admin_channel_message_edit_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(connector_id): Path<String>,
    Json(payload): Json<ChannelMessageEditBody>,
) -> Result<Json<Value>, Response> {
    authorize_headers(&headers, &state.auth).map_err(|error| {
        state.runtime.record_denied();
        auth_error_response(error)
    })?;
    let context = request_context_from_headers(&headers).map_err(|error| {
        state.runtime.record_denied();
        auth_error_response(error)
    })?;
    state.runtime.record_admin_status_request();
    channel_message_edit_response(
        &state,
        &context,
        connector_id,
        payload.request,
        payload.approval_id,
    )
    .await
}

pub(crate) async fn admin_channel_message_delete_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(connector_id): Path<String>,
    Json(payload): Json<ChannelMessageDeleteBody>,
) -> Result<Json<Value>, Response> {
    authorize_headers(&headers, &state.auth).map_err(|error| {
        state.runtime.record_denied();
        auth_error_response(error)
    })?;
    let context = request_context_from_headers(&headers).map_err(|error| {
        state.runtime.record_denied();
        auth_error_response(error)
    })?;
    state.runtime.record_admin_status_request();
    channel_message_delete_response(
        &state,
        &context,
        connector_id,
        payload.request,
        payload.approval_id,
    )
    .await
}

pub(crate) async fn admin_channel_message_react_add_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(connector_id): Path<String>,
    Json(payload): Json<ChannelMessageReactionBody>,
) -> Result<Json<Value>, Response> {
    authorize_headers(&headers, &state.auth).map_err(|error| {
        state.runtime.record_denied();
        auth_error_response(error)
    })?;
    let context = request_context_from_headers(&headers).map_err(|error| {
        state.runtime.record_denied();
        auth_error_response(error)
    })?;
    state.runtime.record_admin_status_request();
    channel_message_reaction_response(
        &state,
        &context,
        connector_id,
        payload.request,
        payload.approval_id,
        channels::DiscordMessageMutationKind::ReactAdd,
    )
    .await
}

pub(crate) async fn admin_channel_message_react_remove_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(connector_id): Path<String>,
    Json(payload): Json<ChannelMessageReactionBody>,
) -> Result<Json<Value>, Response> {
    authorize_headers(&headers, &state.auth).map_err(|error| {
        state.runtime.record_denied();
        auth_error_response(error)
    })?;
    let context = request_context_from_headers(&headers).map_err(|error| {
        state.runtime.record_denied();
        auth_error_response(error)
    })?;
    state.runtime.record_admin_status_request();
    channel_message_reaction_response(
        &state,
        &context,
        connector_id,
        payload.request,
        payload.approval_id,
        channels::DiscordMessageMutationKind::ReactRemove,
    )
    .await
}

pub(crate) async fn admin_channel_logs_query_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(payload): Json<ChannelLogsRequest>,
) -> Result<Json<Value>, Response> {
    authorize_headers(&headers, &state.auth).map_err(|error| {
        state.runtime.record_denied();
        auth_error_response(error)
    })?;
    let _context = request_context_from_headers(&headers).map_err(|error| {
        state.runtime.record_denied();
        auth_error_response(error)
    })?;
    state.runtime.record_admin_status_request();
    admin_channel_logs_response(&state, payload.connector_id, payload.limit)
}

pub(crate) async fn admin_channel_health_refresh_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(connector_id): Path<String>,
    Json(payload): Json<ChannelHealthRefreshRequest>,
) -> Result<Json<Value>, Response> {
    authorize_headers(&headers, &state.auth).map_err(|error| {
        state.runtime.record_denied();
        auth_error_response(error)
    })?;
    let _context = request_context_from_headers(&headers).map_err(|error| {
        state.runtime.record_denied();
        auth_error_response(error)
    })?;
    state.runtime.record_admin_status_request();
    let connector_id = normalize_non_empty_field(connector_id, "connector_id")?;
    let payload = build_channel_health_refresh_payload(
        &state,
        connector_id.as_str(),
        payload.verify_channel_id,
    )
    .await?;
    Ok(Json(payload))
}

#[allow(clippy::result_large_err)]
fn admin_channel_logs_response(
    state: &AppState,
    connector_id: String,
    limit: Option<usize>,
) -> Result<Json<Value>, Response> {
    let connector_id = normalize_non_empty_field(connector_id, "connector_id")?;
    let events = state
        .channels
        .logs(connector_id.as_str(), limit)
        .map_err(channel_platform_error_response)?;
    let dead_letters = state
        .channels
        .dead_letters(connector_id.as_str(), limit)
        .map_err(channel_platform_error_response)?;
    Ok(Json(json!({
        "events": events,
        "dead_letters": dead_letters,
    })))
}

pub(crate) async fn admin_channel_queue_pause_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(connector_id): Path<String>,
) -> Result<Json<Value>, Response> {
    authorize_headers(&headers, &state.auth).map_err(|error| {
        state.runtime.record_denied();
        auth_error_response(error)
    })?;
    let _context = request_context_from_headers(&headers).map_err(|error| {
        state.runtime.record_denied();
        auth_error_response(error)
    })?;
    state.runtime.record_admin_status_request();
    let connector_id = normalize_non_empty_field(connector_id, "connector_id")?;
    let queue = state
        .channels
        .set_queue_paused(
            connector_id.as_str(),
            true,
            Some("operator requested queue pause via admin API"),
        )
        .map_err(channel_platform_error_response)?;
    let mut payload = build_channel_status_payload(&state, connector_id.as_str())?;
    payload["action"] = json!({
        "type": "queue_pause",
        "message": format!("queue paused for connector '{}'", connector_id),
        "queue": queue,
    });
    Ok(Json(payload))
}

pub(crate) async fn admin_channel_queue_resume_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(connector_id): Path<String>,
) -> Result<Json<Value>, Response> {
    authorize_headers(&headers, &state.auth).map_err(|error| {
        state.runtime.record_denied();
        auth_error_response(error)
    })?;
    let _context = request_context_from_headers(&headers).map_err(|error| {
        state.runtime.record_denied();
        auth_error_response(error)
    })?;
    state.runtime.record_admin_status_request();
    let connector_id = normalize_non_empty_field(connector_id, "connector_id")?;
    let queue = state
        .channels
        .set_queue_paused(connector_id.as_str(), false, None)
        .map_err(channel_platform_error_response)?;
    let mut payload = build_channel_status_payload(&state, connector_id.as_str())?;
    payload["action"] = json!({
        "type": "queue_resume",
        "message": format!("queue resumed for connector '{}'", connector_id),
        "queue": queue,
    });
    Ok(Json(payload))
}

pub(crate) async fn admin_channel_queue_drain_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(connector_id): Path<String>,
) -> Result<Json<Value>, Response> {
    authorize_headers(&headers, &state.auth).map_err(|error| {
        state.runtime.record_denied();
        auth_error_response(error)
    })?;
    let _context = request_context_from_headers(&headers).map_err(|error| {
        state.runtime.record_denied();
        auth_error_response(error)
    })?;
    state.runtime.record_admin_status_request();
    let connector_id = normalize_non_empty_field(connector_id, "connector_id")?;
    let drain = state
        .channels
        .drain_due_for_connector(connector_id.as_str())
        .await
        .map_err(channel_platform_error_response)?;
    let mut payload = build_channel_status_payload(&state, connector_id.as_str())?;
    payload["action"] = json!({
        "type": "queue_drain",
        "message": format!("queue drain completed for connector '{}'", connector_id),
        "drain": drain,
    });
    Ok(Json(payload))
}

pub(crate) async fn admin_channel_dead_letter_replay_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(path): Path<DeadLetterActionPath>,
) -> Result<Json<Value>, Response> {
    authorize_headers(&headers, &state.auth).map_err(|error| {
        state.runtime.record_denied();
        auth_error_response(error)
    })?;
    let _context = request_context_from_headers(&headers).map_err(|error| {
        state.runtime.record_denied();
        auth_error_response(error)
    })?;
    state.runtime.record_admin_status_request();
    let connector_id = normalize_non_empty_field(path.connector_id, "connector_id")?;
    let replayed = state
        .channels
        .replay_dead_letter(connector_id.as_str(), path.dead_letter_id)
        .map_err(channel_platform_error_response)?;
    let mut payload = build_channel_status_payload(&state, connector_id.as_str())?;
    payload["action"] = json!({
        "type": "dead_letter_replay",
        "message": format!(
            "dead-letter {} replayed for connector '{}'",
            path.dead_letter_id, connector_id
        ),
        "dead_letter": replayed,
    });
    Ok(Json(payload))
}

pub(crate) async fn admin_channel_dead_letter_discard_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(path): Path<DeadLetterActionPath>,
) -> Result<Json<Value>, Response> {
    authorize_headers(&headers, &state.auth).map_err(|error| {
        state.runtime.record_denied();
        auth_error_response(error)
    })?;
    let _context = request_context_from_headers(&headers).map_err(|error| {
        state.runtime.record_denied();
        auth_error_response(error)
    })?;
    state.runtime.record_admin_status_request();
    let connector_id = normalize_non_empty_field(path.connector_id, "connector_id")?;
    let discarded = state
        .channels
        .discard_dead_letter(connector_id.as_str(), path.dead_letter_id)
        .map_err(channel_platform_error_response)?;
    let mut payload = build_channel_status_payload(&state, connector_id.as_str())?;
    payload["action"] = json!({
        "type": "dead_letter_discard",
        "message": format!(
            "dead-letter {} discarded for connector '{}'",
            path.dead_letter_id, connector_id
        ),
        "dead_letter": discarded,
    });
    Ok(Json(payload))
}

pub(crate) async fn admin_channel_test_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(connector_id): Path<String>,
    Json(payload): Json<ChannelTestRequest>,
) -> Result<Json<Value>, Response> {
    authorize_headers(&headers, &state.auth).map_err(|error| {
        state.runtime.record_denied();
        auth_error_response(error)
    })?;
    let _context = request_context_from_headers(&headers).map_err(|error| {
        state.runtime.record_denied();
        auth_error_response(error)
    })?;
    state.runtime.record_admin_status_request();
    let connector_id = normalize_non_empty_field(connector_id, "connector_id")?;
    let ingest = state
        .channels
        .submit_test_message(
            connector_id.as_str(),
            channels::ChannelTestMessageRequest {
                text: payload.text,
                conversation_id: payload
                    .conversation_id
                    .unwrap_or_else(|| "test:conversation".to_owned()),
                sender_id: payload.sender_id.unwrap_or_else(|| "test-user".to_owned()),
                sender_display: payload.sender_display,
                simulate_crash_once: payload.simulate_crash_once.unwrap_or(false),
                is_direct_message: payload.is_direct_message.unwrap_or(true),
                requested_broadcast: payload.requested_broadcast.unwrap_or(false),
            },
        )
        .await
        .map_err(channel_platform_error_response)?;
    let status =
        state.channels.status(connector_id.as_str()).map_err(channel_platform_error_response)?;
    let runtime = state
        .channels
        .runtime_snapshot(connector_id.as_str())
        .map_err(channel_platform_error_response)?;
    Ok(Json(json!({
        "ingest": ingest,
        "status": status,
        "runtime": runtime,
    })))
}

pub(crate) async fn admin_channel_test_send_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(connector_id): Path<String>,
    Json(payload): Json<ChannelTestSendRequest>,
) -> Result<Json<Value>, Response> {
    authorize_headers(&headers, &state.auth).map_err(|error| {
        state.runtime.record_denied();
        auth_error_response(error)
    })?;
    let _context = request_context_from_headers(&headers).map_err(|error| {
        state.runtime.record_denied();
        auth_error_response(error)
    })?;
    state.runtime.record_admin_status_request();
    let connector_id = normalize_non_empty_field(connector_id, "connector_id")?;
    let dispatch = state
        .channels
        .submit_discord_test_send(
            connector_id.as_str(),
            channels::ChannelDiscordTestSendRequest {
                target: payload.target,
                text: payload.text.unwrap_or_else(|| "palyra discord test message".to_owned()),
                confirm: payload.confirm.unwrap_or(false),
                auto_reaction: payload.auto_reaction,
                thread_id: payload.thread_id,
                reply_to_message_id: payload.reply_to_message_id,
            },
        )
        .await
        .map_err(channel_platform_error_response)?;
    let status =
        state.channels.status(connector_id.as_str()).map_err(channel_platform_error_response)?;
    let runtime = state
        .channels
        .runtime_snapshot(connector_id.as_str())
        .map_err(channel_platform_error_response)?;
    Ok(Json(json!({
        "dispatch": dispatch,
        "status": status,
        "runtime": runtime,
    })))
}

pub(crate) async fn admin_channel_router_rules_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<Value>, Response> {
    authorize_headers(&headers, &state.auth).map_err(|error| {
        state.runtime.record_denied();
        auth_error_response(error)
    })?;
    let _context = request_context_from_headers(&headers).map_err(|error| {
        state.runtime.record_denied();
        auth_error_response(error)
    })?;
    state.runtime.record_admin_status_request();
    let config = state.runtime.channel_router_config_snapshot();
    let config_hash = state.runtime.channel_router_config_hash();
    Ok(Json(json!({
        "config": config,
        "config_hash": config_hash,
    })))
}

pub(crate) async fn admin_channel_router_warnings_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<Value>, Response> {
    authorize_headers(&headers, &state.auth).map_err(|error| {
        state.runtime.record_denied();
        auth_error_response(error)
    })?;
    let _context = request_context_from_headers(&headers).map_err(|error| {
        state.runtime.record_denied();
        auth_error_response(error)
    })?;
    state.runtime.record_admin_status_request();
    Ok(Json(json!({
        "warnings": state.runtime.channel_router_validation_warnings(),
        "config_hash": state.runtime.channel_router_config_hash(),
    })))
}

pub(crate) async fn admin_channel_router_preview_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(payload): Json<ChannelRouterPreviewRequest>,
) -> Result<Json<Value>, Response> {
    authorize_headers(&headers, &state.auth).map_err(|error| {
        state.runtime.record_denied();
        auth_error_response(error)
    })?;
    let _context = request_context_from_headers(&headers).map_err(|error| {
        state.runtime.record_denied();
        auth_error_response(error)
    })?;
    state.runtime.record_admin_status_request();
    let preview_input = build_channel_router_preview_input(payload)?;
    let preview = state.runtime.channel_router_preview(&preview_input);
    Ok(Json(json!({ "preview": preview })))
}

pub(crate) async fn admin_channel_router_pairings_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<ChannelRouterPairingsQuery>,
) -> Result<Json<Value>, Response> {
    authorize_headers(&headers, &state.auth).map_err(|error| {
        state.runtime.record_denied();
        auth_error_response(error)
    })?;
    let _context = request_context_from_headers(&headers).map_err(|error| {
        state.runtime.record_denied();
        auth_error_response(error)
    })?;
    state.runtime.record_admin_status_request();
    let channel = query.channel.as_deref().map(str::trim).filter(|value| !value.is_empty());
    Ok(Json(json!({
        "pairings": state.runtime.channel_router_pairing_snapshot(channel),
        "config_hash": state.runtime.channel_router_config_hash(),
    })))
}

pub(crate) async fn admin_channel_router_pairing_code_mint_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(payload): Json<ChannelRouterPairingCodeMintRequest>,
) -> Result<Json<Value>, Response> {
    authorize_headers(&headers, &state.auth).map_err(|error| {
        state.runtime.record_denied();
        auth_error_response(error)
    })?;
    let context = request_context_from_headers(&headers).map_err(|error| {
        state.runtime.record_denied();
        auth_error_response(error)
    })?;
    state.runtime.record_admin_status_request();
    let channel = normalize_non_empty_field(payload.channel, "channel")?;
    let issued_by =
        payload.issued_by.unwrap_or_else(|| format!("{}@{}", context.principal, context.device_id));
    let code = state
        .runtime
        .channel_router_mint_pairing_code(channel.as_str(), issued_by.as_str(), payload.ttl_ms)
        .map_err(runtime_status_response)?;
    Ok(Json(json!({
        "code": code,
        "config_hash": state.runtime.channel_router_config_hash(),
    })))
}

#[cfg(test)]
mod tests {
    use super::find_matching_message;

    #[test]
    fn find_matching_message_redacts_secret_like_values() {
        let message = find_matching_message(
            [Some("unauthorized: bearer topsecret token=abc123")],
            &["unauthorized", "token"],
        )
        .expect("matching auth failure should be returned");

        assert!(message.contains("<redacted>"), "matching message should be sanitized: {message}");
        assert!(
            !message.contains("topsecret") && !message.contains("token=abc123"),
            "matching message should not leak sensitive values: {message}"
        );
    }
}
