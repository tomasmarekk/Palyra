//! Discord message administration: read, search, edit, delete, and reaction flows.
//!
//! Every operation runs a permission preflight against the target channel first; denied or
//! unavailable targets produce structured denied results rather than errors, and 404/401/403
//! responses are mapped to "missing/no access" outcomes so callers can report them safely.

use palyra_common::redaction::redact_auth_error;
use reqwest::Url;
use serde_json::Value;

use crate::{
    net::ConnectorNetGuard,
    protocol::{
        ConnectorConversationTarget, ConnectorMessageLocator, ConnectorMessageMutationResult,
        ConnectorMessageMutationStatus, ConnectorMessageReactionRequest, ConnectorMessageRecord,
        ConnectorMessageSearchRequest,
    },
    storage::ConnectorInstanceRecord,
    supervisor::ConnectorAdapterError,
};

use super::{
    super::permissions::{self, DiscordMessageOperation},
    records::parse_discord_message_record,
    runtime::DiscordBotIdentity,
    transport::{
        build_channel_url, build_message_url, build_messages_url, build_reaction_url,
        parse_discord_error_summary, parse_rate_limit_snapshot, DiscordCredential,
        DiscordTransportResponse,
    },
    unix_ms_now, DiscordConnectorAdapter,
};

/// Resolves the channel id REST calls must address: the thread id when present (threads are
/// channels in Discord's API), otherwise the conversation id.
pub(super) fn effective_channel_id(target: &ConnectorConversationTarget) -> String {
    target
        .thread_id
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or(target.conversation_id.as_str())
        .to_owned()
}

// Discord serializes the permissions bitfield as a decimal string (it exceeds the safe JSON
// integer range), but tolerate a plain number too.
fn parse_discord_permissions_mask(payload: &Value) -> Option<u64> {
    payload.get("permissions").and_then(|value| {
        value.as_str().and_then(|raw| raw.trim().parse::<u64>().ok()).or_else(|| value.as_u64())
    })
}

fn missing_permission_labels(
    operation: DiscordMessageOperation,
    permission_mask: u64,
) -> Vec<String> {
    permissions::discord_permissions_for_operation(operation)
        .iter()
        .filter(|&(_, bit)| (permission_mask & *bit) == 0)
        .map(|(label, _)| (*label).to_owned())
        .collect()
}

fn operation_name(operation: DiscordMessageOperation) -> &'static str {
    match operation {
        DiscordMessageOperation::Send => "send",
        DiscordMessageOperation::Thread => "thread",
        DiscordMessageOperation::Reply => "reply",
        DiscordMessageOperation::Read => "read",
        DiscordMessageOperation::Search => "search",
        DiscordMessageOperation::Edit => "edit",
        DiscordMessageOperation::Delete => "delete",
        DiscordMessageOperation::ReactAdd => "react_add",
        DiscordMessageOperation::ReactRemove => "react_remove",
    }
}

/// Builds a denied preflight verdict for `operation` with the given reason.
pub(super) fn denied_operation_preflight(
    operation: DiscordMessageOperation,
    reason: String,
) -> crate::protocol::ConnectorOperationPreflight {
    permissions::discord_operation_preflight(operation, false, Some(reason), None, None)
}

/// Builds a denied mutation result carrying the preflight verdict and reason.
pub(super) fn denied_mutation_result(
    operation: DiscordMessageOperation,
    locator: ConnectorMessageLocator,
    reason: String,
) -> ConnectorMessageMutationResult {
    ConnectorMessageMutationResult {
        preflight: denied_operation_preflight(operation, reason.clone()),
        locator,
        status: ConnectorMessageMutationStatus::Denied,
        reason: Some(reason),
        message: None,
        diff: None,
    }
}

/// Applies the client-side search filters (Discord has no message-search REST endpoint for
/// bots, so pages are fetched and filtered locally). Absent filters always match.
pub(super) fn search_message_matches(
    message: &ConnectorMessageRecord,
    request: &ConnectorMessageSearchRequest,
) -> bool {
    let query_match =
        request.query.as_deref().map(str::trim).filter(|value| !value.is_empty()).is_none_or(
            |query| message.body.to_ascii_lowercase().contains(query.to_ascii_lowercase().as_str()),
        );
    let author_match = request
        .author_id
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .is_none_or(|author_id| message.sender_id.eq_ignore_ascii_case(author_id));
    // `required != is_empty()`: has_attachments=true matches non-empty attachment lists,
    // has_attachments=false matches empty ones.
    let attachment_match =
        request.has_attachments.is_none_or(|required| required != message.attachments.is_empty());
    query_match && author_match && attachment_match
}

/// Parameters for one message-history page fetch.
pub(super) struct DiscordMessagePageRequest<'a> {
    pub(super) channel_id: &'a str,
    pub(super) before_message_id: Option<&'a str>,
    pub(super) after_message_id: Option<&'a str>,
    pub(super) around_message_id: Option<&'a str>,
    pub(super) limit: usize,
    pub(super) operation: DiscordMessageOperation,
}

/// Interprets a mutation response: `Ok(None)` for missing/no-access statuses (the caller
/// reports a denied result), the parsed message record on success, `Err` otherwise.
pub(super) fn handle_mutation_message_response(
    context: &DiscordAdminContext,
    operation: DiscordMessageOperation,
    locator: &ConnectorMessageLocator,
    response: DiscordTransportResponse,
) -> Result<Option<ConnectorMessageRecord>, ConnectorAdapterError> {
    if response.status == 404 || response.status == 401 || response.status == 403 {
        return Ok(None);
    }
    if !(200..300).contains(&response.status) {
        return Err(ConnectorAdapterError::Backend(format!(
            "discord {} failed (status={}): {}",
            operation_name(operation),
            response.status,
            parse_discord_error_summary(response.body.as_str())
                .unwrap_or_else(|| "unexpected response".to_owned())
        )));
    }
    let payload = serde_json::from_str::<Value>(response.body.as_str()).map_err(|error| {
        ConnectorAdapterError::Backend(format!(
            "discord mutation response payload is invalid JSON: {error}"
        ))
    })?;
    Ok(parse_discord_message_record(&payload, context.bot_identity.id.as_str(), &locator.target))
}

/// Prepared per-operation context: resolved credential, egress guard, bot identity, the
/// effective target channel, and the permission-preflight verdict.
pub(super) struct DiscordAdminContext {
    pub(super) operation: DiscordMessageOperation,
    pub(super) credential: DiscordCredential,
    pub(super) guard: ConnectorNetGuard,
    pub(super) bot_identity: DiscordBotIdentity,
    pub(super) target_channel_id: String,
    pub(super) preflight_allowed: bool,
    pub(super) preflight_reason: Option<String>,
}

impl DiscordAdminContext {
    /// Materializes the preflight verdict for inclusion in operation results.
    pub(super) fn preflight(&self) -> crate::protocol::ConnectorOperationPreflight {
        permissions::discord_operation_preflight(
            self.operation,
            self.preflight_allowed,
            self.preflight_reason.clone(),
            None,
            None,
        )
    }
}

impl DiscordConnectorAdapter {
    /// Resolves credential, identity, and channel permissions for an admin operation.
    ///
    /// Returns `Ok(None)` when the target channel does not exist (404); otherwise a context
    /// whose `preflight_allowed`/`preflight_reason` carry the permission verdict — including
    /// denied contexts for unresolved identity or access failures.
    pub(super) async fn prepare_admin_context(
        &self,
        instance: &ConnectorInstanceRecord,
        operation: DiscordMessageOperation,
        target: &ConnectorConversationTarget,
    ) -> Result<Option<DiscordAdminContext>, ConnectorAdapterError> {
        let credential =
            self.credential_resolver.resolve_credential(instance).await.map_err(|error| {
                ConnectorAdapterError::Backend(redact_auth_error(error.to_string().as_str()))
            })?;
        self.record_credential_metadata(&credential);
        let guard = self.build_net_guard(instance)?;
        self.validate_url_target(&guard, &self.config.api_base_url)?;

        let Some(bot_identity) = self.resolve_bot_identity(&guard, &credential).await? else {
            // Identity could not be resolved: return a denied context with placeholder
            // identity so callers still get a structured preflight verdict to report.
            return Ok(Some(DiscordAdminContext {
                operation,
                credential,
                guard,
                bot_identity: DiscordBotIdentity {
                    id: "unknown".to_owned(),
                    username: "discord-bot".to_owned(),
                },
                target_channel_id: effective_channel_id(target),
                preflight_allowed: false,
                preflight_reason: Some(
                    "discord bot identity could not be resolved for admin preflight".to_owned(),
                ),
            }));
        };

        let target_channel_id = effective_channel_id(target);
        let channel_url = build_channel_url(&self.config.api_base_url, target_channel_id.as_str())?;
        self.validate_url_target(&guard, &channel_url)?;
        let channel_response = self
            .transport
            .get(&channel_url, credential.token.as_str(), self.config.request_timeout_ms)
            .await
            .map_err(|error| {
                ConnectorAdapterError::Backend(redact_auth_error(error.to_string().as_str()))
            })?;
        let now_unix_ms = unix_ms_now();
        let route_key = format!("discord:get:/channels/{target_channel_id}");
        let snapshot = parse_rate_limit_snapshot(&channel_response);
        self.apply_rate_limit_snapshot(route_key.as_str(), &snapshot, now_unix_ms)?;

        if channel_response.status == 404 {
            return Ok(None);
        }
        if channel_response.status == 401 || channel_response.status == 403 {
            return Ok(Some(DiscordAdminContext {
                operation,
                credential,
                guard,
                bot_identity,
                target_channel_id,
                preflight_allowed: false,
                preflight_reason: Some(format!(
                    "discord target channel access denied (status={}): {}",
                    channel_response.status,
                    parse_discord_error_summary(channel_response.body.as_str())
                        .unwrap_or_else(|| "forbidden".to_owned())
                )),
            }));
        }
        if !(200..300).contains(&channel_response.status) {
            return Err(ConnectorAdapterError::Backend(format!(
                "discord channel preflight failed (status={}): {}",
                channel_response.status,
                parse_discord_error_summary(channel_response.body.as_str())
                    .unwrap_or_else(|| "unexpected response".to_owned())
            )));
        }

        let channel_payload = serde_json::from_str::<Value>(channel_response.body.as_str())
            .map_err(|error| {
                ConnectorAdapterError::Backend(format!(
                    "discord channel preflight payload is invalid JSON: {error}"
                ))
            })?;
        let Some(permission_mask) = parse_discord_permissions_mask(&channel_payload) else {
            return Ok(Some(DiscordAdminContext {
                operation,
                credential,
                guard,
                bot_identity,
                target_channel_id,
                preflight_allowed: false,
                preflight_reason: Some(
                    "discord channel preflight response did not expose effective permissions"
                        .to_owned(),
                ),
            }));
        };
        let missing_permissions = missing_permission_labels(operation, permission_mask);
        let preflight_allowed = missing_permissions.is_empty();
        Ok(Some(DiscordAdminContext {
            operation,
            credential,
            guard,
            bot_identity,
            target_channel_id,
            preflight_allowed,
            preflight_reason: (!preflight_allowed).then(|| {
                format!(
                    "discord target is missing required permissions: {}",
                    missing_permissions.join(", ")
                )
            }),
        }))
    }

    /// Fetches one message by id; `Ok(None)` covers both deleted messages and access loss.
    pub(super) async fn fetch_single_message(
        &self,
        context: &DiscordAdminContext,
        target: &ConnectorConversationTarget,
        message_id: &str,
        operation: DiscordMessageOperation,
    ) -> Result<Option<ConnectorMessageRecord>, ConnectorAdapterError> {
        let message_url = build_message_url(
            &self.config.api_base_url,
            context.target_channel_id.as_str(),
            message_id,
        )?;
        self.validate_url_target(&context.guard, &message_url)?;
        let route_key =
            format!("discord:get:/channels/{}/messages/{}", context.target_channel_id, message_id);
        let response = self
            .transport
            .get(&message_url, context.credential.token.as_str(), self.config.request_timeout_ms)
            .await
            .map_err(|error| {
                ConnectorAdapterError::Backend(redact_auth_error(error.to_string().as_str()))
            })?;
        let snapshot = parse_rate_limit_snapshot(&response);
        self.apply_rate_limit_snapshot(route_key.as_str(), &snapshot, unix_ms_now())?;
        if response.status == 404 {
            return Ok(None);
        }
        if response.status == 401 || response.status == 403 {
            return Ok(None);
        }
        if !(200..300).contains(&response.status) {
            return Err(ConnectorAdapterError::Backend(format!(
                "discord {} fetch failed (status={}): {}",
                operation_name(operation),
                response.status,
                parse_discord_error_summary(response.body.as_str())
                    .unwrap_or_else(|| "unexpected response".to_owned())
            )));
        }
        let payload = serde_json::from_str::<Value>(response.body.as_str()).map_err(|error| {
            ConnectorAdapterError::Backend(format!(
                "discord message payload is invalid JSON: {error}"
            ))
        })?;
        Ok(parse_discord_message_record(&payload, context.bot_identity.id.as_str(), target))
    }

    /// Fetches one page of channel history, newest first, honoring the before/after/around
    /// cursors and Discord's 1..=100 page-size bounds.
    pub(super) async fn fetch_message_page(
        &self,
        context: &DiscordAdminContext,
        request: DiscordMessagePageRequest<'_>,
    ) -> Result<Vec<ConnectorMessageRecord>, ConnectorAdapterError> {
        let mut url = build_messages_url(&self.config.api_base_url, request.channel_id)?;
        {
            let mut pairs = url.query_pairs_mut();
            pairs.append_pair("limit", request.limit.clamp(1, 100).to_string().as_str());
            if let Some(before) = request.before_message_id {
                pairs.append_pair("before", before);
            }
            if let Some(after) = request.after_message_id {
                pairs.append_pair("after", after);
            }
            if let Some(around) = request.around_message_id {
                pairs.append_pair("around", around);
            }
        }
        self.validate_url_target(&context.guard, &url)?;
        let route_key = format!("discord:get:/channels/{}/messages", request.channel_id);
        let response = self
            .transport
            .get(&url, context.credential.token.as_str(), self.config.request_timeout_ms)
            .await
            .map_err(|error| {
                ConnectorAdapterError::Backend(redact_auth_error(error.to_string().as_str()))
            })?;
        let snapshot = parse_rate_limit_snapshot(&response);
        self.apply_rate_limit_snapshot(route_key.as_str(), &snapshot, unix_ms_now())?;
        if !(200..300).contains(&response.status) {
            return Err(ConnectorAdapterError::Backend(format!(
                "discord {} history request failed (status={}): {}",
                operation_name(request.operation),
                response.status,
                parse_discord_error_summary(response.body.as_str())
                    .unwrap_or_else(|| "unexpected response".to_owned())
            )));
        }
        let payload = serde_json::from_str::<Value>(response.body.as_str()).map_err(|error| {
            ConnectorAdapterError::Backend(format!(
                "discord message history payload is invalid JSON: {error}"
            ))
        })?;
        let entries = payload.as_array().ok_or_else(|| {
            ConnectorAdapterError::Backend(
                "discord message history payload must be a JSON array".to_owned(),
            )
        })?;
        Ok(entries
            .iter()
            .filter_map(|entry| {
                parse_discord_message_record(
                    entry,
                    context.bot_identity.id.as_str(),
                    &ConnectorConversationTarget {
                        conversation_id: request.channel_id.to_owned(),
                        thread_id: None,
                    },
                )
            })
            .collect())
    }

    /// Issues a guarded PATCH and folds the response's rate-limit signals into `route_key`.
    pub(super) async fn patch_discord_json(
        &self,
        context: &DiscordAdminContext,
        route_key: &str,
        url: &Url,
        payload: &Value,
    ) -> Result<DiscordTransportResponse, ConnectorAdapterError> {
        self.validate_url_target(&context.guard, url)?;
        let response = self
            .transport
            .patch_json(
                url,
                context.credential.token.as_str(),
                payload,
                self.config.request_timeout_ms,
            )
            .await
            .map_err(|error| {
                ConnectorAdapterError::Backend(redact_auth_error(error.to_string().as_str()))
            })?;
        let snapshot = parse_rate_limit_snapshot(&response);
        self.apply_rate_limit_snapshot(route_key, &snapshot, unix_ms_now())?;
        Ok(response)
    }

    /// Issues a guarded DELETE; returns `false` for missing/no-access statuses, `true` on
    /// success.
    pub(super) async fn delete_discord_request(
        &self,
        context: &DiscordAdminContext,
        route_key: &str,
        url: &Url,
        operation: DiscordMessageOperation,
        _locator: &ConnectorMessageLocator,
    ) -> Result<bool, ConnectorAdapterError> {
        self.validate_url_target(&context.guard, url)?;
        let response = self
            .transport
            .delete(url, context.credential.token.as_str(), self.config.request_timeout_ms)
            .await
            .map_err(|error| {
                ConnectorAdapterError::Backend(redact_auth_error(error.to_string().as_str()))
            })?;
        let snapshot = parse_rate_limit_snapshot(&response);
        self.apply_rate_limit_snapshot(route_key, &snapshot, unix_ms_now())?;
        if response.status == 404 {
            return Ok(false);
        }
        if response.status == 401 || response.status == 403 {
            return Ok(false);
        }
        if !(200..300).contains(&response.status) && response.status != 204 {
            return Err(ConnectorAdapterError::Backend(format!(
                "discord {} failed (status={}): {}",
                operation_name(operation),
                response.status,
                parse_discord_error_summary(response.body.as_str())
                    .unwrap_or_else(|| "unexpected response".to_owned())
            )));
        }
        Ok(true)
    }

    /// Shared implementation for adding and removing the bot's reaction on a message.
    pub(super) async fn apply_reaction_mutation(
        &self,
        instance: &ConnectorInstanceRecord,
        request: &ConnectorMessageReactionRequest,
        add_reaction: bool,
    ) -> Result<ConnectorMessageMutationResult, ConnectorAdapterError> {
        let operation = if add_reaction {
            DiscordMessageOperation::ReactAdd
        } else {
            DiscordMessageOperation::ReactRemove
        };
        let context =
            match self.prepare_admin_context(instance, operation, &request.locator.target).await? {
                Some(context) if context.preflight_allowed => context,
                Some(context) => {
                    return Ok(denied_mutation_result(
                        operation,
                        request.locator.clone(),
                        context
                            .preflight_reason
                            .unwrap_or_else(|| "discord reaction preflight denied".to_owned()),
                    ));
                }
                None => {
                    return Ok(denied_mutation_result(
                        operation,
                        request.locator.clone(),
                        "discord target channel is unavailable or permissions are insufficient"
                            .to_owned(),
                    ));
                }
            };
        let before = self
            .fetch_single_message(
                &context,
                &request.locator.target,
                request.locator.message_id.as_str(),
                operation,
            )
            .await?;
        let Some(before) = before else {
            return Ok(denied_mutation_result(
                operation,
                request.locator.clone(),
                "discord message is missing or stale".to_owned(),
            ));
        };
        let reaction_url = build_reaction_url(
            &self.config.api_base_url,
            context.target_channel_id.as_str(),
            request.locator.message_id.as_str(),
            request.emoji.as_str(),
        )?;
        let route_key = if add_reaction {
            format!(
                "discord:put:/channels/{}/messages/{}/reactions",
                context.target_channel_id, request.locator.message_id
            )
        } else {
            format!(
                "discord:delete:/channels/{}/messages/{}/reactions",
                context.target_channel_id, request.locator.message_id
            )
        };
        let response = if add_reaction {
            self.transport
                .put(
                    &reaction_url,
                    context.credential.token.as_str(),
                    self.config.request_timeout_ms,
                )
                .await
        } else {
            self.transport
                .delete(
                    &reaction_url,
                    context.credential.token.as_str(),
                    self.config.request_timeout_ms,
                )
                .await
        }
        .map_err(|error| {
            ConnectorAdapterError::Backend(redact_auth_error(error.to_string().as_str()))
        })?;
        let snapshot = parse_rate_limit_snapshot(&response);
        self.apply_rate_limit_snapshot(route_key.as_str(), &snapshot, unix_ms_now())?;
        if response.status == 404 || response.status == 401 || response.status == 403 {
            return Ok(denied_mutation_result(
                operation,
                request.locator.clone(),
                format!(
                    "discord {} was rejected (status={}): {}",
                    operation_name(operation),
                    response.status,
                    parse_discord_error_summary(response.body.as_str())
                        .unwrap_or_else(|| "request rejected".to_owned())
                ),
            ));
        }
        if !(200..300).contains(&response.status) && response.status != 204 {
            return Err(ConnectorAdapterError::Backend(format!(
                "discord {} failed (status={}): {}",
                operation_name(operation),
                response.status,
                parse_discord_error_summary(response.body.as_str())
                    .unwrap_or_else(|| "unexpected response".to_owned())
            )));
        }
        // Re-fetch to reflect the updated reaction set; fall back to the pre-mutation record
        // if the message vanished between the mutation and the re-read.
        let message = self
            .fetch_single_message(
                &context,
                &request.locator.target,
                request.locator.message_id.as_str(),
                operation,
            )
            .await?
            .or(Some(before));
        Ok(ConnectorMessageMutationResult {
            preflight: context.preflight(),
            locator: request.locator.clone(),
            status: if add_reaction {
                ConnectorMessageMutationStatus::ReactionAdded
            } else {
                ConnectorMessageMutationStatus::ReactionRemoved
            },
            reason: None,
            message,
            diff: None,
        })
    }
}
