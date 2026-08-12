//! Admin channel message read/search and mutation response builders.
//!
//! Message mutations pass through provider previews, policy evaluation, and
//! one-shot approvals before dispatch so high-risk connector edits remain
//! auditable and deny-by-default.

use super::*;
use crate::application::channels::providers::{
    channel_message_policy_action, channel_message_required_permissions,
    classify_channel_message_mutation_governance,
};
use crate::journal::{
    ApprovalCreateRequest, ApprovalDecision, ApprovalDecisionScope, ApprovalPolicySnapshot,
    ApprovalPromptOption, ApprovalPromptRecord, ApprovalRecord, ApprovalSubjectType,
};
use palyra_connectors::ConnectorMessageRecord;

const CHANNEL_MESSAGE_APPROVAL_TIMEOUT_SECONDS: u32 = 15 * 60;
const CHANNEL_MESSAGE_APPROVAL_POLICY_ID: &str = "discord.message.mutation.approval.v1";

/// Reads channel messages and records an admin console audit event.
///
/// # Errors
/// Returns an error response when connector-id normalization, provider access,
/// or audit-event recording fails.
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

/// Searches channel messages and records an admin console audit event.
///
/// # Errors
/// Returns an error response when connector-id normalization, provider search,
/// or audit-event recording fails.
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

/// Edits a channel message after preview, policy, and approval checks.
///
/// # Errors
/// Returns an error response when connector-id normalization, preview lookup,
/// policy evaluation, approval validation, provider mutation, or audit-event
/// recording fails.
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

/// Deletes a channel message after preview, policy, and approval checks.
///
/// # Errors
/// Returns an error response when connector-id normalization, preview lookup,
/// policy evaluation, approval validation, provider mutation, or audit-event
/// recording fails.
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

/// Adds or removes a channel message reaction after authorization checks.
///
/// # Errors
/// Returns an error response when connector-id normalization, preview lookup,
/// policy evaluation, approval validation, provider mutation, or audit-event
/// recording fails.
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
    let governance = Some(classify_channel_message_mutation_governance(
        state,
        input.connector_id,
        preview,
        input.operation,
    )?);
    let policy_action = channel_message_policy_action(input.operation);
    let subject_id =
        build_channel_message_subject_id(input.connector_id, input.operation, input.locator);
    let resource = build_channel_message_resource(input.connector_id, input.locator);
    let mut policy_config = PolicyEvaluationConfig::default();
    if governance.as_ref().is_some_and(|value| value.approval_required) {
        policy_config.sensitive_actions.push(policy_action.to_owned());
    }
    let resolved_approval = if governance.as_ref().is_some_and(|value| value.approval_required) {
        let Some(governance_ref) = governance.as_ref() else {
            return Err(runtime_status_response(tonic::Status::internal(
                "message mutation governance is unavailable",
            )));
        };
        let expected_policy_hash = build_channel_message_approval_policy_hash(
            input.connector_id,
            input.operation,
            policy_action,
            input.locator,
            preview,
            governance_ref,
            &input.mutation_details,
        );
        load_channel_message_approval(
            state,
            input.approval_id,
            subject_id.as_str(),
            context.principal.as_str(),
            expected_policy_hash.as_str(),
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
        PolicyDecision::Allow => {
            if let Some(approval) = resolved_approval.as_ref() {
                consume_channel_message_approval_once(state, approval).await?;
            }
            Ok(ChannelMessageMutationAuthorization {
                approval_id: resolved_approval.as_ref().map(|record| record.approval_id.clone()),
                governance,
                pending_response: None,
            })
        }
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

async fn consume_channel_message_approval_once(
    state: &AppState,
    approval: &ApprovalRecord,
) -> Result<(), Response> {
    let consumed = state
        .runtime
        .consume_approval_once(
            approval.approval_id.clone(),
            "channel_message_mutation_executed".to_owned(),
        )
        .await
        .map_err(runtime_status_response)?;
    if consumed {
        return Ok(());
    }
    Err(runtime_status_response(tonic::Status::permission_denied(
        "message mutation approval has already been used",
    )))
}

async fn load_channel_message_approval(
    state: &AppState,
    approval_id: Option<&str>,
    subject_id: &str,
    principal: &str,
    expected_policy_hash: &str,
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
    if approval.subject_type != ApprovalSubjectType::Tool
        || approval.policy_snapshot.policy_id != CHANNEL_MESSAGE_APPROVAL_POLICY_ID
        || approval.policy_snapshot.policy_hash != expected_policy_hash
    {
        return Err(runtime_status_response(tonic::Status::permission_denied(
            "approval does not match the requested message mutation details",
        )));
    }
    match approval.decision {
        Some(ApprovalDecision::Allow)
            if approval
                .decision_scope
                .is_some_and(|scope| scope != ApprovalDecisionScope::Once) =>
        {
            Err(runtime_status_response(tonic::Status::permission_denied(
                "message mutation approvals must be scoped to a single mutation",
            )))
        }
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
    let details_json = build_channel_message_approval_details_json(
        input.connector_id,
        input.operation,
        policy_action,
        input.locator,
        input.preview,
        input.governance,
        &input.mutation_details,
    );
    let policy_hash = hex::encode(Sha256::digest(details_json.as_bytes()));
    state
        .runtime
        .create_approval_record(ApprovalCreateRequest {
            approval_id: Ulid::generate().to_string(),
            session_id: Ulid::generate().to_string(),
            run_id: Ulid::generate().to_string(),
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
                policy_id: CHANNEL_MESSAGE_APPROVAL_POLICY_ID.to_owned(),
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

fn build_channel_message_approval_policy_hash(
    connector_id: &str,
    operation: channels::DiscordMessageMutationKind,
    policy_action: &str,
    locator: &ConnectorMessageLocator,
    preview: &ConnectorMessageRecord,
    governance: &channels::DiscordMessageMutationGovernance,
    mutation_details: &Value,
) -> String {
    let details_json = build_channel_message_approval_details_json(
        connector_id,
        operation,
        policy_action,
        locator,
        preview,
        governance,
        mutation_details,
    );
    hex::encode(Sha256::digest(details_json.as_bytes()))
}

fn build_channel_message_approval_details_json(
    connector_id: &str,
    operation: channels::DiscordMessageMutationKind,
    policy_action: &str,
    locator: &ConnectorMessageLocator,
    preview: &ConnectorMessageRecord,
    governance: &channels::DiscordMessageMutationGovernance,
    mutation_details: &Value,
) -> String {
    json!({
        "connector_id": connector_id,
        "operation": operation.as_str(),
        "policy_action": policy_action,
        "locator": locator,
        "preview_message": preview,
        "governance": {
            "risk_level": governance.risk_level.as_str(),
            "approval_required": governance.approval_required,
            "reason": governance.reason,
        },
        "mutation": mutation_details,
        "required_permissions": channel_message_required_permissions(operation),
    })
    .to_string()
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::journal::ApprovalRiskLevel;
    use palyra_connectors::{
        ConnectorConversationTarget, ConnectorMessageLocator, ConnectorMessageReactionRecord,
    };

    fn sample_locator() -> ConnectorMessageLocator {
        ConnectorMessageLocator {
            target: ConnectorConversationTarget {
                conversation_id: "guild-channel-123".to_owned(),
                thread_id: Some("thread-1".to_owned()),
            },
            message_id: "message-456".to_owned(),
        }
    }

    fn sample_preview() -> ConnectorMessageRecord {
        ConnectorMessageRecord {
            locator: sample_locator(),
            sender_id: "discord:user:bot".to_owned(),
            sender_display: Some("Palyra".to_owned()),
            body: "original message".to_owned(),
            created_at_unix_ms: 1_700_000_000_000,
            edited_at_unix_ms: None,
            is_direct_message: false,
            is_connector_authored: true,
            link: None,
            attachments: Vec::new(),
            reactions: vec![ConnectorMessageReactionRecord {
                emoji: "wave".to_owned(),
                count: 1,
                reacted_by_connector: false,
            }],
        }
    }

    fn sample_governance() -> channels::DiscordMessageMutationGovernance {
        channels::DiscordMessageMutationGovernance {
            risk_level: ApprovalRiskLevel::High,
            approval_required: true,
            reason: "public Discord message mutation requires approval".to_owned(),
        }
    }

    #[test]
    fn channel_message_approval_policy_hash_changes_when_mutation_changes() {
        let locator = sample_locator();
        let preview = sample_preview();
        let governance = sample_governance();
        let operation = channels::DiscordMessageMutationKind::Edit;
        let policy_action = channel_message_policy_action(operation);

        let benign_hash = build_channel_message_approval_policy_hash(
            "discord:prod",
            operation,
            policy_action,
            &locator,
            &preview,
            &governance,
            &serde_json::json!({
                "body": "benign typo fix",
                "preview_diff": {
                    "before_body": preview.body.as_str(),
                    "after_body": "benign typo fix",
                },
            }),
        );
        let changed_hash = build_channel_message_approval_policy_hash(
            "discord:prod",
            operation,
            policy_action,
            &locator,
            &preview,
            &governance,
            &serde_json::json!({
                "body": "@everyone replacement",
                "preview_diff": {
                    "before_body": preview.body.as_str(),
                    "after_body": "@everyone replacement",
                },
            }),
        );

        assert_ne!(
            benign_hash, changed_hash,
            "approvals must be bound to the exact Discord mutation details"
        );
    }

    #[test]
    fn channel_message_approval_details_include_mutation_and_preview() {
        let locator = sample_locator();
        let preview = sample_preview();
        let governance = sample_governance();
        let operation = channels::DiscordMessageMutationKind::ReactAdd;
        let details_json = build_channel_message_approval_details_json(
            "discord:prod",
            operation,
            channel_message_policy_action(operation),
            &locator,
            &preview,
            &governance,
            &serde_json::json!({
                "emoji": "wave",
                "existing_reactions": preview.reactions.clone(),
            }),
        );
        let details = serde_json::from_str::<Value>(details_json.as_str())
            .expect("approval details should be valid JSON");

        assert_eq!(details["connector_id"], "discord:prod");
        assert_eq!(details["operation"], "react_add");
        assert_eq!(details["preview_message"]["body"], "original message");
        assert_eq!(details["mutation"]["emoji"], "wave");
    }
}
