//! `palyra message`: connector-bridged message actions (send, read, search,
//! edit, delete, react) plus per-connector capability discovery.
//! Mutations may return an `approval_required` payload instead of a final
//! result; text output compacts message bodies into one-line key=value form.

use anyhow::{bail, Context, Result};
use serde_json::Value;
use std::io::Write;

use crate::{
    app,
    args::MessageCommand,
    build_runtime,
    client::{message, operator::OperatorRuntime},
    normalize_optional_text_arg, normalize_required_text_arg, output,
};

/// Runs a `palyra message` subcommand on a dedicated Tokio runtime.
///
/// `message status` is answered locally; every other subcommand requires a
/// resolvable gateway gRPC connection.
///
/// # Errors
/// Returns an error when required arguments are empty, the gateway connection
/// cannot be resolved, or the connector dispatch fails.
pub(crate) fn run_message(command: MessageCommand) -> Result<()> {
    if let MessageCommand::Status { json } = &command {
        emit_status(output::preferred_json(*json))?;
        std::io::stdout().flush().context("stdout flush failed")?;
        return Ok(());
    }

    let root_context = app::current_root_context()
        .ok_or_else(|| anyhow::anyhow!("CLI root context is unavailable for message command"))?;
    let connection = root_context.resolve_grpc_connection(
        app::ConnectionOverrides::default(),
        app::ConnectionDefaults::USER,
    )?;
    let runtime = build_runtime()?;
    runtime.block_on(run_message_async(command, OperatorRuntime::new(connection)))
}

async fn run_message_async(command: MessageCommand, runtime: OperatorRuntime) -> Result<()> {
    match command {
        MessageCommand::Status { .. } => {
            unreachable!("message status is handled before the gRPC runtime is built")
        }
        MessageCommand::Capabilities {
            connector_id,
            url,
            token,
            principal,
            device_id,
            channel,
            json,
        } => {
            let capabilities = runtime
                .message_capabilities(
                    connector_id.clone(),
                    url,
                    token,
                    principal,
                    device_id,
                    channel,
                )
                .await?;
            emit_capabilities(connector_id.as_str(), &capabilities, output::preferred_json(json))?;
        }
        MessageCommand::Send {
            connector_id,
            to,
            text,
            confirm,
            auto_reaction,
            thread_id,
            reply_to_message_id,
            url,
            token,
            principal,
            device_id,
            channel,
            json,
        } => {
            let text = normalize_required_text_arg(text, "text")?;
            let target = normalize_required_text_arg(to, "to")?;
            let response = runtime
                .send_message(message::MessageDispatchOptions {
                    connector_id: connector_id.clone(),
                    target,
                    text,
                    confirm,
                    auto_reaction: auto_reaction.and_then(normalize_optional_text_arg),
                    thread_id: thread_id.and_then(normalize_optional_text_arg),
                    reply_to_message_id: reply_to_message_id.and_then(normalize_optional_text_arg),
                    url,
                    token,
                    principal,
                    device_id,
                    channel,
                })
                .await?;
            emit_send("send", connector_id.as_str(), response, output::preferred_json(json))?;
        }
        MessageCommand::Thread {
            connector_id,
            to,
            text,
            thread_id,
            confirm,
            auto_reaction,
            reply_to_message_id,
            url,
            token,
            principal,
            device_id,
            channel,
            json,
        } => {
            let text = normalize_required_text_arg(text, "text")?;
            let target = normalize_required_text_arg(to, "to")?;
            let thread_id = normalize_required_text_arg(thread_id, "thread_id")?;
            let response = runtime
                .send_message(message::MessageDispatchOptions {
                    connector_id: connector_id.clone(),
                    target,
                    text,
                    confirm,
                    auto_reaction: auto_reaction.and_then(normalize_optional_text_arg),
                    thread_id: Some(thread_id),
                    reply_to_message_id: reply_to_message_id.and_then(normalize_optional_text_arg),
                    url,
                    token,
                    principal,
                    device_id,
                    channel,
                })
                .await?;
            emit_send("thread", connector_id.as_str(), response, output::preferred_json(json))?;
        }
        MessageCommand::Read {
            connector_id,
            conversation_id,
            thread_id,
            message_id,
            before_message_id,
            after_message_id,
            around_message_id,
            limit,
            url,
            token,
            principal,
            device_id,
            channel,
            json,
        } => {
            ensure_positive_limit("read", limit)?;
            let conversation_id = normalize_required_text_arg(conversation_id, "conversation_id")?;
            let response = runtime
                .read_messages(message::MessageReadOptions {
                    connector_id: connector_id.clone(),
                    conversation_id,
                    thread_id: thread_id.and_then(normalize_optional_text_arg),
                    message_id: message_id.and_then(normalize_optional_text_arg),
                    before_message_id: before_message_id.and_then(normalize_optional_text_arg),
                    after_message_id: after_message_id.and_then(normalize_optional_text_arg),
                    around_message_id: around_message_id.and_then(normalize_optional_text_arg),
                    limit,
                    url,
                    token,
                    principal,
                    device_id,
                    channel,
                })
                .await?;
            emit_read(connector_id.as_str(), &response, output::preferred_json(json))?;
        }
        MessageCommand::Search {
            connector_id,
            conversation_id,
            thread_id,
            query,
            author_id,
            has_attachments,
            before_message_id,
            limit,
            url,
            token,
            principal,
            device_id,
            channel,
            json,
        } => {
            ensure_positive_limit("search", limit)?;
            let conversation_id = normalize_required_text_arg(conversation_id, "conversation_id")?;
            let response = runtime
                .search_messages(message::MessageSearchOptions {
                    connector_id: connector_id.clone(),
                    conversation_id,
                    thread_id: thread_id.and_then(normalize_optional_text_arg),
                    query: query.and_then(normalize_optional_text_arg),
                    author_id: author_id.and_then(normalize_optional_text_arg),
                    has_attachments,
                    before_message_id: before_message_id.and_then(normalize_optional_text_arg),
                    limit,
                    url,
                    token,
                    principal,
                    device_id,
                    channel,
                })
                .await?;
            emit_search(connector_id.as_str(), &response, output::preferred_json(json))?;
        }
        MessageCommand::Edit {
            connector_id,
            conversation_id,
            thread_id,
            message_id,
            text,
            approval_id,
            url,
            token,
            principal,
            device_id,
            channel,
            json,
        } => {
            let conversation_id = normalize_required_text_arg(conversation_id, "conversation_id")?;
            let message_id = normalize_required_text_arg(message_id, "message_id")?;
            let body = normalize_required_text_arg(text, "text")?;
            let response = runtime
                .edit_message(message::MessageEditOptions {
                    connector_id: connector_id.clone(),
                    conversation_id,
                    thread_id: thread_id.and_then(normalize_optional_text_arg),
                    message_id,
                    body,
                    approval_id: approval_id.and_then(normalize_optional_text_arg),
                    url,
                    token,
                    principal,
                    device_id,
                    channel,
                })
                .await?;
            emit_mutation("edit", connector_id.as_str(), &response, output::preferred_json(json))?;
        }
        MessageCommand::Delete {
            connector_id,
            conversation_id,
            thread_id,
            message_id,
            reason,
            approval_id,
            url,
            token,
            principal,
            device_id,
            channel,
            json,
        } => {
            let conversation_id = normalize_required_text_arg(conversation_id, "conversation_id")?;
            let message_id = normalize_required_text_arg(message_id, "message_id")?;
            let response = runtime
                .delete_message(message::MessageDeleteOptions {
                    connector_id: connector_id.clone(),
                    conversation_id,
                    thread_id: thread_id.and_then(normalize_optional_text_arg),
                    message_id,
                    reason: reason.and_then(normalize_optional_text_arg),
                    approval_id: approval_id.and_then(normalize_optional_text_arg),
                    url,
                    token,
                    principal,
                    device_id,
                    channel,
                })
                .await?;
            emit_mutation(
                "delete",
                connector_id.as_str(),
                &response,
                output::preferred_json(json),
            )?;
        }
        MessageCommand::React {
            connector_id,
            conversation_id,
            thread_id,
            message_id,
            emoji,
            remove,
            approval_id,
            url,
            token,
            principal,
            device_id,
            channel,
            json,
        } => {
            let conversation_id = normalize_required_text_arg(conversation_id, "conversation_id")?;
            let message_id = normalize_required_text_arg(message_id, "message_id")?;
            let emoji = normalize_required_text_arg(emoji, "emoji")?;
            let response = if remove {
                runtime
                    .remove_reaction(message::MessageReactionOptions {
                        connector_id: connector_id.clone(),
                        conversation_id,
                        thread_id: thread_id.and_then(normalize_optional_text_arg),
                        message_id,
                        emoji,
                        approval_id: approval_id.and_then(normalize_optional_text_arg),
                        url,
                        token,
                        principal,
                        device_id,
                        channel,
                    })
                    .await?
            } else {
                runtime
                    .add_reaction(message::MessageReactionOptions {
                        connector_id: connector_id.clone(),
                        conversation_id,
                        thread_id: thread_id.and_then(normalize_optional_text_arg),
                        message_id,
                        emoji,
                        approval_id: approval_id.and_then(normalize_optional_text_arg),
                        url,
                        token,
                        principal,
                        device_id,
                        channel,
                    })
                    .await?
            };
            emit_mutation(
                if remove { "react-remove" } else { "react-add" },
                connector_id.as_str(),
                &response,
                output::preferred_json(json),
            )?;
        }
    }

    std::io::stdout().flush().context("stdout flush failed")
}

fn emit_status(json_output: bool) -> Result<()> {
    if json_output {
        output::print_json_pretty(
            &message_status_payload(),
            "failed to encode message status guidance as JSON",
        )?;
    } else {
        output::print_text_line(message_status_text().as_str())?;
    }
    Ok(())
}

fn message_status_payload() -> Value {
    serde_json::json!({
        "surface": "message",
        "status": "connector_bridge",
        "canonical_status_command": "palyra channels status",
        "list_command": "palyra channels list",
        "capabilities_command": "palyra message capabilities <connector-id>"
    })
}

fn message_status_text() -> String {
    "message.status surface=connector_bridge status_command=\"palyra channels status\" list_command=\"palyra channels list\" capabilities_command=\"palyra message capabilities <connector-id>\"".to_owned()
}

fn emit_capabilities(
    connector_id: &str,
    capabilities: &message::MessageCapabilities,
    json_output: bool,
) -> Result<()> {
    let payload = message::encode_capabilities_json(connector_id, capabilities);
    if json_output {
        output::print_json_pretty(
            &payload,
            "failed to encode message capabilities payload as JSON",
        )?;
    } else {
        println!(
            "message.capabilities connector_id={} provider_kind={} supported={} unsupported={}",
            connector_id,
            capabilities.provider_kind,
            if capabilities.supported_actions.is_empty() {
                "none".to_owned()
            } else {
                capabilities.supported_actions.join(",")
            },
            comma_list(capabilities.unsupported_actions.as_slice())
        );
        for detail in &capabilities.action_details {
            println!(
                "message.capability action={} supported={} policy_action={} approval_mode={} risk_level={} audit_event_type={} required_permissions={} reason=\"{}\"",
                detail.action,
                detail.supported,
                detail.policy_action.as_deref().unwrap_or("none"),
                detail.approval_mode.as_deref().unwrap_or("none"),
                detail.risk_level.as_deref().unwrap_or("none"),
                detail.audit_event_type.as_deref().unwrap_or("none"),
                comma_list(detail.required_permissions.as_slice()),
                detail.reason.as_deref().map(compact_text).unwrap_or_else(|| "none".to_owned()),
            );
        }
    }
    Ok(())
}

fn emit_send(action: &str, connector_id: &str, response: Value, json_output: bool) -> Result<()> {
    if json_output {
        return output::print_json_pretty(
            &message::encode_dispatch_json(action, connector_id, response),
            "failed to encode message send payload as JSON",
        );
    }

    let dispatch = response.get("dispatch").unwrap_or(&response);
    println!(
        "message.{} connector_id={} target={} delivered={} retried={} dead_lettered={} native_message_id={} thread_id={} reply_to={}",
        action,
        connector_id,
        dispatch.get("target").and_then(Value::as_str).unwrap_or("unknown"),
        dispatch.get("delivered").and_then(Value::as_u64).unwrap_or(0),
        dispatch.get("retried").and_then(Value::as_u64).unwrap_or(0),
        dispatch.get("dead_lettered").and_then(Value::as_u64).unwrap_or(0),
        dispatch
            .get("native_message_id")
            .and_then(Value::as_str)
            .unwrap_or("none"),
        dispatch.get("thread_id").and_then(Value::as_str).unwrap_or("none"),
        dispatch
            .get("in_reply_to_message_id")
            .and_then(Value::as_str)
            .unwrap_or("none")
    );
    Ok(())
}

fn emit_read(connector_id: &str, response: &Value, json_output: bool) -> Result<()> {
    if json_output {
        return output::print_json_pretty(
            response,
            "failed to encode message read payload as JSON",
        );
    }

    let result = response.pointer("/result").unwrap_or(response);
    let messages =
        result.get("messages").and_then(Value::as_array).map(Vec::as_slice).unwrap_or(&[]);
    println!(
        "message.read connector_id={} conversation_id={} thread_id={} exact_message_id={} count={} next_before={} next_after={} allowed={} policy_action={} approval_mode={} risk_level={} required_permissions={}",
        connector_id,
        result
            .get("conversation_id")
            .and_then(Value::as_str)
            .unwrap_or("unknown"),
        result.get("thread_id").and_then(Value::as_str).unwrap_or("none"),
        result
            .get("exact_message_id")
            .and_then(Value::as_str)
            .unwrap_or("none"),
        messages.len(),
        result
            .get("next_before_message_id")
            .and_then(Value::as_str)
            .unwrap_or("none"),
        result
            .get("next_after_message_id")
            .and_then(Value::as_str)
            .unwrap_or("none"),
        preflight_bool(result, "allowed"),
        preflight_text(result, "policy_action"),
        preflight_text(result, "approval_mode"),
        preflight_text(result, "risk_level"),
        preflight_csv(result, "required_permissions"),
    );
    for message in messages {
        emit_message_record("message.read.record", message);
    }
    Ok(())
}

fn emit_search(connector_id: &str, response: &Value, json_output: bool) -> Result<()> {
    if json_output {
        return output::print_json_pretty(
            response,
            "failed to encode message search payload as JSON",
        );
    }

    let result = response.pointer("/result").unwrap_or(response);
    let matches = result.get("matches").and_then(Value::as_array).map(Vec::as_slice).unwrap_or(&[]);
    println!(
        "message.search connector_id={} conversation_id={} thread_id={} query=\"{}\" author_id={} has_attachments={} count={} next_before={} allowed={} policy_action={} approval_mode={} risk_level={} required_permissions={}",
        connector_id,
        result
            .get("conversation_id")
            .and_then(Value::as_str)
            .unwrap_or("unknown"),
        result.get("thread_id").and_then(Value::as_str).unwrap_or("none"),
        result.get("query").and_then(Value::as_str).map(compact_text).unwrap_or_else(|| "none".to_owned()),
        result.get("author_id").and_then(Value::as_str).unwrap_or("none"),
        result
            .get("has_attachments")
            .map(bool_like)
            .unwrap_or("none".to_owned()),
        matches.len(),
        result
            .get("next_before_message_id")
            .and_then(Value::as_str)
            .unwrap_or("none"),
        preflight_bool(result, "allowed"),
        preflight_text(result, "policy_action"),
        preflight_text(result, "approval_mode"),
        preflight_text(result, "risk_level"),
        preflight_csv(result, "required_permissions"),
    );
    for message in matches {
        emit_message_record("message.search.match", message);
    }
    Ok(())
}

fn emit_mutation(
    action: &str,
    connector_id: &str,
    response: &Value,
    json_output: bool,
) -> Result<()> {
    if json_output {
        return output::print_json_pretty(
            response,
            "failed to encode message mutation payload as JSON",
        );
    }

    if response.get("approval_required").and_then(Value::as_bool).unwrap_or(false) {
        let approval = response.get("approval").unwrap_or(&Value::Null);
        let policy = response.get("policy").unwrap_or(&Value::Null);
        let preview = response.get("preview").unwrap_or(&Value::Null);
        println!(
            "message.{} connector_id={} approval_required=true approval_id={} policy_action={} policy_reason=\"{}\" policy_explanation=\"{}\" preview_message_id={}",
            action,
            connector_id,
            approval.get("approval_id").and_then(Value::as_str).unwrap_or("unknown"),
            policy.get("action").and_then(Value::as_str).unwrap_or("unknown"),
            policy
                .get("reason")
                .and_then(Value::as_str)
                .map(compact_text)
                .unwrap_or_else(|| "none".to_owned()),
            policy
                .get("explanation")
                .and_then(Value::as_str)
                .map(compact_text)
                .unwrap_or_else(|| "none".to_owned()),
            preview
                .pointer("/locator/message_id")
                .and_then(Value::as_str)
                .unwrap_or("unknown"),
        );
        if let Some(message) = preview.get("message") {
            emit_message_record("message.approval.preview", message);
        }
        return Ok(());
    }

    let result = response.pointer("/result").unwrap_or(response);
    println!(
        "message.{} connector_id={} conversation_id={} thread_id={} message_id={} status={} allowed={} policy_action={} approval_mode={} risk_level={} reason=\"{}\"",
        action,
        connector_id,
        result
            .pointer("/locator/conversation_id")
            .and_then(Value::as_str)
            .unwrap_or("unknown"),
        result
            .pointer("/locator/thread_id")
            .and_then(Value::as_str)
            .unwrap_or("none"),
        result
            .pointer("/locator/message_id")
            .and_then(Value::as_str)
            .unwrap_or("unknown"),
        result.get("status").and_then(Value::as_str).unwrap_or("unknown"),
        preflight_bool(result, "allowed"),
        preflight_text(result, "policy_action"),
        preflight_text(result, "approval_mode"),
        preflight_text(result, "risk_level"),
        result
            .get("reason")
            .and_then(Value::as_str)
            .map(compact_text)
            .unwrap_or_else(|| "none".to_owned()),
    );
    if let Some(diff) = result.get("diff") {
        println!(
            "message.{}.diff before=\"{}\" after=\"{}\"",
            action,
            diff.get("before_body")
                .and_then(Value::as_str)
                .map(compact_text)
                .unwrap_or_else(|| "none".to_owned()),
            diff.get("after_body")
                .and_then(Value::as_str)
                .map(compact_text)
                .unwrap_or_else(|| "none".to_owned()),
        );
    }
    if let Some(message) = result.get("message") {
        emit_message_record("message.mutation.message", message);
    }
    Ok(())
}

fn emit_message_record(prefix: &str, message: &Value) {
    println!(
        "{} conversation_id={} thread_id={} message_id={} sender_id={} sender_display={} direct={} connector_authored={} created_at={} edited_at={} attachments={} reactions={} link={} body=\"{}\"",
        prefix,
        message
            .pointer("/locator/conversation_id")
            .and_then(Value::as_str)
            .unwrap_or("unknown"),
        message
            .pointer("/locator/thread_id")
            .and_then(Value::as_str)
            .unwrap_or("none"),
        message
            .pointer("/locator/message_id")
            .and_then(Value::as_str)
            .unwrap_or("unknown"),
        message.get("sender_id").and_then(Value::as_str).unwrap_or("unknown"),
        message
            .get("sender_display")
            .and_then(Value::as_str)
            .unwrap_or("none"),
        bool_like(message.get("is_direct_message").unwrap_or(&Value::Null)),
        bool_like(message.get("is_connector_authored").unwrap_or(&Value::Null)),
        scalar_like(message.get("created_at_unix_ms").unwrap_or(&Value::Null)),
        scalar_like(message.get("edited_at_unix_ms").unwrap_or(&Value::Null)),
        message
            .get("attachments")
            .and_then(Value::as_array)
            .map(|value| value.len())
            .unwrap_or(0),
        reaction_summary(message.get("reactions")),
        message.get("link").and_then(Value::as_str).unwrap_or("none"),
        message
            .get("body")
            .and_then(Value::as_str)
            .map(compact_text)
            .unwrap_or_else(|| "none".to_owned()),
    );
}

fn ensure_positive_limit(action: &str, limit: usize) -> Result<()> {
    if limit == 0 {
        bail!("message {action} requires --limit greater than zero");
    }
    Ok(())
}

// Collapses whitespace and swaps double quotes for single quotes so message
// bodies stay parseable inside one-line key="value" text output; long bodies
// are capped at 160 chars.
fn compact_text(value: &str) -> String {
    let collapsed = value.split_whitespace().collect::<Vec<_>>().join(" ");
    let sanitized = collapsed.replace('"', "'");
    if sanitized.chars().count() > 160 {
        let truncated = sanitized.chars().take(157).collect::<String>();
        format!("{truncated}...")
    } else {
        sanitized
    }
}

fn comma_list(values: &[String]) -> String {
    if values.is_empty() {
        "none".to_owned()
    } else {
        values.join(",")
    }
}

fn bool_like(value: &Value) -> String {
    match value {
        Value::Bool(flag) => flag.to_string(),
        Value::Null => "none".to_owned(),
        _ => scalar_like(value),
    }
}

fn scalar_like(value: &Value) -> String {
    match value {
        Value::String(text) => text.clone(),
        Value::Number(number) => number.to_string(),
        Value::Bool(flag) => flag.to_string(),
        Value::Null => "none".to_owned(),
        other => compact_text(other.to_string().as_str()),
    }
}

fn preflight_text(result: &Value, field: &str) -> String {
    result
        .get("preflight")
        .and_then(|preflight| preflight.get(field))
        .map(scalar_like)
        .unwrap_or_else(|| "none".to_owned())
}

fn preflight_bool(result: &Value, field: &str) -> String {
    result
        .get("preflight")
        .and_then(|preflight| preflight.get(field))
        .map(bool_like)
        .unwrap_or_else(|| "none".to_owned())
}

fn preflight_csv(result: &Value, field: &str) -> String {
    result
        .get("preflight")
        .and_then(|preflight| preflight.get(field))
        .and_then(Value::as_array)
        .map(|entries| {
            let values =
                entries.iter().filter_map(Value::as_str).map(ToOwned::to_owned).collect::<Vec<_>>();
            comma_list(values.as_slice())
        })
        .unwrap_or_else(|| "none".to_owned())
}

fn reaction_summary(value: Option<&Value>) -> String {
    let Some(reactions) = value.and_then(Value::as_array) else {
        return "none".to_owned();
    };
    if reactions.is_empty() {
        return "none".to_owned();
    }
    reactions
        .iter()
        .map(|reaction| {
            format!(
                "{}:{}:{}",
                reaction.get("emoji").and_then(Value::as_str).unwrap_or("?"),
                reaction.get("count").and_then(Value::as_u64).unwrap_or(0),
                reaction.get("reacted_by_connector").and_then(Value::as_bool).unwrap_or(false)
            )
        })
        .collect::<Vec<_>>()
        .join(",")
}
