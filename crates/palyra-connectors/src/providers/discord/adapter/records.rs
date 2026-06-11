//! Conversion of raw Discord message JSON into connector protocol records.
//!
//! Handles both REST responses (message records for read/search/admin flows) and gateway
//! `MESSAGE_CREATE` dispatches (inbound events), tolerating partially populated payloads.

use serde_json::Value;

use crate::protocol::{
    AttachmentKind, AttachmentRef, ConnectorConversationTarget, ConnectorMessageLocator,
    ConnectorMessageReactionRecord, ConnectorMessageRecord, InboundMessageEvent,
};

use super::{effective_channel_id, gateway::deterministic_inbound_envelope_id, unix_ms_now};

/// Parses one Discord message payload into a [`ConnectorMessageRecord`], returning `None`
/// when mandatory identity fields (message id, author id) are missing.
pub(super) fn parse_discord_message_record(
    payload: &Value,
    bot_user_id: &str,
    fallback_target: &ConnectorConversationTarget,
) -> Option<ConnectorMessageRecord> {
    let fallback_channel_id = effective_channel_id(fallback_target);
    let message_id = payload.get("id").and_then(Value::as_str).map(str::trim)?;
    if message_id.is_empty() {
        return None;
    }
    let channel_id = payload
        .get("channel_id")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or(fallback_channel_id.as_str());
    let author = payload.get("author")?;
    let sender_id = author.get("id").and_then(Value::as_str).map(str::trim)?;
    if sender_id.is_empty() {
        return None;
    }
    let thread_id =
        fallback_target.thread_id.clone().or_else(|| parse_discord_thread_id(payload, channel_id));
    let attachments = parse_discord_attachments(payload.get("attachments"));
    let reactions = parse_discord_reactions(payload.get("reactions"));
    let link = build_discord_message_permalink(
        payload
            .get("guild_id")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty()),
        channel_id,
        message_id,
    );
    Some(ConnectorMessageRecord {
        locator: ConnectorMessageLocator {
            target: ConnectorConversationTarget {
                conversation_id: fallback_target.conversation_id.clone(),
                thread_id,
            },
            message_id: message_id.to_owned(),
        },
        sender_id: sender_id.to_owned(),
        sender_display: resolve_discord_sender_display(payload),
        body: payload
            .get("content")
            .and_then(Value::as_str)
            .map(str::trim)
            .unwrap_or_default()
            .to_owned(),
        created_at_unix_ms: parse_discord_snowflake_unix_ms(message_id).unwrap_or_else(unix_ms_now),
        edited_at_unix_ms: None,
        // Discord omits guild_id on direct-message payloads.
        is_direct_message: payload.get("guild_id").and_then(Value::as_str).is_none(),
        is_connector_authored: sender_id.eq_ignore_ascii_case(bot_user_id),
        link,
        attachments,
        reactions,
    })
}

fn parse_discord_reactions(raw: Option<&Value>) -> Vec<ConnectorMessageReactionRecord> {
    let Some(entries) = raw.and_then(Value::as_array) else {
        return Vec::new();
    };
    entries
        .iter()
        .filter_map(|entry| {
            let emoji = entry.get("emoji")?;
            let display = emoji
                .get("name")
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(ToOwned::to_owned)
                .or_else(|| {
                    emoji
                        .get("id")
                        .and_then(Value::as_str)
                        .map(str::trim)
                        .filter(|value| !value.is_empty())
                        .map(ToOwned::to_owned)
                })?;
            Some(ConnectorMessageReactionRecord {
                emoji: display,
                count: entry
                    .get("count")
                    .and_then(Value::as_u64)
                    .and_then(|value| u32::try_from(value).ok())
                    .unwrap_or_default(),
                reacted_by_connector: entry.get("me").and_then(Value::as_bool).unwrap_or(false),
            })
        })
        .collect()
}

fn build_discord_message_permalink(
    guild_id: Option<&str>,
    channel_id: &str,
    message_id: &str,
) -> Option<String> {
    if channel_id.trim().is_empty() || message_id.trim().is_empty() {
        return None;
    }
    Some(format!(
        "https://discord.com/channels/{}/{}/{}",
        guild_id.unwrap_or("@me"),
        channel_id,
        message_id
    ))
}

/// Normalizes a gateway `MESSAGE_CREATE` payload into an [`InboundMessageEvent`].
///
/// Returns `None` for payloads that must not enter the inbound pipeline: missing identity
/// fields, messages authored by the bot itself (prevents echo loops), and messages with
/// neither body text nor attachments.
pub(super) fn normalize_discord_message_create(
    connector_id: &str,
    payload: &Value,
    bot_user_id: Option<&str>,
) -> Option<InboundMessageEvent> {
    let message_id = payload.get("id").and_then(Value::as_str).map(str::trim)?;
    if message_id.is_empty() {
        return None;
    }
    let channel_id = payload.get("channel_id").and_then(Value::as_str).map(str::trim)?;
    if channel_id.is_empty() {
        return None;
    }
    let author = payload.get("author")?;
    let sender_id = author.get("id").and_then(Value::as_str).map(str::trim)?;
    if sender_id.is_empty() {
        return None;
    }
    if bot_user_id.is_some_and(|value| value.eq_ignore_ascii_case(sender_id)) {
        return None;
    }

    let attachments = parse_discord_attachments(payload.get("attachments"));
    let body_text = payload
        .get("content")
        .and_then(Value::as_str)
        .map(str::trim)
        .unwrap_or_default()
        .to_owned();
    if body_text.is_empty() && attachments.is_empty() {
        return None;
    }
    // Attachment-only messages get a placeholder body so downstream routing always sees text.
    let normalized_body = if body_text.is_empty() { "[attachment]".to_owned() } else { body_text };
    let is_direct_message = payload.get("guild_id").and_then(Value::as_str).is_none();
    let thread_id = parse_discord_thread_id(payload, channel_id);
    let received_at_unix_ms =
        parse_discord_snowflake_unix_ms(message_id).unwrap_or_else(unix_ms_now);
    let sender_display = resolve_discord_sender_display(payload);
    Some(InboundMessageEvent {
        envelope_id: deterministic_inbound_envelope_id(connector_id, message_id),
        connector_id: connector_id.to_owned(),
        conversation_id: channel_id.to_owned(),
        thread_id: thread_id.clone(),
        sender_id: sender_id.to_owned(),
        sender_display,
        body: normalized_body,
        adapter_message_id: Some(message_id.to_owned()),
        adapter_thread_id: thread_id,
        received_at_unix_ms,
        is_direct_message,
        requested_broadcast: false,
        attachments,
    })
}

/// Picks the best display name available: guild nickname, then global name, then username.
fn resolve_discord_sender_display(payload: &Value) -> Option<String> {
    payload
        .get("member")
        .and_then(|member| member.get("nick"))
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
        .or_else(|| {
            payload
                .get("author")
                .and_then(|author| author.get("global_name"))
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(ToOwned::to_owned)
        })
        .or_else(|| {
            payload
                .get("author")
                .and_then(|author| author.get("username"))
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(ToOwned::to_owned)
        })
}

/// Infers whether the message lives in a thread.
///
/// Discord does not flag thread messages directly: either the payload embeds a `thread`
/// object, or the message references a parent in a different channel — in both cases the
/// message's own channel id is the thread id.
fn parse_discord_thread_id(payload: &Value, channel_id: &str) -> Option<String> {
    if payload.get("thread").and_then(|value| value.get("id")).and_then(Value::as_str).is_some() {
        return Some(channel_id.to_owned());
    }
    let parent_channel_id = payload
        .get("message_reference")
        .and_then(|value| value.get("channel_id"))
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty());
    if let Some(parent_channel_id) = parent_channel_id {
        if !parent_channel_id.eq_ignore_ascii_case(channel_id) {
            return Some(channel_id.to_owned());
        }
    }
    None
}

/// Converts Discord attachment entries into [`AttachmentRef`]s, skipping entries with no
/// usable metadata at all.
pub(super) fn parse_discord_attachments(raw: Option<&Value>) -> Vec<AttachmentRef> {
    let Some(entries) = raw.and_then(Value::as_array) else {
        return Vec::new();
    };
    entries
        .iter()
        .filter_map(|entry| {
            let url = entry
                .get("url")
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(ToOwned::to_owned);
            let filename = entry
                .get("filename")
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(ToOwned::to_owned);
            let content_type = entry
                .get("content_type")
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(ToOwned::to_owned);
            let size_bytes = entry.get("size").and_then(Value::as_u64);
            if url.is_none() && filename.is_none() && content_type.is_none() && size_bytes.is_none()
            {
                return None;
            }
            let filename_lower = filename.as_deref().map(|value| value.to_ascii_lowercase());
            let content_type_lower =
                content_type.as_deref().map(|value| value.to_ascii_lowercase());
            // SVG is deliberately classified as a plain file, not an image: it can embed
            // scripts, so it must not enter image-rendering paths downstream.
            let is_svg = content_type_lower.as_deref() == Some("image/svg+xml")
                || filename_lower.as_deref().is_some_and(|value| value.ends_with(".svg"));
            let is_image_like = content_type_lower
                .as_deref()
                .map(|value| value.starts_with("image/"))
                .unwrap_or(false)
                || filename_lower.as_deref().is_some_and(|value| {
                    value.ends_with(".png")
                        || value.ends_with(".jpg")
                        || value.ends_with(".jpeg")
                        || value.ends_with(".gif")
                        || value.ends_with(".webp")
                        || value.ends_with(".bmp")
                });
            let kind =
                if is_image_like && !is_svg { AttachmentKind::Image } else { AttachmentKind::File };
            Some(AttachmentRef {
                kind,
                attachment_id: entry
                    .get("id")
                    .and_then(Value::as_str)
                    .map(str::trim)
                    .filter(|value| !value.is_empty())
                    .map(ToOwned::to_owned),
                url,
                artifact_ref: None,
                filename,
                content_type,
                size_bytes,
                origin: Some("discord_inbound".to_owned()),
                ..AttachmentRef::default()
            })
        })
        .collect()
}

/// Extracts the creation time embedded in a Discord snowflake id.
///
/// The top 42 bits of a snowflake are milliseconds since the Discord epoch
/// (2015-01-01T00:00:00Z).
fn parse_discord_snowflake_unix_ms(raw: &str) -> Option<i64> {
    let parsed = raw.trim().parse::<u64>().ok()?;
    let discord_epoch_ms = 1_420_070_400_000_u64;
    let unix_ms = (parsed >> 22).saturating_add(discord_epoch_ms);
    i64::try_from(unix_ms).ok()
}
