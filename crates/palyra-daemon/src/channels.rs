use std::{
    path::PathBuf,
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use palyra_common::{validate_canonical_id, CANONICAL_PROTOCOL_MAJOR};
use palyra_connectors::{
    connectors::default_adapters, AttachmentKind, AttachmentRef, ConnectorInstanceSpec,
    ConnectorKind, ConnectorRouter, ConnectorRouterError, ConnectorStatusSnapshot,
    ConnectorSupervisor, ConnectorSupervisorConfig, ConnectorSupervisorError, InboundIngestOutcome,
    InboundMessageEvent, OutboundA2uiUpdate as ConnectorA2uiUpdate, OutboundAttachment,
    OutboundMessageRequest, RouteInboundResult, RoutedOutboundMessage,
};
use serde_json::Value;
use thiserror::Error;
use tokio::time::{interval, MissedTickBehavior};
use tonic::metadata::MetadataValue;
use tracing::warn;
use ulid::Ulid;

use crate::gateway::{
    proto::palyra::{common::v1 as common_v1, gateway::v1 as gateway_v1},
    GatewayAuthConfig, HEADER_CHANNEL, HEADER_DEVICE_ID, HEADER_PRINCIPAL,
};

mod discord;

pub use discord::{
    discord_connector_id, discord_default_egress_allowlist, discord_principal,
    discord_token_vault_ref, normalize_discord_account_id,
};

const CHANNEL_WORKER_DEVICE_ID: &str = "01ARZ3NDEKTSV4RRFFQ69G5FAV";
const DEFAULT_CHANNEL_WORKER_INTERVAL_MS: u64 = 1_000;
const DEFAULT_LOG_PAGE_LIMIT: usize = 100;

#[derive(Debug, Error)]
pub enum ChannelPlatformError {
    #[error(transparent)]
    Supervisor(#[from] ConnectorSupervisorError),
    #[error(transparent)]
    Store(#[from] palyra_connectors::ConnectorStoreError),
    #[error("invalid test message input: {0}")]
    InvalidInput(String),
}

#[derive(Debug, Clone)]
pub struct ChannelTestMessageRequest {
    pub text: String,
    pub conversation_id: String,
    pub sender_id: String,
    pub sender_display: Option<String>,
    pub simulate_crash_once: bool,
    pub is_direct_message: bool,
    pub requested_broadcast: bool,
}

#[derive(Debug, Clone)]
pub struct ChannelDiscordTestSendRequest {
    pub target: String,
    pub text: String,
    pub confirm: bool,
    pub auto_reaction: Option<String>,
    pub thread_id: Option<String>,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct ChannelDiscordTestSendOutcome {
    pub envelope_id: String,
    pub connector_id: String,
    pub target: String,
    pub enqueued: bool,
    pub delivered: usize,
    pub retried: usize,
    pub dead_lettered: usize,
}

pub struct ChannelPlatform {
    supervisor: Arc<ConnectorSupervisor>,
    worker_interval: Duration,
}

impl ChannelPlatform {
    pub fn initialize(
        grpc_url: String,
        auth: GatewayAuthConfig,
        db_path: PathBuf,
    ) -> Result<Self, ChannelPlatformError> {
        let store = Arc::new(palyra_connectors::ConnectorStore::open(db_path)?);
        let router = Arc::new(GrpcChannelRouter { grpc_url, auth });
        let supervisor = Arc::new(ConnectorSupervisor::new(
            Arc::clone(&store),
            router,
            default_adapters(),
            ConnectorSupervisorConfig::default(),
        ));
        let platform = Self {
            supervisor,
            worker_interval: Duration::from_millis(DEFAULT_CHANNEL_WORKER_INTERVAL_MS),
        };
        platform.ensure_default_connector_inventory()?;
        Ok(platform)
    }

    pub fn list(&self) -> Result<Vec<ConnectorStatusSnapshot>, ChannelPlatformError> {
        self.supervisor.list_status().map_err(ChannelPlatformError::from)
    }

    pub fn status(
        &self,
        connector_id: &str,
    ) -> Result<ConnectorStatusSnapshot, ChannelPlatformError> {
        self.supervisor.status(connector_id).map_err(ChannelPlatformError::from)
    }

    pub fn ensure_discord_connector(
        &self,
        account_id: &str,
    ) -> Result<ConnectorStatusSnapshot, ChannelPlatformError> {
        let normalized_account_id = normalize_discord_account_id(account_id)?;
        let connector_id = discord_connector_id(normalized_account_id.as_str());
        if let Ok(status) = self.supervisor.status(connector_id.as_str()) {
            if status.kind != ConnectorKind::Discord {
                return Err(ChannelPlatformError::InvalidInput(format!(
                    "connector '{}' is not a Discord connector (kind={})",
                    connector_id,
                    status.kind.as_str()
                )));
            }
            return Ok(status);
        }
        let spec = discord::discord_connector_spec(normalized_account_id.as_str(), false);
        self.supervisor.register_connector(&spec)?;
        self.supervisor.status(spec.connector_id.as_str()).map_err(ChannelPlatformError::from)
    }

    pub fn runtime_snapshot(
        &self,
        connector_id: &str,
    ) -> Result<Option<Value>, ChannelPlatformError> {
        self.supervisor.runtime_snapshot(connector_id).map_err(ChannelPlatformError::from)
    }

    pub fn set_enabled(
        &self,
        connector_id: &str,
        enabled: bool,
    ) -> Result<ConnectorStatusSnapshot, ChannelPlatformError> {
        self.supervisor.set_enabled(connector_id, enabled).map_err(ChannelPlatformError::from)
    }

    pub fn logs(
        &self,
        connector_id: &str,
        limit: Option<usize>,
    ) -> Result<Vec<palyra_connectors::ConnectorEventRecord>, ChannelPlatformError> {
        self.supervisor
            .list_logs(connector_id, limit.unwrap_or(DEFAULT_LOG_PAGE_LIMIT))
            .map_err(ChannelPlatformError::from)
    }

    pub fn dead_letters(
        &self,
        connector_id: &str,
        limit: Option<usize>,
    ) -> Result<Vec<palyra_connectors::DeadLetterRecord>, ChannelPlatformError> {
        self.supervisor
            .list_dead_letters(connector_id, limit.unwrap_or(DEFAULT_LOG_PAGE_LIMIT))
            .map_err(ChannelPlatformError::from)
    }

    pub async fn submit_test_message(
        &self,
        connector_id: &str,
        request: ChannelTestMessageRequest,
    ) -> Result<InboundIngestOutcome, ChannelPlatformError> {
        if request.text.trim().is_empty() {
            return Err(ChannelPlatformError::InvalidInput("text cannot be empty".to_owned()));
        }
        if request.conversation_id.trim().is_empty() {
            return Err(ChannelPlatformError::InvalidInput(
                "conversation_id cannot be empty".to_owned(),
            ));
        }
        if request.sender_id.trim().is_empty() {
            return Err(ChannelPlatformError::InvalidInput("sender_id cannot be empty".to_owned()));
        }

        let mut body = request.text;
        if request.simulate_crash_once {
            body.push_str(" [connector-crash-once]");
        }
        let event = InboundMessageEvent {
            envelope_id: Ulid::new().to_string(),
            connector_id: connector_id.trim().to_owned(),
            conversation_id: request.conversation_id.trim().to_owned(),
            thread_id: None,
            sender_id: request.sender_id.trim().to_owned(),
            sender_display: request.sender_display,
            body,
            adapter_message_id: Some(Ulid::new().to_string()),
            adapter_thread_id: None,
            received_at_unix_ms: unix_ms_now(),
            is_direct_message: request.is_direct_message,
            requested_broadcast: request.requested_broadcast,
            attachments: Vec::new(),
        };
        self.supervisor.ingest_inbound(event).await.map_err(ChannelPlatformError::from)
    }

    pub async fn submit_discord_test_send(
        &self,
        connector_id: &str,
        request: ChannelDiscordTestSendRequest,
    ) -> Result<ChannelDiscordTestSendOutcome, ChannelPlatformError> {
        let connector_id = connector_id.trim();
        if connector_id.is_empty() {
            return Err(ChannelPlatformError::InvalidInput(
                "connector_id cannot be empty".to_owned(),
            ));
        }
        if !request.confirm {
            return Err(ChannelPlatformError::InvalidInput(
                "discord test send requires explicit confirmation".to_owned(),
            ));
        }
        let status = self.status(connector_id)?;
        if status.kind != ConnectorKind::Discord {
            return Err(ChannelPlatformError::InvalidInput(format!(
                "discord test send is only supported for discord connectors (received kind={})",
                status.kind.as_str()
            )));
        }

        let text = request.text.trim();
        if text.is_empty() {
            return Err(ChannelPlatformError::InvalidInput(
                "test-send text cannot be empty".to_owned(),
            ));
        }
        let target = discord::normalize_discord_target(request.target.as_str())?;
        let thread_id = request
            .thread_id
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(ToOwned::to_owned);
        let auto_reaction = request
            .auto_reaction
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(ToOwned::to_owned);

        let outbound = OutboundMessageRequest {
            envelope_id: Ulid::new().to_string(),
            connector_id: connector_id.to_owned(),
            conversation_id: target.clone(),
            reply_thread_id: thread_id,
            in_reply_to_message_id: None,
            text: text.to_owned(),
            broadcast: false,
            auto_ack_text: None,
            auto_reaction,
            attachments: Vec::new(),
            structured_json: None,
            a2ui_update: None,
            timeout_ms: 30_000,
            max_payload_bytes: self.supervisor_config().max_outbound_body_bytes,
        };
        let enqueue = self.supervisor.enqueue_outbound(&outbound)?;
        let drain = self
            .supervisor
            .drain_due_outbox_for_connector(
                connector_id,
                self.supervisor_config().immediate_drain_batch_size,
            )
            .await?;
        Ok(ChannelDiscordTestSendOutcome {
            envelope_id: outbound.envelope_id,
            connector_id: connector_id.to_owned(),
            target,
            enqueued: enqueue.created,
            delivered: drain.delivered,
            retried: drain.retried,
            dead_lettered: drain.dead_lettered,
        })
    }

    pub async fn drain_due(&self) -> Result<palyra_connectors::DrainOutcome, ChannelPlatformError> {
        self.supervisor
            .drain_due_outbox(self.supervisor_config().background_drain_batch_size)
            .await
            .map_err(ChannelPlatformError::from)
    }

    pub async fn poll_inbound(&self) -> Result<usize, ChannelPlatformError> {
        self.supervisor
            .poll_inbound(self.supervisor_config().immediate_drain_batch_size)
            .await
            .map_err(ChannelPlatformError::from)
    }

    #[must_use]
    pub fn worker_interval(&self) -> Duration {
        self.worker_interval
    }

    pub fn spawn_worker(self: Arc<Self>) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let mut ticker = interval(self.worker_interval());
            ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
            loop {
                ticker.tick().await;
                if let Err(error) = self.poll_inbound().await {
                    warn!(error = %error, "channel connector worker inbound poll failed");
                }
                if let Err(error) = self.drain_due().await {
                    warn!(error = %error, "channel connector worker drain failed");
                }
            }
        })
    }

    fn supervisor_config(&self) -> ConnectorSupervisorConfig {
        ConnectorSupervisorConfig::default()
    }

    fn ensure_default_connector_inventory(&self) -> Result<(), ChannelPlatformError> {
        for spec in default_connector_specs() {
            let exists =
                self.supervisor.store().get_instance(spec.connector_id.as_str())?.is_some();
            if !exists {
                self.supervisor.register_connector(&spec)?;
            }
        }
        Ok(())
    }
}

fn default_connector_specs() -> Vec<ConnectorInstanceSpec> {
    vec![
        ConnectorInstanceSpec {
            connector_id: "echo:default".to_owned(),
            kind: ConnectorKind::Echo,
            principal: "channel:echo:default".to_owned(),
            auth_profile_ref: None,
            token_vault_ref: None,
            egress_allowlist: Vec::new(),
            enabled: true,
        },
        discord::discord_connector_spec("default", false),
        ConnectorInstanceSpec {
            connector_id: "slack:default".to_owned(),
            kind: ConnectorKind::Slack,
            principal: "channel:slack:default".to_owned(),
            auth_profile_ref: Some("slack.default".to_owned()),
            token_vault_ref: None,
            egress_allowlist: vec!["slack.com".to_owned(), "*.slack.com".to_owned()],
            enabled: false,
        },
        ConnectorInstanceSpec {
            connector_id: "telegram:default".to_owned(),
            kind: ConnectorKind::Telegram,
            principal: "channel:telegram:default".to_owned(),
            auth_profile_ref: Some("telegram.default".to_owned()),
            token_vault_ref: None,
            egress_allowlist: vec!["telegram.org".to_owned(), "*.telegram.org".to_owned()],
            enabled: false,
        },
    ]
}

struct GrpcChannelRouter {
    grpc_url: String,
    auth: GatewayAuthConfig,
}

#[async_trait::async_trait]
impl ConnectorRouter for GrpcChannelRouter {
    async fn route_inbound(
        &self,
        principal: &str,
        event: &InboundMessageEvent,
    ) -> Result<RouteInboundResult, ConnectorRouterError> {
        validate_canonical_id(event.envelope_id.as_str()).map_err(|_| {
            ConnectorRouterError::Message("inbound envelope_id must be a canonical ULID".to_owned())
        })?;
        let discord_connector = discord::is_discord_connector(event.connector_id.as_str());
        let conversation_id = if discord_connector {
            discord::canonical_discord_channel_identity(event.conversation_id.as_str())
        } else {
            event.conversation_id.clone()
        };
        let sender_handle = if discord_connector {
            discord::canonical_discord_sender_identity(event.sender_id.as_str())
        } else {
            event.sender_id.clone()
        };
        let content_text =
            with_attachment_context(event.body.as_str(), event.attachments.as_slice());
        let message_attachments = to_proto_message_attachments(event.attachments.as_slice());
        let mut client = gateway_v1::gateway_service_client::GatewayServiceClient::connect(
            self.grpc_url.clone(),
        )
        .await
        .map_err(|error| ConnectorRouterError::Message(error.to_string()))?;

        let mut request = tonic::Request::new(gateway_v1::RouteMessageRequest {
            v: CANONICAL_PROTOCOL_MAJOR,
            envelope: Some(common_v1::MessageEnvelope {
                v: CANONICAL_PROTOCOL_MAJOR,
                envelope_id: Some(common_v1::CanonicalId { ulid: event.envelope_id.clone() }),
                timestamp_unix_ms: event.received_at_unix_ms,
                origin: Some(common_v1::EnvelopeOrigin {
                    r#type: common_v1::envelope_origin::OriginType::Channel as i32,
                    channel: event.connector_id.clone(),
                    conversation_id,
                    sender_display: event.sender_display.clone().unwrap_or_default(),
                    sender_handle,
                    sender_verified: discord_connector,
                }),
                content: Some(common_v1::MessageContent {
                    text: content_text.clone(),
                    attachments: message_attachments,
                }),
                security: None,
                max_payload_bytes: u64::try_from(content_text.len()).unwrap_or(u64::MAX),
            }),
            is_direct_message: event.is_direct_message,
            request_broadcast: event.requested_broadcast,
            adapter_message_id: event.adapter_message_id.clone().unwrap_or_default(),
            adapter_thread_id: event.adapter_thread_id.clone().unwrap_or_default(),
            retry_attempt: 0,
            session_label: String::new(),
        });
        let (effective_principal, authorization_header) =
            resolve_connector_gateway_auth(&self.auth, principal)
                .map_err(|error| ConnectorRouterError::Message(error.to_string()))?;
        let metadata = request.metadata_mut();
        metadata.insert(
            HEADER_PRINCIPAL,
            MetadataValue::try_from(effective_principal.as_str())
                .map_err(|error| ConnectorRouterError::Message(error.to_string()))?,
        );
        metadata.insert(
            HEADER_DEVICE_ID,
            MetadataValue::try_from(CHANNEL_WORKER_DEVICE_ID)
                .map_err(|error| ConnectorRouterError::Message(error.to_string()))?,
        );
        metadata.insert(
            HEADER_CHANNEL,
            MetadataValue::try_from(event.connector_id.as_str())
                .map_err(|error| ConnectorRouterError::Message(error.to_string()))?,
        );
        if let Some(authorization_header) = authorization_header {
            metadata.insert(
                "authorization",
                MetadataValue::try_from(authorization_header.as_str())
                    .map_err(|error| ConnectorRouterError::Message(error.to_string()))?,
            );
        }

        let route_started_at = Instant::now();
        let response = client
            .route_message(request)
            .await
            .map_err(|error| ConnectorRouterError::Message(error.to_string()))?
            .into_inner();
        let route_message_latency_ms =
            u64::try_from(route_started_at.elapsed().as_millis()).unwrap_or(u64::MAX);
        let outputs = response
            .outputs
            .into_iter()
            .map(|output| RoutedOutboundMessage {
                text: output.text,
                thread_id: non_empty(output.thread_id),
                in_reply_to_message_id: non_empty(output.in_reply_to_message_id),
                broadcast: output.broadcast,
                auto_ack_text: non_empty(output.auto_ack_text),
                auto_reaction: non_empty(output.auto_reaction),
                attachments: from_proto_message_attachments(output.attachments.as_slice()),
                structured_json: non_empty_bytes(output.structured_json),
                a2ui_update: from_proto_a2ui_update(output.a2ui_update),
            })
            .collect();
        Ok(RouteInboundResult {
            accepted: response.accepted,
            queued_for_retry: response.queued_for_retry,
            decision_reason: response.decision_reason,
            outputs,
            route_key: non_empty(response.route_key),
            retry_attempt: response.retry_attempt,
            route_message_latency_ms: Some(route_message_latency_ms),
        })
    }
}

#[allow(clippy::result_large_err)]
fn resolve_connector_gateway_auth(
    auth: &GatewayAuthConfig,
    connector_principal: &str,
) -> Result<(String, Option<String>), ConnectorSupervisorError> {
    if !auth.require_auth {
        return Ok((connector_principal.to_owned(), None));
    }
    if let Some(connector_token) = auth.connector_token.as_deref() {
        return Ok((connector_principal.to_owned(), Some(format!("Bearer {connector_token}"))));
    }
    let admin_token = auth.admin_token.as_deref().ok_or_else(|| {
        ConnectorSupervisorError::Router(
            "admin auth is required but no admin token is configured".to_owned(),
        )
    })?;
    let effective_principal =
        auth.bound_principal.as_deref().unwrap_or(connector_principal).to_owned();
    Ok((effective_principal, Some(format!("Bearer {admin_token}"))))
}

fn non_empty(raw: String) -> Option<String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_owned())
    }
}

fn non_empty_bytes(raw: Vec<u8>) -> Option<Vec<u8>> {
    if raw.is_empty() {
        None
    } else {
        Some(raw)
    }
}

fn with_attachment_context(text: &str, attachments: &[AttachmentRef]) -> String {
    let Some(summary) = render_attachment_context(attachments) else {
        return text.to_owned();
    };
    let trimmed = text.trim_end();
    if trimmed.is_empty() {
        summary
    } else {
        format!("{trimmed}\n\n{summary}")
    }
}

fn render_attachment_context(attachments: &[AttachmentRef]) -> Option<String> {
    if attachments.is_empty() {
        return None;
    }
    let mut lines = Vec::with_capacity(attachments.len().saturating_add(1));
    lines.push("[attachment-metadata]".to_owned());
    for (index, attachment) in attachments.iter().enumerate() {
        lines.push(format!("- {}: {}", index.saturating_add(1), summarize_attachment(attachment)));
    }
    Some(lines.join("\n"))
}

fn summarize_attachment(attachment: &AttachmentRef) -> String {
    let kind = match attachment.kind {
        AttachmentKind::Image => "image",
        AttachmentKind::File => "file",
    };
    let source = attachment
        .url
        .as_deref()
        .or(attachment.artifact_ref.as_deref())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or("unknown");
    let filename = attachment
        .filename
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or("unknown");
    let content_type = attachment
        .content_type
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or("unknown");
    let size = attachment
        .size_bytes
        .map(|value| value.to_string())
        .unwrap_or_else(|| "unknown".to_owned());
    format!(
        "kind={kind}, filename={filename}, content_type={content_type}, size_bytes={size}, source={source}"
    )
}

fn to_proto_message_attachments(
    attachments: &[AttachmentRef],
) -> Vec<common_v1::MessageAttachment> {
    attachments
        .iter()
        .map(|attachment| {
            let artifact_id = attachment
                .artifact_ref
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(|value| common_v1::CanonicalId { ulid: value.to_owned() });
            common_v1::MessageAttachment {
                kind: attachment_kind_to_proto(attachment.kind),
                artifact_id,
                size_bytes: attachment.size_bytes.unwrap_or_default(),
            }
        })
        .collect()
}

fn from_proto_message_attachments(
    attachments: &[common_v1::MessageAttachment],
) -> Vec<OutboundAttachment> {
    attachments
        .iter()
        .map(|attachment| OutboundAttachment {
            kind: attachment_kind_from_proto(attachment.kind),
            url: None,
            artifact_ref: attachment.artifact_id.as_ref().map(|value| value.ulid.clone()),
            filename: None,
            content_type: None,
            size_bytes: if attachment.size_bytes > 0 { Some(attachment.size_bytes) } else { None },
        })
        .collect()
}

fn from_proto_a2ui_update(update: Option<common_v1::A2uiUpdate>) -> Option<ConnectorA2uiUpdate> {
    let update = update?;
    let surface = update.surface.trim();
    if surface.is_empty() || update.patch_json.is_empty() {
        return None;
    }
    Some(ConnectorA2uiUpdate { surface: surface.to_owned(), patch_json: update.patch_json })
}

fn attachment_kind_to_proto(kind: AttachmentKind) -> i32 {
    match kind {
        AttachmentKind::Image => common_v1::message_attachment::AttachmentKind::Image as i32,
        AttachmentKind::File => common_v1::message_attachment::AttachmentKind::File as i32,
    }
}

fn attachment_kind_from_proto(kind: i32) -> AttachmentKind {
    match common_v1::message_attachment::AttachmentKind::try_from(kind).ok() {
        Some(common_v1::message_attachment::AttachmentKind::Image) => AttachmentKind::Image,
        _ => AttachmentKind::File,
    }
}

fn unix_ms_now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis().try_into().unwrap_or(i64::MAX))
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use palyra_connectors::{AttachmentKind, AttachmentRef, ConnectorSupervisorError};

    use super::{
        discord, discord_connector_id, discord_token_vault_ref, from_proto_a2ui_update,
        normalize_discord_account_id, render_attachment_context, resolve_connector_gateway_auth,
        to_proto_message_attachments, with_attachment_context, ChannelPlatformError,
    };
    use crate::gateway::GatewayAuthConfig;

    #[test]
    fn discord_account_id_normalization_enforces_supported_charset() {
        let normalized =
            normalize_discord_account_id(" Ops.Team_1 ").expect("account id should normalize");
        assert_eq!(normalized, "ops.team_1");
        let invalid = normalize_discord_account_id("bad/account")
            .expect_err("unsupported account_id characters should be rejected");
        assert!(
            matches!(invalid, ChannelPlatformError::InvalidInput(_)),
            "invalid account id should return an InvalidInput error"
        );
    }

    #[test]
    fn discord_connector_and_vault_ref_helpers_match_default_conventions() {
        assert_eq!(discord_connector_id("default"), "discord:default");
        assert_eq!(discord_token_vault_ref("default"), "global/discord_bot_token");
        assert_eq!(
            discord_token_vault_ref("ops"),
            "global/discord_bot_token.ops",
            "non-default account should use scoped vault key suffix"
        );
        let spec = discord::discord_connector_spec("default", false);
        for host in [
            "discord.com",
            "*.discord.com",
            "discordapp.com",
            "*.discordapp.com",
            "discord.gg",
            "*.discord.gg",
            "discordapp.net",
            "*.discordapp.net",
        ] {
            assert!(
                spec.egress_allowlist.iter().any(|entry| entry == host),
                "discord connector allowlist should include {host}"
            );
        }
    }

    #[test]
    fn normalize_discord_target_rejects_empty_and_unsupported_values() {
        let normalized = discord::normalize_discord_target(" channel:123456 ")
            .expect("channel prefix should normalize to a target id");
        assert_eq!(normalized, "123456");
        let empty =
            discord::normalize_discord_target("  ").expect_err("empty target should be rejected");
        assert!(
            matches!(empty, ChannelPlatformError::InvalidInput(_)),
            "empty target should return InvalidInput"
        );
        let unsupported = discord::normalize_discord_target("channel:12 34")
            .expect_err("targets with spaces should be rejected");
        assert!(
            matches!(unsupported, ChannelPlatformError::InvalidInput(_)),
            "unsupported target should return InvalidInput"
        );
    }

    #[test]
    fn canonical_discord_identities_apply_expected_prefixes() {
        assert_eq!(
            discord::canonical_discord_sender_identity("12345"),
            "discord:user:12345",
            "plain sender ids should receive discord:user prefix"
        );
        assert_eq!(
            discord::canonical_discord_sender_identity("<@!67890>"),
            "discord:user:67890",
            "mention syntax should normalize to canonical sender identity"
        );
        assert_eq!(
            discord::canonical_discord_channel_identity("thread:abc"),
            "discord:channel:abc",
            "thread/channel aliases should normalize to canonical channel identity"
        );
        assert_eq!(
            discord::canonical_discord_channel_identity("<#C123>"),
            "discord:channel:c123",
            "channel mention syntax should normalize to canonical channel identity"
        );
    }

    #[test]
    fn attachment_context_block_preserves_text_and_metadata_fields() {
        let text = with_attachment_context(
            "user message",
            &[AttachmentRef {
                kind: AttachmentKind::Image,
                url: Some("https://cdn.discordapp.com/a.png".to_owned()),
                artifact_ref: Some("01ARZ3NDEKTSV4RRFFQ69G5FAV".to_owned()),
                filename: Some("a.png".to_owned()),
                content_type: Some("image/png".to_owned()),
                size_bytes: Some(512),
            }],
        );
        assert!(
            text.contains("[attachment-metadata]"),
            "attachment context marker must be appended when attachments are present"
        );
        assert!(
            text.contains("filename=a.png"),
            "attachment filename should be represented in metadata block"
        );
        assert!(
            text.starts_with("user message"),
            "original message text should stay at the beginning"
        );
    }

    #[test]
    fn proto_attachment_mapping_preserves_kind_and_size() {
        let attachments = to_proto_message_attachments(&[AttachmentRef {
            kind: AttachmentKind::Image,
            url: None,
            artifact_ref: Some("01ARZ3NDEKTSV4RRFFQ69G5FAV".to_owned()),
            filename: None,
            content_type: None,
            size_bytes: Some(4_096),
        }]);
        assert_eq!(attachments.len(), 1);
        assert_eq!(
            attachments[0].kind,
            crate::gateway::proto::palyra::common::v1::message_attachment::AttachmentKind::Image
                as i32
        );
        assert_eq!(attachments[0].size_bytes, 4_096);
        assert_eq!(
            attachments[0].artifact_id.as_ref().map(|value| value.ulid.as_str()),
            Some("01ARZ3NDEKTSV4RRFFQ69G5FAV")
        );
    }

    #[test]
    fn proto_a2ui_update_mapping_requires_surface_and_patch_payload() {
        let mapped =
            from_proto_a2ui_update(Some(crate::gateway::proto::palyra::common::v1::A2uiUpdate {
                v: 1,
                surface: "chat".to_owned(),
                patch_json: br#"[{"op":"replace","path":"/title","value":"ok"}]"#.to_vec(),
            }))
            .expect("valid proto A2UI update should map to connector update");
        assert_eq!(mapped.surface, "chat");
        assert_eq!(
            mapped.patch_json,
            br#"[{"op":"replace","path":"/title","value":"ok"}]"#.to_vec()
        );

        assert!(
            from_proto_a2ui_update(Some(crate::gateway::proto::palyra::common::v1::A2uiUpdate {
                v: 1,
                surface: "  ".to_owned(),
                patch_json: br#"{}"#.to_vec(),
            }))
            .is_none(),
            "blank A2UI surface should be rejected"
        );
        assert!(
            from_proto_a2ui_update(Some(crate::gateway::proto::palyra::common::v1::A2uiUpdate {
                v: 1,
                surface: "chat".to_owned(),
                patch_json: Vec::new(),
            }))
            .is_none(),
            "empty A2UI patch_json should be rejected"
        );
    }

    #[test]
    fn attachment_context_renderer_returns_none_for_empty_slice() {
        assert!(
            render_attachment_context(&[]).is_none(),
            "empty attachment list should not emit metadata block"
        );
    }

    #[test]
    fn connector_gateway_auth_prefers_connector_token_over_admin_binding() {
        let auth = GatewayAuthConfig {
            require_auth: true,
            admin_token: Some("admin-secret".to_owned()),
            connector_token: Some("connector-secret".to_owned()),
            bound_principal: Some("admin:ops".to_owned()),
        };
        let (principal, authorization) =
            resolve_connector_gateway_auth(&auth, "channel:discord:default")
                .expect("connector auth resolution should succeed");
        assert_eq!(
            principal, "channel:discord:default",
            "connector token path must preserve channel principal"
        );
        assert_eq!(
            authorization.as_deref(),
            Some("Bearer connector-secret"),
            "connector token should be used when configured"
        );
    }

    #[test]
    fn connector_gateway_auth_uses_admin_token_and_bound_principal_when_connector_token_missing() {
        let auth = GatewayAuthConfig {
            require_auth: true,
            admin_token: Some("admin-secret".to_owned()),
            connector_token: None,
            bound_principal: Some("admin:ops".to_owned()),
        };
        let (principal, authorization) =
            resolve_connector_gateway_auth(&auth, "channel:discord:default")
                .expect("admin auth fallback should succeed");
        assert_eq!(
            principal, "admin:ops",
            "admin fallback should preserve configured principal binding behavior"
        );
        assert_eq!(
            authorization.as_deref(),
            Some("Bearer admin-secret"),
            "admin token should be used when connector token is absent"
        );
    }

    #[test]
    fn connector_gateway_auth_requires_configured_token_when_auth_is_enabled() {
        let auth = GatewayAuthConfig {
            require_auth: true,
            admin_token: None,
            connector_token: None,
            bound_principal: None,
        };
        let error = resolve_connector_gateway_auth(&auth, "channel:discord:default")
            .expect_err("auth-enabled path without token should fail");
        assert!(
            matches!(error, ConnectorSupervisorError::Router(_)),
            "missing token should be surfaced as router error for deterministic channel logs"
        );
    }
}
