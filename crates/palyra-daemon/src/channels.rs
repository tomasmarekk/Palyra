use std::{
    path::PathBuf,
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use base64::{engine::general_purpose::STANDARD as BASE64_STANDARD, Engine as _};
use palyra_common::{validate_canonical_id, CANONICAL_PROTOCOL_MAJOR};
use palyra_connectors::{
    providers::default_adapters, AttachmentKind, AttachmentRef, ConnectorAvailability,
    ConnectorConversationTarget, ConnectorInstanceSpec, ConnectorKind,
    ConnectorMessageDeleteRequest, ConnectorMessageEditRequest, ConnectorMessageLocator,
    ConnectorMessageMutationResult, ConnectorMessageReactionRequest, ConnectorMessageReadRequest,
    ConnectorMessageReadResult, ConnectorMessageRecord, ConnectorMessageSearchRequest,
    ConnectorMessageSearchResult, ConnectorQueueSnapshot, ConnectorRouter, ConnectorRouterError,
    ConnectorStatusSnapshot, ConnectorSupervisor, ConnectorSupervisorConfig,
    ConnectorSupervisorError, DeadLetterRecord, DrainOutcome, InboundIngestOutcome,
    InboundMessageEvent, OutboundA2uiUpdate as ConnectorA2uiUpdate, OutboundAttachment,
    RouteInboundResult, RoutedOutboundMessage,
};
use serde_json::{json, Value};
use thiserror::Error;
use tokio::time::{interval, MissedTickBehavior};
use tonic::metadata::MetadataValue;
use tracing::warn;
use ulid::Ulid;

use crate::media::{
    ConsoleAttachmentStoreRequest, InboundAttachmentIngestRequest, MediaArtifactPayload,
    MediaArtifactStore, MediaDerivedArtifactRecord, MediaDerivedArtifactSelection,
    MediaDerivedArtifactUpsertRequest, MediaDerivedStatsSnapshot,
    MediaFailedDerivedArtifactUpsertRequest, MediaRuntimeConfig,
};
use crate::transport::grpc::{
    auth::{GatewayAuthConfig, HEADER_CHANNEL, HEADER_DEVICE_ID, HEADER_PRINCIPAL},
    proto::palyra::{common::v1 as common_v1, gateway::v1 as gateway_v1},
};

mod discord;

pub(crate) use discord::{
    classify_discord_message_mutation_governance, ChannelDiscordTestSendRequest,
    DiscordMessageMutationGovernance, DiscordMessageMutationKind,
};
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
    #[error(transparent)]
    Media(#[from] crate::media::MediaStoreError),
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
pub struct ChannelMessageReadOperation {
    pub request: ConnectorMessageReadRequest,
}

#[derive(Debug, Clone)]
pub struct ChannelMessageSearchOperation {
    pub request: ConnectorMessageSearchRequest,
}

#[derive(Debug, Clone)]
pub struct ChannelMessageEditOperation {
    pub request: ConnectorMessageEditRequest,
}

#[derive(Debug, Clone)]
pub struct ChannelMessageDeleteOperation {
    pub request: ConnectorMessageDeleteRequest,
}

#[derive(Debug, Clone)]
pub struct ChannelMessageReactionOperation {
    pub request: ConnectorMessageReactionRequest,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct ChannelMessageMutationPreview {
    pub locator: ConnectorMessageLocator,
    pub message: Option<ConnectorMessageRecord>,
    pub approved: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub approval_id: Option<String>,
}

pub struct ConsoleChatAttachmentStoreRequestView<'a> {
    pub session_id: &'a str,
    pub principal: &'a str,
    pub device_id: &'a str,
    pub channel: Option<&'a str>,
    pub filename: &'a str,
    pub declared_content_type: &'a str,
    pub bytes: &'a [u8],
}

pub struct ChannelPlatform {
    supervisor: Arc<ConnectorSupervisor>,
    media_store: Arc<MediaArtifactStore>,
    worker_interval: Duration,
}

impl ChannelPlatform {
    pub fn initialize(
        grpc_url: String,
        auth: GatewayAuthConfig,
        db_path: PathBuf,
        media_config: MediaRuntimeConfig,
    ) -> Result<Self, ChannelPlatformError> {
        let store = Arc::new(palyra_connectors::ConnectorStore::open(db_path)?);
        let media_store = Arc::new(MediaArtifactStore::open(
            media_db_path_from_connector_db_path(store.db_path()),
            media_content_root_from_connector_db_path(store.db_path()),
            media_config,
        )?);
        let router =
            Arc::new(GrpcChannelRouter { grpc_url, auth, media_store: Arc::clone(&media_store) });
        let supervisor = Arc::new(ConnectorSupervisor::new(
            Arc::clone(&store),
            router,
            default_adapters(),
            ConnectorSupervisorConfig::default(),
        ));
        let platform = Self {
            supervisor,
            media_store,
            worker_interval: Duration::from_millis(DEFAULT_CHANNEL_WORKER_INTERVAL_MS),
        };
        platform.ensure_default_connector_inventory()?;
        Ok(platform)
    }

    pub fn list(&self) -> Result<Vec<ConnectorStatusSnapshot>, ChannelPlatformError> {
        let visible = self
            .supervisor
            .list_status()
            .map_err(ChannelPlatformError::from)?
            .into_iter()
            .filter(|status| status.availability != ConnectorAvailability::Deferred)
            .collect();
        Ok(visible)
    }

    pub fn status(
        &self,
        connector_id: &str,
    ) -> Result<ConnectorStatusSnapshot, ChannelPlatformError> {
        self.ensure_operator_visible(connector_id)
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
        self.ensure_operator_visible(connector_id)?;
        let runtime =
            self.supervisor.runtime_snapshot(connector_id).map_err(ChannelPlatformError::from)?;
        let media = serde_json::to_value(self.media_store.build_connector_snapshot(connector_id)?)
            .map_err(|error| {
                ChannelPlatformError::InvalidInput(format!(
                    "failed to serialize media runtime snapshot: {error}"
                ))
            })?;
        Ok(Some(match runtime {
            Some(Value::Object(mut payload)) => {
                payload.insert("media".to_owned(), media);
                Value::Object(payload)
            }
            Some(other) => json!({
                "connector_runtime": other,
                "media": media,
            }),
            None => json!({ "media": media }),
        }))
    }

    pub fn media_snapshot(&self) -> Result<Value, ChannelPlatformError> {
        serde_json::to_value(self.media_store.build_global_snapshot()?).map_err(|error| {
            ChannelPlatformError::InvalidInput(format!(
                "failed to serialize media diagnostics snapshot: {error}"
            ))
        })
    }

    pub fn store_console_chat_attachment(
        &self,
        request: ConsoleChatAttachmentStoreRequestView<'_>,
    ) -> Result<MediaArtifactPayload, ChannelPlatformError> {
        let attachment_id = Ulid::new().to_string();
        self.media_store
            .store_console_attachment(ConsoleAttachmentStoreRequest {
                connector_id: "console_chat",
                session_id: request.session_id,
                principal: request.principal,
                device_id: request.device_id,
                channel: request.channel,
                attachment_id: attachment_id.as_str(),
                filename: request.filename,
                declared_content_type: request.declared_content_type,
                bytes: request.bytes,
            })
            .map_err(ChannelPlatformError::from)
    }

    pub fn load_console_chat_attachment(
        &self,
        artifact_id: &str,
        session_id: &str,
        principal: &str,
        device_id: &str,
        channel: Option<&str>,
    ) -> Result<Option<MediaArtifactPayload>, ChannelPlatformError> {
        self.media_store
            .load_console_attachment(artifact_id, session_id, principal, device_id, channel)
            .map_err(ChannelPlatformError::from)
    }

    pub fn list_console_chat_attachments(
        &self,
        session_id: &str,
        principal: &str,
        device_id: &str,
        channel: Option<&str>,
    ) -> Result<Vec<MediaArtifactPayload>, ChannelPlatformError> {
        self.media_store
            .list_console_attachment_payloads(session_id, principal, device_id, channel)
            .map_err(ChannelPlatformError::from)
    }

    pub fn upsert_console_chat_derived_artifact(
        &self,
        request: MediaDerivedArtifactUpsertRequest<'_>,
    ) -> Result<MediaDerivedArtifactRecord, ChannelPlatformError> {
        self.media_store.upsert_derived_artifact(request).map_err(ChannelPlatformError::from)
    }

    pub fn upsert_console_chat_failed_derived_artifact(
        &self,
        request: MediaFailedDerivedArtifactUpsertRequest<'_>,
    ) -> Result<MediaDerivedArtifactRecord, ChannelPlatformError> {
        self.media_store.upsert_failed_derived_artifact(request).map_err(ChannelPlatformError::from)
    }

    pub fn list_console_chat_derived_artifacts(
        &self,
        session_id: &str,
        principal: &str,
        device_id: &str,
        channel: Option<&str>,
    ) -> Result<Vec<MediaDerivedArtifactRecord>, ChannelPlatformError> {
        self.media_store
            .list_session_derived_artifacts(session_id, principal, device_id, channel)
            .map_err(ChannelPlatformError::from)
    }

    pub fn list_attachment_derived_artifacts(
        &self,
        source_artifact_id: &str,
    ) -> Result<Vec<MediaDerivedArtifactRecord>, ChannelPlatformError> {
        self.media_store
            .list_attachment_derived_artifacts(source_artifact_id)
            .map_err(ChannelPlatformError::from)
    }

    pub fn get_derived_artifact(
        &self,
        derived_artifact_id: &str,
    ) -> Result<Option<MediaDerivedArtifactRecord>, ChannelPlatformError> {
        self.media_store
            .get_derived_artifact(derived_artifact_id)
            .map_err(ChannelPlatformError::from)
    }

    pub fn list_linked_derived_artifacts(
        &self,
        workspace_document_id: Option<&str>,
        memory_item_id: Option<&str>,
        limit: usize,
    ) -> Result<Vec<MediaDerivedArtifactRecord>, ChannelPlatformError> {
        self.media_store
            .list_linked_derived_artifacts(workspace_document_id, memory_item_id, limit)
            .map_err(ChannelPlatformError::from)
    }

    pub fn link_derived_artifact_targets(
        &self,
        derived_artifact_id: &str,
        workspace_document_id: Option<&str>,
        memory_item_id: Option<&str>,
    ) -> Result<(), ChannelPlatformError> {
        self.media_store
            .link_derived_artifact_targets(
                derived_artifact_id,
                workspace_document_id,
                memory_item_id,
            )
            .map_err(ChannelPlatformError::from)
    }

    pub fn select_console_chat_derived_chunks(
        &self,
        source_artifact_ids: &[String],
        query: &str,
        selection_budget_chars: Option<usize>,
    ) -> Result<Vec<MediaDerivedArtifactSelection>, ChannelPlatformError> {
        self.media_store
            .select_derived_prompt_chunks(source_artifact_ids, query, selection_budget_chars)
            .map_err(ChannelPlatformError::from)
    }

    pub fn quarantine_derived_artifact(
        &self,
        derived_artifact_id: &str,
        reason: Option<&str>,
    ) -> Result<Option<MediaDerivedArtifactRecord>, ChannelPlatformError> {
        self.media_store
            .quarantine_derived_artifact(derived_artifact_id, reason)
            .map_err(ChannelPlatformError::from)
    }

    pub fn release_derived_artifact(
        &self,
        derived_artifact_id: &str,
    ) -> Result<Option<MediaDerivedArtifactRecord>, ChannelPlatformError> {
        self.media_store
            .release_derived_artifact(derived_artifact_id)
            .map_err(ChannelPlatformError::from)
    }

    pub fn mark_derived_artifact_recompute_required(
        &self,
        derived_artifact_id: &str,
        required: bool,
    ) -> Result<Option<MediaDerivedArtifactRecord>, ChannelPlatformError> {
        self.media_store
            .mark_derived_artifact_recompute_required(derived_artifact_id, required)
            .map_err(ChannelPlatformError::from)
    }

    pub fn purge_derived_artifact(
        &self,
        derived_artifact_id: &str,
    ) -> Result<Option<MediaDerivedArtifactRecord>, ChannelPlatformError> {
        self.media_store
            .purge_derived_artifact(derived_artifact_id)
            .map_err(ChannelPlatformError::from)
    }

    pub fn derived_stats(&self) -> Result<MediaDerivedStatsSnapshot, ChannelPlatformError> {
        self.media_store.derived_stats().map_err(ChannelPlatformError::from)
    }

    pub fn set_enabled(
        &self,
        connector_id: &str,
        enabled: bool,
    ) -> Result<ConnectorStatusSnapshot, ChannelPlatformError> {
        self.ensure_operator_visible(connector_id)?;
        self.supervisor.set_enabled(connector_id, enabled).map_err(ChannelPlatformError::from)
    }

    pub fn remove_connector(&self, connector_id: &str) -> Result<(), ChannelPlatformError> {
        self.ensure_operator_visible(connector_id)?;
        self.supervisor.remove_connector(connector_id).map_err(ChannelPlatformError::from)
    }

    pub fn logs(
        &self,
        connector_id: &str,
        limit: Option<usize>,
    ) -> Result<Vec<palyra_connectors::ConnectorEventRecord>, ChannelPlatformError> {
        self.ensure_operator_visible(connector_id)?;
        self.supervisor
            .list_logs(connector_id, limit.unwrap_or(DEFAULT_LOG_PAGE_LIMIT))
            .map_err(ChannelPlatformError::from)
    }

    pub fn dead_letters(
        &self,
        connector_id: &str,
        limit: Option<usize>,
    ) -> Result<Vec<DeadLetterRecord>, ChannelPlatformError> {
        self.ensure_operator_visible(connector_id)?;
        self.supervisor
            .list_dead_letters(connector_id, limit.unwrap_or(DEFAULT_LOG_PAGE_LIMIT))
            .map_err(ChannelPlatformError::from)
    }

    pub fn queue_snapshot(
        &self,
        connector_id: &str,
    ) -> Result<ConnectorQueueSnapshot, ChannelPlatformError> {
        self.ensure_operator_visible(connector_id)?;
        self.supervisor.queue_snapshot(connector_id).map_err(ChannelPlatformError::from)
    }

    pub fn set_queue_paused(
        &self,
        connector_id: &str,
        paused: bool,
        reason: Option<&str>,
    ) -> Result<ConnectorQueueSnapshot, ChannelPlatformError> {
        self.ensure_operator_visible(connector_id)?;
        self.supervisor
            .set_queue_paused(connector_id, paused, reason)
            .map_err(ChannelPlatformError::from)
    }

    pub fn replay_dead_letter(
        &self,
        connector_id: &str,
        dead_letter_id: i64,
    ) -> Result<DeadLetterRecord, ChannelPlatformError> {
        self.ensure_operator_visible(connector_id)?;
        self.supervisor
            .replay_dead_letter(connector_id, dead_letter_id)
            .map_err(ChannelPlatformError::from)
    }

    pub fn discard_dead_letter(
        &self,
        connector_id: &str,
        dead_letter_id: i64,
    ) -> Result<DeadLetterRecord, ChannelPlatformError> {
        self.ensure_operator_visible(connector_id)?;
        self.supervisor
            .discard_dead_letter(connector_id, dead_letter_id)
            .map_err(ChannelPlatformError::from)
    }

    pub fn connector_instance(
        &self,
        connector_id: &str,
    ) -> Result<palyra_connectors::ConnectorInstanceRecord, ChannelPlatformError> {
        self.ensure_operator_visible(connector_id)?;
        self.supervisor
            .store()
            .get_instance(connector_id)
            .map_err(ChannelPlatformError::from)?
            .ok_or_else(|| {
                ChannelPlatformError::Supervisor(ConnectorSupervisorError::NotFound(
                    connector_id.to_owned(),
                ))
            })
    }

    pub async fn read_messages(
        &self,
        connector_id: &str,
        operation: ChannelMessageReadOperation,
    ) -> Result<ConnectorMessageReadResult, ChannelPlatformError> {
        self.ensure_operator_visible(connector_id)?;
        operation
            .request
            .validate()
            .map_err(|error| ChannelPlatformError::InvalidInput(error.to_string()))?;
        self.supervisor
            .read_messages(connector_id, &operation.request)
            .await
            .map_err(ChannelPlatformError::from)
    }

    pub async fn search_messages(
        &self,
        connector_id: &str,
        operation: ChannelMessageSearchOperation,
    ) -> Result<ConnectorMessageSearchResult, ChannelPlatformError> {
        self.ensure_operator_visible(connector_id)?;
        operation
            .request
            .validate()
            .map_err(|error| ChannelPlatformError::InvalidInput(error.to_string()))?;
        self.supervisor
            .search_messages(connector_id, &operation.request)
            .await
            .map_err(ChannelPlatformError::from)
    }

    pub async fn edit_message(
        &self,
        connector_id: &str,
        operation: ChannelMessageEditOperation,
    ) -> Result<ConnectorMessageMutationResult, ChannelPlatformError> {
        self.ensure_operator_visible(connector_id)?;
        operation
            .request
            .validate()
            .map_err(|error| ChannelPlatformError::InvalidInput(error.to_string()))?;
        self.supervisor
            .edit_message(connector_id, &operation.request)
            .await
            .map_err(ChannelPlatformError::from)
    }

    pub async fn delete_message(
        &self,
        connector_id: &str,
        operation: ChannelMessageDeleteOperation,
    ) -> Result<ConnectorMessageMutationResult, ChannelPlatformError> {
        self.ensure_operator_visible(connector_id)?;
        operation
            .request
            .validate()
            .map_err(|error| ChannelPlatformError::InvalidInput(error.to_string()))?;
        self.supervisor
            .delete_message(connector_id, &operation.request)
            .await
            .map_err(ChannelPlatformError::from)
    }

    pub async fn add_reaction(
        &self,
        connector_id: &str,
        operation: ChannelMessageReactionOperation,
    ) -> Result<ConnectorMessageMutationResult, ChannelPlatformError> {
        self.ensure_operator_visible(connector_id)?;
        operation
            .request
            .validate()
            .map_err(|error| ChannelPlatformError::InvalidInput(error.to_string()))?;
        self.supervisor
            .add_reaction(connector_id, &operation.request)
            .await
            .map_err(ChannelPlatformError::from)
    }

    pub async fn remove_reaction(
        &self,
        connector_id: &str,
        operation: ChannelMessageReactionOperation,
    ) -> Result<ConnectorMessageMutationResult, ChannelPlatformError> {
        self.ensure_operator_visible(connector_id)?;
        operation
            .request
            .validate()
            .map_err(|error| ChannelPlatformError::InvalidInput(error.to_string()))?;
        self.supervisor
            .remove_reaction(connector_id, &operation.request)
            .await
            .map_err(ChannelPlatformError::from)
    }

    pub async fn fetch_message_preview(
        &self,
        connector_id: &str,
        locator: &ConnectorMessageLocator,
    ) -> Result<Option<ConnectorMessageRecord>, ChannelPlatformError> {
        let result = self
            .read_messages(
                connector_id,
                ChannelMessageReadOperation {
                    request: ConnectorMessageReadRequest {
                        target: ConnectorConversationTarget {
                            conversation_id: locator.target.conversation_id.clone(),
                            thread_id: locator.target.thread_id.clone(),
                        },
                        message_id: Some(locator.message_id.clone()),
                        before_message_id: None,
                        after_message_id: None,
                        around_message_id: None,
                        limit: 1,
                    },
                },
            )
            .await?;
        Ok(result.messages.into_iter().next())
    }

    pub async fn submit_test_message(
        &self,
        connector_id: &str,
        request: ChannelTestMessageRequest,
    ) -> Result<InboundIngestOutcome, ChannelPlatformError> {
        self.ensure_operator_visible(connector_id)?;
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

    pub async fn drain_due(&self) -> Result<palyra_connectors::DrainOutcome, ChannelPlatformError> {
        self.supervisor
            .drain_due_outbox(self.supervisor_config().background_drain_batch_size)
            .await
            .map_err(ChannelPlatformError::from)
    }

    pub async fn drain_due_for_connector(
        &self,
        connector_id: &str,
    ) -> Result<DrainOutcome, ChannelPlatformError> {
        self.ensure_operator_visible(connector_id)?;
        self.supervisor
            .drain_due_outbox_for_connector_force(
                connector_id,
                self.supervisor_config().background_drain_batch_size,
            )
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

    fn find_native_message_id(
        &self,
        connector_id: &str,
        envelope_id: &str,
    ) -> Result<Option<String>, ChannelPlatformError> {
        let logs =
            self.supervisor.list_logs(connector_id, 32).map_err(ChannelPlatformError::from)?;
        Ok(logs.into_iter().find_map(|event| {
            if event.event_type != "outbox.delivered" {
                return None;
            }
            let details = event.details?;
            let matches_envelope = details
                .get("envelope_id")
                .and_then(Value::as_str)
                .map(|value| value == envelope_id)
                .unwrap_or(false);
            if !matches_envelope {
                return None;
            }
            details.get("native_message_id").and_then(Value::as_str).map(ToOwned::to_owned)
        }))
    }

    fn ensure_operator_visible(
        &self,
        connector_id: &str,
    ) -> Result<ConnectorStatusSnapshot, ChannelPlatformError> {
        let status = self.supervisor.status(connector_id).map_err(ChannelPlatformError::from)?;
        if status.availability == ConnectorAvailability::Deferred {
            return Err(ChannelPlatformError::InvalidInput(format!(
                "connector '{}' is deferred and unavailable in the Discord-first runtime",
                connector_id.trim()
            )));
        }
        Ok(status)
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

async fn preprocess_discord_inbound_attachments(
    media_store: &Arc<MediaArtifactStore>,
    event: &InboundMessageEvent,
) -> Result<Vec<AttachmentRef>, crate::media::MediaStoreError> {
    let total_declared_bytes =
        event.attachments.iter().filter_map(|attachment| attachment.size_bytes).sum::<u64>();
    let mut prepared = Vec::with_capacity(event.attachments.len());
    for (attachment_index, attachment) in event.attachments.iter().enumerate() {
        prepared.push(
            media_store
                .ingest_inbound_attachment(InboundAttachmentIngestRequest {
                    connector_id: event.connector_id.as_str(),
                    envelope_id: event.envelope_id.as_str(),
                    conversation_id: event.conversation_id.as_str(),
                    adapter_message_id: event.adapter_message_id.as_deref(),
                    attachment,
                    attachment_index,
                    attachment_count: event.attachments.len(),
                    total_declared_bytes,
                })
                .await?,
        );
    }
    Ok(prepared)
}

fn prepare_outbound_attachments(
    media_store: &Arc<MediaArtifactStore>,
    connector_id: &str,
    attachments: Vec<OutboundAttachment>,
) -> Result<Vec<OutboundAttachment>, crate::media::MediaStoreError> {
    let config = media_store.config().clone();
    let mut prepared = Vec::with_capacity(attachments.len());
    for mut attachment in attachments {
        if !attachment.upload_requested {
            prepared.push(attachment);
            continue;
        }
        if !config.outbound_upload_enabled {
            block_outbound_upload_attachment(
                media_store,
                connector_id,
                &mut attachment,
                "attachment.upload disabled by config",
            )?;
            prepared.push(attachment);
            continue;
        }
        if attachment.inline_base64.is_none() {
            match attachment.artifact_ref.as_deref() {
                Some(artifact_id) => match media_store.load_artifact_payload(artifact_id)? {
                    Some(payload) => {
                        attachment.inline_base64 =
                            Some(BASE64_STANDARD.encode(payload.bytes.as_slice()));
                        attachment.artifact_ref.get_or_insert_with(|| payload.artifact_id.clone());
                        attachment.filename.get_or_insert_with(|| payload.filename.clone());
                        attachment.content_type.get_or_insert_with(|| payload.content_type.clone());
                        attachment.size_bytes.get_or_insert(payload.size_bytes);
                        attachment.content_hash.get_or_insert(payload.sha256.clone());
                        attachment.width_px = attachment.width_px.or(payload.width_px);
                        attachment.height_px = attachment.height_px.or(payload.height_px);
                    }
                    None => {
                        block_outbound_upload_attachment(
                            media_store,
                            connector_id,
                            &mut attachment,
                            "attachment.upload artifact_ref not found",
                        )?;
                        prepared.push(attachment);
                        continue;
                    }
                },
                None => {
                    block_outbound_upload_attachment(
                        media_store,
                        connector_id,
                        &mut attachment,
                        "attachment.upload requires inline content or artifact_ref",
                    )?;
                    prepared.push(attachment);
                    continue;
                }
            }
        }
        let Some(content_type) = attachment
            .content_type
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(ToOwned::to_owned)
        else {
            block_outbound_upload_attachment(
                media_store,
                connector_id,
                &mut attachment,
                "attachment.upload content_type is required",
            )?;
            prepared.push(attachment);
            continue;
        };
        if !config.outbound_allowed_content_types.iter().any(|allowed| allowed == &content_type) {
            block_outbound_upload_attachment(
                media_store,
                connector_id,
                &mut attachment,
                format!("attachment.upload content type '{content_type}' is blocked by policy")
                    .as_str(),
            )?;
            prepared.push(attachment);
            continue;
        }
        let decoded_size = attachment
            .inline_base64
            .as_deref()
            .map(|value| {
                BASE64_STANDARD.decode(value).map(|bytes| bytes.len()).unwrap_or(usize::MAX)
            })
            .unwrap_or_default();
        if decoded_size > config.outbound_max_upload_bytes {
            block_outbound_upload_attachment(
                media_store,
                connector_id,
                &mut attachment,
                format!(
                    "attachment.upload exceeds max_upload_bytes ({decoded_size}/{})",
                    config.outbound_max_upload_bytes
                )
                .as_str(),
            )?;
        }
        prepared.push(attachment);
    }
    Ok(prepared)
}

fn block_outbound_upload_attachment(
    media_store: &Arc<MediaArtifactStore>,
    connector_id: &str,
    attachment: &mut OutboundAttachment,
    reason: &str,
) -> Result<(), crate::media::MediaStoreError> {
    attachment.policy_context = Some(reason.to_owned());
    attachment.upload_requested = false;
    attachment.inline_base64 = None;
    media_store.record_upload_failure(
        connector_id,
        attachment.artifact_ref.as_deref(),
        attachment.filename.as_deref(),
        reason,
    )
}

fn media_db_path_from_connector_db_path(connector_db_path: &std::path::Path) -> PathBuf {
    let parent = connector_db_path
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("."));
    parent.join("media.sqlite3")
}

fn media_content_root_from_connector_db_path(connector_db_path: &std::path::Path) -> PathBuf {
    let parent = connector_db_path
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("."));
    parent.join("media")
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
    ]
}

struct GrpcChannelRouter {
    grpc_url: String,
    auth: GatewayAuthConfig,
    media_store: Arc<MediaArtifactStore>,
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
        let attachments = if discord_connector {
            preprocess_discord_inbound_attachments(&self.media_store, event)
                .await
                .map_err(|error| ConnectorRouterError::Message(error.to_string()))?
        } else {
            event.attachments.clone()
        };
        let content_text = with_attachment_context(event.body.as_str(), attachments.as_slice());
        let message_attachments = to_proto_message_attachments(attachments.as_slice());
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
                max_payload_bytes: route_message_max_payload_bytes(
                    &ConnectorSupervisorConfig::default(),
                ),
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
            .map(|output| {
                Ok(RoutedOutboundMessage {
                    text: output.text,
                    thread_id: non_empty(output.thread_id),
                    in_reply_to_message_id: non_empty(output.in_reply_to_message_id),
                    broadcast: output.broadcast,
                    auto_ack_text: non_empty(output.auto_ack_text),
                    auto_reaction: non_empty(output.auto_reaction),
                    attachments: prepare_outbound_attachments(
                        &self.media_store,
                        event.connector_id.as_str(),
                        from_proto_message_attachments(output.attachments.as_slice()),
                    )?,
                    structured_json: non_empty_bytes(output.structured_json),
                    a2ui_update: from_proto_a2ui_update(output.a2ui_update),
                })
            })
            .collect::<Result<Vec<_>, crate::media::MediaStoreError>>()
            .map_err(|error| ConnectorRouterError::Message(error.to_string()))?;
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

fn route_message_max_payload_bytes(config: &ConnectorSupervisorConfig) -> u64 {
    u64::try_from(config.max_outbound_body_bytes).unwrap_or(u64::MAX)
}

#[allow(clippy::result_large_err)]
fn resolve_connector_gateway_auth(
    auth: &GatewayAuthConfig,
    connector_principal: &str,
) -> Result<(String, Option<String>), ConnectorSupervisorError> {
    if !auth.require_auth {
        return Ok((connector_principal.to_owned(), None));
    }
    let connector_token = auth.connector_token.as_deref().ok_or_else(|| {
        ConnectorSupervisorError::Router(
            "connector_token is required for RouteMessage when gateway auth is enabled".to_owned(),
        )
    })?;
    Ok((connector_principal.to_owned(), Some(format!("Bearer {connector_token}"))))
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
                attachment_id: attachment.attachment_id.clone().unwrap_or_default(),
                filename: attachment.filename.clone().unwrap_or_default(),
                declared_content_type: attachment.content_type.clone().unwrap_or_default(),
                source_url: attachment.url.clone().unwrap_or_default(),
                content_hash: attachment.content_hash.clone().unwrap_or_default(),
                origin: attachment.origin.clone().unwrap_or_default(),
                policy_context: attachment.policy_context.clone().unwrap_or_default(),
                inline_bytes: attachment
                    .inline_base64
                    .as_deref()
                    .and_then(|value| BASE64_STANDARD.decode(value).ok())
                    .unwrap_or_default(),
                upload_requested: attachment.upload_requested,
                width_px: attachment.width_px.unwrap_or_default(),
                height_px: attachment.height_px.unwrap_or_default(),
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
            attachment_id: non_empty(attachment.attachment_id.clone()),
            url: non_empty(attachment.source_url.clone()),
            artifact_ref: attachment.artifact_id.as_ref().map(|value| value.ulid.clone()),
            filename: non_empty(attachment.filename.clone()),
            content_type: non_empty(attachment.declared_content_type.clone()),
            size_bytes: if attachment.size_bytes > 0 { Some(attachment.size_bytes) } else { None },
            content_hash: non_empty(attachment.content_hash.clone()),
            origin: non_empty(attachment.origin.clone()),
            policy_context: non_empty(attachment.policy_context.clone()),
            upload_requested: attachment.upload_requested,
            inline_base64: if attachment.inline_bytes.is_empty() {
                None
            } else {
                Some(BASE64_STANDARD.encode(attachment.inline_bytes.as_slice()))
            },
            width_px: if attachment.width_px > 0 { Some(attachment.width_px) } else { None },
            height_px: if attachment.height_px > 0 { Some(attachment.height_px) } else { None },
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
    use palyra_connectors::{
        AttachmentKind, AttachmentRef, ConnectorAvailability, ConnectorInstanceSpec, ConnectorKind,
        ConnectorSupervisorConfig, ConnectorSupervisorError,
    };
    use tempfile::TempDir;

    use super::{
        default_connector_specs, discord, discord_connector_id, discord_token_vault_ref,
        from_proto_a2ui_update, normalize_discord_account_id, render_attachment_context,
        resolve_connector_gateway_auth, route_message_max_payload_bytes,
        to_proto_message_attachments, with_attachment_context, ChannelPlatform,
        ChannelPlatformError,
    };
    use crate::gateway::GatewayAuthConfig;
    use crate::media::MediaRuntimeConfig;

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
                ..AttachmentRef::default()
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
            artifact_ref: Some("01ARZ3NDEKTSV4RRFFQ69G5FAV".to_owned()),
            size_bytes: Some(4_096),
            ..AttachmentRef::default()
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
    fn connector_gateway_auth_requires_connector_token_when_auth_is_enabled() {
        let auth = GatewayAuthConfig {
            require_auth: true,
            admin_token: Some("admin-secret".to_owned()),
            connector_token: None,
            bound_principal: Some("admin:ops".to_owned()),
        };
        let error = resolve_connector_gateway_auth(&auth, "channel:discord:default")
            .expect_err("auth-enabled path without connector token should fail");
        assert!(
            matches!(error, ConnectorSupervisorError::Router(message) if message.contains("connector_token is required")),
            "missing connector token should be surfaced as deterministic router error"
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

    #[test]
    fn route_message_max_payload_bytes_uses_supervisor_outbound_limit() {
        let config = ConnectorSupervisorConfig {
            max_outbound_body_bytes: 4_096,
            ..ConnectorSupervisorConfig::default()
        };
        let max_payload_bytes = route_message_max_payload_bytes(&config);
        assert_eq!(
            max_payload_bytes, 4_096,
            "route requests should inherit the connector outbound payload budget"
        );
        assert_ne!(
            max_payload_bytes, 5,
            "route reply chunking must not collapse to the size of a short inbound prompt"
        );
    }

    #[test]
    fn default_connector_specs_match_discord_first_runtime_scope() {
        let specs = default_connector_specs();
        let inventory = specs
            .iter()
            .map(|spec| (spec.connector_id.clone(), spec.kind, spec.kind.default_availability()))
            .collect::<Vec<_>>();

        assert_eq!(
            inventory,
            vec![
                (
                    "echo:default".to_owned(),
                    ConnectorKind::Echo,
                    ConnectorAvailability::InternalTestOnly,
                ),
                (
                    "discord:default".to_owned(),
                    ConnectorKind::Discord,
                    ConnectorAvailability::Supported,
                ),
            ]
        );
    }

    #[test]
    fn operator_surface_hides_and_rejects_deferred_connectors_from_existing_inventory() {
        let tempdir = TempDir::new().expect("tempdir should initialize");
        let platform = ChannelPlatform::initialize(
            "http://127.0.0.1:7443".to_owned(),
            GatewayAuthConfig {
                require_auth: false,
                admin_token: None,
                connector_token: None,
                bound_principal: None,
            },
            tempdir.path().join("connectors.sqlite3"),
            MediaRuntimeConfig::default(),
        )
        .expect("platform should initialize");

        platform
            .supervisor
            .register_connector(&ConnectorInstanceSpec {
                connector_id: "slack:default".to_owned(),
                kind: ConnectorKind::Slack,
                principal: "channel:slack:default".to_owned(),
                auth_profile_ref: Some("slack.default".to_owned()),
                token_vault_ref: None,
                egress_allowlist: vec!["slack.com".to_owned(), "*.slack.com".to_owned()],
                enabled: false,
            })
            .expect("legacy deferred connector should remain representable in storage");

        let listed = platform.list().expect("list should resolve");
        assert!(
            listed.iter().all(|entry| entry.connector_id != "slack:default"),
            "deferred connectors must stay hidden from operator listings"
        );

        let error = platform
            .status("slack:default")
            .expect_err("direct operator access should reject deferred connectors");
        assert!(
            matches!(error, ChannelPlatformError::InvalidInput(message) if message.contains("deferred")),
            "deferred connectors should fail with an explicit invalid-input message"
        );
    }
}
