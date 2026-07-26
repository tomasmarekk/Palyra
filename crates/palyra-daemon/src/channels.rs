//! Channel platform facade over the connector runtime.
//!
//! [`ChannelPlatform`] wires the connector supervisor, the media artifact
//! store, and a gRPC gateway router into one operator-facing surface:
//! connector inventory and lifecycle, queue/dead-letter management, message
//! read/search/mutation operations, test ingestion, and the background
//! worker that polls inbound traffic and drains the outbox. Deferred
//! connectors are hidden and rejected at this boundary (Discord-first
//! runtime). Submodules own attachments, default inventory, Discord
//! canonicalization, gateway auth resolution, and proto mapping.

use std::{
    env,
    path::PathBuf,
    sync::Arc,
    time::{Duration, Instant},
};

use palyra_common::{
    qa_fault_injection::QaFaultProbeHandle, validate_canonical_id, CANONICAL_PROTOCOL_MAJOR,
};
use palyra_connectors::{
    providers::default_adapters, ChannelIngressRecord, ChannelIngressStatus, ConnectorAvailability,
    ConnectorConversationTarget, ConnectorKind, ConnectorMessageDeleteRequest,
    ConnectorMessageEditRequest, ConnectorMessageLocator, ConnectorMessageMutationResult,
    ConnectorMessageReactionRequest, ConnectorMessageReadRequest, ConnectorMessageReadResult,
    ConnectorMessageRecord, ConnectorMessageSearchRequest, ConnectorMessageSearchResult,
    ConnectorQueueSnapshot, ConnectorRouter, ConnectorRouterError, ConnectorStatusSnapshot,
    ConnectorSupervisor, ConnectorSupervisorConfig, ConnectorSupervisorError, DeadLetterRecord,
    DeliveryIntentRecord, DeliveryIntentRetryOutcome, DeliveryIntentStatus, DeliveryPipelineMode,
    DrainOutcome, InboundIngestOutcome, InboundMessageEvent, OutboundMessageRequest,
    OutboxEffectState, OutboxEnqueueOutcome, RouteInboundResult, RoutedOutboundMessage,
};
use serde_json::{json, Value};
use thiserror::Error;
use tokio::time::{interval, MissedTickBehavior};
use tonic::metadata::MetadataValue;
use tracing::warn;
use ulid::Ulid;

use crate::media::{
    ConsoleAttachmentStoreRequest, MediaArtifactPayload, MediaArtifactStore,
    MediaDerivedArtifactRecord, MediaDerivedArtifactSelection, MediaDerivedArtifactUpsertRequest,
    MediaDerivedStatsSnapshot, MediaFailedDerivedArtifactUpsertRequest, MediaRuntimeConfig,
};
use crate::transport::grpc::{
    auth::{GatewayAuthConfig, HEADER_CHANNEL, HEADER_DEVICE_ID, HEADER_PRINCIPAL},
    proto::palyra::{common::v1 as common_v1, gateway::v1 as gateway_v1},
};

mod attachments;
mod defaults;
mod discord;
mod gateway_auth;
mod media;
mod proto;

use attachments::{
    prepare_outbound_attachments, preprocess_discord_inbound_attachments, with_attachment_context,
};
use defaults::{
    default_connector_specs, media_content_root_from_connector_db_path,
    media_db_path_from_connector_db_path, route_message_max_payload_bytes, unix_ms_now,
};
use gateway_auth::resolve_connector_gateway_auth;
use proto::{
    from_proto_a2ui_update, from_proto_message_attachments, non_empty, non_empty_bytes,
    to_proto_message_attachments,
};

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
#[cfg(feature = "qa-fault-injection")]
const CONNECTOR_QA_CRASH_RECOVERY_POINTS: [&str; 6] = [
    "connector.outbox.before_intent",
    "connector.outbox.after_intent",
    "connector.outbox.before_effect",
    "connector.outbox.during_delivery",
    "connector.outbox.after_effect_before_ack",
    "connector.outbox.after_ack_before_transition",
];

/// Failure modes of channel-platform operations; supervisor, store, and
/// media errors pass through transparently.
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
    #[error("channel precondition failed: {0}")]
    Precondition(String),
    #[error("unsupported connector: {0}")]
    UnsupportedConnector(String),
}

/// Synthetic inbound message injected by operators to exercise a connector
/// end to end; `simulate_crash_once` plants the crash marker the echo
/// connector honors for retry testing.
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

/// Wrapper for a validated connector message-read request.
#[derive(Debug, Clone)]
pub struct ChannelMessageReadOperation {
    pub request: ConnectorMessageReadRequest,
}

/// Wrapper for a validated connector message-search request.
#[derive(Debug, Clone)]
pub struct ChannelMessageSearchOperation {
    pub request: ConnectorMessageSearchRequest,
}

/// Wrapper for a validated connector message-edit request.
#[derive(Debug, Clone)]
pub struct ChannelMessageEditOperation {
    pub request: ConnectorMessageEditRequest,
}

/// Wrapper for a validated connector message-delete request.
#[derive(Debug, Clone)]
pub struct ChannelMessageDeleteOperation {
    pub request: ConnectorMessageDeleteRequest,
}

/// Wrapper for a validated connector reaction add/remove request.
#[derive(Debug, Clone)]
pub struct ChannelMessageReactionOperation {
    pub request: ConnectorMessageReactionRequest,
}

/// Approval-surface preview of a pending message mutation: the targeted
/// message (when still readable) and the governance decision so far.
#[derive(Debug, Clone, serde::Serialize)]
pub struct ChannelMessageMutationPreview {
    pub locator: ConnectorMessageLocator,
    pub message: Option<ConnectorMessageRecord>,
    pub approved: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub approval_id: Option<String>,
}

/// Borrowed view of a console chat attachment upload, converted into a
/// [`ConsoleAttachmentStoreRequest`] by [`ChannelPlatform`] helpers.
pub struct ConsoleChatAttachmentStoreRequestView<'a> {
    pub session_id: &'a str,
    pub principal: &'a str,
    pub device_id: &'a str,
    pub channel: Option<&'a str>,
    pub filename: &'a str,
    pub declared_content_type: &'a str,
    pub bytes: &'a [u8],
}

/// Operator-facing channel runtime: connector supervisor plus media store,
/// routing inbound traffic to the daemon gateway over gRPC.
pub struct ChannelPlatform {
    supervisor: Arc<ConnectorSupervisor>,
    media_store: Arc<MediaArtifactStore>,
    worker_interval: Duration,
    supervisor_config: ConnectorSupervisorConfig,
}

impl ChannelPlatform {
    /// Idempotently hands a finalized V2 output to the existing connector outbox.
    ///
    /// This method never drains the queue or calls a provider adapter.
    ///
    /// # Errors
    /// Returns validation, connector lookup, or connector-store errors.
    pub(crate) fn enqueue_runtime_final_delivery(
        &self,
        request: &OutboundMessageRequest,
    ) -> Result<OutboxEnqueueOutcome, ChannelPlatformError> {
        self.supervisor.enqueue_outbound(request).map_err(ChannelPlatformError::from)
    }

    /// Returns the payload-free state of a finalized V2 outbox envelope.
    ///
    /// # Errors
    /// Returns connector-store errors or rejects malformed persisted state.
    pub(crate) fn runtime_final_delivery_state(
        &self,
        connector_id: &str,
        envelope_id: &str,
    ) -> Result<
        crate::application::runtime_kernel_v2::finalization::DeliveryOutboxState,
        ChannelPlatformError,
    > {
        use crate::application::runtime_kernel_v2::finalization::DeliveryOutboxState;

        let Some(snapshot) =
            self.supervisor.store().outbox_delivery_snapshot(connector_id, envelope_id)?
        else {
            return Ok(DeliveryOutboxState::Missing);
        };
        if snapshot.effect_state == OutboxEffectState::OutcomeUnknown {
            return Ok(DeliveryOutboxState::OutcomeUnknown);
        }
        match snapshot.status.as_str() {
            "pending" => Ok(DeliveryOutboxState::Queued),
            "delivered" => snapshot
                .native_message_id
                .map(|native_message_id| DeliveryOutboxState::Delivered { native_message_id })
                .ok_or_else(|| {
                    ChannelPlatformError::Precondition(
                        "delivered outbox row is missing native acknowledgement identity"
                            .to_owned(),
                    )
                }),
            "dead" => Ok(DeliveryOutboxState::DeadLetter),
            _ => Err(ChannelPlatformError::Precondition(
                "final delivery outbox row has an unsupported status".to_owned(),
            )),
        }
    }

    /// Opens the connector and media stores next to `db_path`, wires the
    /// gRPC gateway router, and registers the default connector inventory.
    ///
    /// # Errors
    /// Returns a store or media error when the backing databases cannot be
    /// opened, and a supervisor error when default connectors fail to
    /// register.
    #[cfg(any(not(feature = "qa-fault-injection"), test))]
    pub fn initialize(
        grpc_url: String,
        auth: GatewayAuthConfig,
        db_path: PathBuf,
        media_config: MediaRuntimeConfig,
    ) -> Result<Self, ChannelPlatformError> {
        Self::initialize_with_probe(
            grpc_url,
            auth,
            db_path,
            media_config,
            QaFaultProbeHandle::default(),
        )
    }

    #[cfg(feature = "qa-fault-injection")]
    pub(crate) fn initialize_with_qa_fault_probe(
        grpc_url: String,
        auth: GatewayAuthConfig,
        db_path: PathBuf,
        media_config: MediaRuntimeConfig,
        qa_fault_probe: QaFaultProbeHandle,
    ) -> Result<Self, ChannelPlatformError> {
        Self::initialize_with_probe(grpc_url, auth, db_path, media_config, qa_fault_probe)
    }

    fn initialize_with_probe(
        grpc_url: String,
        auth: GatewayAuthConfig,
        db_path: PathBuf,
        media_config: MediaRuntimeConfig,
        qa_fault_probe: QaFaultProbeHandle,
    ) -> Result<Self, ChannelPlatformError> {
        let store = Arc::new(palyra_connectors::ConnectorStore::open(db_path)?);
        let media_store = Arc::new(MediaArtifactStore::open(
            media_db_path_from_connector_db_path(store.db_path()),
            media_content_root_from_connector_db_path(store.db_path()),
            media_config,
        )?);
        let router =
            Arc::new(GrpcChannelRouter { grpc_url, auth, media_store: Arc::clone(&media_store) });
        let supervisor_config = connector_supervisor_config_from_env()?;
        let supervisor = ConnectorSupervisor::new(
            Arc::clone(&store),
            router,
            default_adapters(),
            supervisor_config.clone(),
        );
        #[cfg(feature = "qa-fault-injection")]
        let supervisor = supervisor.with_qa_fault_probe(qa_fault_probe);
        #[cfg(not(feature = "qa-fault-injection"))]
        let supervisor = {
            let _ = qa_fault_probe;
            supervisor
        };
        let supervisor = Arc::new(supervisor);
        let platform = Self {
            supervisor,
            media_store,
            worker_interval: Duration::from_millis(DEFAULT_CHANNEL_WORKER_INTERVAL_MS),
            supervisor_config,
        };
        platform.ensure_default_connector_inventory()?;
        Ok(platform)
    }

    /// Reconciles connector-owned crash boundaries before the generic QA startup sweep.
    ///
    /// Connector recovery must be proved from the durable outbox fence. Recording a generic
    /// outcome first would make a claimed row wait for its lease expiry and could misclassify an
    /// effect whose delivery had already started.
    ///
    /// # Errors
    /// Returns a precondition error when evidence cannot be inspected or recorded, and a
    /// supervisor error when the exact outbox actor does not have the expected durable state.
    #[cfg(feature = "qa-fault-injection")]
    pub(crate) fn reconcile_pending_qa_fault_recoveries(
        &self,
        runtime: &crate::qa_fault_injection::QaFaultRuntime,
    ) -> Result<usize, ChannelPlatformError> {
        let mut recovered = 0usize;
        for point_id in CONNECTOR_QA_CRASH_RECOVERY_POINTS {
            let actors =
                runtime.pending_activation_actors_for_point(point_id).map_err(|error| {
                    ChannelPlatformError::Precondition(format!(
                        "failed to inspect pending connector QA activations at {point_id}: {error}"
                    ))
                })?;
            for actor in actors {
                let recovery_class =
                    self.supervisor.reconcile_pending_qa_fault_actor(point_id, actor.as_str())?;
                let recorded = runtime
                    .record_pending_recovery_for_point_actor(
                        point_id,
                        actor.as_str(),
                        recovery_class,
                        "qa_fault.connector_store_recovery_proved",
                    )
                    .map_err(|error| {
                        ChannelPlatformError::Precondition(format!(
                            "failed to record connector QA recovery at {point_id} for {actor}: {error}"
                        ))
                    })?;
                if !recorded {
                    return Err(ChannelPlatformError::Precondition(format!(
                        "connector QA recovery at {point_id} for {actor} lost its pending activation"
                    )));
                }
                recovered = recovered.saturating_add(1);
            }
        }
        Ok(recovered)
    }

    /// Lists connector statuses, hiding deferred connectors from the
    /// operator surface.
    ///
    /// # Errors
    /// Returns a supervisor error when status enumeration fails.
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

    /// Returns the status of one operator-visible connector.
    ///
    /// # Errors
    /// Returns [`ChannelPlatformError::InvalidInput`] for deferred
    /// connectors and a supervisor error for unknown ids.
    pub fn status(
        &self,
        connector_id: &str,
    ) -> Result<ConnectorStatusSnapshot, ChannelPlatformError> {
        self.ensure_operator_visible(connector_id)
    }

    /// Returns the Discord connector for `account_id`, registering it
    /// (disabled) when it does not exist yet.
    ///
    /// # Errors
    /// Returns [`ChannelPlatformError::InvalidInput`] for malformed account
    /// ids or when the id is already taken by a non-Discord connector, and
    /// a supervisor error when registration fails.
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

    /// Returns the connector's runtime diagnostics merged with its media
    /// pipeline snapshot under a `media` key.
    ///
    /// # Errors
    /// Returns visibility/supervisor errors and
    /// [`ChannelPlatformError::InvalidInput`] when the media snapshot does
    /// not serialize.
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

    /// Enables or disables an operator-visible connector.
    ///
    /// # Errors
    /// Returns visibility or supervisor errors.
    pub fn set_enabled(
        &self,
        connector_id: &str,
        enabled: bool,
    ) -> Result<ConnectorStatusSnapshot, ChannelPlatformError> {
        self.ensure_operator_visible(connector_id)?;
        self.supervisor.set_enabled(connector_id, enabled).map_err(ChannelPlatformError::from)
    }

    /// Removes an operator-visible connector and its stored state.
    ///
    /// # Errors
    /// Returns visibility or supervisor errors.
    pub fn remove_connector(&self, connector_id: &str) -> Result<(), ChannelPlatformError> {
        self.ensure_operator_visible(connector_id)?;
        self.supervisor.remove_connector(connector_id).map_err(ChannelPlatformError::from)
    }

    /// Returns the most recent connector event records (default page
    /// limit when `limit` is `None`).
    ///
    /// # Errors
    /// Returns visibility or supervisor errors.
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

    /// Returns the most recent dead-letter records (default page limit
    /// when `limit` is `None`).
    ///
    /// # Errors
    /// Returns visibility or supervisor errors.
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

    /// Returns recent durable ingress events for a connector.
    ///
    /// # Errors
    /// Returns visibility or storage errors.
    pub fn ingress_events(
        &self,
        connector_id: &str,
        status: Option<ChannelIngressStatus>,
        limit: Option<usize>,
    ) -> Result<Vec<ChannelIngressRecord>, ChannelPlatformError> {
        self.ensure_operator_visible(connector_id)?;
        self.supervisor
            .store()
            .list_channel_ingress_events(
                connector_id,
                status,
                limit.unwrap_or(DEFAULT_LOG_PAGE_LIMIT),
            )
            .map_err(ChannelPlatformError::from)
    }

    /// Returns one durable ingress event.
    ///
    /// # Errors
    /// Returns visibility or storage errors.
    pub fn ingress_event(
        &self,
        connector_id: &str,
        ingress_event_id: i64,
    ) -> Result<ChannelIngressRecord, ChannelPlatformError> {
        self.ensure_operator_visible(connector_id)?;
        self.supervisor
            .store()
            .get_channel_ingress_event(connector_id, ingress_event_id)
            .map_err(ChannelPlatformError::from)
    }

    /// Returns recent delivery intents for a connector.
    ///
    /// # Errors
    /// Returns visibility or storage errors.
    pub fn delivery_intents(
        &self,
        connector_id: &str,
        status: Option<DeliveryIntentStatus>,
        limit: Option<usize>,
    ) -> Result<Vec<DeliveryIntentRecord>, ChannelPlatformError> {
        self.ensure_operator_visible(connector_id)?;
        self.supervisor
            .store()
            .list_delivery_intents(connector_id, status, limit.unwrap_or(DEFAULT_LOG_PAGE_LIMIT))
            .map_err(ChannelPlatformError::from)
    }

    /// Returns one delivery intent.
    ///
    /// # Errors
    /// Returns storage errors, including unknown intent ids.
    pub fn delivery_intent(
        &self,
        intent_id: &str,
    ) -> Result<DeliveryIntentRecord, ChannelPlatformError> {
        let intent = self
            .supervisor
            .store()
            .get_delivery_intent(intent_id)
            .map_err(ChannelPlatformError::from)?;
        self.ensure_operator_visible(intent.connector_id.as_str())?;
        Ok(intent)
    }

    /// Requeues a delivery intent from a safe retry state.
    ///
    /// # Errors
    /// Returns storage errors, including unsafe current states.
    pub fn retry_delivery_intent(
        &self,
        intent_id: &str,
    ) -> Result<DeliveryIntentRetryOutcome, ChannelPlatformError> {
        let intent = self.delivery_intent(intent_id)?;
        self.ensure_operator_visible(intent.connector_id.as_str())?;
        self.supervisor
            .store()
            .retry_delivery_intent(
                intent_id,
                self.supervisor_config().max_retry_attempts,
                unix_ms_now(),
            )
            .map_err(ChannelPlatformError::from)
    }

    /// Returns the connector's outbox/dead-letter queue snapshot.
    ///
    /// # Errors
    /// Returns visibility or supervisor errors.
    pub fn queue_snapshot(
        &self,
        connector_id: &str,
    ) -> Result<ConnectorQueueSnapshot, ChannelPlatformError> {
        self.ensure_operator_visible(connector_id)?;
        self.supervisor.queue_snapshot(connector_id).map_err(ChannelPlatformError::from)
    }

    /// Pauses or resumes the connector's outbound queue, recording the
    /// operator-supplied reason.
    ///
    /// # Errors
    /// Returns visibility or supervisor errors.
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

    /// Requeues one dead letter for delivery.
    ///
    /// # Errors
    /// Returns visibility or supervisor errors (including unknown ids).
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

    /// Discards one dead letter permanently.
    ///
    /// # Errors
    /// Returns visibility or supervisor errors (including unknown ids).
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

    /// Returns the stored instance record for an operator-visible
    /// connector.
    ///
    /// # Errors
    /// Returns visibility/store errors, and a supervisor `NotFound` when
    /// the status exists but the instance row is missing.
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

    /// Reads messages from the connector's conversation history.
    ///
    /// # Errors
    /// Returns visibility errors, [`ChannelPlatformError::InvalidInput`]
    /// when the request fails validation, and supervisor errors from the
    /// adapter call.
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

    /// Searches the connector's messages.
    ///
    /// # Errors
    /// Same shape as [`Self::read_messages`].
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

    /// Edits a message through the connector adapter; governance and
    /// approval gating happen in the calling layer.
    ///
    /// # Errors
    /// Same shape as [`Self::read_messages`].
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

    /// Deletes a message through the connector adapter; governance and
    /// approval gating happen in the calling layer.
    ///
    /// # Errors
    /// Same shape as [`Self::read_messages`].
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

    /// Adds a reaction through the connector adapter.
    ///
    /// # Errors
    /// Same shape as [`Self::read_messages`].
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

    /// Removes a reaction through the connector adapter.
    ///
    /// # Errors
    /// Same shape as [`Self::read_messages`].
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

    /// Fetches the single message a mutation targets, for approval
    /// previews; `None` when the message is no longer readable.
    ///
    /// # Errors
    /// Same shape as [`Self::read_messages`].
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

    /// Ingests a synthetic inbound message as if the connector had
    /// received it, exercising the full routing path.
    ///
    /// # Errors
    /// Returns [`ChannelPlatformError::UnsupportedConnector`] for
    /// internal-test-only connectors,
    /// [`ChannelPlatformError::InvalidInput`] for empty fields, and
    /// visibility/supervisor errors from ingestion.
    pub async fn submit_test_message(
        &self,
        connector_id: &str,
        request: ChannelTestMessageRequest,
    ) -> Result<InboundIngestOutcome, ChannelPlatformError> {
        let status = self.ensure_operator_visible(connector_id)?;
        if status.availability == ConnectorAvailability::InternalTestOnly {
            return Err(ChannelPlatformError::UnsupportedConnector(format!(
                "connector '{}' is internal_test_only and does not support user-facing channel tests; run `palyra message capabilities {}` to inspect supported message actions",
                connector_id.trim(),
                connector_id.trim()
            )));
        }
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

    /// Drains due outbound messages across all connectors with the
    /// background batch size.
    ///
    /// # Errors
    /// Returns supervisor errors from the drain.
    pub async fn drain_due(&self) -> Result<palyra_connectors::DrainOutcome, ChannelPlatformError> {
        self.supervisor
            .process_due_ingress(self.supervisor_config().background_drain_batch_size)
            .await
            .map_err(ChannelPlatformError::from)?;
        self.supervisor
            .drain_due_outbox(self.supervisor_config().background_drain_batch_size)
            .await
            .map_err(ChannelPlatformError::from)
    }

    /// Force-drains the outbox of one connector, bypassing its pause
    /// state.
    ///
    /// # Errors
    /// Returns visibility or supervisor errors.
    pub async fn drain_due_for_connector(
        &self,
        connector_id: &str,
    ) -> Result<DrainOutcome, ChannelPlatformError> {
        self.ensure_operator_visible(connector_id)?;
        self.supervisor
            .process_due_ingress_for_connector(
                connector_id,
                self.supervisor_config().background_drain_batch_size,
                true,
            )
            .await
            .map_err(ChannelPlatformError::from)?;
        self.supervisor
            .drain_due_outbox_for_connector_force(
                connector_id,
                self.supervisor_config().background_drain_batch_size,
            )
            .await
            .map_err(ChannelPlatformError::from)
    }

    /// Polls connectors for inbound messages and returns how many were
    /// ingested.
    ///
    /// # Errors
    /// Returns supervisor errors from the poll.
    pub async fn poll_inbound(&self) -> Result<usize, ChannelPlatformError> {
        self.supervisor
            .poll_inbound(self.supervisor_config().immediate_drain_batch_size)
            .await
            .map_err(ChannelPlatformError::from)
    }

    /// Returns the background worker tick interval.
    #[must_use]
    pub fn worker_interval(&self) -> Duration {
        self.worker_interval
    }

    /// Spawns the background worker that polls inbound traffic and drains
    /// the outbox every tick; errors are logged and the loop continues.
    /// The task runs for the daemon's lifetime; dropping the handle leaves
    /// it running.
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
        self.supervisor_config.clone()
    }

    /// Recovers the provider-native message id of a delivered envelope by
    /// scanning recent `outbox.delivered` log events; `None` when delivery
    /// happened outside the scanned window.
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

    /// Visibility gate shared by every operator entry point: deferred
    /// connectors stay in storage but are rejected here.
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

    /// Registers any default connectors missing from storage; existing
    /// rows (and their operator-set state) are left untouched.
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

fn connector_supervisor_config_from_env() -> Result<ConnectorSupervisorConfig, ChannelPlatformError>
{
    let mut config = ConnectorSupervisorConfig::default();
    if let Some(mode) = optional_env("PALYRA_CHANNEL_DELIVERY_PIPELINE_MODE") {
        config.delivery_pipeline_mode =
            DeliveryPipelineMode::parse(mode.as_str()).ok_or_else(|| {
                ChannelPlatformError::InvalidInput(
                    "PALYRA_CHANNEL_DELIVERY_PIPELINE_MODE must be off, shadow, or enforce"
                        .to_owned(),
                )
            })?;
    }
    if let Some(value) = optional_env("PALYRA_CHANNEL_INGRESS_CLAIM_LEASE_MS") {
        config.ingress_claim_lease_ms =
            parse_positive_i64_env("PALYRA_CHANNEL_INGRESS_CLAIM_LEASE_MS", value.as_str())?;
    }
    if let Some(value) = optional_env("PALYRA_CHANNEL_INGRESS_MAX_RETRY_ATTEMPTS") {
        config.max_ingress_retry_attempts =
            parse_positive_u32_env("PALYRA_CHANNEL_INGRESS_MAX_RETRY_ATTEMPTS", value.as_str())?;
    }
    Ok(config)
}

fn optional_env(name: &str) -> Option<String> {
    env::var(name).ok().filter(|value| !value.trim().is_empty())
}

fn parse_positive_i64_env(name: &str, value: &str) -> Result<i64, ChannelPlatformError> {
    value.trim().parse::<i64>().ok().filter(|parsed| *parsed > 0).ok_or_else(|| {
        ChannelPlatformError::InvalidInput(format!("{name} must be a positive integer"))
    })
}

fn parse_positive_u32_env(name: &str, value: &str) -> Result<u32, ChannelPlatformError> {
    value.trim().parse::<u32>().ok().filter(|parsed| *parsed > 0).ok_or_else(|| {
        ChannelPlatformError::InvalidInput(format!("{name} must be a positive integer"))
    })
}

/// Connector-to-gateway bridge: canonicalizes Discord identities,
/// preprocesses attachments through the media store, and forwards the
/// envelope to the daemon gateway's RouteMessage RPC under the connector
/// principal.
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
                    // Only Discord identities are platform-authenticated;
                    // test/echo senders must stay unverified so allowlist
                    // gates that require verification reject them.
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
            session_id: response.session_id.map(|value| value.ulid),
            run_id: response.run_id.map(|value| value.ulid),
            retry_attempt: response.retry_attempt,
            route_message_latency_ms: Some(route_message_latency_ms),
        })
    }
}

#[cfg(test)]
mod tests {
    use palyra_connectors::{
        providers::provider_availability, AttachmentKind, AttachmentRef, ConnectorAvailability,
        ConnectorInstanceSpec, ConnectorKind, ConnectorSupervisorConfig, ConnectorSupervisorError,
    };
    use tempfile::TempDir;

    use super::attachments::render_attachment_context;
    use super::{
        default_connector_specs, discord, discord_connector_id, discord_token_vault_ref,
        from_proto_a2ui_update, normalize_discord_account_id, parse_positive_i64_env,
        parse_positive_u32_env, resolve_connector_gateway_auth, route_message_max_payload_bytes,
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
    fn channel_rollout_env_parsers_reject_non_positive_values() {
        assert_eq!(
            parse_positive_i64_env("PALYRA_CHANNEL_INGRESS_CLAIM_LEASE_MS", "250")
                .expect("positive lease should parse"),
            250
        );
        assert!(
            parse_positive_i64_env("PALYRA_CHANNEL_INGRESS_CLAIM_LEASE_MS", "0").is_err(),
            "zero claim lease should fail fast"
        );
        assert_eq!(
            parse_positive_u32_env("PALYRA_CHANNEL_INGRESS_MAX_RETRY_ATTEMPTS", "3")
                .expect("positive max retry count should parse"),
            3
        );
        assert!(
            parse_positive_u32_env("PALYRA_CHANNEL_INGRESS_MAX_RETRY_ATTEMPTS", "not-a-number")
                .is_err(),
            "invalid retry count should fail fast"
        );
    }

    #[test]
    fn default_connector_specs_match_discord_first_runtime_scope() {
        let specs = default_connector_specs();
        let inventory = specs
            .iter()
            .map(|spec| (spec.connector_id.clone(), spec.kind, provider_availability(spec.kind)))
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
