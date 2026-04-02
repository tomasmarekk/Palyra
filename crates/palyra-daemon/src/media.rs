use std::{
    fs,
    io::Write,
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::Mutex,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use base64::{engine::general_purpose::STANDARD as BASE64_STANDARD, Engine as _};
use reqwest::{redirect::Policy, Client as HttpClient, Url};
use rusqlite::{params, Connection, OptionalExtension};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

use palyra_common::netguard;
use palyra_connectors::{AttachmentKind, AttachmentRef};

use crate::media_derived::{
    select_prompt_chunks, DerivedArtifactAnchor, DerivedArtifactContent, DerivedArtifactKind,
    DerivedArtifactWarning, DerivedSelectionCandidate,
};

const DEFAULT_ALLOWED_SOURCE_HOSTS: &[&str] =
    &["cdn.discordapp.com", "*.discordapp.com", "*.discordapp.net"];
const DEFAULT_ALLOWED_DOWNLOAD_CONTENT_TYPES: &[&str] =
    &["image/png", "image/jpeg", "image/webp", "image/gif", "text/plain", "application/json"];
const DEFAULT_VISION_CONTENT_TYPES: &[&str] = &["image/png", "image/jpeg", "image/webp"];
const DEFAULT_UPLOAD_CONTENT_TYPES: &[&str] =
    &["image/png", "image/jpeg", "image/webp", "image/gif", "text/plain", "application/json"];
const DEFAULT_MAX_ATTACHMENTS_PER_MESSAGE: usize = 4;
const DEFAULT_MAX_TOTAL_ATTACHMENT_BYTES_PER_MESSAGE: u64 = 8 * 1024 * 1024;
const DEFAULT_MAX_DOWNLOAD_BYTES: usize = 5 * 1024 * 1024;
const DEFAULT_MAX_REDIRECTS: usize = 3;
const DEFAULT_MAX_STORE_BYTES: u64 = 256 * 1024 * 1024;
const DEFAULT_MAX_STORE_ARTIFACTS: usize = 2_048;
const DEFAULT_RETENTION_TTL_MS: i64 = 7 * 24 * 60 * 60 * 1_000;
const DEFAULT_VISION_MAX_IMAGE_COUNT: usize = 3;
const DEFAULT_VISION_MAX_IMAGE_BYTES: usize = 4 * 1024 * 1024;
const DEFAULT_VISION_MAX_TOTAL_BYTES: usize = 8 * 1024 * 1024;
const DEFAULT_VISION_MAX_DIMENSION_PX: u32 = 2_048;
const DEFAULT_OUTBOUND_MAX_UPLOAD_BYTES: usize = 4 * 1024 * 1024;
const RECENT_EVENT_LIMIT: usize = 10;
const RETENTION_PRUNE_MIN_INTERVAL_MS: i64 = 30_000;
const RETENTION_PRUNE_MAX_DEFERRED_INGESTS: u32 = 16;

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct MediaRuntimeConfig {
    pub download_enabled: bool,
    pub outbound_upload_enabled: bool,
    pub allow_http_fixture_urls: bool,
    pub max_attachments_per_message: usize,
    pub max_total_attachment_bytes_per_message: u64,
    pub max_download_bytes: usize,
    pub max_redirects: usize,
    pub allowed_source_hosts: Vec<String>,
    pub allowed_download_content_types: Vec<String>,
    pub vision_allowed_content_types: Vec<String>,
    pub vision_max_image_count: usize,
    pub vision_max_image_bytes: usize,
    pub vision_max_total_bytes: usize,
    pub vision_max_dimension_px: u32,
    pub outbound_allowed_content_types: Vec<String>,
    pub outbound_max_upload_bytes: usize,
    pub store_max_bytes: u64,
    pub store_max_artifacts: usize,
    pub retention_ttl_ms: i64,
}

impl Default for MediaRuntimeConfig {
    fn default() -> Self {
        Self {
            download_enabled: false,
            outbound_upload_enabled: false,
            allow_http_fixture_urls: false,
            max_attachments_per_message: DEFAULT_MAX_ATTACHMENTS_PER_MESSAGE,
            max_total_attachment_bytes_per_message: DEFAULT_MAX_TOTAL_ATTACHMENT_BYTES_PER_MESSAGE,
            max_download_bytes: DEFAULT_MAX_DOWNLOAD_BYTES,
            max_redirects: DEFAULT_MAX_REDIRECTS,
            allowed_source_hosts: DEFAULT_ALLOWED_SOURCE_HOSTS
                .iter()
                .map(|value| (*value).to_owned())
                .collect(),
            allowed_download_content_types: DEFAULT_ALLOWED_DOWNLOAD_CONTENT_TYPES
                .iter()
                .map(|value| (*value).to_owned())
                .collect(),
            vision_allowed_content_types: DEFAULT_VISION_CONTENT_TYPES
                .iter()
                .map(|value| (*value).to_owned())
                .collect(),
            vision_max_image_count: DEFAULT_VISION_MAX_IMAGE_COUNT,
            vision_max_image_bytes: DEFAULT_VISION_MAX_IMAGE_BYTES,
            vision_max_total_bytes: DEFAULT_VISION_MAX_TOTAL_BYTES,
            vision_max_dimension_px: DEFAULT_VISION_MAX_DIMENSION_PX,
            outbound_allowed_content_types: DEFAULT_UPLOAD_CONTENT_TYPES
                .iter()
                .map(|value| (*value).to_owned())
                .collect(),
            outbound_max_upload_bytes: DEFAULT_OUTBOUND_MAX_UPLOAD_BYTES,
            store_max_bytes: DEFAULT_MAX_STORE_BYTES,
            store_max_artifacts: DEFAULT_MAX_STORE_ARTIFACTS,
            retention_ttl_ms: DEFAULT_RETENTION_TTL_MS,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct MediaConnectorSnapshot {
    pub policy: MediaRuntimeConfig,
    pub usage: MediaUsageSnapshot,
    pub retention: MediaRetentionSnapshot,
    pub recent_blocked_reasons: Vec<MediaEventSnapshot>,
    pub recent_upload_failures: Vec<MediaEventSnapshot>,
}

#[derive(Debug, Clone, Serialize)]
pub struct MediaGlobalSnapshot {
    pub policy: MediaRuntimeConfig,
    pub usage: MediaUsageSnapshot,
    pub retention: MediaRetentionSnapshot,
    pub recent_blocked_reasons: Vec<MediaEventSnapshot>,
    pub recent_upload_failures: Vec<MediaEventSnapshot>,
}

#[derive(Debug, Clone, Serialize, Default)]
pub struct MediaUsageSnapshot {
    pub artifact_count: u64,
    pub stored_content_count: u64,
    pub stored_bytes: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct MediaRetentionSnapshot {
    pub max_store_bytes: u64,
    pub max_store_artifacts: usize,
    pub ttl_ms: i64,
}

#[derive(Debug, Clone, Serialize)]
pub struct MediaEventSnapshot {
    pub event_type: String,
    pub artifact_id: Option<String>,
    pub attachment_id: Option<String>,
    pub filename: Option<String>,
    pub reason: String,
    pub created_at_unix_ms: i64,
}

#[derive(Debug, Clone)]
pub struct MediaArtifactPayload {
    pub artifact_id: String,
    pub filename: String,
    pub content_type: String,
    pub size_bytes: u64,
    pub sha256: String,
    pub width_px: Option<u32>,
    pub height_px: Option<u32>,
    pub bytes: Vec<u8>,
}

#[derive(Debug, Clone, Serialize)]
pub struct MediaDerivedStatsSnapshot {
    pub total: u64,
    pub pending: u64,
    pub succeeded: u64,
    pub failed: u64,
    pub quarantined: u64,
    pub purged: u64,
    pub recompute_required: u64,
    pub orphaned: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct MediaDerivedArtifactRecord {
    pub derived_artifact_id: String,
    pub source_artifact_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub attachment_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub principal: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub device_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub channel: Option<String>,
    pub filename: String,
    pub declared_content_type: String,
    pub kind: String,
    pub state: String,
    pub parser_name: String,
    pub parser_version: String,
    pub source_content_hash: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub content_hash: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub content_text: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub summary_text: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub language: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub duration_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub processing_ms: Option<u64>,
    pub warnings: Vec<DerivedArtifactWarning>,
    pub anchors: Vec<DerivedArtifactAnchor>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub failure_reason: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub quarantine_reason: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub workspace_document_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub memory_item_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub background_task_id: Option<String>,
    pub recompute_required: bool,
    pub orphaned: bool,
    pub created_at_unix_ms: i64,
    pub updated_at_unix_ms: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub purged_at_unix_ms: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MediaDerivedArtifactSelection {
    pub derived_artifact_id: String,
    pub source_artifact_id: String,
    pub kind: String,
    pub citation: String,
    pub label: String,
    pub snippet: String,
    pub score: f64,
}

#[derive(Debug, Clone)]
pub struct MediaDerivedArtifactUpsertRequest<'a> {
    pub source_artifact_id: &'a str,
    pub attachment_id: Option<&'a str>,
    pub session_id: Option<&'a str>,
    pub principal: Option<&'a str>,
    pub device_id: Option<&'a str>,
    pub channel: Option<&'a str>,
    pub filename: &'a str,
    pub declared_content_type: &'a str,
    pub source_content_hash: &'a str,
    pub background_task_id: Option<&'a str>,
    pub derived: &'a DerivedArtifactContent,
}

#[derive(Debug, Clone)]
pub struct ConsoleAttachmentStoreRequest<'a> {
    pub connector_id: &'a str,
    pub session_id: &'a str,
    pub principal: &'a str,
    pub device_id: &'a str,
    pub channel: Option<&'a str>,
    pub attachment_id: &'a str,
    pub filename: &'a str,
    pub declared_content_type: &'a str,
    pub bytes: &'a [u8],
}

#[derive(Debug)]
pub struct InboundAttachmentIngestRequest<'a> {
    pub connector_id: &'a str,
    pub envelope_id: &'a str,
    pub conversation_id: &'a str,
    pub adapter_message_id: Option<&'a str>,
    pub attachment: &'a AttachmentRef,
    pub attachment_index: usize,
    pub attachment_count: usize,
    pub total_declared_bytes: u64,
}

#[derive(Debug, thiserror::Error)]
pub enum MediaStoreError {
    #[error("io failure: {0}")]
    Io(String),
    #[error("sqlite failure: {0}")]
    Sql(#[from] rusqlite::Error),
    #[error("invalid media attachment: {0}")]
    InvalidAttachment(String),
    #[error("network policy rejected media attachment: {0}")]
    NetworkPolicy(String),
    #[error("download failed: {0}")]
    Download(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MediaDirection {
    Inbound,
    Outbound,
}

impl MediaDirection {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Inbound => "inbound",
            Self::Outbound => "outbound",
        }
    }
}

#[derive(Debug, Clone)]
struct MediaEventRecord<'a> {
    event_type: &'a str,
    artifact_id: Option<&'a str>,
    attachment_id: Option<&'a str>,
    filename: Option<&'a str>,
    reason: String,
    details: Value,
}

#[derive(Debug, Clone)]
struct StoredArtifactRecord {
    artifact_id: String,
    filename: String,
    content_type: String,
    size_bytes: u64,
    sha256: String,
    width_px: Option<u32>,
    height_px: Option<u32>,
    storage_rel_path: String,
}

#[derive(Debug, Clone)]
struct SniffedContent {
    content_type: String,
    width_px: Option<u32>,
    height_px: Option<u32>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct ConsoleAttachmentOriginRecord {
    principal: String,
    device_id: String,
    channel: Option<String>,
    session_id: String,
}

#[derive(Debug, Clone)]
struct MediaDerivedArtifactRow {
    record: MediaDerivedArtifactRecord,
}

pub struct MediaArtifactStore {
    content_root: PathBuf,
    config: MediaRuntimeConfig,
    http_client: HttpClient,
    connection: Mutex<Connection>,
    maintenance: Mutex<MediaMaintenanceState>,
}

impl MediaArtifactStore {
    pub fn open(
        db_path: PathBuf,
        content_root: PathBuf,
        config: MediaRuntimeConfig,
    ) -> Result<Self, MediaStoreError> {
        if let Some(parent) = db_path.parent() {
            fs::create_dir_all(parent).map_err(|error| {
                MediaStoreError::Io(format!(
                    "failed to create media db parent '{}' : {error}",
                    parent.display()
                ))
            })?;
        }
        fs::create_dir_all(content_root.as_path()).map_err(|error| {
            MediaStoreError::Io(format!(
                "failed to create media content root '{}' : {error}",
                content_root.display()
            ))
        })?;
        let connection = Connection::open(&db_path)?;
        let store = Self {
            content_root,
            config,
            http_client: build_media_http_client()?,
            connection: Mutex::new(connection),
            maintenance: Mutex::new(MediaMaintenanceState {
                last_retention_prune_unix_ms: current_unix_ms()
                    .saturating_sub(RETENTION_PRUNE_MIN_INTERVAL_MS),
                deferred_ingests: 0,
            }),
        };
        store.migrate()?;
        Ok(store)
    }

    #[must_use]
    pub fn config(&self) -> &MediaRuntimeConfig {
        &self.config
    }

    pub async fn ingest_inbound_attachment(
        &self,
        request: InboundAttachmentIngestRequest<'_>,
    ) -> Result<AttachmentRef, MediaStoreError> {
        let mut attachment = request.attachment.clone();
        attachment.origin = Some("discord_inbound".to_owned());
        if request.attachment_count > self.config.max_attachments_per_message {
            let reason = format!(
                "attachment_count_exceeded ({}/{})",
                request.attachment_count, self.config.max_attachments_per_message
            );
            attachment.policy_context = Some(reason.clone());
            self.record_event(
                request.connector_id,
                MediaDirection::Inbound,
                MediaEventRecord {
                    event_type: "attachment.metadata.blocked",
                    artifact_id: None,
                    attachment_id: attachment.attachment_id.as_deref(),
                    filename: attachment.filename.as_deref(),
                    reason,
                    details: json!({
                    "envelope_id": request.envelope_id,
                    "conversation_id": request.conversation_id,
                    }),
                },
            )?;
            return Ok(attachment);
        }
        if request.total_declared_bytes > self.config.max_total_attachment_bytes_per_message {
            let reason = format!(
                "attachment_total_bytes_exceeded ({}/{})",
                request.total_declared_bytes, self.config.max_total_attachment_bytes_per_message
            );
            attachment.policy_context = Some(reason.clone());
            self.record_event(
                request.connector_id,
                MediaDirection::Inbound,
                MediaEventRecord {
                    event_type: "attachment.metadata.blocked",
                    artifact_id: None,
                    attachment_id: attachment.attachment_id.as_deref(),
                    filename: attachment.filename.as_deref(),
                    reason,
                    details: json!({
                    "envelope_id": request.envelope_id,
                    "conversation_id": request.conversation_id,
                    }),
                },
            )?;
            return Ok(attachment);
        }

        if !metadata_type_is_safe(&attachment) {
            let reason = "attachment_metadata_type_blocked".to_owned();
            attachment.policy_context = Some(reason.clone());
            self.record_event(
                request.connector_id,
                MediaDirection::Inbound,
                MediaEventRecord {
                    event_type: "attachment.metadata.blocked",
                    artifact_id: None,
                    attachment_id: attachment.attachment_id.as_deref(),
                    filename: attachment.filename.as_deref(),
                    reason,
                    details: json!({
                    "envelope_id": request.envelope_id,
                    "attachment_index": request.attachment_index,
                    }),
                },
            )?;
            return Ok(attachment);
        }

        if !self.config.download_enabled {
            attachment.policy_context = Some("attachment.download.disabled".to_owned());
            self.record_event(
                request.connector_id,
                MediaDirection::Inbound,
                MediaEventRecord {
                    event_type: "attachment.download.blocked",
                    artifact_id: None,
                    attachment_id: attachment.attachment_id.as_deref(),
                    filename: attachment.filename.as_deref(),
                    reason: "attachment.download disabled by config".to_owned(),
                    details: json!({
                    "envelope_id": request.envelope_id,
                    "conversation_id": request.conversation_id,
                    }),
                },
            )?;
            return Ok(attachment);
        }

        let source_url = attachment
            .url
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .ok_or_else(|| {
                MediaStoreError::InvalidAttachment("attachment source URL is missing".to_owned())
            })?;
        let stored = self
            .download_and_store_discord_attachment(
                request.connector_id,
                request.envelope_id,
                request.conversation_id,
                request.adapter_message_id,
                &attachment,
                source_url,
            )
            .await
            .map_err(|error| match error {
                MediaStoreError::Io(_) | MediaStoreError::Sql(_) => error,
                MediaStoreError::InvalidAttachment(reason)
                | MediaStoreError::NetworkPolicy(reason)
                | MediaStoreError::Download(reason) => {
                    attachment.policy_context = Some(reason.clone());
                    let _ = self.record_event(
                        request.connector_id,
                        MediaDirection::Inbound,
                        MediaEventRecord {
                            event_type: "attachment.download.blocked",
                            artifact_id: None,
                            attachment_id: attachment.attachment_id.as_deref(),
                            filename: attachment.filename.as_deref(),
                            reason,
                            details: json!({
                            "envelope_id": request.envelope_id,
                            "conversation_id": request.conversation_id,
                            }),
                        },
                    );
                    MediaStoreError::InvalidAttachment("attachment download blocked".to_owned())
                }
            });
        let stored = match stored {
            Ok(stored) => stored,
            Err(MediaStoreError::InvalidAttachment(reason))
                if reason == "attachment download blocked" =>
            {
                return Ok(attachment);
            }
            Err(error) => return Err(error),
        };
        attachment.artifact_ref = Some(stored.artifact_id.clone());
        attachment.content_hash = Some(stored.sha256.clone());
        attachment.content_type = Some(stored.content_type.clone());
        attachment.size_bytes = Some(stored.size_bytes);
        attachment.inline_base64 = self
            .load_artifact_payload(stored.artifact_id.as_str())?
            .map(|payload| BASE64_STANDARD.encode(payload.bytes.as_slice()));
        attachment.width_px = stored.width_px;
        attachment.height_px = stored.height_px;
        attachment.policy_context = Some("attachment.download.allowed".to_owned());
        Ok(attachment)
    }

    pub fn load_artifact_payload(
        &self,
        artifact_id: &str,
    ) -> Result<Option<MediaArtifactPayload>, MediaStoreError> {
        let guard = self.connection.lock().map_err(|_| {
            MediaStoreError::Io("media artifact db lock poisoned while loading artifact".to_owned())
        })?;
        let Some(record) = guard
            .query_row(
                r#"
                SELECT
                    a.artifact_id,
                    COALESCE(a.filename, 'artifact.bin'),
                    COALESCE(a.content_type, ''),
                    a.size_bytes,
                    COALESCE(a.content_sha256, ''),
                    a.width_px,
                    a.height_px,
                    c.storage_rel_path
                FROM media_artifacts a
                JOIN media_contents c ON c.content_sha256 = a.content_sha256
                WHERE a.artifact_id = ?1
                "#,
                params![artifact_id],
                |row| {
                    Ok(StoredArtifactRecord {
                        artifact_id: row.get(0)?,
                        filename: row.get(1)?,
                        content_type: row.get(2)?,
                        size_bytes: u64::try_from(row.get::<_, i64>(3)?).unwrap_or_default(),
                        sha256: row.get(4)?,
                        width_px: row.get::<_, Option<u32>>(5)?,
                        height_px: row.get::<_, Option<u32>>(6)?,
                        storage_rel_path: row.get(7)?,
                    })
                },
            )
            .optional()?
        else {
            return Ok(None);
        };
        drop(guard);
        let storage_path = resolve_content_storage_path(
            self.content_root.as_path(),
            record.storage_rel_path.as_str(),
            record.sha256.as_str(),
        )?;
        let bytes = fs::read(storage_path.as_path()).map_err(|error| {
            MediaStoreError::Io(format!(
                "failed to read media artifact '{}' from '{}' : {error}",
                artifact_id,
                storage_path.display()
            ))
        })?;
        Ok(Some(MediaArtifactPayload {
            artifact_id: record.artifact_id,
            filename: record.filename,
            content_type: record.content_type,
            size_bytes: record.size_bytes,
            sha256: record.sha256,
            width_px: record.width_px,
            height_px: record.height_px,
            bytes,
        }))
    }

    pub fn store_console_attachment(
        &self,
        request: ConsoleAttachmentStoreRequest<'_>,
    ) -> Result<MediaArtifactPayload, MediaStoreError> {
        if request.bytes.is_empty() {
            let _ = self.record_upload_failure(
                request.connector_id,
                None,
                Some(request.filename),
                "attachment.upload.empty",
            );
            return Err(MediaStoreError::InvalidAttachment(
                "attachment bytes cannot be empty".to_owned(),
            ));
        }
        if request.bytes.len() > self.config.outbound_max_upload_bytes {
            let reason = format!(
                "attachment.upload.too_large ({}/{})",
                request.bytes.len(),
                self.config.outbound_max_upload_bytes
            );
            let _ = self.record_upload_failure(
                request.connector_id,
                None,
                Some(request.filename),
                reason.clone(),
            );
            return Err(MediaStoreError::InvalidAttachment(reason));
        }
        let sniffed = sniff_content(request.bytes)?;
        if !self
            .config
            .outbound_allowed_content_types
            .iter()
            .any(|allowed| allowed == &sniffed.content_type)
        {
            let reason = format!(
                "attachment content type '{}' is blocked by upload policy",
                sniffed.content_type
            );
            let _ = self.record_upload_failure(
                request.connector_id,
                None,
                Some(request.filename),
                reason.clone(),
            );
            return Err(MediaStoreError::InvalidAttachment(reason));
        }

        let now = current_unix_ms();
        let sha256 = sha256_hex(request.bytes);
        let artifact_id = ulid::Ulid::new().to_string();
        let (storage_path, relative_path) =
            prepare_content_storage_path(self.content_root.as_path(), sha256.as_str())?;
        match fs::OpenOptions::new().write(true).create_new(true).open(storage_path.as_path()) {
            Ok(mut file) => {
                file.write_all(request.bytes).map_err(|error| {
                    MediaStoreError::Io(format!(
                        "failed to persist media artifact '{}' : {error}",
                        storage_path.display()
                    ))
                })?;
            }
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(error) => {
                return Err(MediaStoreError::Io(format!(
                    "failed to persist media artifact '{}' : {error}",
                    storage_path.display()
                )));
            }
        }

        let filename = sanitize_filename(request.filename, sniffed.content_type.as_str());
        {
            let guard = self.connection.lock().map_err(|_| {
                MediaStoreError::Io(
                    "media artifact db lock poisoned while storing console upload".to_owned(),
                )
            })?;
            guard.execute(
                r#"
                INSERT INTO media_contents (
                    content_sha256, storage_rel_path, size_bytes, created_at_unix_ms,
                    last_accessed_at_unix_ms, ref_count
                )
                VALUES (?1, ?2, ?3, ?4, ?4, 1)
                ON CONFLICT(content_sha256) DO UPDATE SET
                    last_accessed_at_unix_ms = excluded.last_accessed_at_unix_ms,
                    ref_count = media_contents.ref_count + 1
                "#,
                params![
                    sha256,
                    relative_path,
                    i64::try_from(request.bytes.len()).unwrap_or(i64::MAX),
                    now
                ],
            )?;
            guard.execute(
                r#"
                INSERT INTO media_artifacts (
                    artifact_id, connector_id, direction, envelope_id, conversation_id,
                    adapter_message_id, attachment_id, kind, filename, declared_content_type,
                    content_type, source_url, content_sha256, size_bytes, width_px, height_px,
                    origin_json, policy_context_json, blocked_reason, created_at_unix_ms,
                    last_accessed_at_unix_ms
                )
                VALUES (?1, ?2, 'outbound', ?3, ?3, NULL, ?4, ?5, ?6, ?7, ?8, NULL, ?9, ?10, ?11, ?12, ?13, ?14, NULL, ?15, ?15)
                "#,
                params![
                    artifact_id,
                    request.connector_id,
                    request.session_id,
                    request.attachment_id,
                    attachment_kind_label(attachment_kind_for_content_type(
                        sniffed.content_type.as_str()
                    )),
                    filename,
                    request.declared_content_type,
                    sniffed.content_type,
                    sha256,
                    i64::try_from(request.bytes.len()).unwrap_or(i64::MAX),
                    sniffed.width_px,
                    sniffed.height_px,
                    json!(ConsoleAttachmentOriginRecord {
                        principal: request.principal.to_owned(),
                        device_id: request.device_id.to_owned(),
                        channel: request.channel.map(ToOwned::to_owned),
                        session_id: request.session_id.to_owned(),
                    })
                    .to_string(),
                    json!({
                        "upload_action": "attachment.upload",
                        "vision_action": if self.is_vision_eligible_content_type(sniffed.content_type.as_str()) {
                            "attachment.vision"
                        } else {
                            ""
                        },
                    })
                    .to_string(),
                    now,
                ],
            )?;
        }
        self.record_upload_success(request.connector_id, artifact_id.as_str(), filename.as_str())?;
        self.run_retention_housekeeping_if_due(now)?;
        Ok(MediaArtifactPayload {
            artifact_id,
            filename,
            content_type: sniffed.content_type,
            size_bytes: u64::try_from(request.bytes.len()).unwrap_or(u64::MAX),
            sha256,
            width_px: sniffed.width_px,
            height_px: sniffed.height_px,
            bytes: request.bytes.to_vec(),
        })
    }

    pub fn load_console_attachment(
        &self,
        artifact_id: &str,
        session_id: &str,
        principal: &str,
        device_id: &str,
        channel: Option<&str>,
    ) -> Result<Option<MediaArtifactPayload>, MediaStoreError> {
        let guard = self.connection.lock().map_err(|_| {
            MediaStoreError::Io(
                "media artifact db lock poisoned while validating console attachment".to_owned(),
            )
        })?;
        let origin_json = guard
            .query_row(
                r#"
                SELECT COALESCE(origin_json, '')
                FROM media_artifacts
                WHERE artifact_id = ?1
                  AND connector_id = 'console_chat'
                  AND direction = 'outbound'
                  AND conversation_id = ?2
                "#,
                params![artifact_id, session_id],
                |row| row.get::<_, String>(0),
            )
            .optional()?;
        drop(guard);
        let Some(origin_json) = origin_json else {
            return Ok(None);
        };
        let Ok(origin) =
            serde_json::from_str::<ConsoleAttachmentOriginRecord>(origin_json.as_str())
        else {
            return Ok(None);
        };
        if origin.principal != principal
            || origin.device_id != device_id
            || origin.session_id != session_id
            || origin.channel.as_deref() != channel
        {
            return Ok(None);
        }
        self.load_artifact_payload(artifact_id)
    }

    pub fn list_console_attachment_payloads(
        &self,
        session_id: &str,
        principal: &str,
        device_id: &str,
        channel: Option<&str>,
    ) -> Result<Vec<MediaArtifactPayload>, MediaStoreError> {
        let guard = self.connection.lock().map_err(|_| {
            MediaStoreError::Io(
                "media artifact db lock poisoned while listing console attachments".to_owned(),
            )
        })?;
        let mut statement = guard.prepare(
            r#"
            SELECT artifact_id
            FROM media_artifacts
            WHERE connector_id = 'console_chat'
              AND direction = 'outbound'
              AND conversation_id = ?1
            ORDER BY created_at_unix_ms ASC
            "#,
        )?;
        let artifact_ids = statement
            .query_map(params![session_id], |row| row.get::<_, String>(0))?
            .collect::<Result<Vec<_>, _>>()?;
        drop(statement);
        drop(guard);
        let mut payloads = Vec::new();
        for artifact_id in artifact_ids {
            if let Some(payload) = self.load_console_attachment(
                artifact_id.as_str(),
                session_id,
                principal,
                device_id,
                channel,
            )? {
                payloads.push(payload);
            }
        }
        Ok(payloads)
    }

    pub fn upsert_derived_artifact(
        &self,
        request: MediaDerivedArtifactUpsertRequest<'_>,
    ) -> Result<MediaDerivedArtifactRecord, MediaStoreError> {
        let now = current_unix_ms();
        let derived_artifact_id = ulid::Ulid::new().to_string();
        let warnings_json =
            serde_json::to_string(&request.derived.warnings).unwrap_or_else(|_| "[]".to_owned());
        let anchors_json =
            serde_json::to_string(&request.derived.anchors).unwrap_or_else(|_| "[]".to_owned());
        let guard = self.connection.lock().map_err(|_| {
            MediaStoreError::Io(
                "media artifact db lock poisoned while upserting derived artifact".to_owned(),
            )
        })?;
        guard.execute(
            r#"
            INSERT INTO media_derived_artifacts (
                derived_artifact_id,
                source_artifact_id,
                attachment_id,
                session_id,
                principal,
                device_id,
                channel,
                filename,
                declared_content_type,
                kind,
                state,
                parser_name,
                parser_version,
                source_content_hash,
                content_hash,
                content_text,
                summary_text,
                language,
                duration_ms,
                processing_ms,
                warnings_json,
                anchors_json,
                failure_reason,
                quarantine_reason,
                workspace_document_id,
                memory_item_id,
                background_task_id,
                recompute_required,
                orphaned,
                created_at_unix_ms,
                updated_at_unix_ms,
                purged_at_unix_ms
            ) VALUES (
                ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, 'succeeded', ?11, ?12, ?13, ?14, ?15,
                ?16, ?17, ?18, ?19, ?20, ?21, NULL, NULL, NULL, NULL, ?22, 0, 0, ?23, ?23, NULL
            )
            ON CONFLICT(source_artifact_id, kind) DO UPDATE SET
                state = 'succeeded',
                parser_name = excluded.parser_name,
                parser_version = excluded.parser_version,
                source_content_hash = excluded.source_content_hash,
                content_hash = excluded.content_hash,
                content_text = excluded.content_text,
                summary_text = excluded.summary_text,
                language = excluded.language,
                duration_ms = excluded.duration_ms,
                processing_ms = excluded.processing_ms,
                warnings_json = excluded.warnings_json,
                anchors_json = excluded.anchors_json,
                failure_reason = NULL,
                quarantine_reason = NULL,
                background_task_id = excluded.background_task_id,
                recompute_required = 0,
                orphaned = 0,
                updated_at_unix_ms = excluded.updated_at_unix_ms,
                purged_at_unix_ms = NULL
            "#,
            params![
                derived_artifact_id,
                request.source_artifact_id,
                request.attachment_id,
                request.session_id,
                request.principal,
                request.device_id,
                request.channel,
                request.filename,
                request.declared_content_type,
                request.derived.kind.as_str(),
                request.derived.parser_name,
                request.derived.parser_version,
                request.source_content_hash,
                request.derived.content_hash,
                request.derived.content_text,
                request.derived.summary_text,
                request.derived.language,
                request.derived.duration_ms.map(|value| i64::try_from(value).unwrap_or(i64::MAX)),
                i64::try_from(request.derived.processing_ms).unwrap_or(i64::MAX),
                warnings_json,
                anchors_json,
                request.background_task_id,
                now,
            ],
        )?;
        drop(guard);
        self.get_derived_artifact_by_source_kind(
            request.source_artifact_id,
            request.derived.kind.as_str(),
        )?
        .ok_or_else(|| {
            MediaStoreError::Io(
                "derived artifact upsert did not return a persisted record".to_owned(),
            )
        })
    }

    pub fn upsert_failed_derived_artifact(
        &self,
        source_artifact_id: &str,
        attachment_id: Option<&str>,
        session_id: Option<&str>,
        principal: Option<&str>,
        device_id: Option<&str>,
        channel: Option<&str>,
        filename: &str,
        declared_content_type: &str,
        source_content_hash: &str,
        kind: DerivedArtifactKind,
        parser_name: &str,
        parser_version: &str,
        background_task_id: Option<&str>,
        failure_reason: &str,
    ) -> Result<MediaDerivedArtifactRecord, MediaStoreError> {
        let now = current_unix_ms();
        let derived_artifact_id = ulid::Ulid::new().to_string();
        let guard = self.connection.lock().map_err(|_| {
            MediaStoreError::Io(
                "media artifact db lock poisoned while recording failed derived artifact"
                    .to_owned(),
            )
        })?;
        guard.execute(
            r#"
            INSERT INTO media_derived_artifacts (
                derived_artifact_id, source_artifact_id, attachment_id, session_id, principal,
                device_id, channel, filename, declared_content_type, kind, state, parser_name,
                parser_version, source_content_hash, content_hash, content_text, summary_text,
                language, duration_ms, processing_ms, warnings_json, anchors_json, failure_reason,
                quarantine_reason, workspace_document_id, memory_item_id, background_task_id,
                recompute_required, orphaned, created_at_unix_ms, updated_at_unix_ms, purged_at_unix_ms
            ) VALUES (
                ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, 'failed', ?11, ?12, ?13, NULL, NULL,
                NULL, NULL, NULL, NULL, '[]', '[]', ?14, NULL, NULL, NULL, ?15, 0, 0, ?16, ?16, NULL
            )
            ON CONFLICT(source_artifact_id, kind) DO UPDATE SET
                state = 'failed',
                parser_name = excluded.parser_name,
                parser_version = excluded.parser_version,
                source_content_hash = excluded.source_content_hash,
                failure_reason = excluded.failure_reason,
                content_hash = NULL,
                content_text = NULL,
                summary_text = NULL,
                warnings_json = '[]',
                anchors_json = '[]',
                background_task_id = excluded.background_task_id,
                recompute_required = 0,
                orphaned = 0,
                updated_at_unix_ms = excluded.updated_at_unix_ms,
                purged_at_unix_ms = NULL
            "#,
            params![
                derived_artifact_id,
                source_artifact_id,
                attachment_id,
                session_id,
                principal,
                device_id,
                channel,
                filename,
                declared_content_type,
                kind.as_str(),
                parser_name,
                parser_version,
                source_content_hash,
                failure_reason,
                background_task_id,
                now,
            ],
        )?;
        drop(guard);
        self.get_derived_artifact_by_source_kind(source_artifact_id, kind.as_str())?.ok_or_else(
            || {
                MediaStoreError::Io(
                    "failed derived artifact upsert did not return a persisted record".to_owned(),
                )
            },
        )
    }

    pub fn list_session_derived_artifacts(
        &self,
        session_id: &str,
        principal: &str,
        device_id: &str,
        channel: Option<&str>,
    ) -> Result<Vec<MediaDerivedArtifactRecord>, MediaStoreError> {
        let guard = self.connection.lock().map_err(|_| {
            MediaStoreError::Io(
                "media artifact db lock poisoned while listing session derived artifacts"
                    .to_owned(),
            )
        })?;
        let mut statement = guard.prepare(
            r#"
            SELECT *
            FROM media_derived_artifacts
            WHERE session_id = ?1
              AND principal = ?2
              AND device_id = ?3
              AND COALESCE(channel, '') = COALESCE(?4, '')
            ORDER BY created_at_unix_ms DESC
            "#,
        )?;
        let rows = statement
            .query_map(
                params![session_id, principal, device_id, channel],
                map_derived_artifact_row,
            )?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(rows.into_iter().map(|row| row.record).collect())
    }

    pub fn list_attachment_derived_artifacts(
        &self,
        source_artifact_id: &str,
    ) -> Result<Vec<MediaDerivedArtifactRecord>, MediaStoreError> {
        let guard = self.connection.lock().map_err(|_| {
            MediaStoreError::Io(
                "media artifact db lock poisoned while listing attachment derived artifacts"
                    .to_owned(),
            )
        })?;
        let mut statement = guard.prepare(
            r#"
            SELECT *
            FROM media_derived_artifacts
            WHERE source_artifact_id = ?1
            ORDER BY created_at_unix_ms ASC
            "#,
        )?;
        let rows = statement
            .query_map(params![source_artifact_id], map_derived_artifact_row)?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(rows.into_iter().map(|row| row.record).collect())
    }

    pub fn get_derived_artifact(
        &self,
        derived_artifact_id: &str,
    ) -> Result<Option<MediaDerivedArtifactRecord>, MediaStoreError> {
        let guard = self.connection.lock().map_err(|_| {
            MediaStoreError::Io(
                "media artifact db lock poisoned while loading derived artifact".to_owned(),
            )
        })?;
        guard
            .query_row(
                "SELECT * FROM media_derived_artifacts WHERE derived_artifact_id = ?1",
                params![derived_artifact_id],
                map_derived_artifact_row,
            )
            .optional()
            .map(|row| row.map(|value| value.record))
            .map_err(Into::into)
    }

    pub fn get_derived_artifact_by_source_kind(
        &self,
        source_artifact_id: &str,
        kind: &str,
    ) -> Result<Option<MediaDerivedArtifactRecord>, MediaStoreError> {
        let guard = self.connection.lock().map_err(|_| {
            MediaStoreError::Io(
                "media artifact db lock poisoned while loading derived artifact by source"
                    .to_owned(),
            )
        })?;
        guard
            .query_row(
                "SELECT * FROM media_derived_artifacts WHERE source_artifact_id = ?1 AND kind = ?2",
                params![source_artifact_id, kind],
                map_derived_artifact_row,
            )
            .optional()
            .map(|row| row.map(|value| value.record))
            .map_err(Into::into)
    }

    pub fn list_linked_derived_artifacts(
        &self,
        workspace_document_id: Option<&str>,
        memory_item_id: Option<&str>,
        limit: usize,
    ) -> Result<Vec<MediaDerivedArtifactRecord>, MediaStoreError> {
        let Some(limit) = i64::try_from(limit.max(1)).ok() else {
            return Ok(Vec::new());
        };
        let guard = self.connection.lock().map_err(|_| {
            MediaStoreError::Io(
                "media artifact db lock poisoned while listing linked derived artifacts".to_owned(),
            )
        })?;
        let rows = match (workspace_document_id, memory_item_id) {
            (Some(workspace_document_id), Some(memory_item_id)) => {
                let mut statement = guard.prepare(
                    r#"
                    SELECT *
                    FROM media_derived_artifacts
                    WHERE workspace_document_id = ?1 OR memory_item_id = ?2
                    ORDER BY updated_at_unix_ms DESC
                    LIMIT ?3
                    "#,
                )?;
                let rows = statement
                    .query_map(
                        params![workspace_document_id, memory_item_id, limit],
                        map_derived_artifact_row,
                    )?
                    .collect::<Result<Vec<_>, _>>()?;
                rows
            }
            (Some(workspace_document_id), None) => {
                let mut statement = guard.prepare(
                    r#"
                    SELECT *
                    FROM media_derived_artifacts
                    WHERE workspace_document_id = ?1
                    ORDER BY updated_at_unix_ms DESC
                    LIMIT ?2
                    "#,
                )?;
                let rows = statement
                    .query_map(params![workspace_document_id, limit], map_derived_artifact_row)?
                    .collect::<Result<Vec<_>, _>>()?;
                rows
            }
            (None, Some(memory_item_id)) => {
                let mut statement = guard.prepare(
                    r#"
                    SELECT *
                    FROM media_derived_artifacts
                    WHERE memory_item_id = ?1
                    ORDER BY updated_at_unix_ms DESC
                    LIMIT ?2
                    "#,
                )?;
                let rows = statement
                    .query_map(params![memory_item_id, limit], map_derived_artifact_row)?
                    .collect::<Result<Vec<_>, _>>()?;
                rows
            }
            (None, None) => return Ok(Vec::new()),
        };
        Ok(rows.into_iter().map(|row| row.record).collect())
    }

    pub fn link_derived_artifact_targets(
        &self,
        derived_artifact_id: &str,
        workspace_document_id: Option<&str>,
        memory_item_id: Option<&str>,
    ) -> Result<(), MediaStoreError> {
        let guard = self.connection.lock().map_err(|_| {
            MediaStoreError::Io(
                "media artifact db lock poisoned while linking derived artifact targets".to_owned(),
            )
        })?;
        guard.execute(
            r#"
            UPDATE media_derived_artifacts
            SET workspace_document_id = COALESCE(?2, workspace_document_id),
                memory_item_id = COALESCE(?3, memory_item_id),
                updated_at_unix_ms = ?4
            WHERE derived_artifact_id = ?1
            "#,
            params![derived_artifact_id, workspace_document_id, memory_item_id, current_unix_ms()],
        )?;
        Ok(())
    }

    pub fn quarantine_derived_artifact(
        &self,
        derived_artifact_id: &str,
        reason: Option<&str>,
    ) -> Result<Option<MediaDerivedArtifactRecord>, MediaStoreError> {
        let now = current_unix_ms();
        let guard = self.connection.lock().map_err(|_| {
            MediaStoreError::Io(
                "media artifact db lock poisoned while quarantining derived artifact".to_owned(),
            )
        })?;
        guard.execute(
            r#"
            UPDATE media_derived_artifacts
            SET state = 'quarantined',
                quarantine_reason = ?2,
                updated_at_unix_ms = ?3
            WHERE derived_artifact_id = ?1
            "#,
            params![derived_artifact_id, reason, now],
        )?;
        drop(guard);
        self.get_derived_artifact(derived_artifact_id)
    }

    pub fn release_derived_artifact(
        &self,
        derived_artifact_id: &str,
    ) -> Result<Option<MediaDerivedArtifactRecord>, MediaStoreError> {
        let now = current_unix_ms();
        let guard = self.connection.lock().map_err(|_| {
            MediaStoreError::Io(
                "media artifact db lock poisoned while releasing derived artifact".to_owned(),
            )
        })?;
        guard.execute(
            r#"
            UPDATE media_derived_artifacts
            SET state = CASE
                    WHEN content_text IS NULL AND failure_reason IS NOT NULL THEN 'failed'
                    WHEN content_text IS NULL THEN state
                    ELSE 'succeeded'
                END,
                quarantine_reason = NULL,
                updated_at_unix_ms = ?2
            WHERE derived_artifact_id = ?1
            "#,
            params![derived_artifact_id, now],
        )?;
        drop(guard);
        self.get_derived_artifact(derived_artifact_id)
    }

    pub fn mark_derived_artifact_recompute_required(
        &self,
        derived_artifact_id: &str,
        required: bool,
    ) -> Result<Option<MediaDerivedArtifactRecord>, MediaStoreError> {
        let now = current_unix_ms();
        let guard = self.connection.lock().map_err(|_| {
            MediaStoreError::Io(
                "media artifact db lock poisoned while updating derived recompute state".to_owned(),
            )
        })?;
        guard.execute(
            r#"
            UPDATE media_derived_artifacts
            SET recompute_required = ?2,
                updated_at_unix_ms = ?3
            WHERE derived_artifact_id = ?1
            "#,
            params![derived_artifact_id, i64::from(required), now],
        )?;
        drop(guard);
        self.get_derived_artifact(derived_artifact_id)
    }

    pub fn purge_derived_artifact(
        &self,
        derived_artifact_id: &str,
    ) -> Result<Option<MediaDerivedArtifactRecord>, MediaStoreError> {
        let now = current_unix_ms();
        let guard = self.connection.lock().map_err(|_| {
            MediaStoreError::Io(
                "media artifact db lock poisoned while purging derived artifact".to_owned(),
            )
        })?;
        guard.execute(
            r#"
            UPDATE media_derived_artifacts
            SET state = 'purged',
                content_hash = NULL,
                content_text = NULL,
                summary_text = NULL,
                language = NULL,
                duration_ms = NULL,
                processing_ms = NULL,
                warnings_json = '[]',
                anchors_json = '[]',
                failure_reason = NULL,
                quarantine_reason = NULL,
                recompute_required = 0,
                orphaned = 0,
                updated_at_unix_ms = ?2,
                purged_at_unix_ms = ?2
            WHERE derived_artifact_id = ?1
            "#,
            params![derived_artifact_id, now],
        )?;
        drop(guard);
        self.get_derived_artifact(derived_artifact_id)
    }

    pub fn derived_stats(&self) -> Result<MediaDerivedStatsSnapshot, MediaStoreError> {
        let guard = self.connection.lock().map_err(|_| {
            MediaStoreError::Io(
                "media artifact db lock poisoned while building derived stats snapshot".to_owned(),
            )
        })?;
        guard
            .query_row(
                r#"
                SELECT
                    COUNT(*),
                    COALESCE(SUM(CASE WHEN state = 'pending' THEN 1 ELSE 0 END), 0),
                    COALESCE(SUM(CASE WHEN state = 'succeeded' THEN 1 ELSE 0 END), 0),
                    COALESCE(SUM(CASE WHEN state = 'failed' THEN 1 ELSE 0 END), 0),
                    COALESCE(SUM(CASE WHEN state = 'quarantined' THEN 1 ELSE 0 END), 0),
                    COALESCE(SUM(CASE WHEN state = 'purged' THEN 1 ELSE 0 END), 0),
                    COALESCE(SUM(CASE WHEN recompute_required = 1 THEN 1 ELSE 0 END), 0),
                    COALESCE(SUM(CASE WHEN orphaned = 1 THEN 1 ELSE 0 END), 0)
                FROM media_derived_artifacts
                "#,
                [],
                |row| {
                    Ok(MediaDerivedStatsSnapshot {
                        total: u64::try_from(row.get::<_, i64>(0)?).unwrap_or_default(),
                        pending: u64::try_from(row.get::<_, i64>(1)?).unwrap_or_default(),
                        succeeded: u64::try_from(row.get::<_, i64>(2)?).unwrap_or_default(),
                        failed: u64::try_from(row.get::<_, i64>(3)?).unwrap_or_default(),
                        quarantined: u64::try_from(row.get::<_, i64>(4)?).unwrap_or_default(),
                        purged: u64::try_from(row.get::<_, i64>(5)?).unwrap_or_default(),
                        recompute_required: u64::try_from(row.get::<_, i64>(6)?)
                            .unwrap_or_default(),
                        orphaned: u64::try_from(row.get::<_, i64>(7)?).unwrap_or_default(),
                    })
                },
            )
            .map_err(Into::into)
    }

    pub fn select_derived_prompt_chunks(
        &self,
        source_artifact_ids: &[String],
        query: &str,
        selection_budget_chars: Option<usize>,
    ) -> Result<Vec<MediaDerivedArtifactSelection>, MediaStoreError> {
        let mut source_records = Vec::new();
        for source_artifact_id in source_artifact_ids {
            source_records
                .extend(self.list_attachment_derived_artifacts(source_artifact_id.as_str())?);
        }
        let candidates = source_records
            .iter()
            .filter(|record| record.state == "succeeded")
            .filter_map(|record| {
                Some(DerivedSelectionCandidate {
                    derived_artifact_id: record.derived_artifact_id.as_str(),
                    source_artifact_id: record.source_artifact_id.as_str(),
                    kind: record.kind.as_str(),
                    content_text: record.content_text.as_deref()?,
                    anchors: record.anchors.as_slice(),
                })
            })
            .collect::<Vec<_>>();
        Ok(select_prompt_chunks(query, candidates.as_slice(), selection_budget_chars)
            .into_iter()
            .map(|chunk| MediaDerivedArtifactSelection {
                derived_artifact_id: chunk.derived_artifact_id,
                source_artifact_id: chunk.source_artifact_id,
                kind: chunk.kind,
                citation: chunk.citation,
                label: chunk.label,
                snippet: chunk.snippet,
                score: chunk.score,
            })
            .collect())
    }

    pub fn build_connector_snapshot(
        &self,
        connector_id: &str,
    ) -> Result<MediaConnectorSnapshot, MediaStoreError> {
        let guard = self.connection.lock().map_err(|_| {
            MediaStoreError::Io(
                "media artifact db lock poisoned while building snapshot".to_owned(),
            )
        })?;
        let usage = guard.query_row(
            r#"
            SELECT
                COUNT(*),
                COUNT(DISTINCT a.content_sha256),
                COALESCE(SUM(CASE WHEN c.content_sha256 IS NOT NULL THEN c.size_bytes ELSE 0 END), 0)
            FROM media_artifacts a
            LEFT JOIN media_contents c ON c.content_sha256 = a.content_sha256
            WHERE a.connector_id = ?1
            "#,
            params![connector_id],
            |row| {
                Ok(MediaUsageSnapshot {
                    artifact_count: u64::try_from(row.get::<_, i64>(0)?).unwrap_or_default(),
                    stored_content_count: u64::try_from(row.get::<_, i64>(1)?).unwrap_or_default(),
                    stored_bytes: u64::try_from(row.get::<_, i64>(2)?).unwrap_or_default(),
                })
            },
        )?;
        let blocked = self.query_recent_events_locked(
            &guard,
            connector_id,
            "attachment.%.blocked",
            RECENT_EVENT_LIMIT,
        )?;
        let upload_failures = self.query_recent_events_locked(
            &guard,
            connector_id,
            "attachment.upload.failed",
            RECENT_EVENT_LIMIT,
        )?;
        Ok(MediaConnectorSnapshot {
            policy: self.config.clone(),
            usage,
            retention: MediaRetentionSnapshot {
                max_store_bytes: self.config.store_max_bytes,
                max_store_artifacts: self.config.store_max_artifacts,
                ttl_ms: self.config.retention_ttl_ms,
            },
            recent_blocked_reasons: blocked,
            recent_upload_failures: upload_failures,
        })
    }

    pub fn build_global_snapshot(&self) -> Result<MediaGlobalSnapshot, MediaStoreError> {
        let guard = self.connection.lock().map_err(|_| {
            MediaStoreError::Io(
                "media artifact db lock poisoned while building global snapshot".to_owned(),
            )
        })?;
        Ok(MediaGlobalSnapshot {
            policy: self.config.clone(),
            usage: current_usage_locked(&guard)?,
            retention: MediaRetentionSnapshot {
                max_store_bytes: self.config.store_max_bytes,
                max_store_artifacts: self.config.store_max_artifacts,
                ttl_ms: self.config.retention_ttl_ms,
            },
            recent_blocked_reasons: self.query_recent_events_global_locked(
                &guard,
                "attachment.%.blocked",
                RECENT_EVENT_LIMIT,
            )?,
            recent_upload_failures: self.query_recent_events_global_locked(
                &guard,
                "attachment.upload.failed",
                RECENT_EVENT_LIMIT,
            )?,
        })
    }

    #[allow(dead_code)]
    pub fn record_upload_success(
        &self,
        connector_id: &str,
        artifact_id: &str,
        filename: &str,
    ) -> Result<(), MediaStoreError> {
        self.record_event(
            connector_id,
            MediaDirection::Outbound,
            MediaEventRecord {
                event_type: "attachment.upload.succeeded",
                artifact_id: Some(artifact_id),
                attachment_id: None,
                filename: Some(filename),
                reason: String::new(),
                details: json!({ "artifact_id": artifact_id, "filename": filename }),
            },
        )
    }

    pub fn record_upload_failure(
        &self,
        connector_id: &str,
        artifact_id: Option<&str>,
        filename: Option<&str>,
        reason: impl Into<String>,
    ) -> Result<(), MediaStoreError> {
        let reason = reason.into();
        self.record_event(
            connector_id,
            MediaDirection::Outbound,
            MediaEventRecord {
                event_type: "attachment.upload.failed",
                artifact_id,
                attachment_id: None,
                filename,
                reason: reason.clone(),
                details: json!({ "artifact_id": artifact_id, "filename": filename, "reason": reason }),
            },
        )
    }

    fn migrate(&self) -> Result<(), MediaStoreError> {
        let guard = self.connection.lock().map_err(|_| {
            MediaStoreError::Io("media artifact db lock poisoned while migrating".to_owned())
        })?;
        guard.execute_batch(
            r#"
            PRAGMA foreign_keys = ON;

            CREATE TABLE IF NOT EXISTS media_contents (
                content_sha256 TEXT PRIMARY KEY,
                storage_rel_path TEXT NOT NULL,
                size_bytes INTEGER NOT NULL,
                created_at_unix_ms INTEGER NOT NULL,
                last_accessed_at_unix_ms INTEGER NOT NULL,
                ref_count INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS media_artifacts (
                artifact_id TEXT PRIMARY KEY,
                connector_id TEXT NOT NULL,
                direction TEXT NOT NULL,
                envelope_id TEXT,
                conversation_id TEXT,
                adapter_message_id TEXT,
                attachment_id TEXT,
                kind TEXT NOT NULL,
                filename TEXT,
                declared_content_type TEXT,
                content_type TEXT,
                source_url TEXT,
                content_sha256 TEXT,
                size_bytes INTEGER NOT NULL,
                width_px INTEGER,
                height_px INTEGER,
                origin_json TEXT NOT NULL,
                policy_context_json TEXT NOT NULL,
                blocked_reason TEXT,
                created_at_unix_ms INTEGER NOT NULL,
                last_accessed_at_unix_ms INTEGER NOT NULL,
                FOREIGN KEY(content_sha256) REFERENCES media_contents(content_sha256)
            );
            CREATE INDEX IF NOT EXISTS idx_media_artifacts_connector_created
                ON media_artifacts(connector_id, created_at_unix_ms DESC);
            CREATE INDEX IF NOT EXISTS idx_media_artifacts_content_sha
                ON media_artifacts(content_sha256);

            CREATE TABLE IF NOT EXISTS media_events (
                event_id INTEGER PRIMARY KEY AUTOINCREMENT,
                connector_id TEXT NOT NULL,
                direction TEXT NOT NULL,
                event_type TEXT NOT NULL,
                artifact_id TEXT,
                attachment_id TEXT,
                filename TEXT,
                reason TEXT NOT NULL,
                details_json TEXT NOT NULL,
                created_at_unix_ms INTEGER NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_media_events_connector_created
                ON media_events(connector_id, created_at_unix_ms DESC);

            CREATE TABLE IF NOT EXISTS media_derived_artifacts (
                derived_artifact_id TEXT PRIMARY KEY,
                source_artifact_id TEXT NOT NULL,
                attachment_id TEXT,
                session_id TEXT,
                principal TEXT,
                device_id TEXT,
                channel TEXT,
                filename TEXT NOT NULL,
                declared_content_type TEXT NOT NULL,
                kind TEXT NOT NULL,
                state TEXT NOT NULL,
                parser_name TEXT NOT NULL,
                parser_version TEXT NOT NULL,
                source_content_hash TEXT NOT NULL,
                content_hash TEXT,
                content_text TEXT,
                summary_text TEXT,
                language TEXT,
                duration_ms INTEGER,
                processing_ms INTEGER,
                warnings_json TEXT NOT NULL,
                anchors_json TEXT NOT NULL,
                failure_reason TEXT,
                quarantine_reason TEXT,
                workspace_document_id TEXT,
                memory_item_id TEXT,
                background_task_id TEXT,
                recompute_required INTEGER NOT NULL DEFAULT 0,
                orphaned INTEGER NOT NULL DEFAULT 0,
                created_at_unix_ms INTEGER NOT NULL,
                updated_at_unix_ms INTEGER NOT NULL,
                purged_at_unix_ms INTEGER,
                FOREIGN KEY(source_artifact_id) REFERENCES media_artifacts(artifact_id) ON DELETE CASCADE
            );
            CREATE UNIQUE INDEX IF NOT EXISTS idx_media_derived_source_kind
                ON media_derived_artifacts(source_artifact_id, kind);
            CREATE INDEX IF NOT EXISTS idx_media_derived_session
                ON media_derived_artifacts(session_id, created_at_unix_ms DESC);
            CREATE INDEX IF NOT EXISTS idx_media_derived_state
                ON media_derived_artifacts(state, recompute_required, updated_at_unix_ms DESC);
            "#,
        )?;
        Ok(())
    }

    async fn download_and_store_discord_attachment(
        &self,
        connector_id: &str,
        envelope_id: &str,
        conversation_id: &str,
        adapter_message_id: Option<&str>,
        attachment: &AttachmentRef,
        source_url: &str,
    ) -> Result<StoredArtifactRecord, MediaStoreError> {
        let mut current_url = Url::parse(source_url).map_err(|error| {
            MediaStoreError::InvalidAttachment(format!("attachment URL is invalid: {error}"))
        })?;
        validate_attachment_url(
            &current_url,
            &self.config.allowed_source_hosts,
            self.config.allow_http_fixture_urls,
        )?;
        let mut redirects_followed = 0usize;
        let body = loop {
            let resolved = resolve_target_addresses(&current_url).await?;
            validate_resolved_addresses(resolved.as_slice())?;
            let response =
                self.http_client.get(current_url.clone()).send().await.map_err(|error| {
                    MediaStoreError::Download(format!(
                        "attachment download request failed: {error}"
                    ))
                })?;
            if response.status().is_redirection() {
                if redirects_followed >= self.config.max_redirects {
                    return Err(MediaStoreError::NetworkPolicy(format!(
                        "attachment redirect limit exceeded ({})",
                        self.config.max_redirects
                    )));
                }
                let Some(location) = response.headers().get(reqwest::header::LOCATION) else {
                    return Err(MediaStoreError::Download(
                        "attachment redirect missing Location header".to_owned(),
                    ));
                };
                let location_str = location.to_str().map_err(|_| {
                    MediaStoreError::Download(
                        "attachment redirect Location is invalid UTF-8".to_owned(),
                    )
                })?;
                current_url = current_url.join(location_str).map_err(|error| {
                    MediaStoreError::Download(format!(
                        "attachment redirect URL is invalid: {error}"
                    ))
                })?;
                validate_attachment_url(
                    &current_url,
                    &self.config.allowed_source_hosts,
                    self.config.allow_http_fixture_urls,
                )?;
                redirects_followed = redirects_followed.saturating_add(1);
                continue;
            }
            let status = response.status();
            if !status.is_success() {
                return Err(MediaStoreError::Download(format!(
                    "attachment download failed with HTTP {}",
                    status.as_u16()
                )));
            }
            break read_response_body_with_limit(response, self.config.max_download_bytes).await?;
        };

        let sniffed = sniff_content(body.as_slice())?;
        if !self
            .config
            .allowed_download_content_types
            .iter()
            .any(|allowed| allowed == &sniffed.content_type)
        {
            return Err(MediaStoreError::InvalidAttachment(format!(
                "attachment content type '{}' is blocked by policy",
                sniffed.content_type
            )));
        }
        let size_bytes = u64::try_from(body.len()).unwrap_or(u64::MAX);
        let sha256 = sha256_hex(body.as_slice());
        let artifact_id = ulid::Ulid::new().to_string();
        let (storage_path, relative_path) =
            prepare_content_storage_path(self.content_root.as_path(), sha256.as_str())?;
        match fs::OpenOptions::new().write(true).create_new(true).open(storage_path.as_path()) {
            Ok(mut file) => {
                file.write_all(body.as_slice()).map_err(|error| {
                    MediaStoreError::Io(format!(
                        "failed to persist media artifact '{}' : {error}",
                        storage_path.display()
                    ))
                })?;
            }
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(error) => {
                return Err(MediaStoreError::Io(format!(
                    "failed to persist media artifact '{}' : {error}",
                    storage_path.display()
                )));
            }
        }
        let now = current_unix_ms();
        let record = StoredArtifactRecord {
            artifact_id: artifact_id.clone(),
            filename: sanitize_filename(
                attachment.filename.as_deref().unwrap_or("attachment.bin"),
                sniffed.content_type.as_str(),
            ),
            content_type: sniffed.content_type.clone(),
            size_bytes,
            sha256: sha256.clone(),
            width_px: sniffed.width_px,
            height_px: sniffed.height_px,
            storage_rel_path: relative_path.clone(),
        };
        {
            let guard = self.connection.lock().map_err(|_| {
                MediaStoreError::Io(
                    "media artifact db lock poisoned while storing artifact".to_owned(),
                )
            })?;
            guard.execute(
                r#"
                INSERT INTO media_contents (
                    content_sha256, storage_rel_path, size_bytes, created_at_unix_ms,
                    last_accessed_at_unix_ms, ref_count
                )
                VALUES (?1, ?2, ?3, ?4, ?4, 1)
                ON CONFLICT(content_sha256) DO UPDATE SET
                    last_accessed_at_unix_ms = excluded.last_accessed_at_unix_ms,
                    ref_count = media_contents.ref_count + 1
                "#,
                params![sha256, relative_path, i64::try_from(size_bytes).unwrap_or(i64::MAX), now],
            )?;
            guard.execute(
                r#"
                INSERT INTO media_artifacts (
                    artifact_id, connector_id, direction, envelope_id, conversation_id,
                    adapter_message_id, attachment_id, kind, filename, declared_content_type,
                    content_type, source_url, content_sha256, size_bytes, width_px, height_px,
                    origin_json, policy_context_json, blocked_reason, created_at_unix_ms,
                    last_accessed_at_unix_ms
                )
                VALUES (?1, ?2, 'inbound', ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, NULL, ?18, ?18)
                "#,
                params![
                    record.artifact_id,
                    connector_id,
                    envelope_id,
                    conversation_id,
                    adapter_message_id,
                    attachment.attachment_id,
                    attachment_kind_label(attachment.kind),
                    record.filename,
                    attachment.content_type,
                    record.content_type,
                    current_url.to_string(),
                    record.sha256,
                    i64::try_from(record.size_bytes).unwrap_or(i64::MAX),
                    record.width_px,
                    record.height_px,
                    json!({
                        "connector_id": connector_id,
                        "direction": "inbound",
                        "conversation_id": conversation_id,
                        "adapter_message_id": adapter_message_id,
                    })
                    .to_string(),
                    json!({
                        "metadata_action": "attachment.metadata.accept",
                        "download_action": "attachment.download",
                        "vision_action": if self.is_vision_eligible_content_type(record.content_type.as_str()) {
                            "attachment.vision"
                        } else {
                            ""
                        },
                    })
                    .to_string(),
                    now,
                ],
            )?;
        }
        self.record_event(
            connector_id,
            MediaDirection::Inbound,
            MediaEventRecord {
                event_type: "attachment.download.stored",
                artifact_id: Some(record.artifact_id.as_str()),
                attachment_id: attachment.attachment_id.as_deref(),
                filename: Some(record.filename.as_str()),
                reason: String::new(),
                details: json!({
                "content_type": record.content_type,
                "size_bytes": record.size_bytes,
                "sha256": record.sha256,
                "source_url": current_url.to_string(),
                }),
            },
        )?;
        self.run_retention_housekeeping_if_due(now)?;
        Ok(record)
    }

    fn run_retention_housekeeping_if_due(&self, now_unix_ms: i64) -> Result<(), MediaStoreError> {
        let should_prune = {
            let mut maintenance = self.maintenance.lock().map_err(|_| {
                MediaStoreError::Io(
                    "media artifact maintenance lock poisoned while scheduling retention prune"
                        .to_owned(),
                )
            })?;
            if should_prune_retention_after_ingest(&maintenance, now_unix_ms) {
                maintenance.last_retention_prune_unix_ms = now_unix_ms;
                maintenance.deferred_ingests = 0;
                true
            } else {
                maintenance.deferred_ingests = maintenance.deferred_ingests.saturating_add(1);
                false
            }
        };
        if should_prune {
            self.prune_retention()?;
        }
        Ok(())
    }

    fn prune_retention(&self) -> Result<(), MediaStoreError> {
        let now = current_unix_ms();
        let cutoff = now.saturating_sub(self.config.retention_ttl_ms.max(1));
        let guard = self.connection.lock().map_err(|_| {
            MediaStoreError::Io("media artifact db lock poisoned while pruning".to_owned())
        })?;
        let mut stale = guard.prepare(
            r#"
            SELECT artifact_id, content_sha256
            FROM media_artifacts
            WHERE created_at_unix_ms < ?1
            ORDER BY created_at_unix_ms ASC
            "#,
        )?;
        let stale_rows = stale
            .query_map(params![cutoff], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, Option<String>>(1)?))
            })?
            .collect::<Result<Vec<_>, _>>()?;
        for (artifact_id, content_sha256) in stale_rows {
            remove_artifact_locked(
                &guard,
                self.content_root.as_path(),
                artifact_id.as_str(),
                content_sha256.as_deref(),
            )?;
        }
        let mut usage = current_usage_locked(&guard)?;
        while usage.artifact_count > self.config.store_max_artifacts as u64
            || usage.stored_bytes > self.config.store_max_bytes
        {
            let oldest = guard
                .query_row(
                    r#"
                    SELECT artifact_id, content_sha256
                    FROM media_artifacts
                    ORDER BY created_at_unix_ms ASC
                    LIMIT 1
                    "#,
                    [],
                    |row| Ok((row.get::<_, String>(0)?, row.get::<_, Option<String>>(1)?)),
                )
                .optional()?;
            let Some((artifact_id, content_sha256)) = oldest else {
                break;
            };
            remove_artifact_locked(
                &guard,
                self.content_root.as_path(),
                artifact_id.as_str(),
                content_sha256.as_deref(),
            )?;
            usage = current_usage_locked(&guard)?;
        }
        Ok(())
    }

    fn query_recent_events_locked(
        &self,
        connection: &Connection,
        connector_id: &str,
        event_like: &str,
        limit: usize,
    ) -> Result<Vec<MediaEventSnapshot>, MediaStoreError> {
        let mut statement = connection.prepare(
            r#"
            SELECT event_type, artifact_id, attachment_id, filename, reason, created_at_unix_ms
            FROM media_events
            WHERE connector_id = ?1 AND event_type LIKE ?2
            ORDER BY created_at_unix_ms DESC
            LIMIT ?3
            "#,
        )?;
        let rows = statement
            .query_map(params![connector_id, event_like, limit as i64], |row| {
                Ok(MediaEventSnapshot {
                    event_type: row.get(0)?,
                    artifact_id: row.get(1)?,
                    attachment_id: row.get(2)?,
                    filename: row.get(3)?,
                    reason: row.get(4)?,
                    created_at_unix_ms: row.get(5)?,
                })
            })?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    fn query_recent_events_global_locked(
        &self,
        connection: &Connection,
        event_like: &str,
        limit: usize,
    ) -> Result<Vec<MediaEventSnapshot>, MediaStoreError> {
        let mut statement = connection.prepare(
            r#"
            SELECT event_type, artifact_id, attachment_id, filename, reason, created_at_unix_ms
            FROM media_events
            WHERE event_type LIKE ?1
            ORDER BY created_at_unix_ms DESC
            LIMIT ?2
            "#,
        )?;
        let rows = statement
            .query_map(params![event_like, limit as i64], |row| {
                Ok(MediaEventSnapshot {
                    event_type: row.get(0)?,
                    artifact_id: row.get(1)?,
                    attachment_id: row.get(2)?,
                    filename: row.get(3)?,
                    reason: row.get(4)?,
                    created_at_unix_ms: row.get(5)?,
                })
            })?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    fn record_event(
        &self,
        connector_id: &str,
        direction: MediaDirection,
        event: MediaEventRecord<'_>,
    ) -> Result<(), MediaStoreError> {
        let guard = self.connection.lock().map_err(|_| {
            MediaStoreError::Io("media artifact db lock poisoned while recording event".to_owned())
        })?;
        guard.execute(
            r#"
            INSERT INTO media_events (
                connector_id, direction, event_type, artifact_id, attachment_id, filename,
                reason, details_json, created_at_unix_ms
            )
            VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
            "#,
            params![
                connector_id,
                direction.as_str(),
                event.event_type,
                event.artifact_id,
                event.attachment_id,
                event.filename,
                event.reason,
                event.details.to_string(),
                current_unix_ms(),
            ],
        )?;
        Ok(())
    }

    fn is_vision_eligible_content_type(&self, content_type: &str) -> bool {
        self.config.vision_allowed_content_types.iter().any(|allowed| allowed == content_type)
    }
}

fn map_derived_artifact_row(
    row: &rusqlite::Row<'_>,
) -> Result<MediaDerivedArtifactRow, rusqlite::Error> {
    let warnings_json: String = row.get(20)?;
    let anchors_json: String = row.get(21)?;
    Ok(MediaDerivedArtifactRow {
        record: MediaDerivedArtifactRecord {
            derived_artifact_id: row.get(0)?,
            source_artifact_id: row.get(1)?,
            attachment_id: row.get(2)?,
            session_id: row.get(3)?,
            principal: row.get(4)?,
            device_id: row.get(5)?,
            channel: row.get(6)?,
            filename: row.get(7)?,
            declared_content_type: row.get(8)?,
            kind: row.get(9)?,
            state: row.get(10)?,
            parser_name: row.get(11)?,
            parser_version: row.get(12)?,
            source_content_hash: row.get(13)?,
            content_hash: row.get(14)?,
            content_text: row.get(15)?,
            summary_text: row.get(16)?,
            language: row.get(17)?,
            duration_ms: row
                .get::<_, Option<i64>>(18)?
                .map(|value| u64::try_from(value.max(0)).unwrap_or_default()),
            processing_ms: row
                .get::<_, Option<i64>>(19)?
                .map(|value| u64::try_from(value.max(0)).unwrap_or_default()),
            warnings: serde_json::from_str(warnings_json.as_str()).unwrap_or_default(),
            anchors: serde_json::from_str(anchors_json.as_str()).unwrap_or_default(),
            failure_reason: row.get(22)?,
            quarantine_reason: row.get(23)?,
            workspace_document_id: row.get(24)?,
            memory_item_id: row.get(25)?,
            background_task_id: row.get(26)?,
            recompute_required: row.get::<_, i64>(27)? > 0,
            orphaned: row.get::<_, i64>(28)? > 0,
            created_at_unix_ms: row.get(29)?,
            updated_at_unix_ms: row.get(30)?,
            purged_at_unix_ms: row.get(31)?,
        },
    })
}

fn current_usage_locked(connection: &Connection) -> Result<MediaUsageSnapshot, MediaStoreError> {
    let usage = connection.query_row(
        r#"
        SELECT
            (SELECT COUNT(*) FROM media_artifacts),
            (SELECT COUNT(*) FROM media_contents),
            (SELECT COALESCE(SUM(size_bytes), 0) FROM media_contents)
        "#,
        [],
        |row| {
            Ok(MediaUsageSnapshot {
                artifact_count: u64::try_from(row.get::<_, i64>(0)?).unwrap_or_default(),
                stored_content_count: u64::try_from(row.get::<_, i64>(1)?).unwrap_or_default(),
                stored_bytes: u64::try_from(row.get::<_, i64>(2)?).unwrap_or_default(),
            })
        },
    )?;
    Ok(usage)
}

fn remove_artifact_locked(
    connection: &Connection,
    content_root: &Path,
    artifact_id: &str,
    content_sha256: Option<&str>,
) -> Result<(), MediaStoreError> {
    connection
        .execute("DELETE FROM media_artifacts WHERE artifact_id = ?1", params![artifact_id])?;
    let Some(content_sha256) = content_sha256 else {
        return Ok(());
    };
    let ref_count = connection
        .query_row(
            "SELECT COUNT(*) FROM media_artifacts WHERE content_sha256 = ?1",
            params![content_sha256],
            |row| row.get::<_, i64>(0),
        )
        .optional()?
        .unwrap_or(0_i64);
    if ref_count > 0 {
        connection.execute(
            "UPDATE media_contents SET ref_count = ?2 WHERE content_sha256 = ?1",
            params![content_sha256, ref_count],
        )?;
        return Ok(());
    }
    let storage_rel_path = connection
        .query_row(
            "SELECT storage_rel_path FROM media_contents WHERE content_sha256 = ?1",
            params![content_sha256],
            |row| row.get::<_, String>(0),
        )
        .optional()?;
    let resolved_storage_path = storage_rel_path
        .as_ref()
        .map(|path| resolve_content_storage_path(content_root, path.as_str(), content_sha256));
    connection
        .execute("DELETE FROM media_contents WHERE content_sha256 = ?1", params![content_sha256])?;
    if let Some(storage_path) = resolved_storage_path {
        let _ = fs::remove_file(storage_path?);
    }
    Ok(())
}

fn metadata_type_is_safe(attachment: &AttachmentRef) -> bool {
    let declared =
        attachment.content_type.as_deref().map(str::trim).map(|value| value.to_ascii_lowercase());
    if declared.as_deref() == Some("image/svg+xml") {
        return false;
    }
    if declared.as_deref().is_some_and(|value| {
        matches!(
            value,
            "image/png"
                | "image/jpeg"
                | "image/webp"
                | "image/gif"
                | "text/plain"
                | "application/json"
        )
    }) {
        return true;
    }
    attachment
        .filename
        .as_deref()
        .map(str::trim)
        .map(|value| value.to_ascii_lowercase())
        .is_some_and(|value| {
            value.ends_with(".png")
                || value.ends_with(".jpg")
                || value.ends_with(".jpeg")
                || value.ends_with(".webp")
                || value.ends_with(".gif")
                || value.ends_with(".txt")
                || value.ends_with(".json")
        })
}

fn validate_attachment_url(
    url: &Url,
    allowed_source_hosts: &[String],
    allow_http_fixture_urls: bool,
) -> Result<(), MediaStoreError> {
    match url.scheme() {
        "https" => {}
        "http"
            if allow_http_fixture_urls && is_fixture_host(url.host_str().unwrap_or_default()) => {}
        other => {
            return Err(MediaStoreError::NetworkPolicy(format!(
                "attachment URL scheme '{other}' is blocked by policy"
            )));
        }
    }
    if !url.username().is_empty() || url.password().is_some() {
        return Err(MediaStoreError::NetworkPolicy(
            "attachment URL credentials are not allowed".to_owned(),
        ));
    }
    let Some(host) = url.host_str() else {
        return Err(MediaStoreError::NetworkPolicy("attachment URL host is missing".to_owned()));
    };
    let normalized_host = host.trim().trim_end_matches('.').to_ascii_lowercase();
    if netguard::is_localhost_hostname(normalized_host.as_str()) {
        return Err(MediaStoreError::NetworkPolicy(
            "attachment URL localhost targets are blocked".to_owned(),
        ));
    }
    if !allowed_source_hosts
        .iter()
        .any(|pattern| host_matches_pattern(normalized_host.as_str(), pattern))
    {
        return Err(MediaStoreError::NetworkPolicy(format!(
            "attachment host '{normalized_host}' is not allowlisted"
        )));
    }
    Ok(())
}

async fn resolve_target_addresses(url: &Url) -> Result<Vec<SocketAddr>, MediaStoreError> {
    let host = url.host_str().ok_or_else(|| {
        MediaStoreError::NetworkPolicy("attachment URL host is missing".to_owned())
    })?;
    let port = url.port_or_known_default().ok_or_else(|| {
        MediaStoreError::NetworkPolicy("attachment URL port is missing".to_owned())
    })?;
    if let Some(ip) =
        netguard::parse_host_ip_literal(host).map_err(MediaStoreError::NetworkPolicy)?
    {
        return Ok(vec![SocketAddr::new(ip, port)]);
    }
    let resolved = tokio::net::lookup_host((host, port)).await.map_err(|error| {
        MediaStoreError::NetworkPolicy(format!(
            "DNS resolution failed for '{host}:{port}': {error}"
        ))
    })?;
    let addrs = resolved.collect::<Vec<_>>();
    if addrs.is_empty() {
        return Err(MediaStoreError::NetworkPolicy(format!(
            "DNS resolution returned no addresses for '{host}:{port}'"
        )));
    }
    Ok(addrs)
}

fn validate_resolved_addresses(addrs: &[SocketAddr]) -> Result<(), MediaStoreError> {
    let ips = addrs.iter().map(|address| address.ip()).collect::<Vec<_>>();
    netguard::validate_resolved_ip_addrs(ips.as_slice(), false)
        .map_err(MediaStoreError::NetworkPolicy)
}

async fn read_response_body_with_limit(
    mut response: reqwest::Response,
    max_download_bytes: usize,
) -> Result<Vec<u8>, MediaStoreError> {
    if let Some(content_length) = response.content_length() {
        if content_length > max_download_bytes as u64 {
            return Err(MediaStoreError::NetworkPolicy(format!(
                "attachment body exceeds max_download_bytes ({})",
                max_download_bytes
            )));
        }
    }

    let mut bytes = Vec::new();
    while let Some(chunk) = response.chunk().await.map_err(|error| {
        MediaStoreError::Download(format!("attachment body read failed: {error}"))
    })? {
        let next_len = bytes.len().checked_add(chunk.len()).ok_or_else(|| {
            MediaStoreError::NetworkPolicy(format!(
                "attachment body exceeds max_download_bytes ({})",
                max_download_bytes
            ))
        })?;
        if next_len > max_download_bytes {
            return Err(MediaStoreError::NetworkPolicy(format!(
                "attachment body exceeds max_download_bytes ({})",
                max_download_bytes
            )));
        }
        bytes.extend_from_slice(&chunk);
    }
    Ok(bytes)
}

fn sniff_content(bytes: &[u8]) -> Result<SniffedContent, MediaStoreError> {
    if is_png(bytes) {
        let (width_px, height_px) = png_dimensions(bytes).ok_or_else(|| {
            MediaStoreError::InvalidAttachment("PNG dimensions are invalid".to_owned())
        })?;
        return Ok(SniffedContent {
            content_type: "image/png".to_owned(),
            width_px: Some(width_px),
            height_px: Some(height_px),
        });
    }
    if is_jpeg(bytes) {
        let (width_px, height_px) = jpeg_dimensions(bytes).ok_or_else(|| {
            MediaStoreError::InvalidAttachment("JPEG dimensions are invalid".to_owned())
        })?;
        return Ok(SniffedContent {
            content_type: "image/jpeg".to_owned(),
            width_px: Some(width_px),
            height_px: Some(height_px),
        });
    }
    if is_webp(bytes) {
        let (width_px, height_px) = webp_dimensions(bytes).ok_or_else(|| {
            MediaStoreError::InvalidAttachment("WEBP dimensions are invalid".to_owned())
        })?;
        return Ok(SniffedContent {
            content_type: "image/webp".to_owned(),
            width_px: Some(width_px),
            height_px: Some(height_px),
        });
    }
    if is_gif(bytes) {
        let (width_px, height_px) = gif_dimensions(bytes).ok_or_else(|| {
            MediaStoreError::InvalidAttachment("GIF dimensions are invalid".to_owned())
        })?;
        return Ok(SniffedContent {
            content_type: "image/gif".to_owned(),
            width_px: Some(width_px),
            height_px: Some(height_px),
        });
    }
    if looks_like_svg(bytes) {
        return Err(MediaStoreError::InvalidAttachment(
            "SVG attachments are blocked by default".to_owned(),
        ));
    }
    if looks_like_json(bytes) {
        return Ok(SniffedContent {
            content_type: "application/json".to_owned(),
            width_px: None,
            height_px: None,
        });
    }
    if looks_like_text(bytes) {
        return Ok(SniffedContent {
            content_type: "text/plain".to_owned(),
            width_px: None,
            height_px: None,
        });
    }
    Err(MediaStoreError::InvalidAttachment(
        "attachment content type could not be safely sniffed".to_owned(),
    ))
}

fn is_png(bytes: &[u8]) -> bool {
    bytes.len() >= 24 && bytes.starts_with(&[0x89, b'P', b'N', b'G', 0x0D, 0x0A, 0x1A, 0x0A])
}

fn png_dimensions(bytes: &[u8]) -> Option<(u32, u32)> {
    if !is_png(bytes) {
        return None;
    }
    let width = u32::from_be_bytes(bytes[16..20].try_into().ok()?);
    let height = u32::from_be_bytes(bytes[20..24].try_into().ok()?);
    Some((width, height))
}

fn is_jpeg(bytes: &[u8]) -> bool {
    bytes.len() >= 4 && bytes[0] == 0xFF && bytes[1] == 0xD8
}

fn jpeg_dimensions(bytes: &[u8]) -> Option<(u32, u32)> {
    if !is_jpeg(bytes) {
        return None;
    }
    let mut index = 2usize;
    while index + 9 < bytes.len() {
        if bytes[index] != 0xFF {
            index = index.saturating_add(1);
            continue;
        }
        let marker = bytes[index + 1];
        index += 2;
        if marker == 0xD8 || marker == 0xD9 {
            continue;
        }
        if index + 2 > bytes.len() {
            break;
        }
        let segment_length = u16::from_be_bytes(bytes[index..index + 2].try_into().ok()?) as usize;
        if segment_length < 2 || index + segment_length > bytes.len() {
            break;
        }
        if matches!(
            marker,
            0xC0 | 0xC1
                | 0xC2
                | 0xC3
                | 0xC5
                | 0xC6
                | 0xC7
                | 0xC9
                | 0xCA
                | 0xCB
                | 0xCD
                | 0xCE
                | 0xCF
        ) && index + 7 < bytes.len()
        {
            let height = u16::from_be_bytes(bytes[index + 3..index + 5].try_into().ok()?) as u32;
            let width = u16::from_be_bytes(bytes[index + 5..index + 7].try_into().ok()?) as u32;
            return Some((width, height));
        }
        index += segment_length;
    }
    None
}

fn is_webp(bytes: &[u8]) -> bool {
    bytes.len() >= 16 && &bytes[0..4] == b"RIFF" && &bytes[8..12] == b"WEBP"
}

fn webp_dimensions(bytes: &[u8]) -> Option<(u32, u32)> {
    if !is_webp(bytes) {
        return None;
    }
    match &bytes[12..16] {
        b"VP8 " if bytes.len() >= 30 => {
            let width = u16::from_le_bytes(bytes[26..28].try_into().ok()?) as u32 & 0x3FFF;
            let height = u16::from_le_bytes(bytes[28..30].try_into().ok()?) as u32 & 0x3FFF;
            Some((width, height))
        }
        b"VP8L" if bytes.len() >= 25 => {
            let value = u32::from_le_bytes([bytes[21], bytes[22], bytes[23], bytes[24]]);
            let width = (value & 0x3FFF).saturating_add(1);
            let height = ((value >> 14) & 0x3FFF).saturating_add(1);
            Some((width, height))
        }
        b"VP8X" if bytes.len() >= 30 => {
            let width = u32::from_le_bytes([bytes[24], bytes[25], bytes[26], 0]).saturating_add(1);
            let height = u32::from_le_bytes([bytes[27], bytes[28], bytes[29], 0]).saturating_add(1);
            Some((width, height))
        }
        _ => None,
    }
}

fn is_gif(bytes: &[u8]) -> bool {
    bytes.len() >= 10 && (bytes.starts_with(b"GIF87a") || bytes.starts_with(b"GIF89a"))
}

fn gif_dimensions(bytes: &[u8]) -> Option<(u32, u32)> {
    if !is_gif(bytes) {
        return None;
    }
    let width = u16::from_le_bytes(bytes[6..8].try_into().ok()?) as u32;
    let height = u16::from_le_bytes(bytes[8..10].try_into().ok()?) as u32;
    Some((width, height))
}

fn looks_like_svg(bytes: &[u8]) -> bool {
    std::str::from_utf8(bytes)
        .ok()
        .map(|text| text.trim_start().to_ascii_lowercase())
        .is_some_and(|value| value.starts_with("<svg") || value.contains("<svg"))
}

fn looks_like_json(bytes: &[u8]) -> bool {
    std::str::from_utf8(bytes).ok().is_some_and(|text| serde_json::from_str::<Value>(text).is_ok())
}

fn looks_like_text(bytes: &[u8]) -> bool {
    std::str::from_utf8(bytes).ok().is_some_and(|text| !text.chars().any(|ch| ch == '\u{0000}'))
}

fn content_relative_path(sha256: &str) -> String {
    let prefix = &sha256[..2.min(sha256.len())];
    format!("{prefix}/{sha256}")
}

fn is_valid_sha256_hex(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn validate_content_digest(expected_sha256: &str) -> Result<(), MediaStoreError> {
    if !is_valid_sha256_hex(expected_sha256)
        || expected_sha256.contains("..")
        || expected_sha256.contains('/')
        || expected_sha256.contains('\\')
    {
        return Err(MediaStoreError::Io(format!(
            "media content digest '{expected_sha256}' is invalid"
        )));
    }
    Ok(())
}

fn canonical_content_root(content_root: &Path) -> Result<PathBuf, MediaStoreError> {
    fs::canonicalize(content_root).map_err(|error| {
        MediaStoreError::Io(format!(
            "failed to canonicalize media content root '{}' : {error}",
            content_root.display()
        ))
    })
}

fn prepare_content_storage_path(
    content_root: &Path,
    expected_sha256: &str,
) -> Result<(PathBuf, String), MediaStoreError> {
    validate_content_digest(expected_sha256)?;
    let normalized_root = canonical_content_root(content_root)?;
    let relative_path = content_relative_path(expected_sha256);
    let storage_path = normalized_root.join(Path::new(relative_path.as_str()));
    let parent = storage_path.parent().ok_or_else(|| {
        MediaStoreError::Io(format!(
            "media content path '{}' is missing a parent directory",
            storage_path.display()
        ))
    })?;
    fs::create_dir_all(parent).map_err(|error| {
        MediaStoreError::Io(format!(
            "failed to create media artifact parent '{}' : {error}",
            parent.display()
        ))
    })?;
    let normalized_parent = fs::canonicalize(parent).map_err(|error| {
        MediaStoreError::Io(format!(
            "failed to canonicalize media artifact parent '{}' : {error}",
            parent.display()
        ))
    })?;
    if !normalized_parent.starts_with(normalized_root.as_path()) {
        return Err(MediaStoreError::Io(format!(
            "media artifact parent '{}' escapes '{}'",
            normalized_parent.display(),
            normalized_root.display()
        )));
    }
    let normalized_storage_path = normalized_parent.join(expected_sha256);
    if !normalized_storage_path.starts_with(normalized_root.as_path()) {
        return Err(MediaStoreError::Io(format!(
            "media artifact path '{}' escapes '{}'",
            normalized_storage_path.display(),
            normalized_root.display()
        )));
    }
    Ok((normalized_storage_path, relative_path))
}

fn resolve_content_storage_path(
    content_root: &Path,
    storage_rel_path: &str,
    expected_sha256: &str,
) -> Result<PathBuf, MediaStoreError> {
    validate_content_digest(expected_sha256)?;
    let normalized_root = canonical_content_root(content_root)?;
    let expected_rel_path = content_relative_path(expected_sha256);
    if storage_rel_path != expected_rel_path {
        return Err(MediaStoreError::Io(format!(
            "media content storage path '{}' does not match digest '{}'",
            storage_rel_path, expected_sha256
        )));
    }
    let normalized_storage_path =
        fs::canonicalize(normalized_root.join(Path::new(storage_rel_path))).map_err(|error| {
            MediaStoreError::Io(format!(
                "failed to canonicalize media artifact '{}' : {error}",
                storage_rel_path
            ))
        })?;
    if !normalized_storage_path.starts_with(normalized_root.as_path()) {
        return Err(MediaStoreError::Io(format!(
            "media artifact path '{}' escapes '{}'",
            normalized_storage_path.display(),
            normalized_root.display()
        )));
    }
    Ok(normalized_storage_path)
}

fn attachment_kind_for_content_type(content_type: &str) -> AttachmentKind {
    if content_type.starts_with("image/") {
        AttachmentKind::Image
    } else {
        AttachmentKind::File
    }
}

fn attachment_kind_label(kind: AttachmentKind) -> &'static str {
    match kind {
        AttachmentKind::Image => "image",
        AttachmentKind::File => "file",
    }
}

fn sanitize_filename(raw: &str, content_type: &str) -> String {
    let mut sanitized = raw
        .chars()
        .map(
            |ch| if ch.is_ascii_alphanumeric() || matches!(ch, '.' | '_' | '-') { ch } else { '_' },
        )
        .collect::<String>()
        .trim_matches('.')
        .to_owned();
    if sanitized.is_empty() {
        sanitized = match content_type {
            "image/png" => "attachment.png".to_owned(),
            "image/jpeg" => "attachment.jpg".to_owned(),
            "image/webp" => "attachment.webp".to_owned(),
            "image/gif" => "attachment.gif".to_owned(),
            "application/json" => "attachment.json".to_owned(),
            "text/plain" => "attachment.txt".to_owned(),
            _ => "attachment.bin".to_owned(),
        };
    }
    sanitized
}

fn host_matches_pattern(host: &str, pattern: &str) -> bool {
    let normalized = pattern.trim().trim_end_matches('.').to_ascii_lowercase();
    if let Some(suffix) = normalized.strip_prefix("*.") {
        return host == suffix || host.ends_with(format!(".{suffix}").as_str());
    }
    host == normalized
}

fn is_fixture_host(host: &str) -> bool {
    let normalized = host.trim().trim_end_matches('.').to_ascii_lowercase();
    netguard::is_localhost_hostname(normalized.as_str()) || normalized.ends_with(".invalid")
}

fn sha256_hex(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

fn current_unix_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| i64::try_from(duration.as_millis()).unwrap_or(i64::MAX))
        .unwrap_or_default()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct MediaMaintenanceState {
    last_retention_prune_unix_ms: i64,
    deferred_ingests: u32,
}

fn build_media_http_client() -> Result<HttpClient, MediaStoreError> {
    HttpClient::builder().redirect(Policy::none()).timeout(Duration::from_secs(15)).build().map_err(
        |error| MediaStoreError::Download(format!("failed to build media client: {error}")),
    )
}

fn should_prune_retention_after_ingest(state: &MediaMaintenanceState, now_unix_ms: i64) -> bool {
    if now_unix_ms.saturating_sub(state.last_retention_prune_unix_ms)
        >= RETENTION_PRUNE_MIN_INTERVAL_MS
    {
        return true;
    }
    state.deferred_ingests.saturating_add(1) >= RETENTION_PRUNE_MAX_DEFERRED_INGESTS
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use palyra_connectors::{AttachmentKind, AttachmentRef};
    use rusqlite::params;
    use tempfile::TempDir;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
    };

    use crate::media_derived::{DerivedArtifactContent, DerivedArtifactKind};

    use super::{
        content_relative_path, read_response_body_with_limit, resolve_content_storage_path,
        should_prune_retention_after_ingest, sniff_content, ConsoleAttachmentStoreRequest,
        InboundAttachmentIngestRequest, MediaArtifactStore, MediaDerivedArtifactUpsertRequest,
        MediaMaintenanceState, MediaRuntimeConfig, RETENTION_PRUNE_MAX_DEFERRED_INGESTS,
        RETENTION_PRUNE_MIN_INTERVAL_MS,
    };

    #[test]
    fn sniff_content_rejects_svg() {
        let error = sniff_content(br#"<svg xmlns="http://www.w3.org/2000/svg"></svg>"#)
            .expect_err("svg should stay blocked by default");
        assert!(error.to_string().contains("SVG"));
    }

    #[test]
    fn store_snapshot_defaults_are_safe() {
        let config = MediaRuntimeConfig::default();
        assert!(!config.download_enabled);
        assert!(!config.outbound_upload_enabled);
        assert_eq!(config.max_attachments_per_message, 4);
        assert_eq!(config.max_redirects, 3);
    }

    #[test]
    fn retention_housekeeping_waits_for_interval_or_deferred_budget() {
        let state =
            MediaMaintenanceState { last_retention_prune_unix_ms: 1_000_000, deferred_ingests: 0 };

        assert!(
            !should_prune_retention_after_ingest(
                &state,
                1_000_000 + RETENTION_PRUNE_MIN_INTERVAL_MS - 1,
            ),
            "recent ingests should not trigger full retention pruning"
        );
    }

    #[test]
    fn retention_housekeeping_runs_after_interval_or_many_deferred_ingests() {
        let startup_state =
            MediaMaintenanceState { last_retention_prune_unix_ms: 0, deferred_ingests: 0 };
        assert!(
            should_prune_retention_after_ingest(&startup_state, RETENTION_PRUNE_MIN_INTERVAL_MS),
            "the first ingest after startup should be able to prune stale retained artifacts"
        );

        let interval_state =
            MediaMaintenanceState { last_retention_prune_unix_ms: 1_000_000, deferred_ingests: 0 };
        assert!(
            should_prune_retention_after_ingest(
                &interval_state,
                1_000_000 + RETENTION_PRUNE_MIN_INTERVAL_MS,
            ),
            "retention pruning should resume after the minimum interval"
        );

        let deferred_state = MediaMaintenanceState {
            last_retention_prune_unix_ms: 1_000_000,
            deferred_ingests: RETENTION_PRUNE_MAX_DEFERRED_INGESTS - 1,
        };
        assert!(
            should_prune_retention_after_ingest(&deferred_state, 1_000_001),
            "retention pruning should still run after enough deferred ingests"
        );
    }

    #[test]
    fn store_initializes_sqlite_schema() {
        let tempdir = TempDir::new().expect("tempdir should build");
        let store = MediaArtifactStore::open(
            tempdir.path().join("media.sqlite3"),
            tempdir.path().join("media"),
            MediaRuntimeConfig::default(),
        )
        .expect("media store should initialize");
        let snapshot =
            store.build_connector_snapshot("discord:default").expect("snapshot should succeed");
        assert_eq!(snapshot.usage.artifact_count, 0);
        assert_eq!(snapshot.policy.max_attachments_per_message, 4);
    }

    #[test]
    fn derived_stats_return_zeroes_for_empty_store() {
        let tempdir = TempDir::new().expect("tempdir should build");
        let store = MediaArtifactStore::open(
            tempdir.path().join("media.sqlite3"),
            tempdir.path().join("media"),
            MediaRuntimeConfig::default(),
        )
        .expect("media store should initialize");

        let stats = store.derived_stats().expect("derived stats should succeed for an empty store");

        assert_eq!(stats.total, 0);
        assert_eq!(stats.pending, 0);
        assert_eq!(stats.succeeded, 0);
        assert_eq!(stats.failed, 0);
        assert_eq!(stats.quarantined, 0);
        assert_eq!(stats.purged, 0);
        assert_eq!(stats.recompute_required, 0);
        assert_eq!(stats.orphaned, 0);
    }

    #[test]
    fn purge_derived_artifact_preserves_lineage_and_updates_stats() {
        let tempdir = TempDir::new().expect("tempdir should build");
        let store = MediaArtifactStore::open(
            tempdir.path().join("media.sqlite3"),
            tempdir.path().join("media"),
            MediaRuntimeConfig { outbound_upload_enabled: true, ..MediaRuntimeConfig::default() },
        )
        .expect("media store should initialize");
        let attachment = store
            .store_console_attachment(ConsoleAttachmentStoreRequest {
                connector_id: "discord:default",
                session_id: "session-1",
                principal: "operator",
                device_id: "device-1",
                channel: Some("discord:channel:test"),
                attachment_id: "attachment-1",
                filename: "notes.txt",
                declared_content_type: "text/plain",
                bytes: b"hello world from a derived artifact",
            })
            .expect("console attachment should store successfully");
        let derived = DerivedArtifactContent {
            kind: DerivedArtifactKind::ExtractedText,
            parser_name: "attachment-document-extractor".to_owned(),
            parser_version: "1".to_owned(),
            content_text: "hello world from a derived artifact".to_owned(),
            content_hash: "content-hash-1".to_owned(),
            summary_text: "hello world from a derived artifact".to_owned(),
            language: Some("en".to_owned()),
            duration_ms: None,
            processing_ms: 12,
            warnings: Vec::new(),
            anchors: Vec::new(),
        };

        let record = store
            .upsert_derived_artifact(MediaDerivedArtifactUpsertRequest {
                source_artifact_id: attachment.artifact_id.as_str(),
                attachment_id: Some("attachment-1"),
                session_id: Some("session-1"),
                principal: Some("operator"),
                device_id: Some("device-1"),
                channel: Some("discord:channel:test"),
                filename: "notes.txt",
                declared_content_type: "text/plain",
                source_content_hash: attachment.sha256.as_str(),
                background_task_id: Some("task-1"),
                derived: &derived,
            })
            .expect("derived artifact should persist");
        let stats_before_purge =
            store.derived_stats().expect("derived stats should succeed after persisting a record");
        assert_eq!(stats_before_purge.total, 1);
        assert_eq!(stats_before_purge.succeeded, 1);

        let purged = store
            .purge_derived_artifact(record.derived_artifact_id.as_str())
            .expect("purge should succeed")
            .expect("purge should return the updated record");

        assert_eq!(purged.state, "purged");
        assert_eq!(purged.source_artifact_id, attachment.artifact_id);
        assert!(purged.content_text.is_none());
        assert!(purged.summary_text.is_none());
        assert!(purged.content_hash.is_none());
        assert!(
            purged.purged_at_unix_ms.is_some(),
            "purged records should keep an explicit purge timestamp"
        );

        let stats_after_purge =
            store.derived_stats().expect("derived stats should succeed after purge");
        assert_eq!(stats_after_purge.total, 1);
        assert_eq!(stats_after_purge.succeeded, 0);
        assert_eq!(stats_after_purge.purged, 1);
    }

    #[test]
    fn resolve_content_storage_path_rejects_digest_mismatch() {
        let tempdir = TempDir::new().expect("tempdir should build");
        let error = resolve_content_storage_path(
            tempdir.path(),
            "aa/not-the-real-digest",
            "bb1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcd",
        )
        .expect_err("digest mismatch should be rejected before joining filesystem paths");
        assert!(
            error.to_string().contains("does not match digest"),
            "digest mismatches should produce a clear validation error"
        );
    }

    #[test]
    fn resolve_content_storage_path_rejects_invalid_digest() {
        let tempdir = TempDir::new().expect("tempdir should build");
        let error = resolve_content_storage_path(
            tempdir.path(),
            "aa/not-a-real-digest",
            "../not-a-valid-sha256",
        )
        .expect_err("invalid digests should be rejected before building a path");
        assert!(
            error.to_string().contains("digest '../not-a-valid-sha256' is invalid"),
            "invalid digests should produce a clear validation error"
        );
    }

    #[test]
    fn load_console_attachment_rejects_tampered_storage_path() {
        let tempdir = TempDir::new().expect("tempdir should build");
        let store = MediaArtifactStore::open(
            tempdir.path().join("media.sqlite3"),
            tempdir.path().join("media"),
            MediaRuntimeConfig { outbound_upload_enabled: true, ..MediaRuntimeConfig::default() },
        )
        .expect("media store should initialize");
        let payload = store
            .store_console_attachment(super::ConsoleAttachmentStoreRequest {
                connector_id: "discord:default",
                session_id: "session-1",
                principal: "operator",
                device_id: "device-1",
                channel: Some("discord:channel:test"),
                attachment_id: "attachment-1",
                filename: "notes.txt",
                declared_content_type: "text/plain",
                bytes: b"hello world",
            })
            .expect("console attachment should store successfully");
        let tampered_path = "../outside.txt";
        let expected_path = content_relative_path(payload.sha256.as_str());
        {
            let guard = store.connection.lock().expect("db lock should succeed in test");
            guard
                .execute(
                    "UPDATE media_contents SET storage_rel_path = ?1 WHERE content_sha256 = ?2",
                    params![tampered_path, payload.sha256.as_str()],
                )
                .expect("test should be able to tamper storage path");
        }

        let error = store
            .load_artifact_payload(payload.artifact_id.as_str())
            .expect_err("tampered storage path should be rejected before filesystem access");
        assert!(
            error.to_string().contains("does not match digest"),
            "tampered storage path should fail explicit digest validation"
        );
        assert_ne!(
            expected_path, tampered_path,
            "test should prove the tampered path differs from the canonical layout"
        );
    }

    #[tokio::test]
    async fn inbound_ingest_keeps_download_denied_by_default() {
        let tempdir = TempDir::new().expect("tempdir should build");
        let store = MediaArtifactStore::open(
            tempdir.path().join("media.sqlite3"),
            tempdir.path().join("media"),
            MediaRuntimeConfig::default(),
        )
        .expect("media store should initialize");
        let attachment = AttachmentRef {
            kind: AttachmentKind::Image,
            attachment_id: Some("att-1".to_owned()),
            url: Some("https://cdn.discordapp.com/attachments/1/2/photo.png".to_owned()),
            filename: Some("photo.png".to_owned()),
            content_type: Some("image/png".to_owned()),
            size_bytes: Some(512),
            ..AttachmentRef::default()
        };

        let ingested = store
            .ingest_inbound_attachment(InboundAttachmentIngestRequest {
                connector_id: "discord:default",
                envelope_id: "01ARZ3NDEKTSV4RRFFQ69G5FAV",
                conversation_id: "discord:channel:test",
                adapter_message_id: Some("message-1"),
                attachment: &attachment,
                attachment_index: 0,
                attachment_count: 1,
                total_declared_bytes: 512,
            })
            .await
            .expect("ingest should not fail closed when downloads are disabled");

        assert_eq!(
            ingested.policy_context.as_deref(),
            Some("attachment.download.disabled"),
            "safe metadata should remain available while download stays denied by default"
        );
        assert!(ingested.artifact_ref.is_none());
        let snapshot = store.build_global_snapshot().expect("global snapshot should succeed");
        assert_eq!(snapshot.recent_blocked_reasons.len(), 1);
    }

    #[tokio::test]
    async fn inbound_ingest_blocks_localhost_attachment_targets() {
        let tempdir = TempDir::new().expect("tempdir should build");
        let config = MediaRuntimeConfig { download_enabled: true, ..MediaRuntimeConfig::default() };
        let store = MediaArtifactStore::open(
            tempdir.path().join("media.sqlite3"),
            tempdir.path().join("media"),
            config,
        )
        .expect("media store should initialize");
        let attachment = AttachmentRef {
            kind: AttachmentKind::Image,
            attachment_id: Some("att-2".to_owned()),
            url: Some("https://localhost/private.png".to_owned()),
            filename: Some("private.png".to_owned()),
            content_type: Some("image/png".to_owned()),
            size_bytes: Some(512),
            ..AttachmentRef::default()
        };

        let ingested = store
            .ingest_inbound_attachment(InboundAttachmentIngestRequest {
                connector_id: "discord:default",
                envelope_id: "01ARZ3NDEKTSV4RRFFQ69G5FAV",
                conversation_id: "discord:channel:test",
                adapter_message_id: Some("message-2"),
                attachment: &attachment,
                attachment_index: 0,
                attachment_count: 1,
                total_declared_bytes: 512,
            })
            .await
            .expect(
                "localhost targets should be downgraded into blocked attachments, not fatal errors",
            );

        assert!(
            ingested.policy_context.as_deref().is_some_and(|reason| reason.contains("localhost")),
            "localhost attachment URLs must be rejected before any download occurs"
        );
        assert!(ingested.artifact_ref.is_none());
        let snapshot = store.build_global_snapshot().expect("global snapshot should succeed");
        assert_eq!(snapshot.recent_blocked_reasons.len(), 1);
    }

    #[tokio::test]
    async fn inbound_ingest_blocks_non_allowlisted_attachment_hosts() {
        let tempdir = TempDir::new().expect("tempdir should build");
        let config = MediaRuntimeConfig { download_enabled: true, ..MediaRuntimeConfig::default() };
        let store = MediaArtifactStore::open(
            tempdir.path().join("media.sqlite3"),
            tempdir.path().join("media"),
            config,
        )
        .expect("media store should initialize");
        let attachment = AttachmentRef {
            kind: AttachmentKind::Image,
            attachment_id: Some("att-3".to_owned()),
            url: Some("https://example.com/photo.png".to_owned()),
            filename: Some("photo.png".to_owned()),
            content_type: Some("image/png".to_owned()),
            size_bytes: Some(512),
            ..AttachmentRef::default()
        };

        let ingested = store
            .ingest_inbound_attachment(InboundAttachmentIngestRequest {
                connector_id: "discord:default",
                envelope_id: "01ARZ3NDEKTSV4RRFFQ69G5FAV",
                conversation_id: "discord:channel:test",
                adapter_message_id: Some("message-3"),
                attachment: &attachment,
                attachment_index: 0,
                attachment_count: 1,
                total_declared_bytes: 512,
            })
            .await
            .expect("non-allowlisted hosts should be downgraded into blocked attachments");

        assert!(
            ingested
                .policy_context
                .as_deref()
                .is_some_and(|reason| reason.contains("not allowlisted")),
            "attachment hosts outside the Discord CDN allowlist must stay blocked"
        );
        assert!(ingested.artifact_ref.is_none());
        let snapshot = store.build_global_snapshot().expect("global snapshot should succeed");
        assert_eq!(snapshot.recent_blocked_reasons.len(), 1);
    }

    #[tokio::test]
    async fn read_response_body_with_limit_rejects_oversized_content_length_before_buffering() {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("test listener should bind");
        let address = listener.local_addr().expect("listener should expose address");
        let payload = Arc::new(vec![b'a'; 64]);
        let payload_for_server = Arc::clone(&payload);
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("server should accept connection");
            let mut request = [0u8; 1024];
            let _ = stream.read(&mut request).await.expect("server should read request");
            let response_head = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                payload_for_server.len()
            );
            stream
                .write_all(response_head.as_bytes())
                .await
                .expect("server should write response headers");
            stream
                .write_all(payload_for_server.as_slice())
                .await
                .expect("server should write payload");
        });

        let response = reqwest::Client::new()
            .get(format!("http://{address}"))
            .send()
            .await
            .expect("client request should succeed");
        let error = read_response_body_with_limit(response, 16)
            .await
            .expect_err("oversized content-length should be denied");
        assert!(
            error.to_string().contains("attachment body exceeds max_download_bytes (16)"),
            "oversized content-length denial should explain configured limit"
        );
        server.await.expect("server task should complete");
    }

    #[tokio::test]
    async fn read_response_body_with_limit_rejects_oversized_chunked_response() {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("test listener should bind");
        let address = listener.local_addr().expect("listener should expose address");
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("server should accept connection");
            let mut request = [0u8; 1024];
            let _ = stream.read(&mut request).await.expect("server should read request");
            stream
                .write_all(
                    b"HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n",
                )
                .await
                .expect("server should write response headers");
            stream
                .write_all(b"10\r\n0123456789ABCDEF\r\n10\r\nfedcba9876543210\r\n0\r\n\r\n")
                .await
                .expect("server should write chunked payload");
        });

        let response = reqwest::Client::new()
            .get(format!("http://{address}"))
            .send()
            .await
            .expect("client request should succeed");
        let error = read_response_body_with_limit(response, 16)
            .await
            .expect_err("oversized chunked response should be denied");
        assert!(
            error.to_string().contains("attachment body exceeds max_download_bytes (16)"),
            "oversized chunked denial should explain configured limit"
        );
        server.await.expect("server task should complete");
    }
}
