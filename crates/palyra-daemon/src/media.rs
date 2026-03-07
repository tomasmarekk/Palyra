use std::{
    fs,
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::Mutex,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use base64::{engine::general_purpose::STANDARD as BASE64_STANDARD, Engine as _};
use reqwest::{redirect::Policy, Url};
use rusqlite::{params, Connection, OptionalExtension};
use serde::Serialize;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

use palyra_common::netguard;
use palyra_connectors::{AttachmentKind, AttachmentRef};

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

pub struct MediaArtifactStore {
    content_root: PathBuf,
    config: MediaRuntimeConfig,
    connection: Mutex<Connection>,
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
        let store = Self { content_root, config, connection: Mutex::new(connection) };
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
        let storage_path = self.content_root.join(record.storage_rel_path.as_str());
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
        let client = reqwest::Client::builder()
            .redirect(Policy::none())
            .timeout(Duration::from_secs(15))
            .build()
            .map_err(|error| {
                MediaStoreError::Download(format!("failed to build media client: {error}"))
            })?;
        let mut redirects_followed = 0usize;
        let body = loop {
            let resolved = resolve_target_addresses(&current_url).await?;
            validate_resolved_addresses(resolved.as_slice())?;
            let response = client.get(current_url.clone()).send().await.map_err(|error| {
                MediaStoreError::Download(format!("attachment download request failed: {error}"))
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
            let bytes = response.bytes().await.map_err(|error| {
                MediaStoreError::Download(format!("attachment body read failed: {error}"))
            })?;
            if bytes.len() > self.config.max_download_bytes {
                return Err(MediaStoreError::NetworkPolicy(format!(
                    "attachment body exceeds max_download_bytes ({})",
                    self.config.max_download_bytes
                )));
            }
            break bytes.to_vec();
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
        let relative_path = content_relative_path(sha256.as_str());
        let storage_path = self.content_root.join(relative_path.as_str());
        if let Some(parent) = storage_path.parent() {
            fs::create_dir_all(parent).map_err(|error| {
                MediaStoreError::Io(format!(
                    "failed to create media artifact parent '{}' : {error}",
                    parent.display()
                ))
            })?;
        }
        if !storage_path.exists() {
            fs::write(storage_path.as_path(), body.as_slice()).map_err(|error| {
                MediaStoreError::Io(format!(
                    "failed to persist media artifact '{}' : {error}",
                    storage_path.display()
                ))
            })?;
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
        self.prune_retention()?;
        Ok(record)
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
    connection
        .execute("DELETE FROM media_contents WHERE content_sha256 = ?1", params![content_sha256])?;
    if let Some(storage_rel_path) = storage_rel_path {
        let _ = fs::remove_file(content_root.join(storage_rel_path.as_str()));
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

#[cfg(test)]
mod tests {
    use palyra_connectors::{AttachmentKind, AttachmentRef};
    use tempfile::TempDir;

    use super::{
        sniff_content, InboundAttachmentIngestRequest, MediaArtifactStore, MediaRuntimeConfig,
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
}
