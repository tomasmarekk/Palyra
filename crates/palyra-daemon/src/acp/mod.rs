//! ACP (Agent Client Protocol) runtime: durable session/conversation bindings
//! between external ACP clients and Palyra sessions, plus pending prompts and
//! binding conflict repair.
//!
//! [`AcpRuntime`] owns a single owner-only `bindings.json` index under the
//! daemon state root and rewrites it atomically (temp file + persist) on every
//! mutation. Security posture: binding configs that carry secret-bearing keys
//! are rejected outright, reloaded session bindings are marked
//! `stale_permissions` until the client re-presents its scopes/capabilities,
//! and repair actions that would widen access across principals or workspaces
//! are planned but never auto-applied. Consumed by the console ACP handlers in
//! `transport::http::handlers::console::acp`.

use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    fs,
    io::Write,
    path::{Path, PathBuf},
    sync::Mutex,
};

use palyra_common::{
    runtime_contracts::{
        AcpBindingConflictKind, AcpBindingRepairActionKind, AcpCapability, AcpClientContext,
        AcpCommand, AcpCommandResultEnvelope, AcpCursor, AcpEventLedgerKind, AcpEventLedgerRecord,
        AcpPendingPromptRecord, AcpProtocolVersionRange, AcpScope, AcpSessionBindingRecord,
        AcpSessionMode, ConversationBindingConflictState, ConversationBindingRecord,
        ConversationBindingSensitivity, StableErrorEnvelope, ACP_DEFAULT_DISCONNECT_GRACE_MS,
        ACP_PROTOCOL_MAX_VERSION, ACP_PROTOCOL_MIN_VERSION,
    },
    validate_canonical_id,
    versioned_json::{parse_versioned_json, VersionedJsonFormat},
};
use palyra_vault::{ensure_owner_only_dir, ensure_owner_only_file};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tempfile::Builder as TempFileBuilder;
use thiserror::Error;
use ulid::Ulid;
use validation::{normalize_scope_strings, normalize_state_root};

use crate::config::AcpRuntimeConfig;
use crate::{sha256_hex, unix_ms_now};

const ACP_BINDINGS_LAYOUT_VERSION: u32 = 1;
const ACP_BINDINGS_INDEX_FILE_NAME: &str = "bindings.json";
const ACP_BINDINGS_INDEX_FORMAT: VersionedJsonFormat =
    VersionedJsonFormat::new("ACP bindings index", ACP_BINDINGS_LAYOUT_VERSION);
const MAX_TEXT_BYTES: usize = 512;
const MAX_CONFIG_BYTES: usize = 16 * 1024;
const MAX_EVENT_LEDGER_PAYLOAD_BYTES: usize = 16 * 1024;
const ACP_PRESENTATION_SCHEMA_VERSION: u32 = 1;
pub(crate) const MAX_EVENT_LEDGER_EVENTS: usize = 1_024;
pub(crate) const MAX_EVENT_LEDGER_EVENTS_PER_SESSION: usize = 200;
const RATE_LIMIT_WINDOW_MS: i64 = 60_000;
const RATE_LIMIT_MAX_REQUESTS_PER_WINDOW: u32 = 120;

/// Failure modes of the ACP runtime; each maps to a stable error code via
/// [`AcpRuntimeError::stable_code`] for transport-facing envelopes.
#[derive(Debug, Error)]
pub(crate) enum AcpRuntimeError {
    #[error("failed to {operation} ACP state at {path}: {source}")]
    Io { operation: &'static str, path: PathBuf, source: std::io::Error },
    #[error("failed to parse ACP state at {path}: {source}")]
    Json { path: PathBuf, source: serde_json::Error },
    #[error("failed to parse versioned ACP state at {path}: {source}")]
    VersionedJson { path: PathBuf, source: anyhow::Error },
    #[error("failed to harden ACP state permissions for {path}: {message}")]
    PermissionHarden { path: PathBuf, message: String },
    #[error("invalid ACP field {field}: {message}")]
    InvalidField { field: &'static str, message: String },
    #[error("ACP protocol version {version} is unsupported")]
    UnsupportedProtocolVersion { version: u32 },
    #[error("ACP compatibility error: {message}")]
    Compatibility { message: String },
    #[error("ACP resource not found: {kind} {id}")]
    NotFound { kind: &'static str, id: String },
    #[error("ACP binding conflict: {message}")]
    Conflict { message: String },
    #[error("ACP state invariant failed: {message}")]
    StateInvariant { message: String },
    #[error("ACP request is not permitted: {message}")]
    Permission { message: String },
    #[error("ACP rate limit exceeded for {bucket}")]
    RateLimited { bucket: String },
}

impl AcpRuntimeError {
    /// Stable `acp/*` error code; part of the console API contract, so codes
    /// must never change for an existing variant.
    pub(crate) fn stable_code(&self) -> &'static str {
        match self {
            Self::Io { .. } | Self::Json { .. } | Self::VersionedJson { .. } => "acp/storage_error",
            Self::PermissionHarden { .. } => "acp/storage_permission_error",
            Self::InvalidField { .. } => "acp/invalid_field",
            Self::UnsupportedProtocolVersion { .. } => "acp/unsupported_protocol_version",
            Self::Compatibility { .. } => "acp/compatibility_error",
            Self::NotFound { .. } => "acp/not_found",
            Self::Conflict { .. } => "acp/conflict",
            Self::StateInvariant { .. } => "acp/state_invariant",
            Self::Permission { .. } => "acp/permission_denied",
            Self::RateLimited { .. } => "acp/rate_limited",
        }
    }

    /// Converts the error into the transport envelope, attaching a
    /// per-variant recovery hint for the client.
    pub(crate) fn to_stable_error(&self) -> StableErrorEnvelope {
        let recovery_hint = match self {
            Self::UnsupportedProtocolVersion { .. } => {
                "Reconnect using a supported ACP protocol version."
            }
            Self::Compatibility { .. } => {
                "Refresh the ACP client/runtime contract and retry with a supported event type."
            }
            Self::RateLimited { .. } => "Wait for the current ACP rate-limit window to reset.",
            Self::Permission { .. } => "Request the required ACP scope or capability.",
            Self::NotFound { .. } => "Refresh the ACP session list and retry with a current id.",
            Self::Conflict { .. } => "Inspect the binding diagnostics and repair the conflict.",
            Self::StateInvariant { .. } => "Reload ACP state and retry the operation.",
            Self::InvalidField { .. } => "Fix the request payload and retry.",
            Self::PermissionHarden { .. }
            | Self::Io { .. }
            | Self::Json { .. }
            | Self::VersionedJson { .. } => "Inspect daemon storage permissions and retry.",
        };
        StableErrorEnvelope::new(self.stable_code(), self.to_string(), recovery_hint)
    }
}

/// Shorthand result type for ACP runtime operations.
pub(crate) type AcpRuntimeResult<T> = Result<T, AcpRuntimeError>;

/// On-disk shape of `bindings.json`: every binding and pending prompt the ACP
/// runtime knows about, versioned by `schema_version`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct AcpBindingsIndex {
    pub(crate) schema_version: u32,
    pub(crate) updated_at_unix_ms: i64,
    #[serde(default)]
    pub(crate) session_bindings: Vec<AcpSessionBindingRecord>,
    #[serde(default)]
    pub(crate) conversation_bindings: Vec<ConversationBindingRecord>,
    #[serde(default)]
    pub(crate) pending_prompts: Vec<AcpPendingPromptRecord>,
    #[serde(default)]
    pub(crate) event_ledger: Vec<AcpEventLedgerRecord>,
}

impl Default for AcpBindingsIndex {
    fn default() -> Self {
        Self {
            schema_version: ACP_BINDINGS_LAYOUT_VERSION,
            updated_at_unix_ms: 0,
            session_bindings: Vec::new(),
            conversation_bindings: Vec::new(),
            pending_prompts: Vec::new(),
            event_ledger: Vec::new(),
        }
    }
}

/// Request to create or refresh the binding between an ACP client session and
/// a Palyra session.
#[derive(Debug, Clone)]
pub(crate) struct AcpSessionBindingUpsert {
    pub(crate) context: AcpClientContext,
    pub(crate) acp_session_id: String,
    pub(crate) palyra_session_id: String,
    pub(crate) session_key: String,
    pub(crate) session_label: Option<String>,
    pub(crate) mode: AcpSessionMode,
    pub(crate) config: Value,
    pub(crate) cursor: AcpCursor,
}

/// Request to remember a prompt (approval/permission ask) that must survive a
/// short client disconnect; `ttl_ms` is clamped to the disconnect grace window.
#[derive(Debug, Clone)]
pub(crate) struct AcpPendingPromptUpsert {
    pub(crate) prompt_id: String,
    pub(crate) acp_client_id: String,
    pub(crate) acp_session_id: String,
    pub(crate) palyra_session_id: String,
    pub(crate) approval_id: Option<String>,
    pub(crate) run_id: Option<String>,
    pub(crate) prompt_kind: String,
    pub(crate) redacted_summary: String,
    pub(crate) ttl_ms: i64,
}

/// Request to append a redacted ACP event into the reconnect replay ledger.
#[derive(Debug, Clone)]
pub(crate) struct AcpEventLedgerAppend {
    pub(crate) context: AcpClientContext,
    pub(crate) acp_session_id: String,
    pub(crate) palyra_session_id: String,
    pub(crate) kind: AcpEventLedgerKind,
    pub(crate) run_id: Option<String>,
    pub(crate) approval_id: Option<String>,
    pub(crate) redacted_summary: String,
    pub(crate) redacted_payload: Value,
}

/// What a reconnecting ACP client gets back: its refreshed binding, prompts
/// still inside the grace window, and the ids of prompts that expired.
#[derive(Debug, Clone)]
pub(crate) struct AcpReconnectOutcome {
    pub(crate) binding: AcpSessionBindingRecord,
    pub(crate) pending_prompts: Vec<AcpPendingPromptRecord>,
    pub(crate) expired_prompt_ids: Vec<String>,
    pub(crate) event_ledger: Vec<AcpEventLedgerRecord>,
}

/// Optional filters for [`AcpRuntime::list_conversation_bindings`]; unset
/// fields match everything, and detached bindings are hidden by default.
#[derive(Debug, Clone, Default)]
pub(crate) struct ConversationBindingFilter {
    pub(crate) owner_principal: Option<String>,
    pub(crate) connector_kind: Option<String>,
    pub(crate) external_identity: Option<String>,
    pub(crate) palyra_session_id: Option<String>,
    pub(crate) include_detached: bool,
    pub(crate) limit: Option<usize>,
}

/// Request to bind an external connector conversation (e.g. a chat thread) to
/// a Palyra session under a single owner principal.
#[derive(Debug, Clone)]
pub(crate) struct ConversationBindingUpsert {
    pub(crate) connector_kind: String,
    pub(crate) external_identity: String,
    pub(crate) external_conversation_id: String,
    pub(crate) palyra_session_id: String,
    pub(crate) owner_principal: String,
    pub(crate) device_id: String,
    pub(crate) channel: Option<String>,
    pub(crate) scopes: Vec<String>,
    pub(crate) sensitivity: ConversationBindingSensitivity,
    pub(crate) delivery_cursor: AcpCursor,
    pub(crate) last_event_id: Option<String>,
}

/// Ordered set of repair actions for binding conflicts; serialized as-is into
/// console responses (and pinned by a golden fixture test).
#[derive(Debug, Clone, Serialize)]
pub(crate) struct BindingRepairPlan {
    pub(crate) dry_run: bool,
    pub(crate) actions: Vec<BindingRepairAction>,
}

/// One planned repair step. `automatic_apply == false` marks actions that
/// would widen access (principal/workspace conflicts) and therefore require
/// an explicit operator decision.
#[derive(Debug, Clone, Serialize)]
pub(crate) struct BindingRepairAction {
    pub(crate) action: String,
    pub(crate) conflict_kind: String,
    pub(crate) target_kind: String,
    pub(crate) binding_id: String,
    pub(crate) reason: String,
    pub(crate) target_session_id: String,
    pub(crate) policy_gate: String,
    pub(crate) automatic_apply: bool,
}

/// Diagnostic view of one binding (session or conversation) with its conflict
/// state and the repair actions that currently target it.
#[derive(Debug, Clone, Serialize)]
pub(crate) struct BindingExplainSnapshot {
    pub(crate) binding_id: String,
    pub(crate) binding_kind: String,
    pub(crate) owner_principal: String,
    pub(crate) palyra_session_id: String,
    pub(crate) external_identity: Option<String>,
    pub(crate) external_conversation_id: Option<String>,
    pub(crate) acp_client_id: Option<String>,
    pub(crate) acp_session_id: Option<String>,
    pub(crate) conflict_state: String,
    pub(crate) conflict_kinds: Vec<String>,
    pub(crate) repair_actions: Vec<String>,
    pub(crate) stale_permissions: bool,
    pub(crate) last_event_id: Option<String>,
    pub(crate) delivery_cursor: u64,
}

/// Input used to build the editor-facing ACP presentation projection.
pub(crate) struct AcpPresentationProjectionInput<'a> {
    pub(crate) event_kind: &'a str,
    pub(crate) run_id: Option<&'a str>,
    pub(crate) session_id: Option<&'a str>,
    pub(crate) tape_segment: Option<&'a str>,
    pub(crate) compaction_generation: Option<u64>,
    pub(crate) source_binding: Option<&'a str>,
    pub(crate) payload: &'a Value,
}

/// Metadata attached to every ACP presentation object.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub(crate) struct AcpPresentationMetadata {
    pub(crate) run_id: Option<String>,
    pub(crate) session_id: Option<String>,
    pub(crate) tape_segment: Option<String>,
    pub(crate) compaction_generation: Option<u64>,
    pub(crate) source_binding: Option<String>,
}

/// Reviewable edit proposal shown by ACP clients before workspace mutation.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub(crate) struct AcpEditProposalPresentation {
    pub(crate) proposal_id: String,
    pub(crate) diff_summary: String,
    pub(crate) risk_level: String,
    pub(crate) affected_files: Vec<String>,
    pub(crate) approval_actions: Vec<String>,
    pub(crate) provenance_refs: Vec<String>,
}

/// Rich content block classes rendered by ACP clients.
#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub(crate) enum AcpRichContentBlockKind {
    Image,
    EmbeddedResource,
    FileResource,
    FileUri,
    SearchResult,
    BrowserResult,
    MemoryResult,
    ArtifactRef,
}

/// Redacted ACP rich content block. It carries references and preview text,
/// not raw binary data or secret-bearing payloads.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub(crate) struct AcpRichContentBlock {
    pub(crate) block_id: String,
    pub(crate) block_kind: AcpRichContentBlockKind,
    pub(crate) source_ref: String,
    pub(crate) title: String,
    pub(crate) preview: Option<String>,
    pub(crate) provenance_refs: Vec<String>,
}

/// Renderer policy for one ACP tool-output class.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub(crate) struct AcpToolOutputRenderer {
    pub(crate) renderer: String,
    pub(crate) content_policy: String,
    pub(crate) redaction_policy: String,
}

/// Editor-facing projection for ACP events, approvals, and replay entries.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub(crate) struct AcpPresentationProjection {
    pub(crate) schema_version: u32,
    pub(crate) event_type: String,
    pub(crate) event_kind: String,
    pub(crate) metadata: AcpPresentationMetadata,
    pub(crate) edit_proposal: Option<AcpEditProposalPresentation>,
    pub(crate) rich_blocks: Vec<AcpRichContentBlock>,
    pub(crate) renderers: Vec<AcpToolOutputRenderer>,
    pub(crate) redaction_level: String,
}

/// Builds a redacted ACP presentation projection from event or approval params.
pub(crate) fn build_acp_presentation_projection(
    input: AcpPresentationProjectionInput<'_>,
) -> AcpPresentationProjection {
    AcpPresentationProjection {
        schema_version: ACP_PRESENTATION_SCHEMA_VERSION,
        event_type: "acp.presentation_projection".to_owned(),
        event_kind: normalize_text_lossy(input.event_kind, 80),
        metadata: AcpPresentationMetadata {
            run_id: input.run_id.map(ToOwned::to_owned),
            session_id: input.session_id.map(ToOwned::to_owned),
            tape_segment: input.tape_segment.map(|value| normalize_text_lossy(value, 120)),
            compaction_generation: input.compaction_generation,
            source_binding: input.source_binding.map(|value| normalize_text_lossy(value, 160)),
        },
        edit_proposal: edit_proposal_from_presentation_payload(input.payload),
        rich_blocks: rich_blocks_from_presentation_payload(input.payload),
        renderers: renderers_from_presentation_payload(input.payload),
        redaction_level: "metadata_only".to_owned(),
    }
}

/// Thread-safe owner of the ACP bindings index and per-client rate limits.
///
/// All reads and writes go through the `index` mutex; mutations call
/// `save_locked_index` while still holding the guard so the file on disk can
/// never get ahead of (or behind) the in-memory state.
#[derive(Debug)]
pub(crate) struct AcpRuntime {
    root: PathBuf,
    index_path: PathBuf,
    index: Mutex<AcpBindingsIndex>,
    rate_limits: Mutex<BTreeMap<String, RateLimitBucket>>,
    live_manager: live_runtime_manager::AcpLiveRuntimeManager,
}

// Fixed-window counter; coarse but sufficient for a local console surface.
#[derive(Debug, Clone)]
struct RateLimitBucket {
    window_started_at_unix_ms: i64,
    requests_in_window: u32,
}

impl AcpRuntime {
    /// Opens (or initializes) the ACP state directory rooted at `root`.
    ///
    /// On load, every session binding is flagged `stale_permissions`: a
    /// daemon restart must not silently trust scopes granted before it, so
    /// clients re-assert them on their next reconnect.
    ///
    /// # Errors
    /// Fails on an invalid root path (empty or traversal components), on
    /// directory/permission hardening failures, or when an existing index
    /// cannot be read, parsed, or validated.
    #[cfg(test)]
    pub(crate) fn open(root: PathBuf) -> AcpRuntimeResult<Self> {
        Self::open_with_live_runtime(root, false, AcpRuntimeConfig::default())
    }

    /// Opens ACP state with the trusted live-runtime registry.
    ///
    /// # Errors
    /// Returns the same storage errors as [`Self::open`] and rejects invalid
    /// persisted live-runtime binding metadata.
    pub(crate) fn open_with_live_runtime(
        root: PathBuf,
        rollout_enabled: bool,
        live_config: AcpRuntimeConfig,
    ) -> AcpRuntimeResult<Self> {
        let root = normalize_state_root(root.as_path())?;
        create_state_dir(root.as_path())?;
        let root = fs::canonicalize(root.as_path()).map_err(|source| AcpRuntimeError::Io {
            operation: "canonicalize",
            path: root.clone(),
            source,
        })?;
        let index_path = root.join(ACP_BINDINGS_INDEX_FILE_NAME);
        let mut index = load_index(index_path.as_path())?;
        let now = unix_ms_now().map_err(|error| AcpRuntimeError::InvalidField {
            field: "system_time",
            message: error.to_string(),
        })?;
        let changed = mark_loaded_permissions_stale(&mut index, now);
        if changed {
            save_index(root.as_path(), &index)?;
        }
        let live_manager = live_runtime_manager::AcpLiveRuntimeManager::open(
            root.as_path(),
            rollout_enabled,
            live_config,
        )?;
        Ok(Self {
            root,
            index_path,
            index: Mutex::new(index),
            rate_limits: Mutex::new(BTreeMap::new()),
            live_manager,
        })
    }

    /// Canonicalized ACP state root directory.
    pub(crate) fn root(&self) -> &Path {
        self.root.as_path()
    }

    /// Live ACP runtime manager backed only by trusted daemon configuration.
    pub(crate) fn live_manager(&self) -> &live_runtime_manager::AcpLiveRuntimeManager {
        &self.live_manager
    }

    /// ACP protocol versions this daemon accepts.
    pub(crate) fn protocol_range(&self) -> AcpProtocolVersionRange {
        AcpProtocolVersionRange::default()
    }

    /// Returns a point-in-time copy of the full bindings index.
    ///
    /// # Errors
    /// Returns a conflict error when the index lock is poisoned.
    pub(crate) fn snapshot(&self) -> AcpRuntimeResult<AcpBindingsIndex> {
        Ok(self.lock_index()?.clone())
    }

    /// Counts one request against the per-client, per-command fixed window.
    ///
    /// # Errors
    /// Returns `RateLimited` once the window budget is exhausted, an invalid
    /// field error for an unusable `client_id`, or a conflict error when the
    /// rate-limit lock is poisoned.
    pub(crate) fn check_rate_limit(
        &self,
        client_id: &str,
        command: AcpCommand,
        now_unix_ms: i64,
    ) -> AcpRuntimeResult<()> {
        let bucket_key = format!("{}:{}", normalize_text(client_id, "client_id", 128)?, command);
        let mut buckets = self.rate_limits.lock().map_err(|_| AcpRuntimeError::Conflict {
            message: "ACP rate-limit state lock is poisoned".to_owned(),
        })?;
        let bucket = buckets.entry(bucket_key.clone()).or_insert(RateLimitBucket {
            window_started_at_unix_ms: now_unix_ms,
            requests_in_window: 0,
        });
        if now_unix_ms.saturating_sub(bucket.window_started_at_unix_ms) >= RATE_LIMIT_WINDOW_MS {
            bucket.window_started_at_unix_ms = now_unix_ms;
            bucket.requests_in_window = 0;
        }
        if bucket.requests_in_window >= RATE_LIMIT_MAX_REQUESTS_PER_WINDOW {
            return Err(AcpRuntimeError::RateLimited { bucket: bucket_key });
        }
        bucket.requests_in_window = bucket.requests_in_window.saturating_add(1);
        Ok(())
    }

    /// Creates or replaces the binding for `(client_id, acp_session_id)`,
    /// validating the protocol version, scopes, ids, and config first.
    ///
    /// # Errors
    /// Fails closed on unsupported protocol versions, empty scope/capability
    /// sets, non-canonical session ids, secret-bearing or oversized configs,
    /// and storage errors.
    pub(crate) fn upsert_session_binding(
        &self,
        request: AcpSessionBindingUpsert,
    ) -> AcpRuntimeResult<AcpSessionBindingRecord> {
        validate_protocol_version(request.context.protocol_version)?;
        validate_scopes_and_capabilities(&request.context)?;
        validate_canonical_id(request.palyra_session_id.as_str()).map_err(|_| {
            AcpRuntimeError::InvalidField {
                field: "palyra_session_id",
                message: "expected canonical Palyra session id".to_owned(),
            }
        })?;
        reject_sensitive_config(&request.config)?;
        let config_bytes = serde_json::to_vec(&request.config)
            .map_err(|source| AcpRuntimeError::Json { path: self.index_path.clone(), source })?;
        if config_bytes.len() > MAX_CONFIG_BYTES {
            return Err(AcpRuntimeError::InvalidField {
                field: "config",
                message: format!("config exceeds {MAX_CONFIG_BYTES} bytes"),
            });
        }

        let now = unix_ms_now().map_err(|error| AcpRuntimeError::InvalidField {
            field: "system_time",
            message: error.to_string(),
        })?;
        let mut index = self.lock_index()?;
        let acp_client_id = normalize_text(&request.context.client_id, "client_id", 128)?;
        let acp_session_id = normalize_text(&request.acp_session_id, "acp_session_id", 128)?;
        let session_key = normalize_text(&request.session_key, "session_key", 512)?;
        let session_label =
            normalize_optional_text(request.session_label.as_deref(), "session_label", 128)?;
        let existing = index.session_bindings.iter().position(|entry| {
            entry.acp_client_id == acp_client_id && entry.acp_session_id == acp_session_id
        });
        let binding_id = existing
            .and_then(|position| index.session_bindings.get(position))
            .map(|entry| entry.binding_id.clone())
            .unwrap_or_else(|| format!("acpbind_{}", Ulid::generate()));
        let record = AcpSessionBindingRecord {
            schema_version: ACP_BINDINGS_LAYOUT_VERSION,
            binding_id,
            acp_client_id,
            acp_session_id,
            palyra_session_id: request.palyra_session_id,
            session_key,
            session_label,
            owner_principal: normalize_text(
                &request.context.owner_principal,
                "owner_principal",
                128,
            )?,
            device_id: normalize_text(&request.context.device_id, "device_id", 128)?,
            channel: normalize_optional_text(request.context.channel.as_deref(), "channel", 128)?,
            scopes: sorted_scopes(request.context.scopes),
            capabilities: sorted_capabilities(request.context.capabilities),
            mode: request.mode,
            config: request.config,
            cursor: request.cursor,
            last_seen_at_unix_ms: now,
            protocol_version: request.context.protocol_version,
            stale_permissions: false,
        };
        if let Some(position) = existing {
            index.session_bindings[position] = record.clone();
        } else {
            index.session_bindings.push(record.clone());
        }
        save_locked_index(self.root.as_path(), &mut index)?;
        Ok(record)
    }

    /// Looks up a session binding by its `binding_id`.
    ///
    /// # Errors
    /// Returns `NotFound` for unknown ids, plus validation/lock errors.
    pub(crate) fn get_session_binding(
        &self,
        binding_id: &str,
    ) -> AcpRuntimeResult<AcpSessionBindingRecord> {
        let binding_id = normalize_text(binding_id, "binding_id", 160)?;
        self.lock_index()?
            .session_bindings
            .iter()
            .find(|entry| entry.binding_id == binding_id)
            .cloned()
            .ok_or(AcpRuntimeError::NotFound { kind: "session_binding", id: binding_id })
    }

    /// Looks up a session binding by its client-facing identity pair.
    ///
    /// # Errors
    /// Returns `NotFound` for unknown pairs, plus validation/lock errors.
    pub(crate) fn session_binding_for_acp(
        &self,
        client_id: &str,
        acp_session_id: &str,
    ) -> AcpRuntimeResult<AcpSessionBindingRecord> {
        let client_id = normalize_text(client_id, "client_id", 128)?;
        let acp_session_id = normalize_text(acp_session_id, "acp_session_id", 128)?;
        self.lock_index()?
            .session_bindings
            .iter()
            .find(|entry| {
                entry.acp_client_id == client_id && entry.acp_session_id == acp_session_id
            })
            .cloned()
            .ok_or(AcpRuntimeError::NotFound {
                kind: "acp_session_binding",
                id: format!("{client_id}/{acp_session_id}"),
            })
    }

    /// Lists session bindings, optionally restricted to one owner principal.
    ///
    /// # Errors
    /// Returns validation errors for an unusable principal filter or a
    /// conflict error when the index lock is poisoned.
    pub(crate) fn list_session_bindings(
        &self,
        owner_principal: Option<&str>,
    ) -> AcpRuntimeResult<Vec<AcpSessionBindingRecord>> {
        let owner_principal = owner_principal
            .map(|value| normalize_text(value, "owner_principal", 128))
            .transpose()?;
        let records = self
            .lock_index()?
            .session_bindings
            .iter()
            .filter(|entry| {
                owner_principal.as_ref().is_none_or(|owner| entry.owner_principal.as_str() == owner)
            })
            .cloned()
            .collect();
        Ok(records)
    }

    /// Re-attaches a returning client to its session binding: refreshes the
    /// cursor, scopes, and capabilities (clearing `stale_permissions`) and
    /// returns prompts still pending inside the disconnect grace window.
    /// Event-ledger replay is included only when the returning client presents
    /// both `events:read` and `session_replay`.
    ///
    /// Only the binding's recorded owner principal may reconnect; this is the
    /// permission boundary that stops a different ACP client identity from
    /// adopting someone else's session.
    ///
    /// # Errors
    /// Fails on unsupported protocol versions, empty scope/capability sets,
    /// unknown bindings, owner mismatch, and storage errors.
    pub(crate) fn reconnect(
        &self,
        context: &AcpClientContext,
        acp_session_id: &str,
        cursor: AcpCursor,
    ) -> AcpRuntimeResult<AcpReconnectOutcome> {
        validate_protocol_version(context.protocol_version)?;
        validate_scopes_and_capabilities(context)?;
        let now = unix_ms_now().map_err(|error| AcpRuntimeError::InvalidField {
            field: "system_time",
            message: error.to_string(),
        })?;
        let mut index = self.lock_index()?;
        let client_id = normalize_text(&context.client_id, "client_id", 128)?;
        let acp_session_id = normalize_text(acp_session_id, "acp_session_id", 128)?;
        let Some(position) = index.session_bindings.iter().position(|entry| {
            entry.acp_client_id == client_id && entry.acp_session_id == acp_session_id
        }) else {
            return Err(AcpRuntimeError::NotFound {
                kind: "acp_session_binding",
                id: format!("{client_id}/{acp_session_id}"),
            });
        };
        if index.session_bindings[position].owner_principal != context.owner_principal {
            return Err(AcpRuntimeError::Permission {
                message: "ACP binding owner does not match authenticated principal".to_owned(),
            });
        }
        let expired_prompt_ids = prune_expired_pending_prompts(&mut index, now);
        index.session_bindings[position].cursor = cursor;
        index.session_bindings[position].last_seen_at_unix_ms = now;
        index.session_bindings[position].protocol_version = context.protocol_version;
        index.session_bindings[position].scopes = sorted_scopes(context.scopes.clone());
        index.session_bindings[position].capabilities =
            sorted_capabilities(context.capabilities.clone());
        index.session_bindings[position].stale_permissions = false;
        let binding = index.session_bindings[position].clone();
        let pending_prompts = index
            .pending_prompts
            .iter()
            .filter(|entry| {
                entry.acp_client_id == binding.acp_client_id
                    && entry.acp_session_id == binding.acp_session_id
                    && entry.expires_at_unix_ms >= now
            })
            .cloned()
            .collect();
        let event_ledger = if can_replay_event_ledger(context) {
            event_ledger_after_cursor(
                &index,
                binding.acp_client_id.as_str(),
                binding.acp_session_id.as_str(),
                binding.palyra_session_id.as_str(),
                cursor.sequence,
                MAX_EVENT_LEDGER_EVENTS_PER_SESSION,
            )
        } else {
            Vec::new()
        };
        save_locked_index(self.root.as_path(), &mut index)?;
        Ok(AcpReconnectOutcome { binding, pending_prompts, expired_prompt_ids, event_ledger })
    }

    /// Persists (or refreshes) a pending prompt so it can be re-delivered if
    /// the client reconnects within the grace window; the stored summary is
    /// expected to be pre-redacted by the caller.
    ///
    /// # Errors
    /// Fails on invalid ids or text fields and on storage errors.
    pub(crate) fn remember_pending_prompt(
        &self,
        request: AcpPendingPromptUpsert,
    ) -> AcpRuntimeResult<AcpPendingPromptRecord> {
        let now = unix_ms_now().map_err(|error| AcpRuntimeError::InvalidField {
            field: "system_time",
            message: error.to_string(),
        })?;
        let ttl_ms = request.ttl_ms.clamp(1_000, ACP_DEFAULT_DISCONNECT_GRACE_MS);
        validate_canonical_id(request.palyra_session_id.as_str()).map_err(|_| {
            AcpRuntimeError::InvalidField {
                field: "palyra_session_id",
                message: "expected canonical Palyra session id".to_owned(),
            }
        })?;
        validate_optional_canonical_id(request.approval_id.as_deref(), "approval_id")?;
        validate_optional_canonical_id(request.run_id.as_deref(), "run_id")?;
        let record = AcpPendingPromptRecord {
            prompt_id: normalize_text(&request.prompt_id, "prompt_id", 160)?,
            acp_client_id: normalize_text(&request.acp_client_id, "acp_client_id", 128)?,
            acp_session_id: normalize_text(&request.acp_session_id, "acp_session_id", 128)?,
            palyra_session_id: request.palyra_session_id,
            approval_id: request.approval_id,
            run_id: request.run_id,
            prompt_kind: normalize_text(&request.prompt_kind, "prompt_kind", 64)?,
            redacted_summary: normalize_text(
                &request.redacted_summary,
                "redacted_summary",
                MAX_TEXT_BYTES,
            )?,
            created_at_unix_ms: now,
            expires_at_unix_ms: now.saturating_add(ttl_ms),
        };
        let mut index = self.lock_index()?;
        prune_expired_pending_prompts(&mut index, now);
        if let Some(position) =
            index.pending_prompts.iter().position(|entry| entry.prompt_id == record.prompt_id)
        {
            index.pending_prompts[position] = record.clone();
        } else {
            index.pending_prompts.push(record.clone());
        }
        save_locked_index(self.root.as_path(), &mut index)?;
        Ok(record)
    }

    /// Appends a redacted ACP event to the bounded reconnect replay ledger.
    ///
    /// The stored record contains only a redacted summary and a SHA-256 digest
    /// of the sanitized payload. The raw payload never reaches `bindings.json`.
    ///
    /// # Errors
    /// Fails on invalid ids, unsupported protocol versions, owner mismatches,
    /// payloads that exceed the bounded hashing budget, and storage errors.
    pub(crate) fn record_event(
        &self,
        request: AcpEventLedgerAppend,
    ) -> AcpRuntimeResult<AcpEventLedgerRecord> {
        validate_protocol_version(request.context.protocol_version)?;
        validate_scopes_and_capabilities(&request.context)?;
        validate_canonical_id(request.palyra_session_id.as_str()).map_err(|_| {
            AcpRuntimeError::InvalidField {
                field: "palyra_session_id",
                message: "expected canonical Palyra session id".to_owned(),
            }
        })?;
        validate_optional_canonical_id(request.run_id.as_deref(), "run_id")?;
        validate_optional_canonical_id(request.approval_id.as_deref(), "approval_id")?;
        let now = unix_ms_now().map_err(|error| AcpRuntimeError::InvalidField {
            field: "system_time",
            message: error.to_string(),
        })?;
        let acp_client_id = normalize_text(&request.context.client_id, "client_id", 128)?;
        let acp_session_id = normalize_text(&request.acp_session_id, "acp_session_id", 128)?;
        let owner_principal =
            normalize_text(&request.context.owner_principal, "owner_principal", 128)?;
        let redacted_summary =
            normalize_text(&request.redacted_summary, "redacted_summary", MAX_TEXT_BYTES)?;
        let mut redacted_payload = request.redacted_payload;
        redact_sensitive_payload_fields(&mut redacted_payload);
        let payload_bytes = serde_json::to_vec(&redacted_payload)
            .map_err(|source| AcpRuntimeError::Json { path: self.index_path.clone(), source })?;
        if payload_bytes.len() > MAX_EVENT_LEDGER_PAYLOAD_BYTES {
            return Err(AcpRuntimeError::InvalidField {
                field: "redacted_payload",
                message: format!("payload exceeds {MAX_EVENT_LEDGER_PAYLOAD_BYTES} bytes"),
            });
        }

        let mut index = self.lock_index()?;
        if !index.session_bindings.iter().any(|entry| {
            entry.acp_client_id == acp_client_id
                && entry.acp_session_id == acp_session_id
                && entry.palyra_session_id == request.palyra_session_id
                && entry.owner_principal == owner_principal
        }) {
            return Err(AcpRuntimeError::NotFound {
                kind: "acp_session_binding",
                id: format!("{acp_client_id}/{acp_session_id}"),
            });
        }
        let sequence = next_event_ledger_sequence(
            &index,
            acp_client_id.as_str(),
            acp_session_id.as_str(),
            request.palyra_session_id.as_str(),
        );
        let record = AcpEventLedgerRecord {
            schema_version: ACP_BINDINGS_LAYOUT_VERSION,
            event_id: format!("acpevt_{}", Ulid::generate()),
            kind: request.kind,
            sequence,
            acp_client_id,
            acp_session_id,
            palyra_session_id: request.palyra_session_id,
            run_id: request.run_id,
            approval_id: request.approval_id,
            redacted_summary,
            payload_sha256: sha256_hex(payload_bytes.as_slice()),
            created_at_unix_ms: now,
            protocol_version: request.context.protocol_version,
        };
        index.event_ledger.push(record.clone());
        prune_event_ledger(&mut index);
        save_locked_index(self.root.as_path(), &mut index)?;
        Ok(record)
    }

    /// Creates or updates a conversation binding, then re-derives conflict
    /// states across all bindings sharing the same external conversation.
    ///
    /// # Errors
    /// Fails on invalid identifiers/scopes and on storage errors; a state
    /// invariant error means the record vanished during normalization.
    pub(crate) fn upsert_conversation_binding(
        &self,
        request: ConversationBindingUpsert,
    ) -> AcpRuntimeResult<ConversationBindingRecord> {
        validate_canonical_id(request.palyra_session_id.as_str()).map_err(|_| {
            AcpRuntimeError::InvalidField {
                field: "palyra_session_id",
                message: "expected canonical Palyra session id".to_owned(),
            }
        })?;
        let now = unix_ms_now().map_err(|error| AcpRuntimeError::InvalidField {
            field: "system_time",
            message: error.to_string(),
        })?;
        let connector_kind =
            normalize_binding_component(&request.connector_kind, "connector_kind")?;
        let external_identity =
            normalize_text(&request.external_identity, "external_identity", 256)?;
        let external_conversation_id =
            normalize_text(&request.external_conversation_id, "external_conversation_id", 256)?;
        let owner_principal = normalize_text(&request.owner_principal, "owner_principal", 128)?;
        let mut index = self.lock_index()?;
        let existing = index.conversation_bindings.iter().position(|entry| {
            entry.connector_kind == connector_kind
                && entry.external_identity == external_identity
                && entry.external_conversation_id == external_conversation_id
                && entry.palyra_session_id == request.palyra_session_id
                && entry.conflict_state != ConversationBindingConflictState::Detached
        });
        let created_at = existing
            .and_then(|position| index.conversation_bindings.get(position))
            .map(|entry| entry.created_at_unix_ms)
            .unwrap_or(now);
        let binding_id = existing
            .and_then(|position| index.conversation_bindings.get(position))
            .map(|entry| entry.binding_id.clone())
            .unwrap_or_else(|| format!("convbind_{}", Ulid::generate()));
        let record = ConversationBindingRecord {
            schema_version: ACP_BINDINGS_LAYOUT_VERSION,
            binding_id,
            connector_kind,
            external_identity,
            external_conversation_id,
            palyra_session_id: request.palyra_session_id,
            owner_principal,
            device_id: normalize_text(&request.device_id, "device_id", 128)?,
            channel: normalize_optional_text(request.channel.as_deref(), "channel", 128)?,
            scopes: normalize_scope_strings(request.scopes)?,
            sensitivity: request.sensitivity,
            delivery_cursor: request.delivery_cursor,
            last_event_id: normalize_optional_text(
                request.last_event_id.as_deref(),
                "last_event_id",
                160,
            )?,
            conflict_state: ConversationBindingConflictState::None,
            created_at_unix_ms: created_at,
            updated_at_unix_ms: now,
        };
        if let Some(position) = existing {
            index.conversation_bindings[position] = record.clone();
        } else {
            index.conversation_bindings.push(record.clone());
        }
        normalize_conversation_conflicts(&mut index);
        let saved = index
            .conversation_bindings
            .iter()
            .find(|entry| entry.binding_id == record.binding_id)
            .cloned()
            .ok_or_else(|| AcpRuntimeError::StateInvariant {
                message:
                    "inserted conversation binding was not present after conflict normalization"
                        .to_owned(),
            })?;
        save_locked_index(self.root.as_path(), &mut index)?;
        Ok(saved)
    }

    /// Lists conversation bindings matching `filter`, newest first, capped at
    /// the clamped limit (1..=500, default 100).
    ///
    /// # Errors
    /// Returns validation errors for unusable filter values or a conflict
    /// error when the index lock is poisoned.
    pub(crate) fn list_conversation_bindings(
        &self,
        filter: ConversationBindingFilter,
    ) -> AcpRuntimeResult<Vec<ConversationBindingRecord>> {
        let owner_principal = filter
            .owner_principal
            .as_deref()
            .map(|value| normalize_text(value, "owner_principal", 128))
            .transpose()?;
        let connector_kind = filter
            .connector_kind
            .as_deref()
            .map(|value| normalize_binding_component(value, "connector_kind"))
            .transpose()?;
        let external_identity = filter
            .external_identity
            .as_deref()
            .map(|value| normalize_text(value, "external_identity", 256))
            .transpose()?;
        let palyra_session_id = filter
            .palyra_session_id
            .as_deref()
            .map(|value| {
                validate_canonical_id(value).map_err(|_| AcpRuntimeError::InvalidField {
                    field: "palyra_session_id",
                    message: "expected canonical Palyra session id".to_owned(),
                })?;
                Ok::<String, AcpRuntimeError>(value.to_owned())
            })
            .transpose()?;
        let limit = filter.limit.unwrap_or(100).clamp(1, 500);
        let mut records = self
            .lock_index()?
            .conversation_bindings
            .iter()
            .filter(|entry| {
                filter.include_detached
                    || entry.conflict_state != ConversationBindingConflictState::Detached
            })
            .filter(|entry| {
                owner_principal.as_ref().is_none_or(|owner| entry.owner_principal.as_str() == owner)
            })
            .filter(|entry| {
                connector_kind.as_ref().is_none_or(|kind| entry.connector_kind.as_str() == kind)
            })
            .filter(|entry| {
                external_identity
                    .as_ref()
                    .is_none_or(|identity| entry.external_identity.as_str() == identity)
            })
            .filter(|entry| {
                palyra_session_id
                    .as_ref()
                    .is_none_or(|session_id| entry.palyra_session_id.as_str() == session_id)
            })
            .cloned()
            .collect::<Vec<_>>();
        records.sort_by_key(|record| std::cmp::Reverse(record.updated_at_unix_ms));
        records.truncate(limit);
        Ok(records)
    }

    /// Looks up a conversation binding by its `binding_id`.
    ///
    /// # Errors
    /// Returns `NotFound` for unknown ids, plus validation/lock errors.
    pub(crate) fn get_conversation_binding(
        &self,
        binding_id: &str,
    ) -> AcpRuntimeResult<ConversationBindingRecord> {
        let binding_id = normalize_text(binding_id, "binding_id", 160)?;
        self.lock_index()?
            .conversation_bindings
            .iter()
            .find(|entry| entry.binding_id == binding_id)
            .cloned()
            .ok_or(AcpRuntimeError::NotFound { kind: "conversation_binding", id: binding_id })
    }

    /// Detaches a conversation binding (soft delete): the record is kept for
    /// audit but stops routing and is excluded from conflict grouping.
    ///
    /// # Errors
    /// Returns `NotFound` for unknown ids, plus validation/storage errors.
    pub(crate) fn detach_conversation_binding(
        &self,
        binding_id: &str,
    ) -> AcpRuntimeResult<ConversationBindingRecord> {
        let now = unix_ms_now().map_err(|error| AcpRuntimeError::InvalidField {
            field: "system_time",
            message: error.to_string(),
        })?;
        let binding_id = normalize_text(binding_id, "binding_id", 160)?;
        let mut index = self.lock_index()?;
        let Some(position) =
            index.conversation_bindings.iter().position(|entry| entry.binding_id == binding_id)
        else {
            return Err(AcpRuntimeError::NotFound { kind: "conversation_binding", id: binding_id });
        };
        index.conversation_bindings[position].conflict_state =
            ConversationBindingConflictState::Detached;
        index.conversation_bindings[position].updated_at_unix_ms = now;
        let record = index.conversation_bindings[position].clone();
        normalize_conversation_conflicts(&mut index);
        save_locked_index(self.root.as_path(), &mut index)?;
        Ok(record)
    }

    /// Builds a dry-run repair plan for current binding conflicts without
    /// changing any state.
    ///
    /// # Errors
    /// Returns a conflict error when the index lock is poisoned.
    pub(crate) fn plan_conversation_binding_repair(&self) -> AcpRuntimeResult<BindingRepairPlan> {
        let index = self.lock_index()?;
        Ok(build_repair_plan(&index, true))
    }

    /// Builds the repair plan and applies only its `automatic_apply` actions;
    /// access-widening actions (principal/workspace conflicts) stay manual
    /// and are returned in the plan untouched.
    ///
    /// # Errors
    /// Fails on clock, lock, or storage errors.
    pub(crate) fn apply_conversation_binding_repair(&self) -> AcpRuntimeResult<BindingRepairPlan> {
        let now = unix_ms_now().map_err(|error| AcpRuntimeError::InvalidField {
            field: "system_time",
            message: error.to_string(),
        })?;
        let mut index = self.lock_index()?;
        let plan = build_repair_plan(&index, false);
        for action in plan.actions.iter().filter(|action| action.automatic_apply) {
            match action.action.as_str() {
                "detach" => {
                    if let Some(record) = index
                        .conversation_bindings
                        .iter_mut()
                        .find(|entry| entry.binding_id == action.binding_id)
                    {
                        record.conflict_state = ConversationBindingConflictState::Detached;
                        record.updated_at_unix_ms = now;
                    }
                }
                "expire" if action.target_kind == "pending_prompt" => {
                    index.pending_prompts.retain(|entry| entry.prompt_id != action.binding_id);
                }
                "mark_stale" => {
                    if let Some(record) = index
                        .conversation_bindings
                        .iter_mut()
                        .find(|entry| entry.binding_id == action.binding_id)
                    {
                        record.conflict_state = match action.conflict_kind.as_str() {
                            "principal_mismatch" => {
                                ConversationBindingConflictState::PrincipalMismatch
                            }
                            "workspace_mismatch" => {
                                ConversationBindingConflictState::WorkspaceMismatch
                            }
                            "parent_missing" => ConversationBindingConflictState::ParentMissing,
                            "expired_referenced" | "expired_reference" => {
                                ConversationBindingConflictState::ExpiredReference
                            }
                            _ => ConversationBindingConflictState::StaleThread,
                        };
                        record.updated_at_unix_ms = now;
                    }
                }
                _ => {}
            }
        }
        normalize_conversation_conflicts(&mut index);
        save_locked_index(self.root.as_path(), &mut index)?;
        Ok(plan)
    }

    /// Explains one binding (session bindings are checked first) including
    /// its conflict kinds and the repair actions that currently target it.
    ///
    /// # Errors
    /// Returns `NotFound` when no binding of either kind matches, plus
    /// validation/lock errors.
    pub(crate) fn explain_binding(
        &self,
        binding_id: &str,
    ) -> AcpRuntimeResult<BindingExplainSnapshot> {
        let binding_id = normalize_text(binding_id, "binding_id", 160)?;
        let index = self.lock_index()?;
        if let Some(record) =
            index.session_bindings.iter().find(|entry| entry.binding_id == binding_id)
        {
            let repair_actions = build_repair_plan(&index, true)
                .actions
                .into_iter()
                .filter(|action| action.binding_id == record.binding_id)
                .map(|action| action.action)
                .collect::<Vec<_>>();
            return Ok(BindingExplainSnapshot {
                binding_id: record.binding_id.clone(),
                binding_kind: "acp_session".to_owned(),
                owner_principal: record.owner_principal.clone(),
                palyra_session_id: record.palyra_session_id.clone(),
                external_identity: None,
                external_conversation_id: None,
                acp_client_id: Some(record.acp_client_id.clone()),
                acp_session_id: Some(record.acp_session_id.clone()),
                conflict_state: "none".to_owned(),
                conflict_kinds: Vec::new(),
                repair_actions,
                stale_permissions: record.stale_permissions,
                last_event_id: None,
                delivery_cursor: record.cursor.sequence,
            });
        }
        if let Some(record) =
            index.conversation_bindings.iter().find(|entry| entry.binding_id == binding_id)
        {
            let repair_actions = build_repair_plan(&index, true)
                .actions
                .into_iter()
                .filter(|action| action.binding_id == record.binding_id)
                .map(|action| action.action)
                .collect::<Vec<_>>();
            return Ok(BindingExplainSnapshot {
                binding_id: record.binding_id.clone(),
                binding_kind: "conversation".to_owned(),
                owner_principal: record.owner_principal.clone(),
                palyra_session_id: record.palyra_session_id.clone(),
                external_identity: Some(record.external_identity.clone()),
                external_conversation_id: Some(record.external_conversation_id.clone()),
                acp_client_id: None,
                acp_session_id: None,
                conflict_state: record.conflict_state.as_str().to_owned(),
                conflict_kinds: conflict_kinds_for_conversation(record, &index),
                repair_actions,
                stale_permissions: false,
                last_event_id: record.last_event_id.clone(),
                delivery_cursor: record.delivery_cursor.sequence,
            });
        }
        Err(AcpRuntimeError::NotFound { kind: "binding", id: binding_id })
    }

    /// Builds the success result envelope for an ACP console command.
    pub(crate) fn success_envelope(
        request_id: String,
        command: AcpCommand,
        result: Value,
        idempotency_key: Option<String>,
    ) -> AcpCommandResultEnvelope {
        AcpCommandResultEnvelope {
            request_id,
            command,
            ok: true,
            result: Some(result),
            error: None,
            idempotency_key,
            replayed: false,
        }
    }

    fn lock_index(&self) -> AcpRuntimeResult<std::sync::MutexGuard<'_, AcpBindingsIndex>> {
        self.index.lock().map_err(|_| AcpRuntimeError::Conflict {
            message: "ACP bindings index lock is poisoned".to_owned(),
        })
    }
}

/// Conventional ACP state directory under the daemon state root.
pub(crate) fn acp_root_from_state_root(state_root: &Path) -> PathBuf {
    state_root.join("acp")
}

/// Maps an internal transcript event type to its ACP wire event name.
///
/// The mapping is a closed allowlist on purpose: only event types with a
/// defined ACP meaning may leave the daemon, so new internal event types fail
/// here until they get an explicit translation.
///
/// # Errors
/// Returns a `Compatibility` error for any event type without a mapping.
pub(crate) fn translate_palyra_event_type(event_type: &str) -> AcpRuntimeResult<&'static str> {
    match event_type.trim() {
        "status" => Ok("run.status"),
        "model_token" => Ok("message.delta"),
        "tool_proposal" => Ok("tool.proposal"),
        "tool_approval_request" => Ok("approval.requested"),
        "tool_result" | "tool.result" => Ok("tool.result"),
        "tool.executed" => Ok("tool.executed"),
        "message.received" => Ok("message.received"),
        "message.replied" => Ok("message.replied"),
        "message.routed" => Ok("message.routed"),
        "message.rejected" => Ok("message.rejected"),
        "flow.created" => Ok("flow.created"),
        other => Err(AcpRuntimeError::Compatibility {
            message: format!("unsupported transcript event type '{other}'"),
        }),
    }
}

fn edit_proposal_from_presentation_payload(payload: &Value) -> Option<AcpEditProposalPresentation> {
    let proposal = payload.get("edit_proposal").unwrap_or(payload);
    let affected_files = string_array_from_json(proposal, "affected_files")
        .or_else(|| string_array_from_json(proposal, "paths"))
        .unwrap_or_default();
    let diff_summary = string_from_json(proposal, "diff_summary")
        .or_else(|| string_from_json(proposal, "summary"));
    if diff_summary.is_none() && affected_files.is_empty() {
        return None;
    }
    let diff_summary = diff_summary.unwrap_or_else(|| {
        format!("{} file(s) require review before mutation", affected_files.len())
    });
    let proposal_id = string_from_json(proposal, "proposal_id")
        .or_else(|| string_from_json(payload, "approval_id"))
        .unwrap_or_else(|| sha256_hex(proposal.to_string().as_bytes()).chars().take(24).collect());
    let provenance_refs = string_array_from_json(proposal, "provenance_refs")
        .or_else(|| string_array_from_json(payload, "evidence_refs"))
        .unwrap_or_default();
    Some(AcpEditProposalPresentation {
        proposal_id: normalize_text_lossy(proposal_id.as_str(), 120),
        diff_summary: normalize_text_lossy(diff_summary.as_str(), 600),
        risk_level: string_from_json(proposal, "risk_level")
            .unwrap_or_else(|| "review".to_owned())
            .trim()
            .to_ascii_lowercase(),
        affected_files: affected_files
            .into_iter()
            .map(|path| normalize_text_lossy(path.as_str(), 240))
            .collect(),
        approval_actions: vec!["approve".to_owned(), "reject".to_owned(), "modify".to_owned()],
        provenance_refs: provenance_refs
            .into_iter()
            .map(|reference| normalize_text_lossy(reference.as_str(), 240))
            .collect(),
    })
}

fn rich_blocks_from_presentation_payload(payload: &Value) -> Vec<AcpRichContentBlock> {
    let mut blocks = Vec::new();
    let mut seen = BTreeSet::new();
    for key in ["rich_blocks", "resources", "source_refs", "evidence_refs"] {
        let Some(values) = payload.get(key).and_then(Value::as_array) else {
            continue;
        };
        for value in values {
            if let Some(block) = rich_block_from_value(value) {
                if seen.insert(block.block_id.clone()) {
                    blocks.push(block);
                }
            }
        }
    }
    blocks.sort_by(|left, right| {
        left.block_kind.cmp(&right.block_kind).then(left.source_ref.cmp(&right.source_ref))
    });
    blocks
}

fn rich_block_from_value(value: &Value) -> Option<AcpRichContentBlock> {
    let (kind, source_ref, title, preview, provenance_refs) = match value {
        Value::Object(_) => {
            let raw_kind = string_from_json(value, "kind")
                .or_else(|| string_from_json(value, "type"))
                .or_else(|| string_from_json(value, "source_type"))?;
            let source_ref = string_from_json(value, "source_ref")
                .or_else(|| string_from_json(value, "ref"))
                .or_else(|| string_from_json(value, "uri"))
                .or_else(|| string_from_json(value, "url"))
                .or_else(|| string_from_json(value, "path"))
                .or_else(|| string_from_json(value, "artifact_id"))
                .or_else(|| string_from_json(value, "memory_id"))
                .or_else(|| string_from_json(value, "document_id"))?;
            (
                parse_rich_block_kind(raw_kind.as_str())?,
                source_ref,
                string_from_json(value, "title").or_else(|| string_from_json(value, "label")),
                string_from_json(value, "preview")
                    .or_else(|| string_from_json(value, "summary"))
                    .or_else(|| string_from_json(value, "snippet")),
                string_array_from_json(value, "provenance_refs").unwrap_or_default(),
            )
        }
        Value::String(raw) => {
            let kind = if raw.starts_with("file://") {
                AcpRichContentBlockKind::FileUri
            } else {
                parse_rich_block_kind(raw)?
            };
            (kind, raw.clone(), None, None, Vec::new())
        }
        _ => return None,
    };
    let source_ref = normalize_text_lossy(source_ref.as_str(), 360);
    let block_id =
        sha256_hex(format!("{kind:?}:{source_ref}").as_bytes()).chars().take(24).collect();
    Some(AcpRichContentBlock {
        block_id,
        block_kind: kind,
        title: normalize_text_lossy(title.unwrap_or_else(|| source_ref.clone()).as_str(), 160),
        source_ref,
        preview: preview.map(|text| normalize_text_lossy(text.as_str(), 600)),
        provenance_refs: provenance_refs
            .into_iter()
            .map(|reference| normalize_text_lossy(reference.as_str(), 240))
            .collect(),
    })
}

fn parse_rich_block_kind(raw: &str) -> Option<AcpRichContentBlockKind> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "image" | "image_ref" => Some(AcpRichContentBlockKind::Image),
        "embedded_resource" | "embedded" => Some(AcpRichContentBlockKind::EmbeddedResource),
        "file_resource" | "file" => Some(AcpRichContentBlockKind::FileResource),
        "file_uri" | "uri" => Some(AcpRichContentBlockKind::FileUri),
        "search_result" | "search" => Some(AcpRichContentBlockKind::SearchResult),
        "browser_result" | "browser" => Some(AcpRichContentBlockKind::BrowserResult),
        "memory_result" | "memory" => Some(AcpRichContentBlockKind::MemoryResult),
        "artifact_ref" | "artifact" | "audit_artifact" => {
            Some(AcpRichContentBlockKind::ArtifactRef)
        }
        _ => None,
    }
}

fn renderers_from_presentation_payload(payload: &Value) -> Vec<AcpToolOutputRenderer> {
    let mut renderers = Vec::new();
    let mut seen = BTreeSet::new();
    for key in ["tool_name", "output_kind", "renderer", "kind"] {
        if let Some(raw) = string_from_json(payload, key) {
            if let Some(renderer) = renderer_for_tool_output(raw.as_str()) {
                seen.insert(renderer.to_owned());
            }
        }
    }
    if let Some(outputs) = payload.get("outputs").and_then(Value::as_array) {
        for output in outputs {
            for key in ["kind", "tool_name", "renderer"] {
                if let Some(raw) = string_from_json(output, key) {
                    if let Some(renderer) = renderer_for_tool_output(raw.as_str()) {
                        seen.insert(renderer.to_owned());
                    }
                }
            }
        }
    }
    for renderer in seen {
        renderers.push(AcpToolOutputRenderer {
            renderer,
            content_policy: "redacted_preview_with_artifact_refs".to_owned(),
            redaction_policy: "secrets_and_large_payloads_withheld".to_owned(),
        });
    }
    renderers
}

fn renderer_for_tool_output(raw: &str) -> Option<&'static str> {
    let normalized = raw.trim().to_ascii_lowercase();
    if normalized.contains("patch") || normalized.contains("diff") {
        Some("workspace_patch")
    } else if normalized.contains("read") || normalized.contains("file") {
        Some("file_read")
    } else if normalized.contains("search") || normalized.contains("grep") {
        Some("search_results")
    } else if normalized.contains("shell") || normalized.contains("process") {
        Some("shell_output")
    } else if normalized.contains("browser") {
        Some("browser_result")
    } else if normalized.contains("memory") || normalized.contains("recall") {
        Some("memory_result")
    } else if normalized.contains("verification") || normalized.contains("test") {
        Some("verification_evidence")
    } else {
        None
    }
}

fn string_from_json(value: &Value, key: &str) -> Option<String> {
    value.get(key)?.as_str().map(|raw| raw.to_owned())
}

fn string_array_from_json(value: &Value, key: &str) -> Option<Vec<String>> {
    value.get(key).and_then(value_string_array)
}

fn value_string_array(value: &Value) -> Option<Vec<String>> {
    let values = value.as_array()?;
    Some(values.iter().filter_map(Value::as_str).map(ToOwned::to_owned).collect())
}

fn normalize_text_lossy(raw: &str, max_chars: usize) -> String {
    let normalized = raw.split_whitespace().collect::<Vec<_>>().join(" ");
    if normalized.chars().count() <= max_chars {
        return normalized;
    }
    let mut truncated = normalized.chars().take(max_chars.saturating_sub(3)).collect::<String>();
    truncated.push_str("...");
    truncated
}

fn create_state_dir(root: &Path) -> AcpRuntimeResult<()> {
    ensure_owner_only_dir(root).map_err(|source| AcpRuntimeError::PermissionHarden {
        path: root.to_path_buf(),
        message: source.to_string(),
    })
}

fn load_index(path: &Path) -> AcpRuntimeResult<AcpBindingsIndex> {
    if !path.exists() {
        return Ok(AcpBindingsIndex::default());
    }
    let payload = fs::read(path).map_err(|source| AcpRuntimeError::Io {
        operation: "read",
        path: path.to_path_buf(),
        source,
    })?;
    let mut index = parse_versioned_json::<AcpBindingsIndex>(
        payload.as_slice(),
        ACP_BINDINGS_INDEX_FORMAT,
        &[],
    )
    .map_err(|source| AcpRuntimeError::VersionedJson { path: path.to_path_buf(), source })?;
    normalize_loaded_index(&mut index)?;
    Ok(index)
}

fn save_locked_index(root: &Path, index: &mut AcpBindingsIndex) -> AcpRuntimeResult<()> {
    normalize_conversation_conflicts(index);
    prune_event_ledger(index);
    save_index(root, index)
}

fn save_index(root: &Path, index: &AcpBindingsIndex) -> AcpRuntimeResult<()> {
    create_state_dir(root)?;
    let root = fs::canonicalize(root).map_err(|source| AcpRuntimeError::Io {
        operation: "canonicalize",
        path: root.to_path_buf(),
        source,
    })?;
    let path = acp_bindings_index_path(root.as_path())?;
    let mut normalized = index.clone();
    normalized.schema_version = ACP_BINDINGS_LAYOUT_VERSION;
    normalized.updated_at_unix_ms = unix_ms_now().map_err(|error| {
        AcpRuntimeError::InvalidField { field: "system_time", message: error.to_string() }
    })?;
    normalize_loaded_index(&mut normalized)?;
    let payload = serde_json::to_vec_pretty(&normalized)
        .map_err(|source| AcpRuntimeError::Json { path: path.clone(), source })?;
    write_atomically(root.as_path(), path.as_path(), payload.as_slice())?;
    ensure_owner_only_file(path.as_path())
        .map_err(|source| AcpRuntimeError::PermissionHarden { path, message: source.to_string() })
}

fn acp_bindings_index_path(root: &Path) -> AcpRuntimeResult<PathBuf> {
    let path = root.join(ACP_BINDINGS_INDEX_FILE_NAME);
    validate_acp_state_child_path(root, path.as_path(), "bindings index")?;
    Ok(path)
}

fn validate_acp_state_child_path(
    root: &Path,
    path: &Path,
    label: &'static str,
) -> AcpRuntimeResult<()> {
    if !path.starts_with(root) {
        return Err(AcpRuntimeError::InvalidField {
            field: "state_path",
            message: format!("{label} path escapes the ACP state root"),
        });
    }
    if path.components().any(|component| {
        matches!(component, std::path::Component::ParentDir | std::path::Component::CurDir)
    }) {
        return Err(AcpRuntimeError::InvalidField {
            field: "state_path",
            message: format!("{label} path cannot contain relative traversal components"),
        });
    }
    Ok(())
}

// Write-to-temp + fsync + rename so a crash mid-write can never leave a
// truncated bindings index; the temp file lives in `root` to keep the rename
// on one filesystem.
fn write_atomically(root: &Path, path: &Path, payload: &[u8]) -> AcpRuntimeResult<()> {
    validate_acp_state_child_path(root, path, "bindings index")?;
    let mut temporary_file =
        TempFileBuilder::new().prefix("bindings.").suffix(".json.tmp").tempfile_in(root).map_err(
            |source| AcpRuntimeError::Io {
                operation: "create_temporary",
                path: root.to_path_buf(),
                source,
            },
        )?;
    temporary_file.write_all(payload).map_err(|source| AcpRuntimeError::Io {
        operation: "write",
        path: path.to_path_buf(),
        source,
    })?;
    temporary_file.as_file_mut().sync_all().map_err(|source| AcpRuntimeError::Io {
        operation: "sync",
        path: path.to_path_buf(),
        source,
    })?;
    temporary_file.persist(path).map(|_| ()).map_err(|source| AcpRuntimeError::Io {
        operation: "persist",
        path: path.to_path_buf(),
        source: source.error,
    })
}

fn normalize_loaded_index(index: &mut AcpBindingsIndex) -> AcpRuntimeResult<()> {
    index.schema_version = ACP_BINDINGS_LAYOUT_VERSION;
    index.session_bindings.sort_by(|left, right| {
        left.acp_client_id
            .cmp(&right.acp_client_id)
            .then(left.acp_session_id.cmp(&right.acp_session_id))
    });
    index.conversation_bindings.sort_by(|left, right| {
        left.connector_kind
            .cmp(&right.connector_kind)
            .then(left.external_identity.cmp(&right.external_identity))
            .then(left.external_conversation_id.cmp(&right.external_conversation_id))
    });
    index.event_ledger.sort_by(|left, right| {
        left.acp_client_id
            .cmp(&right.acp_client_id)
            .then(left.acp_session_id.cmp(&right.acp_session_id))
            .then(left.palyra_session_id.cmp(&right.palyra_session_id))
            .then(left.sequence.cmp(&right.sequence))
            .then(left.event_id.cmp(&right.event_id))
    });
    for binding in &index.session_bindings {
        validate_canonical_id(binding.palyra_session_id.as_str()).map_err(|_| {
            AcpRuntimeError::InvalidField {
                field: "palyra_session_id",
                message: "stored ACP binding contains invalid Palyra session id".to_owned(),
            }
        })?;
    }
    for binding in &index.conversation_bindings {
        validate_canonical_id(binding.palyra_session_id.as_str()).map_err(|_| {
            AcpRuntimeError::InvalidField {
                field: "palyra_session_id",
                message: "stored conversation binding contains invalid Palyra session id"
                    .to_owned(),
            }
        })?;
    }
    for event in &index.event_ledger {
        validate_canonical_id(event.palyra_session_id.as_str()).map_err(|_| {
            AcpRuntimeError::InvalidField {
                field: "palyra_session_id",
                message: "stored ACP event contains invalid Palyra session id".to_owned(),
            }
        })?;
        validate_optional_canonical_id(event.run_id.as_deref(), "run_id")?;
        validate_optional_canonical_id(event.approval_id.as_deref(), "approval_id")?;
    }
    normalize_conversation_conflicts(index);
    prune_event_ledger(index);
    Ok(())
}

// Restart hygiene: scopes/capabilities recorded before this process started
// are treated as unverified until the client reconnects and re-asserts them.
fn mark_loaded_permissions_stale(index: &mut AcpBindingsIndex, now_unix_ms: i64) -> bool {
    let mut changed = false;
    for binding in &mut index.session_bindings {
        if !binding.stale_permissions {
            binding.stale_permissions = true;
            binding.last_seen_at_unix_ms = now_unix_ms;
            changed = true;
        }
    }
    changed
}

fn validate_protocol_version(version: u32) -> AcpRuntimeResult<()> {
    if (ACP_PROTOCOL_MIN_VERSION..=ACP_PROTOCOL_MAX_VERSION).contains(&version) {
        return Ok(());
    }
    Err(AcpRuntimeError::UnsupportedProtocolVersion { version })
}

fn validate_scopes_and_capabilities(context: &AcpClientContext) -> AcpRuntimeResult<()> {
    if context.scopes.is_empty() {
        return Err(AcpRuntimeError::Permission {
            message: "ACP client did not request any scopes".to_owned(),
        });
    }
    if context.capabilities.is_empty() {
        return Err(AcpRuntimeError::Permission {
            message: "ACP client did not request any capabilities".to_owned(),
        });
    }
    Ok(())
}

fn normalize_text(raw: &str, field: &'static str, max_bytes: usize) -> AcpRuntimeResult<String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err(AcpRuntimeError::InvalidField {
            field,
            message: "value must not be empty".to_owned(),
        });
    }
    if trimmed.len() > max_bytes {
        return Err(AcpRuntimeError::InvalidField {
            field,
            message: format!("value exceeds {max_bytes} bytes"),
        });
    }
    if trimmed.chars().any(char::is_control) {
        return Err(AcpRuntimeError::InvalidField {
            field,
            message: "value must not contain control characters".to_owned(),
        });
    }
    Ok(trimmed.to_owned())
}

fn normalize_optional_text(
    raw: Option<&str>,
    field: &'static str,
    max_bytes: usize,
) -> AcpRuntimeResult<Option<String>> {
    raw.map(|value| normalize_text(value, field, max_bytes)).transpose()
}

fn normalize_binding_component(raw: &str, field: &'static str) -> AcpRuntimeResult<String> {
    let value = normalize_text(raw, field, 96)?.to_ascii_lowercase();
    if !value
        .chars()
        .all(|character| character.is_ascii_alphanumeric() || matches!(character, '_' | '-' | '.'))
    {
        return Err(AcpRuntimeError::InvalidField {
            field,
            message: "value must contain only ASCII alphanumerics, '.', '_' or '-'".to_owned(),
        });
    }
    Ok(value)
}

fn sorted_scopes(mut scopes: Vec<AcpScope>) -> Vec<AcpScope> {
    scopes.sort();
    scopes.dedup();
    scopes
}

fn sorted_capabilities(mut capabilities: Vec<AcpCapability>) -> Vec<AcpCapability> {
    capabilities.sort();
    capabilities.dedup();
    capabilities
}

fn validate_optional_canonical_id(raw: Option<&str>, field: &'static str) -> AcpRuntimeResult<()> {
    if let Some(value) = raw {
        validate_canonical_id(value).map_err(|_| AcpRuntimeError::InvalidField {
            field,
            message: "expected canonical Palyra id".to_owned(),
        })?;
    }
    Ok(())
}

fn reject_sensitive_config(config: &Value) -> AcpRuntimeResult<()> {
    let mut path = VecDeque::new();
    if value_contains_sensitive_key(config, &mut path) {
        return Err(AcpRuntimeError::InvalidField {
            field: "config",
            message: format!(
                "config must not persist secret-bearing keys ({})",
                path.into_iter().collect::<Vec<_>>().join(".")
            ),
        });
    }
    path.clear();
    if value_contains_untrusted_launch_key(config, &mut path) {
        return Err(AcpRuntimeError::InvalidField {
            field: "config",
            message: format!(
                "config cannot supply ACP process launch authority ({})",
                path.into_iter().collect::<Vec<_>>().join(".")
            ),
        });
    }
    Ok(())
}

fn value_contains_sensitive_key(value: &Value, path: &mut VecDeque<String>) -> bool {
    match value {
        Value::Object(map) => {
            for (key, child) in map {
                path.push_back(key.clone());
                if is_sensitive_key(key) || value_contains_sensitive_key(child, path) {
                    return true;
                }
                path.pop_back();
            }
            false
        }
        Value::Array(items) => {
            for (index, child) in items.iter().enumerate() {
                path.push_back(index.to_string());
                if value_contains_sensitive_key(child, path) {
                    return true;
                }
                path.pop_back();
            }
            false
        }
        _ => false,
    }
}

fn value_contains_untrusted_launch_key(value: &Value, path: &mut VecDeque<String>) -> bool {
    match value {
        Value::Object(map) => {
            for (key, child) in map {
                path.push_back(key.clone());
                if is_untrusted_launch_key(key) || value_contains_untrusted_launch_key(child, path)
                {
                    return true;
                }
                path.pop_back();
            }
            false
        }
        Value::Array(items) => {
            for (index, child) in items.iter().enumerate() {
                path.push_back(index.to_string());
                if value_contains_untrusted_launch_key(child, path) {
                    return true;
                }
                path.pop_back();
            }
            false
        }
        _ => false,
    }
}

fn is_untrusted_launch_key(key: &str) -> bool {
    matches!(
        key.trim().to_ascii_lowercase().as_str(),
        "executable"
            | "runtime_executable"
            | "runtime_args"
            | "runtime_cwd"
            | "runtime_env"
            | "runtime_capability_digest"
            | "runtime_protocol_version"
            | "runtime_backend_candidates"
    )
}

// Substring matching is intentionally aggressive: a false positive only makes
// the client rename a config key, while a false negative would persist a
// secret to disk in plaintext.
fn is_sensitive_key(key: &str) -> bool {
    let normalized = key.to_ascii_lowercase();
    normalized.contains("secret")
        || normalized.contains("token")
        || normalized.contains("password")
        || normalized.contains("api_key")
        || normalized.contains("apikey")
        || normalized.contains("credential")
        || normalized.contains("private_key")
}

fn prune_expired_pending_prompts(index: &mut AcpBindingsIndex, now_unix_ms: i64) -> Vec<String> {
    let mut expired = Vec::new();
    index.pending_prompts.retain(|entry| {
        if entry.expires_at_unix_ms >= now_unix_ms {
            true
        } else {
            expired.push(entry.prompt_id.clone());
            false
        }
    });
    expired
}

fn event_ledger_after_cursor(
    index: &AcpBindingsIndex,
    acp_client_id: &str,
    acp_session_id: &str,
    palyra_session_id: &str,
    cursor_sequence: u64,
    limit: usize,
) -> Vec<AcpEventLedgerRecord> {
    let mut records = index
        .event_ledger
        .iter()
        .filter(|entry| {
            entry.acp_client_id == acp_client_id
                && entry.acp_session_id == acp_session_id
                && entry.palyra_session_id == palyra_session_id
                && entry.sequence > cursor_sequence
        })
        .cloned()
        .collect::<Vec<_>>();
    records.sort_by(|left, right| {
        left.sequence.cmp(&right.sequence).then(left.event_id.cmp(&right.event_id))
    });
    if records.len() > limit {
        records.split_off(records.len().saturating_sub(limit))
    } else {
        records
    }
}

fn can_replay_event_ledger(context: &AcpClientContext) -> bool {
    context.scopes.contains(&AcpScope::EventsRead)
        && context.capabilities.contains(&AcpCapability::SessionReplay)
}

fn next_event_ledger_sequence(
    index: &AcpBindingsIndex,
    acp_client_id: &str,
    acp_session_id: &str,
    palyra_session_id: &str,
) -> u64 {
    let latest_event_sequence = index
        .event_ledger
        .iter()
        .filter(|entry| {
            entry.acp_client_id == acp_client_id
                && entry.acp_session_id == acp_session_id
                && entry.palyra_session_id == palyra_session_id
        })
        .map(|entry| entry.sequence)
        .max()
        .unwrap_or(0);
    let binding_cursor_sequence = index
        .session_bindings
        .iter()
        .find(|entry| {
            entry.acp_client_id == acp_client_id
                && entry.acp_session_id == acp_session_id
                && entry.palyra_session_id == palyra_session_id
        })
        .map(|entry| entry.cursor.sequence)
        .unwrap_or(0);
    latest_event_sequence.max(binding_cursor_sequence).saturating_add(1)
}

fn prune_event_ledger(index: &mut AcpBindingsIndex) {
    index.event_ledger.sort_by(|left, right| {
        left.created_at_unix_ms
            .cmp(&right.created_at_unix_ms)
            .then(left.sequence.cmp(&right.sequence))
            .then(left.event_id.cmp(&right.event_id))
    });
    let mut retained_per_session = BTreeMap::<(String, String, String), usize>::new();
    let mut retained_event_ids = BTreeSet::<String>::new();
    for entry in index.event_ledger.iter().rev() {
        let key = (
            entry.acp_client_id.clone(),
            entry.acp_session_id.clone(),
            entry.palyra_session_id.clone(),
        );
        let count = retained_per_session.entry(key).or_default();
        if *count < MAX_EVENT_LEDGER_EVENTS_PER_SESSION {
            retained_event_ids.insert(entry.event_id.clone());
            *count = count.saturating_add(1);
        }
    }
    index.event_ledger.retain(|entry| retained_event_ids.contains(entry.event_id.as_str()));
    if index.event_ledger.len() > MAX_EVENT_LEDGER_EVENTS {
        index.event_ledger.sort_by(|left, right| {
            left.created_at_unix_ms
                .cmp(&right.created_at_unix_ms)
                .then(left.sequence.cmp(&right.sequence))
                .then(left.event_id.cmp(&right.event_id))
        });
        let drop_count = index.event_ledger.len().saturating_sub(MAX_EVENT_LEDGER_EVENTS);
        index.event_ledger.drain(0..drop_count);
    }
    index.event_ledger.sort_by(|left, right| {
        left.acp_client_id
            .cmp(&right.acp_client_id)
            .then(left.acp_session_id.cmp(&right.acp_session_id))
            .then(left.palyra_session_id.cmp(&right.palyra_session_id))
            .then(left.sequence.cmp(&right.sequence))
            .then(left.event_id.cmp(&right.event_id))
    });
}

fn redact_sensitive_payload_fields(value: &mut Value) {
    match value {
        Value::Object(map) => {
            for (key, child) in map.iter_mut() {
                if is_sensitive_key(key) {
                    *child = Value::String("[REDACTED]".to_owned());
                } else {
                    redact_sensitive_payload_fields(child);
                }
            }
        }
        Value::Array(items) => {
            for item in items {
                redact_sensitive_payload_fields(item);
            }
        }
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => {}
    }
}

// Re-derives duplicate-style conflict states from scratch for every binding
// group sharing one external conversation. Operator-set terminal states
// (detached, stale, mismatch, expired, parent-missing) are sticky and skipped;
// only the duplicate family is recomputed, so resolving a duplicate clears
// the flag automatically on the next save.
fn normalize_conversation_conflicts(index: &mut AcpBindingsIndex) {
    let mut grouped: BTreeMap<(String, String, String), Vec<usize>> = BTreeMap::new();
    for (position, entry) in index.conversation_bindings.iter_mut().enumerate() {
        match entry.conflict_state {
            ConversationBindingConflictState::Detached
            | ConversationBindingConflictState::StaleThread
            | ConversationBindingConflictState::PrincipalMismatch
            | ConversationBindingConflictState::WorkspaceMismatch
            | ConversationBindingConflictState::ExpiredReference
            | ConversationBindingConflictState::ParentMissing => continue,
            ConversationBindingConflictState::None
            | ConversationBindingConflictState::DuplicateActiveBinding
            | ConversationBindingConflictState::DuplicateExternalIdentity
            | ConversationBindingConflictState::DuplicateSession => {
                entry.conflict_state = ConversationBindingConflictState::None;
                grouped
                    .entry((
                        entry.connector_kind.clone(),
                        entry.external_identity.clone(),
                        entry.external_conversation_id.clone(),
                    ))
                    .or_default()
                    .push(position);
            }
        }
    }
    for positions in grouped.values() {
        if positions.len() <= 1 {
            continue;
        }
        let owner_count = positions
            .iter()
            .filter_map(|position| index.conversation_bindings.get(*position))
            .map(|entry| entry.owner_principal.as_str())
            .collect::<BTreeSet<_>>()
            .len();
        let session_count = positions
            .iter()
            .filter_map(|position| index.conversation_bindings.get(*position))
            .map(|entry| entry.palyra_session_id.as_str())
            .collect::<BTreeSet<_>>()
            .len();
        let conflict_state = if owner_count > 1 {
            ConversationBindingConflictState::PrincipalMismatch
        } else if session_count == 1 {
            ConversationBindingConflictState::DuplicateSession
        } else {
            ConversationBindingConflictState::DuplicateActiveBinding
        };
        for position in positions {
            if let Some(entry) = index.conversation_bindings.get_mut(*position) {
                entry.conflict_state = conflict_state;
            }
        }
    }
}

// Conflict precedence per external conversation: principal mismatch (manual,
// never widened automatically) > workspace mismatch (manual split) > plain
// duplicates (auto-detach everything but the most recently updated binding).
// Orphaned parent-required bindings and expired prompts are auto-repairable.
fn build_repair_plan(index: &AcpBindingsIndex, dry_run: bool) -> BindingRepairPlan {
    let mut grouped: BTreeMap<(String, String, String), Vec<&ConversationBindingRecord>> =
        BTreeMap::new();
    for entry in &index.conversation_bindings {
        if entry.conflict_state == ConversationBindingConflictState::Detached {
            continue;
        }
        grouped
            .entry((
                entry.connector_kind.clone(),
                entry.external_identity.clone(),
                entry.external_conversation_id.clone(),
            ))
            .or_default()
            .push(entry);
    }
    let mut actions = Vec::new();
    for records in grouped.values_mut() {
        if records.len() <= 1 {
            continue;
        }
        let owner_count = records
            .iter()
            .map(|entry| entry.owner_principal.as_str())
            .collect::<BTreeSet<_>>()
            .len();
        let workspace_count = records
            .iter()
            .filter_map(|entry| workspace_key_for_session(index, entry.palyra_session_id.as_str()))
            .collect::<BTreeSet<_>>()
            .len();
        records.sort_by(|left, right| {
            right
                .updated_at_unix_ms
                .cmp(&left.updated_at_unix_ms)
                .then(right.binding_id.cmp(&left.binding_id))
        });
        if owner_count > 1 {
            for record in records.iter() {
                actions.push(binding_repair_action(
                    AcpBindingRepairActionKind::MarkStale,
                    AcpBindingConflictKind::PrincipalMismatch,
                    "conversation_binding",
                    record.binding_id.as_str(),
                    "external conversation is claimed by multiple principals; automatic widening is refused",
                    record.palyra_session_id.as_str(),
                    false,
                ));
            }
            continue;
        }
        if workspace_count > 1 {
            for record in records.iter().skip(1) {
                actions.push(binding_repair_action(
                    AcpBindingRepairActionKind::Split,
                    AcpBindingConflictKind::WorkspaceMismatch,
                    "conversation_binding",
                    record.binding_id.as_str(),
                    "external conversation spans multiple workspace roots and needs explicit operator split",
                    record.palyra_session_id.as_str(),
                    false,
                ));
            }
            continue;
        }
        for duplicate in records.iter().skip(1) {
            actions.push(binding_repair_action(
                AcpBindingRepairActionKind::Detach,
                AcpBindingConflictKind::DuplicateActiveBinding,
                "conversation_binding",
                duplicate.binding_id.as_str(),
                "duplicate active external conversation binding",
                duplicate.palyra_session_id.as_str(),
                true,
            ));
        }
    }
    for binding in &index.conversation_bindings {
        if binding.conflict_state == ConversationBindingConflictState::Detached {
            continue;
        }
        if binding.scopes.iter().any(|scope| scope == "parent:required")
            && !session_exists(index, binding.palyra_session_id.as_str())
        {
            actions.push(binding_repair_action(
                AcpBindingRepairActionKind::MarkStale,
                AcpBindingConflictKind::ParentMissing,
                "conversation_binding",
                binding.binding_id.as_str(),
                "conversation binding references a session with no active ACP session binding",
                binding.palyra_session_id.as_str(),
                true,
            ));
        }
    }
    let now = unix_ms_now().unwrap_or(i64::MAX);
    for prompt in &index.pending_prompts {
        if prompt.expires_at_unix_ms < now {
            actions.push(binding_repair_action(
                AcpBindingRepairActionKind::Expire,
                AcpBindingConflictKind::ExpiredReferenced,
                "pending_prompt",
                prompt.prompt_id.as_str(),
                "pending ACP prompt is past its disconnect grace deadline",
                prompt.palyra_session_id.as_str(),
                true,
            ));
        }
    }
    BindingRepairPlan { dry_run, actions }
}

fn binding_repair_action(
    action: AcpBindingRepairActionKind,
    conflict_kind: AcpBindingConflictKind,
    target_kind: &str,
    binding_id: &str,
    reason: &str,
    target_session_id: &str,
    automatic_apply: bool,
) -> BindingRepairAction {
    BindingRepairAction {
        action: action.as_str().to_owned(),
        conflict_kind: conflict_kind.as_str().to_owned(),
        target_kind: target_kind.to_owned(),
        binding_id: binding_id.to_owned(),
        reason: reason.to_owned(),
        target_session_id: target_session_id.to_owned(),
        policy_gate: "acp.binding.repair".to_owned(),
        automatic_apply,
    }
}

fn session_exists(index: &AcpBindingsIndex, session_id: &str) -> bool {
    index.session_bindings.iter().any(|binding| binding.palyra_session_id == session_id)
}

// Best-effort workspace attribution: explicit config keys win, then the
// `repo:`/`cwd:` session-key prefix heuristic. `None` means "unknown", which
// deliberately never counts as a workspace mismatch.
fn workspace_key_for_session(index: &AcpBindingsIndex, session_id: &str) -> Option<String> {
    index.session_bindings.iter().find(|binding| binding.palyra_session_id == session_id).and_then(
        |binding| {
            binding
                .config
                .get("workspace_id")
                .and_then(Value::as_str)
                .or_else(|| binding.config.get("workspace").and_then(Value::as_str))
                .map(str::to_owned)
                .or_else(|| workspace_from_session_key(binding.session_key.as_str()))
        },
    )
}

fn workspace_from_session_key(session_key: &str) -> Option<String> {
    session_key
        .strip_prefix("repo:")
        .or_else(|| session_key.strip_prefix("cwd:"))
        .map(|value| value.trim().to_ascii_lowercase())
        .filter(|value| !value.is_empty())
}

fn conflict_kinds_for_conversation(
    record: &ConversationBindingRecord,
    index: &AcpBindingsIndex,
) -> Vec<String> {
    let mut kinds = BTreeSet::new();
    match record.conflict_state {
        ConversationBindingConflictState::DuplicateActiveBinding
        | ConversationBindingConflictState::DuplicateExternalIdentity
        | ConversationBindingConflictState::DuplicateSession => {
            kinds.insert(AcpBindingConflictKind::DuplicateActiveBinding.as_str().to_owned());
        }
        ConversationBindingConflictState::StaleThread => {
            kinds.insert(AcpBindingConflictKind::StaleThread.as_str().to_owned());
        }
        ConversationBindingConflictState::PrincipalMismatch => {
            kinds.insert(AcpBindingConflictKind::PrincipalMismatch.as_str().to_owned());
        }
        ConversationBindingConflictState::WorkspaceMismatch => {
            kinds.insert(AcpBindingConflictKind::WorkspaceMismatch.as_str().to_owned());
        }
        ConversationBindingConflictState::ExpiredReference => {
            kinds.insert(AcpBindingConflictKind::ExpiredReferenced.as_str().to_owned());
        }
        ConversationBindingConflictState::ParentMissing => {
            kinds.insert(AcpBindingConflictKind::ParentMissing.as_str().to_owned());
        }
        ConversationBindingConflictState::None | ConversationBindingConflictState::Detached => {}
    }
    for action in build_repair_plan(index, true).actions {
        if action.binding_id == record.binding_id {
            kinds.insert(action.conflict_kind);
        }
    }
    kinds.into_iter().collect()
}

pub(crate) mod live_runtime_manager;
mod permission_relay;
mod replay_translator;
mod runtime_registry;
mod session_actor_queue;
#[cfg(test)]
mod tests;
mod validation;
