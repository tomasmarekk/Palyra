//! Tool security posture: per-scope approval overrides, presets, and
//! friction-based recommendations.
//!
//! [`ToolPostureRegistry`] persists operator overrides
//! (always-allow/ask-each-time/disabled), recommendation actions, and an
//! audit trail in `tool-posture/registry.json` under the state root.
//! [`evaluate_effective_tool_posture`] resolves the effective state through
//! the session -> agent -> workspace -> global scope chain, with runtime lock
//! reasons ([`tool_lock_reason`]) overriding everything. The static
//! [`TOOL_CATALOG`] and [`TOOL_POSTURE_PRESETS`] feed the console UI;
//! approval analytics come from journal [`ApprovalRecord`]s.

use std::{
    fs,
    io::Write,
    path::{Path, PathBuf},
    sync::Mutex,
};

use serde::{Deserialize, Serialize};
use thiserror::Error;
use ulid::Ulid;

use crate::{
    gateway::{current_unix_ms, GatewayRuntimeConfigSnapshot},
    journal::{ApprovalDecision, ApprovalRecord, ApprovalRiskLevel},
    tool_protocol::{self, ToolCapability},
};

const TOOL_POSTURE_DIRECTORY: &str = "tool-posture";
const TOOL_POSTURE_REGISTRY_FILE: &str = "registry.json";
const TOOL_POSTURE_SCHEMA_VERSION: u32 = 2;
const GLOBAL_SCOPE_ID: &str = "global";
/// Lookback window (14 days) for approval analytics fed into recommendations.
pub(crate) const TOOL_POSTURE_ANALYTICS_WINDOW_MS: i64 = 14 * 24 * 60 * 60 * 1_000;
/// Minimum deny-free approvals in the window before an always-allow
/// recommendation is surfaced.
pub(crate) const TOOL_POSTURE_RECOMMENDATION_MIN_APPROVALS: u64 = 5;

/// Failure modes of [`ToolPostureRegistry`] persistence and validation.
#[derive(Debug, Error)]
pub enum ToolPostureRegistryError {
    #[error("tool posture directory could not be created: {path}")]
    CreateDirectory {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("tool posture registry could not be opened: {path}")]
    OpenFile {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("tool posture registry could not be read: {path}")]
    ReadFile {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("tool posture registry could not be written: {path}")]
    WriteFile {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("tool posture registry could not be serialized: {path}")]
    SerializeFile {
        path: PathBuf,
        #[source]
        source: serde_json::Error,
    },
    #[error("tool posture registry could not be parsed: {path}")]
    ParseFile {
        path: PathBuf,
        #[source]
        source: serde_json::Error,
    },
    #[error("tool posture registry lock poisoned")]
    LockPoisoned,
    #[error("tool_name must be a known tool id")]
    UnknownTool,
    #[error("scope_id is required for non-global scopes")]
    MissingScopeId,
}

/// Approval posture of a tool within one scope.
///
/// `AlwaysAllow` is the out-of-box default for an unlocked, allowlisted tool.
/// Operators can opt into `AskEachTime` as an explicit safe-mode override.
/// Serialized snake_case names are persisted state.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash, Default)]
#[serde(rename_all = "snake_case")]
pub enum ToolPostureState {
    #[default]
    AlwaysAllow,
    AskEachTime,
    Disabled,
}

impl ToolPostureState {
    /// Stable snake_case identifier matching the serde representation.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::AlwaysAllow => "always_allow",
            Self::AskEachTime => "ask_each_time",
            Self::Disabled => "disabled",
        }
    }

    /// Human-readable approval-mode label shown in operator surfaces.
    #[must_use]
    pub fn approval_mode_label(self) -> &'static str {
        match self {
            Self::AlwaysAllow => "no approval",
            Self::AskEachTime => "ask each time",
            Self::Disabled => "disabled",
        }
    }
}

/// Scope levels an override can attach to, from broadest to narrowest:
/// global, workspace, agent, session. Narrower scopes win during resolution.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum ToolPostureScopeKind {
    Global,
    Workspace,
    Agent,
    Session,
}

impl ToolPostureScopeKind {
    /// Stable snake_case identifier matching the serde representation.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Global => "global",
            Self::Workspace => "workspace",
            Self::Agent => "agent",
            Self::Session => "session",
        }
    }
}

/// Operator response to a posture recommendation.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ToolPostureRecommendationAction {
    Accepted,
    Dismissed,
    Deferred,
}

/// Kind of change captured by a [`ToolPostureAuditEventRecord`].
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ToolPostureAuditAction {
    OverrideSet,
    OverrideCleared,
    RecommendationAccepted,
    RecommendationDismissed,
    RecommendationDeferred,
}

impl ToolPostureAuditAction {
    /// Stable snake_case identifier matching the serde representation.
    #[allow(dead_code)]
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::OverrideSet => "override_set",
            Self::OverrideCleared => "override_cleared",
            Self::RecommendationAccepted => "recommendation_accepted",
            Self::RecommendationDismissed => "recommendation_dismissed",
            Self::RecommendationDeferred => "recommendation_deferred",
        }
    }
}

impl ToolPostureRecommendationAction {
    /// Stable snake_case identifier matching the serde representation.
    #[allow(dead_code)]
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Accepted => "accepted",
            Self::Dismissed => "dismissed",
            Self::Deferred => "deferred",
        }
    }
}

/// Persisted operator override pinning one tool's posture in one scope.
///
/// Expired records (past `expires_at_unix_ms`) are pruned lazily on the next
/// registry read or write.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ToolPostureOverrideRecord {
    pub tool_name: String,
    pub scope_kind: ToolPostureScopeKind,
    pub scope_id: String,
    pub state: ToolPostureState,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    pub actor_principal: String,
    pub source: String,
    pub created_at_unix_ms: i64,
    pub updated_at_unix_ms: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expires_at_unix_ms: Option<i64>,
}

/// Persisted operator response to a recommendation, keyed by
/// recommendation id and scope so re-actions update in place.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ToolPostureRecommendationActionRecord {
    pub recommendation_id: String,
    pub scope_kind: ToolPostureScopeKind,
    pub scope_id: String,
    pub action: ToolPostureRecommendationAction,
    pub actor_principal: String,
    pub created_at_unix_ms: i64,
}

/// Audit-trail entry for posture changes; newest-first, capped at 2000
/// entries by `append_audit_event`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ToolPostureAuditEventRecord {
    pub audit_id: String,
    pub scope_kind: ToolPostureScopeKind,
    pub scope_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tool_name: Option<String>,
    pub actor_principal: String,
    pub action: ToolPostureAuditAction,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub previous_state: Option<ToolPostureState>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub new_state: Option<ToolPostureState>,
    pub source: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub recommendation_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub preset_id: Option<String>,
    pub created_at_unix_ms: i64,
}

/// Request to create or update one override; see
/// [`ToolPostureRegistry::upsert_override`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ToolPostureOverrideUpsertRequest {
    pub tool_name: String,
    pub scope_kind: ToolPostureScopeKind,
    pub scope_id: String,
    pub state: ToolPostureState,
    pub reason: Option<String>,
    pub actor_principal: String,
    pub source: String,
    pub expires_at_unix_ms: Option<i64>,
    pub now_unix_ms: i64,
}

/// Request to remove one override; see
/// [`ToolPostureRegistry::clear_override`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ToolPostureOverrideClearRequest {
    pub tool_name: String,
    pub scope_kind: ToolPostureScopeKind,
    pub scope_id: String,
    pub actor_principal: String,
    pub source: String,
    pub reason: Option<String>,
    pub now_unix_ms: i64,
}

/// Request to record an operator action on a recommendation; see
/// [`ToolPostureRegistry::record_recommendation_action`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ToolPostureRecommendationActionRequest {
    pub recommendation_id: String,
    pub scope_kind: ToolPostureScopeKind,
    pub scope_id: String,
    pub action: ToolPostureRecommendationAction,
    pub actor_principal: String,
    pub now_unix_ms: i64,
}

/// Request to remove every override in one scope; see
/// [`ToolPostureRegistry::reset_scope`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ToolPostureScopeResetRequest {
    pub scope_kind: ToolPostureScopeKind,
    pub scope_id: String,
    pub actor_principal: String,
    pub source: String,
    pub reason: Option<String>,
    pub now_unix_ms: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct ToolPostureRegistryDocument {
    schema_version: u32,
    #[serde(default)]
    overrides: Vec<ToolPostureOverrideRecord>,
    #[serde(default)]
    recommendation_actions: Vec<ToolPostureRecommendationActionRecord>,
    #[serde(default)]
    audit_events: Vec<ToolPostureAuditEventRecord>,
}

/// Durable store for posture overrides, recommendation actions, and audit
/// events, backed by a single JSON document.
///
/// Mutations update the in-memory document under `document`, then replace the
/// on-disk JSON through a temporary sidecar file.
pub struct ToolPostureRegistry {
    document_path: PathBuf,
    document: Mutex<ToolPostureRegistryDocument>,
}

impl ToolPostureRegistry {
    /// Opens (and creates if needed) the registry under
    /// `<state_root>/tool-posture/registry.json`.
    ///
    /// # Errors
    /// Returns a [`ToolPostureRegistryError`] variant when the directory or
    /// file cannot be created, read, or parsed.
    pub fn open(state_root: &Path) -> Result<Self, ToolPostureRegistryError> {
        let tool_posture_root = state_root.join(TOOL_POSTURE_DIRECTORY);
        fs::create_dir_all(&tool_posture_root).map_err(|source| {
            ToolPostureRegistryError::CreateDirectory { path: tool_posture_root.clone(), source }
        })?;
        let document_path = tool_posture_root.join(TOOL_POSTURE_REGISTRY_FILE);
        fs::File::options()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(&document_path)
            .map_err(|source| ToolPostureRegistryError::OpenFile {
                path: document_path.clone(),
                source,
            })?;
        let document = load_registry_document(&document_path)?;
        Ok(Self { document_path, document: Mutex::new(document) })
    }

    /// Lists current overrides, pruning expired entries as a side effect.
    ///
    /// # Errors
    /// Fails on lock poisoning or when persisting the pruned document fails.
    pub fn list_overrides(
        &self,
    ) -> Result<Vec<ToolPostureOverrideRecord>, ToolPostureRegistryError> {
        let now_unix_ms = current_unix_ms();
        let mut document =
            self.document.lock().map_err(|_| ToolPostureRegistryError::LockPoisoned)?;
        if prune_expired_entries(&mut document, now_unix_ms) {
            persist_registry_document(&self.document_path, &document)?;
        }
        Ok(document.overrides.clone())
    }

    /// Lists recorded recommendation actions.
    ///
    /// # Errors
    /// Fails only on lock poisoning.
    pub fn list_recommendation_actions(
        &self,
    ) -> Result<Vec<ToolPostureRecommendationActionRecord>, ToolPostureRegistryError> {
        let document = self.document.lock().map_err(|_| ToolPostureRegistryError::LockPoisoned)?;
        Ok(document.recommendation_actions.clone())
    }

    /// Lists audit events, newest first.
    ///
    /// # Errors
    /// Fails only on lock poisoning.
    pub fn list_audit_events(
        &self,
    ) -> Result<Vec<ToolPostureAuditEventRecord>, ToolPostureRegistryError> {
        let document = self.document.lock().map_err(|_| ToolPostureRegistryError::LockPoisoned)?;
        Ok(document.audit_events.clone())
    }

    /// Creates or updates the override for `(tool_name, scope_kind,
    /// scope_id)`, appending an `override_set` audit event and persisting.
    ///
    /// # Errors
    /// Returns [`ToolPostureRegistryError::UnknownTool`] for tools outside
    /// [`TOOL_CATALOG`], [`ToolPostureRegistryError::MissingScopeId`] for
    /// non-global scopes without an id, plus lock/persistence failures.
    pub fn upsert_override(
        &self,
        request: ToolPostureOverrideUpsertRequest,
    ) -> Result<ToolPostureOverrideRecord, ToolPostureRegistryError> {
        validate_known_tool(request.tool_name.as_str())?;
        let normalized_scope_id =
            normalize_scope_id(request.scope_kind, Some(request.scope_id.as_str()))?;
        let normalized_reason = normalize_optional_text(request.reason);
        let normalized_source = if request.source.trim().is_empty() {
            "manual".to_owned()
        } else {
            request.source.trim().to_owned()
        };
        let mut document =
            self.document.lock().map_err(|_| ToolPostureRegistryError::LockPoisoned)?;
        prune_expired_entries(&mut document, request.now_unix_ms);
        let record = if let Some(existing_index) = document.overrides.iter().position(|record| {
            record.tool_name == request.tool_name
                && record.scope_kind == request.scope_kind
                && record.scope_id == normalized_scope_id
        }) {
            let existing = &mut document.overrides[existing_index];
            let previous_state = existing.state;
            existing.state = request.state;
            existing.reason = normalized_reason.clone();
            existing.actor_principal = request.actor_principal.clone();
            existing.source = normalized_source.clone();
            existing.updated_at_unix_ms = request.now_unix_ms;
            existing.expires_at_unix_ms = request.expires_at_unix_ms;
            let updated = existing.clone();
            append_audit_event(
                &mut document.audit_events,
                ToolPostureAuditEventRecord {
                    audit_id: Ulid::generate().to_string(),
                    scope_kind: request.scope_kind,
                    scope_id: normalized_scope_id.clone(),
                    tool_name: Some(request.tool_name.clone()),
                    actor_principal: request.actor_principal.clone(),
                    action: ToolPostureAuditAction::OverrideSet,
                    previous_state: Some(previous_state),
                    new_state: Some(request.state),
                    source: normalized_source.clone(),
                    reason: normalized_reason.clone(),
                    recommendation_id: None,
                    preset_id: extract_preset_id(normalized_source.as_str()),
                    created_at_unix_ms: request.now_unix_ms,
                },
            );
            updated
        } else {
            let record = ToolPostureOverrideRecord {
                tool_name: request.tool_name,
                scope_kind: request.scope_kind,
                scope_id: normalized_scope_id.clone(),
                state: request.state,
                reason: normalized_reason,
                actor_principal: request.actor_principal,
                source: normalized_source,
                created_at_unix_ms: request.now_unix_ms,
                updated_at_unix_ms: request.now_unix_ms,
                expires_at_unix_ms: request.expires_at_unix_ms,
            };
            append_audit_event(
                &mut document.audit_events,
                ToolPostureAuditEventRecord {
                    audit_id: Ulid::generate().to_string(),
                    scope_kind: record.scope_kind,
                    scope_id: record.scope_id.clone(),
                    tool_name: Some(record.tool_name.clone()),
                    actor_principal: record.actor_principal.clone(),
                    action: ToolPostureAuditAction::OverrideSet,
                    previous_state: None,
                    new_state: Some(record.state),
                    source: record.source.clone(),
                    reason: record.reason.clone(),
                    recommendation_id: None,
                    preset_id: extract_preset_id(record.source.as_str()),
                    created_at_unix_ms: record.updated_at_unix_ms,
                },
            );
            document.overrides.push(record.clone());
            record
        };
        // Deterministic ordering keeps the persisted JSON diff-stable across
        // upserts regardless of insertion order.
        document.overrides.sort_by(|left, right| {
            left.scope_kind
                .as_str()
                .cmp(right.scope_kind.as_str())
                .then_with(|| left.scope_id.cmp(&right.scope_id))
                .then_with(|| left.tool_name.cmp(&right.tool_name))
        });
        persist_registry_document(&self.document_path, &document)?;
        Ok(record)
    }

    /// Removes the matching override, returning `true` when one existed.
    /// Audit event and persistence happen only on actual removal.
    ///
    /// # Errors
    /// Same validation and persistence failures as
    /// [`ToolPostureRegistry::upsert_override`].
    pub fn clear_override(
        &self,
        request: ToolPostureOverrideClearRequest,
    ) -> Result<bool, ToolPostureRegistryError> {
        validate_known_tool(request.tool_name.as_str())?;
        let normalized_scope_id =
            normalize_scope_id(request.scope_kind, Some(request.scope_id.as_str()))?;
        let normalized_reason = normalize_optional_text(request.reason);
        let normalized_source = if request.source.trim().is_empty() {
            "manual_reset".to_owned()
        } else {
            request.source.trim().to_owned()
        };
        let mut document =
            self.document.lock().map_err(|_| ToolPostureRegistryError::LockPoisoned)?;
        let previous_state = document
            .overrides
            .iter()
            .find(|record| {
                record.tool_name == request.tool_name
                    && record.scope_kind == request.scope_kind
                    && record.scope_id == normalized_scope_id
            })
            .map(|record| record.state);
        let previous_len = document.overrides.len();
        document.overrides.retain(|record| {
            !(record.tool_name == request.tool_name
                && record.scope_kind == request.scope_kind
                && record.scope_id == normalized_scope_id)
        });
        let removed = previous_len != document.overrides.len();
        if removed {
            append_audit_event(
                &mut document.audit_events,
                ToolPostureAuditEventRecord {
                    audit_id: Ulid::generate().to_string(),
                    scope_kind: request.scope_kind,
                    scope_id: normalized_scope_id,
                    tool_name: Some(request.tool_name),
                    actor_principal: request.actor_principal,
                    action: ToolPostureAuditAction::OverrideCleared,
                    previous_state,
                    new_state: None,
                    source: normalized_source,
                    reason: normalized_reason,
                    recommendation_id: None,
                    preset_id: None,
                    created_at_unix_ms: request.now_unix_ms,
                },
            );
            persist_registry_document(&self.document_path, &document)?;
        }
        Ok(removed)
    }

    /// Removes every override in one scope, returning the removed records
    /// and appending one `override_cleared` audit event per removal.
    ///
    /// # Errors
    /// Fails on scope-id validation, lock poisoning, or persistence errors.
    pub fn reset_scope(
        &self,
        request: ToolPostureScopeResetRequest,
    ) -> Result<Vec<ToolPostureOverrideRecord>, ToolPostureRegistryError> {
        let normalized_scope_id =
            normalize_scope_id(request.scope_kind, Some(request.scope_id.as_str()))?;
        let normalized_reason = normalize_optional_text(request.reason);
        let normalized_source = if request.source.trim().is_empty() {
            "manual_reset".to_owned()
        } else {
            request.source.trim().to_owned()
        };
        let mut document =
            self.document.lock().map_err(|_| ToolPostureRegistryError::LockPoisoned)?;
        let mut removed = Vec::new();
        document.overrides.retain(|record| {
            let matches_scope =
                record.scope_kind == request.scope_kind && record.scope_id == normalized_scope_id;
            if matches_scope {
                removed.push(record.clone());
            }
            !matches_scope
        });
        for record in removed.as_slice() {
            append_audit_event(
                &mut document.audit_events,
                ToolPostureAuditEventRecord {
                    audit_id: Ulid::generate().to_string(),
                    scope_kind: request.scope_kind,
                    scope_id: normalized_scope_id.clone(),
                    tool_name: Some(record.tool_name.clone()),
                    actor_principal: request.actor_principal.clone(),
                    action: ToolPostureAuditAction::OverrideCleared,
                    previous_state: Some(record.state),
                    new_state: None,
                    source: normalized_source.clone(),
                    reason: normalized_reason.clone(),
                    recommendation_id: None,
                    preset_id: None,
                    created_at_unix_ms: request.now_unix_ms,
                },
            );
        }
        if !removed.is_empty() {
            persist_registry_document(&self.document_path, &document)?;
        }
        Ok(removed)
    }

    /// Records (or updates in place) the operator action for one
    /// recommendation in one scope, with a matching audit event.
    ///
    /// # Errors
    /// Fails on scope-id validation, lock poisoning, or persistence errors.
    pub fn record_recommendation_action(
        &self,
        request: ToolPostureRecommendationActionRequest,
    ) -> Result<ToolPostureRecommendationActionRecord, ToolPostureRegistryError> {
        let normalized_scope_id =
            normalize_scope_id(request.scope_kind, Some(request.scope_id.as_str()))?;
        let mut document =
            self.document.lock().map_err(|_| ToolPostureRegistryError::LockPoisoned)?;
        if let Some(existing_index) = document.recommendation_actions.iter().position(|record| {
            record.recommendation_id == request.recommendation_id
                && record.scope_kind == request.scope_kind
                && record.scope_id == normalized_scope_id
        }) {
            let existing = &mut document.recommendation_actions[existing_index];
            existing.action = request.action;
            existing.actor_principal = request.actor_principal.clone();
            existing.created_at_unix_ms = request.now_unix_ms;
            let updated = existing.clone();
            append_audit_event(
                &mut document.audit_events,
                ToolPostureAuditEventRecord {
                    audit_id: Ulid::generate().to_string(),
                    scope_kind: request.scope_kind,
                    scope_id: normalized_scope_id.clone(),
                    tool_name: None,
                    actor_principal: request.actor_principal,
                    action: recommendation_action_to_audit_action(request.action),
                    previous_state: None,
                    new_state: None,
                    source: "recommendation".to_owned(),
                    reason: None,
                    recommendation_id: Some(updated.recommendation_id.clone()),
                    preset_id: None,
                    created_at_unix_ms: request.now_unix_ms,
                },
            );
            persist_registry_document(&self.document_path, &document)?;
            return Ok(updated);
        }
        let record = ToolPostureRecommendationActionRecord {
            recommendation_id: request.recommendation_id,
            scope_kind: request.scope_kind,
            scope_id: normalized_scope_id,
            action: request.action,
            actor_principal: request.actor_principal,
            created_at_unix_ms: request.now_unix_ms,
        };
        document.recommendation_actions.push(record.clone());
        append_audit_event(
            &mut document.audit_events,
            ToolPostureAuditEventRecord {
                audit_id: Ulid::generate().to_string(),
                scope_kind: request.scope_kind,
                scope_id: record.scope_id.clone(),
                tool_name: None,
                actor_principal: record.actor_principal.clone(),
                action: recommendation_action_to_audit_action(request.action),
                previous_state: None,
                new_state: None,
                source: "recommendation".to_owned(),
                reason: None,
                recommendation_id: Some(record.recommendation_id.clone()),
                preset_id: None,
                created_at_unix_ms: record.created_at_unix_ms,
            },
        );
        persist_registry_document(&self.document_path, &document)?;
        Ok(record)
    }
}

/// Static metadata describing one tool in the operator-facing catalog.
///
/// `recommend_always_allow` marks tools safe enough that high deny-free
/// approval volume should suggest promoting them to always-allow.
#[derive(Debug, Clone, Copy, Serialize)]
pub struct ToolCatalogEntry {
    pub tool_name: &'static str,
    pub title: &'static str,
    pub description: &'static str,
    pub category: &'static str,
    pub risk_level: ApprovalRiskLevel,
    pub recommend_always_allow: bool,
}

/// Approval friction counters for one tool over the analytics window.
#[derive(Debug, Clone, Serialize, PartialEq, Eq, Default)]
pub struct ToolFrictionMetrics {
    pub requested_14d: u64,
    pub approved_14d: u64,
    pub denied_14d: u64,
    pub pending_14d: u64,
    pub unique_sessions_14d: u64,
}

/// Identifies one scope in a resolution chain, with its display label.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct ToolPostureScopeRef {
    pub kind: ToolPostureScopeKind,
    pub scope_id: String,
    pub label: String,
}

/// One link of the resolution chain shown to operators: the scope plus the
/// override state/source found there, if any.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct ToolPostureChainEntry {
    pub kind: ToolPostureScopeKind,
    pub scope_id: String,
    pub label: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub state: Option<ToolPostureState>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source: Option<String>,
}

/// Resolved posture of one tool for one scope chain, including which scope
/// supplied the winning state and whether the tool is runtime-locked.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct EffectiveToolPosture {
    pub effective_state: ToolPostureState,
    pub default_state: ToolPostureState,
    pub approval_mode: String,
    pub source_scope_kind: ToolPostureScopeKind,
    pub source_scope_id: String,
    pub source_scope_label: String,
    pub chain: Vec<ToolPostureChainEntry>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lock_reason: Option<String>,
    pub editable: bool,
}

/// Suggested ask-each-time -> always-allow promotion derived from approval
/// analytics; see [`build_tool_recommendation`].
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct ToolPostureRecommendation {
    pub recommendation_id: String,
    pub tool_name: String,
    pub scope_kind: ToolPostureScopeKind,
    pub scope_id: String,
    pub current_state: ToolPostureState,
    pub recommended_state: ToolPostureState,
    pub reason: String,
    pub approvals_14d: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub action: Option<ToolPostureRecommendationAction>,
}

/// One tool-state pair inside a preset.
#[derive(Debug, Clone, Copy, Serialize)]
pub struct ToolPosturePresetAssignment {
    pub tool_name: &'static str,
    pub state: ToolPostureState,
}

/// Named bundle of posture assignments operators can apply in one step.
#[derive(Debug, Clone, Copy, Serialize)]
pub struct ToolPosturePresetDefinition {
    pub preset_id: &'static str,
    pub label: &'static str,
    pub description: &'static str,
    pub assignments: &'static [ToolPosturePresetAssignment],
}

/// Operator-facing tool catalog: titles, risk levels, and always-allow
/// recommendation flags. Tools absent from this list are rejected by the
/// posture registry (`validate_known_tool`).
pub const TOOL_CATALOG: &[ToolCatalogEntry] = &[
    ToolCatalogEntry {
        tool_name: "palyra.echo",
        title: "Echo",
        description: "Returns text back to the agent without touching the host.",
        category: "utility",
        risk_level: ApprovalRiskLevel::Low,
        recommend_always_allow: false,
    },
    ToolCatalogEntry {
        tool_name: "palyra.sleep",
        title: "Sleep",
        description: "Waits for a bounded interval without mutating local state.",
        category: "utility",
        risk_level: ApprovalRiskLevel::Low,
        recommend_always_allow: false,
    },
    ToolCatalogEntry {
        tool_name: "palyra.memory.search",
        title: "Memory search",
        description: "Reads indexed memory items without changing workspace state.",
        category: "memory",
        risk_level: ApprovalRiskLevel::Low,
        recommend_always_allow: false,
    },
    ToolCatalogEntry {
        tool_name: "palyra.memory.recall",
        title: "Memory recall",
        description: "Plans bounded recall across memory, workspace, and session evidence.",
        category: "memory",
        risk_level: ApprovalRiskLevel::High,
        recommend_always_allow: false,
    },
    ToolCatalogEntry {
        tool_name: "palyra.http.fetch",
        title: "HTTP fetch",
        description: "Makes outbound HTTP requests through the gateway fetch broker.",
        category: "network",
        risk_level: ApprovalRiskLevel::High,
        recommend_always_allow: false,
    },
    ToolCatalogEntry {
        tool_name: "palyra.web.search",
        title: "Web search",
        description: "Searches public sources through the governed provider-neutral search broker.",
        category: "network",
        risk_level: ApprovalRiskLevel::High,
        recommend_always_allow: false,
    },
    ToolCatalogEntry {
        tool_name: "palyra.image.observe",
        title: "Image observe",
        description: "Returns image metadata or a structured OCR/vision capability error.",
        category: "artifacts",
        risk_level: ApprovalRiskLevel::Low,
        recommend_always_allow: true,
    },
    ToolCatalogEntry {
        tool_name: "palyra.process.run",
        title: "Process runner",
        description: "Executes allowlisted local commands inside the configured sandbox tier.",
        category: "shell",
        risk_level: ApprovalRiskLevel::Critical,
        recommend_always_allow: false,
    },
    ToolCatalogEntry {
        tool_name: "palyra.process.stop",
        title: "Process stop",
        description: "Stops a run-owned background process by PID.",
        category: "shell",
        risk_level: ApprovalRiskLevel::High,
        recommend_always_allow: true,
    },
    ToolCatalogEntry {
        tool_name: "palyra.process.status",
        title: "Process status",
        description: "Checks whether a run-owned background process PID is alive.",
        category: "shell",
        risk_level: ApprovalRiskLevel::Medium,
        recommend_always_allow: true,
    },
    ToolCatalogEntry {
        tool_name: "palyra.process.list",
        title: "Process list",
        description: "Lists background process PIDs tracked for cleanup in the active run.",
        category: "shell",
        risk_level: ApprovalRiskLevel::Medium,
        recommend_always_allow: true,
    },
    ToolCatalogEntry {
        tool_name: "palyra.fs.read_file",
        title: "Workspace file read",
        description: "Reads bounded chunks from files inside the resolved workspace boundary.",
        category: "filesystem",
        risk_level: ApprovalRiskLevel::Medium,
        recommend_always_allow: true,
    },
    ToolCatalogEntry {
        tool_name: "palyra.fs.list_dir",
        title: "Workspace directory list",
        description: "Lists directory entries inside the resolved workspace boundary.",
        category: "filesystem",
        risk_level: ApprovalRiskLevel::Low,
        recommend_always_allow: true,
    },
    ToolCatalogEntry {
        tool_name: "palyra.fs.search",
        title: "Workspace text search",
        description: "Searches workspace text files inside the resolved workspace boundary.",
        category: "filesystem",
        risk_level: ApprovalRiskLevel::Medium,
        recommend_always_allow: true,
    },
    ToolCatalogEntry {
        tool_name: "palyra.fs.apply_patch",
        title: "Workspace patch",
        description: "Applies attested file patches inside the resolved workspace boundary.",
        category: "filesystem",
        risk_level: ApprovalRiskLevel::Critical,
        recommend_always_allow: false,
    },
    ToolCatalogEntry {
        tool_name: "palyra.fs.os_file",
        title: "OS file operation",
        description: "Reads or mutates approved user-owned OS paths with resolved-path audit metadata.",
        category: "filesystem",
        risk_level: ApprovalRiskLevel::Critical,
        recommend_always_allow: false,
    },
    ToolCatalogEntry {
        tool_name: "palyra.browser.session.create",
        title: "Browser session create",
        description: "Starts a browser automation session through the relay broker.",
        category: "browser",
        risk_level: ApprovalRiskLevel::Medium,
        recommend_always_allow: false,
    },
    ToolCatalogEntry {
        tool_name: "palyra.browser.session.close",
        title: "Browser session close",
        description: "Closes a browser automation session.",
        category: "browser",
        risk_level: ApprovalRiskLevel::Medium,
        recommend_always_allow: false,
    },
    ToolCatalogEntry {
        tool_name: "palyra.browser.navigate",
        title: "Browser navigate",
        description: "Navigates the active browser session to a URL.",
        category: "browser",
        risk_level: ApprovalRiskLevel::High,
        recommend_always_allow: false,
    },
    ToolCatalogEntry {
        tool_name: "palyra.browser.click",
        title: "Browser click",
        description: "Clicks a DOM target inside the active browser session.",
        category: "browser",
        risk_level: ApprovalRiskLevel::High,
        recommend_always_allow: false,
    },
    ToolCatalogEntry {
        tool_name: "palyra.browser.type",
        title: "Browser type",
        description: "Types text into the active browser session.",
        category: "browser",
        risk_level: ApprovalRiskLevel::High,
        recommend_always_allow: false,
    },
    ToolCatalogEntry {
        tool_name: "palyra.browser.press",
        title: "Browser key press",
        description: "Sends keyboard events to the active browser session.",
        category: "browser",
        risk_level: ApprovalRiskLevel::High,
        recommend_always_allow: false,
    },
    ToolCatalogEntry {
        tool_name: "palyra.browser.select",
        title: "Browser select",
        description: "Selects an option within the active browser session.",
        category: "browser",
        risk_level: ApprovalRiskLevel::High,
        recommend_always_allow: false,
    },
    ToolCatalogEntry {
        tool_name: "palyra.browser.viewport",
        title: "Browser viewport",
        description: "Sets active browser viewport dimensions for responsive layout verification.",
        category: "browser",
        risk_level: ApprovalRiskLevel::Medium,
        recommend_always_allow: true,
    },
    ToolCatalogEntry {
        tool_name: "palyra.browser.highlight",
        title: "Browser highlight",
        description: "Highlights a DOM target for operator inspection.",
        category: "browser",
        risk_level: ApprovalRiskLevel::Medium,
        recommend_always_allow: true,
    },
    ToolCatalogEntry {
        tool_name: "palyra.browser.scroll",
        title: "Browser scroll",
        description: "Scrolls the active browser session viewport.",
        category: "browser",
        risk_level: ApprovalRiskLevel::Medium,
        recommend_always_allow: true,
    },
    ToolCatalogEntry {
        tool_name: "palyra.browser.wait_for",
        title: "Browser wait",
        description: "Waits for page readiness or target visibility in the browser broker.",
        category: "browser",
        risk_level: ApprovalRiskLevel::Medium,
        recommend_always_allow: true,
    },
    ToolCatalogEntry {
        tool_name: "palyra.browser.title",
        title: "Browser title",
        description: "Reads the active tab title without mutating page state.",
        category: "browser",
        risk_level: ApprovalRiskLevel::Low,
        recommend_always_allow: true,
    },
    ToolCatalogEntry {
        tool_name: "palyra.browser.screenshot",
        title: "Browser screenshot",
        description: "Captures a bounded screenshot from the active browser session.",
        category: "browser",
        risk_level: ApprovalRiskLevel::Medium,
        recommend_always_allow: true,
    },
    ToolCatalogEntry {
        tool_name: "palyra.browser.pdf",
        title: "Browser PDF",
        description: "Exports the active page to PDF through the browser broker.",
        category: "browser",
        risk_level: ApprovalRiskLevel::Medium,
        recommend_always_allow: false,
    },
    ToolCatalogEntry {
        tool_name: "palyra.browser.observe",
        title: "Browser observe",
        description: "Collects DOM or accessibility observations from the browser broker.",
        category: "browser",
        risk_level: ApprovalRiskLevel::Low,
        recommend_always_allow: true,
    },
    ToolCatalogEntry {
        tool_name: "palyra.browser.storage",
        title: "Browser storage",
        description: "Reads browser cookie/localStorage names and value metadata with secret values withheld.",
        category: "browser",
        risk_level: ApprovalRiskLevel::Medium,
        recommend_always_allow: false,
    },
    ToolCatalogEntry {
        tool_name: "palyra.browser.network_log",
        title: "Browser network log",
        description: "Reads captured browser network activity.",
        category: "browser",
        risk_level: ApprovalRiskLevel::Medium,
        recommend_always_allow: true,
    },
    ToolCatalogEntry {
        tool_name: "palyra.browser.console_log",
        title: "Browser console log",
        description: "Reads browser console output for diagnostics.",
        category: "browser",
        risk_level: ApprovalRiskLevel::Medium,
        recommend_always_allow: true,
    },
    ToolCatalogEntry {
        tool_name: "palyra.browser.reset_state",
        title: "Browser reset state",
        description: "Clears cookies, storage, and session state in the browser broker.",
        category: "browser",
        risk_level: ApprovalRiskLevel::High,
        recommend_always_allow: false,
    },
    ToolCatalogEntry {
        tool_name: "palyra.browser.tabs.list",
        title: "Browser tabs list",
        description: "Lists open browser tabs in the active session.",
        category: "browser",
        risk_level: ApprovalRiskLevel::Low,
        recommend_always_allow: true,
    },
    ToolCatalogEntry {
        tool_name: "palyra.browser.tabs.open",
        title: "Browser open tab",
        description: "Opens a new browser tab through the relay broker.",
        category: "browser",
        risk_level: ApprovalRiskLevel::Medium,
        recommend_always_allow: false,
    },
    ToolCatalogEntry {
        tool_name: "palyra.browser.tabs.switch",
        title: "Browser switch tab",
        description: "Switches focus to another browser tab.",
        category: "browser",
        risk_level: ApprovalRiskLevel::Low,
        recommend_always_allow: true,
    },
    ToolCatalogEntry {
        tool_name: "palyra.browser.tabs.close",
        title: "Browser close tab",
        description: "Closes a browser tab in the active session.",
        category: "browser",
        risk_level: ApprovalRiskLevel::Medium,
        recommend_always_allow: false,
    },
    ToolCatalogEntry {
        tool_name: "palyra.browser.permissions.get",
        title: "Browser permissions get",
        description: "Reads browser permission state for the active session.",
        category: "browser",
        risk_level: ApprovalRiskLevel::Low,
        recommend_always_allow: true,
    },
    ToolCatalogEntry {
        tool_name: "palyra.browser.permissions.set",
        title: "Browser permissions set",
        description: "Changes browser permission state in the active session.",
        category: "browser",
        risk_level: ApprovalRiskLevel::High,
        recommend_always_allow: false,
    },
    ToolCatalogEntry {
        tool_name: "palyra.plugin.run",
        title: "Plugin runner",
        description: "Executes a verified skill or inline plugin module inside the WASM runtime.",
        category: "plugins",
        risk_level: ApprovalRiskLevel::Critical,
        recommend_always_allow: false,
    },
];

const PRESET_CONSERVATIVE_CODING: &[ToolPosturePresetAssignment] = &[
    ToolPosturePresetAssignment {
        tool_name: "palyra.memory.search",
        state: ToolPostureState::AlwaysAllow,
    },
    ToolPosturePresetAssignment {
        tool_name: "palyra.memory.recall",
        state: ToolPostureState::AskEachTime,
    },
    ToolPosturePresetAssignment {
        tool_name: "palyra.process.run",
        state: ToolPostureState::AskEachTime,
    },
    ToolPosturePresetAssignment {
        tool_name: "palyra.fs.apply_patch",
        state: ToolPostureState::AskEachTime,
    },
    ToolPosturePresetAssignment {
        tool_name: "palyra.http.fetch",
        state: ToolPostureState::AskEachTime,
    },
    ToolPosturePresetAssignment {
        tool_name: "palyra.web.search",
        state: ToolPostureState::AskEachTime,
    },
    ToolPosturePresetAssignment {
        tool_name: "palyra.browser.navigate",
        state: ToolPostureState::Disabled,
    },
    ToolPosturePresetAssignment {
        tool_name: "palyra.browser.click",
        state: ToolPostureState::Disabled,
    },
    ToolPosturePresetAssignment {
        tool_name: "palyra.browser.type",
        state: ToolPostureState::Disabled,
    },
];

const PRESET_BROWSER_ASSIST: &[ToolPosturePresetAssignment] = &[
    ToolPosturePresetAssignment {
        tool_name: "palyra.memory.search",
        state: ToolPostureState::AlwaysAllow,
    },
    ToolPosturePresetAssignment {
        tool_name: "palyra.memory.recall",
        state: ToolPostureState::AskEachTime,
    },
    ToolPosturePresetAssignment {
        tool_name: "palyra.browser.title",
        state: ToolPostureState::AlwaysAllow,
    },
    ToolPosturePresetAssignment {
        tool_name: "palyra.browser.observe",
        state: ToolPostureState::AlwaysAllow,
    },
    ToolPosturePresetAssignment {
        tool_name: "palyra.browser.viewport",
        state: ToolPostureState::AlwaysAllow,
    },
    ToolPosturePresetAssignment {
        tool_name: "palyra.browser.tabs.list",
        state: ToolPostureState::AlwaysAllow,
    },
    ToolPosturePresetAssignment {
        tool_name: "palyra.browser.navigate",
        state: ToolPostureState::AskEachTime,
    },
    ToolPosturePresetAssignment {
        tool_name: "palyra.browser.click",
        state: ToolPostureState::AskEachTime,
    },
    ToolPosturePresetAssignment {
        tool_name: "palyra.browser.type",
        state: ToolPostureState::AskEachTime,
    },
];

const PRESET_READ_MOSTLY_RESEARCH: &[ToolPosturePresetAssignment] = &[
    ToolPosturePresetAssignment {
        tool_name: "palyra.memory.search",
        state: ToolPostureState::AlwaysAllow,
    },
    ToolPosturePresetAssignment {
        tool_name: "palyra.memory.recall",
        state: ToolPostureState::AskEachTime,
    },
    ToolPosturePresetAssignment {
        tool_name: "palyra.http.fetch",
        state: ToolPostureState::AskEachTime,
    },
    ToolPosturePresetAssignment {
        tool_name: "palyra.web.search",
        state: ToolPostureState::AskEachTime,
    },
    ToolPosturePresetAssignment {
        tool_name: "palyra.browser.title",
        state: ToolPostureState::AlwaysAllow,
    },
    ToolPosturePresetAssignment {
        tool_name: "palyra.browser.observe",
        state: ToolPostureState::AlwaysAllow,
    },
    ToolPosturePresetAssignment {
        tool_name: "palyra.browser.viewport",
        state: ToolPostureState::AlwaysAllow,
    },
    ToolPosturePresetAssignment {
        tool_name: "palyra.browser.screenshot",
        state: ToolPostureState::AskEachTime,
    },
    ToolPosturePresetAssignment {
        tool_name: "palyra.process.run",
        state: ToolPostureState::Disabled,
    },
    ToolPosturePresetAssignment {
        tool_name: "palyra.fs.apply_patch",
        state: ToolPostureState::Disabled,
    },
];

const PRESET_AUTOMATION_REVIEW: &[ToolPosturePresetAssignment] = &[
    ToolPosturePresetAssignment {
        tool_name: "palyra.memory.search",
        state: ToolPostureState::AlwaysAllow,
    },
    ToolPosturePresetAssignment {
        tool_name: "palyra.memory.recall",
        state: ToolPostureState::AskEachTime,
    },
    ToolPosturePresetAssignment {
        tool_name: "palyra.http.fetch",
        state: ToolPostureState::AskEachTime,
    },
    ToolPosturePresetAssignment {
        tool_name: "palyra.process.run",
        state: ToolPostureState::AskEachTime,
    },
    ToolPosturePresetAssignment {
        tool_name: "palyra.fs.apply_patch",
        state: ToolPostureState::AskEachTime,
    },
    ToolPosturePresetAssignment {
        tool_name: "palyra.browser.navigate",
        state: ToolPostureState::AskEachTime,
    },
    ToolPosturePresetAssignment {
        tool_name: "palyra.browser.viewport",
        state: ToolPostureState::AlwaysAllow,
    },
    ToolPosturePresetAssignment {
        tool_name: "palyra.browser.click",
        state: ToolPostureState::AskEachTime,
    },
    ToolPosturePresetAssignment {
        tool_name: "palyra.browser.type",
        state: ToolPostureState::AskEachTime,
    },
];

/// Built-in posture presets. Every preset deliberately keeps
/// `palyra.memory.recall` at ask-each-time (pinned by tests).
pub const TOOL_POSTURE_PRESETS: &[ToolPosturePresetDefinition] = &[
    ToolPosturePresetDefinition {
        preset_id: "conservative_coding",
        label: "Conservative coding",
        description:
            "Keeps code execution and broad recall guarded while leaving memory search open.",
        assignments: PRESET_CONSERVATIVE_CODING,
    },
    ToolPosturePresetDefinition {
        preset_id: "browser_assist",
        label: "Browser assist",
        description:
            "Opens read-mostly browser affordances while keeping mutating actions reviewed.",
        assignments: PRESET_BROWSER_ASSIST,
    },
    ToolPosturePresetDefinition {
        preset_id: "read_mostly_research",
        label: "Read-mostly research",
        description:
            "Optimizes for memory search and browser observation while leaving edits disabled.",
        assignments: PRESET_READ_MOSTLY_RESEARCH,
    },
    ToolPosturePresetDefinition {
        preset_id: "automation_review",
        label: "Automation review",
        description:
            "Keeps broad automation capability available, but always under explicit review.",
        assignments: PRESET_AUTOMATION_REVIEW,
    },
];

/// Looks up the catalog entry for `tool_name`, if it is a known tool.
#[must_use]
pub fn tool_catalog_entry(tool_name: &str) -> Option<&'static ToolCatalogEntry> {
    TOOL_CATALOG.iter().find(|entry| entry.tool_name == tool_name)
}

/// Looks up a built-in preset by id.
#[must_use]
pub fn tool_posture_preset(preset_id: &str) -> Option<&'static ToolPosturePresetDefinition> {
    TOOL_POSTURE_PRESETS.iter().find(|preset| preset.preset_id == preset_id)
}

/// Builds the deterministic recommendation id for one tool/scope pair so
/// repeated analytics runs converge on the same recommendation record.
#[must_use]
pub fn tool_recommendation_id(
    scope_kind: ToolPostureScopeKind,
    scope_id: &str,
    tool_name: &str,
) -> String {
    format!("tool-posture:{}:{}:{}:always_allow", scope_kind.as_str(), scope_id, tool_name)
}

/// Canonicalizes a scope id: global scopes always map to `"global"`, every
/// other scope kind requires a non-empty id.
///
/// # Errors
/// Returns [`ToolPostureRegistryError::MissingScopeId`] when a non-global
/// scope id is absent or blank.
pub fn normalize_scope_id(
    scope_kind: ToolPostureScopeKind,
    scope_id: Option<&str>,
) -> Result<String, ToolPostureRegistryError> {
    match scope_kind {
        ToolPostureScopeKind::Global => Ok(GLOBAL_SCOPE_ID.to_owned()),
        ToolPostureScopeKind::Workspace
        | ToolPostureScopeKind::Agent
        | ToolPostureScopeKind::Session => {
            let Some(scope_id) = scope_id.map(str::trim).filter(|value| !value.is_empty()) else {
                return Err(ToolPostureRegistryError::MissingScopeId);
            };
            Ok(scope_id.to_owned())
        }
    }
}

/// Derives the built-in posture for a tool when no override exists.
///
/// Runtime locks remain fail-closed. Every unlocked, allowlisted tool is
/// usable without approval until an operator applies an `AskEachTime` or
/// `Disabled` override.
#[must_use]
pub fn default_tool_posture_state(
    config: &GatewayRuntimeConfigSnapshot,
    tool_name: &str,
) -> ToolPostureState {
    if tool_lock_reason(config, tool_name).is_some() {
        return ToolPostureState::Disabled;
    }
    ToolPostureState::default()
}

/// Returns the operator-facing reason a tool cannot be enabled at all under
/// the current daemon configuration, or `None` when it is editable.
///
/// A lock wins over any override: not allowlisted, runner/relay disabled by
/// runtime config, or a required capability backend unavailable.
#[must_use]
pub fn tool_lock_reason(config: &GatewayRuntimeConfigSnapshot, tool_name: &str) -> Option<String> {
    let metadata = tool_protocol::tool_metadata(tool_name)?;
    if !config.tool_call.allowed_tools.iter().any(|allowed| allowed.eq_ignore_ascii_case(tool_name))
    {
        return Some("Tool is not present in the daemon allowlist.".to_owned());
    }
    if tool_name == "palyra.process.run" && !config.tool_call.process_runner.enabled {
        return Some("Process runner is disabled in runtime configuration.".to_owned());
    }
    if tool_name == "palyra.plugin.run" && !config.tool_call.wasm_runtime.enabled {
        return Some("WASM plugin runtime is disabled in runtime configuration.".to_owned());
    }
    if tool_name.starts_with("palyra.browser.") && !config.browser_service.enabled {
        return Some("Browser relay is disabled in runtime configuration.".to_owned());
    }
    // Only ProcessExec currently depends on runtime state; the other
    // capabilities are always served by the gateway, so they stay `true`.
    let runtime_available = metadata.capabilities.iter().all(|capability| match capability {
        ToolCapability::ProcessExec => config.tool_call.process_runner.enabled,
        ToolCapability::Network => true,
        ToolCapability::SecretsRead => true,
        ToolCapability::FilesystemRead => true,
        ToolCapability::FilesystemWrite => true,
        ToolCapability::ArtifactsRead => true,
    });
    if !runtime_available {
        return Some("Tool runtime is unavailable for the current daemon posture.".to_owned());
    }
    None
}

/// Builds the narrow-to-broad scope chain used for posture resolution.
///
/// Only a session-scoped request inherits from agent and workspace; agent,
/// workspace, and global requests resolve against themselves plus the global
/// fallback, which is always appended last when not already present.
#[must_use]
pub fn derive_scope_chain(
    active_scope: ToolPostureScopeRef,
    workspace_scope: Option<ToolPostureScopeRef>,
    agent_scope: Option<ToolPostureScopeRef>,
) -> Vec<ToolPostureScopeRef> {
    let mut scopes = Vec::new();
    match active_scope.kind {
        ToolPostureScopeKind::Session => {
            scopes.push(active_scope);
            if let Some(agent_scope) = agent_scope {
                scopes.push(agent_scope);
            }
            if let Some(workspace_scope) = workspace_scope {
                scopes.push(workspace_scope);
            }
        }
        ToolPostureScopeKind::Agent
        | ToolPostureScopeKind::Workspace
        | ToolPostureScopeKind::Global => {
            scopes.push(active_scope);
        }
    }
    if scopes.iter().all(|scope| {
        scope.kind != ToolPostureScopeKind::Global || scope.scope_id != GLOBAL_SCOPE_ID
    }) {
        scopes.push(ToolPostureScopeRef {
            kind: ToolPostureScopeKind::Global,
            scope_id: GLOBAL_SCOPE_ID.to_owned(),
            label: "Global default".to_owned(),
        });
    }
    scopes
}

/// Resolves the effective posture of one tool against a scope chain.
///
/// The first scope in `scope_chain` (narrowest) with a matching override
/// wins; without any override the built-in default applies. A runtime lock
/// reason forces the effective state to `Disabled` regardless of overrides
/// and marks the posture non-editable.
#[must_use]
pub fn evaluate_effective_tool_posture(
    config: &GatewayRuntimeConfigSnapshot,
    overrides: &[ToolPostureOverrideRecord],
    scope_chain: &[ToolPostureScopeRef],
    tool_name: &str,
) -> EffectiveToolPosture {
    let default_state = default_tool_posture_state(config, tool_name);
    let lock_reason = tool_lock_reason(config, tool_name);
    let mut chain = Vec::new();
    let mut effective_state = default_state;
    let mut source_scope = scope_chain.last().cloned().unwrap_or(ToolPostureScopeRef {
        kind: ToolPostureScopeKind::Global,
        scope_id: GLOBAL_SCOPE_ID.to_owned(),
        label: "Global default".to_owned(),
    });
    let mut found_override = false;

    for scope in scope_chain {
        let override_record = overrides.iter().find(|record| {
            record.tool_name == tool_name
                && record.scope_kind == scope.kind
                && record.scope_id == scope.scope_id
        });
        chain.push(ToolPostureChainEntry {
            kind: scope.kind,
            scope_id: scope.scope_id.clone(),
            label: scope.label.clone(),
            state: override_record.map(|record| record.state),
            source: override_record.map(|record| record.source.clone()),
        });
        if !found_override {
            if let Some(override_record) = override_record {
                effective_state = override_record.state;
                source_scope = scope.clone();
                found_override = true;
            }
        }
    }

    if !found_override {
        // Make the implicit default visible in the chain so the UI can show
        // where the effective state came from.
        chain.push(ToolPostureChainEntry {
            kind: ToolPostureScopeKind::Global,
            scope_id: GLOBAL_SCOPE_ID.to_owned(),
            label: "Built-in default".to_owned(),
            state: Some(default_state),
            source: Some("default".to_owned()),
        });
    }

    // Runtime locks are not overridable: a disabled runner/relay must win
    // over any persisted always-allow override.
    if lock_reason.is_some() {
        effective_state = ToolPostureState::Disabled;
    }

    EffectiveToolPosture {
        effective_state,
        default_state,
        approval_mode: effective_state.approval_mode_label().to_owned(),
        source_scope_kind: source_scope.kind,
        source_scope_id: source_scope.scope_id,
        source_scope_label: source_scope.label,
        chain,
        lock_reason,
        editable: tool_lock_reason(config, tool_name).is_none(),
    }
}

/// Aggregates approval friction counters for one tool from journal approval
/// records (the caller pre-filters to the 14-day analytics window).
#[must_use]
pub fn build_tool_friction_metrics(
    approvals: &[ApprovalRecord],
    tool_name: &str,
) -> ToolFrictionMetrics {
    let mut metrics = ToolFrictionMetrics::default();
    let mut session_ids = std::collections::BTreeSet::new();
    for approval in approvals {
        if !tool_approval_subject_matches_tool(approval.subject_id.as_str(), tool_name) {
            continue;
        }
        metrics.requested_14d += 1;
        match approval.decision {
            Some(ApprovalDecision::Allow) => metrics.approved_14d += 1,
            Some(ApprovalDecision::Deny) => metrics.denied_14d += 1,
            None => metrics.pending_14d += 1,
            _ => {}
        }
        session_ids.insert(approval.session_id.clone());
    }
    metrics.unique_sessions_14d = session_ids.len() as u64;
    metrics
}

/// Builds an always-allow recommendation when the friction data supports it,
/// or `None` otherwise.
#[must_use]
pub fn build_tool_recommendation(
    tool_name: &str,
    catalog: &ToolCatalogEntry,
    scope: &ToolPostureScopeRef,
    posture: &EffectiveToolPosture,
    metrics: &ToolFrictionMetrics,
    action: Option<ToolPostureRecommendationAction>,
) -> Option<ToolPostureRecommendation> {
    // Recommend only when all gates hold: the tool currently asks each time,
    // the catalog marks it safe to promote, approvals cleared the volume
    // threshold, and there was not a single deny in the window.
    if posture.effective_state != ToolPostureState::AskEachTime
        || !catalog.recommend_always_allow
        || metrics.approved_14d < TOOL_POSTURE_RECOMMENDATION_MIN_APPROVALS
        || metrics.denied_14d > 0
    {
        return None;
    }
    Some(ToolPostureRecommendation {
        recommendation_id: tool_recommendation_id(scope.kind, scope.scope_id.as_str(), tool_name),
        tool_name: tool_name.to_owned(),
        scope_kind: scope.kind,
        scope_id: scope.scope_id.clone(),
        current_state: posture.effective_state,
        recommended_state: ToolPostureState::AlwaysAllow,
        reason: format!(
            "This tool was approved {} times in the last 14 days across {} session{} without a deny. Consider ask-each-time -> always-allow for {}.",
            metrics.approved_14d,
            metrics.unique_sessions_14d,
            if metrics.unique_sessions_14d == 1 { "" } else { "s" },
            scope.label
        ),
        approvals_14d: metrics.approved_14d,
        action,
    })
}

/// Returns up to `limit` approval records for one tool, preserving the
/// caller's ordering (the journal supplies newest-first).
#[must_use]
pub fn recent_tool_approvals<'a>(
    approvals: &'a [ApprovalRecord],
    tool_name: &str,
    limit: usize,
) -> Vec<&'a ApprovalRecord> {
    approvals
        .iter()
        .filter(|approval| {
            tool_approval_subject_matches_tool(approval.subject_id.as_str(), tool_name)
        })
        .take(limit)
        .collect()
}

fn tool_approval_subject_matches_tool(subject_id: &str, tool_name: &str) -> bool {
    let Some(subject_tool_name) = subject_id.strip_prefix("tool:") else {
        return false;
    };
    subject_tool_name == tool_name
        || subject_tool_name
            .strip_prefix(tool_name)
            .is_some_and(|remainder| remainder.starts_with('|'))
}

// An empty or new file is initialized with an empty current-version document
// so later writes never have to special-case first use.
fn load_registry_document(
    path: &Path,
) -> Result<ToolPostureRegistryDocument, ToolPostureRegistryError> {
    let buffer = match fs::read_to_string(path) {
        Ok(buffer) => buffer,
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => {
            let document = empty_registry_document();
            write_registry_document(path, &document)?;
            return Ok(document);
        }
        Err(source) => {
            return Err(ToolPostureRegistryError::ReadFile { path: path.to_path_buf(), source });
        }
    };
    if buffer.trim().is_empty() {
        let document = empty_registry_document();
        write_registry_document(path, &document)?;
        return Ok(document);
    }
    serde_json::from_str::<ToolPostureRegistryDocument>(&buffer)
        .map_err(|source| ToolPostureRegistryError::ParseFile { path: path.to_path_buf(), source })
}

fn persist_registry_document(
    path: &Path,
    document: &ToolPostureRegistryDocument,
) -> Result<(), ToolPostureRegistryError> {
    write_registry_document(path, document)
}

fn write_registry_document(
    path: &Path,
    document: &ToolPostureRegistryDocument,
) -> Result<(), ToolPostureRegistryError> {
    let mut serialized = serde_json::to_vec_pretty(document).map_err(|source| {
        ToolPostureRegistryError::SerializeFile { path: path.to_path_buf(), source }
    })?;
    serialized.push(b'\n');
    write_registry_payload_atomically(path, serialized.as_slice())
}

fn empty_registry_document() -> ToolPostureRegistryDocument {
    ToolPostureRegistryDocument {
        schema_version: TOOL_POSTURE_SCHEMA_VERSION,
        overrides: vec![],
        recommendation_actions: vec![],
        audit_events: vec![],
    }
}

fn write_registry_payload_atomically(
    path: &Path,
    payload: &[u8],
) -> Result<(), ToolPostureRegistryError> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|source| ToolPostureRegistryError::WriteFile {
            path: parent.to_path_buf(),
            source,
        })?;
    }
    let temp_path = registry_sidecar_path(path, "tmp");
    let result = (|| -> Result<(), std::io::Error> {
        let mut temp_file =
            fs::File::options().write(true).create_new(true).open(temp_path.as_path())?;
        temp_file.write_all(payload)?;
        temp_file.sync_all()?;
        drop(temp_file);
        replace_registry_file(temp_path.as_path(), path)
    })();
    if result.is_err() {
        let _ = fs::remove_file(temp_path.as_path());
    }
    result
        .map_err(|source| ToolPostureRegistryError::WriteFile { path: path.to_path_buf(), source })
}

fn replace_registry_file(temp_path: &Path, path: &Path) -> Result<(), std::io::Error> {
    if let Err(rename_error) = fs::rename(temp_path, path) {
        if !path.exists() || !path.is_file() {
            return Err(rename_error);
        }
        let rollback_path = registry_sidecar_path(path, "swap");
        fs::rename(path, rollback_path.as_path())?;
        if let Err(install_error) = fs::rename(temp_path, path) {
            let _ = fs::rename(rollback_path.as_path(), path);
            return Err(install_error);
        }
        let _ = fs::remove_file(rollback_path);
    }
    Ok(())
}

fn registry_sidecar_path(path: &Path, kind: &str) -> PathBuf {
    let mut sidecar_name = path.as_os_str().to_os_string();
    sidecar_name.push(format!(".{kind}.{}.{}", std::process::id(), Ulid::generate()));
    PathBuf::from(sidecar_name)
}

fn prune_expired_entries(document: &mut ToolPostureRegistryDocument, now_unix_ms: i64) -> bool {
    let before = document.overrides.len();
    document.overrides.retain(|record| {
        record.expires_at_unix_ms.is_none_or(|expires_at_unix_ms| expires_at_unix_ms > now_unix_ms)
    });
    document.overrides.len() != before
}

fn validate_known_tool(tool_name: &str) -> Result<(), ToolPostureRegistryError> {
    if tool_catalog_entry(tool_name).is_some() {
        Ok(())
    } else {
        Err(ToolPostureRegistryError::UnknownTool)
    }
}

fn normalize_optional_text(value: Option<String>) -> Option<String> {
    value.map(|value| value.trim().to_owned()).filter(|value| !value.is_empty())
}

// Keeps the audit trail newest-first and bounded at 2000 entries so the
// registry document cannot grow without limit.
fn append_audit_event(
    audit_events: &mut Vec<ToolPostureAuditEventRecord>,
    event: ToolPostureAuditEventRecord,
) {
    audit_events.push(event);
    audit_events.sort_by(|left, right| {
        right
            .created_at_unix_ms
            .cmp(&left.created_at_unix_ms)
            .then_with(|| right.audit_id.cmp(&left.audit_id))
    });
    if audit_events.len() > 2_000 {
        audit_events.truncate(2_000);
    }
}

fn recommendation_action_to_audit_action(
    action: ToolPostureRecommendationAction,
) -> ToolPostureAuditAction {
    match action {
        ToolPostureRecommendationAction::Accepted => ToolPostureAuditAction::RecommendationAccepted,
        ToolPostureRecommendationAction::Dismissed => {
            ToolPostureAuditAction::RecommendationDismissed
        }
        ToolPostureRecommendationAction::Deferred => ToolPostureAuditAction::RecommendationDeferred,
    }
}

// Override sources written by preset application use the "preset:<id>" form;
// the id is lifted into the audit event for filtering.
fn extract_preset_id(source: &str) -> Option<String> {
    source
        .strip_prefix("preset:")
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::journal::{
        ApprovalDecisionScope, ApprovalPolicySnapshot, ApprovalPromptRecord, ApprovalSubjectType,
    };

    struct TempStateRoot {
        path: PathBuf,
    }

    impl TempStateRoot {
        fn new() -> Self {
            let path =
                std::env::temp_dir().join(format!("palyra-tool-posture-test-{}", Ulid::generate()));
            fs::create_dir_all(&path).expect("temp state root should be created");
            Self { path }
        }
    }

    impl Drop for TempStateRoot {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.path);
        }
    }

    fn approval_record(
        subject_id: &str,
        session_id: &str,
        decision: Option<ApprovalDecision>,
    ) -> ApprovalRecord {
        ApprovalRecord {
            approval_id: format!("approval-{}", Ulid::generate()),
            session_id: session_id.to_owned(),
            run_id: "run-1".to_owned(),
            principal: "operator".to_owned(),
            device_id: "device-1".to_owned(),
            channel: None,
            requested_at_unix_ms: 1_000,
            resolved_at_unix_ms: decision.map(|_| 1_100),
            subject_type: ApprovalSubjectType::Tool,
            subject_id: subject_id.to_owned(),
            request_summary: "test approval".to_owned(),
            decision,
            decision_scope: decision.map(|_| ApprovalDecisionScope::Once),
            decision_reason: None,
            decision_scope_ttl_ms: None,
            policy_snapshot: ApprovalPolicySnapshot {
                policy_id: "test-policy".to_owned(),
                policy_hash: "hash".to_owned(),
                evaluation_summary: "test".to_owned(),
            },
            prompt: ApprovalPromptRecord {
                title: "Approve tool".to_owned(),
                risk_level: ApprovalRiskLevel::Low,
                subject_id: subject_id.to_owned(),
                summary: "test approval".to_owned(),
                options: vec![],
                timeout_seconds: 30,
                details_json: "{}".to_owned(),
                policy_explanation: "test".to_owned(),
            },
            created_at_unix_ms: 1_000,
            updated_at_unix_ms: 1_100,
        }
    }

    #[test]
    fn unlocked_tool_posture_defaults_to_no_approval() {
        assert_eq!(ToolPostureState::default(), ToolPostureState::AlwaysAllow);
    }

    fn registry_path(temp: &TempStateRoot) -> PathBuf {
        temp.path.join(TOOL_POSTURE_DIRECTORY).join(TOOL_POSTURE_REGISTRY_FILE)
    }

    fn registry_sidecars(path: &Path) -> Vec<PathBuf> {
        let Some(parent) = path.parent() else {
            return Vec::new();
        };
        fs::read_dir(parent)
            .expect("registry directory should be readable")
            .filter_map(Result::ok)
            .map(|entry| entry.path())
            .filter(|path| {
                path.file_name()
                    .and_then(|name| name.to_str())
                    .is_some_and(|name| name.contains(".tmp.") || name.contains(".swap."))
            })
            .collect()
    }

    #[test]
    fn registry_records_override_and_clear_audit_events() {
        let temp = TempStateRoot::new();
        let registry = ToolPostureRegistry::open(&temp.path).expect("registry should open");

        registry
            .upsert_override(ToolPostureOverrideUpsertRequest {
                tool_name: "palyra.browser.title".to_owned(),
                scope_kind: ToolPostureScopeKind::Global,
                scope_id: "global".to_owned(),
                state: ToolPostureState::AlwaysAllow,
                reason: Some("frequently used".to_owned()),
                actor_principal: "operator".to_owned(),
                source: "manual".to_owned(),
                expires_at_unix_ms: None,
                now_unix_ms: 1_000,
            })
            .expect("override should persist");

        registry
            .clear_override(ToolPostureOverrideClearRequest {
                tool_name: "palyra.browser.title".to_owned(),
                scope_kind: ToolPostureScopeKind::Global,
                scope_id: "global".to_owned(),
                actor_principal: "operator".to_owned(),
                source: "manual_reset".to_owned(),
                reason: Some("reverting".to_owned()),
                now_unix_ms: 2_000,
            })
            .expect("override clear should persist");

        let audit_events = registry.list_audit_events().expect("audit events should load");
        assert_eq!(audit_events.len(), 2);
        assert_eq!(audit_events[0].action, ToolPostureAuditAction::OverrideCleared);
        assert_eq!(audit_events[0].previous_state, Some(ToolPostureState::AlwaysAllow));
        assert_eq!(audit_events[1].action, ToolPostureAuditAction::OverrideSet);
        assert_eq!(audit_events[1].new_state, Some(ToolPostureState::AlwaysAllow));
    }

    #[test]
    fn registry_can_reset_scope_and_remove_multiple_overrides() {
        let temp = TempStateRoot::new();
        let registry = ToolPostureRegistry::open(&temp.path).expect("registry should open");

        for tool_name in ["palyra.browser.title", "palyra.browser.observe"] {
            registry
                .upsert_override(ToolPostureOverrideUpsertRequest {
                    tool_name: tool_name.to_owned(),
                    scope_kind: ToolPostureScopeKind::Session,
                    scope_id: "session-1".to_owned(),
                    state: ToolPostureState::AlwaysAllow,
                    reason: None,
                    actor_principal: "operator".to_owned(),
                    source: "manual".to_owned(),
                    expires_at_unix_ms: None,
                    now_unix_ms: 1_000,
                })
                .expect("override should persist");
        }

        let removed = registry
            .reset_scope(ToolPostureScopeResetRequest {
                scope_kind: ToolPostureScopeKind::Session,
                scope_id: "session-1".to_owned(),
                actor_principal: "operator".to_owned(),
                source: "manual_scope_reset".to_owned(),
                reason: Some("session cleanup".to_owned()),
                now_unix_ms: 2_000,
            })
            .expect("scope reset should persist");

        assert_eq!(removed.len(), 2);
        assert!(
            registry
                .list_overrides()
                .expect("overrides should load")
                .into_iter()
                .all(|record| record.scope_id != "session-1"),
            "session scope overrides should be removed"
        );
    }

    #[test]
    fn registry_persists_with_replaceable_sidecar_files() {
        let temp = TempStateRoot::new();
        let path = registry_path(&temp);
        let registry = ToolPostureRegistry::open(&temp.path).expect("registry should open");

        registry
            .upsert_override(ToolPostureOverrideUpsertRequest {
                tool_name: "palyra.browser.title".to_owned(),
                scope_kind: ToolPostureScopeKind::Global,
                scope_id: "global".to_owned(),
                state: ToolPostureState::AlwaysAllow,
                reason: None,
                actor_principal: "operator".to_owned(),
                source: "manual".to_owned(),
                expires_at_unix_ms: None,
                now_unix_ms: 1_000,
            })
            .expect("override should persist");

        let raw = fs::read_to_string(path.as_path()).expect("registry should be readable");
        let document = serde_json::from_str::<ToolPostureRegistryDocument>(raw.as_str())
            .expect("registry should contain valid JSON");
        assert_eq!(document.overrides.len(), 1);
        assert_eq!(registry_sidecars(path.as_path()), Vec::<PathBuf>::new());

        let reopened = ToolPostureRegistry::open(&temp.path).expect("registry should reopen");
        assert_eq!(reopened.list_overrides().expect("overrides should load").len(), 1);
    }

    #[test]
    fn tool_approval_subject_matching_uses_exact_tool_segment() {
        let approvals = vec![
            approval_record("tool:x.run", "session-1", Some(ApprovalDecision::Allow)),
            approval_record("tool:x.runner", "session-2", Some(ApprovalDecision::Deny)),
            approval_record("tool:x.run|skill:demo", "session-3", None),
            approval_record("channel:x.run", "session-4", Some(ApprovalDecision::Allow)),
        ];

        let metrics = build_tool_friction_metrics(approvals.as_slice(), "x.run");
        assert_eq!(metrics.requested_14d, 2);
        assert_eq!(metrics.approved_14d, 1);
        assert_eq!(metrics.denied_14d, 0);
        assert_eq!(metrics.pending_14d, 1);
        assert_eq!(metrics.unique_sessions_14d, 2);

        let recent = recent_tool_approvals(approvals.as_slice(), "x.run", 10);
        assert_eq!(recent.len(), 2);
        assert_eq!(recent[0].subject_id, "tool:x.run");
        assert_eq!(recent[1].subject_id, "tool:x.run|skill:demo");
    }

    #[test]
    fn memory_recall_catalog_and_presets_require_review() {
        let catalog = tool_catalog_entry("palyra.memory.recall").expect("catalog entry exists");
        assert_eq!(catalog.risk_level, ApprovalRiskLevel::High);

        for preset in TOOL_POSTURE_PRESETS {
            let recall_assignment = preset
                .assignments
                .iter()
                .find(|assignment| assignment.tool_name == "palyra.memory.recall")
                .expect("preset should define recall posture");
            assert_eq!(
                recall_assignment.state,
                ToolPostureState::AskEachTime,
                "{} should keep broad memory recall under review",
                preset.preset_id
            );
        }
    }

    #[test]
    fn browser_storage_catalog_does_not_recommend_always_allow() {
        let catalog = tool_catalog_entry("palyra.browser.storage").expect("catalog entry exists");
        assert!(!catalog.recommend_always_allow);
        assert!(catalog.description.contains("withheld"));
    }

    #[test]
    fn web_search_catalog_keeps_network_access_reviewed() {
        let catalog = tool_catalog_entry("palyra.web.search").expect("catalog entry exists");
        assert_eq!(catalog.risk_level, ApprovalRiskLevel::High);
        assert!(!catalog.recommend_always_allow);

        let research = tool_posture_preset("read_mostly_research").expect("preset exists");
        let assignment = research
            .assignments
            .iter()
            .find(|assignment| assignment.tool_name == "palyra.web.search")
            .expect("research preset should define web search posture");
        assert_eq!(assignment.state, ToolPostureState::AskEachTime);
    }
}
