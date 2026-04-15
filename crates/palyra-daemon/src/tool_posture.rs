use std::{
    fs,
    io::{Read, Seek, SeekFrom, Write},
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
pub(crate) const TOOL_POSTURE_ANALYTICS_WINDOW_MS: i64 = 14 * 24 * 60 * 60 * 1_000;
pub(crate) const TOOL_POSTURE_RECOMMENDATION_MIN_APPROVALS: u64 = 5;

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

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash, Default)]
#[serde(rename_all = "snake_case")]
pub enum ToolPostureState {
    AlwaysAllow,
    #[default]
    AskEachTime,
    Disabled,
}

impl ToolPostureState {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::AlwaysAllow => "always_allow",
            Self::AskEachTime => "ask_each_time",
            Self::Disabled => "disabled",
        }
    }

    #[must_use]
    pub fn approval_mode_label(self) -> &'static str {
        match self {
            Self::AlwaysAllow => "no approval",
            Self::AskEachTime => "ask each time",
            Self::Disabled => "disabled",
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum ToolPostureScopeKind {
    Global,
    Workspace,
    Agent,
    Session,
}

impl ToolPostureScopeKind {
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

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ToolPostureRecommendationAction {
    Accepted,
    Dismissed,
    Deferred,
}

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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ToolPostureRecommendationActionRecord {
    pub recommendation_id: String,
    pub scope_kind: ToolPostureScopeKind,
    pub scope_id: String,
    pub action: ToolPostureRecommendationAction,
    pub actor_principal: String,
    pub created_at_unix_ms: i64,
}

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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ToolPostureRecommendationActionRequest {
    pub recommendation_id: String,
    pub scope_kind: ToolPostureScopeKind,
    pub scope_id: String,
    pub action: ToolPostureRecommendationAction,
    pub actor_principal: String,
    pub now_unix_ms: i64,
}

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

pub struct ToolPostureRegistry {
    document_path: PathBuf,
    file: Mutex<fs::File>,
    document: Mutex<ToolPostureRegistryDocument>,
}

impl ToolPostureRegistry {
    pub fn open(state_root: &Path) -> Result<Self, ToolPostureRegistryError> {
        let tool_posture_root = state_root.join(TOOL_POSTURE_DIRECTORY);
        fs::create_dir_all(&tool_posture_root).map_err(|source| {
            ToolPostureRegistryError::CreateDirectory { path: tool_posture_root.clone(), source }
        })?;
        let document_path = tool_posture_root.join(TOOL_POSTURE_REGISTRY_FILE);
        let mut file = fs::File::options()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(&document_path)
            .map_err(|source| ToolPostureRegistryError::OpenFile {
                path: document_path.clone(),
                source,
            })?;
        let document = load_registry_document(&document_path, &mut file)?;
        Ok(Self { document_path, file: Mutex::new(file), document: Mutex::new(document) })
    }

    pub fn list_overrides(
        &self,
    ) -> Result<Vec<ToolPostureOverrideRecord>, ToolPostureRegistryError> {
        let now_unix_ms = current_unix_ms();
        let mut document =
            self.document.lock().map_err(|_| ToolPostureRegistryError::LockPoisoned)?;
        prune_expired_entries(&mut document, now_unix_ms);
        persist_registry_document(&self.document_path, &self.file, &document)?;
        Ok(document.overrides.clone())
    }

    pub fn list_recommendation_actions(
        &self,
    ) -> Result<Vec<ToolPostureRecommendationActionRecord>, ToolPostureRegistryError> {
        let document = self.document.lock().map_err(|_| ToolPostureRegistryError::LockPoisoned)?;
        Ok(document.recommendation_actions.clone())
    }

    pub fn list_audit_events(
        &self,
    ) -> Result<Vec<ToolPostureAuditEventRecord>, ToolPostureRegistryError> {
        let document = self.document.lock().map_err(|_| ToolPostureRegistryError::LockPoisoned)?;
        Ok(document.audit_events.clone())
    }

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
                    audit_id: Ulid::new().to_string(),
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
                    audit_id: Ulid::new().to_string(),
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
        document.overrides.sort_by(|left, right| {
            left.scope_kind
                .as_str()
                .cmp(right.scope_kind.as_str())
                .then_with(|| left.scope_id.cmp(&right.scope_id))
                .then_with(|| left.tool_name.cmp(&right.tool_name))
        });
        persist_registry_document(&self.document_path, &self.file, &document)?;
        Ok(record)
    }

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
                    audit_id: Ulid::new().to_string(),
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
            persist_registry_document(&self.document_path, &self.file, &document)?;
        }
        Ok(removed)
    }

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
                    audit_id: Ulid::new().to_string(),
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
            persist_registry_document(&self.document_path, &self.file, &document)?;
        }
        Ok(removed)
    }

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
                    audit_id: Ulid::new().to_string(),
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
            persist_registry_document(&self.document_path, &self.file, &document)?;
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
                audit_id: Ulid::new().to_string(),
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
        persist_registry_document(&self.document_path, &self.file, &document)?;
        Ok(record)
    }
}

#[derive(Debug, Clone, Copy, Serialize)]
pub struct ToolCatalogEntry {
    pub tool_name: &'static str,
    pub title: &'static str,
    pub description: &'static str,
    pub category: &'static str,
    pub risk_level: ApprovalRiskLevel,
    pub recommend_always_allow: bool,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq, Default)]
pub struct ToolFrictionMetrics {
    pub requested_14d: u64,
    pub approved_14d: u64,
    pub denied_14d: u64,
    pub pending_14d: u64,
    pub unique_sessions_14d: u64,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct ToolPostureScopeRef {
    pub kind: ToolPostureScopeKind,
    pub scope_id: String,
    pub label: String,
}

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

#[derive(Debug, Clone, Copy, Serialize)]
pub struct ToolPosturePresetAssignment {
    pub tool_name: &'static str,
    pub state: ToolPostureState,
}

#[derive(Debug, Clone, Copy, Serialize)]
pub struct ToolPosturePresetDefinition {
    pub preset_id: &'static str,
    pub label: &'static str,
    pub description: &'static str,
    pub assignments: &'static [ToolPosturePresetAssignment],
}

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
        tool_name: "palyra.http.fetch",
        title: "HTTP fetch",
        description: "Makes outbound HTTP requests through the gateway fetch broker.",
        category: "network",
        risk_level: ApprovalRiskLevel::High,
        recommend_always_allow: false,
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
        tool_name: "palyra.fs.apply_patch",
        title: "Workspace patch",
        description: "Applies attested file patches inside the resolved workspace boundary.",
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
        tool_name: "palyra.browser.title",
        state: ToolPostureState::AlwaysAllow,
    },
    ToolPosturePresetAssignment {
        tool_name: "palyra.browser.observe",
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
        tool_name: "palyra.http.fetch",
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
        tool_name: "palyra.browser.click",
        state: ToolPostureState::AskEachTime,
    },
    ToolPosturePresetAssignment {
        tool_name: "palyra.browser.type",
        state: ToolPostureState::AskEachTime,
    },
];

pub const TOOL_POSTURE_PRESETS: &[ToolPosturePresetDefinition] = &[
    ToolPosturePresetDefinition {
        preset_id: "conservative_coding",
        label: "Conservative coding",
        description: "Keeps code execution guarded while leaving low-risk recall open.",
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
        description: "Optimizes for recall and browser observation while leaving edits disabled.",
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

#[must_use]
pub fn tool_catalog_entry(tool_name: &str) -> Option<&'static ToolCatalogEntry> {
    TOOL_CATALOG.iter().find(|entry| entry.tool_name == tool_name)
}

#[must_use]
pub fn tool_posture_preset(preset_id: &str) -> Option<&'static ToolPosturePresetDefinition> {
    TOOL_POSTURE_PRESETS.iter().find(|preset| preset.preset_id == preset_id)
}

#[must_use]
pub fn tool_recommendation_id(
    scope_kind: ToolPostureScopeKind,
    scope_id: &str,
    tool_name: &str,
) -> String {
    format!("tool-posture:{}:{}:{}:always_allow", scope_kind.as_str(), scope_id, tool_name)
}

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

#[must_use]
pub fn default_tool_posture_state(
    config: &GatewayRuntimeConfigSnapshot,
    tool_name: &str,
) -> ToolPostureState {
    if tool_lock_reason(config, tool_name).is_some() {
        return ToolPostureState::Disabled;
    }
    if tool_protocol::tool_requires_approval(tool_name) {
        ToolPostureState::AskEachTime
    } else {
        ToolPostureState::AlwaysAllow
    }
}

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
    let runtime_available = metadata.capabilities.iter().all(|capability| match capability {
        ToolCapability::ProcessExec => config.tool_call.process_runner.enabled,
        ToolCapability::Network => true,
        ToolCapability::SecretsRead => true,
        ToolCapability::FilesystemWrite => true,
    });
    if !runtime_available {
        return Some("Tool runtime is unavailable for the current daemon posture.".to_owned());
    }
    None
}

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
        chain.push(ToolPostureChainEntry {
            kind: ToolPostureScopeKind::Global,
            scope_id: GLOBAL_SCOPE_ID.to_owned(),
            label: "Built-in default".to_owned(),
            state: Some(default_state),
            source: Some("default".to_owned()),
        });
    }

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

#[must_use]
pub fn build_tool_friction_metrics(
    approvals: &[ApprovalRecord],
    tool_name: &str,
) -> ToolFrictionMetrics {
    let mut metrics = ToolFrictionMetrics::default();
    let mut session_ids = std::collections::BTreeSet::new();
    let prefix = format!("tool:{tool_name}");
    for approval in approvals {
        if !approval.subject_id.starts_with(prefix.as_str()) {
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

#[must_use]
pub fn build_tool_recommendation(
    tool_name: &str,
    catalog: &ToolCatalogEntry,
    scope: &ToolPostureScopeRef,
    posture: &EffectiveToolPosture,
    metrics: &ToolFrictionMetrics,
    action: Option<ToolPostureRecommendationAction>,
) -> Option<ToolPostureRecommendation> {
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

#[must_use]
pub fn recent_tool_approvals<'a>(
    approvals: &'a [ApprovalRecord],
    tool_name: &str,
    limit: usize,
) -> Vec<&'a ApprovalRecord> {
    let prefix = format!("tool:{tool_name}");
    approvals
        .iter()
        .filter(|approval| approval.subject_id.starts_with(prefix.as_str()))
        .take(limit)
        .collect()
}

fn load_registry_document(
    path: &Path,
    file: &mut fs::File,
) -> Result<ToolPostureRegistryDocument, ToolPostureRegistryError> {
    let mut buffer = String::new();
    file.seek(SeekFrom::Start(0)).map_err(|source| ToolPostureRegistryError::ReadFile {
        path: path.to_path_buf(),
        source,
    })?;
    file.read_to_string(&mut buffer).map_err(|source| ToolPostureRegistryError::ReadFile {
        path: path.to_path_buf(),
        source,
    })?;
    if buffer.trim().is_empty() {
        let document = ToolPostureRegistryDocument {
            schema_version: TOOL_POSTURE_SCHEMA_VERSION,
            overrides: vec![],
            recommendation_actions: vec![],
            audit_events: vec![],
        };
        write_registry_document(path, file, &document)?;
        return Ok(document);
    }
    serde_json::from_str::<ToolPostureRegistryDocument>(&buffer)
        .map_err(|source| ToolPostureRegistryError::ParseFile { path: path.to_path_buf(), source })
}

fn persist_registry_document(
    path: &Path,
    file_mutex: &Mutex<fs::File>,
    document: &ToolPostureRegistryDocument,
) -> Result<(), ToolPostureRegistryError> {
    let mut file = file_mutex.lock().map_err(|_| ToolPostureRegistryError::LockPoisoned)?;
    write_registry_document(path, &mut file, document)
}

fn write_registry_document(
    path: &Path,
    file: &mut fs::File,
    document: &ToolPostureRegistryDocument,
) -> Result<(), ToolPostureRegistryError> {
    let serialized = serde_json::to_vec_pretty(document).map_err(|source| {
        ToolPostureRegistryError::SerializeFile { path: path.to_path_buf(), source }
    })?;
    file.seek(SeekFrom::Start(0)).map_err(|source| ToolPostureRegistryError::WriteFile {
        path: path.to_path_buf(),
        source,
    })?;
    file.set_len(0).map_err(|source| ToolPostureRegistryError::WriteFile {
        path: path.to_path_buf(),
        source,
    })?;
    file.write_all(&serialized)
        .and_then(|_| file.write_all(b"\n"))
        .and_then(|_| file.sync_all())
        .map_err(|source| ToolPostureRegistryError::WriteFile { path: path.to_path_buf(), source })
}

fn prune_expired_entries(document: &mut ToolPostureRegistryDocument, now_unix_ms: i64) {
    document.overrides.retain(|record| {
        record.expires_at_unix_ms.is_none_or(|expires_at_unix_ms| expires_at_unix_ms > now_unix_ms)
    });
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

    struct TempStateRoot {
        path: PathBuf,
    }

    impl TempStateRoot {
        fn new() -> Self {
            let path =
                std::env::temp_dir().join(format!("palyra-tool-posture-test-{}", Ulid::new()));
            fs::create_dir_all(&path).expect("temp state root should be created");
            Self { path }
        }
    }

    impl Drop for TempStateRoot {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.path);
        }
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
}
