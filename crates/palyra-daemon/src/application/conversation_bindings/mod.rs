//! Durable mapping from channel conversation scopes to daemon sessions.
//!
//! A binding ties one (channel, conversation, thread, sender, principal)
//! scope to a session, with idle/max-age expiry, conflict detection, and
//! operator-reviewable repair plans. Binding ids are content-addressed
//! (`cb_<sha256>`), so create_or_touch is idempotent per scope+session.
//! The store persists as a single JSON file and is consumed by
//! `route_message` to resolve which session an inbound message belongs to.

use std::{
    collections::{BTreeMap, BTreeSet},
    error::Error,
    fmt, fs,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

use palyra_common::{
    config_system::write_content_with_backups,
    redaction::{redact_auth_error, redact_url_segments_in_text},
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

const BINDING_SCHEMA_VERSION: u32 = 1;
const DEFAULT_IDLE_TIMEOUT_MS: i64 = 8 * 60 * 60 * 1_000;
const DEFAULT_MAX_AGE_MS: i64 = 30 * 24 * 60 * 60 * 1_000;
const MAX_BINDING_COMPONENT_BYTES: usize = 512;
const MAX_BINDING_LIST_LIMIT: usize = 1_000;
// Tiny test cap so prune-at-limit behavior is exercisable without
// creating thousands of records.
#[cfg(not(test))]
const MAX_BINDING_RECORDS: usize = 10_000;
#[cfg(test)]
const MAX_BINDING_RECORDS: usize = 8;
const HASH_SUFFIX_PREFIX: &str = "#sha256:";

/// Kind of conversation binding.
///
/// INTENTIONAL: the derived `Ord` doubles as resolution priority - among
/// the kinds considered by [`ConversationBindingStore::resolve`], later
/// variants win (DelegatedRun > Thread > Main). Do not reorder variants
/// without revisiting the resolver.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConversationBindingKind {
    Main,
    Thread,
    DelegatedRun,
    Acp,
    Routine,
}

impl ConversationBindingKind {
    /// Stable snake_case label used in snapshots and binding ids.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Main => "main",
            Self::Thread => "thread",
            Self::DelegatedRun => "delegated_run",
            Self::Acp => "acp",
            Self::Routine => "routine",
        }
    }
}

/// Lifecycle state of a binding. Only `Active` bindings resolve; the other
/// states are kept for audit until pruning removes them.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConversationBindingLifecycleState {
    Active,
    Detached,
    Expired,
    Stale,
}

impl ConversationBindingLifecycleState {
    /// Stable snake_case label used in snapshots.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Active => "active",
            Self::Detached => "detached",
            Self::Expired => "expired",
            Self::Stale => "stale",
        }
    }

    /// Returns whether the state is `Active`.
    #[must_use]
    pub const fn is_active(self) -> bool {
        matches!(self, Self::Active)
    }
}

/// Expiry policy for a binding; `None` disables the respective limit.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConversationBindingLifecycle {
    pub idle_timeout_ms: Option<i64>,
    pub max_age_ms: Option<i64>,
    pub detach_on_parent_reset: bool,
}

impl Default for ConversationBindingLifecycle {
    fn default() -> Self {
        Self {
            idle_timeout_ms: Some(DEFAULT_IDLE_TIMEOUT_MS),
            max_age_ms: Some(DEFAULT_MAX_AGE_MS),
            detach_on_parent_reset: true,
        }
    }
}

impl ConversationBindingLifecycle {
    /// Computes the effective expiry: the earlier of idle and max-age
    /// deadlines, or `None` when both limits are disabled.
    ///
    /// Negative TTLs are clamped to zero, i.e. immediate expiry rather than
    /// an expiry in the past of the anchor timestamp.
    #[must_use]
    pub fn expires_at(
        &self,
        created_at_unix_ms: i64,
        last_activity_at_unix_ms: i64,
    ) -> Option<i64> {
        let idle_expiry =
            self.idle_timeout_ms.and_then(|ttl| last_activity_at_unix_ms.checked_add(ttl.max(0)));
        let max_age_expiry =
            self.max_age_ms.and_then(|ttl| created_at_unix_ms.checked_add(ttl.max(0)));
        match (idle_expiry, max_age_expiry) {
            (Some(left), Some(right)) => Some(left.min(right)),
            (Some(value), None) | (None, Some(value)) => Some(value),
            (None, None) => None,
        }
    }
}

/// One persisted binding. All identifier components are normalized
/// (trimmed, byte-bounded) before storage; see `bound_component_bytes`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConversationBindingRecord {
    pub schema_version: u32,
    pub binding_id: String,
    pub binding_kind: ConversationBindingKind,
    pub channel: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub conversation_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub thread_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sender_identity: Option<String>,
    pub principal: String,
    pub session_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub workspace_id: Option<String>,
    pub policy_scope: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parent_binding_id: Option<String>,
    pub lifecycle: ConversationBindingLifecycle,
    pub state: ConversationBindingLifecycleState,
    pub created_at_unix_ms: i64,
    pub updated_at_unix_ms: i64,
    pub last_activity_at_unix_ms: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expires_at_unix_ms: Option<i64>,
}

impl ConversationBindingRecord {
    /// Returns whether the binding is active and not past its expiry.
    #[must_use]
    pub fn active(&self, now_unix_ms: i64) -> bool {
        self.state.is_active()
            && self.expires_at_unix_ms.is_none_or(|expires_at| expires_at > now_unix_ms)
    }

    /// Returns the full per-principal scope key used for resolution.
    #[must_use]
    pub fn scope_key(&self) -> ConversationBindingScopeKey {
        ConversationBindingScopeKey {
            binding_kind: self.binding_kind,
            channel: self.channel.clone(),
            conversation_id: self.conversation_id.clone(),
            thread_id: self.thread_id.clone(),
            sender_identity: self.sender_identity.clone(),
            principal: self.principal.clone(),
        }
    }

    /// Returns the principal-less scope key used to detect cross-principal
    /// and cross-workspace conflicts on the same conversation.
    #[must_use]
    pub fn conflict_scope_key(&self) -> ConversationBindingConflictScopeKey {
        ConversationBindingConflictScopeKey {
            binding_kind: self.binding_kind,
            channel: self.channel.clone(),
            conversation_id: self.conversation_id.clone(),
            thread_id: self.thread_id.clone(),
            sender_identity: self.sender_identity.clone(),
        }
    }

    /// Renders a journal-safe snapshot with channel-derived identifiers
    /// redacted.
    #[must_use]
    pub fn safe_snapshot_json(&self) -> Value {
        json!({
            "schema_version": self.schema_version,
            "binding_id": self.binding_id,
            "binding_kind": self.binding_kind.as_str(),
            "channel": safe_text(self.channel.as_str()),
            "conversation_id": self.conversation_id.as_deref().map(safe_text),
            "thread_id": self.thread_id.as_deref().map(safe_text),
            "sender_identity": self.sender_identity.as_deref().map(safe_text),
            "principal": safe_text(self.principal.as_str()),
            "session_id": self.session_id,
            "workspace_id": self.workspace_id.as_deref().map(safe_text),
            "policy_scope": self.policy_scope,
            "parent_binding_id": self.parent_binding_id,
            "state": self.state.as_str(),
            "created_at_unix_ms": self.created_at_unix_ms,
            "updated_at_unix_ms": self.updated_at_unix_ms,
            "last_activity_at_unix_ms": self.last_activity_at_unix_ms,
            "expires_at_unix_ms": self.expires_at_unix_ms,
        })
    }

    fn touch(&mut self, now_unix_ms: i64) {
        self.updated_at_unix_ms = now_unix_ms;
        self.last_activity_at_unix_ms = now_unix_ms;
        self.expires_at_unix_ms =
            self.lifecycle.expires_at(self.created_at_unix_ms, self.last_activity_at_unix_ms);
    }

    fn expire(&mut self, now_unix_ms: i64) {
        self.state = ConversationBindingLifecycleState::Expired;
        self.updated_at_unix_ms = now_unix_ms;
        self.expires_at_unix_ms = Some(now_unix_ms);
    }

    fn detach(&mut self, now_unix_ms: i64) {
        self.state = ConversationBindingLifecycleState::Detached;
        self.updated_at_unix_ms = now_unix_ms;
        self.expires_at_unix_ms = Some(now_unix_ms);
    }

    fn mark_stale(&mut self, now_unix_ms: i64) {
        self.state = ConversationBindingLifecycleState::Stale;
        self.updated_at_unix_ms = now_unix_ms;
    }

    fn validate(&self) -> Result<(), ConversationBindingError> {
        ensure_non_empty(self.binding_id.as_str(), "binding_id")?;
        ensure_non_empty(self.channel.as_str(), "channel")?;
        ensure_non_empty(self.principal.as_str(), "principal")?;
        ensure_non_empty(self.session_id.as_str(), "session_id")?;
        ensure_non_empty(self.policy_scope.as_str(), "policy_scope")?;
        ensure_component_within_limit(self.binding_id.as_str(), "binding_id")?;
        ensure_component_within_limit(self.channel.as_str(), "channel")?;
        ensure_optional_component_within_limit(self.conversation_id.as_deref(), "conversation_id")?;
        ensure_optional_component_within_limit(self.thread_id.as_deref(), "thread_id")?;
        ensure_optional_component_within_limit(self.sender_identity.as_deref(), "sender_identity")?;
        ensure_component_within_limit(self.principal.as_str(), "principal")?;
        ensure_component_within_limit(self.session_id.as_str(), "session_id")?;
        ensure_optional_component_within_limit(self.workspace_id.as_deref(), "workspace_id")?;
        ensure_component_within_limit(self.policy_scope.as_str(), "policy_scope")?;
        ensure_optional_component_within_limit(
            self.parent_binding_id.as_deref(),
            "parent_binding_id",
        )?;
        Ok(())
    }
}

/// Resolution key: a binding matches a message only when every component,
/// including the principal, is equal.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct ConversationBindingScopeKey {
    pub binding_kind: ConversationBindingKind,
    pub channel: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub conversation_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub thread_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sender_identity: Option<String>,
    pub principal: String,
}

/// Conflict-detection key: like [`ConversationBindingScopeKey`] but without
/// the principal, so two principals bound to one conversation collide.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct ConversationBindingConflictScopeKey {
    pub binding_kind: ConversationBindingKind,
    pub channel: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub conversation_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub thread_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sender_identity: Option<String>,
}

/// Inputs for [`ConversationBindingStore::create_or_touch`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConversationBindingCreateRequest {
    pub binding_kind: ConversationBindingKind,
    pub channel: String,
    pub conversation_id: Option<String>,
    pub thread_id: Option<String>,
    pub sender_identity: Option<String>,
    pub principal: String,
    pub session_id: String,
    pub workspace_id: Option<String>,
    pub policy_scope: String,
    pub parent_binding_id: Option<String>,
    pub lifecycle: ConversationBindingLifecycle,
    pub now_unix_ms: i64,
}

/// Inputs for [`ConversationBindingStore::resolve`]: the scope of one
/// inbound message, without a binding kind (the resolver tries all kinds).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConversationBindingResolveRequest {
    pub channel: String,
    pub conversation_id: Option<String>,
    pub thread_id: Option<String>,
    pub sender_identity: Option<String>,
    pub principal: String,
    pub now_unix_ms: i64,
}

impl ConversationBindingResolveRequest {
    /// Builds the normalized scope key this request would match for
    /// `binding_kind`.
    ///
    /// The thread id participates only for thread-shaped kinds: a main
    /// binding covers the whole conversation, so a message inside a thread
    /// must still fall back to it when no thread binding exists.
    #[must_use]
    pub fn scope_key(&self, binding_kind: ConversationBindingKind) -> ConversationBindingScopeKey {
        ConversationBindingScopeKey {
            binding_kind,
            channel: normalize_component(self.channel.as_str()).unwrap_or_default(),
            conversation_id: normalize_optional_component(self.conversation_id.as_deref()),
            thread_id: if matches!(
                binding_kind,
                ConversationBindingKind::Thread | ConversationBindingKind::DelegatedRun
            ) {
                normalize_optional_component(self.thread_id.as_deref())
            } else {
                None
            },
            sender_identity: normalize_optional_component(self.sender_identity.as_deref()),
            principal: normalize_component(self.principal.as_str()).unwrap_or_default(),
        }
    }
}

/// Filter for [`ConversationBindingStore::list`]; `None` fields match all.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ConversationBindingListFilter {
    pub channel: Option<String>,
    pub principal: Option<String>,
    pub session_id: Option<String>,
    pub include_inactive: bool,
    pub limit: Option<usize>,
}

/// Result of a create-or-touch: the stored record plus whether it was newly
/// created and a stable reason label.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ConversationBindingMutationOutcome {
    pub record: ConversationBindingRecord,
    pub created: bool,
    pub reason: String,
}

/// Result of a resolve: the winning record (if any), why, how many records
/// expired during the lookup, and any detected conflicts.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ConversationBindingResolution {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub record: Option<ConversationBindingRecord>,
    pub reason: String,
    pub expired_count: usize,
    pub conflicts: Vec<ConversationBindingConflict>,
}

/// Categories of binding inconsistencies surfaced by conflict detection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConversationBindingConflictKind {
    DuplicateActiveBinding,
    StaleThread,
    PrincipalMismatch,
    WorkspaceMismatch,
    ExpiredReferenced,
    ParentMissing,
}

impl ConversationBindingConflictKind {
    /// Stable snake_case label used in reports.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::DuplicateActiveBinding => "duplicate_active_binding",
            Self::StaleThread => "stale_thread",
            Self::PrincipalMismatch => "principal_mismatch",
            Self::WorkspaceMismatch => "workspace_mismatch",
            Self::ExpiredReferenced => "expired_referenced",
            Self::ParentMissing => "parent_missing",
        }
    }
}

/// One detected conflict with the bindings involved and a human-readable
/// reason.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConversationBindingConflict {
    pub kind: ConversationBindingConflictKind,
    pub binding_ids: Vec<String>,
    pub reason: String,
}

/// Repair operations a plan can propose. Only `Detach`, `Expire`, and
/// `MarkStale` are applied automatically; `Rebind` and `Split` always wait
/// for an operator.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConversationBindingRepairAction {
    Detach,
    Rebind,
    Expire,
    Split,
    MarkStale,
}

impl ConversationBindingRepairAction {
    /// Stable snake_case label used in reports.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Detach => "detach",
            Self::Rebind => "rebind",
            Self::Expire => "expire",
            Self::Split => "split",
            Self::MarkStale => "mark_stale",
        }
    }
}

/// One proposed repair; `automatic` marks steps safe to apply unattended.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConversationBindingRepairStep {
    pub binding_id: String,
    pub action: ConversationBindingRepairAction,
    pub automatic: bool,
    pub reason: String,
}

/// Conflicts plus the proposed repair steps; `safe_to_auto_apply` is true
/// only when every step is automatic.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ConversationBindingRepairPlan {
    pub conflicts: Vec<ConversationBindingConflict>,
    pub steps: Vec<ConversationBindingRepairStep>,
    pub safe_to_auto_apply: bool,
}

/// Operator-facing explanation of how a scope resolves: candidate
/// snapshots, the repair plan for any conflicts, and a one-line summary.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ConversationBindingExplainReport {
    pub matches: Vec<Value>,
    pub repair_plan: ConversationBindingRepairPlan,
    pub summary: String,
}

/// Result of startup reconciliation: expiry and conflict counts plus the
/// repair plan for whatever remains.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Default)]
pub struct ConversationBindingReconcileReport {
    pub expired_count: usize,
    pub conflict_count: usize,
    pub repair_plan: ConversationBindingRepairPlan,
}

/// Result of applying a repair plan; `records` holds the mutated bindings.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ConversationBindingRepairOutcome {
    pub applied_count: usize,
    pub skipped_count: usize,
    pub records: Vec<ConversationBindingRecord>,
}

/// JSON-file-backed binding store; clones share the same in-memory state.
///
/// Every mutation rewrites the whole file synchronously, which is fine at
/// the enforced `MAX_BINDING_RECORDS` scale.
#[derive(Debug, Clone)]
pub struct ConversationBindingStore {
    path: PathBuf,
    records: Arc<Mutex<BTreeMap<String, ConversationBindingRecord>>>,
}

impl ConversationBindingStore {
    /// Opens (or creates) the store at `path`, normalizing, validating, and
    /// pruning persisted records before first use.
    ///
    /// # Errors
    /// Returns I/O or JSON errors from reading/writing the file and
    /// [`ConversationBindingError::InvalidRecord`] for an unsupported schema
    /// version or a record that fails validation.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, ConversationBindingError> {
        let path = path.as_ref().to_path_buf();
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let envelope = if path.exists() {
            let bytes = fs::read(&path)?;
            if bytes.is_empty() {
                ConversationBindingStoreEnvelope::default()
            } else {
                serde_json::from_slice::<ConversationBindingStoreEnvelope>(bytes.as_slice())?
            }
        } else {
            ConversationBindingStoreEnvelope::default()
        };
        if envelope.schema_version != BINDING_SCHEMA_VERSION {
            return Err(ConversationBindingError::InvalidRecord(format!(
                "unsupported conversation binding schema version {}",
                envelope.schema_version
            )));
        }
        let mut records = BTreeMap::new();
        for mut record in envelope.records.into_values() {
            normalize_record_components(&mut record)?;
            record.validate()?;
            insert_preferred_record(&mut records, record);
        }
        prune_records_to_limit(&mut records, None);
        let store = Self { path, records: Arc::new(Mutex::new(records)) };
        store.persist()?;
        Ok(store)
    }

    /// Opens a store at a unique temp path; test helper only.
    #[cfg(test)]
    pub fn open_temp() -> Self {
        let path = std::env::temp_dir()
            .join(format!("palyra-conversation-bindings-{}.json", ulid::Ulid::generate()));
        Self::open(path).expect("temporary conversation binding store should open")
    }

    /// Creates a binding for the request's scope, or refreshes the existing
    /// one (the content-addressed id makes repeats idempotent).
    ///
    /// Touching also reactivates a detached/stale record and overwrites its
    /// session, workspace, policy scope, parent, and lifecycle from the
    /// request. Due records are expired first; new inserts may prune the
    /// lowest-ranked record to stay within `MAX_BINDING_RECORDS`.
    ///
    /// # Errors
    /// Returns validation errors for malformed components, lock-poisoning,
    /// and persistence (I/O/JSON) failures.
    pub fn create_or_touch(
        &self,
        request: ConversationBindingCreateRequest,
    ) -> Result<ConversationBindingMutationOutcome, ConversationBindingError> {
        let mut record = build_record(request)?;
        record.validate()?;
        let mut guard = self.lock_records()?;
        remove_due_records(&mut guard, record.updated_at_unix_ms);
        let created = !guard.contains_key(record.binding_id.as_str());
        let reason = if let Some(existing) = guard.get_mut(record.binding_id.as_str()) {
            existing.session_id = record.session_id.clone();
            existing.workspace_id = record.workspace_id.clone();
            existing.policy_scope = record.policy_scope.clone();
            existing.parent_binding_id = record.parent_binding_id.clone();
            existing.lifecycle = record.lifecycle.clone();
            existing.state = ConversationBindingLifecycleState::Active;
            existing.touch(record.updated_at_unix_ms);
            record = existing.clone();
            "binding_touched".to_owned()
        } else {
            let reason = "binding_created".to_owned();
            guard.insert(record.binding_id.clone(), record.clone());
            prune_records_to_limit(&mut guard, Some(record.binding_id.as_str()));
            reason
        };
        drop(guard);
        self.persist()?;
        Ok(ConversationBindingMutationOutcome { record, created, reason })
    }

    /// Resolves the active binding for an inbound message scope.
    ///
    /// Precedence: delegated-run over thread over main scope, then most
    /// recent activity, then lowest binding id as the deterministic
    /// tiebreaker. Expires due records first and reports (but does not
    /// repair) any conflicts found in the store.
    ///
    /// # Errors
    /// Returns lock-poisoning and persistence failures from the expiry pass.
    pub fn resolve(
        &self,
        request: ConversationBindingResolveRequest,
    ) -> Result<ConversationBindingResolution, ConversationBindingError> {
        let expired = self.expire_due(request.now_unix_ms)?;
        let guard = self.lock_records()?;
        let delegated_key = request.scope_key(ConversationBindingKind::DelegatedRun);
        let thread_key = request.scope_key(ConversationBindingKind::Thread);
        let main_key = request.scope_key(ConversationBindingKind::Main);
        let mut candidates = guard
            .values()
            .filter(|record| record.active(request.now_unix_ms))
            .filter(|record| {
                let key = record.scope_key();
                key == delegated_key || key == thread_key || key == main_key
            })
            .cloned()
            .collect::<Vec<_>>();
        // Descending by kind: the enum's derived Ord places DelegatedRun
        // above Thread above Main (see ConversationBindingKind docs).
        candidates.sort_by(|left, right| {
            right
                .binding_kind
                .cmp(&left.binding_kind)
                .then_with(|| right.last_activity_at_unix_ms.cmp(&left.last_activity_at_unix_ms))
                .then_with(|| left.binding_id.cmp(&right.binding_id))
        });
        let conflicts = detect_conflicts(guard.values(), request.now_unix_ms);
        let record = candidates.into_iter().next();
        let reason = match record.as_ref() {
            Some(value) if value.binding_kind == ConversationBindingKind::DelegatedRun => {
                "delegated_run_binding_resolved"
            }
            Some(value) if value.binding_kind == ConversationBindingKind::Thread => {
                "thread_binding_resolved"
            }
            Some(_) => "main_binding_resolved",
            None => "binding_not_found",
        }
        .to_owned();
        Ok(ConversationBindingResolution {
            record,
            reason,
            expired_count: expired.len(),
            conflicts,
        })
    }

    /// Refreshes activity timestamps (and thus expiry) for `binding_id`;
    /// returns `None` when the binding does not exist.
    ///
    /// # Errors
    /// Returns lock-poisoning and persistence failures.
    pub fn touch(
        &self,
        binding_id: &str,
        now_unix_ms: i64,
    ) -> Result<Option<ConversationBindingRecord>, ConversationBindingError> {
        let mut guard = self.lock_records()?;
        let Some(record) = guard.get_mut(binding_id) else {
            return Ok(None);
        };
        record.touch(now_unix_ms);
        let record = record.clone();
        drop(guard);
        self.persist()?;
        Ok(Some(record))
    }

    /// Lists bindings matching `filter` in deterministic (channel,
    /// conversation, thread, id) order, capped at `MAX_BINDING_LIST_LIMIT`.
    ///
    /// # Errors
    /// Returns a lock-poisoning failure.
    pub fn list(
        &self,
        filter: ConversationBindingListFilter,
        now_unix_ms: i64,
    ) -> Result<Vec<ConversationBindingRecord>, ConversationBindingError> {
        let guard = self.lock_records()?;
        let limit = filter.limit.unwrap_or(MAX_BINDING_LIST_LIMIT).min(MAX_BINDING_LIST_LIMIT);
        let mut records = guard
            .values()
            .filter(|record| filter.include_inactive || record.active(now_unix_ms))
            .filter(|record| {
                filter
                    .channel
                    .as_deref()
                    .is_none_or(|channel| record.channel.eq_ignore_ascii_case(channel))
            })
            .filter(|record| {
                filter.principal.as_deref().is_none_or(|principal| record.principal == principal)
            })
            .filter(|record| {
                filter
                    .session_id
                    .as_deref()
                    .is_none_or(|session_id| record.session_id == session_id)
            })
            .cloned()
            .collect::<Vec<_>>();
        records.sort_by(|left, right| {
            left.channel
                .cmp(&right.channel)
                .then_with(|| left.conversation_id.cmp(&right.conversation_id))
                .then_with(|| left.thread_id.cmp(&right.thread_id))
                .then_with(|| left.binding_id.cmp(&right.binding_id))
        });
        records.truncate(limit);
        Ok(records)
    }

    /// Detaches `binding_id` so it stops resolving; returns `None` when the
    /// binding does not exist.
    ///
    /// # Errors
    /// Returns lock-poisoning and persistence failures.
    pub fn unbind(
        &self,
        binding_id: &str,
        now_unix_ms: i64,
    ) -> Result<Option<ConversationBindingRecord>, ConversationBindingError> {
        let mut guard = self.lock_records()?;
        let Some(record) = guard.get_mut(binding_id) else {
            return Ok(None);
        };
        record.detach(now_unix_ms);
        let record = record.clone();
        drop(guard);
        self.persist()?;
        Ok(Some(record))
    }

    /// Removes every active binding whose expiry has passed, returning the
    /// removed records marked as expired. Persists only when something
    /// actually expired.
    ///
    /// # Errors
    /// Returns lock-poisoning and persistence failures.
    pub fn expire_due(
        &self,
        now_unix_ms: i64,
    ) -> Result<Vec<ConversationBindingRecord>, ConversationBindingError> {
        let mut guard = self.lock_records()?;
        let expired = remove_due_records(&mut guard, now_unix_ms);
        drop(guard);
        if !expired.is_empty() {
            self.persist()?;
        }
        Ok(expired)
    }

    /// Startup pass: expires due records, then reports remaining conflicts
    /// with a repair plan (which the caller decides whether to apply).
    ///
    /// # Errors
    /// Returns lock-poisoning and persistence failures.
    pub fn reconcile_on_startup(
        &self,
        now_unix_ms: i64,
    ) -> Result<ConversationBindingReconcileReport, ConversationBindingError> {
        let expired = self.expire_due(now_unix_ms)?;
        let guard = self.lock_records()?;
        let conflicts = detect_conflicts(guard.values(), now_unix_ms);
        let repair_plan = build_repair_plan(conflicts);
        Ok(ConversationBindingReconcileReport {
            expired_count: expired.len(),
            conflict_count: repair_plan.conflicts.len(),
            repair_plan,
        })
    }

    /// Explains how `request` resolves: a redacted summary, snapshots of
    /// nearby bindings (including inactive ones), and the repair plan for
    /// any conflicts.
    ///
    /// # Errors
    /// Returns lock-poisoning and persistence failures from the underlying
    /// resolve/list calls.
    pub fn explain(
        &self,
        request: ConversationBindingResolveRequest,
    ) -> Result<ConversationBindingExplainReport, ConversationBindingError> {
        let resolution = self.resolve(request.clone())?;
        let matches = self
            .list(
                ConversationBindingListFilter {
                    channel: Some(request.channel),
                    principal: Some(request.principal),
                    session_id: None,
                    include_inactive: true,
                    limit: Some(64),
                },
                request.now_unix_ms,
            )?
            .iter()
            .map(ConversationBindingRecord::safe_snapshot_json)
            .collect::<Vec<_>>();
        let repair_plan = build_repair_plan(resolution.conflicts);
        let summary = match resolution.record {
            Some(record) => format!(
                "binding {} resolves session {} via {} scope",
                record.binding_id,
                record.session_id,
                record.binding_kind.as_str()
            ),
            None => "no active conversation binding matched this scope".to_owned(),
        };
        Ok(ConversationBindingExplainReport { matches, repair_plan, summary: safe_text(&summary) })
    }

    /// Applies the automatic steps of `plan`; manual steps (`Rebind`,
    /// `Split`) and steps targeting missing bindings are counted as skipped.
    ///
    /// # Errors
    /// Returns lock-poisoning and persistence failures.
    pub fn apply_repair_plan(
        &self,
        plan: &ConversationBindingRepairPlan,
        now_unix_ms: i64,
    ) -> Result<ConversationBindingRepairOutcome, ConversationBindingError> {
        let mut guard = self.lock_records()?;
        let mut records = Vec::new();
        let mut applied_count = 0usize;
        let mut skipped_count = 0usize;
        for step in &plan.steps {
            if !step.automatic {
                skipped_count = skipped_count.saturating_add(1);
                continue;
            }
            let Some(record) = guard.get_mut(step.binding_id.as_str()) else {
                skipped_count = skipped_count.saturating_add(1);
                continue;
            };
            match step.action {
                ConversationBindingRepairAction::Detach => record.detach(now_unix_ms),
                ConversationBindingRepairAction::Expire => record.expire(now_unix_ms),
                ConversationBindingRepairAction::MarkStale => record.mark_stale(now_unix_ms),
                ConversationBindingRepairAction::Rebind
                | ConversationBindingRepairAction::Split => {
                    skipped_count = skipped_count.saturating_add(1);
                    continue;
                }
            }
            applied_count = applied_count.saturating_add(1);
            records.push(record.clone());
        }
        drop(guard);
        if applied_count > 0 {
            self.persist()?;
        }
        Ok(ConversationBindingRepairOutcome { applied_count, skipped_count, records })
    }

    fn lock_records(
        &self,
    ) -> Result<
        std::sync::MutexGuard<'_, BTreeMap<String, ConversationBindingRecord>>,
        ConversationBindingError,
    > {
        self.records
            .lock()
            .map_err(|_| ConversationBindingError::PoisonedLock("conversation binding store"))
    }

    fn persist(&self) -> Result<(), ConversationBindingError> {
        let records = self.lock_records()?.clone();
        let envelope =
            ConversationBindingStoreEnvelope { schema_version: BINDING_SCHEMA_VERSION, records };
        let content = serde_json::to_string_pretty(&envelope)?;
        if let Some(parent) = self.path.parent() {
            fs::create_dir_all(parent)?;
        }
        write_content_with_backups(self.path.as_path(), content.as_str(), 0).map_err(|source| {
            ConversationBindingError::Io(std::io::Error::other(source.to_string()))
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct ConversationBindingStoreEnvelope {
    schema_version: u32,
    records: BTreeMap<String, ConversationBindingRecord>,
}

impl Default for ConversationBindingStoreEnvelope {
    fn default() -> Self {
        Self { schema_version: BINDING_SCHEMA_VERSION, records: BTreeMap::new() }
    }
}

/// Failures from the conversation binding store.
#[derive(Debug)]
pub enum ConversationBindingError {
    Io(std::io::Error),
    Json(serde_json::Error),
    InvalidRecord(String),
    PoisonedLock(&'static str),
}

impl ConversationBindingError {
    /// Display message with channel-derived identifiers redacted, suitable
    /// for journals and user-facing errors.
    #[must_use]
    pub fn safe_message(&self) -> String {
        safe_text(self.to_string().as_str())
    }
}

impl fmt::Display for ConversationBindingError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => write!(formatter, "conversation binding store I/O failed: {error}"),
            Self::Json(error) => {
                write!(formatter, "conversation binding store JSON failed: {error}")
            }
            Self::InvalidRecord(message) => {
                write!(formatter, "invalid conversation binding: {message}")
            }
            Self::PoisonedLock(name) => write!(formatter, "{name} lock poisoned"),
        }
    }
}

impl Error for ConversationBindingError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            Self::Json(error) => Some(error),
            Self::InvalidRecord(_) | Self::PoisonedLock(_) => None,
        }
    }
}

impl From<std::io::Error> for ConversationBindingError {
    fn from(value: std::io::Error) -> Self {
        Self::Io(value)
    }
}

impl From<serde_json::Error> for ConversationBindingError {
    fn from(value: serde_json::Error) -> Self {
        Self::Json(value)
    }
}

fn build_record(
    request: ConversationBindingCreateRequest,
) -> Result<ConversationBindingRecord, ConversationBindingError> {
    let channel = normalize_required_component(request.channel.as_str(), "channel")?;
    let principal = normalize_required_component(request.principal.as_str(), "principal")?;
    let session_id = normalize_required_component(request.session_id.as_str(), "session_id")?;
    let policy_scope = normalize_required_component(request.policy_scope.as_str(), "policy_scope")?;
    let conversation_id = normalize_optional_component(request.conversation_id.as_deref());
    let thread_id = normalize_optional_component(request.thread_id.as_deref());
    let sender_identity = normalize_optional_component(request.sender_identity.as_deref());
    let workspace_id = normalize_optional_component(request.workspace_id.as_deref());
    let parent_binding_id = normalize_optional_component(request.parent_binding_id.as_deref());
    let now = request.now_unix_ms;
    let expires_at_unix_ms = request.lifecycle.expires_at(now, now);
    let binding_id = stable_binding_id(
        request.binding_kind,
        channel.as_str(),
        conversation_id.as_deref(),
        thread_id.as_deref(),
        sender_identity.as_deref(),
        principal.as_str(),
        session_id.as_str(),
    );
    Ok(ConversationBindingRecord {
        schema_version: BINDING_SCHEMA_VERSION,
        binding_id,
        binding_kind: request.binding_kind,
        channel,
        conversation_id,
        thread_id,
        sender_identity,
        principal,
        session_id,
        workspace_id,
        policy_scope,
        parent_binding_id,
        lifecycle: request.lifecycle,
        state: ConversationBindingLifecycleState::Active,
        created_at_unix_ms: now,
        updated_at_unix_ms: now,
        last_activity_at_unix_ms: now,
        expires_at_unix_ms,
    })
}

fn detect_conflicts<'a>(
    records: impl Iterator<Item = &'a ConversationBindingRecord>,
    now_unix_ms: i64,
) -> Vec<ConversationBindingConflict> {
    let records = records.collect::<Vec<_>>();
    let mut conflicts = Vec::new();
    let mut active_by_scope: BTreeMap<ConversationBindingScopeKey, Vec<String>> = BTreeMap::new();
    let mut active_by_conflict_scope: BTreeMap<
        ConversationBindingConflictScopeKey,
        Vec<&ConversationBindingRecord>,
    > = BTreeMap::new();
    let ids = records.iter().map(|record| record.binding_id.clone()).collect::<BTreeSet<_>>();
    for record in records {
        if record.state.is_active()
            && record.expires_at_unix_ms.is_some_and(|expires_at| expires_at <= now_unix_ms)
        {
            conflicts.push(ConversationBindingConflict {
                kind: ConversationBindingConflictKind::ExpiredReferenced,
                binding_ids: vec![record.binding_id.clone()],
                reason: "active binding has expired and must be marked expired".to_owned(),
            });
        }
        if record.binding_kind == ConversationBindingKind::Thread && record.thread_id.is_none() {
            conflicts.push(ConversationBindingConflict {
                kind: ConversationBindingConflictKind::StaleThread,
                binding_ids: vec![record.binding_id.clone()],
                reason: "thread binding is missing thread_id".to_owned(),
            });
        }
        if let Some(parent_id) = record.parent_binding_id.as_deref() {
            if !ids.contains(parent_id) {
                conflicts.push(ConversationBindingConflict {
                    kind: ConversationBindingConflictKind::ParentMissing,
                    binding_ids: vec![record.binding_id.clone()],
                    reason: "binding references a missing parent binding".to_owned(),
                });
            }
        }
        if record.active(now_unix_ms) {
            active_by_scope.entry(record.scope_key()).or_default().push(record.binding_id.clone());
            active_by_conflict_scope.entry(record.conflict_scope_key()).or_default().push(record);
        }
    }
    for binding_ids in active_by_scope.values() {
        if binding_ids.len() > 1 {
            conflicts.push(ConversationBindingConflict {
                kind: ConversationBindingConflictKind::DuplicateActiveBinding,
                binding_ids: binding_ids.clone(),
                reason: "multiple active bindings match the same channel scope".to_owned(),
            });
        }
    }
    for records in active_by_conflict_scope.values() {
        let principals =
            records.iter().map(|record| record.principal.as_str()).collect::<BTreeSet<_>>();
        if principals.len() > 1 {
            conflicts.push(ConversationBindingConflict {
                kind: ConversationBindingConflictKind::PrincipalMismatch,
                binding_ids: records.iter().map(|record| record.binding_id.clone()).collect(),
                reason: "bindings share a channel scope across different principals".to_owned(),
            });
        }
        let workspaces = records
            .iter()
            .filter_map(|record| record.workspace_id.as_deref())
            .collect::<BTreeSet<_>>();
        if workspaces.len() > 1 {
            conflicts.push(ConversationBindingConflict {
                kind: ConversationBindingConflictKind::WorkspaceMismatch,
                binding_ids: records.iter().map(|record| record.binding_id.clone()).collect(),
                reason: "bindings share a channel scope across different workspaces".to_owned(),
            });
        }
    }
    conflicts.sort_by(|left, right| {
        left.kind.cmp(&right.kind).then_with(|| left.binding_ids.cmp(&right.binding_ids))
    });
    conflicts
}

fn build_repair_plan(conflicts: Vec<ConversationBindingConflict>) -> ConversationBindingRepairPlan {
    let mut steps = Vec::new();
    for conflict in &conflicts {
        match conflict.kind {
            ConversationBindingConflictKind::DuplicateActiveBinding => {
                for binding_id in conflict.binding_ids.iter().skip(1) {
                    steps.push(ConversationBindingRepairStep {
                        binding_id: binding_id.clone(),
                        action: ConversationBindingRepairAction::Detach,
                        automatic: true,
                        reason: "detach duplicate active binding after keeping the first stable id"
                            .to_owned(),
                    });
                }
            }
            ConversationBindingConflictKind::ExpiredReferenced => {
                for binding_id in &conflict.binding_ids {
                    steps.push(ConversationBindingRepairStep {
                        binding_id: binding_id.clone(),
                        action: ConversationBindingRepairAction::Expire,
                        automatic: true,
                        reason: "mark expired active binding as expired".to_owned(),
                    });
                }
            }
            ConversationBindingConflictKind::StaleThread
            | ConversationBindingConflictKind::ParentMissing => {
                for binding_id in &conflict.binding_ids {
                    steps.push(ConversationBindingRepairStep {
                        binding_id: binding_id.clone(),
                        action: ConversationBindingRepairAction::MarkStale,
                        automatic: true,
                        reason: "mark binding stale until an operator repairs the scope".to_owned(),
                    });
                }
            }
            ConversationBindingConflictKind::PrincipalMismatch
            | ConversationBindingConflictKind::WorkspaceMismatch => {
                for binding_id in &conflict.binding_ids {
                    steps.push(ConversationBindingRepairStep {
                        binding_id: binding_id.clone(),
                        action: ConversationBindingRepairAction::Split,
                        automatic: false,
                        reason: "scope conflict needs explicit operator review".to_owned(),
                    });
                }
            }
        }
    }
    let safe_to_auto_apply = steps.iter().all(|step| step.automatic);
    ConversationBindingRepairPlan { conflicts, steps, safe_to_auto_apply }
}

// Content-addressed id over scope + session: rebinding the same scope to a
// different session yields a new id (the old binding surfaces as a
// duplicate-active conflict until detached), while repeats of the same
// scope+session are idempotent touches.
fn stable_binding_id(
    binding_kind: ConversationBindingKind,
    channel: &str,
    conversation_id: Option<&str>,
    thread_id: Option<&str>,
    sender_identity: Option<&str>,
    principal: &str,
    session_id: &str,
) -> String {
    let payload = serde_json::to_vec(&json!({
        "schema_version": BINDING_SCHEMA_VERSION,
        "binding_kind": binding_kind.as_str(),
        "channel": channel,
        "conversation_id": conversation_id,
        "thread_id": thread_id,
        "sender_identity": sender_identity,
        "principal": principal,
        "session_id": session_id,
    }))
    .unwrap_or_default();
    format!("cb_{}", sha256_hex(payload.as_slice()))
}

fn ensure_non_empty(value: &str, field: &str) -> Result<(), ConversationBindingError> {
    if value.trim().is_empty() {
        Err(ConversationBindingError::InvalidRecord(format!("{field} must not be empty")))
    } else {
        Ok(())
    }
}

fn ensure_component_within_limit(value: &str, field: &str) -> Result<(), ConversationBindingError> {
    if value.len() > MAX_BINDING_COMPONENT_BYTES {
        Err(ConversationBindingError::InvalidRecord(format!(
            "{field} exceeds {MAX_BINDING_COMPONENT_BYTES} bytes"
        )))
    } else {
        Ok(())
    }
}

fn ensure_optional_component_within_limit(
    value: Option<&str>,
    field: &str,
) -> Result<(), ConversationBindingError> {
    if let Some(value) = value {
        ensure_component_within_limit(value, field)?;
    }
    Ok(())
}

fn normalize_record_components(
    record: &mut ConversationBindingRecord,
) -> Result<(), ConversationBindingError> {
    record.binding_id = normalize_required_component(record.binding_id.as_str(), "binding_id")?;
    record.channel = normalize_required_component(record.channel.as_str(), "channel")?;
    record.conversation_id = normalize_optional_component(record.conversation_id.as_deref());
    record.thread_id = normalize_optional_component(record.thread_id.as_deref());
    record.sender_identity = normalize_optional_component(record.sender_identity.as_deref());
    record.principal = normalize_required_component(record.principal.as_str(), "principal")?;
    record.session_id = normalize_required_component(record.session_id.as_str(), "session_id")?;
    record.workspace_id = normalize_optional_component(record.workspace_id.as_deref());
    record.policy_scope =
        normalize_required_component(record.policy_scope.as_str(), "policy_scope")?;
    record.parent_binding_id = normalize_optional_component(record.parent_binding_id.as_deref());
    Ok(())
}

// On duplicate ids loaded from disk, keep the record with the most recent
// activity (then update) timestamp.
fn insert_preferred_record(
    records: &mut BTreeMap<String, ConversationBindingRecord>,
    record: ConversationBindingRecord,
) {
    let should_replace = records.get(record.binding_id.as_str()).is_none_or(|existing| {
        record
            .last_activity_at_unix_ms
            .cmp(&existing.last_activity_at_unix_ms)
            .then_with(|| record.updated_at_unix_ms.cmp(&existing.updated_at_unix_ms))
            .is_gt()
    });
    if should_replace {
        records.insert(record.binding_id.clone(), record);
    }
}

fn remove_due_records(
    records: &mut BTreeMap<String, ConversationBindingRecord>,
    now_unix_ms: i64,
) -> Vec<ConversationBindingRecord> {
    let expired_ids = records
        .iter()
        .filter(|(_, record)| {
            record.state.is_active()
                && record.expires_at_unix_ms.is_some_and(|expires_at| expires_at <= now_unix_ms)
        })
        .map(|(binding_id, _)| binding_id.clone())
        .collect::<Vec<_>>();
    let mut expired = Vec::with_capacity(expired_ids.len());
    for binding_id in expired_ids {
        if let Some(mut record) = records.remove(binding_id.as_str()) {
            record.expire(now_unix_ms);
            expired.push(record);
        }
    }
    expired
}

// Evicts lowest-ranked records (expired first, then detached/stale, then
// least recently active) until the cap holds, never evicting the binding
// that triggered the prune.
fn prune_records_to_limit(
    records: &mut BTreeMap<String, ConversationBindingRecord>,
    preserve_binding_id: Option<&str>,
) -> usize {
    let mut removed = 0usize;
    while records.len() > MAX_BINDING_RECORDS {
        let remove_id = records
            .iter()
            .filter(|(binding_id, _)| preserve_binding_id != Some(binding_id.as_str()))
            .min_by(|(left_id, left), (right_id, right)| {
                record_retention_rank(left)
                    .cmp(&record_retention_rank(right))
                    .then_with(|| left_id.cmp(right_id))
            })
            .map(|(binding_id, _)| binding_id.clone());
        let Some(remove_id) = remove_id else {
            break;
        };
        records.remove(remove_id.as_str());
        removed = removed.saturating_add(1);
    }
    removed
}

fn record_retention_rank(record: &ConversationBindingRecord) -> (u8, i64, i64) {
    let state_rank = match record.state {
        ConversationBindingLifecycleState::Expired => 0,
        ConversationBindingLifecycleState::Detached | ConversationBindingLifecycleState::Stale => 1,
        ConversationBindingLifecycleState::Active => 2,
    };
    (state_rank, record.last_activity_at_unix_ms, record.updated_at_unix_ms)
}

fn normalize_required_component(
    value: &str,
    field: &str,
) -> Result<String, ConversationBindingError> {
    normalize_component(value).ok_or_else(|| {
        ConversationBindingError::InvalidRecord(format!("{field} must not be empty"))
    })
}

fn normalize_component(value: &str) -> Option<String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(bound_component_bytes(trimmed))
    }
}

fn normalize_optional_component(value: Option<&str>) -> Option<String> {
    value.and_then(normalize_component)
}

// Oversized components are truncated but keep a full-content sha256 suffix,
// so two long identifiers that differ only past the cutoff still produce
// distinct (and stable) normalized values.
fn bound_component_bytes(value: &str) -> String {
    if value.len() <= MAX_BINDING_COMPONENT_BYTES {
        return value.to_owned();
    }
    let suffix = format!("{HASH_SUFFIX_PREFIX}{}", sha256_hex(value.as_bytes()));
    let prefix_budget = MAX_BINDING_COMPONENT_BYTES.saturating_sub(suffix.len());
    let prefix = truncate_utf8_to_bytes(value, prefix_budget);
    format!("{prefix}{suffix}")
}

fn truncate_utf8_to_bytes(value: &str, max_bytes: usize) -> &str {
    if value.len() <= max_bytes {
        return value;
    }
    let mut end = max_bytes.min(value.len());
    while end > 0 && !value.is_char_boundary(end) {
        end -= 1;
    }
    &value[..end]
}

fn safe_text(value: &str) -> String {
    redact_url_segments_in_text(&redact_auth_error(value))
}

fn sha256_hex(payload: &[u8]) -> String {
    let digest = Sha256::digest(payload);
    digest.iter().map(|byte| format!("{byte:02x}")).collect()
}

#[cfg(test)]
mod tests {
    use super::{
        ConversationBindingCreateRequest, ConversationBindingKind, ConversationBindingLifecycle,
        ConversationBindingListFilter, ConversationBindingResolveRequest, ConversationBindingStore,
        MAX_BINDING_COMPONENT_BYTES, MAX_BINDING_RECORDS,
    };

    fn create_request(session_id: &str, now_unix_ms: i64) -> ConversationBindingCreateRequest {
        ConversationBindingCreateRequest {
            binding_kind: ConversationBindingKind::Thread,
            channel: "discord:default".to_owned(),
            conversation_id: Some("conv-1".to_owned()),
            thread_id: Some("thread-1".to_owned()),
            sender_identity: Some("discord:user:42".to_owned()),
            principal: "user:ops".to_owned(),
            session_id: session_id.to_owned(),
            workspace_id: Some("workspace-a".to_owned()),
            policy_scope: "channel:discord:default".to_owned(),
            parent_binding_id: None,
            lifecycle: ConversationBindingLifecycle::default(),
            now_unix_ms,
        }
    }

    #[test]
    fn create_touch_list_and_reload_are_durable() {
        let path = std::env::temp_dir()
            .join(format!("palyra-bindings-test-{}.json", ulid::Ulid::generate()));
        let store = ConversationBindingStore::open(&path).expect("store opens");
        let created = store
            .create_or_touch(create_request("01ARZ3NDEKTSV4RRFFQ69G5FAV", 1_000))
            .expect("binding create succeeds");
        assert!(created.created);

        let touched = store
            .create_or_touch(create_request("01ARZ3NDEKTSV4RRFFQ69G5FAV", 2_000))
            .expect("binding touch succeeds");
        assert!(!touched.created);
        assert_eq!(touched.record.last_activity_at_unix_ms, 2_000);

        let reloaded = ConversationBindingStore::open(&path).expect("store reloads");
        let records =
            reloaded.list(ConversationBindingListFilter::default(), 2_000).expect("list succeeds");
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].binding_id, created.record.binding_id);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn resolver_prefers_thread_scope_and_expires_due_records() {
        let store = ConversationBindingStore::open_temp();
        let mut request = create_request("01ARZ3NDEKTSV4RRFFQ69G5FAW", 1_000);
        request.lifecycle.idle_timeout_ms = Some(100);
        let created = store.create_or_touch(request).expect("binding create succeeds");

        let resolved = store
            .resolve(ConversationBindingResolveRequest {
                channel: "discord:default".to_owned(),
                conversation_id: Some("conv-1".to_owned()),
                thread_id: Some("thread-1".to_owned()),
                sender_identity: Some("discord:user:42".to_owned()),
                principal: "user:ops".to_owned(),
                now_unix_ms: 1_050,
            })
            .expect("resolve succeeds");
        assert_eq!(
            resolved.record.as_ref().map(|record| record.binding_id.as_str()),
            Some(created.record.binding_id.as_str())
        );
        assert_eq!(resolved.reason, "thread_binding_resolved");

        let expired = store
            .resolve(ConversationBindingResolveRequest {
                channel: "discord:default".to_owned(),
                conversation_id: Some("conv-1".to_owned()),
                thread_id: Some("thread-1".to_owned()),
                sender_identity: Some("discord:user:42".to_owned()),
                principal: "user:ops".to_owned(),
                now_unix_ms: 1_101,
            })
            .expect("resolve succeeds after expiry");
        assert!(expired.record.is_none());
        assert_eq!(expired.expired_count, 1);
        let records = store
            .list(
                ConversationBindingListFilter { include_inactive: true, ..Default::default() },
                1_101,
            )
            .expect("list succeeds");
        assert!(records.is_empty());
    }

    #[test]
    fn resolver_prefers_delegated_run_scope_over_thread_scope() {
        let store = ConversationBindingStore::open_temp();
        let thread = store
            .create_or_touch(create_request("01ARZ3NDEKTSV4RRFFQ69G5FAV", 1_000))
            .expect("thread binding create succeeds");
        let mut delegated = create_request("01ARZ3NDEKTSV4RRFFQ69G5FAW", 1_001);
        delegated.binding_kind = ConversationBindingKind::DelegatedRun;
        delegated.thread_id = Some("delegation:task-1".to_owned());
        delegated.sender_identity = Some("delegated-run:child-run".to_owned());
        delegated.policy_scope = "delegation:parent-run".to_owned();
        let delegated = store.create_or_touch(delegated).expect("delegated binding succeeds");

        let resolved = store
            .resolve(ConversationBindingResolveRequest {
                channel: "discord:default".to_owned(),
                conversation_id: Some("conv-1".to_owned()),
                thread_id: Some("delegation:task-1".to_owned()),
                sender_identity: Some("delegated-run:child-run".to_owned()),
                principal: "user:ops".to_owned(),
                now_unix_ms: 1_050,
            })
            .expect("resolve succeeds");

        assert_eq!(
            resolved.record.as_ref().map(|record| record.binding_id.as_str()),
            Some(delegated.record.binding_id.as_str())
        );
        assert_ne!(
            resolved.record.as_ref().map(|record| record.binding_id.as_str()),
            Some(thread.record.binding_id.as_str())
        );
        assert_eq!(resolved.reason, "delegated_run_binding_resolved");
    }

    #[test]
    fn explain_reports_principal_conflict_without_auto_repair() {
        let store = ConversationBindingStore::open_temp();
        store
            .create_or_touch(create_request("01ARZ3NDEKTSV4RRFFQ69G5FAX", 1_000))
            .expect("first create succeeds");
        let mut second = create_request("01ARZ3NDEKTSV4RRFFQ69G5FAY", 1_001);
        second.principal = "user:other".to_owned();
        store.create_or_touch(second).expect("second create succeeds");

        let report = store
            .explain(ConversationBindingResolveRequest {
                channel: "discord:default".to_owned(),
                conversation_id: Some("conv-1".to_owned()),
                thread_id: Some("thread-1".to_owned()),
                sender_identity: Some("discord:user:42".to_owned()),
                principal: "user:ops".to_owned(),
                now_unix_ms: 2_000,
            })
            .expect("explain succeeds");
        assert!(report
            .repair_plan
            .conflicts
            .iter()
            .any(|conflict| conflict.kind.as_str() == "principal_mismatch"));
        assert!(!report.repair_plan.safe_to_auto_apply);
    }

    #[test]
    fn oversized_route_identifiers_are_bounded_and_resolve_stably() {
        let store = ConversationBindingStore::open_temp();
        let oversized = "x".repeat(MAX_BINDING_COMPONENT_BYTES + 128);
        let mut request = create_request("01ARZ3NDEKTSV4RRFFQ69G5FAZ", 1_000);
        request.conversation_id = Some(oversized.clone());
        request.thread_id = Some(format!("thread-{oversized}"));
        request.sender_identity = Some(format!("sender-{oversized}"));

        let created = store.create_or_touch(request).expect("oversized create succeeds");
        assert!(created
            .record
            .conversation_id
            .as_ref()
            .is_some_and(|value| { value.len() <= MAX_BINDING_COMPONENT_BYTES }));
        assert!(created
            .record
            .thread_id
            .as_ref()
            .is_some_and(|value| { value.len() <= MAX_BINDING_COMPONENT_BYTES }));
        assert!(created
            .record
            .sender_identity
            .as_ref()
            .is_some_and(|value| { value.len() <= MAX_BINDING_COMPONENT_BYTES }));

        let resolved = store
            .resolve(ConversationBindingResolveRequest {
                channel: "discord:default".to_owned(),
                conversation_id: Some(oversized.clone()),
                thread_id: Some(format!("thread-{oversized}")),
                sender_identity: Some(format!("sender-{oversized}")),
                principal: "user:ops".to_owned(),
                now_unix_ms: 1_001,
            })
            .expect("oversized resolve succeeds");
        assert_eq!(
            resolved.record.as_ref().map(|record| record.binding_id.as_str()),
            Some(created.record.binding_id.as_str())
        );
    }

    #[test]
    fn create_prunes_oldest_binding_when_record_limit_is_reached() {
        let store = ConversationBindingStore::open_temp();
        for index in 0..MAX_BINDING_RECORDS {
            let mut request =
                create_request(format!("session-{index}").as_str(), 1_000 + index as i64);
            request.conversation_id = Some(format!("conv-{index}"));
            request.thread_id = Some(format!("thread-{index}"));
            store.create_or_touch(request).expect("binding create succeeds");
        }

        let mut newest = create_request("session-new", 2_000);
        newest.conversation_id = Some("conv-new".to_owned());
        newest.thread_id = Some("thread-new".to_owned());
        let created = store.create_or_touch(newest).expect("new binding create succeeds");

        let records = store
            .list(
                ConversationBindingListFilter {
                    include_inactive: true,
                    limit: Some(MAX_BINDING_RECORDS + 1),
                    ..Default::default()
                },
                2_000,
            )
            .expect("list succeeds");
        assert_eq!(records.len(), MAX_BINDING_RECORDS);
        assert!(records.iter().any(|record| record.binding_id == created.record.binding_id));
        assert!(!records.iter().any(|record| record.thread_id.as_deref() == Some("thread-0")));
    }
}
