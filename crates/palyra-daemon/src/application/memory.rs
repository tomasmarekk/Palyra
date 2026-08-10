//! Memory lifecycle core: write classification, the retain/dedupe pipeline,
//! and proto mapping for durable memory items.
//!
//! Ingest path: `retain_memory_candidate` normalizes candidate text, runs
//! [`classify_memory_write`] (category, sensitivity, approval gate), holds
//! low-confidence or sensitive writes for operator review, merges exact and
//! near-duplicates (and correction conflicts) into existing items, and only
//! then ingests a new item through [`GatewayRuntimeState`] into the journal
//! store.
//!
//! Scope model: every item is owned by a principal and optionally narrowed to
//! a channel or session. Scope checks here are deny-by-default -- a requested
//! scope must match the authenticated context, and channel-scoped items are
//! invisible without channel context. The memory tool surface lives in
//! `application::tool_runtime::memory`, recall assembly in
//! `application::recall`, and ranking in `crate::retrieval`.
//!
//! All memory text returned to callers passes
//! [`redact_memory_text_for_output`], which reuses the journal redaction
//! pipeline.

use std::{collections::BTreeSet, sync::Arc};

use serde::Serialize;
use serde_json::{json, Value};
use tonic::Status;
use ulid::Ulid;

use crate::{
    application::service_authorization::{
        authorize_memory_action, principal_has_sensitive_service_role, SensitiveServiceRole,
    },
    gateway::{
        current_unix_ms_status, non_empty, GatewayRuntimeState, MAX_MEMORY_SEARCH_TOP_K,
        MAX_MEMORY_TOOL_TAGS,
    },
    journal::{
        MemoryItemCreateRequest, MemoryItemLifecycleUpdateRequest, MemoryItemRecord,
        MemorySearchHit, MemorySearchRequest, MemorySource,
    },
    transport::grpc::proto::palyra::{common::v1 as common_v1, memory::v1 as memory_v1},
};

/// Resolves the effective channel scope for a memory request.
///
/// An explicitly requested channel must equal the authenticated channel
/// context; otherwise a caller could read or write another channel's
/// memories. When no channel is requested, the context channel (if any)
/// becomes the scope.
///
/// # Errors
/// Returns `PermissionDenied` when the requested channel differs from the
/// authenticated channel context.
#[allow(clippy::result_large_err)]
pub(crate) fn resolve_memory_channel_scope(
    context_channel: Option<&str>,
    requested_channel: Option<String>,
) -> Result<Option<String>, Status> {
    let normalized_requested = requested_channel.and_then(non_empty);
    if let (Some(context_channel), Some(requested_channel)) =
        (context_channel, normalized_requested.as_deref())
    {
        if context_channel != requested_channel {
            return Err(Status::permission_denied(
                "memory scope channel must match authenticated channel context",
            ));
        }
    }
    Ok(normalized_requested.or_else(|| context_channel.map(str::to_owned)))
}

/// Maps a proto `MemorySource` discriminant to the journal enum.
///
/// Unknown discriminants degrade to `Unspecified` and are rejected, so newly
/// added proto values fail loudly instead of being silently misfiled.
///
/// # Errors
/// Returns `InvalidArgument` when the source is unspecified or unknown.
#[allow(clippy::result_large_err)]
pub(crate) fn memory_source_from_proto(raw: i32) -> Result<MemorySource, Status> {
    match memory_v1::MemorySource::try_from(raw).unwrap_or(memory_v1::MemorySource::Unspecified) {
        memory_v1::MemorySource::TapeUserMessage => Ok(MemorySource::TapeUserMessage),
        memory_v1::MemorySource::TapeToolResult => Ok(MemorySource::TapeToolResult),
        memory_v1::MemorySource::Summary => Ok(MemorySource::Summary),
        memory_v1::MemorySource::Manual => Ok(MemorySource::Manual),
        memory_v1::MemorySource::Import => Ok(MemorySource::Import),
        memory_v1::MemorySource::Unspecified => {
            Err(Status::invalid_argument("memory source must be specified"))
        }
    }
}

fn memory_source_to_proto(source: MemorySource) -> i32 {
    match source {
        MemorySource::TapeUserMessage => memory_v1::MemorySource::TapeUserMessage as i32,
        MemorySource::TapeToolResult => memory_v1::MemorySource::TapeToolResult as i32,
        MemorySource::Summary => memory_v1::MemorySource::Summary as i32,
        MemorySource::Manual => memory_v1::MemorySource::Manual as i32,
        MemorySource::Import => memory_v1::MemorySource::Import as i32,
    }
}

fn optional_canonical_id(value: &Option<String>) -> Option<common_v1::CanonicalId> {
    value.as_deref().map(|ulid| common_v1::CanonicalId { ulid: ulid.to_owned() })
}

/// Enforces that a memory item is visible to the authenticated context.
///
/// Visibility rules: the item principal must match, and a channel-scoped
/// item is only visible from that same channel. Principal-scoped items
/// (no channel on the item) stay visible from any channel of the owning
/// principal.
///
/// # Errors
/// Returns `PermissionDenied` on a principal mismatch, a channel mismatch,
/// or when a channel-scoped item is accessed without channel context.
#[allow(clippy::result_large_err)]
pub(crate) fn enforce_memory_item_scope(
    item: &MemoryItemRecord,
    principal: &str,
    channel: Option<&str>,
) -> Result<(), Status> {
    if item.principal != principal {
        return Err(Status::permission_denied("memory item principal does not match context"));
    }
    match (channel, item.channel.as_deref()) {
        (Some(context_channel), Some(item_channel)) if context_channel != item_channel => {
            return Err(Status::permission_denied("memory item channel does not match context"));
        }
        (None, Some(_)) => {
            return Err(Status::permission_denied(
                "memory item is channel-scoped and requires authenticated channel context",
            ));
        }
        _ => {}
    }
    Ok(())
}

/// Enforces destructive scope for deleting an existing memory item.
///
/// The authenticated channel must exactly match the item's channel. Therefore
/// principal-scoped memory requires an unscoped principal context, while
/// channel-scoped memory requires that same channel context.
///
/// # Errors
/// Returns `PermissionDenied` when the principal or exact destructive scope
/// does not match the item being deleted.
#[allow(clippy::result_large_err)]
pub(crate) fn enforce_memory_item_delete_scope(
    item: &MemoryItemRecord,
    principal: &str,
    channel: Option<&str>,
) -> Result<(), Status> {
    enforce_memory_item_scope(item, principal, channel)?;
    if item.channel.as_deref() != channel {
        return Err(Status::permission_denied(
            "principal-scoped memory requires unscoped principal context for deletion",
        ));
    }
    Ok(())
}

/// Redacts memory text before it is returned to any caller or model.
///
/// Wraps the text in a one-field JSON object so it can flow through the
/// journal's payload redaction pipeline, then unwraps the redacted value.
pub(crate) fn redact_memory_text_for_output(raw: &str) -> String {
    if raw.is_empty() {
        return String::new();
    }

    let payload = json!({ "value": raw });
    let redacted_payload = crate::journal::redact_payload_json(payload.to_string().as_bytes());
    redacted_memory_payload_value(redacted_payload).unwrap_or_default()
}

fn redacted_memory_payload_value(
    redacted_payload: Result<String, crate::journal::JournalError>,
) -> Option<String> {
    let redacted_payload = redacted_payload.ok()?;
    match serde_json::from_str::<Value>(redacted_payload.as_str()) {
        Ok(Value::Object(fields)) => fields.get("value").and_then(Value::as_str).map(str::to_owned),
        _ => None,
    }
}

/// Fence marker stamped into memory provenance so downstream consumers can
/// recognize (and version) retrieved-memory context blocks.
pub(crate) const MEMORY_CONTEXT_FENCE_VERSION: &str = "palyra.memory_context.v2";
/// Trust label attached to recalled memory content; it marks the text as
/// retrieved evidence rather than instructions.
pub(crate) const MEMORY_TRUST_LABEL_RETRIEVED: &str = "retrieved_memory";
// Below this confidence a write is held for review instead of auto-retained.
const MEMORY_RETAIN_LOW_CONFIDENCE_THRESHOLD: f64 = 0.45;
// Search score at or above which a hit may merge as a near-duplicate.
const MEMORY_RETAIN_NEAR_DUPLICATE_SCORE: f64 = 0.92;
// Floor for dedupe candidate retrieval; below this, hits are not even
// considered for duplicate/conflict matching.
const MEMORY_RETAIN_DEDUPE_MIN_SCORE: f64 = 0.55;
// Lowercase substrings that flag candidate text as possibly carrying secret
// material; refined by the intent heuristics in
// contains_secret_value_like_memory_write.
const MEMORY_WRITE_SENSITIVE_PATTERNS: &[&str] = &[
    "api key",
    "bearer ",
    "cookie",
    "credential",
    "password",
    "private key",
    "secret",
    "session token",
    "token",
];
// Default TTL for transient runtime facts written without an explicit TTL.
const MEMORY_TRANSIENT_TTL_MS: i64 = 24 * 60 * 60 * 1_000;

/// Visibility scope of a memory write: one session, one channel, or the
/// whole principal. Wider scopes face stricter review gates.
#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum MemoryLifecycleScope {
    Session,
    Channel,
    Principal,
}

impl MemoryLifecycleScope {
    /// Parses a scope keyword; `None` defaults to `session`, and `global` is
    /// accepted as an alias for `principal`.
    ///
    /// # Errors
    /// Returns `InvalidArgument` for any other value.
    pub(crate) fn parse(raw: Option<&str>) -> Result<Self, Status> {
        match raw.unwrap_or("session").trim().to_ascii_lowercase().as_str() {
            "session" => Ok(Self::Session),
            "channel" => Ok(Self::Channel),
            "principal" | "global" => Ok(Self::Principal),
            _ => Err(Status::invalid_argument(
                "memory lifecycle scope must be one of: session|channel|principal",
            )),
        }
    }

    /// Returns the canonical lowercase scope keyword.
    #[must_use]
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Session => "session",
            Self::Channel => "channel",
            Self::Principal => "principal",
        }
    }
}

/// Outcome of a retain attempt: written as new, merged into or updating an
/// existing item, held for review, or rejected outright.
#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum MemoryLifecycleStatus {
    Retained,
    NeedsReview,
    Rejected,
    Merged,
    UpdatedExisting,
}

impl MemoryLifecycleStatus {
    /// Returns the canonical snake_case status keyword used in tool output.
    #[must_use]
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Retained => "retained",
            Self::NeedsReview => "needs_review",
            Self::Rejected => "rejected",
            Self::Merged => "merged",
            Self::UpdatedExisting => "updated_existing",
        }
    }
}

/// Semantic category of a memory write; drives TTL defaults, dedupe and
/// replacement behavior, and operator-review requirements.
#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum MemoryWriteCategory {
    Fact,
    Preference,
    Procedure,
    Constraint,
    Decision,
    Correction,
    TransientRuntimeFact,
}

impl MemoryWriteCategory {
    /// Returns the canonical snake_case category keyword.
    #[must_use]
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Fact => "fact",
            Self::Preference => "preference",
            Self::Procedure => "procedure",
            Self::Constraint => "constraint",
            Self::Decision => "decision",
            Self::Correction => "correction",
            Self::TransientRuntimeFact => "transient_runtime_fact",
        }
    }

    /// Parses a category keyword; returns `None` for unknown values so
    /// callers can choose between rejecting and defaulting.
    #[must_use]
    pub(crate) fn parse(value: &str) -> Option<Self> {
        match value.trim() {
            "fact" => Some(Self::Fact),
            "preference" => Some(Self::Preference),
            "procedure" => Some(Self::Procedure),
            "constraint" => Some(Self::Constraint),
            "decision" => Some(Self::Decision),
            "correction" => Some(Self::Correction),
            "transient_runtime_fact" => Some(Self::TransientRuntimeFact),
            _ => None,
        }
    }

    fn from_tag_value(value: &str) -> Option<Self> {
        Self::parse(value)
    }
}

/// Sensitivity verdict for a memory write: `Sensitive` flags likely secret
/// material, `HighRisk` flags principal-wide policy/approval-weakening
/// intent. Anything but `Normal` forces review.
#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum MemoryWriteSensitivity {
    Normal,
    Sensitive,
    HighRisk,
}

/// Whether a memory write may be auto-retained or must wait for review.
#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum MemoryWriteApprovalState {
    NotRequired,
    Required,
}

/// Provenance reference linking a memory write back to its origin (tape
/// event, artifact, or direct memory write).
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub(crate) struct MemoryWriteSourceRef {
    pub(crate) source_kind: String,
    pub(crate) source_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) session_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) run_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) tape_seq: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) artifact_id: Option<String>,
}

/// Inputs to [`classify_memory_write`]; `now_unix_ms` is injected so
/// classification stays deterministic and testable.
#[derive(Debug, Clone)]
pub(crate) struct MemoryWriteClassificationInput {
    pub(crate) principal: String,
    pub(crate) channel: Option<String>,
    pub(crate) session_id: String,
    pub(crate) scope: MemoryLifecycleScope,
    pub(crate) content_text: String,
    pub(crate) category_hint: Option<MemoryWriteCategory>,
    pub(crate) confidence: f64,
    pub(crate) ttl_unix_ms: Option<i64>,
    pub(crate) provenance: Value,
    pub(crate) now_unix_ms: i64,
}

/// Result of classifying a memory write; serialized into provenance and tool
/// output, so field names are part of the external contract.
#[derive(Debug, Clone, Serialize, PartialEq)]
pub(crate) struct MemoryWriteClassification {
    pub(crate) category: MemoryWriteCategory,
    pub(crate) confidence: f64,
    pub(crate) sensitivity: MemoryWriteSensitivity,
    pub(crate) approval_state: MemoryWriteApprovalState,
    pub(crate) source_refs: Vec<MemoryWriteSourceRef>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) ttl_unix_ms: Option<i64>,
    pub(crate) owner_principal: String,
    pub(crate) scope: String,
    pub(crate) source_hash: String,
    pub(crate) rollback_id: String,
    pub(crate) reason_codes: Vec<String>,
}

/// A candidate memory write submitted to the retain pipeline.
///
/// `replaces_terms` carries the caller's hint about which existing values a
/// correction supersedes; it widens dedupe search and gates conflict merges.
#[derive(Debug, Clone, Serialize, PartialEq)]
pub(crate) struct MemoryLifecycleRetainRequest {
    pub(crate) principal: String,
    pub(crate) channel: Option<String>,
    pub(crate) session_id: String,
    pub(crate) scope: MemoryLifecycleScope,
    pub(crate) source: MemorySource,
    pub(crate) content_text: String,
    pub(crate) category_hint: Option<MemoryWriteCategory>,
    pub(crate) replaces_terms: Vec<String>,
    pub(crate) tags: Vec<String>,
    pub(crate) confidence: Option<f64>,
    pub(crate) ttl_unix_ms: Option<i64>,
    pub(crate) provenance: Value,
}

/// Result of a retain attempt. `durable_memory_write` is the authoritative
/// "was anything persisted" signal; `item` is the written/updated record when
/// it was.
#[derive(Debug, Clone, Serialize, PartialEq)]
pub(crate) struct MemoryLifecycleRetainOutcome {
    pub(crate) status: MemoryLifecycleStatus,
    pub(crate) reason: String,
    pub(crate) scope: MemoryLifecycleScope,
    pub(crate) trust_label: String,
    pub(crate) durable_memory_write: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) item: Option<MemoryItemRecord>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) matched_memory_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) write_classification: Option<MemoryWriteClassification>,
    pub(crate) provenance: Value,
}

/// Entry point for durable memory writes; owns the classify -> review-gate ->
/// dedupe -> ingest pipeline on top of the gateway runtime state.
pub(crate) struct MemoryLifecycleProvider {
    runtime_state: Arc<GatewayRuntimeState>,
}

impl MemoryLifecycleProvider {
    #[must_use]
    pub(crate) fn new(runtime_state: Arc<GatewayRuntimeState>) -> Self {
        Self { runtime_state }
    }

    /// Runs the full retain pipeline for one memory candidate.
    ///
    /// Non-retained outcomes (rejected, needs-review) are reported through
    /// [`MemoryLifecycleRetainOutcome`], not as errors.
    ///
    /// # Errors
    /// Returns `InvalidArgument` for out-of-range confidence,
    /// `PermissionDenied` from scope or policy checks, and storage errors
    /// from the journal-backed search/ingest/update calls.
    #[allow(clippy::result_large_err)]
    pub(crate) async fn retain(
        &self,
        request: MemoryLifecycleRetainRequest,
    ) -> Result<MemoryLifecycleRetainOutcome, Status> {
        retain_memory_candidate(&self.runtime_state, request).await
    }
}

#[allow(clippy::result_large_err)]
async fn retain_memory_candidate(
    runtime_state: &Arc<GatewayRuntimeState>,
    mut request: MemoryLifecycleRetainRequest,
) -> Result<MemoryLifecycleRetainOutcome, Status> {
    request.content_text = normalize_lifecycle_content(request.content_text.as_str());
    let confidence = request.confidence.unwrap_or(0.75);
    if !confidence.is_finite() || !(0.0..=1.0).contains(&confidence) {
        return Err(Status::invalid_argument(
            "palyra.memory.retain confidence must be in range 0.0..=1.0",
        ));
    }

    if request.content_text.is_empty() {
        return Ok(memory_retain_outcome(MemoryRetainOutcomeInput {
            status: MemoryLifecycleStatus::Rejected,
            reason: "memory content is empty after normalization",
            scope: request.scope,
            durable_memory_write: false,
            item: None,
            matched_memory_id: None,
            provenance: request.provenance,
            write_classification: None,
        }));
    }

    let classification = classify_memory_write(MemoryWriteClassificationInput {
        principal: request.principal.clone(),
        channel: request.channel.clone(),
        session_id: request.session_id.clone(),
        scope: request.scope,
        content_text: request.content_text.clone(),
        category_hint: request.category_hint,
        confidence,
        ttl_unix_ms: request.ttl_unix_ms,
        provenance: request.provenance.clone(),
        now_unix_ms: current_unix_ms_status()?,
    });
    request.ttl_unix_ms = classification.ttl_unix_ms;
    request.tags = lifecycle_write_tags(
        request.tags.as_slice(),
        request.scope,
        classification.category,
        classification.source_hash.as_str(),
    );
    request.provenance = memory_write_provenance(request.provenance, &classification);

    if classification.approval_state == MemoryWriteApprovalState::Required
        || confidence < MEMORY_RETAIN_LOW_CONFIDENCE_THRESHOLD
    {
        let reason = if classification.approval_state == MemoryWriteApprovalState::Required {
            format!("memory write requires review: {}", classification.reason_codes.join(","))
        } else {
            "memory confidence is below automatic retention threshold".to_owned()
        };
        return Ok(memory_retain_outcome(MemoryRetainOutcomeInput {
            status: MemoryLifecycleStatus::NeedsReview,
            reason: reason.as_str(),
            scope: request.scope,
            durable_memory_write: false,
            item: None,
            matched_memory_id: None,
            provenance: request.provenance,
            write_classification: Some(classification),
        }));
    }

    let (channel_scope, session_scope, resource) = resolve_lifecycle_write_scope(&request)?;
    authorize_memory_action(request.principal.as_str(), "memory.ingest", resource.as_str())?;
    if let Some(duplicate) = find_lifecycle_duplicate(
        runtime_state,
        &request,
        &classification,
        channel_scope.clone(),
        session_scope.clone(),
    )
    .await?
    {
        let replacement_content = lifecycle_duplicate_replacement_content(
            &classification,
            &duplicate,
            request.content_text.as_str(),
            request.replaces_terms.as_slice(),
        );
        let replaces_with_correction = replacement_content.is_some()
            && classification.category == MemoryWriteCategory::Correction;
        let updates_preference_content = replacement_content.is_some()
            && classification.category == MemoryWriteCategory::Preference;
        let durable_category = if replacement_content.is_some() {
            classification.category
        } else {
            lifecycle_item_write_category(&duplicate.item)
        };
        // A correction supersedes the old value, so its tags replace the
        // existing ones; plain merges keep the union of both tag sets.
        let tags = if replaces_with_correction {
            lifecycle_write_tags(
                request.tags.as_slice(),
                request.scope,
                durable_category,
                classification.source_hash.as_str(),
            )
        } else {
            merge_memory_tags(
                duplicate.item.tags.as_slice(),
                request.tags.as_slice(),
                request.scope,
                durable_category,
                classification.source_hash.as_str(),
            )
        };
        let updated = runtime_state
            .update_memory_item_lifecycle(MemoryItemLifecycleUpdateRequest {
                memory_id: duplicate.item.memory_id.clone(),
                principal: request.principal.clone(),
                channel: duplicate.item.channel.clone(),
                session_id: duplicate.item.session_id.clone(),
                content_text: replacement_content,
                tags,
                confidence: Some(
                    duplicate.item.confidence.unwrap_or(0.0).max(confidence).clamp(0.0, 1.0),
                ),
                ttl_unix_ms: request.ttl_unix_ms,
            })
            .await?;
        if let Some(item) = updated {
            let status = if duplicate.exact() {
                MemoryLifecycleStatus::UpdatedExisting
            } else {
                MemoryLifecycleStatus::Merged
            };
            let reason = if duplicate.exact() {
                "exact duplicate memory updated with lifecycle metadata"
            } else if replaces_with_correction {
                "near-duplicate memory updated with replacement lifecycle content"
            } else if updates_preference_content {
                "near-duplicate preference updated with requested lifecycle content"
            } else {
                "near-duplicate memory merged into existing lifecycle record"
            };
            return Ok(memory_retain_outcome(MemoryRetainOutcomeInput {
                status,
                reason,
                scope: request.scope,
                durable_memory_write: true,
                item: Some(item),
                matched_memory_id: Some(duplicate.item.memory_id),
                provenance: request.provenance,
                write_classification: Some(classification),
            }));
        }
    }

    let item = runtime_state
        .ingest_memory_item(MemoryItemCreateRequest {
            memory_id: Ulid::new().to_string(),
            principal: request.principal.clone(),
            channel: channel_scope,
            session_id: session_scope,
            source: request.source,
            content_text: request.content_text,
            tags: request.tags,
            confidence: Some(confidence),
            ttl_unix_ms: request.ttl_unix_ms,
        })
        .await?;
    Ok(memory_retain_outcome(MemoryRetainOutcomeInput {
        status: MemoryLifecycleStatus::Retained,
        reason: "memory retained in lifecycle store",
        scope: request.scope,
        durable_memory_write: true,
        item: Some(item),
        matched_memory_id: None,
        provenance: request.provenance,
        write_classification: Some(classification),
    }))
}

/// Existing item that a retain candidate collapses into, with the kind of
/// match that justified the merge.
#[derive(Debug, Clone)]
struct LifecycleDuplicate {
    item: MemoryItemRecord,
    match_kind: LifecycleDuplicateMatchKind,
}

impl LifecycleDuplicate {
    fn exact(&self) -> bool {
        self.match_kind == LifecycleDuplicateMatchKind::Exact
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LifecycleDuplicateMatchKind {
    Exact,
    NearDuplicate,
    CorrectionConflict,
}

/// Looks for an existing item the candidate should merge into: an exact
/// normalized-text match, a same-category near-duplicate, or (for
/// corrections) a conflicting prior value. Search-based matching runs first;
/// a bounded scope-list scan backstops corrections whose wording diverges too
/// far for search recall.
#[allow(clippy::result_large_err)]
async fn find_lifecycle_duplicate(
    runtime_state: &Arc<GatewayRuntimeState>,
    request: &MemoryLifecycleRetainRequest,
    classification: &MemoryWriteClassification,
    channel_scope: Option<String>,
    session_scope: Option<String>,
) -> Result<Option<LifecycleDuplicate>, Status> {
    for query in lifecycle_duplicate_search_queries(
        request.content_text.as_str(),
        request.replaces_terms.as_slice(),
    ) {
        let hits = runtime_state
            .search_memory(MemorySearchRequest {
                principal: request.principal.clone(),
                channel: channel_scope.clone(),
                session_id: session_scope.clone(),
                query,
                top_k: 8.min(MAX_MEMORY_SEARCH_TOP_K),
                min_score: MEMORY_RETAIN_DEDUPE_MIN_SCORE,
                tags: Vec::new(),
                sources: Vec::new(),
            })
            .await?;
        for hit in hits {
            // search_memory widens visibility across scopes; dedupe must only
            // merge within the exact scope cell being written.
            if !lifecycle_item_matches_scan_scope(
                &hit.item,
                channel_scope.as_deref(),
                session_scope.as_deref(),
            ) {
                continue;
            }
            let exact =
                normalize_lifecycle_content(hit.item.content_text.as_str()) == request.content_text;
            if exact {
                return Ok(Some(LifecycleDuplicate {
                    item: hit.item,
                    match_kind: LifecycleDuplicateMatchKind::Exact,
                }));
            }
            if hit.score >= MEMORY_RETAIN_NEAR_DUPLICATE_SCORE
                && lifecycle_near_duplicate_categories_compatible(
                    classification.category,
                    &hit.item,
                )
                && lifecycle_near_duplicate_texts_compatible(
                    request.content_text.as_str(),
                    hit.item.content_text.as_str(),
                )
            {
                return Ok(Some(LifecycleDuplicate {
                    item: hit.item,
                    match_kind: LifecycleDuplicateMatchKind::NearDuplicate,
                }));
            }
            if lifecycle_conflict_matches(
                classification.category,
                request.replaces_terms.as_slice(),
                request.content_text.as_str(),
                lifecycle_item_write_category(&hit.item),
                hit.item.content_text.as_str(),
            ) {
                return Ok(Some(LifecycleDuplicate {
                    item: hit.item,
                    match_kind: LifecycleDuplicateMatchKind::CorrectionConflict,
                }));
            }
        }
    }
    if let Some(conflict) = find_lifecycle_conflict_by_scope_scan(
        runtime_state,
        request,
        classification,
        channel_scope,
        session_scope,
    )
    .await?
    {
        return Ok(Some(conflict));
    }
    Ok(None)
}

#[allow(clippy::result_large_err)]
async fn find_lifecycle_conflict_by_scope_scan(
    runtime_state: &Arc<GatewayRuntimeState>,
    request: &MemoryLifecycleRetainRequest,
    classification: &MemoryWriteClassification,
    channel_scope: Option<String>,
    session_scope: Option<String>,
) -> Result<Option<LifecycleDuplicate>, Status> {
    if classification.category != MemoryWriteCategory::Correction {
        return Ok(None);
    }

    let (items, _) = runtime_state
        .list_memory_items(
            None,
            Some(128),
            request.principal.clone(),
            channel_scope.clone(),
            session_scope.clone(),
            Vec::new(),
            Vec::new(),
        )
        .await?;
    Ok(lifecycle_conflict_from_scope_items(
        items,
        classification.category,
        request.replaces_terms.as_slice(),
        request.content_text.as_str(),
        channel_scope.as_deref(),
        session_scope.as_deref(),
    ))
}

/// Scans listed scope items for the strongest correction conflict, ranked by
/// significant-term overlap; an exact text match short-circuits as a plain
/// duplicate.
fn lifecycle_conflict_from_scope_items(
    items: Vec<MemoryItemRecord>,
    candidate_category: MemoryWriteCategory,
    replaces_terms: &[String],
    content_text: &str,
    channel_scope: Option<&str>,
    session_scope: Option<&str>,
) -> Option<LifecycleDuplicate> {
    let mut best = None::<(MemoryItemRecord, usize)>;
    for item in items {
        if !lifecycle_item_matches_scan_scope(&item, channel_scope, session_scope) {
            continue;
        }
        let exact = normalize_lifecycle_content(item.content_text.as_str()) == content_text;
        if exact {
            return Some(LifecycleDuplicate {
                item,
                match_kind: LifecycleDuplicateMatchKind::Exact,
            });
        }
        if !lifecycle_conflict_matches(
            candidate_category,
            replaces_terms,
            content_text,
            lifecycle_item_write_category(&item),
            item.content_text.as_str(),
        ) {
            continue;
        }
        let overlap = lifecycle_conflict_overlap_count(content_text, item.content_text.as_str());
        if best.as_ref().is_none_or(|(_, best_overlap)| overlap > *best_overlap) {
            best = Some((item, overlap));
        }
    }

    best.map(|(item, _)| LifecycleDuplicate {
        item,
        match_kind: LifecycleDuplicateMatchKind::CorrectionConflict,
    })
}

/// Exact scope-cell match: an item only qualifies for dedupe/conflict
/// handling when its channel and session bindings equal the write scope.
/// Principal-scope writes (None, None) must not absorb channel- or
/// session-scoped items, and vice versa.
fn lifecycle_item_matches_scan_scope(
    item: &MemoryItemRecord,
    channel_scope: Option<&str>,
    session_scope: Option<&str>,
) -> bool {
    match (channel_scope, session_scope) {
        (Some(channel), Some(session)) => {
            item.session_id.as_deref() == Some(session)
                && matches!(item.channel.as_deref(), Some(item_channel) if item_channel == channel)
        }
        (Some(channel), None) => {
            item.session_id.is_none()
                && matches!(item.channel.as_deref(), Some(item_channel) if item_channel == channel)
        }
        (None, Some(session)) => {
            item.channel.is_none() && item.session_id.as_deref() == Some(session)
        }
        (None, None) => item.channel.is_none() && item.session_id.is_none(),
    }
}

/// Builds the dedupe search queries: the full candidate text, the caller's
/// replacement terms (so corrections find the value they supersede even when
/// the new wording shares little with the old), and a significant-terms
/// digest of the candidate.
fn lifecycle_duplicate_search_queries(
    content_text: &str,
    replaces_terms: &[String],
) -> Vec<String> {
    let mut queries = vec![content_text.to_owned()];
    let replacement_query = replaces_terms
        .iter()
        .flat_map(|term| lifecycle_duplicate_terms(term))
        .collect::<Vec<_>>()
        .join(" ");
    if !replacement_query.is_empty() && !queries.iter().any(|query| query == &replacement_query) {
        queries.push(replacement_query);
    }
    if let Some(significant_context) = lifecycle_significant_context_query(content_text) {
        if !queries.iter().any(|query| query == &significant_context) {
            queries.push(significant_context);
        }
    }
    queries
}

fn lifecycle_duplicate_terms(input: &str) -> Vec<String> {
    input
        .split(|character: char| !(character.is_alphanumeric() || character == '_'))
        .filter_map(|term| {
            let normalized = term.trim().to_lowercase();
            (normalized.len() >= 3).then_some(normalized)
        })
        .collect()
}

fn lifecycle_significant_context_query(content_text: &str) -> Option<String> {
    let terms = lifecycle_significant_terms(content_text);
    if terms.len() >= 2 {
        Some(terms.into_iter().take(12).collect::<Vec<_>>().join(" "))
    } else {
        None
    }
}

fn lifecycle_significant_terms(content_text: &str) -> Vec<String> {
    let mut terms = Vec::new();
    for term in lifecycle_duplicate_terms(content_text) {
        if !terms.iter().any(|existing| existing == &term) {
            terms.push(term);
        }
    }
    terms
}

/// Decides whether an existing item is the prior value a correction
/// supersedes. Only corrections conflict, only against prior
/// corrections/preferences, the caller's replacement terms must actually
/// reference the existing content, and the texts must share enough
/// significant terms -- all of which keeps unrelated notes from being
/// overwritten.
fn lifecycle_conflict_matches(
    category: MemoryWriteCategory,
    replaces_terms: &[String],
    candidate_content: &str,
    existing_category: MemoryWriteCategory,
    existing_content: &str,
) -> bool {
    if category != MemoryWriteCategory::Correction {
        return false;
    }
    if !matches!(
        existing_category,
        MemoryWriteCategory::Correction | MemoryWriteCategory::Preference
    ) {
        return false;
    }
    if !lifecycle_replacement_terms_reference_existing_value(replaces_terms, existing_content) {
        return false;
    }
    lifecycle_conflict_overlap_count(candidate_content, existing_content)
        >= LIFECYCLE_CONFLICT_MIN_OVERLAP
}

const LIFECYCLE_CONFLICT_MIN_OVERLAP: usize = 2;

fn lifecycle_conflict_overlap_count(candidate_content: &str, existing_content: &str) -> usize {
    let candidate_terms =
        lifecycle_significant_terms(candidate_content).into_iter().collect::<BTreeSet<_>>();
    let existing_terms =
        lifecycle_significant_terms(existing_content).into_iter().collect::<BTreeSet<_>>();
    candidate_terms.intersection(&existing_terms).count()
}

/// Reads the durable category marker written at retain time; untagged
/// (legacy) items default to `Fact`. Historical writers appended the
/// authoritative marker after caller tags, so reverse traversal also
/// recovers records that contain an older forged prefix marker.
pub(crate) fn lifecycle_item_write_category(item: &MemoryItemRecord) -> MemoryWriteCategory {
    item.tags
        .iter()
        .rev()
        .find_map(|tag| tag.strip_prefix("memory_write:"))
        .and_then(MemoryWriteCategory::from_tag_value)
        .unwrap_or(MemoryWriteCategory::Fact)
}

fn lifecycle_near_duplicate_categories_compatible(
    category: MemoryWriteCategory,
    item: &MemoryItemRecord,
) -> bool {
    category == lifecycle_item_write_category(item)
}

fn lifecycle_near_duplicate_texts_compatible(
    candidate_content: &str,
    existing_content: &str,
) -> bool {
    let candidate_terms =
        lifecycle_significant_terms(candidate_content).into_iter().collect::<BTreeSet<_>>();
    let existing_terms =
        lifecycle_significant_terms(existing_content).into_iter().collect::<BTreeSet<_>>();
    if candidate_terms.is_empty() || existing_terms.is_empty() {
        return false;
    }

    let intersection_count = candidate_terms.intersection(&existing_terms).count();
    let smaller_count = candidate_terms.len().min(existing_terms.len());
    let larger_count = candidate_terms.len().max(existing_terms.len());
    // Short texts share terms by accident easily, so they must be fully
    // contained and close in size; longer texts merge on 75% overlap of the
    // smaller set and 60% of the larger. The dual bound keeps distinct items
    // that share boilerplate (e.g. action items citing one source) separate.
    if smaller_count <= 4 {
        return intersection_count == smaller_count && larger_count <= smaller_count + 2;
    }

    intersection_count * 100 >= smaller_count * 75 && intersection_count * 100 >= larger_count * 60
}

fn lifecycle_replacement_terms_reference_existing_value(
    replaces_terms: &[String],
    existing_content: &str,
) -> bool {
    if replaces_terms.is_empty() {
        return false;
    }
    let existing_terms =
        lifecycle_significant_terms(existing_content).into_iter().collect::<BTreeSet<_>>();
    replaces_terms
        .iter()
        .flat_map(|term| lifecycle_significant_terms(term))
        .any(|term| existing_terms.contains(&term))
}

/// Decides whether the merge replaces the existing item's text. Only
/// corrections (over their matched conflict) and near-duplicate preferences
/// carry the new wording; other merges keep the existing text and just
/// refresh metadata.
fn lifecycle_duplicate_replacement_content(
    classification: &MemoryWriteClassification,
    duplicate: &LifecycleDuplicate,
    content_text: &str,
    replaces_terms: &[String],
) -> Option<String> {
    if duplicate.exact() {
        return None;
    }
    let existing_category = lifecycle_item_write_category(&duplicate.item);
    match classification.category {
        MemoryWriteCategory::Correction
            if lifecycle_conflict_matches(
                classification.category,
                replaces_terms,
                content_text,
                existing_category,
                duplicate.item.content_text.as_str(),
            ) =>
        {
            Some(lifecycle_replacement_content(classification, content_text))
        }
        MemoryWriteCategory::Preference
            if duplicate.match_kind == LifecycleDuplicateMatchKind::NearDuplicate
                && existing_category == MemoryWriteCategory::Preference =>
        {
            Some(lifecycle_replacement_content(classification, content_text))
        }
        _ => None,
    }
}

fn lifecycle_replacement_content(
    _classification: &MemoryWriteClassification,
    content_text: &str,
) -> String {
    compact_memory_text(content_text)
}

fn compact_memory_text(input: &str) -> String {
    let mut compact = input.split_whitespace().collect::<Vec<_>>().join(" ");
    // Each replace pass can create new adjacent punctuation pairs (", ," ->
    // "," etc.); three passes reach a fixed point for the patterns handled.
    for _ in 0..3 {
        compact = compact
            .replace(" ,", ",")
            .replace(" .", ".")
            .replace(" ;", ";")
            .replace(", ,", ",")
            .replace(", .", ".")
            .replace("; .", ".")
            .replace(",,", ",")
            .replace("..", ".");
    }
    compact
        .trim_matches(|character: char| {
            character.is_whitespace() || matches!(character, ',' | ';' | '-' | ':')
        })
        .to_owned()
}

/// Maps the requested scope to the (channel, session) storage bindings plus
/// the policy resource string used for the `memory.ingest` authorization.
fn resolve_lifecycle_write_scope(
    request: &MemoryLifecycleRetainRequest,
) -> Result<(Option<String>, Option<String>, String), Status> {
    match request.scope {
        MemoryLifecycleScope::Session => Ok((
            request.channel.clone(),
            Some(request.session_id.clone()),
            format!("memory:session:{}", request.session_id),
        )),
        MemoryLifecycleScope::Channel => {
            let Some(channel) = request.channel.clone() else {
                return Err(Status::permission_denied(
                    "palyra.memory.retain scope=channel requires authenticated channel context",
                ));
            };
            Ok((Some(channel.clone()), None, format!("memory:channel:{channel}")))
        }
        MemoryLifecycleScope::Principal => Ok((None, None, "memory:principal".to_owned())),
    }
}

struct MemoryRetainOutcomeInput<'a> {
    status: MemoryLifecycleStatus,
    reason: &'a str,
    scope: MemoryLifecycleScope,
    durable_memory_write: bool,
    item: Option<MemoryItemRecord>,
    matched_memory_id: Option<String>,
    provenance: Value,
    write_classification: Option<MemoryWriteClassification>,
}

fn memory_retain_outcome(input: MemoryRetainOutcomeInput<'_>) -> MemoryLifecycleRetainOutcome {
    MemoryLifecycleRetainOutcome {
        status: input.status,
        reason: input.reason.to_owned(),
        scope: input.scope,
        trust_label: MEMORY_TRUST_LABEL_RETRIEVED.to_owned(),
        durable_memory_write: input.durable_memory_write,
        item: input.item,
        matched_memory_id: input.matched_memory_id,
        write_classification: input.write_classification,
        provenance: input.provenance,
    }
}

/// Classifies a memory write: category, sensitivity, TTL bounding, source
/// references, and whether the write needs operator approval.
///
/// Approval is required for low confidence, any non-normal sensitivity, or
/// persistent (channel/principal) procedure/constraint rules written by a
/// non-admin principal -- the paths a prompt-injected agent would use to
/// plant durable instructions.
pub(crate) fn classify_memory_write(
    input: MemoryWriteClassificationInput,
) -> MemoryWriteClassification {
    let normalized = normalize_lifecycle_content(input.content_text.as_str());
    let lowered = normalized.to_ascii_lowercase();
    let category = input.category_hint.unwrap_or_else(|| default_memory_write_category(&input));
    let sensitivity = classify_memory_write_sensitivity(lowered.as_str(), input.scope);
    let ttl_unix_ms = match (category, input.ttl_unix_ms) {
        (MemoryWriteCategory::TransientRuntimeFact, None) => {
            Some(input.now_unix_ms.saturating_add(MEMORY_TRANSIENT_TTL_MS))
        }
        (_, ttl) => ttl,
    };
    // Identity hash over (principal, channel, session, scope, normalized
    // text): the same text written into a different scope hashes differently.
    // sha256_hex always yields 64 hex chars, so the [..16] slices below hold.
    let source_hash = crate::sha256_hex(
        format!(
            "{}:{}:{}:{}:{}",
            input.principal,
            input.channel.as_deref().unwrap_or_default(),
            input.session_id,
            input.scope.as_str(),
            normalized
        )
        .as_bytes(),
    );
    let mut reason_codes = vec![format!("category:{}", category.as_str())];
    if ttl_unix_ms.is_some() {
        reason_codes.push("ttl:bounded".to_owned());
    }
    if input.confidence < MEMORY_RETAIN_LOW_CONFIDENCE_THRESHOLD {
        reason_codes.push("confidence:below_auto_retain_threshold".to_owned());
    }
    if input.scope == MemoryLifecycleScope::Principal {
        reason_codes.push("scope:principal".to_owned());
    }
    match sensitivity {
        MemoryWriteSensitivity::Normal => {}
        MemoryWriteSensitivity::Sensitive => reason_codes.push("sensitivity:sensitive".to_owned()),
        MemoryWriteSensitivity::HighRisk => reason_codes.push("sensitivity:high_risk".to_owned()),
    }
    let requires_operator_review =
        persistent_runtime_rule_requires_operator_review(&input, category);
    if requires_operator_review {
        reason_codes.push("policy:operator_review_for_runtime_rule".to_owned());
    }
    let approval_state = if input.confidence < MEMORY_RETAIN_LOW_CONFIDENCE_THRESHOLD
        || sensitivity != MemoryWriteSensitivity::Normal
        || requires_operator_review
    {
        MemoryWriteApprovalState::Required
    } else {
        MemoryWriteApprovalState::NotRequired
    };
    let source_refs = memory_write_source_refs(
        &input.provenance,
        source_hash.as_str(),
        input.session_id.as_str(),
    );
    MemoryWriteClassification {
        category,
        confidence: input.confidence.clamp(0.0, 1.0),
        sensitivity,
        approval_state,
        source_refs,
        ttl_unix_ms,
        owner_principal: input.principal,
        scope: input.scope.as_str().to_owned(),
        rollback_id: format!("memory-rollback-{}", &source_hash[..16]),
        source_hash,
        reason_codes,
    }
}

fn default_memory_write_category(input: &MemoryWriteClassificationInput) -> MemoryWriteCategory {
    if input.ttl_unix_ms.is_some() {
        MemoryWriteCategory::TransientRuntimeFact
    } else {
        MemoryWriteCategory::Fact
    }
}

fn persistent_runtime_rule_requires_operator_review(
    input: &MemoryWriteClassificationInput,
    category: MemoryWriteCategory,
) -> bool {
    matches!(input.scope, MemoryLifecycleScope::Channel | MemoryLifecycleScope::Principal)
        && matches!(category, MemoryWriteCategory::Procedure | MemoryWriteCategory::Constraint)
        && !principal_has_sensitive_service_role(
            input.principal.as_str(),
            SensitiveServiceRole::AdminOrSystem,
        )
}

fn classify_memory_write_sensitivity(
    lowered: &str,
    scope: MemoryLifecycleScope,
) -> MemoryWriteSensitivity {
    if contains_secret_value_like_memory_write(lowered) {
        MemoryWriteSensitivity::Sensitive
    } else if scope == MemoryLifecycleScope::Principal
        && contains_high_risk_memory_write_intent(lowered)
    {
        MemoryWriteSensitivity::HighRisk
    } else {
        MemoryWriteSensitivity::Normal
    }
}

/// Heuristic for "this text probably carries a secret value" (vs. merely
/// talking about secrets). Layered: assignment-like forms always match;
/// otherwise a sensitive keyword must pair with store/remember intent or
/// value-talk, while purely defensive phrasing ("never log ...") is allowed
/// through as normal.
fn contains_secret_value_like_memory_write(lowered: &str) -> bool {
    if contains_any(
        lowered,
        &[
            "api_key=",
            "api key:",
            "bearer ",
            "cookie:",
            "cookie=",
            "credential:",
            "credential=",
            "password:",
            "password=",
            "private key:",
            "private key=",
            "-----begin private key-----",
            "secret:",
            "secret=",
            "session token:",
            "session token=",
            "token:",
            "token=",
        ],
    ) {
        return true;
    }
    if !contains_any(lowered, MEMORY_WRITE_SENSITIVE_PATTERNS) {
        return false;
    }
    if contains_any(lowered, &["equals", " is "])
        || contains_unnegated_memory_write_word(lowered, "remember")
        || contains_unnegated_memory_write_word(lowered, "save")
        || contains_unnegated_memory_write_word(lowered, "store")
    {
        return true;
    }
    if contains_any(
        lowered,
        &[
            "do not log",
            "do not print",
            "do not store",
            "don't log",
            "don't print",
            "don't store",
            "must not log",
            "must not print",
            "must not store",
            "never log",
            "never print",
            "never store",
            "no secrets",
            "redact",
        ],
    ) {
        return false;
    }
    contains_any(lowered, &["actual", "value"])
}

/// True when `word` appears as a whole word that is not negated earlier in
/// the same clause -- "store the token" matches, "never store the token"
/// does not.
fn contains_unnegated_memory_write_word(lowered: &str, word: &str) -> bool {
    lowered.match_indices(word).any(|(index, _)| {
        let clause_prefix = current_clause_prefix(lowered, index);
        has_word_boundaries(lowered, index, word.len())
            && !memory_write_word_is_negated(clause_prefix)
    })
}

/// Returns the clause text preceding `index` (back to the nearest sentence
/// or clause delimiter). `index` comes from `match_indices`, so the slice
/// boundaries are valid char boundaries.
fn current_clause_prefix(text: &str, index: usize) -> &str {
    let prefix = &text[..index];
    let clause_start =
        prefix.rfind(['.', '!', '?', ';', ',', '\n']).map(|offset| offset + 1).unwrap_or(0);
    &text[clause_start..index]
}

fn memory_write_word_is_negated(clause_prefix: &str) -> bool {
    let compact = clause_prefix.split_whitespace().collect::<Vec<_>>().join(" ");
    ["do not", "don't", "must not", "never", "no", "no need to", "no need for", "without"]
        .iter()
        .any(|negation| compact.ends_with(negation))
}

fn has_word_boundaries(text: &str, index: usize, len: usize) -> bool {
    let before = text[..index].chars().next_back();
    let after = text[index + len..].chars().next();
    before.is_none_or(|ch| !memory_classifier_word_char(ch))
        && after.is_none_or(|ch| !memory_classifier_word_char(ch))
}

fn memory_classifier_word_char(ch: char) -> bool {
    ch.is_ascii_alphanumeric() || ch == '_'
}

/// Heuristic for principal-wide writes that try to weaken approvals, auth,
/// policy, or sandboxing. Explicit bypass-approval phrasing always matches;
/// defensive "never bypass ..." phrasing short-circuits to false before the
/// broader bypass/disable/ignore patterns are consulted.
fn contains_high_risk_memory_write_intent(lowered: &str) -> bool {
    if contains_any(
        lowered,
        &[
            "bypass approval",
            "disable approval",
            "never require approval",
            "skip approval",
            "without approval",
        ],
    ) {
        return true;
    }
    if contains_any(
        lowered,
        &[
            "do not bypass",
            "do not disable",
            "do not ignore",
            "must not bypass",
            "must not disable",
            "must not ignore",
            "never bypass",
            "never disable",
            "never ignore",
        ],
    ) {
        return false;
    }
    contains_any(
        lowered,
        &[
            "allow insecure remote bind",
            "allow remote bind",
            "bypass auth",
            "bypass policy",
            "bypass sandbox",
            "bypass security",
            "disable auth",
            "disable policy",
            "disable sandbox",
            "disable security",
            "ignore auth",
            "ignore policy",
            "ignore sandbox",
            "ignore security",
            "public remote bind",
            "weaken policy",
        ],
    )
}

/// Derives the provenance source reference for a write: orchestrator tape
/// when a tape sequence is present, artifact when an artifact id is, and a
/// hash-derived synthetic source otherwise.
fn memory_write_source_refs(
    provenance: &Value,
    source_hash: &str,
    fallback_session_id: &str,
) -> Vec<MemoryWriteSourceRef> {
    let run_id = provenance.get("run_id").and_then(Value::as_str).map(str::to_owned);
    let session_id = provenance
        .get("session_id")
        .and_then(Value::as_str)
        .map(str::to_owned)
        .or_else(|| Some(fallback_session_id.to_owned()));
    let tape_seq = provenance.get("seq").and_then(Value::as_i64);
    let artifact_id = provenance.get("artifact_id").and_then(Value::as_str).map(str::to_owned);
    let source_kind = if tape_seq.is_some() {
        "orchestrator_tape"
    } else if artifact_id.is_some() {
        "artifact"
    } else {
        "memory_write"
    };
    let source_id = run_id
        .clone()
        .or_else(|| artifact_id.clone())
        .unwrap_or_else(|| format!("source-{}", &source_hash[..16]));
    vec![MemoryWriteSourceRef {
        source_kind: source_kind.to_owned(),
        source_id,
        session_id,
        run_id,
        tape_seq,
        artifact_id,
    }]
}

fn memory_write_provenance(
    mut provenance: Value,
    classification: &MemoryWriteClassification,
) -> Value {
    let Value::Object(ref mut fields) = provenance else {
        return json!({
            "input": provenance,
            "memory_write": classification,
        });
    };
    fields.insert("memory_write".to_owned(), json!(classification));
    provenance
}

/// Category buckets offered by the reflect tool when distilling raw
/// observations into retain candidates.
#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum MemoryReflectionCategory {
    Facts,
    Preferences,
    WorkflowRules,
    Risks,
    TemporaryState,
}

impl MemoryReflectionCategory {
    /// Parses a category keyword, accepting singular/plural and shorthand
    /// aliases; returns `None` for unknown values.
    pub(crate) fn parse(raw: &str) -> Option<Self> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "fact" | "facts" => Some(Self::Facts),
            "preference" | "preferences" => Some(Self::Preferences),
            "workflow_rule" | "workflow_rules" | "workflow" | "rules" => Some(Self::WorkflowRules),
            "risk" | "risks" => Some(Self::Risks),
            "temporary_state" | "temporary" | "state" => Some(Self::TemporaryState),
            _ => None,
        }
    }

    /// Returns the canonical snake_case category keyword.
    #[must_use]
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Facts => "facts",
            Self::Preferences => "preferences",
            Self::WorkflowRules => "workflow_rules",
            Self::Risks => "risks",
            Self::TemporaryState => "temporary_state",
        }
    }
}

/// Input to [`reflect_memory_candidates`]: raw observations plus the
/// category whitelist and candidate cap.
#[derive(Debug, Clone, Serialize, PartialEq)]
pub(crate) struct MemoryReflectionRequest {
    pub(crate) observations: Vec<String>,
    pub(crate) allowed_categories: Vec<MemoryReflectionCategory>,
    pub(crate) max_candidates: usize,
    pub(crate) provenance: Value,
}

/// One proposed memory write produced by reflection. `retain_input` is a
/// ready-to-submit `palyra.memory.retain` payload for the candidate.
#[derive(Debug, Clone, Serialize, PartialEq)]
pub(crate) struct MemoryReflectionCandidate {
    pub(crate) category: MemoryReflectionCategory,
    pub(crate) content_text: String,
    pub(crate) confidence: f64,
    pub(crate) tags: Vec<String>,
    pub(crate) trust_label: String,
    pub(crate) retain_input: Value,
}

/// Reflection result. `durable_memory_write` is always `false`: reflection
/// only proposes candidates, it never writes.
#[derive(Debug, Clone, Serialize, PartialEq)]
pub(crate) struct MemoryReflectionOutcome {
    pub(crate) durable_memory_write: bool,
    pub(crate) candidate_count: usize,
    pub(crate) categories: Vec<String>,
    pub(crate) candidates: Vec<MemoryReflectionCandidate>,
    pub(crate) provenance: Value,
}

/// Turns raw observations into session-scoped retain candidates without
/// writing anything; callers review and submit candidates separately.
pub(crate) fn reflect_memory_candidates(
    request: MemoryReflectionRequest,
) -> MemoryReflectionOutcome {
    let mut candidates = Vec::new();
    let allowed_categories = if request.allowed_categories.is_empty() {
        vec![MemoryReflectionCategory::Facts]
    } else {
        request.allowed_categories.clone()
    };
    for observation in request.observations {
        let content_text = normalize_lifecycle_content(observation.as_str());
        if content_text.is_empty() {
            continue;
        }
        let category =
            reflection_category_for_observation(&allowed_categories, content_text.as_str());
        let confidence = reflection_confidence(category, content_text.as_str());
        let tags = lifecycle_tags(
            &["lifecycle:reflect".to_owned(), format!("category:{}", category.as_str())],
            MemoryLifecycleScope::Session,
        );
        let retain_input = json!({
            "content_text": content_text.as_str(),
            "scope": "session",
            "category": memory_write_category_for_reflection(category).as_str(),
            "confidence": confidence,
            "tags": tags.clone(),
            "provenance": request.provenance.clone(),
        });
        candidates.push(MemoryReflectionCandidate {
            category,
            content_text,
            confidence,
            tags,
            trust_label: MEMORY_TRUST_LABEL_RETRIEVED.to_owned(),
            retain_input,
        });
        if candidates.len() >= request.max_candidates {
            break;
        }
    }
    MemoryReflectionOutcome {
        durable_memory_write: false,
        candidate_count: candidates.len(),
        categories: allowed_categories
            .iter()
            .map(|category| category.as_str().to_owned())
            .collect(),
        candidates,
        provenance: request.provenance,
    }
}

fn reflection_category_for_observation(
    allowed_categories: &[MemoryReflectionCategory],
    content_text: &str,
) -> MemoryReflectionCategory {
    let Some((first, rest)) = allowed_categories.split_first() else {
        return MemoryReflectionCategory::Facts;
    };
    let normalized = content_text.to_lowercase();
    let mut selected = *first;
    let mut best_score = reflection_category_score(selected, normalized.as_str());
    for category in rest {
        let score = reflection_category_score(*category, normalized.as_str());
        if score > best_score {
            selected = *category;
            best_score = score;
        }
    }
    selected
}

fn reflection_category_score(category: MemoryReflectionCategory, normalized: &str) -> u8 {
    match category {
        MemoryReflectionCategory::Risks => {
            if contains_any(
                normalized,
                &[
                    "risk", "hazard", "unsafe", "blocker", "blocked", "failure", "failing",
                    "problem", "threat", "error",
                ],
            ) {
                4
            } else {
                0
            }
        }
        MemoryReflectionCategory::TemporaryState => {
            if contains_any(
                normalized,
                &[
                    "temporary",
                    "temporarily",
                    "transient",
                    "rollback",
                    "staging",
                    "scratch",
                    "today",
                    "current",
                    "active",
                    "token",
                    "ephemeral",
                    "provisional",
                    "pending",
                ],
            ) {
                3
            } else {
                0
            }
        }
        MemoryReflectionCategory::Preferences => {
            if contains_any(normalized, &["prefer", "preference", "prefers", "use", "default"]) {
                2
            } else {
                0
            }
        }
        MemoryReflectionCategory::WorkflowRules => {
            if contains_any(
                normalized,
                &[
                    "workflow",
                    "procedure",
                    "rule",
                    "always",
                    "never",
                    "must",
                    "should",
                    "procedure",
                    "policy",
                ],
            ) {
                2
            } else {
                0
            }
        }
        MemoryReflectionCategory::Facts => {
            if contains_any(normalized, &["decision", "decided", "fact"]) {
                1
            } else {
                0
            }
        }
    }
}

fn memory_write_category_for_reflection(category: MemoryReflectionCategory) -> MemoryWriteCategory {
    match category {
        MemoryReflectionCategory::Facts | MemoryReflectionCategory::Risks => {
            MemoryWriteCategory::Fact
        }
        MemoryReflectionCategory::Preferences => MemoryWriteCategory::Preference,
        MemoryReflectionCategory::WorkflowRules => MemoryWriteCategory::Procedure,
        MemoryReflectionCategory::TemporaryState => MemoryWriteCategory::TransientRuntimeFact,
    }
}

fn reflection_confidence(category: MemoryReflectionCategory, content_text: &str) -> f64 {
    let base: f64 = match category {
        MemoryReflectionCategory::Facts => 0.68,
        MemoryReflectionCategory::Preferences => 0.72,
        MemoryReflectionCategory::WorkflowRules => 0.76,
        MemoryReflectionCategory::Risks => 0.64,
        MemoryReflectionCategory::TemporaryState => 0.52,
    };
    if content_text.len() >= 24 {
        (base + 0.06).min(0.92)
    } else {
        base
    }
}

/// Canonical text normalization for memory content: control characters
/// become spaces and whitespace runs collapse to single spaces. Exact-match
/// dedupe compares this form, so it must stay stable.
pub(crate) fn normalize_lifecycle_content(raw: &str) -> String {
    raw.chars()
        .map(|ch| if ch.is_control() { ' ' } else { ch })
        .collect::<String>()
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

fn contains_any(input: &str, patterns: &[&str]) -> bool {
    patterns.iter().any(|pattern| input.contains(pattern))
}

/// Prepends the standard lifecycle/scope/trust tags to caller tags and
/// normalizes the result. The base tags come first so the
/// [`MAX_MEMORY_TOOL_TAGS`] cap never drops them.
pub(crate) fn lifecycle_tags(raw: &[String], scope: MemoryLifecycleScope) -> Vec<String> {
    let mut tags = vec![
        "lifecycle:memory".to_owned(),
        format!("scope:{}", scope.as_str()),
        format!("trust:{MEMORY_TRUST_LABEL_RETRIEVED}"),
    ];
    tags.extend(raw.iter().cloned());
    normalize_lifecycle_tags(tags.as_slice())
}

/// Adds host-owned classification metadata ahead of caller tags so the tag
/// cap cannot discard it. Caller values in either reserved namespace are
/// removed before the canonical markers are written.
fn lifecycle_write_tags(
    raw: &[String],
    scope: MemoryLifecycleScope,
    category: MemoryWriteCategory,
    source_hash: &str,
) -> Vec<String> {
    let mut tags = vec![
        format!("memory_write:{}", category.as_str()),
        format!("source_hash:{}", source_hash.get(..16).unwrap_or("short")),
    ];
    tags.extend(raw.iter().filter(|tag| !is_reserved_lifecycle_metadata_tag(tag)).cloned());
    lifecycle_tags(tags.as_slice(), scope)
}

fn is_reserved_lifecycle_metadata_tag(tag: &str) -> bool {
    let normalized = tag.trim().to_ascii_lowercase();
    normalized.starts_with("memory_write:") || normalized.starts_with("source_hash:")
}

/// Lowercases, restricts to a safe tag charset, deduplicates preserving
/// first occurrence, and caps at [`MAX_MEMORY_TOOL_TAGS`].
fn normalize_lifecycle_tags(raw: &[String]) -> Vec<String> {
    let mut normalized = Vec::new();
    for tag in raw {
        let trimmed = tag.trim().to_ascii_lowercase();
        if trimmed.is_empty() {
            continue;
        }
        if !trimmed.chars().all(|ch| {
            ch.is_ascii_lowercase() || ch.is_ascii_digit() || matches!(ch, ':' | '_' | '-' | '.')
        }) {
            continue;
        }
        if !normalized.iter().any(|existing| existing == &trimmed) {
            normalized.push(trimmed);
        }
        if normalized.len() >= MAX_MEMORY_TOOL_TAGS {
            break;
        }
    }
    normalized
}

fn merge_memory_tags(
    existing: &[String],
    requested: &[String],
    scope: MemoryLifecycleScope,
    category: MemoryWriteCategory,
    source_hash: &str,
) -> Vec<String> {
    let mut merged = existing.to_vec();
    merged.extend(requested.iter().cloned());
    lifecycle_write_tags(merged.as_slice(), scope, category, source_hash)
}

/// Resolves a caller-provided TTL into an absolute expiry timestamp.
/// `ttl_ms` (relative) and `ttl_unix_ms` (absolute) are mutually exclusive.
///
/// # Errors
/// Returns `InvalidArgument` when both fields are set, `ttl_ms` is not
/// positive, or `ttl_unix_ms` is not in the future; also fails when the
/// system clock cannot produce a current timestamp.
#[allow(clippy::result_large_err)]
pub(crate) fn ttl_unix_ms_from_input(
    ttl_ms: Option<i64>,
    ttl_unix_ms: Option<i64>,
) -> Result<Option<i64>, Status> {
    let now = current_unix_ms_status()?;
    match (ttl_ms, ttl_unix_ms) {
        (Some(_), Some(_)) => Err(Status::invalid_argument(
            "memory retention input must set only one of ttl_ms or ttl_unix_ms",
        )),
        (Some(value), None) if value > 0 => Ok(Some(now.saturating_add(value))),
        (Some(_), None) => Err(Status::invalid_argument("ttl_ms must be a positive integer")),
        (None, Some(value)) if value > now => Ok(Some(value)),
        (None, Some(_)) => Err(Status::invalid_argument("ttl_unix_ms must be in the future")),
        (None, None) => Ok(None),
    }
}

/// Maps a journal memory record to its proto message; `content_text` is
/// redacted on the way out.
pub(crate) fn memory_item_message(item: &MemoryItemRecord) -> memory_v1::MemoryItem {
    let session_reference = optional_canonical_id(&item.session_id);
    memory_v1::MemoryItem {
        v: palyra_common::CANONICAL_PROTOCOL_MAJOR,
        memory_id: Some(common_v1::CanonicalId { ulid: item.memory_id.clone() }),
        principal: item.principal.clone(),
        channel: item.channel.clone().unwrap_or_default(),
        session_id: session_reference,
        source: memory_source_to_proto(item.source),
        content_text: redact_memory_text_for_output(item.content_text.as_str()),
        content_hash: item.content_hash.clone(),
        tags: item.tags.clone(),
        confidence: item.confidence.unwrap_or_default(),
        ttl_unix_ms: item.ttl_unix_ms.unwrap_or_default(),
        created_at_unix_ms: item.created_at_unix_ms,
        updated_at_unix_ms: item.updated_at_unix_ms,
    }
}

/// Maps a search hit to its proto message, redacting the snippet and
/// optionally attaching the score breakdown.
pub(crate) fn memory_search_hit_message(
    hit: &MemorySearchHit,
    include_score_breakdown: bool,
) -> memory_v1::MemorySearchHit {
    memory_v1::MemorySearchHit {
        item: Some(memory_item_message(&hit.item)),
        snippet: redact_memory_text_for_output(hit.snippet.as_str()),
        score: hit.score,
        breakdown: if include_score_breakdown {
            Some(memory_v1::MemoryScoreBreakdown {
                lexical_score: hit.breakdown.lexical_score,
                vector_score: hit.breakdown.vector_score,
                recency_score: hit.breakdown.recency_score,
                final_score: hit.breakdown.final_score,
            })
        } else {
            None
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn classification_input(content_text: &str) -> MemoryWriteClassificationInput {
        MemoryWriteClassificationInput {
            principal: "user:alice".to_owned(),
            channel: Some("discord:channel:one".to_owned()),
            session_id: "01H00000000000000000000001".to_owned(),
            scope: MemoryLifecycleScope::Session,
            content_text: content_text.to_owned(),
            category_hint: None,
            confidence: 0.86,
            ttl_unix_ms: None,
            provenance: json!({ "run_id": "run-1", "seq": 7 }),
            now_unix_ms: 1_700_000_000_000,
        }
    }

    #[test]
    fn write_classifier_marks_sensitive_memory_for_review() {
        let classification = classify_memory_write(classification_input(
            "The deployment password is secret and must be remembered.",
        ));

        assert_eq!(classification.sensitivity, MemoryWriteSensitivity::Sensitive);
        assert_eq!(classification.approval_state, MemoryWriteApprovalState::Required);
        assert!(classification.reason_codes.iter().any(|reason| reason == "sensitivity:sensitive"));
        assert_eq!(classification.source_refs[0].source_kind, "orchestrator_tape");
    }

    #[test]
    fn write_classifier_requires_review_for_persistent_runtime_rules() {
        let mut input = classification_input(
            "Workflow rules: never log secrets, redact tokens, follow approval policy, and do not bypass sandbox guardrails.",
        );
        input.scope = MemoryLifecycleScope::Principal;
        input.category_hint = Some(MemoryWriteCategory::Procedure);

        let classification = classify_memory_write(input);

        assert_eq!(classification.sensitivity, MemoryWriteSensitivity::Normal);
        assert_eq!(classification.category, MemoryWriteCategory::Procedure);
        assert_eq!(classification.approval_state, MemoryWriteApprovalState::Required);
        assert!(classification
            .reason_codes
            .iter()
            .any(|reason| reason == "policy:operator_review_for_runtime_rule"));
        assert!(
            !classification.reason_codes.iter().any(|reason| reason.starts_with("sensitivity:")),
            "safe defensive rules should not be treated as secret storage or policy bypass"
        );
    }

    #[test]
    fn write_classifier_allows_safe_principal_preferences() {
        let mut input = classification_input("User prefers concise status summaries.");
        input.scope = MemoryLifecycleScope::Principal;
        input.category_hint = Some(MemoryWriteCategory::Preference);

        let classification = classify_memory_write(input);

        assert_eq!(classification.sensitivity, MemoryWriteSensitivity::Normal);
        assert_eq!(classification.category, MemoryWriteCategory::Preference);
        assert_eq!(classification.approval_state, MemoryWriteApprovalState::NotRequired);
        assert!(!classification
            .reason_codes
            .iter()
            .any(|reason| { reason.starts_with("policy:operator_review") }));
    }

    #[test]
    fn write_classifier_allows_session_scoped_safe_runtime_rules() {
        let mut input = classification_input(
            "Workflow rules for this session: inspect files, run available tests, and summarize results.",
        );
        input.category_hint = Some(MemoryWriteCategory::Procedure);
        let classification = classify_memory_write(input);

        assert_eq!(classification.sensitivity, MemoryWriteSensitivity::Normal);
        assert_eq!(classification.category, MemoryWriteCategory::Procedure);
        assert_eq!(classification.approval_state, MemoryWriteApprovalState::NotRequired);
        assert!(!classification
            .reason_codes
            .iter()
            .any(|reason| reason == "policy:operator_review_for_runtime_rule"));
    }

    #[test]
    fn write_classifier_allows_admin_persistent_runtime_rules() {
        let mut input = classification_input(
            "Workflow rules: always inspect release artifacts and write concise rollout reports.",
        );
        input.principal = "admin:ops".to_owned();
        input.scope = MemoryLifecycleScope::Principal;
        input.category_hint = Some(MemoryWriteCategory::Procedure);

        let classification = classify_memory_write(input);

        assert_eq!(classification.category, MemoryWriteCategory::Procedure);
        assert_eq!(classification.approval_state, MemoryWriteApprovalState::NotRequired);
        assert!(!classification
            .reason_codes
            .iter()
            .any(|reason| reason == "policy:operator_review_for_runtime_rule"));
    }

    #[test]
    fn write_classifier_marks_safe_phrase_secret_values_for_review() {
        let classification = classify_memory_write(classification_input(
            "Never log secrets. The admin token is abc123.",
        ));

        assert_eq!(classification.sensitivity, MemoryWriteSensitivity::Sensitive);
        assert_eq!(classification.approval_state, MemoryWriteApprovalState::Required);
        assert!(classification.reason_codes.iter().any(|reason| reason == "sensitivity:sensitive"));
    }

    #[test]
    fn write_classifier_marks_safe_phrase_positive_store_intent_for_review() {
        let classification = classify_memory_write(classification_input(
            "Never log secrets. Store the admin token abc123 for future deployments.",
        ));

        assert_eq!(classification.sensitivity, MemoryWriteSensitivity::Sensitive);
        assert_eq!(classification.approval_state, MemoryWriteApprovalState::Required);
        assert!(classification.reason_codes.iter().any(|reason| reason == "sensitivity:sensitive"));
    }

    #[test]
    fn write_classifier_marks_comma_prefixed_positive_save_intent_for_review() {
        let classification = classify_memory_write(classification_input(
            "No extra steps, save the admin token abc123 for deployments.",
        ));

        assert_eq!(classification.sensitivity, MemoryWriteSensitivity::Sensitive);
        assert_eq!(classification.approval_state, MemoryWriteApprovalState::Required);
        assert!(classification.reason_codes.iter().any(|reason| reason == "sensitivity:sensitive"));
    }

    #[test]
    fn write_classifier_allows_negated_secret_storage_instruction() {
        let classification =
            classify_memory_write(classification_input("Do not store the admin token abc123."));

        assert_eq!(classification.sensitivity, MemoryWriteSensitivity::Normal);
        assert_eq!(classification.approval_state, MemoryWriteApprovalState::NotRequired);
        assert!(!classification
            .reason_codes
            .iter()
            .any(|reason| reason == "sensitivity:sensitive"));
    }

    #[test]
    fn write_classifier_bounds_transient_runtime_facts_with_ttl() {
        let mut input = classification_input("The current run is waiting on a retry.");
        input.category_hint = Some(MemoryWriteCategory::TransientRuntimeFact);
        let classification = classify_memory_write(input);

        assert_eq!(classification.category, MemoryWriteCategory::TransientRuntimeFact);
        assert_eq!(classification.ttl_unix_ms, Some(1_700_000_000_000 + MEMORY_TRANSIENT_TTL_MS));
    }

    #[test]
    fn write_classifier_uses_structured_category_hint_for_corrections() {
        let mut input =
            classification_input("Browser test project should use TypeScript and Playwright.");
        input.category_hint = Some(MemoryWriteCategory::Correction);
        let classification = classify_memory_write(input);

        assert_eq!(classification.category, MemoryWriteCategory::Correction);
    }

    #[test]
    fn duplicate_queries_include_structured_replacement_terms_for_correction_recall() {
        let replacement_terms = vec!["Vitest".to_owned(), "browser test project".to_owned()];
        let queries = lifecycle_duplicate_search_queries(
            "Browser test project should use TypeScript and Playwright.",
            replacement_terms.as_slice(),
        );

        assert!(queries.iter().any(|query| query.contains("vitest")));
    }

    #[test]
    fn correction_conflict_matches_existing_project_preference() {
        let replacement_terms = vec!["Vitest".to_owned(), "browser test project".to_owned()];
        assert!(lifecycle_conflict_matches(
            MemoryWriteCategory::Correction,
            replacement_terms.as_slice(),
            "Browser test project should use TypeScript and Playwright with concise reports.",
            MemoryWriteCategory::Preference,
            "Project preference: for this browser test project use TypeScript, Vitest, and concise reports.",
        ));
    }

    #[test]
    fn lifecycle_conflict_rejects_ordinary_preference_overlap() {
        let replacement_terms = Vec::new();
        assert!(!lifecycle_conflict_matches(
            MemoryWriteCategory::Preference,
            replacement_terms.as_slice(),
            "I prefer TypeScript Playwright reports to be written in pirate voice for every project.",
            MemoryWriteCategory::Preference,
            "Project preference: TypeScript Playwright reports should use concise summaries.",
        ));
    }

    #[test]
    fn lifecycle_conflict_rejects_untyped_status_note_overlap() {
        let replacement_terms = vec!["pirate voice".to_owned()];
        assert!(!lifecycle_conflict_matches(
            MemoryWriteCategory::Correction,
            replacement_terms.as_slice(),
            "TypeScript Playwright reports should use concise prose.",
            MemoryWriteCategory::Fact,
            "Project status note: TypeScript Playwright reports document normal CI coverage and release notes.",
        ));
    }

    #[test]
    fn lifecycle_conflict_requires_structured_replacement_term_reference() {
        let replacement_terms = Vec::new();
        assert!(!lifecycle_conflict_matches(
            MemoryWriteCategory::Correction,
            replacement_terms.as_slice(),
            "TypeScript Playwright reports should use concise prose for every project.",
            MemoryWriteCategory::Preference,
            "Project preference: TypeScript Playwright reports should use concise summaries.",
        ));
    }

    #[test]
    fn lifecycle_conflict_overlap_counts_all_shared_terms_for_ranking() {
        let overlap = lifecycle_conflict_overlap_count(
            "Prefer TypeScript Vitest concise reports sandbox workspace boundaries",
            "Existing TypeScript Vitest concise reports sandbox workspace boundaries preference",
        );

        assert!(
            overlap > LIFECYCLE_CONFLICT_MIN_OVERLAP,
            "overlap count must not be capped at the minimum threshold"
        );
    }

    #[test]
    fn lifecycle_scope_scan_selects_strongest_scoped_conflict() {
        let weak = lifecycle_test_memory_item(
            "01ARZ3NDEKTSV4RRFFQ69G5W01",
            None,
            None,
            "Existing Vitest preference for TypeScript note",
        );
        let strong = lifecycle_test_memory_item(
            "01ARZ3NDEKTSV4RRFFQ69G5W02",
            None,
            None,
            "Existing TypeScript Vitest reports sandbox workspace boundaries preference",
        );
        let replacement_terms =
            vec!["Vitest".to_owned(), "sandbox workspace boundaries".to_owned()];

        let conflict = lifecycle_conflict_from_scope_items(
            vec![weak, strong],
            MemoryWriteCategory::Correction,
            replacement_terms.as_slice(),
            "Prefer TypeScript Playwright reports sandbox workspace boundaries.",
            None,
            None,
        )
        .expect("stronger conflict should be selected");

        assert_eq!(conflict.item.memory_id, "01ARZ3NDEKTSV4RRFFQ69G5W02");
    }

    #[test]
    fn lifecycle_near_duplicate_rejects_structured_category_mismatch() {
        let mut item = lifecycle_test_memory_item(
            "01ARZ3NDEKTSV4RRFFQ69G5W05",
            None,
            None,
            "Project status note: TypeScript Playwright reports document normal CI coverage.",
        );
        item.tags.clear();

        assert!(!lifecycle_near_duplicate_categories_compatible(
            MemoryWriteCategory::Preference,
            &item
        ));
    }

    #[test]
    fn lifecycle_write_tags_replace_reserved_caller_metadata_before_the_tag_cap() {
        let mut raw = vec!["memory_write:preference".to_owned(), "source_hash:forged".to_owned()];
        raw.extend((0..MAX_MEMORY_TOOL_TAGS).map(|index| format!("caller:{index}")));

        let tags = lifecycle_write_tags(
            raw.as_slice(),
            MemoryLifecycleScope::Principal,
            MemoryWriteCategory::Fact,
            "0123456789abcdef0123456789abcdef",
        );

        assert!(tags.iter().any(|tag| tag == "memory_write:fact"));
        assert!(tags.iter().any(|tag| tag == "source_hash:0123456789abcdef"));
        assert!(!tags.iter().any(|tag| tag == "memory_write:preference"));
        assert!(!tags.iter().any(|tag| tag == "source_hash:forged"));
        assert!(tags.len() <= MAX_MEMORY_TOOL_TAGS);
    }

    #[test]
    fn lifecycle_category_recovers_the_last_historical_authoritative_marker() {
        let mut item = lifecycle_test_memory_item(
            "01ARZ3NDEKTSV4RRFFQ69G5W07",
            None,
            None,
            "Project status note: release validation is pending.",
        );
        item.tags = vec!["memory_write:preference".to_owned(), "memory_write:fact".to_owned()];

        assert_eq!(lifecycle_item_write_category(&item), MemoryWriteCategory::Fact);
    }

    #[test]
    fn lifecycle_near_duplicate_requires_strong_lexical_overlap() {
        assert!(
            lifecycle_near_duplicate_texts_compatible(
                "Project preference: TypeScript Playwright reports should use concise summaries.",
                "Preference: use TypeScript Playwright reports with concise summaries.",
            ),
            "same preference phrased slightly differently should still merge"
        );

        assert!(
            !lifecycle_near_duplicate_texts_compatible(
                "Action item 1/3 (S078, source: tasks/meeting-notes.md): Alice must rotate the staging API token by 2026-06-10.",
                "Action item 2/3 (S078, source: tasks/meeting-notes.md): Boris must update the Windows PATH onboarding note before the next installer test.",
            ),
            "separate action items with shared source boilerplate must stay distinct"
        );
    }

    #[test]
    fn lifecycle_scope_scan_rejects_items_outside_requested_scope() {
        let channel_item = lifecycle_test_memory_item(
            "01ARZ3NDEKTSV4RRFFQ69G5W03",
            Some("slack"),
            None,
            "Existing TypeScript Vitest reports sandbox workspace boundaries preference",
        );
        let session_item = lifecycle_test_memory_item(
            "01ARZ3NDEKTSV4RRFFQ69G5W04",
            None,
            Some("01ARZ3NDEKTSV4RRFFQ69G5S01"),
            "Existing TypeScript Vitest reports sandbox workspace boundaries preference",
        );
        let replacement_terms =
            vec!["Vitest".to_owned(), "sandbox workspace boundaries".to_owned()];

        let conflict = lifecycle_conflict_from_scope_items(
            vec![channel_item, session_item],
            MemoryWriteCategory::Correction,
            replacement_terms.as_slice(),
            "Prefer TypeScript Playwright reports sandbox workspace boundaries.",
            None,
            None,
        );

        assert!(conflict.is_none(), "principal-scope scans must not reuse channel/session items");
    }

    #[test]
    fn correction_replacement_content_uses_structured_corrected_text() {
        let mut input = classification_input("For browser checks use Playwright.");
        input.category_hint = Some(MemoryWriteCategory::Correction);
        let classification = classify_memory_write(input);

        assert_eq!(
            lifecycle_replacement_content(
                &classification,
                "For browser checks use Playwright. Keep concise reports.",
            ),
            "For browser checks use Playwright. Keep concise reports."
        );
    }

    #[test]
    fn preference_replacement_content_uses_requested_text_only() {
        let classification = MemoryWriteClassification {
            category: MemoryWriteCategory::Preference,
            ..classify_memory_write(classification_input(
                "Harness project rules: 1) Brief reports. 2) Sandbox boundary tests write only inside workspace.",
            ))
        };
        let replacement = lifecycle_replacement_content(
            &classification,
            "Harness project rules: 1) Brief reports. 2) Sandbox boundary tests write only inside workspace.",
        );

        assert!(
            replacement.contains("Brief reports") && replacement.contains("Sandbox boundary tests"),
            "replacement preference should include requested rules: {replacement}"
        );
        assert!(!replacement.contains("TypeScript"));
        assert!(!replacement.contains("Do not use Vitest"));
    }

    #[test]
    fn channel_scope_resolution_can_default_to_context_channel() {
        let resolved = resolve_memory_channel_scope(Some("cli"), None)
            .expect("context channel default should be valid");

        assert_eq!(resolved.as_deref(), Some("cli"));
    }

    #[test]
    fn channel_scope_resolution_rejects_mismatched_requested_channel() {
        let error = resolve_memory_channel_scope(Some("cli"), Some("discord:main".to_owned()))
            .expect_err("requested channel must match authenticated channel context");

        assert_eq!(error.code(), tonic::Code::PermissionDenied);
    }

    #[test]
    fn channel_context_can_read_but_not_delete_principal_scoped_memory() {
        let item = lifecycle_test_memory_item(
            "01ARZ3NDEKTSV4RRFFQ69G5W06",
            None,
            None,
            "Principal-scoped preference",
        );

        enforce_memory_item_scope(&item, "user:ops", Some("cli"))
            .expect("principal memory should remain visible from an owning channel");
        let error = enforce_memory_item_delete_scope(&item, "user:ops", Some("cli"))
            .expect_err("channel context must not delete principal-scoped memory");

        assert_eq!(error.code(), tonic::Code::PermissionDenied);
        enforce_memory_item_delete_scope(&item, "user:ops", None)
            .expect("unscoped principal context should delete principal-scoped memory");
    }

    #[test]
    fn redact_memory_text_for_output_masks_sensitive_values() {
        let redacted = redact_memory_text_for_output(
            "Authorization: Bearer topsecret123 api_key=abc token=qwe secret=xyz",
        );

        assert!(redacted.contains("<redacted>"));
        assert!(!redacted.contains("topsecret123"));
        assert!(!redacted.contains("api_key=abc"));
        assert!(!redacted.contains("token=qwe"));
        assert!(!redacted.contains("secret=xyz"));
    }

    #[test]
    fn redacted_memory_payload_value_fails_closed_for_unusable_payloads() {
        assert_eq!(
            redacted_memory_payload_value(Err(crate::journal::JournalError::EmptyPath)),
            None
        );
        assert_eq!(redacted_memory_payload_value(Ok("{}".to_owned())), None);
        assert_eq!(redacted_memory_payload_value(Ok(r#"{"value":42}"#.to_owned())), None);
    }

    fn lifecycle_test_memory_item(
        memory_id: &str,
        channel: Option<&str>,
        session_id: Option<&str>,
        content_text: &str,
    ) -> MemoryItemRecord {
        MemoryItemRecord {
            memory_id: memory_id.to_owned(),
            principal: "user:ops".to_owned(),
            channel: channel.map(str::to_owned),
            session_id: session_id.map(str::to_owned),
            source: MemorySource::Manual,
            content_text: content_text.to_owned(),
            content_hash: format!("hash-{memory_id}"),
            tags: vec!["memory_write:preference".to_owned()],
            confidence: Some(0.9),
            ttl_unix_ms: None,
            created_at_unix_ms: 1_700_000_000_000,
            updated_at_unix_ms: 1_700_000_000_000,
        }
    }
}
