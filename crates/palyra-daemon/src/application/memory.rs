use std::sync::Arc;

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
        (Some(context_channel), Some(item_channel)) => {
            if context_channel != item_channel {
                return Err(Status::permission_denied(
                    "memory item channel does not match context",
                ));
            }
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

pub(crate) fn redact_memory_text_for_output(raw: &str) -> String {
    if raw.is_empty() {
        return String::new();
    }

    let payload = json!({ "value": raw });
    let redacted_payload = match crate::journal::redact_payload_json(payload.to_string().as_bytes())
    {
        Ok(redacted) => redacted,
        Err(_) => return raw.to_owned(),
    };
    match serde_json::from_str::<Value>(redacted_payload.as_str()) {
        Ok(Value::Object(fields)) => fields
            .get("value")
            .and_then(Value::as_str)
            .map(str::to_owned)
            .unwrap_or_else(|| raw.to_owned()),
        _ => raw.to_owned(),
    }
}

pub(crate) const MEMORY_CONTEXT_FENCE_VERSION: &str = "palyra.memory_context.v2";
pub(crate) const MEMORY_TRUST_LABEL_RETRIEVED: &str = "retrieved_memory";
const MEMORY_RETAIN_LOW_CONFIDENCE_THRESHOLD: f64 = 0.45;
const MEMORY_RETAIN_NEAR_DUPLICATE_SCORE: f64 = 0.92;
const MEMORY_RETAIN_DEDUPE_MIN_SCORE: f64 = 0.55;

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum MemoryLifecycleScope {
    Session,
    Channel,
    Principal,
}

impl MemoryLifecycleScope {
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

    #[must_use]
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Session => "session",
            Self::Channel => "channel",
            Self::Principal => "principal",
        }
    }
}

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

#[derive(Debug, Clone, Serialize, PartialEq)]
pub(crate) struct MemoryLifecycleRetainRequest {
    pub(crate) principal: String,
    pub(crate) channel: Option<String>,
    pub(crate) session_id: String,
    pub(crate) scope: MemoryLifecycleScope,
    pub(crate) source: MemorySource,
    pub(crate) content_text: String,
    pub(crate) tags: Vec<String>,
    pub(crate) confidence: Option<f64>,
    pub(crate) ttl_unix_ms: Option<i64>,
    pub(crate) provenance: Value,
}

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
    pub(crate) provenance: Value,
}

pub(crate) struct MemoryLifecycleProvider {
    runtime_state: Arc<GatewayRuntimeState>,
}

impl MemoryLifecycleProvider {
    #[must_use]
    pub(crate) fn new(runtime_state: Arc<GatewayRuntimeState>) -> Self {
        Self { runtime_state }
    }

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
    request.tags = lifecycle_tags(request.tags.as_slice(), request.scope);
    let confidence = request.confidence.unwrap_or(0.75);
    if !confidence.is_finite() || !(0.0..=1.0).contains(&confidence) {
        return Err(Status::invalid_argument(
            "palyra.memory.retain confidence must be in range 0.0..=1.0",
        ));
    }

    if request.content_text.is_empty() {
        return Ok(memory_retain_outcome(
            MemoryLifecycleStatus::Rejected,
            "memory content is empty after normalization",
            request.scope,
            false,
            None,
            None,
            request.provenance,
        ));
    }
    if confidence < MEMORY_RETAIN_LOW_CONFIDENCE_THRESHOLD {
        return Ok(memory_retain_outcome(
            MemoryLifecycleStatus::NeedsReview,
            "memory confidence is below automatic retention threshold",
            request.scope,
            false,
            None,
            None,
            request.provenance,
        ));
    }

    let (channel_scope, session_scope, resource) = resolve_lifecycle_write_scope(&request)?;
    authorize_memory_action(request.principal.as_str(), "memory.ingest", resource.as_str())?;
    if request.scope == MemoryLifecycleScope::Principal
        && !principal_has_sensitive_service_role(
            request.principal.as_str(),
            SensitiveServiceRole::AdminOrSystem,
        )
    {
        return Ok(memory_retain_outcome(
            MemoryLifecycleStatus::NeedsReview,
            "principal-scoped memory retention requires admin/system review",
            request.scope,
            false,
            None,
            None,
            request.provenance,
        ));
    }

    if let Some(duplicate) = find_lifecycle_duplicate(
        runtime_state,
        &request,
        channel_scope.clone(),
        session_scope.clone(),
    )
    .await?
    {
        let merged_tags =
            merge_memory_tags(duplicate.item.tags.as_slice(), request.tags.as_slice());
        let updated = runtime_state
            .update_memory_item_lifecycle(MemoryItemLifecycleUpdateRequest {
                memory_id: duplicate.item.memory_id.clone(),
                principal: request.principal.clone(),
                channel: duplicate.item.channel.clone(),
                session_id: duplicate.item.session_id.clone(),
                tags: merged_tags,
                confidence: Some(
                    duplicate.item.confidence.unwrap_or(0.0).max(confidence).clamp(0.0, 1.0),
                ),
                ttl_unix_ms: request.ttl_unix_ms,
            })
            .await?;
        if let Some(item) = updated {
            let status = if duplicate.exact {
                MemoryLifecycleStatus::UpdatedExisting
            } else {
                MemoryLifecycleStatus::Merged
            };
            let reason = if duplicate.exact {
                "exact duplicate memory updated with lifecycle metadata"
            } else {
                "near-duplicate memory merged into existing lifecycle record"
            };
            return Ok(memory_retain_outcome(
                status,
                reason,
                request.scope,
                true,
                Some(item),
                Some(duplicate.item.memory_id),
                request.provenance,
            ));
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
    Ok(memory_retain_outcome(
        MemoryLifecycleStatus::Retained,
        "memory retained in lifecycle store",
        request.scope,
        true,
        Some(item),
        None,
        request.provenance,
    ))
}

#[derive(Debug, Clone)]
struct LifecycleDuplicate {
    item: MemoryItemRecord,
    exact: bool,
}

#[allow(clippy::result_large_err)]
async fn find_lifecycle_duplicate(
    runtime_state: &Arc<GatewayRuntimeState>,
    request: &MemoryLifecycleRetainRequest,
    channel_scope: Option<String>,
    session_scope: Option<String>,
) -> Result<Option<LifecycleDuplicate>, Status> {
    let hits = runtime_state
        .search_memory(MemorySearchRequest {
            principal: request.principal.clone(),
            channel: channel_scope,
            session_id: session_scope,
            query: request.content_text.clone(),
            top_k: 8.min(MAX_MEMORY_SEARCH_TOP_K),
            min_score: MEMORY_RETAIN_DEDUPE_MIN_SCORE,
            tags: Vec::new(),
            sources: Vec::new(),
        })
        .await?;
    for hit in hits {
        let exact =
            normalize_lifecycle_content(hit.item.content_text.as_str()) == request.content_text;
        if exact || hit.score >= MEMORY_RETAIN_NEAR_DUPLICATE_SCORE {
            return Ok(Some(LifecycleDuplicate { item: hit.item, exact }));
        }
    }
    Ok(None)
}

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

fn memory_retain_outcome(
    status: MemoryLifecycleStatus,
    reason: &str,
    scope: MemoryLifecycleScope,
    durable_memory_write: bool,
    item: Option<MemoryItemRecord>,
    matched_memory_id: Option<String>,
    provenance: Value,
) -> MemoryLifecycleRetainOutcome {
    MemoryLifecycleRetainOutcome {
        status,
        reason: reason.to_owned(),
        scope,
        trust_label: MEMORY_TRUST_LABEL_RETRIEVED.to_owned(),
        durable_memory_write,
        item,
        matched_memory_id,
        provenance,
    }
}

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

#[derive(Debug, Clone, Serialize, PartialEq)]
pub(crate) struct MemoryReflectionRequest {
    pub(crate) observations: Vec<String>,
    pub(crate) allowed_categories: Vec<MemoryReflectionCategory>,
    pub(crate) max_candidates: usize,
    pub(crate) provenance: Value,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub(crate) struct MemoryReflectionCandidate {
    pub(crate) category: MemoryReflectionCategory,
    pub(crate) content_text: String,
    pub(crate) confidence: f64,
    pub(crate) tags: Vec<String>,
    pub(crate) trust_label: String,
    pub(crate) retain_input: Value,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub(crate) struct MemoryReflectionOutcome {
    pub(crate) durable_memory_write: bool,
    pub(crate) candidate_count: usize,
    pub(crate) categories: Vec<String>,
    pub(crate) candidates: Vec<MemoryReflectionCandidate>,
    pub(crate) provenance: Value,
}

pub(crate) fn reflect_memory_candidates(
    request: MemoryReflectionRequest,
) -> MemoryReflectionOutcome {
    let mut candidates = Vec::new();
    let allowed_categories = if request.allowed_categories.is_empty() {
        vec![
            MemoryReflectionCategory::Facts,
            MemoryReflectionCategory::Preferences,
            MemoryReflectionCategory::WorkflowRules,
            MemoryReflectionCategory::Risks,
            MemoryReflectionCategory::TemporaryState,
        ]
    } else {
        request.allowed_categories.clone()
    };
    for observation in request.observations {
        let content_text = normalize_lifecycle_content(observation.as_str());
        if content_text.is_empty() {
            continue;
        }
        let category = classify_reflection_observation(content_text.as_str());
        if !allowed_categories.contains(&category) {
            continue;
        }
        let confidence = reflection_confidence(category, content_text.as_str());
        let tags = lifecycle_tags(
            &["lifecycle:reflect".to_owned(), format!("category:{}", category.as_str())],
            MemoryLifecycleScope::Session,
        );
        let retain_input = json!({
            "content_text": content_text.as_str(),
            "scope": "session",
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

fn classify_reflection_observation(content_text: &str) -> MemoryReflectionCategory {
    let lower = content_text.to_ascii_lowercase();
    if lower.contains("prefer")
        || lower.contains("preference")
        || lower.contains("likes ")
        || lower.contains("wants ")
    {
        MemoryReflectionCategory::Preferences
    } else if lower.contains("always ")
        || lower.contains("never ")
        || lower.contains("workflow")
        || lower.contains("runbook")
        || lower.contains("rule")
    {
        MemoryReflectionCategory::WorkflowRules
    } else if lower.contains("risk")
        || lower.contains("blocked")
        || lower.contains("failure")
        || lower.contains("security")
        || lower.contains("incident")
    {
        MemoryReflectionCategory::Risks
    } else if lower.contains("today")
        || lower.contains("temporary")
        || lower.contains("current")
        || lower.contains("for this run")
    {
        MemoryReflectionCategory::TemporaryState
    } else {
        MemoryReflectionCategory::Facts
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

pub(crate) fn normalize_lifecycle_content(raw: &str) -> String {
    raw.chars()
        .map(|ch| if ch.is_control() { ' ' } else { ch })
        .collect::<String>()
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

pub(crate) fn lifecycle_tags(raw: &[String], scope: MemoryLifecycleScope) -> Vec<String> {
    let mut tags = vec![
        "lifecycle:memory".to_owned(),
        format!("scope:{}", scope.as_str()),
        format!("trust:{MEMORY_TRUST_LABEL_RETRIEVED}"),
    ];
    tags.extend(raw.iter().cloned());
    normalize_lifecycle_tags(tags.as_slice())
}

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

fn merge_memory_tags(existing: &[String], requested: &[String]) -> Vec<String> {
    let mut merged = existing.to_vec();
    merged.extend(requested.iter().cloned());
    normalize_lifecycle_tags(merged.as_slice())
}

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
