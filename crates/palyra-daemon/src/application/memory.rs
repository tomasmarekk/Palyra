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
const MEMORY_WRITE_HIGH_RISK_PATTERNS: &[&str] = &[
    "approval",
    "auth",
    "cryptography",
    "deny",
    "policy",
    "private",
    "remote bind",
    "sandbox",
    "security",
];
const MEMORY_TRANSIENT_TTL_MS: i64 = 24 * 60 * 60 * 1_000;

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
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum MemoryWriteSensitivity {
    Normal,
    Sensitive,
    HighRisk,
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum MemoryWriteApprovalState {
    NotRequired,
    Required,
}

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

#[derive(Debug, Clone)]
pub(crate) struct MemoryWriteClassificationInput {
    pub(crate) principal: String,
    pub(crate) channel: Option<String>,
    pub(crate) session_id: String,
    pub(crate) scope: MemoryLifecycleScope,
    pub(crate) content_text: String,
    pub(crate) confidence: f64,
    pub(crate) ttl_unix_ms: Option<i64>,
    pub(crate) provenance: Value,
    pub(crate) now_unix_ms: i64,
}

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
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) write_classification: Option<MemoryWriteClassification>,
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
        confidence,
        ttl_unix_ms: request.ttl_unix_ms,
        provenance: request.provenance.clone(),
        now_unix_ms: current_unix_ms_status()?,
    });
    request.ttl_unix_ms = classification.ttl_unix_ms;
    request.tags.push(format!("memory_write:{}", classification.category.as_str()));
    request
        .tags
        .push(format!("source_hash:{}", classification.source_hash.get(..16).unwrap_or("short")));
    request.tags = lifecycle_tags(request.tags.as_slice(), request.scope);
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
    if request.scope == MemoryLifecycleScope::Principal
        && !principal_has_sensitive_service_role(
            request.principal.as_str(),
            SensitiveServiceRole::AdminOrSystem,
        )
    {
        return Ok(memory_retain_outcome(MemoryRetainOutcomeInput {
            status: MemoryLifecycleStatus::NeedsReview,
            reason: "principal-scoped memory retention requires admin/system review",
            scope: request.scope,
            durable_memory_write: false,
            item: None,
            matched_memory_id: None,
            provenance: request.provenance,
            write_classification: Some(classification.clone()),
        }));
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

pub(crate) fn classify_memory_write(
    input: MemoryWriteClassificationInput,
) -> MemoryWriteClassification {
    let normalized = normalize_lifecycle_content(input.content_text.as_str());
    let lowered = normalized.to_ascii_lowercase();
    let category = classify_memory_write_category(lowered.as_str());
    let sensitivity = classify_memory_write_sensitivity(lowered.as_str(), input.scope);
    let ttl_unix_ms = match (category, input.ttl_unix_ms) {
        (MemoryWriteCategory::TransientRuntimeFact, None) => {
            Some(input.now_unix_ms.saturating_add(MEMORY_TRANSIENT_TTL_MS))
        }
        (_, ttl) => ttl,
    };
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
        reason_codes.push("scope:principal_review".to_owned());
    }
    match sensitivity {
        MemoryWriteSensitivity::Normal => {}
        MemoryWriteSensitivity::Sensitive => reason_codes.push("sensitivity:sensitive".to_owned()),
        MemoryWriteSensitivity::HighRisk => reason_codes.push("sensitivity:high_risk".to_owned()),
    }
    if !principal_has_sensitive_service_role(
        input.principal.as_str(),
        SensitiveServiceRole::AdminOrSystem,
    ) && matches!(category, MemoryWriteCategory::Procedure | MemoryWriteCategory::Constraint)
    {
        reason_codes.push("policy:operator_review_for_runtime_rule".to_owned());
    }

    let approval_state = if input.confidence < MEMORY_RETAIN_LOW_CONFIDENCE_THRESHOLD
        || input.scope == MemoryLifecycleScope::Principal
        || sensitivity != MemoryWriteSensitivity::Normal
        || reason_codes.iter().any(|reason| reason == "policy:operator_review_for_runtime_rule")
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

fn classify_memory_write_category(lowered: &str) -> MemoryWriteCategory {
    if contains_any(lowered, &["correction", "actually", "instead of", "replace "]) {
        MemoryWriteCategory::Correction
    } else if contains_any(lowered, &["prefer", "preference", "likes ", "wants "]) {
        MemoryWriteCategory::Preference
    } else if contains_any(lowered, &["procedure", "runbook", "workflow", "steps:", "checklist"]) {
        MemoryWriteCategory::Procedure
    } else if contains_any(lowered, &["must ", "must not", "always ", "never ", "constraint"]) {
        MemoryWriteCategory::Constraint
    } else if contains_any(lowered, &["decision", "decided", "we chose", "selected "]) {
        MemoryWriteCategory::Decision
    } else if contains_any(lowered, &["temporary", "today", "current run", "for this run"]) {
        MemoryWriteCategory::TransientRuntimeFact
    } else {
        MemoryWriteCategory::Fact
    }
}

fn classify_memory_write_sensitivity(
    lowered: &str,
    scope: MemoryLifecycleScope,
) -> MemoryWriteSensitivity {
    if contains_any(lowered, MEMORY_WRITE_SENSITIVE_PATTERNS) {
        MemoryWriteSensitivity::Sensitive
    } else if scope == MemoryLifecycleScope::Principal
        && contains_any(lowered, MEMORY_WRITE_HIGH_RISK_PATTERNS)
    {
        MemoryWriteSensitivity::HighRisk
    } else {
        MemoryWriteSensitivity::Normal
    }
}

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

fn contains_any(input: &str, patterns: &[&str]) -> bool {
    patterns.iter().any(|pattern| input.contains(pattern))
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
    fn write_classifier_bounds_transient_runtime_facts_with_ttl() {
        let classification = classify_memory_write(classification_input(
            "Today the current run is waiting on a retry.",
        ));

        assert_eq!(classification.category, MemoryWriteCategory::TransientRuntimeFact);
        assert_eq!(classification.ttl_unix_ms, Some(1_700_000_000_000 + MEMORY_TRANSIENT_TTL_MS));
    }
}
