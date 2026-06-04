use std::{
    path::{Component, Path, PathBuf},
    sync::Arc,
};

use palyra_common::validate_canonical_id;
use serde_json::{json, Map, Value};
use sha2::{Digest, Sha256};
use ulid::Ulid;

use crate::{
    agents::AgentResolveRequest,
    application::{
        memory::{
            enforce_memory_item_scope, normalize_lifecycle_content, redact_memory_text_for_output,
            reflect_memory_candidates, ttl_unix_ms_from_input, MemoryLifecycleProvider,
            MemoryLifecycleRetainOutcome, MemoryLifecycleRetainRequest, MemoryLifecycleScope,
            MemoryLifecycleStatus, MemoryReflectionCategory, MemoryReflectionOutcome,
            MemoryReflectionRequest, MemoryWriteCategory, MEMORY_CONTEXT_FENCE_VERSION,
            MEMORY_TRUST_LABEL_RETRIEVED,
        },
        recall::{preview_recall, RecallPreviewEnvelope, RecallRequest},
        service_authorization::authorize_memory_action,
        session_compaction::truncate_console_text,
        tool_runtime::workspace_scope::workspace_roots_with_run_launch_context,
    },
    domain::workspace::{normalize_workspace_path, normalize_workspace_prefix},
    gateway::{
        current_unix_ms, GatewayRuntimeState, ToolRuntimeExecutionContext, MAX_MEMORY_SEARCH_TOP_K,
        MAX_MEMORY_TOOL_QUERY_BYTES, MAX_MEMORY_TOOL_TAGS,
    },
    journal::{
        MemoryItemLifecycleUpdateRequest, MemoryItemRecord, MemorySearchHit, MemorySearchRequest,
        MemorySource, SessionSearchOutcome, SessionSearchRequest, WorkspaceDocumentDeleteRequest,
        WorkspaceDocumentRecord, WorkspaceDocumentWriteRequest, WorkspaceSearchHit,
        WorkspaceSearchRequest,
    },
    tool_protocol::{ToolAttestation, ToolExecutionOutcome},
    transport::grpc::auth::RequestContext,
};

const DEFAULT_MEMORY_RECALL_MAX_CANDIDATES: usize = 8;
const MAX_MEMORY_RECALL_MAX_CANDIDATES: usize = 12;
const DEFAULT_MEMORY_RECALL_PROMPT_BUDGET_TOKENS: usize = 1_800;
const MIN_MEMORY_RECALL_PROMPT_BUDGET_TOKENS: usize = 512;
const MAX_MEMORY_RECALL_PROMPT_BUDGET_TOKENS: usize = 4_096;
const MEMORY_SOURCE_VALUES: &[&str] =
    &["manual", "summary", "import", "tape:user_message", "tape:tool_result"];
const MEMORY_HITS_PRESENT_CLAIM_BOUNDARY: &str = "memory hits are retrieved evidence; do not claim no stored preference or prior fact exists unless the hits are irrelevant to the user's question";
const MEMORY_HITS_ABSENT_CLAIM_BOUNDARY: &str =
    "no memory hits were returned; do not invent stored preferences or prior facts";
const COMBINED_MEMORY_HITS_PRESENT_CLAIM_BOUNDARY: &str = "durable lifecycle or workspace memory hits are retrieved evidence; do not claim no stored preference or project fact exists unless the hits are irrelevant";
const COMBINED_MEMORY_HITS_ABSENT_CLAIM_BOUNDARY: &str =
    "no durable lifecycle or workspace memory hits were returned";
const DEFAULT_MEMORY_RETAIN_SCOPE: &str = "principal";
const DEFAULT_MEMORY_SEARCH_SCOPE: &str = "all";
const SESSION_SEARCH_HITS_PRESENT_CLAIM_BOUNDARY: &str = "session transcript hits are retrieved evidence from prior conversations; cite them as session recall, not durable memory";
const SESSION_SEARCH_HITS_ABSENT_CLAIM_BOUNDARY: &str =
    "no session transcript hits were returned; do not substitute unrelated durable memory or workspace artifacts for prior-session evidence";
const MAX_WORKSPACE_RECALL_TOOL_SNIPPET_CHARS: usize = 512;

pub(crate) fn memory_search_tool_output_payload(search_hits: &[MemorySearchHit]) -> Value {
    json!({
        "hit_count": search_hits.len(),
        "claim_boundary": memory_search_claim_boundary(search_hits.len()),
        "hits": search_hits.iter().map(|hit| {
            json!({
                "memory_id": hit.item.memory_id,
                "source": hit.item.source.as_str(),
                "snippet": redact_memory_text_for_output(hit.snippet.as_str()),
                "score": hit.score,
                "created_at_unix_ms": hit.item.created_at_unix_ms,
                "content_text": redact_memory_text_for_output(hit.item.content_text.as_str()),
                "content_hash": hit.item.content_hash,
                "tags": hit.item.tags,
                "confidence": hit.item.confidence,
                "trust_label": MEMORY_TRUST_LABEL_RETRIEVED,
                "provenance": memory_hit_provenance(hit),
                "breakdown": {
                    "lexical_score": hit.breakdown.lexical_score,
                    "vector_score": hit.breakdown.vector_score,
                    "recency_score": hit.breakdown.recency_score,
                    "final_score": hit.breakdown.final_score,
                }
            })
        }).collect::<Vec<_>>()
    })
}

pub(crate) fn workspace_search_tool_output_payload(search_hits: &[WorkspaceSearchHit]) -> Value {
    json!({
        "hit_count": search_hits.len(),
        "hits": workspace_search_tool_output_hits(search_hits),
    })
}

fn combined_memory_search_tool_output_payload(
    memory_hits: &[MemorySearchHit],
    workspace_hits: &[WorkspaceSearchHit],
    workspace_prefix: Option<&str>,
) -> Value {
    let memory_payload = memory_search_tool_output_payload(memory_hits);
    let workspace_payload = workspace_search_tool_output_payload(workspace_hits);
    let memory_hit_values =
        memory_payload.get("hits").and_then(Value::as_array).cloned().unwrap_or_default();
    let workspace_hit_values =
        workspace_payload.get("hits").and_then(Value::as_array).cloned().unwrap_or_default();
    let mut hits =
        Vec::with_capacity(memory_hit_values.len().saturating_add(workspace_hit_values.len()));
    hits.extend(memory_hit_values.iter().cloned().map(|mut hit| {
        if let Some(object) = hit.as_object_mut() {
            object.insert("hit_source".to_owned(), json!("lifecycle"));
        }
        hit
    }));
    hits.extend(workspace_hit_values.iter().cloned().map(|mut hit| {
        if let Some(object) = hit.as_object_mut() {
            object.insert("hit_source".to_owned(), json!("workspace"));
        }
        hit
    }));
    let hit_count = memory_hits.len().saturating_add(workspace_hits.len());

    json!({
        "scope": "all",
        "hit_count": hit_count,
        "memory_hit_count": memory_hits.len(),
        "workspace_hit_count": workspace_hits.len(),
        "workspace_prefix": workspace_prefix,
        "claim_boundary": if hit_count == 0 {
            COMBINED_MEMORY_HITS_ABSENT_CLAIM_BOUNDARY
        } else {
            COMBINED_MEMORY_HITS_PRESENT_CLAIM_BOUNDARY
        },
        "hits": hits,
        "memory_hits": memory_payload.get("hits").cloned().unwrap_or_else(|| Value::Array(Vec::new())),
        "workspace_hits": workspace_payload.get("hits").cloned().unwrap_or_else(|| Value::Array(Vec::new())),
    })
}

fn workspace_search_tool_output_hits(search_hits: &[WorkspaceSearchHit]) -> Vec<Value> {
    search_hits.iter().map(workspace_search_hit_tool_output_payload).collect()
}

fn workspace_search_hit_tool_output_payload(hit: &WorkspaceSearchHit) -> Value {
    let redacted_snippet = redact_memory_text_for_output(hit.snippet.as_str());
    let bounded_snippet =
        truncate_console_text(redacted_snippet.as_str(), MAX_WORKSPACE_RECALL_TOOL_SNIPPET_CHARS);

    json!({
        "document": {
            "document_id": hit.document.document_id.as_str(),
            "path": hit.document.path.as_str(),
            "parent_path": hit.document.parent_path.as_deref(),
            "title": hit.document.title.as_str(),
            "kind": hit.document.kind.as_str(),
            "document_class": hit.document.document_class.as_str(),
            "state": hit.document.state.as_str(),
            "prompt_binding": hit.document.prompt_binding.as_str(),
            "risk_state": hit.document.risk_state.as_str(),
            "risk_reasons": hit.document.risk_reasons.as_slice(),
            "pinned": hit.document.pinned,
            "manual_override": hit.document.manual_override,
            "template_id": hit.document.template_id.as_deref(),
            "template_version": hit.document.template_version,
            "source_memory_id": hit.document.source_memory_id.as_deref(),
            "latest_version": hit.document.latest_version,
            "created_at_unix_ms": hit.document.created_at_unix_ms,
            "updated_at_unix_ms": hit.document.updated_at_unix_ms,
            "deleted_at_unix_ms": hit.document.deleted_at_unix_ms,
            "last_recalled_at_unix_ms": hit.document.last_recalled_at_unix_ms,
        },
        "version": hit.version,
        "chunk_index": hit.chunk_index,
        "chunk_count": hit.chunk_count,
        "snippet": bounded_snippet,
        "score": hit.score,
        "reason": hit.reason.as_str(),
        "breakdown": {
            "lexical_score": hit.breakdown.lexical_score,
            "vector_score": hit.breakdown.vector_score,
            "recency_score": hit.breakdown.recency_score,
            "source_quality_score": hit.breakdown.source_quality_score,
            "final_score": hit.breakdown.final_score,
        },
    })
}

pub(crate) fn memory_recall_tool_output_payload(preview: &RecallPreviewEnvelope) -> Value {
    let memory_hits = memory_search_tool_output_payload(preview.memory_hits.as_slice())
        .get("hits")
        .cloned()
        .unwrap_or_else(|| Value::Array(Vec::new()));
    let workspace_hits = workspace_search_tool_output_payload(preview.workspace_hits.as_slice())
        .get("hits")
        .cloned()
        .unwrap_or_else(|| Value::Array(Vec::new()));
    json!({
        "query": preview.query,
        "memory_hit_count": preview.memory_hits.len(),
        "claim_boundary": memory_search_claim_boundary(preview.memory_hits.len()),
        "memory_hits": memory_hits,
        "workspace_hits": workspace_hits,
        "transcript_hits": preview.transcript_hits,
        "checkpoint_hits": preview.checkpoint_hits,
        "compaction_hits": preview.compaction_hits,
        "top_candidates": preview.top_candidates,
        "structured_output": preview.structured_output,
        "plan": preview.plan,
        "parameter_delta": preview.parameter_delta,
        "prompt_preview": preview.prompt_preview,
    })
}

pub(crate) fn memory_session_search_tool_output_payload(outcome: &SessionSearchOutcome) -> Value {
    let window_count = outcome.groups.iter().map(|group| group.windows.len()).sum::<usize>();
    json!({
        "query": outcome.query,
        "group_count": outcome.groups.len(),
        "window_count": window_count,
        "claim_boundary": if window_count == 0 {
            SESSION_SEARCH_HITS_ABSENT_CLAIM_BOUNDARY
        } else {
            SESSION_SEARCH_HITS_PRESENT_CLAIM_BOUNDARY
        },
        "groups": outcome.groups.iter().map(|group| {
            json!({
                "session": {
                    "session_id": group.session.session_id,
                    "session_key": group.session.session_key,
                    "title": group.session.title,
                    "preview": group.session.preview,
                    "last_run_state": group.session.last_run_state,
                    "updated_at_unix_ms": group.session.updated_at_unix_ms,
                },
                "best_score": group.best_score,
                "match_count": group.match_count,
                "lineage": group.lineage,
                "windows": group.windows,
            })
        }).collect::<Vec<_>>(),
        "diagnostics": outcome.diagnostics,
    })
}

fn memory_search_claim_boundary(hit_count: usize) -> &'static str {
    if hit_count == 0 {
        MEMORY_HITS_ABSENT_CLAIM_BOUNDARY
    } else {
        MEMORY_HITS_PRESENT_CLAIM_BOUNDARY
    }
}

pub(crate) async fn execute_memory_retain_tool(
    runtime_state: &Arc<GatewayRuntimeState>,
    context: ToolRuntimeExecutionContext<'_>,
    proposal_id: &str,
    input_json: &[u8],
) -> ToolExecutionOutcome {
    let namespace = b"palyra.memory.retain.attestation.v1";
    let parsed = match parse_memory_tool_object(input_json) {
        Ok(parsed) => parsed,
        Err(error) => {
            return memory_tool_execution_outcome(
                namespace,
                proposal_id,
                input_json,
                false,
                b"{}".to_vec(),
                format!("palyra.memory.retain {error}"),
            );
        }
    };

    let content_text = match required_string_field(&parsed, "content_text") {
        Ok(value) => value,
        Err(error) => {
            return memory_tool_execution_outcome(
                namespace,
                proposal_id,
                input_json,
                false,
                b"{}".to_vec(),
                format!("palyra.memory.retain {error}"),
            );
        }
    };
    if content_text.len() > MAX_MEMORY_TOOL_QUERY_BYTES {
        return memory_tool_execution_outcome(
            namespace,
            proposal_id,
            input_json,
            false,
            b"{}".to_vec(),
            format!(
                "palyra.memory.retain content_text exceeds {MAX_MEMORY_TOOL_QUERY_BYTES} bytes"
            ),
        );
    }
    let scope_text = memory_retain_scope_text(&parsed);
    let workspace_scope = WorkspaceMemoryRetainScope::parse(scope_text.as_str());
    let lifecycle_scope = if workspace_scope.is_none() {
        match MemoryLifecycleScope::parse(Some(scope_text.as_str())) {
            Ok(scope) => Some(scope),
            Err(error) => {
                return memory_tool_execution_outcome(
                    namespace,
                    proposal_id,
                    input_json,
                    false,
                    b"{}".to_vec(),
                    format!("palyra.memory.retain {}", error.message()),
                );
            }
        }
    } else {
        None
    };
    let (source, source_normalization) = match parsed.get("source").and_then(Value::as_str) {
        Some(raw) => match parse_memory_source_literal(raw) {
            Some(source) => (source, None),
            None => (
                MemorySource::Manual,
                Some(json!({
                    "input": raw,
                    "normalized_source": MemorySource::Manual.as_str(),
                    "reason": "unknown_source_defaulted_to_manual",
                    "valid_sources": MEMORY_SOURCE_VALUES,
                })),
            ),
        },
        None => (MemorySource::Manual, None),
    };
    let tags = match parse_string_array_field(parsed.get("tags"), "tags", MAX_MEMORY_TOOL_TAGS) {
        Ok(tags) => tags,
        Err(error) => {
            return memory_tool_execution_outcome(
                namespace,
                proposal_id,
                input_json,
                false,
                b"{}".to_vec(),
                error,
            );
        }
    };
    let category_hint = match parsed.get("category").and_then(Value::as_str) {
        Some(raw) => match MemoryWriteCategory::parse(raw) {
            Some(category) => Some(category),
            None => {
                return memory_tool_execution_outcome(
                    namespace,
                    proposal_id,
                    input_json,
                    false,
                    b"{}".to_vec(),
                    format!("palyra.memory.retain unknown category: {raw}"),
                );
            }
        },
        None => None,
    };
    let replaces_terms =
        match parse_string_array_field(parsed.get("replaces_terms"), "replaces_terms", 32) {
            Ok(terms) => terms,
            Err(error) => {
                return memory_tool_execution_outcome(
                    namespace,
                    proposal_id,
                    input_json,
                    false,
                    b"{}".to_vec(),
                    error,
                );
            }
        };
    let confidence = match parsed.get("confidence").and_then(Value::as_f64) {
        Some(value) if value.is_finite() && (0.0..=1.0).contains(&value) => Some(value),
        Some(_) => {
            return memory_tool_execution_outcome(
                namespace,
                proposal_id,
                input_json,
                false,
                b"{}".to_vec(),
                "palyra.memory.retain confidence must be in range 0.0..=1.0".to_owned(),
            );
        }
        None => None,
    };
    let ttl_unix_ms = match ttl_unix_ms_from_input(
        parsed.get("ttl_ms").and_then(Value::as_i64),
        parsed.get("ttl_unix_ms").and_then(Value::as_i64),
    ) {
        Ok(value) => value,
        Err(error) => {
            return memory_tool_execution_outcome(
                namespace,
                proposal_id,
                input_json,
                false,
                b"{}".to_vec(),
                format!("palyra.memory.retain {}", error.message()),
            );
        }
    };
    let provenance = retain_tool_provenance(context, proposal_id);

    if let Some(scope) = workspace_scope {
        return execute_workspace_memory_retain_tool(
            runtime_state,
            context,
            proposal_id,
            input_json,
            &parsed,
            scope,
            content_text,
            source,
            category_hint,
            replaces_terms,
            tags,
            confidence,
            ttl_unix_ms,
            provenance,
            source_normalization,
        )
        .await;
    }

    let scope = lifecycle_scope.expect("non-workspace memory retain scope should be parsed");

    let provider = MemoryLifecycleProvider::new(Arc::clone(runtime_state));
    let outcome = match provider
        .retain(MemoryLifecycleRetainRequest {
            principal: context.principal.to_owned(),
            channel: context.channel.map(str::to_owned),
            session_id: context.session_id.to_owned(),
            scope,
            source,
            content_text,
            category_hint,
            replaces_terms,
            tags,
            confidence,
            ttl_unix_ms,
            provenance,
        })
        .await
    {
        Ok(outcome) => outcome,
        Err(error) => {
            return memory_tool_execution_outcome(
                namespace,
                proposal_id,
                input_json,
                false,
                b"{}".to_vec(),
                format!("palyra.memory.retain failed: {}", error.message()),
            );
        }
    };
    serialize_memory_lifecycle_outcome(
        namespace,
        proposal_id,
        input_json,
        &outcome,
        source_normalization,
    )
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WorkspaceMemoryRetainScope {
    Workspace,
    Project,
}

impl WorkspaceMemoryRetainScope {
    fn parse(scope: &str) -> Option<Self> {
        match scope {
            "workspace" => Some(Self::Workspace),
            "project" => Some(Self::Project),
            _ => None,
        }
    }

    const fn as_str(self) -> &'static str {
        match self {
            Self::Workspace => "workspace",
            Self::Project => "project",
        }
    }

    const fn default_path(self) -> &'static str {
        match self {
            Self::Workspace => "MEMORY.md",
            Self::Project => "projects/default/MEMORY.md",
        }
    }

    const fn default_title(self) -> &'static str {
        match self {
            Self::Workspace => "Workspace Memory",
            Self::Project => "Project Memory",
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn execute_workspace_memory_retain_tool(
    runtime_state: &Arc<GatewayRuntimeState>,
    context: ToolRuntimeExecutionContext<'_>,
    proposal_id: &str,
    input_json: &[u8],
    parsed: &Map<String, Value>,
    scope: WorkspaceMemoryRetainScope,
    content_text: String,
    source: MemorySource,
    category_hint: Option<MemoryWriteCategory>,
    replaces_terms: Vec<String>,
    tags: Vec<String>,
    confidence: Option<f64>,
    ttl_unix_ms: Option<i64>,
    provenance: Value,
    source_normalization: Option<Value>,
) -> ToolExecutionOutcome {
    let namespace = b"palyra.memory.retain.attestation.v1";
    let content_text = normalize_lifecycle_content(content_text.as_str());
    if content_text.is_empty() {
        return memory_tool_execution_outcome(
            namespace,
            proposal_id,
            input_json,
            false,
            b"{}".to_vec(),
            "palyra.memory.retain memory content is empty after normalization".to_owned(),
        );
    }
    let inferred_project_path = if scope == WorkspaceMemoryRetainScope::Project {
        infer_project_memory_document_path(runtime_state, context).await
    } else {
        None
    };
    let path = match workspace_memory_retain_path(parsed, scope, inferred_project_path.as_deref()) {
        Ok(path) => path,
        Err(error) => {
            return memory_tool_execution_outcome(
                namespace,
                proposal_id,
                input_json,
                false,
                b"{}".to_vec(),
                format!("palyra.memory.retain {error}"),
            );
        }
    };
    if let Err(error) =
        authorize_memory_action(context.principal, "memory.ingest", "memory:workspace")
    {
        return memory_tool_execution_outcome(
            namespace,
            proposal_id,
            input_json,
            false,
            b"{}".to_vec(),
            format!("palyra.memory.retain workspace policy denied: {}", error.message()),
        );
    }

    let agent_id = optional_trimmed_string(parsed.get("agent_id"));
    let existing = match runtime_state
        .workspace_document_by_path(
            context.principal.to_owned(),
            context.channel.map(str::to_owned),
            agent_id.clone(),
            path.clone(),
            false,
        )
        .await
    {
        Ok(document) => document,
        Err(error) => {
            return memory_tool_execution_outcome(
                namespace,
                proposal_id,
                input_json,
                false,
                b"{}".to_vec(),
                format!("palyra.memory.retain workspace document load failed: {}", error.message()),
            );
        }
    };
    let now_unix_ms = current_unix_ms();
    let (existing_content, replaced_entries) = workspace_memory_document_base_content(
        existing.as_ref().map(|document| document.content_text.as_str()),
        category_hint,
        replaces_terms.as_slice(),
    );
    let (content_text_next, appended) = workspace_memory_document_content(
        existing_content.as_deref(),
        scope.default_title(),
        content_text.as_str(),
        source,
        tags.as_slice(),
        confidence,
        ttl_unix_ms,
        now_unix_ms,
    );
    let title = existing
        .as_ref()
        .map(|document| document.title.clone())
        .unwrap_or_else(|| scope.default_title().to_owned());
    let document = if appended || replaced_entries > 0 {
        match runtime_state
            .upsert_workspace_document(WorkspaceDocumentWriteRequest {
                document_id: existing.as_ref().map(|document| document.document_id.clone()),
                principal: context.principal.to_owned(),
                channel: context.channel.map(str::to_owned),
                agent_id,
                session_id: Some(context.session_id.to_owned()),
                path: path.clone(),
                title: Some(title),
                content_text: content_text_next,
                template_id: None,
                template_version: None,
                template_content_hash: None,
                source_memory_id: None,
                manual_override: false,
            })
            .await
        {
            Ok(document) => document,
            Err(error) => {
                return memory_tool_execution_outcome(
                    namespace,
                    proposal_id,
                    input_json,
                    false,
                    b"{}".to_vec(),
                    format!(
                        "palyra.memory.retain workspace document write failed: {}",
                        error.message()
                    ),
                );
            }
        }
    } else if let Some(document) = existing {
        document
    } else {
        return memory_tool_execution_outcome(
            namespace,
            proposal_id,
            input_json,
            false,
            b"{}".to_vec(),
            "palyra.memory.retain workspace document already contained memory but could not be loaded"
                .to_owned(),
        );
    };

    serialize_workspace_memory_retain_outcome(WorkspaceMemoryRetainSerialization {
        namespace,
        proposal_id,
        input_json,
        scope,
        document: &document,
        appended,
        provenance,
        source_normalization,
        replaced_entries,
    })
}

pub(crate) async fn execute_memory_delete_tool(
    runtime_state: &Arc<GatewayRuntimeState>,
    context: ToolRuntimeExecutionContext<'_>,
    proposal_id: &str,
    input_json: &[u8],
) -> ToolExecutionOutcome {
    let namespace = b"palyra.memory.delete.attestation.v1";
    let parsed = match parse_memory_tool_object(input_json) {
        Ok(parsed) => parsed,
        Err(error) => {
            return memory_tool_execution_outcome(
                namespace,
                proposal_id,
                input_json,
                false,
                b"{}".to_vec(),
                format!("palyra.memory.delete {error}"),
            );
        }
    };
    let memory_id = match required_string_field(&parsed, "memory_id") {
        Ok(value) => value,
        Err(error) => {
            return memory_tool_execution_outcome(
                namespace,
                proposal_id,
                input_json,
                false,
                b"{}".to_vec(),
                format!("palyra.memory.delete {error}"),
            );
        }
    };
    if let Err(error) = validate_canonical_id(memory_id.as_str()) {
        return memory_tool_execution_outcome(
            namespace,
            proposal_id,
            input_json,
            false,
            b"{}".to_vec(),
            format!("palyra.memory.delete memory_id must be a canonical ULID: {error}"),
        );
    }
    if let Err(error) = authorize_memory_action(
        context.principal,
        "memory.delete",
        format!("memory:{memory_id}").as_str(),
    ) {
        return memory_tool_execution_outcome(
            namespace,
            proposal_id,
            input_json,
            false,
            b"{}".to_vec(),
            format!("palyra.memory.delete {}", error.message()),
        );
    }
    let mut memory_item_exists = false;
    match runtime_state.memory_item(memory_id.clone()).await {
        Ok(Some(item)) => {
            memory_item_exists = true;
            if let Err(error) = enforce_memory_item_scope(&item, context.principal, context.channel)
            {
                return memory_tool_execution_outcome(
                    namespace,
                    proposal_id,
                    input_json,
                    false,
                    b"{}".to_vec(),
                    format!("palyra.memory.delete {}", error.message()),
                );
            }
        }
        Ok(None) => {}
        Err(error) => {
            return memory_tool_execution_outcome(
                namespace,
                proposal_id,
                input_json,
                false,
                b"{}".to_vec(),
                format!("palyra.memory.delete failed: {}", error.message()),
            );
        }
    }
    if !memory_item_exists {
        if let Some(outcome) = maybe_delete_workspace_document_by_id(
            runtime_state,
            context,
            namespace,
            proposal_id,
            input_json,
            memory_id.as_str(),
        )
        .await
        {
            return outcome;
        }
    }
    let deleted = match runtime_state
        .delete_memory_item(
            memory_id.clone(),
            context.principal.to_owned(),
            context.channel.map(str::to_owned),
        )
        .await
    {
        Ok(deleted) => deleted,
        Err(error) => {
            return memory_tool_execution_outcome(
                namespace,
                proposal_id,
                input_json,
                false,
                b"{}".to_vec(),
                format!("palyra.memory.delete failed: {}", error.message()),
            );
        }
    };
    let payload = json!({
        "memory_id": memory_id,
        "deleted": deleted,
        "status": if deleted { "deleted" } else { "not_found_or_already_deleted" },
        "claim_boundary": if deleted {
            "memory item was deleted and should not be claimed as retained"
        } else {
            "no matching memory item was deleted; do not claim the memory was removed"
        },
    });
    match serde_json::to_vec(&payload) {
        Ok(output_json) => memory_tool_execution_outcome(
            namespace,
            proposal_id,
            input_json,
            true,
            output_json,
            String::new(),
        ),
        Err(error) => memory_tool_execution_outcome(
            namespace,
            proposal_id,
            input_json,
            false,
            b"{}".to_vec(),
            format!("palyra.memory.delete failed to serialize output: {error}"),
        ),
    }
}

async fn maybe_delete_workspace_document_by_id(
    runtime_state: &Arc<GatewayRuntimeState>,
    context: ToolRuntimeExecutionContext<'_>,
    namespace: &'static [u8],
    proposal_id: &str,
    input_json: &[u8],
    document_id: &str,
) -> Option<ToolExecutionOutcome> {
    let document = match runtime_state
        .workspace_document_by_id(
            context.principal.to_owned(),
            context.channel.map(str::to_owned),
            None,
            document_id.to_owned(),
            false,
        )
        .await
    {
        Ok(Some(document)) => document,
        Ok(None) => return None,
        Err(error) => {
            return Some(memory_tool_execution_outcome(
                namespace,
                proposal_id,
                input_json,
                false,
                b"{}".to_vec(),
                format!(
                    "palyra.memory.delete failed to inspect workspace document: {}",
                    error.message()
                ),
            ));
        }
    };
    if let Err(error) = authorize_memory_action(
        context.principal,
        "memory.delete",
        format!("memory:workspace_document:{}", document.document_id).as_str(),
    ) {
        return Some(memory_tool_execution_outcome(
            namespace,
            proposal_id,
            input_json,
            false,
            b"{}".to_vec(),
            format!("palyra.memory.delete {}", error.message()),
        ));
    }
    let deleted_document = match runtime_state
        .soft_delete_workspace_document(WorkspaceDocumentDeleteRequest {
            principal: document.principal.clone(),
            channel: document.channel.clone(),
            agent_id: document.agent_id.clone(),
            session_id: Some(context.session_id.to_owned()),
            path: document.path.clone(),
        })
        .await
    {
        Ok(document) => document,
        Err(error) => {
            return Some(memory_tool_execution_outcome(
                namespace,
                proposal_id,
                input_json,
                false,
                b"{}".to_vec(),
                format!(
                    "palyra.memory.delete failed to delete workspace document: {}",
                    error.message()
                ),
            ));
        }
    };
    let payload = json!({
        "memory_id": document_id,
        "workspace_document_id": deleted_document.document_id.as_str(),
        "deleted": true,
        "status": "workspace_document_deleted",
        "document": workspace_document_output_payload(&deleted_document),
        "claim_boundary": "workspace memory document was soft-deleted and should not be claimed as retained",
    });
    Some(match serde_json::to_vec(&payload) {
        Ok(output_json) => memory_tool_execution_outcome(
            namespace,
            proposal_id,
            input_json,
            true,
            output_json,
            String::new(),
        ),
        Err(error) => memory_tool_execution_outcome(
            namespace,
            proposal_id,
            input_json,
            false,
            b"{}".to_vec(),
            format!("palyra.memory.delete failed to serialize workspace output: {error}"),
        ),
    })
}

pub(crate) async fn execute_memory_replace_tool(
    runtime_state: &Arc<GatewayRuntimeState>,
    context: ToolRuntimeExecutionContext<'_>,
    proposal_id: &str,
    input_json: &[u8],
) -> ToolExecutionOutcome {
    let namespace = b"palyra.memory.replace.attestation.v1";
    let parsed = match parse_memory_tool_object(input_json) {
        Ok(parsed) => parsed,
        Err(error) => {
            return memory_tool_execution_outcome(
                namespace,
                proposal_id,
                input_json,
                false,
                b"{}".to_vec(),
                format!("palyra.memory.replace {error}"),
            );
        }
    };
    let memory_id = match required_string_field(&parsed, "memory_id") {
        Ok(value) => value,
        Err(error) => {
            return memory_tool_execution_outcome(
                namespace,
                proposal_id,
                input_json,
                false,
                b"{}".to_vec(),
                format!("palyra.memory.replace {error}"),
            );
        }
    };
    if let Err(error) = validate_canonical_id(memory_id.as_str()) {
        return memory_tool_execution_outcome(
            namespace,
            proposal_id,
            input_json,
            false,
            b"{}".to_vec(),
            format!("palyra.memory.replace memory_id must be a canonical ULID: {error}"),
        );
    }
    let content_text = match required_string_field(&parsed, "content_text") {
        Ok(value) => normalize_lifecycle_content(value.as_str()),
        Err(error) => {
            return memory_tool_execution_outcome(
                namespace,
                proposal_id,
                input_json,
                false,
                b"{}".to_vec(),
                format!("palyra.memory.replace {error}"),
            );
        }
    };
    if content_text.len() > MAX_MEMORY_TOOL_QUERY_BYTES {
        return memory_tool_execution_outcome(
            namespace,
            proposal_id,
            input_json,
            false,
            b"{}".to_vec(),
            format!(
                "palyra.memory.replace content_text exceeds {MAX_MEMORY_TOOL_QUERY_BYTES} bytes"
            ),
        );
    }
    if content_text.is_empty() {
        return memory_tool_execution_outcome(
            namespace,
            proposal_id,
            input_json,
            false,
            b"{}".to_vec(),
            "palyra.memory.replace content_text is empty after normalization".to_owned(),
        );
    }
    let parsed_tags =
        match parse_string_array_field(parsed.get("tags"), "tags", MAX_MEMORY_TOOL_TAGS) {
            Ok(tags) => tags,
            Err(error) => {
                return memory_tool_execution_outcome(
                    namespace,
                    proposal_id,
                    input_json,
                    false,
                    b"{}".to_vec(),
                    error.replace("palyra.memory.retain", "palyra.memory.replace"),
                );
            }
        };
    let confidence = match parsed.get("confidence").and_then(Value::as_f64) {
        Some(value) if value.is_finite() && (0.0..=1.0).contains(&value) => Some(value),
        Some(_) => {
            return memory_tool_execution_outcome(
                namespace,
                proposal_id,
                input_json,
                false,
                b"{}".to_vec(),
                "palyra.memory.replace confidence must be in range 0.0..=1.0".to_owned(),
            );
        }
        None => None,
    };
    let ttl_unix_ms = match ttl_unix_ms_from_input(
        parsed.get("ttl_ms").and_then(Value::as_i64),
        parsed.get("ttl_unix_ms").and_then(Value::as_i64),
    ) {
        Ok(value) => value,
        Err(error) => {
            return memory_tool_execution_outcome(
                namespace,
                proposal_id,
                input_json,
                false,
                b"{}".to_vec(),
                format!("palyra.memory.replace {}", error.message()),
            );
        }
    };
    let existing_item = match runtime_state.memory_item(memory_id.clone()).await {
        Ok(Some(item)) => item,
        Ok(None) => {
            if let Some(outcome) = maybe_replace_workspace_document_by_id(
                context,
                WorkspaceDocumentReplaceRequest {
                    runtime_state,
                    namespace,
                    proposal_id,
                    input_json,
                    document_id: memory_id.as_str(),
                    content_text: content_text.as_str(),
                    parsed: &parsed,
                },
            )
            .await
            {
                return outcome;
            }
            return memory_tool_execution_outcome(
                namespace,
                proposal_id,
                input_json,
                false,
                b"{}".to_vec(),
                format!("palyra.memory.replace memory item not found: {memory_id}"),
            );
        }
        Err(error) => {
            return memory_tool_execution_outcome(
                namespace,
                proposal_id,
                input_json,
                false,
                b"{}".to_vec(),
                format!("palyra.memory.replace failed: {}", error.message()),
            );
        }
    };
    if let Err(error) =
        enforce_memory_item_scope(&existing_item, context.principal, context.channel)
    {
        return memory_tool_execution_outcome(
            namespace,
            proposal_id,
            input_json,
            false,
            b"{}".to_vec(),
            format!("palyra.memory.replace {}", error.message()),
        );
    }
    let resource = memory_item_write_resource(&existing_item);
    if let Err(error) =
        authorize_memory_action(context.principal, "memory.ingest", resource.as_str())
    {
        return memory_tool_execution_outcome(
            namespace,
            proposal_id,
            input_json,
            false,
            b"{}".to_vec(),
            format!("palyra.memory.replace {}", error.message()),
        );
    }
    let tags = if parsed_tags.is_empty() { existing_item.tags.clone() } else { parsed_tags };
    let updated = match runtime_state
        .update_memory_item_lifecycle(MemoryItemLifecycleUpdateRequest {
            memory_id: memory_id.clone(),
            principal: context.principal.to_owned(),
            channel: existing_item.channel.clone(),
            session_id: existing_item.session_id.clone(),
            content_text: Some(content_text),
            tags,
            confidence,
            ttl_unix_ms,
        })
        .await
    {
        Ok(Some(item)) => item,
        Ok(None) => {
            return memory_tool_execution_outcome(
                namespace,
                proposal_id,
                input_json,
                false,
                b"{}".to_vec(),
                format!("palyra.memory.replace memory item not found: {memory_id}"),
            );
        }
        Err(error) => {
            return memory_tool_execution_outcome(
                namespace,
                proposal_id,
                input_json,
                false,
                b"{}".to_vec(),
                format!("palyra.memory.replace failed: {}", error.message()),
            );
        }
    };
    let payload = json!({
        "memory_id": memory_id,
        "status": "replaced",
        "durable_memory_write": true,
        "previous_content_hash": existing_item.content_hash,
        "item": memory_item_output_payload(&updated),
        "claim_boundary": "memory item content was replaced in place; use the returned item as the current durable value",
    });
    match serde_json::to_vec(&payload) {
        Ok(output_json) => memory_tool_execution_outcome(
            namespace,
            proposal_id,
            input_json,
            true,
            output_json,
            String::new(),
        ),
        Err(error) => memory_tool_execution_outcome(
            namespace,
            proposal_id,
            input_json,
            false,
            b"{}".to_vec(),
            format!("palyra.memory.replace failed to serialize output: {error}"),
        ),
    }
}

struct WorkspaceDocumentReplaceRequest<'a> {
    runtime_state: &'a Arc<GatewayRuntimeState>,
    namespace: &'static [u8],
    proposal_id: &'a str,
    input_json: &'a [u8],
    document_id: &'a str,
    content_text: &'a str,
    parsed: &'a Map<String, Value>,
}

async fn maybe_replace_workspace_document_by_id(
    context: ToolRuntimeExecutionContext<'_>,
    request: WorkspaceDocumentReplaceRequest<'_>,
) -> Option<ToolExecutionOutcome> {
    let document = match request
        .runtime_state
        .workspace_document_by_id(
            context.principal.to_owned(),
            context.channel.map(str::to_owned),
            optional_trimmed_string(request.parsed.get("agent_id")),
            request.document_id.to_owned(),
            false,
        )
        .await
    {
        Ok(Some(document)) => document,
        Ok(None) => return None,
        Err(error) => {
            return Some(memory_tool_execution_outcome(
                request.namespace,
                request.proposal_id,
                request.input_json,
                false,
                b"{}".to_vec(),
                format!(
                    "palyra.memory.replace failed to inspect workspace document: {}",
                    error.message()
                ),
            ));
        }
    };
    if let Err(error) = authorize_memory_action(
        context.principal,
        "memory.ingest",
        format!("memory:workspace_document:{}", document.document_id).as_str(),
    ) {
        return Some(memory_tool_execution_outcome(
            request.namespace,
            request.proposal_id,
            request.input_json,
            false,
            b"{}".to_vec(),
            format!("palyra.memory.replace {}", error.message()),
        ));
    }
    let previous_content_hash = document.content_hash.clone();
    let updated_document = match request
        .runtime_state
        .upsert_workspace_document(WorkspaceDocumentWriteRequest {
            document_id: Some(document.document_id.clone()),
            principal: document.principal.clone(),
            channel: document.channel.clone(),
            agent_id: document.agent_id.clone(),
            session_id: Some(context.session_id.to_owned()),
            path: document.path.clone(),
            title: Some(document.title.clone()),
            content_text: request.content_text.to_owned(),
            template_id: document.template_id.clone(),
            template_version: document.template_version,
            template_content_hash: None,
            source_memory_id: document.source_memory_id.clone(),
            manual_override: document.manual_override,
        })
        .await
    {
        Ok(document) => document,
        Err(error) => {
            return Some(memory_tool_execution_outcome(
                request.namespace,
                request.proposal_id,
                request.input_json,
                false,
                b"{}".to_vec(),
                format!(
                    "palyra.memory.replace failed to update workspace document: {}",
                    error.message()
                ),
            ));
        }
    };
    let payload = workspace_memory_replace_payload(
        request.document_id,
        previous_content_hash.as_str(),
        &updated_document,
    );
    Some(match serde_json::to_vec(&payload) {
        Ok(output_json) => memory_tool_execution_outcome(
            request.namespace,
            request.proposal_id,
            request.input_json,
            true,
            output_json,
            String::new(),
        ),
        Err(error) => memory_tool_execution_outcome(
            request.namespace,
            request.proposal_id,
            request.input_json,
            false,
            b"{}".to_vec(),
            format!("palyra.memory.replace failed to serialize workspace output: {error}"),
        ),
    })
}

fn workspace_memory_replace_payload(
    memory_id: &str,
    previous_content_hash: &str,
    document: &WorkspaceDocumentRecord,
) -> Value {
    json!({
        "memory_id": memory_id,
        "workspace_document_id": document.document_id.as_str(),
        "status": "workspace_document_replaced",
        "durable_memory_write": true,
        "previous_content_hash": previous_content_hash,
        "document": workspace_document_output_payload(document),
        "claim_boundary": "workspace memory document content was replaced in place; use the returned workspace document as the current durable project value",
    })
}

pub(crate) async fn execute_memory_reflect_tool(
    context: ToolRuntimeExecutionContext<'_>,
    proposal_id: &str,
    input_json: &[u8],
) -> ToolExecutionOutcome {
    let namespace = b"palyra.memory.reflect.attestation.v1";
    let parsed = match parse_memory_tool_object(input_json) {
        Ok(parsed) => parsed,
        Err(error) => {
            return memory_tool_execution_outcome(
                namespace,
                proposal_id,
                input_json,
                false,
                b"{}".to_vec(),
                format!("palyra.memory.reflect {error}"),
            );
        }
    };
    let observations = match parse_reflection_observations(&parsed) {
        Ok(observations) => observations,
        Err(error) => {
            return memory_tool_execution_outcome(
                namespace,
                proposal_id,
                input_json,
                false,
                b"{}".to_vec(),
                error,
            );
        }
    };
    let categories = match parse_reflection_categories(parsed.get("categories")) {
        Ok(categories) => categories,
        Err(error) => {
            return memory_tool_execution_outcome(
                namespace,
                proposal_id,
                input_json,
                false,
                b"{}".to_vec(),
                error,
            );
        }
    };
    let max_candidates = parsed
        .get("max_candidates")
        .and_then(Value::as_u64)
        .map(|value| (value as usize).clamp(1, 16))
        .unwrap_or(8);
    let provenance = parsed
        .get("provenance")
        .cloned()
        .unwrap_or_else(|| retain_tool_provenance(context, proposal_id));
    let outcome = reflect_memory_candidates(MemoryReflectionRequest {
        observations,
        allowed_categories: categories,
        max_candidates,
        provenance,
    });
    serialize_memory_reflection_outcome(namespace, proposal_id, input_json, &outcome)
}

pub(crate) async fn execute_memory_search_tool(
    runtime_state: &Arc<GatewayRuntimeState>,
    context: ToolRuntimeExecutionContext<'_>,
    proposal_id: &str,
    input_json: &[u8],
) -> ToolExecutionOutcome {
    let principal = context.principal;
    let channel = context.channel;
    let session_id = context.session_id;
    let attestation_namespace = b"palyra.memory.search.attestation.v1";
    let parsed = match serde_json::from_slice::<Value>(input_json) {
        Ok(Value::Object(map)) => map,
        Ok(_) => {
            return memory_tool_execution_outcome(
                attestation_namespace,
                proposal_id,
                input_json,
                false,
                b"{}".to_vec(),
                "palyra.memory.search requires JSON object input".to_owned(),
            );
        }
        Err(error) => {
            return memory_tool_execution_outcome(
                attestation_namespace,
                proposal_id,
                input_json,
                false,
                b"{}".to_vec(),
                format!("palyra.memory.search invalid JSON input: {error}"),
            );
        }
    };

    let query = match parsed.get("query").and_then(Value::as_str).map(str::trim) {
        Some(value) if !value.is_empty() => value.to_owned(),
        _ => {
            return memory_tool_execution_outcome(
                attestation_namespace,
                proposal_id,
                input_json,
                false,
                b"{}".to_vec(),
                "palyra.memory.search requires non-empty string field 'query'".to_owned(),
            );
        }
    };
    if query.len() > MAX_MEMORY_TOOL_QUERY_BYTES {
        return memory_tool_execution_outcome(
            attestation_namespace,
            proposal_id,
            input_json,
            false,
            b"{}".to_vec(),
            format!("palyra.memory.search query exceeds {MAX_MEMORY_TOOL_QUERY_BYTES} bytes"),
        );
    }

    let min_score = parsed.get("min_score").and_then(Value::as_f64).unwrap_or(0.0);
    if !min_score.is_finite() || !(0.0..=1.0).contains(&min_score) {
        return memory_tool_execution_outcome(
            attestation_namespace,
            proposal_id,
            input_json,
            false,
            b"{}".to_vec(),
            "palyra.memory.search min_score must be in range 0.0..=1.0".to_owned(),
        );
    }
    let top_k = parsed
        .get("top_k")
        .and_then(Value::as_u64)
        .map(|value| (value as usize).clamp(1, MAX_MEMORY_SEARCH_TOP_K))
        .unwrap_or(8);

    let scope = memory_search_scope_text(&parsed);
    if matches!(scope.as_str(), "workspace" | "project") {
        if let Err(error) = authorize_memory_action(principal, "memory.search", "memory:workspace")
        {
            return memory_tool_execution_outcome(
                attestation_namespace,
                proposal_id,
                input_json,
                false,
                b"{}".to_vec(),
                format!("memory policy denied tool workspace search request: {}", error.message()),
            );
        }
        let explicit_workspace_prefix = optional_trimmed_string(parsed.get("workspace_prefix"))
            .or_else(|| optional_trimmed_string(parsed.get("prefix")));
        let inferred_project_prefix = if scope == "project" && explicit_workspace_prefix.is_none() {
            infer_project_memory_prefix(runtime_state, context).await
        } else {
            None
        };
        let workspace_prefix = match workspace_memory_search_prefix(
            explicit_workspace_prefix.as_deref(),
            scope.as_str(),
            inferred_project_prefix.as_deref(),
        ) {
            Ok(prefix) => prefix,
            Err(error) => {
                return memory_tool_execution_outcome(
                    attestation_namespace,
                    proposal_id,
                    input_json,
                    false,
                    b"{}".to_vec(),
                    format!("palyra.memory.search {error}"),
                );
            }
        };
        let search_hits = match runtime_state
            .search_workspace_documents(WorkspaceSearchRequest {
                principal: principal.to_owned(),
                channel: channel.map(str::to_owned),
                agent_id: optional_trimmed_string(parsed.get("agent_id")),
                query,
                prefix: workspace_prefix.clone(),
                top_k,
                min_score,
                include_historical: parsed
                    .get("include_workspace_historical")
                    .and_then(Value::as_bool)
                    .unwrap_or(false),
                include_quarantined: parsed
                    .get("include_workspace_quarantined")
                    .and_then(Value::as_bool)
                    .unwrap_or(false),
            })
            .await
        {
            Ok(hits) => hits,
            Err(error) => {
                return memory_tool_execution_outcome(
                    attestation_namespace,
                    proposal_id,
                    input_json,
                    false,
                    b"{}".to_vec(),
                    format!("palyra.memory.search workspace search failed: {}", error.message()),
                );
            }
        };
        let mut payload = workspace_search_tool_output_payload(search_hits.as_slice());
        if let Some(object) = payload.as_object_mut() {
            object.insert("scope".to_owned(), json!(scope));
            object.insert("workspace_prefix".to_owned(), json!(workspace_prefix));
        }
        return match serde_json::to_vec(&payload) {
            Ok(output_json) => memory_tool_execution_outcome(
                attestation_namespace,
                proposal_id,
                input_json,
                true,
                output_json,
                String::new(),
            ),
            Err(error) => memory_tool_execution_outcome(
                attestation_namespace,
                proposal_id,
                input_json,
                false,
                b"{}".to_vec(),
                format!("palyra.memory.search failed to serialize workspace output: {error}"),
            ),
        };
    }

    if scope == "all" {
        if let Err(error) = authorize_memory_action(principal, "memory.search", "memory:principal")
        {
            return memory_tool_execution_outcome(
                attestation_namespace,
                proposal_id,
                input_json,
                false,
                b"{}".to_vec(),
                format!("memory policy denied tool search request: {}", error.message()),
            );
        }
        if let Err(error) = authorize_memory_action(principal, "memory.search", "memory:workspace")
        {
            return memory_tool_execution_outcome(
                attestation_namespace,
                proposal_id,
                input_json,
                false,
                b"{}".to_vec(),
                format!("memory policy denied tool workspace search request: {}", error.message()),
            );
        }
        let tags = match parse_memory_search_tags(&parsed) {
            Ok(tags) => tags,
            Err(error) => {
                return memory_tool_execution_outcome(
                    attestation_namespace,
                    proposal_id,
                    input_json,
                    false,
                    b"{}".to_vec(),
                    error,
                );
            }
        };
        let sources = match parse_memory_search_sources(&parsed) {
            Ok(sources) => sources,
            Err(error) => {
                return memory_tool_execution_outcome(
                    attestation_namespace,
                    proposal_id,
                    input_json,
                    false,
                    b"{}".to_vec(),
                    error,
                );
            }
        };
        let memory_hits = match runtime_state
            .search_memory(MemorySearchRequest {
                principal: principal.to_owned(),
                channel: channel.map(str::to_owned),
                session_id: None,
                query: query.clone(),
                top_k,
                min_score,
                tags,
                sources,
            })
            .await
        {
            Ok(hits) => hits,
            Err(error) => {
                return memory_tool_execution_outcome(
                    attestation_namespace,
                    proposal_id,
                    input_json,
                    false,
                    b"{}".to_vec(),
                    format!("palyra.memory.search failed: {}", error.message()),
                );
            }
        };
        let explicit_workspace_prefix = optional_trimmed_string(parsed.get("workspace_prefix"))
            .or_else(|| optional_trimmed_string(parsed.get("prefix")));
        let workspace_prefix = match workspace_memory_search_prefix(
            explicit_workspace_prefix.as_deref(),
            "workspace",
            None,
        ) {
            Ok(prefix) => prefix,
            Err(error) => {
                return memory_tool_execution_outcome(
                    attestation_namespace,
                    proposal_id,
                    input_json,
                    false,
                    b"{}".to_vec(),
                    format!("palyra.memory.search {error}"),
                );
            }
        };
        let workspace_hits = match runtime_state
            .search_workspace_documents(WorkspaceSearchRequest {
                principal: principal.to_owned(),
                channel: channel.map(str::to_owned),
                agent_id: optional_trimmed_string(parsed.get("agent_id")),
                query,
                prefix: workspace_prefix.clone(),
                top_k,
                min_score,
                include_historical: parsed
                    .get("include_workspace_historical")
                    .and_then(Value::as_bool)
                    .unwrap_or(false),
                include_quarantined: parsed
                    .get("include_workspace_quarantined")
                    .and_then(Value::as_bool)
                    .unwrap_or(false),
            })
            .await
        {
            Ok(hits) => hits,
            Err(error) => {
                return memory_tool_execution_outcome(
                    attestation_namespace,
                    proposal_id,
                    input_json,
                    false,
                    b"{}".to_vec(),
                    format!("palyra.memory.search workspace search failed: {}", error.message()),
                );
            }
        };
        let payload = combined_memory_search_tool_output_payload(
            memory_hits.as_slice(),
            workspace_hits.as_slice(),
            workspace_prefix.as_deref(),
        );
        return match serde_json::to_vec(&payload) {
            Ok(output_json) => memory_tool_execution_outcome(
                attestation_namespace,
                proposal_id,
                input_json,
                true,
                output_json,
                String::new(),
            ),
            Err(error) => memory_tool_execution_outcome(
                attestation_namespace,
                proposal_id,
                input_json,
                false,
                b"{}".to_vec(),
                format!("palyra.memory.search failed to serialize output: {error}"),
            ),
        };
    }

    let (channel_scope, session_scope, resource) = match scope.as_str() {
        "principal" => (channel.map(str::to_owned), None, "memory:principal".to_owned()),
        "channel" => {
            let Some(channel) = channel.map(str::to_owned) else {
                return memory_tool_execution_outcome(
                    attestation_namespace,
                    proposal_id,
                    input_json,
                    false,
                    b"{}".to_vec(),
                    "palyra.memory.search scope=channel requires authenticated channel context"
                        .to_owned(),
                );
            };
            let resource = format!("memory:channel:{channel}");
            (Some(channel), None, resource)
        }
        "session" => {
            let channel = channel.map(str::to_owned);
            let session = Some(session_id.to_owned());
            (channel, session, format!("memory:session:{session_id}"))
        }
        _ => {
            return memory_tool_execution_outcome(
                attestation_namespace,
                proposal_id,
                input_json,
                false,
                b"{}".to_vec(),
                "palyra.memory.search scope must be one of: all|session|channel|principal|workspace|project"
                    .to_owned(),
            );
        }
    };

    if let Err(error) = authorize_memory_action(principal, "memory.search", resource.as_str()) {
        return memory_tool_execution_outcome(
            attestation_namespace,
            proposal_id,
            input_json,
            false,
            b"{}".to_vec(),
            format!("memory policy denied tool search request: {}", error.message()),
        );
    }

    let tags = match parse_memory_search_tags(&parsed) {
        Ok(tags) => tags,
        Err(error) => {
            return memory_tool_execution_outcome(
                attestation_namespace,
                proposal_id,
                input_json,
                false,
                b"{}".to_vec(),
                error,
            );
        }
    };
    let sources = match parse_memory_search_sources(&parsed) {
        Ok(sources) => sources,
        Err(error) => {
            return memory_tool_execution_outcome(
                attestation_namespace,
                proposal_id,
                input_json,
                false,
                b"{}".to_vec(),
                error,
            );
        }
    };

    let search_hits = match runtime_state
        .search_memory(MemorySearchRequest {
            principal: principal.to_owned(),
            channel: channel_scope,
            session_id: session_scope,
            query,
            top_k,
            min_score,
            tags,
            sources,
        })
        .await
    {
        Ok(hits) => hits,
        Err(error) => {
            return memory_tool_execution_outcome(
                attestation_namespace,
                proposal_id,
                input_json,
                false,
                b"{}".to_vec(),
                format!("palyra.memory.search failed: {}", error.message()),
            );
        }
    };
    let payload = memory_search_tool_output_payload(search_hits.as_slice());
    match serde_json::to_vec(&payload) {
        Ok(output_json) => memory_tool_execution_outcome(
            attestation_namespace,
            proposal_id,
            input_json,
            true,
            output_json,
            String::new(),
        ),
        Err(error) => memory_tool_execution_outcome(
            attestation_namespace,
            proposal_id,
            input_json,
            false,
            b"{}".to_vec(),
            format!("palyra.memory.search failed to serialize output: {error}"),
        ),
    }
}

fn parse_memory_search_tags(parsed: &Map<String, Value>) -> Result<Vec<String>, String> {
    match parsed.get("tags") {
        Some(Value::Array(values)) => {
            if values.len() > MAX_MEMORY_TOOL_TAGS {
                return Err(format!(
                    "palyra.memory.search tags exceeds limit ({})",
                    MAX_MEMORY_TOOL_TAGS
                ));
            }
            let mut parsed_tags = Vec::new();
            for value in values {
                let Some(tag) = value.as_str() else {
                    return Err("palyra.memory.search tags must be strings".to_owned());
                };
                if !tag.trim().is_empty() {
                    parsed_tags.push(tag.trim().to_owned());
                }
            }
            Ok(parsed_tags)
        }
        Some(_) => Err("palyra.memory.search tags must be an array of strings".to_owned()),
        None => Ok(Vec::new()),
    }
}

fn parse_memory_search_sources(parsed: &Map<String, Value>) -> Result<Vec<MemorySource>, String> {
    match parsed.get("sources") {
        Some(Value::Array(values)) => {
            let mut parsed_sources = Vec::new();
            for value in values {
                let Some(source) = value.as_str() else {
                    return Err(
                        "palyra.memory.search sources must be an array of strings".to_owned()
                    );
                };
                let Some(memory_source) = parse_memory_source_literal(source) else {
                    return Err(format!("palyra.memory.search unknown source value: {source}"));
                };
                parsed_sources.push(memory_source);
            }
            Ok(parsed_sources)
        }
        Some(_) => Err("palyra.memory.search sources must be an array of strings".to_owned()),
        None => Ok(Vec::new()),
    }
}

pub(crate) async fn execute_memory_recall_tool(
    runtime_state: &Arc<GatewayRuntimeState>,
    context: ToolRuntimeExecutionContext<'_>,
    proposal_id: &str,
    input_json: &[u8],
) -> ToolExecutionOutcome {
    let parsed = match serde_json::from_slice::<Value>(input_json) {
        Ok(Value::Object(map)) => map,
        Ok(_) => {
            return memory_tool_execution_outcome(
                b"palyra.memory.recall.attestation.v1",
                proposal_id,
                input_json,
                false,
                b"{}".to_vec(),
                "palyra.memory.recall requires JSON object input".to_owned(),
            );
        }
        Err(error) => {
            return memory_tool_execution_outcome(
                b"palyra.memory.recall.attestation.v1",
                proposal_id,
                input_json,
                false,
                b"{}".to_vec(),
                format!("palyra.memory.recall invalid JSON input: {error}"),
            );
        }
    };

    let query = match parsed.get("query").and_then(Value::as_str).map(str::trim) {
        Some(value) if !value.is_empty() => value.to_owned(),
        _ => {
            return memory_tool_execution_outcome(
                b"palyra.memory.recall.attestation.v1",
                proposal_id,
                input_json,
                false,
                b"{}".to_vec(),
                "palyra.memory.recall requires non-empty string field 'query'".to_owned(),
            );
        }
    };
    if query.len() > MAX_MEMORY_TOOL_QUERY_BYTES {
        return memory_tool_execution_outcome(
            b"palyra.memory.recall.attestation.v1",
            proposal_id,
            input_json,
            false,
            b"{}".to_vec(),
            format!("palyra.memory.recall query exceeds {MAX_MEMORY_TOOL_QUERY_BYTES} bytes"),
        );
    }

    let requested_channel = match parsed.get("channel") {
        Some(Value::String(value)) if !value.trim().is_empty() => Some(value.trim().to_owned()),
        Some(Value::Null) | None => None,
        Some(_) => {
            return memory_tool_execution_outcome(
                b"palyra.memory.recall.attestation.v1",
                proposal_id,
                input_json,
                false,
                b"{}".to_vec(),
                "palyra.memory.recall channel must be a string when provided".to_owned(),
            );
        }
    };
    if let Some(requested_channel) = requested_channel.as_deref() {
        match context.channel {
            Some(current_channel) if current_channel == requested_channel => {}
            Some(_) => {
                return memory_tool_execution_outcome(
                    b"palyra.memory.recall.attestation.v1",
                    proposal_id,
                    input_json,
                    false,
                    b"{}".to_vec(),
                    "palyra.memory.recall channel must match the authenticated runtime channel"
                        .to_owned(),
                );
            }
            None => {
                return memory_tool_execution_outcome(
                    b"palyra.memory.recall.attestation.v1",
                    proposal_id,
                    input_json,
                    false,
                    b"{}".to_vec(),
                    "palyra.memory.recall channel override requires authenticated channel context"
                        .to_owned(),
                );
            }
        }
    }

    let min_score = parsed.get("min_score").and_then(Value::as_f64).unwrap_or(0.0);
    if !min_score.is_finite() || !(0.0..=1.0).contains(&min_score) {
        return memory_tool_execution_outcome(
            b"palyra.memory.recall.attestation.v1",
            proposal_id,
            input_json,
            false,
            b"{}".to_vec(),
            "palyra.memory.recall min_score must be in range 0.0..=1.0".to_owned(),
        );
    }

    let memory_top_k = match parse_optional_recall_limit(parsed.get("memory_top_k"), 16) {
        Ok(value) => value.unwrap_or(4),
        Err(error) => {
            return memory_tool_execution_outcome(
                b"palyra.memory.recall.attestation.v1",
                proposal_id,
                input_json,
                false,
                b"{}".to_vec(),
                error,
            );
        }
    };
    let workspace_top_k = match parse_optional_recall_limit(parsed.get("workspace_top_k"), 16) {
        Ok(value) => value.unwrap_or(4),
        Err(error) => {
            return memory_tool_execution_outcome(
                b"palyra.memory.recall.attestation.v1",
                proposal_id,
                input_json,
                false,
                b"{}".to_vec(),
                error,
            );
        }
    };
    let max_candidates = match parse_optional_recall_limit(
        parsed.get("max_candidates"),
        MAX_MEMORY_RECALL_MAX_CANDIDATES,
    ) {
        Ok(value) => value.unwrap_or(DEFAULT_MEMORY_RECALL_MAX_CANDIDATES),
        Err(error) => {
            return memory_tool_execution_outcome(
                b"palyra.memory.recall.attestation.v1",
                proposal_id,
                input_json,
                false,
                b"{}".to_vec(),
                error,
            );
        }
    };
    let prompt_budget_tokens = match parsed.get("prompt_budget_tokens").and_then(Value::as_u64) {
        Some(value) => {
            let value = value as usize;
            if !(MIN_MEMORY_RECALL_PROMPT_BUDGET_TOKENS..=MAX_MEMORY_RECALL_PROMPT_BUDGET_TOKENS)
                .contains(&value)
            {
                return memory_tool_execution_outcome(
                    b"palyra.memory.recall.attestation.v1",
                    proposal_id,
                    input_json,
                    false,
                    b"{}".to_vec(),
                    format!(
                        "palyra.memory.recall prompt_budget_tokens must be in range {}..={}",
                        MIN_MEMORY_RECALL_PROMPT_BUDGET_TOKENS,
                        MAX_MEMORY_RECALL_PROMPT_BUDGET_TOKENS
                    ),
                );
            }
            value
        }
        None => DEFAULT_MEMORY_RECALL_PROMPT_BUDGET_TOKENS,
    };

    let request_context = RequestContext {
        principal: context.principal.to_owned(),
        device_id: context.device_id.to_owned(),
        channel: context.channel.map(str::to_owned),
    };
    let raw_workspace_prefix = optional_trimmed_string(parsed.get("workspace_prefix"))
        .or_else(|| optional_trimmed_string(parsed.get("prefix")));
    let workspace_scope = memory_recall_workspace_scope_text(&parsed);
    let inferred_project_prefix = if workspace_scope == "project" && raw_workspace_prefix.is_none()
    {
        infer_project_memory_prefix(runtime_state, context).await
    } else {
        None
    };
    let workspace_prefix = match workspace_memory_search_prefix(
        raw_workspace_prefix.as_deref(),
        workspace_scope.as_str(),
        inferred_project_prefix.as_deref(),
    ) {
        Ok(prefix) => prefix,
        Err(error) => {
            return memory_tool_execution_outcome(
                b"palyra.memory.recall.attestation.v1",
                proposal_id,
                input_json,
                false,
                b"{}".to_vec(),
                format!("palyra.memory.recall {error}"),
            );
        }
    };

    let request = RecallRequest {
        query,
        channel: requested_channel.or_else(|| context.channel.map(str::to_owned)),
        session_id: optional_trimmed_string(parsed.get("session_id"))
            .or_else(|| Some(context.session_id.to_owned())),
        agent_id: optional_trimmed_string(parsed.get("agent_id")),
        memory_top_k,
        workspace_top_k,
        min_score,
        workspace_prefix,
        include_workspace_historical: parsed
            .get("include_workspace_historical")
            .and_then(Value::as_bool)
            .unwrap_or(false),
        include_workspace_quarantined: parsed
            .get("include_workspace_quarantined")
            .and_then(Value::as_bool)
            .unwrap_or(false),
        max_candidates,
        prompt_budget_tokens,
    };

    let preview = match preview_recall(runtime_state, &request_context, request).await {
        Ok(preview) => preview,
        Err(error) => {
            return memory_tool_execution_outcome(
                b"palyra.memory.recall.attestation.v1",
                proposal_id,
                input_json,
                false,
                b"{}".to_vec(),
                format!("palyra.memory.recall failed: {}", error.message()),
            );
        }
    };

    let payload = memory_recall_tool_output_payload(&preview);
    match serde_json::to_vec(&payload) {
        Ok(output_json) => memory_tool_execution_outcome(
            b"palyra.memory.recall.attestation.v1",
            proposal_id,
            input_json,
            true,
            output_json,
            String::new(),
        ),
        Err(error) => memory_tool_execution_outcome(
            b"palyra.memory.recall.attestation.v1",
            proposal_id,
            input_json,
            false,
            b"{}".to_vec(),
            format!("palyra.memory.recall failed to serialize output: {error}"),
        ),
    }
}

pub(crate) async fn execute_memory_session_search_tool(
    runtime_state: &Arc<GatewayRuntimeState>,
    context: ToolRuntimeExecutionContext<'_>,
    proposal_id: &str,
    input_json: &[u8],
) -> ToolExecutionOutcome {
    let attestation_namespace = b"palyra.memory.session_search.attestation.v1";
    let parsed = match parse_memory_tool_object(input_json) {
        Ok(parsed) => parsed,
        Err(error) => {
            return memory_tool_execution_outcome(
                attestation_namespace,
                proposal_id,
                input_json,
                false,
                b"{}".to_vec(),
                format!("palyra.memory.session_search {error}"),
            );
        }
    };
    let query = match required_string_field(&parsed, "query") {
        Ok(value) => value,
        Err(error) => {
            return memory_tool_execution_outcome(
                attestation_namespace,
                proposal_id,
                input_json,
                false,
                b"{}".to_vec(),
                format!("palyra.memory.session_search {error}"),
            );
        }
    };
    if query.len() > MAX_MEMORY_TOOL_QUERY_BYTES {
        return memory_tool_execution_outcome(
            attestation_namespace,
            proposal_id,
            input_json,
            false,
            b"{}".to_vec(),
            format!(
                "palyra.memory.session_search query exceeds {MAX_MEMORY_TOOL_QUERY_BYTES} bytes"
            ),
        );
    }

    let requested_channel = match parsed.get("channel") {
        Some(Value::String(value)) if !value.trim().is_empty() => Some(value.trim().to_owned()),
        Some(Value::Null) | None => None,
        Some(_) => {
            return memory_tool_execution_outcome(
                attestation_namespace,
                proposal_id,
                input_json,
                false,
                b"{}".to_vec(),
                "palyra.memory.session_search channel must be a string when provided".to_owned(),
            );
        }
    };
    let channel = match requested_channel {
        Some(requested_channel) => match context.channel {
            Some(current_channel) if current_channel == requested_channel => {
                Some(requested_channel)
            }
            Some(_) => {
                return memory_tool_execution_outcome(
                    attestation_namespace,
                    proposal_id,
                    input_json,
                    false,
                    b"{}".to_vec(),
                    "palyra.memory.session_search channel must match the authenticated runtime channel"
                        .to_owned(),
                );
            }
            None => {
                return memory_tool_execution_outcome(
                    attestation_namespace,
                    proposal_id,
                    input_json,
                    false,
                    b"{}".to_vec(),
                    "palyra.memory.session_search channel override requires authenticated channel context"
                        .to_owned(),
                );
            }
        },
        None => context.channel.map(str::to_owned),
    };

    if let Err(error) =
        authorize_memory_action(context.principal, "memory.search", "memory:sessions")
    {
        return memory_tool_execution_outcome(
            attestation_namespace,
            proposal_id,
            input_json,
            false,
            b"{}".to_vec(),
            format!("memory policy denied session search request: {}", error.message()),
        );
    }

    let min_score = parsed.get("min_score").and_then(Value::as_f64).unwrap_or(0.0);
    if !min_score.is_finite() || !(0.0..=1.0).contains(&min_score) {
        return memory_tool_execution_outcome(
            attestation_namespace,
            proposal_id,
            input_json,
            false,
            b"{}".to_vec(),
            "palyra.memory.session_search min_score must be in range 0.0..=1.0".to_owned(),
        );
    }

    let top_k = match parse_optional_session_search_limit(parsed.get("top_k"), "top_k", 1, 24) {
        Ok(value) => value.unwrap_or(8),
        Err(error) => {
            return memory_tool_execution_outcome(
                attestation_namespace,
                proposal_id,
                input_json,
                false,
                b"{}".to_vec(),
                error,
            );
        }
    };
    let window_before = match parse_optional_session_search_limit(
        parsed.get("window_before"),
        "window_before",
        0,
        8,
    ) {
        Ok(value) => value.unwrap_or(2),
        Err(error) => {
            return memory_tool_execution_outcome(
                attestation_namespace,
                proposal_id,
                input_json,
                false,
                b"{}".to_vec(),
                error,
            );
        }
    };
    let window_after =
        match parse_optional_session_search_limit(parsed.get("window_after"), "window_after", 0, 8)
        {
            Ok(value) => value.unwrap_or(2),
            Err(error) => {
                return memory_tool_execution_outcome(
                    attestation_namespace,
                    proposal_id,
                    input_json,
                    false,
                    b"{}".to_vec(),
                    error,
                );
            }
        };
    let max_windows_per_session = match parse_optional_session_search_limit(
        parsed.get("max_windows_per_session"),
        "max_windows_per_session",
        1,
        8,
    ) {
        Ok(value) => value.unwrap_or(3),
        Err(error) => {
            return memory_tool_execution_outcome(
                attestation_namespace,
                proposal_id,
                input_json,
                false,
                b"{}".to_vec(),
                error,
            );
        }
    };
    let include_current_session =
        parsed.get("include_current_session").and_then(Value::as_bool).unwrap_or(false);

    let request = SessionSearchRequest {
        principal: context.principal.to_owned(),
        device_id: context.device_id.to_owned(),
        channel,
        session_id: None,
        exclude_session_id: if include_current_session {
            None
        } else {
            Some(context.session_id.to_owned())
        },
        query,
        top_k,
        min_score,
        window_before,
        window_after,
        max_windows_per_session,
        include_archived: parsed.get("include_archived").and_then(Value::as_bool).unwrap_or(false),
    };

    let outcome = match runtime_state.search_orchestrator_session_windows(request).await {
        Ok(outcome) => outcome,
        Err(error) => {
            return memory_tool_execution_outcome(
                attestation_namespace,
                proposal_id,
                input_json,
                false,
                b"{}".to_vec(),
                format!("palyra.memory.session_search failed: {}", error.message()),
            );
        }
    };
    let payload = memory_session_search_tool_output_payload(&outcome);
    match serde_json::to_vec(&payload) {
        Ok(output_json) => memory_tool_execution_outcome(
            attestation_namespace,
            proposal_id,
            input_json,
            true,
            output_json,
            String::new(),
        ),
        Err(error) => memory_tool_execution_outcome(
            attestation_namespace,
            proposal_id,
            input_json,
            false,
            b"{}".to_vec(),
            format!("palyra.memory.session_search failed to serialize output: {error}"),
        ),
    }
}

fn parse_memory_tool_object(input_json: &[u8]) -> Result<Map<String, Value>, String> {
    match serde_json::from_slice::<Value>(input_json) {
        Ok(Value::Object(map)) => Ok(map),
        Ok(_) => Err("requires JSON object input".to_owned()),
        Err(error) => Err(format!("invalid JSON input: {error}")),
    }
}

fn parse_optional_session_search_limit(
    value: Option<&Value>,
    field: &str,
    min: usize,
    max: usize,
) -> Result<Option<usize>, String> {
    match value.and_then(Value::as_u64) {
        Some(value) => Ok(Some((value as usize).clamp(min, max))),
        None if value.is_none() || matches!(value, Some(Value::Null)) => Ok(None),
        None => Err(format!(
            "palyra.memory.session_search {field} must be an integer in range {min}..={max}"
        )),
    }
}

fn required_string_field(parsed: &Map<String, Value>, field: &str) -> Result<String, String> {
    parsed
        .get(field)
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
        .ok_or_else(|| format!("requires non-empty string field '{field}'"))
}

fn parse_string_array_field(
    value: Option<&Value>,
    field: &str,
    max_items: usize,
) -> Result<Vec<String>, String> {
    let Some(value) = value else {
        return Ok(Vec::new());
    };
    let Value::Array(values) = value else {
        return Err(format!("palyra.memory.retain {field} must be an array of strings"));
    };
    if values.len() > max_items {
        return Err(format!("palyra.memory.retain {field} exceeds limit ({max_items})"));
    }
    let mut parsed = Vec::new();
    for value in values {
        let Some(raw) = value.as_str() else {
            return Err(format!("palyra.memory.retain {field} must be an array of strings"));
        };
        let normalized = raw.trim();
        if !normalized.is_empty() {
            parsed.push(normalized.to_owned());
        }
    }
    Ok(parsed)
}

fn parse_reflection_observations(parsed: &Map<String, Value>) -> Result<Vec<String>, String> {
    if let Some(value) = parsed.get("observations") {
        let Value::Array(values) = value else {
            return Err("palyra.memory.reflect observations must be an array of strings".to_owned());
        };
        let mut observations = Vec::new();
        for value in values {
            let Some(raw) = value.as_str() else {
                return Err(
                    "palyra.memory.reflect observations must be an array of strings".to_owned()
                );
            };
            let normalized = normalize_lifecycle_content(raw);
            if !normalized.is_empty() {
                observations.push(normalized);
            }
        }
        if !observations.is_empty() {
            return Ok(observations);
        }
    }
    if let Some(value) = parsed.get("messages") {
        let Value::Array(values) = value else {
            return Err("palyra.memory.reflect messages must be an array".to_owned());
        };
        let observations = values
            .iter()
            .filter_map(|value| {
                value.get("content").and_then(Value::as_str).map(normalize_lifecycle_content)
            })
            .filter(|value| !value.is_empty())
            .collect::<Vec<_>>();
        if !observations.is_empty() {
            return Ok(observations);
        }
    }
    match parsed.get("content_text").and_then(Value::as_str).map(normalize_lifecycle_content) {
        Some(value) if !value.is_empty() => Ok(value
            .split(['\n', ';'])
            .map(normalize_lifecycle_content)
            .filter(|entry| !entry.is_empty())
            .collect()),
        _ => {
            Err("palyra.memory.reflect requires observations, messages, or content_text".to_owned())
        }
    }
}

fn parse_reflection_categories(
    value: Option<&Value>,
) -> Result<Vec<MemoryReflectionCategory>, String> {
    let Some(value) = value else {
        return Ok(Vec::new());
    };
    let Value::Array(values) = value else {
        return Err("palyra.memory.reflect categories must be an array of strings".to_owned());
    };
    let mut categories = Vec::new();
    for value in values {
        let Some(raw) = value.as_str() else {
            return Err("palyra.memory.reflect categories must be an array of strings".to_owned());
        };
        let Some(category) = MemoryReflectionCategory::parse(raw) else {
            return Err(format!("palyra.memory.reflect unknown category: {raw}"));
        };
        if !categories.contains(&category) {
            categories.push(category);
        }
    }
    Ok(categories)
}

fn retain_tool_provenance(context: ToolRuntimeExecutionContext<'_>, proposal_id: &str) -> Value {
    json!({
        "tool_proposal_id": proposal_id,
        "run_id": context.run_id,
        "session_id": context.session_id,
        "principal": context.principal,
        "channel": context.channel,
        "source": "tool_call",
    })
}

async fn infer_project_memory_document_path(
    runtime_state: &Arc<GatewayRuntimeState>,
    context: ToolRuntimeExecutionContext<'_>,
) -> Option<String> {
    infer_project_memory_prefix(runtime_state, context)
        .await
        .map(|prefix| format!("{prefix}/MEMORY.md"))
}

async fn infer_project_memory_prefix(
    runtime_state: &Arc<GatewayRuntimeState>,
    context: ToolRuntimeExecutionContext<'_>,
) -> Option<String> {
    let workspace_roots = resolve_memory_agent_workspace_roots(runtime_state, context).await;
    workspace_roots
        .first()
        .and_then(|root| project_memory_prefix_from_workspace_root(root.as_path()))
}

async fn resolve_memory_agent_workspace_roots(
    runtime_state: &Arc<GatewayRuntimeState>,
    context: ToolRuntimeExecutionContext<'_>,
) -> Vec<PathBuf> {
    let workspace_roots = match runtime_state
        .resolve_agent_for_context(AgentResolveRequest {
            principal: context.principal.to_owned(),
            channel: context.channel.map(str::to_owned),
            session_id: Some(context.session_id.to_owned()),
            preferred_agent_id: None,
            persist_session_binding: false,
        })
        .await
    {
        Ok(agent_outcome) => {
            agent_outcome.agent.workspace_roots.iter().map(PathBuf::from).collect::<Vec<_>>()
        }
        Err(_) => Vec::new(),
    };
    workspace_roots_with_run_launch_context(runtime_state, context.run_id, &workspace_roots).await
}

pub(crate) fn project_memory_prefix_candidates_from_workspace_root(root: &Path) -> Vec<String> {
    let mut prefixes = Vec::new();
    if let Some(identity_prefix) = project_memory_prefix_from_workspace_root(root) {
        prefixes.push(identity_prefix);
    }
    if let Some(name) = last_normal_path_segment(root) {
        let basename_prefix = format!("projects/{name}");
        if let Ok(prefix) = normalize_workspace_prefix(basename_prefix.as_str()) {
            if !prefixes.iter().any(|existing| existing == &prefix) {
                prefixes.push(prefix);
            }
        }
    }
    prefixes
}

fn project_memory_prefix_from_workspace_root(root: &Path) -> Option<String> {
    let canonical = std::fs::canonicalize(root).unwrap_or_else(|_| root.to_path_buf());
    let name = last_normal_path_segment(canonical.as_path())?;
    let slug = project_memory_slug(name.as_str());
    let fingerprint = project_memory_root_fingerprint(canonical.as_path());
    let digest = hex::encode(Sha256::digest(fingerprint.as_bytes()));
    let hash = digest.get(..10)?;
    let segment = format!("project-{slug}-{hash}");
    let prefix = format!("projects/{segment}");
    normalize_workspace_prefix(prefix.as_str()).ok()
}

fn last_normal_path_segment(path: &Path) -> Option<String> {
    path.components().rev().find_map(|component| match component {
        Component::Normal(value) => {
            value.to_str().map(str::trim).filter(|value| !value.is_empty()).map(str::to_owned)
        }
        _ => None,
    })
}

fn project_memory_slug(name: &str) -> String {
    const MAX_SLUG_CHARS: usize = 80;

    let mut slug = String::new();
    let mut previous_separator = false;
    for ch in name.chars() {
        if ch.is_ascii_alphanumeric() {
            slug.push(ch.to_ascii_lowercase());
            previous_separator = false;
        } else if !previous_separator && !slug.is_empty() {
            slug.push('-');
            previous_separator = true;
        }
        if slug.chars().count() >= MAX_SLUG_CHARS {
            break;
        }
    }
    let slug = slug.trim_matches('-');
    if slug.is_empty() {
        "workspace".to_owned()
    } else {
        slug.to_owned()
    }
}

fn project_memory_root_fingerprint(root: &Path) -> String {
    let normalized = root.to_string_lossy().replace('\\', "/").trim_end_matches('/').to_owned();
    #[cfg(windows)]
    {
        normalized.to_ascii_lowercase()
    }
    #[cfg(not(windows))]
    {
        normalized
    }
}

fn workspace_memory_retain_path(
    parsed: &Map<String, Value>,
    scope: WorkspaceMemoryRetainScope,
    inferred_project_path: Option<&str>,
) -> Result<String, String> {
    let explicit_raw_path = optional_trimmed_string(parsed.get("workspace_path"))
        .or_else(|| optional_trimmed_string(parsed.get("workspace_prefix")))
        .or_else(|| optional_trimmed_string(parsed.get("prefix")));
    let raw_path = explicit_raw_path.clone().unwrap_or_else(|| {
        inferred_project_path.map(str::to_owned).unwrap_or_else(|| scope.default_path().to_owned())
    });
    let candidate = workspace_memory_document_candidate(raw_path.as_str());
    let normalized = match normalize_workspace_path(candidate.as_str()) {
        Ok(path_info) => path_info.normalized_path,
        Err(error) => {
            let Some(raw_path) = explicit_raw_path.as_deref() else {
                return Err(format!(
                    "workspace_path is not an allowed workspace document path: {error}"
                ));
            };
            let Some(project_candidate) = workspace_memory_project_document_candidate(raw_path)
            else {
                return Err(format!(
                    "workspace_path is not an allowed workspace document path: {error}"
                ));
            };
            normalize_workspace_path(project_candidate.as_str())
                .map_err(|fallback_error| {
                    format!(
                        "workspace_path is not an allowed workspace document path: {error}; \
                         project/workspace prefix mapping failed: {fallback_error}"
                    )
                })?
                .normalized_path
        }
    };
    if scope == WorkspaceMemoryRetainScope::Project && !normalized.starts_with("projects/") {
        return Err(
            "scope=project requires workspace_path or workspace_prefix under projects/".to_owned()
        );
    }
    Ok(normalized)
}

fn workspace_memory_search_prefix(
    explicit_prefix: Option<&str>,
    scope: &str,
    inferred_project_prefix: Option<&str>,
) -> Result<Option<String>, String> {
    let Some(raw_prefix) = explicit_prefix else {
        if scope == "project" {
            return Ok(Some(
                inferred_project_prefix
                    .map(str::to_owned)
                    .unwrap_or_else(|| "projects/default".to_owned()),
            ));
        }
        return Ok(None);
    };

    let normalized = match normalize_workspace_prefix(raw_prefix) {
        Ok(prefix) => prefix,
        Err(error) => {
            let Some(project_prefix) = workspace_memory_project_prefix_candidate(raw_prefix) else {
                return Err(format!(
                    "workspace_prefix is not an allowed workspace document prefix: {error}"
                ));
            };
            normalize_workspace_prefix(project_prefix.as_str()).map_err(|fallback_error| {
                format!(
                    "workspace_prefix is not an allowed workspace document prefix: {error}; \
                     project/workspace prefix mapping failed: {fallback_error}"
                )
            })?
        }
    };
    if scope == "project" && !normalized.starts_with("projects/") {
        return Err(
            "scope=project requires workspace_prefix under projects/ or an active project root"
                .to_owned(),
        );
    }
    Ok(Some(normalized))
}

fn workspace_memory_document_candidate(raw_path: &str) -> String {
    if workspace_memory_path_has_allowed_extension(raw_path) {
        raw_path.to_owned()
    } else {
        format!("{}/MEMORY.md", raw_path.trim_end_matches(&['/', '\\'][..]))
    }
}

fn workspace_memory_project_document_candidate(raw_path: &str) -> Option<String> {
    let target = workspace_memory_project_target(raw_path)?;
    let candidate = if workspace_memory_path_has_allowed_extension(target.as_str()) {
        format!("projects/{target}")
    } else {
        let project_target = format!("projects/{target}");
        format!("{}/MEMORY.md", project_target.trim_end_matches('/'))
    };
    Some(candidate)
}

fn workspace_memory_project_prefix_candidate(raw_path: &str) -> Option<String> {
    let target = workspace_memory_project_target(raw_path)?;
    Some(format!("projects/{}", target.trim_end_matches('/')))
}

fn workspace_memory_project_target(raw_path: &str) -> Option<String> {
    let trimmed = raw_path.trim().trim_matches('"').trim_matches('\'').trim();
    if trimmed.is_empty() {
        return None;
    }
    let normalized = trimmed.replace('\\', "/");
    let absolute_like = normalized.starts_with('/')
        || normalized.as_bytes().get(1).is_some_and(|value| *value == b':')
        || normalized.contains(":/");
    let segments = normalized
        .split('/')
        .map(str::trim)
        .filter(|segment| !segment.is_empty() && *segment != ".")
        .collect::<Vec<_>>();
    if absolute_like {
        return segments.last().map(|segment| (*segment).to_owned());
    }
    (!segments.is_empty()).then(|| segments.join("/"))
}

fn workspace_memory_path_has_allowed_extension(path: &str) -> bool {
    let lower = path.trim().to_ascii_lowercase();
    ["md", "txt", "json", "yml", "yaml"]
        .iter()
        .any(|extension| lower.ends_with(format!(".{extension}").as_str()))
}

fn workspace_memory_document_base_content(
    existing_content: Option<&str>,
    category_hint: Option<MemoryWriteCategory>,
    replaces_terms: &[String],
) -> (Option<String>, usize) {
    let Some(existing_content) = existing_content else {
        return (None, 0);
    };
    if category_hint != Some(MemoryWriteCategory::Correction) || replaces_terms.is_empty() {
        return (Some(existing_content.to_owned()), 0);
    }
    let (content, replaced_entries) =
        workspace_memory_remove_replaced_entries(existing_content, replaces_terms);
    (Some(content), replaced_entries)
}

fn workspace_memory_remove_replaced_entries(
    existing_content: &str,
    replaces_terms: &[String],
) -> (String, usize) {
    let mut output = String::new();
    let mut current_entry = Vec::<String>::new();
    let mut in_entry = false;
    let mut removed_entries = 0usize;

    for line in existing_content.lines() {
        if line.starts_with("- remembered_at_unix_ms=") {
            if !current_entry.is_empty() {
                removed_entries +=
                    flush_workspace_memory_entry(&mut output, &mut current_entry, replaces_terms);
            }
            in_entry = true;
            current_entry.push(line.to_owned());
        } else if in_entry {
            current_entry.push(line.to_owned());
        } else {
            output.push_str(line);
            output.push('\n');
        }
    }
    if !current_entry.is_empty() {
        removed_entries +=
            flush_workspace_memory_entry(&mut output, &mut current_entry, replaces_terms);
    }

    (output.trim_end().to_owned(), removed_entries)
}

fn flush_workspace_memory_entry(
    output: &mut String,
    current_entry: &mut Vec<String>,
    replaces_terms: &[String],
) -> usize {
    let entry = current_entry.join("\n");
    current_entry.clear();
    if workspace_memory_entry_matches_replacement(entry.as_str(), replaces_terms) {
        return 1;
    }
    if !output.trim_end().is_empty() {
        output.push_str("\n\n");
    }
    output.push_str(entry.trim_end());
    output.push('\n');
    0
}

fn workspace_memory_entry_matches_replacement(entry: &str, replaces_terms: &[String]) -> bool {
    let entry_tokens = workspace_memory_replacement_tokens(entry);
    if entry_tokens.is_empty() {
        return false;
    }
    replaces_terms.iter().any(|term| {
        workspace_memory_replacement_tokens(term)
            .into_iter()
            .filter(|token| {
                token.chars().count() >= 5 || token.chars().any(|ch| ch.is_ascii_digit())
            })
            .any(|token| entry_tokens.iter().any(|entry_token| entry_token == &token))
    })
}

fn workspace_memory_replacement_tokens(input: &str) -> Vec<String> {
    let mut tokens = Vec::new();
    let mut current = String::new();
    for ch in input.chars() {
        if ch.is_alphanumeric() || ch == '_' {
            current.extend(ch.to_lowercase());
        } else if !current.is_empty() {
            tokens.push(std::mem::take(&mut current));
        }
    }
    if !current.is_empty() {
        tokens.push(current);
    }
    tokens
}

#[allow(clippy::too_many_arguments)]
fn workspace_memory_document_content(
    existing_content: Option<&str>,
    title: &str,
    content_text: &str,
    source: MemorySource,
    tags: &[String],
    confidence: Option<f64>,
    ttl_unix_ms: Option<i64>,
    now_unix_ms: i64,
) -> (String, bool) {
    if let Some(existing) = existing_content {
        if existing.contains(content_text) {
            return (existing.to_owned(), false);
        }
        let mut next = existing.trim_end().to_owned();
        if !next.is_empty() {
            next.push_str("\n\n");
        }
        next.push_str(
            workspace_memory_markdown_entry(
                content_text,
                source,
                tags,
                confidence,
                ttl_unix_ms,
                now_unix_ms,
            )
            .as_str(),
        );
        next.push('\n');
        return (next, true);
    }

    let mut content = format!("# {title}\n\n");
    content.push_str(
        workspace_memory_markdown_entry(
            content_text,
            source,
            tags,
            confidence,
            ttl_unix_ms,
            now_unix_ms,
        )
        .as_str(),
    );
    content.push('\n');
    (content, true)
}

fn workspace_memory_markdown_entry(
    content_text: &str,
    source: MemorySource,
    tags: &[String],
    confidence: Option<f64>,
    ttl_unix_ms: Option<i64>,
    now_unix_ms: i64,
) -> String {
    let mut metadata =
        vec![format!("remembered_at_unix_ms={now_unix_ms}"), format!("source={}", source.as_str())];
    if let Some(confidence) = confidence {
        metadata.push(format!("confidence={confidence:.3}"));
    }
    if let Some(ttl_unix_ms) = ttl_unix_ms {
        metadata.push(format!("ttl_unix_ms={ttl_unix_ms}"));
    }
    if !tags.is_empty() {
        metadata.push(format!("tags={}", tags.join(",")));
    }
    let indented_content =
        content_text.lines().map(|line| format!("  {}", line.trim_end())).collect::<Vec<_>>();
    format!("- {}\n{}", metadata.join(" "), indented_content.join("\n"))
}

fn workspace_document_output_payload(document: &WorkspaceDocumentRecord) -> Value {
    json!({
        "document_id": document.document_id.as_str(),
        "path": document.path.as_str(),
        "parent_path": document.parent_path.as_deref(),
        "title": document.title.as_str(),
        "kind": document.kind.as_str(),
        "document_class": document.document_class.as_str(),
        "state": document.state.as_str(),
        "prompt_binding": document.prompt_binding.as_str(),
        "latest_version": document.latest_version,
        "updated_at_unix_ms": document.updated_at_unix_ms,
    })
}

fn memory_hit_provenance(hit: &MemorySearchHit) -> Value {
    json!({
        "memory_id": hit.item.memory_id.as_str(),
        "source": hit.item.source.as_str(),
        "scope": memory_item_scope_label(&hit.item),
        "session_id": hit.item.session_id.as_deref(),
        "channel": hit.item.channel.as_deref(),
        "content_hash": hit.item.content_hash.as_str(),
        "fence": MEMORY_CONTEXT_FENCE_VERSION,
    })
}

fn memory_item_scope_label(item: &crate::journal::MemoryItemRecord) -> &'static str {
    if item.session_id.is_some() {
        "session"
    } else if item.channel.is_some() {
        "channel"
    } else {
        "principal"
    }
}

fn memory_item_write_resource(item: &MemoryItemRecord) -> String {
    if let Some(session_id) = item.session_id.as_deref() {
        format!("memory:session:{session_id}")
    } else if let Some(channel) = item.channel.as_deref() {
        format!("memory:channel:{channel}")
    } else {
        "memory:principal".to_owned()
    }
}

struct WorkspaceMemoryRetainSerialization<'a> {
    namespace: &'static [u8],
    proposal_id: &'a str,
    input_json: &'a [u8],
    scope: WorkspaceMemoryRetainScope,
    document: &'a WorkspaceDocumentRecord,
    appended: bool,
    provenance: Value,
    source_normalization: Option<Value>,
    replaced_entries: usize,
}

fn serialize_workspace_memory_retain_outcome(
    input: WorkspaceMemoryRetainSerialization<'_>,
) -> ToolExecutionOutcome {
    let mut payload = json!({
        "status": if input.replaced_entries > 0 {
            "merged"
        } else if input.appended {
            "retained"
        } else {
            "updated_existing"
        },
        "reason": if input.replaced_entries > 0 {
            "workspace memory correction replaced obsolete entries"
        } else if input.appended {
            "memory retained in workspace document"
        } else {
            "workspace document already contained this memory content"
        },
        "scope": input.scope.as_str(),
        "review_state": "written",
        "approval_required": false,
        "trust_label": "workspace_memory",
        "durable_memory_write": true,
        "content_appended": input.appended,
        "replaced_entries": input.replaced_entries,
        "workspace_prefix": input.document.parent_path.as_deref(),
        "visibility": {
            "scope": input.scope.as_str(),
            "cross_session": true,
            "claim_boundary": "workspace/project memory is stored in an indexed workspace document and is available through palyra.memory.search or palyra.memory.recall with workspace/project scope",
        },
        "provenance": input.provenance,
        "document": workspace_document_output_payload(input.document),
    });
    if let Some(normalization) = input.source_normalization {
        if let Some(fields) = payload.as_object_mut() {
            fields.insert("source_normalization".to_owned(), normalization);
        }
    }
    match serde_json::to_vec(&payload) {
        Ok(output_json) => memory_tool_execution_outcome(
            input.namespace,
            input.proposal_id,
            input.input_json,
            true,
            output_json,
            String::new(),
        ),
        Err(error) => memory_tool_execution_outcome(
            input.namespace,
            input.proposal_id,
            input.input_json,
            false,
            b"{}".to_vec(),
            format!("palyra.memory.retain failed to serialize workspace output: {error}"),
        ),
    }
}

fn serialize_memory_lifecycle_outcome(
    namespace: &'static [u8],
    proposal_id: &str,
    input_json: &[u8],
    outcome: &MemoryLifecycleRetainOutcome,
    source_normalization: Option<Value>,
) -> ToolExecutionOutcome {
    let review_state = memory_lifecycle_review_state(outcome);
    let review_required = review_state == "not_written_requires_review";
    let mut payload = json!({
        "status": outcome.status.as_str(),
        "reason": outcome.reason.as_str(),
        "scope": outcome.scope.as_str(),
        "review_state": review_state,
        "approval_required": review_required,
        "trust_label": outcome.trust_label.as_str(),
        "durable_memory_write": outcome.durable_memory_write,
        "matched_memory_id": outcome.matched_memory_id.as_deref(),
        "write_classification": outcome.write_classification.clone(),
        "visibility": memory_lifecycle_visibility_payload(outcome),
        "provenance": outcome.provenance.clone(),
        "item": outcome.item.as_ref().map(memory_item_output_payload),
    });
    if let Some(review) = memory_lifecycle_review_payload(outcome) {
        if let Some(fields) = payload.as_object_mut() {
            fields.insert("review".to_owned(), review);
        }
    }
    if let Some(normalization) = source_normalization {
        if let Some(fields) = payload.as_object_mut() {
            fields.insert("source_normalization".to_owned(), normalization);
        }
    }
    let success = outcome.durable_memory_write;
    let error = if success {
        String::new()
    } else {
        format!(
            "palyra.memory.retain did not write memory: status={} review_state={} durable_memory_write=false reason={}; do not claim this memory is stored or available for future recall",
            outcome.status.as_str(),
            review_state,
            outcome.reason
        )
    };
    match serde_json::to_vec(&payload) {
        Ok(output_json) => memory_tool_execution_outcome(
            namespace,
            proposal_id,
            input_json,
            success,
            output_json,
            error,
        ),
        Err(error) => memory_tool_execution_outcome(
            namespace,
            proposal_id,
            input_json,
            false,
            b"{}".to_vec(),
            format!("palyra.memory.retain failed to serialize output: {error}"),
        ),
    }
}

fn memory_lifecycle_visibility_payload(outcome: &MemoryLifecycleRetainOutcome) -> Value {
    let cross_session =
        outcome.durable_memory_write && outcome.scope == MemoryLifecycleScope::Principal;
    let claim_boundary = if cross_session {
        "principal-scoped memory is available to future sessions for this principal"
    } else if outcome.durable_memory_write {
        "memory was written, but this scope is not principal-wide; do not claim it will affect future sessions or principal recall"
    } else {
        "memory was not written; do not claim it is available for future recall"
    };
    json!({
        "scope": outcome.scope.as_str(),
        "cross_session": cross_session,
        "claim_boundary": claim_boundary,
    })
}

fn memory_lifecycle_review_state(outcome: &MemoryLifecycleRetainOutcome) -> &'static str {
    if outcome.durable_memory_write {
        "written"
    } else if outcome.status == MemoryLifecycleStatus::NeedsReview {
        "not_written_requires_review"
    } else {
        "not_written"
    }
}

fn memory_lifecycle_review_payload(outcome: &MemoryLifecycleRetainOutcome) -> Option<Value> {
    if outcome.status != MemoryLifecycleStatus::NeedsReview {
        return None;
    }
    Some(json!({
        "state": "requires_manual_operator_review",
        "queue": "not_queued",
        "review_identifier": Value::Null,
        "completion_kind": "manual_memory_ingest",
        "completion_commands": [memory_lifecycle_review_command(outcome)],
        "operator_note": "No durable memory was written. Review the original retained content, then either run the ingest command with approved content or leave the memory unwritten.",
    }))
}

fn memory_lifecycle_review_command(outcome: &MemoryLifecycleRetainOutcome) -> String {
    let mut command =
        "palyra memory ingest \"<reviewed memory content>\" --source manual --confidence 1.0"
            .to_owned();
    if outcome.scope == MemoryLifecycleScope::Session {
        if let Some(session_id) = outcome
            .provenance
            .get("session_id")
            .and_then(Value::as_str)
            .and_then(memory_lifecycle_review_command_arg)
        {
            command.push_str(" --session ");
            command.push_str(session_id);
        }
    }
    if outcome.scope == MemoryLifecycleScope::Channel {
        if let Some(channel) = outcome
            .provenance
            .get("channel")
            .and_then(Value::as_str)
            .and_then(memory_lifecycle_review_command_arg)
        {
            command.push_str(" --channel ");
            command.push_str(channel);
        }
    }
    command
}

fn memory_lifecycle_review_command_arg(raw: &str) -> Option<&str> {
    let value = raw.trim();
    if value.is_empty() || value.len() > 256 || value.len() != raw.len() {
        return None;
    }
    if value
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b':' | b'-' | b'_' | b'.'))
    {
        Some(value)
    } else {
        None
    }
}

fn memory_item_output_payload(item: &crate::journal::MemoryItemRecord) -> Value {
    json!({
        "memory_id": item.memory_id.as_str(),
        "source": item.source.as_str(),
        "scope": memory_item_scope_label(item),
        "channel": item.channel.as_deref(),
        "session_id": item.session_id.as_deref(),
        "content_text": redact_memory_text_for_output(item.content_text.as_str()),
        "content_hash": item.content_hash.as_str(),
        "tags": item.tags.clone(),
        "confidence": item.confidence,
        "ttl_unix_ms": item.ttl_unix_ms,
        "created_at_unix_ms": item.created_at_unix_ms,
        "updated_at_unix_ms": item.updated_at_unix_ms,
        "trust_label": MEMORY_TRUST_LABEL_RETRIEVED,
        "provenance": {
            "memory_id": item.memory_id.as_str(),
            "source": item.source.as_str(),
            "scope": memory_item_scope_label(item),
            "content_hash": item.content_hash.as_str(),
            "fence": MEMORY_CONTEXT_FENCE_VERSION,
        },
    })
}

fn serialize_memory_reflection_outcome(
    namespace: &'static [u8],
    proposal_id: &str,
    input_json: &[u8],
    outcome: &MemoryReflectionOutcome,
) -> ToolExecutionOutcome {
    match serde_json::to_vec(outcome) {
        Ok(output_json) => memory_tool_execution_outcome(
            namespace,
            proposal_id,
            input_json,
            true,
            output_json,
            String::new(),
        ),
        Err(error) => memory_tool_execution_outcome(
            namespace,
            proposal_id,
            input_json,
            false,
            b"{}".to_vec(),
            format!("palyra.memory.reflect failed to serialize output: {error}"),
        ),
    }
}

fn parse_memory_source_literal(raw: &str) -> Option<MemorySource> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "tape:user_message" | "tape_user_message" | "user_message" => {
            Some(MemorySource::TapeUserMessage)
        }
        "tape:tool_result" | "tape_tool_result" | "tool_result" => {
            Some(MemorySource::TapeToolResult)
        }
        "summary" => Some(MemorySource::Summary),
        "manual" => Some(MemorySource::Manual),
        "import" => Some(MemorySource::Import),
        _ => None,
    }
}

fn memory_tool_execution_outcome(
    attestation_namespace: &'static [u8],
    proposal_id: &str,
    input_json: &[u8],
    success: bool,
    output_json: Vec<u8>,
    error: String,
) -> ToolExecutionOutcome {
    let executed_at_unix_ms = current_unix_ms();
    let mut hasher = Sha256::new();
    hasher.update(attestation_namespace);
    hasher.update((proposal_id.len() as u64).to_be_bytes());
    hasher.update(proposal_id.as_bytes());
    hasher.update((input_json.len() as u64).to_be_bytes());
    hasher.update(input_json);
    hasher.update([u8::from(success)]);
    hasher.update((output_json.len() as u64).to_be_bytes());
    hasher.update(output_json.as_slice());
    hasher.update((error.len() as u64).to_be_bytes());
    hasher.update(error.as_bytes());
    hasher.update(executed_at_unix_ms.to_be_bytes());
    let execution_sha256 = hex::encode(hasher.finalize());

    ToolExecutionOutcome {
        success,
        output_json,
        error,
        attestation: ToolAttestation {
            attestation_id: Ulid::new().to_string(),
            execution_sha256,
            executed_at_unix_ms,
            timed_out: false,
            executor: "memory_runtime".to_owned(),
            sandbox_enforcement: "none".to_owned(),
        },
    }
}

fn parse_optional_recall_limit(value: Option<&Value>, max: usize) -> Result<Option<usize>, String> {
    match value.and_then(Value::as_u64) {
        Some(value) => Ok(Some((value as usize).clamp(0, max))),
        None if value.is_none() || matches!(value, Some(Value::Null)) => Ok(None),
        None => {
            Err(format!("palyra.memory.recall numeric limits must be integers in range 0..={max}"))
        }
    }
}

fn optional_trimmed_string(value: Option<&Value>) -> Option<String> {
    value
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
}

fn memory_retain_scope_text(parsed: &Map<String, Value>) -> String {
    memory_scope_text(parsed, DEFAULT_MEMORY_RETAIN_SCOPE)
}

fn memory_search_scope_text(parsed: &Map<String, Value>) -> String {
    if !parsed.contains_key("scope")
        && (optional_trimmed_string(parsed.get("workspace_prefix")).is_some()
            || optional_trimmed_string(parsed.get("prefix")).is_some())
    {
        return "workspace".to_owned();
    }
    memory_scope_text(parsed, DEFAULT_MEMORY_SEARCH_SCOPE)
}

fn memory_recall_workspace_scope_text(parsed: &Map<String, Value>) -> String {
    let scope = memory_scope_text(parsed, "workspace");
    if scope == "project" {
        "project".to_owned()
    } else {
        "workspace".to_owned()
    }
}

fn memory_scope_text(parsed: &Map<String, Value>, default_scope: &str) -> String {
    parsed
        .get("scope")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_ascii_lowercase)
        .unwrap_or_else(|| default_scope.to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        application::recall::{RecallBudgetExplain, RecallPlan, StructuredRecallOutput},
        journal::{WorkspaceDocumentRecord, WorkspaceScoreBreakdown, WorkspaceSearchHit},
    };

    #[test]
    fn parse_session_search_limits_match_schema_bounds() {
        assert_eq!(
            parse_optional_session_search_limit(
                Some(&serde_json::json!(0)),
                "window_before",
                0,
                8,
            )
            .expect("zero window should be valid"),
            Some(0)
        );
        assert_eq!(
            parse_optional_session_search_limit(Some(&serde_json::json!(0)), "top_k", 1, 24)
                .expect("top_k should clamp to minimum"),
            Some(1)
        );
        assert_eq!(
            parse_optional_session_search_limit(
                Some(&serde_json::json!(99)),
                "window_after",
                0,
                8,
            )
            .expect("window should clamp to maximum"),
            Some(8)
        );
        assert_eq!(
            parse_optional_session_search_limit(None, "top_k", 1, 24)
                .expect("absent limit should use caller default"),
            None
        );
        let error = parse_optional_session_search_limit(
            Some(&serde_json::json!("2")),
            "window_before",
            0,
            8,
        )
        .expect_err("string limits should be rejected");

        assert!(error.contains("window_before must be an integer"));
    }

    #[test]
    fn memory_recall_payload_sanitizes_workspace_hits() {
        let preview = RecallPreviewEnvelope {
            query: "deployment notes".to_owned(),
            memory_hits: Vec::new(),
            workspace_hits: vec![WorkspaceSearchHit {
                document: workspace_document_record(
                    "P0C_FULL_WORKSPACE_SECRET_DO_NOT_EXPOSE__line1\nline2 private",
                ),
                version: 3,
                chunk_index: 1,
                chunk_count: 4,
                snippet: "visible deployment excerpt api_key=secret123".to_owned(),
                score: 0.82,
                reason: "lexical match".to_owned(),
                breakdown: WorkspaceScoreBreakdown {
                    lexical_score: 0.8,
                    vector_score: 0.6,
                    recency_score: 0.2,
                    source_quality_score: 0.9,
                    final_score: 0.82,
                },
            }],
            transcript_hits: Vec::new(),
            checkpoint_hits: Vec::new(),
            compaction_hits: Vec::new(),
            top_candidates: Vec::new(),
            structured_output: structured_recall_output(),
            plan: RecallPlan {
                original_query: "deployment notes".to_owned(),
                expanded_queries: Vec::new(),
                session_scoped: false,
                budget: RecallBudgetExplain { prompt_budget_tokens: 1_800, candidate_limit: 8 },
                sources: Vec::new(),
            },
            diagnostics: Vec::new(),
            parameter_delta: json!({}),
            prompt_preview: "preview".to_owned(),
        };

        let payload = memory_recall_tool_output_payload(&preview);
        let encoded = serde_json::to_string(&payload).expect("payload should serialize");
        let workspace_hit =
            payload["workspace_hits"][0].as_object().expect("workspace hit should be an object");
        let document = workspace_hit
            .get("document")
            .and_then(Value::as_object)
            .expect("workspace hit should include document metadata");

        assert_eq!(document.get("document_id"), Some(&json!("workspace-doc-1")));
        assert!(
            !document.contains_key("content_text"),
            "tool output must not serialize full workspace document content"
        );
        assert!(
            !document.contains_key("content_hash"),
            "tool output should avoid stable content fingerprints"
        );
        assert!(
            !encoded.contains("P0C_FULL_WORKSPACE_SECRET_DO_NOT_EXPOSE"),
            "full workspace body must not leak through recall output"
        );
        assert!(
            !encoded.contains("line2 private"),
            "workspace content outside the snippet must stay out of tool output"
        );
        assert!(
            !encoded.contains("secret123"),
            "workspace snippets should be redacted before returning to the model"
        );
        let snippet = workspace_hit
            .get("snippet")
            .and_then(Value::as_str)
            .expect("workspace hit should include a snippet");
        assert!(!snippet.is_empty(), "workspace snippets should remain present after sanitization");
        assert_ne!(
            snippet, "visible deployment excerpt api_key=secret123",
            "workspace snippets should pass through the redaction layer"
        );
    }

    #[test]
    fn retain_visibility_distinguishes_session_from_principal_scope() {
        let mut outcome = MemoryLifecycleRetainOutcome {
            status: MemoryLifecycleStatus::Retained,
            reason: "memory retained in lifecycle store".to_owned(),
            scope: MemoryLifecycleScope::Session,
            trust_label: "retrieved_memory".to_owned(),
            durable_memory_write: true,
            item: None,
            matched_memory_id: None,
            write_classification: None,
            provenance: serde_json::json!({}),
        };

        let session_visibility = memory_lifecycle_visibility_payload(&outcome);
        assert_eq!(session_visibility["cross_session"], false);
        assert!(session_visibility["claim_boundary"]
            .as_str()
            .unwrap_or_default()
            .contains("do not claim"));

        outcome.scope = MemoryLifecycleScope::Principal;
        let principal_visibility = memory_lifecycle_visibility_payload(&outcome);
        assert_eq!(principal_visibility["cross_session"], true);
        assert!(principal_visibility["claim_boundary"]
            .as_str()
            .unwrap_or_default()
            .contains("future sessions"));
    }

    #[test]
    fn memory_tool_scopes_default_to_all_memory_search() {
        let parsed = Map::new();

        assert_eq!(memory_retain_scope_text(&parsed), "principal");
        assert_eq!(memory_search_scope_text(&parsed), "all");
        assert_eq!(memory_recall_workspace_scope_text(&parsed), "workspace");

        let mut explicit = Map::new();
        explicit.insert("scope".to_owned(), serde_json::json!("session"));

        assert_eq!(memory_retain_scope_text(&explicit), "session");
        assert_eq!(memory_search_scope_text(&explicit), "session");

        let mut workspace_prefix = Map::new();
        workspace_prefix.insert("workspace_prefix".to_owned(), serde_json::json!("projects/S033"));
        assert_eq!(memory_search_scope_text(&workspace_prefix), "workspace");

        let mut project_recall = Map::new();
        project_recall.insert("scope".to_owned(), serde_json::json!("project"));
        assert_eq!(memory_recall_workspace_scope_text(&project_recall), "project");
    }

    #[test]
    fn workspace_memory_replace_payload_marks_document_replaced_in_place() {
        let document = workspace_document_record(
            "S034-PREF-20260602 use Playwright for UI E2E tests in this project.",
        );

        let payload = workspace_memory_replace_payload("workspace-doc-1", "old-hash", &document);

        assert_eq!(payload["status"], "workspace_document_replaced");
        assert_eq!(payload["durable_memory_write"], true);
        assert_eq!(payload["workspace_document_id"].as_str(), Some(document.document_id.as_str()));
        assert_eq!(payload["previous_content_hash"].as_str(), Some("old-hash"));
        assert!(payload["claim_boundary"]
            .as_str()
            .unwrap_or_default()
            .contains("replaced in place"));
    }

    #[test]
    fn workspace_memory_retain_path_defaults_and_validates_project_scope() {
        let parsed = Map::new();
        assert_eq!(
            workspace_memory_retain_path(&parsed, WorkspaceMemoryRetainScope::Workspace, None)
                .expect("workspace default should be valid"),
            "MEMORY.md"
        );
        assert_eq!(
            workspace_memory_retain_path(&parsed, WorkspaceMemoryRetainScope::Project, None)
                .expect("project default should be valid"),
            "projects/default/MEMORY.md"
        );

        let mut with_prefix = Map::new();
        with_prefix.insert("workspace_prefix".to_owned(), json!("projects/palyra"));
        assert_eq!(
            workspace_memory_retain_path(&with_prefix, WorkspaceMemoryRetainScope::Project, None)
                .expect("project prefix should write to nested MEMORY.md"),
            "projects/palyra/MEMORY.md"
        );

        let mut outside_project = Map::new();
        outside_project.insert("workspace_path".to_owned(), json!("MEMORY.md"));
        let error = workspace_memory_retain_path(
            &outside_project,
            WorkspaceMemoryRetainScope::Project,
            None,
        )
        .expect_err("project scope must stay under projects/");
        assert!(error.contains("scope=project"), "{error}");
    }

    #[test]
    fn workspace_memory_retain_path_uses_inferred_project_default() {
        let parsed = Map::new();
        assert_eq!(
            workspace_memory_retain_path(
                &parsed,
                WorkspaceMemoryRetainScope::Project,
                Some("projects/project-client-portal-deadbeef00/MEMORY.md"),
            )
            .expect("inferred project path should be valid"),
            "projects/project-client-portal-deadbeef00/MEMORY.md"
        );
    }

    #[test]
    fn project_memory_prefix_uses_workspace_root_identity() {
        let prefix = project_memory_prefix_from_workspace_root(Path::new("/tmp/client-portal"))
            .expect("workspace root should produce a project prefix");
        assert!(prefix.starts_with("projects/project-client-portal-"), "{prefix}");
        assert!(normalize_workspace_prefix(prefix.as_str()).is_ok());
    }

    #[test]
    fn project_memory_prefix_candidates_include_explicit_basename_prefix() {
        let prefixes =
            project_memory_prefix_candidates_from_workspace_root(Path::new("/tmp/S079-project-A"));

        assert!(
            prefixes.iter().any(|prefix| prefix.starts_with("projects/project-s079-project-a-")),
            "{prefixes:?}"
        );
        assert!(prefixes.iter().any(|prefix| prefix == "projects/S079-project-A"), "{prefixes:?}");
    }

    #[test]
    fn workspace_memory_search_prefix_maps_project_inputs() {
        assert_eq!(
            workspace_memory_search_prefix(
                None,
                "project",
                Some("projects/project-client-portal-deadbeef00"),
            )
            .expect("inferred project prefix should be accepted"),
            Some("projects/project-client-portal-deadbeef00".to_owned())
        );
        assert_eq!(
            workspace_memory_search_prefix(Some("client-portal"), "project", None)
                .expect("bare project prefix should map under projects"),
            Some("projects/client-portal".to_owned())
        );
        assert_eq!(
            workspace_memory_search_prefix(Some("projects/client-portal"), "project", None)
                .expect("project prefix should remain scoped"),
            Some("projects/client-portal".to_owned())
        );
        assert!(
            workspace_memory_search_prefix(Some("MEMORY.md"), "project", None).is_err(),
            "project search must not widen to root workspace memory"
        );
    }

    #[test]
    fn workspace_memory_retain_path_accepts_unscoped_project_prefixes() {
        let mut with_project_prefix = Map::new();
        with_project_prefix.insert("workspace_prefix".to_owned(), json!("client-audit-20260527"));
        assert_eq!(
            workspace_memory_retain_path(
                &with_project_prefix,
                WorkspaceMemoryRetainScope::Project,
                None,
            )
            .expect("bare project prefix should map under projects"),
            "projects/client-audit-20260527/MEMORY.md"
        );

        let mut with_nested_workspace_file = Map::new();
        with_nested_workspace_file
            .insert("workspace_path".to_owned(), json!("project-notes/notes.md"));
        assert_eq!(
            workspace_memory_retain_path(
                &with_nested_workspace_file,
                WorkspaceMemoryRetainScope::Workspace,
                None,
            )
            .expect("bare workspace document path should map under projects"),
            "projects/project-notes/notes.md"
        );
    }

    #[test]
    fn workspace_memory_retain_path_maps_absolute_workspace_roots_to_project_basename() {
        for raw_path in [
            r"C:\agent-workspaces\client-audit-20260527",
            "/agent-workspaces/client-audit-20260527",
        ] {
            let mut parsed = Map::new();
            parsed.insert("workspace_path".to_owned(), json!(raw_path));

            assert_eq!(
                workspace_memory_retain_path(&parsed, WorkspaceMemoryRetainScope::Project, None)
                    .expect("absolute workspace roots should map to logical project memory"),
                "projects/client-audit-20260527/MEMORY.md"
            );
        }
    }

    #[test]
    fn workspace_memory_document_content_appends_without_exact_duplicates() {
        let tags = vec!["project".to_owned(), "decision".to_owned()];
        let (created, created_appended) = workspace_memory_document_content(
            None,
            "Project Memory",
            "Use the local test harness state root for Windows E2E.",
            MemorySource::Manual,
            tags.as_slice(),
            Some(0.9),
            None,
            1_747_000_000_000,
        );
        assert!(created_appended);
        assert!(created.contains("# Project Memory"));
        assert!(created.contains("source=manual"));
        assert!(created.contains("confidence=0.900"));
        assert!(created.contains("Use the local test harness state root"));

        let (deduped, duplicate_appended) = workspace_memory_document_content(
            Some(created.as_str()),
            "Project Memory",
            "Use the local test harness state root for Windows E2E.",
            MemorySource::Manual,
            tags.as_slice(),
            Some(0.9),
            None,
            1_747_000_000_001,
        );
        assert!(!duplicate_appended);
        assert_eq!(deduped, created);

        let (updated, updated_appended) = workspace_memory_document_content(
            Some(created.as_str()),
            "Project Memory",
            "Run MiniMax smoke before claiming onboarding success.",
            MemorySource::Manual,
            tags.as_slice(),
            Some(0.8),
            None,
            1_747_000_000_002,
        );
        assert!(updated_appended);
        assert!(updated.contains("Run MiniMax smoke"));
    }

    #[test]
    fn workspace_memory_document_base_content_removes_corrected_entries() {
        let existing = "# Project Memory\n\n- remembered_at_unix_ms=1 source=manual\n  Use Mocha for browser checks.\n\n- remembered_at_unix_ms=2 source=manual\n  Keep reports concise.\n";
        let replaces_terms = vec!["Mocha".to_owned()];
        let (base, replaced_entries) = workspace_memory_document_base_content(
            Some(existing),
            Some(MemoryWriteCategory::Correction),
            replaces_terms.as_slice(),
        );
        let base = base.expect("existing content should remain present");

        assert_eq!(replaced_entries, 1);
        assert!(!base.contains("Mocha"), "{base}");
        assert!(base.contains("Keep reports concise."), "{base}");

        let (updated, appended) = workspace_memory_document_content(
            Some(base.as_str()),
            "Project Memory",
            "Use Playwright for browser checks.",
            MemorySource::Manual,
            &[],
            Some(0.9),
            None,
            3,
        );
        assert!(appended);
        assert!(updated.contains("Use Playwright for browser checks."));
        assert!(!updated.contains("Use Mocha for browser checks."));
    }

    fn workspace_document_record(content_text: &str) -> WorkspaceDocumentRecord {
        WorkspaceDocumentRecord {
            document_id: "workspace-doc-1".to_owned(),
            principal: "user:ops".to_owned(),
            channel: Some("console".to_owned()),
            agent_id: Some("agent-1".to_owned()),
            latest_session_id: Some("session-1".to_owned()),
            path: "docs/deploy.md".to_owned(),
            parent_path: Some("docs".to_owned()),
            title: "Deploy".to_owned(),
            kind: "markdown".to_owned(),
            document_class: "workspace".to_owned(),
            state: "active".to_owned(),
            prompt_binding: "context".to_owned(),
            risk_state: "clean".to_owned(),
            risk_reasons: Vec::new(),
            pinned: true,
            manual_override: false,
            template_id: None,
            template_version: None,
            source_memory_id: None,
            latest_version: 3,
            content_text: content_text.to_owned(),
            content_hash: "full-content-hash".to_owned(),
            created_at_unix_ms: 1_000,
            updated_at_unix_ms: 2_000,
            deleted_at_unix_ms: None,
            last_recalled_at_unix_ms: Some(2_500),
        }
    }

    fn structured_recall_output() -> StructuredRecallOutput {
        StructuredRecallOutput {
            summary: String::new(),
            facts: Vec::new(),
            evidence: Vec::new(),
            unresolved: Vec::new(),
            contradictions: Vec::new(),
            source_refs: Vec::new(),
            provider_usage: Vec::new(),
            synthesis_hash: "empty".to_owned(),
            why_relevant_now: String::new(),
            suggested_next_step: String::new(),
            confidence: None,
        }
    }
}
