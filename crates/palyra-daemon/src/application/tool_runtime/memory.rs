use std::sync::Arc;

use serde_json::{json, Map, Value};
use sha2::{Digest, Sha256};
use ulid::Ulid;

use crate::{
    application::{
        memory::{
            normalize_lifecycle_content, redact_memory_text_for_output, reflect_memory_candidates,
            ttl_unix_ms_from_input, MemoryLifecycleProvider, MemoryLifecycleRetainOutcome,
            MemoryLifecycleRetainRequest, MemoryLifecycleScope, MemoryReflectionCategory,
            MemoryReflectionOutcome, MemoryReflectionRequest, MEMORY_CONTEXT_FENCE_VERSION,
            MEMORY_TRUST_LABEL_RETRIEVED,
        },
        recall::{preview_recall, RecallPreviewEnvelope, RecallRequest},
        service_authorization::authorize_memory_action,
    },
    gateway::{
        current_unix_ms, GatewayRuntimeState, ToolRuntimeExecutionContext, MAX_MEMORY_SEARCH_TOP_K,
        MAX_MEMORY_TOOL_QUERY_BYTES, MAX_MEMORY_TOOL_TAGS,
    },
    journal::{MemorySearchHit, MemorySearchRequest, MemorySource},
    tool_protocol::{ToolAttestation, ToolExecutionOutcome},
    transport::grpc::auth::RequestContext,
};

const DEFAULT_MEMORY_RECALL_MAX_CANDIDATES: usize = 8;
const MAX_MEMORY_RECALL_MAX_CANDIDATES: usize = 12;
const DEFAULT_MEMORY_RECALL_PROMPT_BUDGET_TOKENS: usize = 1_800;
const MIN_MEMORY_RECALL_PROMPT_BUDGET_TOKENS: usize = 512;
const MAX_MEMORY_RECALL_PROMPT_BUDGET_TOKENS: usize = 4_096;

pub(crate) fn memory_search_tool_output_payload(search_hits: &[MemorySearchHit]) -> Value {
    json!({
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

pub(crate) fn memory_recall_tool_output_payload(preview: &RecallPreviewEnvelope) -> Value {
    let memory_hits = memory_search_tool_output_payload(preview.memory_hits.as_slice())
        .get("hits")
        .cloned()
        .unwrap_or_else(|| Value::Array(Vec::new()));
    json!({
        "query": preview.query,
        "memory_hits": memory_hits,
        "workspace_hits": preview.workspace_hits,
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
    let scope = match MemoryLifecycleScope::parse(parsed.get("scope").and_then(Value::as_str)) {
        Ok(scope) => scope,
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
    let source = match parsed.get("source").and_then(Value::as_str) {
        Some(raw) => match parse_memory_source_literal(raw) {
            Some(source) => source,
            None => {
                return memory_tool_execution_outcome(
                    namespace,
                    proposal_id,
                    input_json,
                    false,
                    b"{}".to_vec(),
                    format!("palyra.memory.retain unknown source value: {raw}"),
                );
            }
        },
        None => MemorySource::Manual,
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
    let provenance = parsed
        .get("provenance")
        .cloned()
        .unwrap_or_else(|| retain_tool_provenance(context, proposal_id));

    let provider = MemoryLifecycleProvider::new(Arc::clone(runtime_state));
    let outcome = match provider
        .retain(MemoryLifecycleRetainRequest {
            principal: context.principal.to_owned(),
            channel: context.channel.map(str::to_owned),
            session_id: context.session_id.to_owned(),
            scope,
            source,
            content_text,
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
    serialize_memory_lifecycle_outcome(namespace, proposal_id, input_json, &outcome)
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
    principal: &str,
    channel: Option<&str>,
    session_id: &str,
    proposal_id: &str,
    input_json: &[u8],
) -> ToolExecutionOutcome {
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

    let scope = parsed.get("scope").and_then(Value::as_str).unwrap_or("session");
    let (channel_scope, session_scope, resource) = match scope {
        "principal" => {
            let channel_scope = channel.map(str::to_owned);
            let resource = channel_scope
                .as_deref()
                .map(|value| format!("memory:channel:{value}"))
                .unwrap_or_else(|| "memory:principal".to_owned());
            (channel_scope, None, resource)
        }
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
                "palyra.memory.search scope must be one of: session|channel|principal".to_owned(),
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
    let tags = match parsed.get("tags") {
        Some(Value::Array(values)) => {
            if values.len() > MAX_MEMORY_TOOL_TAGS {
                return memory_tool_execution_outcome(
                    attestation_namespace,
                    proposal_id,
                    input_json,
                    false,
                    b"{}".to_vec(),
                    format!("palyra.memory.search tags exceeds limit ({})", MAX_MEMORY_TOOL_TAGS),
                );
            }
            let mut parsed_tags = Vec::new();
            for value in values {
                let Some(tag) = value.as_str() else {
                    return memory_tool_execution_outcome(
                        attestation_namespace,
                        proposal_id,
                        input_json,
                        false,
                        b"{}".to_vec(),
                        "palyra.memory.search tags must be strings".to_owned(),
                    );
                };
                if !tag.trim().is_empty() {
                    parsed_tags.push(tag.trim().to_owned());
                }
            }
            parsed_tags
        }
        Some(_) => {
            return memory_tool_execution_outcome(
                attestation_namespace,
                proposal_id,
                input_json,
                false,
                b"{}".to_vec(),
                "palyra.memory.search tags must be an array of strings".to_owned(),
            );
        }
        None => Vec::new(),
    };
    let sources = match parsed.get("sources") {
        Some(Value::Array(values)) => {
            let mut parsed_sources = Vec::new();
            for value in values {
                let Some(source) = value.as_str() else {
                    return memory_tool_execution_outcome(
                        attestation_namespace,
                        proposal_id,
                        input_json,
                        false,
                        b"{}".to_vec(),
                        "palyra.memory.search sources must be an array of strings".to_owned(),
                    );
                };
                let Some(memory_source) = parse_memory_source_literal(source) else {
                    return memory_tool_execution_outcome(
                        attestation_namespace,
                        proposal_id,
                        input_json,
                        false,
                        b"{}".to_vec(),
                        format!("palyra.memory.search unknown source value: {source}"),
                    );
                };
                parsed_sources.push(memory_source);
            }
            parsed_sources
        }
        Some(_) => {
            return memory_tool_execution_outcome(
                attestation_namespace,
                proposal_id,
                input_json,
                false,
                b"{}".to_vec(),
                "palyra.memory.search sources must be an array of strings".to_owned(),
            );
        }
        None => Vec::new(),
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
    let request = RecallRequest {
        query,
        channel: requested_channel.or_else(|| context.channel.map(str::to_owned)),
        session_id: optional_trimmed_string(parsed.get("session_id"))
            .or_else(|| Some(context.session_id.to_owned())),
        agent_id: optional_trimmed_string(parsed.get("agent_id")),
        memory_top_k,
        workspace_top_k,
        min_score,
        workspace_prefix: optional_trimmed_string(parsed.get("workspace_prefix")),
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

fn parse_memory_tool_object(input_json: &[u8]) -> Result<Map<String, Value>, String> {
    match serde_json::from_slice::<Value>(input_json) {
        Ok(Value::Object(map)) => Ok(map),
        Ok(_) => Err("requires JSON object input".to_owned()),
        Err(error) => Err(format!("invalid JSON input: {error}")),
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

fn serialize_memory_lifecycle_outcome(
    namespace: &'static [u8],
    proposal_id: &str,
    input_json: &[u8],
    outcome: &MemoryLifecycleRetainOutcome,
) -> ToolExecutionOutcome {
    let payload = json!({
        "status": outcome.status.as_str(),
        "reason": outcome.reason.as_str(),
        "scope": outcome.scope.as_str(),
        "trust_label": outcome.trust_label.as_str(),
        "durable_memory_write": outcome.durable_memory_write,
        "matched_memory_id": outcome.matched_memory_id.as_deref(),
        "provenance": outcome.provenance.clone(),
        "item": outcome.item.as_ref().map(memory_item_output_payload),
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
            format!("palyra.memory.retain failed to serialize output: {error}"),
        ),
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
