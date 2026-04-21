use std::{
    collections::{BTreeSet, HashMap},
    sync::Arc,
};

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tonic::Status;

use crate::{
    application::{
        provider_input::{render_memory_augmented_prompt, sanitize_prompt_inline_value},
        session_compaction::{extract_transcript_search_text, truncate_console_text},
    },
    gateway::{current_unix_ms, GatewayRuntimeState, MEMORY_AUTO_INJECT_MIN_SCORE},
    journal::{
        MemorySearchHit, MemorySearchRequest, OrchestratorCheckpointRecord,
        OrchestratorCompactionArtifactRecord, OrchestratorSessionResolveRequest,
        OrchestratorSessionTranscriptRecord, RetrievalBranchDiagnostics, WorkspaceSearchHit,
        WorkspaceSearchRequest,
    },
    retrieval::{
        checkpoint_source_quality as retrieval_checkpoint_source_quality,
        compaction_source_quality as retrieval_compaction_source_quality,
        lexical_overlap_score as retrieval_lexical_overlap_score,
        proxy_vector_score as retrieval_proxy_vector_score,
        recency_score as retrieval_recency_score, score_with_profile,
        transcript_source_quality as retrieval_transcript_source_quality, RetrievalRuntimeConfig,
        RetrievalSourceProfileKind,
    },
    transport::grpc::auth::RequestContext,
};

const DEFAULT_MEMORY_TOP_K: usize = 4;
const DEFAULT_WORKSPACE_TOP_K: usize = 4;
const DEFAULT_TRANSCRIPT_TOP_K: usize = 4;
const DEFAULT_CHECKPOINT_TOP_K: usize = 3;
const DEFAULT_COMPACTION_TOP_K: usize = 3;
const MAX_RECALL_TOP_K: usize = 16;
const DEFAULT_MAX_TOP_CANDIDATES: usize = 8;
const MAX_TOP_CANDIDATES: usize = 12;
const DEFAULT_RECALL_PROMPT_BUDGET_TOKENS: usize = 1_800;
const MAX_RECALL_QUERY_VARIANTS: usize = 4;
#[cfg(test)]
#[allow(dead_code)]
const MIN_TRANSCRIPT_RECENCY_SCORE: f64 = 0.15;
#[cfg(test)]
#[allow(dead_code)]
const MIN_SOURCE_QUALITY_SCORE: f64 = 0.20;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub(crate) enum RecallSourceKind {
    Memory,
    WorkspaceDocument,
    Transcript,
    Checkpoint,
    CompactionArtifact,
}

impl RecallSourceKind {
    pub(crate) const fn as_str(&self) -> &'static str {
        match self {
            Self::Memory => "memory",
            Self::WorkspaceDocument => "workspace_document",
            Self::Transcript => "transcript",
            Self::Checkpoint => "checkpoint",
            Self::CompactionArtifact => "compaction_artifact",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum RecallSourceDecision {
    Selected,
    Skipped,
    Blocked,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct RecallPlanSource {
    pub(crate) source_kind: RecallSourceKind,
    pub(crate) decision: RecallSourceDecision,
    pub(crate) reason: String,
    pub(crate) requested_top_k: usize,
    pub(crate) query: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct RecallBudgetExplain {
    pub(crate) prompt_budget_tokens: usize,
    pub(crate) candidate_limit: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct RecallPlan {
    pub(crate) original_query: String,
    #[serde(default)]
    pub(crate) expanded_queries: Vec<String>,
    pub(crate) session_scoped: bool,
    pub(crate) budget: RecallBudgetExplain,
    #[serde(default)]
    pub(crate) sources: Vec<RecallPlanSource>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub(crate) struct RecallScoreBreakdown {
    pub(crate) lexical_score: f64,
    pub(crate) vector_score: f64,
    pub(crate) recency_score: f64,
    pub(crate) source_quality_score: f64,
    pub(crate) final_score: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct TranscriptRecallRef {
    pub(crate) run_id: String,
    pub(crate) seq: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub(crate) struct TranscriptRecallHit {
    pub(crate) run_id: String,
    pub(crate) seq: i64,
    pub(crate) event_type: String,
    pub(crate) snippet: String,
    pub(crate) created_at_unix_ms: i64,
    pub(crate) reason: String,
    pub(crate) score: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub(crate) struct CheckpointRecallHit {
    pub(crate) checkpoint_id: String,
    pub(crate) name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) note: Option<String>,
    #[serde(default)]
    pub(crate) workspace_paths: Vec<String>,
    pub(crate) created_at_unix_ms: i64,
    pub(crate) reason: String,
    pub(crate) score: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub(crate) struct CompactionRecallHit {
    pub(crate) artifact_id: String,
    pub(crate) mode: String,
    pub(crate) strategy: String,
    pub(crate) trigger_reason: String,
    pub(crate) summary_preview: String,
    pub(crate) created_at_unix_ms: i64,
    pub(crate) reason: String,
    pub(crate) score: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub(crate) struct RecallCandidate {
    pub(crate) candidate_id: String,
    pub(crate) source_kind: RecallSourceKind,
    pub(crate) source_ref: String,
    pub(crate) title: String,
    pub(crate) snippet: String,
    pub(crate) created_at_unix_ms: i64,
    pub(crate) rationale: String,
    pub(crate) score: RecallScoreBreakdown,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct RecallFact {
    pub(crate) statement: String,
    #[serde(default)]
    pub(crate) evidence_ids: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub(crate) struct RecallEvidenceRecord {
    pub(crate) evidence_id: String,
    pub(crate) source_kind: RecallSourceKind,
    pub(crate) source_ref: String,
    pub(crate) title: String,
    pub(crate) snippet: String,
    pub(crate) rationale: String,
    pub(crate) score: RecallScoreBreakdown,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub(crate) struct StructuredRecallOutput {
    #[serde(default)]
    pub(crate) facts: Vec<RecallFact>,
    #[serde(default)]
    pub(crate) evidence: Vec<RecallEvidenceRecord>,
    pub(crate) why_relevant_now: String,
    pub(crate) suggested_next_step: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) confidence: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub(crate) struct ExplicitRecallSelection {
    pub(crate) query: String,
    #[serde(default)]
    pub(crate) channel: Option<String>,
    #[serde(default)]
    pub(crate) session_id: Option<String>,
    #[serde(default)]
    pub(crate) agent_id: Option<String>,
    #[serde(default)]
    pub(crate) min_score: Option<f64>,
    #[serde(default)]
    pub(crate) workspace_prefix: Option<String>,
    #[serde(default)]
    pub(crate) include_workspace_historical: bool,
    #[serde(default)]
    pub(crate) include_workspace_quarantined: bool,
    #[serde(default)]
    pub(crate) memory_item_ids: Vec<String>,
    #[serde(default)]
    pub(crate) workspace_document_ids: Vec<String>,
    #[serde(default)]
    pub(crate) transcript_refs: Vec<TranscriptRecallRef>,
    #[serde(default)]
    pub(crate) checkpoint_ids: Vec<String>,
    #[serde(default)]
    pub(crate) compaction_artifact_ids: Vec<String>,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub(crate) struct RecallPreviewEnvelope {
    pub(crate) query: String,
    #[serde(default)]
    pub(crate) memory_hits: Vec<MemorySearchHit>,
    #[serde(default)]
    pub(crate) workspace_hits: Vec<WorkspaceSearchHit>,
    #[serde(default)]
    pub(crate) transcript_hits: Vec<TranscriptRecallHit>,
    #[serde(default)]
    pub(crate) checkpoint_hits: Vec<CheckpointRecallHit>,
    #[serde(default)]
    pub(crate) compaction_hits: Vec<CompactionRecallHit>,
    #[serde(default)]
    pub(crate) top_candidates: Vec<RecallCandidate>,
    pub(crate) structured_output: StructuredRecallOutput,
    pub(crate) plan: RecallPlan,
    #[serde(default)]
    pub(crate) diagnostics: Vec<RetrievalBranchDiagnostics>,
    pub(crate) parameter_delta: Value,
    pub(crate) prompt_preview: String,
}

#[derive(Debug, Clone)]
pub(crate) struct RecallRequest {
    pub(crate) query: String,
    pub(crate) channel: Option<String>,
    pub(crate) session_id: Option<String>,
    pub(crate) agent_id: Option<String>,
    pub(crate) memory_top_k: usize,
    pub(crate) workspace_top_k: usize,
    pub(crate) min_score: f64,
    pub(crate) workspace_prefix: Option<String>,
    pub(crate) include_workspace_historical: bool,
    pub(crate) include_workspace_quarantined: bool,
    pub(crate) max_candidates: usize,
    pub(crate) prompt_budget_tokens: usize,
}

#[derive(Debug, Clone)]
pub(crate) struct MaterializedRecallContext {
    pub(crate) memory_hits: Vec<MemorySearchHit>,
    pub(crate) workspace_hits: Vec<WorkspaceSearchHit>,
    pub(crate) transcript_hits: Vec<TranscriptRecallHit>,
    pub(crate) checkpoint_hits: Vec<CheckpointRecallHit>,
    pub(crate) compaction_hits: Vec<CompactionRecallHit>,
    pub(crate) top_candidates: Vec<RecallCandidate>,
    pub(crate) structured_output: StructuredRecallOutput,
}

#[derive(Debug, Clone, Deserialize)]
struct RecallParameterDeltaEnvelope {
    #[serde(default)]
    explicit_recall: Option<ExplicitRecallSelection>,
}

#[derive(Debug, Clone)]
struct RecallExecution {
    memory_hits: Vec<MemorySearchHit>,
    workspace_hits: Vec<WorkspaceSearchHit>,
    transcript_hits: Vec<TranscriptRecallHit>,
    checkpoint_hits: Vec<CheckpointRecallHit>,
    compaction_hits: Vec<CompactionRecallHit>,
    top_candidates: Vec<RecallCandidate>,
    structured_output: StructuredRecallOutput,
    parameter_delta: Value,
    prompt_preview: String,
    plan: RecallPlan,
    diagnostics: Vec<RetrievalBranchDiagnostics>,
}

#[derive(Debug, Clone)]
struct CandidateRecord {
    candidate: RecallCandidate,
}

struct RecallCandidateSources<'a> {
    memory_hits: &'a [MemorySearchHit],
    workspace_hits: &'a [WorkspaceSearchHit],
    transcript_hits: &'a [TranscriptRecallHit],
    checkpoint_hits: &'a [CheckpointRecallHit],
    compaction_hits: &'a [CompactionRecallHit],
}

impl RecallRequest {
    pub(crate) fn normalized(self) -> Self {
        Self {
            query: self.query,
            channel: trim_option(self.channel),
            session_id: trim_option(self.session_id),
            agent_id: trim_option(self.agent_id),
            memory_top_k: self.memory_top_k.clamp(0, MAX_RECALL_TOP_K),
            workspace_top_k: self.workspace_top_k.clamp(0, MAX_RECALL_TOP_K),
            min_score: self.min_score.clamp(0.0, 1.0),
            workspace_prefix: trim_option(self.workspace_prefix),
            include_workspace_historical: self.include_workspace_historical,
            include_workspace_quarantined: self.include_workspace_quarantined,
            max_candidates: self.max_candidates.clamp(1, MAX_TOP_CANDIDATES),
            prompt_budget_tokens: self.prompt_budget_tokens.max(512),
        }
    }
}

pub(crate) fn default_recall_request(
    query: impl Into<String>,
    session_id: Option<String>,
    channel: Option<String>,
) -> RecallRequest {
    RecallRequest {
        query: query.into(),
        channel,
        session_id,
        agent_id: None,
        memory_top_k: DEFAULT_MEMORY_TOP_K,
        workspace_top_k: DEFAULT_WORKSPACE_TOP_K,
        min_score: MEMORY_AUTO_INJECT_MIN_SCORE,
        workspace_prefix: None,
        include_workspace_historical: false,
        include_workspace_quarantined: false,
        max_candidates: DEFAULT_MAX_TOP_CANDIDATES,
        prompt_budget_tokens: DEFAULT_RECALL_PROMPT_BUDGET_TOKENS,
    }
}

pub(crate) fn parse_explicit_recall_selection(
    parameter_delta_json: Option<&str>,
) -> Option<ExplicitRecallSelection> {
    let raw = parameter_delta_json?.trim();
    if raw.is_empty() {
        return None;
    }
    serde_json::from_str::<RecallParameterDeltaEnvelope>(raw)
        .ok()
        .and_then(|value| value.explicit_recall)
}

#[allow(clippy::result_large_err)]
pub(crate) async fn preview_recall(
    runtime_state: &Arc<GatewayRuntimeState>,
    context: &RequestContext,
    request: RecallRequest,
) -> Result<RecallPreviewEnvelope, Status> {
    let request = request.normalized();
    let query = request.query.trim();
    if query.is_empty() {
        return Err(Status::invalid_argument("query cannot be empty"));
    }
    let execution = execute_recall(runtime_state, context, &request).await?;
    Ok(RecallPreviewEnvelope {
        query: query.to_owned(),
        memory_hits: execution.memory_hits,
        workspace_hits: execution.workspace_hits,
        transcript_hits: execution.transcript_hits,
        checkpoint_hits: execution.checkpoint_hits,
        compaction_hits: execution.compaction_hits,
        top_candidates: execution.top_candidates,
        structured_output: execution.structured_output,
        plan: execution.plan,
        diagnostics: execution.diagnostics,
        parameter_delta: execution.parameter_delta,
        prompt_preview: execution.prompt_preview,
    })
}

#[allow(clippy::result_large_err)]
pub(crate) async fn materialize_explicit_recall_context(
    runtime_state: &Arc<GatewayRuntimeState>,
    context: &RequestContext,
    request: RecallRequest,
    selection: &ExplicitRecallSelection,
) -> Result<MaterializedRecallContext, Status> {
    let retrieval_config = runtime_state.retrieval_config_snapshot();
    let request = request.normalized();
    let query = selection.query.trim();
    if query.is_empty() {
        return Err(Status::invalid_argument("explicit recall query cannot be empty"));
    }

    let selection_context =
        build_selection_context(runtime_state, context, &request, selection).await?;
    let query_variants = build_query_variants(query);
    let candidate_records = build_candidate_records(
        query_variants.as_slice(),
        &RecallCandidateSources {
            memory_hits: selection_context.memory_hits.as_slice(),
            workspace_hits: selection_context.workspace_hits.as_slice(),
            transcript_hits: selection_context.transcript_hits.as_slice(),
            checkpoint_hits: selection_context.checkpoint_hits.as_slice(),
            compaction_hits: selection_context.compaction_hits.as_slice(),
        },
        current_unix_ms(),
        &retrieval_config,
    );
    let top_candidates = finalize_top_candidates(
        candidate_records,
        request.max_candidates,
        request.prompt_budget_tokens,
    );
    let structured_output = build_structured_output(top_candidates.as_slice(), query);
    Ok(MaterializedRecallContext {
        memory_hits: selection_context.memory_hits,
        workspace_hits: selection_context.workspace_hits,
        transcript_hits: selection_context.transcript_hits,
        checkpoint_hits: selection_context.checkpoint_hits,
        compaction_hits: selection_context.compaction_hits,
        top_candidates,
        structured_output,
    })
}

pub(crate) fn explicit_recall_tape_payload(
    selection: &ExplicitRecallSelection,
    context: &MaterializedRecallContext,
) -> Value {
    json!({
        "query": selection.query,
        "memory_hits": context.memory_hits,
        "workspace_hits": context.workspace_hits,
        "transcript_hits": context.transcript_hits,
        "checkpoint_hits": context.checkpoint_hits,
        "compaction_hits": context.compaction_hits,
        "top_candidates": context.top_candidates,
        "structured_output": context.structured_output,
    })
}

pub(crate) fn recall_preview_console_payload(preview: &RecallPreviewEnvelope) -> Value {
    json!({
        "query": preview.query,
        "plan": preview.plan,
        "memory_hits": preview.memory_hits.len(),
        "workspace_hits": preview.workspace_hits.len(),
        "transcript_hits": preview.transcript_hits.len(),
        "checkpoint_hits": preview.checkpoint_hits.len(),
        "compaction_hits": preview.compaction_hits.len(),
        "top_candidates": preview.top_candidates.len(),
        "structured_output": preview.structured_output,
        "diagnostics": preview.diagnostics,
    })
}

pub(crate) fn render_explicit_recall_prompt(
    memory_hits: &[MemorySearchHit],
    workspace_hits: &[WorkspaceSearchHit],
    transcript_hits: &[TranscriptRecallHit],
    checkpoint_hits: &[CheckpointRecallHit],
    compaction_hits: &[CompactionRecallHit],
    input_text: &str,
) -> String {
    let mut sections = Vec::new();
    if !memory_hits.is_empty() {
        sections.push(render_memory_recall_section(memory_hits));
    }
    if !workspace_hits.is_empty() {
        sections.push(render_workspace_recall_section(workspace_hits));
    }
    if !transcript_hits.is_empty() {
        sections.push(render_transcript_recall_section(transcript_hits));
    }
    if !checkpoint_hits.is_empty() {
        sections.push(render_checkpoint_recall_section(checkpoint_hits));
    }
    if !compaction_hits.is_empty() {
        sections.push(render_compaction_recall_section(compaction_hits));
    }
    let mut prompt = sections.join("\n\n");
    if !prompt.is_empty() {
        prompt.push('\n');
        prompt.push('\n');
    }
    prompt.push_str(input_text);
    prompt
}

fn render_memory_recall_section(memory_hits: &[MemorySearchHit]) -> String {
    let rendered = render_memory_augmented_prompt(memory_hits, "");
    rendered.trim().to_owned()
}

fn render_workspace_recall_section(workspace_hits: &[WorkspaceSearchHit]) -> String {
    let mut block = String::from("<workspace_context>\n");
    for (index, hit) in workspace_hits.iter().enumerate() {
        let path = sanitize_prompt_inline_value(hit.document.path.as_str());
        let snippet = sanitize_prompt_inline_value(hit.snippet.as_str());
        block.push_str(
            format!(
                "{}. document_id={} path={} version={} reason={} risk_state={} snippet={}\n",
                index + 1,
                hit.document.document_id,
                path,
                hit.version,
                hit.reason,
                hit.document.risk_state,
                truncate_console_text(snippet.as_str(), 256),
            )
            .as_str(),
        );
    }
    block.push_str("</workspace_context>");
    block
}

fn render_transcript_recall_section(transcript_hits: &[TranscriptRecallHit]) -> String {
    let mut block = String::from("<transcript_context>\n");
    for (index, hit) in transcript_hits.iter().enumerate() {
        block.push_str(
            format!(
                "{}. run_id={} seq={} event_type={} created_at_unix_ms={} reason={} snippet={}\n",
                index + 1,
                hit.run_id,
                hit.seq,
                hit.event_type,
                hit.created_at_unix_ms,
                sanitize_prompt_inline_value(hit.reason.as_str()),
                sanitize_prompt_inline_value(hit.snippet.as_str()),
            )
            .as_str(),
        );
    }
    block.push_str("</transcript_context>");
    block
}

fn render_checkpoint_recall_section(checkpoint_hits: &[CheckpointRecallHit]) -> String {
    let mut block = String::from("<checkpoint_context>\n");
    for (index, hit) in checkpoint_hits.iter().enumerate() {
        let note = hit
            .note
            .as_deref()
            .map(sanitize_prompt_inline_value)
            .unwrap_or_else(|| "none".to_owned());
        let workspace_paths = if hit.workspace_paths.is_empty() {
            "none".to_owned()
        } else {
            hit.workspace_paths.join(",")
        };
        block.push_str(
            format!(
                "{}. checkpoint_id={} name={} created_at_unix_ms={} reason={} workspace_paths={} note={}\n",
                index + 1,
                hit.checkpoint_id,
                sanitize_prompt_inline_value(hit.name.as_str()),
                hit.created_at_unix_ms,
                sanitize_prompt_inline_value(hit.reason.as_str()),
                sanitize_prompt_inline_value(workspace_paths.as_str()),
                note,
            )
            .as_str(),
        );
    }
    block.push_str("</checkpoint_context>");
    block
}

fn render_compaction_recall_section(compaction_hits: &[CompactionRecallHit]) -> String {
    let mut block = String::from("<compaction_context>\n");
    for (index, hit) in compaction_hits.iter().enumerate() {
        block.push_str(
            format!(
                "{}. artifact_id={} mode={} strategy={} created_at_unix_ms={} reason={} summary={}\n",
                index + 1,
                hit.artifact_id,
                hit.mode,
                hit.strategy,
                hit.created_at_unix_ms,
                sanitize_prompt_inline_value(hit.reason.as_str()),
                sanitize_prompt_inline_value(hit.summary_preview.as_str()),
            )
            .as_str(),
        );
    }
    block.push_str("</compaction_context>");
    block
}

#[allow(clippy::result_large_err)]
async fn execute_recall(
    runtime_state: &Arc<GatewayRuntimeState>,
    context: &RequestContext,
    request: &RecallRequest,
) -> Result<RecallExecution, Status> {
    let retrieval_config = runtime_state.retrieval_config_snapshot();
    let query = request.query.trim();
    let query_variants = build_query_variants(query);
    let scoped_session_id =
        validate_recall_session_scope(runtime_state, context, request.session_id.clone()).await?;
    let plan =
        build_recall_plan(query, query_variants.as_slice(), scoped_session_id.is_some(), request);

    let (memory_hits, memory_diagnostics) =
        if plan_source_selected(plan.sources.as_slice(), RecallSourceKind::Memory) {
            let outcome = runtime_state
                .search_memory_with_diagnostics(MemorySearchRequest {
                    principal: context.principal.clone(),
                    channel: request.channel.clone().or_else(|| context.channel.clone()),
                    session_id: scoped_session_id.clone(),
                    query: query.to_owned(),
                    top_k: request.memory_top_k.max(1),
                    min_score: request.min_score,
                    tags: Vec::new(),
                    sources: Vec::new(),
                })
                .await?;
            (outcome.hits, vec![outcome.diagnostics])
        } else {
            (Vec::new(), Vec::new())
        };
    let (workspace_hits, workspace_diagnostics) =
        if plan_source_selected(plan.sources.as_slice(), RecallSourceKind::WorkspaceDocument) {
            let outcome = runtime_state
                .search_workspace_documents_with_diagnostics(WorkspaceSearchRequest {
                    principal: context.principal.clone(),
                    channel: request.channel.clone().or_else(|| context.channel.clone()),
                    agent_id: request.agent_id.clone(),
                    query: query.to_owned(),
                    prefix: request.workspace_prefix.clone(),
                    top_k: request.workspace_top_k.max(1),
                    min_score: request.min_score,
                    include_historical: request.include_workspace_historical,
                    include_quarantined: request.include_workspace_quarantined,
                })
                .await?;
            (outcome.hits, vec![outcome.diagnostics])
        } else {
            (Vec::new(), Vec::new())
        };
    let mut diagnostics = memory_diagnostics;
    diagnostics.extend(workspace_diagnostics);
    let transcript_records =
        if plan_source_selected(plan.sources.as_slice(), RecallSourceKind::Transcript) {
            runtime_state
                .list_orchestrator_session_transcript(
                    scoped_session_id.clone().expect("session scope should exist for transcript"),
                )
                .await?
        } else {
            Vec::new()
        };
    let checkpoint_records =
        if plan_source_selected(plan.sources.as_slice(), RecallSourceKind::Checkpoint) {
            runtime_state
                .list_orchestrator_checkpoints(
                    scoped_session_id.clone().expect("session scope should exist for checkpoints"),
                )
                .await?
        } else {
            Vec::new()
        };
    let compaction_records =
        if plan_source_selected(plan.sources.as_slice(), RecallSourceKind::CompactionArtifact) {
            runtime_state
                .list_orchestrator_compaction_artifacts(
                    scoped_session_id.clone().expect("session scope should exist for compaction"),
                )
                .await?
        } else {
            Vec::new()
        };

    let transcript_hits = build_transcript_hits(
        transcript_records.as_slice(),
        query_variants.as_slice(),
        request.min_score,
        DEFAULT_TRANSCRIPT_TOP_K,
        &retrieval_config,
    );
    let checkpoint_hits = build_checkpoint_hits(
        checkpoint_records.as_slice(),
        query_variants.as_slice(),
        request.min_score,
        DEFAULT_CHECKPOINT_TOP_K,
        &retrieval_config,
    );
    let compaction_hits = build_compaction_hits(
        compaction_records.as_slice(),
        query_variants.as_slice(),
        request.min_score,
        DEFAULT_COMPACTION_TOP_K,
        &retrieval_config,
    );

    let candidate_records = build_candidate_records(
        query_variants.as_slice(),
        &RecallCandidateSources {
            memory_hits: memory_hits.as_slice(),
            workspace_hits: workspace_hits.as_slice(),
            transcript_hits: transcript_hits.as_slice(),
            checkpoint_hits: checkpoint_hits.as_slice(),
            compaction_hits: compaction_hits.as_slice(),
        },
        current_unix_ms(),
        &retrieval_config,
    );
    let top_candidates = finalize_top_candidates(
        candidate_records,
        request.max_candidates,
        request.prompt_budget_tokens,
    );
    let selection = selection_from_candidates(
        query,
        request,
        top_candidates.as_slice(),
        context.channel.clone(),
        scoped_session_id,
    );
    let selected_memory_hits =
        select_memory_hits(memory_hits, selection.memory_item_ids.as_slice());
    let selected_workspace_hits =
        select_workspace_hits(workspace_hits, selection.workspace_document_ids.as_slice());
    let selected_transcript_hits =
        select_transcript_hits(transcript_hits, selection.transcript_refs.as_slice());
    let selected_checkpoint_hits =
        select_checkpoint_hits(checkpoint_hits, selection.checkpoint_ids.as_slice());
    let selected_compaction_hits =
        select_compaction_hits(compaction_hits, selection.compaction_artifact_ids.as_slice());
    let structured_output = build_structured_output(top_candidates.as_slice(), query);
    let prompt_preview = render_explicit_recall_prompt(
        selected_memory_hits.as_slice(),
        selected_workspace_hits.as_slice(),
        selected_transcript_hits.as_slice(),
        selected_checkpoint_hits.as_slice(),
        selected_compaction_hits.as_slice(),
        query,
    );
    let parameter_delta = json!({
        "explicit_recall": selection,
    });

    Ok(RecallExecution {
        memory_hits: selected_memory_hits,
        workspace_hits: selected_workspace_hits,
        transcript_hits: selected_transcript_hits,
        checkpoint_hits: selected_checkpoint_hits,
        compaction_hits: selected_compaction_hits,
        top_candidates,
        structured_output,
        parameter_delta,
        prompt_preview,
        plan,
        diagnostics,
    })
}

async fn build_selection_context(
    runtime_state: &Arc<GatewayRuntimeState>,
    context: &RequestContext,
    request: &RecallRequest,
    selection: &ExplicitRecallSelection,
) -> Result<MaterializedRecallContext, Status> {
    let retrieval_config = runtime_state.retrieval_config_snapshot();
    let session_id = validate_recall_session_scope(
        runtime_state,
        context,
        selection.session_id.clone().or_else(|| request.session_id.clone()),
    )
    .await?;
    let query_variants = build_query_variants(selection.query.as_str());
    let recall_channel = selection
        .channel
        .clone()
        .or_else(|| request.channel.clone())
        .or_else(|| context.channel.clone());
    let mut memory_hits = Vec::new();
    if !selection.memory_item_ids.is_empty() {
        let candidate_hits = runtime_state
            .search_memory(MemorySearchRequest {
                principal: context.principal.clone(),
                channel: recall_channel.clone(),
                session_id: session_id.clone(),
                query: selection.query.clone(),
                top_k: selection.memory_item_ids.len().saturating_mul(4).clamp(8, 32),
                min_score: selection.min_score.unwrap_or(request.min_score),
                tags: Vec::new(),
                sources: Vec::new(),
            })
            .await?;
        memory_hits = select_memory_hits(candidate_hits, selection.memory_item_ids.as_slice());
    }

    let mut workspace_hits = Vec::new();
    if !selection.workspace_document_ids.is_empty() {
        let candidate_hits = runtime_state
            .search_workspace_documents(WorkspaceSearchRequest {
                principal: context.principal.clone(),
                channel: recall_channel.clone(),
                agent_id: selection.agent_id.clone().or_else(|| request.agent_id.clone()),
                query: selection.query.clone(),
                prefix: selection
                    .workspace_prefix
                    .clone()
                    .or_else(|| request.workspace_prefix.clone()),
                top_k: selection.workspace_document_ids.len().saturating_mul(4).clamp(8, 32),
                min_score: selection.min_score.unwrap_or(request.min_score),
                include_historical: selection.include_workspace_historical,
                include_quarantined: selection.include_workspace_quarantined,
            })
            .await?;
        workspace_hits =
            select_workspace_hits(candidate_hits, selection.workspace_document_ids.as_slice());
        let recalled_at_unix_ms = current_unix_ms();
        for hit in &workspace_hits {
            runtime_state
                .record_workspace_document_recall(
                    hit.document.document_id.clone(),
                    recalled_at_unix_ms,
                )
                .await?;
        }
    }

    let transcript_hits = if !selection.transcript_refs.is_empty() {
        let transcript_records = runtime_state
            .list_orchestrator_session_transcript(
                session_id.clone().expect("session scope should exist for transcript recall"),
            )
            .await?;
        let built_hits = build_transcript_hits(
            transcript_records.as_slice(),
            query_variants.as_slice(),
            selection.min_score.unwrap_or(request.min_score),
            selection.transcript_refs.len().max(DEFAULT_TRANSCRIPT_TOP_K),
            &retrieval_config,
        );
        select_transcript_hits(built_hits, selection.transcript_refs.as_slice())
    } else {
        Vec::new()
    };

    let checkpoint_hits = if !selection.checkpoint_ids.is_empty() {
        let checkpoint_records = runtime_state
            .list_orchestrator_checkpoints(
                session_id.clone().expect("session scope should exist for checkpoint recall"),
            )
            .await?;
        let built_hits = build_checkpoint_hits(
            checkpoint_records.as_slice(),
            query_variants.as_slice(),
            selection.min_score.unwrap_or(request.min_score),
            selection.checkpoint_ids.len().max(DEFAULT_CHECKPOINT_TOP_K),
            &retrieval_config,
        );
        select_checkpoint_hits(built_hits, selection.checkpoint_ids.as_slice())
    } else {
        Vec::new()
    };

    let compaction_hits = if !selection.compaction_artifact_ids.is_empty() {
        let compaction_records = runtime_state
            .list_orchestrator_compaction_artifacts(
                session_id.expect("session scope should exist for compaction recall"),
            )
            .await?;
        let built_hits = build_compaction_hits(
            compaction_records.as_slice(),
            query_variants.as_slice(),
            selection.min_score.unwrap_or(request.min_score),
            selection.compaction_artifact_ids.len().max(DEFAULT_COMPACTION_TOP_K),
            &retrieval_config,
        );
        select_compaction_hits(built_hits, selection.compaction_artifact_ids.as_slice())
    } else {
        Vec::new()
    };

    Ok(MaterializedRecallContext {
        memory_hits,
        workspace_hits,
        transcript_hits,
        checkpoint_hits,
        compaction_hits,
        top_candidates: Vec::new(),
        structured_output: StructuredRecallOutput {
            facts: Vec::new(),
            evidence: Vec::new(),
            why_relevant_now: String::new(),
            suggested_next_step: String::new(),
            confidence: None,
        },
    })
}

async fn validate_recall_session_scope(
    runtime_state: &Arc<GatewayRuntimeState>,
    context: &RequestContext,
    requested_session_id: Option<String>,
) -> Result<Option<String>, Status> {
    let Some(session_id) = trim_option(requested_session_id) else {
        return Ok(None);
    };
    runtime_state
        .resolve_orchestrator_session(OrchestratorSessionResolveRequest {
            session_id: Some(session_id.clone()),
            session_key: None,
            session_label: None,
            principal: context.principal.clone(),
            device_id: context.device_id.clone(),
            channel: context.channel.clone(),
            require_existing: true,
            reset_session: false,
        })
        .await?;
    Ok(Some(session_id))
}

fn build_recall_plan(
    query: &str,
    query_variants: &[String],
    session_scoped: bool,
    request: &RecallRequest,
) -> RecallPlan {
    let lowered = query.to_ascii_lowercase();
    let mentions_history = contains_any(
        lowered.as_str(),
        &["earlier", "before", "history", "transcript", "conversation", "said", "previous"],
    );
    let mentions_checkpoint =
        contains_any(lowered.as_str(), &["checkpoint", "rollback", "restore", "undo", "revert"]);
    let mentions_compaction = contains_any(
        lowered.as_str(),
        &["compaction", "summary", "artifact", "condensed", "drift"],
    );
    let mentions_workspace = contains_any(
        lowered.as_str(),
        &["workspace", "document", "file", "path", "notes", "spec", "project", ".md", "/"],
    );
    let mentions_memory = contains_any(
        lowered.as_str(),
        &["remember", "fact", "preference", "decision", "rule", "policy", "why", "context"],
    );

    let mut sources = Vec::new();
    sources.push(RecallPlanSource {
        source_kind: RecallSourceKind::Memory,
        decision: if request.memory_top_k == 0 {
            RecallSourceDecision::Skipped
        } else {
            RecallSourceDecision::Selected
        },
        reason: if mentions_memory {
            "query asks for remembered facts, preferences, or decisions".to_owned()
        } else {
            "memory remains enabled as the durable recall baseline".to_owned()
        },
        requested_top_k: request.memory_top_k.max(DEFAULT_MEMORY_TOP_K),
        query: query.to_owned(),
    });
    sources.push(RecallPlanSource {
        source_kind: RecallSourceKind::WorkspaceDocument,
        decision: if request.workspace_top_k == 0 {
            RecallSourceDecision::Skipped
        } else {
            RecallSourceDecision::Selected
        },
        reason: if mentions_workspace {
            "query looks document- or file-oriented".to_owned()
        } else {
            "workspace search stays enabled for durable project notes".to_owned()
        },
        requested_top_k: request.workspace_top_k.max(DEFAULT_WORKSPACE_TOP_K),
        query: query.to_owned(),
    });

    for (source_kind, selected, reason, top_k) in [
        (
            RecallSourceKind::Transcript,
            session_scoped && (mentions_history || mentions_memory),
            if !session_scoped {
                "session-scoped transcript recall requires a scoped session".to_owned()
            } else if mentions_history {
                "query asks about prior conversation or earlier decisions".to_owned()
            } else {
                "planner adds transcript because remembered context may still live only in the tape"
                    .to_owned()
            },
            DEFAULT_TRANSCRIPT_TOP_K,
        ),
        (
            RecallSourceKind::Checkpoint,
            session_scoped && (mentions_checkpoint || mentions_history),
            if !session_scoped {
                "checkpoints are unavailable without a scoped session".to_owned()
            } else if mentions_checkpoint {
                "query explicitly mentions rollback, restore, or checkpoint state".to_owned()
            } else {
                "planner adds checkpoints because session history may need restorable anchors"
                    .to_owned()
            },
            DEFAULT_CHECKPOINT_TOP_K,
        ),
        (
            RecallSourceKind::CompactionArtifact,
            session_scoped && (mentions_compaction || mentions_history || mentions_memory),
            if !session_scoped {
                "compaction artifacts are unavailable without a scoped session".to_owned()
            } else if mentions_compaction {
                "query explicitly mentions summaries, artifacts, or compaction".to_owned()
            } else {
                "planner adds compaction summaries as a bounded history source".to_owned()
            },
            DEFAULT_COMPACTION_TOP_K,
        ),
    ] {
        sources.push(RecallPlanSource {
            source_kind,
            decision: if !session_scoped {
                RecallSourceDecision::Blocked
            } else if selected {
                RecallSourceDecision::Selected
            } else {
                RecallSourceDecision::Skipped
            },
            reason,
            requested_top_k: top_k,
            query: query_variants.first().cloned().unwrap_or_else(|| query.to_owned()),
        });
    }

    RecallPlan {
        original_query: query.to_owned(),
        expanded_queries: query_variants.iter().skip(1).cloned().collect(),
        session_scoped,
        budget: RecallBudgetExplain {
            prompt_budget_tokens: request.prompt_budget_tokens,
            candidate_limit: request.max_candidates,
        },
        sources,
    }
}

fn build_query_variants(query: &str) -> Vec<String> {
    let trimmed = query.trim();
    if trimmed.is_empty() {
        return Vec::new();
    }

    let mut variants = Vec::new();
    push_unique_variant(&mut variants, trimmed.to_owned());

    let normalized = trimmed
        .chars()
        .map(|character| {
            if character.is_alphanumeric() || matches!(character, '/' | '.' | '_' | '-') {
                character.to_ascii_lowercase()
            } else {
                ' '
            }
        })
        .collect::<String>()
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ");
    if normalized != trimmed {
        push_unique_variant(&mut variants, normalized);
    }

    let keyword_variant = trimmed
        .split_whitespace()
        .filter(|token| {
            let lowered = token
                .trim_matches(|character: char| !character.is_alphanumeric())
                .to_ascii_lowercase();
            !matches!(
                lowered.as_str(),
                "the"
                    | "a"
                    | "an"
                    | "and"
                    | "or"
                    | "to"
                    | "for"
                    | "of"
                    | "in"
                    | "on"
                    | "with"
                    | "about"
                    | "from"
                    | "how"
                    | "what"
                    | "why"
                    | "where"
                    | "when"
            )
        })
        .collect::<Vec<_>>()
        .join(" ");
    if keyword_variant != trimmed {
        push_unique_variant(&mut variants, keyword_variant);
    }

    if contains_any(trimmed.to_ascii_lowercase().as_str(), &["checkpoint", "rollback", "restore"]) {
        push_unique_variant(&mut variants, format!("{trimmed} restore checkpoint rollback"));
    }
    variants.truncate(MAX_RECALL_QUERY_VARIANTS);
    variants
}

fn push_unique_variant(variants: &mut Vec<String>, candidate: String) {
    let trimmed = candidate.trim();
    if trimmed.is_empty() {
        return;
    }
    if variants.iter().any(|existing| existing.eq_ignore_ascii_case(trimmed)) {
        return;
    }
    variants.push(trimmed.to_owned());
}

fn build_transcript_hits(
    transcript_records: &[OrchestratorSessionTranscriptRecord],
    query_variants: &[String],
    min_score: f64,
    top_k: usize,
    retrieval_config: &RetrievalRuntimeConfig,
) -> Vec<TranscriptRecallHit> {
    let now = current_unix_ms();
    let profile = retrieval_config.scoring.profile_for(RetrievalSourceProfileKind::Transcript);
    let mut hits = transcript_records
        .iter()
        .filter_map(|record| {
            let snippet = extract_transcript_search_text(record)?;
            let lexical_score = retrieval_lexical_overlap_score(
                snippet.as_str(),
                query_variants,
                retrieval_config.scoring.phrase_match_bonus_bps,
            );
            if lexical_score < min_score {
                return None;
            }
            let vector_score = retrieval_proxy_vector_score(snippet.as_str(), query_variants);
            let recency_score =
                retrieval_recency_score(record.created_at_unix_ms, now, profile.min_recency_bps);
            let source_quality_score =
                retrieval_transcript_source_quality(record.event_type.as_str(), profile);
            let score = score_with_profile(
                lexical_score,
                vector_score,
                recency_score,
                source_quality_score,
                false,
                profile,
            )
            .final_score;
            Some(TranscriptRecallHit {
                run_id: record.run_id.clone(),
                seq: record.seq,
                event_type: record.event_type.clone(),
                snippet: truncate_console_text(snippet.as_str(), 256),
                created_at_unix_ms: record.created_at_unix_ms,
                reason: transcript_reason(record, lexical_score, recency_score),
                score,
            })
        })
        .collect::<Vec<_>>();
    hits.sort_by(|left, right| compare_scores(right.score, left.score));
    hits.truncate(top_k.max(1));
    hits
}

fn build_checkpoint_hits(
    checkpoints: &[OrchestratorCheckpointRecord],
    query_variants: &[String],
    min_score: f64,
    top_k: usize,
    retrieval_config: &RetrievalRuntimeConfig,
) -> Vec<CheckpointRecallHit> {
    let now = current_unix_ms();
    let profile = retrieval_config.scoring.profile_for(RetrievalSourceProfileKind::Checkpoint);
    let mut hits = checkpoints
        .iter()
        .filter_map(|checkpoint| {
            let workspace_paths =
                serde_json::from_str::<Vec<String>>(checkpoint.workspace_paths_json.as_str())
                    .unwrap_or_default();
            let searchable = [
                checkpoint.name.as_str(),
                checkpoint.note.as_deref().unwrap_or_default(),
                workspace_paths.join(" ").as_str(),
            ]
            .join(" ");
            let lexical_score = retrieval_lexical_overlap_score(
                searchable.as_str(),
                query_variants,
                retrieval_config.scoring.phrase_match_bonus_bps,
            );
            if lexical_score < min_score {
                return None;
            }
            let vector_score = retrieval_proxy_vector_score(searchable.as_str(), query_variants);
            let recency_score = retrieval_recency_score(
                checkpoint.created_at_unix_ms,
                now,
                profile.min_recency_bps,
            );
            let source_quality_score = retrieval_checkpoint_source_quality(checkpoint, profile);
            let score = score_with_profile(
                lexical_score,
                vector_score,
                recency_score,
                source_quality_score,
                false,
                profile,
            )
            .final_score;
            Some(CheckpointRecallHit {
                checkpoint_id: checkpoint.checkpoint_id.clone(),
                name: checkpoint.name.clone(),
                note: checkpoint.note.clone(),
                workspace_paths,
                created_at_unix_ms: checkpoint.created_at_unix_ms,
                reason: checkpoint_reason(checkpoint, lexical_score),
                score,
            })
        })
        .collect::<Vec<_>>();
    hits.sort_by(|left, right| compare_scores(right.score, left.score));
    hits.truncate(top_k.max(1));
    hits
}

fn build_compaction_hits(
    artifacts: &[OrchestratorCompactionArtifactRecord],
    query_variants: &[String],
    min_score: f64,
    top_k: usize,
    retrieval_config: &RetrievalRuntimeConfig,
) -> Vec<CompactionRecallHit> {
    let now = current_unix_ms();
    let profile =
        retrieval_config.scoring.profile_for(RetrievalSourceProfileKind::CompactionArtifact);
    let mut hits = artifacts
        .iter()
        .filter_map(|artifact| {
            let searchable = [
                artifact.trigger_reason.as_str(),
                artifact.summary_text.as_str(),
                artifact.summary_preview.as_str(),
            ]
            .join(" ");
            let lexical_score = retrieval_lexical_overlap_score(
                searchable.as_str(),
                query_variants,
                retrieval_config.scoring.phrase_match_bonus_bps,
            );
            if lexical_score < min_score {
                return None;
            }
            let vector_score = retrieval_proxy_vector_score(searchable.as_str(), query_variants);
            let recency_score =
                retrieval_recency_score(artifact.created_at_unix_ms, now, profile.min_recency_bps);
            let source_quality_score = retrieval_compaction_source_quality(artifact, profile);
            let score = score_with_profile(
                lexical_score,
                vector_score,
                recency_score,
                source_quality_score,
                false,
                profile,
            )
            .final_score;
            Some(CompactionRecallHit {
                artifact_id: artifact.artifact_id.clone(),
                mode: artifact.mode.clone(),
                strategy: artifact.strategy.clone(),
                trigger_reason: artifact.trigger_reason.clone(),
                summary_preview: truncate_console_text(artifact.summary_text.as_str(), 256),
                created_at_unix_ms: artifact.created_at_unix_ms,
                reason: compaction_reason(artifact, lexical_score),
                score,
            })
        })
        .collect::<Vec<_>>();
    hits.sort_by(|left, right| compare_scores(right.score, left.score));
    hits.truncate(top_k.max(1));
    hits
}

fn build_candidate_records(
    query_variants: &[String],
    sources: &RecallCandidateSources<'_>,
    now_unix_ms: i64,
    retrieval_config: &RetrievalRuntimeConfig,
) -> Vec<CandidateRecord> {
    let mut records = Vec::new();
    for hit in sources.memory_hits {
        let score = RecallScoreBreakdown {
            lexical_score: hit.breakdown.lexical_score,
            vector_score: hit.breakdown.vector_score,
            recency_score: hit.breakdown.recency_score,
            source_quality_score: hit.breakdown.source_quality_score,
            final_score: hit.breakdown.final_score,
        };
        let candidate = RecallCandidate {
            candidate_id: format!("memory:{}", hit.item.memory_id),
            source_kind: RecallSourceKind::Memory,
            source_ref: hit.item.memory_id.clone(),
            title: format!("Memory {}", hit.item.memory_id),
            snippet: truncate_console_text(hit.snippet.as_str(), 256),
            created_at_unix_ms: hit.item.created_at_unix_ms,
            rationale: format!(
                "matched durable memory via lexical {:.2}, vector {:.2}, recency {:.2}",
                hit.breakdown.lexical_score,
                hit.breakdown.vector_score,
                hit.breakdown.recency_score
            ),
            score,
        };
        records.push(CandidateRecord { candidate });
    }
    for hit in sources.workspace_hits {
        let score = RecallScoreBreakdown {
            lexical_score: hit.breakdown.lexical_score,
            vector_score: hit.breakdown.vector_score,
            recency_score: hit.breakdown.recency_score,
            source_quality_score: hit.breakdown.source_quality_score,
            final_score: hit.breakdown.final_score,
        };
        let candidate = RecallCandidate {
            candidate_id: format!("workspace:{}", hit.document.document_id),
            source_kind: RecallSourceKind::WorkspaceDocument,
            source_ref: hit.document.document_id.clone(),
            title: hit.document.title.clone(),
            snippet: truncate_console_text(hit.snippet.as_str(), 256),
            created_at_unix_ms: hit.document.updated_at_unix_ms,
            rationale: format!(
                "matched workspace snippet with lexical {:.2} and document quality {:.2}",
                hit.breakdown.lexical_score, hit.breakdown.source_quality_score
            ),
            score,
        };
        records.push(CandidateRecord { candidate });
    }
    for hit in sources.transcript_hits {
        let profile = retrieval_config.scoring.profile_for(RetrievalSourceProfileKind::Transcript);
        let lexical_score = retrieval_lexical_overlap_score(
            hit.snippet.as_str(),
            query_variants,
            retrieval_config.scoring.phrase_match_bonus_bps,
        );
        let vector_score = retrieval_proxy_vector_score(hit.snippet.as_str(), query_variants);
        let recency_score =
            retrieval_recency_score(hit.created_at_unix_ms, now_unix_ms, profile.min_recency_bps);
        let source_quality_score =
            retrieval_transcript_source_quality(hit.event_type.as_str(), profile);
        let score_breakdown = score_with_profile(
            lexical_score,
            vector_score,
            recency_score,
            source_quality_score,
            false,
            profile,
        );
        let score = RecallScoreBreakdown {
            lexical_score: score_breakdown.lexical_score,
            vector_score: score_breakdown.vector_score,
            recency_score: score_breakdown.recency_score,
            source_quality_score: score_breakdown.source_quality_score,
            final_score: score_breakdown.final_score,
        };
        let candidate = RecallCandidate {
            candidate_id: format!("transcript:{}:{}", hit.run_id, hit.seq),
            source_kind: RecallSourceKind::Transcript,
            source_ref: format!("{}:{}", hit.run_id, hit.seq),
            title: format!("Transcript {}#{}", hit.run_id, hit.seq),
            snippet: hit.snippet.clone(),
            created_at_unix_ms: hit.created_at_unix_ms,
            rationale: format!(
                "matched transcript event with lexical {:.2} and recency {:.2}",
                lexical_score, recency_score
            ),
            score,
        };
        records.push(CandidateRecord { candidate });
    }
    for hit in sources.checkpoint_hits {
        let searchable = checkpoint_search_text(hit);
        let profile = retrieval_config.scoring.profile_for(RetrievalSourceProfileKind::Checkpoint);
        let lexical_score = retrieval_lexical_overlap_score(
            searchable.as_str(),
            query_variants,
            retrieval_config.scoring.phrase_match_bonus_bps,
        );
        let vector_score = retrieval_proxy_vector_score(searchable.as_str(), query_variants);
        let recency_score =
            retrieval_recency_score(hit.created_at_unix_ms, now_unix_ms, profile.min_recency_bps);
        let source_quality_score =
            0.88_f64.clamp(f64::from(profile.min_source_quality_bps) / 10_000.0, 1.0);
        let score_breakdown = score_with_profile(
            lexical_score,
            vector_score,
            recency_score,
            source_quality_score,
            false,
            profile,
        );
        let score = RecallScoreBreakdown {
            lexical_score: score_breakdown.lexical_score,
            vector_score: score_breakdown.vector_score,
            recency_score: score_breakdown.recency_score,
            source_quality_score: score_breakdown.source_quality_score,
            final_score: score_breakdown.final_score,
        };
        let candidate = RecallCandidate {
            candidate_id: format!("checkpoint:{}", hit.checkpoint_id),
            source_kind: RecallSourceKind::Checkpoint,
            source_ref: hit.checkpoint_id.clone(),
            title: hit.name.clone(),
            snippet: truncate_console_text(searchable.as_str(), 256),
            created_at_unix_ms: hit.created_at_unix_ms,
            rationale: format!(
                "matched checkpoint metadata with lexical {:.2} and source quality {:.2}",
                lexical_score, source_quality_score
            ),
            score,
        };
        records.push(CandidateRecord { candidate });
    }
    for hit in sources.compaction_hits {
        let searchable = compaction_search_text(hit);
        let profile =
            retrieval_config.scoring.profile_for(RetrievalSourceProfileKind::CompactionArtifact);
        let lexical_score = retrieval_lexical_overlap_score(
            searchable.as_str(),
            query_variants,
            retrieval_config.scoring.phrase_match_bonus_bps,
        );
        let vector_score = retrieval_proxy_vector_score(searchable.as_str(), query_variants);
        let recency_score =
            retrieval_recency_score(hit.created_at_unix_ms, now_unix_ms, profile.min_recency_bps);
        let source_quality_score =
            0.84_f64.clamp(f64::from(profile.min_source_quality_bps) / 10_000.0, 1.0);
        let score_breakdown = score_with_profile(
            lexical_score,
            vector_score,
            recency_score,
            source_quality_score,
            false,
            profile,
        );
        let score = RecallScoreBreakdown {
            lexical_score: score_breakdown.lexical_score,
            vector_score: score_breakdown.vector_score,
            recency_score: score_breakdown.recency_score,
            source_quality_score: score_breakdown.source_quality_score,
            final_score: score_breakdown.final_score,
        };
        let candidate = RecallCandidate {
            candidate_id: format!("compaction:{}", hit.artifact_id),
            source_kind: RecallSourceKind::CompactionArtifact,
            source_ref: hit.artifact_id.clone(),
            title: format!("Compaction {}", hit.artifact_id),
            snippet: hit.summary_preview.clone(),
            created_at_unix_ms: hit.created_at_unix_ms,
            rationale: format!(
                "matched compaction summary with lexical {:.2} and recency {:.2}",
                lexical_score, recency_score
            ),
            score,
        };
        records.push(CandidateRecord { candidate });
    }

    records
}

fn finalize_top_candidates(
    mut candidate_records: Vec<CandidateRecord>,
    max_candidates: usize,
    prompt_budget_tokens: usize,
) -> Vec<RecallCandidate> {
    candidate_records.sort_by(|left, right| compare_candidate_records(right, left));
    let mut selected = Vec::new();
    let mut used_tokens = 0usize;
    for record in candidate_records {
        if selected.len() >= max_candidates {
            break;
        }
        let candidate_tokens = estimate_tokens(
            format!("{}\n{}", record.candidate.title, record.candidate.snippet).as_str(),
        );
        if !selected.is_empty()
            && used_tokens.saturating_add(candidate_tokens) > prompt_budget_tokens
        {
            continue;
        }
        used_tokens = used_tokens.saturating_add(candidate_tokens);
        selected.push(record.candidate);
    }
    selected
}

fn selection_from_candidates(
    query: &str,
    request: &RecallRequest,
    top_candidates: &[RecallCandidate],
    default_channel: Option<String>,
    scoped_session_id: Option<String>,
) -> ExplicitRecallSelection {
    let mut memory_item_ids = Vec::new();
    let mut workspace_document_ids = Vec::new();
    let mut transcript_refs = Vec::new();
    let mut checkpoint_ids = Vec::new();
    let mut compaction_artifact_ids = Vec::new();
    for candidate in top_candidates {
        match candidate.source_kind {
            RecallSourceKind::Memory => memory_item_ids.push(candidate.source_ref.clone()),
            RecallSourceKind::WorkspaceDocument => {
                workspace_document_ids.push(candidate.source_ref.clone());
            }
            RecallSourceKind::Transcript => {
                if let Some((run_id, seq)) = candidate.source_ref.split_once(':') {
                    if let Ok(seq) = seq.parse::<i64>() {
                        transcript_refs
                            .push(TranscriptRecallRef { run_id: run_id.to_owned(), seq });
                    }
                }
            }
            RecallSourceKind::Checkpoint => checkpoint_ids.push(candidate.source_ref.clone()),
            RecallSourceKind::CompactionArtifact => {
                compaction_artifact_ids.push(candidate.source_ref.clone());
            }
        }
    }
    ExplicitRecallSelection {
        query: query.to_owned(),
        channel: request.channel.clone().or(default_channel),
        session_id: scoped_session_id,
        agent_id: request.agent_id.clone(),
        min_score: Some(request.min_score),
        workspace_prefix: request.workspace_prefix.clone(),
        include_workspace_historical: request.include_workspace_historical,
        include_workspace_quarantined: request.include_workspace_quarantined,
        memory_item_ids,
        workspace_document_ids,
        transcript_refs,
        checkpoint_ids,
        compaction_artifact_ids,
    }
}

fn build_structured_output(
    top_candidates: &[RecallCandidate],
    query: &str,
) -> StructuredRecallOutput {
    let evidence = top_candidates
        .iter()
        .enumerate()
        .map(|(index, candidate)| RecallEvidenceRecord {
            evidence_id: format!("evidence-{}", index + 1),
            source_kind: candidate.source_kind.clone(),
            source_ref: candidate.source_ref.clone(),
            title: candidate.title.clone(),
            snippet: candidate.snippet.clone(),
            rationale: candidate.rationale.clone(),
            score: candidate.score.clone(),
        })
        .collect::<Vec<_>>();
    let facts = evidence
        .iter()
        .take(4)
        .map(|evidence| RecallFact {
            statement: format!(
                "{}: {}",
                evidence.source_kind.as_str(),
                summarize_fact(evidence.snippet.as_str()),
            ),
            evidence_ids: vec![evidence.evidence_id.clone()],
        })
        .collect::<Vec<_>>();
    let confidence = if evidence.is_empty() {
        None
    } else {
        Some(
            evidence.iter().take(3).map(|record| record.score.final_score).sum::<f64>()
                / evidence.len().min(3) as f64,
        )
    };
    let dominant_sources = evidence
        .iter()
        .map(|record| record.source_kind.as_str())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    let why_relevant_now = if evidence.is_empty() {
        format!("No recall evidence crossed the planner threshold for query '{query}'.")
    } else {
        format!(
            "Top evidence for '{query}' came from {} with {} selected candidates.",
            dominant_sources.join(", "),
            evidence.len()
        )
    };
    let suggested_next_step = if evidence.is_empty() {
        "Refine the query or narrow the scope before attaching recall to the next prompt."
            .to_owned()
    } else if dominant_sources.contains(&"workspace_document") {
        "Open the strongest workspace document or attach the selected recall set to the next prompt."
            .to_owned()
    } else if dominant_sources.contains(&"checkpoint") {
        "Inspect the checkpoint evidence before deciding whether session state should be restored."
            .to_owned()
    } else {
        "Attach the selected recall evidence to the next prompt and keep the query specific."
            .to_owned()
    };
    StructuredRecallOutput { facts, evidence, why_relevant_now, suggested_next_step, confidence }
}

fn summarize_fact(snippet: &str) -> String {
    let normalized = sanitize_prompt_inline_value(snippet);
    let sentence = normalized
        .split_terminator(['.', '!', '?'])
        .find(|candidate| !candidate.trim().is_empty())
        .map(str::trim)
        .unwrap_or(normalized.as_str());
    truncate_console_text(sentence, 140)
}

fn plan_source_selected(sources: &[RecallPlanSource], kind: RecallSourceKind) -> bool {
    sources.iter().any(|source| {
        source.source_kind == kind && source.decision == RecallSourceDecision::Selected
    })
}

fn compare_candidate_records(
    left: &CandidateRecord,
    right: &CandidateRecord,
) -> std::cmp::Ordering {
    compare_scores(left.candidate.score.final_score, right.candidate.score.final_score)
        .then_with(|| {
            compare_scores(
                left.candidate.score.source_quality_score,
                right.candidate.score.source_quality_score,
            )
        })
        .then_with(|| left.candidate.created_at_unix_ms.cmp(&right.candidate.created_at_unix_ms))
        .then_with(|| left.candidate.source_ref.cmp(&right.candidate.source_ref))
}

fn compare_scores(left: f64, right: f64) -> std::cmp::Ordering {
    left.partial_cmp(&right).unwrap_or(std::cmp::Ordering::Equal)
}

fn checkpoint_search_text(hit: &CheckpointRecallHit) -> String {
    format!(
        "{} {} {}",
        hit.name,
        hit.note.as_deref().unwrap_or_default(),
        hit.workspace_paths.join(" ")
    )
}

fn compaction_search_text(hit: &CompactionRecallHit) -> String {
    format!("{} {}", hit.trigger_reason, hit.summary_preview)
}

fn transcript_reason(
    record: &OrchestratorSessionTranscriptRecord,
    lexical_score: f64,
    recency_score: f64,
) -> String {
    format!(
        "matched {} event with lexical {:.2} and recency {:.2}",
        record.event_type, lexical_score, recency_score
    )
}

fn checkpoint_reason(checkpoint: &OrchestratorCheckpointRecord, lexical_score: f64) -> String {
    format!("matched checkpoint '{}' with lexical {:.2}", checkpoint.name, lexical_score)
}

fn compaction_reason(
    artifact: &OrchestratorCompactionArtifactRecord,
    lexical_score: f64,
) -> String {
    format!(
        "matched compaction artifact '{}' with lexical {:.2}",
        artifact.artifact_id, lexical_score
    )
}

#[cfg(test)]
#[allow(dead_code)]
fn transcript_source_quality(record: &OrchestratorSessionTranscriptRecord) -> f64 {
    let base: f64 = match record.event_type.as_str() {
        "message.received" | "queued.input" => 0.72,
        "message.replied" => 0.76,
        _ => 0.70,
    };
    base.clamp(MIN_SOURCE_QUALITY_SCORE, 1.0)
}

#[cfg(test)]
#[allow(dead_code)]
fn memory_source_quality(hit: &MemorySearchHit) -> f64 {
    let confidence = hit.item.confidence.unwrap_or(0.75).clamp(0.0, 1.0);
    let source_bias = match hit.item.source {
        crate::journal::MemorySource::Manual => 0.94,
        crate::journal::MemorySource::Summary => 0.88,
        crate::journal::MemorySource::Import => 0.84,
        crate::journal::MemorySource::TapeUserMessage => 0.78,
        crate::journal::MemorySource::TapeToolResult => 0.74,
    };
    ((confidence * 0.6) + (source_bias * 0.4)).clamp(MIN_SOURCE_QUALITY_SCORE, 1.0)
}

#[cfg(test)]
#[allow(dead_code)]
fn workspace_source_quality(hit: &WorkspaceSearchHit) -> f64 {
    let mut quality: f64 = 0.78;
    if hit.document.pinned {
        quality += 0.10;
    }
    if hit.document.manual_override {
        quality += 0.05;
    }
    if hit.document.prompt_binding == "system_candidate" {
        quality += 0.04;
    }
    if hit.document.risk_state != "clean" {
        quality -= 0.12;
    }
    quality.clamp(MIN_SOURCE_QUALITY_SCORE, 1.0)
}

#[cfg(test)]
#[allow(dead_code)]
fn checkpoint_source_quality(checkpoint: &OrchestratorCheckpointRecord) -> f64 {
    let mut quality: f64 = 0.86;
    if checkpoint.restore_count > 0 {
        quality += 0.04;
    }
    if checkpoint.note.as_deref().is_some_and(|note| !note.trim().is_empty()) {
        quality += 0.02;
    }
    quality.clamp(MIN_SOURCE_QUALITY_SCORE, 1.0)
}

#[cfg(test)]
#[allow(dead_code)]
fn compaction_source_quality(artifact: &OrchestratorCompactionArtifactRecord) -> f64 {
    let summary = serde_json::from_str::<Value>(artifact.summary_json.as_str()).unwrap_or_default();
    let review_penalty = summary
        .pointer("/planner/review_candidate_count")
        .and_then(Value::as_u64)
        .unwrap_or_default() as f64
        * 0.02;
    let poisoned_penalty = summary
        .pointer("/quality_gates/poisoned_candidate_count")
        .and_then(Value::as_u64)
        .unwrap_or_default() as f64
        * 0.08;
    (0.88 - review_penalty - poisoned_penalty).clamp(MIN_SOURCE_QUALITY_SCORE, 1.0)
}

#[cfg(test)]
#[allow(dead_code)]
fn lexical_overlap_score(text: &str, query_variants: &[String]) -> f64 {
    query_variants.iter().map(|query| lexical_overlap_for_query(text, query)).fold(0.0, f64::max)
}

#[cfg(test)]
#[allow(dead_code)]
fn lexical_overlap_for_query(text: &str, query: &str) -> f64 {
    let haystack = normalized_tokens(text);
    let needles = normalized_tokens(query);
    if haystack.is_empty() || needles.is_empty() {
        return 0.0;
    }
    let needle_set = needles.iter().collect::<BTreeSet<_>>();
    let match_count = haystack.iter().filter(|token| needle_set.contains(token)).count();
    let phrase_bonus = if text.to_ascii_lowercase().contains(query.to_ascii_lowercase().as_str()) {
        0.20
    } else {
        0.0
    };
    ((match_count as f64 / needle_set.len().max(1) as f64) + phrase_bonus).min(1.0)
}

#[cfg(test)]
#[allow(dead_code)]
fn proxy_vector_score(text: &str, query_variants: &[String]) -> f64 {
    query_variants
        .iter()
        .map(|query| {
            let text_ngrams = char_ngrams(text);
            let query_ngrams = char_ngrams(query);
            if text_ngrams.is_empty() || query_ngrams.is_empty() {
                return 0.0;
            }
            let shared = text_ngrams.intersection(&query_ngrams).count();
            shared as f64 / query_ngrams.len().max(1) as f64
        })
        .fold(0.0, f64::max)
        .clamp(0.0, 1.0)
}

#[cfg(test)]
#[allow(dead_code)]
fn recency_score(created_at_unix_ms: i64, now_unix_ms: i64) -> f64 {
    if created_at_unix_ms <= 0 || now_unix_ms <= created_at_unix_ms {
        return 1.0;
    }
    let age_days = (now_unix_ms - created_at_unix_ms) as f64 / 86_400_000.0;
    (1.0 / (1.0 + age_days / 7.0)).clamp(MIN_TRANSCRIPT_RECENCY_SCORE, 1.0)
}

#[cfg(test)]
#[allow(dead_code)]
fn final_score(
    lexical_score: f64,
    vector_score: f64,
    recency_score: f64,
    source_quality_score: f64,
) -> f64 {
    ((lexical_score * 0.42)
        + (vector_score * 0.24)
        + (recency_score * 0.16)
        + (source_quality_score * 0.18))
        .clamp(0.0, 1.0)
}

#[cfg(test)]
#[allow(dead_code)]
fn normalized_tokens(input: &str) -> Vec<String> {
    input
        .chars()
        .map(|character| {
            if character.is_alphanumeric() || matches!(character, '/' | '.' | '_' | '-') {
                character.to_ascii_lowercase()
            } else {
                ' '
            }
        })
        .collect::<String>()
        .split_whitespace()
        .map(ToOwned::to_owned)
        .collect()
}

#[cfg(test)]
#[allow(dead_code)]
fn char_ngrams(input: &str) -> BTreeSet<String> {
    let normalized = input
        .chars()
        .map(|character| if character.is_control() { ' ' } else { character.to_ascii_lowercase() })
        .collect::<String>();
    let chars = normalized.chars().collect::<Vec<_>>();
    if chars.len() < 3 {
        return chars.into_iter().map(|character| character.to_string()).collect();
    }
    let mut grams = BTreeSet::new();
    for window in chars.windows(3) {
        grams.insert(window.iter().collect::<String>());
    }
    grams
}

fn estimate_tokens(input: &str) -> usize {
    let trimmed = input.trim();
    if trimmed.is_empty() {
        0
    } else {
        trimmed.chars().count().div_ceil(4)
    }
}

fn contains_any(input: &str, patterns: &[&str]) -> bool {
    patterns.iter().any(|pattern| input.contains(pattern))
}

fn trim_option(value: Option<String>) -> Option<String> {
    value.and_then(|value| {
        let trimmed = value.trim();
        (!trimmed.is_empty()).then(|| trimmed.to_owned())
    })
}

fn select_memory_hits(hits: Vec<MemorySearchHit>, selected_ids: &[String]) -> Vec<MemorySearchHit> {
    let mut by_id =
        hits.into_iter().map(|hit| (hit.item.memory_id.clone(), hit)).collect::<HashMap<_, _>>();
    selected_ids.iter().filter_map(|memory_id| by_id.remove(memory_id)).collect()
}

fn select_workspace_hits(
    hits: Vec<WorkspaceSearchHit>,
    selected_ids: &[String],
) -> Vec<WorkspaceSearchHit> {
    let mut by_id = hits
        .into_iter()
        .map(|hit| (hit.document.document_id.clone(), hit))
        .collect::<HashMap<_, _>>();
    selected_ids.iter().filter_map(|document_id| by_id.remove(document_id)).collect()
}

fn select_transcript_hits(
    hits: Vec<TranscriptRecallHit>,
    selected_refs: &[TranscriptRecallRef],
) -> Vec<TranscriptRecallHit> {
    let mut by_key =
        hits.into_iter().map(|hit| ((hit.run_id.clone(), hit.seq), hit)).collect::<HashMap<_, _>>();
    selected_refs
        .iter()
        .filter_map(|reference| by_key.remove(&(reference.run_id.clone(), reference.seq)))
        .collect()
}

fn select_checkpoint_hits(
    hits: Vec<CheckpointRecallHit>,
    selected_ids: &[String],
) -> Vec<CheckpointRecallHit> {
    let mut by_id =
        hits.into_iter().map(|hit| (hit.checkpoint_id.clone(), hit)).collect::<HashMap<_, _>>();
    selected_ids.iter().filter_map(|checkpoint_id| by_id.remove(checkpoint_id)).collect()
}

fn select_compaction_hits(
    hits: Vec<CompactionRecallHit>,
    selected_ids: &[String],
) -> Vec<CompactionRecallHit> {
    let mut by_id =
        hits.into_iter().map(|hit| (hit.artifact_id.clone(), hit)).collect::<HashMap<_, _>>();
    selected_ids.iter().filter_map(|artifact_id| by_id.remove(artifact_id)).collect()
}

#[cfg(test)]
mod tests {
    use super::{
        build_query_variants, build_recall_plan, build_structured_output, finalize_top_candidates,
        lexical_overlap_score, proxy_vector_score, recall_preview_console_payload,
        selection_from_candidates, ExplicitRecallSelection, RecallCandidate, RecallRequest,
        RecallScoreBreakdown, RecallSourceDecision, RecallSourceKind, TranscriptRecallRef,
    };
    use serde_json::json;

    fn candidate(
        id: &str,
        source_kind: RecallSourceKind,
        final_score: f64,
        source_quality_score: f64,
        created_at_unix_ms: i64,
    ) -> RecallCandidate {
        RecallCandidate {
            candidate_id: id.to_owned(),
            source_kind,
            source_ref: id.to_owned(),
            title: id.to_owned(),
            snippet: format!("snippet for {id}"),
            created_at_unix_ms,
            rationale: "fixture".to_owned(),
            score: RecallScoreBreakdown {
                lexical_score: final_score,
                vector_score: final_score,
                recency_score: final_score,
                source_quality_score,
                final_score,
            },
        }
    }

    #[test]
    fn planner_selects_history_sources_for_history_queries() {
        let plan = build_recall_plan(
            "what did we say earlier about the rollback checkpoint",
            build_query_variants("what did we say earlier about the rollback checkpoint")
                .as_slice(),
            true,
            &RecallRequest {
                query: "what did we say earlier about the rollback checkpoint".to_owned(),
                channel: Some("console".to_owned()),
                session_id: Some("01ARZ3NDEKTSV4RRFFQ69G5FAV".to_owned()),
                agent_id: None,
                memory_top_k: 4,
                workspace_top_k: 4,
                min_score: 0.1,
                workspace_prefix: None,
                include_workspace_historical: false,
                include_workspace_quarantined: false,
                max_candidates: 8,
                prompt_budget_tokens: 1_800,
            },
        );
        assert!(plan
            .sources
            .iter()
            .any(|source| source.source_kind == RecallSourceKind::Transcript
                && source.decision == RecallSourceDecision::Selected));
        assert!(plan
            .sources
            .iter()
            .any(|source| source.source_kind == RecallSourceKind::Checkpoint
                && source.decision == RecallSourceDecision::Selected));
    }

    #[test]
    fn reranker_breaks_ties_by_source_quality_then_recency() {
        let top = finalize_top_candidates(
            vec![
                super::CandidateRecord {
                    candidate: candidate("memory-a", RecallSourceKind::Memory, 0.8, 0.7, 10),
                },
                super::CandidateRecord {
                    candidate: candidate("memory-b", RecallSourceKind::Memory, 0.8, 0.9, 8),
                },
                super::CandidateRecord {
                    candidate: candidate("memory-c", RecallSourceKind::Memory, 0.8, 0.9, 12),
                },
            ],
            3,
            2_000,
        );
        assert_eq!(top[0].source_ref, "memory-c");
        assert_eq!(top[1].source_ref, "memory-b");
        assert_eq!(top[2].source_ref, "memory-a");
    }

    #[test]
    fn structured_output_preserves_evidence_links() {
        let top_candidates = vec![
            candidate("memory-1", RecallSourceKind::Memory, 0.91, 0.92, 10),
            candidate("checkpoint-1", RecallSourceKind::Checkpoint, 0.84, 0.90, 9),
        ];
        let structured = build_structured_output(top_candidates.as_slice(), "rollback checklist");
        assert_eq!(structured.evidence.len(), 2);
        assert_eq!(structured.facts.len(), 2);
        assert_eq!(structured.facts[0].evidence_ids, vec!["evidence-1".to_owned()]);
        assert_eq!(structured.evidence[1].source_ref, "checkpoint-1");
    }

    #[test]
    fn query_variants_keep_original_and_compact_keywords() {
        let variants = build_query_variants("Where is the rollback checkpoint for apps/web?");
        assert_eq!(variants[0], "Where is the rollback checkpoint for apps/web?");
        assert!(
            variants.iter().any(|variant| variant.contains("rollback checkpoint apps/web")),
            "keyword-compacted variant should be present: {variants:?}"
        );
    }

    #[test]
    fn lexical_and_vector_proxy_scores_reward_overlap() {
        let lexical = lexical_overlap_score(
            "rollback checklist release train",
            &["rollback checklist".to_owned()],
        );
        let vector = proxy_vector_score(
            "rollback checklist release train",
            &["rollback checklist".to_owned()],
        );
        assert!(lexical > 0.9, "lexical overlap should be strong");
        assert!(vector > 0.5, "ngram overlap should be strong");
    }

    #[test]
    fn selection_tracks_source_specific_ids() {
        let selection = selection_from_candidates(
            "rollback",
            &RecallRequest {
                query: "rollback".to_owned(),
                channel: Some("cli".to_owned()),
                session_id: Some("01ARZ3NDEKTSV4RRFFQ69G5FAV".to_owned()),
                agent_id: Some("agent-1".to_owned()),
                memory_top_k: 4,
                workspace_top_k: 4,
                min_score: 0.1,
                workspace_prefix: Some("docs".to_owned()),
                include_workspace_historical: true,
                include_workspace_quarantined: false,
                max_candidates: 8,
                prompt_budget_tokens: 1_800,
            },
            &[
                RecallCandidate {
                    candidate_id: "memory-1".to_owned(),
                    source_kind: RecallSourceKind::Memory,
                    source_ref: "memory-1".to_owned(),
                    title: "Memory".to_owned(),
                    snippet: "memory".to_owned(),
                    created_at_unix_ms: 1,
                    rationale: "memory".to_owned(),
                    score: RecallScoreBreakdown {
                        lexical_score: 1.0,
                        vector_score: 1.0,
                        recency_score: 1.0,
                        source_quality_score: 1.0,
                        final_score: 1.0,
                    },
                },
                RecallCandidate {
                    candidate_id: "transcript-1".to_owned(),
                    source_kind: RecallSourceKind::Transcript,
                    source_ref: "run-1:7".to_owned(),
                    title: "Transcript".to_owned(),
                    snippet: "transcript".to_owned(),
                    created_at_unix_ms: 2,
                    rationale: "transcript".to_owned(),
                    score: RecallScoreBreakdown {
                        lexical_score: 0.9,
                        vector_score: 0.9,
                        recency_score: 0.9,
                        source_quality_score: 0.9,
                        final_score: 0.9,
                    },
                },
            ],
            Some("console".to_owned()),
            Some("session-1".to_owned()),
        );
        assert_eq!(selection.memory_item_ids, vec!["memory-1".to_owned()]);
        assert_eq!(
            selection.transcript_refs,
            vec![TranscriptRecallRef { run_id: "run-1".to_owned(), seq: 7 }]
        );
        assert_eq!(selection.session_id.as_deref(), Some("session-1"));
    }

    #[test]
    fn console_payload_summarizes_preview_counts() {
        let payload = recall_preview_console_payload(&super::RecallPreviewEnvelope {
            query: "rollback".to_owned(),
            memory_hits: Vec::new(),
            workspace_hits: Vec::new(),
            transcript_hits: Vec::new(),
            checkpoint_hits: Vec::new(),
            compaction_hits: Vec::new(),
            top_candidates: vec![candidate("memory-1", RecallSourceKind::Memory, 0.8, 0.8, 1)],
            structured_output: build_structured_output(
                &[candidate("memory-1", RecallSourceKind::Memory, 0.8, 0.8, 1)],
                "rollback",
            ),
            plan: super::RecallPlan {
                original_query: "rollback".to_owned(),
                expanded_queries: vec!["rollback history".to_owned()],
                session_scoped: true,
                budget: super::RecallBudgetExplain {
                    prompt_budget_tokens: 1_800,
                    candidate_limit: 8,
                },
                sources: Vec::new(),
            },
            diagnostics: Vec::new(),
            parameter_delta: json!({
                "explicit_recall": ExplicitRecallSelection {
                    query: "rollback".to_owned(),
                    channel: Some("cli".to_owned()),
                    session_id: Some("session-1".to_owned()),
                    agent_id: None,
                    min_score: Some(0.1),
                    workspace_prefix: None,
                    include_workspace_historical: false,
                    include_workspace_quarantined: false,
                    memory_item_ids: vec!["memory-1".to_owned()],
                    workspace_document_ids: Vec::new(),
                    transcript_refs: Vec::new(),
                    checkpoint_ids: Vec::new(),
                    compaction_artifact_ids: Vec::new(),
                }
            }),
            prompt_preview: "preview".to_owned(),
        });
        assert_eq!(payload.get("top_candidates").and_then(serde_json::Value::as_u64), Some(1));
    }
}
