use std::{
    collections::hash_map::DefaultHasher,
    hash::{Hash, Hasher},
    sync::Arc,
};

use palyra_safety::{
    transform_text_for_prompt, SafetyAction, SafetyContentKind, SafetySourceKind, TrustLabel,
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use tonic::Status;
use tracing::warn;

use crate::{
    application::{
        context_references::{render_context_reference_block, ContextReferencePreviewEnvelope},
        learning::render_preference_prompt_context,
        provider_input::{
            build_attachment_recall_prompt, build_explicit_recall_prompt,
            build_memory_augmented_prompt, build_previous_run_context_prompt,
            build_project_context_prompt, build_provider_image_inputs,
            resolve_latest_session_compaction_artifact, MemoryPromptFailureMode,
            PrepareModelProviderInputRequest, PreparedModelProviderInput,
        },
    },
    gateway::{ingest_memory_best_effort, GatewayRuntimeState},
    journal::{
        OrchestratorCheckpointRecord, OrchestratorCompactionArtifactRecord,
        OrchestratorTapeAppendRequest,
    },
    transport::grpc::auth::RequestContext,
};

const DEFAULT_CONTEXT_WINDOW_TOKENS: u64 = 8_192;
const MIN_CONTEXT_WINDOW_TOKENS: u64 = 2_048;
const MAX_RESERVED_COMPLETION_TOKENS: u64 = 2_048;
const MIN_RESERVED_COMPLETION_TOKENS: u64 = 512;
const RESERVED_TOOL_RESULT_TOKENS: u64 = 512;
const PROVIDER_OVERHEAD_TOKENS: u64 = 192;
const SEGMENT_PREVIEW_CHARS: usize = 180;
const CONTEXT_ENGINE_PLAN_EVENT: &str = "context.engine.plan";

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ContextEngineStrategy {
    Noop,
    CheckpointAware,
    Summarizing,
    CostAware,
    ProviderAware,
}

impl ContextEngineStrategy {
    fn as_str(self) -> &'static str {
        match self {
            Self::Noop => "noop",
            Self::CheckpointAware => "checkpoint_aware",
            Self::Summarizing => "summarizing",
            Self::CostAware => "cost_aware",
            Self::ProviderAware => "provider_aware",
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ContextSegmentKind {
    PreferenceContext,
    ProjectContext,
    SessionCompactionSummary,
    CheckpointSummary,
    ContextReferences,
    AttachmentRecall,
    ExplicitRecall,
    MemoryRecall,
    SessionTail,
    ToolExchange,
    UserInput,
}

impl ContextSegmentKind {
    #[allow(dead_code)]
    fn as_str(self) -> &'static str {
        match self {
            Self::PreferenceContext => "preference_context",
            Self::ProjectContext => "project_context",
            Self::SessionCompactionSummary => "session_compaction_summary",
            Self::CheckpointSummary => "checkpoint_summary",
            Self::ContextReferences => "context_references",
            Self::AttachmentRecall => "attachment_recall",
            Self::ExplicitRecall => "explicit_recall",
            Self::MemoryRecall => "memory_recall",
            Self::SessionTail => "session_tail",
            Self::ToolExchange => "tool_exchange",
            Self::UserInput => "user_input",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct ContextEngineSegmentExplain {
    pub(crate) kind: ContextSegmentKind,
    pub(crate) label: String,
    pub(crate) estimated_tokens: u64,
    pub(crate) stable: bool,
    pub(crate) protected: bool,
    pub(crate) trust_label: TrustLabel,
    pub(crate) safety_action: SafetyAction,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub(crate) safety_findings: Vec<String>,
    pub(crate) group_id: Option<String>,
    pub(crate) preview: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct ContextEngineDroppedSegmentExplain {
    pub(crate) kind: ContextSegmentKind,
    pub(crate) label: String,
    pub(crate) estimated_tokens: u64,
    pub(crate) reason: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct ContextEngineBudgetExplain {
    pub(crate) model_id: String,
    pub(crate) max_context_tokens: u64,
    pub(crate) reserved_completion_tokens: u64,
    pub(crate) reserved_tool_result_tokens: u64,
    pub(crate) provider_overhead_tokens: u64,
    pub(crate) input_budget_tokens: u64,
    pub(crate) selected_tokens: u64,
    pub(crate) dropped_tokens: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct ContextEngineCacheExplain {
    pub(crate) provider_cache_supported: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) stable_prefix_hash: Option<String>,
    pub(crate) stable_prefix_tokens: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) cache_scope_key: Option<String>,
    pub(crate) trust_scope: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct SummaryQualityGateExplain {
    pub(crate) verdict: String,
    pub(crate) repeated_compaction_depth: usize,
    pub(crate) contradiction_signals: usize,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub(crate) reasons: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct ContextEngineExplain {
    pub(crate) strategy: ContextEngineStrategy,
    pub(crate) rollout_enabled: bool,
    pub(crate) budget: ContextEngineBudgetExplain,
    pub(crate) cache: ContextEngineCacheExplain,
    pub(crate) summary_quality: Option<SummaryQualityGateExplain>,
    pub(crate) selected_segments: Vec<ContextEngineSegmentExplain>,
    pub(crate) dropped_segments: Vec<ContextEngineDroppedSegmentExplain>,
}

#[derive(Debug, Clone)]
struct ContextSegment {
    kind: ContextSegmentKind,
    label: String,
    content: String,
    estimated_tokens: u64,
    priority: u8,
    stable: bool,
    protected: bool,
    group_id: Option<String>,
    trust_label: TrustLabel,
    safety_action: SafetyAction,
    safety_findings: Vec<String>,
}

impl ContextSegment {
    fn trusted(
        kind: ContextSegmentKind,
        label: impl Into<String>,
        content: String,
        priority: u8,
        stable: bool,
        protected: bool,
        group_id: Option<String>,
    ) -> Self {
        Self {
            kind,
            label: label.into(),
            estimated_tokens: estimate_tokens(content.as_str()),
            content,
            priority,
            stable,
            protected,
            group_id,
            trust_label: TrustLabel::TrustedLocal,
            safety_action: SafetyAction::Allow,
            safety_findings: Vec::new(),
        }
    }

    fn with_safety(
        mut self,
        trust_label: TrustLabel,
        safety_action: SafetyAction,
        mut safety_findings: Vec<String>,
    ) -> Self {
        safety_findings.sort();
        safety_findings.dedup();
        self.trust_label = trust_label;
        self.safety_action = safety_action;
        self.safety_findings = safety_findings;
        self
    }
}

#[derive(Debug, Clone, Copy)]
struct ProviderContextBudget {
    max_context_tokens: u64,
    reserved_completion_tokens: u64,
    reserved_tool_result_tokens: u64,
    provider_overhead_tokens: u64,
    provider_cache_supported: bool,
}

impl ProviderContextBudget {
    fn input_budget_tokens(self) -> u64 {
        self.max_context_tokens
            .saturating_sub(self.reserved_completion_tokens)
            .saturating_sub(self.reserved_tool_result_tokens)
            .saturating_sub(self.provider_overhead_tokens)
            .max(1_024)
    }
}

#[derive(Debug, Clone)]
struct CompactionContextDecision {
    segment: Option<ContextSegment>,
    summary_quality: Option<SummaryQualityGateExplain>,
    checkpoint_summary_present: bool,
}

#[derive(Debug, Clone, Deserialize)]
struct ContextReferenceParameterDelta {
    #[serde(default)]
    context_references: Option<ContextReferencePreviewEnvelope>,
}

#[allow(clippy::result_large_err)]
pub(crate) async fn prepare_model_provider_input_with_context_engine(
    runtime_state: &Arc<GatewayRuntimeState>,
    context: &RequestContext,
    request: PrepareModelProviderInputRequest<'_>,
) -> Result<PreparedModelProviderInput, Status> {
    let PrepareModelProviderInputRequest {
        run_id,
        tape_seq,
        session_id,
        previous_run_id,
        parameter_delta_json,
        input_text,
        attachments,
        memory_ingest_reason,
        memory_prompt_failure_mode,
        channel_for_log,
    } = request;

    let normalized_input_text = normalized_input_text(parameter_delta_json, input_text);
    ingest_memory_best_effort(
        runtime_state,
        context.principal.as_str(),
        context.channel.as_deref(),
        Some(session_id),
        crate::journal::MemorySource::TapeUserMessage,
        normalized_input_text.as_str(),
        Vec::new(),
        Some(0.9),
        memory_ingest_reason,
    )
    .await;

    let provider_budget =
        resolve_provider_context_budget(&runtime_state.model_provider_status_snapshot(), None);
    let vision_inputs = build_provider_image_inputs(attachments, &runtime_state.config.media);
    let mut segments = Vec::new();

    if let Ok(Some(preference_context)) =
        render_preference_prompt_context(runtime_state, context).await
    {
        push_segment(
            &mut segments,
            ContextSegment::trusted(
                ContextSegmentKind::PreferenceContext,
                "preference_context",
                preference_context,
                92,
                true,
                true,
                None,
            ),
        );
    }

    if let Some(project_context_block) =
        build_project_context_prompt(runtime_state, run_id, tape_seq, parameter_delta_json, "")
            .await?
            .and_then(clean_segment_content)
    {
        push_segment(
            &mut segments,
            ContextSegment::trusted(
                ContextSegmentKind::ProjectContext,
                "project_context",
                project_context_block,
                86,
                true,
                false,
                None,
            ),
        );
    }

    let compaction_decision = collect_compaction_context_decision(
        runtime_state,
        context,
        run_id,
        tape_seq,
        session_id,
        provider_budget,
    )
    .await?;
    if let Some(segment) = compaction_decision.segment.clone() {
        push_segment(&mut segments, segment);
    }

    if let Some(context_reference_segment) =
        build_context_reference_segment(runtime_state, run_id, tape_seq, parameter_delta_json)
            .await?
    {
        push_segment(&mut segments, context_reference_segment);
    }

    if let Some(attachment_recall_block) =
        build_attachment_recall_prompt(runtime_state, run_id, tape_seq, parameter_delta_json, "")
            .await?
            .and_then(clean_segment_content)
    {
        let transformed = transform_text_for_prompt(
            attachment_recall_block.as_str(),
            SafetySourceKind::AttachmentRecall,
            SafetyContentKind::AttachmentRecall,
            TrustLabel::ExternalUntrusted,
        );
        push_segment(
            &mut segments,
            ContextSegment::trusted(
                ContextSegmentKind::AttachmentRecall,
                "attachment_recall",
                transformed.transformed_text,
                88,
                false,
                false,
                None,
            )
            .with_safety(
                transformed.scan.trust_label,
                transformed.scan.recommended_action,
                transformed.scan.finding_codes(),
            ),
        );
    }

    let explicit_recall_block = build_explicit_recall_prompt(
        runtime_state,
        context,
        run_id,
        tape_seq,
        session_id,
        parameter_delta_json,
        "",
    )
    .await?
    .and_then(clean_segment_content);
    if let Some(block) = explicit_recall_block.clone() {
        push_segment(
            &mut segments,
            ContextSegment::trusted(
                ContextSegmentKind::ExplicitRecall,
                "explicit_recall",
                block,
                90,
                false,
                false,
                None,
            ),
        );
    }

    if explicit_recall_block.is_none() {
        match build_memory_augmented_prompt(
            runtime_state,
            context,
            run_id,
            tape_seq,
            session_id,
            normalized_input_text.as_str(),
            "",
        )
        .await
        .map(clean_segment_content)
        {
            Ok(Some(memory_block)) => push_segment(
                &mut segments,
                ContextSegment::trusted(
                    ContextSegmentKind::MemoryRecall,
                    "memory_auto_inject",
                    memory_block,
                    72,
                    false,
                    false,
                    None,
                ),
            ),
            Ok(None) => {}
            Err(error) => match memory_prompt_failure_mode {
                MemoryPromptFailureMode::Fail => return Err(error),
                MemoryPromptFailureMode::FallbackToRawInput { warn_message } => {
                    warn!(
                        run_id,
                        principal = %context.principal,
                        session_id,
                        channel = channel_for_log,
                        status_code = ?error.code(),
                        status_message = %error.message(),
                        "{warn_message}"
                    );
                }
            },
        }
    }

    if let Some(previous_run_context_block) = clean_segment_content(
        build_previous_run_context_prompt(runtime_state, previous_run_id, "").await?,
    ) {
        push_segment(
            &mut segments,
            ContextSegment::trusted(
                ContextSegmentKind::SessionTail,
                "recent_conversation",
                previous_run_context_block,
                84,
                false,
                true,
                None,
            ),
        );
    }

    push_segment(
        &mut segments,
        ContextSegment::trusted(
            ContextSegmentKind::UserInput,
            "user_input",
            normalized_input_text.clone(),
            100,
            false,
            true,
            None,
        ),
    );

    let strategy = select_strategy(
        segments.as_slice(),
        provider_budget,
        compaction_decision.summary_quality.as_ref(),
        compaction_decision.checkpoint_summary_present,
    );
    let assembled = assemble_segments(
        segments.as_slice(),
        strategy,
        provider_budget,
        context,
        session_id,
        compaction_decision.summary_quality.clone(),
    );

    record_context_engine_plan(runtime_state, run_id, tape_seq, assembled.explain.clone()).await?;

    Ok(PreparedModelProviderInput { provider_input_text: assembled.prompt_text, vision_inputs })
}

#[derive(Debug)]
struct AssembledPrompt {
    prompt_text: String,
    explain: ContextEngineExplain,
}

#[derive(Debug, Clone)]
struct IndexedContextSegment {
    order: usize,
    segment: ContextSegment,
}

fn assemble_segments(
    segments: &[ContextSegment],
    strategy: ContextEngineStrategy,
    budget: ProviderContextBudget,
    context: &RequestContext,
    session_id: &str,
    summary_quality: Option<SummaryQualityGateExplain>,
) -> AssembledPrompt {
    let budget_tokens = budget.input_budget_tokens();
    let mut selected = segments
        .iter()
        .cloned()
        .enumerate()
        .map(|(order, segment)| IndexedContextSegment { order, segment })
        .collect::<Vec<_>>();
    let mut dropped = Vec::new();
    let mut selected_tokens =
        selected.iter().map(|entry| entry.segment.estimated_tokens).sum::<u64>();

    while selected_tokens > budget_tokens {
        let Some(drop_index) = selected
            .iter()
            .enumerate()
            .filter(|(_, entry)| !entry.segment.protected)
            .min_by(|(_, left), (_, right)| {
                left.segment
                    .priority
                    .cmp(&right.segment.priority)
                    .then_with(|| left.segment.stable.cmp(&right.segment.stable))
                    .then_with(|| {
                        left.segment.estimated_tokens.cmp(&right.segment.estimated_tokens).reverse()
                    })
                    .then_with(|| left.order.cmp(&right.order).reverse())
            })
            .map(|(index, _)| index)
        else {
            break;
        };

        let drop_group_id = selected[drop_index].segment.group_id.clone();
        let mut removed_indexes = selected
            .iter()
            .enumerate()
            .filter(|(_, entry)| {
                drop_group_id
                    .as_deref()
                    .is_some_and(|group_id| entry.segment.group_id.as_deref() == Some(group_id))
            })
            .map(|(index, _)| index)
            .collect::<Vec<_>>();
        if removed_indexes.is_empty() {
            removed_indexes.push(drop_index);
        }

        removed_indexes.sort_unstable();
        while let Some(index) = removed_indexes.pop() {
            let removed = selected.remove(index);
            selected_tokens = selected_tokens.saturating_sub(removed.segment.estimated_tokens);
            dropped.push(ContextEngineDroppedSegmentExplain {
                kind: removed.segment.kind,
                label: removed.segment.label,
                estimated_tokens: removed.segment.estimated_tokens,
                reason: if drop_group_id.is_some() {
                    "dropped_by_budget_group".to_owned()
                } else {
                    "dropped_by_budget".to_owned()
                },
            });
        }
    }

    selected.sort_by_key(|entry| entry.order);
    let prompt_text = selected
        .iter()
        .map(|entry| entry.segment.content.as_str())
        .collect::<Vec<_>>()
        .join("\n\n");
    let dropped_tokens = dropped.iter().map(|segment| segment.estimated_tokens).sum::<u64>();
    let stable_prefix = selected
        .iter()
        .take_while(|entry| entry.segment.stable)
        .map(|entry| entry.segment.clone())
        .collect::<Vec<_>>();
    let stable_prefix_tokens =
        stable_prefix.iter().map(|segment| segment.estimated_tokens).sum::<u64>();
    let stable_prefix_hash = (!stable_prefix.is_empty()).then(|| {
        let mut hasher = DefaultHasher::new();
        strategy.hash(&mut hasher);
        session_id.hash(&mut hasher);
        context.principal.hash(&mut hasher);
        context.channel.hash(&mut hasher);
        for segment in &stable_prefix {
            segment.kind.hash(&mut hasher);
            segment.content.hash(&mut hasher);
        }
        format!("{:016x}", hasher.finish())
    });
    let trust_scope =
        if selected.iter().any(|entry| entry.segment.trust_label != TrustLabel::TrustedLocal) {
            "mixed".to_owned()
        } else {
            "trusted".to_owned()
        };
    let cache_scope_key = stable_prefix_hash.as_ref().map(|hash| {
        format!(
            "session={session_id};principal={};channel={};strategy={};trust={trust_scope};prefix={hash}",
            context.principal,
            context.channel.as_deref().unwrap_or("none"),
            strategy.as_str(),
        )
    });

    AssembledPrompt {
        prompt_text,
        explain: ContextEngineExplain {
            strategy,
            rollout_enabled: true,
            budget: ContextEngineBudgetExplain {
                model_id: context_budget_model_id(session_id, budget),
                max_context_tokens: budget.max_context_tokens,
                reserved_completion_tokens: budget.reserved_completion_tokens,
                reserved_tool_result_tokens: budget.reserved_tool_result_tokens,
                provider_overhead_tokens: budget.provider_overhead_tokens,
                input_budget_tokens: budget_tokens,
                selected_tokens,
                dropped_tokens,
            },
            cache: ContextEngineCacheExplain {
                provider_cache_supported: budget.provider_cache_supported,
                stable_prefix_hash,
                stable_prefix_tokens,
                cache_scope_key,
                trust_scope,
            },
            summary_quality,
            selected_segments: selected
                .iter()
                .map(|entry| ContextEngineSegmentExplain {
                    kind: entry.segment.kind,
                    label: entry.segment.label.clone(),
                    estimated_tokens: entry.segment.estimated_tokens,
                    stable: entry.segment.stable,
                    protected: entry.segment.protected,
                    trust_label: entry.segment.trust_label,
                    safety_action: entry.segment.safety_action,
                    safety_findings: entry.segment.safety_findings.clone(),
                    group_id: entry.segment.group_id.clone(),
                    preview: preview_text(entry.segment.content.as_str(), SEGMENT_PREVIEW_CHARS),
                })
                .collect(),
            dropped_segments: dropped,
        },
    }
}

fn context_budget_model_id(session_id: &str, budget: ProviderContextBudget) -> String {
    format!("{session_id}:{}", budget.max_context_tokens)
}

fn resolve_provider_context_budget(
    snapshot: &crate::model_provider::ProviderStatusSnapshot,
    model_id_hint: Option<&str>,
) -> ProviderContextBudget {
    let model_id = model_id_hint
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
        .or_else(|| snapshot.registry.default_chat_model_id.clone())
        .or_else(|| snapshot.model_id.clone());
    let max_context_tokens = model_id
        .as_ref()
        .and_then(|model_id| {
            snapshot
                .registry
                .models
                .iter()
                .find(|model| model.model_id == *model_id && model.enabled)
                .and_then(|model| model.capabilities.max_context_tokens)
        })
        .or(snapshot.capabilities.max_context_tokens)
        .map(u64::from)
        .unwrap_or(DEFAULT_CONTEXT_WINDOW_TOKENS)
        .max(MIN_CONTEXT_WINDOW_TOKENS);
    let reserved_completion_tokens = (max_context_tokens / 5)
        .clamp(MIN_RESERVED_COMPLETION_TOKENS, MAX_RESERVED_COMPLETION_TOKENS);
    ProviderContextBudget {
        max_context_tokens,
        reserved_completion_tokens,
        reserved_tool_result_tokens: RESERVED_TOOL_RESULT_TOKENS,
        provider_overhead_tokens: PROVIDER_OVERHEAD_TOKENS,
        provider_cache_supported: snapshot.registry.response_cache_enabled,
    }
}

fn select_strategy(
    segments: &[ContextSegment],
    budget: ProviderContextBudget,
    summary_quality: Option<&SummaryQualityGateExplain>,
    checkpoint_summary_present: bool,
) -> ContextEngineStrategy {
    let selected_tokens = segments.iter().map(|segment| segment.estimated_tokens).sum::<u64>();
    let budget_pressure = selected_tokens > budget.input_budget_tokens();
    let has_compaction_summary =
        segments.iter().any(|segment| segment.kind == ContextSegmentKind::SessionCompactionSummary);
    if has_compaction_summary && budget_pressure {
        return ContextEngineStrategy::Summarizing;
    }
    if checkpoint_summary_present
        || summary_quality.is_some_and(|quality| quality.verdict == "fallback")
    {
        return ContextEngineStrategy::CheckpointAware;
    }
    if budget_pressure {
        return ContextEngineStrategy::CostAware;
    }
    if budget.provider_cache_supported && segments.iter().any(|segment| segment.stable) {
        return ContextEngineStrategy::ProviderAware;
    }
    ContextEngineStrategy::Noop
}

async fn collect_compaction_context_decision(
    runtime_state: &Arc<GatewayRuntimeState>,
    context: &RequestContext,
    run_id: &str,
    tape_seq: &mut i64,
    session_id: &str,
    budget: ProviderContextBudget,
) -> Result<CompactionContextDecision, Status> {
    let latest_artifact = resolve_latest_session_compaction_artifact(
        runtime_state,
        context,
        run_id,
        tape_seq,
        session_id,
    )
    .await?;
    let checkpoints = runtime_state.list_orchestrator_checkpoints(session_id.to_owned()).await?;
    let Some(artifact) = latest_artifact else {
        return Ok(CompactionContextDecision {
            segment: latest_checkpoint_segment(checkpoints.as_slice(), None),
            summary_quality: None,
            checkpoint_summary_present: !checkpoints.is_empty(),
        });
    };
    let quality = evaluate_summary_quality(
        &artifact,
        checkpoints.as_slice(),
        runtime_state.list_orchestrator_compaction_artifacts(session_id.to_owned()).await?.len(),
        budget,
    );
    let checkpoint_segment = latest_checkpoint_segment(checkpoints.as_slice(), Some(&artifact));
    let segment = match quality.verdict.as_str() {
        "allow" => Some(ContextSegment::trusted(
            ContextSegmentKind::SessionCompactionSummary,
            "session_compaction_summary",
            crate::application::session_compaction::render_compaction_prompt_block(
                artifact.artifact_id.as_str(),
                artifact.mode.as_str(),
                artifact.trigger_reason.as_str(),
                artifact.summary_text.as_str(),
            ),
            82,
            true,
            false,
            None,
        )),
        "fallback" | "reject" => checkpoint_segment,
        _ => None,
    };
    Ok(CompactionContextDecision {
        checkpoint_summary_present: segment
            .as_ref()
            .is_some_and(|segment| segment.kind == ContextSegmentKind::CheckpointSummary),
        segment,
        summary_quality: Some(quality),
    })
}

fn latest_checkpoint_segment(
    checkpoints: &[OrchestratorCheckpointRecord],
    artifact: Option<&OrchestratorCompactionArtifactRecord>,
) -> Option<ContextSegment> {
    let checkpoint = checkpoints.first()?;
    let workspace_paths =
        serde_json::from_str::<Vec<String>>(checkpoint.workspace_paths_json.as_str())
            .unwrap_or_default();
    let related_ids =
        serde_json::from_str::<Vec<String>>(checkpoint.referenced_compaction_ids_json.as_str())
            .unwrap_or_default();
    let mut block = format!(
        "<session_checkpoint checkpoint_id=\"{}\" name=\"{}\">\n",
        checkpoint.checkpoint_id, checkpoint.name
    );
    if let Some(note) = checkpoint.note.as_deref().filter(|value| !value.trim().is_empty()) {
        block.push_str("note=");
        block.push_str(note.trim());
        block.push('\n');
    }
    if let Some(artifact) = artifact {
        block.push_str(format!("artifact_id={}\n", artifact.artifact_id).as_str());
        block.push_str(format!("artifact_preview={}\n", artifact.summary_preview.trim()).as_str());
    }
    if !related_ids.is_empty() {
        block.push_str(format!("related_compactions={}\n", related_ids.join(",")).as_str());
    }
    if !workspace_paths.is_empty() {
        block.push_str(format!("workspace_paths={}\n", workspace_paths.join(",")).as_str());
    }
    block.push_str("</session_checkpoint>");
    Some(ContextSegment::trusted(
        ContextSegmentKind::CheckpointSummary,
        "checkpoint_summary",
        block,
        80,
        true,
        false,
        None,
    ))
}

fn evaluate_summary_quality(
    artifact: &OrchestratorCompactionArtifactRecord,
    checkpoints: &[OrchestratorCheckpointRecord],
    artifact_depth: usize,
    budget: ProviderContextBudget,
) -> SummaryQualityGateExplain {
    let summary_value = serde_json::from_str::<serde_json::Value>(artifact.summary_json.as_str())
        .unwrap_or_default();
    let review_required = summary_value
        .pointer("/planner/review_candidate_count")
        .and_then(serde_json::Value::as_u64)
        .unwrap_or_default();
    let poisoned = summary_value
        .pointer("/quality_gates/poisoned_candidate_count")
        .and_then(serde_json::Value::as_u64)
        .unwrap_or_default();
    let sensitive = summary_value
        .pointer("/quality_gates/sensitive_candidate_count")
        .and_then(serde_json::Value::as_u64)
        .unwrap_or_default();
    let contradiction_signals = count_contradiction_signals(artifact.summary_text.as_str());
    let low_information_summary =
        artifact.condensed_event_count >= 6 && artifact.summary_text.lines().count() <= 3;
    let budget_pressure = artifact.estimated_output_tokens > budget.input_budget_tokens();
    let mut reasons = Vec::new();
    let verdict = if poisoned > 0 || contradiction_signals > 0 {
        if poisoned > 0 {
            reasons.push("summary_contains_poisoned_candidates".to_owned());
        }
        if contradiction_signals > 0 {
            reasons.push("summary_contradiction_signal_detected".to_owned());
        }
        "reject"
    } else if low_information_summary
        || review_required > 0
        || sensitive > 0
        || artifact_depth > 2
        || budget_pressure
    {
        if low_information_summary {
            reasons.push("summary_coverage_too_shallow".to_owned());
        }
        if review_required > 0 {
            reasons.push("summary_requires_manual_review".to_owned());
        }
        if sensitive > 0 {
            reasons.push("summary_contains_sensitive_candidates".to_owned());
        }
        if artifact_depth > 2 {
            reasons.push("summary_drift_risk_from_repeated_compaction".to_owned());
        }
        if budget_pressure {
            reasons.push("summary_output_exceeds_input_budget".to_owned());
        }
        "fallback"
    } else {
        if checkpoints.is_empty() {
            reasons.push("no_checkpoint_fallback_available".to_owned());
        }
        "allow"
    };
    SummaryQualityGateExplain {
        verdict: verdict.to_owned(),
        repeated_compaction_depth: artifact_depth,
        contradiction_signals,
        reasons,
    }
}

fn count_contradiction_signals(summary_text: &str) -> usize {
    const CONTRADICTION_PAIRS: &[(&str, &str)] = &[
        ("enable", "disable"),
        ("allow", "deny"),
        ("must", "must not"),
        ("use", "avoid"),
        ("keep", "remove"),
        ("remote", "local"),
        ("public", "private"),
    ];
    let lowered = summary_text.to_ascii_lowercase();
    CONTRADICTION_PAIRS
        .iter()
        .filter(|(left, right)| lowered.contains(left) && lowered.contains(right))
        .count()
}

#[allow(clippy::result_large_err)]
async fn build_context_reference_segment(
    runtime_state: &Arc<GatewayRuntimeState>,
    run_id: &str,
    tape_seq: &mut i64,
    parameter_delta_json: Option<&str>,
) -> Result<Option<ContextSegment>, Status> {
    let preview = parse_context_reference_preview(parameter_delta_json);
    let Some(preview) = preview else {
        return Ok(None);
    };
    if preview.references.is_empty() {
        return Ok(None);
    }

    runtime_state
        .append_orchestrator_tape_event(OrchestratorTapeAppendRequest {
            run_id: run_id.to_owned(),
            seq: *tape_seq,
            event_type: "context_references".to_owned(),
            payload_json: json!({
                "clean_prompt": preview.clean_prompt,
                "total_estimated_tokens": preview.total_estimated_tokens,
                "trust_label": preview.trust_label.as_str(),
                "safety_action": preview.safety_action.as_str(),
                "safety_findings": preview.safety_findings,
                "warnings": preview.warnings,
                "errors": preview.errors,
                "references": preview.references.iter().map(|reference| {
                    json!({
                        "reference_id": reference.reference_id,
                        "kind": reference.kind.as_str(),
                        "target": reference.display_target,
                        "estimated_tokens": reference.estimated_tokens,
                        "trust_label": reference.trust_label.as_str(),
                        "safety_action": reference.safety_action.as_str(),
                        "safety_findings": reference.safety_findings,
                        "warnings": reference.warnings,
                        "provenance": reference.provenance,
                    })
                }).collect::<Vec<_>>(),
            })
            .to_string(),
        })
        .await?;
    *tape_seq = tape_seq.saturating_add(1);
    let Some(rendered_block) = render_context_reference_block(&preview) else {
        return Ok(None);
    };
    let transformed = transform_text_for_prompt(
        rendered_block.as_str(),
        SafetySourceKind::ContextReference,
        SafetyContentKind::ContextReference,
        preview.trust_label,
    );
    let mut safety_findings = preview.safety_findings;
    safety_findings.extend(transformed.scan.finding_codes());
    Ok(clean_segment_content(transformed.transformed_text).map(|content| {
        ContextSegment::trusted(
            ContextSegmentKind::ContextReferences,
            "context_references",
            content,
            96,
            false,
            true,
            None,
        )
        .with_safety(
            preview.trust_label,
            preview.safety_action.max(transformed.scan.recommended_action),
            safety_findings,
        )
    }))
}

fn parse_context_reference_preview(
    parameter_delta_json: Option<&str>,
) -> Option<ContextReferencePreviewEnvelope> {
    let raw = parameter_delta_json?.trim();
    if raw.is_empty() {
        return None;
    }
    serde_json::from_str::<ContextReferenceParameterDelta>(raw)
        .ok()
        .and_then(|value| value.context_references)
}

fn normalized_input_text(parameter_delta_json: Option<&str>, input_text: &str) -> String {
    parse_context_reference_preview(parameter_delta_json)
        .map(|preview| preview.clean_prompt.trim().to_owned())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| input_text.to_owned())
}

async fn record_context_engine_plan(
    runtime_state: &Arc<GatewayRuntimeState>,
    run_id: &str,
    tape_seq: &mut i64,
    explain: ContextEngineExplain,
) -> Result<(), Status> {
    runtime_state
        .append_orchestrator_tape_event(OrchestratorTapeAppendRequest {
            run_id: run_id.to_owned(),
            seq: *tape_seq,
            event_type: CONTEXT_ENGINE_PLAN_EVENT.to_owned(),
            payload_json: serde_json::to_string(&explain).map_err(|error| {
                Status::internal(format!("failed to serialize context plan: {error}"))
            })?,
        })
        .await?;
    *tape_seq = tape_seq.saturating_add(1);
    Ok(())
}

fn push_segment(segments: &mut Vec<ContextSegment>, segment: ContextSegment) {
    if segment.content.trim().is_empty() {
        return;
    }
    segments.push(segment);
}

fn clean_segment_content(raw: String) -> Option<String> {
    let trimmed = raw.trim();
    (!trimmed.is_empty()).then(|| trimmed.to_owned())
}

fn preview_text(raw: &str, max_chars: usize) -> String {
    let normalized = raw.replace(['\r', '\n'], " ");
    let trimmed = normalized.split_whitespace().collect::<Vec<_>>().join(" ");
    if trimmed.chars().count() <= max_chars {
        return trimmed;
    }
    let mut truncated = trimmed.chars().take(max_chars).collect::<String>();
    truncated.push_str("...");
    truncated
}

fn estimate_tokens(text: &str) -> u64 {
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return 0;
    }
    trimmed.chars().count().div_ceil(4) as u64
}

#[cfg(test)]
mod tests {
    use super::{
        assemble_segments, select_strategy, ContextEngineStrategy, ContextSegment,
        ContextSegmentKind, ProviderContextBudget, SummaryQualityGateExplain,
    };
    use crate::transport::grpc::auth::RequestContext;
    use palyra_safety::{SafetyAction, TrustLabel};
    use serde_json::json;

    fn segment(
        kind: ContextSegmentKind,
        label: &str,
        estimated_tokens: u64,
        priority: u8,
        stable: bool,
        protected: bool,
        group_id: Option<&str>,
    ) -> ContextSegment {
        let mut segment = ContextSegment::trusted(
            kind,
            label,
            label.to_owned(),
            priority,
            stable,
            protected,
            group_id.map(ToOwned::to_owned),
        );
        segment.estimated_tokens = estimated_tokens;
        segment
    }

    fn segment_with_safety(
        kind: ContextSegmentKind,
        label: &str,
        estimated_tokens: u64,
        priority: u8,
        stable: bool,
        protected: bool,
        group_id: Option<&str>,
        trust_label: TrustLabel,
        safety_action: SafetyAction,
        safety_findings: &[&str],
    ) -> ContextSegment {
        segment(kind, label, estimated_tokens, priority, stable, protected, group_id).with_safety(
            trust_label,
            safety_action,
            safety_findings.iter().map(|value| (*value).to_owned()).collect(),
        )
    }

    #[test]
    fn select_strategy_prefers_summarizing_when_budget_is_tight() {
        let strategy = select_strategy(
            &[segment(
                ContextSegmentKind::SessionCompactionSummary,
                "summary",
                4_000,
                80,
                true,
                false,
                None,
            )],
            ProviderContextBudget {
                max_context_tokens: 3_072,
                reserved_completion_tokens: 512,
                reserved_tool_result_tokens: 512,
                provider_overhead_tokens: 192,
                provider_cache_supported: true,
            },
            None,
            false,
        );
        assert_eq!(strategy, ContextEngineStrategy::Summarizing);
    }

    #[test]
    fn select_strategy_falls_back_to_checkpoint_aware_when_summary_is_unsafe() {
        let strategy = select_strategy(
            &[segment(
                ContextSegmentKind::CheckpointSummary,
                "checkpoint",
                320,
                80,
                true,
                false,
                None,
            )],
            ProviderContextBudget {
                max_context_tokens: 8_192,
                reserved_completion_tokens: 1_024,
                reserved_tool_result_tokens: 512,
                provider_overhead_tokens: 192,
                provider_cache_supported: false,
            },
            Some(&SummaryQualityGateExplain {
                verdict: "fallback".to_owned(),
                repeated_compaction_depth: 3,
                contradiction_signals: 0,
                reasons: vec!["summary_drift_risk_from_repeated_compaction".to_owned()],
            }),
            true,
        );
        assert_eq!(strategy, ContextEngineStrategy::CheckpointAware);
    }

    #[test]
    fn assembly_drops_low_priority_segments_before_protected_segments() {
        let assembled = assemble_segments(
            &[
                segment(ContextSegmentKind::PreferenceContext, "stable", 240, 90, true, true, None),
                segment(ContextSegmentKind::ProjectContext, "project", 280, 70, true, false, None),
                segment(ContextSegmentKind::MemoryRecall, "memory", 640, 40, false, false, None),
                segment(ContextSegmentKind::UserInput, "question", 220, 100, false, true, None),
            ],
            ContextEngineStrategy::CostAware,
            ProviderContextBudget {
                max_context_tokens: 1_024,
                reserved_completion_tokens: 512,
                reserved_tool_result_tokens: 128,
                provider_overhead_tokens: 128,
                provider_cache_supported: true,
            },
            &RequestContext {
                principal: "user:ops".to_owned(),
                device_id: "device".to_owned(),
                channel: Some("cli".to_owned()),
            },
            "session-1",
            None,
        );
        assert!(
            !assembled.prompt_text.contains("memory"),
            "low-priority memory segment should be dropped first under pressure"
        );
        assert!(
            assembled.prompt_text.contains("question"),
            "protected user input must survive budgeting"
        );
    }

    #[test]
    fn explain_output_snapshot_is_stable_for_budgeted_segments() {
        let assembled = assemble_segments(
            &[
                segment(
                    ContextSegmentKind::PreferenceContext,
                    "stable policy",
                    64,
                    90,
                    true,
                    true,
                    None,
                ),
                segment_with_safety(
                    ContextSegmentKind::ContextReferences,
                    "focused files",
                    48,
                    95,
                    false,
                    true,
                    None,
                    TrustLabel::ExternalUntrusted,
                    SafetyAction::Annotate,
                    &["prompt_injection.ignore_previous_instructions"],
                ),
                segment(ContextSegmentKind::UserInput, "ship it", 24, 100, false, true, None),
            ],
            ContextEngineStrategy::ProviderAware,
            ProviderContextBudget {
                max_context_tokens: 4_096,
                reserved_completion_tokens: 768,
                reserved_tool_result_tokens: 256,
                provider_overhead_tokens: 128,
                provider_cache_supported: true,
            },
            &RequestContext {
                principal: "user:ops".to_owned(),
                device_id: "device".to_owned(),
                channel: Some("cli".to_owned()),
            },
            "session-1",
            None,
        );
        let actual = serde_json::to_value(&assembled.explain).expect("explain should serialize");
        assert_eq!(
            actual,
            json!({
                "strategy": "provider_aware",
                "rollout_enabled": true,
                "budget": {
                    "model_id": "session-1:4096",
                    "max_context_tokens": 4096,
                    "reserved_completion_tokens": 768,
                    "reserved_tool_result_tokens": 256,
                    "provider_overhead_tokens": 128,
                    "input_budget_tokens": 2944,
                    "selected_tokens": 136,
                    "dropped_tokens": 0
                },
                "cache": {
                    "provider_cache_supported": true,
                    "stable_prefix_hash": actual.pointer("/cache/stable_prefix_hash").cloned().expect("stable prefix hash should exist"),
                    "stable_prefix_tokens": 64,
                    "cache_scope_key": actual.pointer("/cache/cache_scope_key").cloned().expect("cache scope key should exist"),
                    "trust_scope": "mixed"
                },
                "summary_quality": null,
                "selected_segments": [
                    {
                        "kind": "preference_context",
                        "label": "stable policy",
                        "estimated_tokens": 64,
                        "stable": true,
                        "protected": true,
                        "trust_label": "trusted_local",
                        "safety_action": "allow",
                        "group_id": null,
                        "preview": "stable policy"
                    },
                    {
                        "kind": "context_references",
                        "label": "focused files",
                        "estimated_tokens": 48,
                        "stable": false,
                        "protected": true,
                        "trust_label": "external_untrusted",
                        "safety_action": "annotate",
                        "safety_findings": ["prompt_injection.ignore_previous_instructions"],
                        "group_id": null,
                        "preview": "focused files"
                    },
                    {
                        "kind": "user_input",
                        "label": "ship it",
                        "estimated_tokens": 24,
                        "stable": false,
                        "protected": true,
                        "trust_label": "trusted_local",
                        "safety_action": "allow",
                        "group_id": null,
                        "preview": "ship it"
                    }
                ],
                "dropped_segments": []
            })
        );
    }

    #[test]
    fn assembly_drops_grouped_segments_together_under_budget_pressure() {
        let assembled = assemble_segments(
            &[
                segment(
                    ContextSegmentKind::ToolExchange,
                    "tool_call",
                    520,
                    30,
                    false,
                    false,
                    Some("tool:1"),
                ),
                segment(
                    ContextSegmentKind::ToolExchange,
                    "tool_result",
                    560,
                    30,
                    false,
                    false,
                    Some("tool:1"),
                ),
                segment(ContextSegmentKind::UserInput, "question", 220, 100, false, true, None),
            ],
            ContextEngineStrategy::CheckpointAware,
            ProviderContextBudget {
                max_context_tokens: 1_024,
                reserved_completion_tokens: 512,
                reserved_tool_result_tokens: 128,
                provider_overhead_tokens: 128,
                provider_cache_supported: false,
            },
            &RequestContext {
                principal: "user:ops".to_owned(),
                device_id: "device".to_owned(),
                channel: Some("cli".to_owned()),
            },
            "session-2",
            None,
        );
        assert!(
            !assembled.prompt_text.contains("tool_call")
                && !assembled.prompt_text.contains("tool_result"),
            "grouped tool exchange segments should drop together when the pair no longer fits"
        );
        assert_eq!(assembled.explain.dropped_segments.len(), 2);
        assert!(
            assembled
                .explain
                .dropped_segments
                .iter()
                .all(|segment| segment.reason == "dropped_by_budget_group"),
            "grouped drops should explain that the whole group was removed"
        );
    }
}
