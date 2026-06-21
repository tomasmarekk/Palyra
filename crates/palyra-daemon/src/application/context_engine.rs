//! Context engine: assembles the model-provider prompt for a run turn from
//! prioritized, trust-labeled segments under an explicit token budget.
//!
//! Assembly model: every candidate block (compiled instructions, preference
//! and project context, compaction/checkpoint summaries, context references,
//! recall, session tail, user input) becomes a [`ContextSegment`] carrying a
//! priority, stability/protection flags, and a safety-scan result.
//! [`assemble_segments`] evicts the lowest-priority unprotected segments
//! (whole groups at a time) until the selection fits the provider input
//! budget derived in [`resolve_provider_context_budget`], then emits the
//! prompt text plus a deterministic [`ContextEngineExplain`] trace journaled
//! as [`CONTEXT_ENGINE_PLAN_EVENT`].
//!
//! Relationships: instruction text comes from
//! `application::instruction_compiler`, reference resolution from
//! `application::context_references`, and recall/summary blocks from
//! `application::provider_input`, whose `prepare_model_provider_input`
//! delegates here whenever the context-engine feature rollout is enabled.

use std::sync::Arc;

use palyra_safety::{
    transform_text_for_prompt, SafetyAction, SafetyContentKind, SafetySourceKind, TrustLabel,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tonic::Status;
use tracing::warn;

use crate::{
    application::{
        context_compression::{shrink_json_value, JsonShrinkConfig},
        context_references::{render_context_reference_block, ContextReferencePreviewEnvelope},
        instruction_compiler::{
            CompiledInstructions, InstructionCompiler, InstructionCompilerInput,
            InstructionTrustSummary,
        },
        learning::render_preference_prompt_context,
        provider_input::{
            build_attachment_recall_prompt, build_explicit_recall_prompt,
            build_memory_augmented_prompt, build_previous_run_context_prompt,
            build_previous_run_provider_messages, build_project_context_prompt,
            build_provider_image_inputs, parse_provider_reasoning_effort_override,
            parse_provider_service_tier_override, record_provider_pruning_decision,
            resolve_latest_session_compaction_artifact, MemoryPromptFailureMode,
            PrepareModelProviderInputRequest, PreparedModelProviderInput,
        },
        session_pruning::{
            classify_pruning_task, context_engine_pruning_outcome, detect_pruning_risk,
            pruning_decision_from_config,
        },
        tool_registry::{ModelVisibleToolCatalogSnapshot, ToolExposureSurface},
    },
    gateway::{ingest_memory_best_effort, GatewayRuntimeState},
    journal::{
        OrchestratorCheckpointRecord, OrchestratorCompactionArtifactRecord,
        OrchestratorTapeAppendRequest,
    },
    model_provider::ProviderMessageRole,
    transport::grpc::auth::RequestContext,
};

// Conservative fallbacks/reserves used when the provider registry does not
// advertise model capabilities; all values are estimated tokens, and the
// estimator deliberately overcounts so the real provider limit is never hit.
const DEFAULT_CONTEXT_WINDOW_TOKENS: u64 = 8_192;
const MIN_CONTEXT_WINDOW_TOKENS: u64 = 2_048;
const MAX_RESERVED_COMPLETION_TOKENS: u64 = 8_192;
const MIN_RESERVED_COMPLETION_TOKENS: u64 = 512;
const RESERVED_TOOL_RESULT_TOKENS: u64 = 512;
const PROVIDER_OVERHEAD_TOKENS: u64 = 192;
const CONTEXT_BUDGET_SAFETY_MARGIN_TOKENS: u64 = 256;
const TOOL_SCHEMA_BASE_OVERHEAD_TOKENS: u64 = 24;
const TOOL_SCHEMA_PER_TOOL_OVERHEAD_TOKENS: u64 = 12;
const SEGMENT_PREVIEW_CHARS: usize = 180;
/// Orchestrator tape event type under which the assembly trace is journaled.
pub(crate) const CONTEXT_ENGINE_PLAN_EVENT: &str = "context.engine.plan";
/// Schema version stamped into [`ContextEngineExplain`] and its trace hash.
pub(crate) const CONTEXT_ASSEMBLY_TRACE_SCHEMA_VERSION: u32 = 1;

/// High-level plan label chosen by [`select_strategy`] and surfaced in the
/// assembly trace; it describes why the prompt was shaped the way it was.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ContextEngineStrategy {
    /// Everything fits; no compression or cache shaping was needed.
    Noop,
    /// A checkpoint summary stands in for a rejected/fallback compaction.
    CheckpointAware,
    /// A compaction summary is carrying the session under budget pressure.
    Summarizing,
    /// Over budget without a summary; low-priority segments were dropped.
    CostAware,
    /// Under budget with a cacheable stable prefix on a caching provider.
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

/// What a prompt segment contains; drives ordering labels, source-kind
/// mapping, and the assembly-step grouping in the trace.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ContextSegmentKind {
    SystemInstructions,
    DeveloperInstructions,
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
    fn as_str(self) -> &'static str {
        match self {
            Self::SystemInstructions => "system_instructions",
            Self::DeveloperInstructions => "developer_instructions",
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

/// Alias kept so explain/trace consumers can name the trust label without
/// importing `palyra_safety` directly.
pub(crate) type ContextTrustLabel = TrustLabel;

/// Provenance bucket for a segment, derived from its kind by
/// [`source_kind_for_segment`]; serialized into the assembly trace.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ContextSourceKind {
    System,
    Developer,
    User,
    Workspace,
    Memory,
    Retrieval,
    Attachment,
    ToolResult,
}

/// Trace entry for one segment that survived budgeting. The `preview` field
/// is already shrunk/redacted; raw segment content never enters the trace.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct ContextEngineSegmentExplain {
    pub(crate) kind: ContextSegmentKind,
    pub(crate) source_kind: ContextSourceKind,
    pub(crate) label: String,
    pub(crate) estimated_tokens: u64,
    pub(crate) include_reason: String,
    pub(crate) redaction_status: String,
    pub(crate) stable: bool,
    pub(crate) protected: bool,
    pub(crate) trust_label: ContextTrustLabel,
    pub(crate) safety_action: SafetyAction,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub(crate) safety_findings: Vec<String>,
    pub(crate) group_id: Option<String>,
    #[serde(default)]
    pub(crate) source_refs: Vec<String>,
    pub(crate) preview: String,
}

/// Trace entry for a segment evicted by budgeting (metadata only, no
/// content); `reason` distinguishes single drops from group drops.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct ContextEngineDroppedSegmentExplain {
    pub(crate) kind: ContextSegmentKind,
    pub(crate) label: String,
    pub(crate) estimated_tokens: u64,
    pub(crate) reason: String,
}

/// Flattened included/excluded view of the assembly pipeline, one entry per
/// segment, grouped by the pipeline step that produced it.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct PromptAssemblyStepExplain {
    pub(crate) step: String,
    pub(crate) label: String,
    pub(crate) included: bool,
    pub(crate) token_estimate: u64,
    pub(crate) include_reason: String,
    pub(crate) redaction_status: String,
    #[serde(default)]
    pub(crate) source_refs: Vec<String>,
}

/// Token accounting snapshot for the turn: the resolved budget profile, every
/// reserve subtracted from the context window, and the selected/dropped/
/// overflow totals after budgeting.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct ContextEngineBudgetExplain {
    pub(crate) profile_id: String,
    pub(crate) provider_id: String,
    pub(crate) provider_kind: String,
    pub(crate) model_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) failover_budget_model_id: Option<String>,
    pub(crate) max_context_tokens: u64,
    pub(crate) reserved_completion_tokens: u64,
    pub(crate) reserved_tool_result_tokens: u64,
    pub(crate) provider_overhead_tokens: u64,
    pub(crate) safety_margin_tokens: u64,
    pub(crate) tool_schema_overhead_tokens: u64,
    pub(crate) input_budget_tokens: u64,
    pub(crate) selected_tokens: u64,
    pub(crate) dropped_tokens: u64,
    pub(crate) overflow_tokens: u64,
}

/// Provider prompt-cache view: hash and size of the stable segment prefix
/// plus the identity-bearing scope key (redacted to a hash in diagnostics).
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

/// Outcome of the compaction-summary quality gate; `verdict` is `allow`,
/// `fallback` (use checkpoint instead), or `reject` (drop the summary).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct SummaryQualityGateExplain {
    pub(crate) verdict: String,
    pub(crate) repeated_compaction_depth: usize,
    pub(crate) contradiction_signals: usize,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub(crate) reasons: Vec<String>,
}

/// Identity of the compiled instruction set used for the turn (version,
/// content hash, and the provider/model/surface it was compiled for).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct ContextEngineInstructionExplain {
    pub(crate) version: u32,
    pub(crate) hash: String,
    pub(crate) provider_kind: String,
    pub(crate) model_family: String,
    pub(crate) surface: ToolExposureSurface,
}

/// Full deterministic trace of one prompt assembly: strategy, budget and
/// cache accounting, quality-gate verdicts, and per-segment include/drop
/// decisions. Serialized verbatim into the [`CONTEXT_ENGINE_PLAN_EVENT`]
/// tape event; its serde shape is pinned by snapshot tests.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct ContextEngineExplain {
    pub(crate) schema_version: u32,
    pub(crate) trace_id: String,
    pub(crate) strategy: ContextEngineStrategy,
    pub(crate) rollout_enabled: bool,
    pub(crate) budget: ContextEngineBudgetExplain,
    pub(crate) cache: ContextEngineCacheExplain,
    pub(crate) summary_quality: Option<SummaryQualityGateExplain>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) instruction: Option<ContextEngineInstructionExplain>,
    pub(crate) reason_codes: Vec<String>,
    pub(crate) assembly_steps: Vec<PromptAssemblyStepExplain>,
    pub(crate) selected_segments: Vec<ContextEngineSegmentExplain>,
    pub(crate) dropped_segments: Vec<ContextEngineDroppedSegmentExplain>,
}

/// Alias used where the explain payload is consumed as a journaled trace
/// rather than as a live planning result.
pub(crate) type ContextAssemblyTrace = ContextEngineExplain;

/// One candidate prompt block before budgeting. `priority` decides eviction
/// order (higher survives longer), `stable` marks cache-prefix-eligible
/// content, `protected` exempts the segment from eviction entirely, and
/// `group_id` ties segments that must be dropped as a unit (for example a
/// tool call and its result).
#[derive(Debug, Clone)]
struct ContextSegment {
    kind: ContextSegmentKind,
    label: String,
    content: String,
    provider_role: Option<ProviderMessageRole>,
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
            provider_role: None,
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

    fn instruction(
        kind: ContextSegmentKind,
        label: impl Into<String>,
        content: String,
        provider_role: ProviderMessageRole,
        estimated_tokens: u64,
    ) -> Self {
        Self {
            kind,
            label: label.into(),
            content,
            provider_role: Some(provider_role),
            estimated_tokens,
            // Compiled instructions outrank everything except the live user
            // input (priority 100) and can never be evicted; the shared group
            // id keeps system/developer segments traceable as one unit.
            priority: 99,
            stable: true,
            protected: true,
            group_id: Some("instruction_compiler:v1".to_owned()),
            trust_label: TrustLabel::TrustedLocal,
            safety_action: SafetyAction::Allow,
            safety_findings: Vec::new(),
        }
    }
}

/// Identity-bearing summary of the budget inputs for one turn. `profile_id`
/// is a content hash of the other fields, so identical provider/model/limit
/// combinations always produce the same id in traces and journals.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct ProviderBudgetProfile {
    pub(crate) profile_id: String,
    pub(crate) provider_id: String,
    pub(crate) provider_kind: String,
    pub(crate) model_id: String,
    pub(crate) context_window_tokens: u64,
    pub(crate) max_output_tokens: u64,
    pub(crate) safety_margin_tokens: u64,
    pub(crate) tool_schema_overhead_tokens: u64,
    pub(crate) provider_cache_supported: bool,
    pub(crate) failover_policy: String,
    pub(crate) failover_budget_model_id: Option<String>,
}

/// Working budget for one assembly: the context window plus every reserve
/// that must be subtracted before prompt segments may claim tokens.
#[derive(Debug, Clone)]
struct ProviderContextBudget {
    profile: ProviderBudgetProfile,
    max_context_tokens: u64,
    reserved_completion_tokens: u64,
    reserved_tool_result_tokens: u64,
    provider_overhead_tokens: u64,
    safety_margin_tokens: u64,
    tool_schema_overhead_tokens: u64,
    provider_cache_supported: bool,
}

impl ProviderContextBudget {
    /// Tokens available to prompt segments after all reserves. Clamped to at
    /// least 1 so budgeting code never divides by or loops on a zero budget
    /// even when reserves exceed a tiny context window.
    fn input_budget_tokens(&self) -> u64 {
        self.max_context_tokens
            .saturating_sub(self.reserved_completion_tokens)
            .saturating_sub(self.reserved_tool_result_tokens)
            .saturating_sub(self.provider_overhead_tokens)
            .saturating_sub(self.safety_margin_tokens)
            .saturating_sub(self.tool_schema_overhead_tokens)
            .max(1)
    }
}

/// Resolution of the session-summary question for this turn: at most one
/// summary segment (compaction or checkpoint fallback), the quality-gate
/// verdict that picked it, and whether the pick was a checkpoint.
#[derive(Debug, Clone)]
struct CompactionContextDecision {
    segment: Option<ContextSegment>,
    summary_quality: Option<SummaryQualityGateExplain>,
    checkpoint_summary_present: bool,
}

/// Minimal view of the run parameter delta: only the optional
/// `context_references` preview is read here; all other keys are ignored.
#[derive(Debug, Clone, Deserialize)]
struct ContextReferenceParameterDelta {
    #[serde(default)]
    context_references: Option<ContextReferencePreviewEnvelope>,
}

/// Assembles the full model-provider input for one run turn using the
/// context engine: collects candidate segments, compiles instructions,
/// budgets and orders them, journals the assembly trace, and records the
/// pruning decision.
///
/// Mutates `tape_seq` for every orchestrator tape event appended on the way
/// (context references, assembly plan).
///
/// # Errors
/// Returns `Status::resource_exhausted("context_budget_exhausted")` when even
/// after eviction the protected segments exceed the input budget, and
/// propagates journal, memory, recall, and tape-append failures. Memory
/// recall failures are downgraded to a warning when the caller selected
/// [`MemoryPromptFailureMode::FallbackToRawInput`].
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
        provider_kind_hint,
        provider_model_id_hint,
        tool_catalog_snapshot,
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

    let provider_budget = resolve_provider_context_budget(
        &runtime_state.model_provider_status_snapshot(),
        provider_kind_hint,
        provider_model_id_hint,
        tool_catalog_snapshot,
    );
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
        let transformed = transform_text_for_prompt(
            project_context_block.as_str(),
            SafetySourceKind::Workspace,
            SafetyContentKind::WorkspaceDocument,
            TrustLabel::TrustedLocal,
        );
        push_segment(
            &mut segments,
            ContextSegment::trusted(
                ContextSegmentKind::ProjectContext,
                "project_context",
                transformed.transformed_text,
                86,
                true,
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

    let compaction_decision = collect_compaction_context_decision(
        runtime_state,
        context,
        run_id,
        tape_seq,
        session_id,
        provider_budget.clone(),
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
    let explicit_recall_present = explicit_recall_block.is_some();
    if let Some(block) = explicit_recall_block {
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

    // Automatic memory injection is suppressed when the user explicitly
    // recalled memory: the explicit block already covers the same need at a
    // higher priority and auto-injection would duplicate content.
    if !explicit_recall_present {
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
            normalized_input_text,
            100,
            false,
            true,
            None,
        ),
    );

    // Instructions are compiled only after every other segment exists: the
    // developer message embeds a trust summary of the actually selected
    // blocks. They are then prepended so the stable instruction prefix leads
    // the prompt, which is what makes provider prompt caching effective.
    let compiled_instructions = InstructionCompiler.compile(InstructionCompilerInput {
        provider_kind: provider_budget.profile.provider_kind.as_str(),
        model_family: provider_budget.profile.model_id.as_str(),
        surface: tool_catalog_snapshot
            .map(|snapshot| snapshot.surface)
            .unwrap_or(ToolExposureSurface::RunStream),
        tool_catalog: tool_catalog_snapshot,
        approval_mode: "policy_gate",
        trust_summary: instruction_trust_summary(segments.as_slice()),
    });
    let mut ordered_segments = instruction_segments(&compiled_instructions);
    ordered_segments.append(&mut segments);
    segments = ordered_segments;

    let strategy = select_strategy(
        segments.as_slice(),
        provider_budget.clone(),
        compaction_decision.summary_quality.as_ref(),
        compaction_decision.checkpoint_summary_present,
    );
    let mut assembled = assemble_segments(
        segments.as_slice(),
        strategy,
        provider_budget.clone(),
        context,
        session_id,
        compaction_decision.summary_quality.clone(),
    );
    assembled.explain.instruction = Some(ContextEngineInstructionExplain {
        version: compiled_instructions.version,
        hash: compiled_instructions.hash.clone(),
        provider_kind: compiled_instructions.provider_kind.clone(),
        model_family: compiled_instructions.model_family.clone(),
        surface: compiled_instructions.surface,
    });

    record_context_engine_plan(runtime_state, run_id, tape_seq, assembled.explain.clone()).await?;
    if assembled.explain.budget.overflow_tokens > 0 {
        return Err(Status::resource_exhausted("context_budget_exhausted"));
    }
    let pruning_task_class = classify_pruning_task(memory_ingest_reason, parameter_delta_json);
    let pruning_risk_level = detect_pruning_risk(assembled.prompt_text.as_str());
    let pruning_decision = pruning_decision_from_config(
        &runtime_state.config.pruning_policy_matrix,
        pruning_task_class,
        pruning_risk_level,
    );
    if let Some(pruning_outcome) = context_engine_pruning_outcome(
        &pruning_decision,
        assembled.explain.budget.selected_tokens,
        assembled.explain.budget.dropped_tokens,
        serde_json::to_value(&assembled.explain.dropped_segments).unwrap_or_else(|_| json!([])),
    ) {
        record_provider_pruning_decision(
            runtime_state,
            context,
            run_id,
            tape_seq,
            session_id,
            &pruning_outcome,
        )
        .await?;
    }

    let mut provider_messages = compiled_instructions.provider_messages();
    provider_messages
        .extend(build_previous_run_provider_messages(runtime_state, previous_run_id).await?);

    Ok(PreparedModelProviderInput {
        provider_input_text: assembled.prompt_text,
        provider_messages,
        vision_inputs,
        instruction_hash: Some(compiled_instructions.hash),
        context_trace_id: Some(assembled.explain.trace_id),
        budget_profile: Some(assembled.explain.budget.profile_id),
        max_output_tokens: Some(assembled.explain.budget.reserved_completion_tokens),
        reasoning_effort: parse_provider_reasoning_effort_override(parameter_delta_json)?,
        service_tier: parse_provider_service_tier_override(parameter_delta_json)?,
    })
}

/// Final budgeted prompt text plus the trace explaining how it was built.
#[derive(Debug)]
struct AssembledPrompt {
    prompt_text: String,
    explain: ContextEngineExplain,
}

/// Segment paired with its original insertion index so the prompt can be
/// re-emitted in pipeline order after eviction shuffles the working set.
#[derive(Debug, Clone)]
struct IndexedContextSegment {
    order: usize,
    segment: ContextSegment,
}

/// Evicts unprotected segments until the selection fits the input budget,
/// then renders the surviving segments (in original order) and the full
/// deterministic explain trace.
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
        // Eviction order: lowest priority first; among equals prefer
        // unstable segments (preserves the cacheable prefix), then the
        // largest segment (frees the most budget per drop), then the most
        // recently appended one. If only protected segments remain the loop
        // exits and the residual is reported as overflow.
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

        // Grouped unprotected segments fall together so a tool call never
        // survives without its result. Protected members remain pinned even if
        // a malformed or future mixed group id links them to evictable content.
        let drop_group_id = selected[drop_index].segment.group_id.clone();
        let mut removed_indexes = selected
            .iter()
            .enumerate()
            .filter(|(_, entry)| {
                !entry.segment.protected
                    && drop_group_id
                        .as_deref()
                        .is_some_and(|group_id| entry.segment.group_id.as_deref() == Some(group_id))
            })
            .map(|(index, _)| index)
            .collect::<Vec<_>>();
        if removed_indexes.is_empty() {
            removed_indexes.push(drop_index);
        }

        // Remove from the back so earlier indexes stay valid.
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
    // Instruction segments (those with a provider role) are delivered as
    // structured provider messages, not inlined into the plain prompt text.
    let prompt_text = selected
        .iter()
        .filter(|entry| entry.segment.provider_role.is_none())
        .map(|entry| entry.segment.content.as_str())
        .collect::<Vec<_>>()
        .join("\n\n");
    let dropped_tokens = dropped.iter().map(|segment| segment.estimated_tokens).sum::<u64>();
    // The cacheable prefix is the run of stable segments at the head of the
    // prompt; the first unstable segment ends it.
    let stable_prefix = selected
        .iter()
        .take_while(|entry| entry.segment.stable)
        .map(|entry| &entry.segment)
        .collect::<Vec<_>>();
    let stable_prefix_tokens =
        stable_prefix.iter().map(|segment| segment.estimated_tokens).sum::<u64>();
    let stable_prefix_hash = (!stable_prefix.is_empty()).then(|| {
        stable_sha256_json(&json!({
            "schema_version": 1,
            "strategy": strategy.as_str(),
            "profile_id": budget.profile.profile_id.as_str(),
            "session_id": session_id,
            "principal": context.principal.as_str(),
            "channel": context.channel.as_deref(),
            "segments": stable_prefix.iter().map(|segment| {
                json!({
                    "kind": segment.kind.as_str(),
                    "label": segment.label.as_str(),
                    "content": segment.content.as_str(),
                    "trust_label": segment.trust_label.as_str(),
                    "safety_action": segment.safety_action.as_str(),
                    "stable": segment.stable,
                })
            }).collect::<Vec<_>>(),
        }))
    });
    let trust_scope =
        if selected.iter().any(|entry| entry.segment.trust_label != TrustLabel::TrustedLocal) {
            "mixed".to_owned()
        } else {
            "trusted".to_owned()
        };
    // The cache scope key binds the prefix hash to session, principal,
    // channel, and trust posture so a provider cache entry can never be
    // shared across identities or across trusted/mixed prompts.
    let cache_scope_key = stable_prefix_hash.as_ref().map(|hash| {
        format!(
            "session={session_id};principal={};channel={};strategy={};trust={trust_scope};prefix={hash}",
            context.principal,
            context.channel.as_deref().unwrap_or("none"),
            strategy.as_str(),
        )
    });

    let selected_segment_explain = selected
        .iter()
        .map(|entry| {
            let preview = explain_preview_for_segment(&entry.segment);
            ContextEngineSegmentExplain {
                kind: entry.segment.kind,
                source_kind: source_kind_for_segment(&entry.segment),
                label: entry.segment.label.clone(),
                estimated_tokens: entry.segment.estimated_tokens,
                include_reason: include_reason_for_segment(&entry.segment),
                redaction_status: preview.redaction_status,
                stable: entry.segment.stable,
                protected: entry.segment.protected,
                trust_label: entry.segment.trust_label,
                safety_action: entry.segment.safety_action,
                safety_findings: entry.segment.safety_findings.clone(),
                group_id: entry.segment.group_id.clone(),
                source_refs: source_refs_for_segment(&entry.segment),
                preview: preview.text,
            }
        })
        .collect::<Vec<_>>();
    let assembly_steps = build_prompt_assembly_steps(
        selected.as_slice(),
        dropped.as_slice(),
        selected_segment_explain.as_slice(),
    );
    let overflow_tokens = selected_tokens.saturating_sub(budget_tokens);
    let trace_id = stable_sha256_json(&json!({
        "schema_version": CONTEXT_ASSEMBLY_TRACE_SCHEMA_VERSION,
        "session_id": session_id,
        "profile_id": budget.profile.profile_id.as_str(),
        "strategy": strategy.as_str(),
        "selected": selected_segment_explain.iter().map(|segment| {
            json!({
                "kind": segment.kind.as_str(),
                "source_kind": segment.source_kind,
                "label": segment.label.as_str(),
                "estimated_tokens": segment.estimated_tokens,
                "stable": segment.stable,
                "trust_label": segment.trust_label.as_str(),
                "safety_action": segment.safety_action.as_str(),
                "safety_findings": segment.safety_findings.as_slice(),
            })
        }).collect::<Vec<_>>(),
        "dropped": dropped.iter().map(|segment| {
            json!({
                "kind": segment.kind.as_str(),
                "label": segment.label.as_str(),
                "estimated_tokens": segment.estimated_tokens,
                "reason": segment.reason.as_str(),
            })
        }).collect::<Vec<_>>(),
    }));
    let mut reason_codes = context_assembly_reason_codes(
        strategy,
        selected_segment_explain.as_slice(),
        dropped.as_slice(),
        overflow_tokens,
        summary_quality.as_ref(),
    );
    if budget.profile.failover_budget_model_id.is_some() {
        reason_codes.push("failover_budget_constrained".to_owned());
        reason_codes.sort();
        reason_codes.dedup();
    }

    AssembledPrompt {
        prompt_text,
        explain: ContextEngineExplain {
            schema_version: CONTEXT_ASSEMBLY_TRACE_SCHEMA_VERSION,
            trace_id: format!("ctx_{}", &trace_id[..16]),
            strategy,
            rollout_enabled: true,
            budget: ContextEngineBudgetExplain {
                profile_id: budget.profile.profile_id.clone(),
                provider_id: budget.profile.provider_id.clone(),
                provider_kind: budget.profile.provider_kind.clone(),
                model_id: budget.profile.model_id.clone(),
                failover_budget_model_id: budget.profile.failover_budget_model_id.clone(),
                max_context_tokens: budget.max_context_tokens,
                reserved_completion_tokens: budget.reserved_completion_tokens,
                reserved_tool_result_tokens: budget.reserved_tool_result_tokens,
                provider_overhead_tokens: budget.provider_overhead_tokens,
                safety_margin_tokens: budget.safety_margin_tokens,
                tool_schema_overhead_tokens: budget.tool_schema_overhead_tokens,
                input_budget_tokens: budget_tokens,
                selected_tokens,
                dropped_tokens,
                overflow_tokens,
            },
            cache: ContextEngineCacheExplain {
                provider_cache_supported: budget.provider_cache_supported,
                stable_prefix_hash,
                stable_prefix_tokens,
                cache_scope_key,
                trust_scope,
            },
            summary_quality,
            instruction: None,
            reason_codes,
            assembly_steps,
            selected_segments: selected_segment_explain,
            dropped_segments: dropped,
        },
    }
}

/// Derives the token budget for a turn from the provider registry snapshot.
///
/// Resolution precedence for the model: explicit hint, registry default chat
/// model, then the snapshot's active model. The context window may be shrunk
/// to the smallest credible failover model so an assembled prompt still fits
/// after a mid-run provider failover.
fn resolve_provider_context_budget(
    snapshot: &crate::model_provider::ProviderStatusSnapshot,
    provider_kind_hint: Option<&str>,
    model_id_hint: Option<&str>,
    tool_catalog_snapshot: Option<&ModelVisibleToolCatalogSnapshot>,
) -> ProviderContextBudget {
    let model_id = model_id_hint
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
        .or_else(|| snapshot.registry.default_chat_model_id.clone())
        .or_else(|| snapshot.model_id.clone());
    let model = model_id.as_ref().and_then(|model_id| {
        snapshot.registry.models.iter().find(|model| model.model_id == *model_id && model.enabled)
    });
    let provider_id = model
        .map(|model| model.provider_id.clone())
        .unwrap_or_else(|| snapshot.provider_id.clone());
    let provider_kind = provider_kind_hint
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
        .or_else(|| {
            snapshot
                .registry
                .providers
                .iter()
                .find(|provider| provider.provider_id == provider_id)
                .map(|provider| provider.kind.clone())
        })
        .unwrap_or_else(|| snapshot.kind.clone());
    let model_id = model_id.unwrap_or_else(|| "unknown".to_owned());
    let selected_context_tokens = model_context_window_tokens(model, snapshot);
    let failover_budget_constraint =
        failover_context_budget_constraint(snapshot, model_id.as_str(), selected_context_tokens);
    let max_context_tokens = failover_budget_constraint
        .as_ref()
        .map(|constraint| constraint.context_window_tokens)
        .unwrap_or(selected_context_tokens);
    let failover_budget_model_id =
        failover_budget_constraint.as_ref().map(|constraint| constraint.model_id.clone());
    // Reserve roughly 20% of the window for the completion, bounded so tiny
    // windows still leave room to answer and huge windows do not over-reserve.
    let reserved_completion_tokens = (max_context_tokens / 5)
        .clamp(MIN_RESERVED_COMPLETION_TOKENS, MAX_RESERVED_COMPLETION_TOKENS);
    let tool_schema_overhead_tokens = estimate_tool_schema_overhead_tokens(tool_catalog_snapshot);
    let provider_cache_supported = snapshot.registry.response_cache_enabled;
    let failover_policy = if snapshot.registry.failover_enabled {
        "registry_failover_enabled"
    } else {
        "registry_failover_disabled"
    }
    .to_owned();
    let profile_payload = json!({
        "schema_version": 1,
        "provider_id": provider_id.as_str(),
        "provider_kind": provider_kind.as_str(),
        "model_id": model_id.as_str(),
        "context_window_tokens": max_context_tokens,
        "max_output_tokens": reserved_completion_tokens,
        "safety_margin_tokens": CONTEXT_BUDGET_SAFETY_MARGIN_TOKENS,
        "tool_schema_overhead_tokens": tool_schema_overhead_tokens,
        "provider_cache_supported": provider_cache_supported,
        "failover_policy": failover_policy.as_str(),
        "failover_budget_model_id": failover_budget_model_id.as_deref(),
    });
    let profile_hash = stable_sha256_json(&profile_payload);
    let profile = ProviderBudgetProfile {
        profile_id: format!("budget_{}", &profile_hash[..16]),
        provider_id,
        provider_kind,
        model_id,
        context_window_tokens: max_context_tokens,
        max_output_tokens: reserved_completion_tokens,
        safety_margin_tokens: CONTEXT_BUDGET_SAFETY_MARGIN_TOKENS,
        tool_schema_overhead_tokens,
        provider_cache_supported,
        failover_policy,
        failover_budget_model_id,
    };
    ProviderContextBudget {
        profile,
        max_context_tokens,
        reserved_completion_tokens,
        reserved_tool_result_tokens: RESERVED_TOOL_RESULT_TOKENS,
        provider_overhead_tokens: PROVIDER_OVERHEAD_TOKENS,
        safety_margin_tokens: CONTEXT_BUDGET_SAFETY_MARGIN_TOKENS,
        tool_schema_overhead_tokens,
        provider_cache_supported,
    }
}

/// Failover target whose smaller context window caps this turn's budget.
#[derive(Debug, Clone, PartialEq, Eq)]
struct FailoverBudgetConstraint {
    model_id: String,
    context_window_tokens: u64,
}

/// Context window for a model, falling back to the provider snapshot and
/// then the conservative default; never below the configured minimum.
fn model_context_window_tokens(
    model: Option<&crate::model_provider::ProviderRegistryModelSnapshot>,
    snapshot: &crate::model_provider::ProviderStatusSnapshot,
) -> u64 {
    model
        .and_then(|model| model.capabilities.max_context_tokens)
        .or(snapshot.capabilities.max_context_tokens)
        .map(u64::from)
        .unwrap_or(DEFAULT_CONTEXT_WINDOW_TOKENS)
        .max(MIN_CONTEXT_WINDOW_TOKENS)
}

/// Finds the enabled chat model with the smallest context window that is
/// both different from the selected model and smaller than it. Budgeting to
/// that worst-case target guarantees the assembled prompt survives a
/// registry failover without re-assembly. Returns `None` when failover is
/// disabled or no alternative model exists.
fn failover_context_budget_constraint(
    snapshot: &crate::model_provider::ProviderStatusSnapshot,
    selected_model_id: &str,
    selected_context_tokens: u64,
) -> Option<FailoverBudgetConstraint> {
    if !snapshot.registry.failover_enabled {
        return None;
    }
    let mut candidates = snapshot
        .registry
        .models
        .iter()
        .filter(|model| {
            model.enabled
                && model.role.eq_ignore_ascii_case("chat")
                && registry_provider_enabled(snapshot, model.provider_id.as_str())
        })
        .map(|model| FailoverBudgetConstraint {
            model_id: model.model_id.clone(),
            context_window_tokens: model_context_window_tokens(Some(model), snapshot),
        })
        .collect::<Vec<_>>();
    // The candidate list includes the selected model itself, so fewer than
    // two entries means there is no alternative to fail over to.
    if candidates.len() < 2 {
        return None;
    }
    // Sort window-then-id so the constraint is deterministic when several
    // models share the same context window.
    candidates.sort_by(|left, right| {
        left.context_window_tokens
            .cmp(&right.context_window_tokens)
            .then_with(|| left.model_id.cmp(&right.model_id))
    });
    candidates.into_iter().find(|candidate| {
        candidate.model_id != selected_model_id
            && candidate.context_window_tokens < selected_context_tokens
    })
}

/// Whether a provider id is usable for failover. An id missing from the
/// registry counts as enabled only for legacy single-provider snapshots
/// (empty registry) or when it is the snapshot's own active provider.
fn registry_provider_enabled(
    snapshot: &crate::model_provider::ProviderStatusSnapshot,
    provider_id: &str,
) -> bool {
    match snapshot.registry.providers.iter().find(|provider| provider.provider_id == provider_id) {
        Some(provider) => provider.enabled,
        None => snapshot.registry.providers.is_empty() || snapshot.provider_id == provider_id,
    }
}

/// Estimates the prompt-token cost of exposing the tool catalog (per-tool
/// description plus JSON schema, plus fixed wrapping overhead).
fn estimate_tool_schema_overhead_tokens(
    tool_catalog_snapshot: Option<&ModelVisibleToolCatalogSnapshot>,
) -> u64 {
    let Some(snapshot) = tool_catalog_snapshot else {
        return 0;
    };
    if snapshot.tools.is_empty() {
        return 0;
    }
    snapshot
        .tools
        .iter()
        .map(|tool| {
            let schema = tool.provider_schema.to_string();
            estimate_tokens(tool.description.as_str())
                .saturating_add(estimate_tokens(schema.as_str()))
                .saturating_add(TOOL_SCHEMA_PER_TOOL_OVERHEAD_TOKENS)
        })
        .sum::<u64>()
        .saturating_add(TOOL_SCHEMA_BASE_OVERHEAD_TOKENS)
}

/// Hashes a JSON value deterministically. Serializing an in-memory `Value`
/// cannot realistically fail (keys are always strings); the `null` fallback
/// only keeps hashing total instead of panicking.
fn stable_sha256_json(value: &Value) -> String {
    let payload = serde_json::to_vec(value).unwrap_or_else(|_| b"null".to_vec());
    crate::sha256_hex(payload.as_slice())
}

fn cache_scope_hash(value: &str) -> String {
    crate::sha256_hex(value.as_bytes())
}

/// Builds the sorted, deduplicated reason-code list summarizing why the
/// prompt was shaped this way (strategy, drops, trust posture, injection
/// signals, summary-gate verdicts).
fn context_assembly_reason_codes(
    strategy: ContextEngineStrategy,
    selected: &[ContextEngineSegmentExplain],
    dropped: &[ContextEngineDroppedSegmentExplain],
    overflow_tokens: u64,
    summary_quality: Option<&SummaryQualityGateExplain>,
) -> Vec<String> {
    let mut reasons = vec![format!("strategy_{}", strategy.as_str())];
    if dropped.iter().any(|segment| segment.reason == "dropped_by_budget") {
        reasons.push("budget_dropped_segment".to_owned());
    }
    if dropped.iter().any(|segment| segment.reason == "dropped_by_budget_group") {
        reasons.push("budget_dropped_group".to_owned());
    }
    if overflow_tokens > 0 {
        reasons.push("context_budget_exhausted".to_owned());
    }
    if selected.iter().any(|segment| segment.trust_label != TrustLabel::TrustedLocal) {
        reasons.push("mixed_trust_context".to_owned());
    }
    if selected.iter().any(|segment| !segment.safety_findings.is_empty()) {
        reasons.push("prompt_injection_signal_present".to_owned());
    }
    if let Some(summary_quality) = summary_quality {
        reasons.push(format!("summary_quality_{}", summary_quality.verdict));
        reasons.extend(summary_quality.reasons.iter().cloned());
    }
    reasons.sort();
    reasons.dedup();
    reasons
}

/// Condenses the trust posture of all candidate segments into the summary
/// the instruction compiler embeds in the developer message.
fn instruction_trust_summary(segments: &[ContextSegment]) -> InstructionTrustSummary {
    if segments.is_empty() {
        return InstructionTrustSummary::trusted();
    }
    let untrusted_blocks =
        segments.iter().filter(|segment| segment.trust_label != TrustLabel::TrustedLocal).count();
    let prompt_injection_finding_count = segments
        .iter()
        .flat_map(|segment| segment.safety_findings.iter())
        .filter(|finding| finding.starts_with("prompt_injection."))
        .count();
    let highest_safety_action =
        segments.iter().map(|segment| segment.safety_action).max().unwrap_or(SafetyAction::Allow);
    InstructionTrustSummary {
        selected_blocks: segments.len(),
        untrusted_blocks,
        mixed_trust: untrusted_blocks > 0,
        highest_safety_action,
        prompt_injection_finding_count,
    }
}

/// Converts compiled instructions into protected prompt segments. Only
/// system/developer roles participate in budgeting; other roles would be
/// conversation content, which the compiler never emits.
fn instruction_segments(compiled: &CompiledInstructions) -> Vec<ContextSegment> {
    compiled
        .segments
        .iter()
        .filter_map(|segment| {
            let kind = match segment.role {
                ProviderMessageRole::System => ContextSegmentKind::SystemInstructions,
                ProviderMessageRole::Developer => ContextSegmentKind::DeveloperInstructions,
                ProviderMessageRole::User
                | ProviderMessageRole::Assistant
                | ProviderMessageRole::Tool => return None,
            };
            Some(ContextSegment::instruction(
                kind,
                segment.label.clone(),
                segment.content.clone(),
                segment.role,
                segment.estimated_tokens,
            ))
        })
        .collect()
}

/// Picks the assembly strategy label. Precedence: summarizing (compaction
/// summary under budget pressure), checkpoint-aware (checkpoint stands in
/// for a rejected summary), cost-aware (pressure without a summary),
/// provider-aware (cacheable prefix on a caching provider), then noop.
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

/// Decides which session-summary segment (if any) joins the prompt: the
/// latest compaction summary when it passes the quality gate, otherwise the
/// latest checkpoint as a fallback, otherwise nothing.
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

/// Renders the newest checkpoint as a stable prompt segment, optionally
/// annotated with a preview of the compaction artifact it replaces.
fn latest_checkpoint_segment(
    checkpoints: &[OrchestratorCheckpointRecord],
    artifact: Option<&OrchestratorCompactionArtifactRecord>,
) -> Option<ContextSegment> {
    // The journal returns checkpoints newest-first, so `first()` is the
    // most recent one.
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

/// Quality-gates a compaction summary before it may carry session context.
/// Verdict precedence: `reject` (poisoned candidates or contradiction
/// signals -- the summary is unsafe to trust), `fallback` (shallow coverage,
/// pending review, sensitive content, repeated-compaction drift, or budget
/// overrun -- prefer a checkpoint), otherwise `allow`.
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

/// Counts opposing-directive pairs both present in a summary, a coarse
/// signal that compaction merged contradictory instructions.
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
    let tokens = normalized_word_tokens(summary_text);
    CONTRADICTION_PAIRS
        .iter()
        .filter(|(left, right)| contains_non_overlapping_terms(tokens.as_slice(), left, right))
        .count()
}

fn normalized_word_tokens(input: &str) -> Vec<String> {
    input
        .split(|character: char| !character.is_alphanumeric())
        .filter(|token| !token.is_empty())
        .map(str::to_ascii_lowercase)
        .collect()
}

fn contains_non_overlapping_terms(tokens: &[String], left: &str, right: &str) -> bool {
    let left_tokens = normalized_word_tokens(left);
    let right_tokens = normalized_word_tokens(right);
    let left_ranges = term_ranges(tokens, left_tokens.as_slice());
    let right_ranges = term_ranges(tokens, right_tokens.as_slice());
    left_ranges.iter().any(|left_range| {
        right_ranges.iter().any(|right_range| {
            left_range.end <= right_range.start || right_range.end <= left_range.start
        })
    })
}

fn term_ranges(tokens: &[String], term_tokens: &[String]) -> Vec<std::ops::Range<usize>> {
    if term_tokens.is_empty() || term_tokens.len() > tokens.len() {
        return Vec::new();
    }
    tokens
        .windows(term_tokens.len())
        .enumerate()
        .filter_map(|(index, window)| {
            (window == term_tokens).then_some(index..index + term_tokens.len())
        })
        .collect()
}

/// Turns the pre-resolved context-reference preview from the parameter
/// delta into a protected prompt segment. Journals a `context_references`
/// tape event for auditability before rendering, so the event exists even
/// when the rendered block ends up empty.
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

/// Extracts the optional context-reference preview from the parameter delta;
/// malformed JSON is treated as "no references" rather than an error.
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

/// Prefers the reference-stripped clean prompt over the raw input text so
/// `@file:`/`@url:` markers do not appear twice (once as markers, once as
/// resolved reference blocks).
fn normalized_input_text(parameter_delta_json: Option<&str>, input_text: &str) -> String {
    parse_context_reference_preview(parameter_delta_json)
        .map(|preview| preview.clean_prompt.trim().to_owned())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| input_text.to_owned())
}

/// Journals the assembly trace as a [`CONTEXT_ENGINE_PLAN_EVENT`] tape event
/// and mirrors a privacy-reduced copy into runtime diagnostics.
async fn record_context_engine_plan(
    runtime_state: &Arc<GatewayRuntimeState>,
    run_id: &str,
    tape_seq: &mut i64,
    explain: ContextAssemblyTrace,
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
    runtime_state.record_context_assembly_trace(context_assembly_diagnostics_payload(&explain));
    *tape_seq = tape_seq.saturating_add(1);
    Ok(())
}

/// Privacy-reduced trace for runtime diagnostics: segment previews and
/// source refs are omitted entirely (only their redaction state is
/// disclosed) and the identity-bearing cache scope key is replaced by a
/// non-reversible hash. Pinned by the diagnostics redaction tests.
fn context_assembly_diagnostics_payload(explain: &ContextAssemblyTrace) -> Value {
    json!({
        "schema_version": explain.schema_version,
        "trace_id": explain.trace_id.as_str(),
        "strategy": explain.strategy,
        "reason_codes": explain.reason_codes.as_slice(),
        "instruction": explain.instruction.as_ref().map(|instruction| json!({
            "version": instruction.version,
            "hash": instruction.hash.as_str(),
            "provider_kind": instruction.provider_kind.as_str(),
            "model_family": instruction.model_family.as_str(),
            "surface": instruction.surface,
        })),
        "budget": {
            "profile_id": explain.budget.profile_id.as_str(),
            "provider_id": explain.budget.provider_id.as_str(),
            "provider_kind": explain.budget.provider_kind.as_str(),
            "model_id": explain.budget.model_id.as_str(),
            "failover_budget_model_id": explain.budget.failover_budget_model_id.as_deref(),
            "max_context_tokens": explain.budget.max_context_tokens,
            "reserved_completion_tokens": explain.budget.reserved_completion_tokens,
            "reserved_tool_result_tokens": explain.budget.reserved_tool_result_tokens,
            "provider_overhead_tokens": explain.budget.provider_overhead_tokens,
            "safety_margin_tokens": explain.budget.safety_margin_tokens,
            "tool_schema_overhead_tokens": explain.budget.tool_schema_overhead_tokens,
            "input_budget_tokens": explain.budget.input_budget_tokens,
            "selected_tokens": explain.budget.selected_tokens,
            "dropped_tokens": explain.budget.dropped_tokens,
            "overflow_tokens": explain.budget.overflow_tokens,
        },
        "cache": {
            "provider_cache_supported": explain.cache.provider_cache_supported,
            "stable_prefix_hash": explain.cache.stable_prefix_hash.as_deref(),
            "stable_prefix_tokens": explain.cache.stable_prefix_tokens,
            "cache_scope_hash": explain.cache.cache_scope_key.as_deref().map(cache_scope_hash),
            "cache_scope_key_redacted": explain.cache.cache_scope_key.is_some(),
            "trust_scope": explain.cache.trust_scope.as_str(),
        },
        "selected_segments": explain.selected_segments.iter().map(|segment| json!({
            "kind": segment.kind,
            "source_kind": segment.source_kind,
            "label": segment.label.as_str(),
            "estimated_tokens": segment.estimated_tokens,
            "redaction_status": segment.redaction_status.as_str(),
            "trust_label": segment.trust_label.as_str(),
            "safety_action": segment.safety_action.as_str(),
            "safety_findings": segment.safety_findings.as_slice(),
            "source_ref_count": segment.source_refs.len(),
            "source_refs_redacted": !segment.source_refs.is_empty(),
            "preview_redacted": !segment.preview.is_empty(),
        })).collect::<Vec<_>>(),
        "dropped_segments": explain.dropped_segments.iter().map(|segment| json!({
            "kind": segment.kind,
            "label": segment.label.as_str(),
            "estimated_tokens": segment.estimated_tokens,
            "reason": segment.reason.as_str(),
        })).collect::<Vec<_>>(),
    })
}

/// Appends a segment unless its content is blank; empty segments would only
/// add separator noise to the prompt and the trace.
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

/// Trace-safe segment preview plus the redaction label describing how it
/// was sanitized.
#[derive(Debug, Clone)]
struct ExplainPreview {
    text: String,
    redaction_status: String,
}

/// Flattens selected and dropped segments into the per-step assembly view
/// of the trace (included entries first, then drops with metadata only).
fn build_prompt_assembly_steps(
    selected: &[IndexedContextSegment],
    dropped: &[ContextEngineDroppedSegmentExplain],
    selected_explain: &[ContextEngineSegmentExplain],
) -> Vec<PromptAssemblyStepExplain> {
    let mut steps = selected
        .iter()
        .zip(selected_explain.iter())
        .map(|(entry, explain)| PromptAssemblyStepExplain {
            step: assembly_step_for_kind(entry.segment.kind).to_owned(),
            label: entry.segment.label.clone(),
            included: true,
            token_estimate: entry.segment.estimated_tokens,
            include_reason: explain.include_reason.clone(),
            redaction_status: explain.redaction_status.clone(),
            source_refs: explain.source_refs.clone(),
        })
        .collect::<Vec<_>>();
    steps.extend(dropped.iter().map(|entry| PromptAssemblyStepExplain {
        step: assembly_step_for_kind(entry.kind).to_owned(),
        label: entry.label.clone(),
        included: false,
        token_estimate: entry.estimated_tokens,
        include_reason: entry.reason.clone(),
        redaction_status: "metadata_only".to_owned(),
        source_refs: vec![format!("segment:{}:{}", entry.kind.as_str(), entry.label)],
    }));
    steps
}

fn include_reason_for_segment(segment: &ContextSegment) -> String {
    if segment.protected {
        "protected_active_context".to_owned()
    } else if segment.stable {
        "stable_context_prefix".to_owned()
    } else if segment.trust_label != TrustLabel::TrustedLocal {
        "included_with_trust_annotation".to_owned()
    } else {
        "selected_by_prompt_assembly_budget".to_owned()
    }
}

fn source_refs_for_segment(segment: &ContextSegment) -> Vec<String> {
    let mut refs = vec![format!("segment:{}:{}", segment.kind.as_str(), segment.label)];
    if let Some(group_id) = segment.group_id.as_deref() {
        refs.push(format!("group:{group_id}"));
    }
    refs
}

fn assembly_step_for_kind(kind: ContextSegmentKind) -> &'static str {
    match kind {
        ContextSegmentKind::SystemInstructions | ContextSegmentKind::DeveloperInstructions => {
            "instruction_compiler"
        }
        ContextSegmentKind::PreferenceContext | ContextSegmentKind::ProjectContext => {
            "policy_system"
        }
        ContextSegmentKind::SessionCompactionSummary | ContextSegmentKind::CheckpointSummary => {
            "session_state"
        }
        ContextSegmentKind::ContextReferences => "active_task",
        ContextSegmentKind::MemoryRecall => "memory",
        ContextSegmentKind::ToolExchange => "tool_previews",
        ContextSegmentKind::AttachmentRecall => "artifact_refs",
        ContextSegmentKind::ExplicitRecall | ContextSegmentKind::SessionTail => {
            "historical_context"
        }
        ContextSegmentKind::UserInput => "user_turn",
    }
}

fn source_kind_for_segment(segment: &ContextSegment) -> ContextSourceKind {
    match segment.kind {
        ContextSegmentKind::SystemInstructions => ContextSourceKind::System,
        ContextSegmentKind::DeveloperInstructions | ContextSegmentKind::PreferenceContext => {
            ContextSourceKind::Developer
        }
        ContextSegmentKind::ProjectContext => ContextSourceKind::Workspace,
        ContextSegmentKind::MemoryRecall
        | ContextSegmentKind::SessionCompactionSummary
        | ContextSegmentKind::CheckpointSummary => ContextSourceKind::Memory,
        ContextSegmentKind::ContextReferences
        | ContextSegmentKind::ExplicitRecall
        | ContextSegmentKind::SessionTail => ContextSourceKind::Retrieval,
        ContextSegmentKind::AttachmentRecall => ContextSourceKind::Attachment,
        ContextSegmentKind::ToolExchange => ContextSourceKind::ToolResult,
        ContextSegmentKind::UserInput => ContextSourceKind::User,
    }
}

fn explain_preview_for_segment(segment: &ContextSegment) -> ExplainPreview {
    // Compiled instruction text never enters traces or journals, only a
    // fixed marker; the instruction hash is the auditable identity instead.
    if segment.provider_role.is_some() {
        return ExplainPreview {
            text: "<instruction_redacted>".to_owned(),
            redaction_status: "instruction_redacted".to_owned(),
        };
    }
    explain_preview_text(segment.content.as_str(), SEGMENT_PREVIEW_CHARS)
}

/// Builds a trace-safe preview: whitespace-normalized, JSON shrunk when the
/// content parses as JSON, secret-redacted, then length-capped.
fn explain_preview_text(raw: &str, max_chars: usize) -> ExplainPreview {
    let normalized = raw.replace(['\r', '\n'], " ");
    let trimmed = normalized.split_whitespace().collect::<Vec<_>>().join(" ");
    let (candidate, shrunk) = shrink_json_preview_if_possible(trimmed.as_str());
    let redacted = redact_explain_preview(candidate.as_str());
    let redaction_status = if redacted != candidate {
        "redacted"
    } else if shrunk {
        "json_shrunk"
    } else {
        "clean"
    };
    ExplainPreview {
        text: preview_text(redacted.as_str(), max_chars),
        redaction_status: redaction_status.to_owned(),
    }
}

fn shrink_json_preview_if_possible(raw: &str) -> (String, bool) {
    let trimmed = raw.trim();
    if !(trimmed.starts_with('{') || trimmed.starts_with('[')) {
        return (raw.to_owned(), false);
    }
    let Ok(value) = serde_json::from_str::<Value>(trimmed) else {
        return (raw.to_owned(), false);
    };
    let outcome = shrink_json_value(&value, JsonShrinkConfig::default());
    let rendered = serde_json::to_string(&outcome.value).unwrap_or_else(|_| raw.to_owned());
    (rendered, outcome.truncated)
}

/// Runs the preview through the journal's secret redaction by wrapping it in
/// a one-field JSON document, reusing the exact redaction rules journaled
/// payloads get instead of maintaining a second pattern list here.
fn redact_explain_preview(raw: &str) -> String {
    let payload = json!({ "preview": raw }).to_string();
    let redacted = match crate::journal::redact_payload_json(payload.as_bytes()) {
        Ok(value) => value,
        Err(_) => return raw.to_owned(),
    };
    serde_json::from_str::<Value>(redacted.as_str())
        .ok()
        .and_then(|value| value.get("preview").and_then(Value::as_str).map(ToOwned::to_owned))
        .unwrap_or_else(|| raw.to_owned())
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

/// Provider-agnostic token estimate: ~4 characters per token, rounded up so
/// the budget overcounts rather than undercounts.
fn estimate_tokens(text: &str) -> u64 {
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return 0;
    }
    // usize -> u64 cannot truncate on any supported target.
    trimmed.chars().count().div_ceil(4) as u64
}

#[cfg(test)]
mod tests {
    use super::{
        assemble_segments, context_assembly_diagnostics_payload, resolve_provider_context_budget,
        select_strategy, ContextEngineStrategy, ContextSegment, ContextSegmentKind,
        ProviderBudgetProfile, ProviderContextBudget, SummaryQualityGateExplain,
    };
    use crate::model_provider::{
        ProviderCapabilitiesSnapshot, ProviderCircuitBreakerSnapshot, ProviderDiscoverySnapshot,
        ProviderHealthProbeSnapshot, ProviderRegistryModelSnapshot,
        ProviderRegistryProviderSnapshot, ProviderRegistrySnapshot, ProviderResponseCacheSnapshot,
        ProviderRetryPolicySnapshot, ProviderRouteSelectionTrace, ProviderRuntimeMetricsSnapshot,
        ProviderStatusSnapshot,
    };
    use crate::transport::grpc::auth::RequestContext;
    use palyra_safety::{SafetyAction, TrustLabel};
    use serde_json::{json, Value};

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
    #[allow(clippy::too_many_arguments)]
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

    fn segment_with_content(
        kind: ContextSegmentKind,
        label: &str,
        content: String,
        estimated_tokens: u64,
        priority: u8,
        protected: bool,
    ) -> ContextSegment {
        let mut segment =
            ContextSegment::trusted(kind, label, content, priority, false, protected, None);
        segment.estimated_tokens = estimated_tokens;
        segment
    }

    fn budget(
        max_context_tokens: u64,
        reserved_completion_tokens: u64,
        reserved_tool_result_tokens: u64,
        provider_overhead_tokens: u64,
        provider_cache_supported: bool,
    ) -> ProviderContextBudget {
        ProviderContextBudget {
            profile: ProviderBudgetProfile {
                profile_id: format!("budget_test_{max_context_tokens}"),
                provider_id: "provider-test".to_owned(),
                provider_kind: "deterministic".to_owned(),
                model_id: "model-test".to_owned(),
                context_window_tokens: max_context_tokens,
                max_output_tokens: reserved_completion_tokens,
                safety_margin_tokens: 0,
                tool_schema_overhead_tokens: 0,
                provider_cache_supported,
                failover_policy: "test".to_owned(),
                failover_budget_model_id: None,
            },
            max_context_tokens,
            reserved_completion_tokens,
            reserved_tool_result_tokens,
            provider_overhead_tokens,
            safety_margin_tokens: 0,
            tool_schema_overhead_tokens: 0,
            provider_cache_supported,
        }
    }

    fn provider_capabilities(max_context_tokens: u32) -> ProviderCapabilitiesSnapshot {
        ProviderCapabilitiesSnapshot {
            streaming_tokens: true,
            tool_calls: true,
            json_mode: true,
            vision: false,
            audio_transcribe: false,
            embeddings: false,
            reasoning: false,
            reasoning_efforts: Vec::new(),
            service_tier: false,
            service_tiers: Vec::new(),
            max_context_tokens: Some(max_context_tokens),
            cost_tier: "standard".to_owned(),
            latency_tier: "standard".to_owned(),
            recommended_use_cases: Vec::new(),
            known_limitations: Vec::new(),
            operator_override: false,
            metadata_source: "test".to_owned(),
        }
    }

    fn provider_runtime_metrics() -> ProviderRuntimeMetricsSnapshot {
        ProviderRuntimeMetricsSnapshot {
            request_count: 0,
            error_count: 0,
            error_rate_bps: 0,
            total_retry_attempts: 0,
            total_prompt_tokens: 0,
            total_completion_tokens: 0,
            avg_prompt_tokens_per_run: 0,
            avg_completion_tokens_per_run: 0,
            last_latency_ms: 0,
            avg_latency_ms: 0,
            max_latency_ms: 0,
            last_used_at_unix_ms: None,
            last_success_at_unix_ms: None,
            last_error_at_unix_ms: None,
            last_error: None,
        }
    }

    fn provider_registry_entry(provider_id: &str, kind: &str) -> ProviderRegistryProviderSnapshot {
        ProviderRegistryProviderSnapshot {
            provider_id: provider_id.to_owned(),
            credential_id: format!("credential-{provider_id}"),
            display_name: provider_id.to_owned(),
            kind: kind.to_owned(),
            enabled: true,
            endpoint_base_url: None,
            auth_profile_id: Some(format!("auth-{provider_id}")),
            auth_profile_provider_kind: Some(kind.to_owned()),
            credential_source: Some("auth_profile_api_key".to_owned()),
            api_key_configured: true,
            retry_policy: ProviderRetryPolicySnapshot { max_retries: 1, retry_backoff_ms: 25 },
            circuit_breaker: ProviderCircuitBreakerSnapshot {
                failure_threshold: 3,
                cooldown_ms: 30_000,
                consecutive_failures: 0,
                open: false,
            },
            runtime_metrics: provider_runtime_metrics(),
            health: ProviderHealthProbeSnapshot {
                state: "ok".to_owned(),
                message: "ok".to_owned(),
                checked_at_unix_ms: Some(0),
                latency_ms: Some(1),
                source: "test".to_owned(),
            },
            discovery: ProviderDiscoverySnapshot {
                status: "static".to_owned(),
                checked_at_unix_ms: Some(0),
                expires_at_unix_ms: None,
                discovered_model_ids: Vec::new(),
                source: "test".to_owned(),
                message: None,
            },
        }
    }

    fn provider_registry_model(
        model_id: &str,
        provider_id: &str,
        max_context_tokens: u32,
    ) -> ProviderRegistryModelSnapshot {
        ProviderRegistryModelSnapshot {
            model_id: model_id.to_owned(),
            provider_id: provider_id.to_owned(),
            role: "chat".to_owned(),
            enabled: true,
            capabilities: provider_capabilities(max_context_tokens),
        }
    }

    fn provider_snapshot_for_budget(failover_enabled: bool) -> ProviderStatusSnapshot {
        let default_capabilities = provider_capabilities(128_000);
        ProviderStatusSnapshot {
            kind: "openai_compatible".to_owned(),
            provider_id: "openai".to_owned(),
            credential_id: "credential-openai".to_owned(),
            model_id: Some("large".to_owned()),
            capabilities: default_capabilities.clone(),
            openai_base_url: Some("https://api.openai.test/v1".to_owned()),
            anthropic_base_url: Some("https://api.anthropic.test".to_owned()),
            openai_model: Some("large".to_owned()),
            anthropic_model: None,
            openai_embeddings_model: None,
            openai_embeddings_dims: None,
            auth_profile_id: Some("auth-openai".to_owned()),
            auth_profile_provider_kind: Some("openai_compatible".to_owned()),
            credential_source: Some("auth_profile_api_key".to_owned()),
            api_key_configured: true,
            retry_policy: ProviderRetryPolicySnapshot { max_retries: 1, retry_backoff_ms: 25 },
            circuit_breaker: ProviderCircuitBreakerSnapshot {
                failure_threshold: 3,
                cooldown_ms: 30_000,
                consecutive_failures: 0,
                open: false,
            },
            runtime_metrics: provider_runtime_metrics(),
            response_cache: ProviderResponseCacheSnapshot {
                enabled: true,
                entry_count: 0,
                hit_count: 0,
                miss_count: 0,
            },
            health: ProviderHealthProbeSnapshot {
                state: "ok".to_owned(),
                message: "ok".to_owned(),
                checked_at_unix_ms: Some(0),
                latency_ms: Some(1),
                source: "test".to_owned(),
            },
            discovery: ProviderDiscoverySnapshot {
                status: "static".to_owned(),
                checked_at_unix_ms: Some(0),
                expires_at_unix_ms: None,
                discovered_model_ids: vec!["large".to_owned(), "small".to_owned()],
                source: "test".to_owned(),
                message: None,
            },
            registry: ProviderRegistrySnapshot {
                default_chat_model_id: Some("large".to_owned()),
                default_embeddings_model_id: None,
                default_audio_transcription_model_id: None,
                failover_enabled,
                response_cache_enabled: true,
                providers: vec![
                    provider_registry_entry("openai", "openai_compatible"),
                    provider_registry_entry("anthropic", "anthropic"),
                ],
                credentials: Vec::new(),
                models: vec![
                    provider_registry_model("large", "openai", 128_000),
                    provider_registry_model("small", "anthropic", 8_192),
                ],
            },
            route_selection: ProviderRouteSelectionTrace::empty(),
        }
    }

    #[test]
    fn provider_budget_uses_smaller_failover_context_window_when_registry_can_fallback() {
        let snapshot = provider_snapshot_for_budget(true);
        let budget = resolve_provider_context_budget(
            &snapshot,
            Some("openai_compatible"),
            Some("large"),
            None,
        );

        assert_eq!(budget.profile.model_id, "large");
        assert_eq!(budget.max_context_tokens, 8_192);
        assert_eq!(budget.profile.failover_budget_model_id.as_deref(), Some("small"));
        assert_eq!(budget.profile.failover_policy, "registry_failover_enabled");
    }

    #[test]
    fn provider_budget_keeps_primary_context_window_when_failover_is_disabled() {
        let snapshot = provider_snapshot_for_budget(false);
        let budget = resolve_provider_context_budget(
            &snapshot,
            Some("openai_compatible"),
            Some("large"),
            None,
        );

        assert_eq!(budget.max_context_tokens, 128_000);
        assert_eq!(budget.profile.failover_budget_model_id, None);
        assert_eq!(budget.profile.failover_policy, "registry_failover_disabled");
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
            budget(3_072, 512, 512, 192, true),
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
            budget(8_192, 1_024, 512, 192, false),
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
            budget(1_024, 512, 128, 128, true),
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
            budget(4_096, 768, 256, 128, true),
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
                "schema_version": 1,
                "trace_id": actual.pointer("/trace_id").cloned().expect("trace id should exist"),
                "strategy": "provider_aware",
                "rollout_enabled": true,
                "budget": {
                    "profile_id": "budget_test_4096",
                    "provider_id": "provider-test",
                    "provider_kind": "deterministic",
                    "model_id": "model-test",
                    "max_context_tokens": 4096,
                    "reserved_completion_tokens": 768,
                    "reserved_tool_result_tokens": 256,
                    "provider_overhead_tokens": 128,
                    "safety_margin_tokens": 0,
                    "tool_schema_overhead_tokens": 0,
                    "input_budget_tokens": 2944,
                    "selected_tokens": 136,
                    "dropped_tokens": 0,
                    "overflow_tokens": 0
                },
                "cache": {
                    "provider_cache_supported": true,
                    "stable_prefix_hash": actual.pointer("/cache/stable_prefix_hash").cloned().expect("stable prefix hash should exist"),
                    "stable_prefix_tokens": 64,
                    "cache_scope_key": actual.pointer("/cache/cache_scope_key").cloned().expect("cache scope key should exist"),
                    "trust_scope": "mixed"
                },
                "summary_quality": null,
                "reason_codes": [
                    "mixed_trust_context",
                    "prompt_injection_signal_present",
                    "strategy_provider_aware"
                ],
                "assembly_steps": [
                    {
                        "step": "policy_system",
                        "label": "stable policy",
                        "included": true,
                        "token_estimate": 64,
                        "include_reason": "protected_active_context",
                        "redaction_status": "clean",
                        "source_refs": ["segment:preference_context:stable policy"]
                    },
                    {
                        "step": "active_task",
                        "label": "focused files",
                        "included": true,
                        "token_estimate": 48,
                        "include_reason": "protected_active_context",
                        "redaction_status": "clean",
                        "source_refs": ["segment:context_references:focused files"]
                    },
                    {
                        "step": "user_turn",
                        "label": "ship it",
                        "included": true,
                        "token_estimate": 24,
                        "include_reason": "protected_active_context",
                        "redaction_status": "clean",
                        "source_refs": ["segment:user_input:ship it"]
                    }
                ],
                "selected_segments": [
                    {
                        "kind": "preference_context",
                        "source_kind": "developer",
                        "label": "stable policy",
                        "estimated_tokens": 64,
                        "include_reason": "protected_active_context",
                        "redaction_status": "clean",
                        "stable": true,
                        "protected": true,
                        "trust_label": "trusted_local",
                        "safety_action": "allow",
                        "group_id": null,
                        "source_refs": ["segment:preference_context:stable policy"],
                        "preview": "stable policy"
                    },
                    {
                        "kind": "context_references",
                        "source_kind": "retrieval",
                        "label": "focused files",
                        "estimated_tokens": 48,
                        "include_reason": "protected_active_context",
                        "redaction_status": "clean",
                        "stable": false,
                        "protected": true,
                        "trust_label": "external_untrusted",
                        "safety_action": "annotate",
                        "safety_findings": ["prompt_injection.ignore_previous_instructions"],
                        "group_id": null,
                        "source_refs": ["segment:context_references:focused files"],
                        "preview": "focused files"
                    },
                    {
                        "kind": "user_input",
                        "source_kind": "user",
                        "label": "ship it",
                        "estimated_tokens": 24,
                        "include_reason": "protected_active_context",
                        "redaction_status": "clean",
                        "stable": false,
                        "protected": true,
                        "trust_label": "trusted_local",
                        "safety_action": "allow",
                        "group_id": null,
                        "source_refs": ["segment:user_input:ship it"],
                        "preview": "ship it"
                    }
                ],
                "dropped_segments": []
            })
        );
    }

    #[test]
    fn diagnostics_payload_redacts_context_previews_and_scope_identifiers() {
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
                segment(ContextSegmentKind::UserInput, "ship it", 24, 100, false, true, None),
            ],
            ContextEngineStrategy::ProviderAware,
            budget(4_096, 768, 256, 128, true),
            &RequestContext {
                principal: "user:alice".to_owned(),
                device_id: "device".to_owned(),
                channel: Some("cli".to_owned()),
            },
            "session-alice",
            None,
        );

        let payload = context_assembly_diagnostics_payload(&assembled.explain);
        assert!(
            payload.pointer("/cache/cache_scope_key").is_none(),
            "diagnostics payload must not expose principal/session/channel cache scope"
        );
        assert!(
            payload.pointer("/cache/cache_scope_hash").and_then(Value::as_str).is_some(),
            "diagnostics payload should retain a non-reversible cache scope correlation hash"
        );
        let segments =
            payload.pointer("/selected_segments").and_then(Value::as_array).expect("segments");
        assert!(
            segments.iter().all(|segment| segment.get("preview").is_none()),
            "diagnostics payload must not expose prompt previews"
        );
        assert!(
            segments.iter().all(|segment| segment.get("source_refs").is_none()),
            "diagnostics payload must not expose source references"
        );
        assert!(
            segments
                .iter()
                .all(|segment| segment.get("preview_redacted").and_then(Value::as_bool).is_some()),
            "diagnostics payload should disclose only preview redaction state"
        );
    }

    #[test]
    fn explain_preview_redacts_secret_values_after_json_shrink() {
        let secret_json = serde_json::json!({
            "api_key": "sk-test-secret-token",
            "items": (0..20).map(|index| serde_json::json!({"index": index, "body": "x".repeat(64)})).collect::<Vec<_>>()
        })
        .to_string();
        let assembled = assemble_segments(
            &[
                segment_with_content(
                    ContextSegmentKind::MemoryRecall,
                    "memory json",
                    secret_json,
                    512,
                    80,
                    false,
                ),
                segment(ContextSegmentKind::UserInput, "question", 24, 100, false, true, None),
            ],
            ContextEngineStrategy::CostAware,
            budget(4_096, 768, 256, 128, false),
            &RequestContext {
                principal: "user:ops".to_owned(),
                device_id: "device".to_owned(),
                channel: Some("cli".to_owned()),
            },
            "session-redaction",
            None,
        );

        let memory_segment = assembled
            .explain
            .selected_segments
            .iter()
            .find(|segment| segment.label == "memory json")
            .expect("memory JSON segment should be selected");
        assert_eq!(memory_segment.redaction_status, "redacted");
        assert!(
            !memory_segment.preview.contains("sk-test-secret-token"),
            "prompt explain preview must not leak raw provider-style secrets"
        );
        assert!(
            memory_segment.preview.contains("<redacted>"),
            "redacted previews should show the redaction marker"
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
            budget(1_024, 512, 128, 128, false),
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

    #[test]
    fn assembly_does_not_drop_protected_member_of_mixed_group() {
        let assembled = assemble_segments(
            &[
                segment(
                    ContextSegmentKind::ToolExchange,
                    "protected_group_member",
                    500,
                    100,
                    false,
                    true,
                    Some("mixed:1"),
                ),
                segment(
                    ContextSegmentKind::ToolExchange,
                    "unprotected_group_member",
                    520,
                    30,
                    false,
                    false,
                    Some("mixed:1"),
                ),
                segment(ContextSegmentKind::UserInput, "question", 220, 100, false, true, None),
            ],
            ContextEngineStrategy::CheckpointAware,
            budget(1_536, 512, 128, 128, false),
            &RequestContext {
                principal: "user:ops".to_owned(),
                device_id: "device".to_owned(),
                channel: Some("cli".to_owned()),
            },
            "session-2",
            None,
        );

        assert!(assembled.prompt_text.contains("protected_group_member"));
        assert!(!assembled.prompt_text.contains("unprotected_group_member"));
        assert_eq!(assembled.explain.dropped_segments.len(), 1);
        assert_eq!(assembled.explain.dropped_segments[0].label, "unprotected_group_member");
    }

    #[test]
    fn contradiction_signals_require_non_overlapping_word_terms() {
        assert_eq!(
            super::count_contradiction_signals("The warehouse note should avoid churn."),
            0,
            "embedded words must not count as directive terms"
        );
        assert_eq!(
            super::count_contradiction_signals("The rollout must not expose secrets."),
            0,
            "a phrase must not satisfy its own shorter prefix term"
        );
        assert_eq!(
            super::count_contradiction_signals("The rollout must proceed, but must not leak."),
            1
        );
    }
}
