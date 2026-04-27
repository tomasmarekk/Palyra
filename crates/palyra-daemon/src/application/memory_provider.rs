use std::sync::Arc;

use async_trait::async_trait;
use serde::Serialize;
use serde_json::{json, Value};
use tonic::Status;
use ulid::Ulid;

use crate::{
    application::memory::{
        classify_memory_write, redact_memory_text_for_output, MemoryLifecycleProvider,
        MemoryLifecycleRetainOutcome, MemoryLifecycleRetainRequest, MemoryLifecycleScope,
        MemoryWriteApprovalState, MemoryWriteClassification, MemoryWriteClassificationInput,
        MemoryWriteSourceRef, MEMORY_TRUST_LABEL_RETRIEVED,
    },
    domain::workspace::scan_workspace_content_for_prompt_injection,
    gateway::{current_unix_ms, current_unix_ms_status, GatewayRuntimeState},
    journal::{
        MemorySearchHit, MemorySearchRequest, MemorySource, WorkspaceDocumentListFilter,
        WorkspaceDocumentRecord, WorkspaceSearchHit, WorkspaceSearchRequest,
    },
    transport::grpc::auth::RequestContext,
};

const MEMORY_PROVIDER_SYSTEM_PROMPT_MIN_CONFIDENCE: f64 = 0.82;
const MEMORY_PROVIDER_PREFETCH_MIN_SCORE: f64 = 0.05;
const MEMORY_PROVIDER_SYSTEM_PROMPT_TOP_K: usize = 8;
const MEMORY_PROVIDER_PREFETCH_TOP_K: usize = 8;

#[allow(dead_code)]
#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum MemoryProviderHealth {
    Ready,
    Degraded,
    Unavailable,
}

#[allow(dead_code)]
#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum MemoryProviderIndexState {
    Ready,
    Stale,
    Reindexing,
    Unavailable,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub(crate) struct MemoryProviderDegradedMode {
    pub(crate) active: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) reason_code: Option<String>,
    pub(crate) fallback: String,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub(crate) struct MemoryProviderQuota {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) max_entries: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) max_bytes: Option<u64>,
    pub(crate) max_prompt_tokens: usize,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub(crate) struct MemoryProviderPolicyLimits {
    pub(crate) sensitivity_ceiling: String,
    pub(crate) policy_scope: String,
    pub(crate) cross_workspace_recall_allowed: bool,
    pub(crate) durable_write_requires_approval: bool,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub(crate) struct MemoryProviderStatus {
    pub(crate) provider_id: String,
    pub(crate) provider_kind: String,
    pub(crate) health: MemoryProviderHealth,
    pub(crate) index_state: MemoryProviderIndexState,
    pub(crate) queue_depth: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) last_error: Option<String>,
    pub(crate) quota: MemoryProviderQuota,
    pub(crate) policy_limits: MemoryProviderPolicyLimits,
    pub(crate) degraded_mode: MemoryProviderDegradedMode,
    pub(crate) updated_at_unix_ms: i64,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub(crate) struct MemoryProviderScoreBreakdown {
    pub(crate) lexical_score: f64,
    pub(crate) semantic_score: f64,
    pub(crate) recency_score: f64,
    pub(crate) source_quality_score: f64,
    pub(crate) sensitivity_penalty: f64,
    pub(crate) exact_phrase_match: f64,
    pub(crate) final_score: f64,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub(crate) struct MemoryProviderCitation {
    pub(crate) source_kind: String,
    pub(crate) source_ref: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) session_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) run_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) tape_seq: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) workspace_path: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) artifact_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub(crate) struct MemoryProviderHit {
    pub(crate) hit_id: String,
    pub(crate) provider_id: String,
    pub(crate) title: String,
    pub(crate) snippet: String,
    pub(crate) score: MemoryProviderScoreBreakdown,
    pub(crate) citation: MemoryProviderCitation,
    pub(crate) trust_label: String,
    pub(crate) confidence: f64,
    pub(crate) injection_scan_status: String,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub(crate) struct MemoryProviderContextBlock {
    pub(crate) block_id: String,
    pub(crate) provider_id: String,
    pub(crate) content: String,
    pub(crate) trust_label: String,
    pub(crate) source_refs: Vec<MemoryWriteSourceRef>,
    pub(crate) confidence: f64,
    pub(crate) injection_scan_status: String,
    pub(crate) sensitivity: String,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub(crate) struct MemoryProviderWriteOutcome {
    pub(crate) classification: MemoryWriteClassification,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) lifecycle: Option<MemoryLifecycleRetainOutcome>,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub(crate) struct MemoryProviderHookOutcome {
    pub(crate) provider_id: String,
    pub(crate) reason_code: String,
    pub(crate) degraded: bool,
    #[serde(default)]
    pub(crate) blocks: Vec<MemoryProviderContextBlock>,
    #[serde(default)]
    pub(crate) hits: Vec<MemoryProviderHit>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) write: Option<MemoryProviderWriteOutcome>,
    #[serde(default)]
    pub(crate) diagnostics: Value,
}

impl MemoryProviderHookOutcome {
    fn empty(provider_id: &str, reason_code: &str) -> Self {
        Self {
            provider_id: provider_id.to_owned(),
            reason_code: reason_code.to_owned(),
            degraded: false,
            blocks: Vec::new(),
            hits: Vec::new(),
            write: None,
            diagnostics: json!({}),
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct MemoryProviderHookContext {
    pub(crate) principal: String,
    pub(crate) device_id: String,
    pub(crate) channel: Option<String>,
    pub(crate) session_id: Option<String>,
    pub(crate) agent_id: Option<String>,
    pub(crate) workspace_id: Option<String>,
    pub(crate) sensitivity_ceiling: String,
    pub(crate) policy_scope: String,
    pub(crate) objective: Option<String>,
    pub(crate) active_files: Vec<String>,
    pub(crate) recent_context: Vec<String>,
    pub(crate) provenance: Value,
}

impl MemoryProviderHookContext {
    #[must_use]
    pub(crate) fn from_request_context(context: &RequestContext) -> Self {
        Self {
            principal: context.principal.clone(),
            device_id: context.device_id.clone(),
            channel: context.channel.clone(),
            session_id: None,
            agent_id: None,
            workspace_id: None,
            sensitivity_ceiling: "normal".to_owned(),
            policy_scope: "console".to_owned(),
            objective: None,
            active_files: Vec::new(),
            recent_context: Vec::new(),
            provenance: json!({
                "principal": context.principal.clone(),
                "device_id": context.device_id.clone(),
                "channel": context.channel.clone(),
            }),
        }
    }

    fn prefetch_query(&self) -> String {
        [
            self.objective.clone().unwrap_or_default(),
            self.active_files.join(" "),
            self.recent_context.join(" "),
        ]
        .join(" ")
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
    }
}

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub(crate) struct MemoryProviderWriteCandidate {
    pub(crate) scope: MemoryLifecycleScope,
    pub(crate) source: MemorySource,
    pub(crate) content_text: String,
    pub(crate) tags: Vec<String>,
    pub(crate) confidence: Option<f64>,
    pub(crate) ttl_unix_ms: Option<i64>,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub(crate) struct MemoryProviderReindexOutcome {
    pub(crate) job_id: String,
    pub(crate) provider_id: String,
    pub(crate) state: String,
    pub(crate) cancel_supported: bool,
    pub(crate) cancelled: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) cancel_reason: Option<String>,
    pub(crate) ran_at_unix_ms: i64,
    pub(crate) progress: MemoryProviderReindexProgress,
    pub(crate) artifact_log: Vec<String>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub(crate) struct MemoryProviderReindexProgress {
    pub(crate) batch_size: usize,
    pub(crate) scanned_count: u64,
    pub(crate) updated_count: u64,
    pub(crate) pending_count: u64,
    pub(crate) complete: bool,
    pub(crate) target_model_id: String,
    pub(crate) target_dims: usize,
    pub(crate) target_version: i64,
}

#[async_trait]
#[allow(dead_code)]
pub(crate) trait MemoryProviderRuntime: Send + Sync {
    fn provider_id(&self) -> &'static str;
    fn provider_kind(&self) -> &'static str;

    async fn initialize(
        &self,
        _context: &MemoryProviderHookContext,
    ) -> Result<MemoryProviderHookOutcome, Status> {
        Ok(MemoryProviderHookOutcome::empty(self.provider_id(), "initialized"))
    }

    async fn status(
        &self,
        context: &MemoryProviderHookContext,
    ) -> Result<MemoryProviderStatus, Status>;

    async fn system_prompt_block(
        &self,
        _context: &MemoryProviderHookContext,
    ) -> Result<MemoryProviderHookOutcome, Status> {
        Ok(MemoryProviderHookOutcome::empty(self.provider_id(), "system_prompt_block_empty"))
    }

    async fn prefetch(
        &self,
        _context: &MemoryProviderHookContext,
    ) -> Result<MemoryProviderHookOutcome, Status> {
        Ok(MemoryProviderHookOutcome::empty(self.provider_id(), "prefetch_empty"))
    }

    async fn sync_turn(
        &self,
        _context: &MemoryProviderHookContext,
    ) -> Result<MemoryProviderHookOutcome, Status> {
        Ok(MemoryProviderHookOutcome::empty(self.provider_id(), "sync_turn_noop"))
    }

    async fn on_session_end(
        &self,
        _context: &MemoryProviderHookContext,
    ) -> Result<MemoryProviderHookOutcome, Status> {
        Ok(MemoryProviderHookOutcome::empty(self.provider_id(), "session_end_noop"))
    }

    async fn on_pre_compact(
        &self,
        _context: &MemoryProviderHookContext,
    ) -> Result<MemoryProviderHookOutcome, Status> {
        Ok(MemoryProviderHookOutcome::empty(self.provider_id(), "pre_compact_noop"))
    }

    async fn on_memory_write(
        &self,
        _context: &MemoryProviderHookContext,
        _candidate: MemoryProviderWriteCandidate,
    ) -> Result<MemoryProviderHookOutcome, Status> {
        Ok(MemoryProviderHookOutcome::empty(self.provider_id(), "memory_write_noop"))
    }
}

pub(crate) struct BuiltinJournalMemoryProvider {
    runtime_state: Arc<GatewayRuntimeState>,
}

impl BuiltinJournalMemoryProvider {
    #[must_use]
    pub(crate) fn new(runtime_state: Arc<GatewayRuntimeState>) -> Self {
        Self { runtime_state }
    }
}

#[async_trait]
impl MemoryProviderRuntime for BuiltinJournalMemoryProvider {
    fn provider_id(&self) -> &'static str {
        "builtin_journal"
    }

    fn provider_kind(&self) -> &'static str {
        "journal_memory"
    }

    async fn status(
        &self,
        context: &MemoryProviderHookContext,
    ) -> Result<MemoryProviderStatus, Status> {
        let maintenance = self.runtime_state.memory_maintenance_status().await?;
        let embeddings = self.runtime_state.memory_embeddings_status().await?;
        let retrieval = self.runtime_state.retrieval_backend_snapshot()?;
        let memory_config = self.runtime_state.memory_config_snapshot();
        let degraded_reason = embeddings.degraded_reason_code.clone().or_else(|| {
            retrieval.external_index.as_ref().and_then(|index| index.last_error.clone())
        });
        let health = if degraded_reason.is_some() {
            MemoryProviderHealth::Degraded
        } else {
            MemoryProviderHealth::Ready
        };
        Ok(MemoryProviderStatus {
            provider_id: self.provider_id().to_owned(),
            provider_kind: self.provider_kind().to_owned(),
            health,
            index_state: if degraded_reason.is_some() {
                MemoryProviderIndexState::Stale
            } else {
                MemoryProviderIndexState::Ready
            },
            queue_depth: 0,
            last_error: degraded_reason
                .as_deref()
                .map(redact_memory_text_for_output)
                .filter(|value| !value.is_empty()),
            quota: MemoryProviderQuota {
                max_entries: memory_config.retention_max_entries,
                max_bytes: memory_config.retention_max_bytes,
                max_prompt_tokens: memory_config.max_item_tokens,
            },
            policy_limits: MemoryProviderPolicyLimits {
                sensitivity_ceiling: context.sensitivity_ceiling.clone(),
                policy_scope: context.policy_scope.clone(),
                cross_workspace_recall_allowed: false,
                durable_write_requires_approval: true,
            },
            degraded_mode: MemoryProviderDegradedMode {
                active: degraded_reason.is_some(),
                reason_code: degraded_reason,
                fallback: "lexical_journal_search".to_owned(),
            },
            updated_at_unix_ms: maintenance
                .last_run
                .as_ref()
                .map_or_else(current_unix_ms, |run| run.ran_at_unix_ms),
        })
    }

    async fn system_prompt_block(
        &self,
        context: &MemoryProviderHookContext,
    ) -> Result<MemoryProviderHookOutcome, Status> {
        let hits = self
            .runtime_state
            .search_memory(MemorySearchRequest {
                principal: context.principal.clone(),
                channel: context.channel.clone(),
                session_id: None,
                query: "preference procedure workflow rule constraint".to_owned(),
                top_k: MEMORY_PROVIDER_SYSTEM_PROMPT_TOP_K,
                min_score: MEMORY_PROVIDER_PREFETCH_MIN_SCORE,
                tags: Vec::new(),
                sources: Vec::new(),
            })
            .await?;
        let blocks = hits
            .iter()
            .filter(|hit| {
                hit.item.confidence.unwrap_or_default()
                    >= MEMORY_PROVIDER_SYSTEM_PROMPT_MIN_CONFIDENCE
                    && stable_memory_hit_for_system_prompt(hit)
            })
            .map(|hit| memory_hit_to_context_block(self.provider_id(), hit))
            .collect::<Vec<_>>();
        Ok(MemoryProviderHookOutcome {
            provider_id: self.provider_id().to_owned(),
            reason_code: if blocks.is_empty() {
                "no_high_confidence_stable_memory".to_owned()
            } else {
                "high_confidence_stable_memory".to_owned()
            },
            degraded: false,
            blocks,
            hits: Vec::new(),
            write: None,
            diagnostics: json!({
                "candidate_count": hits.len(),
                "trust_label": MEMORY_TRUST_LABEL_RETRIEVED,
                "device_id": context.device_id.clone(),
                "policy_scope": context.policy_scope.clone(),
            }),
        })
    }

    async fn prefetch(
        &self,
        context: &MemoryProviderHookContext,
    ) -> Result<MemoryProviderHookOutcome, Status> {
        let query = context.prefetch_query();
        if query.is_empty() {
            return Ok(MemoryProviderHookOutcome::empty(self.provider_id(), "prefetch_no_query"));
        }
        let outcome = self
            .runtime_state
            .search_memory_with_diagnostics(MemorySearchRequest {
                principal: context.principal.clone(),
                channel: context.channel.clone(),
                session_id: context.session_id.clone(),
                query: query.clone(),
                top_k: MEMORY_PROVIDER_PREFETCH_TOP_K,
                min_score: MEMORY_PROVIDER_PREFETCH_MIN_SCORE,
                tags: Vec::new(),
                sources: Vec::new(),
            })
            .await?;
        let hits = outcome
            .hits
            .iter()
            .map(|hit| memory_hit_to_provider_hit(self.provider_id(), hit, query.as_str()))
            .collect::<Vec<_>>();
        Ok(MemoryProviderHookOutcome {
            provider_id: self.provider_id().to_owned(),
            reason_code: "prefetch_completed".to_owned(),
            degraded: outcome.diagnostics.degraded_reason.is_some(),
            blocks: Vec::new(),
            hits,
            write: None,
            diagnostics: json!({
                "search": outcome.diagnostics,
                "device_id": context.device_id.clone(),
                "policy_scope": context.policy_scope.clone(),
            }),
        })
    }

    async fn on_memory_write(
        &self,
        context: &MemoryProviderHookContext,
        candidate: MemoryProviderWriteCandidate,
    ) -> Result<MemoryProviderHookOutcome, Status> {
        let session_id = context.session_id.clone().unwrap_or_else(|| Ulid::new().to_string());
        let classification = classify_memory_write(MemoryWriteClassificationInput {
            principal: context.principal.clone(),
            channel: context.channel.clone(),
            session_id: session_id.clone(),
            scope: candidate.scope,
            content_text: candidate.content_text.clone(),
            confidence: candidate.confidence.unwrap_or(0.75),
            ttl_unix_ms: candidate.ttl_unix_ms,
            provenance: context.provenance.clone(),
            now_unix_ms: current_unix_ms_status()?,
        });
        let lifecycle = if classification.approval_state == MemoryWriteApprovalState::Required {
            None
        } else {
            Some(
                MemoryLifecycleProvider::new(Arc::clone(&self.runtime_state))
                    .retain(MemoryLifecycleRetainRequest {
                        principal: context.principal.clone(),
                        channel: context.channel.clone(),
                        session_id,
                        scope: candidate.scope,
                        source: candidate.source,
                        content_text: candidate.content_text,
                        tags: candidate.tags,
                        confidence: candidate.confidence,
                        ttl_unix_ms: classification.ttl_unix_ms,
                        provenance: context.provenance.clone(),
                    })
                    .await?,
            )
        };
        Ok(MemoryProviderHookOutcome {
            provider_id: self.provider_id().to_owned(),
            reason_code: "memory_write_classified".to_owned(),
            degraded: false,
            blocks: Vec::new(),
            hits: Vec::new(),
            write: Some(MemoryProviderWriteOutcome { classification, lifecycle }),
            diagnostics: json!({}),
        })
    }
}

pub(crate) struct WorkspaceMemoryProvider {
    runtime_state: Arc<GatewayRuntimeState>,
}

impl WorkspaceMemoryProvider {
    #[must_use]
    pub(crate) fn new(runtime_state: Arc<GatewayRuntimeState>) -> Self {
        Self { runtime_state }
    }
}

#[async_trait]
impl MemoryProviderRuntime for WorkspaceMemoryProvider {
    fn provider_id(&self) -> &'static str {
        "workspace"
    }

    fn provider_kind(&self) -> &'static str {
        "workspace_memory"
    }

    async fn status(
        &self,
        context: &MemoryProviderHookContext,
    ) -> Result<MemoryProviderStatus, Status> {
        let documents = self
            .runtime_state
            .list_workspace_documents(WorkspaceDocumentListFilter {
                principal: context.principal.clone(),
                channel: context.channel.clone(),
                agent_id: context.agent_id.clone(),
                prefix: context.workspace_id.clone(),
                include_deleted: false,
                limit: 1,
            })
            .await?;
        Ok(MemoryProviderStatus {
            provider_id: self.provider_id().to_owned(),
            provider_kind: self.provider_kind().to_owned(),
            health: MemoryProviderHealth::Ready,
            index_state: MemoryProviderIndexState::Ready,
            queue_depth: 0,
            last_error: None,
            quota: MemoryProviderQuota {
                max_entries: None,
                max_bytes: None,
                max_prompt_tokens: 1_800,
            },
            policy_limits: MemoryProviderPolicyLimits {
                sensitivity_ceiling: context.sensitivity_ceiling.clone(),
                policy_scope: context.policy_scope.clone(),
                cross_workspace_recall_allowed: false,
                durable_write_requires_approval: true,
            },
            degraded_mode: MemoryProviderDegradedMode {
                active: false,
                reason_code: None,
                fallback: "lexical_workspace_search".to_owned(),
            },
            updated_at_unix_ms: documents
                .first()
                .map_or_else(current_unix_ms, |document| document.updated_at_unix_ms),
        })
    }

    async fn system_prompt_block(
        &self,
        context: &MemoryProviderHookContext,
    ) -> Result<MemoryProviderHookOutcome, Status> {
        let documents = self
            .runtime_state
            .list_workspace_documents(WorkspaceDocumentListFilter {
                principal: context.principal.clone(),
                channel: context.channel.clone(),
                agent_id: context.agent_id.clone(),
                prefix: context.workspace_id.clone(),
                include_deleted: false,
                limit: 16,
            })
            .await?;
        let blocks = documents
            .iter()
            .filter(|document| workspace_document_visible_in_system_prompt(document))
            .map(|document| workspace_document_to_context_block(self.provider_id(), document))
            .collect::<Vec<_>>();
        Ok(MemoryProviderHookOutcome {
            provider_id: self.provider_id().to_owned(),
            reason_code: if blocks.is_empty() {
                "no_scoped_workspace_system_context".to_owned()
            } else {
                "scoped_workspace_system_context".to_owned()
            },
            degraded: false,
            blocks,
            hits: Vec::new(),
            write: None,
            diagnostics: json!({
                "candidate_count": documents.len(),
                "cross_workspace_recall_allowed": false,
                "device_id": context.device_id.clone(),
                "policy_scope": context.policy_scope.clone(),
            }),
        })
    }

    async fn prefetch(
        &self,
        context: &MemoryProviderHookContext,
    ) -> Result<MemoryProviderHookOutcome, Status> {
        let query = context.prefetch_query();
        if query.is_empty() {
            return Ok(MemoryProviderHookOutcome::empty(self.provider_id(), "prefetch_no_query"));
        }
        let outcome = self
            .runtime_state
            .search_workspace_documents_with_diagnostics(WorkspaceSearchRequest {
                principal: context.principal.clone(),
                channel: context.channel.clone(),
                agent_id: context.agent_id.clone(),
                query: query.clone(),
                prefix: context.workspace_id.clone(),
                top_k: MEMORY_PROVIDER_PREFETCH_TOP_K,
                min_score: MEMORY_PROVIDER_PREFETCH_MIN_SCORE,
                include_historical: false,
                include_quarantined: false,
            })
            .await?;
        let hits = outcome
            .hits
            .iter()
            .map(|hit| workspace_hit_to_provider_hit(self.provider_id(), hit, query.as_str()))
            .collect::<Vec<_>>();
        Ok(MemoryProviderHookOutcome {
            provider_id: self.provider_id().to_owned(),
            reason_code: "prefetch_completed".to_owned(),
            degraded: outcome.diagnostics.degraded_reason.is_some(),
            blocks: Vec::new(),
            hits,
            write: None,
            diagnostics: json!({
                "search": outcome.diagnostics,
                "device_id": context.device_id.clone(),
                "policy_scope": context.policy_scope.clone(),
            }),
        })
    }
}

pub(crate) async fn memory_provider_status_snapshot(
    runtime_state: Arc<GatewayRuntimeState>,
    context: &MemoryProviderHookContext,
) -> Result<Vec<MemoryProviderStatus>, Status> {
    let mut statuses = Vec::new();
    for provider in default_memory_providers(runtime_state) {
        statuses.push(provider.status(context).await?);
    }
    Ok(statuses)
}

pub(crate) async fn memory_provider_system_prompt_snapshot(
    runtime_state: Arc<GatewayRuntimeState>,
    context: &MemoryProviderHookContext,
) -> Result<Vec<MemoryProviderHookOutcome>, Status> {
    let mut outcomes = Vec::new();
    for provider in default_memory_providers(runtime_state) {
        outcomes.push(provider.system_prompt_block(context).await?);
    }
    Ok(outcomes)
}

pub(crate) async fn memory_provider_prefetch_snapshot(
    runtime_state: Arc<GatewayRuntimeState>,
    context: &MemoryProviderHookContext,
) -> Result<Vec<MemoryProviderHookOutcome>, Status> {
    let mut outcomes = Vec::new();
    for provider in default_memory_providers(runtime_state) {
        outcomes.push(provider.prefetch(context).await?);
    }
    Ok(outcomes)
}

fn default_memory_providers(
    runtime_state: Arc<GatewayRuntimeState>,
) -> Vec<Box<dyn MemoryProviderRuntime>> {
    vec![
        Box::new(BuiltinJournalMemoryProvider::new(Arc::clone(&runtime_state))),
        Box::new(WorkspaceMemoryProvider::new(runtime_state)),
    ]
}

pub(crate) async fn run_memory_provider_reindex(
    runtime_state: Arc<GatewayRuntimeState>,
    batch_size: usize,
) -> Result<MemoryProviderReindexOutcome, Status> {
    let outcome = runtime_state.run_memory_embeddings_backfill(batch_size).await?;
    Ok(MemoryProviderReindexOutcome {
        job_id: Ulid::new().to_string(),
        provider_id: "builtin_journal".to_owned(),
        state: if outcome.is_complete() { "completed" } else { "running" }.to_owned(),
        cancel_supported: true,
        cancelled: false,
        cancel_reason: None,
        ran_at_unix_ms: outcome.ran_at_unix_ms,
        progress: MemoryProviderReindexProgress {
            batch_size: outcome.batch_size,
            scanned_count: outcome.scanned_count,
            updated_count: outcome.updated_count,
            pending_count: outcome.pending_count,
            complete: outcome.is_complete(),
            target_model_id: outcome.target_model_id.clone(),
            target_dims: outcome.target_dims,
            target_version: outcome.target_version,
        },
        artifact_log: vec![format!(
            "memory embeddings backfill scanned={} updated={} pending={}",
            outcome.scanned_count, outcome.updated_count, outcome.pending_count
        )],
    })
}

pub(crate) fn memory_hit_to_provider_hit(
    provider_id: &str,
    hit: &MemorySearchHit,
    query: &str,
) -> MemoryProviderHit {
    MemoryProviderHit {
        hit_id: format!("{provider_id}:{}", hit.item.memory_id),
        provider_id: provider_id.to_owned(),
        title: format!("Memory {}", hit.item.memory_id),
        snippet: redact_memory_text_for_output(hit.snippet.as_str()),
        score: MemoryProviderScoreBreakdown {
            lexical_score: hit.breakdown.lexical_score,
            semantic_score: hit.breakdown.vector_score,
            recency_score: hit.breakdown.recency_score,
            source_quality_score: hit.breakdown.source_quality_score,
            sensitivity_penalty: sensitivity_penalty(hit.snippet.as_str()),
            exact_phrase_match: exact_phrase_match(query, hit.snippet.as_str()),
            final_score: hit.breakdown.final_score,
        },
        citation: MemoryProviderCitation {
            source_kind: "memory".to_owned(),
            source_ref: hit.item.memory_id.clone(),
            session_id: hit.item.session_id.clone(),
            run_id: None,
            tape_seq: None,
            workspace_path: None,
            artifact_id: None,
        },
        trust_label: MEMORY_TRUST_LABEL_RETRIEVED.to_owned(),
        confidence: hit.item.confidence.unwrap_or(0.0),
        injection_scan_status: "clean".to_owned(),
    }
}

pub(crate) fn workspace_hit_to_provider_hit(
    provider_id: &str,
    hit: &WorkspaceSearchHit,
    query: &str,
) -> MemoryProviderHit {
    MemoryProviderHit {
        hit_id: format!("{provider_id}:{}:{}", hit.document.document_id, hit.chunk_index),
        provider_id: provider_id.to_owned(),
        title: hit.document.title.clone(),
        snippet: redact_memory_text_for_output(hit.snippet.as_str()),
        score: MemoryProviderScoreBreakdown {
            lexical_score: hit.breakdown.lexical_score,
            semantic_score: hit.breakdown.vector_score,
            recency_score: hit.breakdown.recency_score,
            source_quality_score: hit.breakdown.source_quality_score,
            sensitivity_penalty: sensitivity_penalty(hit.snippet.as_str()),
            exact_phrase_match: exact_phrase_match(query, hit.snippet.as_str()),
            final_score: hit.breakdown.final_score,
        },
        citation: MemoryProviderCitation {
            source_kind: "workspace_document".to_owned(),
            source_ref: hit.document.document_id.clone(),
            session_id: hit.document.latest_session_id.clone(),
            run_id: None,
            tape_seq: None,
            workspace_path: Some(hit.document.path.clone()),
            artifact_id: None,
        },
        trust_label: "workspace_memory".to_owned(),
        confidence: if hit.document.pinned { 0.92 } else { 0.78 },
        injection_scan_status: hit.document.risk_state.clone(),
    }
}

pub(crate) fn explain_provider_hit(hit: &MemoryProviderHit) -> Value {
    json!({
        "hit_id": hit.hit_id,
        "provider_id": hit.provider_id,
        "source_kind": hit.citation.source_kind,
        "source_ref": hit.citation.source_ref,
        "score": hit.score,
        "trust_label": hit.trust_label,
        "confidence": hit.confidence,
        "injection_scan_status": hit.injection_scan_status,
    })
}

fn memory_hit_to_context_block(
    provider_id: &str,
    hit: &MemorySearchHit,
) -> MemoryProviderContextBlock {
    MemoryProviderContextBlock {
        block_id: format!("{provider_id}:{}", hit.item.memory_id),
        provider_id: provider_id.to_owned(),
        content: redact_memory_text_for_output(hit.item.content_text.as_str()),
        trust_label: MEMORY_TRUST_LABEL_RETRIEVED.to_owned(),
        source_refs: vec![MemoryWriteSourceRef {
            source_kind: "memory".to_owned(),
            source_id: hit.item.memory_id.clone(),
            session_id: hit.item.session_id.clone(),
            run_id: None,
            tape_seq: None,
            artifact_id: None,
        }],
        confidence: hit.item.confidence.unwrap_or(0.0),
        injection_scan_status: "clean".to_owned(),
        sensitivity: "normal".to_owned(),
    }
}

fn workspace_document_to_context_block(
    provider_id: &str,
    document: &WorkspaceDocumentRecord,
) -> MemoryProviderContextBlock {
    let scan = scan_workspace_content_for_prompt_injection(document.content_text.as_str());
    MemoryProviderContextBlock {
        block_id: format!("{provider_id}:{}", document.document_id),
        provider_id: provider_id.to_owned(),
        content: redact_memory_text_for_output(document.content_text.as_str()),
        trust_label: "workspace_memory".to_owned(),
        source_refs: vec![MemoryWriteSourceRef {
            source_kind: "workspace_document".to_owned(),
            source_id: document.document_id.clone(),
            session_id: document.latest_session_id.clone(),
            run_id: None,
            tape_seq: None,
            artifact_id: None,
        }],
        confidence: if document.pinned { 0.92 } else { 0.84 },
        injection_scan_status: scan.state.as_str().to_owned(),
        sensitivity: "normal".to_owned(),
    }
}

fn stable_memory_hit_for_system_prompt(hit: &MemorySearchHit) -> bool {
    hit.item.tags.iter().any(|tag| {
        matches!(
            tag.as_str(),
            "category:preferences"
                | "category:workflow_rules"
                | "category:procedure"
                | "category:constraint"
        )
    }) || {
        let lower = hit.item.content_text.to_ascii_lowercase();
        lower.contains("prefer") || lower.contains("workflow") || lower.contains("always ")
    }
}

fn workspace_document_visible_in_system_prompt(document: &WorkspaceDocumentRecord) -> bool {
    document.state == "active"
        && document.risk_state == "clean"
        && (document.pinned || document.prompt_binding == "system_candidate")
}

fn exact_phrase_match(query: &str, snippet: &str) -> f64 {
    let query = query.trim().to_ascii_lowercase();
    if query.is_empty() {
        return 0.0;
    }
    if snippet.to_ascii_lowercase().contains(query.as_str()) {
        1.0
    } else {
        0.0
    }
}

fn sensitivity_penalty(snippet: &str) -> f64 {
    let lower = snippet.to_ascii_lowercase();
    if ["api key", "password", "private key", "secret", "session token", "token"]
        .iter()
        .any(|pattern| lower.contains(pattern))
    {
        0.25
    } else {
        0.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn context() -> MemoryProviderHookContext {
        MemoryProviderHookContext {
            principal: "user:alice".to_owned(),
            device_id: "01H00000000000000000000000".to_owned(),
            channel: Some("discord:channel:one".to_owned()),
            session_id: Some("01H00000000000000000000001".to_owned()),
            agent_id: None,
            workspace_id: Some("project".to_owned()),
            sensitivity_ceiling: "normal".to_owned(),
            policy_scope: "test".to_owned(),
            objective: Some("ship auth workflow".to_owned()),
            active_files: vec!["crates/palyra-auth/src/lib.rs".to_owned()],
            recent_context: Vec::new(),
            provenance: json!({ "run_id": "run-1", "seq": 4 }),
        }
    }

    #[test]
    fn prefetch_query_combines_objective_and_files() {
        let context = context();
        assert_eq!(context.prefetch_query(), "ship auth workflow crates/palyra-auth/src/lib.rs");
    }

    #[test]
    fn provider_score_exposes_exact_phrase_and_sensitivity_penalty() {
        assert_eq!(exact_phrase_match("auth workflow", "The auth workflow is documented."), 1.0);
        assert_eq!(exact_phrase_match("missing", "The auth workflow is documented."), 0.0);
        assert!(sensitivity_penalty("contains api key material") > 0.0);
    }
}
