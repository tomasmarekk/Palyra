use std::sync::{Arc, RwLock};

use anyhow::Result;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{
    journal::{
        HashMemoryEmbeddingProvider, JournalError, JournalStore, MemoryEmbeddingProvider,
        MemoryEmbeddingsMode, MemoryScoreBreakdown, MemorySearchCandidateOutcome,
        MemorySearchCandidateRecord, MemorySearchHit, MemorySearchRequest, MemorySource,
        OrchestratorCheckpointRecord, OrchestratorCompactionArtifactRecord,
        WorkspaceScoreBreakdown, WorkspaceSearchCandidateOutcome, WorkspaceSearchCandidateRecord,
        WorkspaceSearchHit, WorkspaceSearchRequest,
    },
    model_provider::{
        build_embeddings_provider, ModelProviderConfig, ModelProviderCredentialSource,
        ModelProviderKind, ProviderModelRole,
    },
};

const DEFAULT_PRODUCTION_EMBEDDINGS_MODEL_ID: &str = "text-embedding-3-small";
const DEFAULT_PRODUCTION_EMBEDDINGS_DIMS: usize = 1_536;
const DEFAULT_EMBEDDINGS_BATCH_LIMIT: usize = 64;
const DEFAULT_BACKFILL_STRATEGY: &str = "lazy_reindex";
const DEFAULT_EXTERNAL_INDEX_FRESHNESS_SLO_MS: u64 = 60_000;
const DEFAULT_EXTERNAL_QUERY_LATENCY_SLO_MS: u64 = 250;
const DEFAULT_EXTERNAL_DEGRADED_FALLBACK_RATE_BPS: u32 = 500;
const DEFAULT_EXTERNAL_RECONCILIATION_SUCCESS_RATE_BPS: u32 = 9_500;

const DEFAULT_RECALL_PHRASE_MATCH_BONUS_BPS: u16 = 2_000;
const DEFAULT_LEGACY_MIN_RECENCY_BPS: u16 = 1_500;
const DEFAULT_LEGACY_MIN_SOURCE_QUALITY_BPS: u16 = 2_000;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum RetrievalBackendKind {
    JournalSqliteFts,
    ExternalDerivedPreview,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum RetrievalBackendState {
    Ready,
    Degraded,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct RetrievalBackendCapabilities {
    pub(crate) lexical_search: bool,
    pub(crate) vector_search: bool,
    pub(crate) lexical_candidate_generation: bool,
    pub(crate) vector_candidate_generation: bool,
    pub(crate) hybrid_fusion: bool,
    pub(crate) source_aware_fusion: bool,
    pub(crate) branch_diagnostics: bool,
    pub(crate) transcript_fusion: bool,
    pub(crate) checkpoint_fusion: bool,
    pub(crate) compaction_fusion: bool,
    pub(crate) lazy_reindex: bool,
    pub(crate) batch_backfill: bool,
    pub(crate) external_derived_index: bool,
    pub(crate) journal_source_of_truth: bool,
    pub(crate) journal_fallback: bool,
    pub(crate) drift_detection: bool,
    pub(crate) async_indexer: bool,
    pub(crate) scale_slos: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct RetrievalBackendSnapshot {
    pub(crate) kind: RetrievalBackendKind,
    pub(crate) state: RetrievalBackendState,
    pub(crate) reason: String,
    pub(crate) capabilities: RetrievalBackendCapabilities,
    pub(crate) externalization: RetrievalExternalizationPolicySnapshot,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) external_index: Option<ExternalRetrievalIndexSnapshot>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum RetrievalExternalizationClass {
    DerivedIndex,
    JournalOnly,
    ArtifactOnly,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct RetrievalDerivedFieldPolicy {
    pub(crate) field: String,
    pub(crate) classification: RetrievalExternalizationClass,
    pub(crate) reason: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct RetrievalExternalizationPolicySnapshot {
    pub(crate) source_of_truth: String,
    pub(crate) derived_index_allowed: bool,
    pub(crate) replay_requires_live_external_index: bool,
    pub(crate) field_policy: Vec<RetrievalDerivedFieldPolicy>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct ExternalRetrievalIndexSnapshot {
    pub(crate) provider: String,
    pub(crate) state: RetrievalBackendState,
    pub(crate) reason: String,
    pub(crate) indexed_memory_items: u64,
    pub(crate) indexed_workspace_chunks: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) last_indexed_at_unix_ms: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) journal_watermark_unix_ms: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) freshness_lag_ms: Option<u64>,
    pub(crate) drift_count: u64,
    pub(crate) pending_reconciliation_count: u64,
    pub(crate) scale_slos: ExternalRetrievalScaleSloSnapshot,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) last_error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct ExternalRetrievalScaleSloSnapshot {
    pub(crate) freshness_lag_ms: u64,
    pub(crate) freshness_target_ms: u64,
    pub(crate) freshness_ok: bool,
    pub(crate) query_latency_p95_ms: u64,
    pub(crate) query_latency_target_ms: u64,
    pub(crate) query_latency_ok: bool,
    pub(crate) degraded_fallback_rate_bps: u32,
    pub(crate) degraded_fallback_target_bps: u32,
    pub(crate) degraded_fallback_ok: bool,
    pub(crate) reconciliation_success_rate_bps: u32,
    pub(crate) reconciliation_success_target_bps: u32,
    pub(crate) reconciliation_success_ok: bool,
    pub(crate) preview_gate_state: String,
}

impl Default for ExternalRetrievalScaleSloSnapshot {
    fn default() -> Self {
        Self {
            freshness_lag_ms: u64::MAX,
            freshness_target_ms: DEFAULT_EXTERNAL_INDEX_FRESHNESS_SLO_MS,
            freshness_ok: false,
            query_latency_p95_ms: 0,
            query_latency_target_ms: DEFAULT_EXTERNAL_QUERY_LATENCY_SLO_MS,
            query_latency_ok: true,
            degraded_fallback_rate_bps: 10_000,
            degraded_fallback_target_bps: DEFAULT_EXTERNAL_DEGRADED_FALLBACK_RATE_BPS,
            degraded_fallback_ok: false,
            reconciliation_success_rate_bps: 0,
            reconciliation_success_target_bps: DEFAULT_EXTERNAL_RECONCILIATION_SUCCESS_RATE_BPS,
            reconciliation_success_ok: false,
            preview_gate_state: "preview_blocked".to_owned(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct ExternalRetrievalIndexerOutcome {
    pub(crate) ran_at_unix_ms: i64,
    pub(crate) batch_size: usize,
    pub(crate) attempt_count: u32,
    pub(crate) indexed_memory_items: u64,
    pub(crate) indexed_workspace_chunks: u64,
    pub(crate) pending_memory_items: u64,
    pub(crate) pending_workspace_chunks: u64,
    pub(crate) journal_watermark_unix_ms: i64,
    pub(crate) checkpoint_committed: bool,
    pub(crate) complete: bool,
    pub(crate) retry_policy: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct ExternalRetrievalDriftReport {
    pub(crate) checked_at_unix_ms: i64,
    pub(crate) indexed_memory_items: u64,
    pub(crate) journal_memory_items: u64,
    pub(crate) memory_drift: i64,
    pub(crate) indexed_workspace_chunks: u64,
    pub(crate) journal_workspace_chunks: u64,
    pub(crate) workspace_chunk_drift: i64,
    pub(crate) freshness_lag_ms: u64,
    pub(crate) drift_count: u64,
    pub(crate) reconciliation_required: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct ExternalRetrievalReconciliationOutcome {
    pub(crate) checked_at_unix_ms: i64,
    pub(crate) drift_before: ExternalRetrievalDriftReport,
    pub(crate) indexer: ExternalRetrievalIndexerOutcome,
    pub(crate) drift_after: ExternalRetrievalDriftReport,
    pub(crate) success: bool,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum RetrievalSourceProfileKind {
    Memory,
    WorkspaceDocument,
    Transcript,
    Checkpoint,
    CompactionArtifact,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct RetrievalBackendConfig {
    pub(crate) kind: RetrievalBackendKind,
}

impl Default for RetrievalBackendConfig {
    fn default() -> Self {
        Self { kind: RetrievalBackendKind::JournalSqliteFts }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct RetrievalSourceScoringProfile {
    pub(crate) lexical_bps: u16,
    pub(crate) vector_bps: u16,
    pub(crate) recency_bps: u16,
    pub(crate) source_quality_bps: u16,
    pub(crate) min_recency_bps: u16,
    pub(crate) min_source_quality_bps: u16,
    pub(crate) pinned_bonus_bps: u16,
}

impl RetrievalSourceScoringProfile {
    fn validate(&self, label: &str) -> Result<()> {
        let total = u32::from(self.lexical_bps)
            + u32::from(self.vector_bps)
            + u32::from(self.recency_bps)
            + u32::from(self.source_quality_bps);
        anyhow::ensure!(
            total == 10_000,
            "{label} weights must sum to 10000 basis points, got {total}"
        );
        anyhow::ensure!(self.min_recency_bps <= 10_000, "{label}.min_recency_bps must be <= 10000");
        anyhow::ensure!(
            self.min_source_quality_bps <= 10_000,
            "{label}.min_source_quality_bps must be <= 10000"
        );
        anyhow::ensure!(
            self.pinned_bonus_bps <= 10_000,
            "{label}.pinned_bonus_bps must be <= 10000"
        );
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct RetrievalScoringConfig {
    pub(crate) phrase_match_bonus_bps: u16,
    pub(crate) default_profile: RetrievalSourceScoringProfile,
    pub(crate) memory: RetrievalSourceScoringProfile,
    pub(crate) workspace: RetrievalSourceScoringProfile,
    pub(crate) transcript: RetrievalSourceScoringProfile,
    pub(crate) checkpoint: RetrievalSourceScoringProfile,
    pub(crate) compaction: RetrievalSourceScoringProfile,
}

impl RetrievalScoringConfig {
    pub(crate) fn validate(&self) -> Result<()> {
        anyhow::ensure!(
            self.phrase_match_bonus_bps <= 10_000,
            "memory.retrieval.scoring.phrase_match_bonus_bps must be <= 10000"
        );
        self.default_profile.validate("memory.retrieval.scoring.default_profile")?;
        self.memory.validate("memory.retrieval.scoring.memory")?;
        self.workspace.validate("memory.retrieval.scoring.workspace")?;
        self.transcript.validate("memory.retrieval.scoring.transcript")?;
        self.checkpoint.validate("memory.retrieval.scoring.checkpoint")?;
        self.compaction.validate("memory.retrieval.scoring.compaction")?;
        Ok(())
    }

    #[must_use]
    pub(crate) fn profile_for(
        &self,
        kind: RetrievalSourceProfileKind,
    ) -> &RetrievalSourceScoringProfile {
        match kind {
            RetrievalSourceProfileKind::Memory => &self.memory,
            RetrievalSourceProfileKind::WorkspaceDocument => &self.workspace,
            RetrievalSourceProfileKind::Transcript => &self.transcript,
            RetrievalSourceProfileKind::Checkpoint => &self.checkpoint,
            RetrievalSourceProfileKind::CompactionArtifact => &self.compaction,
        }
    }
}

impl Default for RetrievalScoringConfig {
    fn default() -> Self {
        let default_profile = RetrievalSourceScoringProfile {
            lexical_bps: 4_200,
            vector_bps: 2_400,
            recency_bps: 1_600,
            source_quality_bps: 1_800,
            min_recency_bps: DEFAULT_LEGACY_MIN_RECENCY_BPS,
            min_source_quality_bps: DEFAULT_LEGACY_MIN_SOURCE_QUALITY_BPS,
            pinned_bonus_bps: 0,
        };
        Self {
            phrase_match_bonus_bps: DEFAULT_RECALL_PHRASE_MATCH_BONUS_BPS,
            memory: RetrievalSourceScoringProfile {
                lexical_bps: 5_500,
                vector_bps: 3_500,
                recency_bps: 1_000,
                source_quality_bps: 0,
                min_recency_bps: DEFAULT_LEGACY_MIN_RECENCY_BPS,
                min_source_quality_bps: DEFAULT_LEGACY_MIN_SOURCE_QUALITY_BPS,
                pinned_bonus_bps: 0,
            },
            workspace: RetrievalSourceScoringProfile {
                lexical_bps: 5_500,
                vector_bps: 3_500,
                recency_bps: 1_000,
                source_quality_bps: 0,
                min_recency_bps: DEFAULT_LEGACY_MIN_RECENCY_BPS,
                min_source_quality_bps: DEFAULT_LEGACY_MIN_SOURCE_QUALITY_BPS,
                pinned_bonus_bps: 500,
            },
            transcript: default_profile.clone(),
            checkpoint: default_profile.clone(),
            compaction: default_profile.clone(),
            default_profile,
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct RetrievalRuntimeConfig {
    pub(crate) backend: RetrievalBackendConfig,
    pub(crate) scoring: RetrievalScoringConfig,
}

impl RetrievalRuntimeConfig {
    pub(crate) fn validate(&self) -> Result<()> {
        self.scoring.validate()
    }
}

#[must_use]
pub(crate) fn retrieval_externalization_policy() -> RetrievalExternalizationPolicySnapshot {
    RetrievalExternalizationPolicySnapshot {
        source_of_truth: "journal_store_and_artifact_storage".to_owned(),
        derived_index_allowed: true,
        replay_requires_live_external_index: false,
        field_policy: vec![
            RetrievalDerivedFieldPolicy {
                field: "memory_items.memory_ulid".to_owned(),
                classification: RetrievalExternalizationClass::DerivedIndex,
                reason: "opaque ids are required to join external candidates back to JournalStore evidence"
                    .to_owned(),
            },
            RetrievalDerivedFieldPolicy {
                field: "memory_items.normalized_search_text".to_owned(),
                classification: RetrievalExternalizationClass::DerivedIndex,
                reason: "bounded normalized text may be projected for lexical/vector candidate generation"
                    .to_owned(),
            },
            RetrievalDerivedFieldPolicy {
                field: "memory_vectors.embedding_vector".to_owned(),
                classification: RetrievalExternalizationClass::DerivedIndex,
                reason: "embeddings are derived from journal memory content and can be rebuilt"
                    .to_owned(),
            },
            RetrievalDerivedFieldPolicy {
                field: "workspace_document_chunks.normalized_search_text".to_owned(),
                classification: RetrievalExternalizationClass::DerivedIndex,
                reason: "workspace chunk excerpts are derived projections for retrieval only"
                    .to_owned(),
            },
            RetrievalDerivedFieldPolicy {
                field: "orchestrator_tape.payload_json".to_owned(),
                classification: RetrievalExternalizationClass::JournalOnly,
                reason: "full tape payloads remain audit evidence and must not become external index authority"
                    .to_owned(),
            },
            RetrievalDerivedFieldPolicy {
                field: "approvals.*".to_owned(),
                classification: RetrievalExternalizationClass::JournalOnly,
                reason: "approval records and hash-chain evidence stay in JournalStore"
                    .to_owned(),
            },
            RetrievalDerivedFieldPolicy {
                field: "artifact_storage.bytes".to_owned(),
                classification: RetrievalExternalizationClass::ArtifactOnly,
                reason: "large artifacts remain in artifact storage; indexes may keep only ids and bounded snippets"
                    .to_owned(),
            },
        ],
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum MemoryEmbeddingsPosture {
    ProductionDefault,
    DegradedOffline,
    DegradedConfigFallback,
    DegradedProviderFallback,
    DegradedUnsupportedProvider,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct MemoryEmbeddingsRuntimeProfile {
    pub(crate) posture: MemoryEmbeddingsPosture,
    pub(crate) desired_model_id: Option<String>,
    pub(crate) active_model_id: String,
    pub(crate) active_dims: usize,
    pub(crate) degraded_reason_code: Option<String>,
    pub(crate) warning: Option<String>,
    pub(crate) production_default_active: bool,
    pub(crate) backfill_strategy: String,
    pub(crate) batch_limit: usize,
    pub(crate) request_timeout_ms: u64,
    pub(crate) retry_max: u32,
}

impl MemoryEmbeddingsRuntimeProfile {
    #[must_use]
    pub(crate) fn legacy_from_provider(model_name: &str, dimensions: usize) -> Self {
        let hash_fallback = model_name == HashMemoryEmbeddingProvider::default().model_name();
        if hash_fallback {
            return Self {
                posture: MemoryEmbeddingsPosture::DegradedConfigFallback,
                desired_model_id: None,
                active_model_id: model_name.to_owned(),
                active_dims: dimensions.max(1),
                degraded_reason_code: Some("legacy_hash_fallback".to_owned()),
                warning: Some(
                    "retrieval embeddings are using the legacy hash fallback because no production embedding selection was recorded"
                        .to_owned(),
                ),
                production_default_active: false,
                backfill_strategy: DEFAULT_BACKFILL_STRATEGY.to_owned(),
                batch_limit: DEFAULT_EMBEDDINGS_BATCH_LIMIT,
                request_timeout_ms: 0,
                retry_max: 0,
            };
        }

        Self {
            posture: MemoryEmbeddingsPosture::ProductionDefault,
            desired_model_id: Some(model_name.to_owned()),
            active_model_id: model_name.to_owned(),
            active_dims: dimensions.max(1),
            degraded_reason_code: None,
            warning: None,
            production_default_active: true,
            backfill_strategy: DEFAULT_BACKFILL_STRATEGY.to_owned(),
            batch_limit: DEFAULT_EMBEDDINGS_BATCH_LIMIT,
            request_timeout_ms: 15_000,
            retry_max: 2,
        }
    }

    #[must_use]
    pub(crate) fn mode(&self) -> MemoryEmbeddingsMode {
        if self.production_default_active {
            MemoryEmbeddingsMode::ModelProvider
        } else {
            MemoryEmbeddingsMode::HashFallback
        }
    }

    fn degraded_hash_fallback(
        posture: MemoryEmbeddingsPosture,
        desired_model_id: Option<String>,
        degraded_reason_code: &str,
        warning: String,
        dimensions: usize,
        request_timeout_ms: u64,
        retry_max: u32,
    ) -> MemoryEmbeddingRuntimeSelection {
        MemoryEmbeddingRuntimeSelection {
            provider: Arc::new(HashMemoryEmbeddingProvider::with_dimensions(dimensions.max(1))),
            profile: MemoryEmbeddingsRuntimeProfile {
                posture,
                desired_model_id,
                active_model_id: HashMemoryEmbeddingProvider::default().model_name().to_owned(),
                active_dims: dimensions.max(1),
                degraded_reason_code: Some(degraded_reason_code.to_owned()),
                warning: Some(warning),
                production_default_active: false,
                backfill_strategy: DEFAULT_BACKFILL_STRATEGY.to_owned(),
                batch_limit: DEFAULT_EMBEDDINGS_BATCH_LIMIT,
                request_timeout_ms,
                retry_max,
            },
        }
    }
}

#[derive(Clone)]
pub(crate) struct MemoryEmbeddingRuntimeSelection {
    pub(crate) provider: Arc<dyn MemoryEmbeddingProvider>,
    pub(crate) profile: MemoryEmbeddingsRuntimeProfile,
}

pub(crate) trait RetrievalBackend: Send + Sync {
    fn snapshot(
        &self,
        config: &RetrievalRuntimeConfig,
        embeddings_status: &crate::journal::MemoryEmbeddingsStatus,
    ) -> RetrievalBackendSnapshot;

    fn search_memory_candidates(
        &self,
        store: &JournalStore,
        request: &MemorySearchRequest,
        config: &RetrievalRuntimeConfig,
    ) -> Result<Vec<MemorySearchCandidateRecord>, JournalError>;

    fn search_memory_candidate_outcome(
        &self,
        store: &JournalStore,
        request: &MemorySearchRequest,
        config: &RetrievalRuntimeConfig,
    ) -> Result<MemorySearchCandidateOutcome, JournalError>;

    fn search_workspace_candidate_outcome(
        &self,
        store: &JournalStore,
        request: &WorkspaceSearchRequest,
        config: &RetrievalRuntimeConfig,
    ) -> Result<WorkspaceSearchCandidateOutcome, JournalError>;
}

#[derive(Debug, Default, Clone, Copy)]
pub(crate) struct JournalRetrievalBackend;

impl RetrievalBackend for JournalRetrievalBackend {
    fn snapshot(
        &self,
        config: &RetrievalRuntimeConfig,
        embeddings_status: &crate::journal::MemoryEmbeddingsStatus,
    ) -> RetrievalBackendSnapshot {
        let degraded = !embeddings_status.production_default_active;
        RetrievalBackendSnapshot {
            kind: config.backend.kind,
            state: if degraded {
                RetrievalBackendState::Degraded
            } else {
                RetrievalBackendState::Ready
            },
            reason: if degraded {
                embeddings_status.warning.clone().unwrap_or_else(|| {
                    "retrieval is operating in an explicitly degraded embeddings mode".to_owned()
                })
            } else {
                format!("journal-backed retrieval is ready with {} scoring profiles", 5)
            },
            capabilities: RetrievalBackendCapabilities {
                lexical_search: true,
                vector_search: true,
                lexical_candidate_generation: true,
                vector_candidate_generation: true,
                hybrid_fusion: true,
                source_aware_fusion: true,
                branch_diagnostics: true,
                transcript_fusion: true,
                checkpoint_fusion: true,
                compaction_fusion: true,
                lazy_reindex: true,
                batch_backfill: true,
                external_derived_index: false,
                journal_source_of_truth: true,
                journal_fallback: false,
                drift_detection: false,
                async_indexer: false,
                scale_slos: false,
            },
            externalization: retrieval_externalization_policy(),
            external_index: None,
        }
    }

    fn search_memory_candidates(
        &self,
        store: &JournalStore,
        request: &MemorySearchRequest,
        _config: &RetrievalRuntimeConfig,
    ) -> Result<Vec<MemorySearchCandidateRecord>, JournalError> {
        store.search_memory_candidates(request)
    }

    fn search_memory_candidate_outcome(
        &self,
        store: &JournalStore,
        request: &MemorySearchRequest,
        _config: &RetrievalRuntimeConfig,
    ) -> Result<MemorySearchCandidateOutcome, JournalError> {
        store.search_memory_candidate_outcome(request)
    }

    fn search_workspace_candidate_outcome(
        &self,
        store: &JournalStore,
        request: &WorkspaceSearchRequest,
        _config: &RetrievalRuntimeConfig,
    ) -> Result<WorkspaceSearchCandidateOutcome, JournalError> {
        store.search_workspace_candidate_outcome(request)
    }
}

pub(crate) trait ExternalRetrievalIndex: Send + Sync {
    fn snapshot(&self) -> ExternalRetrievalIndexSnapshot;

    fn search_memory_candidate_outcome(
        &self,
        store: &JournalStore,
        request: &MemorySearchRequest,
    ) -> Result<Option<MemorySearchCandidateOutcome>, JournalError>;

    fn search_workspace_candidate_outcome(
        &self,
        store: &JournalStore,
        request: &WorkspaceSearchRequest,
    ) -> Result<Option<WorkspaceSearchCandidateOutcome>, JournalError>;
}

#[derive(Debug, Clone)]
struct ExternalRetrievalRuntimeState {
    snapshot: ExternalRetrievalIndexSnapshot,
    search_attempts: u64,
    degraded_fallbacks: u64,
    query_latency_samples_ms: Vec<u64>,
    reconciliation_attempts: u64,
    reconciliation_successes: u64,
}

#[derive(Debug)]
pub(crate) struct ExternalRetrievalRuntime {
    state: RwLock<ExternalRetrievalRuntimeState>,
}

impl Default for ExternalRetrievalRuntime {
    fn default() -> Self {
        Self {
            state: RwLock::new(ExternalRetrievalRuntimeState {
                snapshot: ExternalRetrievalIndexSnapshot {
                    provider: "memory_external_preview".to_owned(),
                    state: RetrievalBackendState::Degraded,
                    reason:
                        "external retrieval index has not completed a journal-derived checkpoint"
                            .to_owned(),
                    indexed_memory_items: 0,
                    indexed_workspace_chunks: 0,
                    last_indexed_at_unix_ms: None,
                    journal_watermark_unix_ms: None,
                    freshness_lag_ms: None,
                    drift_count: 0,
                    pending_reconciliation_count: 0,
                    scale_slos: ExternalRetrievalScaleSloSnapshot::default(),
                    last_error: None,
                },
                search_attempts: 0,
                degraded_fallbacks: 0,
                query_latency_samples_ms: Vec::new(),
                reconciliation_attempts: 0,
                reconciliation_successes: 0,
            }),
        }
    }
}

impl ExternalRetrievalRuntime {
    #[must_use]
    pub(crate) fn snapshot(&self) -> ExternalRetrievalIndexSnapshot {
        self.state.read().unwrap_or_else(|error| error.into_inner()).snapshot.clone()
    }

    pub(crate) fn run_indexer(
        &self,
        store: &JournalStore,
        batch_size: usize,
        attempt_count: u32,
        ran_at_unix_ms: i64,
    ) -> Result<ExternalRetrievalIndexerOutcome, JournalError> {
        let memory = store.memory_embeddings_status()?;
        let workspace = store.workspace_retrieval_index_status()?;
        let mut guard = self.state.write().unwrap_or_else(|error| error.into_inner());
        let effective_batch = batch_size.max(1) as u64;
        let next_memory = next_indexed_count(
            guard.snapshot.indexed_memory_items,
            memory.total_count,
            effective_batch,
        );
        let next_workspace = next_indexed_count(
            guard.snapshot.indexed_workspace_chunks,
            workspace.chunk_count,
            effective_batch,
        );
        let pending_memory_items = memory.total_count.saturating_sub(next_memory);
        let pending_workspace_chunks = workspace.chunk_count.saturating_sub(next_workspace);
        let pending_total = pending_memory_items.saturating_add(pending_workspace_chunks);
        let complete = pending_total == 0;
        let freshness_lag_ms = if complete {
            0
        } else {
            DEFAULT_EXTERNAL_INDEX_FRESHNESS_SLO_MS.saturating_add(pending_total)
        };
        guard.snapshot.indexed_memory_items = next_memory;
        guard.snapshot.indexed_workspace_chunks = next_workspace;
        guard.snapshot.last_indexed_at_unix_ms = Some(ran_at_unix_ms);
        guard.snapshot.journal_watermark_unix_ms = Some(ran_at_unix_ms);
        guard.snapshot.freshness_lag_ms = Some(freshness_lag_ms);
        guard.snapshot.drift_count = pending_total;
        guard.snapshot.pending_reconciliation_count = pending_total;
        guard.snapshot.state =
            if complete { RetrievalBackendState::Ready } else { RetrievalBackendState::Degraded };
        guard.snapshot.reason = if complete {
            "external retrieval index checkpoint is caught up with journal-derived memory and workspace projections"
                .to_owned()
        } else {
            format!(
                "external retrieval index checkpoint is behind journal by {pending_total} derived records"
            )
        };
        guard.snapshot.last_error = None;
        recompute_external_slos(&mut guard);
        Ok(ExternalRetrievalIndexerOutcome {
            ran_at_unix_ms,
            batch_size,
            attempt_count,
            indexed_memory_items: next_memory,
            indexed_workspace_chunks: next_workspace,
            pending_memory_items,
            pending_workspace_chunks,
            journal_watermark_unix_ms: ran_at_unix_ms,
            checkpoint_committed: true,
            complete,
            retry_policy: "max_attempts=3, backoff_ms=250, idempotent_checkpoint".to_owned(),
        })
    }

    pub(crate) fn detect_drift(
        &self,
        store: &JournalStore,
        checked_at_unix_ms: i64,
    ) -> Result<ExternalRetrievalDriftReport, JournalError> {
        let memory = store.memory_embeddings_status()?;
        let workspace = store.workspace_retrieval_index_status()?;
        let mut guard = self.state.write().unwrap_or_else(|error| error.into_inner());
        let report = external_drift_report(
            &guard.snapshot,
            memory.total_count,
            workspace.chunk_count,
            checked_at_unix_ms,
        );
        guard.snapshot.indexed_memory_items = report.indexed_memory_items;
        guard.snapshot.indexed_workspace_chunks = report.indexed_workspace_chunks;
        guard.snapshot.freshness_lag_ms = Some(report.freshness_lag_ms);
        guard.snapshot.drift_count = report.drift_count;
        guard.snapshot.pending_reconciliation_count = report.drift_count;
        if report.reconciliation_required {
            guard.snapshot.state = RetrievalBackendState::Degraded;
            guard.snapshot.reason = format!(
                "external retrieval index checkpoint is behind journal by {} derived records",
                report.drift_count
            );
        } else if guard.snapshot.last_indexed_at_unix_ms.is_some() {
            guard.snapshot.state = RetrievalBackendState::Ready;
            guard.snapshot.reason =
                "external retrieval index checkpoint is caught up with journal-derived memory and workspace projections"
                    .to_owned();
        }
        recompute_external_slos(&mut guard);
        Ok(report)
    }

    pub(crate) fn reconcile(
        &self,
        store: &JournalStore,
        batch_size: usize,
        checked_at_unix_ms: i64,
    ) -> Result<ExternalRetrievalReconciliationOutcome, JournalError> {
        let drift_before = self.detect_drift(store, checked_at_unix_ms)?;
        {
            let mut guard = self.state.write().unwrap_or_else(|error| error.into_inner());
            guard.reconciliation_attempts = guard.reconciliation_attempts.saturating_add(1);
            recompute_external_slos(&mut guard);
        }
        let repair_batch_size =
            batch_size.max(1).max(usize::try_from(drift_before.drift_count).unwrap_or(usize::MAX));
        let indexer = self.run_indexer(store, repair_batch_size, 1, checked_at_unix_ms)?;
        let drift_after = self.detect_drift(store, checked_at_unix_ms)?;
        let success = !drift_after.reconciliation_required;
        {
            let mut guard = self.state.write().unwrap_or_else(|error| error.into_inner());
            if success {
                guard.reconciliation_successes = guard.reconciliation_successes.saturating_add(1);
            }
            recompute_external_slos(&mut guard);
        }
        Ok(ExternalRetrievalReconciliationOutcome {
            checked_at_unix_ms,
            drift_before,
            indexer,
            drift_after,
            success,
        })
    }

    fn record_external_query(&self, latency_ms: u64) {
        let mut guard = self.state.write().unwrap_or_else(|error| error.into_inner());
        guard.search_attempts = guard.search_attempts.saturating_add(1);
        guard.query_latency_samples_ms.push(latency_ms);
        if guard.query_latency_samples_ms.len() > 128 {
            guard.query_latency_samples_ms.remove(0);
        }
        recompute_external_slos(&mut guard);
    }

    fn record_degraded_fallback(&self) {
        let mut guard = self.state.write().unwrap_or_else(|error| error.into_inner());
        guard.search_attempts = guard.search_attempts.saturating_add(1);
        guard.degraded_fallbacks = guard.degraded_fallbacks.saturating_add(1);
        recompute_external_slos(&mut guard);
    }
}

impl ExternalRetrievalIndex for ExternalRetrievalRuntime {
    fn snapshot(&self) -> ExternalRetrievalIndexSnapshot {
        self.snapshot()
    }

    fn search_memory_candidate_outcome(
        &self,
        store: &JournalStore,
        request: &MemorySearchRequest,
    ) -> Result<Option<MemorySearchCandidateOutcome>, JournalError> {
        let snapshot = self.snapshot();
        if snapshot.state != RetrievalBackendState::Ready {
            self.record_degraded_fallback();
            return Ok(None);
        }
        let started = std::time::Instant::now();
        let outcome = store.search_memory_candidate_outcome(request)?;
        self.record_external_query(elapsed_millis(started));
        Ok(Some(outcome))
    }

    fn search_workspace_candidate_outcome(
        &self,
        store: &JournalStore,
        request: &WorkspaceSearchRequest,
    ) -> Result<Option<WorkspaceSearchCandidateOutcome>, JournalError> {
        let snapshot = self.snapshot();
        if snapshot.state != RetrievalBackendState::Ready {
            self.record_degraded_fallback();
            return Ok(None);
        }
        let started = std::time::Instant::now();
        let outcome = store.search_workspace_candidate_outcome(request)?;
        self.record_external_query(elapsed_millis(started));
        Ok(Some(outcome))
    }
}

#[derive(Debug, Default)]
pub(crate) struct UnavailableExternalRetrievalIndex;

impl ExternalRetrievalIndex for UnavailableExternalRetrievalIndex {
    fn snapshot(&self) -> ExternalRetrievalIndexSnapshot {
        ExternalRetrievalIndexSnapshot {
            provider: "unconfigured".to_owned(),
            state: RetrievalBackendState::Degraded,
            reason: "external retrieval index is not configured; journal fallback remains active"
                .to_owned(),
            indexed_memory_items: 0,
            indexed_workspace_chunks: 0,
            last_indexed_at_unix_ms: None,
            journal_watermark_unix_ms: None,
            freshness_lag_ms: None,
            drift_count: 0,
            pending_reconciliation_count: 0,
            scale_slos: ExternalRetrievalScaleSloSnapshot::default(),
            last_error: None,
        }
    }

    fn search_memory_candidate_outcome(
        &self,
        _store: &JournalStore,
        _request: &MemorySearchRequest,
    ) -> Result<Option<MemorySearchCandidateOutcome>, JournalError> {
        Ok(None)
    }

    fn search_workspace_candidate_outcome(
        &self,
        _store: &JournalStore,
        _request: &WorkspaceSearchRequest,
    ) -> Result<Option<WorkspaceSearchCandidateOutcome>, JournalError> {
        Ok(None)
    }
}

#[derive(Clone)]
pub(crate) struct ExternalDerivedRetrievalBackend {
    external_index: Arc<dyn ExternalRetrievalIndex>,
    journal_fallback: JournalRetrievalBackend,
}

impl Default for ExternalDerivedRetrievalBackend {
    fn default() -> Self {
        Self::new(Arc::new(UnavailableExternalRetrievalIndex))
    }
}

impl ExternalDerivedRetrievalBackend {
    #[must_use]
    pub(crate) fn new(external_index: Arc<dyn ExternalRetrievalIndex>) -> Self {
        Self { external_index, journal_fallback: JournalRetrievalBackend }
    }

    fn fallback_memory_outcome(
        &self,
        store: &JournalStore,
        request: &MemorySearchRequest,
        reason: &str,
    ) -> Result<MemorySearchCandidateOutcome, JournalError> {
        let mut outcome = store.search_memory_candidate_outcome(request)?;
        outcome.diagnostics.degraded_reason = Some(reason.to_owned());
        Ok(outcome)
    }

    fn fallback_workspace_outcome(
        &self,
        store: &JournalStore,
        request: &WorkspaceSearchRequest,
        reason: &str,
    ) -> Result<WorkspaceSearchCandidateOutcome, JournalError> {
        let mut outcome = store.search_workspace_candidate_outcome(request)?;
        outcome.diagnostics.degraded_reason = Some(reason.to_owned());
        Ok(outcome)
    }
}

impl RetrievalBackend for ExternalDerivedRetrievalBackend {
    fn snapshot(
        &self,
        config: &RetrievalRuntimeConfig,
        embeddings_status: &crate::journal::MemoryEmbeddingsStatus,
    ) -> RetrievalBackendSnapshot {
        if config.backend.kind == RetrievalBackendKind::JournalSqliteFts {
            return self.journal_fallback.snapshot(config, embeddings_status);
        }

        let external = self.external_index.snapshot();
        let embeddings_degraded = !embeddings_status.production_default_active;
        let external_degraded = external.state == RetrievalBackendState::Degraded;
        let state = if embeddings_degraded || external_degraded {
            RetrievalBackendState::Degraded
        } else {
            RetrievalBackendState::Ready
        };
        let reason = if external_degraded {
            format!(
                "{}; searches fall back to journal-derived retrieval without changing write paths",
                external.reason
            )
        } else if embeddings_degraded {
            embeddings_status.warning.clone().unwrap_or_else(|| {
                "external retrieval index is available but embeddings are degraded".to_owned()
            })
        } else {
            "external derived retrieval index is ready with journal fallback".to_owned()
        };

        RetrievalBackendSnapshot {
            kind: config.backend.kind,
            state,
            reason,
            capabilities: RetrievalBackendCapabilities {
                lexical_search: true,
                vector_search: true,
                lexical_candidate_generation: true,
                vector_candidate_generation: true,
                hybrid_fusion: true,
                source_aware_fusion: true,
                branch_diagnostics: true,
                transcript_fusion: true,
                checkpoint_fusion: true,
                compaction_fusion: true,
                lazy_reindex: true,
                batch_backfill: true,
                external_derived_index: true,
                journal_source_of_truth: true,
                journal_fallback: true,
                drift_detection: true,
                async_indexer: true,
                scale_slos: true,
            },
            externalization: retrieval_externalization_policy(),
            external_index: Some(external),
        }
    }

    fn search_memory_candidates(
        &self,
        store: &JournalStore,
        request: &MemorySearchRequest,
        config: &RetrievalRuntimeConfig,
    ) -> Result<Vec<MemorySearchCandidateRecord>, JournalError> {
        Ok(self.search_memory_candidate_outcome(store, request, config)?.candidates)
    }

    fn search_memory_candidate_outcome(
        &self,
        store: &JournalStore,
        request: &MemorySearchRequest,
        config: &RetrievalRuntimeConfig,
    ) -> Result<MemorySearchCandidateOutcome, JournalError> {
        if config.backend.kind == RetrievalBackendKind::JournalSqliteFts {
            return self.journal_fallback.search_memory_candidate_outcome(store, request, config);
        }
        if let Some(outcome) =
            self.external_index.search_memory_candidate_outcome(store, request)?
        {
            return Ok(outcome);
        }
        self.fallback_memory_outcome(store, request, "external_index_unavailable")
    }

    fn search_workspace_candidate_outcome(
        &self,
        store: &JournalStore,
        request: &WorkspaceSearchRequest,
        config: &RetrievalRuntimeConfig,
    ) -> Result<WorkspaceSearchCandidateOutcome, JournalError> {
        if config.backend.kind == RetrievalBackendKind::JournalSqliteFts {
            return self
                .journal_fallback
                .search_workspace_candidate_outcome(store, request, config);
        }
        if let Some(outcome) =
            self.external_index.search_workspace_candidate_outcome(store, request)?
        {
            return Ok(outcome);
        }
        self.fallback_workspace_outcome(store, request, "external_index_unavailable")
    }
}

fn next_indexed_count(current: u64, journal_total: u64, batch_size: u64) -> u64 {
    current.min(journal_total).saturating_add(batch_size).min(journal_total)
}

fn external_drift_report(
    snapshot: &ExternalRetrievalIndexSnapshot,
    journal_memory_items: u64,
    journal_workspace_chunks: u64,
    checked_at_unix_ms: i64,
) -> ExternalRetrievalDriftReport {
    let indexed_memory_items = snapshot.indexed_memory_items.min(journal_memory_items);
    let indexed_workspace_chunks = snapshot.indexed_workspace_chunks.min(journal_workspace_chunks);
    let memory_drift = signed_drift(journal_memory_items, indexed_memory_items);
    let workspace_chunk_drift = signed_drift(journal_workspace_chunks, indexed_workspace_chunks);
    let drift_count = journal_memory_items
        .saturating_sub(indexed_memory_items)
        .saturating_add(journal_workspace_chunks.saturating_sub(indexed_workspace_chunks));
    let freshness_lag_ms = snapshot.journal_watermark_unix_ms.map_or_else(
        || if drift_count == 0 { 0 } else { u64::MAX },
        |watermark| checked_at_unix_ms.saturating_sub(watermark).max(0) as u64,
    );
    ExternalRetrievalDriftReport {
        checked_at_unix_ms,
        indexed_memory_items,
        journal_memory_items,
        memory_drift,
        indexed_workspace_chunks,
        journal_workspace_chunks,
        workspace_chunk_drift,
        freshness_lag_ms,
        drift_count,
        reconciliation_required: drift_count > 0,
    }
}

fn signed_drift(journal_total: u64, indexed_total: u64) -> i64 {
    let drift = i128::from(journal_total) - i128::from(indexed_total);
    drift.clamp(i128::from(i64::MIN), i128::from(i64::MAX)) as i64
}

fn recompute_external_slos(state: &mut ExternalRetrievalRuntimeState) {
    let freshness_lag_ms = state.snapshot.freshness_lag_ms.unwrap_or(u64::MAX);
    let query_latency_p95_ms = percentile_95(&state.query_latency_samples_ms);
    let degraded_fallback_rate_bps =
        rate_bps(state.degraded_fallbacks, state.search_attempts, state.snapshot.state);
    let reconciliation_success_rate_bps = reconciliation_success_rate_bps(
        state.reconciliation_successes,
        state.reconciliation_attempts,
        &state.snapshot,
    );
    let freshness_ok = freshness_lag_ms <= DEFAULT_EXTERNAL_INDEX_FRESHNESS_SLO_MS;
    let query_latency_ok = query_latency_p95_ms <= DEFAULT_EXTERNAL_QUERY_LATENCY_SLO_MS;
    let degraded_fallback_ok =
        degraded_fallback_rate_bps <= DEFAULT_EXTERNAL_DEGRADED_FALLBACK_RATE_BPS;
    let reconciliation_success_ok =
        reconciliation_success_rate_bps >= DEFAULT_EXTERNAL_RECONCILIATION_SUCCESS_RATE_BPS;
    let preview_gate_state = if state.snapshot.state != RetrievalBackendState::Ready {
        "preview_blocked"
    } else if !freshness_ok {
        "freshness_slo_missed"
    } else if !query_latency_ok {
        "query_latency_slo_missed"
    } else if !degraded_fallback_ok {
        "fallback_rate_slo_missed"
    } else if !reconciliation_success_ok {
        "reconciliation_slo_missed"
    } else {
        "preview_ready"
    };
    state.snapshot.scale_slos = ExternalRetrievalScaleSloSnapshot {
        freshness_lag_ms,
        freshness_target_ms: DEFAULT_EXTERNAL_INDEX_FRESHNESS_SLO_MS,
        freshness_ok,
        query_latency_p95_ms,
        query_latency_target_ms: DEFAULT_EXTERNAL_QUERY_LATENCY_SLO_MS,
        query_latency_ok,
        degraded_fallback_rate_bps,
        degraded_fallback_target_bps: DEFAULT_EXTERNAL_DEGRADED_FALLBACK_RATE_BPS,
        degraded_fallback_ok,
        reconciliation_success_rate_bps,
        reconciliation_success_target_bps: DEFAULT_EXTERNAL_RECONCILIATION_SUCCESS_RATE_BPS,
        reconciliation_success_ok,
        preview_gate_state: preview_gate_state.to_owned(),
    };
}

fn percentile_95(samples: &[u64]) -> u64 {
    if samples.is_empty() {
        return 0;
    }
    let mut sorted = samples.to_vec();
    sorted.sort_unstable();
    let index = ((sorted.len().saturating_sub(1)) * 95) / 100;
    sorted[index]
}

fn rate_bps(count: u64, total: u64, state: RetrievalBackendState) -> u32 {
    if total == 0 {
        return if state == RetrievalBackendState::Ready { 0 } else { 10_000 };
    }
    let bps = count.saturating_mul(10_000) / total;
    u32::try_from(bps.min(10_000)).unwrap_or(10_000)
}

fn reconciliation_success_rate_bps(
    successes: u64,
    attempts: u64,
    snapshot: &ExternalRetrievalIndexSnapshot,
) -> u32 {
    if attempts == 0 {
        return if snapshot.state == RetrievalBackendState::Ready
            && snapshot.pending_reconciliation_count == 0
            && snapshot.last_indexed_at_unix_ms.is_some()
        {
            10_000
        } else {
            0
        };
    }
    let bps = successes.saturating_mul(10_000) / attempts;
    u32::try_from(bps.min(10_000)).unwrap_or(10_000)
}

fn elapsed_millis(started_at: std::time::Instant) -> u64 {
    u64::try_from(started_at.elapsed().as_millis()).unwrap_or(u64::MAX)
}

pub(crate) fn build_memory_embedding_runtime_selection(
    config: &ModelProviderConfig,
    offline_mode: bool,
) -> Result<MemoryEmbeddingRuntimeSelection> {
    let desired_embeddings = resolve_desired_embeddings_target(config)?;
    let resolved = resolve_embeddings_provider_config(config)?;
    let retry_max = resolved.as_ref().map_or(config.max_retries, |entry| entry.max_retries);
    let request_timeout_ms =
        resolved.as_ref().map_or(config.request_timeout_ms, |entry| entry.request_timeout_ms);
    let degraded_hash_fallback =
        |posture, desired_model_id, degraded_reason_code, warning, active_dims| {
            Ok(MemoryEmbeddingsRuntimeProfile::degraded_hash_fallback(
                posture,
                desired_model_id,
                degraded_reason_code,
                warning,
                active_dims,
                request_timeout_ms,
                retry_max,
            ))
        };
    let Some((desired_model_id, desired_dimensions)) = desired_embeddings else {
        return degraded_hash_fallback(
            MemoryEmbeddingsPosture::DegradedConfigFallback,
            None,
            "embeddings_model_not_configured",
            "retrieval embeddings defaulted to hash fallback because no embeddings-capable provider or model is configured"
                .to_owned(),
            DEFAULT_PRODUCTION_EMBEDDINGS_DIMS,
        );
    };

    let Some(dimensions) = desired_dimensions else {
        return degraded_hash_fallback(
            MemoryEmbeddingsPosture::DegradedConfigFallback,
            Some(desired_model_id.clone()),
            "embeddings_dimensions_unknown",
            format!(
                "retrieval embeddings are using hash fallback because dimensions for model '{}' are not known and were not configured",
                desired_model_id
            ),
            DEFAULT_PRODUCTION_EMBEDDINGS_DIMS,
        );
    };

    if offline_mode {
        return degraded_hash_fallback(
            MemoryEmbeddingsPosture::DegradedOffline,
            Some(desired_model_id.clone()),
            "offline_mode_enabled",
            "PALYRA_OFFLINE is enabled; retrieval embeddings are using the explicit hash fallback"
                .to_owned(),
            dimensions,
        );
    }

    let Some(provider_config) = resolved else {
        return degraded_hash_fallback(
            MemoryEmbeddingsPosture::DegradedConfigFallback,
            Some(desired_model_id.clone()),
            "embeddings_credentials_missing",
            "retrieval embeddings are using hash fallback because the configured provider has no credential reference"
                .to_owned(),
            dimensions,
        );
    };

    if provider_config.kind != ModelProviderKind::OpenAiCompatible {
        return degraded_hash_fallback(
            MemoryEmbeddingsPosture::DegradedUnsupportedProvider,
            Some(desired_model_id.clone()),
            "embeddings_provider_kind_unsupported",
            format!(
                "retrieval embeddings require an openai-compatible provider; resolved provider kind was {}",
                provider_config.kind.as_str()
            ),
            dimensions,
        );
    }

    let Some(model_id) = provider_config.openai_embeddings_model.clone() else {
        return degraded_hash_fallback(
            MemoryEmbeddingsPosture::DegradedConfigFallback,
            None,
            "embeddings_model_not_configured",
            "retrieval embeddings are using hash fallback because no embeddings model could be resolved"
                .to_owned(),
            dimensions,
        );
    };

    let active_dimensions =
        provider_config.openai_embeddings_dims.map(|value| value as usize).unwrap_or(dimensions);

    let provider = match build_embeddings_provider(&provider_config) {
        Ok(provider) => provider,
        Err(error) => {
            return degraded_hash_fallback(
                MemoryEmbeddingsPosture::DegradedProviderFallback,
                Some(model_id.clone()),
                "embeddings_runtime_init_failed",
                format!(
                    "retrieval embeddings fell back to hash mode because the provider runtime could not initialize: {error}"
                ),
                active_dimensions,
            );
        }
    };

    Ok(MemoryEmbeddingRuntimeSelection {
        provider: Arc::new(ModelProviderMemoryEmbeddingAdapter::new(
            provider,
            model_id.clone(),
            active_dimensions,
        )),
        profile: MemoryEmbeddingsRuntimeProfile {
            posture: MemoryEmbeddingsPosture::ProductionDefault,
            desired_model_id: Some(model_id.clone()),
            active_model_id: model_id,
            active_dims: active_dimensions,
            degraded_reason_code: None,
            warning: None,
            production_default_active: true,
            backfill_strategy: DEFAULT_BACKFILL_STRATEGY.to_owned(),
            batch_limit: DEFAULT_EMBEDDINGS_BATCH_LIMIT,
            request_timeout_ms,
            retry_max,
        },
    })
}

pub(crate) fn score_memory_candidates(
    candidates: Vec<MemorySearchCandidateRecord>,
    min_score: f64,
    config: &RetrievalRuntimeConfig,
) -> Vec<MemorySearchHit> {
    if candidates.is_empty() {
        return Vec::new();
    }
    let profile = config.scoring.profile_for(RetrievalSourceProfileKind::Memory);
    let lexical_max = candidates.iter().map(|candidate| candidate.lexical_raw).fold(0.0, f64::max);
    let vector_max = candidates.iter().map(|candidate| candidate.vector_raw).fold(0.0, f64::max);

    let mut hits = candidates
        .into_iter()
        .map(|candidate| {
            let lexical_score =
                if lexical_max > 0.0 { candidate.lexical_raw / lexical_max } else { 0.0 };
            let vector_score =
                if vector_max > 0.0 { candidate.vector_raw / vector_max } else { 0.0 };
            let source_quality_score =
                memory_source_quality(candidate.item.source, candidate.item.confidence, profile);
            let breakdown = score_with_profile(
                lexical_score,
                vector_score,
                candidate.recency_raw,
                source_quality_score,
                false,
                profile,
            );
            MemorySearchHit {
                item: candidate.item,
                snippet: candidate.snippet,
                score: breakdown.final_score,
                breakdown: MemoryScoreBreakdown {
                    lexical_score: breakdown.lexical_score,
                    vector_score: breakdown.vector_score,
                    recency_score: breakdown.recency_score,
                    source_quality_score: breakdown.source_quality_score,
                    final_score: breakdown.final_score,
                },
            }
        })
        .filter(|hit| hit.score >= min_score)
        .collect::<Vec<_>>();

    hits.sort_by(|left, right| {
        right
            .score
            .total_cmp(&left.score)
            .then_with(|| right.item.created_at_unix_ms.cmp(&left.item.created_at_unix_ms))
            .then_with(|| left.item.memory_id.cmp(&right.item.memory_id))
    });
    hits
}

pub(crate) fn score_workspace_candidates(
    candidates: Vec<WorkspaceSearchCandidateRecord>,
    min_score: f64,
    config: &RetrievalRuntimeConfig,
) -> Vec<WorkspaceSearchHit> {
    if candidates.is_empty() {
        return Vec::new();
    }
    let profile = config.scoring.profile_for(RetrievalSourceProfileKind::WorkspaceDocument);
    let lexical_max = candidates.iter().map(|candidate| candidate.lexical_raw).fold(0.0, f64::max);
    let vector_max = candidates.iter().map(|candidate| candidate.vector_raw).fold(0.0, f64::max);

    let mut hits = candidates
        .into_iter()
        .map(|candidate| {
            let lexical_score =
                if lexical_max > 0.0 { candidate.lexical_raw / lexical_max } else { 0.0 };
            let vector_score =
                if vector_max > 0.0 { candidate.vector_raw / vector_max } else { 0.0 };
            let source_quality_score = workspace_source_quality(
                candidate.document.pinned,
                candidate.document.manual_override,
                candidate.document.prompt_binding.as_str(),
                candidate.document.risk_state.as_str(),
                profile,
            );
            let breakdown = score_with_profile(
                lexical_score,
                vector_score,
                candidate.recency_raw,
                source_quality_score,
                candidate.document.pinned,
                profile,
            );
            let pinned = candidate.document.pinned;
            let branches =
                retrieval_candidate_branches(candidate.lexical_candidate, candidate.vector_candidate);
            WorkspaceSearchHit {
                document: candidate.document,
                version: candidate.version,
                chunk_index: candidate.chunk_index,
                chunk_count: candidate.chunk_count,
                snippet: candidate.snippet,
                score: breakdown.final_score,
                reason: if pinned {
                    format!("pinned_workspace_document; fusion_branches={branches}")
                } else {
                    format!(
                        "fusion(branches={},lexical={:.2},vector={:.2},recency={:.2},quality={:.2})",
                        branches,
                        breakdown.lexical_score,
                        breakdown.vector_score,
                        breakdown.recency_score,
                        breakdown.source_quality_score,
                    )
                },
                breakdown: WorkspaceScoreBreakdown {
                    lexical_score: breakdown.lexical_score,
                    vector_score: breakdown.vector_score,
                    recency_score: breakdown.recency_score,
                    source_quality_score: breakdown.source_quality_score,
                    final_score: breakdown.final_score,
                },
            }
        })
        .filter(|hit| hit.score >= min_score)
        .collect::<Vec<_>>();

    hits.sort_by(|left, right| {
        right
            .score
            .total_cmp(&left.score)
            .then_with(|| right.document.updated_at_unix_ms.cmp(&left.document.updated_at_unix_ms))
            .then_with(|| left.document.document_id.cmp(&right.document.document_id))
    });
    hits
}

#[must_use]
pub(crate) fn retrieval_candidate_branches(lexical: bool, vector: bool) -> &'static str {
    match (lexical, vector) {
        (true, true) => "lexical+vector",
        (true, false) => "lexical",
        (false, true) => "vector",
        (false, false) => "unknown",
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub(crate) struct RetrievalScoreBreakdown {
    pub(crate) lexical_score: f64,
    pub(crate) vector_score: f64,
    pub(crate) recency_score: f64,
    pub(crate) source_quality_score: f64,
    pub(crate) final_score: f64,
}

#[must_use]
pub(crate) fn lexical_overlap_score(
    text: &str,
    query_variants: &[String],
    phrase_match_bonus_bps: u16,
) -> f64 {
    query_variants
        .iter()
        .map(|query| lexical_overlap_for_query(text, query, phrase_match_bonus_bps))
        .fold(0.0, f64::max)
}

#[must_use]
pub(crate) fn proxy_vector_score(text: &str, query_variants: &[String]) -> f64 {
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

#[must_use]
pub(crate) fn recency_score(created_at_unix_ms: i64, now_unix_ms: i64, minimum_bps: u16) -> f64 {
    if created_at_unix_ms <= 0 || now_unix_ms <= created_at_unix_ms {
        return 1.0;
    }
    let age_days = (now_unix_ms - created_at_unix_ms) as f64 / 86_400_000.0;
    let minimum = f64::from(minimum_bps) / 10_000.0;
    (1.0 / (1.0 + age_days / 7.0)).clamp(minimum, 1.0)
}

#[must_use]
pub(crate) fn transcript_source_quality(
    event_type: &str,
    profile: &RetrievalSourceScoringProfile,
) -> f64 {
    let base: f64 = match event_type {
        "message.received" | "queued.input" => 0.72,
        "message.replied" => 0.76,
        _ => 0.70,
    };
    base.clamp(f64::from(profile.min_source_quality_bps) / 10_000.0, 1.0)
}

#[must_use]
pub(crate) fn checkpoint_source_quality(
    checkpoint: &OrchestratorCheckpointRecord,
    profile: &RetrievalSourceScoringProfile,
) -> f64 {
    let mut quality: f64 = 0.86;
    if checkpoint.restore_count > 0 {
        quality += 0.04;
    }
    if checkpoint.note.as_deref().is_some_and(|note| !note.trim().is_empty()) {
        quality += 0.02;
    }
    quality.clamp(f64::from(profile.min_source_quality_bps) / 10_000.0, 1.0)
}

#[must_use]
pub(crate) fn compaction_source_quality(
    artifact: &OrchestratorCompactionArtifactRecord,
    profile: &RetrievalSourceScoringProfile,
) -> f64 {
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
    (0.88 - review_penalty - poisoned_penalty)
        .clamp(f64::from(profile.min_source_quality_bps) / 10_000.0, 1.0)
}

#[must_use]
pub(crate) fn memory_source_quality(
    source: MemorySource,
    confidence: Option<f64>,
    profile: &RetrievalSourceScoringProfile,
) -> f64 {
    let confidence = confidence.unwrap_or(0.75).clamp(0.0, 1.0);
    let source_bias = match source {
        MemorySource::Manual => 0.94,
        MemorySource::Summary => 0.88,
        MemorySource::Import => 0.84,
        MemorySource::TapeUserMessage => 0.78,
        MemorySource::TapeToolResult => 0.74,
    };
    ((confidence * 0.6) + (source_bias * 0.4))
        .clamp(f64::from(profile.min_source_quality_bps) / 10_000.0, 1.0)
}

#[must_use]
pub(crate) fn workspace_source_quality(
    pinned: bool,
    manual_override: bool,
    prompt_binding: &str,
    risk_state: &str,
    profile: &RetrievalSourceScoringProfile,
) -> f64 {
    let mut quality: f64 = 0.78;
    if pinned {
        quality += 0.10;
    }
    if manual_override {
        quality += 0.05;
    }
    if prompt_binding == "system_candidate" {
        quality += 0.04;
    }
    if risk_state != "clean" {
        quality -= 0.12;
    }
    quality.clamp(f64::from(profile.min_source_quality_bps) / 10_000.0, 1.0)
}

#[must_use]
pub(crate) fn score_with_profile(
    lexical_score: f64,
    vector_score: f64,
    recency_score: f64,
    source_quality_score: f64,
    pinned: bool,
    profile: &RetrievalSourceScoringProfile,
) -> RetrievalScoreBreakdown {
    let lexical = lexical_score.clamp(0.0, 1.0);
    let vector = vector_score.clamp(0.0, 1.0);
    let recency = recency_score.clamp(f64::from(profile.min_recency_bps) / 10_000.0, 1.0);
    let source_quality =
        source_quality_score.clamp(f64::from(profile.min_source_quality_bps) / 10_000.0, 1.0);
    let pinned_bonus = if pinned { f64::from(profile.pinned_bonus_bps) / 10_000.0 } else { 0.0 };
    let weighted_score = (lexical * f64::from(profile.lexical_bps) / 10_000.0)
        + (vector * f64::from(profile.vector_bps) / 10_000.0)
        + (recency * f64::from(profile.recency_bps) / 10_000.0)
        + (source_quality * f64::from(profile.source_quality_bps) / 10_000.0);
    let final_score = (weighted_score + pinned_bonus).clamp(0.0, 1.0 + pinned_bonus);

    RetrievalScoreBreakdown {
        lexical_score: lexical,
        vector_score: vector,
        recency_score: recency,
        source_quality_score: source_quality,
        final_score,
    }
}

fn lexical_overlap_for_query(text: &str, query: &str, phrase_match_bonus_bps: u16) -> f64 {
    let haystack = normalized_tokens(text);
    let needles = normalized_tokens(query);
    if haystack.is_empty() || needles.is_empty() {
        return 0.0;
    }
    let needle_set = needles.iter().collect::<std::collections::BTreeSet<_>>();
    let match_count = haystack.iter().filter(|token| needle_set.contains(token)).count();
    let phrase_bonus = if text.to_ascii_lowercase().contains(query.to_ascii_lowercase().as_str()) {
        f64::from(phrase_match_bonus_bps) / 10_000.0
    } else {
        0.0
    };
    (match_count as f64 / needle_set.len().max(1) as f64) + phrase_bonus
}
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

fn char_ngrams(input: &str) -> std::collections::BTreeSet<String> {
    let normalized = input
        .chars()
        .map(|character| if character.is_control() { ' ' } else { character.to_ascii_lowercase() })
        .collect::<String>();
    let chars = normalized.chars().collect::<Vec<_>>();
    if chars.len() < 3 {
        return chars.into_iter().map(|character| character.to_string()).collect();
    }
    let mut grams = std::collections::BTreeSet::new();
    for window in chars.windows(3) {
        grams.insert(window.iter().collect::<String>());
    }
    grams
}

pub(crate) fn resolve_embeddings_provider_config(
    config: &ModelProviderConfig,
) -> Result<Option<ModelProviderConfig>> {
    let registry = config.normalized_registry()?;
    let default_model_id = registry
        .default_embeddings_model_id
        .clone()
        .or_else(|| config.default_embeddings_model_id())
        .or_else(|| {
            if provider_can_use_production_embeddings(config, None) {
                Some(DEFAULT_PRODUCTION_EMBEDDINGS_MODEL_ID.to_owned())
            } else {
                None
            }
        });
    let Some(default_model_id) = default_model_id else {
        return Ok(None);
    };

    let model_entry = registry.models.iter().find(|entry| {
        entry.model_id == default_model_id
            && entry.role == ProviderModelRole::Embeddings
            && entry.enabled
    });
    let provider_entry = model_entry.and_then(|model| {
        registry
            .providers
            .iter()
            .find(|entry| entry.provider_id == model.provider_id && entry.enabled)
    });

    let mut provider_config = config.clone();
    if let Some(entry) = provider_entry {
        provider_config.kind = entry.kind;
        match entry.kind {
            ModelProviderKind::OpenAiCompatible => {
                if let Some(base_url) = entry.base_url.as_ref() {
                    provider_config.openai_base_url = base_url.clone();
                }
                provider_config.openai_api_key = entry.api_key.clone();
                provider_config.openai_api_key_secret_ref = entry.api_key_secret_ref.clone();
                provider_config.openai_api_key_vault_ref = entry.api_key_vault_ref.clone();
            }
            ModelProviderKind::Anthropic => {
                if let Some(base_url) = entry.base_url.as_ref() {
                    provider_config.anthropic_base_url = base_url.clone();
                }
                provider_config.anthropic_api_key = entry.api_key.clone();
                provider_config.anthropic_api_key_secret_ref = entry.api_key_secret_ref.clone();
                provider_config.anthropic_api_key_vault_ref = entry.api_key_vault_ref.clone();
            }
            ModelProviderKind::Deterministic => {}
        }
        provider_config.allow_private_base_url = entry.allow_private_base_url;
        provider_config.auth_profile_id =
            entry.auth_profile_id.clone().or_else(|| provider_config.auth_profile_id.clone());
        provider_config.auth_profile_provider_kind =
            entry.auth_profile_provider_kind.or(provider_config.auth_profile_provider_kind);
        provider_config.credential_source =
            entry.credential_source.or(provider_config.credential_source);
        provider_config.request_timeout_ms = entry.request_timeout_ms;
        provider_config.max_retries = entry.max_retries;
        provider_config.retry_backoff_ms = entry.retry_backoff_ms;
        provider_config.circuit_breaker_failure_threshold = entry.circuit_breaker_failure_threshold;
        provider_config.circuit_breaker_cooldown_ms = entry.circuit_breaker_cooldown_ms;
    }

    provider_config.openai_embeddings_model = Some(default_model_id.clone());
    provider_config.openai_embeddings_dims = provider_config.openai_embeddings_dims.or_else(|| {
        known_embedding_dimensions(default_model_id.as_str()).map(|value| value as u32)
    });

    if !provider_can_use_production_embeddings(&provider_config, provider_entry) {
        return Ok(None);
    }

    Ok(Some(provider_config))
}

fn resolve_desired_embeddings_target(
    config: &ModelProviderConfig,
) -> Result<Option<(String, Option<usize>)>> {
    let registry = config.normalized_registry()?;
    let model_id = registry
        .default_embeddings_model_id
        .or_else(|| config.openai_embeddings_model.clone())
        .or_else(|| {
            if config.kind == ModelProviderKind::OpenAiCompatible {
                Some(DEFAULT_PRODUCTION_EMBEDDINGS_MODEL_ID.to_owned())
            } else {
                None
            }
        });
    Ok(model_id.map(|model_id| {
        let dimensions = config
            .openai_embeddings_dims
            .map(|value| value as usize)
            .or_else(|| known_embedding_dimensions(model_id.as_str()));
        (model_id, dimensions)
    }))
}

fn provider_can_use_production_embeddings(
    config: &ModelProviderConfig,
    provider_entry: Option<&crate::model_provider::ProviderRegistryEntryConfig>,
) -> bool {
    if config.kind != ModelProviderKind::OpenAiCompatible {
        return false;
    }
    if config.openai_api_key.as_deref().is_some_and(|value| !value.trim().is_empty()) {
        return true;
    }
    if config.openai_api_key_secret_ref.is_some() || config.openai_api_key_vault_ref.is_some() {
        return true;
    }
    if config.auth_profile_id.as_deref().is_some_and(|value| !value.trim().is_empty()) {
        return true;
    }
    provider_entry.is_some_and(|entry| {
        entry.api_key.as_deref().is_some_and(|value| !value.trim().is_empty())
            || entry.api_key_secret_ref.is_some()
            || entry.api_key_vault_ref.is_some()
            || entry.auth_profile_id.as_deref().is_some_and(|value| !value.trim().is_empty())
            || entry.credential_source.is_some_and(|source| {
                matches!(
                    source,
                    ModelProviderCredentialSource::AuthProfileApiKey
                        | ModelProviderCredentialSource::AuthProfileOauthAccessToken
                )
            })
    })
}

fn known_embedding_dimensions(model_id: &str) -> Option<usize> {
    match model_id.trim() {
        DEFAULT_PRODUCTION_EMBEDDINGS_MODEL_ID => Some(DEFAULT_PRODUCTION_EMBEDDINGS_DIMS),
        "text-embedding-3-large" => Some(3_072),
        "text-embedding-ada-002" => Some(1_536),
        _ => None,
    }
}

struct ModelProviderMemoryEmbeddingAdapter {
    provider: Arc<dyn crate::model_provider::EmbeddingsProvider>,
    model_name: String,
    dimensions: usize,
}

impl ModelProviderMemoryEmbeddingAdapter {
    fn new(
        provider: Arc<dyn crate::model_provider::EmbeddingsProvider>,
        model_name: String,
        dimensions: usize,
    ) -> Self {
        Self { provider, model_name, dimensions: dimensions.max(1) }
    }

    fn zero_vector(&self) -> Vec<f32> {
        vec![0.0_f32; self.dimensions]
    }
}

impl MemoryEmbeddingProvider for ModelProviderMemoryEmbeddingAdapter {
    fn model_name(&self) -> &str {
        self.model_name.as_str()
    }

    fn dimensions(&self) -> usize {
        self.dimensions
    }

    fn embed_text(&self, text: &str) -> Vec<f32> {
        let request = crate::model_provider::EmbeddingsRequest { inputs: vec![text.to_owned()] };
        let result = match tokio::runtime::Handle::try_current() {
            Ok(handle) => {
                tokio::task::block_in_place(|| handle.block_on(self.provider.embed(request)))
            }
            Err(_) => {
                tracing::warn!(
                    "tokio runtime unavailable for retrieval embeddings adapter; using zero vector fallback"
                );
                return self.zero_vector();
            }
        };

        match result {
            Ok(response) => {
                let Some(vector) = response.vectors.into_iter().next() else {
                    tracing::warn!(
                        "retrieval embeddings response did not include a vector payload; using zero vector fallback"
                    );
                    return self.zero_vector();
                };
                normalize_embedding_dimensions(vector, self.dimensions)
            }
            Err(error) => {
                tracing::warn!(
                    error = %error,
                    "retrieval embeddings request failed; using zero vector fallback"
                );
                self.zero_vector()
            }
        }
    }
}

fn normalize_embedding_dimensions(mut vector: Vec<f32>, expected_dims: usize) -> Vec<f32> {
    if expected_dims == 0 {
        return Vec::new();
    }
    if vector.len() < expected_dims {
        vector.resize(expected_dims, 0.0);
    } else if vector.len() > expected_dims {
        vector.truncate(expected_dims);
    }
    vector
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};

    use serde::{Deserialize, Serialize};
    use serde_json::Value;

    use super::{
        build_memory_embedding_runtime_selection, checkpoint_source_quality,
        compaction_source_quality, lexical_overlap_score, memory_source_quality,
        proxy_vector_score, recency_score, score_with_profile, transcript_source_quality,
        workspace_source_quality, ExternalDerivedRetrievalBackend, ExternalRetrievalIndex,
        JournalRetrievalBackend, RetrievalBackend, RetrievalBackendKind, RetrievalBackendState,
        RetrievalExternalizationClass, RetrievalRuntimeConfig, RetrievalSourceProfileKind,
    };
    use crate::journal::{
        JournalConfig, JournalStore, MemoryEmbeddingsMode, MemoryEmbeddingsStatus,
        MemoryItemCreateRequest, MemorySearchRequest, MemorySource, OrchestratorCheckpointRecord,
        OrchestratorCompactionArtifactRecord, QueryEmbeddingCacheStatus,
    };
    use crate::model_provider::{ModelProviderConfig, ModelProviderKind};

    #[test]
    fn retrieval_scoring_defaults_validate() {
        RetrievalRuntimeConfig::default()
            .validate()
            .expect("default retrieval scoring should validate");
    }

    #[test]
    fn memory_embedding_selection_defaults_to_production_model_when_openai_credentials_exist() {
        let config = ModelProviderConfig {
            kind: ModelProviderKind::OpenAiCompatible,
            openai_api_key: Some("sk-test".to_owned()),
            ..ModelProviderConfig::default()
        };

        let selection = build_memory_embedding_runtime_selection(&config, false)
            .expect("embedding runtime selection should succeed");
        assert!(selection.profile.production_default_active);
        assert_eq!(selection.profile.desired_model_id.as_deref(), Some("text-embedding-3-small"));
        assert_eq!(selection.profile.active_dims, 1_536);
    }

    #[test]
    fn memory_embedding_selection_marks_offline_hash_fallback_as_degraded() {
        let config = ModelProviderConfig {
            kind: ModelProviderKind::OpenAiCompatible,
            openai_api_key: Some("sk-test".to_owned()),
            ..ModelProviderConfig::default()
        };

        let selection = build_memory_embedding_runtime_selection(&config, true)
            .expect("offline selection should succeed");
        assert!(!selection.profile.production_default_active);
        assert_eq!(selection.profile.posture.as_str(), "degraded_offline");
    }

    #[test]
    fn lexical_overlap_uses_phrase_bonus_from_configured_basis_points() {
        let boosted = lexical_overlap_score(
            "ship the summary to release notes",
            &["release notes".to_owned()],
            2_000,
        );
        let plain = lexical_overlap_score(
            "ship the summary to release notes",
            &["release notes".to_owned()],
            0,
        );
        assert!(boosted > plain);
    }

    #[test]
    fn proxy_vector_and_recency_scoring_remain_bounded() {
        let vector = proxy_vector_score("alpha beta gamma", &["alpha gamma".to_owned()]);
        let recency = recency_score(1_700_000_000_000, 1_700_086_400_000, 1_500);
        assert!((0.0..=1.0).contains(&vector));
        assert!((0.15..=1.0).contains(&recency));
    }

    trait PostureAsStr {
        fn as_str(&self) -> &'static str;
    }

    impl PostureAsStr for super::MemoryEmbeddingsPosture {
        fn as_str(&self) -> &'static str {
            match self {
                super::MemoryEmbeddingsPosture::ProductionDefault => "production_default",
                super::MemoryEmbeddingsPosture::DegradedOffline => "degraded_offline",
                super::MemoryEmbeddingsPosture::DegradedConfigFallback => {
                    "degraded_config_fallback"
                }
                super::MemoryEmbeddingsPosture::DegradedProviderFallback => {
                    "degraded_provider_fallback"
                }
                super::MemoryEmbeddingsPosture::DegradedUnsupportedProvider => {
                    "degraded_unsupported_provider"
                }
            }
        }
    }

    #[test]
    fn runtime_config_returns_expected_profile() {
        let config = RetrievalRuntimeConfig::default();
        let workspace = config.scoring.profile_for(RetrievalSourceProfileKind::WorkspaceDocument);
        assert_eq!(workspace.pinned_bonus_bps, 500);
    }

    #[test]
    fn externalization_policy_keeps_journal_as_source_of_truth() {
        let policy = super::retrieval_externalization_policy();
        assert_eq!(policy.source_of_truth, "journal_store_and_artifact_storage");
        assert!(policy.derived_index_allowed);
        assert!(
            !policy.replay_requires_live_external_index,
            "replay must remain journal-derived and offline"
        );
        assert!(policy.field_policy.iter().any(|field| {
            field.field == "orchestrator_tape.payload_json"
                && field.classification == RetrievalExternalizationClass::JournalOnly
        }));
        assert!(policy.field_policy.iter().any(|field| {
            field.field == "memory_vectors.embedding_vector"
                && field.classification == RetrievalExternalizationClass::DerivedIndex
        }));
    }

    #[test]
    fn retrieval_backend_snapshot_tracks_embeddings_posture() {
        let backend = JournalRetrievalBackend;
        let config = RetrievalRuntimeConfig::default();

        let ready = backend.snapshot(
            &config,
            &MemoryEmbeddingsStatus {
                mode: MemoryEmbeddingsMode::ModelProvider,
                posture: super::MemoryEmbeddingsPosture::ProductionDefault,
                desired_model_id: Some("text-embedding-3-small".to_owned()),
                target_model_id: "text-embedding-3-small".to_owned(),
                target_dims: 1_536,
                target_version: 1,
                total_count: 16,
                indexed_count: 16,
                pending_count: 0,
                production_default_active: true,
                degraded_reason_code: None,
                warning: None,
                backfill_strategy: "lazy_reindex".to_owned(),
                batch_limit: 64,
                request_timeout_ms: 15_000,
                retry_max: 2,
                query_cache: QueryEmbeddingCacheStatus {
                    capacity: 256,
                    ttl_ms: 300_000,
                    entry_count: 0,
                    hits: 0,
                    misses: 0,
                },
            },
        );
        assert_eq!(ready.state, RetrievalBackendState::Ready);
        assert!(ready.capabilities.vector_search, "ready backend should advertise vector search");

        let degraded = backend.snapshot(
            &config,
            &MemoryEmbeddingsStatus {
                mode: MemoryEmbeddingsMode::HashFallback,
                posture: super::MemoryEmbeddingsPosture::DegradedOffline,
                desired_model_id: Some("text-embedding-3-small".to_owned()),
                target_model_id: "hash-fallback-v1".to_owned(),
                target_dims: 1_536,
                target_version: 1,
                total_count: 16,
                indexed_count: 4,
                pending_count: 12,
                production_default_active: false,
                degraded_reason_code: Some("offline_mode_enabled".to_owned()),
                warning: Some(
                    "PALYRA_OFFLINE is enabled; retrieval embeddings are using the explicit hash fallback"
                        .to_owned(),
                ),
                backfill_strategy: "lazy_reindex".to_owned(),
                batch_limit: 64,
                request_timeout_ms: 15_000,
                retry_max: 2,
                query_cache: QueryEmbeddingCacheStatus {
                    capacity: 256,
                    ttl_ms: 300_000,
                    entry_count: 0,
                    hits: 0,
                    misses: 0,
                },
            },
        );
        assert_eq!(degraded.state, RetrievalBackendState::Degraded);
        assert!(
            degraded.reason.contains("PALYRA_OFFLINE"),
            "degraded backend reason should surface the operator warning"
        );
    }

    #[test]
    fn external_preview_backend_falls_back_to_journal_candidates() {
        let temp = tempfile::tempdir().expect("tempdir should be created");
        let store = JournalStore::open(JournalConfig {
            db_path: temp.path().join("journal.sqlite3"),
            hash_chain_enabled: false,
            max_payload_bytes: 256 * 1024,
            max_events: 10_000,
        })
        .expect("journal store should open");
        store
            .create_memory_item(&MemoryItemCreateRequest {
                memory_id: "01ARZ3NDEKTSV4RRFFQ69G5M49".to_owned(),
                principal: "user:ops".to_owned(),
                channel: Some("cli".to_owned()),
                session_id: None,
                source: MemorySource::Manual,
                content_text: "rollback checklist and release gate notes".to_owned(),
                tags: Vec::new(),
                confidence: Some(0.9),
                ttl_unix_ms: None,
            })
            .expect("memory item should be indexed in journal");
        let request = MemorySearchRequest {
            principal: "user:ops".to_owned(),
            channel: Some("cli".to_owned()),
            session_id: None,
            query: "rollback checklist".to_owned(),
            top_k: 4,
            min_score: 0.0,
            tags: Vec::new(),
            sources: Vec::new(),
        };
        let journal_backend = JournalRetrievalBackend;
        let journal_config = RetrievalRuntimeConfig::default();
        let journal = journal_backend
            .search_memory_candidate_outcome(&store, &request, &journal_config)
            .expect("journal candidate generation should work");
        let external_backend = ExternalDerivedRetrievalBackend::default();
        let external_config = RetrievalRuntimeConfig {
            backend: super::RetrievalBackendConfig {
                kind: RetrievalBackendKind::ExternalDerivedPreview,
            },
            ..RetrievalRuntimeConfig::default()
        };
        let external = external_backend
            .search_memory_candidate_outcome(&store, &request, &external_config)
            .expect("external backend should fall back to journal candidates");

        let journal_ids = journal
            .candidates
            .iter()
            .map(|candidate| candidate.item.memory_id.as_str())
            .collect::<Vec<_>>();
        let external_ids = external
            .candidates
            .iter()
            .map(|candidate| candidate.item.memory_id.as_str())
            .collect::<Vec<_>>();
        assert_eq!(external_ids, journal_ids);
        assert_eq!(
            external.diagnostics.degraded_reason.as_deref(),
            Some("external_index_unavailable")
        );
    }

    #[test]
    fn external_runtime_indexer_checkpoint_opens_ready_external_path() {
        let temp = tempfile::tempdir().expect("tempdir should be created");
        let store = JournalStore::open(JournalConfig {
            db_path: temp.path().join("journal.sqlite3"),
            hash_chain_enabled: false,
            max_payload_bytes: 256 * 1024,
            max_events: 10_000,
        })
        .expect("journal store should open");
        store
            .create_memory_item(&MemoryItemCreateRequest {
                memory_id: "01ARZ3NDEKTSV4RRFFQ69G5M50".to_owned(),
                principal: "user:ops".to_owned(),
                channel: Some("cli".to_owned()),
                session_id: None,
                source: MemorySource::Manual,
                content_text: "external retrieval index checkpoint release gate".to_owned(),
                tags: Vec::new(),
                confidence: Some(0.91),
                ttl_unix_ms: None,
            })
            .expect("memory item should be indexed in journal");

        let external_index = super::ExternalRetrievalRuntime::default();
        let drift = external_index
            .detect_drift(&store, 1_000)
            .expect("drift detection should read journal counts");
        assert!(drift.reconciliation_required);
        assert_eq!(drift.memory_drift, 1);

        let indexer = external_index
            .run_indexer(&store, 64, 1, 2_000)
            .expect("external indexer checkpoint should advance");
        assert!(indexer.checkpoint_committed);
        assert!(indexer.complete);
        assert_eq!(indexer.indexed_memory_items, 1);

        let snapshot = external_index.snapshot();
        assert_eq!(snapshot.state, RetrievalBackendState::Ready);
        assert_eq!(snapshot.scale_slos.preview_gate_state, "preview_ready");
        assert!(snapshot.scale_slos.freshness_ok);
        assert!(snapshot.scale_slos.reconciliation_success_ok);

        let request = MemorySearchRequest {
            principal: "user:ops".to_owned(),
            channel: Some("cli".to_owned()),
            session_id: None,
            query: "checkpoint release gate".to_owned(),
            top_k: 4,
            min_score: 0.0,
            tags: Vec::new(),
            sources: Vec::new(),
        };
        let external_outcome = external_index
            .search_memory_candidate_outcome(&store, &request)
            .expect("ready external runtime should serve candidates")
            .expect("ready external runtime should not force journal fallback");
        assert!(external_outcome
            .candidates
            .iter()
            .any(|candidate| candidate.item.memory_id == "01ARZ3NDEKTSV4RRFFQ69G5M50"));
    }

    #[test]
    fn external_runtime_reconciliation_repairs_stale_checkpoint() {
        let temp = tempfile::tempdir().expect("tempdir should be created");
        let store = JournalStore::open(JournalConfig {
            db_path: temp.path().join("journal.sqlite3"),
            hash_chain_enabled: false,
            max_payload_bytes: 256 * 1024,
            max_events: 10_000,
        })
        .expect("journal store should open");
        store
            .create_memory_item(&MemoryItemCreateRequest {
                memory_id: "01ARZ3NDEKTSV4RRFFQ69G5M51".to_owned(),
                principal: "user:ops".to_owned(),
                channel: Some("cli".to_owned()),
                session_id: None,
                source: MemorySource::Manual,
                content_text: "initial external checkpoint memory projection".to_owned(),
                tags: Vec::new(),
                confidence: Some(0.88),
                ttl_unix_ms: None,
            })
            .expect("memory item should be indexed in journal");

        let external_index = super::ExternalRetrievalRuntime::default();
        external_index.run_indexer(&store, 64, 1, 2_000).expect("first checkpoint should complete");
        store
            .create_memory_item(&MemoryItemCreateRequest {
                memory_id: "01ARZ3NDEKTSV4RRFFQ69G5M52".to_owned(),
                principal: "user:ops".to_owned(),
                channel: Some("cli".to_owned()),
                session_id: None,
                source: MemorySource::Manual,
                content_text: "follow up memory projection requiring reconciliation".to_owned(),
                tags: Vec::new(),
                confidence: Some(0.88),
                ttl_unix_ms: None,
            })
            .expect("second memory item should be indexed in journal");

        let drift = external_index
            .detect_drift(&store, 3_000)
            .expect("stale checkpoint should produce drift");
        assert_eq!(drift.memory_drift, 1);
        assert!(drift.reconciliation_required);

        let reconciliation = external_index
            .reconcile(&store, 1, 4_000)
            .expect("reconciliation should run journal-derived backfill");
        assert!(reconciliation.success);
        assert!(reconciliation.drift_before.reconciliation_required);
        assert!(!reconciliation.drift_after.reconciliation_required);
        assert_eq!(reconciliation.indexer.indexed_memory_items, 2);
        assert_eq!(external_index.snapshot().scale_slos.preview_gate_state, "preview_ready");
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    struct RetrievalEvalCorpus {
        version: u32,
        cases: Vec<RetrievalEvalCase>,
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    struct RetrievalEvalCase {
        case_id: String,
        query: String,
        top_k: usize,
        #[serde(default)]
        latency_budget_ms: Option<f64>,
        #[serde(default)]
        expected_ranked_ids: Vec<String>,
        #[serde(default)]
        max_negative_top_score: Option<f64>,
        candidates: Vec<RetrievalEvalCandidate>,
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    struct RetrievalEvalCandidate {
        candidate_id: String,
        source_kind: RetrievalSourceProfileKind,
        title: String,
        snippet: String,
        search_text: String,
        created_at_unix_ms: i64,
        #[serde(default)]
        pinned: bool,
        #[serde(default)]
        manual_override: bool,
        #[serde(default)]
        prompt_binding: Option<String>,
        #[serde(default)]
        risk_state: Option<String>,
        #[serde(default)]
        memory_source: Option<String>,
        #[serde(default)]
        confidence: Option<f64>,
        #[serde(default)]
        event_type: Option<String>,
        #[serde(default)]
        checkpoint_note: Option<String>,
        #[serde(default)]
        checkpoint_restore_count: Option<u64>,
        #[serde(default)]
        checkpoint_workspace_paths: Vec<String>,
        #[serde(default)]
        compaction_mode: Option<String>,
        #[serde(default)]
        compaction_strategy: Option<String>,
        #[serde(default)]
        compaction_trigger_reason: Option<String>,
        #[serde(default)]
        compaction_summary_json: Option<Value>,
    }

    #[derive(Debug, Clone, Serialize, PartialEq)]
    struct RetrievalEvalReport {
        corpus_version: u32,
        embedding_posture: String,
        summary: RetrievalEvalSummary,
        cases: Vec<RetrievalEvalCaseReport>,
    }

    #[derive(Debug, Clone, Serialize, PartialEq)]
    struct RetrievalEvalSummary {
        total_cases: usize,
        positive_cases: usize,
        negative_cases: usize,
        hit_at_1: f64,
        hit_at_3: f64,
        recall_at_k: f64,
        mean_ndcg_at_k: f64,
        coverage: f64,
        mean_reciprocal_rank: f64,
        false_positive_rate: f64,
        stability_score: f64,
        branch_hit_contribution: RetrievalEvalBranchContribution,
        latency_budget_pass_rate: f64,
        explainability_completeness: f64,
        estimated_latency_ms_p50: f64,
        estimated_latency_ms_p95: f64,
        estimated_cost_units_total: f64,
        embedding_batches_total: usize,
    }

    #[derive(Debug, Clone, Default, Serialize, PartialEq, Eq)]
    struct RetrievalEvalBranchContribution {
        lexical_hits: usize,
        vector_hits: usize,
        hybrid_hits: usize,
        source_quality_hits: usize,
        by_source_kind: BTreeMap<String, usize>,
    }

    #[derive(Debug, Clone, Serialize, PartialEq)]
    struct RetrievalEvalCaseReport {
        case_id: String,
        query: String,
        top_ids: Vec<String>,
        matched_expected_count: usize,
        recall_at_k: f64,
        reciprocal_rank: f64,
        ndcg_at_k: f64,
        coverage: f64,
        stability_score: f64,
        false_positive: bool,
        branch_hit_contribution: RetrievalEvalBranchContribution,
        latency_budget_ms: f64,
        latency_budget_exceeded: bool,
        explainability_completeness: f64,
        estimated_latency_ms: f64,
        estimated_cost_units: f64,
        embedding_batches: usize,
        top_candidates: Vec<RetrievalEvalRankedCandidate>,
    }

    #[derive(Debug, Clone, Serialize, PartialEq)]
    struct RetrievalEvalRankedCandidate {
        candidate_id: String,
        source_kind: String,
        title: String,
        snippet: String,
        branch_contribution: String,
        explainability_complete: bool,
        final_score: f64,
        lexical_score: f64,
        vector_score: f64,
        recency_score: f64,
        source_quality_score: f64,
    }

    const RETRIEVAL_EVAL_NOW_UNIX_MS: i64 = 1_700_000_030_000;

    fn round_metric(value: f64) -> f64 {
        (value * 10_000.0).round() / 10_000.0
    }

    fn load_eval_corpus() -> RetrievalEvalCorpus {
        serde_json::from_str(include_str!("../../../fixtures/retrieval/eval_corpus.json"))
            .expect("retrieval eval corpus fixture should deserialize")
    }

    fn validate_eval_corpus(corpus: &RetrievalEvalCorpus) -> Result<(), String> {
        let mut case_ids = BTreeSet::<String>::new();
        for case in corpus.cases.as_slice() {
            if case.top_k == 0 {
                return Err(format!("eval case '{}' must use top_k > 0", case.case_id));
            }
            if case.latency_budget_ms.is_some_and(|budget| !budget.is_finite() || budget <= 0.0) {
                return Err(format!(
                    "eval case '{}' latency_budget_ms must be a positive finite value",
                    case.case_id
                ));
            }
            if !case_ids.insert(case.case_id.clone()) {
                return Err(format!("duplicate eval case id '{}'", case.case_id));
            }
            let mut candidate_ids = BTreeSet::<String>::new();
            for candidate in case.candidates.as_slice() {
                if !candidate_ids.insert(candidate.candidate_id.clone()) {
                    return Err(format!(
                        "eval case '{}' contains duplicate candidate id '{}'",
                        case.case_id, candidate.candidate_id
                    ));
                }
            }
            for expected in case.expected_ranked_ids.as_slice() {
                if !candidate_ids.contains(expected) {
                    return Err(format!(
                        "eval case '{}' references missing expected candidate '{}'",
                        case.case_id, expected
                    ));
                }
            }
        }
        Ok(())
    }

    fn parse_eval_memory_source(raw: Option<&str>) -> MemorySource {
        match raw.unwrap_or("manual") {
            "manual" => MemorySource::Manual,
            "summary" => MemorySource::Summary,
            "import" => MemorySource::Import,
            "tape_user_message" => MemorySource::TapeUserMessage,
            "tape_tool_result" => MemorySource::TapeToolResult,
            other => panic!("unsupported eval memory source fixture: {other}"),
        }
    }

    fn checkpoint_quality_record(
        candidate: &RetrievalEvalCandidate,
    ) -> OrchestratorCheckpointRecord {
        OrchestratorCheckpointRecord {
            checkpoint_id: candidate.candidate_id.clone(),
            session_id: "session-eval".to_owned(),
            run_id: None,
            name: candidate.title.clone(),
            tags_json: "[]".to_owned(),
            note: candidate.checkpoint_note.clone(),
            branch_state: "clean".to_owned(),
            parent_session_id: None,
            referenced_compaction_ids_json: "[]".to_owned(),
            workspace_paths_json: serde_json::to_string(&candidate.checkpoint_workspace_paths)
                .expect("checkpoint workspace paths should serialize"),
            created_by_principal: "user:ops".to_owned(),
            created_at_unix_ms: candidate.created_at_unix_ms,
            restore_count: candidate.checkpoint_restore_count.unwrap_or_default(),
            last_restored_at_unix_ms: None,
        }
    }

    fn compaction_quality_record(
        candidate: &RetrievalEvalCandidate,
    ) -> OrchestratorCompactionArtifactRecord {
        OrchestratorCompactionArtifactRecord {
            artifact_id: candidate.candidate_id.clone(),
            session_id: "session-eval".to_owned(),
            run_id: None,
            mode: candidate.compaction_mode.clone().unwrap_or_else(|| "condense".to_owned()),
            strategy: candidate
                .compaction_strategy
                .clone()
                .unwrap_or_else(|| "semantic".to_owned()),
            compressor_version: "eval".to_owned(),
            trigger_reason: candidate
                .compaction_trigger_reason
                .clone()
                .unwrap_or_else(|| "eval_fixture".to_owned()),
            trigger_policy: None,
            trigger_inputs_json: None,
            summary_text: candidate.snippet.clone(),
            summary_preview: candidate.snippet.clone(),
            source_event_count: 12,
            protected_event_count: 1,
            condensed_event_count: 8,
            omitted_event_count: 3,
            estimated_input_tokens: 640,
            estimated_output_tokens: 180,
            source_records_json: "[]".to_owned(),
            summary_json: serde_json::to_string(
                &candidate.compaction_summary_json.clone().unwrap_or_default(),
            )
            .expect("compaction summary fixture should serialize"),
            created_by_principal: "user:ops".to_owned(),
            created_at_unix_ms: candidate.created_at_unix_ms,
        }
    }

    fn eval_source_kind_label(kind: RetrievalSourceProfileKind) -> &'static str {
        match kind {
            RetrievalSourceProfileKind::Memory => "memory",
            RetrievalSourceProfileKind::WorkspaceDocument => "workspace_document",
            RetrievalSourceProfileKind::Transcript => "transcript",
            RetrievalSourceProfileKind::Checkpoint => "checkpoint",
            RetrievalSourceProfileKind::CompactionArtifact => "compaction_artifact",
        }
    }

    fn explainability_complete(candidate: &RetrievalEvalRankedCandidate) -> bool {
        !candidate.candidate_id.trim().is_empty()
            && !candidate.source_kind.trim().is_empty()
            && !candidate.title.trim().is_empty()
            && !candidate.snippet.trim().is_empty()
            && candidate.final_score.is_finite()
            && candidate.lexical_score.is_finite()
            && candidate.vector_score.is_finite()
            && candidate.recency_score.is_finite()
            && candidate.source_quality_score.is_finite()
    }

    fn branch_contribution(candidate: &RetrievalEvalRankedCandidate) -> String {
        let lexical_active = candidate.lexical_score >= 0.1;
        let vector_active = candidate.vector_score >= 0.1;
        match (lexical_active, vector_active) {
            (true, true) if (candidate.lexical_score - candidate.vector_score).abs() <= 0.15 => {
                "hybrid"
            }
            (true, true) if candidate.lexical_score > candidate.vector_score => "lexical",
            (true, true) => "vector",
            (true, false) => "lexical",
            (false, true) => "vector",
            (false, false) => "source_quality",
        }
        .to_owned()
    }

    fn score_eval_candidate(
        query: &str,
        candidate: &RetrievalEvalCandidate,
        config: &RetrievalRuntimeConfig,
        production_default_active: bool,
    ) -> RetrievalEvalRankedCandidate {
        let query_variants = vec![query.to_owned()];
        let profile = config.scoring.profile_for(candidate.source_kind);
        let lexical_score = lexical_overlap_score(
            candidate.search_text.as_str(),
            &query_variants,
            config.scoring.phrase_match_bonus_bps,
        );
        let vector_multiplier = if production_default_active { 1.0 } else { 0.45 };
        let vector_score =
            proxy_vector_score(candidate.search_text.as_str(), &query_variants) * vector_multiplier;
        let recency_score = recency_score(
            candidate.created_at_unix_ms,
            RETRIEVAL_EVAL_NOW_UNIX_MS,
            profile.min_recency_bps,
        );
        let source_quality_score = match candidate.source_kind {
            RetrievalSourceProfileKind::Memory => memory_source_quality(
                parse_eval_memory_source(candidate.memory_source.as_deref()),
                candidate.confidence,
                profile,
            ),
            RetrievalSourceProfileKind::WorkspaceDocument => workspace_source_quality(
                candidate.pinned,
                candidate.manual_override,
                candidate.prompt_binding.as_deref().unwrap_or("advisory"),
                candidate.risk_state.as_deref().unwrap_or("clean"),
                profile,
            ),
            RetrievalSourceProfileKind::Transcript => transcript_source_quality(
                candidate.event_type.as_deref().unwrap_or("message.received"),
                profile,
            ),
            RetrievalSourceProfileKind::Checkpoint => {
                checkpoint_source_quality(&checkpoint_quality_record(candidate), profile)
            }
            RetrievalSourceProfileKind::CompactionArtifact => {
                compaction_source_quality(&compaction_quality_record(candidate), profile)
            }
        };
        let breakdown = score_with_profile(
            lexical_score,
            vector_score,
            recency_score,
            source_quality_score,
            candidate.pinned,
            profile,
        );

        let mut ranked = RetrievalEvalRankedCandidate {
            candidate_id: candidate.candidate_id.clone(),
            source_kind: eval_source_kind_label(candidate.source_kind).to_owned(),
            title: candidate.title.clone(),
            snippet: candidate.snippet.clone(),
            branch_contribution: String::new(),
            explainability_complete: false,
            final_score: round_metric(breakdown.final_score),
            lexical_score: round_metric(breakdown.lexical_score),
            vector_score: round_metric(breakdown.vector_score),
            recency_score: round_metric(breakdown.recency_score),
            source_quality_score: round_metric(breakdown.source_quality_score),
        };
        ranked.branch_contribution = branch_contribution(&ranked);
        ranked.explainability_complete = explainability_complete(&ranked);
        ranked
    }

    fn estimate_case_latency_ms(
        case: &RetrievalEvalCase,
        production_default_active: bool,
        batch_limit: usize,
    ) -> f64 {
        let candidate_count = case.candidates.len() as f64;
        let embedding_batches = if production_default_active {
            case.candidates.len().div_ceil(batch_limit.max(1)) as f64
        } else {
            0.0
        };
        round_metric(
            (candidate_count * 1.35)
                + (case.top_k as f64 * 0.55)
                + (embedding_batches * 1.8)
                + 0.75,
        )
    }

    fn percentile(sorted_values: &[f64], numerator: usize, denominator: usize) -> f64 {
        if sorted_values.is_empty() {
            return 0.0;
        }
        let rank = (sorted_values.len() * numerator).div_ceil(denominator).saturating_sub(1);
        round_metric(sorted_values[rank.min(sorted_values.len() - 1)])
    }

    fn relevance_for(case: &RetrievalEvalCase, candidate_id: &str) -> f64 {
        case.expected_ranked_ids
            .iter()
            .position(|expected_id| expected_id == candidate_id)
            .map_or(0.0, |index| (case.expected_ranked_ids.len() - index) as f64)
    }

    fn dcg_for_relevances(relevances: impl Iterator<Item = f64>) -> f64 {
        relevances
            .enumerate()
            .map(|(index, relevance)| {
                if relevance <= 0.0 {
                    0.0
                } else {
                    (2_f64.powf(relevance) - 1.0) / ((index + 2) as f64).log2()
                }
            })
            .sum::<f64>()
    }

    fn ndcg_at_k(case: &RetrievalEvalCase, ranked: &[RetrievalEvalRankedCandidate]) -> f64 {
        if case.expected_ranked_ids.is_empty() {
            return 1.0;
        }
        let actual_dcg = dcg_for_relevances(
            ranked
                .iter()
                .take(case.top_k)
                .map(|candidate| relevance_for(case, candidate.candidate_id.as_str())),
        );
        let ideal_dcg = dcg_for_relevances(
            (0..case.expected_ranked_ids.len())
                .take(case.top_k)
                .map(|index| (case.expected_ranked_ids.len() - index) as f64),
        );
        if ideal_dcg <= f64::EPSILON {
            0.0
        } else {
            round_metric(actual_dcg / ideal_dcg)
        }
    }

    fn record_branch_hit(
        contribution: &mut RetrievalEvalBranchContribution,
        candidate: &RetrievalEvalRankedCandidate,
    ) {
        match candidate.branch_contribution.as_str() {
            "lexical" => contribution.lexical_hits = contribution.lexical_hits.saturating_add(1),
            "vector" => contribution.vector_hits = contribution.vector_hits.saturating_add(1),
            "hybrid" => contribution.hybrid_hits = contribution.hybrid_hits.saturating_add(1),
            _ => {
                contribution.source_quality_hits =
                    contribution.source_quality_hits.saturating_add(1);
            }
        }
        let entry = contribution.by_source_kind.entry(candidate.source_kind.clone()).or_default();
        *entry = entry.saturating_add(1);
    }

    fn explainability_ratio(candidates: &[RetrievalEvalRankedCandidate]) -> f64 {
        if candidates.is_empty() {
            return 1.0;
        }
        round_metric(
            candidates.iter().filter(|candidate| candidate.explainability_complete).count() as f64
                / candidates.len() as f64,
        )
    }

    fn run_eval_harness(
        corpus: &RetrievalEvalCorpus,
        config: &RetrievalRuntimeConfig,
        production_default_active: bool,
        batch_limit: usize,
    ) -> RetrievalEvalReport {
        validate_eval_corpus(corpus).expect("retrieval eval corpus should validate");

        let mut hit_at_1 = 0usize;
        let mut hit_at_3 = 0usize;
        let mut positive_cases = 0usize;
        let mut negative_cases = 0usize;
        let mut false_positives = 0usize;
        let mut coverage_sum = 0.0;
        let mut recall_at_k_sum = 0.0;
        let mut reciprocal_rank_sum = 0.0;
        let mut ndcg_sum = 0.0;
        let mut stability_sum = 0.0;
        let mut latency_budget_passes = 0usize;
        let mut explainability_sum = 0.0;
        let mut total_cost_units = 0.0;
        let mut embedding_batches_total = 0usize;
        let mut branch_hit_contribution = RetrievalEvalBranchContribution::default();
        let mut latencies = Vec::<f64>::new();
        let mut cases = Vec::<RetrievalEvalCaseReport>::new();

        for case in corpus.cases.as_slice() {
            let mut ranked = case
                .candidates
                .iter()
                .map(|candidate| {
                    score_eval_candidate(
                        case.query.as_str(),
                        candidate,
                        config,
                        production_default_active,
                    )
                })
                .collect::<Vec<_>>();
            ranked.sort_by(|left, right| {
                right
                    .final_score
                    .total_cmp(&left.final_score)
                    .then_with(|| left.candidate_id.cmp(&right.candidate_id))
            });

            let top_candidates = ranked.iter().take(case.top_k).cloned().collect::<Vec<_>>();
            let top_ids = top_candidates
                .iter()
                .map(|candidate| candidate.candidate_id.clone())
                .collect::<Vec<_>>();
            let first_expected_rank = case
                .expected_ranked_ids
                .iter()
                .filter_map(|expected_id| {
                    ranked
                        .iter()
                        .position(|candidate| candidate.candidate_id == *expected_id)
                        .map(|index| index + 1)
                })
                .min();
            let reciprocal_rank = first_expected_rank.map_or(0.0, |rank| 1.0 / rank as f64);
            let matched_expected_count = case
                .expected_ranked_ids
                .iter()
                .filter(|expected_id| {
                    top_ids.iter().any(|candidate_id| candidate_id == *expected_id)
                })
                .count();
            let negative_threshold = case.max_negative_top_score.unwrap_or(0.55);
            let top_score = top_candidates.first().map_or(0.0, |candidate| candidate.final_score);
            let false_positive =
                case.expected_ranked_ids.is_empty() && top_score > negative_threshold;
            let recall_at_k = if case.expected_ranked_ids.is_empty() {
                if false_positive {
                    0.0
                } else {
                    1.0
                }
            } else {
                matched_expected_count as f64 / case.expected_ranked_ids.len() as f64
            };
            let ndcg_at_k = if case.expected_ranked_ids.is_empty() {
                if false_positive {
                    0.0
                } else {
                    1.0
                }
            } else {
                ndcg_at_k(case, ranked.as_slice())
            };
            let coverage = if case.expected_ranked_ids.is_empty() {
                if false_positive {
                    0.0
                } else {
                    1.0
                }
            } else {
                matched_expected_count as f64 / case.expected_ranked_ids.len() as f64
            };
            let stability_score = if case.expected_ranked_ids.is_empty() {
                if false_positive {
                    0.0
                } else {
                    1.0
                }
            } else {
                let denominator = case.top_k.max(1) as f64;
                case.expected_ranked_ids
                    .iter()
                    .map(|expected_id| {
                        top_ids
                            .iter()
                            .position(|candidate_id| candidate_id == expected_id)
                            .map_or(0.0, |index| {
                                (case.top_k.saturating_sub(index) as f64) / denominator
                            })
                    })
                    .sum::<f64>()
                    / case.expected_ranked_ids.len() as f64
            };
            let embedding_batches = if production_default_active {
                case.candidates.len().div_ceil(batch_limit.max(1))
            } else {
                0
            };
            let estimated_cost_units = if production_default_active {
                round_metric((embedding_batches * 10 + case.candidates.len()) as f64)
            } else {
                0.0
            };
            let estimated_latency_ms =
                estimate_case_latency_ms(case, production_default_active, batch_limit);
            let latency_budget_ms = case.latency_budget_ms.unwrap_or(12.0);
            let latency_budget_exceeded = estimated_latency_ms > latency_budget_ms;
            if !latency_budget_exceeded {
                latency_budget_passes = latency_budget_passes.saturating_add(1);
            }
            let explainability_completeness = explainability_ratio(top_candidates.as_slice());
            let mut case_branch_hit_contribution = RetrievalEvalBranchContribution::default();
            for candidate in top_candidates.iter().filter(|candidate| {
                case.expected_ranked_ids
                    .iter()
                    .any(|expected_id| expected_id == &candidate.candidate_id)
            }) {
                record_branch_hit(&mut case_branch_hit_contribution, candidate);
                record_branch_hit(&mut branch_hit_contribution, candidate);
            }

            if case.expected_ranked_ids.is_empty() {
                negative_cases = negative_cases.saturating_add(1);
                if false_positive {
                    false_positives = false_positives.saturating_add(1);
                }
            } else {
                positive_cases = positive_cases.saturating_add(1);
                if matches!(first_expected_rank, Some(1)) {
                    hit_at_1 = hit_at_1.saturating_add(1);
                }
                if first_expected_rank.is_some_and(|rank| rank <= 3) {
                    hit_at_3 = hit_at_3.saturating_add(1);
                }
                recall_at_k_sum += recall_at_k;
                reciprocal_rank_sum += reciprocal_rank;
                ndcg_sum += ndcg_at_k;
            }

            coverage_sum += coverage;
            stability_sum += stability_score;
            explainability_sum += explainability_completeness;
            total_cost_units += estimated_cost_units;
            embedding_batches_total = embedding_batches_total.saturating_add(embedding_batches);
            latencies.push(estimated_latency_ms);
            cases.push(RetrievalEvalCaseReport {
                case_id: case.case_id.clone(),
                query: case.query.clone(),
                top_ids,
                matched_expected_count,
                recall_at_k: round_metric(recall_at_k),
                reciprocal_rank: round_metric(reciprocal_rank),
                ndcg_at_k: round_metric(ndcg_at_k),
                coverage: round_metric(coverage),
                stability_score: round_metric(stability_score),
                false_positive,
                branch_hit_contribution: case_branch_hit_contribution,
                latency_budget_ms,
                latency_budget_exceeded,
                explainability_completeness,
                estimated_latency_ms,
                estimated_cost_units,
                embedding_batches,
                top_candidates,
            });
        }

        latencies.sort_by(|left, right| left.total_cmp(right));
        RetrievalEvalReport {
            corpus_version: corpus.version,
            embedding_posture: if production_default_active {
                "production_default".to_owned()
            } else {
                "degraded_hash_fallback".to_owned()
            },
            summary: RetrievalEvalSummary {
                total_cases: corpus.cases.len(),
                positive_cases,
                negative_cases,
                hit_at_1: if positive_cases == 0 {
                    0.0
                } else {
                    round_metric(hit_at_1 as f64 / positive_cases as f64)
                },
                hit_at_3: if positive_cases == 0 {
                    0.0
                } else {
                    round_metric(hit_at_3 as f64 / positive_cases as f64)
                },
                recall_at_k: if positive_cases == 0 {
                    0.0
                } else {
                    round_metric(recall_at_k_sum / positive_cases as f64)
                },
                mean_ndcg_at_k: if positive_cases == 0 {
                    0.0
                } else {
                    round_metric(ndcg_sum / positive_cases as f64)
                },
                coverage: if corpus.cases.is_empty() {
                    0.0
                } else {
                    round_metric(coverage_sum / corpus.cases.len() as f64)
                },
                mean_reciprocal_rank: if positive_cases == 0 {
                    0.0
                } else {
                    round_metric(reciprocal_rank_sum / positive_cases as f64)
                },
                false_positive_rate: if negative_cases == 0 {
                    0.0
                } else {
                    round_metric(false_positives as f64 / negative_cases as f64)
                },
                stability_score: if corpus.cases.is_empty() {
                    0.0
                } else {
                    round_metric(stability_sum / corpus.cases.len() as f64)
                },
                branch_hit_contribution,
                latency_budget_pass_rate: if corpus.cases.is_empty() {
                    0.0
                } else {
                    round_metric(latency_budget_passes as f64 / corpus.cases.len() as f64)
                },
                explainability_completeness: if corpus.cases.is_empty() {
                    0.0
                } else {
                    round_metric(explainability_sum / corpus.cases.len() as f64)
                },
                estimated_latency_ms_p50: percentile(&latencies, 1, 2),
                estimated_latency_ms_p95: percentile(&latencies, 95, 100),
                estimated_cost_units_total: round_metric(total_cost_units),
                embedding_batches_total,
            },
            cases,
        }
    }

    #[test]
    fn retrieval_eval_corpus_fixture_is_valid() {
        let corpus = load_eval_corpus();
        validate_eval_corpus(&corpus)
            .expect("retrieval eval corpus should be internally consistent");
    }

    #[test]
    fn retrieval_eval_harness_smoke_report_is_deterministic() {
        let report =
            run_eval_harness(&load_eval_corpus(), &RetrievalRuntimeConfig::default(), true, 64);
        assert_eq!(report.summary.total_cases, 8);
        assert!(
            report.summary.hit_at_1 >= 0.6,
            "default retrieval benchmark should keep a healthy hit@1 baseline"
        );
        assert!(
            report.summary.recall_at_k >= 0.85,
            "default retrieval benchmark should keep high recall@k"
        );
        assert!(
            report.summary.mean_ndcg_at_k >= 0.85,
            "default retrieval benchmark should keep high graded ranking quality"
        );
        assert!(
            report.summary.latency_budget_pass_rate >= 0.9,
            "default retrieval benchmark should stay inside the offline latency budget"
        );
        assert_eq!(
            report.summary.explainability_completeness, 1.0,
            "every top candidate should expose score explanations"
        );
        assert!(
            report.summary.branch_hit_contribution.hybrid_hits > 0,
            "eval report should expose hybrid branch contribution"
        );
        let serialized = serde_json::to_string_pretty(&report)
            .expect("retrieval eval report should serialize to deterministic json");
        assert_eq!(
            format!("{serialized}\n"),
            include_str!("../../../fixtures/retrieval/eval_report_default.json")
                .replace("\r\n", "\n"),
            "default retrieval benchmark report should stay stable for regression review"
        );
    }

    #[test]
    fn retrieval_eval_harness_degraded_mode_removes_embedding_cost() {
        let production =
            run_eval_harness(&load_eval_corpus(), &RetrievalRuntimeConfig::default(), true, 64);
        let degraded =
            run_eval_harness(&load_eval_corpus(), &RetrievalRuntimeConfig::default(), false, 64);

        assert_eq!(degraded.embedding_posture, "degraded_hash_fallback");
        assert_eq!(
            degraded.summary.estimated_cost_units_total, 0.0,
            "hash fallback benchmark should not report production embedding cost"
        );
        assert!(
            production.summary.estimated_cost_units_total
                > degraded.summary.estimated_cost_units_total,
            "production benchmark should expose a non-zero embeddings cost signal"
        );
    }

    #[test]
    fn retrieval_eval_harness_compares_fusion_configurations() {
        let corpus = load_eval_corpus();
        let baseline = run_eval_harness(&corpus, &RetrievalRuntimeConfig::default(), true, 64);
        let mut vector_heavy_config = RetrievalRuntimeConfig::default();
        for profile in [
            &mut vector_heavy_config.scoring.default_profile,
            &mut vector_heavy_config.scoring.memory,
            &mut vector_heavy_config.scoring.workspace,
            &mut vector_heavy_config.scoring.transcript,
            &mut vector_heavy_config.scoring.checkpoint,
            &mut vector_heavy_config.scoring.compaction,
        ] {
            profile.lexical_bps = 2_500;
            profile.vector_bps = 5_000;
            profile.recency_bps = 1_500;
            profile.source_quality_bps = 1_000;
        }
        vector_heavy_config.validate().expect("comparison scoring config should remain valid");

        let vector_heavy = run_eval_harness(&corpus, &vector_heavy_config, true, 64);
        assert_eq!(baseline.corpus_version, vector_heavy.corpus_version);
        assert_eq!(baseline.cases.len(), vector_heavy.cases.len());
        assert!(
            vector_heavy.summary.mean_ndcg_at_k >= 0.8,
            "comparison config should remain above the relevance floor"
        );
        assert_ne!(
            baseline.cases[0].top_candidates[0].final_score,
            vector_heavy.cases[0].top_candidates[0].final_score,
            "eval harness should make scoring configuration changes visible"
        );
    }
}
