use std::sync::Arc;

use anyhow::Result;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{
    journal::{
        HashMemoryEmbeddingProvider, JournalError, JournalStore, MemoryEmbeddingProvider,
        MemoryEmbeddingsMode, MemoryScoreBreakdown, MemorySearchCandidateRecord, MemorySearchHit,
        MemorySearchRequest, MemorySource, OrchestratorCheckpointRecord,
        OrchestratorCompactionArtifactRecord, WorkspaceScoreBreakdown,
        WorkspaceSearchCandidateRecord, WorkspaceSearchHit, WorkspaceSearchRequest,
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

const DEFAULT_RECALL_PHRASE_MATCH_BONUS_BPS: u16 = 2_000;
const DEFAULT_LEGACY_MIN_RECENCY_BPS: u16 = 1_500;
const DEFAULT_LEGACY_MIN_SOURCE_QUALITY_BPS: u16 = 2_000;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum RetrievalBackendKind {
    JournalSqliteFts,
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
    pub(crate) hybrid_fusion: bool,
    pub(crate) transcript_fusion: bool,
    pub(crate) checkpoint_fusion: bool,
    pub(crate) compaction_fusion: bool,
    pub(crate) lazy_reindex: bool,
    pub(crate) batch_backfill: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct RetrievalBackendSnapshot {
    pub(crate) kind: RetrievalBackendKind,
    pub(crate) state: RetrievalBackendState,
    pub(crate) reason: String,
    pub(crate) capabilities: RetrievalBackendCapabilities,
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
    ) -> Result<Vec<MemorySearchCandidateRecord>, JournalError>;

    fn search_workspace_candidates(
        &self,
        store: &JournalStore,
        request: &WorkspaceSearchRequest,
    ) -> Result<Vec<WorkspaceSearchCandidateRecord>, JournalError>;
}

#[derive(Debug, Default)]
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
                hybrid_fusion: true,
                transcript_fusion: true,
                checkpoint_fusion: true,
                compaction_fusion: true,
                lazy_reindex: true,
                batch_backfill: true,
            },
        }
    }

    fn search_memory_candidates(
        &self,
        store: &JournalStore,
        request: &MemorySearchRequest,
    ) -> Result<Vec<MemorySearchCandidateRecord>, JournalError> {
        store.search_memory_candidates(request)
    }

    fn search_workspace_candidates(
        &self,
        store: &JournalStore,
        request: &WorkspaceSearchRequest,
    ) -> Result<Vec<WorkspaceSearchCandidateRecord>, JournalError> {
        store.search_workspace_candidates(request)
    }
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
            WorkspaceSearchHit {
                document: candidate.document,
                version: candidate.version,
                chunk_index: candidate.chunk_index,
                chunk_count: candidate.chunk_count,
                snippet: candidate.snippet,
                score: breakdown.final_score,
                reason: if pinned {
                    "pinned_workspace_document".to_owned()
                } else {
                    format!(
                        "hybrid(lexical={:.2},vector={:.2},recency={:.2},quality={:.2})",
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
    use std::collections::BTreeSet;

    use serde::{Deserialize, Serialize};
    use serde_json::Value;

    use super::{
        build_memory_embedding_runtime_selection, checkpoint_source_quality,
        compaction_source_quality, lexical_overlap_score, memory_source_quality,
        proxy_vector_score, recency_score, score_with_profile, transcript_source_quality,
        workspace_source_quality, JournalRetrievalBackend, RetrievalBackend, RetrievalBackendState,
        RetrievalRuntimeConfig, RetrievalSourceProfileKind,
    };
    use crate::journal::{
        MemoryEmbeddingsMode, MemoryEmbeddingsStatus, MemorySource, OrchestratorCheckpointRecord,
        OrchestratorCompactionArtifactRecord,
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
            },
        );
        assert_eq!(degraded.state, RetrievalBackendState::Degraded);
        assert!(
            degraded.reason.contains("PALYRA_OFFLINE"),
            "degraded backend reason should surface the operator warning"
        );
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
        coverage: f64,
        mean_reciprocal_rank: f64,
        false_positive_rate: f64,
        stability_score: f64,
        estimated_latency_ms_p50: f64,
        estimated_latency_ms_p95: f64,
        estimated_cost_units_total: f64,
        embedding_batches_total: usize,
    }

    #[derive(Debug, Clone, Serialize, PartialEq)]
    struct RetrievalEvalCaseReport {
        case_id: String,
        query: String,
        top_ids: Vec<String>,
        matched_expected_count: usize,
        reciprocal_rank: f64,
        coverage: f64,
        stability_score: f64,
        false_positive: bool,
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

        RetrievalEvalRankedCandidate {
            candidate_id: candidate.candidate_id.clone(),
            source_kind: match candidate.source_kind {
                RetrievalSourceProfileKind::Memory => "memory".to_owned(),
                RetrievalSourceProfileKind::WorkspaceDocument => "workspace_document".to_owned(),
                RetrievalSourceProfileKind::Transcript => "transcript".to_owned(),
                RetrievalSourceProfileKind::Checkpoint => "checkpoint".to_owned(),
                RetrievalSourceProfileKind::CompactionArtifact => "compaction_artifact".to_owned(),
            },
            title: candidate.title.clone(),
            final_score: round_metric(breakdown.final_score),
            lexical_score: round_metric(breakdown.lexical_score),
            vector_score: round_metric(breakdown.vector_score),
            recency_score: round_metric(breakdown.recency_score),
            source_quality_score: round_metric(breakdown.source_quality_score),
        }
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
        let mut reciprocal_rank_sum = 0.0;
        let mut stability_sum = 0.0;
        let mut total_cost_units = 0.0;
        let mut embedding_batches_total = 0usize;
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
                reciprocal_rank_sum += reciprocal_rank;
            }

            coverage_sum += coverage;
            stability_sum += stability_score;
            total_cost_units += estimated_cost_units;
            embedding_batches_total = embedding_batches_total.saturating_add(embedding_batches);
            latencies.push(estimated_latency_ms);
            cases.push(RetrievalEvalCaseReport {
                case_id: case.case_id.clone(),
                query: case.query.clone(),
                top_ids,
                matched_expected_count,
                reciprocal_rank: round_metric(reciprocal_rank),
                coverage: round_metric(coverage),
                stability_score: round_metric(stability_score),
                false_positive,
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
        assert_eq!(report.summary.total_cases, 7);
        assert!(
            report.summary.hit_at_1 >= 0.6,
            "default retrieval benchmark should keep a healthy hit@1 baseline"
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
}
