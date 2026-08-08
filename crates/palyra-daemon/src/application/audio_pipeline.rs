//! Connector-neutral audio transcription and post-delivery synthesis.
//!
//! Raw media remains an ephemeral job input. Durable contracts contain only
//! bounded, redacted text, content hashes, provenance, usage, and retention.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex, MutexGuard},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;
use palyra_model_providers::{AudioSynthesisResponse, AudioTranscriptionResponse};
use palyra_safety::{redact_text_for_export, SafetyContentKind, SafetySourceKind, TrustLabel};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::sync::watch;
use ulid::Ulid;

use crate::sha256_hex;

const AUDIO_PIPELINE_SCHEMA_VERSION: u32 = 1;
const DEFAULT_MEDIA_JOB_TIMEOUT_MS: u64 = 60_000;
const DEFAULT_MAX_AUDIO_BYTES: u64 = 25 * 1024 * 1024;
const DEFAULT_MAX_AUDIO_DURATION_MS: u64 = 15 * 60 * 1_000;
const DEFAULT_MAX_SESSION_MEDIA_BYTES: u64 = 100 * 1024 * 1024;
const DEFAULT_MAX_SESSION_MEDIA_DURATION_MS: u64 = 30 * 60 * 1_000;
const DEFAULT_MAX_TRANSCRIPT_BYTES: usize = 256 * 1024;
/// Maximum final-text bytes admitted to one post-delivery speech job.
pub const MAX_SYNTHESIS_TEXT_BYTES: usize = 32 * 1024;
const DEFAULT_RETENTION_MS: u64 = 24 * 60 * 60 * 1_000;
const MAX_TRACKED_MEDIA_SESSIONS: usize = 1_024;
const MAX_ACTIVE_MEDIA_JOBS_PER_SESSION: usize = 32;

/// Media origin without connector-specific business semantics.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AudioArtifactProvenance {
    pub source_kind: String,
    pub source_reference_sha256: String,
    pub received_at_unix_ms: u64,
    pub principal_scope_sha256: String,
    pub session_id: String,
}

/// Durable metadata for one retained input artifact.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AudioInputArtifactV1 {
    pub v: u32,
    pub artifact_id: String,
    pub file_name: String,
    pub content_type: String,
    pub codec: String,
    pub bytes: u64,
    pub duration_ms: u64,
    pub language_hint: Option<String>,
    pub sha256: String,
    pub created_at_unix_ms: u64,
    pub expires_at_unix_ms: u64,
    pub provenance: AudioArtifactProvenance,
}

/// Caller-supplied metadata kept separate from raw audio bytes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AudioInputDescriptor {
    pub file_name: String,
    pub content_type: String,
    pub codec: String,
    pub duration_ms: u64,
    pub language_hint: Option<String>,
}

impl AudioInputArtifactV1 {
    /// Builds input metadata while keeping the raw payload outside the contract.
    pub fn from_payload(
        descriptor: AudioInputDescriptor,
        bytes: &[u8],
        provenance: AudioArtifactProvenance,
        retention: MediaRetentionPolicy,
    ) -> Self {
        let created_at_unix_ms = unix_ms();
        Self {
            v: AUDIO_PIPELINE_SCHEMA_VERSION,
            artifact_id: Ulid::new().to_string(),
            file_name: descriptor.file_name,
            content_type: descriptor.content_type,
            codec: descriptor.codec,
            bytes: u64::try_from(bytes.len()).unwrap_or(u64::MAX),
            duration_ms: descriptor.duration_ms,
            language_hint: descriptor.language_hint,
            sha256: sha256_hex(bytes),
            created_at_unix_ms,
            expires_at_unix_ms: created_at_unix_ms.saturating_add(retention.raw_audio_ttl_ms),
            provenance,
        }
    }
}

/// Model-facing transcript context that explicitly carries no instruction authority.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UntrustedTranscriptContextSegment {
    pub text: String,
    pub trust_label: String,
    pub instruction_authority: bool,
    pub artifact_citation: String,
    pub source_artifact_sha256: String,
}

/// Durable, redacted result of one STT job.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TranscriptionArtifact {
    pub v: u32,
    pub artifact_id: String,
    pub source_artifact_id: String,
    pub source_artifact_sha256: String,
    pub transcript_sha256: String,
    pub text: String,
    pub detected_language: Option<String>,
    pub confidence: Option<f32>,
    pub duration_ms: u64,
    pub model_name: String,
    pub usage: MediaUsage,
    pub redacted: bool,
    pub created_at_unix_ms: u64,
    pub expires_at_unix_ms: u64,
    pub context_segment: UntrustedTranscriptContextSegment,
}

/// Connector-neutral destination descriptor for a synthesized artifact.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MediaDeliveryDescriptor {
    pub delivery_key: String,
    pub destination_scope_sha256: String,
    pub content_type: String,
    pub file_name: String,
}

/// Durable metadata for synthesized speech.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AudioOutputArtifactV1 {
    pub v: u32,
    pub artifact_id: String,
    pub source_run_id: String,
    pub source_text_sha256: String,
    pub content_type: String,
    pub codec: String,
    pub bytes: u64,
    pub duration_ms: u64,
    pub sha256: String,
    pub model_name: String,
    pub voice_id: String,
    pub usage: MediaUsage,
    pub created_at_unix_ms: u64,
    pub expires_at_unix_ms: u64,
    pub delivery: MediaDeliveryDescriptor,
}

/// Metering recorded independently from connector delivery.
#[derive(Debug, Clone, Copy, Default, PartialEq, Serialize, Deserialize)]
pub struct MediaUsage {
    pub input_bytes: u64,
    pub output_bytes: u64,
    pub audio_duration_ms: u64,
    pub billable_units: u64,
    pub estimated_cost_microunits: u64,
}

/// Session-wide limits that prevent many individually-valid jobs from escaping bounds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct MediaSessionBudget {
    pub max_job_bytes: u64,
    pub max_job_duration_ms: u64,
    pub max_session_bytes: u64,
    pub max_session_duration_ms: u64,
    pub max_transcript_bytes: usize,
    pub job_timeout_ms: u64,
}

impl Default for MediaSessionBudget {
    fn default() -> Self {
        Self {
            max_job_bytes: DEFAULT_MAX_AUDIO_BYTES,
            max_job_duration_ms: DEFAULT_MAX_AUDIO_DURATION_MS,
            max_session_bytes: DEFAULT_MAX_SESSION_MEDIA_BYTES,
            max_session_duration_ms: DEFAULT_MAX_SESSION_MEDIA_DURATION_MS,
            max_transcript_bytes: DEFAULT_MAX_TRANSCRIPT_BYTES,
            job_timeout_ms: DEFAULT_MEDIA_JOB_TIMEOUT_MS,
        }
    }
}

/// Retention is explicit for raw, derived-text, and synthesized media classes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct MediaRetentionPolicy {
    pub raw_audio_ttl_ms: u64,
    pub transcript_ttl_ms: u64,
    pub synthesized_audio_ttl_ms: u64,
}

impl Default for MediaRetentionPolicy {
    fn default() -> Self {
        Self {
            raw_audio_ttl_ms: DEFAULT_RETENTION_MS,
            transcript_ttl_ms: 7 * DEFAULT_RETENTION_MS,
            synthesized_audio_ttl_ms: DEFAULT_RETENTION_MS,
        }
    }
}

/// Mutable accounting held by one session's media coordinator.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct MediaSessionUsage {
    pub bytes: u64,
    pub duration_ms: u64,
    pub billable_units: u64,
    pub estimated_cost_microunits: u64,
}

/// Cloneable cancellation source shared with in-flight STT and TTS jobs.
#[derive(Debug, Clone)]
pub struct AudioJobCancellation {
    sender: watch::Sender<bool>,
}

impl Default for AudioJobCancellation {
    fn default() -> Self {
        let (sender, _) = watch::channel(false);
        Self { sender }
    }
}

impl AudioJobCancellation {
    pub fn cancel(&self) {
        self.sender.send_replace(true);
    }

    async fn cancelled(&self) {
        let mut receiver = self.sender.subscribe();
        if *receiver.borrow() {
            return;
        }
        while receiver.changed().await.is_ok() {
            if *receiver.borrow() {
                return;
            }
        }
    }
}

/// Redacted per-session media state exposed to diagnostics and durable events.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MediaSessionDiagnosticsV1 {
    pub v: u32,
    pub active_jobs: u32,
    pub usage: MediaSessionUsage,
    pub reason_code: String,
}

#[derive(Debug)]
struct MediaSessionRuntime {
    usage: Arc<Mutex<MediaSessionUsage>>,
    active_jobs: HashMap<String, AudioJobCancellation>,
    last_touched_at_unix_ms: u64,
}

/// Bounded daemon-owned registry for session usage and in-flight cancellation.
///
/// The registry deliberately stores no raw audio, transcript text, principal,
/// or connector identity. Callers persist redacted job artifacts separately.
#[derive(Debug)]
pub struct AudioSessionRegistry {
    budget: MediaSessionBudget,
    retention: MediaRetentionPolicy,
    sessions: Mutex<HashMap<String, MediaSessionRuntime>>,
}

impl Default for AudioSessionRegistry {
    fn default() -> Self {
        Self::new(MediaSessionBudget::default(), MediaRetentionPolicy::default())
    }
}

impl AudioSessionRegistry {
    #[must_use]
    pub fn new(budget: MediaSessionBudget, retention: MediaRetentionPolicy) -> Self {
        Self { budget, retention, sessions: Mutex::new(HashMap::new()) }
    }

    /// Registers one in-flight job and returns a lease sharing the session budget.
    ///
    /// # Errors
    /// Returns a typed error for invalid identities, duplicate jobs, or bounded
    /// registry/job capacity exhaustion.
    pub fn begin_job(
        self: &Arc<Self>,
        session_id: &str,
        job_id: &str,
    ) -> Result<AudioSessionJob, AudioPipelineError> {
        validate_media_identity(session_id)?;
        validate_media_identity(job_id)?;
        let now_unix_ms = unix_ms();
        let mut sessions = lock_media_sessions(&self.sessions);
        if !sessions.contains_key(session_id) && sessions.len() >= MAX_TRACKED_MEDIA_SESSIONS {
            let evictable = sessions
                .iter()
                .filter(|(_, session)| session.active_jobs.is_empty())
                .min_by_key(|(_, session)| session.last_touched_at_unix_ms)
                .map(|(session_id, _)| session_id.clone());
            if let Some(evictable) = evictable {
                sessions.remove(evictable.as_str());
            }
        }
        if !sessions.contains_key(session_id) && sessions.len() >= MAX_TRACKED_MEDIA_SESSIONS {
            return Err(AudioPipelineError::SessionRegistryCapacityExceeded);
        }
        let session =
            sessions.entry(session_id.to_owned()).or_insert_with(|| MediaSessionRuntime {
                usage: Arc::new(Mutex::new(MediaSessionUsage::default())),
                active_jobs: HashMap::new(),
                last_touched_at_unix_ms: now_unix_ms,
            });
        if session.active_jobs.contains_key(job_id) {
            return Err(AudioPipelineError::DuplicateMediaJob);
        }
        if session.active_jobs.len() >= MAX_ACTIVE_MEDIA_JOBS_PER_SESSION {
            return Err(AudioPipelineError::SessionJobLimitExceeded);
        }
        let cancellation = AudioJobCancellation::default();
        session.active_jobs.insert(job_id.to_owned(), cancellation.clone());
        session.last_touched_at_unix_ms = now_unix_ms;
        let usage = Arc::clone(&session.usage);
        drop(sessions);
        Ok(AudioSessionJob {
            registry: Arc::clone(self),
            session_id: session_id.to_owned(),
            job_id: job_id.to_owned(),
            pipeline: AudioPipeline::with_shared_usage(self.budget, self.retention, usage),
            cancellation,
        })
    }

    /// Requests cancellation for one exact in-flight media job.
    #[must_use]
    pub fn cancel_job(&self, session_id: &str, job_id: &str) -> bool {
        let mut sessions = lock_media_sessions(&self.sessions);
        let Some(session) = sessions.get_mut(session_id) else {
            return false;
        };
        let Some(cancellation) = session.active_jobs.get(job_id) else {
            return false;
        };
        cancellation.cancel();
        session.last_touched_at_unix_ms = unix_ms();
        true
    }

    /// Requests cancellation for every in-flight job owned by one session.
    #[must_use]
    pub fn cancel_session(&self, session_id: &str) -> usize {
        let mut sessions = lock_media_sessions(&self.sessions);
        let Some(session) = sessions.get_mut(session_id) else {
            return 0;
        };
        for cancellation in session.active_jobs.values() {
            cancellation.cancel();
        }
        session.last_touched_at_unix_ms = unix_ms();
        session.active_jobs.len()
    }

    /// Returns a redacted bounded snapshot without exposing the session id.
    #[must_use]
    pub fn diagnostics(&self, session_id: &str) -> Option<MediaSessionDiagnosticsV1> {
        let sessions = lock_media_sessions(&self.sessions);
        let session = sessions.get(session_id)?;
        let usage = *lock_media_usage(&session.usage);
        Some(MediaSessionDiagnosticsV1 {
            v: AUDIO_PIPELINE_SCHEMA_VERSION,
            active_jobs: u32::try_from(session.active_jobs.len()).unwrap_or(u32::MAX),
            usage,
            reason_code: if session.active_jobs.is_empty() {
                "audio.session.idle"
            } else {
                "audio.session.active"
            }
            .to_owned(),
        })
    }

    fn finish_job(&self, session_id: &str, job_id: &str) {
        let mut sessions = lock_media_sessions(&self.sessions);
        if let Some(session) = sessions.get_mut(session_id) {
            session.active_jobs.remove(job_id);
            session.last_touched_at_unix_ms = unix_ms();
        }
    }
}

/// RAII owner for one registered media job.
#[derive(Debug)]
pub struct AudioSessionJob {
    registry: Arc<AudioSessionRegistry>,
    session_id: String,
    job_id: String,
    pipeline: AudioPipeline,
    cancellation: AudioJobCancellation,
}

impl AudioSessionJob {
    /// Returns the session-budgeted pipeline owned by this job lease.
    pub fn pipeline_mut(&mut self) -> &mut AudioPipeline {
        &mut self.pipeline
    }

    /// Returns a cancellation source tied to this exact job generation.
    #[must_use]
    pub fn cancellation(&self) -> AudioJobCancellation {
        self.cancellation.clone()
    }

    /// Returns the aggregate media usage committed by this session.
    #[must_use]
    pub fn usage(&self) -> MediaSessionUsage {
        self.pipeline.usage()
    }
}

impl Drop for AudioSessionJob {
    fn drop(&mut self) {
        self.registry.finish_job(self.session_id.as_str(), self.job_id.as_str());
    }
}

/// Provider-neutral STT request after media admission.
#[derive(Debug, Clone)]
pub struct AudioTranscriptionJobRequest {
    pub file_name: String,
    pub content_type: String,
    pub bytes: Vec<u8>,
    pub language_hint: Option<String>,
}

/// Provider-neutral STT response used to create the durable artifact.
#[derive(Debug, Clone)]
pub struct AudioTranscriptionBackendResult {
    pub text: String,
    pub detected_language: Option<String>,
    pub confidence: Option<f32>,
    pub duration_ms: Option<u64>,
    pub model_name: String,
    pub usage: MediaUsage,
}

impl From<AudioTranscriptionResponse> for AudioTranscriptionBackendResult {
    fn from(response: AudioTranscriptionResponse) -> Self {
        let duration_ms = response.duration_ms;
        Self {
            text: response.text,
            detected_language: response.language,
            confidence: None,
            duration_ms,
            model_name: response.model_name,
            usage: MediaUsage {
                audio_duration_ms: duration_ms.unwrap_or_default(),
                billable_units: duration_ms.unwrap_or_default(),
                ..MediaUsage::default()
            },
        }
    }
}

#[async_trait]
pub trait AudioTranscriptionBackend: Send + Sync {
    async fn transcribe(
        &self,
        request: AudioTranscriptionJobRequest,
    ) -> Result<AudioTranscriptionBackendResult, String>;
}

/// Provider-neutral TTS request executed only after text delivery settles.
#[derive(Debug, Clone)]
pub struct AudioSynthesisJobRequest {
    pub source_run_id: String,
    pub text: String,
    pub voice_id: String,
    pub codec: String,
    pub delivery: MediaDeliveryDescriptor,
}

/// Explicit caller opt-in for one bounded post-delivery speech transform.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AudioOutputRequestV1 {
    pub voice_id: String,
    pub codec: String,
}

/// Ephemeral TTS payload plus the metadata needed for durable storage.
#[derive(Debug, Clone)]
pub struct AudioSynthesisBackendResult {
    pub bytes: Vec<u8>,
    pub content_type: String,
    pub codec: String,
    pub duration_ms: u64,
    pub model_name: String,
    pub voice_id: String,
    pub usage: MediaUsage,
}

impl From<AudioSynthesisResponse> for AudioSynthesisBackendResult {
    fn from(response: AudioSynthesisResponse) -> Self {
        Self {
            bytes: response.bytes,
            content_type: response.content_type,
            codec: response.codec,
            duration_ms: 0,
            model_name: response.model_name,
            voice_id: response.voice,
            usage: MediaUsage {
                billable_units: response.input_characters,
                ..MediaUsage::default()
            },
        }
    }
}

/// Provider-neutral synthesis failures that are safe to expose to the media lifecycle.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum AudioSynthesisBackendError {
    #[error("audio synthesis is unsupported by the configured provider")]
    UnsupportedProvider,
    #[error("audio synthesis backend failed")]
    Failed,
}

#[async_trait]
pub trait AudioSynthesisBackend: Send + Sync {
    async fn synthesize(
        &self,
        request: AudioSynthesisJobRequest,
    ) -> Result<AudioSynthesisBackendResult, AudioSynthesisBackendError>;
}

/// Text delivery is immutable input to the optional post-transform.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TextDeliveryReceipt {
    pub run_id: String,
    pub text: String,
    pub success: bool,
    pub delivered_at_unix_ms: u64,
}

/// TTS outcome deliberately repeats text success to prove it cannot replace it.
#[derive(Debug, Clone)]
pub struct TtsPostDeliveryOutcome {
    pub text_run_success: bool,
    pub state: MediaJobState,
    pub reason_code: String,
    pub artifact: Option<AudioOutputArtifactV1>,
    pub payload: Option<Vec<u8>>,
}

/// Stable media lifecycle states used by diagnostics and metadata traces.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum MediaJobState {
    Succeeded,
    Failed,
    TimedOut,
    Cancelled,
    Blocked,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum AudioPipelineError {
    #[error("media job cancelled")]
    Cancelled,
    #[error("media job timed out")]
    TimedOut,
    #[error("media job exceeds per-job byte budget")]
    JobByteBudgetExceeded,
    #[error("media job exceeds per-job duration budget")]
    JobDurationBudgetExceeded,
    #[error("media session byte budget exhausted")]
    SessionByteBudgetExceeded,
    #[error("media session duration budget exhausted")]
    SessionDurationBudgetExceeded,
    #[error("media session or job identity is invalid")]
    InvalidMediaIdentity,
    #[error("media session already owns this job")]
    DuplicateMediaJob,
    #[error("media session registry capacity exhausted")]
    SessionRegistryCapacityExceeded,
    #[error("media session active job limit exhausted")]
    SessionJobLimitExceeded,
    #[error("audio payload does not match artifact metadata")]
    ArtifactIntegrityMismatch,
    #[error("audio artifact metadata is invalid")]
    ArtifactContractInvalid,
    #[error("transcription output exceeds transcript budget")]
    TranscriptBudgetExceeded,
    #[error("media backend returned invalid output")]
    BackendOutputInvalid,
    #[error("media backend failed: {0}")]
    Backend(String),
}

impl AudioPipelineError {
    pub fn reason_code(&self) -> &'static str {
        match self {
            Self::Cancelled => "audio_job_cancelled",
            Self::TimedOut => "audio_job_timeout",
            Self::JobByteBudgetExceeded => "audio_job_byte_budget_exceeded",
            Self::JobDurationBudgetExceeded => "audio_job_duration_budget_exceeded",
            Self::SessionByteBudgetExceeded => "audio_session_byte_budget_exceeded",
            Self::SessionDurationBudgetExceeded => "audio_session_duration_budget_exceeded",
            Self::InvalidMediaIdentity => "audio_session_identity_invalid",
            Self::DuplicateMediaJob => "audio_session_job_duplicate",
            Self::SessionRegistryCapacityExceeded => "audio_session_registry_capacity_exceeded",
            Self::SessionJobLimitExceeded => "audio_session_job_limit_exceeded",
            Self::ArtifactIntegrityMismatch => "audio_artifact_integrity_mismatch",
            Self::ArtifactContractInvalid => "audio_artifact_contract_invalid",
            Self::TranscriptBudgetExceeded => "audio_transcript_budget_exceeded",
            Self::BackendOutputInvalid => "audio_backend_output_invalid",
            Self::Backend(_) => "audio_backend_failed",
        }
    }
}

/// Admitted connector-neutral media pipeline for one session.
#[derive(Debug)]
pub struct AudioPipeline {
    budget: MediaSessionBudget,
    retention: MediaRetentionPolicy,
    usage: Arc<Mutex<MediaSessionUsage>>,
}

impl AudioPipeline {
    pub fn new(budget: MediaSessionBudget, retention: MediaRetentionPolicy) -> Self {
        Self::with_shared_usage(
            budget,
            retention,
            Arc::new(Mutex::new(MediaSessionUsage::default())),
        )
    }

    fn with_shared_usage(
        budget: MediaSessionBudget,
        retention: MediaRetentionPolicy,
        usage: Arc<Mutex<MediaSessionUsage>>,
    ) -> Self {
        Self { budget, retention, usage }
    }

    pub fn usage(&self) -> MediaSessionUsage {
        *lock_media_usage(&self.usage)
    }

    /// Runs STT and produces a redacted, non-authoritative context segment.
    pub async fn transcribe<B: AudioTranscriptionBackend>(
        &mut self,
        input: &AudioInputArtifactV1,
        raw_audio: &[u8],
        backend: &B,
        cancellation: &AudioJobCancellation,
    ) -> Result<TranscriptionArtifact, AudioPipelineError> {
        self.validate_job_bounds(input.bytes, input.duration_ms)?;
        if !valid_audio_input_artifact(input) {
            return Err(AudioPipelineError::ArtifactContractInvalid);
        }
        if input.bytes != u64::try_from(raw_audio.len()).unwrap_or(u64::MAX)
            || input.sha256 != sha256_hex(raw_audio)
        {
            return Err(AudioPipelineError::ArtifactIntegrityMismatch);
        }
        let request = AudioTranscriptionJobRequest {
            file_name: input.file_name.clone(),
            content_type: input.content_type.clone(),
            bytes: raw_audio.to_vec(),
            language_hint: input.language_hint.clone(),
        };
        let backend_future = tokio::time::timeout(
            Duration::from_millis(self.budget.job_timeout_ms.max(1)),
            backend.transcribe(request),
        );
        let result = tokio::select! {
            // A reset cancellation must win when an immediately-ready backend races it.
            biased;
            () = cancellation.cancelled() => return Err(AudioPipelineError::Cancelled),
            result = backend_future => result,
        };
        let response = result
            .map_err(|_| AudioPipelineError::TimedOut)?
            .map_err(AudioPipelineError::Backend)?;
        if response.text.is_empty() || response.text.len() > self.budget.max_transcript_bytes {
            return Err(AudioPipelineError::TranscriptBudgetExceeded);
        }
        if !valid_bounded_text(response.model_name.as_str(), 256)
            || response
                .detected_language
                .as_deref()
                .is_some_and(|language| !valid_language_tag(language))
            || response.confidence.is_some_and(|confidence| !confidence.is_finite())
        {
            return Err(AudioPipelineError::BackendOutputInvalid);
        }
        let redaction = redact_text_for_export(
            response.text.as_str(),
            SafetySourceKind::AttachmentRecall,
            SafetyContentKind::PlainText,
            TrustLabel::ExternalUntrusted,
        );
        let created_at_unix_ms = unix_ms();
        let artifact_id = Ulid::new().to_string();
        let text = redaction.redacted_text;
        let transcript_sha256 = sha256_hex(text.as_bytes());
        let duration_ms = response.duration_ms.unwrap_or(input.duration_ms);
        let usage = MediaUsage {
            input_bytes: input.bytes,
            audio_duration_ms: duration_ms,
            ..response.usage
        };
        self.commit_usage(usage)?;
        let context_segment = UntrustedTranscriptContextSegment {
            text: text.clone(),
            trust_label: "external_untrusted".to_owned(),
            instruction_authority: false,
            artifact_citation: format!("artifact://{artifact_id}"),
            source_artifact_sha256: input.sha256.clone(),
        };
        Ok(TranscriptionArtifact {
            v: AUDIO_PIPELINE_SCHEMA_VERSION,
            artifact_id,
            source_artifact_id: input.artifact_id.clone(),
            source_artifact_sha256: input.sha256.clone(),
            transcript_sha256,
            text,
            detected_language: response.detected_language.or_else(|| input.language_hint.clone()),
            confidence: response.confidence.map(|value| value.clamp(0.0, 1.0)),
            duration_ms,
            model_name: response.model_name,
            usage,
            redacted: redaction.redacted,
            created_at_unix_ms,
            expires_at_unix_ms: created_at_unix_ms.saturating_add(self.retention.transcript_ttl_ms),
            context_segment,
        })
    }

    /// Runs optional TTS without changing the already-settled text receipt.
    pub async fn synthesize_after_delivery<B: AudioSynthesisBackend>(
        &mut self,
        receipt: &TextDeliveryReceipt,
        output_request: &AudioOutputRequestV1,
        delivery: MediaDeliveryDescriptor,
        backend: &B,
        cancellation: &AudioJobCancellation,
    ) -> TtsPostDeliveryOutcome {
        if !receipt.success {
            return TtsPostDeliveryOutcome {
                text_run_success: false,
                state: MediaJobState::Blocked,
                reason_code: "tts_text_delivery_not_successful".to_owned(),
                artifact: None,
                payload: None,
            };
        }
        if validate_media_identity(receipt.run_id.as_str()).is_err()
            || receipt.text.trim().is_empty()
            || receipt.text.len() > MAX_SYNTHESIS_TEXT_BYTES
            || !valid_media_delivery_descriptor(&delivery)
            || !valid_bounded_text(output_request.voice_id.as_str(), 128)
            || audio_output_content_type(output_request.codec.as_str()).is_none()
        {
            return failed_tts_outcome(
                true,
                MediaJobState::Blocked,
                "tts_request_contract_invalid",
            );
        }
        let request = AudioSynthesisJobRequest {
            source_run_id: receipt.run_id.clone(),
            text: receipt.text.clone(),
            voice_id: output_request.voice_id.clone(),
            codec: output_request.codec.clone(),
            delivery: delivery.clone(),
        };
        let backend_future = tokio::time::timeout(
            Duration::from_millis(self.budget.job_timeout_ms.max(1)),
            backend.synthesize(request),
        );
        let response = tokio::select! {
            // Preserve the reset fence even if a backend becomes ready in the same poll.
            biased;
            () = cancellation.cancelled() => {
                return failed_tts_outcome(true, MediaJobState::Cancelled, "tts_job_cancelled");
            }
            response = backend_future => response,
        };
        let response = match response {
            Err(_) => {
                return failed_tts_outcome(true, MediaJobState::TimedOut, "tts_job_timeout");
            }
            Ok(Err(AudioSynthesisBackendError::UnsupportedProvider)) => {
                return failed_tts_outcome(
                    true,
                    MediaJobState::Blocked,
                    "tts_provider_unsupported",
                );
            }
            Ok(Err(AudioSynthesisBackendError::Failed)) => {
                return failed_tts_outcome(true, MediaJobState::Failed, "tts_backend_failed");
            }
            Ok(Ok(response)) => response,
        };
        let output_bytes = u64::try_from(response.bytes.len()).unwrap_or(u64::MAX);
        if response.bytes.is_empty()
            || output_bytes > DEFAULT_MAX_AUDIO_BYTES
            || response.duration_ms > DEFAULT_MAX_AUDIO_DURATION_MS
            || response.content_type != delivery.content_type
            || response.codec != output_request.codec
            || response.voice_id != output_request.voice_id
            || !valid_bounded_text(response.model_name.as_str(), 256)
        {
            return failed_tts_outcome(true, MediaJobState::Failed, "tts_backend_output_invalid");
        }
        let usage =
            MediaUsage { output_bytes, audio_duration_ms: response.duration_ms, ..response.usage };
        if let Err(error) = self.commit_usage(usage) {
            return failed_tts_outcome(true, MediaJobState::Blocked, error.reason_code());
        }
        let created_at_unix_ms = unix_ms();
        let artifact = AudioOutputArtifactV1 {
            v: AUDIO_PIPELINE_SCHEMA_VERSION,
            artifact_id: Ulid::new().to_string(),
            source_run_id: receipt.run_id.clone(),
            source_text_sha256: sha256_hex(receipt.text.as_bytes()),
            content_type: response.content_type,
            codec: response.codec,
            bytes: output_bytes,
            duration_ms: response.duration_ms,
            sha256: sha256_hex(response.bytes.as_slice()),
            model_name: response.model_name,
            voice_id: response.voice_id,
            usage,
            created_at_unix_ms,
            expires_at_unix_ms: created_at_unix_ms
                .saturating_add(self.retention.synthesized_audio_ttl_ms),
            delivery,
        };
        TtsPostDeliveryOutcome {
            text_run_success: true,
            state: MediaJobState::Succeeded,
            reason_code: "tts_succeeded".to_owned(),
            artifact: Some(artifact),
            payload: Some(response.bytes),
        }
    }

    fn validate_job_bounds(&self, bytes: u64, duration_ms: u64) -> Result<(), AudioPipelineError> {
        if bytes > self.budget.max_job_bytes {
            return Err(AudioPipelineError::JobByteBudgetExceeded);
        }
        if duration_ms > self.budget.max_job_duration_ms {
            return Err(AudioPipelineError::JobDurationBudgetExceeded);
        }
        Ok(())
    }

    fn commit_usage(&self, usage: MediaUsage) -> Result<(), AudioPipelineError> {
        let bytes = usage.input_bytes.saturating_add(usage.output_bytes);
        self.validate_job_bounds(bytes, usage.audio_duration_ms)?;
        let mut session_usage = lock_media_usage(&self.usage);
        if session_usage.bytes.saturating_add(bytes) > self.budget.max_session_bytes {
            return Err(AudioPipelineError::SessionByteBudgetExceeded);
        }
        if session_usage.duration_ms.saturating_add(usage.audio_duration_ms)
            > self.budget.max_session_duration_ms
        {
            return Err(AudioPipelineError::SessionDurationBudgetExceeded);
        }
        session_usage.bytes = session_usage.bytes.saturating_add(bytes);
        session_usage.duration_ms =
            session_usage.duration_ms.saturating_add(usage.audio_duration_ms);
        session_usage.billable_units =
            session_usage.billable_units.saturating_add(usage.billable_units);
        session_usage.estimated_cost_microunits =
            session_usage.estimated_cost_microunits.saturating_add(usage.estimated_cost_microunits);
        Ok(())
    }
}

/// Returns the media type for one supported post-delivery codec.
#[must_use]
pub fn audio_output_content_type(codec: &str) -> Option<&'static str> {
    match codec {
        "mp3" => Some("audio/mpeg"),
        "opus" => Some("audio/ogg"),
        "aac" => Some("audio/aac"),
        "flac" => Some("audio/flac"),
        "wav" => Some("audio/wav"),
        _ => None,
    }
}

/// Returns the safe filename extension for one supported post-delivery codec.
#[must_use]
pub fn audio_output_file_extension(codec: &str) -> Option<&'static str> {
    match codec {
        "mp3" => Some("mp3"),
        "opus" => Some("ogg"),
        "aac" => Some("aac"),
        "flac" => Some("flac"),
        "wav" => Some("wav"),
        _ => None,
    }
}

fn valid_media_delivery_descriptor(delivery: &MediaDeliveryDescriptor) -> bool {
    validate_media_identity(delivery.delivery_key.as_str()).is_ok()
        && valid_sha256(delivery.destination_scope_sha256.as_str())
        && valid_audio_media_type(delivery.content_type.as_str())
        && !delivery.file_name.trim().is_empty()
        && delivery.file_name.len() <= 512
        && !delivery.file_name.chars().any(char::is_control)
        && !delivery.file_name.chars().any(|character| matches!(character, '/' | '\\'))
        && !matches!(delivery.file_name.as_str(), "." | "..")
}

fn valid_media_type_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric()
        || matches!(byte, b'!' | b'#' | b'$' | b'&' | b'^' | b'_' | b'.' | b'+' | b'-')
}

fn validate_media_identity(value: &str) -> Result<(), AudioPipelineError> {
    if value.trim().is_empty()
        || value.len() > 128
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'.' | b':' | b'-'))
    {
        return Err(AudioPipelineError::InvalidMediaIdentity);
    }
    Ok(())
}

fn valid_audio_input_artifact(input: &AudioInputArtifactV1) -> bool {
    input.v == AUDIO_PIPELINE_SCHEMA_VERSION
        && validate_media_identity(input.artifact_id.as_str()).is_ok()
        && valid_file_name(input.file_name.as_str())
        && valid_audio_media_type(input.content_type.as_str())
        && valid_bounded_text(input.codec.as_str(), 128)
        && (1..=DEFAULT_MAX_AUDIO_BYTES).contains(&input.bytes)
        && input.duration_ms <= DEFAULT_MAX_AUDIO_DURATION_MS
        && input.language_hint.as_deref().is_none_or(valid_language_tag)
        && valid_sha256(input.sha256.as_str())
        && input.expires_at_unix_ms >= input.created_at_unix_ms
        && valid_bounded_text(input.provenance.source_kind.as_str(), 128)
        && valid_sha256(input.provenance.source_reference_sha256.as_str())
        && valid_sha256(input.provenance.principal_scope_sha256.as_str())
        && validate_media_identity(input.provenance.session_id.as_str()).is_ok()
}

fn valid_audio_media_type(value: &str) -> bool {
    value.len() <= 128
        && value.split_once('/').is_some_and(|(kind, subtype)| {
            kind == "audio" && !subtype.is_empty() && subtype.bytes().all(valid_media_type_byte)
        })
}

fn valid_bounded_text(value: &str, max_bytes: usize) -> bool {
    !value.trim().is_empty() && value.len() <= max_bytes && !value.chars().any(char::is_control)
}

fn valid_file_name(value: &str) -> bool {
    valid_bounded_text(value, 512)
        && !value.chars().any(|character| matches!(character, '/' | '\\'))
        && !matches!(value, "." | "..")
}

fn valid_language_tag(value: &str) -> bool {
    (2..=35).contains(&value.len())
        && value.bytes().all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
}

fn valid_sha256(value: &str) -> bool {
    value.len() == 64
        && value.bytes().all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
}

fn lock_media_sessions(
    sessions: &Mutex<HashMap<String, MediaSessionRuntime>>,
) -> MutexGuard<'_, HashMap<String, MediaSessionRuntime>> {
    sessions.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn lock_media_usage(usage: &Mutex<MediaSessionUsage>) -> MutexGuard<'_, MediaSessionUsage> {
    usage.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn failed_tts_outcome(
    text_run_success: bool,
    state: MediaJobState,
    reason_code: &str,
) -> TtsPostDeliveryOutcome {
    TtsPostDeliveryOutcome {
        text_run_success,
        state,
        reason_code: reason_code.to_owned(),
        artifact: None,
        payload: None,
    }
}

fn unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use std::sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    };

    use super::*;

    #[derive(Debug)]
    struct StubStt {
        delay: Duration,
        result: Result<AudioTranscriptionBackendResult, String>,
    }

    #[async_trait]
    impl AudioTranscriptionBackend for StubStt {
        async fn transcribe(
            &self,
            _request: AudioTranscriptionJobRequest,
        ) -> Result<AudioTranscriptionBackendResult, String> {
            tokio::time::sleep(self.delay).await;
            self.result.clone()
        }
    }

    #[derive(Debug)]
    struct StubTts {
        fail: bool,
        invoked: Arc<AtomicBool>,
    }

    #[async_trait]
    impl AudioSynthesisBackend for StubTts {
        async fn synthesize(
            &self,
            request: AudioSynthesisJobRequest,
        ) -> Result<AudioSynthesisBackendResult, AudioSynthesisBackendError> {
            self.invoked.store(true, Ordering::SeqCst);
            if self.fail {
                return Err(AudioSynthesisBackendError::Failed);
            }
            Ok(AudioSynthesisBackendResult {
                bytes: b"synthetic-audio".to_vec(),
                content_type: "audio/ogg".to_owned(),
                codec: request.codec,
                duration_ms: 900,
                model_name: "tts-test".to_owned(),
                voice_id: request.voice_id,
                usage: MediaUsage {
                    billable_units: 14,
                    estimated_cost_microunits: 2,
                    ..MediaUsage::default()
                },
            })
        }
    }

    #[derive(Debug)]
    struct SlowTts;

    #[async_trait]
    impl AudioSynthesisBackend for SlowTts {
        async fn synthesize(
            &self,
            _request: AudioSynthesisJobRequest,
        ) -> Result<AudioSynthesisBackendResult, AudioSynthesisBackendError> {
            tokio::time::sleep(Duration::from_millis(100)).await;
            Err(AudioSynthesisBackendError::Failed)
        }
    }

    #[derive(Debug)]
    struct UnsupportedTts;

    #[async_trait]
    impl AudioSynthesisBackend for UnsupportedTts {
        async fn synthesize(
            &self,
            _request: AudioSynthesisJobRequest,
        ) -> Result<AudioSynthesisBackendResult, AudioSynthesisBackendError> {
            Err(AudioSynthesisBackendError::UnsupportedProvider)
        }
    }

    fn provenance() -> AudioArtifactProvenance {
        AudioArtifactProvenance {
            source_kind: "connector_attachment".to_owned(),
            source_reference_sha256: sha256_hex(b"source"),
            received_at_unix_ms: unix_ms(),
            principal_scope_sha256: sha256_hex(b"principal"),
            session_id: "session-1".to_owned(),
        }
    }

    fn input(bytes: &[u8], duration_ms: u64, language_hint: Option<&str>) -> AudioInputArtifactV1 {
        AudioInputArtifactV1::from_payload(
            AudioInputDescriptor {
                file_name: "voice.ogg".to_owned(),
                content_type: "audio/ogg".to_owned(),
                codec: "opus".to_owned(),
                duration_ms,
                language_hint: language_hint.map(str::to_owned),
            },
            bytes,
            provenance(),
            MediaRetentionPolicy::default(),
        )
    }

    fn stt_result(text: &str, language: Option<&str>) -> AudioTranscriptionBackendResult {
        AudioTranscriptionBackendResult {
            text: text.to_owned(),
            detected_language: language.map(str::to_owned),
            confidence: Some(0.91),
            duration_ms: Some(1_250),
            model_name: "stt-test".to_owned(),
            usage: MediaUsage {
                billable_units: 1_250,
                estimated_cost_microunits: 3,
                ..MediaUsage::default()
            },
        }
    }

    #[tokio::test]
    async fn stt_success_auto_detects_language_and_builds_untrusted_citation() {
        let bytes = b"audio";
        let artifact = input(bytes, 1_250, None);
        let backend =
            StubStt { delay: Duration::ZERO, result: Ok(stt_result("Dobry den", Some("cs"))) };
        let mut pipeline =
            AudioPipeline::new(MediaSessionBudget::default(), MediaRetentionPolicy::default());
        let transcript = pipeline
            .transcribe(&artifact, bytes, &backend, &AudioJobCancellation::default())
            .await
            .expect("STT should succeed");

        assert_eq!(transcript.detected_language.as_deref(), Some("cs"));
        assert_eq!(transcript.context_segment.trust_label, "external_untrusted");
        assert!(!transcript.context_segment.instruction_authority);
        assert!(transcript.context_segment.artifact_citation.starts_with("artifact://"));
        assert_eq!(pipeline.usage().duration_ms, 1_250);
        assert_eq!(transcript.usage.estimated_cost_microunits, 3);
    }

    #[tokio::test]
    async fn stt_timeout_cancel_long_audio_and_secret_transcript_fail_closed() {
        let bytes = b"audio";
        let artifact = input(bytes, 1_250, Some("en"));
        let mut timeout_pipeline = AudioPipeline::new(
            MediaSessionBudget { job_timeout_ms: 5, ..MediaSessionBudget::default() },
            MediaRetentionPolicy::default(),
        );
        let slow_backend =
            StubStt { delay: Duration::from_millis(100), result: Ok(stt_result("late", None)) };
        assert_eq!(
            timeout_pipeline
                .transcribe(&artifact, bytes, &slow_backend, &AudioJobCancellation::default())
                .await,
            Err(AudioPipelineError::TimedOut)
        );

        let cancellation = AudioJobCancellation::default();
        cancellation.cancel();
        assert_eq!(
            timeout_pipeline.transcribe(&artifact, bytes, &slow_backend, &cancellation).await,
            Err(AudioPipelineError::Cancelled)
        );

        let long = input(bytes, DEFAULT_MAX_AUDIO_DURATION_MS + 1, None);
        assert_eq!(
            timeout_pipeline
                .transcribe(&long, bytes, &slow_backend, &AudioJobCancellation::default())
                .await,
            Err(AudioPipelineError::JobDurationBudgetExceeded)
        );

        let secret_backend = StubStt {
            delay: Duration::ZERO,
            result: Ok(stt_result("authorization: Bearer sk-super-secret-value", Some("en"))),
        };
        let mut redacting_pipeline =
            AudioPipeline::new(MediaSessionBudget::default(), MediaRetentionPolicy::default());
        let transcript = redacting_pipeline
            .transcribe(&artifact, bytes, &secret_backend, &AudioJobCancellation::default())
            .await
            .expect("secret-like transcript should be redacted, not rejected");
        assert!(transcript.redacted);
        assert!(!transcript.text.contains("sk-super-secret-value"));
    }

    #[tokio::test]
    async fn stt_rejects_schema_invalid_artifacts_and_backend_metadata() {
        let bytes = b"audio";
        let mut artifact = input(bytes, 1_250, None);
        artifact.file_name = "../voice.ogg".to_owned();
        let backend =
            StubStt { delay: Duration::ZERO, result: Ok(stt_result("transcript", Some("en"))) };
        let mut pipeline =
            AudioPipeline::new(MediaSessionBudget::default(), MediaRetentionPolicy::default());
        assert_eq!(
            pipeline.transcribe(&artifact, bytes, &backend, &AudioJobCancellation::default()).await,
            Err(AudioPipelineError::ArtifactContractInvalid)
        );

        let invalid_backend = StubStt {
            delay: Duration::ZERO,
            result: Ok(AudioTranscriptionBackendResult {
                model_name: String::new(),
                ..stt_result("transcript", Some("en"))
            }),
        };
        assert_eq!(
            pipeline
                .transcribe(
                    &input(bytes, 1_250, None),
                    bytes,
                    &invalid_backend,
                    &AudioJobCancellation::default(),
                )
                .await,
            Err(AudioPipelineError::BackendOutputInvalid)
        );
    }

    #[tokio::test]
    async fn tts_failure_and_cancel_preserve_text_success_and_delivery_is_connector_neutral() {
        let receipt = TextDeliveryReceipt {
            run_id: "run-1".to_owned(),
            text: "Delivered text".to_owned(),
            success: true,
            delivered_at_unix_ms: unix_ms(),
        };
        let delivery = MediaDeliveryDescriptor {
            delivery_key: "delivery-1".to_owned(),
            destination_scope_sha256: sha256_hex(b"destination"),
            content_type: "audio/ogg".to_owned(),
            file_name: "reply.ogg".to_owned(),
        };
        let invoked = Arc::new(AtomicBool::new(false));
        let failing = StubTts { fail: true, invoked: Arc::clone(&invoked) };
        let output_request =
            AudioOutputRequestV1 { voice_id: "voice-a".to_owned(), codec: "opus".to_owned() };
        let mut pipeline =
            AudioPipeline::new(MediaSessionBudget::default(), MediaRetentionPolicy::default());
        let failure = pipeline
            .synthesize_after_delivery(
                &receipt,
                &output_request,
                delivery.clone(),
                &failing,
                &AudioJobCancellation::default(),
            )
            .await;
        assert!(failure.text_run_success);
        assert_eq!(failure.state, MediaJobState::Failed);
        assert_eq!(failure.reason_code, "tts_backend_failed");
        assert!(invoked.load(Ordering::SeqCst));

        let cancellation = AudioJobCancellation::default();
        cancellation.cancel();
        let cancelled = pipeline
            .synthesize_after_delivery(
                &receipt,
                &output_request,
                delivery.clone(),
                &failing,
                &cancellation,
            )
            .await;
        assert!(cancelled.text_run_success);
        assert_eq!(cancelled.state, MediaJobState::Cancelled);

        let successful = StubTts { fail: false, invoked: Arc::new(AtomicBool::new(false)) };
        let success = pipeline
            .synthesize_after_delivery(
                &receipt,
                &output_request,
                delivery.clone(),
                &successful,
                &AudioJobCancellation::default(),
            )
            .await;
        assert_eq!(success.state, MediaJobState::Succeeded);
        let artifact = success.artifact.expect("TTS artifact should exist");
        assert_eq!(artifact.delivery, delivery);
        assert_eq!(artifact.source_run_id, receipt.run_id);
        assert_eq!(artifact.usage.audio_duration_ms, 900);
        assert_eq!(pipeline.usage().duration_ms, 900);
        assert!(success.payload.is_some());
    }

    #[tokio::test]
    async fn tts_never_runs_before_successful_text_delivery() {
        let receipt = TextDeliveryReceipt {
            run_id: "run-1".to_owned(),
            text: "Unsettled text".to_owned(),
            success: false,
            delivered_at_unix_ms: unix_ms(),
        };
        let invoked = Arc::new(AtomicBool::new(false));
        let backend = StubTts { fail: false, invoked: Arc::clone(&invoked) };
        let output_request =
            AudioOutputRequestV1 { voice_id: "voice-a".to_owned(), codec: "opus".to_owned() };
        let mut pipeline =
            AudioPipeline::new(MediaSessionBudget::default(), MediaRetentionPolicy::default());
        let outcome = pipeline
            .synthesize_after_delivery(
                &receipt,
                &output_request,
                MediaDeliveryDescriptor {
                    delivery_key: "delivery-1".to_owned(),
                    destination_scope_sha256: sha256_hex(b"destination"),
                    content_type: "audio/ogg".to_owned(),
                    file_name: "reply.ogg".to_owned(),
                },
                &backend,
                &AudioJobCancellation::default(),
            )
            .await;

        assert!(!outcome.text_run_success);
        assert_eq!(outcome.state, MediaJobState::Blocked);
        assert_eq!(outcome.reason_code, "tts_text_delivery_not_successful");
        assert!(!invoked.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn tts_rejects_unsafe_delivery_filename_before_backend_dispatch() {
        let receipt = TextDeliveryReceipt {
            run_id: "run-1".to_owned(),
            text: "Delivered text".to_owned(),
            success: true,
            delivered_at_unix_ms: unix_ms(),
        };
        let invoked = Arc::new(AtomicBool::new(false));
        let backend = StubTts { fail: false, invoked: Arc::clone(&invoked) };
        let output_request =
            AudioOutputRequestV1 { voice_id: "voice-a".to_owned(), codec: "opus".to_owned() };
        let mut pipeline =
            AudioPipeline::new(MediaSessionBudget::default(), MediaRetentionPolicy::default());
        let outcome = pipeline
            .synthesize_after_delivery(
                &receipt,
                &output_request,
                MediaDeliveryDescriptor {
                    delivery_key: "delivery-1".to_owned(),
                    destination_scope_sha256: sha256_hex(b"destination"),
                    content_type: "audio/ogg".to_owned(),
                    file_name: "../reply.ogg".to_owned(),
                },
                &backend,
                &AudioJobCancellation::default(),
            )
            .await;

        assert!(outcome.text_run_success);
        assert_eq!(outcome.state, MediaJobState::Blocked);
        assert_eq!(outcome.reason_code, "tts_request_contract_invalid");
        assert!(!invoked.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn tts_timeout_and_unsupported_provider_preserve_text_success() {
        let receipt = TextDeliveryReceipt {
            run_id: "run-1".to_owned(),
            text: "Delivered text".to_owned(),
            success: true,
            delivered_at_unix_ms: unix_ms(),
        };
        let request =
            AudioOutputRequestV1 { voice_id: "voice-a".to_owned(), codec: "mp3".to_owned() };
        let delivery = MediaDeliveryDescriptor {
            delivery_key: "delivery-1".to_owned(),
            destination_scope_sha256: sha256_hex(b"destination"),
            content_type: "audio/mpeg".to_owned(),
            file_name: "reply.mp3".to_owned(),
        };
        let mut pipeline = AudioPipeline::new(
            MediaSessionBudget { job_timeout_ms: 5, ..MediaSessionBudget::default() },
            MediaRetentionPolicy::default(),
        );
        let timed_out = pipeline
            .synthesize_after_delivery(
                &receipt,
                &request,
                delivery.clone(),
                &SlowTts,
                &AudioJobCancellation::default(),
            )
            .await;
        assert!(timed_out.text_run_success);
        assert_eq!(timed_out.state, MediaJobState::TimedOut);
        assert_eq!(timed_out.reason_code, "tts_job_timeout");

        let unsupported = pipeline
            .synthesize_after_delivery(
                &receipt,
                &request,
                delivery,
                &UnsupportedTts,
                &AudioJobCancellation::default(),
            )
            .await;
        assert!(unsupported.text_run_success);
        assert_eq!(unsupported.state, MediaJobState::Blocked);
        assert_eq!(unsupported.reason_code, "tts_provider_unsupported");
    }

    #[tokio::test]
    async fn session_registry_shares_budget_usage_and_cancellation_across_jobs() {
        let registry = Arc::new(AudioSessionRegistry::new(
            MediaSessionBudget {
                max_job_bytes: 8,
                max_session_bytes: 10,
                ..MediaSessionBudget::default()
            },
            MediaRetentionPolicy::default(),
        ));
        let first_bytes = b"first!";
        let second_bytes = b"second";
        let backend = StubStt {
            delay: Duration::ZERO,
            result: Ok(stt_result("shared transcript", Some("en"))),
        };
        let mut first = registry.begin_job("session-shared", "job-1").expect("first job");
        let first_cancellation = first.cancellation();
        first
            .pipeline_mut()
            .transcribe(
                &input(first_bytes, 1_250, None),
                first_bytes,
                &backend,
                &first_cancellation,
            )
            .await
            .expect("first job should fit the shared budget");
        assert_eq!(first.usage().bytes, 6);
        assert_eq!(first.usage().estimated_cost_microunits, 3);

        let mut second = registry.begin_job("session-shared", "job-2").expect("second job");
        let second_cancellation = second.cancellation();
        assert_eq!(
            second
                .pipeline_mut()
                .transcribe(
                    &input(second_bytes, 1_250, None),
                    second_bytes,
                    &backend,
                    &second_cancellation,
                )
                .await,
            Err(AudioPipelineError::SessionByteBudgetExceeded)
        );
        assert_eq!(second.usage().bytes, 6);

        assert!(registry.cancel_job("session-shared", "job-2"));
        assert_eq!(
            second
                .pipeline_mut()
                .transcribe(&input(b"x", 1, None), b"x", &backend, &second_cancellation,)
                .await,
            Err(AudioPipelineError::Cancelled)
        );
        drop(first);
        drop(second);
        let diagnostics = registry.diagnostics("session-shared").expect("diagnostics");
        assert_eq!(diagnostics.active_jobs, 0);
        assert_eq!(diagnostics.usage.bytes, 6);
        assert_eq!(diagnostics.reason_code, "audio.session.idle");
    }

    #[test]
    fn session_registry_rejects_duplicate_and_invalid_job_identity() {
        let registry = Arc::new(AudioSessionRegistry::default());
        let _job = registry.begin_job("session-1", "job-1").expect("first job");
        assert!(matches!(
            registry.begin_job("session-1", "job-1"),
            Err(AudioPipelineError::DuplicateMediaJob)
        ));
        assert!(matches!(
            registry.begin_job("", "job-2"),
            Err(AudioPipelineError::InvalidMediaIdentity)
        ));
    }

    #[tokio::test]
    async fn session_reset_cancels_registered_synthesis_before_provider_dispatch() {
        let registry = Arc::new(AudioSessionRegistry::default());
        let mut job = registry.begin_job("session-reset", "tts:run-1").expect("media job");
        let cancellation = job.cancellation();
        assert_eq!(registry.cancel_session("session-reset"), 1);
        let invoked = Arc::new(AtomicBool::new(false));
        let backend = StubTts { fail: false, invoked: Arc::clone(&invoked) };
        let outcome = job
            .pipeline_mut()
            .synthesize_after_delivery(
                &TextDeliveryReceipt {
                    run_id: "run-1".to_owned(),
                    text: "Delivered text".to_owned(),
                    success: true,
                    delivered_at_unix_ms: unix_ms(),
                },
                &AudioOutputRequestV1 { voice_id: "voice-a".to_owned(), codec: "mp3".to_owned() },
                MediaDeliveryDescriptor {
                    delivery_key: "tts:run-1".to_owned(),
                    destination_scope_sha256: sha256_hex(b"destination"),
                    content_type: "audio/mpeg".to_owned(),
                    file_name: "reply.mp3".to_owned(),
                },
                &backend,
                &cancellation,
            )
            .await;

        assert_eq!(outcome.state, MediaJobState::Cancelled);
        assert!(outcome.text_run_success);
        assert!(!invoked.load(Ordering::SeqCst));
    }
}
