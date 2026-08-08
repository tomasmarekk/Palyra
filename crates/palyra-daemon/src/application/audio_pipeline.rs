//! Connector-neutral audio transcription and post-delivery synthesis.
//!
//! Raw media remains an ephemeral job input. Durable contracts contain only
//! bounded, redacted text, content hashes, provenance, usage, and retention.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use palyra_model_providers::AudioTranscriptionResponse;
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
const DEFAULT_RETENTION_MS: u64 = 24 * 60 * 60 * 1_000;

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
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct MediaSessionUsage {
    pub bytes: u64,
    pub duration_ms: u64,
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

#[async_trait]
pub trait AudioSynthesisBackend: Send + Sync {
    async fn synthesize(
        &self,
        request: AudioSynthesisJobRequest,
    ) -> Result<AudioSynthesisBackendResult, String>;
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
    #[error("audio payload does not match artifact metadata")]
    ArtifactIntegrityMismatch,
    #[error("transcription output exceeds transcript budget")]
    TranscriptBudgetExceeded,
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
            Self::ArtifactIntegrityMismatch => "audio_artifact_integrity_mismatch",
            Self::TranscriptBudgetExceeded => "audio_transcript_budget_exceeded",
            Self::Backend(_) => "audio_backend_failed",
        }
    }
}

/// Admitted connector-neutral media pipeline for one session.
#[derive(Debug)]
pub struct AudioPipeline {
    budget: MediaSessionBudget,
    retention: MediaRetentionPolicy,
    usage: MediaSessionUsage,
}

impl AudioPipeline {
    pub fn new(budget: MediaSessionBudget, retention: MediaRetentionPolicy) -> Self {
        Self { budget, retention, usage: MediaSessionUsage::default() }
    }

    pub fn usage(&self) -> MediaSessionUsage {
        self.usage
    }

    /// Runs STT and produces a redacted, non-authoritative context segment.
    pub async fn transcribe<B: AudioTranscriptionBackend>(
        &mut self,
        input: &AudioInputArtifactV1,
        raw_audio: &[u8],
        backend: &B,
        cancellation: &AudioJobCancellation,
    ) -> Result<TranscriptionArtifact, AudioPipelineError> {
        self.admit(input.bytes, input.duration_ms)?;
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
            () = cancellation.cancelled() => return Err(AudioPipelineError::Cancelled),
            result = backend_future => result,
        };
        let response = result
            .map_err(|_| AudioPipelineError::TimedOut)?
            .map_err(AudioPipelineError::Backend)?;
        if response.text.len() > self.budget.max_transcript_bytes {
            return Err(AudioPipelineError::TranscriptBudgetExceeded);
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
        self.admit(input.bytes, duration_ms)?;
        let context_segment = UntrustedTranscriptContextSegment {
            text: text.clone(),
            trust_label: "external_untrusted".to_owned(),
            instruction_authority: false,
            artifact_citation: format!("artifact://{artifact_id}"),
            source_artifact_sha256: input.sha256.clone(),
        };
        self.usage.bytes = self.usage.bytes.saturating_add(input.bytes);
        self.usage.duration_ms = self.usage.duration_ms.saturating_add(duration_ms);
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
            usage: MediaUsage {
                input_bytes: input.bytes,
                audio_duration_ms: duration_ms,
                ..response.usage
            },
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
        voice_id: &str,
        codec: &str,
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
        let request = AudioSynthesisJobRequest {
            source_run_id: receipt.run_id.clone(),
            text: receipt.text.clone(),
            voice_id: voice_id.to_owned(),
            codec: codec.to_owned(),
            delivery: delivery.clone(),
        };
        let backend_future = tokio::time::timeout(
            Duration::from_millis(self.budget.job_timeout_ms.max(1)),
            backend.synthesize(request),
        );
        let response = tokio::select! {
            () = cancellation.cancelled() => {
                return failed_tts_outcome(true, MediaJobState::Cancelled, "tts_job_cancelled");
            }
            response = backend_future => response,
        };
        let response = match response {
            Err(_) => {
                return failed_tts_outcome(true, MediaJobState::TimedOut, "tts_job_timeout");
            }
            Ok(Err(_)) => {
                return failed_tts_outcome(true, MediaJobState::Failed, "tts_backend_failed");
            }
            Ok(Ok(response)) => response,
        };
        let output_bytes = u64::try_from(response.bytes.len()).unwrap_or(u64::MAX);
        if let Err(error) = self.admit(output_bytes, response.duration_ms) {
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
            usage: MediaUsage { output_bytes, ..response.usage },
            created_at_unix_ms,
            expires_at_unix_ms: created_at_unix_ms
                .saturating_add(self.retention.synthesized_audio_ttl_ms),
            delivery,
        };
        self.usage.bytes = self.usage.bytes.saturating_add(output_bytes);
        self.usage.duration_ms = self.usage.duration_ms.saturating_add(response.duration_ms);
        TtsPostDeliveryOutcome {
            text_run_success: true,
            state: MediaJobState::Succeeded,
            reason_code: "tts_succeeded".to_owned(),
            artifact: Some(artifact),
            payload: Some(response.bytes),
        }
    }

    fn admit(&self, bytes: u64, duration_ms: u64) -> Result<(), AudioPipelineError> {
        if bytes > self.budget.max_job_bytes {
            return Err(AudioPipelineError::JobByteBudgetExceeded);
        }
        if duration_ms > self.budget.max_job_duration_ms {
            return Err(AudioPipelineError::JobDurationBudgetExceeded);
        }
        if self.usage.bytes.saturating_add(bytes) > self.budget.max_session_bytes {
            return Err(AudioPipelineError::SessionByteBudgetExceeded);
        }
        if self.usage.duration_ms.saturating_add(duration_ms) > self.budget.max_session_duration_ms
        {
            return Err(AudioPipelineError::SessionDurationBudgetExceeded);
        }
        Ok(())
    }
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
        ) -> Result<AudioSynthesisBackendResult, String> {
            self.invoked.store(true, Ordering::SeqCst);
            if self.fail {
                return Err("provider unavailable".to_owned());
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
        let mut pipeline =
            AudioPipeline::new(MediaSessionBudget::default(), MediaRetentionPolicy::default());
        let failure = pipeline
            .synthesize_after_delivery(
                &receipt,
                "voice-a",
                "opus",
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
                "voice-a",
                "opus",
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
                "voice-a",
                "opus",
                delivery.clone(),
                &successful,
                &AudioJobCancellation::default(),
            )
            .await;
        assert_eq!(success.state, MediaJobState::Succeeded);
        let artifact = success.artifact.expect("TTS artifact should exist");
        assert_eq!(artifact.delivery, delivery);
        assert_eq!(artifact.source_run_id, receipt.run_id);
        assert!(success.payload.is_some());
    }
}
