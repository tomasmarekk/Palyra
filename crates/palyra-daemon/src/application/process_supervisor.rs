//! Host-owned actor for durable, bounded subprocess execution.
//! The actor retains the cleanup authority while dedicated I/O workers feed a
//! cursor-addressable spool that can be recovered without persisting secrets.

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
use std::process::{ChildStdin, ExitStatus};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, SyncSender, TrySendError};
use std::sync::{Mutex, MutexGuard};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use palyra_common::runtime_contracts::{ProcessLeaseV1, ProcessProvenance, RuntimeGeneration};
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use thiserror::Error;

use super::local_resource_governor::{
    LocalResourceGovernor, ResourceLeaseRequestV1, ResourceLeaseV1, ResourcePriority,
    ResourceServiceKind, ResourceUnitsV1,
};
use crate::sandbox_runner::{
    redact_process_output_projection, spawn_managed_stdio_process, ManagedStdioProcess,
    ManagedStdioProcessConfig,
};

const PROCESS_SESSION_SCHEMA_VERSION: u32 = 2;
const PROCESS_OUTPUT_SCHEMA_VERSION: u32 = 2;
const ACTOR_CHANNEL_CAPACITY: usize = 256;
const STDIN_CHANNEL_CAPACITY: usize = 32;
const READER_CHUNK_BYTES: usize = 8 * 1024;
const ACTOR_POLL_INTERVAL: Duration = Duration::from_millis(25);
const COMMAND_RESPONSE_TIMEOUT: Duration = Duration::from_secs(10);
const MAX_OWNER_TEXT_BYTES: usize = 256;
const MAX_REASON_TEXT_BYTES: usize = 512;

/// Stream associated with a process output chunk.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProcessOutputStream {
    /// Standard output.
    Stdout,
    /// Standard error.
    Stderr,
}

/// Cursor-addressable process output retained by the supervisor.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProcessOutputChunkV2 {
    /// Output contract schema version.
    pub schema_version: u32,
    /// Monotonic session-local cursor.
    pub sequence: u64,
    /// Originating process stream.
    pub stream: ProcessOutputStream,
    /// Capture timestamp from the host clock.
    pub captured_at_unix_ms: i64,
    /// Original byte count, before decoding and redaction.
    pub byte_count: u64,
    /// Redacted lossy UTF-8 projection for display and durable artifacts.
    pub text_projection: String,
    /// Whether the canonical process-output policy removed sensitive content.
    pub redacted: bool,
    /// Stable reasons emitted by the canonical redactor.
    pub redaction_reason_codes: Vec<String>,
}

impl ProcessOutputChunkV2 {
    fn from_bytes(sequence: u64, stream: ProcessOutputStream, bytes: &[u8]) -> Self {
        let decoded = String::from_utf8_lossy(bytes);
        let (text_projection, redacted, redaction_reason_codes) =
            redact_process_output_projection(decoded.as_ref());
        Self {
            schema_version: PROCESS_OUTPUT_SCHEMA_VERSION,
            sequence,
            stream,
            captured_at_unix_ms: unix_time_ms(),
            byte_count: u64::try_from(bytes.len()).unwrap_or(u64::MAX),
            text_projection,
            redacted,
            redaction_reason_codes,
        }
    }

    fn original_len(&self) -> usize {
        usize::try_from(self.byte_count).unwrap_or(usize::MAX)
    }
}

#[derive(Debug, Clone)]
struct RetainedOutputChunk {
    visible: ProcessOutputChunkV2,
    raw_bytes: Vec<u8>,
}

/// Durable lifecycle state of a supervised process.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProcessSessionState {
    /// Child is running and may accept input.
    Running,
    /// Child exited and output readers are completing.
    Draining,
    /// Child exited successfully and cleanup was verified.
    Succeeded,
    /// Child exited unsuccessfully and cleanup was verified.
    Failed,
    /// A configured execution deadline elapsed.
    TimedOut,
    /// The owner requested cancellation.
    Cancelled,
    /// Exact process-tree cleanup or durable settlement failed.
    CleanupFailed,
    /// Restart recovery found metadata without recoverable live authority.
    Orphaned,
}

impl ProcessSessionState {
    /// Returns whether no live process authority remains for this session.
    #[must_use]
    pub const fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Succeeded
                | Self::Failed
                | Self::TimedOut
                | Self::Cancelled
                | Self::CleanupFailed
                | Self::Orphaned
        )
    }
}

/// Redacted ownership and correlation metadata for one process.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProcessOwnerV2 {
    /// Owning chat session identity.
    pub session_id: String,
    /// Owning runtime run identity.
    pub run_id: String,
    /// Owning turn identity.
    pub turn_id: String,
    /// Owning agent identity.
    pub agent_id: String,
    /// Host-issued correlation identity.
    pub correlation_id: String,
}

impl ProcessOwnerV2 {
    fn validate(&self) -> Result<(), ProcessSupervisorError> {
        for (field, value) in [
            ("session_id", self.session_id.as_str()),
            ("run_id", self.run_id.as_str()),
            ("turn_id", self.turn_id.as_str()),
            ("agent_id", self.agent_id.as_str()),
            ("correlation_id", self.correlation_id.as_str()),
        ] {
            if value.trim().is_empty()
                || value.len() > MAX_OWNER_TEXT_BYTES
                || value.chars().any(char::is_control)
            {
                return Err(ProcessSupervisorError::InvalidLaunch(format!(
                    "{field} must be non-empty, bounded, and free of control characters"
                )));
            }
        }
        Ok(())
    }
}

/// Sanitized command metadata retained for audit and recovery.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProcessCommandRecordV2 {
    /// Hash of the executable and exact argument vector.
    pub command_sha256: String,
    /// Basename-only executable label.
    pub executable_label: String,
    /// Verified executable digest.
    pub executable_sha256: String,
    /// Canonically redacted and bounded argument preview.
    pub redacted_argv_preview: Vec<String>,
    /// Digest of the working directory rather than its host path.
    pub cwd_sha256: String,
    /// Environment variable names; values are never persisted.
    pub environment_key_names: Vec<String>,
    /// Host process backend that owns the OS handle and I/O.
    pub backend: ProcessBackendV2,
}

/// Host backend used for a supervised process.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProcessBackendV2 {
    /// Portable managed stdio primitive with exact process-tree cleanup.
    ManagedStdio,
}

/// Terminal outcome retained after I/O has drained and cleanup is verified.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProcessOutcomeV2 {
    /// Platform exit code when one was observable.
    pub exit_code: Option<i32>,
    /// Stable bounded terminal reason.
    pub reason_code: String,
    /// Completion timestamp after the drain barrier.
    pub completed_at_unix_ms: i64,
    /// Whether exact process-tree absence was verified.
    pub cleanup_verified: bool,
}

/// Durable process record reconstructed after daemon restart.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProcessSessionRecordV2 {
    /// Record schema version.
    pub schema_version: u32,
    /// Stable process-session identity.
    pub process_session_id: String,
    /// Redacted ownership metadata.
    pub owner: ProcessOwnerV2,
    /// Sanitized command metadata.
    pub command: ProcessCommandRecordV2,
    /// Exact host process lease; PID alone never grants authority.
    pub process_lease: ProcessLeaseV1,
    /// Current durable lifecycle state.
    pub state: ProcessSessionState,
    /// Record creation timestamp.
    pub created_at_unix_ms: i64,
    /// Most recent durable update timestamp.
    pub updated_at_unix_ms: i64,
    /// Overall execution deadline.
    pub deadline_at_unix_ms: i64,
    /// Optional deadline that advances whenever output is observed.
    #[serde(default)]
    pub no_output_deadline_at_unix_ms: Option<i64>,
    /// Most recent output timestamp, or the creation timestamp before first output.
    #[serde(default)]
    pub last_output_at_unix_ms: i64,
    /// Oldest cursor still present in the inline spool.
    pub first_retained_cursor: u64,
    /// Most recently issued output cursor.
    pub next_cursor: u64,
    /// Bytes retained in the inline spool.
    pub retained_output_bytes: u64,
    /// Whether any output was evicted or omitted from the artifact.
    pub output_truncated: bool,
    /// Terminal outcome after cleanup, when available.
    pub outcome: Option<ProcessOutcomeV2>,
}

/// Trusted host observation used only to decide restart adoption posture.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProcessAdoptionObservationV1 {
    /// Observed operating-system process id.
    pub pid: u32,
    /// Observed runtime generation.
    pub generation: RuntimeGeneration,
    /// Re-verified start token, executable digest, nonce, and ownership anchor.
    pub provenance: ProcessProvenance,
}

/// Safe restart-adoption decision.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProcessAdoptionDecisionV1 {
    /// PID or generation no longer identifies the recorded runtime instance.
    RefusedIdentityMismatch,
    /// One or more provenance fields differ from the durable lease.
    RefusedProvenanceMismatch,
    /// Provenance matches, but stdio and exact cleanup authority cannot be recovered.
    RefusedAuthorityUnavailable,
}

/// Redacted restart-adoption outcome that never authorizes signalling by PID alone.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProcessAdoptionOutcomeV1 {
    /// Adoption decision.
    pub decision: ProcessAdoptionDecisionV1,
    /// Stable diagnostics and recovery reason.
    pub reason_code: String,
    /// Whether the observer may signal or kill the process.
    pub signalling_authorized: bool,
}

/// Compares a trusted OS observation with durable provenance.
///
/// A matching PID is intentionally insufficient. Even a full provenance match
/// is refused when the daemon cannot reconstruct the original stdio streams and
/// platform ownership handle; the process is then an external orphan for
/// operator reconciliation, never a target for PID-only cleanup.
#[must_use]
pub fn evaluate_process_adoption(
    record: &ProcessSessionRecordV2,
    observation: &ProcessAdoptionObservationV1,
) -> ProcessAdoptionOutcomeV1 {
    let recorded = &record.process_lease;
    if observation.pid != recorded.pid || observation.generation != recorded.generation {
        return ProcessAdoptionOutcomeV1 {
            decision: ProcessAdoptionDecisionV1::RefusedIdentityMismatch,
            reason_code: "process.adoption.identity_mismatch".to_owned(),
            signalling_authorized: false,
        };
    }
    if observation.provenance != recorded.provenance {
        return ProcessAdoptionOutcomeV1 {
            decision: ProcessAdoptionDecisionV1::RefusedProvenanceMismatch,
            reason_code: "process.adoption.provenance_mismatch".to_owned(),
            signalling_authorized: false,
        };
    }
    ProcessAdoptionOutcomeV1 {
        decision: ProcessAdoptionDecisionV1::RefusedAuthorityUnavailable,
        reason_code: "process.adoption.authority_unavailable".to_owned(),
        signalling_authorized: false,
    }
}

/// Launch policy for a process admitted to the supervisor.
#[derive(Debug, Clone)]
pub struct ProcessLaunchSpec {
    /// Absolute trusted executable path.
    pub executable: PathBuf,
    /// Bounded argument vector passed without a shell.
    pub args: Vec<String>,
    /// Absolute existing working directory.
    pub cwd: PathBuf,
    /// Explicit environment passed after clearing inherited values.
    pub env: BTreeMap<String, String>,
    /// Owning run metadata.
    pub owner: ProcessOwnerV2,
    /// Overall process deadline.
    pub timeout: Duration,
    /// Optional maximum silent interval before deterministic termination.
    pub no_output_timeout: Option<Duration>,
    /// Process lease duration, which must cover the deadline.
    pub lease_duration: Duration,
    /// Pressure-retention priority.
    pub resource_priority: ResourcePriority,
    /// Service class charged by the shared resource governor.
    pub resource_service: ResourceServiceKind,
    /// Atomic local capacity reserved before process creation.
    pub resource_units: ResourceUnitsV1,
}

/// Bounded storage and lifecycle settings for the process actor.
#[derive(Clone)]
pub struct ProcessSupervisorConfig {
    /// Absolute local state root.
    pub state_root: PathBuf,
    /// Maximum number of concurrently active sessions.
    pub max_sessions: usize,
    /// Maximum resident chunks per session.
    pub max_retained_chunks_per_session: usize,
    /// Maximum resident output bytes per session.
    pub max_retained_bytes_per_session: usize,
    /// Maximum durable artifact bytes per session.
    pub max_artifact_bytes_per_session: u64,
    /// Time allowed for stdout and stderr to drain after child exit.
    pub drain_timeout: Duration,
    /// Shared local admission authority.
    pub resource_governor: LocalResourceGovernor,
}

impl ProcessSupervisorConfig {
    fn validate(&self) -> Result<(), ProcessSupervisorError> {
        if !self.state_root.is_absolute()
            || self.max_sessions == 0
            || self.max_retained_chunks_per_session == 0
            || self.max_retained_bytes_per_session == 0
            || self.max_artifact_bytes_per_session == 0
            || self.drain_timeout.is_zero()
        {
            return Err(ProcessSupervisorError::InvalidConfiguration);
        }
        Ok(())
    }

    fn records_root(&self) -> PathBuf {
        self.state_root.join("process-supervisor-v2").join("records")
    }

    fn artifacts_root(&self) -> PathBuf {
        self.state_root.join("process-supervisor-v2").join("artifacts")
    }
}

/// Cursor page returned to process waiters and polling clients.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProcessOutputPageV2 {
    /// Output page schema version.
    pub schema_version: u32,
    /// Ordered chunks after the requested cursor.
    pub chunks: Vec<ProcessOutputChunkV2>,
    /// Latest issued cursor, including chunks not returned by the page limit.
    pub next_cursor: u64,
    /// Cursor of the last chunk actually returned to this consumer.
    pub last_returned_cursor: u64,
    /// Whether additional retained chunks remain after `last_returned_cursor`.
    pub has_more: bool,
    /// Whether the requested cursor preceded retained history.
    pub cursor_reset: bool,
    /// Whether the spool or artifact omitted output.
    pub truncated: bool,
}

#[derive(Debug, Clone)]
pub(crate) struct ProcessRawOutputChunk {
    pub(crate) sequence: u64,
    pub(crate) stream: ProcessOutputStream,
    pub(crate) bytes: Vec<u8>,
}

#[derive(Debug, Clone)]
pub(crate) struct ProcessRawOutputPage {
    pub(crate) chunks: Vec<ProcessRawOutputChunk>,
    pub(crate) last_returned_cursor: u64,
    pub(crate) has_more: bool,
    pub(crate) cursor_reset: bool,
    pub(crate) truncated: bool,
}

/// Process completion projected only after the drain barrier closes.
#[derive(Debug, Clone)]
pub struct ProcessCompletion {
    /// Terminal durable record.
    pub record: ProcessSessionRecordV2,
    /// Bounded output page captured at completion.
    pub output: ProcessOutputPageV2,
}

/// Failures returned by the host process authority.
#[derive(Debug, Error)]
pub enum ProcessSupervisorError {
    /// Configuration violates bounded supervisor policy.
    #[error("process supervisor configuration is invalid")]
    InvalidConfiguration,
    /// Launch metadata or paths violate process policy.
    #[error("process launch is invalid: {0}")]
    InvalidLaunch(String),
    /// Active process concurrency reached its configured bound.
    #[error("process supervisor is at its session limit")]
    SessionLimit,
    /// The bounded actor command queue cannot currently admit work.
    #[error("process supervisor command queue is full")]
    Busy,
    /// The process actor has stopped or disconnected.
    #[error("process supervisor actor is unavailable")]
    Unavailable,
    /// A bounded command response deadline elapsed.
    #[error("process supervisor command timed out")]
    CommandTimeout,
    /// The supplied process-session identity is unknown.
    #[error("process session was not found")]
    SessionNotFound,
    /// The session is terminal or its stdin has closed.
    #[error("process session is no longer writable")]
    SessionNotWritable,
    /// The bounded stdin writer queue cannot currently admit bytes.
    #[error("process stdin queue is full")]
    StdinBusy,
    /// Process creation or ownership establishment failed.
    #[error("process launch failed: {0}")]
    Spawn(String),
    /// Durable record or artifact storage failed.
    #[error("process persistence failed: {0}")]
    Persistence(String),
    /// Process stream handling failed.
    #[error("process I/O failed: {0}")]
    Io(String),
    /// Local capacity could not be reserved or released.
    #[error("process resource admission failed: {0}")]
    ResourceAdmission(String),
}

type ActorResponse<T> = SyncSender<Result<T, ProcessSupervisorError>>;

enum ActorMessage {
    Launch {
        spec: Box<ProcessLaunchSpec>,
        response: ActorResponse<ProcessSessionRecordV2>,
    },
    Output {
        process_session_id: String,
        stream: ProcessOutputStream,
        bytes: Vec<u8>,
    },
    OutputClosed {
        process_session_id: String,
    },
    StdinFailed {
        process_session_id: String,
        reason: String,
    },
    Write {
        process_session_id: String,
        bytes: Vec<u8>,
        response: ActorResponse<()>,
    },
    CloseStdin {
        process_session_id: String,
        response: ActorResponse<()>,
    },
    Interrupt {
        process_session_id: String,
        response: ActorResponse<ProcessSessionRecordV2>,
    },
    Terminate {
        process_session_id: String,
        response: ActorResponse<ProcessSessionRecordV2>,
    },
    Status {
        process_session_id: String,
        response: ActorResponse<ProcessSessionRecordV2>,
    },
    Tail {
        process_session_id: String,
        after_cursor: Option<u64>,
        max_chunks: usize,
        response: ActorResponse<ProcessOutputPageV2>,
    },
    RawTail {
        process_session_id: String,
        after_cursor: Option<u64>,
        max_chunks: usize,
        response: ActorResponse<ProcessRawOutputPage>,
    },
    Wait {
        process_session_id: String,
        after_cursor: Option<u64>,
        max_chunks: usize,
        response: ActorResponse<ProcessCompletion>,
    },
    Shutdown {
        response: ActorResponse<()>,
    },
}

enum StdinMessage {
    Write(Vec<u8>),
    Close,
}

struct LiveProcessSession {
    record: ProcessSessionRecordV2,
    process: Option<ManagedStdioProcess>,
    resource_lease: Option<ResourceLeaseV1>,
    stdin_tx: Option<SyncSender<StdinMessage>>,
    output: VecDeque<RetainedOutputChunk>,
    output_bytes: usize,
    artifact_bytes: u64,
    open_readers: u8,
    drain_deadline: Option<Instant>,
    pending_state: Option<ProcessSessionState>,
    pending_reason: Option<String>,
    exit_code: Option<i32>,
    waiters: Vec<(Option<u64>, usize, ActorResponse<ProcessCompletion>)>,
}

impl LiveProcessSession {
    fn output_page(&self, after_cursor: Option<u64>, max_chunks: usize) -> ProcessOutputPageV2 {
        let requested = after_cursor.unwrap_or(0);
        let cursor_reset = requested.saturating_add(1) < self.record.first_retained_cursor;
        let effective = if cursor_reset {
            self.record.first_retained_cursor.saturating_sub(1)
        } else {
            requested
        };
        let chunks = self
            .output
            .iter()
            .filter(|chunk| chunk.visible.sequence > effective)
            .take(max_chunks.max(1))
            .map(|chunk| chunk.visible.clone())
            .collect::<Vec<_>>();
        let last_returned_cursor =
            chunks.last().map_or(effective.min(self.record.next_cursor), |chunk| chunk.sequence);
        ProcessOutputPageV2 {
            schema_version: PROCESS_OUTPUT_SCHEMA_VERSION,
            chunks,
            next_cursor: self.record.next_cursor,
            last_returned_cursor,
            has_more: last_returned_cursor < self.record.next_cursor,
            cursor_reset,
            truncated: self.record.output_truncated,
        }
    }

    fn raw_output_page(
        &self,
        after_cursor: Option<u64>,
        max_chunks: usize,
    ) -> ProcessRawOutputPage {
        let requested = after_cursor.unwrap_or(0);
        let cursor_reset = requested.saturating_add(1) < self.record.first_retained_cursor;
        let effective = if cursor_reset {
            self.record.first_retained_cursor.saturating_sub(1)
        } else {
            requested
        };
        let chunks = self
            .output
            .iter()
            .filter(|chunk| chunk.visible.sequence > effective)
            .take(max_chunks.max(1))
            .map(|chunk| ProcessRawOutputChunk {
                sequence: chunk.visible.sequence,
                stream: chunk.visible.stream,
                bytes: chunk.raw_bytes.clone(),
            })
            .collect::<Vec<_>>();
        let last_returned_cursor =
            chunks.last().map_or(effective.min(self.record.next_cursor), |chunk| chunk.sequence);
        ProcessRawOutputPage {
            chunks,
            last_returned_cursor,
            has_more: last_returned_cursor < self.record.next_cursor,
            cursor_reset,
            truncated: self.record.output_truncated,
        }
    }
}

/// Cloneable command handle for the single process actor.
pub struct ProcessSupervisor {
    tx: SyncSender<ActorMessage>,
    actor: Mutex<Option<JoinHandle<()>>>,
}

impl ProcessSupervisor {
    /// Starts the host process authority and creates its hardened state roots.
    ///
    /// # Errors
    /// Returns an error when configuration or durable state initialization fails.
    pub fn start(config: ProcessSupervisorConfig) -> Result<Self, ProcessSupervisorError> {
        config.validate()?;
        create_private_dir(config.records_root().as_path())?;
        create_private_dir(config.artifacts_root().as_path())?;
        mark_unresolved_records_orphaned(&config)?;
        let (tx, rx) = mpsc::sync_channel(ACTOR_CHANNEL_CAPACITY);
        let actor_tx = tx.clone();
        let actor_config = config.clone();
        let actor = thread::Builder::new()
            .name("palyra-process-supervisor".to_owned())
            .spawn(move || run_actor(actor_config, actor_tx, rx))
            .map_err(|error| ProcessSupervisorError::Io(error.to_string()))?;
        Ok(Self { tx, actor: Mutex::new(Some(actor)) })
    }

    /// Launches a process only after ownership and durable metadata are established.
    ///
    /// # Errors
    /// Returns an error for invalid policy, saturation, spawn, or persistence failure.
    pub fn launch(
        &self,
        spec: ProcessLaunchSpec,
    ) -> Result<ProcessSessionRecordV2, ProcessSupervisorError> {
        request(&self.tx, |response| ActorMessage::Launch { spec: Box::new(spec), response })
    }

    /// Enqueues bounded stdin bytes without blocking the actor on child I/O.
    ///
    /// # Errors
    /// Returns an error when the session is terminal or the bounded queue is full.
    pub fn write(
        &self,
        process_session_id: &str,
        bytes: Vec<u8>,
    ) -> Result<(), ProcessSupervisorError> {
        if bytes.is_empty() || bytes.len() > READER_CHUNK_BYTES {
            return Err(ProcessSupervisorError::Io(
                "stdin writes must contain 1..=8192 bytes".to_owned(),
            ));
        }
        request(&self.tx, |response| ActorMessage::Write {
            process_session_id: process_session_id.to_owned(),
            bytes,
            response,
        })
    }

    /// Closes the child stdin stream and leaves output draining under actor ownership.
    ///
    /// # Errors
    /// Returns an error when the session is absent or no longer writable.
    pub fn close_stdin(&self, process_session_id: &str) -> Result<(), ProcessSupervisorError> {
        request(&self.tx, |response| ActorMessage::CloseStdin {
            process_session_id: process_session_id.to_owned(),
            response,
        })
    }

    /// Interrupts a pipe-backed process through exact owned-tree cancellation.
    ///
    /// PTY sessions use the terminal backend's platform signal path. A plain
    /// pipe cannot safely synthesize terminal Ctrl+C semantics, so this command
    /// records the distinct interruption reason while using verified cleanup.
    ///
    /// # Errors
    /// Returns an error when the session is absent or cleanup persistence fails.
    pub fn interrupt(
        &self,
        process_session_id: &str,
    ) -> Result<ProcessSessionRecordV2, ProcessSupervisorError> {
        request(&self.tx, |response| ActorMessage::Interrupt {
            process_session_id: process_session_id.to_owned(),
            response,
        })
    }

    /// Terminates the exact owned process tree and waits for cleanup evidence.
    ///
    /// # Errors
    /// Returns an error when the session is absent or persistence fails.
    pub fn terminate(
        &self,
        process_session_id: &str,
    ) -> Result<ProcessSessionRecordV2, ProcessSupervisorError> {
        request(&self.tx, |response| ActorMessage::Terminate {
            process_session_id: process_session_id.to_owned(),
            response,
        })
    }

    /// Returns the latest durable process record.
    ///
    /// # Errors
    /// Returns an error when the session is absent.
    pub fn status(
        &self,
        process_session_id: &str,
    ) -> Result<ProcessSessionRecordV2, ProcessSupervisorError> {
        request(&self.tx, |response| ActorMessage::Status {
            process_session_id: process_session_id.to_owned(),
            response,
        })
    }

    /// Reads a bounded output page after the supplied cursor.
    ///
    /// # Errors
    /// Returns an error when the session is absent.
    pub fn tail(
        &self,
        process_session_id: &str,
        after_cursor: Option<u64>,
        max_chunks: usize,
    ) -> Result<ProcessOutputPageV2, ProcessSupervisorError> {
        request(&self.tx, |response| ActorMessage::Tail {
            process_session_id: process_session_id.to_owned(),
            after_cursor,
            max_chunks,
            response,
        })
    }

    /// Reads lossless process bytes for a trusted in-process protocol adapter.
    ///
    /// This method is crate-private so raw output cannot cross a model, console,
    /// or serialization boundary. Public polling always returns the canonical
    /// redacted projection from [`Self::tail`].
    pub(crate) fn tail_raw(
        &self,
        process_session_id: &str,
        after_cursor: Option<u64>,
        max_chunks: usize,
    ) -> Result<ProcessRawOutputPage, ProcessSupervisorError> {
        request(&self.tx, |response| ActorMessage::RawTail {
            process_session_id: process_session_id.to_owned(),
            after_cursor,
            max_chunks,
            response,
        })
    }

    /// Waits for terminal state after the output-drain barrier has closed.
    ///
    /// # Errors
    /// Returns an error when the session is absent or the actor becomes unavailable.
    pub fn wait(
        &self,
        process_session_id: &str,
        after_cursor: Option<u64>,
        max_chunks: usize,
        timeout: Duration,
    ) -> Result<ProcessCompletion, ProcessSupervisorError> {
        let (response_tx, response_rx) = mpsc::sync_channel(1);
        send_actor(
            &self.tx,
            ActorMessage::Wait {
                process_session_id: process_session_id.to_owned(),
                after_cursor,
                max_chunks,
                response: response_tx,
            },
        )?;
        response_rx.recv_timeout(timeout).map_err(|error| match error {
            RecvTimeoutError::Timeout => ProcessSupervisorError::CommandTimeout,
            RecvTimeoutError::Disconnected => ProcessSupervisorError::Unavailable,
        })?
    }

    /// Loads durable records without claiming process ownership.
    ///
    /// # Errors
    /// Returns an error when a record cannot be read or decoded.
    pub fn recover_records(
        config: &ProcessSupervisorConfig,
    ) -> Result<Vec<ProcessSessionRecordV2>, ProcessSupervisorError> {
        config.validate()?;
        read_records(config.records_root().as_path())
    }

    /// Stops admission, terminates every owned process tree, and joins the actor.
    ///
    /// The operation is idempotent so daemon lifecycle code can invoke it
    /// explicitly while [`Drop`] remains a final safety net.
    ///
    /// # Errors
    /// Returns an error when the actor cannot acknowledge or join shutdown.
    pub fn shutdown(&self) -> Result<(), ProcessSupervisorError> {
        let mut actor = self.lock_actor()?;
        if actor.is_none() {
            return Ok(());
        }
        let (response_tx, response_rx) = mpsc::sync_channel(1);
        let result = self
            .tx
            .send(ActorMessage::Shutdown { response: response_tx })
            .map_err(|_| ProcessSupervisorError::Unavailable)
            .and_then(|()| {
                response_rx.recv_timeout(COMMAND_RESPONSE_TIMEOUT).map_err(|error| match error {
                    RecvTimeoutError::Timeout => ProcessSupervisorError::CommandTimeout,
                    RecvTimeoutError::Disconnected => ProcessSupervisorError::Unavailable,
                })?
            });
        if let Some(actor) = actor.take() {
            actor.join().map_err(|_| ProcessSupervisorError::Unavailable)?;
        }
        result
    }

    fn lock_actor(&self) -> Result<MutexGuard<'_, Option<JoinHandle<()>>>, ProcessSupervisorError> {
        self.actor.lock().map_err(|_| ProcessSupervisorError::Unavailable)
    }
}

impl Drop for ProcessSupervisor {
    fn drop(&mut self) {
        let _ = self.shutdown();
    }
}

fn request<T>(
    tx: &SyncSender<ActorMessage>,
    build: impl FnOnce(ActorResponse<T>) -> ActorMessage,
) -> Result<T, ProcessSupervisorError> {
    let (response_tx, response_rx) = mpsc::sync_channel(1);
    send_actor(tx, build(response_tx))?;
    response_rx.recv_timeout(COMMAND_RESPONSE_TIMEOUT).map_err(|error| match error {
        RecvTimeoutError::Timeout => ProcessSupervisorError::CommandTimeout,
        RecvTimeoutError::Disconnected => ProcessSupervisorError::Unavailable,
    })?
}

fn send_actor(
    tx: &SyncSender<ActorMessage>,
    message: ActorMessage,
) -> Result<(), ProcessSupervisorError> {
    tx.try_send(message).map_err(|error| match error {
        TrySendError::Full(_) => ProcessSupervisorError::Busy,
        TrySendError::Disconnected(_) => ProcessSupervisorError::Unavailable,
    })
}

fn run_actor(
    config: ProcessSupervisorConfig,
    actor_tx: SyncSender<ActorMessage>,
    rx: Receiver<ActorMessage>,
) {
    let mut sessions = HashMap::<String, LiveProcessSession>::new();
    loop {
        let mut shutdown = false;
        match rx.recv_timeout(ACTOR_POLL_INTERVAL) {
            Ok(message) => {
                shutdown = handle_actor_message(&config, &actor_tx, &mut sessions, message);
            }
            Err(RecvTimeoutError::Timeout) => {}
            Err(RecvTimeoutError::Disconnected) => shutdown = true,
        }
        poll_processes(&config, &mut sessions);
        if shutdown {
            terminate_all(&config, &mut sessions);
            break;
        }
    }
}

fn handle_actor_message(
    config: &ProcessSupervisorConfig,
    actor_tx: &SyncSender<ActorMessage>,
    sessions: &mut HashMap<String, LiveProcessSession>,
    message: ActorMessage,
) -> bool {
    match message {
        ActorMessage::Launch { spec, response } => {
            let result = launch_session(config, actor_tx, sessions, *spec);
            let _ = response.send(result);
        }
        ActorMessage::Output { process_session_id, stream, bytes } => {
            if let Some(session) = sessions.get_mut(process_session_id.as_str()) {
                append_output(config, session, stream, bytes.as_slice());
            }
        }
        ActorMessage::OutputClosed { process_session_id } => {
            if let Some(session) = sessions.get_mut(process_session_id.as_str()) {
                session.open_readers = session.open_readers.saturating_sub(1);
            }
        }
        ActorMessage::StdinFailed { process_session_id, reason } => {
            if let Some(session) = sessions.get_mut(process_session_id.as_str()) {
                session.stdin_tx = None;
                session.pending_reason = Some(bounded_reason(reason.as_str()));
            }
        }
        ActorMessage::Write { process_session_id, bytes, response } => {
            let result = sessions
                .get(process_session_id.as_str())
                .ok_or(ProcessSupervisorError::SessionNotFound)
                .and_then(|session| {
                    if session.record.state != ProcessSessionState::Running {
                        return Err(ProcessSupervisorError::SessionNotWritable);
                    }
                    let stdin_tx = session
                        .stdin_tx
                        .as_ref()
                        .ok_or(ProcessSupervisorError::SessionNotWritable)?;
                    stdin_tx.try_send(StdinMessage::Write(bytes)).map_err(|error| match error {
                        TrySendError::Full(_) => ProcessSupervisorError::StdinBusy,
                        TrySendError::Disconnected(_) => ProcessSupervisorError::SessionNotWritable,
                    })
                });
            let _ = response.send(result);
        }
        ActorMessage::CloseStdin { process_session_id, response } => {
            let result = sessions
                .get_mut(process_session_id.as_str())
                .ok_or(ProcessSupervisorError::SessionNotFound)
                .and_then(|session| {
                    let stdin_tx = session
                        .stdin_tx
                        .take()
                        .ok_or(ProcessSupervisorError::SessionNotWritable)?;
                    stdin_tx
                        .try_send(StdinMessage::Close)
                        .map_err(|_| ProcessSupervisorError::SessionNotWritable)
                });
            let _ = response.send(result);
        }
        ActorMessage::Interrupt { process_session_id, response } => {
            let result = sessions
                .get_mut(process_session_id.as_str())
                .ok_or(ProcessSupervisorError::SessionNotFound)
                .and_then(|session| {
                    begin_cleanup(
                        config,
                        session,
                        ProcessSessionState::Cancelled,
                        "process.interrupted",
                    )?;
                    Ok(session.record.clone())
                });
            let _ = response.send(result);
        }
        ActorMessage::Terminate { process_session_id, response } => {
            let result = sessions
                .get_mut(process_session_id.as_str())
                .ok_or(ProcessSupervisorError::SessionNotFound)
                .and_then(|session| {
                    begin_cleanup(
                        config,
                        session,
                        ProcessSessionState::Cancelled,
                        "process.cancelled",
                    )?;
                    Ok(session.record.clone())
                });
            let _ = response.send(result);
        }
        ActorMessage::Status { process_session_id, response } => {
            let result = sessions
                .get(process_session_id.as_str())
                .map(|session| session.record.clone())
                .ok_or(ProcessSupervisorError::SessionNotFound);
            let _ = response.send(result);
        }
        ActorMessage::Tail { process_session_id, after_cursor, max_chunks, response } => {
            let result = sessions
                .get(process_session_id.as_str())
                .map(|session| session.output_page(after_cursor, max_chunks))
                .ok_or(ProcessSupervisorError::SessionNotFound);
            let _ = response.send(result);
        }
        ActorMessage::RawTail { process_session_id, after_cursor, max_chunks, response } => {
            let result = sessions
                .get(process_session_id.as_str())
                .map(|session| session.raw_output_page(after_cursor, max_chunks))
                .ok_or(ProcessSupervisorError::SessionNotFound);
            let _ = response.send(result);
        }
        ActorMessage::Wait { process_session_id, after_cursor, max_chunks, response } => {
            match sessions.get_mut(process_session_id.as_str()) {
                Some(session) if session.record.state.is_terminal() => {
                    let completion = ProcessCompletion {
                        record: session.record.clone(),
                        output: session.output_page(after_cursor, max_chunks),
                    };
                    let _ = response.send(Ok(completion));
                }
                Some(session) => session.waiters.push((after_cursor, max_chunks, response)),
                None => {
                    let _ = response.send(Err(ProcessSupervisorError::SessionNotFound));
                }
            }
        }
        ActorMessage::Shutdown { response } => {
            let _ = response.send(Ok(()));
            return true;
        }
    }
    false
}

fn launch_session(
    config: &ProcessSupervisorConfig,
    actor_tx: &SyncSender<ActorMessage>,
    sessions: &mut HashMap<String, LiveProcessSession>,
    spec: ProcessLaunchSpec,
) -> Result<ProcessSessionRecordV2, ProcessSupervisorError> {
    validate_launch_spec(&spec)?;
    if sessions.values().filter(|session| !session.record.state.is_terminal()).count()
        >= config.max_sessions
    {
        return Err(ProcessSupervisorError::SessionLimit);
    }
    let generation = next_generation();
    let resource_lease = config
        .resource_governor
        .acquire(ResourceLeaseRequestV1 {
            owner_id: spec.owner.run_id.clone(),
            generation,
            service: spec.resource_service,
            priority: spec.resource_priority,
            requested: spec.resource_units,
            duration: spec.lease_duration,
        })
        .map_err(|error| ProcessSupervisorError::ResourceAdmission(error.to_string()))?;
    let result = launch_admitted_session(
        config,
        actor_tx,
        sessions,
        spec,
        generation,
        resource_lease.clone(),
    );
    if result.is_err() {
        let _ = config
            .resource_governor
            .release(resource_lease.lease_id.as_str(), resource_lease.generation);
    }
    result
}

fn launch_admitted_session(
    config: &ProcessSupervisorConfig,
    actor_tx: &SyncSender<ActorMessage>,
    sessions: &mut HashMap<String, LiveProcessSession>,
    spec: ProcessLaunchSpec,
    generation: u64,
    resource_lease: ResourceLeaseV1,
) -> Result<ProcessSessionRecordV2, ProcessSupervisorError> {
    let mut process = spawn_managed_stdio_process(&ManagedStdioProcessConfig {
        executable: spec.executable.clone(),
        args: spec.args.clone(),
        cwd: spec.cwd.clone(),
        env: spec.env.clone(),
        generation,
        lease_duration: spec.lease_duration,
    })
    .map_err(|error| ProcessSupervisorError::Spawn(error.message))?;
    let stdout =
        process.take_stdout().map_err(|error| ProcessSupervisorError::Spawn(error.message))?;
    let stderr =
        process.take_stderr().map_err(|error| ProcessSupervisorError::Spawn(error.message))?;
    let stdin =
        process.take_stdin().map_err(|error| ProcessSupervisorError::Spawn(error.message))?;
    let process_session_id = format!("process_{}", ulid::Ulid::generate());
    let created_at_unix_ms = unix_time_ms();
    let deadline_delta = i64::try_from(spec.timeout.as_millis()).unwrap_or(i64::MAX);
    let no_output_deadline_at_unix_ms = spec.no_output_timeout.map(|timeout| {
        let delta = i64::try_from(timeout.as_millis()).unwrap_or(i64::MAX);
        created_at_unix_ms.saturating_add(delta)
    });
    let command = sanitized_command_record(&spec, process.lease())?;
    let record = ProcessSessionRecordV2 {
        schema_version: PROCESS_SESSION_SCHEMA_VERSION,
        process_session_id: process_session_id.clone(),
        owner: spec.owner,
        command,
        process_lease: process.lease().clone(),
        state: ProcessSessionState::Running,
        created_at_unix_ms,
        updated_at_unix_ms: created_at_unix_ms,
        deadline_at_unix_ms: created_at_unix_ms.saturating_add(deadline_delta),
        no_output_deadline_at_unix_ms,
        last_output_at_unix_ms: created_at_unix_ms,
        first_retained_cursor: 1,
        next_cursor: 0,
        retained_output_bytes: 0,
        output_truncated: false,
        outcome: None,
    };
    persist_record(config, &record)?;
    create_private_file(artifact_path(config, process_session_id.as_str()).as_path())?;

    let (stdin_tx, stdin_rx) = mpsc::sync_channel(STDIN_CHANNEL_CAPACITY);
    spawn_stdin_writer(process_session_id.clone(), stdin, stdin_rx, actor_tx.clone())?;
    spawn_output_reader(
        process_session_id.clone(),
        ProcessOutputStream::Stdout,
        stdout,
        actor_tx.clone(),
    )?;
    spawn_output_reader(
        process_session_id.clone(),
        ProcessOutputStream::Stderr,
        stderr,
        actor_tx.clone(),
    )?;
    sessions.insert(
        process_session_id,
        LiveProcessSession {
            record: record.clone(),
            process: Some(process),
            resource_lease: Some(resource_lease),
            stdin_tx: Some(stdin_tx),
            output: VecDeque::new(),
            output_bytes: 0,
            artifact_bytes: 0,
            open_readers: 2,
            drain_deadline: None,
            pending_state: None,
            pending_reason: None,
            exit_code: None,
            waiters: Vec::new(),
        },
    );
    Ok(record)
}

fn validate_launch_spec(spec: &ProcessLaunchSpec) -> Result<(), ProcessSupervisorError> {
    spec.owner.validate()?;
    if !spec.executable.is_absolute()
        || !spec.executable.is_file()
        || !spec.cwd.is_absolute()
        || !spec.cwd.is_dir()
        || spec.timeout.is_zero()
        || spec.no_output_timeout.is_some_and(|timeout| timeout.is_zero())
        || spec.no_output_timeout.is_some_and(|timeout| timeout > spec.timeout)
        || spec.lease_duration < spec.timeout
        || spec.resource_service == ResourceServiceKind::Pty
        || spec.resource_units.processes == 0
        || spec.resource_units.is_zero()
    {
        return Err(ProcessSupervisorError::InvalidLaunch(
            "paths must exist and be absolute, timeout must be non-zero, and lease must cover timeout"
                .to_owned(),
        ));
    }
    Ok(())
}

fn sanitized_command_record(
    spec: &ProcessLaunchSpec,
    lease: &ProcessLeaseV1,
) -> Result<ProcessCommandRecordV2, ProcessSupervisorError> {
    let executable_label = spec
        .executable
        .file_name()
        .and_then(|value| value.to_str())
        .unwrap_or("process")
        .chars()
        .take(128)
        .collect();
    let executable_sha256 = lease.provenance.executable_sha256.clone();
    if executable_sha256.is_empty() {
        return Err(ProcessSupervisorError::InvalidLaunch(
            "verified executable digest is required".to_owned(),
        ));
    }
    let redacted_argv_preview = spec
        .args
        .iter()
        .take(16)
        .map(|argument| {
            let bounded = argument.chars().take(256).collect::<String>();
            redact_process_output_projection(bounded.as_str()).0
        })
        .collect::<Vec<_>>();
    Ok(ProcessCommandRecordV2 {
        command_sha256: command_sha256(spec),
        executable_label,
        executable_sha256,
        redacted_argv_preview,
        cwd_sha256: sha256_text(spec.cwd.to_string_lossy().as_ref()),
        environment_key_names: spec.env.keys().take(128).cloned().collect(),
        backend: ProcessBackendV2::ManagedStdio,
    })
}

fn command_sha256(spec: &ProcessLaunchSpec) -> String {
    let mut hasher = Sha256::new();
    hash_command_component(&mut hasher, spec.executable.as_os_str().as_encoded_bytes());
    for argument in &spec.args {
        hash_command_component(&mut hasher, argument.as_bytes());
    }
    hex::encode(hasher.finalize())
}

fn hash_command_component(hasher: &mut Sha256, value: &[u8]) {
    hasher.update(u64::try_from(value.len()).unwrap_or(u64::MAX).to_be_bytes());
    hasher.update(value);
}

fn spawn_stdin_writer(
    process_session_id: String,
    mut stdin: ChildStdin,
    rx: Receiver<StdinMessage>,
    actor_tx: SyncSender<ActorMessage>,
) -> Result<(), ProcessSupervisorError> {
    thread::Builder::new()
        .name("palyra-process-stdin".to_owned())
        .spawn(move || {
            while let Ok(message) = rx.recv() {
                match message {
                    StdinMessage::Write(bytes) => {
                        if let Err(error) =
                            stdin.write_all(bytes.as_slice()).and_then(|_| stdin.flush())
                        {
                            let _ = actor_tx.send(ActorMessage::StdinFailed {
                                process_session_id: process_session_id.clone(),
                                reason: error.to_string(),
                            });
                            return;
                        }
                    }
                    StdinMessage::Close => return,
                }
            }
        })
        .map(|_| ())
        .map_err(|error| ProcessSupervisorError::Io(error.to_string()))
}

fn spawn_output_reader(
    process_session_id: String,
    stream: ProcessOutputStream,
    mut reader: impl Read + Send + 'static,
    actor_tx: SyncSender<ActorMessage>,
) -> Result<(), ProcessSupervisorError> {
    thread::Builder::new()
        .name("palyra-process-output".to_owned())
        .spawn(move || {
            let mut buffer = vec![0_u8; READER_CHUNK_BYTES];
            loop {
                match reader.read(buffer.as_mut_slice()) {
                    Ok(0) => break,
                    Ok(count) => {
                        if actor_tx
                            .send(ActorMessage::Output {
                                process_session_id: process_session_id.clone(),
                                stream,
                                bytes: buffer[..count].to_vec(),
                            })
                            .is_err()
                        {
                            return;
                        }
                    }
                    Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
                    Err(_) => break,
                }
            }
            let _ = actor_tx.send(ActorMessage::OutputClosed { process_session_id });
        })
        .map(|_| ())
        .map_err(|error| ProcessSupervisorError::Io(error.to_string()))
}

fn append_output(
    config: &ProcessSupervisorConfig,
    session: &mut LiveProcessSession,
    stream: ProcessOutputStream,
    bytes: &[u8],
) {
    let sequence = session.record.next_cursor.saturating_add(1);
    let visible = ProcessOutputChunkV2::from_bytes(sequence, stream, bytes);
    let retained = RetainedOutputChunk { visible: visible.clone(), raw_bytes: bytes.to_vec() };
    session.record.next_cursor = sequence;
    session.output_bytes = session.output_bytes.saturating_add(bytes.len());
    session.output.push_back(retained);
    while session.output.len() > config.max_retained_chunks_per_session
        || session.output_bytes > config.max_retained_bytes_per_session
    {
        if let Some(evicted) = session.output.pop_front() {
            session.output_bytes =
                session.output_bytes.saturating_sub(evicted.visible.original_len());
            session.record.output_truncated = true;
        } else {
            break;
        }
    }
    session.record.first_retained_cursor = session
        .output
        .front()
        .map_or(sequence.saturating_add(1), |retained| retained.visible.sequence);
    session.record.retained_output_bytes = u64::try_from(session.output_bytes).unwrap_or(u64::MAX);
    session.record.updated_at_unix_ms = unix_time_ms();
    if let Some(deadline) = session.record.no_output_deadline_at_unix_ms {
        let interval = deadline.saturating_sub(session.record.last_output_at_unix_ms);
        session.record.no_output_deadline_at_unix_ms =
            Some(session.record.updated_at_unix_ms.saturating_add(interval));
    }
    session.record.last_output_at_unix_ms = session.record.updated_at_unix_ms;
    if session.artifact_bytes < config.max_artifact_bytes_per_session {
        match append_artifact(config, session.record.process_session_id.as_str(), &visible) {
            Ok(written) => {
                session.artifact_bytes = session.artifact_bytes.saturating_add(written);
                if session.artifact_bytes >= config.max_artifact_bytes_per_session {
                    session.record.output_truncated = true;
                }
            }
            Err(error) => {
                session.record.output_truncated = true;
                session.pending_reason = Some(bounded_reason(error.to_string().as_str()));
            }
        }
    } else {
        session.record.output_truncated = true;
    }
    if persist_record(config, &session.record).is_err() {
        session.pending_state = Some(ProcessSessionState::CleanupFailed);
        session.pending_reason = Some("process.persistence_failed".to_owned());
    }
}

fn append_artifact(
    config: &ProcessSupervisorConfig,
    process_session_id: &str,
    chunk: &ProcessOutputChunkV2,
) -> Result<u64, ProcessSupervisorError> {
    let mut encoded = serde_json::to_vec(chunk)
        .map_err(|error| ProcessSupervisorError::Persistence(error.to_string()))?;
    encoded.push(b'\n');
    let encoded_len = u64::try_from(encoded.len()).unwrap_or(u64::MAX);
    let path = artifact_path(config, process_session_id);
    let current_len = fs::metadata(path.as_path()).map(|value| value.len()).unwrap_or(0);
    if current_len.saturating_add(encoded_len) > config.max_artifact_bytes_per_session {
        return Ok(config.max_artifact_bytes_per_session.saturating_sub(current_len));
    }
    let mut artifact = OpenOptions::new()
        .append(true)
        .open(path)
        .map_err(|error| ProcessSupervisorError::Persistence(error.to_string()))?;
    artifact
        .write_all(encoded.as_slice())
        .map_err(|error| ProcessSupervisorError::Persistence(error.to_string()))?;
    Ok(encoded_len)
}

fn poll_processes(
    config: &ProcessSupervisorConfig,
    sessions: &mut HashMap<String, LiveProcessSession>,
) {
    let now_unix_ms = unix_time_ms();
    let now = Instant::now();
    for session in sessions.values_mut() {
        if session.record.state.is_terminal() {
            continue;
        }
        let no_output_timed_out = session
            .record
            .no_output_deadline_at_unix_ms
            .is_some_and(|deadline| now_unix_ms >= deadline);
        if session.record.state == ProcessSessionState::Running && no_output_timed_out {
            let _ = begin_cleanup(
                config,
                session,
                ProcessSessionState::TimedOut,
                "process.no_output_timeout",
            );
        } else if session.record.state == ProcessSessionState::Running
            && now_unix_ms >= session.record.deadline_at_unix_ms
        {
            let _ =
                begin_cleanup(config, session, ProcessSessionState::TimedOut, "process.timeout");
        } else if session.record.state == ProcessSessionState::Running {
            match session.process.as_mut().map(ManagedStdioProcess::try_wait) {
                Some(Ok(Some(status))) => begin_drain(config, session, status),
                Some(Err(error)) => {
                    session.pending_state = Some(ProcessSessionState::CleanupFailed);
                    session.pending_reason = Some(bounded_reason(error.to_string().as_str()));
                    let _ = begin_cleanup(
                        config,
                        session,
                        ProcessSessionState::CleanupFailed,
                        "process.wait_failed",
                    );
                }
                _ => {}
            }
        }
        if session.record.state == ProcessSessionState::Draining
            && (session.open_readers == 0
                || session.drain_deadline.is_some_and(|deadline| now >= deadline))
        {
            finish_draining(config, session);
        }
    }
}

fn begin_drain(
    config: &ProcessSupervisorConfig,
    session: &mut LiveProcessSession,
    status: ExitStatus,
) {
    session.stdin_tx = None;
    session.exit_code = status.code();
    session.record.state = ProcessSessionState::Draining;
    session.record.updated_at_unix_ms = unix_time_ms();
    session.drain_deadline = Some(Instant::now() + config.drain_timeout);
    session.pending_state = Some(if status.success() {
        ProcessSessionState::Succeeded
    } else {
        ProcessSessionState::Failed
    });
    session.pending_reason = Some(
        if status.success() {
            "process.exited_successfully"
        } else {
            "process.exited_unsuccessfully"
        }
        .to_owned(),
    );
    let _ = persist_record(config, &session.record);
}

fn finish_draining(config: &ProcessSupervisorConfig, session: &mut LiveProcessSession) {
    let terminal_state = session.pending_state.take().unwrap_or(ProcessSessionState::Failed);
    let reason = session.pending_reason.take().unwrap_or_else(|| "process.completed".to_owned());
    let cleanup_verified = session
        .process
        .take()
        .map(|process| process.cleanup(false))
        .is_some_and(|report| report.reason_code == "runtime.cleanup.completed");
    complete_session(
        config,
        session,
        if cleanup_verified { terminal_state } else { ProcessSessionState::CleanupFailed },
        reason.as_str(),
        cleanup_verified,
    );
}

fn begin_cleanup(
    config: &ProcessSupervisorConfig,
    session: &mut LiveProcessSession,
    requested_state: ProcessSessionState,
    reason_code: &str,
) -> Result<(), ProcessSupervisorError> {
    if session.record.state.is_terminal() {
        return Ok(());
    }
    session.stdin_tx = None;
    let cleanup_verified = session
        .process
        .take()
        .map(|process| process.cleanup(true))
        .is_some_and(|report| report.reason_code == "runtime.cleanup.completed");
    complete_session(
        config,
        session,
        if cleanup_verified { requested_state } else { ProcessSessionState::CleanupFailed },
        reason_code,
        cleanup_verified,
    );
    Ok(())
}

fn complete_session(
    config: &ProcessSupervisorConfig,
    session: &mut LiveProcessSession,
    state: ProcessSessionState,
    reason_code: &str,
    cleanup_verified: bool,
) {
    let completed_at_unix_ms = unix_time_ms();
    session.record.state = state;
    session.record.updated_at_unix_ms = completed_at_unix_ms;
    session.record.outcome = Some(ProcessOutcomeV2 {
        exit_code: session.exit_code,
        reason_code: bounded_reason(reason_code),
        completed_at_unix_ms,
        cleanup_verified,
    });
    if let Some(resource_lease) = session.resource_lease.take() {
        if config
            .resource_governor
            .release(resource_lease.lease_id.as_str(), resource_lease.generation)
            .is_err()
        {
            session.record.state = ProcessSessionState::CleanupFailed;
            if let Some(outcome) = session.record.outcome.as_mut() {
                outcome.reason_code = "process.resource_release_failed".to_owned();
                outcome.cleanup_verified = false;
            }
        }
    }
    let persist_result = persist_record(config, &session.record);
    if persist_result.is_err() {
        session.record.state = ProcessSessionState::CleanupFailed;
        if let Some(outcome) = session.record.outcome.as_mut() {
            outcome.reason_code = "process.persistence_failed".to_owned();
            outcome.cleanup_verified = false;
        }
    }
    let waiters = std::mem::take(&mut session.waiters);
    for (after_cursor, max_chunks, response) in waiters {
        let completion = ProcessCompletion {
            record: session.record.clone(),
            output: session.output_page(after_cursor, max_chunks),
        };
        let _ = response.send(Ok(completion));
    }
}

fn terminate_all(
    config: &ProcessSupervisorConfig,
    sessions: &mut HashMap<String, LiveProcessSession>,
) {
    for session in sessions.values_mut() {
        let _ = begin_cleanup(
            config,
            session,
            ProcessSessionState::Cancelled,
            "process.supervisor_shutdown",
        );
    }
}

fn persist_record(
    config: &ProcessSupervisorConfig,
    record: &ProcessSessionRecordV2,
) -> Result<(), ProcessSupervisorError> {
    let payload = serde_json::to_vec_pretty(record)
        .map_err(|error| ProcessSupervisorError::Persistence(error.to_string()))?;
    atomic_replace(
        record_path(config, record.process_session_id.as_str()).as_path(),
        payload.as_slice(),
    )
}

fn mark_unresolved_records_orphaned(
    config: &ProcessSupervisorConfig,
) -> Result<(), ProcessSupervisorError> {
    for mut record in read_records(config.records_root().as_path())? {
        if !record.state.is_terminal() {
            let completed_at_unix_ms = unix_time_ms();
            record.state = ProcessSessionState::Orphaned;
            record.updated_at_unix_ms = completed_at_unix_ms;
            record.outcome = Some(ProcessOutcomeV2 {
                exit_code: None,
                reason_code: "process.restart_requires_verified_adoption".to_owned(),
                completed_at_unix_ms,
                cleanup_verified: false,
            });
            persist_record(config, &record)?;
        }
    }
    Ok(())
}

fn read_records(
    records_root: &Path,
) -> Result<Vec<ProcessSessionRecordV2>, ProcessSupervisorError> {
    if !records_root.exists() {
        return Ok(Vec::new());
    }
    let mut records = Vec::new();
    for entry in fs::read_dir(records_root)
        .map_err(|error| ProcessSupervisorError::Persistence(error.to_string()))?
    {
        let entry =
            entry.map_err(|error| ProcessSupervisorError::Persistence(error.to_string()))?;
        if entry.path().extension().and_then(|value| value.to_str()) != Some("json") {
            continue;
        }
        let bytes = fs::read(entry.path())
            .map_err(|error| ProcessSupervisorError::Persistence(error.to_string()))?;
        let record = serde_json::from_slice::<ProcessSessionRecordV2>(bytes.as_slice())
            .map_err(|error| ProcessSupervisorError::Persistence(error.to_string()))?;
        if record.schema_version != PROCESS_SESSION_SCHEMA_VERSION {
            return Err(ProcessSupervisorError::Persistence(
                "unsupported process-session record schema version".to_owned(),
            ));
        }
        records.push(record);
    }
    records.sort_by(|left, right| left.process_session_id.cmp(&right.process_session_id));
    Ok(records)
}

fn record_path(config: &ProcessSupervisorConfig, process_session_id: &str) -> PathBuf {
    config.records_root().join(format!("{process_session_id}.json"))
}

fn artifact_path(config: &ProcessSupervisorConfig, process_session_id: &str) -> PathBuf {
    config.artifacts_root().join(format!("{process_session_id}.jsonl"))
}

fn create_private_dir(path: &Path) -> Result<(), ProcessSupervisorError> {
    fs::create_dir_all(path)
        .map_err(|error| ProcessSupervisorError::Persistence(error.to_string()))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        fs::set_permissions(path, fs::Permissions::from_mode(0o700))
            .map_err(|error| ProcessSupervisorError::Persistence(error.to_string()))?;
    }
    Ok(())
}

fn create_private_file(path: &Path) -> Result<(), ProcessSupervisorError> {
    File::create(path).map_err(|error| ProcessSupervisorError::Persistence(error.to_string()))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        fs::set_permissions(path, fs::Permissions::from_mode(0o600))
            .map_err(|error| ProcessSupervisorError::Persistence(error.to_string()))?;
    }
    Ok(())
}

fn atomic_replace(path: &Path, payload: &[u8]) -> Result<(), ProcessSupervisorError> {
    let timestamp_ns = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_nanos();
    let mut temporary_name = path.as_os_str().to_os_string();
    temporary_name.push(format!(".tmp.{}.{}", std::process::id(), timestamp_ns));
    let temporary_path = PathBuf::from(temporary_name);
    fs::write(temporary_path.as_path(), payload)
        .map_err(|error| ProcessSupervisorError::Persistence(error.to_string()))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        fs::set_permissions(temporary_path.as_path(), fs::Permissions::from_mode(0o600))
            .map_err(|error| ProcessSupervisorError::Persistence(error.to_string()))?;
    }
    if let Err(rename_error) = fs::rename(temporary_path.as_path(), path) {
        if !path.is_file() {
            let _ = fs::remove_file(temporary_path.as_path());
            return Err(ProcessSupervisorError::Persistence(rename_error.to_string()));
        }
        // Windows cannot replace an existing open destination with rename, so
        // retain a rollback file until the new record is installed.
        let mut rollback_name = path.as_os_str().to_os_string();
        rollback_name.push(format!(".swap.{}.{}", std::process::id(), timestamp_ns));
        let rollback_path = PathBuf::from(rollback_name);
        fs::rename(path, rollback_path.as_path())
            .map_err(|error| ProcessSupervisorError::Persistence(error.to_string()))?;
        if let Err(install_error) = fs::rename(temporary_path.as_path(), path) {
            let _ = fs::rename(rollback_path.as_path(), path);
            let _ = fs::remove_file(temporary_path.as_path());
            return Err(ProcessSupervisorError::Persistence(install_error.to_string()));
        }
        let _ = fs::remove_file(rollback_path);
    }
    Ok(())
}

fn sha256_text(value: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(value.as_bytes());
    hex::encode(hasher.finalize())
}

fn bounded_reason(value: &str) -> String {
    value.chars().take(MAX_REASON_TEXT_BYTES).collect()
}

fn next_generation() -> u64 {
    let millis =
        u64::try_from(SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_millis())
            .unwrap_or(u64::MAX);
    millis.max(1)
}

fn unix_time_ms() -> i64 {
    i64::try_from(SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_millis())
        .unwrap_or(i64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::application::local_resource_governor::LocalResourceGovernorConfig;
    use std::io::{BufRead as _, BufReader};

    fn test_config(root: &Path) -> ProcessSupervisorConfig {
        let limit = ResourceUnitsV1 {
            processes: 16,
            memory_bytes: 4 * 1024 * 1024 * 1024,
            file_descriptors: 1_024,
            sockets: 256,
            spool_bytes: 64 * 1024 * 1024,
            concurrency: 64,
        };
        ProcessSupervisorConfig {
            state_root: root.to_path_buf(),
            max_sessions: 4,
            max_retained_chunks_per_session: 8,
            max_retained_bytes_per_session: 16 * 1024,
            max_artifact_bytes_per_session: 64 * 1024,
            drain_timeout: Duration::from_secs(2),
            resource_governor: LocalResourceGovernor::open(LocalResourceGovernorConfig {
                registry_path: root.join("resource-governor").join("leases.json"),
                global_limit: limit,
                per_owner_limit: limit,
                max_records: 128,
            })
            .expect("open resource governor"),
        }
    }

    fn owner() -> ProcessOwnerV2 {
        ProcessOwnerV2 {
            session_id: "session-test".to_owned(),
            run_id: "run-test".to_owned(),
            turn_id: "turn-test".to_owned(),
            agent_id: "agent-test".to_owned(),
            correlation_id: "correlation-test".to_owned(),
        }
    }

    #[cfg(windows)]
    fn shell_spec(script: &str, cwd: &Path, timeout: Duration) -> ProcessLaunchSpec {
        let executable = PathBuf::from(
            std::env::var_os("COMSPEC").unwrap_or_else(|| "C:\\Windows\\System32\\cmd.exe".into()),
        );
        ProcessLaunchSpec {
            executable,
            args: vec![
                "/D".to_owned(),
                "/V:ON".to_owned(),
                "/S".to_owned(),
                "/C".to_owned(),
                script.to_owned(),
            ],
            cwd: cwd.to_path_buf(),
            env: BTreeMap::from([(
                "SYSTEMROOT".to_owned(),
                std::env::var("SYSTEMROOT").unwrap_or_else(|_| "C:\\Windows".to_owned()),
            )]),
            owner: owner(),
            timeout,
            no_output_timeout: None,
            lease_duration: timeout + Duration::from_secs(5),
            resource_priority: ResourcePriority::Foreground,
            resource_service: ResourceServiceKind::Process,
            resource_units: ResourceUnitsV1 {
                processes: 1,
                memory_bytes: 64 * 1024 * 1024,
                file_descriptors: 4,
                sockets: 0,
                spool_bytes: 64 * 1024,
                concurrency: 1,
            },
        }
    }

    #[cfg(not(windows))]
    fn shell_spec(script: &str, cwd: &Path, timeout: Duration) -> ProcessLaunchSpec {
        ProcessLaunchSpec {
            executable: PathBuf::from("/bin/sh"),
            args: vec!["-c".to_owned(), script.to_owned()],
            cwd: cwd.to_path_buf(),
            env: BTreeMap::from([("PATH".to_owned(), "/usr/bin:/bin".to_owned())]),
            owner: owner(),
            timeout,
            no_output_timeout: None,
            lease_duration: timeout + Duration::from_secs(5),
            resource_priority: ResourcePriority::Foreground,
            resource_service: ResourceServiceKind::Process,
            resource_units: ResourceUnitsV1 {
                processes: 1,
                memory_bytes: 64 * 1024 * 1024,
                file_descriptors: 4,
                sockets: 0,
                spool_bytes: 64 * 1024,
                concurrency: 1,
            },
        }
    }

    #[test]
    fn completion_waits_for_both_output_streams_and_persists_record() {
        let temp = tempfile::tempdir().expect("temp dir");
        let supervisor =
            ProcessSupervisor::start(test_config(temp.path())).expect("start supervisor");
        #[cfg(windows)]
        let script = "echo alpha & echo beta 1>&2";
        #[cfg(not(windows))]
        let script = "printf alpha; printf beta >&2";
        let record = supervisor
            .launch(shell_spec(script, temp.path(), Duration::from_secs(5)))
            .expect("launch process");
        let completion = supervisor
            .wait(record.process_session_id.as_str(), None, 16, Duration::from_secs(10))
            .expect("wait process");
        assert_eq!(completion.record.state, ProcessSessionState::Succeeded);
        assert!(completion
            .output
            .chunks
            .iter()
            .any(|chunk| chunk.text_projection.contains("alpha")));
        assert!(completion
            .output
            .chunks
            .iter()
            .any(|chunk| chunk.text_projection.contains("beta")));
        let recovered =
            ProcessSupervisor::recover_records(&test_config(temp.path())).expect("recover records");
        assert_eq!(recovered.len(), 1);
        assert_eq!(recovered[0].state, ProcessSessionState::Succeeded);
    }

    #[test]
    fn timeout_terminates_owned_process_tree_and_wakes_waiter() {
        let temp = tempfile::tempdir().expect("temp dir");
        let supervisor =
            ProcessSupervisor::start(test_config(temp.path())).expect("start supervisor");
        #[cfg(windows)]
        let script = "for /L %i in (1,1,2147483647) do @rem";
        #[cfg(not(windows))]
        let script = "sleep 30";
        let record = supervisor
            .launch(shell_spec(script, temp.path(), Duration::from_millis(150)))
            .expect("launch process");
        let completion = supervisor
            .wait(record.process_session_id.as_str(), None, 8, Duration::from_secs(10))
            .expect("wait timeout");
        assert_eq!(
            completion.record.state,
            ProcessSessionState::TimedOut,
            "unexpected outcome: {:?}; output: {:?}",
            completion.record.outcome,
            completion.output.chunks
        );
        assert!(completion.record.outcome.as_ref().is_some_and(|outcome| outcome.cleanup_verified));
    }

    #[test]
    fn no_output_timeout_has_a_distinct_terminal_reason() {
        let temp = tempfile::tempdir().expect("temp dir");
        let supervisor =
            ProcessSupervisor::start(test_config(temp.path())).expect("start supervisor");
        #[cfg(windows)]
        let script = "for /L %i in (1,1,2147483647) do @rem";
        #[cfg(not(windows))]
        let script = "sleep 30";
        let mut spec = shell_spec(script, temp.path(), Duration::from_secs(5));
        spec.no_output_timeout = Some(Duration::from_millis(150));
        let record = supervisor.launch(spec).expect("launch process");
        let completion = supervisor
            .wait(record.process_session_id.as_str(), None, 8, Duration::from_secs(10))
            .expect("wait no-output timeout");
        assert_eq!(completion.record.state, ProcessSessionState::TimedOut);
        assert_eq!(
            completion.record.outcome.as_ref().map(|outcome| outcome.reason_code.as_str()),
            Some("process.no_output_timeout")
        );
    }

    #[test]
    fn status_tail_and_wait_survive_exit_poll_race() {
        let temp = tempfile::tempdir().expect("temp dir");
        let supervisor =
            ProcessSupervisor::start(test_config(temp.path())).expect("start supervisor");
        let record = supervisor
            .launch(shell_spec("echo raced", temp.path(), Duration::from_secs(5)))
            .expect("launch process");
        let _ = supervisor.status(record.process_session_id.as_str()).expect("poll status");
        let first_page =
            supervisor.tail(record.process_session_id.as_str(), None, 1).expect("tail process");
        let completion = supervisor
            .wait(
                record.process_session_id.as_str(),
                Some(first_page.next_cursor),
                8,
                Duration::from_secs(10),
            )
            .expect("wait process");
        assert_eq!(completion.record.state, ProcessSessionState::Succeeded);
    }

    #[test]
    fn explicit_termination_is_idempotent_after_cleanup() {
        let temp = tempfile::tempdir().expect("temp dir");
        let supervisor =
            ProcessSupervisor::start(test_config(temp.path())).expect("start supervisor");
        #[cfg(windows)]
        let script = "for /L %i in (1,1,2147483647) do @rem";
        #[cfg(not(windows))]
        let script = "sleep 30";
        let record = supervisor
            .launch(shell_spec(script, temp.path(), Duration::from_secs(5)))
            .expect("launch process");
        let cancelled =
            supervisor.terminate(record.process_session_id.as_str()).expect("terminate process");
        assert_eq!(cancelled.state, ProcessSessionState::Cancelled);
        let repeated =
            supervisor.terminate(record.process_session_id.as_str()).expect("repeat termination");
        assert_eq!(repeated.state, ProcessSessionState::Cancelled);
    }

    #[test]
    fn cursor_reports_eviction_after_output_flood() {
        let temp = tempfile::tempdir().expect("temp dir");
        let mut config = test_config(temp.path());
        config.max_retained_chunks_per_session = 1;
        config.max_retained_bytes_per_session = 32;
        let supervisor = ProcessSupervisor::start(config).expect("start supervisor");
        #[cfg(windows)]
        let script = "for /L %i in (1,1,40) do @echo 0123456789abcdef";
        #[cfg(not(windows))]
        let script = "i=0; while [ $i -lt 40 ]; do echo 0123456789abcdef; i=$((i+1)); done";
        let record = supervisor
            .launch(shell_spec(script, temp.path(), Duration::from_secs(5)))
            .expect("launch process");
        let completion = supervisor
            .wait(record.process_session_id.as_str(), Some(0), 8, Duration::from_secs(10))
            .expect("wait process");
        assert!(completion.record.output_truncated);
        assert!(completion.output.cursor_reset);
        assert!(completion.output.chunks.len() <= 1);
    }

    #[test]
    fn bounded_stdin_writer_preserves_interactive_input() {
        let temp = tempfile::tempdir().expect("temp dir");
        let supervisor =
            ProcessSupervisor::start(test_config(temp.path())).expect("start supervisor");
        #[cfg(windows)]
        let script = "setlocal EnableDelayedExpansion & set /p value=& echo got:!value!";
        #[cfg(not(windows))]
        let script = "read value; printf 'got:%s\\n' \"$value\"";
        let record = supervisor
            .launch(shell_spec(script, temp.path(), Duration::from_secs(5)))
            .expect("launch process");
        supervisor
            .write(record.process_session_id.as_str(), b"hello\n".to_vec())
            .expect("write stdin");
        supervisor.close_stdin(record.process_session_id.as_str()).expect("close stdin");
        let completion = supervisor
            .wait(record.process_session_id.as_str(), None, 8, Duration::from_secs(10))
            .expect("wait process");
        assert!(
            completion
                .output
                .chunks
                .iter()
                .any(|chunk| chunk.text_projection.contains("got:hello")),
            "unexpected output: {:?}",
            completion.output.chunks
        );
    }

    #[test]
    fn input_and_interrupt_race_finishes_with_verified_cleanup() {
        let temp = tempfile::tempdir().expect("temp dir");
        let supervisor =
            ProcessSupervisor::start(test_config(temp.path())).expect("start supervisor");
        #[cfg(windows)]
        let script = "set /p value= & for /L %i in (1,1,2147483647) do @rem";
        #[cfg(not(windows))]
        let script = "read value; sleep 30";
        let record = supervisor
            .launch(shell_spec(script, temp.path(), Duration::from_secs(5)))
            .expect("launch process");
        let _ = supervisor.write(record.process_session_id.as_str(), b"partial".to_vec());
        let interrupted =
            supervisor.interrupt(record.process_session_id.as_str()).expect("interrupt process");
        assert_eq!(interrupted.state, ProcessSessionState::Cancelled);
        assert_eq!(
            interrupted.outcome.as_ref().map(|outcome| outcome.reason_code.as_str()),
            Some("process.interrupted")
        );
        assert!(interrupted.outcome.as_ref().is_some_and(|outcome| outcome.cleanup_verified));
    }

    #[test]
    fn restart_marks_unowned_running_record_as_orphaned() {
        let temp = tempfile::tempdir().expect("temp dir");
        let config = test_config(temp.path());
        create_private_dir(config.records_root().as_path()).expect("records root");
        create_private_dir(config.artifacts_root().as_path()).expect("artifacts root");
        let spec = shell_spec("echo ignored", temp.path(), Duration::from_secs(5));
        let process = spawn_managed_stdio_process(&ManagedStdioProcessConfig {
            executable: spec.executable.clone(),
            args: spec.args.clone(),
            cwd: spec.cwd.clone(),
            env: spec.env.clone(),
            generation: 1,
            lease_duration: spec.lease_duration,
        })
        .expect("spawn record process");
        let now = unix_time_ms();
        let command = sanitized_command_record(&spec, process.lease()).expect("command");
        let record = ProcessSessionRecordV2 {
            schema_version: PROCESS_SESSION_SCHEMA_VERSION,
            process_session_id: "process-restart".to_owned(),
            owner: spec.owner,
            command,
            process_lease: process.lease().clone(),
            state: ProcessSessionState::Running,
            created_at_unix_ms: now,
            updated_at_unix_ms: now,
            deadline_at_unix_ms: now + 5_000,
            no_output_deadline_at_unix_ms: None,
            last_output_at_unix_ms: now,
            first_retained_cursor: 1,
            next_cursor: 0,
            retained_output_bytes: 0,
            output_truncated: false,
            outcome: None,
        };
        persist_record(&config, &record).expect("persist record");
        drop(process);
        let supervisor = ProcessSupervisor::start(config.clone()).expect("restart supervisor");
        let recovered = ProcessSupervisor::recover_records(&config).expect("recover records");
        assert_eq!(recovered[0].state, ProcessSessionState::Orphaned);
        assert_eq!(
            recovered[0].outcome.as_ref().map(|value| value.reason_code.as_str()),
            Some("process.restart_requires_verified_adoption")
        );
        drop(supervisor);
    }

    #[test]
    fn adoption_rejects_pid_reuse_and_matching_but_unrecoverable_authority() {
        let temp = tempfile::tempdir().expect("temp dir");
        let supervisor =
            ProcessSupervisor::start(test_config(temp.path())).expect("start supervisor");
        let record = supervisor
            .launch(shell_spec("echo adoption", temp.path(), Duration::from_secs(5)))
            .expect("launch process");
        let matching = ProcessAdoptionObservationV1 {
            pid: record.process_lease.pid,
            generation: record.process_lease.generation,
            provenance: record.process_lease.provenance.clone(),
        };
        let exact = evaluate_process_adoption(&record, &matching);
        assert_eq!(exact.decision, ProcessAdoptionDecisionV1::RefusedAuthorityUnavailable);
        assert!(!exact.signalling_authorized);

        let mut reused = matching.clone();
        reused.provenance.start_token.push_str("-reused");
        let mismatch = evaluate_process_adoption(&record, &reused);
        assert_eq!(mismatch.decision, ProcessAdoptionDecisionV1::RefusedProvenanceMismatch);
        assert!(!mismatch.signalling_authorized);

        let mut different_pid = matching;
        different_pid.pid = different_pid.pid.saturating_add(1);
        assert_eq!(
            evaluate_process_adoption(&record, &different_pid).decision,
            ProcessAdoptionDecisionV1::RefusedIdentityMismatch
        );
    }

    #[test]
    fn command_record_and_output_artifact_do_not_persist_secret_values_or_cwd() {
        let temp = tempfile::tempdir().expect("temp dir");
        let supervisor =
            ProcessSupervisor::start(test_config(temp.path())).expect("start supervisor");
        let mut spec =
            shell_spec("echo api_key=super-secret-value", temp.path(), Duration::from_secs(5));
        spec.env.insert("SECRET_TOKEN".to_owned(), "never-persist-me".to_owned());
        let record = supervisor.launch(spec).expect("launch process");
        let completion = supervisor
            .wait(record.process_session_id.as_str(), None, 8, Duration::from_secs(10))
            .expect("wait process");
        let persisted = fs::read_to_string(record_path(
            &test_config(temp.path()),
            completion.record.process_session_id.as_str(),
        ))
        .expect("read record");
        assert!(!persisted.contains("never-persist-me"));
        assert!(!persisted.contains("super-secret-value"));
        assert!(!persisted.contains(temp.path().to_string_lossy().as_ref()));
        let artifact = fs::read_to_string(artifact_path(
            &test_config(temp.path()),
            record.process_session_id.as_str(),
        ))
        .expect("read artifact");
        assert!(!artifact.contains("super-secret-value"));
        assert!(artifact.contains("[REDACTED_SECRET]"));
        assert!(artifact.contains("\"redacted\":true"));
    }

    #[test]
    fn visible_output_is_redacted_while_internal_protocol_bytes_remain_lossless() {
        let raw = b"\xffapi_key=super-secret-value";
        let visible = ProcessOutputChunkV2::from_bytes(1, ProcessOutputStream::Stdout, raw);
        let retained = RetainedOutputChunk { visible: visible.clone(), raw_bytes: raw.to_vec() };
        assert_eq!(retained.raw_bytes, raw);
        assert!(visible.text_projection.contains('\u{fffd}'));
        assert!(!visible.text_projection.contains("super-secret-value"));
        assert!(visible.redacted);
        assert!(!visible.redaction_reason_codes.is_empty());
        let serialized = serde_json::to_string(&visible).expect("serialize visible output");
        assert!(!serialized.contains("super-secret-value"));
    }

    #[test]
    fn read_records_ignores_temporary_swap_files() {
        let temp = tempfile::tempdir().expect("temp dir");
        fs::write(temp.path().join("record.json.tmp.1"), "{}").expect("write temp");
        fs::write(temp.path().join("record.json.swap.1"), "{}").expect("write swap");
        assert!(read_records(temp.path()).expect("read records").is_empty());
    }

    #[test]
    fn restart_rejects_unknown_record_versions_and_fields_without_rewrite() {
        let mutators: [fn(&mut serde_json::Value); 2] = [
            |value: &mut serde_json::Value| value["schema_version"] = serde_json::json!(999),
            |value: &mut serde_json::Value| {
                value["unknown_record_field"] = serde_json::json!("must fail closed");
            },
        ];
        for mutate in mutators {
            let temp = tempfile::tempdir().expect("temp dir");
            let config = test_config(temp.path());
            let supervisor = ProcessSupervisor::start(config.clone()).expect("start supervisor");
            let record = supervisor
                .launch(shell_spec("echo contract", temp.path(), Duration::from_secs(5)))
                .expect("launch process");
            supervisor
                .wait(record.process_session_id.as_str(), None, 16, Duration::from_secs(10))
                .expect("wait process");
            supervisor.shutdown().expect("shutdown supervisor");
            let path = record_path(&config, record.process_session_id.as_str());
            let mut value: serde_json::Value =
                serde_json::from_slice(fs::read(path.as_path()).expect("read record").as_slice())
                    .expect("decode record");
            mutate(&mut value);
            let bytes = serde_json::to_vec_pretty(&value).expect("encode invalid record");
            fs::write(path.as_path(), bytes.as_slice()).expect("write invalid record");

            let error = match ProcessSupervisor::start(config.clone()) {
                Ok(_) => panic!("invalid process record was accepted"),
                Err(error) => error,
            };
            assert!(matches!(error, ProcessSupervisorError::Persistence(_)));
            assert_eq!(fs::read(path.as_path()).expect("read unchanged record"), bytes);
        }
    }

    #[test]
    fn artifact_file_is_valid_json_lines() {
        let temp = tempfile::tempdir().expect("temp dir");
        let config = test_config(temp.path());
        create_private_dir(config.artifacts_root().as_path()).expect("artifacts root");
        let path = artifact_path(&config, "process-jsonl");
        create_private_file(path.as_path()).expect("artifact file");
        let chunk = ProcessOutputChunkV2::from_bytes(1, ProcessOutputStream::Stdout, b"alpha");
        append_artifact(&config, "process-jsonl", &chunk).expect("append artifact");
        let file = File::open(path).expect("open artifact");
        let lines = BufReader::new(file).lines().collect::<Result<Vec<_>, _>>().expect("lines");
        assert_eq!(lines.len(), 1);
        let decoded: ProcessOutputChunkV2 =
            serde_json::from_str(lines[0].as_str()).expect("decode chunk");
        assert_eq!(decoded, chunk);
    }
}
