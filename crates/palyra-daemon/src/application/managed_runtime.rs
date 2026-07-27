//! Reusable process transport for external harness and ACP runtimes.
//!
//! A bounded actor owns stdin and the exact process lease while reader threads
//! validate generation-pinned JSON-line frames and publish redacted events.

use std::{
    collections::BTreeMap,
    io::{BufRead, BufReader, Read, Write},
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
        mpsc::{self, Receiver, SyncSender, TryRecvError, TrySendError},
        Arc, Mutex,
    },
    thread,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;
use palyra_common::{
    redaction::redact_diagnostic_text,
    runtime_contracts::{CleanupReportV1, ProcessLeaseV1},
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use tokio::sync::{broadcast, oneshot};

use crate::sandbox_runner::{
    spawn_managed_stdio_process, ManagedStdioProcess, ManagedStdioProcessConfig,
};

const MAX_RUNTIME_FRAME_BYTES: usize = 1024 * 1024;
const MAX_RUNTIME_METHOD_BYTES: usize = 128;
const MAX_RUNTIME_EVENTS: usize = 4_096;
const MAX_STDERR_TAIL_BYTES: usize = 16 * 1024;
const ACTOR_POLL_INTERVAL: Duration = Duration::from_millis(5);

/// Reusable process transport descriptor.
#[derive(Debug, Clone)]
pub struct ManagedRuntimeDescriptor {
    pub runtime_id: String,
    pub protocol_version: String,
    pub capability_digest: String,
    pub executable: PathBuf,
    pub args: Vec<String>,
    pub cwd: PathBuf,
    pub env: BTreeMap<String, String>,
    pub handshake_timeout: Duration,
    pub command_timeout: Duration,
    pub lease_duration: Duration,
}

impl ManagedRuntimeDescriptor {
    /// Validates the bounded process and protocol plan.
    ///
    /// # Errors
    /// Returns [`RuntimeTransportError::InvalidDescriptor`] for unsafe launch metadata.
    pub fn validate(&self) -> Result<(), RuntimeTransportError> {
        if self.runtime_id.trim().is_empty()
            || self.runtime_id.len() > 128
            || self.protocol_version.trim().is_empty()
            || self.protocol_version.len() > 128
            || !is_sha256(self.capability_digest.as_str())
            || !self.executable.is_absolute()
            || !self.executable.is_file()
            || !self.cwd.is_absolute()
            || !self.cwd.is_dir()
            || self.args.len() > 128
            || self.args.iter().any(|arg| arg.len() > 4_096)
            || self.env.len() > 28
            || self.handshake_timeout.is_zero()
            || self.command_timeout.is_zero()
            || self.lease_duration.is_zero()
        {
            return Err(RuntimeTransportError::InvalidDescriptor);
        }
        Ok(())
    }
}

/// Generation-pinned transport startup request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManagedRuntimeStartRequest {
    pub session_id: String,
    pub generation: u64,
    pub resume_metadata_json: Option<String>,
}

/// Command sent to a managed runtime.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RuntimeTransportCommand {
    pub command_id: String,
    pub generation: u64,
    pub method: String,
    pub payload: Value,
    pub deadline_unix_ms: i64,
}

impl RuntimeTransportCommand {
    fn validate(&self) -> Result<(), RuntimeTransportError> {
        let bytes =
            serde_json::to_vec(self).map_err(|_| RuntimeTransportError::InvalidCommand)?.len();
        if self.command_id.trim().is_empty()
            || self.command_id.len() > 128
            || self.generation == 0
            || self.method.trim().is_empty()
            || self.method.len() > MAX_RUNTIME_METHOD_BYTES
            || self.deadline_unix_ms <= now_unix_ms()
            || bytes > MAX_RUNTIME_FRAME_BYTES
        {
            return Err(RuntimeTransportError::InvalidCommand);
        }
        Ok(())
    }
}

/// Event emitted by the runtime transport.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum RuntimeTransportEvent {
    Accepted { command_id: String, generation: u64, sequence: u64 },
    Event { command_id: String, generation: u64, sequence: u64, method: String, payload: Value },
    Terminal { command_id: String, generation: u64, sequence: u64, outcome: String, payload: Value },
    ChildExited { generation: u64, exit_code: Option<i32> },
    ProtocolError { generation: u64, reason_code: String },
    Cleanup { generation: u64, report: CleanupReportV1 },
}

/// Runtime health and quarantine snapshot.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ManagedRuntimeHealth {
    pub state: ManagedRuntimeHealthState,
    pub generation: u64,
    pub protocol_strikes: u32,
    pub last_reason_code: String,
    pub stderr_tail_redacted: String,
}

/// Managed runtime lifecycle state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ManagedRuntimeHealthState {
    Starting,
    Ready,
    Draining,
    Closed,
    Crashed,
    Quarantined,
}

/// Durable, redaction-safe binding for a running process transport.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct RuntimeBindingRecord {
    pub runtime_id: String,
    pub session_id: String,
    pub generation: u64,
    pub protocol_version: String,
    pub capability_digest: String,
    pub nonce_sha256: String,
    pub lease: ProcessLeaseV1,
    pub resume_metadata_json: Option<String>,
    pub last_acknowledged_sequence: u64,
}

/// Async transport contract shared by Codex and ACP adapters.
#[async_trait]
pub trait RuntimeTransport: Send + Sync {
    async fn start(
        &self,
        request: ManagedRuntimeStartRequest,
    ) -> Result<RuntimeBindingRecord, RuntimeTransportError>;
    async fn send_command(
        &self,
        command: RuntimeTransportCommand,
    ) -> Result<(), RuntimeTransportError>;
    fn event_stream(
        &self,
    ) -> Result<broadcast::Receiver<RuntimeTransportEvent>, RuntimeTransportError>;
    async fn cancel(&self, command_id: &str, generation: u64) -> Result<(), RuntimeTransportError>;
    async fn close(&self) -> Result<CleanupReportV1, RuntimeTransportError>;
    fn binding(&self) -> Result<Option<RuntimeBindingRecord>, RuntimeTransportError>;
    fn health(&self) -> ManagedRuntimeHealth;
}

#[derive(Debug)]
enum ActorCommand {
    Send {
        frame: Vec<u8>,
        generation: u64,
        acknowledgement: oneshot::Sender<Result<(), RuntimeTransportError>>,
    },
    Cancel {
        command_id: String,
        generation: u64,
        acknowledgement: oneshot::Sender<Result<(), RuntimeTransportError>>,
    },
    Close {
        acknowledgement: oneshot::Sender<Result<CleanupReportV1, RuntimeTransportError>>,
    },
    ProtocolViolation {
        reason_code: &'static str,
    },
}

struct RunningRuntime {
    generation: u64,
    binding: Arc<Mutex<RuntimeBindingRecord>>,
    cleanup_report: Arc<Mutex<Option<CleanupReportV1>>>,
    normal: SyncSender<ActorCommand>,
    priority: SyncSender<ActorCommand>,
    actor: Option<thread::JoinHandle<()>>,
}

struct StdoutReaderState {
    generation: u64,
    events: broadcast::Sender<RuntimeTransportEvent>,
    health: Arc<Mutex<ManagedRuntimeHealth>>,
    last_sequence: Arc<AtomicU64>,
    events_in_attempt: Arc<AtomicUsize>,
    attempt_open: Arc<AtomicBool>,
    binding: Arc<Mutex<RuntimeBindingRecord>>,
    priority: SyncSender<ActorCommand>,
}

struct RuntimeActorState {
    process: ManagedStdioProcess,
    stdin: std::process::ChildStdin,
    generation: u64,
    command_timeout: Duration,
    normal: Receiver<ActorCommand>,
    priority: Receiver<ActorCommand>,
    events: broadcast::Sender<RuntimeTransportEvent>,
    health: Arc<Mutex<ManagedRuntimeHealth>>,
    cleanup_report: Arc<Mutex<Option<CleanupReportV1>>>,
}

/// Bounded JSON-line process transport.
pub struct StdioRuntimeTransport {
    descriptor: ManagedRuntimeDescriptor,
    running: Mutex<Option<RunningRuntime>>,
    events: broadcast::Sender<RuntimeTransportEvent>,
    health: Arc<Mutex<ManagedRuntimeHealth>>,
    last_sequence: Arc<AtomicU64>,
}

impl StdioRuntimeTransport {
    /// Creates a stopped runtime transport.
    ///
    /// # Errors
    /// Returns [`RuntimeTransportError::InvalidDescriptor`] for an unsafe process plan.
    pub fn new(descriptor: ManagedRuntimeDescriptor) -> Result<Self, RuntimeTransportError> {
        descriptor.validate()?;
        let (events, _) = broadcast::channel(MAX_RUNTIME_EVENTS);
        Ok(Self {
            descriptor,
            running: Mutex::new(None),
            events,
            health: Arc::new(Mutex::new(ManagedRuntimeHealth {
                state: ManagedRuntimeHealthState::Closed,
                generation: 0,
                protocol_strikes: 0,
                last_reason_code: "runtime.transport.not_started".to_owned(),
                stderr_tail_redacted: String::new(),
            })),
            last_sequence: Arc::new(AtomicU64::new(0)),
        })
    }

    fn running_sender(
        &self,
        generation: u64,
        priority: bool,
    ) -> Result<SyncSender<ActorCommand>, RuntimeTransportError> {
        let running = self.running.lock().map_err(|_| RuntimeTransportError::Unavailable)?;
        let running = running.as_ref().ok_or(RuntimeTransportError::NotStarted)?;
        if running.generation != generation {
            return Err(RuntimeTransportError::StaleGeneration {
                active: running.generation,
                observed: generation,
            });
        }
        Ok(if priority { running.priority.clone() } else { running.normal.clone() })
    }

    fn update_health(&self, update: impl FnOnce(&mut ManagedRuntimeHealth)) {
        if let Ok(mut health) = self.health.lock() {
            update(&mut health);
        }
    }

    async fn finish_close_after_actor_exit(
        &self,
        running: &mut RunningRuntime,
    ) -> Result<CleanupReportV1, RuntimeTransportError> {
        if let Some(actor) = running.actor.take() {
            tokio::task::spawn_blocking(move || actor.join())
                .await
                .map_err(|_| RuntimeTransportError::Unavailable)?
                .map_err(|_| RuntimeTransportError::Unavailable)?;
        }
        let report = running
            .cleanup_report
            .lock()
            .map_err(|_| RuntimeTransportError::Unavailable)?
            .clone()
            .ok_or(RuntimeTransportError::Unavailable)?;
        self.update_health(|health| {
            health.state = ManagedRuntimeHealthState::Closed;
            health.last_reason_code = "runtime.transport.closed_after_exit".to_owned();
        });
        Ok(report)
    }
}

#[async_trait]
impl RuntimeTransport for StdioRuntimeTransport {
    async fn start(
        &self,
        request: ManagedRuntimeStartRequest,
    ) -> Result<RuntimeBindingRecord, RuntimeTransportError> {
        validate_start_request(&request)?;
        {
            let running = self.running.lock().map_err(|_| RuntimeTransportError::Unavailable)?;
            if running.is_some() {
                return Err(RuntimeTransportError::AlreadyStarted);
            }
        }
        self.last_sequence.store(0, Ordering::Release);
        self.update_health(|health| {
            health.state = ManagedRuntimeHealthState::Starting;
            health.generation = request.generation;
            health.last_reason_code = "runtime.transport.starting".to_owned();
        });
        let nonce = issue_nonce()?;
        let descriptor = self.descriptor.clone();
        let request_for_spawn = request.clone();
        let nonce_for_spawn = nonce.clone();
        let startup = tokio::task::spawn_blocking(move || {
            start_runtime_process(&descriptor, &request_for_spawn, nonce_for_spawn.as_str())
        })
        .await
        .map_err(|_| RuntimeTransportError::Unavailable)?;
        let startup = match startup {
            Ok(startup) => startup,
            Err(error) => {
                self.update_health(|health| {
                    health.state = ManagedRuntimeHealthState::Quarantined;
                    if matches!(
                        &error,
                        RuntimeTransportError::HandshakeTimedOut
                            | RuntimeTransportError::HandshakeMismatch
                            | RuntimeTransportError::MalformedFrame
                    ) {
                        health.protocol_strikes = health.protocol_strikes.saturating_add(1);
                    }
                    health.last_reason_code = runtime_start_failure_reason(&error).to_owned();
                });
                return Err(error);
            }
        };
        let StartedRuntimeProcess { process, stdin, stdout, stderr, hello } = startup;
        if hello.protocol_version != self.descriptor.protocol_version
            || hello.capability_digest != self.descriptor.capability_digest
            || hello.nonce != nonce
            || hello.generation != request.generation
        {
            let report = process.cleanup(false);
            let _ = self
                .events
                .send(RuntimeTransportEvent::Cleanup { generation: request.generation, report });
            self.update_health(|health| {
                health.state = ManagedRuntimeHealthState::Quarantined;
                health.protocol_strikes = health.protocol_strikes.saturating_add(1);
                health.last_reason_code = "runtime.transport.handshake_mismatch".to_owned();
            });
            return Err(RuntimeTransportError::HandshakeMismatch);
        }
        let binding = RuntimeBindingRecord {
            runtime_id: self.descriptor.runtime_id.clone(),
            session_id: request.session_id,
            generation: request.generation,
            protocol_version: hello.protocol_version,
            capability_digest: hello.capability_digest,
            nonce_sha256: sha256_hex(nonce.as_bytes()),
            lease: process.lease().clone(),
            resume_metadata_json: request.resume_metadata_json,
            last_acknowledged_sequence: 0,
        };
        let shared_binding = Arc::new(Mutex::new(binding.clone()));
        let cleanup_report = Arc::new(Mutex::new(None));
        let events_in_attempt = Arc::new(AtomicUsize::new(0));
        let attempt_open = Arc::new(AtomicBool::new(false));
        let (normal_tx, normal_rx) = mpsc::sync_channel(64);
        let (priority_tx, priority_rx) = mpsc::sync_channel(8);
        let events = self.events.clone();
        let health = Arc::clone(&self.health);
        let last_sequence = Arc::clone(&self.last_sequence);
        let reader_priority = priority_tx.clone();
        let generation = request.generation;
        spawn_stdout_reader(
            stdout,
            StdoutReaderState {
                generation,
                events: events.clone(),
                health: Arc::clone(&health),
                last_sequence: Arc::clone(&last_sequence),
                events_in_attempt,
                attempt_open,
                binding: Arc::clone(&shared_binding),
                priority: reader_priority,
            },
        )?;
        spawn_stderr_reader(stderr, Arc::clone(&health))?;
        let command_timeout = self.descriptor.command_timeout;
        let actor_cleanup_report = Arc::clone(&cleanup_report);
        let actor = thread::Builder::new()
            .name(format!("palyra-runtime-actor-{}", self.descriptor.runtime_id))
            .spawn(move || {
                runtime_actor(RuntimeActorState {
                    process,
                    stdin,
                    generation,
                    command_timeout,
                    normal: normal_rx,
                    priority: priority_rx,
                    events,
                    health,
                    cleanup_report: actor_cleanup_report,
                });
            })
            .map_err(|_| RuntimeTransportError::Unavailable)?;
        let running = RunningRuntime {
            generation,
            binding: shared_binding,
            cleanup_report,
            normal: normal_tx,
            priority: priority_tx,
            actor: Some(actor),
        };
        *self.running.lock().map_err(|_| RuntimeTransportError::Unavailable)? = Some(running);
        self.update_health(|health| {
            health.state = ManagedRuntimeHealthState::Ready;
            health.last_reason_code = "runtime.transport.ready".to_owned();
        });
        Ok(binding)
    }

    async fn send_command(
        &self,
        command: RuntimeTransportCommand,
    ) -> Result<(), RuntimeTransportError> {
        command.validate()?;
        let sender = self.running_sender(command.generation, false)?;
        let frame = encode_wire_command(&command)?;
        let (acknowledgement, receiver) = oneshot::channel();
        sender
            .try_send(ActorCommand::Send { frame, generation: command.generation, acknowledgement })
            .map_err(map_try_send_error)?;
        tokio::time::timeout(self.descriptor.command_timeout, receiver)
            .await
            .map_err(|_| RuntimeTransportError::CommandTimedOut)?
            .map_err(|_| RuntimeTransportError::Unavailable)?
    }

    fn event_stream(
        &self,
    ) -> Result<broadcast::Receiver<RuntimeTransportEvent>, RuntimeTransportError> {
        if self.running.lock().map_err(|_| RuntimeTransportError::Unavailable)?.is_none() {
            return Err(RuntimeTransportError::NotStarted);
        }
        Ok(self.events.subscribe())
    }

    async fn cancel(&self, command_id: &str, generation: u64) -> Result<(), RuntimeTransportError> {
        if command_id.trim().is_empty() || command_id.len() > 128 {
            return Err(RuntimeTransportError::InvalidCommand);
        }
        let sender = self.running_sender(generation, true)?;
        let (acknowledgement, receiver) = oneshot::channel();
        sender
            .try_send(ActorCommand::Cancel {
                command_id: command_id.to_owned(),
                generation,
                acknowledgement,
            })
            .map_err(map_try_send_error)?;
        tokio::time::timeout(self.descriptor.command_timeout, receiver)
            .await
            .map_err(|_| RuntimeTransportError::CommandTimedOut)?
            .map_err(|_| RuntimeTransportError::Unavailable)?
    }

    async fn close(&self) -> Result<CleanupReportV1, RuntimeTransportError> {
        let mut running =
            self.running.lock().map_err(|_| RuntimeTransportError::Unavailable)?.take();
        let Some(mut running) = running.take() else {
            return Err(RuntimeTransportError::NotStarted);
        };
        self.update_health(|health| {
            health.state = ManagedRuntimeHealthState::Draining;
            health.last_reason_code = "runtime.transport.draining".to_owned();
        });
        let (acknowledgement, receiver) = oneshot::channel();
        match running.priority.try_send(ActorCommand::Close { acknowledgement }) {
            Ok(()) => {}
            Err(TrySendError::Full(_)) => {
                *self.running.lock().map_err(|_| RuntimeTransportError::Unavailable)? =
                    Some(running);
                return Err(RuntimeTransportError::Backpressure);
            }
            Err(TrySendError::Disconnected(_)) => {
                return self.finish_close_after_actor_exit(&mut running).await;
            }
        }
        let report = match tokio::time::timeout(self.descriptor.command_timeout, receiver).await {
            Ok(Ok(result)) => result?,
            // A spontaneous child exit can persist cleanup evidence before dropping the
            // queued close acknowledgement, so recover that exact report from the actor.
            Ok(Err(_)) => return self.finish_close_after_actor_exit(&mut running).await,
            Err(_) => {
                // Preserve the owned actor so a later close can still collect its evidence.
                *self.running.lock().map_err(|_| RuntimeTransportError::Unavailable)? =
                    Some(running);
                return Err(RuntimeTransportError::CommandTimedOut);
            }
        };
        if let Some(actor) = running.actor.take() {
            tokio::task::spawn_blocking(move || actor.join())
                .await
                .map_err(|_| RuntimeTransportError::Unavailable)?
                .map_err(|_| RuntimeTransportError::Unavailable)?;
        }
        self.update_health(|health| {
            health.state = ManagedRuntimeHealthState::Closed;
            health.last_reason_code = "runtime.transport.closed".to_owned();
        });
        Ok(report)
    }

    fn binding(&self) -> Result<Option<RuntimeBindingRecord>, RuntimeTransportError> {
        let running = self.running.lock().map_err(|_| RuntimeTransportError::Unavailable)?;
        running
            .as_ref()
            .map(|runtime| {
                runtime
                    .binding
                    .lock()
                    .map(|binding| binding.clone())
                    .map_err(|_| RuntimeTransportError::Unavailable)
            })
            .transpose()
    }

    fn health(&self) -> ManagedRuntimeHealth {
        self.health.lock().map_or_else(
            |_| ManagedRuntimeHealth {
                state: ManagedRuntimeHealthState::Quarantined,
                generation: 0,
                protocol_strikes: u32::MAX,
                last_reason_code: "runtime.transport.health_lock_failed".to_owned(),
                stderr_tail_redacted: String::new(),
            },
            |health| health.clone(),
        )
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RuntimeHello {
    #[serde(rename = "type")]
    frame_type: String,
    protocol_version: String,
    capability_digest: String,
    nonce: String,
    generation: u64,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum RuntimeWireEvent {
    Accepted { command_id: String, generation: u64, sequence: u64 },
    Event { command_id: String, generation: u64, sequence: u64, method: String, payload: Value },
    Terminal { command_id: String, generation: u64, sequence: u64, outcome: String, payload: Value },
}

struct StartedRuntimeProcess {
    process: ManagedStdioProcess,
    stdin: std::process::ChildStdin,
    stdout: BufReader<std::process::ChildStdout>,
    stderr: std::process::ChildStderr,
    hello: RuntimeHello,
}

fn start_runtime_process(
    descriptor: &ManagedRuntimeDescriptor,
    request: &ManagedRuntimeStartRequest,
    nonce: &str,
) -> Result<StartedRuntimeProcess, RuntimeTransportError> {
    let mut config = ManagedStdioProcessConfig {
        executable: descriptor.executable.clone(),
        args: descriptor.args.clone(),
        cwd: descriptor.cwd.clone(),
        env: descriptor.env.clone(),
        generation: request.generation,
        lease_duration: descriptor.lease_duration,
    };
    config.env.insert("PALYRA_RUNTIME_NONCE".to_owned(), nonce.to_owned());
    config.env.insert("PALYRA_RUNTIME_GENERATION".to_owned(), request.generation.to_string());
    config
        .env
        .insert("PALYRA_RUNTIME_PROTOCOL_VERSION".to_owned(), descriptor.protocol_version.clone());
    config.env.insert(
        "PALYRA_RUNTIME_CAPABILITY_DIGEST".to_owned(),
        descriptor.capability_digest.clone(),
    );
    if let Some(resume_metadata_json) = request.resume_metadata_json.as_ref() {
        config
            .env
            .insert("PALYRA_RUNTIME_RESUME_METADATA_JSON".to_owned(), resume_metadata_json.clone());
    }
    let mut process = spawn_managed_stdio_process(&config).map_err(|error| {
        RuntimeTransportError::SpawnFailed {
            safe_message: redact_diagnostic_text(error.message.as_str()),
        }
    })?;
    let stdin = process.take_stdin().map_err(|_| RuntimeTransportError::StdioUnavailable)?;
    let stdout = process.take_stdout().map_err(|_| RuntimeTransportError::StdioUnavailable)?;
    let stderr = process.take_stderr().map_err(|_| RuntimeTransportError::StdioUnavailable)?;
    let (hello_tx, hello_rx) = mpsc::sync_channel(1);
    let hello_reader = thread::Builder::new()
        .name(format!("palyra-runtime-handshake-{}", descriptor.runtime_id))
        .spawn(move || {
            let mut stdout = BufReader::new(stdout);
            let result = read_bounded_blocking_line(&mut stdout, MAX_RUNTIME_FRAME_BYTES)
                .map(|line| (line, stdout));
            let _ = hello_tx.send(result);
        })
        .map_err(|_| RuntimeTransportError::Unavailable)?;
    let (hello_line, stdout) = match hello_rx.recv_timeout(descriptor.handshake_timeout) {
        Ok(result) => result?,
        Err(mpsc::RecvTimeoutError::Timeout) => {
            drop(stdin);
            drop(stderr);
            let _ = process.cleanup(false);
            let _ = hello_reader.join();
            return Err(RuntimeTransportError::HandshakeTimedOut);
        }
        Err(mpsc::RecvTimeoutError::Disconnected) => {
            drop(stdin);
            drop(stderr);
            let _ = process.cleanup(false);
            let _ = hello_reader.join();
            return Err(RuntimeTransportError::ReadFailed);
        }
    };
    hello_reader.join().map_err(|_| RuntimeTransportError::ReadFailed)?;
    let hello: RuntimeHello =
        serde_json::from_slice(&hello_line).map_err(|_| RuntimeTransportError::MalformedFrame)?;
    if hello.frame_type != "hello" {
        return Err(RuntimeTransportError::HandshakeMismatch);
    }
    Ok(StartedRuntimeProcess { process, stdin, stdout, stderr, hello })
}

fn spawn_stdout_reader(
    mut reader: BufReader<std::process::ChildStdout>,
    state: StdoutReaderState,
) -> Result<(), RuntimeTransportError> {
    let StdoutReaderState {
        generation,
        events,
        health,
        last_sequence,
        events_in_attempt,
        attempt_open,
        binding,
        priority,
    } = state;
    thread::Builder::new()
        .name(format!("palyra-runtime-stdout-{generation}"))
        .spawn(move || loop {
            let mut line = Vec::new();
            match reader.read_until(b'\n', &mut line) {
                Ok(0) => break,
                Ok(_) if line.len() <= MAX_RUNTIME_FRAME_BYTES => {}
                Ok(_) => {
                    protocol_violation(
                        generation,
                        "runtime.transport.frame_too_large",
                        &events,
                        &health,
                        &priority,
                    );
                    break;
                }
                Err(_) => {
                    protocol_violation(
                        generation,
                        "runtime.transport.read_failed",
                        &events,
                        &health,
                        &priority,
                    );
                    break;
                }
            }
            let frame = match serde_json::from_slice::<RuntimeWireEvent>(&line) {
                Ok(frame) => frame,
                Err(_) => {
                    protocol_violation(
                        generation,
                        "runtime.transport.malformed_frame",
                        &events,
                        &health,
                        &priority,
                    );
                    break;
                }
            };
            let event = match validate_wire_event(
                frame,
                generation,
                &last_sequence,
                &events_in_attempt,
                &attempt_open,
            ) {
                Ok(event) => event,
                Err(reason_code) => {
                    protocol_violation(generation, reason_code, &events, &health, &priority);
                    break;
                }
            };
            update_runtime_binding(&binding, &event);
            let _ = events.send(event);
        })
        .map_err(|_| RuntimeTransportError::Unavailable)?;
    Ok(())
}

fn update_runtime_binding(
    binding: &Arc<Mutex<RuntimeBindingRecord>>,
    event: &RuntimeTransportEvent,
) {
    let (sequence, resume_metadata) = match event {
        RuntimeTransportEvent::Accepted { sequence, .. }
        | RuntimeTransportEvent::Event { sequence, .. } => (*sequence, None),
        RuntimeTransportEvent::Terminal { sequence, payload, .. } => {
            let metadata = payload
                .get("codex_thread_id")
                .and_then(Value::as_str)
                .filter(|value| !value.trim().is_empty() && value.len() <= 256)
                .map(|thread_id| {
                    serde_json::json!({
                        "codex_thread_id": thread_id,
                        "server_version": payload
                            .get("server_version")
                            .and_then(Value::as_str)
                            .unwrap_or("unknown"),
                    })
                    .to_string()
                });
            (*sequence, metadata)
        }
        RuntimeTransportEvent::ChildExited { .. }
        | RuntimeTransportEvent::ProtocolError { .. }
        | RuntimeTransportEvent::Cleanup { .. } => return,
    };
    if let Ok(mut binding) = binding.lock() {
        binding.last_acknowledged_sequence = sequence;
        if let Some(metadata) = resume_metadata {
            binding.resume_metadata_json = Some(metadata);
        }
    }
}

fn spawn_stderr_reader(
    stderr: std::process::ChildStderr,
    health: Arc<Mutex<ManagedRuntimeHealth>>,
) -> Result<(), RuntimeTransportError> {
    thread::Builder::new()
        .name("palyra-runtime-stderr".to_owned())
        .spawn(move || {
            let mut reader = BufReader::new(stderr);
            let mut buffer = [0_u8; 1024];
            let mut tail = Vec::new();
            loop {
                match reader.read(&mut buffer) {
                    Ok(0) => break,
                    Ok(count) => {
                        tail.extend_from_slice(&buffer[..count]);
                        if tail.len() > MAX_STDERR_TAIL_BYTES {
                            tail.drain(..tail.len().saturating_sub(MAX_STDERR_TAIL_BYTES));
                        }
                        if let Ok(mut health) = health.lock() {
                            health.stderr_tail_redacted =
                                redact_diagnostic_text(String::from_utf8_lossy(&tail).as_ref());
                        }
                    }
                    Err(_) => break,
                }
            }
        })
        .map_err(|_| RuntimeTransportError::Unavailable)?;
    Ok(())
}

fn runtime_actor(state: RuntimeActorState) {
    let RuntimeActorState {
        mut process,
        mut stdin,
        generation,
        command_timeout,
        normal,
        priority,
        events,
        health,
        cleanup_report,
    } = state;
    loop {
        let command = match priority.try_recv() {
            Ok(command) => Some(command),
            Err(TryRecvError::Disconnected | TryRecvError::Empty) => normal.try_recv().ok(),
        };
        if let Some(command) = command {
            match command {
                ActorCommand::Send { frame, generation: observed, acknowledgement } => {
                    let outcome = if observed != generation {
                        Err(RuntimeTransportError::StaleGeneration { active: generation, observed })
                    } else {
                        write_frame(&mut stdin, frame.as_slice())
                    };
                    let _ = acknowledgement.send(outcome);
                }
                ActorCommand::Cancel { command_id, generation: observed, acknowledgement } => {
                    let outcome = if observed != generation {
                        Err(RuntimeTransportError::StaleGeneration { active: generation, observed })
                    } else {
                        let frame = serde_json::to_vec(&serde_json::json!({
                            "type": "cancel",
                            "command_id": command_id,
                            "generation": generation,
                            "deadline_unix_ms": now_unix_ms()
                                .saturating_add(i64::try_from(command_timeout.as_millis()).unwrap_or(i64::MAX)),
                        }))
                        .map_err(|_| RuntimeTransportError::InvalidCommand)
                        .and_then(|mut frame| {
                            frame.push(b'\n');
                            write_frame(&mut stdin, frame.as_slice())
                        });
                        frame
                    };
                    let _ = acknowledgement.send(outcome);
                }
                ActorCommand::Close { acknowledgement } => {
                    let graceful = serde_json::to_vec(&serde_json::json!({
                        "type": "close",
                        "generation": generation,
                    }))
                    .map(|mut frame| {
                        frame.push(b'\n');
                        write_frame(&mut stdin, frame.as_slice()).is_ok()
                    })
                    .unwrap_or(false);
                    drop(stdin);
                    let report = process.cleanup(graceful);
                    store_cleanup_report(&cleanup_report, &report);
                    let _ = events.send(RuntimeTransportEvent::Cleanup {
                        generation,
                        report: report.clone(),
                    });
                    let _ = acknowledgement.send(Ok(report));
                    break;
                }
                ActorCommand::ProtocolViolation { reason_code } => {
                    if let Ok(mut runtime_health) = health.lock() {
                        runtime_health.state = ManagedRuntimeHealthState::Quarantined;
                        runtime_health.last_reason_code = reason_code.to_owned();
                    }
                    drop(stdin);
                    let report = process.cleanup(false);
                    store_cleanup_report(&cleanup_report, &report);
                    let _ = events.send(RuntimeTransportEvent::Cleanup { generation, report });
                    break;
                }
            }
        }
        match process.try_wait() {
            Ok(Some(status)) => {
                if let Ok(mut runtime_health) = health.lock() {
                    runtime_health.state = ManagedRuntimeHealthState::Crashed;
                    runtime_health.last_reason_code = "runtime.transport.child_exited".to_owned();
                }
                let _ = events.send(RuntimeTransportEvent::ChildExited {
                    generation,
                    exit_code: status.code(),
                });
                let report = process.cleanup(false);
                store_cleanup_report(&cleanup_report, &report);
                let _ = events.send(RuntimeTransportEvent::Cleanup { generation, report });
                break;
            }
            Ok(None) => {}
            Err(_) => {
                if let Ok(mut runtime_health) = health.lock() {
                    runtime_health.state = ManagedRuntimeHealthState::Quarantined;
                    runtime_health.protocol_strikes =
                        runtime_health.protocol_strikes.saturating_add(1);
                    runtime_health.last_reason_code =
                        "runtime.transport.child_status_failed".to_owned();
                }
                let _ = events.send(RuntimeTransportEvent::ProtocolError {
                    generation,
                    reason_code: "runtime.transport.child_status_failed".to_owned(),
                });
                drop(stdin);
                let report = process.cleanup(false);
                store_cleanup_report(&cleanup_report, &report);
                let _ = events.send(RuntimeTransportEvent::Cleanup { generation, report });
                break;
            }
        }
        thread::sleep(ACTOR_POLL_INTERVAL);
    }
}

fn validate_wire_event(
    frame: RuntimeWireEvent,
    active_generation: u64,
    last_sequence: &AtomicU64,
    events_in_attempt: &AtomicUsize,
    attempt_open: &AtomicBool,
) -> Result<RuntimeTransportEvent, &'static str> {
    let (generation, sequence) = match &frame {
        RuntimeWireEvent::Accepted { generation, sequence, .. }
        | RuntimeWireEvent::Event { generation, sequence, .. }
        | RuntimeWireEvent::Terminal { generation, sequence, .. } => (*generation, *sequence),
    };
    if generation != active_generation {
        return Err("runtime.transport.stale_generation");
    }
    let previous = last_sequence.load(Ordering::Acquire);
    let expected = previous.checked_add(1).ok_or("runtime.transport.invalid_sequence")?;
    if sequence != expected {
        return Err("runtime.transport.non_monotonic_sequence");
    }
    last_sequence.store(sequence, Ordering::Release);
    match frame {
        RuntimeWireEvent::Accepted { command_id, generation, sequence }
            if attempt_open
                .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                .is_ok() =>
        {
            events_in_attempt.store(0, Ordering::Release);
            Ok(RuntimeTransportEvent::Accepted { command_id, generation, sequence })
        }
        RuntimeWireEvent::Event { command_id, generation, sequence, method, payload }
            if valid_wire_fields(command_id.as_str(), method.as_str(), &payload)
                && attempt_open.load(Ordering::Acquire) =>
        {
            if events_in_attempt.fetch_add(1, Ordering::AcqRel) >= MAX_RUNTIME_EVENTS {
                return Err("runtime.transport.event_flood");
            }
            Ok(RuntimeTransportEvent::Event { command_id, generation, sequence, method, payload })
        }
        RuntimeWireEvent::Terminal { command_id, generation, sequence, outcome, payload }
            if valid_wire_fields(command_id.as_str(), outcome.as_str(), &payload) =>
        {
            if !attempt_open.swap(false, Ordering::AcqRel) {
                return Err("runtime.transport.invalid_event");
            }
            events_in_attempt.store(0, Ordering::Release);
            Ok(RuntimeTransportEvent::Terminal {
                command_id,
                generation,
                sequence,
                outcome,
                payload,
            })
        }
        RuntimeWireEvent::Accepted { .. }
        | RuntimeWireEvent::Event { .. }
        | RuntimeWireEvent::Terminal { .. } => Err("runtime.transport.invalid_event"),
    }
}

fn store_cleanup_report(slot: &Arc<Mutex<Option<CleanupReportV1>>>, report: &CleanupReportV1) {
    if let Ok(mut current) = slot.lock() {
        *current = Some(report.clone());
    }
}

fn protocol_violation(
    generation: u64,
    reason_code: &'static str,
    events: &broadcast::Sender<RuntimeTransportEvent>,
    health: &Arc<Mutex<ManagedRuntimeHealth>>,
    priority: &SyncSender<ActorCommand>,
) {
    if let Ok(mut runtime_health) = health.lock() {
        runtime_health.protocol_strikes = runtime_health.protocol_strikes.saturating_add(1);
        runtime_health.last_reason_code = reason_code.to_owned();
        runtime_health.state = ManagedRuntimeHealthState::Quarantined;
    }
    let _ = events.send(RuntimeTransportEvent::ProtocolError {
        generation,
        reason_code: reason_code.to_owned(),
    });
    let _ = priority.try_send(ActorCommand::ProtocolViolation { reason_code });
}

fn encode_wire_command(
    command: &RuntimeTransportCommand,
) -> Result<Vec<u8>, RuntimeTransportError> {
    let mut frame = serde_json::to_vec(&serde_json::json!({
        "type": "command",
        "command_id": command.command_id,
        "generation": command.generation,
        "method": command.method,
        "payload": command.payload,
        "deadline_unix_ms": command.deadline_unix_ms,
    }))
    .map_err(|_| RuntimeTransportError::InvalidCommand)?;
    frame.push(b'\n');
    if frame.len() > MAX_RUNTIME_FRAME_BYTES {
        return Err(RuntimeTransportError::InvalidCommand);
    }
    Ok(frame)
}

fn write_frame(
    stdin: &mut std::process::ChildStdin,
    frame: &[u8],
) -> Result<(), RuntimeTransportError> {
    stdin.write_all(frame).map_err(|_| RuntimeTransportError::WriteFailed)?;
    stdin.flush().map_err(|_| RuntimeTransportError::WriteFailed)
}

fn validate_start_request(
    request: &ManagedRuntimeStartRequest,
) -> Result<(), RuntimeTransportError> {
    if request.session_id.trim().is_empty()
        || request.session_id.len() > 128
        || request.generation == 0
        || request.resume_metadata_json.as_deref().is_some_and(|metadata| {
            metadata.len() > 8 * 1024 || serde_json::from_str::<Value>(metadata).is_err()
        })
    {
        return Err(RuntimeTransportError::InvalidStartRequest);
    }
    Ok(())
}

fn valid_wire_fields(identity: &str, label: &str, payload: &Value) -> bool {
    !identity.trim().is_empty()
        && identity.len() <= 128
        && !label.trim().is_empty()
        && label.len() <= MAX_RUNTIME_METHOD_BYTES
        && serde_json::to_vec(payload).is_ok_and(|bytes| bytes.len() <= MAX_RUNTIME_FRAME_BYTES)
}

fn read_bounded_blocking_line<Reader>(
    reader: &mut Reader,
    max_bytes: usize,
) -> Result<Vec<u8>, RuntimeTransportError>
where
    Reader: BufRead,
{
    let mut line = Vec::new();
    let read = reader
        .take(u64::try_from(max_bytes.saturating_add(1)).unwrap_or(u64::MAX))
        .read_until(b'\n', &mut line)
        .map_err(|_| RuntimeTransportError::ReadFailed)?;
    if read == 0 || line.len() > max_bytes || !line.ends_with(b"\n") {
        return Err(RuntimeTransportError::MalformedFrame);
    }
    Ok(line)
}

fn issue_nonce() -> Result<String, RuntimeTransportError> {
    let mut random = [0_u8; 32];
    getrandom::fill(&mut random).map_err(|_| RuntimeTransportError::Unavailable)?;
    Ok(hex::encode(random))
}

fn map_try_send_error(error: TrySendError<ActorCommand>) -> RuntimeTransportError {
    match error {
        TrySendError::Full(_) => RuntimeTransportError::Backpressure,
        TrySendError::Disconnected(_) => RuntimeTransportError::Unavailable,
    }
}

fn now_unix_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| i64::try_from(duration.as_millis()).unwrap_or(i64::MAX))
        .unwrap_or(0)
}

fn sha256_hex(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

fn is_sha256(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn runtime_start_failure_reason(error: &RuntimeTransportError) -> &'static str {
    match error {
        RuntimeTransportError::SpawnFailed { .. } => "runtime.transport.spawn_failed",
        RuntimeTransportError::StdioUnavailable => "runtime.transport.stdio_unavailable",
        RuntimeTransportError::HandshakeTimedOut => "runtime.transport.handshake_timeout",
        RuntimeTransportError::HandshakeMismatch => "runtime.transport.handshake_mismatch",
        RuntimeTransportError::MalformedFrame => "runtime.transport.malformed_handshake",
        RuntimeTransportError::ReadFailed => "runtime.transport.handshake_read_failed",
        _ => "runtime.transport.start_failed",
    }
}

/// Fail-closed process transport error.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum RuntimeTransportError {
    #[error("managed runtime descriptor is invalid")]
    InvalidDescriptor,
    #[error("managed runtime start request is invalid")]
    InvalidStartRequest,
    #[error("managed runtime command is invalid")]
    InvalidCommand,
    #[error("managed runtime is already started")]
    AlreadyStarted,
    #[error("managed runtime has not started")]
    NotStarted,
    #[error("managed runtime generation is stale")]
    StaleGeneration { active: u64, observed: u64 },
    #[error("managed runtime process failed to start: {safe_message}")]
    SpawnFailed { safe_message: String },
    #[error("managed runtime stdio is unavailable")]
    StdioUnavailable,
    #[error("managed runtime handshake timed out")]
    HandshakeTimedOut,
    #[error("managed runtime handshake did not match the launch authority")]
    HandshakeMismatch,
    #[error("managed runtime emitted a malformed frame")]
    MalformedFrame,
    #[error("managed runtime frame read failed")]
    ReadFailed,
    #[error("managed runtime frame write failed")]
    WriteFailed,
    #[error("managed runtime command timed out")]
    CommandTimedOut,
    #[error("managed runtime command queue is full")]
    Backpressure,
    #[error("managed runtime is unavailable")]
    Unavailable,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wire_events_require_generation_and_monotonic_sequence() {
        let sequence = AtomicU64::new(0);
        let events_in_attempt = AtomicUsize::new(0);
        let attempt_open = AtomicBool::new(false);
        let accepted = validate_wire_event(
            RuntimeWireEvent::Accepted {
                command_id: "command-1".to_owned(),
                generation: 3,
                sequence: 1,
            },
            3,
            &sequence,
            &events_in_attempt,
            &attempt_open,
        )
        .expect("accepted");
        assert!(matches!(accepted, RuntimeTransportEvent::Accepted { .. }));
        let stale = validate_wire_event(
            RuntimeWireEvent::Terminal {
                command_id: "command-1".to_owned(),
                generation: 2,
                sequence: 2,
                outcome: "completed".to_owned(),
                payload: Value::Null,
            },
            3,
            &sequence,
            &events_in_attempt,
            &attempt_open,
        )
        .expect_err("stale");
        assert_eq!(stale, "runtime.transport.stale_generation");
    }

    #[test]
    fn descriptor_requires_exact_binary_and_capability_digest() {
        let descriptor = ManagedRuntimeDescriptor {
            runtime_id: "fixture".to_owned(),
            protocol_version: "fixture.v1".to_owned(),
            capability_digest: "a".repeat(64),
            executable: PathBuf::from("relative"),
            args: Vec::new(),
            cwd: PathBuf::from("relative"),
            env: BTreeMap::new(),
            handshake_timeout: Duration::from_secs(1),
            command_timeout: Duration::from_secs(1),
            lease_duration: Duration::from_secs(30),
        };
        assert_eq!(descriptor.validate(), Err(RuntimeTransportError::InvalidDescriptor));
    }
}
