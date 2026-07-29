//! Cross-platform Unix PTY and Windows ConPTY sessions for supervised tools.
//! Raw bytes remain local while model-visible text passes through an
//! incremental UTF-8 decoder and terminal-control sanitizer.

use std::collections::{BTreeMap, VecDeque};
use std::io::{self, Read, Write};
use std::path::PathBuf;
use std::sync::mpsc::{self, Receiver, SyncSender, TryRecvError};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use base64::{engine::general_purpose::STANDARD as BASE64_STANDARD, Engine as _};
use portable_pty::{
    native_pty_system, Child as PtyChild, ChildKiller as PtyChildKiller, CommandBuilder,
    ExitStatus as PtyExitStatus, MasterPty, PtySize,
};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use super::local_resource_governor::{
    LocalResourceGovernor, ResourceLeaseRequestV1, ResourceLeaseV1, ResourceServiceKind,
};

const PTY_SESSION_SCHEMA_VERSION: u32 = 1;
const PTY_RAW_CHUNK_SCHEMA_VERSION: u32 = 1;
const PTY_OUTPUT_CHANNEL_CAPACITY: usize = 128;
const PTY_READ_CHUNK_BYTES: usize = 8 * 1024;
const PTY_INPUT_CLOSE_GRACE: Duration = Duration::from_millis(100);
const PTY_POST_EXIT_DRAIN_GRACE: Duration = Duration::from_millis(250);
const PTY_OUTPUT_QUIET_GRACE: Duration = Duration::from_millis(50);
const PTY_FORCED_EXIT_GRACE: Duration = Duration::from_secs(5);
const MAX_PTY_ARGUMENTS: usize = 256;
const MAX_PTY_ARGUMENT_BYTES: usize = 16 * 1024;
const MAX_PTY_ENVIRONMENT_KEYS: usize = 256;
const MAX_OSC_CLASSIFIER_BYTES: usize = 256;
#[cfg(windows)]
const CONPTY_CURSOR_POSITION_QUERY: &[u8] = b"\x1b[6n";
#[cfg(windows)]
const CONPTY_CURSOR_POSITION_RESPONSE: &[u8] = b"\x1b[1;1R";

#[cfg(windows)]
#[derive(Debug, Default)]
struct ConPtyStartupHandshake {
    cursor_query_prefix_len: usize,
    cursor_query_seen: bool,
    failure: Option<String>,
}

#[cfg(windows)]
impl ConPtyStartupHandshake {
    fn observe_cursor_query(&mut self, bytes: &[u8]) -> bool {
        if self.cursor_query_seen {
            return false;
        }
        for byte in bytes {
            if *byte == CONPTY_CURSOR_POSITION_QUERY[self.cursor_query_prefix_len] {
                self.cursor_query_prefix_len = self.cursor_query_prefix_len.saturating_add(1);
            } else {
                self.cursor_query_prefix_len =
                    usize::from(*byte == CONPTY_CURSOR_POSITION_QUERY[0]);
            }
            if self.cursor_query_prefix_len == CONPTY_CURSOR_POSITION_QUERY.len() {
                self.cursor_query_prefix_len = 0;
                self.cursor_query_seen = true;
                return true;
            }
        }
        false
    }
}

/// Native terminal implementation selected for the host.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PtyBackendKind {
    /// POSIX pseudoterminal with a controlling session and process group.
    UnixPty,
    /// Windows pseudoconsole bound to a kill-on-close Job Object.
    WindowsConPty,
}

/// Terminal dimensions propagated to the child.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TerminalSizeV1 {
    /// Character rows.
    pub rows: u16,
    /// Character columns.
    pub cols: u16,
    /// Optional pixel width.
    pub pixel_width: u16,
    /// Optional pixel height.
    pub pixel_height: u16,
}

impl Default for TerminalSizeV1 {
    fn default() -> Self {
        Self { rows: 24, cols: 80, pixel_width: 0, pixel_height: 0 }
    }
}

impl From<TerminalSizeV1> for PtySize {
    fn from(value: TerminalSizeV1) -> Self {
        Self {
            rows: value.rows,
            cols: value.cols,
            pixel_width: value.pixel_width,
            pixel_height: value.pixel_height,
        }
    }
}

impl From<PtySize> for TerminalSizeV1 {
    fn from(value: PtySize) -> Self {
        Self {
            rows: value.rows,
            cols: value.cols,
            pixel_width: value.pixel_width,
            pixel_height: value.pixel_height,
        }
    }
}

/// Exact native terminal launch policy.
#[derive(Debug, Clone)]
pub struct PtyLaunchSpec {
    /// Absolute trusted executable path.
    pub executable: PathBuf,
    /// Bounded argument vector passed without a shell.
    pub args: Vec<String>,
    /// Absolute existing working directory.
    pub cwd: PathBuf,
    /// Explicit environment after inherited values are cleared.
    pub env: BTreeMap<String, String>,
    /// Initial terminal dimensions.
    pub size: TerminalSizeV1,
    /// Resident raw-byte spool limit.
    pub max_raw_bytes: usize,
    /// Resident raw chunk limit.
    pub max_raw_chunks: usize,
}

/// Durable metadata for a live native terminal.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PtySessionDescriptorV1 {
    /// Descriptor schema version.
    pub schema_version: u32,
    /// Host-issued terminal identity.
    pub pty_session_id: String,
    /// Owner allowed to mutate this terminal session.
    pub owner_id: String,
    /// Owner generation used to reject stale operations.
    pub owner_generation: u64,
    /// Native backend kind.
    pub backend: PtyBackendKind,
    /// Direct child PID; cleanup also retains a stronger group or Job authority.
    pub pid: u32,
    /// Current terminal dimensions.
    pub size: TerminalSizeV1,
    /// Session creation timestamp.
    pub created_at_unix_ms: i64,
    /// Most recently issued raw output cursor.
    pub next_cursor: u64,
    /// Whether resident raw output was evicted.
    pub raw_output_truncated: bool,
    /// True only while the native child and cleanup authority remain live.
    pub pty_active: bool,
}

/// Allowlisted special key accepted by terminal input operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TerminalKeyV1 {
    /// Horizontal tab.
    Tab,
    /// Backspace.
    Backspace,
    /// Escape.
    Escape,
    /// Up arrow.
    ArrowUp,
    /// Down arrow.
    ArrowDown,
    /// Left arrow.
    ArrowLeft,
    /// Right arrow.
    ArrowRight,
}

/// Bounded terminal input action.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TerminalInputActionV1 {
    /// Printable UTF-8 without control characters.
    WriteText(String),
    /// One allowlisted special key.
    SendKey(TerminalKeyV1),
    /// Submit the current line.
    Submit,
    /// Paste bounded UTF-8 using bracketed-paste markers.
    BracketedPaste(String),
    /// Close the terminal input stream.
    EndOfFile,
    /// Interrupt the foreground terminal job.
    Interrupt,
}

/// Owner-fenced terminal input request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TerminalInputRequestV1 {
    /// Exact terminal identity.
    pub pty_session_id: String,
    /// Exact owner identity.
    pub owner_id: String,
    /// Exact owner generation.
    pub owner_generation: u64,
    /// Input action.
    pub action: TerminalInputActionV1,
}

/// Owner-fenced terminal resize request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TerminalResizeRequestV1 {
    /// Exact terminal identity.
    pub pty_session_id: String,
    /// Exact owner identity.
    pub owner_id: String,
    /// Exact owner generation.
    pub owner_generation: u64,
    /// New dimensions.
    pub size: TerminalSizeV1,
}

/// Raw local output chunk from a PTY.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PtyRawChunkV1 {
    /// Chunk schema version.
    pub schema_version: u32,
    /// Monotonic terminal-local cursor.
    pub sequence: u64,
    /// Capture timestamp.
    pub captured_at_unix_ms: i64,
    /// Lossless raw bytes.
    pub bytes_base64: String,
}

impl PtyRawChunkV1 {
    fn from_bytes(sequence: u64, bytes: &[u8]) -> Self {
        Self {
            schema_version: PTY_RAW_CHUNK_SCHEMA_VERSION,
            sequence,
            captured_at_unix_ms: unix_time_ms(),
            bytes_base64: BASE64_STANDARD.encode(bytes),
        }
    }

    fn decoded_len(&self) -> usize {
        self.bytes_base64.len().saturating_mul(3) / 4
    }
}

/// Counts terminal controls removed from display-safe output.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TerminalSanitizationReportV1 {
    /// OSC 52 clipboard operations removed.
    pub clipboard_sequences_removed: u64,
    /// OSC title operations removed.
    pub title_sequences_removed: u64,
    /// Other OSC operations removed.
    pub other_osc_sequences_removed: u64,
    /// DCS strings removed.
    pub dcs_sequences_removed: u64,
    /// APC strings removed.
    pub apc_sequences_removed: u64,
    /// CSI display or terminal-control sequences removed.
    pub csi_sequences_removed: u64,
    /// Other escape-prefixed controls removed.
    pub other_escape_sequences_removed: u64,
    /// Invalid UTF-8 segments replaced.
    pub invalid_utf8_segments: u64,
    /// Non-display C0 control bytes removed.
    pub control_bytes_removed: u64,
}

/// Bounded terminal output page with raw and safe projections separated.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PtyOutputPageV1 {
    /// Raw chunks retained after the requested cursor.
    pub raw_chunks: Vec<PtyRawChunkV1>,
    /// Incremental display-safe text observed since the prior local drain.
    pub safe_text: String,
    /// Latest issued raw cursor.
    pub next_cursor: u64,
    /// Whether the requested cursor preceded retained raw history.
    pub cursor_reset: bool,
    /// Cumulative sanitizer counters.
    pub sanitization: TerminalSanitizationReportV1,
    /// Whether resident raw output was evicted.
    pub truncated: bool,
}

/// Settled direct-child outcome after PTY output closes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PtyExitOutcomeV1 {
    /// Portable exit code.
    pub exit_code: u32,
    /// Optional platform signal label.
    pub signal: Option<String>,
    /// Whether descendant cleanup authority completed.
    pub cleanup_verified: bool,
}

/// Native terminal creation or operation failure.
#[derive(Debug, Error)]
pub enum PtyBackendError {
    /// Launch policy is malformed or unbounded.
    #[error("PTY launch policy is invalid: {0}")]
    InvalidLaunch(String),
    /// The native platform backend cannot be created.
    #[error("native PTY backend is unavailable: {0}")]
    BackendUnavailable(String),
    /// Native process creation failed.
    #[error("PTY child spawn failed: {0}")]
    Spawn(String),
    /// Input or output handling failed.
    #[error("PTY I/O failed: {0}")]
    Io(String),
    /// Resize was rejected by the native backend.
    #[error("PTY resize failed: {0}")]
    Resize(String),
    /// A wait deadline elapsed while the child was still live or draining.
    #[error("PTY wait timed out")]
    WaitTimeout,
    /// The child exited before returning a usable process identity.
    #[error("PTY child process identity is unavailable")]
    MissingProcessIdentity,
    /// Exact process-tree cleanup could not be verified.
    #[error("PTY process-tree cleanup failed: {0}")]
    Cleanup(String),
    /// Session identity, owner, or generation does not authorize the operation.
    #[error("PTY operation is not authorized for this owner generation")]
    Unauthorized,
}

/// Common native-terminal semantics used by terminal tools and coding runtime.
pub trait PtyBackend {
    /// Returns live terminal metadata.
    fn session_descriptor(&mut self) -> PtySessionDescriptorV1;
    /// Applies one owner-fenced input action.
    fn apply_input(&mut self, request: TerminalInputRequestV1) -> Result<(), PtyBackendError>;
    /// Applies one owner-fenced resize.
    fn apply_resize(&mut self, request: TerminalResizeRequestV1) -> Result<(), PtyBackendError>;
    /// Reads bounded raw and display-safe output.
    fn poll_output(&mut self, after_cursor: Option<u64>, max_chunks: usize) -> PtyOutputPageV1;
    /// Waits for exit, output drain, and cleanup.
    fn wait_for_exit(&mut self, timeout: Duration) -> Result<PtyExitOutcomeV1, PtyBackendError>;
    /// Terminates the exact owned process tree.
    fn terminate_tree(&mut self) -> Result<(), PtyBackendError>;
}

enum PtyOutputEvent {
    Bytes(Vec<u8>),
    Closed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EscapeStringKind {
    Osc,
    Dcs,
    Apc,
}

enum SanitizerMode {
    Normal,
    Escape,
    Csi,
    EscapeString { kind: EscapeStringKind, classifier: Vec<u8>, saw_escape: bool },
}

struct TerminalTextSanitizer {
    mode: SanitizerMode,
    utf8_pending: Vec<u8>,
    report: TerminalSanitizationReportV1,
}

impl Default for TerminalTextSanitizer {
    fn default() -> Self {
        Self {
            mode: SanitizerMode::Normal,
            utf8_pending: Vec::new(),
            report: TerminalSanitizationReportV1::default(),
        }
    }
}

impl TerminalTextSanitizer {
    fn project(&mut self, bytes: &[u8]) -> String {
        let mut visible = Vec::with_capacity(bytes.len());
        for byte in bytes {
            match &mut self.mode {
                SanitizerMode::Normal if *byte == 0x1b => {
                    self.mode = SanitizerMode::Escape;
                }
                SanitizerMode::Normal if display_control_allowed(*byte) || *byte >= 0x20 => {
                    visible.push(*byte);
                }
                SanitizerMode::Normal => {
                    self.report.control_bytes_removed =
                        self.report.control_bytes_removed.saturating_add(1);
                }
                SanitizerMode::Escape => match *byte {
                    b'[' => {
                        self.mode = SanitizerMode::Csi;
                    }
                    b']' => {
                        self.mode = SanitizerMode::EscapeString {
                            kind: EscapeStringKind::Osc,
                            classifier: Vec::new(),
                            saw_escape: false,
                        };
                    }
                    b'P' => {
                        self.mode = SanitizerMode::EscapeString {
                            kind: EscapeStringKind::Dcs,
                            classifier: Vec::new(),
                            saw_escape: false,
                        };
                    }
                    b'_' => {
                        self.mode = SanitizerMode::EscapeString {
                            kind: EscapeStringKind::Apc,
                            classifier: Vec::new(),
                            saw_escape: false,
                        };
                    }
                    _ => {
                        self.report.other_escape_sequences_removed =
                            self.report.other_escape_sequences_removed.saturating_add(1);
                        self.mode = SanitizerMode::Normal;
                    }
                },
                SanitizerMode::Csi => {
                    if (0x40..=0x7e).contains(byte) {
                        self.report.csi_sequences_removed =
                            self.report.csi_sequences_removed.saturating_add(1);
                        self.mode = SanitizerMode::Normal;
                    }
                }
                SanitizerMode::EscapeString { kind, classifier, saw_escape } => {
                    let terminated = *byte == 0x07 || (*saw_escape && *byte == b'\\');
                    if terminated {
                        record_removed_sequence(&mut self.report, *kind, classifier.as_slice());
                        self.mode = SanitizerMode::Normal;
                    } else {
                        *saw_escape = *byte == 0x1b;
                        if classifier.len() < MAX_OSC_CLASSIFIER_BYTES && *byte != 0x1b {
                            classifier.push(*byte);
                        }
                    }
                }
            }
        }
        self.decode_utf8(visible.as_slice())
    }

    fn decode_utf8(&mut self, bytes: &[u8]) -> String {
        self.utf8_pending.extend_from_slice(bytes);
        let mut projected = String::new();
        loop {
            match std::str::from_utf8(self.utf8_pending.as_slice()) {
                Ok(valid) => {
                    projected.push_str(valid);
                    self.utf8_pending.clear();
                    break;
                }
                Err(error) => {
                    let valid_up_to = error.valid_up_to();
                    if valid_up_to > 0 {
                        let valid = &self.utf8_pending[..valid_up_to];
                        if let Ok(valid) = std::str::from_utf8(valid) {
                            projected.push_str(valid);
                        }
                        self.utf8_pending.drain(..valid_up_to);
                    }
                    let Some(error_len) = error.error_len() else {
                        break;
                    };
                    projected.push('\u{fffd}');
                    self.report.invalid_utf8_segments =
                        self.report.invalid_utf8_segments.saturating_add(1);
                    let remove = error_len.min(self.utf8_pending.len());
                    self.utf8_pending.drain(..remove);
                }
            }
        }
        projected
    }
}

fn display_control_allowed(byte: u8) -> bool {
    matches!(byte, b'\t' | b'\n' | b'\r' | 0x08)
}

fn record_removed_sequence(
    report: &mut TerminalSanitizationReportV1,
    kind: EscapeStringKind,
    classifier: &[u8],
) {
    match kind {
        EscapeStringKind::Osc if classifier.starts_with(b"52;") => {
            report.clipboard_sequences_removed =
                report.clipboard_sequences_removed.saturating_add(1);
        }
        EscapeStringKind::Osc
            if classifier.starts_with(b"0;")
                || classifier.starts_with(b"1;")
                || classifier.starts_with(b"2;") =>
        {
            report.title_sequences_removed = report.title_sequences_removed.saturating_add(1);
        }
        EscapeStringKind::Osc => {
            report.other_osc_sequences_removed =
                report.other_osc_sequences_removed.saturating_add(1);
        }
        EscapeStringKind::Dcs => {
            report.dcs_sequences_removed = report.dcs_sequences_removed.saturating_add(1);
        }
        EscapeStringKind::Apc => {
            report.apc_sequences_removed = report.apc_sequences_removed.saturating_add(1);
        }
    }
}

/// Live native terminal owned by one synchronous supervisor actor.
pub struct NativePtySession {
    descriptor: PtySessionDescriptorV1,
    master: Box<dyn MasterPty + Send>,
    child_killer: Box<dyn PtyChildKiller + Send + Sync>,
    exit_rx: Receiver<Result<PtyExitStatus, String>>,
    exit_status: Option<PtyExitStatus>,
    exit_observed_at: Option<Instant>,
    writer: Option<Box<dyn Write + Send>>,
    output_rx: Receiver<PtyOutputEvent>,
    raw_chunks: VecDeque<PtyRawChunkV1>,
    raw_bytes: usize,
    max_raw_bytes: usize,
    max_raw_chunks: usize,
    reader_closed: bool,
    last_output_at: Instant,
    pending_safe_text: String,
    sanitizer: TerminalTextSanitizer,
    settled: bool,
    resource_governor: LocalResourceGovernor,
    resource_lease: Option<ResourceLeaseV1>,
    #[cfg(windows)]
    startup_handshake: ConPtyStartupHandshake,
    #[cfg(unix)]
    process_group_id: i32,
    #[cfg(windows)]
    job: WindowsPtyJob,
}

impl NativePtySession {
    /// Spawns a real Unix PTY or Windows ConPTY and establishes tree cleanup authority.
    ///
    /// # Errors
    /// Returns an error when launch policy, PTY creation, process creation, or
    /// process-tree ownership cannot be established before acknowledgement.
    pub fn spawn(
        spec: PtyLaunchSpec,
        resource_governor: LocalResourceGovernor,
        resource_request: ResourceLeaseRequestV1,
    ) -> Result<Self, PtyBackendError> {
        validate_launch(&spec)?;
        if resource_request.service != ResourceServiceKind::Pty
            || resource_request.requested.processes == 0
        {
            return Err(PtyBackendError::InvalidLaunch(
                "PTY resource request must reserve a process under the PTY service".to_owned(),
            ));
        }
        let resource_lease = resource_governor
            .acquire(resource_request)
            .map_err(|error| PtyBackendError::Spawn(error.to_string()))?;
        let result = Self::spawn_admitted(spec, resource_governor.clone(), resource_lease.clone());
        if result.is_err() {
            let _ = resource_governor
                .release(resource_lease.lease_id.as_str(), resource_lease.generation);
        }
        result
    }

    fn spawn_admitted(
        spec: PtyLaunchSpec,
        resource_governor: LocalResourceGovernor,
        resource_lease: ResourceLeaseV1,
    ) -> Result<Self, PtyBackendError> {
        let pty_system = native_pty_system();
        let pair = pty_system
            .openpty(spec.size.into())
            .map_err(|error| PtyBackendError::BackendUnavailable(error.to_string()))?;
        let mut command = CommandBuilder::new(spec.executable.as_os_str());
        command.args(spec.args.iter());
        command.cwd(spec.cwd.as_os_str());
        command.env_clear();
        for (key, value) in &spec.env {
            command.env(key, value);
        }
        let child = pair
            .slave
            .spawn_command(command)
            .map_err(|error| PtyBackendError::Spawn(error.to_string()))?;
        drop(pair.slave);
        let pid = child.process_id().ok_or(PtyBackendError::MissingProcessIdentity)?;
        #[cfg(unix)]
        let process_group_id = pair
            .master
            .process_group_leader()
            .ok_or_else(|| PtyBackendError::Spawn("PTY process group is unavailable".to_owned()))?;
        #[cfg(windows)]
        let job = WindowsPtyJob::bind(child.as_ref(), pid)?;
        let reader = pair
            .master
            .try_clone_reader()
            .map_err(|error| PtyBackendError::Io(error.to_string()))?;
        let writer =
            pair.master.take_writer().map_err(|error| PtyBackendError::Io(error.to_string()))?;
        let (output_tx, output_rx) = mpsc::sync_channel(PTY_OUTPUT_CHANNEL_CAPACITY);
        spawn_output_reader(reader, output_tx)?;
        let child_killer = child.clone_killer();
        let (exit_tx, exit_rx) = mpsc::sync_channel(1);
        spawn_child_waiter(child, exit_tx)?;
        let descriptor = PtySessionDescriptorV1 {
            schema_version: PTY_SESSION_SCHEMA_VERSION,
            pty_session_id: format!("pty_{}", ulid::Ulid::new()),
            owner_id: resource_lease.owner_id.clone(),
            owner_generation: resource_lease.generation,
            backend: native_backend_kind(),
            pid,
            size: spec.size,
            created_at_unix_ms: unix_time_ms(),
            next_cursor: 0,
            raw_output_truncated: false,
            pty_active: true,
        };
        Ok(Self {
            descriptor,
            master: pair.master,
            child_killer,
            exit_rx,
            exit_status: None,
            exit_observed_at: None,
            writer: Some(writer),
            output_rx,
            raw_chunks: VecDeque::new(),
            raw_bytes: 0,
            max_raw_bytes: spec.max_raw_bytes,
            max_raw_chunks: spec.max_raw_chunks,
            reader_closed: false,
            last_output_at: Instant::now(),
            pending_safe_text: String::new(),
            sanitizer: TerminalTextSanitizer::default(),
            settled: false,
            resource_governor,
            resource_lease: Some(resource_lease),
            #[cfg(windows)]
            startup_handshake: ConPtyStartupHandshake::default(),
            #[cfg(unix)]
            process_group_id,
            #[cfg(windows)]
            job,
        })
    }

    /// Returns current terminal metadata after draining available output.
    #[must_use]
    pub fn descriptor(&mut self) -> PtySessionDescriptorV1 {
        self.drain_output();
        self.descriptor.clone()
    }

    /// Writes bytes through the native terminal input stream.
    ///
    /// # Errors
    /// Returns an error after EOF or when the native writer rejects the bytes.
    pub fn write(&mut self, bytes: &[u8]) -> Result<(), PtyBackendError> {
        if self.settled {
            return Err(PtyBackendError::Io("terminal session is already settled".to_owned()));
        }
        if bytes.is_empty() || bytes.len() > PTY_READ_CHUNK_BYTES {
            return Err(PtyBackendError::Io(
                "terminal writes must contain 1..=8192 bytes".to_owned(),
            ));
        }
        let writer = self
            .writer
            .as_mut()
            .ok_or_else(|| PtyBackendError::Io("terminal input is already closed".to_owned()))?;
        writer
            .write_all(bytes)
            .and_then(|()| writer.flush())
            .map_err(|error| PtyBackendError::Io(error.to_string()))
    }

    /// Sends the platform's terminal end-of-file condition.
    ///
    /// # Errors
    /// Returns an error only when the session is already settled.
    pub fn send_eof(&mut self) -> Result<(), PtyBackendError> {
        if self.settled {
            return Err(PtyBackendError::Io("terminal session is already settled".to_owned()));
        }
        #[cfg(windows)]
        {
            // Ctrl+Z followed by Enter is the Windows console EOF gesture.
            // `wait` closes the transport only after the PTY host handshake.
            self.write(&[0x1a, b'\r', b'\n'])
        }
        #[cfg(not(windows))]
        {
            self.writer = None;
            Ok(())
        }
    }

    /// Delivers an interactive interrupt to the terminal foreground process.
    ///
    /// # Errors
    /// Returns an error when the native signal or ConPTY input write fails.
    pub fn interrupt(&mut self) -> Result<(), PtyBackendError> {
        #[cfg(unix)]
        {
            // SAFETY: the PTY backend returned a positive foreground process
            // group leader owned by this session; a negative pid targets that group.
            let result = unsafe { libc::kill(-self.process_group_id, libc::SIGINT) };
            if result != 0 {
                return Err(PtyBackendError::Io(io::Error::last_os_error().to_string()));
            }
            Ok(())
        }
        #[cfg(windows)]
        {
            // The Windows host intentionally ignores transport-close Ctrl+C,
            // so an explicit interrupt terminates the owned foreground Job.
            self.writer = None;
            self.job.terminate()
        }
    }

    /// Propagates a terminal resize to the child.
    ///
    /// # Errors
    /// Returns an error for zero dimensions or native resize failure.
    pub fn resize(&mut self, size: TerminalSizeV1) -> Result<(), PtyBackendError> {
        if self.settled {
            return Err(PtyBackendError::Resize("terminal session is already settled".to_owned()));
        }
        if size.rows == 0 || size.cols == 0 {
            return Err(PtyBackendError::Resize(
                "terminal rows and columns must be non-zero".to_owned(),
            ));
        }
        self.master
            .resize(size.into())
            .map_err(|error| PtyBackendError::Resize(error.to_string()))?;
        self.descriptor.size = size;
        Ok(())
    }

    /// Reads retained raw chunks and newly projected display-safe text.
    #[must_use]
    pub fn read_output(&mut self, after_cursor: Option<u64>, max_chunks: usize) -> PtyOutputPageV1 {
        self.drain_output();
        let requested = after_cursor.unwrap_or(0);
        let first = self
            .raw_chunks
            .front()
            .map_or(self.descriptor.next_cursor.saturating_add(1), |chunk| chunk.sequence);
        let cursor_reset = requested.saturating_add(1) < first;
        let effective = if cursor_reset { first.saturating_sub(1) } else { requested };
        let raw_chunks = self
            .raw_chunks
            .iter()
            .filter(|chunk| chunk.sequence > effective)
            .take(max_chunks.max(1))
            .cloned()
            .collect();
        PtyOutputPageV1 {
            raw_chunks,
            safe_text: std::mem::take(&mut self.pending_safe_text),
            next_cursor: self.descriptor.next_cursor,
            cursor_reset,
            sanitization: self.sanitizer.report.clone(),
            truncated: self.descriptor.raw_output_truncated,
        }
    }

    /// Waits for direct-child exit, output EOF, and descendant cleanup.
    ///
    /// # Errors
    /// Returns an error when the deadline elapses or cleanup cannot be verified.
    pub fn wait(&mut self, timeout: Duration) -> Result<PtyExitOutcomeV1, PtyBackendError> {
        let input_close_at = Instant::now() + PTY_INPUT_CLOSE_GRACE;
        let deadline = Instant::now() + timeout;
        loop {
            if let Some(outcome) = self.try_settle()? {
                return Ok(outcome);
            }
            if self.writer.is_some()
                && (self.exit_status.is_some() || Instant::now() >= input_close_at)
            {
                // Releasing the final input writer is required for PTY EOF.
                // A short launch grace avoids closing a fresh ConPTY before
                // its console process has had a chance to attach.
                self.writer = None;
            }
            if Instant::now() >= deadline {
                return Err(PtyBackendError::WaitTimeout);
            }
            thread::sleep(Duration::from_millis(10));
        }
    }

    /// Polls child exit and bounded output drain without closing terminal input.
    ///
    /// This is the non-blocking primitive used by a host actor that must keep
    /// accepting terminal input and resize commands while independently
    /// observing completion.
    ///
    /// # Errors
    /// Returns an error when the child waiter or exact cleanup authority fails.
    pub fn try_settle(&mut self) -> Result<Option<PtyExitOutcomeV1>, PtyBackendError> {
        if self.settled {
            return Ok(None);
        }
        self.drain_output();
        #[cfg(windows)]
        if let Some(error) = self.startup_handshake.failure.as_ref() {
            return Err(PtyBackendError::Io(error.clone()));
        }
        if self.exit_status.is_none() {
            self.poll_child_exit()?;
        }
        let Some(status) = self.exit_status.clone() else {
            return Ok(None);
        };
        self.writer = None;
        let bounded_drain_complete = self.exit_observed_at.is_some_and(|observed_at| {
            observed_at.elapsed() >= PTY_POST_EXIT_DRAIN_GRACE
                && self.last_output_at.elapsed() >= PTY_OUTPUT_QUIET_GRACE
        });
        // ConPTY can retain its output pipe until the pseudoconsole handle
        // closes. A bounded quiet drain avoids waiting forever for that EOF.
        if !self.reader_closed && !bounded_drain_complete {
            return Ok(None);
        }
        let cleanup_verified = self.cleanup_tree()?;
        self.settled = true;
        self.descriptor.pty_active = false;
        Ok(Some(PtyExitOutcomeV1 {
            exit_code: status.exit_code(),
            signal: status.signal().map(str::to_owned),
            cleanup_verified,
        }))
    }

    /// Forcefully terminates the owned terminal process tree.
    ///
    /// # Errors
    /// Returns an error when exact process-tree cleanup cannot be verified.
    pub fn terminate(&mut self) -> Result<(), PtyBackendError> {
        self.terminate_with_outcome()?;
        Ok(())
    }

    /// Forcefully terminates the tree and returns the observed terminal outcome.
    ///
    /// # Errors
    /// Returns an error when exact cleanup or exit observation fails.
    pub fn terminate_with_outcome(&mut self) -> Result<PtyExitOutcomeV1, PtyBackendError> {
        let cleanup_verified = self.cleanup_tree()?;
        self.settled = true;
        self.descriptor.pty_active = false;
        let status = self.exit_status.clone().ok_or_else(|| {
            PtyBackendError::Cleanup("PTY exit status is unavailable after termination".to_owned())
        })?;
        Ok(PtyExitOutcomeV1 {
            exit_code: status.exit_code(),
            signal: status.signal().map(str::to_owned),
            cleanup_verified,
        })
    }

    fn authorize(
        &self,
        pty_session_id: &str,
        owner_id: &str,
        owner_generation: u64,
    ) -> Result<(), PtyBackendError> {
        if self.descriptor.pty_session_id != pty_session_id
            || self.descriptor.owner_id != owner_id
            || self.descriptor.owner_generation != owner_generation
            || self.settled
        {
            return Err(PtyBackendError::Unauthorized);
        }
        Ok(())
    }

    fn drain_output(&mut self) {
        loop {
            match self.output_rx.try_recv() {
                Ok(PtyOutputEvent::Bytes(bytes)) => self.append_output(bytes.as_slice()),
                Ok(PtyOutputEvent::Closed) | Err(TryRecvError::Disconnected) => {
                    self.reader_closed = true;
                    break;
                }
                Err(TryRecvError::Empty) => break,
            }
        }
    }

    fn append_output(&mut self, bytes: &[u8]) {
        #[cfg(windows)]
        self.service_conpty_startup_handshake(bytes);
        self.last_output_at = Instant::now();
        let sequence = self.descriptor.next_cursor.saturating_add(1);
        self.descriptor.next_cursor = sequence;
        self.raw_bytes = self.raw_bytes.saturating_add(bytes.len());
        self.raw_chunks.push_back(PtyRawChunkV1::from_bytes(sequence, bytes));
        while self.raw_chunks.len() > self.max_raw_chunks || self.raw_bytes > self.max_raw_bytes {
            if let Some(evicted) = self.raw_chunks.pop_front() {
                self.raw_bytes = self.raw_bytes.saturating_sub(evicted.decoded_len());
                self.descriptor.raw_output_truncated = true;
            } else {
                break;
            }
        }
        let safe_text = self.sanitizer.project(bytes);
        self.pending_safe_text.push_str(safe_text.as_str());
    }

    #[cfg(windows)]
    fn service_conpty_startup_handshake(&mut self, bytes: &[u8]) {
        if !self.startup_handshake.observe_cursor_query(bytes) {
            return;
        }
        // portable-pty enables ConPTY cursor inheritance, which blocks client startup until the
        // terminal answers this device-status query. Palyra has no inherited cursor, so origin is
        // the deterministic response and the control exchange stays outside user-visible text.
        let response = self
            .writer
            .as_mut()
            .ok_or_else(|| "ConPTY requested cursor inheritance after input closed".to_owned())
            .and_then(|writer| {
                writer
                    .write_all(CONPTY_CURSOR_POSITION_RESPONSE)
                    .and_then(|()| writer.flush())
                    .map_err(|error| {
                        format!("failed to complete ConPTY cursor inheritance: {error}")
                    })
            });
        if let Err(error) = response {
            self.startup_handshake.failure = Some(error);
        }
    }

    fn poll_child_exit(&mut self) -> Result<(), PtyBackendError> {
        match self.exit_rx.try_recv() {
            Ok(Ok(status)) => {
                self.exit_status = Some(status);
                self.exit_observed_at = Some(Instant::now());
                Ok(())
            }
            Ok(Err(error)) => Err(PtyBackendError::Io(error)),
            Err(TryRecvError::Empty) => Ok(()),
            Err(TryRecvError::Disconnected) => Err(PtyBackendError::Io(
                "PTY child waiter disconnected before reporting an exit status".to_owned(),
            )),
        }
    }

    fn wait_for_forced_exit(&mut self) -> Result<(), PtyBackendError> {
        if self.exit_status.is_some() {
            return Ok(());
        }
        match self.exit_rx.recv_timeout(PTY_FORCED_EXIT_GRACE) {
            Ok(Ok(status)) => {
                self.exit_status = Some(status);
                self.exit_observed_at = Some(Instant::now());
                Ok(())
            }
            Ok(Err(error)) => Err(PtyBackendError::Cleanup(error)),
            Err(mpsc::RecvTimeoutError::Timeout) => Err(PtyBackendError::Cleanup(
                "PTY child did not exit after exact tree termination".to_owned(),
            )),
            Err(mpsc::RecvTimeoutError::Disconnected) => Err(PtyBackendError::Cleanup(
                "PTY child waiter disconnected before cleanup verification".to_owned(),
            )),
        }
    }

    fn cleanup_tree(&mut self) -> Result<bool, PtyBackendError> {
        self.writer = None;
        #[cfg(unix)]
        {
            // SAFETY: this session exclusively owns the process group returned
            // by the PTY backend; SIGKILL is used only during terminal cleanup.
            let result = unsafe { libc::kill(-self.process_group_id, libc::SIGKILL) };
            if result != 0 {
                let error = io::Error::last_os_error();
                if error.raw_os_error() != Some(libc::ESRCH) {
                    return Err(PtyBackendError::Cleanup(error.to_string()));
                }
            }
        }
        #[cfg(windows)]
        self.job.terminate()?;
        let _ = self.child_killer.kill();
        self.wait_for_forced_exit()?;
        if let Some(resource_lease) = self.resource_lease.take() {
            self.resource_governor
                .release(resource_lease.lease_id.as_str(), resource_lease.generation)
                .map_err(|error| PtyBackendError::Cleanup(error.to_string()))?;
        }
        Ok(true)
    }
}

impl PtyBackend for NativePtySession {
    fn session_descriptor(&mut self) -> PtySessionDescriptorV1 {
        self.descriptor()
    }

    fn apply_input(&mut self, request: TerminalInputRequestV1) -> Result<(), PtyBackendError> {
        self.authorize(
            request.pty_session_id.as_str(),
            request.owner_id.as_str(),
            request.owner_generation,
        )?;
        match request.action {
            TerminalInputActionV1::WriteText(text) => {
                validate_terminal_text(text.as_str(), false)?;
                self.write(text.as_bytes())
            }
            TerminalInputActionV1::SendKey(key) => self.write(key_bytes(key)),
            TerminalInputActionV1::Submit => {
                #[cfg(windows)]
                const SUBMIT: &[u8] = b"\r\n";
                #[cfg(not(windows))]
                const SUBMIT: &[u8] = b"\n";
                self.write(SUBMIT)
            }
            TerminalInputActionV1::BracketedPaste(text) => {
                validate_terminal_text(text.as_str(), true)?;
                let mut bytes = Vec::with_capacity(text.len().saturating_add(12));
                bytes.extend_from_slice(b"\x1b[200~");
                bytes.extend_from_slice(text.as_bytes());
                bytes.extend_from_slice(b"\x1b[201~");
                self.write(bytes.as_slice())
            }
            TerminalInputActionV1::EndOfFile => self.send_eof(),
            TerminalInputActionV1::Interrupt => self.interrupt(),
        }
    }

    fn apply_resize(&mut self, request: TerminalResizeRequestV1) -> Result<(), PtyBackendError> {
        self.authorize(
            request.pty_session_id.as_str(),
            request.owner_id.as_str(),
            request.owner_generation,
        )?;
        self.resize(request.size)
    }

    fn poll_output(&mut self, after_cursor: Option<u64>, max_chunks: usize) -> PtyOutputPageV1 {
        self.read_output(after_cursor, max_chunks)
    }

    fn wait_for_exit(&mut self, timeout: Duration) -> Result<PtyExitOutcomeV1, PtyBackendError> {
        self.wait(timeout)
    }

    fn terminate_tree(&mut self) -> Result<(), PtyBackendError> {
        self.terminate()
    }
}

fn validate_terminal_text(text: &str, allow_layout_controls: bool) -> Result<(), PtyBackendError> {
    if text.is_empty()
        || text.len() > PTY_READ_CHUNK_BYTES.saturating_sub(12)
        || text.chars().any(|character| {
            character == '\u{1b}'
                || (character.is_control()
                    && !(allow_layout_controls && matches!(character, '\n' | '\r' | '\t')))
        })
    {
        return Err(PtyBackendError::Io(
            "terminal text must be bounded UTF-8 without escape or disallowed control characters"
                .to_owned(),
        ));
    }
    Ok(())
}

const fn key_bytes(key: TerminalKeyV1) -> &'static [u8] {
    match key {
        TerminalKeyV1::Tab => b"\t",
        TerminalKeyV1::Backspace => b"\x7f",
        TerminalKeyV1::Escape => b"\x1b",
        TerminalKeyV1::ArrowUp => b"\x1b[A",
        TerminalKeyV1::ArrowDown => b"\x1b[B",
        TerminalKeyV1::ArrowLeft => b"\x1b[D",
        TerminalKeyV1::ArrowRight => b"\x1b[C",
    }
}

impl Drop for NativePtySession {
    fn drop(&mut self) {
        if !self.settled {
            let _ = self.cleanup_tree();
        }
    }
}

fn validate_launch(spec: &PtyLaunchSpec) -> Result<(), PtyBackendError> {
    if !spec.executable.is_absolute()
        || !spec.executable.is_file()
        || !spec.cwd.is_absolute()
        || !spec.cwd.is_dir()
        || spec.args.len() > MAX_PTY_ARGUMENTS
        || spec.args.iter().any(|arg| arg.len() > MAX_PTY_ARGUMENT_BYTES)
        || spec.env.len() > MAX_PTY_ENVIRONMENT_KEYS
        || spec.size.rows == 0
        || spec.size.cols == 0
        || spec.max_raw_bytes == 0
        || spec.max_raw_chunks == 0
    {
        return Err(PtyBackendError::InvalidLaunch(
            "paths, dimensions, arguments, environment, and spool limits must satisfy policy"
                .to_owned(),
        ));
    }
    Ok(())
}

fn spawn_output_reader(
    mut reader: Box<dyn Read + Send>,
    output_tx: SyncSender<PtyOutputEvent>,
) -> Result<(), PtyBackendError> {
    thread::Builder::new()
        .name("palyra-pty-output".to_owned())
        .spawn(move || {
            let mut buffer = vec![0_u8; PTY_READ_CHUNK_BYTES];
            loop {
                match reader.read(buffer.as_mut_slice()) {
                    Ok(0) => break,
                    Ok(count) => {
                        if output_tx.send(PtyOutputEvent::Bytes(buffer[..count].to_vec())).is_err()
                        {
                            return;
                        }
                    }
                    Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
                    Err(_) => break,
                }
            }
            let _ = output_tx.send(PtyOutputEvent::Closed);
        })
        .map(|_| ())
        .map_err(|error| PtyBackendError::Io(error.to_string()))
}

fn spawn_child_waiter(
    mut child: Box<dyn PtyChild + Send + Sync>,
    exit_tx: SyncSender<Result<PtyExitStatus, String>>,
) -> Result<(), PtyBackendError> {
    thread::Builder::new()
        .name("palyra-pty-child-wait".to_owned())
        .spawn(move || {
            let outcome = child.wait().map_err(|error| error.to_string());
            let _ = exit_tx.send(outcome);
        })
        .map(|_| ())
        .map_err(|error| PtyBackendError::Io(error.to_string()))
}

#[cfg(unix)]
const fn native_backend_kind() -> PtyBackendKind {
    PtyBackendKind::UnixPty
}

#[cfg(windows)]
const fn native_backend_kind() -> PtyBackendKind {
    PtyBackendKind::WindowsConPty
}

#[cfg(windows)]
struct WindowsPtyJob {
    handle: std::os::windows::io::OwnedHandle,
}

#[cfg(windows)]
impl WindowsPtyJob {
    fn bind(child: &dyn PtyChild, pid: u32) -> Result<Self, PtyBackendError> {
        use windows_sys::Win32::Foundation::{CloseHandle, INVALID_HANDLE_VALUE};
        use windows_sys::Win32::System::JobObjects::{
            AssignProcessToJobObject, CreateJobObjectW, JobObjectExtendedLimitInformation,
            SetInformationJobObject, JOBOBJECT_EXTENDED_LIMIT_INFORMATION,
            JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE,
        };

        // SAFETY: a null security descriptor and name request an anonymous Job
        // Object owned exclusively by this PTY session.
        let handle = unsafe { CreateJobObjectW(std::ptr::null(), std::ptr::null()) };
        if handle.is_null() || handle == INVALID_HANDLE_VALUE {
            return Err(PtyBackendError::Spawn(io::Error::last_os_error().to_string()));
        }
        let mut information = JOBOBJECT_EXTENDED_LIMIT_INFORMATION::default();
        information.BasicLimitInformation.LimitFlags = JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE;
        let information_size =
            u32::try_from(std::mem::size_of::<JOBOBJECT_EXTENDED_LIMIT_INFORMATION>())
                .unwrap_or(u32::MAX);
        // SAFETY: `handle` is live and `information` has the exact structure
        // required by `JobObjectExtendedLimitInformation`.
        let configured = unsafe {
            SetInformationJobObject(
                handle,
                JobObjectExtendedLimitInformation,
                std::ptr::from_ref(&information).cast(),
                information_size,
            )
        };
        if configured == 0 {
            let error = io::Error::last_os_error();
            // SAFETY: `handle` remains owned by this function on the error path.
            unsafe { CloseHandle(handle) };
            return Err(PtyBackendError::Spawn(error.to_string()));
        }
        let process_handle =
            child.as_raw_handle().ok_or(PtyBackendError::MissingProcessIdentity)?;
        // SAFETY: the portable-pty child guarantees that its raw handle remains
        // valid while `child` is live, and this Job handle is valid and exclusive.
        let assigned = unsafe { AssignProcessToJobObject(handle, process_handle.cast()) };
        if assigned == 0 {
            let error = io::Error::last_os_error();
            // SAFETY: `handle` remains owned by this function on the error path.
            unsafe { CloseHandle(handle) };
            return Err(PtyBackendError::Spawn(format!(
                "failed to bind ConPTY pid {pid} to Job Object: {error}"
            )));
        }
        use std::os::windows::io::FromRawHandle as _;
        // SAFETY: all fallible configuration and assignment has completed, so
        // ownership of this live anonymous Job handle transfers exactly once
        // into `OwnedHandle`.
        let handle = unsafe { std::os::windows::io::OwnedHandle::from_raw_handle(handle.cast()) };
        Ok(Self { handle })
    }

    fn terminate(&self) -> Result<(), PtyBackendError> {
        use std::os::windows::io::AsRawHandle as _;
        use windows_sys::Win32::System::JobObjects::TerminateJobObject;
        // SAFETY: the handle is retained by this session and remains valid
        // until `Drop`; the exit code is an internal forced-cleanup marker.
        let terminated = unsafe { TerminateJobObject(self.handle.as_raw_handle().cast(), 137) };
        if terminated == 0 {
            return Err(PtyBackendError::Cleanup(io::Error::last_os_error().to_string()));
        }
        Ok(())
    }
}

fn unix_time_ms() -> i64 {
    i64::try_from(SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_millis())
        .unwrap_or(i64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::application::local_resource_governor::{
        LocalResourceGovernorConfig, ResourcePriority, ResourceUnitsV1,
    };

    #[test]
    fn sanitizer_strips_clipboard_title_dcs_and_apc_sequences() {
        let mut sanitizer = TerminalTextSanitizer::default();
        let projected = sanitizer.project(
            b"before\x1b]52;c;secret\x07mid\x1b]2;title\x1b\\\x1bPprivate\x1b\\\x1b_hidden\x1b\\\x1b[31mred\x1b[0mafter",
        );
        assert_eq!(projected, "beforemidredafter");
        assert_eq!(sanitizer.report.clipboard_sequences_removed, 1);
        assert_eq!(sanitizer.report.title_sequences_removed, 1);
        assert_eq!(sanitizer.report.dcs_sequences_removed, 1);
        assert_eq!(sanitizer.report.apc_sequences_removed, 1);
        assert_eq!(sanitizer.report.csi_sequences_removed, 2);
    }

    #[test]
    fn sanitizer_decodes_utf8_split_across_chunks() {
        let mut sanitizer = TerminalTextSanitizer::default();
        assert_eq!(sanitizer.project(&[0xe2, 0x82]), "");
        assert_eq!(sanitizer.project(&[0xac]), "€");
        assert_eq!(sanitizer.report.invalid_utf8_segments, 0);
        assert_eq!(sanitizer.project(&[0xff]), "\u{fffd}");
        assert_eq!(sanitizer.report.invalid_utf8_segments, 1);
    }

    #[cfg(windows)]
    #[test]
    fn conpty_startup_handshake_detects_a_split_cursor_query_once() {
        let mut handshake = ConPtyStartupHandshake::default();
        assert!(!handshake.observe_cursor_query(b"\x1b[x\x1b[6"));
        assert!(handshake.observe_cursor_query(b"n"));
        assert!(!handshake.observe_cursor_query(b"\x1b[6n"));
    }

    #[test]
    fn native_pty_supports_interactive_io_and_resize() {
        let temp = tempfile::tempdir().expect("temp dir");
        let limit = ResourceUnitsV1 {
            processes: 4,
            memory_bytes: 512 * 1024 * 1024,
            file_descriptors: 128,
            sockets: 16,
            spool_bytes: 4 * 1024 * 1024,
            concurrency: 8,
        };
        let governor = LocalResourceGovernor::open(LocalResourceGovernorConfig {
            registry_path: temp.path().join("governor").join("leases.json"),
            global_limit: limit,
            per_owner_limit: limit,
            max_records: 16,
        })
        .expect("open governor");
        let request = ResourceLeaseRequestV1 {
            owner_id: "pty-test".to_owned(),
            generation: 1,
            service: ResourceServiceKind::Pty,
            priority: ResourcePriority::Interactive,
            requested: ResourceUnitsV1 {
                processes: 1,
                memory_bytes: 64 * 1024 * 1024,
                file_descriptors: 4,
                sockets: 0,
                spool_bytes: 64 * 1024,
                concurrency: 1,
            },
            duration: Duration::from_secs(30),
        };
        let mut session = NativePtySession::spawn(test_shell_spec(temp.path()), governor, request)
            .expect("spawn native PTY");
        let descriptor = session.session_descriptor();
        assert!(descriptor.pty_active);
        assert_eq!(descriptor.owner_id, "pty-test");
        let unauthorized = session
            .apply_input(TerminalInputRequestV1 {
                pty_session_id: descriptor.pty_session_id.clone(),
                owner_id: "foreign-owner".to_owned(),
                owner_generation: descriptor.owner_generation,
                action: TerminalInputActionV1::WriteText("blocked".to_owned()),
            })
            .expect_err("foreign owner must be rejected");
        assert!(matches!(unauthorized, PtyBackendError::Unauthorized));
        let requested = TerminalSizeV1 { rows: 41, cols: 101, pixel_width: 0, pixel_height: 0 };
        session
            .apply_resize(TerminalResizeRequestV1 {
                pty_session_id: descriptor.pty_session_id.clone(),
                owner_id: descriptor.owner_id.clone(),
                owner_generation: descriptor.owner_generation,
                size: requested,
            })
            .expect("resize terminal");
        assert_eq!(session.descriptor().size, requested);
        session
            .apply_input(TerminalInputRequestV1 {
                pty_session_id: descriptor.pty_session_id.clone(),
                owner_id: descriptor.owner_id.clone(),
                owner_generation: descriptor.owner_generation,
                action: TerminalInputActionV1::BracketedPaste("hello".to_owned()),
            })
            .expect("paste terminal");
        session
            .apply_input(TerminalInputRequestV1 {
                pty_session_id: descriptor.pty_session_id.clone(),
                owner_id: descriptor.owner_id.clone(),
                owner_generation: descriptor.owner_generation,
                action: TerminalInputActionV1::Submit,
            })
            .expect("submit terminal");
        let deadline = Instant::now() + Duration::from_secs(10);
        let mut safe_text = String::new();
        let mut raw_output_observed = false;
        let mut csi_sequences_removed = 0;
        while Instant::now() < deadline && !safe_text.contains("got:hello") {
            let page = session.read_output(None, 32);
            safe_text.push_str(page.safe_text.as_str());
            raw_output_observed |= !page.raw_chunks.is_empty();
            csi_sequences_removed = page.sanitization.csi_sequences_removed;
            thread::sleep(Duration::from_millis(10));
        }
        #[cfg(not(windows))]
        {
            assert!(safe_text.contains("PTY_OK"));
            assert!(safe_text.contains("got:hello"));
            assert!(csi_sequences_removed >= 2);
            assert!(raw_output_observed);
        }
        #[cfg(windows)]
        if safe_text.contains("got:hello") {
            assert!(safe_text.contains("PTY_OK"));
            assert!(csi_sequences_removed >= 2);
            assert!(raw_output_observed);
        }
        session.terminate().expect("terminate PTY tree");
        assert!(!session.descriptor().pty_active);
    }

    #[cfg(windows)]
    fn test_shell_spec(cwd: &std::path::Path) -> PtyLaunchSpec {
        PtyLaunchSpec {
            executable: PathBuf::from(
                std::env::var_os("COMSPEC")
                    .unwrap_or_else(|| "C:\\Windows\\System32\\cmd.exe".into()),
            ),
            args: vec![
                "/D".to_owned(),
                "/V:ON".to_owned(),
                "/S".to_owned(),
                "/C".to_owned(),
                "echo PTY_OK & set /p value=& echo got:!value!".to_owned(),
            ],
            cwd: cwd.to_path_buf(),
            env: BTreeMap::from([(
                "SYSTEMROOT".to_owned(),
                std::env::var("SYSTEMROOT").unwrap_or_else(|_| "C:\\Windows".to_owned()),
            )]),
            size: TerminalSizeV1::default(),
            max_raw_bytes: 64 * 1024,
            max_raw_chunks: 64,
        }
    }

    #[cfg(not(windows))]
    fn test_shell_spec(cwd: &std::path::Path) -> PtyLaunchSpec {
        PtyLaunchSpec {
            executable: PathBuf::from("/bin/sh"),
            args: vec![
                "-c".to_owned(),
                "test -t 0 && echo PTY_OK; read value; printf 'got:%s\\n' \"$value\"".to_owned(),
            ],
            cwd: cwd.to_path_buf(),
            env: BTreeMap::from([("PATH".to_owned(), "/usr/bin:/bin".to_owned())]),
            size: TerminalSizeV1::default(),
            max_raw_bytes: 64 * 1024,
            max_raw_chunks: 64,
        }
    }
}
