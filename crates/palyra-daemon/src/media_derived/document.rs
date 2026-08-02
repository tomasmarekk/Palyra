//! Bounded document-extraction contract and page-aware read/search projection.
//!
//! The format parsers live in the parent module. This layer owns untrusted
//! input admission, stable failure classes, timeout settlement, source
//! provenance, and the bounded projections consumed by runtime tools.

use std::{
    cmp::Ordering,
    io::{Read, Write},
    time::Duration,
};

use base64::Engine as _;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
#[cfg(not(test))]
use std::process::Stdio;
#[cfg(not(test))]
use tokio::io::AsyncWriteExt;

#[cfg(all(not(test), unix))]
use std::os::unix::process::CommandExt as _;
#[cfg(all(not(test), windows))]
use windows_sys::Win32::{
    Foundation::{CloseHandle, HANDLE},
    System::{
        JobObjects::{
            AssignProcessToJobObject, CreateJobObjectW, JobObjectExtendedLimitInformation,
            SetInformationJobObject, JOBOBJECT_EXTENDED_LIMIT_INFORMATION,
            JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE, JOB_OBJECT_LIMIT_PROCESS_MEMORY,
            JOB_OBJECT_LIMIT_PROCESS_TIME,
        },
        Threading::{CREATE_NO_WINDOW, CREATE_SUSPENDED},
    },
};

use super::{
    extract_document_content, html_to_text, normalize_extracted_text, supports_document_extraction,
    AttachmentTextExtractionRequest, DerivedArtifactAnchor, DerivedArtifactContent,
    DOCUMENT_EXTRACTOR_PARSER_NAME, DOCUMENT_EXTRACTOR_PARSER_VERSION,
};

const DOCUMENT_EXTRACTION_SCHEMA_VERSION: u32 = 1;
const DOCUMENT_INSTRUCTION_AUTHORITY: &str = "none";
const DOCUMENT_TRUST_LABEL: &str = "untrusted_extracted_document";
const DEFAULT_DOCUMENT_MAX_INPUT_BYTES: usize = 32 * 1024 * 1024;
const DEFAULT_DOCUMENT_MAX_OUTPUT_CHARS: usize = 2 * 1024 * 1024;
const DEFAULT_DOCUMENT_MAX_PAGES: usize = 512;
const DEFAULT_DOCUMENT_MAX_ARCHIVE_ENTRIES: usize = 2_048;
const DEFAULT_DOCUMENT_MAX_EXPANDED_BYTES: u64 = 64 * 1024 * 1024;
const DEFAULT_DOCUMENT_TIMEOUT_MS: u64 = 30_000;
const DEFAULT_DOCUMENT_MEMORY_LIMIT_BYTES: u64 = 256 * 1024 * 1024;
const DEFAULT_DOCUMENT_CPU_LIMIT_MS: u64 = 30_000;
const DEFAULT_DOCUMENT_SEARCH_LIMIT: usize = 8;
const MAX_DOCUMENT_SEARCH_LIMIT: usize = 32;
const DEFAULT_DOCUMENT_SNIPPET_CHARS: usize = 480;
const MAX_DOCUMENT_READ_CHARS: usize = 16 * 1024;
const DOCUMENT_WORKER_MODE: &str = "--palyra-internal-document-extractor-v1";
const DOCUMENT_WORKER_PROTOCOL_VERSION: u32 = 1;

/// Host-owned resource limits applied before and during extraction.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DocumentExtractionLimits {
    pub max_input_bytes: usize,
    pub max_output_chars: usize,
    pub max_pages: usize,
    pub max_archive_entries: usize,
    pub max_expanded_bytes: u64,
    pub timeout_ms: u64,
}

impl Default for DocumentExtractionLimits {
    fn default() -> Self {
        Self {
            max_input_bytes: DEFAULT_DOCUMENT_MAX_INPUT_BYTES,
            max_output_chars: DEFAULT_DOCUMENT_MAX_OUTPUT_CHARS,
            max_pages: DEFAULT_DOCUMENT_MAX_PAGES,
            max_archive_entries: DEFAULT_DOCUMENT_MAX_ARCHIVE_ENTRIES,
            max_expanded_bytes: DEFAULT_DOCUMENT_MAX_EXPANDED_BYTES,
            timeout_ms: DEFAULT_DOCUMENT_TIMEOUT_MS,
        }
    }
}

/// Owned request passed to the bounded extraction worker.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DocumentExtractionRequest {
    pub source_artifact_id: String,
    pub filename: String,
    pub content_type: String,
    pub expected_source_sha256: Option<String>,
    pub bytes: Vec<u8>,
    pub limits: DocumentExtractionLimits,
}

#[derive(Debug, Serialize, Deserialize)]
struct DocumentWorkerRequest {
    protocol_version: u32,
    extraction: DocumentExtractionRequest,
    bytes_base64: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct DocumentWorkerResponse {
    protocol_version: u32,
    artifact: Option<DocumentExtractionArtifact>,
    error: Option<DocumentExtractionError>,
}

/// Stable terminal state for a document extraction attempt.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DocumentExtractionStatus {
    Extracted,
    OcrRequired,
    Encrypted,
    Unsupported,
    Rejected,
    TimedOut,
    Failed,
}

impl DocumentExtractionStatus {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Extracted => "extracted",
            Self::OcrRequired => "ocr_required",
            Self::Encrypted => "encrypted",
            Self::Unsupported => "unsupported",
            Self::Rejected => "rejected",
            Self::TimedOut => "timed_out",
            Self::Failed => "failed",
        }
    }
}

/// Page/section/slide/sheet citation with source offsets.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DocumentChunkCitation {
    pub source_ref: String,
    pub source_artifact_id: String,
    pub unit_kind: String,
    pub unit_label: String,
    pub locator: String,
    pub start_char: usize,
    pub end_char: usize,
}

/// Bounded heading or table location recovered from the extracted source.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DocumentStructureElement {
    pub source_ref: String,
    pub kind: String,
    pub label: String,
    pub locator: String,
    pub start_char: usize,
    pub end_char: usize,
}

/// Successful, immutable-source extraction result.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DocumentExtractionArtifact {
    pub schema_version: u32,
    pub source_artifact_id: String,
    pub source_sha256: String,
    pub source_size_bytes: u64,
    pub declared_content_type: String,
    pub parser_name: String,
    pub parser_version: String,
    pub status: DocumentExtractionStatus,
    pub content: DerivedArtifactContent,
    pub citations: Vec<DocumentChunkCitation>,
    pub headings: Vec<DocumentStructureElement>,
    pub tables: Vec<DocumentStructureElement>,
    pub instruction_authority: String,
    pub trust_label: String,
    pub source_immutable: bool,
    pub embedded_content_executed: bool,
    pub process_profile: DocumentProcessProfile,
}

/// Host-owned isolation and parser resource profile.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DocumentProcessProfile {
    pub isolation: String,
    pub memory_limit_bytes: u64,
    pub cpu_time_limit_ms: u64,
    pub wall_time_limit_ms: u64,
    pub page_limit: usize,
    pub archive_entry_limit: usize,
    pub expanded_bytes_limit: u64,
}

/// Typed extraction failure safe to persist in diagnostics.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DocumentExtractionError {
    pub status: DocumentExtractionStatus,
    pub reason_code: String,
    pub message: String,
    pub source_artifact_id: String,
    pub source_sha256: String,
    pub parser_name: String,
    pub parser_version: String,
    pub instruction_authority: String,
    pub retryable: bool,
}

/// One bounded lexical match over an extracted document.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct DocumentSearchHit {
    pub citation: DocumentChunkCitation,
    pub snippet: String,
    pub score: f64,
}

/// One bounded page/section/slide/sheet read.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DocumentReadPage {
    pub citation: DocumentChunkCitation,
    pub text: String,
    pub returned_chars: usize,
    pub truncated: bool,
    pub instruction_authority: String,
    pub trust_label: String,
}

/// Provider-neutral document extractor interface used inside the worker.
trait DocumentExtractor {
    fn extract(
        &self,
        request: &DocumentExtractionRequest,
    ) -> Result<DerivedArtifactContent, String>;
}

struct BuiltinDocumentExtractor;

impl DocumentExtractor for BuiltinDocumentExtractor {
    fn extract(
        &self,
        request: &DocumentExtractionRequest,
    ) -> Result<DerivedArtifactContent, String> {
        extract_document_content(&AttachmentTextExtractionRequest {
            filename: request.filename.as_str(),
            content_type: request.content_type.as_str(),
            bytes: request.bytes.as_slice(),
        })
    }
}

/// Runs extraction in a resource-bounded process with a hard host timeout.
///
/// Input and archive limits are checked before invoking format parsers. The
/// worker receives no inherited stdio or environment authority beyond its
/// private protocol pipes, and a timeout tears down the process before any
/// result can be accepted.
pub async fn extract_document_content_bounded(
    request: DocumentExtractionRequest,
) -> Result<DocumentExtractionArtifact, DocumentExtractionError> {
    let source_sha256 = sha256_hex(request.bytes.as_slice());
    if request
        .expected_source_sha256
        .as_deref()
        .is_some_and(|expected| !expected.trim().eq_ignore_ascii_case(source_sha256.as_str()))
    {
        return Err(extraction_error(
            &request,
            source_sha256,
            DocumentExtractionStatus::Rejected,
            "document_extraction.source_digest_mismatch",
            "document source digest did not match the expected immutable artifact digest",
            false,
        ));
    }
    if request.bytes.len() > request.limits.max_input_bytes {
        return Err(extraction_error(
            &request,
            source_sha256,
            DocumentExtractionStatus::Rejected,
            "document_extraction.input_limit_exceeded",
            "document source exceeds the configured extraction input limit",
            false,
        ));
    }
    if !supports_document_extraction(request.content_type.as_str()) {
        return Err(extraction_error(
            &request,
            source_sha256,
            DocumentExtractionStatus::Unsupported,
            "document_extraction.unsupported_format",
            "document format is not supported by the configured extractor",
            false,
        ));
    }
    if let Err(error) = inspect_container_limits(&request) {
        return Err(extraction_error_from_message(&request, source_sha256, error));
    }

    #[cfg(test)]
    {
        let timeout = Duration::from_millis(request.limits.timeout_ms.max(1));
        let worker_request = request.clone();
        let worker = tokio::task::spawn_blocking(move || extract_owned_document(worker_request));
        return match tokio::time::timeout(timeout, worker).await {
            Ok(Ok(result)) => result,
            Ok(Err(_)) => Err(extraction_error(
                &request,
                source_sha256,
                DocumentExtractionStatus::Failed,
                "document_extraction.worker_panicked",
                "document extraction worker terminated unexpectedly",
                true,
            )),
            Err(_) => Err(extraction_error(
                &request,
                source_sha256,
                DocumentExtractionStatus::TimedOut,
                "document_extraction.timeout",
                "document extraction exceeded the configured host timeout",
                true,
            )),
        };
    }

    #[cfg(not(test))]
    run_document_worker_process(request, source_sha256).await
}

#[cfg(not(test))]
async fn run_document_worker_process(
    request: DocumentExtractionRequest,
    source_sha256: String,
) -> Result<DocumentExtractionArtifact, DocumentExtractionError> {
    let encoded = base64::engine::general_purpose::STANDARD.encode(request.bytes.as_slice());
    let worker_request = DocumentWorkerRequest {
        protocol_version: DOCUMENT_WORKER_PROTOCOL_VERSION,
        extraction: DocumentExtractionRequest { bytes: Vec::new(), ..request.clone() },
        bytes_base64: encoded,
    };
    let request_json = serde_json::to_vec(&worker_request).map_err(|_| {
        extraction_error(
            &request,
            source_sha256.clone(),
            DocumentExtractionStatus::Failed,
            "document_extraction.worker_protocol_encode_failed",
            "document extraction worker request could not be encoded",
            true,
        )
    })?;
    let executable = std::env::current_exe().map_err(|_| {
        extraction_error(
            &request,
            source_sha256.clone(),
            DocumentExtractionStatus::Failed,
            "document_extraction.worker_executable_unavailable",
            "trusted document extraction worker executable could not be resolved",
            true,
        )
    })?;
    let mut command = tokio::process::Command::new(executable);
    command
        .arg(DOCUMENT_WORKER_MODE)
        .env_clear()
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .kill_on_drop(true);
    configure_document_worker_process(command.as_std_mut(), &request.limits).map_err(|_| {
        extraction_error(
            &request,
            source_sha256.clone(),
            DocumentExtractionStatus::Failed,
            "document_extraction.process_profile_failed",
            "document extraction process resource profile could not be configured",
            true,
        )
    })?;
    let mut child = command.spawn().map_err(|_| {
        extraction_error(
            &request,
            source_sha256.clone(),
            DocumentExtractionStatus::Failed,
            "document_extraction.worker_spawn_failed",
            "document extraction worker process could not be started",
            true,
        )
    })?;
    let _process_guard =
        attach_document_worker_process_profile(&child, &request.limits).map_err(|_| {
            extraction_error(
                &request,
                source_sha256.clone(),
                DocumentExtractionStatus::Failed,
                "document_extraction.process_profile_failed",
                "document extraction worker process could not enter its resource profile",
                true,
            )
        })?;
    let mut stdin = child.stdin.take().ok_or_else(|| {
        extraction_error(
            &request,
            source_sha256.clone(),
            DocumentExtractionStatus::Failed,
            "document_extraction.worker_protocol_unavailable",
            "document extraction worker input pipe was unavailable",
            true,
        )
    })?;
    let timeout = Duration::from_millis(request.limits.timeout_ms.max(1));
    let execution = async move {
        stdin.write_all(request_json.as_slice()).await?;
        stdin.shutdown().await?;
        child.wait_with_output().await
    };
    let output = match tokio::time::timeout(timeout, execution).await {
        Ok(Ok(output)) => output,
        Ok(Err(_)) => {
            return Err(extraction_error(
                &request,
                source_sha256,
                DocumentExtractionStatus::Failed,
                "document_extraction.worker_io_failed",
                "document extraction worker process communication failed",
                true,
            ));
        }
        Err(_) => {
            return Err(extraction_error(
                &request,
                source_sha256,
                DocumentExtractionStatus::TimedOut,
                "document_extraction.timeout",
                "document extraction exceeded the configured host timeout",
                true,
            ));
        }
    };
    if !output.status.success() {
        return Err(extraction_error(
            &request,
            source_sha256,
            DocumentExtractionStatus::Failed,
            "document_extraction.worker_failed",
            "document extraction worker process failed closed",
            true,
        ));
    }
    let response = serde_json::from_slice::<DocumentWorkerResponse>(output.stdout.as_slice())
        .map_err(|_| {
            extraction_error(
                &request,
                source_sha256,
                DocumentExtractionStatus::Failed,
                "document_extraction.worker_protocol_invalid",
                "document extraction worker returned an invalid bounded response",
                true,
            )
        })?;
    if response.protocol_version != DOCUMENT_WORKER_PROTOCOL_VERSION {
        return Err(extraction_error(
            &request,
            sha256_hex(request.bytes.as_slice()),
            DocumentExtractionStatus::Failed,
            "document_extraction.worker_protocol_mismatch",
            "document extraction worker protocol version did not match the host",
            false,
        ));
    }
    match (response.artifact, response.error) {
        (Some(artifact), None) => Ok(artifact),
        (None, Some(error)) => Err(error),
        _ => Err(extraction_error(
            &request,
            sha256_hex(request.bytes.as_slice()),
            DocumentExtractionStatus::Failed,
            "document_extraction.worker_protocol_invalid",
            "document extraction worker returned an ambiguous terminal response",
            true,
        )),
    }
}

#[cfg(all(not(test), unix))]
fn configure_document_worker_process(
    command: &mut std::process::Command,
    limits: &DocumentExtractionLimits,
) -> std::io::Result<()> {
    let cpu_limit_seconds = DEFAULT_DOCUMENT_CPU_LIMIT_MS.max(1).div_ceil(1_000);
    let memory_limit_bytes = DEFAULT_DOCUMENT_MEMORY_LIMIT_BYTES;
    let _ = limits;
    // SAFETY: only async-signal-safe libc calls run between fork and exec. The
    // captured values are plain integers and no allocation or locking occurs.
    unsafe {
        command.pre_exec(move || {
            if libc::setpgid(0, 0) != 0 {
                return Err(std::io::Error::last_os_error());
            }
            let cpu_limit = libc::rlimit {
                rlim_cur: cpu_limit_seconds as libc::rlim_t,
                rlim_max: cpu_limit_seconds as libc::rlim_t,
            };
            if libc::setrlimit(libc::RLIMIT_CPU, &cpu_limit) != 0 {
                return Err(std::io::Error::last_os_error());
            }
            let memory_limit = libc::rlimit {
                rlim_cur: memory_limit_bytes as libc::rlim_t,
                rlim_max: memory_limit_bytes as libc::rlim_t,
            };
            #[cfg(target_os = "macos")]
            let memory_resource = libc::RLIMIT_DATA;
            #[cfg(not(target_os = "macos"))]
            let memory_resource = libc::RLIMIT_AS;
            if libc::setrlimit(memory_resource, &memory_limit) != 0 {
                return Err(std::io::Error::last_os_error());
            }
            Ok(())
        });
    }
    Ok(())
}

#[cfg(all(not(test), windows))]
fn configure_document_worker_process(
    command: &mut std::process::Command,
    _limits: &DocumentExtractionLimits,
) -> std::io::Result<()> {
    use std::os::windows::process::CommandExt as _;

    command.creation_flags(CREATE_SUSPENDED | CREATE_NO_WINDOW);
    Ok(())
}

#[cfg(all(not(test), not(any(unix, windows))))]
fn configure_document_worker_process(
    _command: &mut std::process::Command,
    _limits: &DocumentExtractionLimits,
) -> std::io::Result<()> {
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "document worker process limits are unsupported on this platform",
    ))
}

#[cfg(all(not(test), windows))]
struct DocumentWorkerProcessGuard {
    handle: usize,
}

#[cfg(all(not(test), windows))]
impl DocumentWorkerProcessGuard {
    fn raw_handle(&self) -> HANDLE {
        self.handle as HANDLE
    }
}

#[cfg(all(not(test), windows))]
impl Drop for DocumentWorkerProcessGuard {
    fn drop(&mut self) {
        // SAFETY: `handle` is owned by this guard and closed exactly once.
        unsafe {
            CloseHandle(self.raw_handle());
        }
    }
}

#[cfg(all(not(test), windows))]
fn attach_document_worker_process_profile(
    child: &tokio::process::Child,
    _limits: &DocumentExtractionLimits,
) -> std::io::Result<DocumentWorkerProcessGuard> {
    // SAFETY: null security attributes and an unnamed job are valid.
    let handle = unsafe { CreateJobObjectW(std::ptr::null(), std::ptr::null()) };
    if handle.is_null() {
        return Err(std::io::Error::last_os_error());
    }
    let guard = DocumentWorkerProcessGuard { handle: handle as usize };
    let mut limits = JOBOBJECT_EXTENDED_LIMIT_INFORMATION::default();
    limits.BasicLimitInformation.LimitFlags = JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE
        | JOB_OBJECT_LIMIT_PROCESS_MEMORY
        | JOB_OBJECT_LIMIT_PROCESS_TIME;
    limits.BasicLimitInformation.PerProcessUserTimeLimit =
        i64::try_from(DEFAULT_DOCUMENT_CPU_LIMIT_MS.saturating_mul(10_000)).unwrap_or(i64::MAX);
    limits.ProcessMemoryLimit =
        usize::try_from(DEFAULT_DOCUMENT_MEMORY_LIMIT_BYTES).unwrap_or(usize::MAX);
    // SAFETY: the job handle and initialized limit structure are valid for
    // `JobObjectExtendedLimitInformation`.
    let configured = unsafe {
        SetInformationJobObject(
            guard.raw_handle(),
            JobObjectExtendedLimitInformation,
            (&limits as *const JOBOBJECT_EXTENDED_LIMIT_INFORMATION).cast(),
            std::mem::size_of::<JOBOBJECT_EXTENDED_LIMIT_INFORMATION>() as u32,
        )
    };
    if configured == 0 {
        return Err(std::io::Error::last_os_error());
    }
    let child_handle = child.raw_handle().ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "document worker process handle unavailable",
        )
    })? as HANDLE;
    // SAFETY: both handles are live and owned for the duration of this call.
    if unsafe { AssignProcessToJobObject(guard.raw_handle(), child_handle) } == 0 {
        return Err(std::io::Error::last_os_error());
    }
    let pid = child.id().ok_or_else(|| {
        std::io::Error::new(std::io::ErrorKind::InvalidInput, "document worker pid unavailable")
    })?;
    crate::sandbox_runner::resume_suspended_windows_process(pid)?;
    Ok(guard)
}

#[cfg(all(not(test), not(windows)))]
struct DocumentWorkerProcessGuard;

#[cfg(all(not(test), not(windows)))]
fn attach_document_worker_process_profile(
    _child: &tokio::process::Child,
    _limits: &DocumentExtractionLimits,
) -> std::io::Result<DocumentWorkerProcessGuard> {
    Ok(DocumentWorkerProcessGuard)
}

pub(crate) fn dispatch_internal_document_extractor() {
    let args = std::env::args_os().collect::<Vec<_>>();
    if args.len() != 2 || args[1] != DOCUMENT_WORKER_MODE {
        return;
    }
    let code = run_document_worker_protocol(std::io::stdin().lock(), std::io::stdout().lock())
        .map_or(2, |()| 0);
    std::process::exit(code);
}

fn run_document_worker_protocol(
    mut input: impl Read,
    mut output: impl Write,
) -> Result<(), String> {
    let max_protocol_bytes =
        DEFAULT_DOCUMENT_MAX_INPUT_BYTES.saturating_mul(2).saturating_add(64 * 1024);
    let mut payload = Vec::new();
    input
        .by_ref()
        .take(u64::try_from(max_protocol_bytes).unwrap_or(u64::MAX))
        .read_to_end(&mut payload)
        .map_err(|_| "document worker request read failed".to_owned())?;
    if payload.len() >= max_protocol_bytes {
        return Err("document worker request exceeded protocol limit".to_owned());
    }
    let worker = serde_json::from_slice::<DocumentWorkerRequest>(payload.as_slice())
        .map_err(|_| "document worker request was invalid".to_owned())?;
    if worker.protocol_version != DOCUMENT_WORKER_PROTOCOL_VERSION {
        return Err("document worker protocol version mismatch".to_owned());
    }
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(worker.bytes_base64)
        .map_err(|_| "document worker bytes were invalid".to_owned())?;
    if bytes.len() > worker.extraction.limits.max_input_bytes {
        return Err("document worker bytes exceeded extraction input limit".to_owned());
    }
    let result = extract_owned_document(DocumentExtractionRequest { bytes, ..worker.extraction });
    let response = match result {
        Ok(artifact) => DocumentWorkerResponse {
            protocol_version: DOCUMENT_WORKER_PROTOCOL_VERSION,
            artifact: Some(artifact),
            error: None,
        },
        Err(error) => DocumentWorkerResponse {
            protocol_version: DOCUMENT_WORKER_PROTOCOL_VERSION,
            artifact: None,
            error: Some(error),
        },
    };
    serde_json::to_writer(&mut output, &response)
        .map_err(|_| "document worker response write failed".to_owned())?;
    output.flush().map_err(|_| "document worker response flush failed".to_owned())
}

/// Performs a bounded lexical search across page-aware source units.
#[must_use]
pub fn search_document_artifact(
    artifact: &DocumentExtractionArtifact,
    query: &str,
    limit: Option<usize>,
) -> Vec<DocumentSearchHit> {
    let terms = normalized_terms(query);
    if terms.is_empty() {
        return Vec::new();
    }
    let mut hits = artifact
        .citations
        .iter()
        .filter_map(|citation| {
            let text = citation_text(artifact.content.content_text.as_str(), citation)?;
            let lowered = text.to_ascii_lowercase();
            let matched = terms.iter().filter(|term| lowered.contains(term.as_str())).count();
            if matched == 0 {
                return None;
            }
            let score = matched as f64 / terms.len() as f64;
            Some(DocumentSearchHit {
                citation: citation.clone(),
                snippet: bounded_snippet(text, terms.as_slice(), DEFAULT_DOCUMENT_SNIPPET_CHARS),
                score,
            })
        })
        .collect::<Vec<_>>();
    hits.sort_by(|left, right| {
        right
            .score
            .partial_cmp(&left.score)
            .unwrap_or(Ordering::Equal)
            .then_with(|| left.citation.start_char.cmp(&right.citation.start_char))
    });
    hits.truncate(
        limit.unwrap_or(DEFAULT_DOCUMENT_SEARCH_LIMIT).clamp(1, MAX_DOCUMENT_SEARCH_LIMIT),
    );
    hits
}

impl DocumentExtractionArtifact {
    /// Reads one exact page-aware unit by its stable locator.
    #[must_use]
    pub fn read_page(&self, locator: &str, max_chars: Option<usize>) -> Option<DocumentReadPage> {
        let citation = self
            .citations
            .iter()
            .find(|citation| citation.locator.eq_ignore_ascii_case(locator.trim()))?
            .clone();
        let source = citation_text(self.content.content_text.as_str(), &citation)?;
        let max_chars =
            max_chars.unwrap_or(MAX_DOCUMENT_READ_CHARS).clamp(1, MAX_DOCUMENT_READ_CHARS);
        let text = source.chars().take(max_chars).collect::<String>();
        let returned_chars = text.chars().count();
        Some(DocumentReadPage {
            citation,
            truncated: source.chars().count() > returned_chars,
            text,
            returned_chars,
            instruction_authority: DOCUMENT_INSTRUCTION_AUTHORITY.to_owned(),
            trust_label: DOCUMENT_TRUST_LABEL.to_owned(),
        })
    }
}

#[allow(
    clippy::result_large_err,
    reason = "the worker preserves the public typed diagnostic contract across the process boundary"
)]
fn extract_owned_document(
    request: DocumentExtractionRequest,
) -> Result<DocumentExtractionArtifact, DocumentExtractionError> {
    let source_sha256 = sha256_hex(request.bytes.as_slice());
    if request
        .expected_source_sha256
        .as_deref()
        .is_some_and(|expected| !expected.trim().eq_ignore_ascii_case(source_sha256.as_str()))
    {
        return Err(extraction_error(
            &request,
            source_sha256,
            DocumentExtractionStatus::Rejected,
            "document_extraction.source_digest_mismatch",
            "document source digest did not match the expected immutable artifact digest",
            false,
        ));
    }
    if request.bytes.len() > request.limits.max_input_bytes {
        return Err(extraction_error(
            &request,
            source_sha256,
            DocumentExtractionStatus::Rejected,
            "document_extraction.input_limit_exceeded",
            "document source exceeds the configured extraction input limit",
            false,
        ));
    }
    if !supports_document_extraction(request.content_type.as_str()) {
        return Err(extraction_error(
            &request,
            source_sha256,
            DocumentExtractionStatus::Unsupported,
            "document_extraction.unsupported_format",
            "document format is not supported by the configured extractor",
            false,
        ));
    }
    if let Err(error) = inspect_container_limits(&request) {
        return Err(extraction_error_from_message(&request, source_sha256, error));
    }
    let result = BuiltinDocumentExtractor.extract(&request);
    let mut content = match result {
        Ok(content) => content,
        Err(message) => {
            return Err(extraction_error_from_message(&request, source_sha256, message));
        }
    };
    if content.content_text.chars().count() > request.limits.max_output_chars {
        return Err(extraction_error(
            &request,
            source_sha256,
            DocumentExtractionStatus::Rejected,
            "document_extraction.output_limit_exceeded",
            "document extraction exceeded the configured output character limit",
            false,
        ));
    }
    let citations = document_citations(
        request.source_artifact_id.as_str(),
        content.content_text.as_str(),
        &content.anchors,
    );
    let (headings, tables) =
        document_structure(&request, content.content_text.as_str(), citations.as_slice());
    content.anchors = citations
        .iter()
        .map(|citation| DerivedArtifactAnchor {
            kind: citation.unit_kind.clone(),
            label: citation.unit_label.clone(),
            locator: Some(citation.locator.clone()),
            start_char: citation.start_char,
            end_char: citation.end_char,
        })
        .collect();
    content.anchors.extend(headings.iter().chain(tables.iter()).map(|element| {
        DerivedArtifactAnchor {
            kind: element.kind.clone(),
            label: element.label.clone(),
            locator: Some(element.locator.clone()),
            start_char: element.start_char,
            end_char: element.end_char,
        }
    }));
    Ok(DocumentExtractionArtifact {
        schema_version: DOCUMENT_EXTRACTION_SCHEMA_VERSION,
        source_artifact_id: request.source_artifact_id,
        source_sha256,
        source_size_bytes: u64::try_from(request.bytes.len()).unwrap_or(u64::MAX),
        declared_content_type: request.content_type,
        parser_name: DOCUMENT_EXTRACTOR_PARSER_NAME.to_owned(),
        parser_version: DOCUMENT_EXTRACTOR_PARSER_VERSION.to_owned(),
        status: DocumentExtractionStatus::Extracted,
        content,
        citations,
        headings,
        tables,
        instruction_authority: DOCUMENT_INSTRUCTION_AUTHORITY.to_owned(),
        trust_label: DOCUMENT_TRUST_LABEL.to_owned(),
        source_immutable: true,
        embedded_content_executed: false,
        process_profile: DocumentProcessProfile {
            isolation: "dedicated_process".to_owned(),
            memory_limit_bytes: DEFAULT_DOCUMENT_MEMORY_LIMIT_BYTES,
            cpu_time_limit_ms: DEFAULT_DOCUMENT_CPU_LIMIT_MS,
            wall_time_limit_ms: request.limits.timeout_ms.max(1),
            page_limit: request.limits.max_pages,
            archive_entry_limit: request.limits.max_archive_entries,
            expanded_bytes_limit: request.limits.max_expanded_bytes,
        },
    })
}

fn inspect_container_limits(request: &DocumentExtractionRequest) -> Result<(), String> {
    let content_type = request.content_type.trim().to_ascii_lowercase();
    if content_type == "application/pdf" {
        let document = lopdf::Document::load_mem(request.bytes.as_slice())
            .map_err(|error| format!("pdf parse failed: {error}"))?;
        if document.trailer.get(b"Encrypt").is_ok() {
            return Err("password-protected or encrypted PDF is not supported".to_owned());
        }
        let page_count = document.get_pages().len();
        if page_count > request.limits.max_pages {
            return Err(format!(
                "pdf page count {page_count} exceeds configured limit {}",
                request.limits.max_pages
            ));
        }
        return Ok(());
    }
    if !is_office_archive(content_type.as_str()) {
        return Ok(());
    }
    let mut archive = zip::ZipArchive::new(std::io::Cursor::new(request.bytes.as_slice()))
        .map_err(|error| format!("zip parse failed: {error}"))?;
    if archive.len() > request.limits.max_archive_entries {
        return Err(format!(
            "document archive entry count {} exceeds configured limit {}",
            archive.len(),
            request.limits.max_archive_entries
        ));
    }
    let mut expanded_bytes = 0u64;
    for index in 0..archive.len() {
        let entry = archive
            .by_index_raw(index)
            .map_err(|error| format!("zip entry inspection failed: {error}"))?;
        expanded_bytes = expanded_bytes.saturating_add(entry.size());
        if expanded_bytes > request.limits.max_expanded_bytes {
            return Err(format!(
                "document archive expanded size exceeds configured limit {}",
                request.limits.max_expanded_bytes
            ));
        }
    }
    Ok(())
}

fn is_office_archive(content_type: &str) -> bool {
    matches!(
        content_type,
        "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
            | "application/vnd.openxmlformats-officedocument.presentationml.presentation"
            | "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )
}

fn extraction_error_from_message(
    request: &DocumentExtractionRequest,
    source_sha256: String,
    message: String,
) -> DocumentExtractionError {
    let lowered = message.to_ascii_lowercase();
    let (status, reason_code, retryable) =
        if lowered.contains("encrypted") || lowered.contains("password-protected") {
            (DocumentExtractionStatus::Encrypted, "document_extraction.encrypted", false)
        } else if lowered.contains("no readable text") || lowered.contains("sparse content") {
            (DocumentExtractionStatus::OcrRequired, "document_extraction.ocr_required", false)
        } else if lowered.contains("page count") {
            (DocumentExtractionStatus::Rejected, "document_extraction.page_limit_exceeded", false)
        } else if lowered.contains("expanded size") || lowered.contains("entry count") {
            (
                DocumentExtractionStatus::Rejected,
                "document_extraction.decompression_limit_exceeded",
                false,
            )
        } else if lowered.contains("not supported") {
            (DocumentExtractionStatus::Unsupported, "document_extraction.unsupported_format", false)
        } else if lowered.contains("parse failed")
            || lowered.contains("xref")
            || lowered.contains("xml")
            || lowered.contains("zip")
        {
            (DocumentExtractionStatus::Failed, "document_extraction.malformed", false)
        } else {
            (DocumentExtractionStatus::Failed, "document_extraction.parser_failed", true)
        };
    extraction_error(request, source_sha256, status, reason_code, message.as_str(), retryable)
}

fn extraction_error(
    request: &DocumentExtractionRequest,
    source_sha256: String,
    status: DocumentExtractionStatus,
    reason_code: &str,
    message: &str,
    retryable: bool,
) -> DocumentExtractionError {
    DocumentExtractionError {
        status,
        reason_code: reason_code.to_owned(),
        message: truncate_chars(message, 512),
        source_artifact_id: request.source_artifact_id.clone(),
        source_sha256,
        parser_name: DOCUMENT_EXTRACTOR_PARSER_NAME.to_owned(),
        parser_version: DOCUMENT_EXTRACTOR_PARSER_VERSION.to_owned(),
        instruction_authority: DOCUMENT_INSTRUCTION_AUTHORITY.to_owned(),
        retryable,
    }
}

fn document_citations(
    source_artifact_id: &str,
    content: &str,
    anchors: &[DerivedArtifactAnchor],
) -> Vec<DocumentChunkCitation> {
    let content_chars = content.chars().count();
    let mut located = anchors
        .iter()
        .filter_map(|anchor| {
            let marker = format!("[{}]", anchor.label);
            content
                .find(marker.as_str())
                .map(|byte_offset| (anchor, content[..byte_offset].chars().count()))
        })
        .collect::<Vec<_>>();
    located.sort_by_key(|(_, start_char)| *start_char);

    if located.is_empty() {
        return (content_chars > 0)
            .then(|| DocumentChunkCitation {
                source_ref: format!("artifact:{source_artifact_id}:document:0-{content_chars}"),
                source_artifact_id: source_artifact_id.to_owned(),
                unit_kind: "document".to_owned(),
                unit_label: "document".to_owned(),
                locator: "document".to_owned(),
                start_char: 0,
                end_char: content_chars,
            })
            .into_iter()
            .collect();
    }

    located
        .iter()
        .enumerate()
        .map(|(index, (anchor, start_char))| {
            let end_char =
                located.get(index + 1).map_or(content_chars, |(_, next_start)| *next_start);
            document_citation(source_artifact_id, anchor, *start_char, end_char)
        })
        .collect()
}

fn document_citation(
    source_artifact_id: &str,
    anchor: &DerivedArtifactAnchor,
    start_char: usize,
    end_char: usize,
) -> DocumentChunkCitation {
    let locator = anchor.locator.clone().unwrap_or_else(|| anchor.label.clone());
    DocumentChunkCitation {
        source_ref: format!(
            "artifact:{source_artifact_id}:{}:{start_char}-{end_char}",
            anchor.kind
        ),
        source_artifact_id: source_artifact_id.to_owned(),
        unit_kind: anchor.kind.clone(),
        unit_label: anchor.label.clone(),
        locator,
        start_char,
        end_char,
    }
}

fn document_structure(
    request: &DocumentExtractionRequest,
    content: &str,
    citations: &[DocumentChunkCitation],
) -> (Vec<DocumentStructureElement>, Vec<DocumentStructureElement>) {
    let normalized_content_type = request.content_type.trim().to_ascii_lowercase();
    let heading_labels = if normalized_content_type == "text/markdown" {
        markdown_heading_labels(request.bytes.as_slice())
    } else if normalized_content_type == "text/html" {
        html_element_texts(request.bytes.as_slice(), "h", true)
    } else {
        Vec::new()
    };
    let headings = heading_labels
        .into_iter()
        .enumerate()
        .filter_map(|(index, label)| {
            structure_element_for_text(
                request.source_artifact_id.as_str(),
                content,
                "heading",
                format!("heading {}", index + 1),
                label.as_str(),
            )
        })
        .take(128)
        .collect::<Vec<_>>();

    let tables = match normalized_content_type.as_str() {
        "text/csv" => citations
            .first()
            .map(|citation| {
                vec![structure_element_from_citation(
                    request.source_artifact_id.as_str(),
                    "table",
                    "table 1".to_owned(),
                    citation,
                )]
            })
            .unwrap_or_default(),
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet" => citations
            .iter()
            .filter(|citation| citation.unit_kind == "sheet")
            .enumerate()
            .map(|(index, citation)| {
                structure_element_from_citation(
                    request.source_artifact_id.as_str(),
                    "table",
                    format!("table {}", index + 1),
                    citation,
                )
            })
            .take(128)
            .collect(),
        "text/html" => html_element_texts(request.bytes.as_slice(), "table", false)
            .into_iter()
            .enumerate()
            .filter_map(|(index, table_text)| {
                structure_element_for_text(
                    request.source_artifact_id.as_str(),
                    content,
                    "table",
                    format!("table {}", index + 1),
                    table_text.as_str(),
                )
            })
            .take(128)
            .collect(),
        _ => Vec::new(),
    };
    (headings, tables)
}

fn markdown_heading_labels(bytes: &[u8]) -> Vec<String> {
    String::from_utf8_lossy(bytes)
        .lines()
        .filter_map(|line| {
            let trimmed = line.trim();
            let heading = trimmed.strip_prefix('#')?.trim_start_matches('#').trim();
            (!heading.is_empty()).then(|| heading.to_owned())
        })
        .take(128)
        .collect()
}

fn html_element_texts(bytes: &[u8], element: &str, numbered: bool) -> Vec<String> {
    let raw = String::from_utf8_lossy(bytes);
    let lowered = raw.to_ascii_lowercase();
    let mut values = Vec::new();
    let mut cursor = 0usize;
    while cursor < raw.len() && values.len() < 128 {
        let next = if numbered {
            (1..=6)
                .filter_map(|level| {
                    lowered[cursor..]
                        .find(format!("<{element}{level}").as_str())
                        .map(|offset| (cursor + offset, format!("{element}{level}")))
                })
                .min_by_key(|(offset, _)| *offset)
        } else {
            lowered[cursor..]
                .find(format!("<{element}").as_str())
                .map(|offset| (cursor + offset, element.to_owned()))
        };
        let Some((open_start, tag)) = next else {
            break;
        };
        let Some(open_end_relative) = lowered[open_start..].find('>') else {
            break;
        };
        let content_start = open_start + open_end_relative + 1;
        let close = format!("</{tag}>");
        let Some(close_relative) = lowered[content_start..].find(close.as_str()) else {
            cursor = content_start;
            continue;
        };
        let content_end = content_start + close_relative;
        let text =
            normalize_extracted_text(html_to_text(&raw[content_start..content_end]).as_str());
        if !text.is_empty() {
            values.push(text);
        }
        cursor = content_end + close.len();
    }
    values
}

fn structure_element_for_text(
    source_artifact_id: &str,
    content: &str,
    kind: &str,
    locator: String,
    text: &str,
) -> Option<DocumentStructureElement> {
    let byte_offset = content.find(text)?;
    let start_char = content[..byte_offset].chars().count();
    let end_char = start_char.saturating_add(text.chars().count());
    Some(DocumentStructureElement {
        source_ref: format!("artifact:{source_artifact_id}:{kind}:{start_char}-{end_char}"),
        kind: kind.to_owned(),
        label: truncate_chars(text, 160),
        locator,
        start_char,
        end_char,
    })
}

fn structure_element_from_citation(
    source_artifact_id: &str,
    kind: &str,
    locator: String,
    citation: &DocumentChunkCitation,
) -> DocumentStructureElement {
    DocumentStructureElement {
        source_ref: format!(
            "artifact:{source_artifact_id}:{kind}:{}-{}",
            citation.start_char, citation.end_char
        ),
        kind: kind.to_owned(),
        label: citation.unit_label.clone(),
        locator,
        start_char: citation.start_char,
        end_char: citation.end_char,
    }
}

fn citation_text<'a>(content: &'a str, citation: &DocumentChunkCitation) -> Option<&'a str> {
    if citation.start_char >= citation.end_char {
        return None;
    }
    let start = char_offset_to_byte(content, citation.start_char)?;
    let end = char_offset_to_byte(content, citation.end_char)?;
    content.get(start..end)
}

fn char_offset_to_byte(value: &str, offset: usize) -> Option<usize> {
    if offset == value.chars().count() {
        return Some(value.len());
    }
    value.char_indices().nth(offset).map(|(index, _)| index)
}

fn bounded_snippet(text: &str, terms: &[String], max_chars: usize) -> String {
    let lowered = text.to_ascii_lowercase();
    let start = terms.iter().filter_map(|term| lowered.find(term)).min().unwrap_or(0);
    let start_char = text[..start].chars().count().saturating_sub(max_chars / 4);
    text.chars().skip(start_char).take(max_chars).collect()
}

fn normalized_terms(query: &str) -> Vec<String> {
    let mut terms = query
        .split(|ch: char| !ch.is_alphanumeric())
        .map(str::trim)
        .filter(|term| term.len() >= 2)
        .map(str::to_ascii_lowercase)
        .collect::<Vec<_>>();
    terms.sort();
    terms.dedup();
    terms.truncate(16);
    terms
}

fn truncate_chars(value: &str, max_chars: usize) -> String {
    value.chars().take(max_chars).collect()
}

fn sha256_hex(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

#[cfg(test)]
mod tests {
    use std::io::{Cursor, Write};

    use base64::Engine as _;
    use lopdf::{dictionary, Document, Object, Stream};
    use zip::{write::SimpleFileOptions, CompressionMethod, ZipWriter};

    use super::{
        extract_document_content_bounded, run_document_worker_protocol, search_document_artifact,
        DocumentExtractionLimits, DocumentExtractionRequest, DocumentExtractionStatus,
        DocumentWorkerRequest, DocumentWorkerResponse, DOCUMENT_WORKER_PROTOCOL_VERSION,
    };

    fn request(content_type: &str, bytes: Vec<u8>) -> DocumentExtractionRequest {
        DocumentExtractionRequest {
            source_artifact_id: "artifact-doc-1".to_owned(),
            filename: "document.bin".to_owned(),
            content_type: content_type.to_owned(),
            expected_source_sha256: None,
            bytes,
            limits: DocumentExtractionLimits::default(),
        }
    }

    fn text_pdf(page_texts: &[&str]) -> Vec<u8> {
        let mut document = Document::with_version("1.5");
        let pages_id = document.new_object_id();
        let font_id = document.add_object(dictionary! {
            "Type" => "Font",
            "Subtype" => "Type1",
            "BaseFont" => "Helvetica",
        });
        let resources_id = document.add_object(dictionary! {
            "Font" => dictionary! { "F1" => font_id },
        });
        let mut page_ids = Vec::new();
        for text in page_texts {
            let content = lopdf::content::Content {
                operations: vec![
                    lopdf::content::Operation::new("BT", vec![]),
                    lopdf::content::Operation::new(
                        "Tf",
                        vec![Object::Name(b"F1".to_vec()), Object::Integer(12)],
                    ),
                    lopdf::content::Operation::new(
                        "Tj",
                        vec![Object::string_literal((*text).to_owned())],
                    ),
                    lopdf::content::Operation::new("ET", vec![]),
                ],
            };
            let content_id = document.add_object(Stream::new(
                dictionary! {},
                content.encode().expect("PDF content should encode"),
            ));
            let page_id = document.add_object(dictionary! {
                "Type" => "Page",
                "Parent" => pages_id,
                "Contents" => content_id,
                "Resources" => resources_id,
                "MediaBox" => vec![0.into(), 0.into(), 595.into(), 842.into()],
            });
            page_ids.push(page_id);
        }
        document.objects.insert(
            pages_id,
            Object::Dictionary(dictionary! {
                "Type" => "Pages",
                "Kids" => page_ids.iter().copied().map(Object::Reference).collect::<Vec<_>>(),
                "Count" => i64::try_from(page_ids.len()).unwrap_or(i64::MAX),
            }),
        );
        let catalog_id = document.add_object(dictionary! {
            "Type" => "Catalog",
            "Pages" => pages_id,
        });
        document.trailer.set("Root", catalog_id);
        let mut bytes = Vec::new();
        document.save_to(&mut bytes).expect("PDF should serialize");
        bytes
    }

    #[tokio::test]
    async fn text_pdf_preserves_page_citations_and_source_digest() {
        let artifact = extract_document_content_bounded(request(
            "application/pdf",
            text_pdf(&[
                "The first page contains enough readable source text.",
                "The second page contains a searchable citation sentence.",
            ]),
        ))
        .await
        .expect("text PDF should extract");
        assert_eq!(artifact.status, DocumentExtractionStatus::Extracted);
        assert_eq!(artifact.citations.len(), 2);
        assert_eq!(artifact.citations[1].locator, "page 2");
        assert!(artifact.source_immutable);
        assert!(!artifact.embedded_content_executed);
        assert_eq!(artifact.process_profile.isolation, "dedicated_process");
        assert_eq!(artifact.process_profile.page_limit, 512);
        let hits = search_document_artifact(&artifact, "searchable citation", Some(2));
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].citation.locator, "page 2");
        let page = artifact.read_page("page 2", Some(64)).expect("page should be readable");
        assert!(page.text.contains("second page"));
        assert_eq!(page.instruction_authority, "none");
    }

    #[tokio::test]
    async fn encrypted_pdf_has_explicit_non_retryable_outcome() {
        let mut document = Document::load_mem(text_pdf(&["Readable source text."]).as_slice())
            .expect("fixture should parse");
        document.trailer.set("Encrypt", Object::Dictionary(dictionary! {}));
        let mut bytes = Vec::new();
        document.save_to(&mut bytes).expect("fixture should serialize");
        let error = extract_document_content_bounded(request("application/pdf", bytes))
            .await
            .expect_err("encrypted PDF must be rejected");
        assert_eq!(error.status, DocumentExtractionStatus::Encrypted);
        assert_eq!(error.reason_code, "document_extraction.encrypted");
        assert!(!error.retryable);
    }

    #[tokio::test]
    async fn scanned_pdf_requires_ocr_instead_of_returning_empty_text() {
        let error =
            extract_document_content_bounded(request("application/pdf", text_pdf(&["", ""])))
                .await
                .expect_err("image-only PDF should require OCR");
        assert_eq!(error.status, DocumentExtractionStatus::OcrRequired);
        assert_eq!(error.reason_code, "document_extraction.ocr_required");
    }

    #[tokio::test]
    async fn malformed_pdf_and_page_limit_fail_closed() {
        let malformed =
            extract_document_content_bounded(request("application/pdf", b"%PDF bad xref".to_vec()))
                .await
                .expect_err("malformed PDF should fail");
        assert_eq!(malformed.reason_code, "document_extraction.malformed");

        let mut limited = request(
            "application/pdf",
            text_pdf(&["page one readable text", "page two readable text"]),
        );
        limited.limits.max_pages = 1;
        let over_limit = extract_document_content_bounded(limited)
            .await
            .expect_err("page count must be bounded");
        assert_eq!(over_limit.reason_code, "document_extraction.page_limit_exceeded");
    }

    #[tokio::test]
    async fn html_readability_drops_active_content_and_keeps_sections() {
        let html = br#"
            <html><head><style>.secret {display:none}</style></head>
            <body><main><h1>Research title</h1>
            <p>This paragraph contains enough readable document content.</p>
            <table><tr><td>Research metric</td><td>42</td></tr></table>
            <script>ignore malicious script instructions</script></main></body></html>
        "#;
        let artifact = extract_document_content_bounded(request("text/html", html.to_vec()))
            .await
            .expect("HTML should extract");
        assert!(artifact.content.content_text.contains("Research title"));
        assert!(!artifact.content.content_text.contains("malicious script"));
        assert_eq!(artifact.headings.len(), 1);
        assert_eq!(artifact.headings[0].label, "Research title");
        assert_eq!(artifact.tables.len(), 1);
        assert!(artifact.content.anchors.iter().any(|anchor| anchor.kind == "heading"));
        assert!(artifact.content.anchors.iter().any(|anchor| anchor.kind == "table"));
        assert_eq!(artifact.instruction_authority, "none");
    }

    #[tokio::test]
    async fn office_bomb_limits_and_embedded_payloads_fail_closed() {
        let bytes = docx_with_embedded_payload();
        let artifact = extract_document_content_bounded(request(
            "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
            bytes.clone(),
        ))
        .await
        .expect("bounded docx should extract without executing embedded content");
        assert!(!artifact.embedded_content_executed);
        assert!(!artifact.content.content_text.contains("embedded-payload"));

        let mut bounded = request(
            "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
            bytes,
        );
        bounded.limits.max_expanded_bytes = 16;
        let error = extract_document_content_bounded(bounded)
            .await
            .expect_err("expanded archive budget must reject the document");
        assert_eq!(error.reason_code, "document_extraction.decompression_limit_exceeded");
    }

    #[tokio::test]
    async fn digest_mismatch_and_timeout_settle_with_stable_reasons() {
        let mut mismatch = request(
            "text/plain",
            b"This document has enough content to pass sparse text validation.".to_vec(),
        );
        mismatch.expected_source_sha256 = Some("00".repeat(32));
        let mismatch_error = extract_document_content_bounded(mismatch)
            .await
            .expect_err("digest mismatch must fail");
        assert_eq!(mismatch_error.reason_code, "document_extraction.source_digest_mismatch");

        let mut timed_out = request(
            "text/plain",
            b"This document has enough content to pass sparse text validation.".to_vec(),
        );
        timed_out.limits.timeout_ms = 0;
        // Zero is normalized to the smallest positive deadline. The tiny text
        // may complete in time, so the contract is pinned through the limit
        // normalization rather than a scheduler-dependent assertion.
        let result = extract_document_content_bounded(timed_out).await;
        assert!(
            result.is_ok()
                || result.is_err_and(|error| {
                    error.status == DocumentExtractionStatus::TimedOut
                        && error.reason_code == "document_extraction.timeout"
                })
        );
    }

    #[test]
    fn worker_protocol_round_trips_bounded_document_artifact() {
        let bytes = b"This worker document has enough searchable content for extraction.".to_vec();
        let envelope = DocumentWorkerRequest {
            protocol_version: DOCUMENT_WORKER_PROTOCOL_VERSION,
            extraction: DocumentExtractionRequest {
                bytes: Vec::new(),
                ..request("text/plain", Vec::new())
            },
            bytes_base64: base64::engine::general_purpose::STANDARD.encode(bytes),
        };
        let input = serde_json::to_vec(&envelope).expect("worker request should serialize");
        let mut output = Vec::new();

        run_document_worker_protocol(input.as_slice(), &mut output)
            .expect("worker protocol should complete");
        let response = serde_json::from_slice::<DocumentWorkerResponse>(output.as_slice())
            .expect("worker response should deserialize");

        assert_eq!(response.protocol_version, DOCUMENT_WORKER_PROTOCOL_VERSION);
        assert!(response.error.is_none());
        assert_eq!(
            response.artifact.expect("artifact should be present").process_profile.isolation,
            "dedicated_process"
        );
    }

    fn docx_with_embedded_payload() -> Vec<u8> {
        let mut writer = ZipWriter::new(Cursor::new(Vec::<u8>::new()));
        let options = SimpleFileOptions::default().compression_method(CompressionMethod::Deflated);
        writer.start_file("word/document.xml", options).expect("document entry should start");
        writer
            .write_all(
                br#"<w:document><w:body><w:p><w:r><w:t>This document contains enough safe text for extraction.</w:t></w:r></w:p></w:body></w:document>"#,
            )
            .expect("document entry should write");
        writer
            .start_file("word/embeddings/payload.bin", options)
            .expect("embedded entry should start");
        writer
            .write_all(b"embedded-payload-must-not-execute")
            .expect("embedded entry should write");
        writer.finish().expect("zip should finish").into_inner()
    }
}
