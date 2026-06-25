//! Download capture, sandboxing, and quarantine for browser sessions.
//!
//! Artifacts live in a per-session temp sandbox split into allowlist and quarantine
//! directories. Quarantined artifacts stay listable, but their content is never released to
//! callers; quarantine is decided by the extension/MIME allowlists at store time.

use crate::*;
use std::io::Read as _;

/// Metadata for one captured download stored in the session sandbox.
///
/// When `quarantined` is set, [`get_download_artifact_content`] refuses to return the bytes and
/// `quarantine_reason` carries pipe-joined machine-readable reasons.
#[derive(Debug, Clone)]
pub(crate) struct DownloadArtifactRecord {
    pub(crate) artifact_id: String,
    pub(crate) session_id: String,
    pub(crate) profile_id: Option<String>,
    pub(crate) source_url: String,
    pub(crate) file_name: String,
    pub(crate) mime_type: String,
    pub(crate) size_bytes: u64,
    pub(crate) sha256: String,
    pub(crate) created_at_unix_ms: u64,
    pub(crate) quarantined: bool,
    pub(crate) quarantine_reason: String,
    pub(crate) storage_path: PathBuf,
}

/// Bounded content read from a stored download artifact.
#[derive(Debug, Clone)]
pub(crate) struct DownloadArtifactContent {
    pub(crate) artifact: DownloadArtifactRecord,
    pub(crate) content: Vec<u8>,
    pub(crate) offset_bytes: u64,
    pub(crate) limit_bytes: u64,
    pub(crate) truncated: bool,
}

/// Per-session temp-dir sandbox holding download artifacts under a total byte budget.
///
/// Dropping the session removes the backing [`TempDir`] and with it every stored artifact.
#[derive(Debug)]
pub(crate) struct DownloadSandboxSession {
    pub(crate) root_dir: TempDir,
    pub(crate) used_bytes: u64,
    pub(crate) max_bytes: u64,
    pub(crate) artifacts: VecDeque<DownloadArtifactRecord>,
}

impl DownloadSandboxSession {
    /// Allocates a fresh sandbox temp dir with allowlist and quarantine subdirectories.
    ///
    /// # Errors
    /// Returns an error string when the temp dir or its subdirectories cannot be created.
    pub(crate) fn new() -> Result<Self, String> {
        let root_dir = tempfile::Builder::new()
            .prefix("palyra-browserd-downloads-")
            .tempdir()
            .map_err(|error| format!("failed to allocate download sandbox: {error}"))?;
        fs::create_dir_all(root_dir.path().join(DOWNLOADS_DIR_ALLOWLIST))
            .map_err(|error| format!("failed to initialize download allowlist dir: {error}"))?;
        fs::create_dir_all(root_dir.path().join(DOWNLOADS_DIR_QUARANTINE))
            .map_err(|error| format!("failed to initialize download quarantine dir: {error}"))?;
        Ok(Self {
            root_dir,
            used_bytes: 0,
            max_bytes: DOWNLOAD_MAX_TOTAL_BYTES_PER_SESSION,
            artifacts: VecDeque::new(),
        })
    }
}

/// Converts an artifact record into its proto form; the source URL is query-redacted.
pub(crate) fn download_artifact_to_proto(
    record: &DownloadArtifactRecord,
) -> browser_v1::DownloadArtifact {
    browser_v1::DownloadArtifact {
        v: CANONICAL_PROTOCOL_MAJOR,
        artifact_id: Some(proto::palyra::common::v1::CanonicalId {
            ulid: record.artifact_id.clone(),
        }),
        session_id: Some(proto::palyra::common::v1::CanonicalId {
            ulid: record.session_id.clone(),
        }),
        profile_id: record
            .profile_id
            .clone()
            .map(|value| proto::palyra::common::v1::CanonicalId { ulid: value }),
        source_url: normalize_url_with_redaction(record.source_url.as_str()),
        file_name: record.file_name.clone(),
        mime_type: record.mime_type.clone(),
        size_bytes: record.size_bytes,
        sha256: record.sha256.clone(),
        created_at_unix_ms: record.created_at_unix_ms,
        quarantined: record.quarantined,
        quarantine_reason: record.quarantine_reason.clone(),
    }
}

/// Loads a stored artifact content prefix, enforcing quarantine and byte caps.
///
/// A `max_bytes` of 0 means "use the global cap"; the effective cap never exceeds
/// `DOWNLOAD_MAX_FILE_BYTES`.
///
/// # Errors
/// Returns an error string when the sandbox or artifact is missing, the artifact is
/// quarantined, or the file read fails.
pub(crate) async fn get_download_artifact_content(
    runtime: &BrowserRuntimeState,
    session_id: &str,
    artifact_id: &str,
    max_bytes: u64,
) -> Result<DownloadArtifactContent, String> {
    let max_bytes = if max_bytes == 0 {
        DOWNLOAD_MAX_FILE_BYTES
    } else {
        max_bytes.min(DOWNLOAD_MAX_FILE_BYTES)
    };
    let artifact = {
        let guard = runtime.download_sessions.lock().await;
        let sandbox = guard
            .get(session_id)
            .ok_or_else(|| "download sandbox is not active for this session".to_owned())?;
        sandbox
            .artifacts
            .iter()
            .find(|record| record.artifact_id == artifact_id)
            .cloned()
            .ok_or_else(|| "download artifact not found".to_owned())?
    };
    if artifact.quarantined {
        return Err(format!(
            "download artifact '{}' is quarantined: {}",
            artifact.artifact_id, artifact.quarantine_reason
        ));
    }
    let storage_path = artifact.storage_path.clone();
    let artifact_id = artifact.artifact_id.clone();
    let content = tokio::task::spawn_blocking(move || {
        let file = fs::File::open(storage_path.as_path()).map_err(|error| {
            format!("failed to read download artifact '{artifact_id}' from storage: {error}")
        })?;
        let mut reader = file.take(max_bytes);
        let mut content = Vec::new();
        reader.read_to_end(&mut content).map_err(|error| {
            format!("failed to read download artifact '{artifact_id}' from storage: {error}")
        })?;
        Ok::<Vec<u8>, String>(content)
    })
    .await
    .map_err(|error| format!("download artifact read task failed: {error}"))??;
    let returned_bytes = u64::try_from(content.len()).unwrap_or(u64::MAX);
    Ok(DownloadArtifactContent {
        truncated: artifact.size_bytes > returned_bytes,
        artifact,
        content,
        offset_bytes: 0,
        limit_bytes: max_bytes,
    })
}

/// Captures the download a click on `selector` would trigger, based on the page snapshot.
///
/// Private-profile sessions fetch without profile attribution so the artifact is not linked to
/// the profile.
///
/// # Errors
/// Returns an error string when the selector does not resolve to a download source tag, the
/// target cannot be resolved, or the fetch/storage pipeline fails.
pub(crate) async fn capture_download_artifact_for_click(
    runtime: &BrowserRuntimeState,
    session_id: &str,
    selector: &str,
    context: &ActionSessionSnapshot,
    timeout_ms: u64,
) -> Result<DownloadArtifactRecord, String> {
    let Some(tag) = find_matching_html_tag(selector, context.page_body.as_str()) else {
        return Err("failed to resolve download source tag for click selector".to_owned());
    };
    let (source_url, file_name) =
        resolve_download_target(tag.as_str(), context.current_url.as_deref())?;
    fetch_download_artifact(
        runtime,
        session_id,
        if context.private_profile { None } else { context.profile_id.as_deref() },
        source_url.as_str(),
        file_name.as_str(),
        context.allow_private_targets,
        timeout_ms,
    )
    .await
}

/// Fetches `source_url` and stores it only when the response is an HTTP attachment.
///
/// Returns `Ok(None)` when the response lacks an attachment Content-Disposition, so callers
/// can fall back to regular navigation handling.
///
/// # Errors
/// Returns an error string when URL validation, the request, byte limits, or sandbox storage
/// fail.
pub(crate) async fn fetch_http_attachment_download_artifact(
    runtime: &BrowserRuntimeState,
    session_id: &str,
    profile_id: Option<&str>,
    source_url: &str,
    file_name: &str,
    allow_private_targets: bool,
    timeout_ms: u64,
) -> Result<Option<DownloadArtifactRecord>, String> {
    fetch_download_artifact_inner(DownloadArtifactFetch {
        runtime,
        session_id,
        profile_id,
        source_url,
        file_name,
        allow_private_targets,
        timeout_ms,
        mode: DownloadCaptureMode::HttpAttachmentOnly,
    })
    .await
}

/// Stores an in-memory payload (e.g. a rendered PDF) as a download artifact.
///
/// Creates the session sandbox on demand; the same quarantine and size rules apply as for
/// fetched downloads, and the oldest artifacts are evicted when the per-session count cap is
/// reached.
///
/// # Errors
/// Returns an error string when the payload exceeds the file byte cap, the sandbox byte budget
/// would be exceeded, or sandbox allocation/writing fails.
pub(crate) async fn store_generated_artifact(
    runtime: &BrowserRuntimeState,
    session_id: &str,
    profile_id: Option<&str>,
    source_url: &str,
    file_name: &str,
    mime_type: &str,
    body: &[u8],
) -> Result<DownloadArtifactRecord, String> {
    let quarantine_reason = download_quarantine_reason(file_name, mime_type);
    let quarantined = quarantine_reason.is_some();
    let quarantine_reason = quarantine_reason.unwrap_or_default();

    if body.len() as u64 > DOWNLOAD_MAX_FILE_BYTES {
        return Err(format!(
            "download exceeds max file bytes ({} > {})",
            body.len(),
            DOWNLOAD_MAX_FILE_BYTES
        ));
    }

    let artifact_id = Ulid::new().to_string();
    let sanitized_name = sanitize_download_file_name(file_name);
    let stored_name = format!("{}-{}", artifact_id, sanitized_name);
    let mut guard = runtime.download_sessions.lock().await;
    let sandbox = match guard.get_mut(session_id) {
        Some(value) => value,
        None => {
            let sandbox = DownloadSandboxSession::new()?;
            guard.insert(session_id.to_owned(), sandbox);
            guard
                .get_mut(session_id)
                .ok_or_else(|| "download sandbox is not active for this session".to_owned())?
        }
    };
    if sandbox.used_bytes.saturating_add(body.len() as u64) > sandbox.max_bytes {
        return Err(format!(
            "download sandbox size limit exceeded ({} > {})",
            sandbox.used_bytes.saturating_add(body.len() as u64),
            sandbox.max_bytes
        ));
    }
    while sandbox.artifacts.len() >= MAX_DOWNLOAD_ARTIFACTS_PER_SESSION {
        if let Some(removed) = sandbox.artifacts.pop_front() {
            let _ = fs::remove_file(removed.storage_path.as_path());
            sandbox.used_bytes = sandbox.used_bytes.saturating_sub(removed.size_bytes);
        }
    }
    let target_dir = if quarantined {
        sandbox.root_dir.path().join(DOWNLOADS_DIR_QUARANTINE)
    } else {
        sandbox.root_dir.path().join(DOWNLOADS_DIR_ALLOWLIST)
    };
    let storage_path = target_dir.join(stored_name);
    fs::write(storage_path.as_path(), body).map_err(|error| {
        format!("failed to persist generated artifact to '{}' : {error}", storage_path.display())
    })?;
    sandbox.used_bytes = sandbox.used_bytes.saturating_add(body.len() as u64);
    let artifact = DownloadArtifactRecord {
        artifact_id,
        session_id: session_id.to_owned(),
        profile_id: profile_id.map(str::to_owned),
        source_url: source_url.to_owned(),
        file_name: sanitized_name,
        mime_type: mime_type.to_owned(),
        size_bytes: body.len() as u64,
        sha256: sha256_hex(body),
        created_at_unix_ms: current_unix_ms(),
        quarantined,
        quarantine_reason,
        storage_path,
    };
    sandbox.artifacts.push_back(artifact.clone());
    Ok(artifact)
}

/// Resolves a download anchor tag's href (absolute or relative) and target file name.
///
/// An explicit `download` attribute names the file; otherwise the name is inferred from the
/// resolved URL's last path segment.
///
/// # Errors
/// Returns an error string when the href is missing/empty or relative resolution fails (no or
/// invalid current page URL).
pub(crate) fn resolve_download_target(
    tag: &str,
    current_url: Option<&str>,
) -> Result<(String, String), String> {
    let href = extract_attr_value_case_insensitive(tag, "href")
        .ok_or_else(|| "download-like element is missing href".to_owned())?;
    if href.trim().is_empty() {
        return Err("download-like element has an empty href".to_owned());
    }
    let resolved_url = if let Ok(url) = Url::parse(href.as_str()) {
        url.to_string()
    } else {
        let Some(base) = current_url else {
            return Err("download URL is relative but current page URL is unavailable".to_owned());
        };
        let base_url =
            Url::parse(base).map_err(|error| format!("invalid active page URL: {error}"))?;
        base_url
            .join(href.as_str())
            .map_err(|error| format!("failed to resolve relative download URL: {error}"))?
            .to_string()
    };
    let explicit_name = extract_attr_value_case_insensitive(tag, "download").unwrap_or_default();
    let file_name = if explicit_name.trim().is_empty() {
        infer_download_file_name(resolved_url.as_str())
    } else {
        sanitize_download_file_name(explicit_name.as_str())
    };
    Ok((resolved_url, file_name))
}

/// Derives a sanitized file name from a URL's last path segment, with a fixed fallback.
pub(crate) fn infer_download_file_name(raw_url: &str) -> String {
    let Ok(url) = Url::parse(raw_url) else {
        return DOWNLOAD_FILE_NAME_FALLBACK.to_owned();
    };
    let Some(value) = url
        .path_segments()
        .and_then(|mut segments| segments.next_back())
        .filter(|segment| !segment.trim().is_empty())
    else {
        return DOWNLOAD_FILE_NAME_FALLBACK.to_owned();
    };
    sanitize_download_file_name(value)
}

/// Reports whether a Content-Disposition header declares an attachment.
pub(crate) fn content_disposition_is_attachment(raw: &str) -> bool {
    raw.split(';').next().is_some_and(|value| value.trim().eq_ignore_ascii_case("attachment"))
}

/// Extracts a sanitized file name from an attachment Content-Disposition header.
///
/// Prefers the RFC 5987 `filename*` form over plain `filename`; returns `None` for
/// non-attachment dispositions or when no usable name is present.
pub(crate) fn content_disposition_attachment_file_name(raw: &str) -> Option<String> {
    if !content_disposition_is_attachment(raw) {
        return None;
    }
    let mut fallback = None;
    for part in raw.split(';').skip(1) {
        let Some((name, value)) = part.split_once('=') else {
            continue;
        };
        let name = name.trim();
        let value = unquote_content_disposition_value(value.trim());
        if name.eq_ignore_ascii_case("filename*") {
            if let Some(decoded) = decode_rfc5987_filename(value.as_str()) {
                return Some(sanitize_download_file_name(decoded.as_str()));
            }
        } else if name.eq_ignore_ascii_case("filename") && fallback.is_none() {
            let sanitized = sanitize_download_file_name(value.as_str());
            if !sanitized.trim().is_empty() {
                fallback = Some(sanitized);
            }
        }
    }
    fallback
}

fn unquote_content_disposition_value(raw: &str) -> String {
    let trimmed = raw.trim();
    if trimmed.len() < 2 || !trimmed.starts_with('"') || !trimmed.ends_with('"') {
        return trimmed.to_owned();
    }
    let mut output = String::new();
    let mut escaped = false;
    for character in trimmed[1..trimmed.len() - 1].chars() {
        if escaped {
            output.push(character);
            escaped = false;
            continue;
        }
        if character == '\\' {
            escaped = true;
            continue;
        }
        output.push(character);
    }
    output
}

fn decode_rfc5987_filename(raw: &str) -> Option<String> {
    let (_, encoded) = raw.split_once("''")?;
    let bytes = percent_decode_ascii(encoded)?;
    String::from_utf8(bytes).ok().filter(|value| !value.trim().is_empty())
}

fn percent_decode_ascii(raw: &str) -> Option<Vec<u8>> {
    let bytes = raw.as_bytes();
    let mut output = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'%' {
            let high = *bytes.get(index + 1)?;
            let low = *bytes.get(index + 2)?;
            output.push(hex_value(high)?.saturating_mul(16).saturating_add(hex_value(low)?));
            index += 3;
        } else {
            output.push(bytes[index]);
            index += 1;
        }
    }
    Some(output)
}

fn hex_value(value: u8) -> Option<u8> {
    match value {
        b'0'..=b'9' => Some(value - b'0'),
        b'a'..=b'f' => Some(value - b'a' + 10),
        b'A'..=b'F' => Some(value - b'A' + 10),
        _ => None,
    }
}

/// Reduces a file name to a safe ASCII subset, falling back to a fixed name when empty.
///
/// Leading/trailing dots and underscores are stripped so the result can never be a dotfile or
/// a traversal component; the result is capped at 96 bytes.
pub(crate) fn sanitize_download_file_name(raw: &str) -> String {
    let mut sanitized = raw
        .chars()
        .map(|value| {
            if value.is_ascii_alphanumeric() || matches!(value, '.' | '-' | '_') {
                value
            } else {
                '_'
            }
        })
        .collect::<String>();
    sanitized = sanitized.trim_matches('.').trim_matches('_').to_owned();
    if sanitized.is_empty() {
        return DOWNLOAD_FILE_NAME_FALLBACK.to_owned();
    }
    truncate_utf8_bytes(sanitized.as_str(), 96)
}

/// Determines a download's MIME type from the response header, magic bytes, then extension.
///
/// A non-empty declared Content-Type always wins; magic-byte sniffing only runs without one.
/// Container formats (ZIP, MP4 `ftyp`) defer to the extension mapping for the specific subtype.
pub(crate) fn sniff_download_mime_type(
    header_content_type: Option<&str>,
    file_name: &str,
    bytes: &[u8],
) -> String {
    let extension = Path::new(file_name)
        .extension()
        .and_then(|value| value.to_str())
        .map(|value| value.to_ascii_lowercase())
        .unwrap_or_default();
    if let Some(content_type) = header_content_type {
        let normalized =
            content_type.split(';').next().unwrap_or_default().trim().to_ascii_lowercase();
        if !normalized.is_empty() {
            return normalized;
        }
    }
    if bytes.starts_with(b"%PDF-") {
        return "application/pdf".to_owned();
    }
    if bytes.starts_with(&[0x89, b'P', b'N', b'G', 0x0D, 0x0A, 0x1A, 0x0A]) {
        return "image/png".to_owned();
    }
    if bytes.starts_with(&[0xFF, 0xD8, 0xFF]) {
        return "image/jpeg".to_owned();
    }
    if bytes.starts_with(b"GIF87a") || bytes.starts_with(b"GIF89a") {
        return "image/gif".to_owned();
    }
    if bytes.len() >= 12 && bytes.starts_with(b"RIFF") && bytes[8..12] == *b"WEBP" {
        return "image/webp".to_owned();
    }
    if bytes.len() >= 12 && bytes[4..8] == *b"ftyp" {
        return mime_type_for_download_extension(extension.as_str())
            .unwrap_or("video/mp4")
            .to_owned();
    }
    if bytes.starts_with(&[0x50, 0x4B, 0x03, 0x04]) {
        return mime_type_for_download_extension(extension.as_str())
            .unwrap_or("application/zip")
            .to_owned();
    }
    if bytes.starts_with(&[0x1F, 0x8B]) {
        return "application/gzip".to_owned();
    }
    mime_type_for_download_extension(extension.as_str())
        .unwrap_or("application/octet-stream")
        .to_owned()
}

/// Reports whether a file name's extension is on the download allowlist.
pub(crate) fn extension_is_allowed(file_name: &str) -> bool {
    let extension = Path::new(file_name)
        .extension()
        .and_then(|value| value.to_str())
        .map(|value| value.to_ascii_lowercase())
        .unwrap_or_default();
    DOWNLOAD_ALLOWED_EXTENSIONS.iter().any(|candidate| candidate == &extension)
}

/// Reports whether a MIME type is allowlisted for downloads.
///
/// Whole media families (audio, font, image, text, video) are allowed; application types must
/// be individually allowlisted.
pub(crate) fn mime_type_is_allowed(mime_type: &str) -> bool {
    let normalized = normalize_mime_type(mime_type);
    if normalized.starts_with("audio/")
        || normalized.starts_with("font/")
        || normalized.starts_with("image/")
        || normalized.starts_with("text/")
        || normalized.starts_with("video/")
    {
        return true;
    }
    DOWNLOAD_ALLOWED_MIME_TYPES.contains(&normalized.as_str())
}

// Generic binary MIME types are trusted only when the extension is already allowlisted; the
// returned reasons are pipe-joined stable tokens consumed by clients and tests.
fn download_quarantine_reason(file_name: &str, mime_type: &str) -> Option<String> {
    let extension_allowed = extension_is_allowed(file_name);
    let mime_allowed = mime_type_is_allowed(mime_type)
        || (extension_allowed && mime_type_is_generic_binary(mime_type));
    let mut reasons = Vec::new();
    if !extension_allowed {
        reasons.push("extension_not_allowlisted");
    }
    if !mime_allowed {
        reasons.push("mime_type_not_allowlisted");
    }
    (!reasons.is_empty()).then(|| reasons.join("|"))
}

fn normalize_mime_type(mime_type: &str) -> String {
    mime_type.split(';').next().unwrap_or_default().trim().to_ascii_lowercase()
}

fn mime_type_is_generic_binary(mime_type: &str) -> bool {
    matches!(
        normalize_mime_type(mime_type).as_str(),
        "application/octet-stream" | "binary/octet-stream"
    )
}

fn mime_type_for_download_extension(extension: &str) -> Option<&'static str> {
    match extension {
        "7z" => Some("application/x-7z-compressed"),
        "aac" => Some("audio/aac"),
        "avif" => Some("image/avif"),
        "bmp" => Some("image/bmp"),
        "br" => Some("application/x-brotli"),
        "bz2" | "tbz2" => Some("application/x-bzip2"),
        "cjs" | "js" | "mjs" => Some("text/javascript"),
        "conf" | "ini" | "log" | "txt" => Some("text/plain"),
        "css" => Some("text/css"),
        "csv" => Some("text/csv"),
        "doc" => Some("application/msword"),
        "docx" => Some("application/vnd.openxmlformats-officedocument.wordprocessingml.document"),
        "eot" => Some("application/vnd.ms-fontobject"),
        "flac" => Some("audio/flac"),
        "gif" => Some("image/gif"),
        "gz" | "tgz" => Some("application/gzip"),
        "heic" => Some("image/heic"),
        "heif" => Some("image/heif"),
        "htm" | "html" => Some("text/html"),
        "ico" => Some("image/x-icon"),
        "jpeg" | "jpg" => Some("image/jpeg"),
        "json" | "map" => Some("application/json"),
        "jsonl" => Some("application/x-ndjson"),
        "m4a" => Some("audio/mp4"),
        "m4v" | "mp4" => Some("video/mp4"),
        "markdown" | "md" => Some("text/markdown"),
        "mov" => Some("video/quicktime"),
        "mp3" => Some("audio/mpeg"),
        "mpeg" | "mpg" => Some("video/mpeg"),
        "odf" => Some("application/vnd.oasis.opendocument.formula"),
        "odp" => Some("application/vnd.oasis.opendocument.presentation"),
        "ods" => Some("application/vnd.oasis.opendocument.spreadsheet"),
        "odt" => Some("application/vnd.oasis.opendocument.text"),
        "oga" | "ogg" => Some("audio/ogg"),
        "opus" => Some("audio/opus"),
        "otf" => Some("font/otf"),
        "pdf" => Some("application/pdf"),
        "png" => Some("image/png"),
        "ppt" => Some("application/vnd.ms-powerpoint"),
        "pptx" => Some("application/vnd.openxmlformats-officedocument.presentationml.presentation"),
        "rar" => Some("application/vnd.rar"),
        "rtf" => Some("application/rtf"),
        "svg" => Some("image/svg+xml"),
        "tar" => Some("application/x-tar"),
        "tif" | "tiff" => Some("image/tiff"),
        "toml" => Some("application/toml"),
        "tsv" => Some("text/tab-separated-values"),
        "ttf" => Some("font/ttf"),
        "wasm" => Some("application/wasm"),
        "wav" => Some("audio/wav"),
        "webm" => Some("video/webm"),
        "webp" => Some("image/webp"),
        "woff" => Some("font/woff"),
        "woff2" => Some("font/woff2"),
        "xls" => Some("application/vnd.ms-excel"),
        "xlsx" => Some("application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"),
        "xml" => Some("application/xml"),
        "xz" => Some("application/x-xz"),
        "yaml" | "yml" => Some("application/x-yaml"),
        "zip" => Some("application/zip"),
        "zst" => Some("application/zstd"),
        _ => None,
    }
}

/// Fetches `source_url` and stores any successful response as a download artifact.
///
/// # Errors
/// Returns an error string when URL validation, the request, redirect handling, byte limits,
/// or sandbox storage fail.
pub(crate) async fn fetch_download_artifact(
    runtime: &BrowserRuntimeState,
    session_id: &str,
    profile_id: Option<&str>,
    source_url: &str,
    file_name: &str,
    allow_private_targets: bool,
    timeout_ms: u64,
) -> Result<DownloadArtifactRecord, String> {
    fetch_download_artifact_inner(DownloadArtifactFetch {
        runtime,
        session_id,
        profile_id,
        source_url,
        file_name,
        allow_private_targets,
        timeout_ms,
        mode: DownloadCaptureMode::AnySuccessfulResponse,
    })
    .await?
    .ok_or_else(|| "download response was not captured".to_owned())
}

/// Controls which HTTP responses `fetch_download_artifact_inner` is allowed to capture.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DownloadCaptureMode {
    AnySuccessfulResponse,
    HttpAttachmentOnly,
}

struct DownloadArtifactFetch<'a> {
    runtime: &'a BrowserRuntimeState,
    session_id: &'a str,
    profile_id: Option<&'a str>,
    source_url: &'a str,
    file_name: &'a str,
    allow_private_targets: bool,
    timeout_ms: u64,
    mode: DownloadCaptureMode,
}

async fn fetch_download_artifact_inner(
    request: DownloadArtifactFetch<'_>,
) -> Result<Option<DownloadArtifactRecord>, String> {
    let DownloadArtifactFetch {
        runtime,
        session_id,
        profile_id,
        source_url,
        file_name,
        allow_private_targets,
        timeout_ms,
        mode,
    } = request;

    let mut current_url =
        Url::parse(source_url).map_err(|error| format!("invalid download URL: {error}"))?;
    let mut redirects = 0_u32;
    // Redirects are followed manually (capped at 3) so every hop is re-validated against the
    // private-target policy and fetched with a freshly pinned client: a redirect must not be
    // able to steer the download into a private address space.
    let response = loop {
        let validated_target = validate_target_url(&current_url, allow_private_targets).await?;
        let client = build_pinned_http_client(timeout_ms, &validated_target)
            .map_err(|error| format!("failed to build download HTTP client: {error}"))?;
        let response = client
            .get(current_url.clone())
            .send()
            .await
            .map_err(|error| format!("download request failed: {error}"))?;
        if response.status().is_redirection() {
            if redirects >= 3 {
                return Err("download redirect limit exceeded (3)".to_owned());
            }
            let Some(location) = response.headers().get(reqwest::header::LOCATION) else {
                return Err("download redirect missing Location header".to_owned());
            };
            let location = location
                .to_str()
                .map_err(|_| "download redirect location header is invalid UTF-8".to_owned())?;
            current_url = current_url
                .join(location)
                .map_err(|error| format!("invalid download redirect target: {error}"))?;
            redirects = redirects.saturating_add(1);
            continue;
        }
        break response;
    };

    if !response.status().is_success() {
        return Err(format!("download request returned HTTP {}", response.status().as_u16()));
    }
    let content_disposition = response
        .headers()
        .get(reqwest::header::CONTENT_DISPOSITION)
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);
    if mode == DownloadCaptureMode::HttpAttachmentOnly
        && !content_disposition.as_deref().is_some_and(content_disposition_is_attachment)
    {
        return Ok(None);
    }
    let header_content_type = response
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);
    let content_length = response.content_length();
    if matches!(content_length, Some(length) if length > DOWNLOAD_MAX_FILE_BYTES) {
        return Err(format!(
            "download exceeds max file bytes ({} > {})",
            content_length.unwrap_or_default(),
            DOWNLOAD_MAX_FILE_BYTES
        ));
    }
    let mut response = response;
    let mut body = Vec::with_capacity(
        content_length
            .map(|length| length.min(DOWNLOAD_MAX_FILE_BYTES) as usize)
            .unwrap_or_default(),
    );
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|error| format!("failed to read download response body: {error}"))?
    {
        let next_size = (body.len() as u64).saturating_add(chunk.len() as u64);
        if next_size > DOWNLOAD_MAX_FILE_BYTES {
            return Err(format!(
                "download exceeds max file bytes ({} > {})",
                next_size, DOWNLOAD_MAX_FILE_BYTES
            ));
        }
        body.extend_from_slice(chunk.as_ref());
    }
    let file_name = content_disposition
        .as_deref()
        .and_then(content_disposition_attachment_file_name)
        .unwrap_or_else(|| sanitize_download_file_name(file_name));
    let mime_type =
        sniff_download_mime_type(header_content_type.as_deref(), file_name.as_str(), body.as_ref());
    let quarantine_reason = download_quarantine_reason(file_name.as_str(), mime_type.as_str());
    let quarantined = quarantine_reason.is_some();
    let quarantine_reason = quarantine_reason.unwrap_or_default();

    let artifact_id = Ulid::new().to_string();
    let sanitized_name = sanitize_download_file_name(file_name.as_str());
    let stored_name = format!("{}-{}", artifact_id, sanitized_name);
    let mut guard = runtime.download_sessions.lock().await;
    let Some(sandbox) = guard.get_mut(session_id) else {
        return Err("download sandbox is not active for this session".to_owned());
    };
    if sandbox.used_bytes.saturating_add(body.len() as u64) > sandbox.max_bytes {
        return Err(format!(
            "download sandbox size limit exceeded ({} > {})",
            sandbox.used_bytes.saturating_add(body.len() as u64),
            sandbox.max_bytes
        ));
    }
    while sandbox.artifacts.len() >= MAX_DOWNLOAD_ARTIFACTS_PER_SESSION {
        if let Some(removed) = sandbox.artifacts.pop_front() {
            let _ = fs::remove_file(removed.storage_path.as_path());
            sandbox.used_bytes = sandbox.used_bytes.saturating_sub(removed.size_bytes);
        }
    }
    let target_dir = if quarantined {
        sandbox.root_dir.path().join(DOWNLOADS_DIR_QUARANTINE)
    } else {
        sandbox.root_dir.path().join(DOWNLOADS_DIR_ALLOWLIST)
    };
    let storage_path = target_dir.join(stored_name);
    fs::write(storage_path.as_path(), body.as_slice()).map_err(|error| {
        format!("failed to persist downloaded artifact to '{}' : {error}", storage_path.display())
    })?;
    sandbox.used_bytes = sandbox.used_bytes.saturating_add(body.len() as u64);
    let artifact = DownloadArtifactRecord {
        artifact_id,
        session_id: session_id.to_owned(),
        profile_id: profile_id.map(str::to_owned),
        source_url: current_url.to_string(),
        file_name: sanitized_name,
        mime_type,
        size_bytes: body.len() as u64,
        sha256: sha256_hex(body.as_ref()),
        created_at_unix_ms: current_unix_ms(),
        quarantined,
        quarantine_reason,
        storage_path,
    };
    sandbox.artifacts.push_back(artifact.clone());
    Ok(Some(artifact))
}

// Chromium sessions may have retained an allowance for a specific local origin (e.g. a page the
// browser itself was permitted to reach); downloads from that same origin reuse it.
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn download_allowlist_accepts_common_user_file_types() {
        for file_name in [
            "photo.png",
            "diagram.svg",
            "report.docx",
            "spreadsheet.xlsx",
            "slides.pptx",
            "archive.7z",
            "clip.mp4",
            "font.woff2",
            "notes.md",
        ] {
            assert!(extension_is_allowed(file_name), "{file_name} should be extension-allowed");
        }

        for mime_type in [
            "image/png",
            "image/svg+xml",
            "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            "application/vnd.openxmlformats-officedocument.presentationml.presentation",
            "application/x-7z-compressed",
            "video/mp4",
            "font/woff2",
            "text/markdown",
        ] {
            assert!(mime_type_is_allowed(mime_type), "{mime_type} should be MIME-allowed");
        }
    }

    #[test]
    fn download_allowlist_accepts_generic_binary_for_allowed_extensions_only() {
        assert_eq!(download_quarantine_reason("report.xlsx", "application/octet-stream"), None);
        assert_eq!(
            download_quarantine_reason("payload.exe", "application/octet-stream").as_deref(),
            Some("extension_not_allowlisted|mime_type_not_allowlisted")
        );
    }

    #[test]
    fn download_sniffing_maps_common_extensions_without_content_type() {
        assert_eq!(
            sniff_download_mime_type(None, "photo.png", b"\x89PNG\r\n\x1A\npayload"),
            "image/png"
        );
        assert_eq!(
            sniff_download_mime_type(None, "report.docx", b"PK\x03\x04"),
            "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
        );
        assert_eq!(sniff_download_mime_type(None, "notes.md", b"# Notes"), "text/markdown");
    }

    #[test]
    fn content_disposition_attachment_filename_supports_quoted_and_extended_forms() {
        assert_eq!(
            content_disposition_attachment_file_name(
                "attachment; filename=\"palyra-orders-export.csv\""
            )
            .as_deref(),
            Some("palyra-orders-export.csv")
        );
        assert_eq!(
            content_disposition_attachment_file_name(
                "attachment; filename*=UTF-8''palyra%20orders.csv; filename=fallback.csv"
            )
            .as_deref(),
            Some("palyra_orders.csv")
        );
        assert_eq!(content_disposition_attachment_file_name("inline; filename=x.csv"), None);
    }

    #[tokio::test]
    async fn scoped_private_target_policy_does_not_upgrade_download_fetch_permission() {
        let policy = std::sync::Arc::new(ChromiumPrivateTargetPolicy::new(false));
        let _scoped = policy
            .scoped_url_allowance("http://127.0.0.1:43191/")
            .expect("scoped local browser allowance should parse")
            .expect("local browser target should create a scoped allowance");
        let download_url =
            Url::parse("http://127.0.0.1:43191/export.csv").expect("URL should parse");

        assert!(
            policy.allows_url(download_url.as_str()),
            "setup must prove Chromium temporarily allows the same-host private target"
        );
        let denied = validate_target_url(&download_url, false)
            .await
            .expect_err("download fetch must honor the current explicit private-target denial");
        assert!(denied.contains("private"), "unexpected validation error: {denied}");
        validate_target_url(&download_url, true)
            .await
            .expect("explicit current allow_private_targets=true should still permit the target");
    }
}
