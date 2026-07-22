//! Tool-result artifact support: bounded journal payloads and the
//! `palyra.artifact.read` tool backend.
//!
//! [`bounded_tool_result_artifact_content`] caps oversized tool outputs into
//! a valid-JSON truncation envelope before journal storage, and
//! [`execute_artifact_read_tool`] serves scoped artifact reads back to the
//! model, downgrading sensitive full reads to redacted text previews so the
//! content stays model-visible instead of failing outright.

use std::sync::Arc;

use palyra_common::runtime_contracts::{ArtifactReadRequest, ToolTurnBudget};
use serde_json::json;

use crate::{
    gateway::{GatewayRuntimeState, ToolRuntimeExecutionContext, ARTIFACT_READ_TOOL_NAME},
    journal::ToolResultArtifactReadRequest,
    tool_protocol::{build_tool_execution_outcome, ToolExecutionOutcome},
};

const TOOL_RESULT_ARTIFACT_TRUNCATION_RESERVE_BYTES: usize = 2 * 1024;
// PDF and transcript extraction front-ends are intentionally not wired here;
// these projection contracts stay unit-tested until a parser backend calls them.
#[allow(dead_code)]
const PDF_EXTRACTION_SCHEMA_VERSION: u64 = 1;
const TOOL_RESULT_ARTIFACT_TRUNCATION_MESSAGE: &str = "Original tool output exceeded the journal artifact payload limit; this artifact stores a bounded UTF-8 prefix.";
// Must stay byte-identical to the denial reason emitted by the journal's
// artifact read path; the preview retry below matches on it by substring.
const SENSITIVE_FULL_READ_DENIED_REASON: &str = "sensitive_full_read_denied";

/// Journal-ready artifact payload produced by
/// [`bounded_tool_result_artifact_content`].
///
/// When `truncated` is true, `content` is a JSON truncation envelope rather
/// than the raw tool output.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct BoundedToolResultArtifactContent {
    pub content: Vec<u8>,
    pub original_output_bytes: usize,
    pub stored_output_bytes: usize,
    pub truncated: bool,
}

/// Inclusive page range requested from a PDF extraction.
#[allow(dead_code)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PdfExtractionPageRange {
    pub start_page: u32,
    pub end_page: u32,
}

/// Text extracted from a single PDF page by an upstream parser.
#[allow(dead_code)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PdfPageText {
    pub page: u32,
    pub text: String,
}

/// Stable citation anchor for one extracted page.
#[allow(dead_code)]
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub(crate) struct PdfExtractionAnchor {
    pub source_ref: String,
    pub artifact_id: String,
    pub page: u32,
    pub run_id: String,
    pub turn_id: String,
    pub tool_call_id: String,
}

/// One bounded PDF page extraction result.
#[allow(dead_code)]
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub(crate) struct PdfExtractionPage {
    pub page: u32,
    pub text: String,
    pub truncated: bool,
    pub anchor: PdfExtractionAnchor,
}

/// Budgeted PDF extraction projection returned to model-visible surfaces.
#[allow(dead_code)]
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub(crate) struct PdfExtractionProjection {
    pub schema_version: u64,
    pub artifact_id: String,
    pub pages: Vec<PdfExtractionPage>,
    pub returned_chars: usize,
    pub truncated: bool,
    pub next_page: Option<u32>,
    pub trust_label: String,
    pub redaction_level: String,
}

/// Stable citation anchor for transcript-backed reads and searches.
#[allow(dead_code)]
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub(crate) struct TranscriptCitationAnchor {
    pub source_ref: String,
    pub session_id: String,
    pub run_id: String,
    pub turn_id: String,
    pub tool_call_id: Option<String>,
    pub timestamp_unix_ms: i64,
}

/// Bounds a tool output to `max_payload_bytes` for journal artifact storage.
///
/// Outputs within the limit are stored raw; larger outputs are replaced by a
/// valid-JSON envelope carrying a bounded UTF-8 prefix plus truncation
/// metadata, so downstream consumers never see partial, unparseable JSON.
///
/// # Errors
/// Returns an error when the envelope cannot be serialized or when
/// `max_payload_bytes` is too small to hold even the metadata-only envelope.
pub(crate) fn bounded_tool_result_artifact_content(
    output_json: &[u8],
    max_payload_bytes: usize,
) -> Result<BoundedToolResultArtifactContent, String> {
    if output_json.len() <= max_payload_bytes {
        return Ok(BoundedToolResultArtifactContent {
            content: output_json.to_vec(),
            original_output_bytes: output_json.len(),
            stored_output_bytes: output_json.len(),
            truncated: false,
        });
    }

    // JSON string escaping can expand the stored prefix well past its raw
    // byte length, so reserve headroom for the envelope and halve the prefix
    // until the serialized payload actually fits.
    let mut prefix_limit = output_json
        .len()
        .min(max_payload_bytes.saturating_sub(TOOL_RESULT_ARTIFACT_TRUNCATION_RESERVE_BYTES));
    loop {
        let stored_prefix = truncate_utf8_lossy(output_json, prefix_limit);
        let stored_output_bytes = stored_prefix.len();
        let content = serde_json::to_vec(&json!({
            "schema_version": 1,
            "artifact_content_truncated": true,
            "original_output_bytes": output_json.len(),
            "stored_output_bytes": stored_output_bytes,
            "stored_output_utf8_prefix": stored_prefix,
            "message": TOOL_RESULT_ARTIFACT_TRUNCATION_MESSAGE,
        }))
        .map_err(|error| format!("failed to serialize truncated tool result artifact: {error}"))?;
        if content.len() <= max_payload_bytes {
            return Ok(BoundedToolResultArtifactContent {
                content,
                original_output_bytes: output_json.len(),
                stored_output_bytes,
                truncated: true,
            });
        }
        if prefix_limit == 0 {
            break;
        }
        prefix_limit /= 2;
    }

    let content = serde_json::to_vec(&json!({
        "schema_version": 1,
        "artifact_content_truncated": true,
        "original_output_bytes": output_json.len(),
        "stored_output_bytes": 0,
        "message": TOOL_RESULT_ARTIFACT_TRUNCATION_MESSAGE,
    }))
    .map_err(|error| format!("failed to serialize truncated tool result artifact: {error}"))?;
    if content.len() <= max_payload_bytes {
        Ok(BoundedToolResultArtifactContent {
            content,
            original_output_bytes: output_json.len(),
            stored_output_bytes: 0,
            truncated: true,
        })
    } else {
        Err(format!(
            "tool result artifact payload limit ({max_payload_bytes} bytes) is too small to store truncation metadata"
        ))
    }
}

fn truncate_utf8_lossy(output_json: &[u8], max_bytes: usize) -> String {
    let value = String::from_utf8_lossy(output_json);
    if value.len() <= max_bytes {
        return value.into_owned();
    }
    value
        .char_indices()
        .take_while(|(index, ch)| index.saturating_add(ch.len_utf8()) <= max_bytes)
        .map(|(_, ch)| ch)
        .collect()
}

/// Projects already-extracted PDF page text into a bounded, page-anchored
/// response. The parser stays outside this function; this layer enforces
/// model-visible budget, stable citation anchors, and trust labeling.
#[must_use]
#[allow(dead_code)]
pub(crate) fn build_pdf_extraction_projection(
    artifact_id: &str,
    run_id: &str,
    turn_id: &str,
    tool_call_id: &str,
    pages: &[PdfPageText],
    page_ranges: &[PdfExtractionPageRange],
    max_chars: usize,
) -> PdfExtractionProjection {
    let mut returned_chars = 0usize;
    let mut projected_pages = Vec::new();
    let mut truncated = false;
    let mut next_page = None;
    for page in pages.iter().filter(|page| page_selected(page.page, page_ranges)) {
        if returned_chars >= max_chars {
            truncated = true;
            next_page = Some(page.page);
            break;
        }
        let remaining = max_chars.saturating_sub(returned_chars);
        let (text, page_truncated) = truncate_chars(page.text.as_str(), remaining);
        returned_chars = returned_chars.saturating_add(text.chars().count());
        truncated |= page_truncated;
        if page_truncated {
            next_page = Some(page.page);
        }
        projected_pages.push(PdfExtractionPage {
            page: page.page,
            text,
            truncated: page_truncated,
            anchor: pdf_extraction_anchor(artifact_id, run_id, turn_id, tool_call_id, page.page),
        });
        if page_truncated {
            break;
        }
    }
    PdfExtractionProjection {
        schema_version: PDF_EXTRACTION_SCHEMA_VERSION,
        artifact_id: artifact_id.to_owned(),
        pages: projected_pages,
        returned_chars,
        truncated,
        next_page,
        trust_label: "extracted_document_text".to_owned(),
        redaction_level: "bounded_text".to_owned(),
    }
}

/// Builds a stable transcript citation anchor for old turns returned by
/// transcript read/search surfaces.
#[must_use]
#[allow(dead_code)]
pub(crate) fn transcript_citation_anchor(
    session_id: &str,
    run_id: &str,
    turn_id: &str,
    tool_call_id: Option<&str>,
    timestamp_unix_ms: i64,
) -> TranscriptCitationAnchor {
    let tool_part = tool_call_id.unwrap_or("none");
    TranscriptCitationAnchor {
        source_ref: format!(
            "transcript:{session_id}:run:{run_id}:turn:{turn_id}:tool:{tool_part}:ts:{timestamp_unix_ms}"
        ),
        session_id: session_id.to_owned(),
        run_id: run_id.to_owned(),
        turn_id: turn_id.to_owned(),
        tool_call_id: tool_call_id.map(ToOwned::to_owned),
        timestamp_unix_ms,
    }
}

#[allow(dead_code)]
fn page_selected(page: u32, page_ranges: &[PdfExtractionPageRange]) -> bool {
    page_ranges.is_empty()
        || page_ranges.iter().any(|range| page >= range.start_page && page <= range.end_page)
}

#[allow(dead_code)]
fn pdf_extraction_anchor(
    artifact_id: &str,
    run_id: &str,
    turn_id: &str,
    tool_call_id: &str,
    page: u32,
) -> PdfExtractionAnchor {
    PdfExtractionAnchor {
        source_ref: format!(
            "artifact:{artifact_id}:page:{page}:run:{run_id}:turn:{turn_id}:tool:{tool_call_id}"
        ),
        artifact_id: artifact_id.to_owned(),
        page,
        run_id: run_id.to_owned(),
        turn_id: turn_id.to_owned(),
        tool_call_id: tool_call_id.to_owned(),
    }
}

#[allow(dead_code)]
fn truncate_chars(value: &str, max_chars: usize) -> (String, bool) {
    let mut chars = value.chars();
    let text = chars.by_ref().take(max_chars).collect::<String>();
    let truncated = chars.next().is_some();
    (text, truncated)
}

/// Executes a `palyra.artifact.read` tool call against the journal.
///
/// The read is scoped to the caller's session/run/principal/device taken from
/// `context` (never from model input), and the requested byte budget is
/// clamped to [`ToolTurnBudget::max_artifact_read_bytes`]. When the journal
/// denies a full read of content whose sensitivity allows redacted previews,
/// the call is retried once as a bounded text preview.
/// All failures are reported as unsuccessful [`ToolExecutionOutcome`]s rather
/// than errors, keeping the tool loop alive.
pub(crate) async fn execute_artifact_read_tool(
    runtime_state: &Arc<GatewayRuntimeState>,
    context: ToolRuntimeExecutionContext<'_>,
    proposal_id: &str,
    input_json: &[u8],
) -> ToolExecutionOutcome {
    let request = match serde_json::from_slice::<ArtifactReadRequest>(input_json) {
        Ok(request) if !request.artifact_id.trim().is_empty() => request,
        Ok(_) => {
            return artifact_read_outcome(
                proposal_id,
                input_json,
                false,
                b"{}".to_vec(),
                "palyra.artifact.read requires non-empty artifact_id".to_owned(),
            );
        }
        Err(error) => {
            return artifact_read_outcome(
                proposal_id,
                input_json,
                false,
                b"{}".to_vec(),
                format!("palyra.artifact.read input must match artifact read schema: {error}"),
            );
        }
    };

    let budget = ToolTurnBudget::default();
    let requested_max = usize::try_from(request.max_bytes)
        .ok()
        .filter(|value| *value > 0)
        .unwrap_or(budget.max_artifact_read_bytes)
        .min(budget.max_artifact_read_bytes);

    let read = ToolResultArtifactReadRequest {
        artifact_id: request.artifact_id,
        session_id: context.session_id.to_owned(),
        run_id: context.run_id.to_owned(),
        principal: context.principal.to_owned(),
        device_id: context.device_id.to_owned(),
        channel: context.channel.map(ToOwned::to_owned),
        expected_digest_sha256: request.expected_digest_sha256,
        offset_bytes: request.offset_bytes,
        max_bytes: requested_max,
        text_preview: request.text_preview,
    };

    match runtime_state.read_tool_result_artifact(read.clone()).await {
        Ok(response) => serialize_artifact_read_success(proposal_id, input_json, response),
        Err(status) => {
            if should_retry_artifact_read_as_text_preview(&read, &status) {
                let preview_read = ToolResultArtifactReadRequest { text_preview: true, ..read };
                match runtime_state.read_tool_result_artifact(preview_read).await {
                    Ok(response) => {
                        return serialize_artifact_read_success(proposal_id, input_json, response);
                    }
                    Err(preview_status) => {
                        return artifact_read_error_outcome(
                            proposal_id,
                            input_json,
                            &preview_status,
                        );
                    }
                }
            }
            artifact_read_error_outcome(proposal_id, input_json, &status)
        }
    }
}

fn serialize_artifact_read_success(
    proposal_id: &str,
    input_json: &[u8],
    response: palyra_common::runtime_contracts::ArtifactReadResponse,
) -> ToolExecutionOutcome {
    match serde_json::to_vec(&response) {
        Ok(output_json) => {
            artifact_read_outcome(proposal_id, input_json, true, output_json, String::new())
        }
        Err(error) => artifact_read_outcome(
            proposal_id,
            input_json,
            false,
            b"{}".to_vec(),
            format!("failed to serialize artifact read response: {error}"),
        ),
    }
}

fn should_retry_artifact_read_as_text_preview(
    request: &ToolResultArtifactReadRequest,
    status: &tonic::Status,
) -> bool {
    !request.text_preview && status.message().contains(SENSITIVE_FULL_READ_DENIED_REASON)
}

fn artifact_read_error_outcome(
    proposal_id: &str,
    input_json: &[u8],
    status: &tonic::Status,
) -> ToolExecutionOutcome {
    let output_json = serde_json::to_vec(&json!({
        "error": {
            "code": format!("{:?}", status.code()).to_ascii_lowercase(),
            "message": status.message(),
        }
    }))
    .unwrap_or_else(|_| b"{}".to_vec());
    artifact_read_outcome(
        proposal_id,
        input_json,
        false,
        output_json,
        format!("artifact read failed: {}", status.message()),
    )
}

fn artifact_read_outcome(
    proposal_id: &str,
    input_json: &[u8],
    success: bool,
    output_json: Vec<u8>,
    error: String,
) -> ToolExecutionOutcome {
    build_tool_execution_outcome(
        proposal_id,
        ARTIFACT_READ_TOOL_NAME,
        input_json,
        success,
        output_json,
        error,
        false,
        "gateway_artifacts".to_owned(),
        "artifact_scope".to_owned(),
    )
}

#[cfg(test)]
mod tests {
    use super::{
        bounded_tool_result_artifact_content, build_pdf_extraction_projection,
        should_retry_artifact_read_as_text_preview, transcript_citation_anchor,
        PdfExtractionPageRange, PdfPageText, PDF_EXTRACTION_SCHEMA_VERSION,
    };
    use crate::journal::ToolResultArtifactReadRequest;
    use serde_json::{json, Value};
    use tonic::{Code, Status};

    #[test]
    fn bounded_tool_result_artifact_content_keeps_small_payload_raw() {
        let output_json = serde_json::to_vec(&json!({"ok": true, "value": "small"}))
            .expect("test json should serialize");

        let content = bounded_tool_result_artifact_content(output_json.as_slice(), 256 * 1024)
            .expect("small payload should fit");

        assert!(!content.truncated);
        assert_eq!(content.original_output_bytes, output_json.len());
        assert_eq!(content.stored_output_bytes, output_json.len());
        assert_eq!(content.content, output_json);
    }

    #[test]
    fn bounded_tool_result_artifact_content_caps_large_payload_as_valid_json() {
        let output_json = serde_json::to_vec(&json!({
            "items": ["x".repeat(385_000)],
        }))
        .expect("test json should serialize");
        let max_payload_bytes = 256 * 1024;

        let content =
            bounded_tool_result_artifact_content(output_json.as_slice(), max_payload_bytes)
                .expect("large payload should be bounded");

        assert!(content.truncated);
        assert!(content.content.len() <= max_payload_bytes);
        assert_eq!(content.original_output_bytes, output_json.len());
        assert!(content.stored_output_bytes < content.original_output_bytes);

        let projected = serde_json::from_slice::<Value>(content.content.as_slice())
            .expect("bounded artifact content should stay valid JSON");
        assert_eq!(
            projected.pointer("/artifact_content_truncated").and_then(Value::as_bool),
            Some(true)
        );
        assert_eq!(
            projected.pointer("/original_output_bytes").and_then(Value::as_u64),
            Some(output_json.len() as u64)
        );
        assert!(
            projected.pointer("/stored_output_utf8_prefix").and_then(Value::as_str).is_some(),
            "truncated artifact should retain a bounded useful prefix"
        );
    }

    #[test]
    fn artifact_read_retries_sensitive_full_read_as_text_preview() {
        let request = ToolResultArtifactReadRequest {
            artifact_id: "artifact-1".to_owned(),
            session_id: "session-1".to_owned(),
            run_id: "run-1".to_owned(),
            principal: "user:test".to_owned(),
            device_id: "device-1".to_owned(),
            channel: None,
            expected_digest_sha256: None,
            offset_bytes: 0,
            max_bytes: 4096,
            text_preview: false,
        };
        let status = Status::new(
            Code::PermissionDenied,
            "tool result artifact read denied: sensitive_full_read_denied",
        );

        assert!(should_retry_artifact_read_as_text_preview(&request, &status));
    }

    #[test]
    fn artifact_read_does_not_retry_when_preview_was_requested() {
        let request = ToolResultArtifactReadRequest {
            artifact_id: "artifact-1".to_owned(),
            session_id: "session-1".to_owned(),
            run_id: "run-1".to_owned(),
            principal: "user:test".to_owned(),
            device_id: "device-1".to_owned(),
            channel: None,
            expected_digest_sha256: None,
            offset_bytes: 0,
            max_bytes: 4096,
            text_preview: true,
        };
        let status = Status::new(
            Code::PermissionDenied,
            "tool result artifact read denied: sensitive_full_read_denied",
        );

        assert!(!should_retry_artifact_read_as_text_preview(&request, &status));
    }

    #[test]
    fn pdf_extraction_projection_is_page_anchored_and_budgeted() {
        let pages = vec![
            PdfPageText { page: 1, text: "cover".to_owned() },
            PdfPageText { page: 2, text: "alpha beta gamma".to_owned() },
            PdfPageText { page: 3, text: "delta".to_owned() },
        ];

        let projection = build_pdf_extraction_projection(
            "artifact-1",
            "run-1",
            "turn-1",
            "tool-1",
            pages.as_slice(),
            &[PdfExtractionPageRange { start_page: 2, end_page: 3 }],
            10,
        );

        assert_eq!(projection.schema_version, PDF_EXTRACTION_SCHEMA_VERSION);
        assert_eq!(projection.pages.len(), 1);
        assert_eq!(projection.pages[0].page, 2);
        assert_eq!(projection.pages[0].text, "alpha beta");
        assert!(projection.pages[0].truncated);
        assert!(projection.truncated);
        assert_eq!(projection.next_page, Some(2));
        assert_eq!(
            projection.pages[0].anchor.source_ref,
            "artifact:artifact-1:page:2:run:run-1:turn:turn-1:tool:tool-1"
        );
        assert_eq!(projection.trust_label, "extracted_document_text");
    }

    #[test]
    fn transcript_citation_anchor_is_stable_for_old_turns() {
        let anchor =
            transcript_citation_anchor("session-1", "run-1", "turn-7", Some("tool-3"), 42_000);

        assert_eq!(
            anchor.source_ref,
            "transcript:session-1:run:run-1:turn:turn-7:tool:tool-3:ts:42000"
        );
        assert_eq!(anchor.tool_call_id.as_deref(), Some("tool-3"));
        let serialized = serde_json::to_value(anchor).expect("anchor should serialize");
        assert_eq!(serialized["timestamp_unix_ms"], 42_000);
    }
}
