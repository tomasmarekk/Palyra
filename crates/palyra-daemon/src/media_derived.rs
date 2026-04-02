use std::{
    cmp::Ordering,
    io::{Cursor, Read},
    time::Instant,
};

use quick_xml::{events::Event, Reader};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use zip::ZipArchive;

use crate::model_provider::AudioTranscriptionResponse;

pub const METADATA_SUMMARY_PARSER_NAME: &str = "attachment-metadata";
pub const METADATA_SUMMARY_PARSER_VERSION: &str = "1";
pub const DOCUMENT_EXTRACTOR_PARSER_NAME: &str = "attachment-document-extractor";
pub const DOCUMENT_EXTRACTOR_PARSER_VERSION: &str = "1";
pub const AUDIO_TRANSCRIBER_PARSER_NAME: &str = "attachment-audio-transcriber";
pub const AUDIO_TRANSCRIBER_PARSER_VERSION: &str = "1";
const DEFAULT_SUMMARY_MAX_CHARS: usize = 320;
const DEFAULT_CHUNK_TARGET_CHARS: usize = 420;
const DEFAULT_SELECTION_BUDGET_CHARS: usize = 1_600;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DerivedArtifactKind {
    MetadataSummary,
    ExtractedText,
    Transcript,
}

impl DerivedArtifactKind {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::MetadataSummary => "metadata_summary",
            Self::ExtractedText => "extracted_text",
            Self::Transcript => "transcript",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DerivedArtifactWarning {
    pub code: String,
    pub message: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DerivedArtifactAnchor {
    pub kind: String,
    pub label: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub locator: Option<String>,
    pub start_char: usize,
    pub end_char: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DerivedArtifactContent {
    pub kind: DerivedArtifactKind,
    pub parser_name: String,
    pub parser_version: String,
    pub content_text: String,
    pub content_hash: String,
    pub summary_text: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub language: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub duration_ms: Option<u64>,
    pub processing_ms: u64,
    pub warnings: Vec<DerivedArtifactWarning>,
    pub anchors: Vec<DerivedArtifactAnchor>,
}

#[derive(Debug, Clone)]
pub struct AttachmentTextExtractionRequest<'a> {
    pub filename: &'a str,
    pub content_type: &'a str,
    pub bytes: &'a [u8],
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SelectedDerivedChunk {
    pub derived_artifact_id: String,
    pub source_artifact_id: String,
    pub label: String,
    pub citation: String,
    pub snippet: String,
    pub score: f64,
    pub kind: String,
}

#[derive(Debug, Clone)]
pub struct DerivedSelectionCandidate<'a> {
    pub derived_artifact_id: &'a str,
    pub source_artifact_id: &'a str,
    pub kind: &'a str,
    pub content_text: &'a str,
    pub anchors: &'a [DerivedArtifactAnchor],
}

#[must_use]
pub fn build_metadata_summary_content(
    filename: &str,
    content_type: &str,
    size_bytes: u64,
    content_hash: &str,
    width_px: Option<u32>,
    height_px: Option<u32>,
) -> DerivedArtifactContent {
    let started_at = Instant::now();
    let mut lines = vec![
        format!("filename: {filename}"),
        format!("content_type: {content_type}"),
        format!("size_bytes: {size_bytes}"),
        format!("content_hash: {content_hash}"),
    ];
    if let (Some(width_px), Some(height_px)) = (width_px, height_px) {
        lines.push(format!("dimensions_px: {width_px}x{height_px}"));
    }
    let content_text = lines.join("\n");
    DerivedArtifactContent {
        kind: DerivedArtifactKind::MetadataSummary,
        parser_name: METADATA_SUMMARY_PARSER_NAME.to_owned(),
        parser_version: METADATA_SUMMARY_PARSER_VERSION.to_owned(),
        summary_text: content_text.clone(),
        content_hash: sha256_hex(content_text.as_bytes()),
        content_text,
        language: None,
        duration_ms: None,
        processing_ms: started_at.elapsed().as_millis() as u64,
        warnings: Vec::new(),
        anchors: Vec::new(),
    }
}

#[must_use]
pub fn supports_document_extraction(content_type: &str) -> bool {
    matches!(
        content_type.trim().to_ascii_lowercase().as_str(),
        "text/plain"
            | "text/markdown"
            | "text/csv"
            | "application/json"
            | "text/html"
            | "application/pdf"
            | "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
            | "application/vnd.openxmlformats-officedocument.presentationml.presentation"
            | "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )
}

#[must_use]
pub fn supports_audio_transcription(content_type: &str) -> bool {
    let normalized = content_type.trim().to_ascii_lowercase();
    normalized.starts_with("audio/")
        || matches!(
            normalized.as_str(),
            "application/ogg" | "application/x-wav" | "application/vnd.wave"
        )
}

pub fn extract_document_content(
    request: &AttachmentTextExtractionRequest<'_>,
) -> Result<DerivedArtifactContent, String> {
    let started_at = Instant::now();
    let normalized_content_type = request.content_type.trim().to_ascii_lowercase();
    let (content_text, anchors, mut warnings) = match normalized_content_type.as_str() {
        "text/plain" | "text/markdown" | "text/csv" => {
            let text = decode_reasonable_text(request.bytes)?;
            let anchors = paragraph_anchors(text.as_str(), "section");
            (text, anchors, Vec::new())
        }
        "application/json" => {
            let parsed = serde_json::from_slice::<serde_json::Value>(request.bytes)
                .map_err(|error| format!("json parse failed: {error}"))?;
            let text = serde_json::to_string_pretty(&parsed)
                .map_err(|error| format!("json formatting failed: {error}"))?;
            let anchors = paragraph_anchors(text.as_str(), "section");
            (text, anchors, Vec::new())
        }
        "text/html" => extract_html_content(request.bytes)?,
        "application/pdf" => extract_pdf_content(request.bytes)?,
        "application/vnd.openxmlformats-officedocument.wordprocessingml.document" => {
            extract_docx_content(request.bytes)?
        }
        "application/vnd.openxmlformats-officedocument.presentationml.presentation" => {
            extract_pptx_content(request.bytes)?
        }
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet" => {
            extract_xlsx_content(request.bytes)?
        }
        _ => {
            return Err(format!(
                "document extraction is not supported for '{}' with content type '{}'",
                request.filename, request.content_type
            ))
        }
    };
    let normalized = normalize_extracted_text(content_text.as_str());
    if normalized.len() < 24 {
        warnings.push(DerivedArtifactWarning {
            code: "content_too_sparse".to_owned(),
            message: "Extractor returned too little structured text to trust the result."
                .to_owned(),
        });
        return Err(format!(
            "document extraction returned only sparse content for '{}'",
            request.filename
        ));
    }
    Ok(DerivedArtifactContent {
        kind: DerivedArtifactKind::ExtractedText,
        parser_name: DOCUMENT_EXTRACTOR_PARSER_NAME.to_owned(),
        parser_version: DOCUMENT_EXTRACTOR_PARSER_VERSION.to_owned(),
        summary_text: summarize_text(normalized.as_str(), DEFAULT_SUMMARY_MAX_CHARS),
        content_hash: sha256_hex(normalized.as_bytes()),
        content_text: normalized,
        language: None,
        duration_ms: None,
        processing_ms: started_at.elapsed().as_millis() as u64,
        warnings,
        anchors,
    })
}

#[must_use]
pub fn select_prompt_chunks(
    query: &str,
    candidates: &[DerivedSelectionCandidate<'_>],
    selection_budget_chars: Option<usize>,
) -> Vec<SelectedDerivedChunk> {
    let budget = selection_budget_chars.unwrap_or(DEFAULT_SELECTION_BUDGET_CHARS).max(1);
    let mut scored = Vec::new();
    for candidate in candidates {
        for (chunk_index, chunk) in chunk_text(candidate.content_text).into_iter().enumerate() {
            let score = lexical_score(query, chunk.text.as_str());
            let anchor = candidate.anchors.iter().find(|anchor| {
                anchor.start_char <= chunk.range.0 && anchor.end_char >= chunk.range.1
            });
            let label = anchor
                .map(|value| value.label.clone())
                .unwrap_or_else(|| format!("chunk {}", chunk_index + 1));
            let citation = match anchor.and_then(|value| value.locator.clone()) {
                Some(locator) => locator,
                None => label.clone(),
            };
            scored.push(SelectedDerivedChunk {
                derived_artifact_id: candidate.derived_artifact_id.to_owned(),
                source_artifact_id: candidate.source_artifact_id.to_owned(),
                label,
                citation,
                snippet: chunk.text,
                score,
                kind: candidate.kind.to_owned(),
            });
        }
    }
    scored.sort_by(|left, right| {
        right
            .score
            .partial_cmp(&left.score)
            .unwrap_or(Ordering::Equal)
            .then_with(|| left.snippet.len().cmp(&right.snippet.len()))
    });

    let mut selected = Vec::new();
    let mut used = 0usize;
    for chunk in scored {
        if used >= budget {
            break;
        }
        let next_len = chunk.snippet.chars().count();
        if !selected.is_empty() && used.saturating_add(next_len) > budget {
            continue;
        }
        used = used.saturating_add(next_len);
        selected.push(chunk);
    }
    selected
}

pub fn build_transcription_content(
    response: AudioTranscriptionResponse,
    processing_ms: u64,
) -> Result<DerivedArtifactContent, String> {
    let normalized = normalize_extracted_text(response.text.as_str());
    if normalized.trim().is_empty() {
        return Err("audio transcription returned empty text".to_owned());
    }
    let mut content_text = String::new();
    let mut anchors = Vec::new();
    if response.segments.is_empty() {
        content_text.push_str(normalized.as_str());
    } else {
        for segment in &response.segments {
            let start = content_text.chars().count();
            let locator = format!(
                "{}-{}",
                format_duration_ms(segment.start_ms),
                format_duration_ms(segment.end_ms),
            );
            let line = format!("[segment {locator}] {}\n", segment.text.trim());
            content_text.push_str(line.as_str());
            let end = content_text.chars().count();
            anchors.push(DerivedArtifactAnchor {
                kind: "segment".to_owned(),
                label: format!("segment {locator}"),
                locator: Some(locator),
                start_char: start,
                end_char: end,
            });
        }
    }
    let final_text = normalize_extracted_text(content_text.as_str());
    Ok(DerivedArtifactContent {
        kind: DerivedArtifactKind::Transcript,
        parser_name: AUDIO_TRANSCRIBER_PARSER_NAME.to_owned(),
        parser_version: AUDIO_TRANSCRIBER_PARSER_VERSION.to_owned(),
        summary_text: summarize_text(final_text.as_str(), DEFAULT_SUMMARY_MAX_CHARS),
        content_hash: sha256_hex(final_text.as_bytes()),
        content_text: final_text,
        language: response.language,
        duration_ms: response.duration_ms,
        processing_ms,
        warnings: Vec::new(),
        anchors,
    })
}

fn extract_html_content(
    bytes: &[u8],
) -> Result<(String, Vec<DerivedArtifactAnchor>, Vec<DerivedArtifactWarning>), String> {
    let raw = decode_reasonable_text(bytes)?;
    let without_scripts = strip_html_sections(raw.as_str(), "script");
    let without_style = strip_html_sections(without_scripts.as_str(), "style");
    let text = html_to_text(without_style.as_str());
    Ok((text.clone(), paragraph_anchors(text.as_str(), "section"), Vec::new()))
}

fn extract_pdf_content(
    bytes: &[u8],
) -> Result<(String, Vec<DerivedArtifactAnchor>, Vec<DerivedArtifactWarning>), String> {
    let document =
        lopdf::Document::load_mem(bytes).map_err(|error| format!("pdf parse failed: {error}"))?;
    if document.trailer.get(b"Encrypt").is_ok() {
        return Err("password-protected or encrypted PDF is not supported".to_owned());
    }
    let pages = document.get_pages().into_keys().collect::<Vec<_>>();
    if pages.is_empty() {
        return Err("pdf did not contain any pages".to_owned());
    }
    let mut content = String::new();
    let mut anchors = Vec::new();
    for page in pages {
        let start = content.chars().count();
        let text = document
            .extract_text(&[page])
            .map_err(|error| format!("pdf text extraction failed on page {page}: {error}"))?;
        let normalized_page = normalize_extracted_text(text.as_str());
        if normalized_page.is_empty() {
            continue;
        }
        content.push_str(format!("[page {page}]\n{normalized_page}\n\n").as_str());
        let end = content.chars().count();
        anchors.push(DerivedArtifactAnchor {
            kind: "page".to_owned(),
            label: format!("page {page}"),
            locator: Some(format!("page {page}")),
            start_char: start,
            end_char: end,
        });
    }
    if content.trim().is_empty() {
        return Err("pdf extractor returned no readable text".to_owned());
    }
    Ok((content, anchors, Vec::new()))
}

fn extract_docx_content(
    bytes: &[u8],
) -> Result<(String, Vec<DerivedArtifactAnchor>, Vec<DerivedArtifactWarning>), String> {
    let mut archive = zip_archive(bytes)?;
    let document_xml = read_zip_text(&mut archive, "word/document.xml")?;
    let paragraphs = extract_xml_text_blocks(document_xml.as_str(), &[b"w:t"], &[b"w:p"])?;
    let mut content = String::new();
    let mut anchors = Vec::new();
    for (index, paragraph) in paragraphs.into_iter().enumerate() {
        let normalized = normalize_extracted_text(paragraph.as_str());
        if normalized.is_empty() {
            continue;
        }
        let start = content.chars().count();
        content.push_str(format!("[section {}]\n{}\n\n", index + 1, normalized).as_str());
        let end = content.chars().count();
        anchors.push(DerivedArtifactAnchor {
            kind: "section".to_owned(),
            label: format!("section {}", index + 1),
            locator: Some(format!("section {}", index + 1)),
            start_char: start,
            end_char: end,
        });
    }
    if content.trim().is_empty() {
        return Err("docx extractor returned no readable text".to_owned());
    }
    Ok((content, anchors, Vec::new()))
}

fn extract_pptx_content(
    bytes: &[u8],
) -> Result<(String, Vec<DerivedArtifactAnchor>, Vec<DerivedArtifactWarning>), String> {
    let mut archive = zip_archive(bytes)?;
    let mut slide_names = archive
        .file_names()
        .filter(|name| name.starts_with("ppt/slides/slide") && name.ends_with(".xml"))
        .map(ToOwned::to_owned)
        .collect::<Vec<_>>();
    slide_names.sort_by_key(|name| extract_trailing_number(name));
    let mut content = String::new();
    let mut anchors = Vec::new();
    for (index, name) in slide_names.into_iter().enumerate() {
        let xml = read_zip_text(&mut archive, name.as_str())?;
        let text_blocks = extract_xml_text_blocks(xml.as_str(), &[b"a:t"], &[b"a:p"])?;
        let normalized = normalize_extracted_text(text_blocks.join("\n").as_str());
        if normalized.is_empty() {
            continue;
        }
        let start = content.chars().count();
        content.push_str(format!("[slide {}]\n{}\n\n", index + 1, normalized).as_str());
        let end = content.chars().count();
        anchors.push(DerivedArtifactAnchor {
            kind: "slide".to_owned(),
            label: format!("slide {}", index + 1),
            locator: Some(format!("slide {}", index + 1)),
            start_char: start,
            end_char: end,
        });
    }
    if content.trim().is_empty() {
        return Err("pptx extractor returned no readable text".to_owned());
    }
    Ok((content, anchors, Vec::new()))
}

fn extract_xlsx_content(
    bytes: &[u8],
) -> Result<(String, Vec<DerivedArtifactAnchor>, Vec<DerivedArtifactWarning>), String> {
    let mut archive = zip_archive(bytes)?;
    let workbook_xml = read_zip_text(&mut archive, "xl/workbook.xml")?;
    let sheet_names = extract_xml_attribute_values(workbook_xml.as_str(), b"sheet", b"name")?;
    let shared_strings = read_optional_zip_text(&mut archive, "xl/sharedStrings.xml")
        .map(|xml| extract_xml_text_blocks(xml.as_str(), &[b"t"], &[b"si"]).unwrap_or_default())
        .unwrap_or_default();
    let mut worksheet_files = archive
        .file_names()
        .filter(|name| name.starts_with("xl/worksheets/sheet") && name.ends_with(".xml"))
        .map(ToOwned::to_owned)
        .collect::<Vec<_>>();
    worksheet_files.sort_by_key(|name| extract_trailing_number(name));

    let mut content = String::new();
    let mut anchors = Vec::new();
    for (index, worksheet_name) in worksheet_files.into_iter().enumerate() {
        let xml = read_zip_text(&mut archive, worksheet_name.as_str())?;
        let rendered = render_xlsx_sheet(xml.as_str(), shared_strings.as_slice())?;
        let normalized = normalize_extracted_text(rendered.as_str());
        if normalized.is_empty() {
            continue;
        }
        let sheet_label =
            sheet_names.get(index).cloned().unwrap_or_else(|| format!("sheet {}", index + 1));
        let start = content.chars().count();
        content.push_str(format!("[sheet {sheet_label}]\n{}\n\n", normalized).as_str());
        let end = content.chars().count();
        anchors.push(DerivedArtifactAnchor {
            kind: "sheet".to_owned(),
            label: format!("sheet {sheet_label}"),
            locator: Some(format!("sheet {sheet_label}")),
            start_char: start,
            end_char: end,
        });
    }
    if content.trim().is_empty() {
        return Err("xlsx extractor returned no readable cell text".to_owned());
    }
    Ok((content, anchors, Vec::new()))
}

fn render_xlsx_sheet(xml: &str, shared_strings: &[String]) -> Result<String, String> {
    let mut reader = Reader::from_str(xml);
    reader.config_mut().trim_text(true);
    let mut buf = Vec::new();
    let mut inside_cell = false;
    let mut cell_ref = String::new();
    let mut cell_type = String::new();
    let mut reading_value = false;
    let mut rows = Vec::new();
    let mut current_row = Vec::new();

    loop {
        match reader.read_event_into(&mut buf) {
            Ok(Event::Start(event)) => match event.name().as_ref() {
                b"row" => current_row.clear(),
                b"c" => {
                    inside_cell = true;
                    cell_ref = attribute_value(&event, b"r").unwrap_or_default();
                    cell_type = attribute_value(&event, b"t").unwrap_or_default();
                }
                b"v" if inside_cell => reading_value = true,
                _ => {}
            },
            Ok(Event::Text(text)) if reading_value => {
                let raw =
                    text.decode().map_err(|error| format!("xlsx text decode failed: {error}"))?;
                let value = if cell_type == "s" {
                    raw.parse::<usize>()
                        .ok()
                        .and_then(|index| shared_strings.get(index).cloned())
                        .unwrap_or(raw.into_owned())
                } else {
                    raw.into_owned()
                };
                if !value.trim().is_empty() {
                    current_row.push(format!("{cell_ref}: {}", value.trim()));
                }
            }
            Ok(Event::End(event)) => match event.name().as_ref() {
                b"v" => reading_value = false,
                b"c" => {
                    inside_cell = false;
                    cell_ref.clear();
                    cell_type.clear();
                    reading_value = false;
                }
                b"row" => {
                    if !current_row.is_empty() {
                        rows.push(current_row.join(" | "));
                    }
                    current_row.clear();
                }
                _ => {}
            },
            Ok(Event::Eof) => break,
            Err(error) => return Err(format!("xlsx worksheet xml parse failed: {error}")),
            _ => {}
        }
        buf.clear();
    }
    Ok(rows.join("\n"))
}

fn extract_xml_text_blocks(
    xml: &str,
    text_tags: &[&[u8]],
    block_tags: &[&[u8]],
) -> Result<Vec<String>, String> {
    let mut reader = Reader::from_str(xml);
    reader.config_mut().trim_text(true);
    let mut buf = Vec::new();
    let mut current = String::new();
    let mut blocks = Vec::new();
    loop {
        match reader.read_event_into(&mut buf) {
            Ok(Event::Text(text)) => {
                let decoded =
                    text.decode().map_err(|error| format!("xml text decode failed: {error}"))?;
                if !decoded.trim().is_empty() {
                    if !current.is_empty() {
                        current.push(' ');
                    }
                    current.push_str(decoded.trim());
                }
            }
            Ok(Event::End(event)) => {
                if block_tags.iter().any(|tag| *tag == event.name().as_ref()) {
                    if !current.trim().is_empty() {
                        blocks.push(current.trim().to_owned());
                    }
                    current.clear();
                } else if text_tags.iter().any(|tag| *tag == event.name().as_ref())
                    && current.ends_with(' ')
                {
                    current.pop();
                }
            }
            Ok(Event::Eof) => break,
            Err(error) => return Err(format!("xml parse failed: {error}")),
            _ => {}
        }
        buf.clear();
    }
    if !current.trim().is_empty() {
        blocks.push(current.trim().to_owned());
    }
    Ok(blocks)
}

fn extract_xml_attribute_values(
    xml: &str,
    tag_name: &[u8],
    attr_name: &[u8],
) -> Result<Vec<String>, String> {
    let mut reader = Reader::from_str(xml);
    reader.config_mut().trim_text(true);
    let mut buf = Vec::new();
    let mut values = Vec::new();
    loop {
        match reader.read_event_into(&mut buf) {
            Ok(Event::Start(event)) | Ok(Event::Empty(event))
                if event.name().as_ref() == tag_name =>
            {
                if let Some(value) = attribute_value(&event, attr_name) {
                    values.push(value);
                }
            }
            Ok(Event::Eof) => break,
            Err(error) => return Err(format!("xml attribute parse failed: {error}")),
            _ => {}
        }
        buf.clear();
    }
    Ok(values)
}

fn attribute_value(event: &quick_xml::events::BytesStart<'_>, name: &[u8]) -> Option<String> {
    event
        .attributes()
        .flatten()
        .find(|attribute| attribute.key.as_ref() == name)
        .and_then(|attribute| attribute.unescape_value().ok().map(|value| value.into_owned()))
}

fn zip_archive(bytes: &[u8]) -> Result<ZipArchive<Cursor<&[u8]>>, String> {
    ZipArchive::new(Cursor::new(bytes)).map_err(|error| format!("zip parse failed: {error}"))
}

fn read_zip_text<R: Read + std::io::Seek>(
    archive: &mut ZipArchive<R>,
    path: &str,
) -> Result<String, String> {
    let mut file = archive
        .by_name(path)
        .map_err(|error| format!("zip entry '{path}' missing or unreadable: {error}"))?;
    let mut text = String::new();
    file.read_to_string(&mut text)
        .map_err(|error| format!("zip entry '{path}' is not valid UTF-8 text: {error}"))?;
    Ok(text)
}

fn read_optional_zip_text<R: Read + std::io::Seek>(
    archive: &mut ZipArchive<R>,
    path: &str,
) -> Option<String> {
    read_zip_text(archive, path).ok()
}

fn decode_reasonable_text(bytes: &[u8]) -> Result<String, String> {
    if bytes.is_empty() {
        return Err("attachment bytes cannot be empty".to_owned());
    }
    let decoded = String::from_utf8_lossy(bytes).replace('\u{feff}', "");
    let printable_chars =
        decoded.chars().filter(|ch| !ch.is_control() || matches!(ch, '\n' | '\r' | '\t')).count();
    let total_chars = decoded.chars().count().max(1);
    if printable_chars * 4 < total_chars * 3 {
        return Err("attachment bytes do not look like readable text".to_owned());
    }
    Ok(decoded)
}

fn normalize_extracted_text(raw: &str) -> String {
    raw.lines()
        .map(str::trim_end)
        .collect::<Vec<_>>()
        .join("\n")
        .replace(['\r', '\t'], " ")
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .replace(" [", "\n[")
}

fn paragraph_anchors(text: &str, kind: &str) -> Vec<DerivedArtifactAnchor> {
    let mut anchors = Vec::new();
    let mut start = 0usize;
    for (index, paragraph) in text.split("\n\n").enumerate() {
        if paragraph.trim().is_empty() {
            start = start.saturating_add(paragraph.chars().count() + 2);
            continue;
        }
        let end = start.saturating_add(paragraph.chars().count());
        anchors.push(DerivedArtifactAnchor {
            kind: kind.to_owned(),
            label: format!("{kind} {}", index + 1),
            locator: Some(format!("{kind} {}", index + 1)),
            start_char: start,
            end_char: end,
        });
        start = end.saturating_add(2);
    }
    anchors
}

fn strip_html_sections(input: &str, tag_name: &str) -> String {
    let lower = input.to_ascii_lowercase();
    let open = format!("<{tag_name}");
    let close = format!("</{tag_name}>");
    let mut output = String::new();
    let mut cursor = 0usize;
    while let Some(start_index) = lower[cursor..].find(open.as_str()) {
        let start = cursor + start_index;
        output.push_str(&input[cursor..start]);
        if let Some(end_index) = lower[start..].find(close.as_str()) {
            cursor = start + end_index + close.len();
        } else {
            cursor = input.len();
            break;
        }
    }
    output.push_str(&input[cursor..]);
    output
}

fn html_to_text(input: &str) -> String {
    let mut output = String::new();
    let mut inside_tag = false;
    let mut entity = String::new();
    let mut inside_entity = false;
    for ch in input.chars() {
        match ch {
            '<' => inside_tag = true,
            '>' => {
                inside_tag = false;
                output.push(' ');
            }
            '&' if !inside_tag => {
                inside_entity = true;
                entity.clear();
            }
            ';' if inside_entity => {
                output.push_str(decode_html_entity(entity.as_str()).as_str());
                inside_entity = false;
            }
            _ if inside_tag => {}
            _ if inside_entity => entity.push(ch),
            _ => output.push(ch),
        }
    }
    if inside_entity {
        output.push('&');
        output.push_str(entity.as_str());
    }
    output.replace(['\r', '\n', '\t'], " ").split_whitespace().collect::<Vec<_>>().join(" ")
}

fn decode_html_entity(entity: &str) -> String {
    match entity {
        "amp" => "&".to_owned(),
        "lt" => "<".to_owned(),
        "gt" => ">".to_owned(),
        "quot" => "\"".to_owned(),
        "apos" => "'".to_owned(),
        "nbsp" => " ".to_owned(),
        _ => format!("&{entity};"),
    }
}

fn summarize_text(raw: &str, max_chars: usize) -> String {
    if raw.chars().count() <= max_chars {
        return raw.to_owned();
    }
    let mut summary = raw.chars().take(max_chars).collect::<String>();
    summary.push_str("...");
    summary
}

fn extract_trailing_number(raw: &str) -> usize {
    let digits = raw
        .chars()
        .rev()
        .take_while(|ch| ch.is_ascii_digit())
        .collect::<String>()
        .chars()
        .rev()
        .collect::<String>();
    digits.parse::<usize>().unwrap_or(0)
}

fn lexical_score(query: &str, candidate: &str) -> f64 {
    let query_terms = normalized_terms(query);
    if query_terms.is_empty() {
        return 0.1;
    }
    let candidate_terms = normalized_terms(candidate);
    if candidate_terms.is_empty() {
        return 0.0;
    }
    let overlap = query_terms.iter().filter(|term| candidate_terms.contains(*term)).count();
    overlap as f64 / query_terms.len().max(1) as f64
}

fn normalized_terms(raw: &str) -> Vec<String> {
    raw.split(|ch: char| !ch.is_alphanumeric())
        .filter(|term| term.len() >= 3)
        .map(|term| term.to_ascii_lowercase())
        .collect::<Vec<_>>()
}

struct TextChunk {
    range: (usize, usize),
    text: String,
}

fn chunk_text(raw: &str) -> Vec<TextChunk> {
    let mut chunks = Vec::new();
    let mut start = 0usize;
    let mut current = String::new();
    for paragraph in raw.split('\n') {
        let paragraph = paragraph.trim();
        if paragraph.is_empty() {
            continue;
        }
        if !current.is_empty()
            && current.chars().count().saturating_add(paragraph.chars().count() + 1)
                > DEFAULT_CHUNK_TARGET_CHARS
        {
            let end = start.saturating_add(current.chars().count());
            chunks.push(TextChunk { range: (start, end), text: current.trim().to_owned() });
            start = end.saturating_add(1);
            current.clear();
        }
        if !current.is_empty() {
            current.push('\n');
        }
        current.push_str(paragraph);
    }
    if !current.trim().is_empty() {
        let end = start.saturating_add(current.chars().count());
        chunks.push(TextChunk { range: (start, end), text: current.trim().to_owned() });
    }
    chunks
}

fn format_duration_ms(value: u64) -> String {
    let total_seconds = value / 1_000;
    let hours = total_seconds / 3_600;
    let minutes = (total_seconds % 3_600) / 60;
    let seconds = total_seconds % 60;
    if hours > 0 {
        format!("{hours:02}:{minutes:02}:{seconds:02}")
    } else {
        format!("{minutes:02}:{seconds:02}")
    }
}

fn sha256_hex(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    hex::encode(hasher.finalize())
}
