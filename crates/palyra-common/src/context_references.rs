//! Parser for inline `@kind:target` context references in operator chat input.
//!
//! Extracts references like `@file:README.md` or `@memory:"release notes"` from a message,
//! reporting malformed ones as errors and returning the message text with references
//! stripped. `@@` escapes a literal `@`; references only start at word boundaries so
//! e-mail addresses pass through untouched.

use serde::{Deserialize, Serialize};

/// What a context reference points at.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ContextReferenceKind {
    File,
    Folder,
    Diff,
    Staged,
    Url,
    Memory,
}

impl ContextReferenceKind {
    /// Returns the canonical snake_case wire name for this kind.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::File => "file",
            Self::Folder => "folder",
            Self::Diff => "diff",
            Self::Staged => "staged",
            Self::Url => "url",
            Self::Memory => "memory",
        }
    }

    fn parse(raw: &str) -> Option<Self> {
        match raw {
            "file" => Some(Self::File),
            "folder" => Some(Self::Folder),
            "diff" => Some(Self::Diff),
            "staged" => Some(Self::Staged),
            "url" => Some(Self::Url),
            "memory" => Some(Self::Memory),
            _ => None,
        }
    }

    // Diff and staged describe workspace state as a whole, so a target is optional.
    const fn requires_target(self) -> bool {
        !matches!(self, Self::Diff | Self::Staged)
    }
}

/// One successfully parsed reference with byte offsets into the original input.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ParsedContextReference {
    pub kind: ContextReferenceKind,
    pub raw_text: String,
    pub target: Option<String>,
    pub start_offset: usize,
    pub end_offset: usize,
}

/// A malformed reference (missing target, unterminated quote) with its input location.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContextReferenceParseError {
    pub raw_text: String,
    pub message: String,
    pub start_offset: usize,
    pub end_offset: usize,
}

/// Full parse outcome: extracted references, errors, and the reference-free message text.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContextReferenceParseResult {
    pub references: Vec<ParsedContextReference>,
    pub errors: Vec<ContextReferenceParseError>,
    pub clean_text: String,
}

/// Parses all context references out of a chat message.
///
/// Valid references are removed from `clean_text` (whitespace collapsed); malformed ones
/// are reported in `errors` and left in place; `@` sequences that don't form a reference
/// pass through verbatim.
#[must_use]
pub fn parse_context_references(input: &str) -> ContextReferenceParseResult {
    let mut references = Vec::new();
    let mut errors = Vec::new();
    let mut clean_text = String::with_capacity(input.len());

    let bytes = input.as_bytes();
    let mut index = 0usize;
    while index < bytes.len() {
        if bytes[index] != b'@' {
            push_current_char(input, index, &mut clean_text);
            index += current_char_len(input, index);
            continue;
        }

        // `@@` is the escape for a literal `@`.
        if index + 1 < bytes.len() && bytes[index + 1] == b'@' {
            clean_text.push('@');
            index += 2;
            continue;
        }

        if !is_reference_boundary(input, index) {
            clean_text.push('@');
            index += 1;
            continue;
        }

        match parse_reference_at(input, index) {
            Some(Ok((parsed, next_index))) => {
                references.push(parsed);
                index = next_index;
            }
            Some(Err((error, next_index))) => {
                errors.push(error);
                clean_text.push_str(&input[index..next_index]);
                index = next_index;
            }
            None => {
                clean_text.push('@');
                index += 1;
            }
        }
    }

    ContextReferenceParseResult { references, errors, clean_text: normalize_clean_text(clean_text) }
}

fn parse_reference_at(
    input: &str,
    start_offset: usize,
) -> Option<Result<(ParsedContextReference, usize), (ContextReferenceParseError, usize)>> {
    let bytes = input.as_bytes();
    let mut cursor = start_offset + 1;
    while cursor < bytes.len() && bytes[cursor].is_ascii_alphabetic() {
        cursor += 1;
    }
    if cursor == start_offset + 1 {
        return None;
    }

    let kind_text = &input[start_offset + 1..cursor];
    let kind = ContextReferenceKind::parse(kind_text)?;

    if cursor >= bytes.len() || bytes[cursor].is_ascii_whitespace() {
        if kind.requires_target() {
            let raw_text = input[start_offset..cursor].to_owned();
            return Some(Err((
                ContextReferenceParseError {
                    raw_text,
                    message: format!("@{} requires a target", kind.as_str()),
                    start_offset,
                    end_offset: cursor,
                },
                cursor,
            )));
        }
        let raw_text = input[start_offset..cursor].to_owned();
        return Some(Ok((
            ParsedContextReference {
                kind,
                raw_text,
                target: None,
                start_offset,
                end_offset: cursor,
            },
            cursor,
        )));
    }

    if bytes[cursor] != b':' {
        if matches!(kind, ContextReferenceKind::Diff | ContextReferenceKind::Staged) {
            let raw_text = input[start_offset..cursor].to_owned();
            return Some(Ok((
                ParsedContextReference {
                    kind,
                    raw_text,
                    target: None,
                    start_offset,
                    end_offset: cursor,
                },
                cursor,
            )));
        }
        return None;
    }

    let value_start = cursor + 1;
    if value_start >= bytes.len() {
        let raw_text = input[start_offset..value_start].to_owned();
        return Some(Err((
            ContextReferenceParseError {
                raw_text,
                message: format!("@{} requires a non-empty target", kind.as_str()),
                start_offset,
                end_offset: value_start,
            },
            value_start,
        )));
    }

    let (target, next_index) = if matches!(bytes[value_start], b'"' | b'\'') {
        match parse_quoted_value(input, value_start) {
            Ok((value, next_index)) => (value, next_index),
            Err(next_index) => {
                return Some(Err((
                    ContextReferenceParseError {
                        raw_text: input[start_offset..next_index].to_owned(),
                        message: format!(
                            "@{} contains an unterminated quoted target",
                            kind.as_str()
                        ),
                        start_offset,
                        end_offset: next_index,
                    },
                    next_index,
                )));
            }
        }
    } else {
        let mut value_end = value_start;
        while value_end < bytes.len() && !bytes[value_end].is_ascii_whitespace() {
            value_end += 1;
        }
        (input[value_start..value_end].to_owned(), value_end)
    };

    if target.trim().is_empty() {
        return Some(Err((
            ContextReferenceParseError {
                raw_text: input[start_offset..next_index].to_owned(),
                message: format!("@{} requires a non-empty target", kind.as_str()),
                start_offset,
                end_offset: next_index,
            },
            next_index,
        )));
    }

    Some(Ok((
        ParsedContextReference {
            kind,
            raw_text: input[start_offset..next_index].to_owned(),
            target: Some(target),
            start_offset,
            end_offset: next_index,
        },
        next_index,
    )))
}

// Reads a quote-delimited target starting at `quote_start`; `Err` carries the byte offset
// where the unterminated quote scan ended.
fn parse_quoted_value(input: &str, quote_start: usize) -> Result<(String, usize), usize> {
    let bytes = input.as_bytes();
    let quote = bytes[quote_start];
    let mut cursor = quote_start + 1;
    let mut value = String::new();
    while cursor < bytes.len() {
        match bytes[cursor] {
            b'\\' => {
                cursor += 1;
                if cursor >= bytes.len() {
                    return Err(cursor);
                }
                // AIDEV-NOTE: escaping a multi-byte UTF-8 char (a backslash followed by
                // any non-ASCII char) pushes only its lead byte as a Latin-1 char and
                // advances the cursor mid-character; the
                // next iteration then slices `input[cursor..]` off a char boundary and
                // panics. Fixing requires changing observable behavior for such inputs,
                // so it is left as-is and only documented here.
                value.push(match bytes[cursor] {
                    b'n' => '\n',
                    b'r' => '\r',
                    b't' => '\t',
                    b'\\' => '\\',
                    b'"' => '"',
                    b'\'' => '\'',
                    other => other as char,
                });
                cursor += 1;
            }
            current if current == quote => {
                cursor += 1;
                return Ok((value, cursor));
            }
            _ => {
                let ch = input[cursor..]
                    .chars()
                    .next()
                    .expect("quoted parse cursor should remain on a valid char");
                value.push(ch);
                cursor += ch.len_utf8();
            }
        }
    }
    Err(bytes.len())
}

// Collapses the whitespace holes left by removed references: runs of spaces become one
// space, spaces before newlines are dropped, and blank-line runs collapse to one newline.
fn normalize_clean_text(raw: String) -> String {
    let mut normalized = String::with_capacity(raw.len());
    let mut previous_was_whitespace = false;
    for ch in raw.chars() {
        if ch.is_whitespace() {
            if ch == '\n' {
                while normalized.ends_with(' ') {
                    normalized.pop();
                }
                if !normalized.ends_with('\n') {
                    normalized.push('\n');
                }
                previous_was_whitespace = true;
                continue;
            }
            if !previous_was_whitespace {
                normalized.push(' ');
            }
            previous_was_whitespace = true;
        } else {
            normalized.push(ch);
            previous_was_whitespace = false;
        }
    }
    normalized.trim().to_owned()
}

// References must start after whitespace or an opening bracket/quote so that infix `@`
// (e-mail addresses, handles) is never treated as a reference.
fn is_reference_boundary(input: &str, at_offset: usize) -> bool {
    if at_offset == 0 {
        return true;
    }
    let previous = input[..at_offset].chars().next_back();
    previous
        .is_none_or(|value| value.is_whitespace() || matches!(value, '(' | '[' | '{' | '"' | '\''))
}

fn push_current_char(input: &str, byte_index: usize, output: &mut String) {
    output.push(
        input[byte_index..]
            .chars()
            .next()
            .expect("byte index should point at a valid UTF-8 boundary"),
    );
}

fn current_char_len(input: &str, byte_index: usize) -> usize {
    input[byte_index..]
        .chars()
        .next()
        .expect("byte index should point at a valid UTF-8 boundary")
        .len_utf8()
}

#[cfg(test)]
mod tests {
    use super::{parse_context_references, ContextReferenceKind};

    #[test]
    fn parses_multiple_reference_kinds_and_strips_them_from_clean_text() {
        let parsed = parse_context_references(
            "Summarize @file:README.md and compare with @diff:apps/web plus @memory:\"release notes\".",
        );
        assert!(parsed.errors.is_empty(), "expected clean parse: {:?}", parsed.errors);
        assert_eq!(parsed.references.len(), 3);
        assert_eq!(parsed.references[0].kind, ContextReferenceKind::File);
        assert_eq!(parsed.references[0].target.as_deref(), Some("README.md"));
        assert_eq!(parsed.references[1].kind, ContextReferenceKind::Diff);
        assert_eq!(parsed.references[1].target.as_deref(), Some("apps/web"));
        assert_eq!(parsed.references[2].kind, ContextReferenceKind::Memory);
        assert_eq!(parsed.references[2].target.as_deref(), Some("release notes"));
        assert_eq!(parsed.clean_text, "Summarize and compare with plus .");
    }

    #[test]
    fn treats_double_at_as_literal_escape() {
        let parsed = parse_context_references("Mention @@file and keep @staged as context.");
        assert!(parsed.errors.is_empty(), "expected clean parse: {:?}", parsed.errors);
        assert_eq!(parsed.references.len(), 1);
        assert_eq!(parsed.references[0].kind, ContextReferenceKind::Staged);
        assert_eq!(parsed.clean_text, "Mention @file and keep as context.");
    }

    #[test]
    fn reports_missing_target_for_targeted_reference() {
        let parsed = parse_context_references("Inspect @file before send.");
        assert_eq!(parsed.references.len(), 0);
        assert_eq!(parsed.errors.len(), 1);
        assert!(parsed.errors[0].message.contains("requires a target"));
        assert_eq!(parsed.clean_text, "Inspect @file before send.");
    }

    #[test]
    fn ignores_non_boundary_mentions_like_emails() {
        let parsed = parse_context_references("mail me at ops@example.com about @diff");
        assert!(parsed.errors.is_empty(), "expected clean parse: {:?}", parsed.errors);
        assert_eq!(parsed.references.len(), 1);
        assert_eq!(parsed.references[0].kind, ContextReferenceKind::Diff);
        assert_eq!(parsed.clean_text, "mail me at ops@example.com about");
    }
}
