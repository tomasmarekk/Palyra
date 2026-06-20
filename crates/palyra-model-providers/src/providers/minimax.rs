//! MiniMax identity and Anthropic-compatible capability overrides.

use ulid::Ulid;

use crate::config::ProviderCapabilitiesSnapshot;
use crate::contract::ProviderEvent;

pub(crate) const PROVIDER_ID: &str = "minimax-primary";
pub(crate) const DISPLAY_NAME: &str = "MiniMax";

pub(crate) fn chat_capabilities() -> ProviderCapabilitiesSnapshot {
    let mut capabilities = super::anthropic::chat_capabilities();
    capabilities.vision = false;
    capabilities
        .known_limitations
        .push("vision unsupported by MiniMax Anthropic-compatible chat".to_owned());
    capabilities
        .recommended_use_cases
        .retain(|use_case| !use_case.to_ascii_lowercase().contains("vision"));
    capabilities
}

/// Tool-call proposals recovered from provider text that used raw tag markup.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RawToolCallMarkupExtraction {
    /// Text with raw tool-call blocks removed.
    pub cleaned_text: String,
    /// Tool proposal events parsed from the raw markup.
    pub tool_events: Vec<ProviderEvent>,
}

/// Recovers tool invocations that Anthropic-compatible providers, notably
/// MiniMax, may emit as inline tag markup inside the text body.
///
/// ASCII lowercasing preserves byte offsets, so indices found in the lower-case
/// copy can safely slice the original text.
///
/// # Errors
/// Returns an error when a detected raw tool-call block is malformed or when
/// the recovered tool arguments exceed provider tool-argument bounds.
pub fn coerce_raw_tool_call_markup(
    text: &str,
) -> Result<Option<RawToolCallMarkupExtraction>, String> {
    let lower = text.to_ascii_lowercase();
    if !lower.contains("<minimax:tool_call") && !lower.contains("<tool_call") {
        return Ok(None);
    }

    let mut cursor = 0usize;
    let mut cleaned_text = String::new();
    let mut tool_events = Vec::new();
    while let Some(block) = find_next_raw_tool_call_block(&lower, cursor) {
        cleaned_text.push_str(&text[cursor..block.start]);
        let tag_end = text[block.start..]
            .find('>')
            .map(|offset| block.start + offset)
            .ok_or_else(|| "raw tool-call opening tag is missing '>'".to_owned())?;
        let content_start = tag_end.saturating_add(1);
        let close_start =
            lower[content_start..].find(block.close_tag).map(|offset| content_start + offset);
        let (block_content, block_end, missing_outer_close) = if let Some(close_start) = close_start
        {
            (&text[content_start..close_start], close_start + block.close_tag.len(), false)
        } else {
            (&text[content_start..], text.len(), true)
        };
        let mut parsed_events = parse_raw_tool_call_invocations(block_content)?;
        if missing_outer_close && parsed_events.is_empty() {
            return Err("raw tool-call block is missing a closing tag".to_owned());
        }
        tool_events.append(&mut parsed_events);
        cursor = block_end;
    }
    cleaned_text.push_str(&text[cursor..]);

    if tool_events.is_empty() {
        return Err("raw tool-call markup did not contain any invoke blocks".to_owned());
    }

    Ok(Some(RawToolCallMarkupExtraction {
        cleaned_text: cleaned_text.trim().to_owned(),
        tool_events,
    }))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct RawToolCallBlock<'a> {
    start: usize,
    close_tag: &'a str,
}

fn find_next_raw_tool_call_block(lower: &str, cursor: usize) -> Option<RawToolCallBlock<'static>> {
    let minimax = lower[cursor..].find("<minimax:tool_call").map(|offset| RawToolCallBlock {
        start: cursor + offset,
        close_tag: "</minimax:tool_call>",
    });
    let generic = lower[cursor..]
        .find("<tool_call")
        .map(|offset| RawToolCallBlock { start: cursor + offset, close_tag: "</tool_call>" });
    match (minimax, generic) {
        (Some(left), Some(right)) => Some(if left.start <= right.start { left } else { right }),
        (Some(block), None) | (None, Some(block)) => Some(block),
        (None, None) => None,
    }
}

fn parse_raw_tool_call_invocations(block: &str) -> Result<Vec<ProviderEvent>, String> {
    let lower = block.to_ascii_lowercase();
    let mut cursor = 0usize;
    let mut events = Vec::new();
    while let Some(relative_start) = lower[cursor..].find("<invoke") {
        let start = cursor + relative_start;
        let tag_end = block[start..]
            .find('>')
            .map(|offset| start + offset)
            .ok_or_else(|| "invoke opening tag is missing '>'".to_owned())?;
        let opening_tag = &block[start..=tag_end];
        let tool_name = extract_raw_tool_invoke_name(opening_tag)
            .ok_or_else(|| "invoke tag is missing a valid name attribute".to_owned())?;
        let arguments_start = tag_end.saturating_add(1);
        let (arguments, next_cursor) = if let Some(close_start) =
            lower[arguments_start..].find("</invoke>").map(|offset| arguments_start + offset)
        {
            (block[arguments_start..close_start].trim(), close_start + "</invoke>".len())
        } else {
            let trailing_arguments = block[arguments_start..].trim();
            if trailing_arguments.is_empty() {
                return Err("invoke block is missing </invoke>".to_owned());
            }
            (trailing_arguments, block.len())
        };
        let input_json = super::normalize_tool_arguments(arguments)?;
        events.push(ProviderEvent::ToolProposal {
            proposal_id: Ulid::new().to_string(),
            tool_name,
            input_json,
        });
        cursor = next_cursor;
    }
    Ok(events)
}

fn extract_raw_tool_invoke_name(opening_tag: &str) -> Option<String> {
    let lower = opening_tag.to_ascii_lowercase();
    let mut cursor = lower.find("name")? + "name".len();
    let bytes = opening_tag.as_bytes();
    while bytes.get(cursor).is_some_and(u8::is_ascii_whitespace) {
        cursor = cursor.saturating_add(1);
    }
    if bytes.get(cursor).copied() != Some(b'=') {
        return None;
    }
    cursor = cursor.saturating_add(1);
    while bytes.get(cursor).is_some_and(u8::is_ascii_whitespace) {
        cursor = cursor.saturating_add(1);
    }
    let quote = bytes.get(cursor).copied()?;
    if !matches!(quote, b'"' | b'\'') {
        return None;
    }
    cursor = cursor.saturating_add(1);
    let value_start = cursor;
    while let Some(byte) = bytes.get(cursor).copied() {
        if byte == quote {
            let value = opening_tag[value_start..cursor].trim();
            return (!value.is_empty()).then(|| value.to_owned());
        }
        cursor = cursor.saturating_add(1);
    }
    None
}

#[cfg(test)]
mod tests {
    use super::coerce_raw_tool_call_markup;
    use crate::contract::ProviderEvent;

    #[test]
    fn raw_minimax_tool_call_markup_is_coerced_to_tool_event() {
        let raw = r#"<minimax:tool_call>
<invoke name="palyra.fs.apply_patch">
{"patch":"*** Begin Patch\n*** Add File: app.js\n+console.log('ok');\n*** End Patch\n"}
</invoke>
</minimax:tool_call>"#;

        let extraction = coerce_raw_tool_call_markup(raw)
            .expect("raw MiniMax markup should parse")
            .expect("raw MiniMax markup should be detected");

        assert!(extraction.cleaned_text.is_empty());
        assert_eq!(extraction.tool_events.len(), 1);
        match &extraction.tool_events[0] {
            ProviderEvent::ToolProposal { tool_name, input_json, .. } => {
                assert_eq!(tool_name, "palyra.fs.apply_patch");
                let input: serde_json::Value =
                    serde_json::from_slice(input_json).expect("tool input should stay valid JSON");
                assert!(
                    input["patch"].as_str().is_some_and(|patch| patch.contains("*** Add File")),
                    "{input}"
                );
            }
            other => panic!("expected tool proposal, got {other:?}"),
        }
    }

    #[test]
    fn raw_tool_call_markup_accepts_complete_invoke_without_outer_close() {
        let raw = r#"<tool_call>
<invoke name="palyra.fs.read_file">
{"path":"app.js"}
</invoke>"#;

        let extraction = coerce_raw_tool_call_markup(raw)
            .expect("complete invoke should be recoverable without outer close")
            .expect("raw markup should be detected");

        assert!(extraction.cleaned_text.is_empty());
        assert_eq!(extraction.tool_events.len(), 1);
        match &extraction.tool_events[0] {
            ProviderEvent::ToolProposal { tool_name, input_json, .. } => {
                assert_eq!(tool_name, "palyra.fs.read_file");
                let input: serde_json::Value =
                    serde_json::from_slice(input_json).expect("tool input should stay valid JSON");
                assert_eq!(input["path"], "app.js");
            }
            other => panic!("expected tool proposal, got {other:?}"),
        }
    }

    #[test]
    fn raw_tool_call_markup_accepts_valid_json_when_invoke_close_is_missing() {
        let raw = r#"<tool_call><invoke name="palyra.fs.read_file">{"path":"app.js"}</tool_call>"#;

        let extraction = coerce_raw_tool_call_markup(raw)
            .expect("valid raw invocation should be recoverable without closing invoke")
            .expect("raw markup should be detected");

        assert!(extraction.cleaned_text.is_empty());
        assert_eq!(extraction.tool_events.len(), 1);
        match &extraction.tool_events[0] {
            ProviderEvent::ToolProposal { tool_name, input_json, .. } => {
                assert_eq!(tool_name, "palyra.fs.read_file");
                let input: serde_json::Value =
                    serde_json::from_slice(input_json).expect("tool input should stay valid JSON");
                assert_eq!(input["path"], "app.js");
            }
            other => panic!("expected tool proposal, got {other:?}"),
        }
    }

    #[test]
    fn raw_tool_call_markup_is_removed_from_surrounding_text() {
        let raw = r#"I will inspect it.
<tool_call>
<invoke name='palyra.fs.read_file'>
{"path":"app.js"}
</invoke>
</tool_call>
Then I will continue."#;

        let extraction = coerce_raw_tool_call_markup(raw)
            .expect("raw generic markup should parse")
            .expect("raw generic markup should be detected");

        assert!(!extraction.cleaned_text.contains("<tool_call>"));
        assert!(extraction.cleaned_text.contains("I will inspect it."));
        assert!(extraction.cleaned_text.contains("Then I will continue."));
        assert_eq!(extraction.tool_events.len(), 1);
    }
}
