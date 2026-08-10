//! Shared projection of provider turn output into bounded tape payloads.

use palyra_model_providers::{
    bounded_provider_turn_output_for_persistence, ProviderRawProviderRefs, ProviderTurnOutput,
};
use tonic::Status;

const PROVIDER_OUTPUT_TRUNCATED_MARKER: &str = "\n\n[provider output truncated]";
const PROVIDER_OUTPUT_TRUNCATION_REF: &str = "provider-output-inline-truncated";

/// Serializes and redacts a provider turn while guaranteeing that the result
/// fits the journal's configured per-event payload limit.
///
/// Ordinary turns retain the complete bounded contract. Oversized turns keep
/// their replay-visible text and accounting metadata, while redundant content
/// parts and opaque provider references are discarded before the text itself
/// is shortened.
#[allow(clippy::result_large_err)]
pub(crate) fn provider_turn_output_tape_payload(
    output: &ProviderTurnOutput,
    max_payload_bytes: usize,
) -> Result<String, Status> {
    let mut projected = bounded_provider_turn_output_for_persistence(output);
    let payload = serialize_redacted_provider_output(&projected)?;
    if payload.len() <= max_payload_bytes {
        return Ok(payload);
    }

    let full_text = std::mem::take(&mut projected.full_text);
    projected.content_parts.clear();
    projected.raw_provider_refs = ProviderRawProviderRefs {
        stream_spill_ref: Some(PROVIDER_OUTPUT_TRUNCATION_REF.to_owned()),
        ..ProviderRawProviderRefs::default()
    };
    projected.redaction_state.output_redacted = true;
    if !matches!(projected.usage.source.as_str(), "provider" | "estimated") {
        projected.usage.source = "bounded".to_owned();
    }

    let base_payload = serialize_redacted_provider_output(&projected)?;
    if base_payload.len() > max_payload_bytes {
        return Err(Status::resource_exhausted(format!(
            "journal payload limit {max_payload_bytes} bytes is too small for provider output metadata"
        )));
    }
    if full_text.is_empty() {
        return Ok(base_payload);
    }

    let source_text =
        full_text.strip_suffix(PROVIDER_OUTPUT_TRUNCATED_MARKER).unwrap_or(full_text.as_str());
    let mut low = 0_usize;
    let mut high = source_text.len();
    let mut best_payload = base_payload;
    while low <= high {
        let midpoint = low + (high - low) / 2;
        let prefix = utf8_prefix(source_text, midpoint);
        projected.full_text = String::with_capacity(
            prefix.len().saturating_add(PROVIDER_OUTPUT_TRUNCATED_MARKER.len()),
        );
        projected.full_text.push_str(prefix);
        projected.full_text.push_str(PROVIDER_OUTPUT_TRUNCATED_MARKER);
        let candidate = serialize_redacted_provider_output(&projected)?;
        if candidate.len() <= max_payload_bytes {
            best_payload = candidate;
            low = midpoint.saturating_add(1);
        } else if midpoint == 0 {
            break;
        } else {
            high = midpoint - 1;
        }
    }
    Ok(best_payload)
}

#[allow(clippy::result_large_err)]
fn serialize_redacted_provider_output(output: &ProviderTurnOutput) -> Result<String, Status> {
    let payload = serde_json::to_string(output).map_err(|error| {
        Status::internal(format!("failed to serialize provider turn output: {error}"))
    })?;
    Ok(crate::journal::redact_payload_json(payload.as_bytes()).unwrap_or(payload))
}

fn utf8_prefix(value: &str, max_bytes: usize) -> &str {
    if value.len() <= max_bytes {
        return value;
    }
    let mut boundary = max_bytes;
    while boundary > 0 && !value.is_char_boundary(boundary) {
        boundary -= 1;
    }
    &value[..boundary]
}

#[cfg(test)]
mod tests {
    use palyra_model_providers::{
        ProviderFinishReason, ProviderOutputContentPart, ProviderRawProviderRefs,
        ProviderRedactionState, ProviderTurnOutput, ProviderUsage,
    };
    use serde_json::json;

    use super::provider_turn_output_tape_payload;

    #[test]
    fn oversized_provider_contract_fits_configured_tape_limit() {
        let output = ProviderTurnOutput {
            full_text: "answer ".repeat(20_000),
            content_parts: (0..256)
                .map(|index| ProviderOutputContentPart::ToolCall {
                    proposal_id: format!("proposal-{index}-{}", "p".repeat(1_024)),
                    tool_name: format!("tool-{index}-{}", "t".repeat(1_024)),
                    input_json: json!({"value": "v".repeat(4_096)}),
                })
                .collect(),
            finish_reason: ProviderFinishReason::ToolCalls,
            usage: ProviderUsage::new(1, 2, "untrusted-source".repeat(1_024)),
            raw_provider_refs: ProviderRawProviderRefs {
                provider_response_id: Some("r".repeat(64 * 1_024)),
                provider_model_id: Some("m".repeat(64 * 1_024)),
                system_fingerprint: Some("f".repeat(64 * 1_024)),
                provider_trace_ref: Some("t".repeat(64 * 1_024)),
                stream_spill_ref: None,
            },
            redaction_state: ProviderRedactionState::default(),
        };

        let payload = provider_turn_output_tape_payload(&output, 4_096)
            .expect("oversized output should project into the tape budget");
        let persisted: ProviderTurnOutput =
            serde_json::from_str(&payload).expect("projected payload should remain valid");

        assert!(payload.len() <= 4_096);
        assert!(persisted.content_parts.is_empty());
        assert!(persisted.redaction_state.output_redacted);
        assert_eq!(persisted.usage.source, "bounded");
        assert_eq!(
            persisted.raw_provider_refs.stream_spill_ref.as_deref(),
            Some("provider-output-inline-truncated")
        );
    }

    #[test]
    fn ordinary_provider_contract_is_preserved() {
        let output = ProviderTurnOutput::text(
            "ordinary answer".to_owned(),
            ProviderFinishReason::Stop,
            ProviderUsage::new(1, 2, "provider"),
            ProviderRawProviderRefs::default(),
        );

        let payload = provider_turn_output_tape_payload(&output, 4_096)
            .expect("ordinary output should serialize");
        let persisted: ProviderTurnOutput =
            serde_json::from_str(&payload).expect("ordinary payload should remain valid");

        assert_eq!(persisted, output);
    }
}
