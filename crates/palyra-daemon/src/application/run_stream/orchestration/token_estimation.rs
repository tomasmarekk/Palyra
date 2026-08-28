//! Conservative token estimates used by runtime planning and budget guards.

use super::*;

pub(super) fn runtime_kernel_provider_request_input_tokens(request: &ProviderRequest) -> u64 {
    let message_tokens = request
        .effective_messages()
        .iter()
        .map(estimate_provider_message_input_tokens)
        .fold(0_u64, u64::saturating_add);
    let tool_catalog_tokens = request
        .tool_catalog_snapshot
        .as_ref()
        .map(estimate_provider_visible_tool_catalog_tokens)
        .unwrap_or_default();
    let vision_tokens =
        u64::try_from(request.vision_inputs.len()).unwrap_or(u64::MAX).saturating_mul(256);
    message_tokens.saturating_add(tool_catalog_tokens).saturating_add(vision_tokens)
}

fn estimate_provider_visible_tool_catalog_tokens(catalog: &Value) -> u64 {
    let Some(exposed_bytes) = catalog.get("estimated_exposed_tool_bytes").and_then(Value::as_u64)
    else {
        // Compatibility requests without a typed catalog snapshot retain the
        // conservative whole-value estimate.
        return estimate_background_budget_text_tokens(catalog.to_string().as_str());
    };
    let exposed_tool_count =
        catalog.get("exposed_tool_count").and_then(Value::as_u64).unwrap_or_default();
    // The catalog builder measures the exact serialized provider tool
    // payloads. Add bounded array framing before converting bytes to the same
    // dense-text token estimate used for messages.
    let wire_bytes =
        exposed_bytes.saturating_add(exposed_tool_count.saturating_sub(1)).saturating_add(2);
    wire_bytes.saturating_add(3) / 4
}

fn estimate_provider_message_input_tokens(message: &ProviderMessage) -> u64 {
    let content_tokens = message
        .content
        .iter()
        .map(|part| match part {
            ProviderMessageContentPart::Text { text } => {
                estimate_background_budget_text_tokens(text)
            }
            ProviderMessageContentPart::Image { .. } => 256,
        })
        .fold(0_u64, u64::saturating_add);
    let tool_call_tokens = message
        .tool_calls
        .iter()
        .map(|tool_call| {
            estimate_background_budget_text_tokens(tool_call.proposal_id.as_str())
                .saturating_add(estimate_background_budget_text_tokens(
                    tool_call.tool_name.as_str(),
                ))
                .saturating_add(estimate_background_budget_text_tokens(
                    tool_call.input_json.to_string().as_str(),
                ))
        })
        .fold(0_u64, u64::saturating_add);
    content_tokens.saturating_add(tool_call_tokens).saturating_add(4)
}

// Dense text without whitespace must still consume a conservative share of
// the provider budget.
fn estimate_background_budget_text_tokens(value: &str) -> u64 {
    if value.is_empty() {
        return 0;
    }
    let whitespace_tokens = estimate_token_count(value);
    let character_tokens =
        u64::try_from(value.chars().count()).unwrap_or(u64::MAX).saturating_add(3) / 4;
    whitespace_tokens.max(character_tokens).max(1)
}
