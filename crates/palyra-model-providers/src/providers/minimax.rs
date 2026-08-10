//! MiniMax identity and Anthropic-compatible capability overrides.

use crate::config::ProviderCapabilitiesSnapshot;

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
