//! Fuzzes provider transcript repair across every supported dialect.

#![no_main]

use libfuzzer_sys::fuzz_target;
use palyra_model_providers::{
    project_provider_transcript, ProviderTranscriptDialect,
    ProviderTranscriptProjectionRequest, ProviderTranscriptSourceMessage,
};

const MAX_FUZZ_INPUT_BYTES: usize = 64 * 1024;

fuzz_target!(|data: &[u8]| {
    if data.len() > MAX_FUZZ_INPUT_BYTES {
        return;
    }
    let Ok(messages) = serde_json::from_slice::<Vec<ProviderTranscriptSourceMessage>>(data) else {
        return;
    };
    let dialect = match data.first().copied().unwrap_or_default() % 4 {
        0 => ProviderTranscriptDialect::ProviderNeutral,
        1 => ProviderTranscriptDialect::OpenAiChatCompletions,
        2 => ProviderTranscriptDialect::OpenAiResponses,
        _ => ProviderTranscriptDialect::AnthropicMessages,
    };
    let _ = project_provider_transcript(ProviderTranscriptProjectionRequest {
        dialect,
        model_id: "fuzz-model".to_owned(),
        projection_epoch: 1,
        messages,
    });
});
