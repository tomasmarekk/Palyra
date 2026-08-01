//! Fuzzes closed provider transcript request decoding and projection.

#![no_main]

use libfuzzer_sys::fuzz_target;
use palyra_model_providers::{
    project_provider_transcript, ProviderTranscriptProjectionRequest,
};

const MAX_FUZZ_INPUT_BYTES: usize = 64 * 1024;

fuzz_target!(|data: &[u8]| {
    if data.len() > MAX_FUZZ_INPUT_BYTES {
        return;
    }
    let Ok(request) = serde_json::from_slice::<ProviderTranscriptProjectionRequest>(data) else {
        return;
    };
    let _ = project_provider_transcript(request);
});
