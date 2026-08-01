//! Fuzzes plugin invocation frame decoding and lifecycle validation.

#![no_main]

use libfuzzer_sys::fuzz_target;
use palyra_plugins_sdk::{PluginInvocationFrameV2, PluginInvocationTranscriptV2};

const MAX_FUZZ_INPUT_BYTES: usize = 64 * 1024;

fuzz_target!(|data: &[u8]| {
    if data.len() > MAX_FUZZ_INPUT_BYTES {
        return;
    }
    let Ok(frames) = serde_json::from_slice::<Vec<PluginInvocationFrameV2>>(data) else {
        return;
    };
    let _ = PluginInvocationTranscriptV2::from_frames(frames);
});
