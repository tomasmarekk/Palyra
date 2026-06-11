//! Fuzzes process-runner tool input parsing under the configured byte limit.

#![no_main]

use libfuzzer_sys::fuzz_target;
use palyra_common::process_runner_input::parse_process_runner_tool_input;

const MAX_FUZZ_INPUT_BYTES: usize = 32 * 1024;

fuzz_target!(|data: &[u8]| {
    if data.len() > MAX_FUZZ_INPUT_BYTES {
        return;
    }
    let _ = parse_process_runner_tool_input(data);
});
