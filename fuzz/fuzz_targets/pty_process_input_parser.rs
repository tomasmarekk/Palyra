//! Fuzzes the closed process input contract used to request PTY sessions.

#![no_main]

use libfuzzer_sys::fuzz_target;
use palyra_common::process_runner_input::parse_process_runner_tool_input;

const MAX_FUZZ_INPUT_BYTES: usize = 32 * 1024;

fuzz_target!(|data: &[u8]| {
    if data.len() > MAX_FUZZ_INPUT_BYTES {
        return;
    }
    let Ok(input) = parse_process_runner_tool_input(data) else {
        return;
    };
    if input.pty {
        let _ = input.stdin_requested();
        let _ = input.effective_lifetime_mode();
    }
});
