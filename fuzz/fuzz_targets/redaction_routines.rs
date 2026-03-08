#![no_main]

use libfuzzer_sys::fuzz_target;
use palyra_common::redaction::{
    redact_auth_error, redact_cookie, redact_header, redact_token, redact_url,
};

fuzz_target!(|data: &[u8]| {
    let Ok(input) = std::str::from_utf8(data) else {
        return;
    };
    let midpoint = input.len() / 2;
    let split_at = input
        .char_indices()
        .map(|(index, _)| index)
        .find(|index| *index >= midpoint)
        .unwrap_or(input.len());
    let (header_name, header_value) = input.split_at(split_at);
    let _ = redact_auth_error(input);
    let _ = redact_url(input);
    let _ = redact_token(input);
    let _ = redact_cookie(input);
    let _ = redact_header(header_name, header_value);
});
