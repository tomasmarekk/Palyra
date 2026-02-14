#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let _ = palyra_a2ui::parse_and_validate_document(data);
});
