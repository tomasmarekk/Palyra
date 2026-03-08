#![no_main]

use libfuzzer_sys::fuzz_target;
use palyra_connectors::{InboundMessageEvent, OutboundMessageRequest};

fuzz_target!(|data: &[u8]| {
    let Ok(payload) = serde_json::from_slice::<serde_json::Value>(data) else {
        return;
    };
    if let Ok(event) = serde_json::from_value::<InboundMessageEvent>(payload.clone()) {
        let _ = event.validate(16 * 1024);
    }
    if let Ok(request) = serde_json::from_value::<OutboundMessageRequest>(payload) {
        let _ = request.validate(16 * 1024);
    }
});
