#![no_main]

use std::{collections::HashSet, sync::Mutex};

use libfuzzer_sys::fuzz_target;
use palyra_common::{
    verify_webhook_payload, ReplayNonceStore, WebhookPayloadError, WebhookSignatureVerifier,
};

#[derive(Default)]
struct InMemoryReplayNonceStore {
    consumed: Mutex<HashSet<String>>,
}

impl ReplayNonceStore for InMemoryReplayNonceStore {
    fn consume_once(&self, nonce: &str, _timestamp_unix_ms: u64) -> Result<(), WebhookPayloadError> {
        let mut guard = self
            .consumed
            .lock()
            .map_err(|_| WebhookPayloadError::InvalidValue("replay_protection.nonce"))?;
        if !guard.insert(nonce.to_owned()) {
            return Err(WebhookPayloadError::InvalidValue("replay_protection.nonce"));
        }
        Ok(())
    }
}

struct AcceptingSignatureVerifier;

impl WebhookSignatureVerifier for AcceptingSignatureVerifier {
    fn verify(&self, _payload_bytes: &[u8], signature: &str) -> Result<(), WebhookPayloadError> {
        if signature.trim().is_empty() {
            return Err(WebhookPayloadError::MissingField("replay_protection.signature"));
        }
        Ok(())
    }
}

fuzz_target!(|data: &[u8]| {
    let nonce_store = InMemoryReplayNonceStore::default();
    let verifier = AcceptingSignatureVerifier;
    let _ = verify_webhook_payload(data, &nonce_store, &verifier);
});
