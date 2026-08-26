//! Principal-bound credentials shared by the daemon and browser service.

use ring::hmac;

const BROWSER_PRINCIPAL_TOKEN_CONTEXT: &[u8] = b"palyra.browserd.principal-token.v1\0";

/// Derives the bearer credential for one normalized browser caller principal.
///
/// The root browser-service secret is never sent on principal-owned RPCs.
/// Callers must pass the exact trimmed principal that will be carried in
/// `x-palyra-principal`; changing either the secret or principal changes the
/// derived credential.
#[must_use]
pub fn derive_browser_principal_token(root_secret: &[u8], principal: &str) -> String {
    let key = hmac::Key::new(hmac::HMAC_SHA256, root_secret);
    let mut context = hmac::Context::with_key(&key);
    context.update(BROWSER_PRINCIPAL_TOKEN_CONTEXT);
    context.update(principal.as_bytes());
    hex::encode(context.sign().as_ref())
}

#[cfg(test)]
mod tests {
    use super::derive_browser_principal_token;

    #[test]
    fn browser_principal_tokens_are_deterministic_and_identity_bound() {
        let first = derive_browser_principal_token(b"root-secret", "user:alpha");
        let repeated = derive_browser_principal_token(b"root-secret", "user:alpha");
        let other_principal = derive_browser_principal_token(b"root-secret", "user:beta");
        let other_secret = derive_browser_principal_token(b"other-secret", "user:alpha");

        assert_eq!(first, repeated);
        assert_ne!(first, other_principal);
        assert_ne!(first, other_secret);
        assert!(!first.contains("root-secret"));
        assert_eq!(first.len(), 64);
    }
}
