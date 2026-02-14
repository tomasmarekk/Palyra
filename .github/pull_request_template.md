## Summary

- What changed and why?
- Which milestone/deliverable does this PR satisfy?

## Validation

- [ ] `cargo fmt --all --check`
- [ ] `cargo clippy --workspace --all-targets -- -D warnings`
- [ ] `cargo test --workspace --locked`
- [ ] relevant docs updated

## Security Threat Review (required)

### Trust boundary impact

- Which trust boundary changed (if any)?
- What untrusted inputs were added or modified?
- What new capabilities can now be exercised?

### Abuse and failure analysis

- What are the top abuse cases for this change?
- How does policy deny or require approval for high-risk paths?
- Which tests prove negative/denied behavior?

### Data sensitivity and logging

- Could this change expose secrets, tokens, or PII?
- How is sensitive data redacted in logs/audit output?

## High-risk change checklist

- [ ] This PR does not touch high-risk areas.
- [ ] This PR touches one or more high-risk areas:
  - [ ] sandbox/runtime boundaries
  - [ ] policy enforcement and approvals
  - [ ] cryptography/key handling
  - [ ] updater/install/release trust chain
  - [ ] security CI gates or secret scanning
- [ ] If high-risk is selected, security sign-off (`@palo`) is requested before merge.

## Rollout and rollback

- Feature flag needed?
- Safe rollback plan if issue is found post-merge?
