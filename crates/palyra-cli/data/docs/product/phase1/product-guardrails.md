# Product Guardrails And Non-Regression Contract

Phase 1 productization work must improve discoverability and operator ergonomics without weakening the properties that make Palyra safe to operate.

## Non-negotiable guardrails

1. Keep the fail-closed security posture. UX shortcuts may explain or surface security decisions, but they must not silently bypass auth, policy, approvals, pairing, or transport trust boundaries.
2. Keep approvals explicit. Basic mode, guided flows, and handoffs may reduce clutter, but they must not auto-authorize sensitive actions or hide pending approvals from an authorized operator.
3. Keep browser relay opt-in and bounded. Product work must not replace the existing relay and pairing trust model with an always-on managed browser default.
4. Keep operator depth available. Basic mode is progressive disclosure over the same control plane, not a separate reduced backend or a permanent capability filter.
5. Keep deterministic project context separate from learned memory. `PALYRA.md`, workspace rules, and file-backed context remain distinct from learned memory and recall.
6. Keep auditability intact. New shell modes, telemetry, and handoffs must remain inspectable through existing diagnostics or journal-backed system events.
7. Keep privacy-preserving telemetry. Baseline UX events must record steps, outcomes, counts, and bounded identifiers only. They must never include prompt bodies, secret values, raw attachments, or arbitrary file contents.

## Required review questions

Any milestone that changes onboarding, shell navigation, approvals, browser affordances, session continuity, memory recall, or automation discovery must answer these questions:

- Does the change preserve fail-closed behavior when configuration, auth, or trust material is missing?
- Can an operator still inspect pending approvals, deployment posture, browser pairing state, and diagnostics without switching products or bypassing a warning?
- Does any handoff carry only stable identifiers and intent, without secrets, tokens, or prompt content?
- Does Basic mode reduce noise without permanently hiding a privileged section from an authorized operator?
- Are new telemetry fields limited to bounded metadata and redacted identifiers?
- Does the change preserve an audit trail or a diagnosable trace for operator troubleshooting?

## Canonical invariants to keep covered by tests

- Console login and desktop browser handoff remain authenticated and CSRF-protected.
- Secret-bearing diagnostics, auth failures, and config surfaces remain redacted.
- Sensitive tool execution still requires explicit approval unless policy already allows it.
- Browser relay and pairing flows keep their existing trust boundaries.
- Desktop offline drafts remain available when the control plane is unavailable.
- The primary operator sections remain present in the control-plane shell even when Basic mode is enabled.

## How future milestones should use this contract

- Reference this document instead of re-stating the same trust and UX boundaries.
- Add milestone-specific constraints only when they are stricter than this baseline.
- Extend the invariant list when a new durable surface becomes first-class enough to deserve regression coverage.
