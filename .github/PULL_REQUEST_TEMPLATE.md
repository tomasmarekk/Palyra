## Scope

- [ ] The change is small and scoped to the stated product goal.
- [ ] I confirmed the diff does not include unrelated files or generated local-only artifacts.

## Guardrails

- [ ] Fail-closed security posture remains intact.
- [ ] Sensitive actions still require the existing approval or policy flow.
- [ ] Browser relay and pairing trust boundaries are unchanged unless this PR explicitly documents the change.
- [ ] Basic mode or onboarding shortcuts do not permanently hide approvals, diagnostics, or operator depth.
- [ ] Deterministic project context remains separate from learned memory.

## UX Telemetry And Handoffs

- [ ] New UX telemetry is bounded, content-free, and free of secrets.
- [ ] New handoffs carry only stable identifiers and operator intent.
- [ ] Unsupported handoffs degrade to a nearby safe view instead of failing silently.

## Review Notes

- [ ] I called out any high-risk area touched by this change.
- [ ] I listed the local validation I ran.
- [ ] I updated docs when the operator workflow or durable product contract changed.
