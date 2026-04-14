# I18n And Shell Foundation

Phase 1 establishes two shared UI foundations:

- locale-aware copy and pseudo-localization for the most exposed shell strings,
- Basic and Advanced shell modes as progressive disclosure over the same backend.

## Locale decisions

- Primary locale: `en`.
- Secondary validation locale for layout sanity: `qps-ploc` pseudo-localization.
- Fallback behavior: missing keys always fall back to `en`.
- Web and desktop should use the same message-key vocabulary for shell and guidance copy.
- TUI should centralize visible copy into resources even if the first iteration stays English-first.

## Shell mode decisions

- `Basic` highlights only the core first-success surfaces.
- `Advanced` exposes the full operator rail directly.
- Mode switches are reversible, persisted per operator/browser context, and must never hide approvals or critical posture signals.
- Direct links to advanced sections remain valid even when Basic mode is active.
