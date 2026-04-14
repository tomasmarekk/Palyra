# UX Telemetry Baseline Contract

Phase 1 UX telemetry is intentionally narrow: it measures workflow progress and friction without collecting conversation content or secret-bearing artifacts.

## Storage and audit model

- UX events are emitted as authenticated `system.operator.*` journal events.
- The canonical event name prefix for product telemetry is `system.operator.ux.`.
- Event payloads remain bounded, structured, and inspectable through existing diagnostics and system-event views.

## Privacy rules

- Allowed: surface name, section, mode, locale, stable identifiers, tool names, bounded outcome labels, latency buckets, and funnel step names.
- Forbidden: prompt text, transcript content, attachment bytes, raw workspace file contents, auth tokens, CSRF tokens, secret values, or arbitrary exception dumps.

## Baseline events

- `ux.surface.opened`
- `ux.mode.changed`
- `ux.handoff.opened`
- `ux.onboarding.step`
- `ux.chat.prompt_submitted`
- `ux.approval.resolved`
- `ux.run.inspected`
- `ux.session.resumed`
- `ux.voice.entry`
- `ux.canvas.entry`
- `ux.rollback.previewed`

## Funnel targets for Phase 1

- `setup_started`
- `provider_verified`
- `first_prompt_sent`
- `first_approval_resolved`
- `first_run_inspected`
- `second_session_resumed`

## Aggregates expected from the dashboard layer

- Funnel progression across the baseline steps above.
- Approval fatigue by tool, session, and surface.
- Top friction points by blocked/error outcome.
- Mode adoption, handoff usage, and canvas or voice entry counts by surface.

The machine-readable schema lives in `schemas/json/phase1_ux_telemetry_event.schema.json`.
