# Cross-Surface Handoff Contract

The handoff contract defines how Palyra surfaces carry operator context between web, desktop, TUI, and future mobile flows.

## Goals

- Carry stable context without carrying secrets.
- Reuse one identifier model across surfaces.
- Keep unsupported handoffs diagnosable and safely degradable.

## Canonical fields

- `section`: target product section or surface route.
- `sessionId`: orchestrator session identifier.
- `runId`: run identifier.
- `deviceId`: inventory or paired-device identifier.
- `objectiveId`: objective identifier.
- `canvasId`: canvas identifier.
- `intent`: CTA intent such as `approve`, `open-workspace`, `reopen-canvas`, or `resume-session`.
- `source`: optional source surface label such as `web`, `desktop`, or `tui`.

## Rules

1. Handoffs may carry only stable identifiers and operator intent.
2. Handoffs must never carry admin tokens, CSRF tokens, raw prompt text, secret values, or arbitrary file content.
3. If the target surface does not support the requested section or intent, it must fall back to the nearest meaningful view and preserve any safe identifiers that still apply.
4. A handoff may narrow attention, but it may not grant permissions or bypass existing approval flows.

## Current Phase 1 fallback expectations

- Unsupported section: open the closest safe section for the supplied identifiers.
- Unsupported intent: keep the target section and show the surrounding context without auto-executing the CTA.
- Unsupported identifier: ignore only the unsupported field and keep the rest of the handoff payload intact.

The machine-readable schema lives in `schemas/json/phase1_cross_surface_handoff.schema.json`.
