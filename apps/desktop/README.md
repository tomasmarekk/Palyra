# Desktop Companion

`apps/desktop` hosts the **Palyra Desktop Companion** implemented with Tauri. The current desktop
runtime keeps the original control-center supervision features, but now layers a companion shell on
top of the same local supervisor and `/console/v1` control-plane APIs.

## Platform support (v1)

- Supported desktop runtime targets: **Windows** and **macOS**.
- Linux desktop runtime remains disabled while the Tauri Linux dependency chain is still pinned
  below `glib 0.20.0`.
- The repository keeps a downstream backport patch for
  `GHSA-wrw7-89jp-8q8g` / `RUSTSEC-2024-0429` in `glib 0.18.5` because
  upstream Tauri Linux dependency constraints still pin the graph below `glib 0.20.0`.
- Patch source is vendored at `src-tauri/third_party/glib-0.18.5-patched`.

## What it does

- Starts/stops/restarts `palyrad` sidecar process.
- Optionally starts/stops/restarts `palyra-browserd` sidecar process.
- Starts/stops/restarts the first-party `palyra node` host when the local desktop node is enrolled.
- Shows a runtime launcher and monitor surface with:
  - gateway version + git hash,
  - uptime,
  - dashboard URL + access mode (`local`/`remote`),
  - gateway, browserd, and node-host process state,
  - browser service status.
- Hosts the desktop companion shell with:
  - session-aware chat,
  - approval inbox actions,
  - desktop/system notifications,
  - tray/menu-bar ambient runtime with quick panel and voice overlay surfaces,
  - inventory and capability detail,
  - desktop node enrollment, repair, and local reset controls,
  - reconnect-safe offline drafts,
  - onboarding and rollout state summary,
  - voice audit trail, consent posture, selected microphone/voice preferences, and optional
    silence detection.
- Shows the current warning/diagnostics queue sourced from `/console/v1/diagnostics`.
- Opens the discovered web dashboard target in the default browser with scoped handoff to chat,
  approvals, inventory/access, or overview routes.
- Persists local companion state so active section, selected session/device, notifications, rollout
  flags, and offline drafts survive restarts.

## Companion architecture

- Native Tauri surface:
  - sidecar supervision,
  - local persisted state,
  - native window reveal, tray/menu-bar runtime, start-on-login sync, global hotkey registration,
    and notification permission handling,
  - secure browser handoff built on the existing admin-token + CSRF console session flow.
- Shared control-plane contracts:
  - desktop reads the same chat session catalog, transcript, approvals, and inventory APIs as the
    web console,
  - desktop handoff targets the browser console instead of reimplementing every advanced surface.
- First-party node host:
  - the desktop supervises the existing CLI-managed `palyra node` lifecycle instead of duplicating
    pairing or capability runtime logic inside Tauri,
  - enrollment mints a node pairing code through `/console/v1/pairing/requests/code`, runs
    `palyra node install`, approves the resulting request through the authenticated control-plane
    session, and then lets the supervisor keep `palyra node run --json` alive,
  - node status is read locally from `palyra node status --json`, so enrollment, trust expiry, and
    repair state remain visible even before the browser console is opened.
- Persisted local state:
  - rollout flags: `companion_shell_enabled`, `desktop_notifications_enabled`,
    `ambient_companion_enabled`, `offline_drafts_enabled`, `voice_capture_enabled`,
    `voice_overlay_enabled`, `voice_silence_detection_enabled`, `tts_playback_enabled`,
    `release_channel`,
  - companion preferences: active section, session, device, and latest run,
  - ambient preferences: start-on-login, global hotkey, last preferred surface, and hotkey
    conflict state,
  - bounded notifications, bounded offline draft queue, and bounded voice audit trail.
- Trust model:
  - desktop companion never bypasses existing approval or browser handoff trust boundaries,
  - desktop node enrollment still uses the same pairing + mTLS trust chain as any other node
    client; the desktop only automates the already-authenticated operator path,
  - native capability execution stays intentionally narrow in v1: `desktop.open_url` and
    `desktop.open_path` require local mediation posture, while `system.health` and
    `system.identity` stay automatic and audit-friendly,
  - offline drafts are stored locally and only sent after explicit operator action.
  - voice capture is push-to-talk only, uploads only after the operator stops recording, reuses the
    existing attachment/transcript pipeline, and keeps ambient listening disabled,
  - TTS playback is a local convenience layer only and stays behind the same rollout/consent
    posture as the companion shell.

## Desktop node lifecycle

- Enrollment:
  - requires the local gateway to be running,
  - uses the local gateway identity store via `PALYRA_GATEWAY_IDENTITY_STORE_DIR`,
  - stores node-host config and identity material under `<runtime_root>/node-host/`.
- Runtime:
  - the node host publishes capability inventory during registration,
  - dispatched capability requests move through `queued`, `dispatched`,
    `awaiting_local_mediation`, `succeeded`, `failed`, or `timed_out`,
  - inventory detail in the web console and the desktop companion both surface the active device,
    capability posture, and recent request outcomes.
- Repair and reset:
  - `Repair node` re-runs the local install flow with the existing device id when possible and is
    intended for trust mismatch or expired local material,
  - `Reset local node` removes only local node-host state; remote revoke/remove remains an explicit
    operator action through inventory and approvals surfaces,
  - a stopped or missing enrolled node host degrades the desktop runtime snapshot instead of
    pretending the local desktop capability path is still available.

## Rollout and reconnect behavior

- The richer shell is feature-flagged by local persisted rollout state instead of replacing the
  supervisor contract outright.
- Connection state is surfaced as `connected`, `reconnecting`, or `offline` from companion refresh
  results plus local runtime expectations.
- Desktop notifications are informative only; they do not auto-approve or auto-send anything.
- Tray/menu-bar tooltip keeps the current connection state, unread notifications, pending
  approvals, active-run count, and queued draft count visible even when the full window is hidden.
- The quick panel is the default ambient invoke surface; it keeps recent sessions, mini composer,
  pending approvals, active-run handoff, and offline draft retry/discard controls available.
- The voice overlay is a separate ambient surface for the structured lifecycle
  `idle -> recording -> transcribing -> review -> sending -> speaking/error/cancelled`.
- Voice overlay focus rules differ from the quick panel: the quick panel auto-hides on focus loss,
  while the voice overlay stays visible until the operator hides it so transcript review and TTS
  controls are not lost during app switching.
- Offline drafts are bounded, local-only, and removed only after successful resend or explicit
  operator discard.
- Browser handoff preserves the current `sessionId`, `deviceId`, and optional `runId` whenever a
  destination route supports that context.
- Experimental governance keeps native canvas and ambient companion work under the same structured
  A2UI contract:
  - `/console/v1/diagnostics` publishes native canvas rollout state, bounded limits, security
    review checklist, and explicit exit criteria,
  - the desktop UI mirrors that governance locally so operators can see when canvas is in preview
    and confirm that ambient mode remains `push-to-talk` only,
  - disabling `canvas_host.enabled` or the local voice/TTS rollout flags cleanly removes the
    experimental surface without touching the core companion shell.

## Portable release layout

- Release bundles keep `palyra-desktop-control-center`, `palyrad`, `palyra-browserd`, and `palyra`
  in the same directory.
- `scripts/release/install-desktop-package.ps1` exposes `palyra` as a first-class user command by
  default. The managed CLI root is `%LOCALAPPDATA%\Palyra\bin` on Windows and `~/.local/bin` on
  macOS/Linux, or an explicit test root when `-CliCommandRoot` is provided.
- Sidecar resolution already prefers colocated binaries next to the desktop executable, so the
  portable archive layout matches the runtime contract.
- The canonical packaging smoke is `pwsh -NoLogo -File ../../scripts/test/run-release-smoke.ps1`.
- Windows clean-install manual testing can use:
  - `pwsh -NoLogo -File ../../scripts/test/install-clean-desktop.ps1 -Launch`
  - `pwsh -NoLogo -File ../../scripts/test/uninstall-clean-desktop.ps1`
  - default harness root: `%LOCALAPPDATA%\Palyra-TestHarness`
- Installer metadata is written to `install-metadata.json` inside the install root and is consumed
  by `scripts/release/uninstall-package.ps1` for reversible CLI shim cleanup.

## Security behavior

- Control-plane HTTP calls are loopback-only (`127.0.0.1`).
- Console auth uses existing admin token login flow (`/console/v1/auth/login`).
- Logs are redacted with shared `palyra-common` redaction helpers.
- No channel secrets are stored by the desktop app.
- App-local desktop state is stored in `<state_root>/desktop-control-center/state.json`.
- Desktop runtime state defaults to `<state_root>/desktop-control-center/runtime` and can be
  confirmed or overridden during onboarding.
- Desktop node-host trust material is stored under the runtime root and continues to rely on the
  existing mTLS certificate issuance and approval model; there is no desktop-only bypass path.
- Voice capture remains explicit push-to-talk only:
  - the operator must grant microphone consent before first use,
  - audio is uploaded only after recording stops,
  - the selected microphone, TTS voice, mute state, consent timestamps, and last voice audit
    entries are persisted locally for restart-safe review,
  - optional silence detection is guarded behind rollout and local preference toggles,
  - TTS playback uses the OS default output device and reads only explicit assistant selections.
- Linux `glib` advisory mitigation is documented in:
  - `src-tauri/docs/security/advisories/GHSA-wrw7-89jp-8q8g.md`
  - `src-tauri/docs/security/dependency-graph/glib.md`

## Running locally

1. Build runtime binaries:

```bash
vp install
./scripts/test/ensure-desktop-ui.sh
cargo build --workspace --locked
```

2. Launch the desktop control center:

```bash
cargo run --manifest-path apps/desktop/src-tauri/Cargo.toml
```

Linux release-mode regression check:

```bash
cargo test --manifest-path apps/desktop/src-tauri/Cargo.toml --release --locked
```

3. If binaries are not on `PATH`, set explicit overrides:

```bash
PALYRA_DESKTOP_PALYRAD_BIN=/abs/path/palyrad
PALYRA_DESKTOP_BROWSERD_BIN=/abs/path/palyra-browserd
PALYRA_DESKTOP_PALYRA_BIN=/abs/path/palyra
```

Windows PowerShell equivalents:

```powershell
$env:PALYRA_DESKTOP_PALYRAD_BIN = "C:\path\to\palyrad.exe"
$env:PALYRA_DESKTOP_BROWSERD_BIN = "C:\path\to\palyra-browserd.exe"
$env:PALYRA_DESKTOP_PALYRA_BIN = "C:\path\to\palyra.exe"
```

For a release-like clean install loop on Windows, use the test harness scripts above instead of
`cargo run`; they package the portable bundle, install it outside the repo, and launch it with an
isolated `PALYRA_STATE_ROOT`.

For UI-only iteration without launching the desktop runtime, use:

```bash
vp run desktop-ui:dev
```

Validate the desktop companion changes with:

```bash
npm --prefix ui run typecheck
npm --prefix ui run build
cargo check --manifest-path src-tauri/Cargo.toml
cargo test --manifest-path src-tauri/Cargo.toml --locked
```

## QA checklist

- Windows/macOS launch:
  - desktop window opens and can reveal itself after startup,
  - `start`, `stop`, `restart`, and `Open dashboard` still work.
- Companion shell:
  - session list loads,
  - chat transcript refresh works,
  - sending while online creates a run and refreshes transcript,
  - sending while control plane is unavailable queues an offline draft instead of silently losing
    input,
  - ambient quick panel can create a session, send a prompt, inspect recent sessions, and hand off
    to approvals/run/browser detail,
  - tray/menu-bar tooltip reflects connection state, approvals, active runs, and queued drafts.
- Ambient runtime:
  - closing the main window leaves the tray runtime alive,
  - `Start on Login` and `Global Hotkey` can be toggled from desktop settings and tray menu,
  - hotkey conflicts surface a readable error instead of failing silently,
  - quick panel auto-hides on focus loss, while voice overlay remains available for review until
    explicitly hidden.
- Voice workflow:
  - voice overlay can safely create a quick session when no active session is selected,
  - lifecycle is visible as `idle`, `recording`, `transcribing`, `review`, `sending`,
    `speaking`, `error`, or `cancelled`,
  - hold-to-talk starts and stops deterministically, transcript review stays editable before send,
  - microphone/TTS consent, mute posture, selected microphone/voice, and voice audit trail render
    in the full companion,
  - denied microphone permission, missing device, transcription failures, queued-offline send, TTS
    failure, and mute/stop controls all leave the voice draft or state understandable,
  - optional silence detection can be enabled only when rollout allows it and should stop a silent
    recording into review without breaking normal push-to-talk flow.
- Desktop node:
  - `Enroll node` pairs a desktop-first node client and the process monitor shows `node_host`,
  - `Repair node` recovers from missing local trust material without bypassing pairing approvals,
  - `Reset local node` removes local node-host state and leaves a clear handoff to remote
    revoke/remove actions,
  - capability inventory shows execution posture (`automatic` vs `local_mediation`) and capability
    request history shows queued, mediation, success, failure, and timeout outcomes.
- Approval flow:
  - pending approvals are visible on desktop,
  - approve/deny actions call the same protected API as the browser console,
  - browser handoff keeps session/run context.
- Access/inventory:
  - device capability summary renders,
  - degraded or stale devices remain visible,
  - handoff to browser access/chat routes preserves the selected device/session.
- Rollout and onboarding:
  - release channel and rollout flags are visible,
  - notification permission can be requested,
  - reconnect transitions produce readable notifications without auto-sending queued drafts.

## File layout

- `src-tauri/`: Rust backend + Tauri host.
- `ui/`: React/Vite/TypeScript desktop frontend rendered by Tauri.
