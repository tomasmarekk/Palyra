# Palyra Browser Extension (Relay Companion v1)

This directory ships a minimal Manifest V3 extension artifact for M37 relay workflows.

The extension is intentionally narrow:

- local-only relay dispatch to daemon loopback URL,
- capture active tab URL + bounded DOM/text snapshot,
- bounded screenshot capture (payload omitted when above cap),
- relay action helpers for:
  - `open_tab`
  - `capture_selection`
  - `send_page_snapshot`

## Security posture

- Relay base URL is validated as loopback-only (`127.0.0.1`, `localhost`, `::1`).
- Relay token is never persisted outside extension local storage.
- `open_tab` is guarded by configurable URL prefix allowlist.
- Payloads are bounded before sending to daemon/browserd.
- No raw CDP passthrough or high-risk browser APIs are used.

## Load in Chrome / Chromium

1. Open `chrome://extensions`.
2. Enable `Developer mode`.
3. Click `Load unpacked`.
4. Select this folder: `apps/browser-extension`.
5. Copy extension runtime ID from extension details (or use auto-filled value in popup).

## Pairing / relay flow

1. In Palyra Console Browser section, mint relay token:
   - endpoint: `/console/v1/browser/relay/tokens`
   - use target `session_id`
   - set `extension_id` equal to this extension runtime ID
2. Open extension popup and set:
   - relay base URL (default `http://127.0.0.1:7142`)
   - session ID
   - relay token
   - extension ID
3. Save config.
4. Use relay buttons to dispatch supported actions.

## Local tests

```bash
npm --prefix apps/browser-extension test
```
