# Desktop Control Center (M50)

`apps/desktop` now hosts the **Palyra Desktop Control Center v1** implemented with Tauri.

## Platform support (v1)

- Supported: **Windows**, **macOS**, and **Linux**.
- Linux path currently uses a downstream backport patch for
  `GHSA-wrw7-89jp-8q8g` / `RUSTSEC-2024-0429` in `glib 0.18.5` because
  upstream Tauri Linux dependency constraints still pin the graph below `glib 0.20.0`.
- Patch source is vendored at `src-tauri/third_party/glib-0.18.5-patched`.

## What it does

- Starts/stops/restarts `palyrad` sidecar process.
- Optionally starts/stops/restarts `palyra-browserd` sidecar process.
- Shows health and quick facts:
  - gateway version + git hash,
  - uptime,
  - dashboard URL + access mode (`local`/`remote`),
  - Discord connector status (`discord:default`),
  - browser service status.
- Shows last redacted diagnostics errors from `/console/v1/diagnostics`.
- Shows redacted sidecar logs.
- Exports support bundles via `palyra support-bundle export --output ...`.
- Opens the discovered web dashboard target in the default browser (local or configured remote URL).

## Security behavior

- Control-plane HTTP calls are loopback-only (`127.0.0.1`).
- Console auth uses existing admin token login flow (`/console/v1/auth/login`).
- Logs are redacted with shared `palyra-common` redaction helpers.
- No channel secrets are stored by the desktop app.
- App-local state is stored in `<state_root>/desktop-control-center/state.json`.
- Linux `glib` advisory mitigation is documented in:
  - `src-tauri/docs/security/advisories/GHSA-wrw7-89jp-8q8g.md`
  - `src-tauri/docs/security/dependency-graph/glib.md`

## Running locally

1. Build runtime binaries:

```bash
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

## File layout

- `src-tauri/`: Rust backend + Tauri host.
- `ui/`: lightweight web UI rendered by Tauri.
