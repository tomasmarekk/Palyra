# Desktop Control Center

`apps/desktop` now hosts the **Palyra Desktop Control Center** implemented with Tauri.

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
- Shows a minimal launcher and monitor surface with:
  - gateway version + git hash,
  - uptime,
  - dashboard URL + access mode (`local`/`remote`),
  - gateway and browserd process state,
  - browser service status.
- Shows the current warning/diagnostics queue sourced from `/console/v1/diagnostics`.
- Opens the discovered web dashboard target in the default browser (local or configured remote URL).
- Does not re-embed the old onboarding/OpenAI/Discord/support/log/settings desktop workflows.

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

## File layout

- `src-tauri/`: Rust backend + Tauri host.
- `ui/`: React/Vite/TypeScript desktop frontend rendered by Tauri.
