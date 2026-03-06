# glib Dependency Graph (desktop `src-tauri`)

Captured on 2026-03-06 with downstream patch governance automation enabled for `glib 0.18.5`.

## Commands

```bash
cargo tree --manifest-path apps/desktop/src-tauri/Cargo.toml --target all -i glib
cargo metadata --manifest-path apps/desktop/src-tauri/Cargo.toml --format-version 1 --locked
bash scripts/check-desktop-glib-patch.sh
```

## `cargo tree` evidence

```text
glib v0.18.5 (<repo-root>/apps/desktop/src-tauri/third_party/glib-0.18.5-patched)
├── atk v0.18.2
│   └── gtk v0.18.2
│       └── tauri v2.10.2
│           └── palyra-desktop-control-center v0.1.0
...
```

## `cargo metadata` evidence

```text
glib.id=path+file:///<repo-root>/apps/desktop/src-tauri/third_party/glib-0.18.5-patched#glib@0.18.5
glib.source=
glib.manifest_path=<repo-root>/apps/desktop/src-tauri/third_party/glib-0.18.5-patched/Cargo.toml
```

`<repo-root>` is intentionally redacted to avoid publishing machine-local filesystem details.

The `path+file://` package id confirms that the desktop crate resolves `glib` from the local patched source, not from crates.io.

## Governance checksum

- Governance contract: `apps/desktop/src-tauri/third_party/glib-0.18.5-patched/PALYRA_PATCH_GOVERNANCE.env`
- Owner: `@marektomas-cz`
- Review cadence: every 30 days
- Patched file checksum (`src/variant_iter.rs`, SHA-256): `821cbd7f2bdbf5407236cf15cc848e2d660581d97428924de77ca32899c31a95`

The governance script cross-checks the checksum above against the vendored file on disk and fails if
`cargo metadata` no longer resolves the desktop crate through the vendored patch path.
