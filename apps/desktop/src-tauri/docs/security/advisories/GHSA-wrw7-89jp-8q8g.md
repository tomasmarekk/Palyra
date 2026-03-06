# GHSA-wrw7-89jp-8q8g (`glib`) downstream mitigation

## Scope

- Component: `apps/desktop/src-tauri`
- Advisory: `GHSA-wrw7-89jp-8q8g` / `RUSTSEC-2024-0429`
- Affected crate range: `glib >=0.15.0, <0.20.0`
- First upstream fixed version: `glib 0.20.0`

## Why this is relevant

The advisory describes UB in `glib::VariantStrIter` (`Iterator` and `DoubleEndedIterator`) caused by passing an out-pointer as `&p` instead of `&mut p` in `impl_get`.

For desktop Linux builds, Tauri currently resolves through GTK 0.18-era crates, which constrain the graph to `glib 0.18.x`. A direct upgrade to `glib >=0.20.0` is not resolvable without upstream runtime migration.

## Why we cannot upgrade directly yet

At resolution time Dependabot and cargo both report:

- latest resolvable in current graph: `glib 0.18.5`
- lowest non-vulnerable upstream release: `glib 0.20.0`

This is caused by upstream dependency constraints in the Tauri Linux stack (via `tauri-runtime-wry` / GTK 0.18 lineage).

## Applied mitigation (downstream backport)

We vendor `glib 0.18.5` under:

- `apps/desktop/src-tauri/third_party/glib-0.18.5-patched`

and override crates.io for this crate via:

- `[patch.crates-io] glib = { path = "third_party/glib-0.18.5-patched" }`

Backported code change:

- File: `third_party/glib-0.18.5-patched/src/variant_iter.rs`
- Function: `VariantStrIter::impl_get`
- Change:
  - `let p: *mut libc::c_char = std::ptr::null_mut();` -> `let mut p: *mut libc::c_char = std::ptr::null_mut();`
  - `&p` -> `&mut p` in `ffi::g_variant_get_child(...)`

Reference upstream fix:

- `gtk-rs/gtk-rs-core#1343`

Governance record:

- Machine-readable contract: `third_party/glib-0.18.5-patched/PALYRA_PATCH_GOVERNANCE.env`
- Owner: `@marektomas-cz`
- Review cadence: every 30 days while the downstream patch remains active
- Patched file checksum (`src/variant_iter.rs`, SHA-256): `821cbd7f2bdbf5407236cf15cc848e2d660581d97428924de77ca32899c31a95`
- Verification command: `bash scripts/check-desktop-glib-patch.sh`

## Validation and regression coverage

- Dependency resolution proof: `docs/security/dependency-graph/glib.md`
- Governance verification:
  - `bash scripts/check-desktop-glib-patch.sh`
  - verifies the expected patched file checksum, the `Cargo.toml` patch override, and `cargo metadata`
    resolution to the vendored path
- Linux-only regression test:
  - `tests/glib_variantstriter_regression.rs`
  - exercises both forward collection and `DoubleEndedIterator` methods (`next_back`, `nth_back`)
- CI executes release-mode regression path on Linux:
  - `cargo test --manifest-path apps/desktop/src-tauri/Cargo.toml --release --locked`

## Alert handling policy

GitHub Dependabot alert for this advisory is expected to stay non-upgradable until upstream constraints are lifted.

Repository handling:

- keep downstream backport active,
- keep the governance record current (owner, cadence, checksum, upstream fix reference),
- keep regression test and Linux release CI gate active,
- keep the desktop glib patch governance check green in local pre-push and CI security gates,
- keep advisory dismissal comment linked to this mitigation and to upstream fix reference.

## Exit plan (remove this patch)

Remove this downstream patch when all are true:

1. Tauri Linux dependency chain resolves to non-vulnerable upstream `glib` (`>=0.20.0`) without local override.
2. `cargo tree -i glib` for desktop no longer resolves to local patched path.
3. Desktop Linux release-mode tests pass without local patch.
4. Documentation and alert dismissal note are updated/removed accordingly.
