#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
governance_file="$repo_root/apps/desktop/src-tauri/third_party/glib-0.18.5-patched/PALYRA_PATCH_GOVERNANCE.env"
desktop_manifest="$repo_root/apps/desktop/src-tauri/Cargo.toml"
desktop_lockfile="$repo_root/apps/desktop/src-tauri/Cargo.lock"
desktop_regression_test="$repo_root/apps/desktop/src-tauri/tests/glib_variantstriter_regression.rs"

if [[ ! -f "$governance_file" ]]; then
  echo "Desktop glib patch governance file is missing: $governance_file" >&2
  exit 1
fi

# shellcheck disable=SC1090
source "$governance_file"

required_vars=(
  PALYRA_GLIB_PATCH_CRATE_NAME
  PALYRA_GLIB_PATCH_CRATE_VERSION
  PALYRA_GLIB_PATCH_DIR
  PALYRA_GLIB_PATCH_FILE
  PALYRA_GLIB_PATCH_FILE_SHA256
  PALYRA_GLIB_PATCH_ADVISORY
  PALYRA_GLIB_PATCH_RUSTSEC
  PALYRA_GLIB_PATCH_UPSTREAM_FIX_REF
  PALYRA_GLIB_PATCH_SOURCE_CRATE_DOWNLOAD_URL
  PALYRA_GLIB_PATCH_OWNER
  PALYRA_GLIB_PATCH_REVIEW_CADENCE_DAYS
  PALYRA_GLIB_PATCH_RECORDED_AT
  PALYRA_GLIB_PATCH_EXIT_STRATEGY
)

for var_name in "${required_vars[@]}"; do
  if [[ -z "${!var_name:-}" ]]; then
    echo "Desktop glib patch governance variable is missing: ${var_name}" >&2
    exit 1
  fi
done

patch_dir="$repo_root/$PALYRA_GLIB_PATCH_DIR"
patch_file="$patch_dir/$PALYRA_GLIB_PATCH_FILE"

if [[ ! -d "$patch_dir" ]]; then
  echo "Desktop glib patch directory is missing: $patch_dir" >&2
  exit 1
fi

if [[ ! -f "$patch_file" ]]; then
  echo "Desktop glib patch file is missing: $patch_file" >&2
  exit 1
fi

if [[ ! -f "$desktop_manifest" ]]; then
  echo "Desktop Cargo manifest is missing: $desktop_manifest" >&2
  exit 1
fi

if [[ ! -f "$desktop_lockfile" ]]; then
  echo "Desktop Cargo.lock is missing: $desktop_lockfile" >&2
  exit 1
fi

if [[ ! -f "$desktop_regression_test" ]]; then
  echo "Desktop glib regression test is missing: $desktop_regression_test" >&2
  exit 1
fi

sha256_file() {
  local target="$1"
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$target" | awk '{print $1}'
    return 0
  fi
  if command -v shasum >/dev/null 2>&1; then
    shasum -a 256 "$target" | awk '{print $1}'
    return 0
  fi
  if command -v openssl >/dev/null 2>&1; then
    openssl dgst -sha256 "$target" | awk '{print $NF}'
    return 0
  fi
  echo "No SHA256 tool available (expected sha256sum, shasum, or openssl)." >&2
  exit 1
}

extract_lock_package_block() {
  local lockfile="$1"
  local package_name="$2"
  local package_version="$3"

  awk -v target_name="$package_name" -v target_version="$package_version" '
    BEGIN { RS="\\[\\[package\\]\\]\n"; ORS="" }
    NR == 1 { next }
    {
      block = "[[package]]\n" $0
      if (block ~ "name = \"" target_name "\"" && block ~ "version = \"" target_version "\"") {
        print block
        exit
      }
    }
  ' "$lockfile"
}

patch_sha256="$(sha256_file "$patch_file")"
if [[ "$patch_sha256" != "$PALYRA_GLIB_PATCH_FILE_SHA256" ]]; then
  echo "Desktop glib patch checksum drift detected." >&2
  echo "Expected: $PALYRA_GLIB_PATCH_FILE_SHA256" >&2
  echo "Actual:   $patch_sha256" >&2
  exit 1
fi

if ! grep -F 'glib = { path = "third_party/glib-0.18.5-patched" }' "$desktop_manifest" >/dev/null; then
  echo "Desktop Cargo manifest no longer patches crates.io glib to the vendored path." >&2
  exit 1
fi

if ! grep -F 'let mut p: *mut libc::c_char = std::ptr::null_mut();' "$patch_file" >/dev/null; then
  echo "Desktop glib patch file no longer contains the expected mutable out-pointer fix." >&2
  exit 1
fi

if ! grep -F '&mut p,' "$patch_file" >/dev/null; then
  echo "Desktop glib patch file no longer passes the out-pointer as &mut p." >&2
  exit 1
fi

glib_lock_block="$(extract_lock_package_block "$desktop_lockfile" "$PALYRA_GLIB_PATCH_CRATE_NAME" "$PALYRA_GLIB_PATCH_CRATE_VERSION")"
if [[ -z "$glib_lock_block" ]]; then
  echo "Desktop Cargo.lock no longer contains the expected glib package entry." >&2
  exit 1
fi

if printf '%s' "$glib_lock_block" | grep -F 'source = "registry+https://github.com/rust-lang/crates.io-index"' >/dev/null; then
  echo "Desktop Cargo.lock resolves glib from crates.io instead of the vendored patched path." >&2
  exit 1
fi

echo "Desktop glib patch governance check passed."
echo "crate=${PALYRA_GLIB_PATCH_CRATE_NAME}@${PALYRA_GLIB_PATCH_CRATE_VERSION} advisory=${PALYRA_GLIB_PATCH_ADVISORY}"
echo "owner=${PALYRA_GLIB_PATCH_OWNER} review_cadence_days=${PALYRA_GLIB_PATCH_REVIEW_CADENCE_DAYS} checksum=${patch_sha256} lockfile_resolution=vendored"
