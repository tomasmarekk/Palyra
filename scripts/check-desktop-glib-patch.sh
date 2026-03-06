#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
governance_file="$repo_root/apps/desktop/src-tauri/third_party/glib-0.18.5-patched/PALYRA_PATCH_GOVERNANCE.env"
desktop_manifest="$repo_root/apps/desktop/src-tauri/Cargo.toml"
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

resolve_cargo() {
  if command -v cargo >/dev/null 2>&1; then
    command -v cargo
    return 0
  fi
  if command -v cargo.exe >/dev/null 2>&1; then
    command -v cargo.exe
    return 0
  fi

  local candidates=(
    "${HOME:-}/.cargo/bin/cargo"
    "${HOME:-}/.cargo/bin/cargo.exe"
    "${USERPROFILE:-}/.cargo/bin/cargo.exe"
  )
  local candidate
  for candidate in "${candidates[@]}"; do
    if [[ -n "$candidate" && -x "$candidate" ]]; then
      printf '%s\n' "$candidate"
      return 0
    fi
  done

  echo "cargo is required for desktop glib patch governance checks." >&2
  exit 1
}

to_windows_path() {
  local target="$1"
  if command -v wslpath >/dev/null 2>&1; then
    wslpath -w "$target"
    return 0
  fi
  if command -v cygpath >/dev/null 2>&1; then
    cygpath -w "$target"
    return 0
  fi
  printf '%s\n' "$target"
}

run_cargo_metadata() {
  local cargo_path="$1"
  local manifest_path="$2"
  if [[ "$cargo_path" == *.exe ]] && command -v powershell.exe >/dev/null 2>&1; then
    local cargo_windows
    cargo_windows="$(to_windows_path "$cargo_path")"
    powershell.exe -NoLogo -NoProfile -Command \
      "\$OutputEncoding = [Console]::OutputEncoding = [System.Text.UTF8Encoding]::new(\$false); & '$cargo_windows' metadata --manifest-path '$manifest_path' --format-version 1 --locked"
    return 0
  fi

  "$cargo_path" metadata --manifest-path "$manifest_path" --format-version 1 --locked
}

metadata_resolves_vendored_patch() {
  local cargo_path="$1"
  local manifest_path="$2"

  if [[ "$cargo_path" == *.exe ]] && command -v python3 >/dev/null 2>&1; then
    python3 - "$cargo_path" "$manifest_path" <<'PY'
import subprocess
import sys

cargo_path, manifest_path = sys.argv[1:3]
result = subprocess.run(
    [cargo_path, "metadata", "--manifest-path", manifest_path, "--format-version", "1", "--locked"],
    capture_output=True,
    text=True,
)
if result.returncode != 0:
    sys.stderr.write(result.stderr)
    raise SystemExit(result.returncode)
raise SystemExit(0 if "glib-0.18.5-patched#glib@0.18.5" in result.stdout else 1)
PY
    return $?
  fi

  if [[ "$cargo_path" == *.exe ]] && command -v powershell.exe >/dev/null 2>&1; then
    local cargo_windows
    cargo_windows="$(to_windows_path "$cargo_path")"
    powershell.exe -NoLogo -NoProfile -Command \
      "\$OutputEncoding = [Console]::OutputEncoding = [System.Text.UTF8Encoding]::new(\$false); \$json = & '$cargo_windows' metadata --manifest-path '$manifest_path' --format-version 1 --locked | Out-String; if (\$json.Contains('glib-0.18.5-patched#glib@0.18.5')) { exit 0 } else { exit 1 }"
    return $?
  fi

  local metadata_json
  metadata_json="$(
    run_cargo_metadata "$cargo_path" "$manifest_path"
  )"
  metadata_json="${metadata_json//\\//}"
  printf '%s' "$metadata_json" | grep -F "glib-0.18.5-patched#glib@0.18.5" >/dev/null
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

cargo_bin="$(resolve_cargo)"
desktop_manifest_for_cargo="$desktop_manifest"
if [[ "$cargo_bin" == *.exe ]] && command -v wslpath >/dev/null 2>&1; then
  desktop_manifest_for_cargo="$(to_windows_path "$desktop_manifest")"
elif command -v cygpath >/dev/null 2>&1 && [[ "$(uname -s)" =~ ^(MINGW|MSYS|CYGWIN) ]]; then
  desktop_manifest_for_cargo="$(to_windows_path "$desktop_manifest")"
fi
if ! metadata_resolves_vendored_patch "$cargo_bin" "$desktop_manifest_for_cargo"; then
  echo "cargo metadata no longer resolves glib through the vendored patched path." >&2
  exit 1
fi

echo "Desktop glib patch governance check passed."
echo "crate=${PALYRA_GLIB_PATCH_CRATE_NAME}@${PALYRA_GLIB_PATCH_CRATE_VERSION} advisory=${PALYRA_GLIB_PATCH_ADVISORY}"
echo "owner=${PALYRA_GLIB_PATCH_OWNER} review_cadence_days=${PALYRA_GLIB_PATCH_REVIEW_CADENCE_DAYS} checksum=${patch_sha256}"
