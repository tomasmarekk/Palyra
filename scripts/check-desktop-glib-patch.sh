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

  echo "cargo is required for desktop glib governance checks." >&2
  exit 1
}

CARGO_BIN="$(resolve_cargo)"

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

select_python_interpreter() {
  if [[ -x /mnt/c/Windows/py.exe ]] && /mnt/c/Windows/py.exe -3 -c "import sys" >/dev/null 2>&1; then
    printf '%s\n' '/mnt/c/Windows/py.exe -3'
    return 0
  fi
  if command -v py >/dev/null 2>&1 && py -3 -c "import sys" >/dev/null 2>&1; then
    printf '%s\n' 'py -3'
    return 0
  fi
  if command -v python3 >/dev/null 2>&1 && python3 -c "import sys" >/dev/null 2>&1; then
    printf '%s\n' python3
    return 0
  fi
  if command -v python >/dev/null 2>&1 && python -c "import sys" >/dev/null 2>&1; then
    printf '%s\n' python
    return 0
  fi
  echo "A working Python 3 interpreter is required for desktop glib metadata validation." >&2
  exit 1
}

assert_resolved_vendored_glib() {
  local manifest_path="$1"
  local expected_crate_name="$2"
  local expected_crate_version="$3"
  local expected_patch_dir="$4"
  local cargo_manifest_path="$manifest_path"
  local python_selector
  python_selector="$(select_python_interpreter)"
  local -a python_cmd=()
  IFS=' ' read -r -a python_cmd <<< "$python_selector"

  if [[ "$CARGO_BIN" == *.exe ]] && command -v wslpath >/dev/null 2>&1; then
    cargo_manifest_path="$(wslpath -w "$manifest_path")"
  fi

  local metadata_json
  if ! metadata_json="$("$CARGO_BIN" metadata --format-version 1 --locked --manifest-path "$cargo_manifest_path")"; then
    echo "Failed to load Cargo metadata for desktop app." >&2
    exit 1
  fi

  local metadata_validator
  metadata_validator="$(cat <<'PY'
import json
import os
import re
import sys

crate_name = sys.argv[1]
crate_version = sys.argv[2]
def normalize_manifest_path(raw: str) -> str:
    candidate = raw.strip()
    if re.match(r"^[A-Za-z]:[\\\\/]", candidate):
        drive = candidate[0].lower()
        tail = candidate[2:].replace("\\", "/")
        candidate = f"/mnt/{drive}{tail}"
    return os.path.realpath(candidate)

expected_patch_dir = normalize_manifest_path(sys.argv[3])
expected_manifest = os.path.join(expected_patch_dir, "Cargo.toml")

packages = json.load(sys.stdin).get("packages", [])
matches = [
    pkg for pkg in packages
    if pkg.get("name") == crate_name and pkg.get("version") == crate_version
]

if not matches:
    print("Desktop dependency graph no longer contains the expected glib package entry.", file=sys.stderr)
    sys.exit(1)

for pkg in matches:
    pkg_source = pkg.get("source")
    if pkg_source:
        print(f"Desktop dependency graph resolves glib from unexpected source: {pkg_source}", file=sys.stderr)
        sys.exit(1)

    manifest_path = normalize_manifest_path(pkg.get("manifest_path", ""))
    if manifest_path != expected_manifest:
        print(
            "Desktop dependency graph resolves glib from an unexpected path "
            f"({manifest_path}) instead of {expected_manifest}.",
            file=sys.stderr,
        )
        sys.exit(1)
PY
)"

  if ! "${python_cmd[@]}" -c "$metadata_validator" \
      "$expected_crate_name" \
      "$expected_crate_version" \
      "$expected_patch_dir" \
      <<<"$metadata_json"; then
    exit 1
  fi
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

assert_resolved_vendored_glib \
  "$desktop_manifest" \
  "$PALYRA_GLIB_PATCH_CRATE_NAME" \
  "$PALYRA_GLIB_PATCH_CRATE_VERSION" \
  "$patch_dir"

echo "Desktop glib patch governance check passed."
echo "crate=${PALYRA_GLIB_PATCH_CRATE_NAME}@${PALYRA_GLIB_PATCH_CRATE_VERSION} advisory=${PALYRA_GLIB_PATCH_ADVISORY}"
echo "owner=${PALYRA_GLIB_PATCH_OWNER} review_cadence_days=${PALYRA_GLIB_PATCH_REVIEW_CADENCE_DAYS} checksum=${patch_sha256} lockfile_resolution=vendored"
