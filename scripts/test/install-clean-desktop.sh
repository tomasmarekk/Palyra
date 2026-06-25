#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage: scripts/test/install-clean-desktop.sh [options]

Options:
  --workspace-root <path>  Override the clean desktop harness root.
  --skip-build            Reuse existing release binaries and web bundles.
  --install-system-deps   Install missing desktop build dependencies with apt-get/rustup. This is the default.
  --no-system-deps-install
                          Check desktop build dependencies but do not install them.
  --launch                Launch the installed desktop app after install.
  --no-launch             Install only; do not launch the desktop app.
  -h, --help              Show this help.

Linux defaults to ${XDG_DATA_HOME:-$HOME/.local/share}/Palyra-TestHarness.
USAGE
}

die() {
  echo "$*" >&2
  exit 1
}

require_tool() {
  command -v "$1" >/dev/null 2>&1 || die "$1 is required for clean desktop install testing."
}

linux_apt_build_packages() {
  cat <<'PACKAGES'
build-essential
pkg-config
libssl-dev
libgtk-3-dev
libwebkit2gtk-4.1-dev
libsoup-3.0-dev
libjavascriptcoregtk-4.1-dev
libxdo-dev
libayatana-appindicator3-dev
librsvg2-dev
PACKAGES
}

linux_pkg_config_modules() {
  cat <<'MODULES'
openssl
gtk+-3.0
webkit2gtk-4.1
libsoup-3.0
javascriptcoregtk-4.1
xdo
ayatana-appindicator3-0.1
librsvg-2.0
MODULES
}

missing_debian_packages() {
  local package status
  while IFS= read -r package; do
    [[ -n "$package" ]] || continue
    status="$(dpkg-query -W -f='${db:Status-Abbrev}' "$package" 2>/dev/null || true)"
    [[ "$status" == "ii "* ]] || printf '%s\n' "$package"
  done < <(linux_apt_build_packages)
}

install_apt_packages() {
  local packages=("$@")
  [[ ${#packages[@]} -gt 0 ]] || return 0
  if [[ "${EUID:-$(id -u)}" -eq 0 ]]; then
    apt-get update
    DEBIAN_FRONTEND=noninteractive apt-get install -y "${packages[@]}"
  else
    command -v sudo >/dev/null 2>&1 || die "Missing Linux desktop build packages: ${packages[*]}. Install them as root, or install sudo and rerun this script."
    sudo -v || die "sudo authentication is required to install Linux desktop build packages: ${packages[*]}"
    sudo apt-get update
    sudo env DEBIAN_FRONTEND=noninteractive apt-get install -y "${packages[@]}"
  fi
}

rustfmt_is_available() {
  command -v rustfmt >/dev/null 2>&1 && rustfmt --version >/dev/null 2>&1
}

ensure_rustfmt_available() {
  rustfmt_is_available && return 0
  if [[ "$install_system_deps" == true ]] && command -v rustup >/dev/null 2>&1; then
    echo "Installing missing Rust rustfmt component with rustup..." >&2
    (
      cd "$repo_root"
      rustup component add rustfmt
    )
    rustfmt_is_available && return 0
  fi
  if command -v rustup >/dev/null 2>&1; then
    die "rustfmt is required for release builds because headless_chrome generates CDP bindings during compilation.
Run: rustup component add rustfmt
Or rerun this script with --install-system-deps."
  fi
  die "rustfmt is required for release builds because headless_chrome generates CDP bindings during compilation. Install rustfmt for the active Rust toolchain and ensure it is on PATH."
}

ensure_linux_build_dependencies() {
  [[ "$(uname -s)" == "Linux" ]] || return 0
  if command -v apt-get >/dev/null 2>&1 && command -v dpkg-query >/dev/null 2>&1; then
    local missing=()
    mapfile -t missing < <(missing_debian_packages)
    [[ ${#missing[@]} -eq 0 ]] && return 0
    if [[ "$install_system_deps" == true ]]; then
      echo "Installing missing Linux desktop build packages: ${missing[*]}" >&2
      install_apt_packages "${missing[@]}"
      return 0
    fi
    die "Missing Linux desktop build packages: ${missing[*]}
Run: sudo apt-get update && sudo apt-get install -y ${missing[*]}
Or rerun this script with --install-system-deps."
  fi

  command -v pkg-config >/dev/null 2>&1 || die "pkg-config is required for Linux desktop builds. Install your distribution's pkg-config/pkgconf package and OpenSSL development headers."
  local module missing_modules=()
  while IFS= read -r module; do
    [[ -n "$module" ]] || continue
    pkg-config --exists "$module" || missing_modules+=("$module")
  done < <(linux_pkg_config_modules)
  [[ ${#missing_modules[@]} -eq 0 ]] || die "Missing Linux pkg-config modules required for desktop builds: ${missing_modules[*]}. Install your distribution equivalents for OpenSSL, GTK 3, WebKitGTK 4.1, libsoup 3, xdo, Ayatana AppIndicator, and librsvg development packages."
}

require_value() {
  local flag="$1"
  local value="${2:-}"
  [[ -n "$value" ]] || die "$flag requires a value."
}

abs_path() {
  python3 -c 'import os, sys; print(os.path.abspath(sys.argv[1]))' "$1"
}

shell_quote() {
  local value="$1"
  printf "'%s'" "${value//\'/\'\"\'\"\'}"
}

timestamp_utc() {
  date -u +"%Y-%m-%dT%H:%M:%SZ"
}

workspace_version() {
  python3 - "$repo_root/Cargo.toml" <<'PY'
import re
import sys
content = open(sys.argv[1], encoding="utf-8").read()
match = re.search(r'(?ms)^\[workspace\.package\].*?^version\s*=\s*"([^"]+)"', content)
if not match:
    raise SystemExit(f"Unable to locate [workspace.package] version in {sys.argv[1]}")
print(match.group(1))
PY
}

platform_slug() {
  local os_part arch raw_arch
  case "$(uname -s)" in
    Linux) os_part="linux" ;;
    Darwin) os_part="macos" ;;
    *) die "Unsupported operating system for clean desktop shell install: $(uname -s)" ;;
  esac
  raw_arch="$(uname -m | tr '[:upper:]' '[:lower:]')"
  case "$raw_arch" in
    amd64|x86_64|x64) arch="x64" ;;
    arm64|aarch64) arch="arm64" ;;
    *) arch="$raw_arch" ;;
  esac
  printf '%s-%s\n' "$os_part" "$arch"
}

default_harness_root() {
  case "$(uname -s)" in
    Linux)
      printf '%s\n' "${XDG_DATA_HOME:-$HOME/.local/share}/Palyra-TestHarness"
      ;;
    Darwin)
      printf '%s\n' "$HOME/Library/Application Support/Palyra-TestHarness"
      ;;
    *)
      die "Unsupported operating system for clean desktop shell install: $(uname -s)"
      ;;
  esac
}

path_contains() {
  local needle="$1"
  case ":$PATH:" in
    *":$needle:"*) return 0 ;;
    *) return 1 ;;
  esac
}

profile_paths() {
  printf '%s\n' "$HOME/.profile"
  if [[ "$(uname -s)" == "Darwin" ]]; then
    printf '%s\n' "$HOME/.zprofile"
  fi
  if [[ -f "$HOME/.bash_profile" ]]; then
    printf '%s\n' "$HOME/.bash_profile"
  fi
}

profile_block() {
  local quoted_root
  quoted_root="$(shell_quote "$1")"
  cat <<EOF
# >>> Palyra CLI >>>
PALYRA_CLI_BIN=$quoted_root
case ":\$PATH:" in
  *":\$PALYRA_CLI_BIN:"*) ;;
  *) export PATH="\$PALYRA_CLI_BIN:\$PATH" ;;
esac
# <<< Palyra CLI <<<
EOF
}

ensure_profile_block() {
  local profile_path="$1"
  local command_root="$2"
  python3 - "$profile_path" "$command_root" <<'PY'
import os
import re
import sys

profile_path, command_root = sys.argv[1], sys.argv[2]
quoted = "'" + command_root.replace("'", "'\"'\"'") + "'"
block = f"""# >>> Palyra CLI >>>
PALYRA_CLI_BIN={quoted}
case ":$PATH:" in
  *":$PALYRA_CLI_BIN:"*) ;;
  *) export PATH="$PALYRA_CLI_BIN:$PATH" ;;
esac
# <<< Palyra CLI <<<"""
pattern = re.compile(r"# >>> Palyra CLI >>>.*?# <<< Palyra CLI <<<\r?\n?", re.S)
try:
    with open(profile_path, encoding="utf-8") as fh:
        existing = fh.read()
except FileNotFoundError:
    existing = ""
if not existing.strip():
    updated = block
elif pattern.search(existing):
    updated = pattern.sub(block + "\n", existing).rstrip("\r\n")
else:
    updated = existing.rstrip("\r\n") + "\n\n" + block
if updated != existing:
    parent = os.path.dirname(profile_path)
    if parent:
        os.makedirs(parent, exist_ok=True)
    with open(profile_path, "w", encoding="utf-8") as fh:
        fh.write(updated)
    print(profile_path)
PY
}

copy_file() {
  local source="$1"
  local destination="$2"
  [[ -f "$source" ]] || die "Required file does not exist: $source"
  mkdir -p "$(dirname "$destination")"
  cp -f "$source" "$destination"
}

copy_dir() {
  local source="$1"
  local destination="$2"
  [[ -d "$source" ]] || die "Required directory does not exist: $source"
  rm -rf "$destination"
  mkdir -p "$(dirname "$destination")"
  cp -R "$source" "$destination"
}

create_zip_from_payload() {
  local payload_root="$1"
  local archive_path="$2"
  python3 - "$payload_root" "$archive_path" <<'PY'
import os
import sys
import zipfile

payload_root, archive_path = sys.argv[1], sys.argv[2]
with zipfile.ZipFile(archive_path, "w", zipfile.ZIP_DEFLATED) as archive:
    for dirpath, dirnames, filenames in os.walk(payload_root):
        dirnames.sort()
        filenames.sort()
        for filename in filenames:
            path = os.path.join(dirpath, filename)
            archive_name = os.path.relpath(path, payload_root).replace(os.sep, "/")
            info = zipfile.ZipInfo.from_file(path, arcname=archive_name)
            with open(path, "rb") as fh:
                archive.writestr(info, fh.read(), compress_type=zipfile.ZIP_DEFLATED)
PY
}

extract_zip_safely() {
  local archive_path="$1"
  local destination="$2"
  python3 - "$archive_path" "$destination" <<'PY'
import os
import sys
import zipfile

archive_path, destination = sys.argv[1], sys.argv[2]
destination_root = os.path.abspath(destination)
destination_prefix = destination_root + os.sep
with zipfile.ZipFile(archive_path) as archive:
    for member in archive.infolist():
        target = os.path.abspath(os.path.join(destination_root, member.filename))
        if target != destination_root and not target.startswith(destination_prefix):
            raise SystemExit(f"Archive entry escapes install root: {member.filename}")
    archive.extractall(destination_root)
PY
}

write_package_metadata() {
  local payload_root="$1"
  local manifest_path="$2"
  local checksums_path="$3"
  local artifact_name="$4"
  local version="$5"
  local platform="$6"
  local source_sha="$7"
  python3 - "$payload_root" "$manifest_path" "$checksums_path" "$artifact_name" "$version" "$platform" "$source_sha" <<'PY'
import hashlib
import json
import os
import sys
from datetime import datetime, timezone

payload_root, manifest_path, checksums_path, artifact_name, version, platform, source_sha = sys.argv[1:]

def sha256_hex(path):
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()

files = []
for dirpath, dirnames, filenames in os.walk(payload_root):
    dirnames.sort()
    filenames.sort()
    for filename in filenames:
        path = os.path.join(dirpath, filename)
        rel = os.path.relpath(path, payload_root).replace(os.sep, "/")
        files.append((rel, path))

binary_names = {
    "palyra-desktop-control-center": "palyra-desktop-control-center",
    "palyrad": "palyrad",
    "palyra-browserd": "palyra-browserd",
    "palyra": "palyra",
}
binaries = []
for rel, path in files:
    if rel in binary_names:
        binaries.append({
            "logical_name": binary_names[rel],
            "file_name": rel,
            "sha256": sha256_hex(path),
            "size_bytes": os.path.getsize(path),
        })

deployment_recipes = []
for rel, path in files:
    if rel.startswith("deployment/"):
        parts = rel.split("/")
        profile = parts[1] if len(parts) > 1 else ""
        deployment_recipes.append({
            "profile": profile,
            "relative_path": rel,
            "sha256": sha256_hex(path),
            "size_bytes": os.path.getsize(path),
        })

manifest = {
    "schema_version": 1,
    "generated_at_utc": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
    "artifact_kind": "desktop",
    "artifact_name": artifact_name,
    "version": version,
    "platform": platform,
    "install_mode": "portable-archive",
    "source_sha": source_sha,
    "binaries": binaries,
    "deployment_recipes": deployment_recipes,
    "packaging_boundaries": {
        "excluded_patterns": [
            "*.sqlite",
            "*.sqlite3",
            "*.sqlite3-*",
            "*.db",
            "*.db-*",
            "*.wal",
            "*.shm",
            "*.log",
            "support-bundle*.json",
            "browser-profile/*",
            "browser-profiles/*",
            "downloads/*",
            "node_modules/*",
            "dist/*",
        ]
    },
}

with open(manifest_path, "w", encoding="utf-8") as fh:
    json.dump(manifest, fh, indent=2)
    fh.write("\n")
with open(checksums_path, "w", encoding="utf-8") as fh:
    for rel, path in files:
        fh.write(f"{sha256_hex(path)}  {rel}\n")
PY
}

write_json_metadata() {
  local output_path="$1"
  shift
  python3 - "$output_path" "$@" <<'PY'
import json
import sys

output_path = sys.argv[1]
pairs = sys.argv[2:]
data = {}
for pair in pairs:
    key, value = pair.split("=", 1)
    if value == "true":
        data[key] = True
    elif value == "false":
        data[key] = False
    elif value == "":
        data[key] = None
    else:
        data[key] = value
with open(output_path, "w", encoding="utf-8") as fh:
    json.dump(data, fh, indent=2)
    fh.write("\n")
PY
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

require_tool python3
require_tool git

workspace_root=""
skip_build=false
launch_arg=""
install_system_deps=true

while [[ $# -gt 0 ]]; do
  case "$1" in
    --workspace-root)
      require_value "$1" "${2:-}"
      workspace_root="$2"
      shift 2
      ;;
    --workspace-root=*)
      workspace_root="${1#*=}"
      shift
      ;;
    --skip-build)
      skip_build=true
      shift
      ;;
    --install-system-deps)
      install_system_deps=true
      shift
      ;;
    --no-system-deps-install)
      install_system_deps=false
      shift
      ;;
    --launch)
      [[ "$launch_arg" != "no-launch" ]] || die "Pass either --launch or --no-launch, not both."
      launch_arg="launch"
      shift
      ;;
    --no-launch)
      [[ "$launch_arg" != "launch" ]] || die "Pass either --launch or --no-launch, not both."
      launch_arg="no-launch"
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      die "Unknown argument: $1"
      ;;
  esac
done

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
workspace_root="$(abs_path "${workspace_root:-$(default_harness_root)}")"
artifacts_root="$workspace_root/artifacts"
desktop_package_output="$artifacts_root/desktop"
cargo_target_root="$artifacts_root/cargo-target"
install_root="$workspace_root/install"
state_root="$workspace_root/state"
cli_command_root="$workspace_root/cli-bin"
desktop_executable="palyra-desktop-control-center"
daemon_executable="palyrad"
browser_executable="palyra-browserd"
cli_executable="palyra"

mkdir -p "$workspace_root"

if [[ "$skip_build" == false ]]; then
  require_tool cargo
  require_tool npm
  ensure_rustfmt_available
  ensure_linux_build_dependencies
  mkdir -p "$cargo_target_root"
  (
    cd "$repo_root"
    export CARGO_TARGET_DIR="$cargo_target_root"
    bash "$repo_root/scripts/test/ensure-desktop-ui.sh"
    bash "$repo_root/scripts/test/ensure-web-ui.sh"
    cargo build -p palyra-daemon -p palyra-browserd -p palyra-cli --release --locked
    cargo build --manifest-path "$repo_root/apps/desktop/src-tauri/Cargo.toml" --release --locked
  )
fi

version="$(workspace_version)"
platform="$(platform_slug)"
artifact_name="palyra-desktop-$version-$platform"
payload_root="$desktop_package_output/$artifact_name/payload"
archive_path="$desktop_package_output/$artifact_name.zip"

isolated_desktop_binary="$cargo_target_root/release/$desktop_executable"
isolated_daemon_binary="$cargo_target_root/release/$daemon_executable"
isolated_browser_binary="$cargo_target_root/release/$browser_executable"
isolated_cli_binary="$cargo_target_root/release/$cli_executable"
desktop_binary="$isolated_desktop_binary"
daemon_binary="$isolated_daemon_binary"
browser_binary="$isolated_browser_binary"
cli_binary="$isolated_cli_binary"
[[ -f "$desktop_binary" ]] || desktop_binary="$repo_root/apps/desktop/src-tauri/target/release/$desktop_executable"
[[ -f "$daemon_binary" ]] || daemon_binary="$repo_root/target/release/$daemon_executable"
[[ -f "$browser_binary" ]] || browser_binary="$repo_root/target/release/$browser_executable"
[[ -f "$cli_binary" ]] || cli_binary="$repo_root/target/release/$cli_executable"

[[ -f "$desktop_binary" ]] || die "Desktop binary does not exist: $desktop_binary"
[[ -f "$daemon_binary" ]] || die "Daemon binary does not exist: $daemon_binary"
[[ -f "$browser_binary" ]] || die "Browser service binary does not exist: $browser_binary"
[[ -f "$cli_binary" ]] || die "CLI binary does not exist: $cli_binary"
[[ -f "$repo_root/apps/web/dist/index.html" ]] || die "Web dashboard bundle is missing: $repo_root/apps/web/dist/index.html"
[[ -f "$repo_root/crates/palyra-cli/tests/help_snapshots/docs-help.txt" ]] || die "CLI help snapshot bundle is missing."

rm -rf "$desktop_package_output"
mkdir -p "$payload_root"
copy_file "$desktop_binary" "$payload_root/$desktop_executable"
copy_file "$daemon_binary" "$payload_root/$daemon_executable"
copy_file "$browser_binary" "$payload_root/$browser_executable"
copy_file "$cli_binary" "$payload_root/$cli_executable"
chmod 755 "$payload_root/$desktop_executable" "$payload_root/$daemon_executable" "$payload_root/$browser_executable" "$payload_root/$cli_executable"
copy_file "$repo_root/LICENSE" "$payload_root/LICENSE.txt"
copy_dir "$repo_root/apps/web/dist" "$payload_root/web"
mkdir -p "$payload_root/docs"
copy_dir "$repo_root/crates/palyra-cli/tests/help_snapshots" "$payload_root/docs/help_snapshots"

cat > "$payload_root/README.txt" <<EOF
Palyra portable desktop bundle
Version: $version
Platform: $platform

Install
1. Extract this archive into a dedicated directory.
2. Keep palyra-desktop-control-center, palyrad, palyra-browserd, palyra, and web/ together.
3. Run palyra directly from this directory or expose it on PATH with a shim or symlink.
4. Launch the desktop control center binary from this directory.
EOF
cat > "$payload_root/ROLLBACK.txt" <<EOF
Rollback guidance
1. Stop the currently running Palyra processes.
2. Restore the previous extracted archive contents or switch launchers back to the previous install directory.
3. Keep the state root unchanged.
EOF
cat > "$payload_root/RELEASE_NOTES.txt" <<EOF
Release notes for Palyra $version

Portable desktop bundle generated by the clean desktop test harness.
EOF
cat > "$payload_root/MIGRATION_NOTES.txt" <<EOF
Migration notes for Palyra $version

No state-root relocation is required for this local test bundle.
EOF

for profile in single-vm worker-enabled; do
  recipe_root="$payload_root/deployment/$profile"
  mkdir -p "$recipe_root"
  "$payload_root/$cli_executable" deployment recipe --deployment-profile "$profile" --output-dir "$recipe_root" >/dev/null
done

source_sha="$(git -C "$repo_root" rev-parse HEAD)"
write_package_metadata \
  "$payload_root" \
  "$payload_root/release-manifest.json" \
  "$payload_root/checksums.txt" \
  "$artifact_name" \
  "$version" \
  "$platform" \
  "$source_sha"
create_zip_from_payload "$payload_root" "$archive_path"
rm -rf "$desktop_package_output/$artifact_name"

rm -rf "$state_root"
mkdir -p "$state_root/config"
config_path="$state_root/config/palyra.toml"

rm -rf "$install_root"
mkdir -p "$install_root"
extract_zip_safely "$archive_path" "$install_root"
chmod 755 "$install_root/$desktop_executable" "$install_root/$daemon_executable" "$install_root/$browser_executable" "$install_root/$cli_executable"

cli_binary_installed="$install_root/$cli_executable"
mkdir -p "$cli_command_root"
cli_command_path="$cli_command_root/palyra"
cat > "$cli_command_path" <<EOF
#!/usr/bin/env sh
set -eu
export PALYRA_STATE_ROOT=$(shell_quote "$state_root")
export PALYRA_CONFIG=$(shell_quote "$config_path")
exec $(shell_quote "$cli_binary_installed") "\$@"
EOF
chmod 755 "$cli_command_path"

command_root_already_on_path=false
if path_contains "$cli_command_root"; then
  command_root_already_on_path=true
fi
profile_files=()
cli_user_path_updated=false
if [[ "$command_root_already_on_path" == false ]]; then
  while IFS= read -r profile_path; do
    if [[ -n "$profile_path" ]]; then
      if updated_profile="$(ensure_profile_block "$profile_path" "$cli_command_root")" && [[ -n "$updated_profile" ]]; then
        cli_user_path_updated=true
      fi
      profile_files+=("$profile_path")
    fi
  done < <(profile_paths)
fi
export PATH="$cli_command_root:$PATH"
cli_session_path_updated=true
cli_persistence_strategy="posix-profile"
[[ "$command_root_already_on_path" == true ]] && cli_persistence_strategy="existing-path"
cli_current_shell_command="$cli_command_path"
cli_new_shell_command="palyra"
cli_parent_shell_note="The installer updated PATH for this installer process and the selected persistent shell profile, but the parent shell that launched it cannot inherit child-process PATH changes. Use cli_command_path in the current parent shell, or open a new terminal before running 'palyra'."

PALYRA_STATE_ROOT="$state_root" "$cli_binary_installed" setup --mode local --path "$config_path" --force >/dev/null
PALYRA_STATE_ROOT="$state_root" PALYRA_CONFIG="$config_path" "$cli_binary_installed" version >/dev/null
"$install_root/$daemon_executable" --help >/dev/null
"$install_root/$browser_executable" --help >/dev/null
"$cli_command_path" version >/dev/null
"$cli_command_path" --help >/dev/null
"$cli_command_path" gateway --help >/dev/null
"$cli_command_path" browser --help >/dev/null
"$cli_command_path" docs search gateway >/dev/null
PALYRA_STATE_ROOT="$state_root" PALYRA_CONFIG="$config_path" "$cli_command_path" doctor --json >/dev/null
"$cli_command_path" skills seed-e2e-fixtures --json >/dev/null
"$cli_command_path" skills list --json > "$workspace_root/e2e-skills-list.json"
python3 - "$workspace_root/e2e-skills-list.json" <<'PY'
import json
import sys

with open(sys.argv[1], encoding="utf-8") as fh:
    payload = json.load(fh)
if not any(entry.get("skill_id") == "e2e.reporter" for entry in payload.get("entries", [])):
    raise SystemExit("Clean desktop harness e2e.reporter skill fixture is missing from skills list.")
PY

cli_path_preflight_source="$(command -v palyra || true)"
cli_path_preflight_matches_command_root=false
if [[ "$(dirname "$cli_path_preflight_source")" == "$cli_command_root" ]]; then
  cli_path_preflight_matches_command_root=true
else
  die "Clean desktop install resolved 'palyra' to '$cli_path_preflight_source' instead of '$cli_command_root'. Use cli_command_path='$cli_command_path' in this shell, or open a new shell after PATH persistence."
fi

launcher_path="$install_root/launch-palyra-test.sh"
cat > "$launcher_path" <<EOF
#!/usr/bin/env bash
set -euo pipefail

install_root="\$(CDPATH= cd -- "\$(dirname -- "\$0")" && pwd)"
state_root=$(shell_quote "$state_root")
config_path=$(shell_quote "$config_path")
mkdir -p "\$state_root"

export PALYRA_STATE_ROOT="\$state_root"
export PALYRA_CONFIG="\$config_path"
export PALYRA_DESKTOP_PALYRAD_BIN="\$install_root/$daemon_executable"
export PALYRA_DESKTOP_BROWSERD_BIN="\$install_root/$browser_executable"
export PALYRA_DESKTOP_PALYRA_BIN="\$install_root/$cli_executable"

desktop_binary="\$install_root/$desktop_executable"
if [[ "\${1:-}" == "--wait" || "\${1:-}" == "-w" ]]; then
  exec "\$desktop_binary"
fi

"\$desktop_binary" >/dev/null 2>&1 &
desktop_pid=\$!
sleep 2
if ! kill -0 "\$desktop_pid" 2>/dev/null; then
  if wait "\$desktop_pid"; then
    echo "Palyra desktop exited cleanly before the launcher timeout. If no new window appeared, another instance may already be running."
    exit 0
  fi
  exit_code=\$?
  echo "Palyra desktop exited immediately with code \$exit_code. Re-run launch-palyra-test.sh with --wait to surface the startup error directly." >&2
  exit "\$exit_code"
fi

echo "Palyra desktop launched with pid=\$desktop_pid"
EOF
chmod 755 "$launcher_path"

installed_at="$(timestamp_utc)"
profile_files_joined="$(IFS=:; echo "${profile_files[*]}")"
write_json_metadata "$install_root/install-metadata.json" \
  "schema_version=2" \
  "artifact_kind=desktop" \
  "installed_at_utc=$installed_at" \
  "archive_path=$archive_path" \
  "install_root=$install_root" \
  "config_path=$config_path" \
  "state_root=$state_root" \
  "cli_command_root=$cli_command_root" \
  "cli_command_path=$cli_command_path" \
  "cli_target_binary_path=$cli_binary_installed" \
  "cli_persistence_strategy=$cli_persistence_strategy" \
  "cli_user_path_updated=$cli_user_path_updated" \
  "cli_profile_files=$profile_files_joined"

should_launch=true
[[ "$launch_arg" == "no-launch" ]] && should_launch=false
write_json_metadata "$workspace_root/clean-install-metadata.json" \
  "installed_at_utc=$installed_at" \
  "repo_root=$repo_root" \
  "workspace_root=$workspace_root" \
  "artifacts_root=$artifacts_root" \
  "archive_path=$archive_path" \
  "install_root=$install_root" \
  "config_path=$config_path" \
  "state_root=$state_root" \
  "cli_command_root=$cli_command_root" \
  "cli_command_path=$cli_command_path" \
  "cli_persistence_strategy=$cli_persistence_strategy" \
  "cli_session_path_updated=$cli_session_path_updated" \
  "cli_user_path_updated=$cli_user_path_updated" \
  "cli_current_shell_command=$cli_current_shell_command" \
  "cli_new_shell_command=$cli_new_shell_command" \
  "cli_parent_shell_note=$cli_parent_shell_note" \
  "cli_path_preflight_source=$cli_path_preflight_source" \
  "cli_path_preflight_matches_command_root=$cli_path_preflight_matches_command_root" \
  "cli_path_preflight_matches_harness=$cli_path_preflight_matches_command_root" \
  "e2e_skill_fixtures_seeded=true" \
  "launcher_path=$launcher_path" \
  "launched=$should_launch"

if [[ "$should_launch" == true ]]; then
  "$launcher_path"
fi

echo "workspace_root=$workspace_root"
echo "archive_path=$archive_path"
echo "install_root=$install_root"
echo "config_path=$config_path"
echo "state_root=$state_root"
echo "cli_command_root=$cli_command_root"
echo "cli_command_path=$cli_command_path"
echo "cli_persistence_strategy=$cli_persistence_strategy"
echo "cli_session_path_updated=$cli_session_path_updated"
echo "cli_user_path_updated=$cli_user_path_updated"
echo "cli_current_shell_command=$cli_current_shell_command"
echo "cli_new_shell_command=$cli_new_shell_command"
echo "cli_parent_shell_note=$cli_parent_shell_note"
echo "cli_path_preflight_source=$cli_path_preflight_source"
echo "cli_path_preflight_matches_command_root=$cli_path_preflight_matches_command_root"
echo "cli_path_preflight_matches_harness=$cli_path_preflight_matches_command_root"
echo "e2e_skill_fixtures_seeded=true"
echo "launcher_path=$launcher_path"
echo "launched=$should_launch"
