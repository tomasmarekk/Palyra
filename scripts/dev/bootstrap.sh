#!/usr/bin/env bash
set -euo pipefail

CARGO_AUDIT_VERSION="${CARGO_AUDIT_VERSION:-0.22.1}"
CARGO_DENY_VERSION="${CARGO_DENY_VERSION:-0.19.0}"
OSV_VERSION="${OSV_VERSION:-v2.2.2}"
GITLEAKS_VERSION="${GITLEAKS_VERSION:-v8.30.0}"
BIN_DIR="${BIN_DIR:-$HOME/.local/bin}"
FORCE_INSTALL=0

usage() {
  cat <<EOF
Usage: $(basename "$0") [--force] [--bin-dir PATH]

Installs local dev security tooling:
  - cargo-audit
  - cargo-deny
  - osv-scanner
  - gitleaks

Environment overrides:
  CARGO_AUDIT_VERSION (default: ${CARGO_AUDIT_VERSION})
  CARGO_DENY_VERSION  (default: ${CARGO_DENY_VERSION})
  OSV_VERSION         (default: ${OSV_VERSION})
  GITLEAKS_VERSION    (default: ${GITLEAKS_VERSION})
  BIN_DIR             (default: ${BIN_DIR})
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --force)
      FORCE_INSTALL=1
      shift
      ;;
    --bin-dir)
      if [[ $# -lt 2 ]]; then
        echo "missing value for --bin-dir" >&2
        exit 2
      fi
      BIN_DIR="$2"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "unknown argument: $1" >&2
      usage
      exit 2
      ;;
  esac
done

require_command() {
  local cmd="$1"
  if ! command -v "${cmd}" >/dev/null 2>&1; then
    echo "required command '${cmd}' is not available" >&2
    exit 1
  fi
}

download_file() {
  local url="$1"
  local destination="$2"
  curl --proto '=https' --tlsv1.2 --retry 5 --retry-connrefused --location --silent --show-error --fail \
    "${url}" \
    -o "${destination}"
}

sha256_file() {
  local file="$1"
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "${file}" | awk '{print tolower($1)}'
    return
  fi
  if command -v shasum >/dev/null 2>&1; then
    shasum -a 256 "${file}" | awk '{print tolower($1)}'
    return
  fi
  echo "neither sha256sum nor shasum is available for checksum verification" >&2
  exit 1
}

checksum_from_manifest() {
  local manifest="$1"
  local asset="$2"
  awk -v target="${asset}" '{name=$2; sub(/^\*/, "", name); if (name == target) {print tolower($1); exit 0}}' "${manifest}"
}

verify_checksum() {
  local file="$1"
  local expected="$2"
  local actual
  actual="$(sha256_file "${file}")"
  if [[ "${actual}" != "${expected}" ]]; then
    echo "checksum mismatch for ${file}: expected ${expected}, got ${actual}" >&2
    exit 1
  fi
}

install_cargo_subcommand() {
  local subcommand="$1"
  local crate="$2"
  local version="$3"
  if [[ "${FORCE_INSTALL}" -eq 0 ]] && cargo "${subcommand}" --version >/dev/null 2>&1; then
    echo "cargo ${subcommand} already installed; skipping"
    return
  fi
  cargo install --locked "${crate}" --version "${version}"
}

install_osv_scanner() {
  local os arch osv_arch asset checksums_url asset_url tmp_dir checksum_file asset_file expected

  os="$(uname -s | tr '[:upper:]' '[:lower:]')"
  arch="$(uname -m)"
  case "${os}" in
    linux|darwin) ;;
    *)
      echo "unsupported OS '${os}' for osv-scanner bootstrap" >&2
      exit 1
      ;;
  esac
  case "${arch}" in
    x86_64|amd64) osv_arch="amd64" ;;
    arm64|aarch64) osv_arch="arm64" ;;
    *)
      echo "unsupported architecture '${arch}' for osv-scanner bootstrap" >&2
      exit 1
      ;;
  esac

  if [[ "${FORCE_INSTALL}" -eq 0 ]] && command -v osv-scanner >/dev/null 2>&1; then
    echo "osv-scanner already installed; skipping"
    return
  fi

  asset="osv-scanner_${os}_${osv_arch}"
  checksums_url="https://github.com/google/osv-scanner/releases/download/${OSV_VERSION}/osv-scanner_SHA256SUMS"
  asset_url="https://github.com/google/osv-scanner/releases/download/${OSV_VERSION}/${asset}"

  tmp_dir="$(mktemp -d)"
  checksum_file="${tmp_dir}/osv-scanner_SHA256SUMS"
  asset_file="${tmp_dir}/${asset}"
  trap 'rm -rf "${tmp_dir}"' RETURN

  download_file "${checksums_url}" "${checksum_file}"
  download_file "${asset_url}" "${asset_file}"
  expected="$(checksum_from_manifest "${checksum_file}" "${asset}")"
  if [[ -z "${expected}" ]]; then
    echo "failed to find checksum for ${asset} in ${checksums_url}" >&2
    exit 1
  fi
  verify_checksum "${asset_file}" "${expected}"

  mkdir -p "${BIN_DIR}"
  cp "${asset_file}" "${BIN_DIR}/osv-scanner"
  chmod +x "${BIN_DIR}/osv-scanner"
  trap - RETURN
  rm -rf "${tmp_dir}"
}

install_gitleaks() {
  local os arch gitleaks_arch version_no_v asset checksums_url asset_url tmp_dir checksum_file archive_file expected

  os="$(uname -s | tr '[:upper:]' '[:lower:]')"
  arch="$(uname -m)"
  case "${os}" in
    linux|darwin) ;;
    *)
      echo "unsupported OS '${os}' for gitleaks bootstrap" >&2
      exit 1
      ;;
  esac
  case "${arch}" in
    x86_64|amd64) gitleaks_arch="x64" ;;
    arm64|aarch64) gitleaks_arch="arm64" ;;
    *)
      echo "unsupported architecture '${arch}' for gitleaks bootstrap" >&2
      exit 1
      ;;
  esac

  if [[ "${FORCE_INSTALL}" -eq 0 ]] && command -v gitleaks >/dev/null 2>&1; then
    echo "gitleaks already installed; skipping"
    return
  fi

  version_no_v="${GITLEAKS_VERSION#v}"
  asset="gitleaks_${version_no_v}_${os}_${gitleaks_arch}.tar.gz"
  checksums_url="https://github.com/gitleaks/gitleaks/releases/download/${GITLEAKS_VERSION}/gitleaks_${version_no_v}_checksums.txt"
  asset_url="https://github.com/gitleaks/gitleaks/releases/download/${GITLEAKS_VERSION}/${asset}"

  tmp_dir="$(mktemp -d)"
  checksum_file="${tmp_dir}/gitleaks_checksums.txt"
  archive_file="${tmp_dir}/${asset}"
  trap 'rm -rf "${tmp_dir}"' RETURN

  download_file "${checksums_url}" "${checksum_file}"
  download_file "${asset_url}" "${archive_file}"
  expected="$(checksum_from_manifest "${checksum_file}" "${asset}")"
  if [[ -z "${expected}" ]]; then
    echo "failed to find checksum for ${asset} in ${checksums_url}" >&2
    exit 1
  fi
  verify_checksum "${archive_file}" "${expected}"

  tar -xzf "${archive_file}" -C "${tmp_dir}"
  if [[ ! -f "${tmp_dir}/gitleaks" ]]; then
    echo "gitleaks archive did not contain expected 'gitleaks' binary" >&2
    exit 1
  fi

  mkdir -p "${BIN_DIR}"
  cp "${tmp_dir}/gitleaks" "${BIN_DIR}/gitleaks"
  chmod +x "${BIN_DIR}/gitleaks"
  trap - RETURN
  rm -rf "${tmp_dir}"
}

main() {
  require_command cargo
  require_command curl
  require_command tar

  install_cargo_subcommand "audit" "cargo-audit" "${CARGO_AUDIT_VERSION}"
  install_cargo_subcommand "deny" "cargo-deny" "${CARGO_DENY_VERSION}"
  install_osv_scanner
  install_gitleaks

  echo "Installed toolchain binaries in '${BIN_DIR}'."
  if [[ ":${PATH}:" != *":${BIN_DIR}:"* ]]; then
    echo "Add '${BIN_DIR}' to PATH to run osv-scanner and gitleaks from your shell."
  fi

  cargo audit --version
  cargo deny --version
  "${BIN_DIR}/osv-scanner" --version
  "${BIN_DIR}/gitleaks" version
}

main "$@"
