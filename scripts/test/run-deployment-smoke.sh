#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

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

  echo "cargo is required for deployment smoke checks." >&2
  exit 1
}

cd "$ROOT_DIR"

run_windows_smoke() {
  local script_path="$ROOT_DIR/scripts/test/run-deployment-smoke.ps1"
  if command -v cygpath >/dev/null 2>&1; then
    script_path="$(cygpath -w "$script_path")"
  elif command -v wslpath >/dev/null 2>&1; then
    script_path="$(wslpath -w "$script_path")"
  fi

  if command -v pwsh.exe >/dev/null 2>&1; then
    pwsh.exe -NoLogo -File "$script_path"
    return $?
  fi
  if command -v pwsh >/dev/null 2>&1; then
    pwsh -NoLogo -File "$script_path"
    return $?
  fi
  if command -v powershell.exe >/dev/null 2>&1; then
    powershell.exe -NoProfile -ExecutionPolicy Bypass -File "$script_path"
    return $?
  fi
  return 1
}

host_uname="$(uname -s 2>/dev/null || true)"
if { [[ "${OS:-}" == "Windows_NT" ]] || [[ "$host_uname" == MINGW* ]] || [[ "$host_uname" == MSYS* ]] || [[ "$host_uname" == CYGWIN* ]] || command -v cmd.exe >/dev/null 2>&1; }; then
  run_windows_smoke
  exit $?
fi

CARGO_BIN="$(resolve_cargo)"
SMOKE_ROOT="${PALYRA_DEPLOYMENT_SMOKE_DIR:-$(mktemp -d)}"
cleanup_smoke_root=0
if [[ -z "${PALYRA_DEPLOYMENT_SMOKE_DIR:-}" ]]; then
  cleanup_smoke_root=1
fi

cleanup() {
  if [[ "$cleanup_smoke_root" == "1" && -n "$SMOKE_ROOT" && "$SMOKE_ROOT" == /tmp/* ]]; then
    rm -rf "$SMOKE_ROOT"
  fi
}
trap cleanup EXIT

export PALYRA_STATE_ROOT="$SMOKE_ROOT/state"
unset PALYRA_CONFIG
mkdir -p "$PALYRA_STATE_ROOT" "$SMOKE_ROOT/configs" "$SMOKE_ROOT/recipes" "$SMOKE_ROOT/reports"

run_cli() {
  "$CARGO_BIN" run -p palyra-cli --locked -- "$@"
}

run_cli deployment profiles --json > "$SMOKE_ROOT/reports/profiles.json"

for profile in local single-vm worker-enabled; do
  mode="remote"
  if [[ "$profile" == "local" ]]; then
    mode="local"
  fi
  config_path="$SMOKE_ROOT/configs/$profile.toml"
  recipe_dir="$SMOKE_ROOT/recipes/$profile"

  run_cli setup --mode "$mode" --deployment-profile "$profile" --path "$config_path" --force --tls-scaffold none > "$SMOKE_ROOT/reports/setup-$profile.txt"
  run_cli config validate --path "$config_path" > "$SMOKE_ROOT/reports/validate-$profile.txt"
  run_cli deployment preflight --deployment-profile "$profile" --path "$config_path" --json > "$SMOKE_ROOT/reports/preflight-$profile.json"
  run_cli deployment manifest --deployment-profile "$profile" --output "$SMOKE_ROOT/reports/manifest-$profile.json" > "$SMOKE_ROOT/reports/manifest-$profile.txt"
  if [[ "$profile" != "local" ]]; then
    run_cli deployment recipe --deployment-profile "$profile" --output-dir "$recipe_dir" > "$SMOKE_ROOT/reports/recipe-$profile.txt"
    test -f "$recipe_dir/profile-manifest.json"
    test -f "$recipe_dir/env/palyra.env.example"
    test -f "$recipe_dir/docker/Dockerfile.palyra"
  fi
done

test -f "$SMOKE_ROOT/recipes/single-vm/compose/single-vm.yml"
test -f "$SMOKE_ROOT/recipes/worker-enabled/compose/worker-enabled.yml"
test -f "$SMOKE_ROOT/recipes/worker-enabled/systemd/palyra-workerd.service"

worker_config="$SMOKE_ROOT/configs/worker-enabled.toml"
run_cli deployment upgrade-smoke --deployment-profile worker-enabled --path "$worker_config" --json > "$SMOKE_ROOT/reports/upgrade-smoke-worker-enabled.json"
run_cli deployment promotion-check --deployment-profile worker-enabled --json > "$SMOKE_ROOT/reports/promotion-worker-enabled.json"
run_cli deployment rollback-plan --deployment-profile worker-enabled --output "$SMOKE_ROOT/reports/rollback-worker-enabled.json" > "$SMOKE_ROOT/reports/rollback-worker-enabled.txt"

echo "deployment_smoke=passed"
echo "smoke_root=$SMOKE_ROOT"
