#!/usr/bin/env bash
# Qualifies the production Docker backend against a real Linux daemon.
# The lane crosses a real runner-owner crash/restart boundary, uses an immutable
# image digest, and leaves no managed container behind.

set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$repo_root"

artifact_root="target/qa-lab/docker-backend"
mkdir -p "$artifact_root"

cleanup_residual_containers() {
  local residual
  residual="$(docker ps -aq --filter label=palyra.managed=true 2>/dev/null || true)"
  if [[ -n "$residual" ]]; then
    docker inspect $residual >"$artifact_root/residual-containers-on-exit.json" 2>/dev/null || true
    docker rm -f $residual >/dev/null 2>&1 || true
  fi
}
trap cleanup_residual_containers EXIT

docker info >/dev/null
docker pull alpine:3.21 >/dev/null
live_image="$(docker image inspect alpine:3.21 --format '{{index .RepoDigests 0}}')"
if [[ "$live_image" != *@sha256:* ]]; then
  echo "Docker live image did not resolve to an immutable repo digest" >&2
  exit 1
fi

export PALYRA_DOCKER_LIVE_IMAGE="$live_image"
export PALYRA_DOCKER_LIVE_REPORT="$artifact_root/capability-report.json"
cargo test -p palyra-daemon --lib docker_live_e2e --locked -- --ignored --test-threads=1

residual="$(docker ps -aq --filter label=palyra.managed=true)"
if [[ -n "$residual" ]]; then
  docker inspect $residual >"$artifact_root/residual-containers.json" || true
  echo "Docker live qualification left managed containers behind: $residual" >&2
  exit 1
fi
test -s "$PALYRA_DOCKER_LIVE_REPORT"
test -s "$artifact_root/owner-crash-recovery-report.json"
