#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$repo_root"

PALYRA_UPDATE_GOLDENS=1 cargo test -p palyra-daemon --test current_state_inventory --locked
