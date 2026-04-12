#!/usr/bin/env bash
set -euo pipefail

ROOT="$(git rev-parse --show-toplevel 2>/dev/null || pwd)"
cd "$ROOT"

python3 - <<'PY'
from __future__ import annotations

import subprocess
import sys
from pathlib import Path
from shutil import which

CHECKS = [
    {
        "name": "direct-discord-helper-usage",
        "pattern": r"normalize_discord|discord_connector_id|canonical_discord|use palyra_connectors::providers::discord|use crate::application::channels::providers::discord",
        "paths": [
            "crates/palyra-daemon/src",
            "crates/palyra-cli/src",
            "crates/palyra-connectors/src",
        ],
        "allow_prefixes": [
            "crates/palyra-daemon/src/application/channels/providers/",
            "crates/palyra-daemon/src/transport/http/handlers/admin/channels/connectors/discord.rs",
            "crates/palyra-daemon/src/transport/http/handlers/console/channels/connectors/discord.rs",
            "crates/palyra-daemon/src/transport/http/contracts/channels/discord.rs",
            "crates/palyra-daemon/src/channels/discord.rs",
            "crates/palyra-daemon/src/channels.rs",
            "crates/palyra-daemon/src/lib.rs",
            "crates/palyra-daemon/src/transport/http/router.rs",
            "crates/palyra-cli/src/commands/channels/providers/",
            "crates/palyra-cli/src/commands/channels/connectors/discord/",
            "crates/palyra-connectors/src/providers/",
        ],
    },
    {
        "name": "connector-kind-discord-scatter",
        "pattern": r"ConnectorKind::Discord",
        "paths": [
            "crates/palyra-daemon/src",
            "crates/palyra-cli/src",
            "crates/palyra-connectors/src",
        ],
        "allow_prefixes": [
            "crates/palyra-daemon/src/application/channels/providers/",
            "crates/palyra-daemon/src/channels/discord.rs",
            "crates/palyra-daemon/src/channels.rs",
            "crates/palyra-connectors/src/providers/",
            "crates/palyra-connectors/src/connectors/mod.rs",
            "crates/palyra-connectors/src/lib.rs",
        ],
    },
    {
        "name": "legacy-connector-architecture-names",
        "pattern": r"palyra-connector-core|palyra-connector-discord|palyra_connector_core|palyra_connector_discord",
        "paths": [
            "AGENTS.md",
            "Cargo.toml",
            "crates",
            "apps",
            ".github",
            "scripts",
            "justfile",
            "Makefile",
        ],
        "allow_prefixes": [
            "scripts/check-channel-provider-boundaries.sh",
            "scripts/dev/report-connector-leakage.sh",
        ],
    },
]


def run_rg(pattern: str, paths: list[str]) -> list[str]:
    existing_paths = [path for path in paths if Path(path).exists()]
    if not existing_paths:
        return []
    if which("rg"):
        command = ["rg", "-n", pattern, *existing_paths]
    else:
        command = ["grep", "-R", "-n", "-E", "-I", pattern, *existing_paths]
    result = subprocess.run(command, capture_output=True, text=True)
    if result.returncode not in (0, 1):
        sys.stderr.write(result.stderr)
        sys.exit(result.returncode)
    return [line for line in result.stdout.splitlines() if line.strip()]


def is_allowed(path: str, allow_prefixes: list[str]) -> bool:
    return any(path.startswith(prefix) for prefix in allow_prefixes)


violations: list[tuple[str, str]] = []
for check in CHECKS:
    lines = run_rg(check["pattern"], check["paths"])
    for line in lines:
        path = line.split(":", 1)[0]
        if not is_allowed(path, check["allow_prefixes"]):
            violations.append((check["name"], line))

if violations:
    print("channel/provider boundary violations:")
    for check_name, line in violations:
        print(f"[{check_name}] {line}")
    sys.exit(1)

print("channel/provider boundary checks passed")
PY
