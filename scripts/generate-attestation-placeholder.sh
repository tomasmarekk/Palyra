#!/usr/bin/env bash
set -euo pipefail

output_path="${1:-security-artifacts/attestation-placeholder.json}"
mkdir -p "$(dirname "$output_path")"

generated_at="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"

cat >"$output_path" <<EOF
{
  "schema_version": 1,
  "type": "palyra.build_attestation_placeholder",
  "generated_at": "$generated_at",
  "git": {
    "repository": "${GITHUB_REPOSITORY:-local}",
    "ref": "${GITHUB_REF:-local}",
    "commit": "${GITHUB_SHA:-local}"
  },
  "build": {
    "workflow": "${GITHUB_WORKFLOW:-local}",
    "run_id": "${GITHUB_RUN_ID:-local}",
    "job": "${GITHUB_JOB:-local}"
  },
  "attestation": {
    "mode": "placeholder",
    "signed": false,
    "status": "pending_sigstore_integration"
  }
}
EOF

echo "Generated attestation placeholder at $output_path"
