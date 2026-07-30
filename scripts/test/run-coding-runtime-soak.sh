#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
ITERATIONS="${PALYRA_CODING_RUNTIME_SOAK_ITERATIONS:-12}"
REPORT_PATH="${PALYRA_CODING_RUNTIME_SOAK_REPORT:-target/qa-lab/coding-runtime/coding-runtime-soak.json}"
SCHEMA_PATH="schemas/json/common/coding-runtime-soak-report.v1.json"
BASELINE_PATH="qa/baselines/coding-runtime-warm-lsp.v1.json"

if [[ ! "$ITERATIONS" =~ ^[0-9]+$ ]] || ((ITERATIONS < 1 || ITERATIONS > 64)); then
  echo "PALYRA_CODING_RUNTIME_SOAK_ITERATIONS must be an integer from 1 through 64." >&2
  exit 1
fi

cd "$ROOT_DIR"
if [[ "$REPORT_PATH" != /* ]]; then
  REPORT_PATH="$ROOT_DIR/$REPORT_PATH"
fi
mkdir -p "$(dirname "$REPORT_PATH")"
REPORT_PATH="$(cd "$(dirname "$REPORT_PATH")" && pwd)/$(basename "$REPORT_PATH")"
export PALYRA_CODING_RUNTIME_SOAK_ITERATIONS="$ITERATIONS"
export PALYRA_CODING_RUNTIME_SOAK_REPORT="$REPORT_PATH"

cargo test -p palyra-daemon --test coding_runtime --locked -j 1 \
  warm_lsp_repeated_diagnostics_soak_is_bounded_and_leaves_no_services \
  -- --exact --nocapture

node - "$REPORT_PATH" "$ITERATIONS" "$SCHEMA_PATH" "$BASELINE_PATH" <<'NODE'
const fs = require("node:fs");
const [reportPath, expectedIterations, schemaPath, baselinePath] = process.argv.slice(2);
const report = JSON.parse(fs.readFileSync(reportPath, "utf8"));
const schema = JSON.parse(fs.readFileSync(schemaPath, "utf8"));
const baseline = JSON.parse(fs.readFileSync(baselinePath, "utf8"));
if (schema.additionalProperties !== false) {
  throw new Error("Coding runtime soak schema must be closed.");
}
const actualKeys = Object.keys(report).sort();
const requiredKeys = [...schema.required].sort();
if (JSON.stringify(actualKeys) !== JSON.stringify(requiredKeys)) {
  throw new Error("Coding runtime soak report does not match the closed schema shape.");
}
for (const [key, rule] of Object.entries(schema.properties)) {
  const value = report[key];
  if (rule.type === "integer" && !Number.isInteger(value)) {
    throw new Error(`Coding runtime soak field ${key} must be an integer.`);
  }
  if (Object.hasOwn(rule, "const") && value !== rule.const) {
    throw new Error(`Coding runtime soak field ${key} violates its constant contract.`);
  }
  if (Object.hasOwn(rule, "minimum") && value < rule.minimum) {
    throw new Error(`Coding runtime soak field ${key} is below its minimum.`);
  }
  if (Object.hasOwn(rule, "maximum") && value > rule.maximum) {
    throw new Error(`Coding runtime soak field ${key} exceeds its maximum.`);
  }
}
const platform = { linux: "linux", darwin: "macos", win32: "windows" }[process.platform];
if (
  !platform ||
  !baseline.supported_runner_os.includes(platform) ||
  report.iterations !== Number(expectedIterations) ||
  (process.env.CI && report.iterations !== baseline.ci_iterations) ||
  report.iterations > baseline.max_iterations ||
  report.patch_observations !== report.iterations * 2 ||
  report.introduced_total !== report.iterations ||
  report.resolved_total !== report.iterations ||
  report.patch_latency_p95_ms > baseline.max_patch_latency_p95_ms ||
  report.cleanup_active_process_count !== baseline.required_cleanup_active_process_count ||
  report.cleanup_lsp_settled !== baseline.required_cleanup_lsp_settled ||
  report.remaining_resource_leases !== baseline.required_remaining_resource_leases
) {
  throw new Error("Coding runtime soak report failed its performance or cleanup baseline.");
}
NODE

printf 'Coding runtime soak evidence: %s\n' "$REPORT_PATH"
