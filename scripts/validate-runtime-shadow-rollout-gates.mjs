#!/usr/bin/env node

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const REQUIRED_METRICS = new Map([
  ["runtime_shadow_selected_observations_total", ">= 1000 before promotion review"],
  ["runtime_shadow_invariant_violation_total", "= 0"],
  ["runtime_shadow_context_safety_invariant_violation_total", "= 0"],
  ["runtime_shadow_side_effect_denials_total", "= 0"],
]);
const REQUIRED_BLOCKERS = new Set([
  "runtime_shadow_invariant_violation_total>0",
  "runtime_shadow_context_safety_invariant_violation_total>0",
  "runtime_shadow_side_effect_denials_total>0",
  "runtime_kernel_v2_shadow_qa_suite_not_green",
]);
const SHADOW_SUITE = "qa/suites/runtime_kernel_v2_shadow.yaml";
const AUTHORITATIVE_SUITE = "qa/suites/runtime_kernel_v2_authoritative.yaml";
const AUTHORITATIVE_BLOCKER = "runtime_kernel_v2_authoritative_qa_suite_not_green";
const REQUIRED_SUITES = [SHADOW_SUITE, AUTHORITATIVE_SUITE];
const REQUIRED_QA_RUNNERS = [
  "scripts/test/run-release-eval-gate.sh",
  "scripts/test/run-release-eval-gate.ps1",
];

function parseArgs(argv) {
  const values = new Map();
  for (let index = 0; index < argv.length; index += 1) {
    const token = argv[index];
    if (!token.startsWith("--")) {
      throw new Error(`unexpected argument: ${token}`);
    }
    const value = argv[index + 1];
    if (!value || value.startsWith("--")) {
      throw new Error(`missing value for ${token}`);
    }
    values.set(token.slice(2), value);
    index += 1;
  }
  return values;
}

function validateUniqueIds(items, field, pathName, errors) {
  const seen = new Set();
  for (const item of items) {
    const id = item?.[field];
    if (typeof id !== "string" || id.length === 0) {
      errors.push(`${pathName} contains an entry without ${field}`);
    } else if (seen.has(id)) {
      errors.push(`${pathName} contains duplicate ${field} ${id}`);
    } else {
      seen.add(id);
    }
  }
}

function extractIndentedBlock(source, key, indentation) {
  const lines = source.split(/\r?\n/u);
  const prefix = " ".repeat(indentation);
  const start = lines.findIndex((line) => line.trimEnd() === `${prefix}${key}:`);
  if (start < 0) {
    return "";
  }
  let end = lines.length;
  for (let index = start + 1; index < lines.length; index += 1) {
    const line = lines[index];
    if (line.trim().length === 0 || line.trimStart().startsWith("#")) {
      continue;
    }
    const leadingSpaces = line.length - line.trimStart().length;
    if (leadingSpaces <= indentation) {
      end = index;
      break;
    }
  }
  return lines.slice(start, end).join("\n");
}

function namedWorkflowSteps(jobBlock) {
  const lines = jobBlock.split(/\r?\n/u);
  const starts = [];
  for (let index = 0; index < lines.length; index += 1) {
    if (/^ {6}- name:/u.test(lines[index])) {
      starts.push(index);
    }
  }
  return starts.map((start, index) =>
    lines.slice(start, starts[index + 1] ?? lines.length).join("\n"),
  );
}

export function validateQaLabWorkflow(ciWorkflow) {
  const errors = [];
  const qaLab = extractIndentedBlock(ciWorkflow, "qa-lab", 2);
  if (qaLab.length === 0) {
    return ["required qa-lab CI job is missing"];
  }
  if (!qaLab.includes("cargo build -p palyra-daemon --bin palyrad --locked")) {
    errors.push("qa-lab must build the palyrad binary used by runtime gates");
  }
  const steps = namedWorkflowSteps(qaLab);
  for (const suite of REQUIRED_SUITES) {
    const step = steps.find((candidate) => candidate.includes(suite));
    if (!step) {
      errors.push(`qa-lab must directly execute ${suite}`);
      continue;
    }
    const normalized = step.replace(/\s+/gu, " ");
    if (!normalized.includes(`qa gate --suite ${suite}`)) {
      errors.push(`qa-lab must execute ${suite} through qa gate`);
    }
    if (!step.includes("PALYRA_QA_PALYRAD_BIN: ${{ github.workspace }}/target/debug/palyrad")) {
      errors.push(`qa-lab must bind ${suite} to its built palyrad binary`);
    }
    if (
      !step.includes('test -f "$PALYRA_QA_PALYRAD_BIN"') ||
      !step.includes('test ! -L "$PALYRA_QA_PALYRAD_BIN"')
    ) {
      errors.push(`qa-lab must verify the built palyrad binary before ${suite}`);
    }
  }
  return errors;
}

export function validateRuntimeShadowRolloutGates(manifest, repoRoot) {
  const errors = [];
  const metrics = Array.isArray(manifest?.success_metrics) ? manifest.success_metrics : [];
  const gates = Array.isArray(manifest?.gates) ? manifest.gates : [];
  validateUniqueIds(metrics, "metric_id", "success_metrics", errors);
  validateUniqueIds(gates, "feature_id", "gates", errors);

  const metricsById = new Map(metrics.map((metric) => [metric.metric_id, metric]));
  for (const [metricId, requiredTarget] of REQUIRED_METRICS) {
    const metric = metricsById.get(metricId);
    if (!metric) {
      errors.push(`missing required shadow metric ${metricId}`);
      continue;
    }
    if (metric.source !== "runtime shadow differential diagnostics") {
      errors.push(`${metricId} must use runtime shadow differential diagnostics`);
    }
    if (metric.target !== requiredTarget) {
      errors.push(`${metricId} target must be exactly ${requiredTarget}`);
    }
  }

  const gate = gates.find((candidate) => candidate.feature_id === "runtime_kernel_v2_shadow");
  if (!gate) {
    errors.push("missing runtime_kernel_v2_shadow promotion gate");
  } else {
    if (gate.release_stage !== "shadow") {
      errors.push("runtime_kernel_v2_shadow release_stage must remain shadow");
    }
    if (gate.required_qa_suite !== SHADOW_SUITE) {
      errors.push(`runtime_kernel_v2_shadow required_qa_suite must be ${SHADOW_SUITE}`);
    }
    const blockers = new Set(Array.isArray(gate.promotion_blockers) ? gate.promotion_blockers : []);
    for (const blocker of REQUIRED_BLOCKERS) {
      if (!blockers.has(blocker)) {
        errors.push(`missing hard promotion blocker ${blocker}`);
      }
    }
  }

  const authoritativeGate = gates.find(
    (candidate) => candidate.feature_id === "runtime_kernel_v2_authoritative",
  );
  if (!authoritativeGate) {
    errors.push("missing runtime_kernel_v2_authoritative promotion gate");
  } else {
    if (authoritativeGate.release_stage !== "main_qualification") {
      errors.push("runtime_kernel_v2_authoritative release_stage must remain main_qualification");
    }
    if (authoritativeGate.required_qa_suite !== AUTHORITATIVE_SUITE) {
      errors.push(
        `runtime_kernel_v2_authoritative required_qa_suite must be ${AUTHORITATIVE_SUITE}`,
      );
    }
    const blockers = new Set(
      Array.isArray(authoritativeGate.promotion_blockers)
        ? authoritativeGate.promotion_blockers
        : [],
    );
    if (!blockers.has(AUTHORITATIVE_BLOCKER)) {
      errors.push(`missing hard promotion blocker ${AUTHORITATIVE_BLOCKER}`);
    }
  }

  for (const suite of REQUIRED_SUITES) {
    if (!fs.existsSync(path.join(repoRoot, suite))) {
      errors.push(`required QA suite does not exist: ${suite}`);
    }
  }
  for (const runnerPath of REQUIRED_QA_RUNNERS) {
    const absoluteRunnerPath = path.join(repoRoot, runnerPath);
    if (!fs.existsSync(absoluteRunnerPath)) {
      errors.push(`required promotion runner does not exist: ${runnerPath}`);
      continue;
    }
    const runner = fs.readFileSync(absoluteRunnerPath, "utf8");
    for (const suite of REQUIRED_SUITES) {
      if (!runner.includes(`--suite ${suite}`)) {
        errors.push(`promotion runner does not execute ${suite}: ${runnerPath}`);
      }
    }
  }
  const packageJson = JSON.parse(fs.readFileSync(path.join(repoRoot, "package.json"), "utf8"));
  if (!packageJson.scripts?.["js:check"]?.includes("npm run runtime-shadow:check")) {
    errors.push("js:check must invoke runtime-shadow:check");
  }
  const shadowCheck = packageJson.scripts?.["runtime-shadow:check"] ?? "";
  if (
    !shadowCheck.includes("validate-runtime-shadow-rollout-gates.mjs") ||
    !shadowCheck.includes("validate-runtime-shadow-rollout-gates.test.mjs")
  ) {
    errors.push("runtime-shadow:check must run the validator and its node:test suite");
  }
  const ciWorkflow = fs.readFileSync(path.join(repoRoot, ".github", "workflows", "ci.yml"), "utf8");
  if (!ciWorkflow.includes("run: npm run js:check")) {
    errors.push("the existing CI Quality job must execute npm run js:check");
  }
  errors.push(...validateQaLabWorkflow(ciWorkflow));
  return errors;
}

function main() {
  const args = parseArgs(process.argv.slice(2));
  const scriptDir = path.dirname(fileURLToPath(import.meta.url));
  const defaultRoot = path.resolve(scriptDir, "..");
  const repoRoot = path.resolve(args.get("repo-root") ?? defaultRoot);
  const manifestPath = path.resolve(
    args.get("manifest") ?? path.join(repoRoot, "infra/release/rollout-gates.json"),
  );
  const manifest = JSON.parse(fs.readFileSync(manifestPath, "utf8"));
  const errors = validateRuntimeShadowRolloutGates(manifest, repoRoot);
  if (errors.length > 0) {
    for (const error of errors) {
      console.error(`runtime shadow rollout gate: ${error}`);
    }
    process.exitCode = 1;
    return;
  }
  console.log("RuntimeKernelV2 shadow rollout gate is valid.");
}

if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  try {
    main();
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    console.error(`failed to validate runtime shadow rollout gate: ${message}`);
    process.exitCode = 2;
  }
}
