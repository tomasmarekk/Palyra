#!/usr/bin/env node

import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { test } from "node:test";
import { fileURLToPath } from "node:url";

import {
  validateQaLabWorkflow,
  validateRuntimeShadowRolloutGates,
} from "./validate-runtime-shadow-rollout-gates.mjs";

const repoRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const manifestPath = path.join(repoRoot, "infra/release/rollout-gates.json");

function manifest() {
  return JSON.parse(fs.readFileSync(manifestPath, "utf8"));
}

test("current RuntimeKernelV2 shadow promotion gate is complete", () => {
  assert.deepEqual(validateRuntimeShadowRolloutGates(manifest(), repoRoot), []);
});

test("invariant thresholds cannot be weakened", () => {
  const fixture = manifest();
  const invariantMetric = fixture.success_metrics.find(
    (metric) => metric.metric_id === "runtime_shadow_invariant_violation_total",
  );
  invariantMetric.target = "<= 1";

  assert.ok(
    validateRuntimeShadowRolloutGates(fixture, repoRoot).some((error) =>
      error.includes("target must be exactly = 0"),
    ),
  );
});

test("every invariant and authority breach remains a hard promotion blocker", () => {
  const fixture = manifest();
  const gate = fixture.gates.find(
    (candidate) => candidate.feature_id === "runtime_kernel_v2_shadow",
  );
  gate.promotion_blockers = gate.promotion_blockers.filter(
    (blocker) => blocker !== "runtime_shadow_context_safety_invariant_violation_total>0",
  );

  assert.ok(
    validateRuntimeShadowRolloutGates(fixture, repoRoot).some((error) =>
      error.includes(
        "missing hard promotion blocker runtime_shadow_context_safety_invariant_violation_total>0",
      ),
    ),
  );
});

test("authoritative V2 qualification remains a hard promotion blocker", () => {
  const fixture = manifest();
  const gate = fixture.gates.find(
    (candidate) => candidate.feature_id === "runtime_kernel_v2_authoritative",
  );
  gate.promotion_blockers = [];

  assert.ok(
    validateRuntimeShadowRolloutGates(fixture, repoRoot).some((error) =>
      error.includes(
        "missing hard promotion blocker runtime_kernel_v2_authoritative_qa_suite_not_green",
      ),
    ),
  );
});

test("authoritative V2 remains at the stable release stage", () => {
  const fixture = manifest();
  const gate = fixture.gates.find(
    (candidate) => candidate.feature_id === "runtime_kernel_v2_authoritative",
  );
  gate.release_stage = "main_qualification";

  assert.ok(
    validateRuntimeShadowRolloutGates(fixture, repoRoot).some((error) =>
      error.includes("runtime_kernel_v2_authoritative release_stage must remain stable"),
    ),
  );
});

test("qa-lab directly gates shadow and authoritative suites with its built daemon", () => {
  const workflowPath = path.join(repoRoot, ".github", "workflows", "ci.yml");
  const workflow = fs.readFileSync(workflowPath, "utf8");
  assert.deepEqual(validateQaLabWorkflow(workflow), []);

  const previewOnly = workflow.replace(
    "qa gate --suite qa/suites/runtime_kernel_v2_shadow.yaml",
    "qa run-pack --path qa/scenarios/runtime_kernel_v2",
  );
  assert.ok(
    validateQaLabWorkflow(previewOnly).some((error) =>
      error.includes("qa-lab must directly execute qa/suites/runtime_kernel_v2_shadow.yaml"),
    ),
  );
});
