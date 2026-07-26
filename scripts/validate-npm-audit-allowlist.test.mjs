#!/usr/bin/env node

import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { test } from "node:test";
import { fileURLToPath } from "node:url";

const scriptPath = fileURLToPath(new URL("./validate-npm-audit-allowlist.mjs", import.meta.url));

function dateWithOffsetDays(days) {
  const date = new Date();
  date.setUTCDate(date.getUTCDate() + days);
  return date.toISOString().slice(0, 10);
}

function writeJsonFile(baseDir, fileName, payload) {
  const filePath = path.join(baseDir, fileName);
  fs.writeFileSync(filePath, `${JSON.stringify(payload, null, 2)}\n`, "utf8");
  return filePath;
}

function emptyAuditReport() {
  return { vulnerabilities: {} };
}

function auditWithAdvisory(id, packageName = "eslint") {
  return {
    vulnerabilities: {
      [packageName]: {
        via: [
          {
            severity: "high",
            title: "fixture advisory",
            url: `https://github.com/advisories/${id}`,
          },
        ],
      },
    },
  };
}

function createTempFixtureDir(testName) {
  return fs.mkdtempSync(path.join(os.tmpdir(), `${testName}-`));
}

function runValidator({ full, runtime, allowlist, summary }) {
  return spawnSync(
    process.execPath,
    [
      scriptPath,
      "--full",
      full,
      "--runtime",
      runtime,
      "--allowlist",
      allowlist,
      "--summary",
      summary,
      "--threshold",
      "high",
    ],
    { encoding: "utf8" },
  );
}

test("fails when allowlist contains stale expired entry", (t) => {
  const tmpDir = createTempFixtureDir("allowlist-expired-stale");
  t.after(() => fs.rmSync(tmpDir, { recursive: true, force: true }));

  const fullPath = writeJsonFile(tmpDir, "full.json", emptyAuditReport());
  const runtimePath = writeJsonFile(tmpDir, "runtime.json", emptyAuditReport());
  const allowlistPath = writeJsonFile(tmpDir, "allowlist.json", {
    version: 2,
    entries: [
      {
        id: "GHSA-stale-expired-0001",
        scope: "dev",
        expires_on: dateWithOffsetDays(-2),
        owner: "@tomasmarekk",
        reason: "fixture",
      },
    ],
  });
  const summaryPath = path.join(tmpDir, "summary.json");

  const result = runValidator({
    full: fullPath,
    runtime: runtimePath,
    allowlist: allowlistPath,
    summary: summaryPath,
  });

  assert.equal(result.status, 1);
  assert.match(result.stdout, /Expired dev allowlist entry GHSA-stale-expired-0001/);
  assert.match(result.stderr, /expired_allowlist=1/);

  const summary = JSON.parse(fs.readFileSync(summaryPath, "utf8"));
  assert.equal(summary.counts.dev_only_tracked, 0);
  assert.equal(summary.counts.expired, 1);
  assert.equal(summary.counts.expired_dev_only, 0);
  assert.equal(summary.counts.stale_allowlist, 1);
});

test("passes when stale allowlist entry is not expired", (t) => {
  const tmpDir = createTempFixtureDir("allowlist-stale-not-expired");
  t.after(() => fs.rmSync(tmpDir, { recursive: true, force: true }));

  const fullPath = writeJsonFile(tmpDir, "full.json", emptyAuditReport());
  const runtimePath = writeJsonFile(tmpDir, "runtime.json", emptyAuditReport());
  const allowlistPath = writeJsonFile(tmpDir, "allowlist.json", {
    version: 2,
    entries: [
      {
        id: "GHSA-stale-valid-0001",
        scope: "dev",
        expires_on: dateWithOffsetDays(14),
        owner: "@tomasmarekk",
        reason: "fixture",
      },
    ],
  });
  const summaryPath = path.join(tmpDir, "summary.json");

  const result = runValidator({
    full: fullPath,
    runtime: runtimePath,
    allowlist: allowlistPath,
    summary: summaryPath,
  });

  assert.equal(result.status, 0);
  assert.equal(result.stderr, "");

  const summary = JSON.parse(fs.readFileSync(summaryPath, "utf8"));
  assert.equal(summary.counts.dev_only_tracked, 0);
  assert.equal(summary.counts.expired, 0);
  assert.equal(summary.counts.expired_dev_only, 0);
  assert.equal(summary.counts.stale_allowlist, 1);
});

test("fails when active dev advisory uses an expired allowlist entry", (t) => {
  const tmpDir = createTempFixtureDir("allowlist-active-expired");
  t.after(() => fs.rmSync(tmpDir, { recursive: true, force: true }));

  const advisoryId = "GHSA-active-expired-0001";
  const fullPath = writeJsonFile(tmpDir, "full.json", auditWithAdvisory(advisoryId));
  const runtimePath = writeJsonFile(tmpDir, "runtime.json", emptyAuditReport());
  const allowlistPath = writeJsonFile(tmpDir, "allowlist.json", {
    version: 2,
    entries: [
      {
        id: advisoryId,
        scope: "dev",
        expires_on: dateWithOffsetDays(-2),
        owner: "@tomasmarekk",
        reason: "fixture",
      },
    ],
  });
  const summaryPath = path.join(tmpDir, "summary.json");

  const result = runValidator({
    full: fullPath,
    runtime: runtimePath,
    allowlist: allowlistPath,
    summary: summaryPath,
  });

  assert.equal(result.status, 1);
  assert.match(result.stdout, /Expired dev allowlist entry GHSA-active-expired-0001/);
  assert.match(result.stderr, /unallowlisted=0, expired_allowlist=1/);

  const summary = JSON.parse(fs.readFileSync(summaryPath, "utf8"));
  assert.equal(summary.counts.dev_only_tracked, 1);
  assert.equal(summary.counts.unallowlisted, 0);
  assert.equal(summary.counts.expired, 1);
  assert.equal(summary.counts.expired_dev_only, 1);
});

test("fails when a high runtime advisory is not allowlisted", (t) => {
  const tmpDir = createTempFixtureDir("allowlist-runtime-missing");
  t.after(() => fs.rmSync(tmpDir, { recursive: true, force: true }));

  const advisoryId = "GHSA-runtime-missing-0001";
  const report = auditWithAdvisory(advisoryId, "runtime-package");
  const fullPath = writeJsonFile(tmpDir, "full.json", report);
  const runtimePath = writeJsonFile(tmpDir, "runtime.json", report);
  const allowlistPath = writeJsonFile(tmpDir, "allowlist.json", {
    version: 2,
    entries: [],
  });
  const summaryPath = path.join(tmpDir, "summary.json");

  const result = runValidator({
    full: fullPath,
    runtime: runtimePath,
    allowlist: allowlistPath,
    summary: summaryPath,
  });

  assert.equal(result.status, 1);
  assert.match(result.stdout, /Unallowlisted runtime advisory GHSA-runtime-missing-0001/);
  assert.match(result.stderr, /unallowlisted=1/);

  const summary = JSON.parse(fs.readFileSync(summaryPath, "utf8"));
  assert.equal(summary.counts.runtime_tracked, 1);
  assert.equal(summary.counts.unallowlisted_runtime, 1);
});

test("passes only when a high runtime advisory has a runtime-scoped exception", (t) => {
  const tmpDir = createTempFixtureDir("allowlist-runtime-active");
  t.after(() => fs.rmSync(tmpDir, { recursive: true, force: true }));

  const advisoryId = "GHSA-runtime-active-0001";
  const report = auditWithAdvisory(advisoryId, "runtime-package");
  const fullPath = writeJsonFile(tmpDir, "full.json", report);
  const runtimePath = writeJsonFile(tmpDir, "runtime.json", report);
  const allowlistPath = writeJsonFile(tmpDir, "allowlist.json", {
    version: 2,
    entries: [
      {
        id: advisoryId,
        scope: "runtime",
        expires_on: dateWithOffsetDays(14),
        owner: "@tomasmarekk",
        reason: "fixture",
      },
    ],
  });
  const summaryPath = path.join(tmpDir, "summary.json");

  const result = runValidator({
    full: fullPath,
    runtime: runtimePath,
    allowlist: allowlistPath,
    summary: summaryPath,
  });

  assert.equal(result.status, 0);
  assert.equal(result.stderr, "");

  const summary = JSON.parse(fs.readFileSync(summaryPath, "utf8"));
  assert.equal(summary.counts.runtime_tracked, 1);
  assert.equal(summary.counts.unallowlisted_runtime, 0);
  assert.equal(summary.counts.expired_runtime, 0);
});

test("rejects a dev-scoped exception for a runtime advisory", (t) => {
  const tmpDir = createTempFixtureDir("allowlist-runtime-scope-mismatch");
  t.after(() => fs.rmSync(tmpDir, { recursive: true, force: true }));

  const advisoryId = "GHSA-runtime-scope-0001";
  const report = auditWithAdvisory(advisoryId, "runtime-package");
  const fullPath = writeJsonFile(tmpDir, "full.json", report);
  const runtimePath = writeJsonFile(tmpDir, "runtime.json", report);
  const allowlistPath = writeJsonFile(tmpDir, "allowlist.json", {
    version: 2,
    entries: [
      {
        id: advisoryId,
        scope: "dev",
        expires_on: dateWithOffsetDays(14),
        owner: "@tomasmarekk",
        reason: "fixture",
      },
    ],
  });
  const summaryPath = path.join(tmpDir, "summary.json");

  const result = runValidator({
    full: fullPath,
    runtime: runtimePath,
    allowlist: allowlistPath,
    summary: summaryPath,
  });

  assert.equal(result.status, 1);
  assert.match(result.stdout, /Unallowlisted runtime advisory GHSA-runtime-scope-0001/);

  const summary = JSON.parse(fs.readFileSync(summaryPath, "utf8"));
  assert.equal(summary.counts.unallowlisted_runtime, 1);
  assert.equal(summary.counts.stale_allowlist, 1);
});
