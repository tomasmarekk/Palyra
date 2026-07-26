#!/usr/bin/env node

import fs from "node:fs";
import path from "node:path";

const SEVERITY_ORDER = new Map([
  ["info", 0],
  ["low", 1],
  ["moderate", 2],
  ["high", 3],
  ["critical", 4],
  ["unknown", -1],
]);

function parseArgs(argv) {
  const args = new Map();
  for (let i = 0; i < argv.length; i += 1) {
    const token = argv[i];
    if (!token.startsWith("--")) {
      continue;
    }
    const key = token.slice(2);
    const value = argv[i + 1];
    if (!value || value.startsWith("--")) {
      throw new Error(`missing value for --${key}`);
    }
    args.set(key, value);
    i += 1;
  }
  return args;
}

function requiredArg(args, key) {
  const value = args.get(key);
  if (!value) {
    throw new Error(`missing required argument --${key}`);
  }
  return value;
}

function readJson(filePath) {
  const absolutePath = path.resolve(filePath);
  return JSON.parse(fs.readFileSync(absolutePath, "utf8"));
}

function normalizeSeverity(input) {
  if (typeof input !== "string") {
    return "unknown";
  }
  const normalized = input.trim().toLowerCase();
  return SEVERITY_ORDER.has(normalized) ? normalized : "unknown";
}

function maxSeverity(a, b) {
  return (SEVERITY_ORDER.get(a) ?? -1) >= (SEVERITY_ORDER.get(b) ?? -1) ? a : b;
}

function advisoryIdFromVia(via) {
  if (via && typeof via === "object") {
    const url = typeof via.url === "string" ? via.url : "";
    const ghsa = url.match(/GHSA-[A-Za-z0-9-]+/);
    if (ghsa) {
      return ghsa[0];
    }
    if (typeof via.source === "number") {
      return `NPM-${via.source}`;
    }
    if (url.length > 0) {
      return url;
    }
  }
  return null;
}

function collectAdvisories(report) {
  const vulnerabilities =
    report && typeof report === "object" && report.vulnerabilities ? report.vulnerabilities : {};
  const advisories = new Map();

  function ensureEntry(id, data) {
    const existing = advisories.get(id);
    if (existing) {
      existing.severity = maxSeverity(existing.severity, data.severity);
      if (!existing.url && data.url) {
        existing.url = data.url;
      }
      if (!existing.title && data.title) {
        existing.title = data.title;
      }
      existing.packages.add(data.packageName);
      return;
    }
    advisories.set(id, {
      id,
      severity: data.severity,
      title: data.title ?? "",
      url: data.url ?? "",
      packages: new Set([data.packageName]),
    });
  }

  function walkVia(packageName, viaItem, seenRefs) {
    if (typeof viaItem === "string") {
      if (seenRefs.has(viaItem)) {
        return;
      }
      const referenced = vulnerabilities[viaItem];
      if (!referenced || !Array.isArray(referenced.via)) {
        return;
      }
      const nextSeen = new Set(seenRefs);
      nextSeen.add(viaItem);
      for (const nested of referenced.via) {
        walkVia(packageName, nested, nextSeen);
      }
      return;
    }

    if (!viaItem || typeof viaItem !== "object") {
      return;
    }

    const id = advisoryIdFromVia(viaItem);
    if (!id) {
      return;
    }

    ensureEntry(id, {
      packageName,
      severity: normalizeSeverity(viaItem.severity),
      title: typeof viaItem.title === "string" ? viaItem.title : "",
      url: typeof viaItem.url === "string" ? viaItem.url : "",
    });
  }

  for (const [packageName, vulnerability] of Object.entries(vulnerabilities)) {
    if (!vulnerability || typeof vulnerability !== "object") {
      continue;
    }
    const viaEntries = Array.isArray(vulnerability.via) ? vulnerability.via : [];
    for (const viaItem of viaEntries) {
      walkVia(packageName, viaItem, new Set([packageName]));
    }
  }

  return advisories;
}

function advisoryMapToObject(map) {
  const output = {};
  for (const [id, advisory] of map) {
    output[id] = {
      id: advisory.id,
      severity: advisory.severity,
      title: advisory.title,
      url: advisory.url,
      packages: [...advisory.packages].sort((a, b) => a.localeCompare(b)),
    };
  }
  return output;
}

function parseExpiryDate(raw) {
  if (typeof raw !== "string") {
    return null;
  }
  const parsed = new Date(`${raw}T23:59:59Z`);
  if (Number.isNaN(parsed.getTime())) {
    return null;
  }
  return parsed;
}

function formatUtcDate(date) {
  return date.toISOString().slice(0, 10);
}

function collectExpiredAllowlistEntries(entries, now) {
  const expiredEntries = [];
  for (const entry of entries) {
    if (!entry || typeof entry !== "object" || typeof entry.id !== "string") {
      continue;
    }

    const expiry = parseExpiryDate(entry.expires_on);
    if (!expiry || expiry.getTime() < now.getTime()) {
      expiredEntries.push({
        id: entry.id,
        scope: entry.scope ?? null,
        expires_on: entry.expires_on ?? null,
        owner: entry.owner ?? null,
        reason: entry.reason ?? null,
      });
    }
  }
  return expiredEntries;
}

function main() {
  const args = parseArgs(process.argv.slice(2));
  const fullPath = requiredArg(args, "full");
  const runtimePath = requiredArg(args, "runtime");
  const allowlistPath = requiredArg(args, "allowlist");
  const summaryPath = args.get("summary");
  const threshold = normalizeSeverity(args.get("threshold") ?? "high");
  const thresholdRank = SEVERITY_ORDER.get(threshold) ?? 3;

  const now = new Date();
  const fullReport = readJson(fullPath);
  const runtimeReport = readJson(runtimePath);
  const allowlist = readJson(allowlistPath);

  if (allowlist.version !== 2) {
    throw new Error("npm audit allowlist version must be 2");
  }
  const allowlistEntries = Array.isArray(allowlist.entries) ? allowlist.entries : [];
  const allowlistByScopeAndId = new Map();
  for (const entry of allowlistEntries) {
    if (
      !entry ||
      typeof entry !== "object" ||
      typeof entry.id !== "string" ||
      !["runtime", "dev"].includes(entry.scope) ||
      typeof entry.owner !== "string" ||
      entry.owner.trim().length === 0 ||
      typeof entry.reason !== "string" ||
      entry.reason.trim().length === 0
    ) {
      throw new Error(
        "each npm audit allowlist entry requires id, runtime/dev scope, owner, and reason",
      );
    }
    const key = `${entry.scope}:${entry.id}`;
    if (allowlistByScopeAndId.has(key)) {
      throw new Error(`duplicate npm audit allowlist entry ${key}`);
    }
    allowlistByScopeAndId.set(key, entry);
  }
  const expiredAllowlist = collectExpiredAllowlistEntries(allowlistEntries, now);
  const expiredAllowlistKeys = new Set(
    expiredAllowlist.map((entry) => `${entry.scope}:${entry.id}`),
  );

  const fullAdvisories = collectAdvisories(fullReport);
  const runtimeAdvisories = collectAdvisories(runtimeReport);

  const runtimeTrackedAdvisories = [];
  for (const [id, advisory] of runtimeAdvisories) {
    const severityRank = SEVERITY_ORDER.get(advisory.severity) ?? -1;
    if (severityRank < thresholdRank) {
      continue;
    }
    const key = `runtime:${id}`;
    const allow = allowlistByScopeAndId.get(key);
    runtimeTrackedAdvisories.push({
      id,
      severity: advisory.severity,
      title: advisory.title,
      url: advisory.url,
      packages: [...advisory.packages].sort((a, b) => a.localeCompare(b)),
      allowlisted: Boolean(allow),
      expires_on: allow?.expires_on ?? null,
      expired: allow ? expiredAllowlistKeys.has(key) : false,
      owner: allow?.owner ?? null,
      reason: allow?.reason ?? null,
    });
  }

  const devOnlyAdvisories = [];
  for (const [id, advisory] of fullAdvisories) {
    if (runtimeAdvisories.has(id)) {
      continue;
    }
    const severityRank = SEVERITY_ORDER.get(advisory.severity) ?? -1;
    if (severityRank < thresholdRank) {
      continue;
    }
    const key = `dev:${id}`;
    const allow = allowlistByScopeAndId.get(key);
    const expired = allow ? expiredAllowlistKeys.has(key) : false;
    devOnlyAdvisories.push({
      id,
      severity: advisory.severity,
      title: advisory.title,
      url: advisory.url,
      packages: [...advisory.packages].sort((a, b) => a.localeCompare(b)),
      allowlisted: Boolean(allow),
      expires_on: allow?.expires_on ?? null,
      expired,
      owner: allow?.owner ?? null,
      reason: allow?.reason ?? null,
    });
  }

  devOnlyAdvisories.sort((a, b) => {
    const rankDiff =
      (SEVERITY_ORDER.get(b.severity) ?? -1) - (SEVERITY_ORDER.get(a.severity) ?? -1);
    if (rankDiff !== 0) {
      return rankDiff;
    }
    return a.id.localeCompare(b.id);
  });
  runtimeTrackedAdvisories.sort((a, b) => {
    const rankDiff =
      (SEVERITY_ORDER.get(b.severity) ?? -1) - (SEVERITY_ORDER.get(a.severity) ?? -1);
    if (rankDiff !== 0) {
      return rankDiff;
    }
    return a.id.localeCompare(b.id);
  });

  const unallowlistedRuntime = runtimeTrackedAdvisories.filter((item) => !item.allowlisted);
  const unallowlistedDevOnly = devOnlyAdvisories.filter((item) => !item.allowlisted);
  const unallowlisted = [...unallowlistedRuntime, ...unallowlistedDevOnly];
  const expiredRuntime = runtimeTrackedAdvisories.filter(
    (item) => item.allowlisted && item.expired,
  );
  const expiredDevOnly = devOnlyAdvisories.filter((item) => item.allowlisted && item.expired);

  const activeAllowlistKeys = new Set([
    ...runtimeTrackedAdvisories.map((item) => `runtime:${item.id}`),
    ...devOnlyAdvisories.map((item) => `dev:${item.id}`),
  ]);
  const staleAllowlist = allowlistEntries
    .filter((entry) => !activeAllowlistKeys.has(`${entry.scope}:${entry.id}`))
    .map((entry) => ({
      id: entry.id,
      scope: entry.scope,
      expires_on: entry.expires_on ?? null,
      owner: entry.owner ?? null,
      reason: entry.reason ?? null,
    }));

  const summary = {
    generated_at: now.toISOString(),
    threshold,
    counts: {
      full_advisories: fullAdvisories.size,
      runtime_advisories: runtimeAdvisories.size,
      runtime_tracked: runtimeTrackedAdvisories.length,
      dev_only_tracked: devOnlyAdvisories.length,
      unallowlisted: unallowlisted.length,
      unallowlisted_runtime: unallowlistedRuntime.length,
      unallowlisted_dev_only: unallowlistedDevOnly.length,
      expired: expiredAllowlist.length,
      expired_runtime: expiredRuntime.length,
      expired_dev_only: expiredDevOnly.length,
      stale_allowlist: staleAllowlist.length,
    },
    full_advisories: advisoryMapToObject(fullAdvisories),
    runtime_advisories: advisoryMapToObject(runtimeAdvisories),
    runtime_tracked_advisories: runtimeTrackedAdvisories,
    dev_only_advisories: devOnlyAdvisories,
    unallowlisted,
    expired_allowlist: expiredAllowlist,
    expired_dev_only: expiredDevOnly,
    stale_allowlist: staleAllowlist,
  };

  if (summaryPath) {
    const resolvedSummaryPath = path.resolve(summaryPath);
    fs.mkdirSync(path.dirname(resolvedSummaryPath), { recursive: true });
    fs.writeFileSync(resolvedSummaryPath, `${JSON.stringify(summary, null, 2)}\n`, "utf8");
  }

  console.log(
    `npm advisories (severity >= ${threshold}): runtime=${runtimeTrackedAdvisories.length}, dev_only=${devOnlyAdvisories.length}, unallowlisted=${unallowlisted.length}, expired_allowlist_entries=${expiredAllowlist.length}`,
  );

  for (const item of unallowlistedRuntime) {
    console.log(
      `::warning::Unallowlisted runtime advisory ${item.id} (${item.severity}) affects ${item.packages.join(", ")}`,
    );
  }

  for (const item of unallowlistedDevOnly) {
    console.log(
      `::warning::Unallowlisted dev advisory ${item.id} (${item.severity}) affects ${item.packages.join(", ")}`,
    );
  }

  for (const item of expiredAllowlist) {
    console.log(
      `::warning::Expired ${item.scope ?? "unknown"} allowlist entry ${item.id} expired on ${item.expires_on} (owner: ${item.owner ?? "n/a"})`,
    );
  }

  for (const item of staleAllowlist) {
    console.log(
      `::notice::Stale ${item.scope} allowlist entry ${item.id} is not present in current audit results`,
    );
  }

  if (unallowlisted.length > 0 || expiredAllowlist.length > 0) {
    console.error(
      `npm advisory governance check failed on ${formatUtcDate(now)}: unallowlisted=${unallowlisted.length}, expired_allowlist=${expiredAllowlist.length}`,
    );
    process.exit(1);
  }

  console.log("npm advisory governance check passed");
}

try {
  main();
} catch (error) {
  const message = error instanceof Error ? error.message : String(error);
  console.error(`failed to validate npm audit allowlist: ${message}`);
  process.exit(2);
}
