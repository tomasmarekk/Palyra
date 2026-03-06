#!/usr/bin/env node

import { spawnSync } from "node:child_process";
import { existsSync, readdirSync, rmSync, statSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const packageRoot = dirname(dirname(fileURLToPath(import.meta.url)));
const GENERATED_PATHS = ["dist", "coverage", "node_modules", ".vite"];
const SUPPORTED_NODE_MAJOR_MIN = 20;
const SUPPORTED_NODE_MAJOR_MAX_EXCLUSIVE = 25;
const SUPPORTED_NPM_MAJOR_MIN = 10;
const SUPPORTED_NPM_MAJOR_MAX_EXCLUSIVE = 12;

function npmCommand() {
  return process.platform === "win32" ? "npm.cmd" : "npm";
}

function npmInvocation() {
  if (typeof process.env.npm_execpath === "string" && process.env.npm_execpath.length > 0) {
    return {
      command: process.execPath,
      baseArgs: [process.env.npm_execpath]
    };
  }
  return {
    command: npmCommand(),
    baseArgs: []
  };
}

function parseMajor(version) {
  const match = /^v?(\d+)/.exec(version.trim());
  if (!match) {
    throw new Error(`Unable to parse major version from '${version}'.`);
  }
  return Number.parseInt(match[1], 10);
}

function assertSupportedNodeVersion() {
  const nodeMajor = parseMajor(process.version);
  if (nodeMajor < SUPPORTED_NODE_MAJOR_MIN || nodeMajor >= SUPPORTED_NODE_MAJOR_MAX_EXCLUSIVE) {
    throw new Error(
      `apps/web requires Node ${SUPPORTED_NODE_MAJOR_MIN}-${SUPPORTED_NODE_MAJOR_MAX_EXCLUSIVE - 1}.x; current version is ${process.version}.`
    );
  }
}

function npmVersion() {
  const { command, baseArgs } = npmInvocation();
  const result = spawnSync(command, [...baseArgs, "--version"], {
    cwd: packageRoot,
    encoding: "utf8"
  });
  if (result.error) {
    throw new Error(`Failed to resolve npm version: ${result.error.message}`);
  }
  if (result.status !== 0) {
    throw new Error(`Failed to resolve npm version: ${result.stderr || result.stdout}`.trim());
  }
  return result.stdout.trim();
}

function assertSupportedNpmVersion() {
  const version = npmVersion();
  const npmMajor = parseMajor(version);
  if (npmMajor < SUPPORTED_NPM_MAJOR_MIN || npmMajor >= SUPPORTED_NPM_MAJOR_MAX_EXCLUSIVE) {
    throw new Error(
      `apps/web requires npm ${SUPPORTED_NPM_MAJOR_MIN}-${SUPPORTED_NPM_MAJOR_MAX_EXCLUSIVE - 1}.x; current version is ${version}.`
    );
  }
}

function run(command, args, description) {
  const result = spawnSync(command, args, {
    cwd: packageRoot,
    stdio: "inherit",
    env: process.env
  });
  if (result.error) {
    throw new Error(`${description} failed to start: ${result.error.message}`);
  }
  if (result.status !== 0) {
    throw new Error(`${description} failed with exit code ${result.status ?? "unknown"}.`);
  }
}

function runNpm(args, description) {
  const { command, baseArgs } = npmInvocation();
  run(command, [...baseArgs, ...args], description);
}

function assertToolchainVersions() {
  assertSupportedNodeVersion();
  assertSupportedNpmVersion();
}

function assertNodeModulesInstalled() {
  if (!existsSync(join(packageRoot, "node_modules"))) {
    throw new Error(
      "apps/web dependencies are missing. Run 'npm --prefix apps/web run bootstrap' or 'npm --prefix apps/web ci'."
    );
  }
}

function verifyLauncherPermissions() {
  if (process.platform === "win32") {
    return;
  }

  for (const bin of ["eslint", "tsc", "vite", "vitest"]) {
    const launcherPath = join(packageRoot, "node_modules", ".bin", bin);
    if (!existsSync(launcherPath)) {
      throw new Error(
        `Missing launcher '${launcherPath}'. Remove apps/web/node_modules and rerun 'npm --prefix apps/web ci'.`
      );
    }
    const mode = statSync(launcherPath).mode;
    if ((mode & 0o111) === 0) {
      throw new Error(
        `Launcher '${launcherPath}' is not executable. Archived node_modules handoffs are unsupported; remove apps/web/node_modules and rerun 'npm --prefix apps/web ci'.`
      );
    }
  }
}

function expectedRollupNativePackage() {
  switch (process.platform) {
    case "win32":
      if (process.arch === "x64") {
        return "@rollup/rollup-win32-x64-msvc";
      }
      if (process.arch === "arm64") {
        return "@rollup/rollup-win32-arm64-msvc";
      }
      break;
    case "linux":
      if (process.arch === "x64") {
        return "@rollup/rollup-linux-x64-gnu";
      }
      if (process.arch === "arm64") {
        return "@rollup/rollup-linux-arm64-gnu";
      }
      break;
    case "darwin":
      if (process.arch === "x64") {
        return "@rollup/rollup-darwin-x64";
      }
      if (process.arch === "arm64") {
        return "@rollup/rollup-darwin-arm64";
      }
      break;
    default:
      break;
  }
  return null;
}

function installedRollupNativePackages() {
  const rollupDir = join(packageRoot, "node_modules", "@rollup");
  if (!existsSync(rollupDir)) {
    return [];
  }
  return readdirSync(rollupDir)
    .filter((entry) => entry.startsWith("rollup-"))
    .map((entry) => `@rollup/${entry}`)
    .sort();
}

async function verifyRollupInstall() {
  const expectedPackage = expectedRollupNativePackage();
  const installedPackages = installedRollupNativePackages();

  if (expectedPackage !== null && !installedPackages.includes(expectedPackage)) {
    const installedSummary =
      installedPackages.length > 0 ? installedPackages.join(", ") : "(none installed)";
    throw new Error(
      `Expected Rollup native package '${expectedPackage}' for ${process.platform}/${process.arch}, found ${installedSummary}. Remove apps/web/node_modules and rerun 'npm --prefix apps/web ci'.`
    );
  }

  try {
    await import("rollup");
  } catch (error) {
    const reason = error instanceof Error ? error.message : String(error);
    throw new Error(
      `Rollup import failed after install (${reason}). Remove apps/web/node_modules and rerun 'npm --prefix apps/web ci'.`
    );
  }
}

async function verifyInstall() {
  assertToolchainVersions();
  assertNodeModulesInstalled();
  verifyLauncherPermissions();
  await verifyRollupInstall();
  console.log("apps/web install verification passed.");
}

function clean() {
  for (const relativePath of GENERATED_PATHS) {
    rmSync(join(packageRoot, relativePath), { force: true, recursive: true });
  }
  console.log(`Removed generated apps/web outputs: ${GENERATED_PATHS.join(", ")}`);
}

function bootstrap() {
  assertToolchainVersions();
  runNpm(["ci"], "npm ci");
}

async function ciCheck() {
  await verifyInstall();
  runNpm(["run", "lint"], "npm run lint");
  runNpm(["run", "typecheck"], "npm run typecheck");
  runNpm(["run", "test:run"], "npm run test:run");
  runNpm(["run", "build"], "npm run build");
}

async function main() {
  const command = process.argv[2];

  switch (command) {
    case "bootstrap":
      bootstrap();
      await verifyInstall();
      break;
    case "clean":
      clean();
      break;
    case "verify-install":
      await verifyInstall();
      break;
    case "ci-check":
      await ciCheck();
      break;
    case "cleanroom-check":
      clean();
      bootstrap();
      await ciCheck();
      break;
    default:
      console.error(
        "Usage: node ./scripts/devex.mjs <bootstrap|clean|verify-install|ci-check|cleanroom-check>"
      );
      process.exit(1);
  }
}

await main();
