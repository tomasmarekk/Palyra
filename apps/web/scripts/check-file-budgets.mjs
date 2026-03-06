import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const scriptDir = path.dirname(fileURLToPath(import.meta.url));
const webRoot = path.resolve(scriptDir, "..");
const repoRoot = path.resolve(webRoot, "..", "..");

const budgets = [
  { path: "apps/web/src/App.tsx", maxLines: 140 },
  { path: "apps/web/src/console/ConsoleSectionContent.tsx", maxLines: 500 },
  { path: "apps/web/src/console/useConsoleAppState.tsx", maxLines: 1900 },
  { path: "apps/web/src/chat/ChatConsolePanel.tsx", maxLines: 900 },
  { path: "apps/desktop/src-tauri/src/lib.rs", maxLines: 1000 }
];

let hasFailure = false;

for (const budget of budgets) {
  const absolutePath = path.join(repoRoot, budget.path);
  const source = fs.readFileSync(absolutePath, "utf8");
  const lines = source.split(/\r?\n/).length;
  if (lines > budget.maxLines) {
    console.error(
      `file budget exceeded: ${budget.path} has ${lines} lines (max ${budget.maxLines})`
    );
    hasFailure = true;
    continue;
  }
  console.log(`file budget ok: ${budget.path} has ${lines}/${budget.maxLines} lines`);
}

if (hasFailure) {
  process.exit(1);
}
