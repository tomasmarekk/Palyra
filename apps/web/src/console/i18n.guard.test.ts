import { readFileSync } from "node:fs";

import { describe, expect, it } from "vite-plus/test";

const SENSITIVE_FILES = [
  "./ConsoleShell.tsx",
  "./components/layout/ConsoleBootScreen.tsx",
  "./components/layout/ConsoleAuthScreen.tsx",
  "./components/layout/ConsoleSidebarNav.tsx",
] as const;

const BANNED_COPY = [
  "Web Dashboard Operator Surface",
  "Operator Dashboard",
  "Palyra console",
  "Navigation",
  "Advanced session identity",
  "Signing in...",
  "Sign out",
] as const;

describe("console i18n guardrails", () => {
  it("keeps sensitive shell copy behind translation keys", () => {
    for (const file of SENSITIVE_FILES) {
      const source = readFileSync(new URL(file, import.meta.url), "utf8");
      for (const banned of BANNED_COPY) {
        const quoted = [`"${banned}"`, `'${banned}'`, `\`${banned}\``];
        expect(
          quoted.some((candidate) => source.includes(candidate)),
          `${file} should not hardcode '${banned}'`,
        ).toBe(false);
      }
    }
  });
});
