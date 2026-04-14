import { describe, expect, it } from "vite-plus/test";

import authScreenSource from "./components/layout/ConsoleAuthScreen.tsx?raw";
import bootScreenSource from "./components/layout/ConsoleBootScreen.tsx?raw";
import sidebarNavSource from "./components/layout/ConsoleSidebarNav.tsx?raw";
import consoleShellSource from "./ConsoleShell.tsx?raw";

const SENSITIVE_FILES = [
  ["./ConsoleShell.tsx", consoleShellSource],
  ["./components/layout/ConsoleBootScreen.tsx", bootScreenSource],
  ["./components/layout/ConsoleAuthScreen.tsx", authScreenSource],
  ["./components/layout/ConsoleSidebarNav.tsx", sidebarNavSource],
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
    for (const [file, source] of SENSITIVE_FILES) {
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
