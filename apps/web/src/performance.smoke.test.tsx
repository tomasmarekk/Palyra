import { cleanup, render, screen } from "@testing-library/react";
import { afterEach, describe, expect, it } from "vite-plus/test";

import { applyAssistantTokenBatch } from "./chat/chatShared";
import { PrettyJsonBlock } from "./console/shared";
import { shouldAutoRefreshSection } from "./console/useConsoleAppState";

afterEach(() => {
  cleanup();
});

describe("web performance smoke", () => {
  it("coalesces assistant transcript updates per run instead of appending duplicate entries", () => {
    const assistantEntries = new Map<string, string>();

    const firstPass = applyAssistantTokenBatch(
      [],
      assistantEntries,
      [["run-1", { token: "Hello", isFinal: false }]],
      100,
    );
    const secondPass = applyAssistantTokenBatch(
      firstPass,
      assistantEntries,
      [["run-1", { token: " world", isFinal: true }]],
      120,
    );

    expect(secondPass).toHaveLength(1);
    expect(secondPass[0]).toMatchObject({
      id: "assistant-run-1-100",
      kind: "assistant",
      text: "Hello world",
      is_final: true,
    });
  });

  it("skips eager auto-refresh while diagnostics and support sections are still fresh", () => {
    expect(shouldAutoRefreshSection("operations", null, 20_000)).toBe(true);
    expect(shouldAutoRefreshSection("operations", 15_500, 20_000)).toBe(false);
    expect(shouldAutoRefreshSection("operations", 9_500, 20_000)).toBe(true);

    expect(shouldAutoRefreshSection("chat", 19_500, 20_000)).toBe(true);
    expect(shouldAutoRefreshSection("support", 15_500, 20_000)).toBe(false);
    expect(shouldAutoRefreshSection("support", 9_500, 20_000)).toBe(true);
  });

  it("renders redacted pretty-json blocks without exposing sensitive values", () => {
    render(
      <PrettyJsonBlock
        value={{
          api_key: "sk-test-secret",
          nested: {
            authorization: "Bearer top-secret",
          },
        }}
        revealSensitiveValues={false}
      />,
    );

    const block = screen.getByText((content) => content.includes("[redacted]"));
    expect(block.textContent).not.toContain("sk-test-secret");
    expect(block.textContent).not.toContain("top-secret");
  });
});
