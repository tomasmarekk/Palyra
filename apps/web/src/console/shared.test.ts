import { describe, expect, it } from "vitest";

import { redactSensitiveText } from "./shared";

describe("redactSensitiveText", () => {
  it("masks embedded credentials unless sensitive values are revealed", () => {
    const snippet = "Stored provider key: sk-example-secret";

    expect(redactSensitiveText(snippet, false)).toBe("[redacted]");
    expect(redactSensitiveText(snippet, true)).toBe(snippet);
  });

  it("preserves ordinary memory snippets", () => {
    expect(redactSensitiveText("The deployment completed successfully.", false)).toBe(
      "The deployment completed successfully.",
    );
  });
});
