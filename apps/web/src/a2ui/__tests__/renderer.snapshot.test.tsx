import { render, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";

import { applyPatchDocument } from "../patch";
import { A2uiRenderer } from "../renderer";
import { createDemoDocument } from "../sample";
import type { JsonValue, PatchDocument } from "../types";
import { normalizeA2uiDocument } from "../normalize";

describe("A2uiRenderer coverage", () => {
  it("renders deterministic markup for baseline document", () => {
    const document = createDemoDocument();
    render(<A2uiRenderer document={document} />);

    expect(screen.getByText("Renderer online. Waiting for incremental patches.")).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Apply" })).toBeInTheDocument();
    expect(screen.getByRole("grid", { name: "metrics" })).toBeInTheDocument();
    expect(screen.getByText("Patch Loop Latency (ms)")).toBeInTheDocument();
  });

  it("renders deterministic markup after patch updates", () => {
    const patch: PatchDocument = {
      v: 1,
      ops: [
        {
          op: "replace",
          path: "/components/0/props/value",
          value: "Renderer online. Snapshot patch applied."
        },
        {
          op: "add",
          path: "/components/2/props/items/-",
          value: "Snapshot list extension"
        },
        {
          op: "replace",
          path: "/components/5/props/series/1/value",
          value: 14
        }
      ]
    };
    const patchedState = applyPatchDocument(createDemoDocument() as unknown as JsonValue, patch);
    const patchedDocument = normalizeA2uiDocument(patchedState);
    render(<A2uiRenderer document={patchedDocument} />);

    expect(screen.getByText("Renderer online. Snapshot patch applied.")).toBeInTheDocument();
    expect(screen.getByText("Snapshot list extension")).toBeInTheDocument();
    expect(screen.getByText("14")).toBeInTheDocument();
  });
});
