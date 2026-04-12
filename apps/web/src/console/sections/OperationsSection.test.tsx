import { cleanup, render, screen } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vite-plus/test";

import { OperationsSection } from "./OperationsSection";

afterEach(() => {
  cleanup();
  vi.restoreAllMocks();
});

describe("OperationsSection", () => {
  it("renders diagnostics when usage insights omit optional arrays", () => {
    render(
      <OperationsSection
        app={{
          auditBusy: false,
          auditFilterContains: "",
          setAuditFilterContains: vi.fn(),
          auditFilterPrincipal: "",
          setAuditFilterPrincipal: vi.fn(),
          auditEvents: [],
          refreshAudit: vi.fn(async () => {}),
          diagnosticsBusy: false,
          diagnosticsSnapshot: {
            model_provider: { state: "ok", provider: "deterministic" },
            auth_profiles: { state: "ok", profiles: [] },
            browserd: { state: "disabled", engine_mode: "headless_chrome" },
            observability: {},
          } as never,
          refreshDiagnostics: vi.fn(async () => {}),
          overviewUsageInsights: {
            routing: { default_mode: "suggest" },
            budgets: {},
          } as never,
          overviewCatalog: null,
          memoryStatus: null,
          refreshMemoryStatus: vi.fn(async () => {}),
          revealSensitiveValues: false,
        }}
      />,
    );

    expect(screen.getByRole("heading", { name: "Diagnostics" })).toBeInTheDocument();
    expect(screen.getByText("0 active alerts")).toBeInTheDocument();
  });
});
