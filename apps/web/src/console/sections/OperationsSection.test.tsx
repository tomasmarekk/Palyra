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
            observability: {
              config_ref_health: {
                state: "degraded",
                summary: { blocking_refs: 1, warning_refs: 1 },
                recommendations: [
                  "Restart the daemon to refresh this config ref in the running runtime.",
                ],
                items: [
                  {
                    ref_id: "admin.auth_token_secret_ref:fp-1",
                    config_path: "admin.auth_token_secret_ref",
                    state: "stale",
                    severity: "warning",
                    reload_mode: "restart_required",
                    advice: "Restart the daemon to refresh this config ref in the running runtime.",
                  },
                ],
              },
            },
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
    expect(screen.getAllByText("Config ref health").length).toBeGreaterThan(0);
    expect(
      screen.getAllByText("Restart the daemon to refresh this config ref in the running runtime.")
        .length,
    ).toBeGreaterThan(0);
  });
});
