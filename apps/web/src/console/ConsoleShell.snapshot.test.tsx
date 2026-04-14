import { cleanup, render, screen } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vite-plus/test";
import { MemoryRouter } from "react-router-dom";

import { translateConsoleMessage, type ConsoleMessageKey } from "./i18n";
import { ConsoleShell } from "./ConsoleShell";
import type { ConsoleAppState } from "./useConsoleAppState";

afterEach(() => {
  cleanup();
});

describe("ConsoleShell layout coverage", () => {
  it("renders deterministic shell navigation for the M56 parity surface", () => {
    const app = {
      session: {
        principal: "admin:web-console",
        device_id: "device-1",
        channel: "web",
        csrf_token: "csrf-1",
        issued_at_unix_ms: 100,
        expires_at_unix_ms: 200,
      },
      locale: "en",
      theme: "dark",
      setTheme: vi.fn(),
      uiMode: "advanced",
      setUiMode: vi.fn(),
      setLocale: vi.fn(),
      t: (key: ConsoleMessageKey, variables?: Record<string, string | number>) =>
        translateConsoleMessage("en", key, variables),
      revealSensitiveValues: false,
      setRevealSensitiveValues: vi.fn(),
      signOut: vi.fn(),
      logoutBusy: false,
      section: "overview",
      setSection: vi.fn(),
      notice: "Overview refreshed.",
      error: null,
      uxTelemetryBusy: false,
      uxTelemetryAggregate: null,
      uxTelemetryEvents: [],
      refreshUxTelemetry: vi.fn(),
      emitUxEvent: vi.fn(async () => undefined),
    } as unknown as ConsoleAppState;

    render(
      <MemoryRouter>
        <ConsoleShell app={app}>
          <section className="console-card">
            <h2>Overview</h2>
            <p>Baseline shell snapshot.</p>
          </section>
        </ConsoleShell>
      </MemoryRouter>,
    );

    expect(
      screen.getByRole("heading", { name: "Web Dashboard Operator Surface" }),
    ).toBeInTheDocument();
    expect(screen.getByText("Overview refreshed.")).toBeInTheDocument();
    expect(screen.getByText("admin:web-console")).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Reveal sensitive values" })).toBeInTheDocument();
    expect(screen.getByText("Cookie session + CSRF")).toBeInTheDocument();
  });
});
