import { cleanup, render } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";
import { MemoryRouter } from "react-router-dom";

import { ConsoleShell } from "./ConsoleShell";
import type { ConsoleAppState } from "./useConsoleAppState";

afterEach(() => {
  cleanup();
});

describe("ConsoleShell snapshot coverage", () => {
  it("renders deterministic shell navigation for the M56 parity surface", () => {
    const app = {
      session: {
        principal: "admin:web-console",
        device_id: "device-1",
        channel: "web",
        csrf_token: "csrf-1",
        issued_at_unix_ms: 100,
        expires_at_unix_ms: 200
      },
      theme: "dark",
      setTheme: vi.fn(),
      revealSensitiveValues: false,
      setRevealSensitiveValues: vi.fn(),
      signOut: vi.fn(),
      logoutBusy: false,
      section: "overview",
      setSection: vi.fn(),
      notice: "Overview refreshed.",
      error: null
    } as unknown as ConsoleAppState;

    const rendered = render(
      <MemoryRouter>
        <ConsoleShell app={app}>
          <section className="console-card">
            <h2>Overview</h2>
            <p>Baseline shell snapshot.</p>
          </section>
        </ConsoleShell>
      </MemoryRouter>
    );

    expect(rendered.container.firstChild).toMatchSnapshot();
  });
});
