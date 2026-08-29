// @vitest-environment jsdom
// Verifies that the desktop companion refresh scheduler honors its configured delay.
// This guards the native control plane from render-driven polling storms.

import { act } from "react";
import { createRoot, type Root } from "react-dom/client";
import { afterEach, beforeEach, describe, expect, it, vi } from "vite-plus/test";

import { DESKTOP_PREVIEW_COMPANION_SNAPSHOT, getDesktopCompanionSnapshot } from "../lib/desktopApi";
import { useDesktopCompanion } from "./useDesktopCompanion";

vi.mock("../lib/desktopApi", () => ({
  DESKTOP_PREVIEW_COMPANION_SNAPSHOT: { connection_state: "disconnected" },
  getDesktopCompanionSnapshot: vi.fn(),
  isDesktopHostAvailable: () => true,
}));

function CompanionProbe() {
  useDesktopCompanion();
  return null;
}

describe("useDesktopCompanion", () => {
  let container: HTMLDivElement;
  let root: Root;

  beforeEach(() => {
    vi.stubGlobal("IS_REACT_ACT_ENVIRONMENT", true);
    vi.useFakeTimers();
    container = document.createElement("div");
    document.body.append(container);
    root = createRoot(container);
  });

  afterEach(() => {
    act(() => root.unmount());
    container.remove();
    vi.useRealTimers();
    vi.clearAllMocks();
    vi.unstubAllGlobals();
  });

  it("waits for the configured interval after a successful refresh", async () => {
    const getSnapshot = vi.mocked(getDesktopCompanionSnapshot);
    getSnapshot
      .mockResolvedValueOnce({
        ...DESKTOP_PREVIEW_COMPANION_SNAPSHOT,
        connection_state: "connected",
      })
      .mockImplementation(() => new Promise(() => {}));

    await act(async () => {
      root.render(<CompanionProbe />);
      await Promise.resolve();
    });

    expect(getSnapshot).toHaveBeenCalledTimes(1);

    await act(async () => {
      await vi.advanceTimersByTimeAsync(4_999);
    });
    expect(getSnapshot).toHaveBeenCalledTimes(1);

    await act(async () => {
      await vi.advanceTimersByTimeAsync(1);
    });
    expect(getSnapshot).toHaveBeenCalledTimes(2);
  });
});
