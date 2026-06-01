import { afterEach, describe, expect, it, vi } from "vite-plus/test";

import {
  DESKTOP_LOCALE_STORAGE_KEY,
  readStoredDesktopLocale,
  writeStoredDesktopLocale,
} from "./preferences";

afterEach(() => {
  vi.unstubAllGlobals();
});

describe("desktop preferences", () => {
  it("falls back to English when locale storage cannot be read", () => {
    vi.stubGlobal("window", {
      localStorage: {
        getItem() {
          throw new Error("storage unavailable");
        },
      },
    });

    expect(readStoredDesktopLocale()).toBe("en");
  });

  it("ignores locale persistence failures", () => {
    const setItem = vi.fn(() => {
      throw new Error("storage unavailable");
    });
    vi.stubGlobal("window", {
      localStorage: {
        getItem: vi.fn(),
        setItem,
      },
    });

    expect(() => writeStoredDesktopLocale("qps-ploc")).not.toThrow();
    expect(setItem).toHaveBeenCalledWith(DESKTOP_LOCALE_STORAGE_KEY, "qps-ploc");
  });
});
