import { describe, expect, it } from "vite-plus/test";

import {
  describeConsoleLocale,
  nextConsoleLocale,
  readStoredConsoleLocale,
  translateConsoleMessage,
} from "./i18n";

describe("console i18n", () => {
  it("cycles locales through english and pseudo-localization", () => {
    expect(nextConsoleLocale("en")).toBe("qps-ploc");
    expect(nextConsoleLocale("qps-ploc")).toBe("en");
  });

  it("keeps english as the primary locale", () => {
    expect(translateConsoleMessage("en", "shell.signOut")).toBe("Sign out");
    expect(describeConsoleLocale("en")).toBe("English");
  });

  it("keeps pseudo localization visible", () => {
    expect(translateConsoleMessage("qps-ploc", "shell.signOut")).toContain("[~ ");
  });

  it("falls back to english for unsupported stored locales", () => {
    window.localStorage.setItem("palyra.console.locale", "zz");
    expect(readStoredConsoleLocale()).toBe("en");
  });
});
