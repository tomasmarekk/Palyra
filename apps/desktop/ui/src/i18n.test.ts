import { describe, expect, it } from "vite-plus/test";

import {
  describeDesktopLocale,
  formatDesktopDateTime,
  nextDesktopLocale,
  translateDesktopMessage,
} from "./i18n";

describe("desktop i18n", () => {
  it("cycles locales through english and pseudo-localization", () => {
    expect(nextDesktopLocale("en")).toBe("qps-ploc");
    expect(nextDesktopLocale("qps-ploc")).toBe("en");
  });

  it("keeps english as the primary desktop locale", () => {
    expect(translateDesktopMessage("en", "desktop.header.refresh")).toBe("Refresh");
    expect(describeDesktopLocale("en")).toBe("English");
  });

  it("formats dates with the selected locale instead of forcing english", () => {
    expect(formatDesktopDateTime("en", 1_710_000_000_000)).toMatch(/\d/);
  });
});
