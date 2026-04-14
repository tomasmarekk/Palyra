export type DesktopLocale = "en" | "qps-ploc";

export const DESKTOP_LOCALE_STORAGE_KEY = "palyra.desktop.locale";

export function readStoredDesktopLocale(): DesktopLocale {
  if (typeof window === "undefined") {
    return "en";
  }
  const stored = window.localStorage.getItem(DESKTOP_LOCALE_STORAGE_KEY);
  return stored === "qps-ploc" ? "qps-ploc" : "en";
}
