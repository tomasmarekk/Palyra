export type DesktopLocale = "en" | "qps-ploc";

export const DESKTOP_LOCALE_STORAGE_KEY = "palyra.desktop.locale";

export function readStoredDesktopLocale(): DesktopLocale {
  if (typeof window === "undefined") {
    return "en";
  }
  let stored: string | null;
  try {
    stored = window.localStorage.getItem(DESKTOP_LOCALE_STORAGE_KEY);
  } catch {
    return "en";
  }
  return stored === "qps-ploc" ? "qps-ploc" : "en";
}

export function writeStoredDesktopLocale(locale: DesktopLocale): void {
  if (typeof window === "undefined") {
    return;
  }
  try {
    window.localStorage.setItem(DESKTOP_LOCALE_STORAGE_KEY, locale);
  } catch {
    // Storage can be unavailable in locked-down WebView environments.
  }
}
