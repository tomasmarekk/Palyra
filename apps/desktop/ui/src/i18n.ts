import type { DesktopLocale } from "./preferences";

const DESKTOP_MESSAGES = {
  "desktop.header.eyebrow": "Desktop Companion",
  "desktop.header.title": "Palyra companion shell",
  "desktop.header.description":
    "One desktop surface for runtime control, active sessions, approvals, trust review, and reconnect-safe drafts.",
  "desktop.controlCenter.eyebrow": "Desktop Control Center",
  "desktop.controlCenter.title": "Launch the runtime, verify posture, then open the dashboard.",
  "desktop.controlCenter.description":
    "Desktop stays intentionally short: lifecycle controls, runtime health, and the fastest handoff into the full operator console.",
  "desktop.controlCenter.snapshotRefreshing": "Refreshing snapshot",
  "desktop.controlCenter.snapshotReady": "Snapshot ready",
  "desktop.controlCenter.overallState": "Overall state",
  "desktop.controlCenter.dashboardMode": "Dashboard mode",
  "desktop.controlCenter.lastSnapshot": "Last snapshot",
  "desktop.header.refresh": "Refresh",
  "desktop.header.refreshing": "Refreshing...",
  "desktop.header.openDashboard": "Open dashboard",
  "desktop.header.locale.label": "Language: {locale}",
  "desktop.common.english": "English",
  "desktop.common.czech": "Czech",
  "desktop.common.pseudo": "Pseudo",
  "desktop.notice.preview.title": "Preview data active",
  "desktop.notice.preview.body":
    "The Tauri bridge or local runtime data is unavailable, so the companion shell is rendering preview data.",
  "desktop.notice.result.title": "Desktop action result",
  "desktop.notice.refreshFailed.title": "Companion refresh failed",
  "desktop.notice.warnings.title": "Companion warnings",
  "desktop.notice.profile.title": "Active profile: {label}",
  "desktop.notice.profile.body": "Environment {environment}, risk {riskLevel}, mode {mode}.",
  "desktop.common.never": "Never",
  "desktop.section.home": "Home",
  "desktop.section.chat": "Chat",
  "desktop.section.approvals": "Approvals",
  "desktop.section.access": "Access",
  "desktop.section.onboarding": "Onboarding",
  "desktop.onboarding.title": "Onboarding and rollout",
  "desktop.onboarding.description":
    "Desktop keeps current onboarding progress, authentication readiness, and release rollout state visible in one place.",
  "desktop.onboarding.browserHandoff": "Browser handoff",
  "desktop.onboarding.toggleShell": "Toggle shell",
  "desktop.onboarding.recoveryHint": "Recovery hint",
  "desktop.onboarding.readiness.title": "Readiness",
  "desktop.onboarding.readiness.description":
    "Completion criteria for the desktop companion release path and operator onboarding handoff.",
  "desktop.onboarding.progress.label": "Onboarding progress",
  "desktop.onboarding.auth.label": "OpenAI auth",
  "desktop.onboarding.auth.ready": "Ready",
  "desktop.onboarding.auth.attention": "Attention",
  "desktop.onboarding.auth.emptyNote": "No auth note published.",
  "desktop.onboarding.completion.label": "Last completion",
  "desktop.onboarding.completion.detail": "Persisted locally so desktop can resume after restart.",
} as const;

export type DesktopMessageKey = keyof typeof DESKTOP_MESSAGES;

const DESKTOP_MESSAGES_CS: Readonly<Record<DesktopMessageKey, string>> = {
  "desktop.header.eyebrow": "Desktop Companion",
  "desktop.header.title": "Companion shell Palyry",
  "desktop.header.description":
    "Jedna desktopová plocha pro řízení runtime, aktivní relace, schválení, trust review a drafty odolné proti reconnectu.",
  "desktop.controlCenter.eyebrow": "Desktop Control Center",
  "desktop.controlCenter.title": "Spusť runtime, ověř postoj a pak otevři dashboard.",
  "desktop.controlCenter.description":
    "Desktop zůstává záměrně stručný: lifecycle ovládání, health runtime a nejrychlejší handoff do plné operátorské konzole.",
  "desktop.controlCenter.snapshotRefreshing": "Obnovuji snapshot",
  "desktop.controlCenter.snapshotReady": "Snapshot připraven",
  "desktop.controlCenter.overallState": "Celkový stav",
  "desktop.controlCenter.dashboardMode": "Režim dashboardu",
  "desktop.controlCenter.lastSnapshot": "Poslední snapshot",
  "desktop.header.refresh": "Obnovit",
  "desktop.header.refreshing": "Obnovuji...",
  "desktop.header.openDashboard": "Otevřít dashboard",
  "desktop.header.locale.label": "Jazyk: {locale}",
  "desktop.common.english": "Angličtina",
  "desktop.common.czech": "Čeština",
  "desktop.common.pseudo": "Pseudo",
  "desktop.notice.preview.title": "Aktivní preview data",
  "desktop.notice.preview.body":
    "Bridge Tauri nebo lokální runtime data nejsou dostupná, takže companion shell vykresluje preview data.",
  "desktop.notice.result.title": "Výsledek desktopové akce",
  "desktop.notice.refreshFailed.title": "Obnovení companionu selhalo",
  "desktop.notice.warnings.title": "Varování companionu",
  "desktop.notice.profile.title": "Aktivní profil: {label}",
  "desktop.notice.profile.body": "Prostředí {environment}, riziko {riskLevel}, režim {mode}.",
  "desktop.common.never": "Nikdy",
  "desktop.section.home": "Domů",
  "desktop.section.chat": "Chat",
  "desktop.section.approvals": "Schválení",
  "desktop.section.access": "Přístup",
  "desktop.section.onboarding": "Onboarding",
  "desktop.onboarding.title": "Onboarding a rollout",
  "desktop.onboarding.description":
    "Desktop drží na jednom místě viditelný aktuální onboarding progress, auth připravenost a stav release rolloutu.",
  "desktop.onboarding.browserHandoff": "Předání do prohlížeče",
  "desktop.onboarding.toggleShell": "Přepnout shell",
  "desktop.onboarding.recoveryHint": "Recovery hint",
  "desktop.onboarding.readiness.title": "Připravenost",
  "desktop.onboarding.readiness.description":
    "Kritéria dokončení pro release cestu desktop companionu a handoff operátorského onboardingu.",
  "desktop.onboarding.progress.label": "Průběh onboardingu",
  "desktop.onboarding.auth.label": "OpenAI auth",
  "desktop.onboarding.auth.ready": "Připraveno",
  "desktop.onboarding.auth.attention": "Pozornost",
  "desktop.onboarding.auth.emptyNote": "Není publikovaná žádná auth poznámka.",
  "desktop.onboarding.completion.label": "Poslední dokončení",
  "desktop.onboarding.completion.detail": "Uloženo lokálně, aby desktop mohl po restartu navázat.",
};

export const DESKTOP_LOCALES: readonly DesktopLocale[] = ["en", "cs", "qps-ploc"] as const;

export function translateDesktopMessage(
  locale: DesktopLocale,
  key: DesktopMessageKey,
  variables?: Record<string, string | number>,
): string {
  const template = (locale === "cs" ? DESKTOP_MESSAGES_CS : DESKTOP_MESSAGES)[key];
  const formatted = template.replaceAll(/\{(\w+)\}/g, (_, name: string) => {
    const value = variables?.[name];
    return value === undefined ? "" : String(value);
  });
  return locale === "qps-ploc" ? pseudoLocalize(formatted) : formatted;
}

export function formatDesktopDateTime(locale: DesktopLocale, unixMs?: number): string {
  if (unixMs === undefined || unixMs <= 0 || !Number.isFinite(unixMs)) {
    return translateDesktopMessage(locale, "desktop.common.never");
  }
  return new Intl.DateTimeFormat(resolveDesktopIntlLocale(locale), {
    dateStyle: "medium",
    timeStyle: "short",
  }).format(unixMs);
}

export function nextDesktopLocale(current: DesktopLocale): DesktopLocale {
  const index = DESKTOP_LOCALES.indexOf(current);
  return DESKTOP_LOCALES[(index + 1) % DESKTOP_LOCALES.length] ?? "en";
}

export function describeDesktopLocale(locale: DesktopLocale): string {
  switch (locale) {
    case "cs":
      return translateDesktopMessage(locale, "desktop.common.czech");
    case "qps-ploc":
      return translateDesktopMessage(locale, "desktop.common.pseudo");
    default:
      return translateDesktopMessage(locale, "desktop.common.english");
  }
}

function resolveDesktopIntlLocale(locale: DesktopLocale): string {
  return locale === "qps-ploc" ? "en-XA" : locale;
}

function pseudoLocalize(value: string): string {
  const expanded = value.replaceAll(/[aeiouAEIOU]/g, (match) => `${match}${match}`);
  return `[~ ${expanded} ~]`;
}
