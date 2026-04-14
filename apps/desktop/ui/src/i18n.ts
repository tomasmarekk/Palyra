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
  "desktop.header.locale.switchToEnglish": "English",
  "desktop.header.locale.switchToPseudo": "Pseudo",
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

export function translateDesktopMessage(
  locale: DesktopLocale,
  key: DesktopMessageKey,
  variables?: Record<string, string | number>,
): string {
  const template = DESKTOP_MESSAGES[key];
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
  return new Intl.DateTimeFormat(locale === "qps-ploc" ? "en-XA" : "en", {
    dateStyle: "medium",
    timeStyle: "short",
  }).format(unixMs);
}

function pseudoLocalize(value: string): string {
  const expanded = value.replaceAll(/[aeiouAEIOU]/g, (match) => `${match}${match}`);
  return `[~ ${expanded} ~]`;
}
