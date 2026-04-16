import { CONSOLE_LOCALE_STORAGE_KEY, type ConsoleLocale, type ConsoleUiMode } from "./preferences";

const EN_MESSAGES = {
  "boot.label": "Palyra console",
  "boot.title": "Web Dashboard",
  "boot.body": "Checking the current session and loading the dashboard shell.",
  "auth.label": "Palyra console",
  "auth.title": "Operator Dashboard",
  "auth.body":
    "Desktop handoff remains the shortest path, but direct browser sign-in still uses the same admin token, cookie session, and CSRF guardrails.",
  "auth.adminToken": "Admin token",
  "auth.advancedIdentity": "Advanced session identity",
  "auth.operatorPrincipal": "Operator principal",
  "auth.deviceLabel": "Device label",
  "auth.channelLabel": "Channel label",
  "auth.optional": "Optional",
  "auth.browserPathTitle": "Browser sign-in path",
  "auth.browserPathBody":
    "Manual browser sign-in still keeps the existing session cookie and CSRF guardrails in place. Open from desktop for the shortest local path on a single machine.",
  "auth.restoreDefaults": "Restore defaults",
  "auth.signIn": "Sign in",
  "auth.signingIn": "Signing in...",
  "auth.failed": "Sign-in failed",
  "shell.title": "Web Dashboard Operator Surface",
  "shell.subtitle": "{group} domain focused on {detail}.",
  "shell.authenticated": "Authenticated",
  "shell.theme": "Theme: {theme}",
  "shell.locale": "Locale: {locale}",
  "shell.mode": "Mode: {mode}",
  "shell.signOut": "Sign out",
  "shell.signingOut": "Signing out...",
  "shell.sessionContext": "Session context",
  "shell.sessionContextBody":
    "Principal, device, and disclosure controls stay compact and secondary to the page.",
  "shell.revealSensitive": "Reveal sensitive values: {state}",
  "shell.actionResult": "Action result",
  "shell.actionBlocked": "Action blocked",
  "shell.strictPosture": "Strict posture",
  "shell.on": "On",
  "shell.off": "Off",
  "shell.basic": "Basic",
  "shell.advanced": "Advanced",
  "shell.english": "English",
  "shell.pseudo": "Pseudo",
  "shell.profileActive": "Active profile: {label}",
  "shell.profileBody": "Mode {mode}, environment {environment}, risk {risk}.",
  "shell.principal": "Principal",
  "shell.device": "Device",
  "shell.channel": "Channel",
  "shell.transport": "Transport",
  "shell.profile": "Profile",
  "shell.environment": "Environment",
  "shell.risk": "Risk",
  "shell.none": "none",
  "shell.notApplicable": "n/a",
  "shell.transportValue": "Cookie session + CSRF",
  "nav.title": "Navigation",
  "nav.subtitle":
    "Chat, observability, control, agent, and settings stay grouped as one working rail.",
  "nav.basicTitle": "Basic mode guidance",
  "nav.basicBody":
    "Basic mode keeps the first-success rail visible and hides the rest behind an explicit switch back to the full operator surface.",
  "nav.switchAdvanced": "Switch to Advanced",
  "nav.switchBasic": "Switch to Basic",
  "nav.group.chat": "Chat",
  "nav.group.control": "Observability",
  "nav.group.operations": "Control",
  "nav.group.agent": "Agent",
  "nav.group.settings": "Settings",
  "section.overview.label": "Overview",
  "section.overview.detail": "Operational posture, risks, and next actions",
  "section.chat.label": "Chat",
  "section.chat.detail": "Primary operator workspace",
  "section.canvas.label": "Canvas",
  "section.canvas.detail": "Session-linked rich surface and history",
  "section.sessions.label": "Sessions",
  "section.sessions.detail": "Catalog, lifecycle, and run posture",
  "section.usage.label": "Usage",
  "section.usage.detail": "Capacity, tokens, latency, and cost posture",
  "section.logs.label": "Logs",
  "section.logs.detail": "Unified runtime stream across palyrad, browserd, and channels",
  "section.inventory.label": "Inventory",
  "section.inventory.detail": "Nodes, devices, pending pairings, and runtime instances",
  "section.approvals.label": "Approvals",
  "section.approvals.detail": "Sensitive action gate",
  "section.cron.label": "Automations",
  "section.cron.detail":
    "Heartbeats, standing orders, programs, routines, runs, templates, and approvals",
  "section.channels.label": "Channels",
  "section.channels.detail": "Connectors, router, and Discord setup",
  "section.browser.label": "Browser",
  "section.browser.detail": "Profiles, relay, and downloads",
  "section.agents.label": "Agents",
  "section.agents.detail": "Registry, defaults, and workspace setup",
  "section.memory.label": "Memory",
  "section.memory.detail": "Retention, search, and purge",
  "section.skills.label": "Skills",
  "section.skills.detail": "Install and runtime trust posture",
  "section.auth.label": "Profiles",
  "section.auth.detail": "Provider auth profiles and health posture",
  "section.config.label": "Config",
  "section.config.detail": "Inspect, validate, mutate, and recover config",
  "section.secrets.label": "Secrets",
  "section.secrets.detail": "Vault-backed secret metadata and reveal flows",
  "section.access.label": "Access",
  "section.access.detail": "Pairing lifecycle and remote access posture",
  "section.operations.label": "Diagnostics",
  "section.operations.detail": "Runtime snapshots, audit, and CLI handoffs",
  "section.support.label": "Support",
  "section.support.detail": "Bundles, recovery, and operator handoff",
  "guidance.nextAction": "Next action",
  "guidance.checklist": "Onboarding checklist",
  "guidance.troubleshooting": "Troubleshooting",
  "guidance.scenario": "Scenario",
  "guidance.cta": "Open",
  "overview.telemetryTitle": "Activation baseline",
  "overview.telemetryBody":
    "Phase 1 baseline metrics stay audit-backed through system events instead of a separate analytics vendor pipeline.",
  "overview.telemetryFunnel": "Funnel progress",
  "overview.telemetryApprovals": "Approval fatigue",
  "overview.telemetryFriction": "Top friction surface",
  "overview.telemetryEmpty": "No UX baseline events recorded yet.",
  "overview.modeGuidanceTitle": "Mode guidance",
  "overview.modeGuidanceBody":
    "Basic mode narrows attention to first-success surfaces. Advanced mode restores the full operator rail instantly without changing backend capability.",
  "mode.basic.description":
    "Chat, overview, sessions, approvals, access, and support stay primary.",
  "mode.advanced.description": "Every operator section stays visible directly in the sidebar.",
} as const;

export type ConsoleMessageKey = keyof typeof EN_MESSAGES;

export function readStoredConsoleLocale(): ConsoleLocale {
  if (typeof window === "undefined") {
    return "en";
  }
  const stored = window.localStorage.getItem(CONSOLE_LOCALE_STORAGE_KEY);
  return stored === "qps-ploc" ? "qps-ploc" : "en";
}

export function translateConsoleMessage(
  locale: ConsoleLocale,
  key: ConsoleMessageKey,
  variables?: Record<string, string | number>,
): string {
  const template = EN_MESSAGES[key];
  const resolved = variables === undefined ? template : interpolate(template, variables);
  return locale === "qps-ploc" ? pseudoLocalize(resolved) : resolved;
}

export function describeConsoleMode(locale: ConsoleLocale, mode: ConsoleUiMode): string {
  return translateConsoleMessage(locale, mode === "basic" ? "shell.basic" : "shell.advanced");
}

export function formatConsoleDateTime(
  locale: ConsoleLocale,
  unixMs: number,
  options: Intl.DateTimeFormatOptions,
): string {
  return new Intl.DateTimeFormat(resolveIntlLocale(locale), options).format(new Date(unixMs));
}

export function formatConsoleNumber(locale: ConsoleLocale, value: number): string {
  return new Intl.NumberFormat(resolveIntlLocale(locale)).format(value);
}

function interpolate(template: string, variables: Record<string, string | number>): string {
  return template.replaceAll(/\{([a-zA-Z0-9_]+)\}/g, (_, key) => `${variables[key] ?? ""}`);
}

function pseudoLocalize(value: string): string {
  const replaced = value.replaceAll(/[A-Za-z]/g, (character) => PSEUDO_MAP[character] ?? character);
  return `[~ ${replaced} ~]`;
}

function resolveIntlLocale(locale: ConsoleLocale): string {
  return locale === "qps-ploc" ? "en" : locale;
}

const PSEUDO_MAP: Readonly<Record<string, string>> = {
  A: "Å",
  B: "Ɓ",
  C: "Č",
  D: "Đ",
  E: "Ē",
  F: "Ƒ",
  G: "Ğ",
  H: "Ħ",
  I: "Ī",
  J: "Ĵ",
  K: "Ķ",
  L: "Ŀ",
  M: "Ṁ",
  N: "Ń",
  O: "Ø",
  P: "Ṕ",
  Q: "Ǫ",
  R: "Ŕ",
  S: "Š",
  T: "Ŧ",
  U: "Ū",
  V: "Ṽ",
  W: "Ŵ",
  X: "Ẋ",
  Y: "Ŷ",
  Z: "Ž",
  a: "å",
  b: "ƀ",
  c: "č",
  d: "đ",
  e: "ē",
  f: "ƒ",
  g: "ğ",
  h: "ħ",
  i: "ī",
  j: "ĵ",
  k: "ķ",
  l: "ŀ",
  m: "ṁ",
  n: "ń",
  o: "ø",
  p: "ṕ",
  q: "ǫ",
  r: "ŕ",
  s: "š",
  t: "ŧ",
  u: "ū",
  v: "ṽ",
  w: "ŵ",
  x: "ẋ",
  y: "ŷ",
  z: "ž",
} as const;
