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
  "shell.czech": "Czech",
  "shell.pseudo": "Pseudo",
  "shell.expires": "Expires {value} UTC",
  "shell.dashboardDomains": "Dashboard domains",
  "shell.revealSensitiveAria": "Reveal sensitive values",
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

const CS_MESSAGES: Readonly<Record<ConsoleMessageKey, string>> = {
  "boot.label": "Konzole Palyra",
  "boot.title": "Webový dashboard",
  "boot.body": "Ověřuji aktuální relaci a načítám shell dashboardu.",
  "auth.label": "Konzole Palyra",
  "auth.title": "Operátorský dashboard",
  "auth.body":
    "Předání z desktopu zůstává nejkratší cestou, ale přímé přihlášení v prohlížeči stále používá stejný admin token, cookie relaci a CSRF guardraily.",
  "auth.adminToken": "Admin token",
  "auth.advancedIdentity": "Rozšířená identita relace",
  "auth.operatorPrincipal": "Principál operátora",
  "auth.deviceLabel": "Označení zařízení",
  "auth.channelLabel": "Označení kanálu",
  "auth.optional": "Volitelné",
  "auth.browserPathTitle": "Cesta přihlášení přes prohlížeč",
  "auth.browserPathBody":
    "Ruční přihlášení v prohlížeči stále zachovává existující session cookie a CSRF guardraily. Na jednom stroji zůstává nejkratší lokální cestou otevření z desktopu.",
  "auth.restoreDefaults": "Obnovit výchozí",
  "auth.signIn": "Přihlásit se",
  "auth.signingIn": "Přihlašuji...",
  "auth.failed": "Přihlášení selhalo",
  "shell.title": "Operátorská plocha webového dashboardu",
  "shell.subtitle": "Doména {group} zaměřená na {detail}.",
  "shell.authenticated": "Ověřeno",
  "shell.theme": "Motiv: {theme}",
  "shell.locale": "Jazyk: {locale}",
  "shell.mode": "Režim: {mode}",
  "shell.signOut": "Odhlásit",
  "shell.signingOut": "Odhlašuji...",
  "shell.sessionContext": "Kontext relace",
  "shell.sessionContextBody":
    "Principál, zařízení a ovládání zveřejnění zůstávají kompaktní a druhotné vůči samotné stránce.",
  "shell.revealSensitive": "Zobrazit citlivé hodnoty: {state}",
  "shell.actionResult": "Výsledek akce",
  "shell.actionBlocked": "Akce zablokována",
  "shell.strictPosture": "Přísný postoj",
  "shell.on": "Zapnuto",
  "shell.off": "Vypnuto",
  "shell.basic": "Základní",
  "shell.advanced": "Pokročilý",
  "shell.english": "Angličtina",
  "shell.czech": "Čeština",
  "shell.pseudo": "Pseudo",
  "shell.expires": "Platnost končí {value} UTC",
  "shell.dashboardDomains": "Oblasti dashboardu",
  "shell.revealSensitiveAria": "Zobrazit citlivé hodnoty",
  "shell.profileActive": "Aktivní profil: {label}",
  "shell.profileBody": "Režim {mode}, prostředí {environment}, riziko {risk}.",
  "shell.principal": "Principál",
  "shell.device": "Zařízení",
  "shell.channel": "Kanál",
  "shell.transport": "Transport",
  "shell.profile": "Profil",
  "shell.environment": "Prostředí",
  "shell.risk": "Riziko",
  "shell.none": "žádné",
  "shell.notApplicable": "n/a",
  "shell.transportValue": "Cookie relace + CSRF",
  "nav.title": "Navigace",
  "nav.subtitle":
    "Chat, observabilita, řízení, agent a nastavení zůstávají seskupené jako jedna pracovní lišta.",
  "nav.basicTitle": "Nápověda pro základní režim",
  "nav.basicBody":
    "Základní režim drží viditelnou cestu k prvnímu úspěchu a zbytek schovává za explicitní přepnutí zpět na plný operátorský povrch.",
  "nav.switchAdvanced": "Přepnout na Pokročilý",
  "nav.switchBasic": "Přepnout na Základní",
  "nav.group.chat": "Chat",
  "nav.group.control": "Observabilita",
  "nav.group.operations": "Řízení",
  "nav.group.agent": "Agent",
  "nav.group.settings": "Nastavení",
  "section.overview.label": "Přehled",
  "section.overview.detail": "Operátorský postoj, rizika a další kroky",
  "section.chat.label": "Chat",
  "section.chat.detail": "Primární operátorský workspace",
  "section.canvas.label": "Canvas",
  "section.canvas.detail": "Rich surface navázaná na relaci a historii",
  "section.sessions.label": "Relace",
  "section.sessions.detail": "Katalog, lifecycle a stav běhů",
  "section.usage.label": "Využití",
  "section.usage.detail": "Kapacita, tokeny, latence a nákladový postoj",
  "section.logs.label": "Logy",
  "section.logs.detail": "Sjednocený runtime stream napříč palyrad, browserd a kanály",
  "section.inventory.label": "Inventář",
  "section.inventory.detail": "Nody, zařízení, čekající párování a runtime instance",
  "section.approvals.label": "Schválení",
  "section.approvals.detail": "Brána pro citlivé akce",
  "section.cron.label": "Automatizace",
  "section.cron.detail": "Heartbeaty, standing orders, programy, rutiny, běhy, šablony a schválení",
  "section.channels.label": "Kanály",
  "section.channels.detail": "Konektory, router a nastavení Discordu",
  "section.browser.label": "Prohlížeč",
  "section.browser.detail": "Profily, relay a stažené soubory",
  "section.agents.label": "Agenti",
  "section.agents.detail": "Registr, výchozí nastavení a setup workspace",
  "section.memory.label": "Paměť",
  "section.memory.detail": "Retence, hledání a purge",
  "section.skills.label": "Skills",
  "section.skills.detail": "Instalace a runtime trust posture",
  "section.auth.label": "Profily",
  "section.auth.detail": "Auth profily providerů a jejich health posture",
  "section.config.label": "Konfigurace",
  "section.config.detail": "Prohlížení, validace, změny a obnova konfigurace",
  "section.secrets.label": "Tajné údaje",
  "section.secrets.detail": "Vault metadata tajných údajů a reveal flow",
  "section.access.label": "Přístup",
  "section.access.detail": "Lifecycle párování a postura vzdáleného přístupu",
  "section.operations.label": "Diagnostika",
  "section.operations.detail": "Runtime snapshoty, audit a CLI handoffy",
  "section.support.label": "Podpora",
  "section.support.detail": "Bundle, obnova a operátorské předání",
  "guidance.nextAction": "Další krok",
  "guidance.checklist": "Onboardingový checklist",
  "guidance.troubleshooting": "Řešení problémů",
  "guidance.scenario": "Scénář",
  "guidance.cta": "Otevřít",
  "overview.telemetryTitle": "Aktivační baseline",
  "overview.telemetryBody":
    "Metriky baseline z fáze 1 zůstávají auditně podložené přes systémové události místo samostatné pipeline analytického dodavatele.",
  "overview.telemetryFunnel": "Průchod trychtýřem",
  "overview.telemetryApprovals": "Únava ze schvalování",
  "overview.telemetryFriction": "Největší třecí plocha",
  "overview.telemetryEmpty": "Zatím nejsou zaznamenány žádné UX baseline události.",
  "overview.modeGuidanceTitle": "Doporučení k režimu",
  "overview.modeGuidanceBody":
    "Základní režim zužuje pozornost na plochy pro první úspěch. Pokročilý režim okamžitě obnoví plnou operátorskou lištu bez změny backendových schopností.",
  "mode.basic.description": "Chat, přehled, relace, schválení, přístup a podpora zůstávají hlavní.",
  "mode.advanced.description":
    "Všechny operátorské sekce zůstávají viditelné přímo v postranním panelu.",
};

export const CONSOLE_LOCALES: readonly ConsoleLocale[] = ["en", "cs", "qps-ploc"] as const;

export function readStoredConsoleLocale(): ConsoleLocale {
  if (typeof window === "undefined") {
    return "en";
  }
  const stored = window.localStorage.getItem(CONSOLE_LOCALE_STORAGE_KEY);
  if (stored === "cs") {
    return "cs";
  }
  return stored === "qps-ploc" ? "qps-ploc" : "en";
}

export function nextConsoleLocale(current: ConsoleLocale): ConsoleLocale {
  const index = CONSOLE_LOCALES.indexOf(current);
  return CONSOLE_LOCALES[(index + 1) % CONSOLE_LOCALES.length] ?? "en";
}

export function translateConsoleMessage(
  locale: ConsoleLocale,
  key: ConsoleMessageKey,
  variables?: Record<string, string | number>,
): string {
  const template = (locale === "cs" ? CS_MESSAGES : EN_MESSAGES)[key];
  const resolved = variables === undefined ? template : interpolate(template, variables);
  return locale === "qps-ploc" ? pseudoLocalizeText(resolved) : resolved;
}

export function describeConsoleMode(locale: ConsoleLocale, mode: ConsoleUiMode): string {
  return translateConsoleMessage(locale, mode === "basic" ? "shell.basic" : "shell.advanced");
}

export function describeConsoleLocale(locale: ConsoleLocale): string {
  switch (locale) {
    case "cs":
      return translateConsoleMessage(locale, "shell.czech");
    case "qps-ploc":
      return translateConsoleMessage(locale, "shell.pseudo");
    default:
      return translateConsoleMessage(locale, "shell.english");
  }
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

export function pseudoLocalizeText(value: string): string {
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
