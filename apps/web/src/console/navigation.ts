export const CONSOLE_SECTIONS = [
  { id: "overview", label: "Overview", detail: "Product posture and capability exposure map" },
  { id: "chat", label: "Chat", detail: "Primary operator workspace" },
  { id: "approvals", label: "Approvals", detail: "Sensitive action gate" },
  { id: "cron", label: "Automation", detail: "Scheduled jobs, runs, and history" },
  { id: "channels", label: "Channels", detail: "Connectors, router, and Discord setup" },
  { id: "browser", label: "Browser", detail: "Profiles, relay, and downloads" },
  { id: "memory", label: "Memory", detail: "Retention, search, and purge" },
  { id: "skills", label: "Skills", detail: "Install and runtime trust posture" },
  { id: "auth", label: "OpenAI and Auth Profiles", detail: "OpenAI and auth profile state" },
  { id: "config", label: "Config and Secrets", detail: "Config lifecycle and secrets controls" },
  { id: "access", label: "Pairing and Gateway Access", detail: "Pairing lifecycle and remote access posture" },
  { id: "operations", label: "Diagnostics and Audit", detail: "Runtime snapshots, audit, and CLI handoffs" },
  { id: "support", label: "Support", detail: "Bundles, recovery, and operator handoff" }
] as const;

export type Section = (typeof CONSOLE_SECTIONS)[number]["id"];
export type NavigationEntry = (typeof CONSOLE_SECTIONS)[number];

export type NavigationGroup = {
  id: "chat" | "control" | "agent" | "settings";
  items: readonly NavigationEntry[];
  label: string;
};

const SECTION_LOOKUP: Readonly<Record<Section, NavigationEntry>> = Object.fromEntries(
  CONSOLE_SECTIONS.map((entry) => [entry.id, entry])
) as Record<Section, NavigationEntry>;
const NAV_GROUP_LABELS = {
  chat: "Chat",
  control: "Control",
  agent: "Agent",
  settings: "Settings"
} as const;

function resolveEntries(sections: readonly Section[]): readonly NavigationEntry[] {
  return sections.map((section) => SECTION_LOOKUP[section]);
}

export const CONSOLE_NAV_GROUPS: readonly NavigationGroup[] = [
  {
    id: "chat",
    label: "Chat",
    items: resolveEntries(["chat"])
  },
  {
    id: "control",
    label: "Control",
    items: resolveEntries(["overview", "approvals", "cron", "channels", "browser", "support"])
  },
  {
    id: "agent",
    label: "Agent",
    items: resolveEntries(["skills", "memory"])
  },
  {
    id: "settings",
    label: "Settings",
    items: resolveEntries(["auth", "config", "access", "operations"])
  }
] as const;

const SECTION_PATHS: Readonly<Record<Section, string>> = {
  overview: "/control/overview",
  chat: "/chat",
  approvals: "/control/approvals",
  cron: "/control/automation",
  channels: "/control/channels",
  browser: "/control/browser",
  memory: "/agent/memory",
  skills: "/agent/skills",
  auth: "/settings/profiles",
  config: "/settings/config",
  access: "/settings/access",
  operations: "/settings/diagnostics",
  support: "/control/support"
};
const SECTION_PATH_ALIASES: Readonly<Record<string, Section>> = {
  "/control": "overview"
};
const SECTION_GROUPS: Readonly<Record<Section, NavigationGroup["id"]>> = {
  overview: "control",
  chat: "chat",
  approvals: "control",
  cron: "control",
  channels: "control",
  browser: "control",
  memory: "agent",
  skills: "agent",
  auth: "settings",
  config: "settings",
  access: "settings",
  operations: "settings",
  support: "control"
};

export function getSectionPath(section: Section): string {
  return SECTION_PATHS[section];
}

export function getNavigationEntry(section: Section): NavigationEntry {
  return SECTION_LOOKUP[section];
}

export function getNavigationGroupLabel(section: Section): string {
  return NAV_GROUP_LABELS[SECTION_GROUPS[section]];
}

export function findSectionByPath(pathname: string): Section | null {
  const normalizedPath =
    pathname.endsWith("/") && pathname.length > 1 ? pathname.slice(0, -1) : pathname;
  const alias = SECTION_PATH_ALIASES[normalizedPath];

  if (alias !== undefined) {
    return alias;
  }

  for (const [section, path] of Object.entries(SECTION_PATHS) as Array<[Section, string]>) {
    if (path === normalizedPath) {
      return section;
    }
  }

  return null;
}
