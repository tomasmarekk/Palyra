import { CONSOLE_SECTIONS, type Section } from "./sectionMetadata";

export type NavigationEntry = (typeof CONSOLE_SECTIONS)[number];

export type NavigationGroup = {
  id: "chat" | "control" | "operations" | "agent" | "settings";
  items: readonly NavigationEntry[];
  label: string;
};

const SECTION_LOOKUP: Readonly<Record<Section, NavigationEntry>> = Object.fromEntries(
  CONSOLE_SECTIONS.map((entry) => [entry.id, entry]),
) as Record<Section, NavigationEntry>;
const NAV_GROUP_LABELS = {
  chat: "Chat",
  control: "Observability",
  operations: "Control",
  agent: "Agent",
  settings: "Settings",
} as const;

function resolveEntries(sections: readonly Section[]): readonly NavigationEntry[] {
  return sections.map((section) => SECTION_LOOKUP[section]);
}

export const CONSOLE_NAV_GROUPS: readonly NavigationGroup[] = [
  {
    id: "chat",
    label: "Chat",
    items: resolveEntries(["chat"]),
  },
  {
    id: "control",
    label: "Observability",
    items: resolveEntries([
      "overview",
      "sessions",
      "usage",
      "logs",
      "inventory",
      "support",
    ]),
  },
  {
    id: "operations",
    label: "Control",
    items: resolveEntries([
      "approvals",
      "cron",
      "channels",
      "browser",
    ]),
  },
  {
    id: "agent",
    label: "Agent",
    items: resolveEntries(["agents", "skills", "memory"]),
  },
  {
    id: "settings",
    label: "Settings",
    items: resolveEntries(["auth", "access", "config", "secrets", "operations"]),
  },
] as const;

const SECTION_PATHS: Readonly<Record<Section, string>> = {
  overview: "/control/overview",
  chat: "/chat",
  sessions: "/control/sessions",
  usage: "/control/usage",
  logs: "/control/logs",
  inventory: "/control/inventory",
  approvals: "/control/approvals",
  cron: "/control/automation",
  channels: "/control/channels",
  browser: "/control/browser",
  agents: "/agent/agents",
  memory: "/agent/memory",
  skills: "/agent/skills",
  auth: "/settings/profiles",
  config: "/settings/config",
  secrets: "/settings/secrets",
  access: "/settings/access",
  operations: "/settings/diagnostics",
  support: "/control/support",
};
const SECTION_PATH_ALIASES: Readonly<Record<string, Section>> = {
  "/control": "overview",
};
const SECTION_GROUPS: Readonly<Record<Section, NavigationGroup["id"]>> = {
  overview: "control",
  chat: "chat",
  sessions: "control",
  usage: "control",
  logs: "control",
  inventory: "control",
  approvals: "operations",
  cron: "operations",
  channels: "operations",
  browser: "operations",
  agents: "agent",
  memory: "agent",
  skills: "agent",
  auth: "settings",
  config: "settings",
  secrets: "settings",
  access: "settings",
  operations: "settings",
  support: "control",
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
