import type { ConsoleMessageKey } from "./i18n";
import type { ConsoleUiMode } from "./preferences";

type ConsoleSectionDefinition = {
  id: string;
  label: string;
  detail: string;
  labelKey: ConsoleMessageKey;
  detailKey: ConsoleMessageKey;
  modes: readonly ConsoleUiMode[];
};

export const CONSOLE_SECTIONS = [
  {
    id: "overview",
    label: "Overview",
    detail: "Operational posture, risks, and next actions",
    labelKey: "section.overview.label",
    detailKey: "section.overview.detail",
    modes: ["basic", "advanced"],
  },
  {
    id: "chat",
    label: "Chat",
    detail: "Primary operator workspace",
    labelKey: "section.chat.label",
    detailKey: "section.chat.detail",
    modes: ["basic", "advanced"],
  },
  {
    id: "sessions",
    label: "Sessions",
    detail: "Catalog, lifecycle, and run posture",
    labelKey: "section.sessions.label",
    detailKey: "section.sessions.detail",
    modes: ["basic", "advanced"],
  },
  {
    id: "usage",
    label: "Usage",
    detail: "Capacity, tokens, latency, and cost posture",
    labelKey: "section.usage.label",
    detailKey: "section.usage.detail",
    modes: ["advanced"],
  },
  {
    id: "logs",
    label: "Logs",
    detail: "Unified runtime stream across palyrad, browserd, and channels",
    labelKey: "section.logs.label",
    detailKey: "section.logs.detail",
    modes: ["advanced"],
  },
  {
    id: "inventory",
    label: "Inventory",
    detail: "Nodes, devices, pending pairings, and runtime instances",
    labelKey: "section.inventory.label",
    detailKey: "section.inventory.detail",
    modes: ["advanced"],
  },
  {
    id: "approvals",
    label: "Approvals",
    detail: "Sensitive action gate",
    labelKey: "section.approvals.label",
    detailKey: "section.approvals.detail",
    modes: ["basic", "advanced"],
  },
  {
    id: "cron",
    label: "Automations",
    detail: "Heartbeats, standing orders, programs, routines, runs, templates, and approvals",
    labelKey: "section.cron.label",
    detailKey: "section.cron.detail",
    modes: ["advanced"],
  },
  {
    id: "channels",
    label: "Channels",
    detail: "Connectors, router, and Discord setup",
    labelKey: "section.channels.label",
    detailKey: "section.channels.detail",
    modes: ["advanced"],
  },
  {
    id: "browser",
    label: "Browser",
    detail: "Profiles, relay, and downloads",
    labelKey: "section.browser.label",
    detailKey: "section.browser.detail",
    modes: ["advanced"],
  },
  {
    id: "agents",
    label: "Agents",
    detail: "Registry, defaults, and workspace setup",
    labelKey: "section.agents.label",
    detailKey: "section.agents.detail",
    modes: ["advanced"],
  },
  {
    id: "memory",
    label: "Memory",
    detail: "Retention, search, and purge",
    labelKey: "section.memory.label",
    detailKey: "section.memory.detail",
    modes: ["advanced"],
  },
  {
    id: "skills",
    label: "Skills",
    detail: "Install and runtime trust posture",
    labelKey: "section.skills.label",
    detailKey: "section.skills.detail",
    modes: ["advanced"],
  },
  {
    id: "auth",
    label: "Profiles",
    detail: "Provider auth profiles and health posture",
    labelKey: "section.auth.label",
    detailKey: "section.auth.detail",
    modes: ["advanced"],
  },
  {
    id: "config",
    label: "Config",
    detail: "Inspect, validate, mutate, and recover config",
    labelKey: "section.config.label",
    detailKey: "section.config.detail",
    modes: ["advanced"],
  },
  {
    id: "secrets",
    label: "Secrets",
    detail: "Vault-backed secret metadata and reveal flows",
    labelKey: "section.secrets.label",
    detailKey: "section.secrets.detail",
    modes: ["advanced"],
  },
  {
    id: "access",
    label: "Access",
    detail: "Pairing lifecycle and remote access posture",
    labelKey: "section.access.label",
    detailKey: "section.access.detail",
    modes: ["basic", "advanced"],
  },
  {
    id: "operations",
    label: "Diagnostics",
    detail: "Runtime snapshots, audit, and CLI handoffs",
    labelKey: "section.operations.label",
    detailKey: "section.operations.detail",
    modes: ["advanced"],
  },
  {
    id: "support",
    label: "Support",
    detail: "Bundles, recovery, and operator handoff",
    labelKey: "section.support.label",
    detailKey: "section.support.detail",
    modes: ["basic", "advanced"],
  },
] as const satisfies readonly ConsoleSectionDefinition[];

export type Section = (typeof CONSOLE_SECTIONS)[number]["id"];
export type ConsoleSectionMetadata = (typeof CONSOLE_SECTIONS)[number];
export type ConsoleSectionMessageKey = ConsoleMessageKey;

const SECTION_LOOKUP: Readonly<Record<Section, ConsoleSectionMetadata>> = Object.fromEntries(
  CONSOLE_SECTIONS.map((entry) => [entry.id, entry]),
) as Record<Section, ConsoleSectionMetadata>;

export const BASIC_MODE_SECTION_IDS = CONSOLE_SECTIONS.filter((entry) =>
  (entry.modes as readonly ConsoleUiMode[]).includes("basic"),
).map((entry) => entry.id) as readonly Section[];

export function getConsoleSection(section: Section): ConsoleSectionMetadata {
  return SECTION_LOOKUP[section];
}

export function isSectionVisibleInMode(section: Section, mode: ConsoleUiMode): boolean {
  return (SECTION_LOOKUP[section].modes as readonly ConsoleUiMode[]).includes(mode);
}
