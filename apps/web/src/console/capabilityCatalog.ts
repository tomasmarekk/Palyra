import type { CapabilityCatalog, CapabilityEntry } from "../consoleApi";

import { CONSOLE_SECTIONS, type Section } from "./sectionMetadata";

export type CapabilityExposureMode =
  | "direct_action"
  | "cli_handoff"
  | "internal_only";

const SECTION_FALLBACK_BY_DOMAIN: Record<string, Section> = {
  approvals: "approvals",
  audit: "operations",
  auth: "auth",
  browser: "browser",
  channels: "channels",
  chat: "chat",
  config: "config",
  cron: "cron",
  deployment: "access",
  memory: "memory",
  pairing: "access",
  protocol: "operations",
  runtime: "operations",
  secrets: "secrets",
  skills: "skills",
  support: "support"
};

export function normalizeCapabilityExposureMode(
  entry: CapabilityEntry
): CapabilityExposureMode {
  if (entry.dashboard_exposure === "direct_action") {
    return "direct_action";
  }
  if (entry.dashboard_exposure === "cli_handoff" || entry.dashboard_exposure === "internal_only") {
    return entry.dashboard_exposure;
  }
  if (entry.execution_mode === "read_only_cli_handoff" || entry.execution_mode === "generated_cli") {
    return "cli_handoff";
  }
  if (entry.execution_mode === "internal_only" || entry.execution_mode === "internal") {
    return "internal_only";
  }
  return "direct_action";
}

export function capabilityExecutionLabel(mode: CapabilityExposureMode): string {
  if (mode === "cli_handoff") {
    return "CLI handoff";
  }
  if (mode === "internal_only") {
    return "Internal only";
  }
  return "Direct action";
}

export function capabilitySection(entry: CapabilityEntry): Section {
  const declared = entry.dashboard_section as Section;
  if (CONSOLE_SECTIONS.some((section) => section.id === declared)) {
    return declared;
  }
  return SECTION_FALLBACK_BY_DOMAIN[entry.domain] ?? "overview";
}

export function capabilitiesForSection(
  catalog: CapabilityCatalog | null | undefined,
  section: Section
): CapabilityEntry[] {
  if (catalog === null || catalog === undefined) {
    return [];
  }
  return catalog.capabilities.filter((entry) => capabilitySection(entry) === section);
}

export function capabilityModeCounts(entries: CapabilityEntry[]): Record<CapabilityExposureMode, number> {
  return entries.reduce<Record<CapabilityExposureMode, number>>(
    (counts, entry) => {
      counts[normalizeCapabilityExposureMode(entry)] += 1;
      return counts;
    },
    {
      direct_action: 0,
      cli_handoff: 0,
      internal_only: 0
    }
  );
}

export function sectionCapabilityCounts(
  catalog: CapabilityCatalog | null | undefined
): Array<{
  section: Section;
  label: string;
  counts: Record<CapabilityExposureMode, number>;
}> {
  return CONSOLE_SECTIONS.map((section) => ({
    section: section.id,
    label: section.label,
    counts: capabilityModeCounts(capabilitiesForSection(catalog, section.id))
  }));
}

export function capabilitiesByMode(
  entries: CapabilityEntry[]
): Record<CapabilityExposureMode, CapabilityEntry[]> {
  const grouped: Record<CapabilityExposureMode, CapabilityEntry[]> = {
    direct_action: [],
    cli_handoff: [],
    internal_only: []
  };
  for (const entry of entries) {
    grouped[normalizeCapabilityExposureMode(entry)].push(entry);
  }
  return grouped;
}
