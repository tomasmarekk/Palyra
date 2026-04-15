import type { SessionCatalogRecord } from "./desktopApi";

type DesktopStatusTone = "default" | "success" | "warning" | "danger" | "accent";

type DesktopSessionBadge = {
  label: string;
  tone: DesktopStatusTone;
};

type DesktopSessionDetailItem = {
  label: string;
  value: string;
};

export function buildDesktopSessionListBadges(
  session: SessionCatalogRecord,
): DesktopSessionBadge[] {
  const badges: DesktopSessionBadge[] = [];
  if (session.manual_title_locked) {
    badges.push({ label: "manual title", tone: "accent" });
  }
  if (session.family.family_size > 1) {
    badges.push({
      label: `family ${session.family.sequence}/${session.family.family_size}`,
      tone: "default",
    });
  }
  if (session.has_context_files) {
    badges.push({ label: "project rules", tone: "accent" });
  }
  if ((session.recap.project_context?.warnings.length ?? 0) > 0) {
    badges.push({ label: "context warning", tone: "warning" });
  }
  if (session.quick_controls.model.override_active) {
    badges.push({
      label: `model ${session.quick_controls.model.display_value}`,
      tone: "accent",
    });
  }
  return badges.slice(0, 4);
}

export function buildDesktopSessionMeta(session: SessionCatalogRecord): string {
  const parts = [formatDesktopBranchState(session.branch_state)];
  if (session.quick_controls.agent.override_active) {
    parts.push(`agent ${session.quick_controls.agent.display_value}`);
  }
  if (session.quick_controls.trace.override_active) {
    parts.push(`trace ${session.quick_controls.trace.value ? "on" : "off"}`);
  }
  return parts.join(" · ");
}

export function buildDesktopSessionDetailBadges(
  session: SessionCatalogRecord,
): DesktopSessionBadge[] {
  return [
    {
      label: formatDesktopBranchState(session.branch_state),
      tone: session.branch_state === "root" ? "default" : "accent",
    },
    {
      label: session.manual_title_locked ? "manual title" : session.title_generation_state,
      tone: session.manual_title_locked ? "accent" : "default",
    },
    {
      label: `family ${session.family.sequence}/${session.family.family_size}`,
      tone: session.family.family_size > 1 ? "accent" : "default",
    },
    {
      label: session.has_context_files
        ? `${session.recap.project_context?.active_entries ?? (session.recap.active_context_files.length || 1)} project rules`
        : "no context",
      tone: session.has_context_files ? "accent" : "default",
    },
    {
      label: session.quick_controls.thinking.value ? "thinking on" : "thinking off",
      tone: toneForDesktopQuickToggle(session.quick_controls.thinking.value),
    },
    {
      label: session.quick_controls.trace.value ? "trace on" : "trace off",
      tone: toneForDesktopQuickToggle(session.quick_controls.trace.value),
    },
    {
      label: session.quick_controls.verbose.value ? "verbose on" : "verbose off",
      tone: toneForDesktopQuickToggle(session.quick_controls.verbose.value),
    },
  ];
}

export function buildDesktopSessionDetailItems(
  session: SessionCatalogRecord,
): DesktopSessionDetailItem[] {
  return [
    {
      label: "Family root",
      value: session.family.root_title,
    },
    {
      label: "Agent",
      value: `${session.quick_controls.agent.display_value} (${session.quick_controls.agent.source})`,
    },
    {
      label: "Model",
      value: `${session.quick_controls.model.display_value} (${session.quick_controls.model.source})`,
    },
    {
      label: "Context",
      value:
        session.last_context_file ??
        (session.recap.project_context !== undefined
          ? `${session.recap.project_context.active_entries} active deterministic rules`
          : session.has_context_files
            ? "active files attached"
            : "none"),
    },
    {
      label: "Artifacts",
      value: String(session.artifact_count),
    },
    {
      label: "Summary state",
      value: session.last_summary_state,
    },
  ];
}

export function buildDesktopSessionRecap(session: SessionCatalogRecord): string | null {
  const segments: string[] = [];
  if (session.last_summary?.trim()) {
    segments.push(session.last_summary.trim());
  }
  if (session.recap.touched_files.length > 0) {
    segments.push(`Touched files: ${session.recap.touched_files.slice(0, 3).join(", ")}`);
  }
  if (session.recap.active_context_files.length > 0) {
    segments.push(
      `Project rules: ${session.recap.active_context_files.slice(0, 2).join(", ")}`,
    );
  }
  if ((session.recap.project_context?.warnings.length ?? 0) > 0) {
    segments.push(
      `Context warnings: ${session.recap.project_context?.warnings.slice(0, 2).join(", ")}`,
    );
  }
  if (session.recap.ctas.length > 0) {
    segments.push(`Next steps: ${session.recap.ctas.slice(0, 2).join(", ")}`);
  }
  return segments.length > 0 ? segments.join(" ") : null;
}

function formatDesktopBranchState(branchState: string): string {
  switch (branchState) {
    case "root":
      return "root";
    case "active_branch":
    case "branched":
      return "branch";
    case "branch_source":
      return "branch source";
    case "missing":
      return "no lineage";
    default:
      return branchState.replaceAll("_", " ");
  }
}

function toneForDesktopQuickToggle(value: boolean): DesktopStatusTone {
  return value ? "success" : "warning";
}
