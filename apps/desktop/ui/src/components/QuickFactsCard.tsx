import { Skeleton } from "@heroui/react";

import type { ControlCenterSnapshot } from "../lib/desktopApi";
import { browserTone, fallbackText, formatUnixMs, formatUptime } from "./desktopPresentation";
import { KeyValueList, SectionCard, StatusChip } from "./ui";

type QuickFactsCardProps = {
  snapshot: ControlCenterSnapshot;
  loading: boolean;
};

function textValue(value: string, loading: boolean, className?: string) {
  if (loading) {
    return <Skeleton className="desktop-skeleton desktop-skeleton--text" />;
  }

  return <span className={className}>{value}</span>;
}

export function QuickFactsCard({ snapshot, loading }: QuickFactsCardProps) {
  return (
    <SectionCard
      eyebrow="Handoff"
      title="Dashboard target"
      description="Desktop stays focused on quick launch and verification, then hands control to the full dashboard."
      actions={<StatusChip tone="accent">{snapshot.quick_facts.dashboard_access_mode}</StatusChip>}
    >
      <div className="desktop-url-block">
        {loading ? (
          <Skeleton className="desktop-skeleton desktop-skeleton--url" />
        ) : (
          <code className="desktop-mono">{snapshot.quick_facts.dashboard_url}</code>
        )}
      </div>

      <KeyValueList
        items={[
          {
            label: "Version",
            value: textValue(snapshot.quick_facts.gateway_version ?? "Unavailable", loading)
          },
          {
            label: "Git hash",
            value: textValue(snapshot.quick_facts.gateway_git_hash ?? "-", loading, "desktop-mono")
          },
          {
            label: "Gateway uptime",
            value: textValue(formatUptime(snapshot.quick_facts.gateway_uptime_seconds), loading)
          },
          {
            label: "Browser health",
            value: loading ? (
              <Skeleton className="desktop-skeleton desktop-skeleton--chip" />
            ) : (
              <StatusChip tone={browserTone(snapshot.quick_facts.browser_service)}>
                {snapshot.quick_facts.browser_service.healthy ? "healthy" : "needs attention"}
              </StatusChip>
            )
          },
          {
            label: "Browser last error",
            value: textValue(fallbackText(snapshot.quick_facts.browser_service.last_error), loading)
          },
          {
            label: "Diagnostics time",
            value: textValue(formatUnixMs(snapshot.diagnostics.generated_at_unix_ms), loading)
          }
        ]}
      />
    </SectionCard>
  );
}
