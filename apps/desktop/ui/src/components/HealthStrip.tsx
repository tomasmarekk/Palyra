import { Skeleton } from "@heroui/react";

import type { ControlCenterSnapshot } from "../lib/desktopApi";
import {
  attentionTone,
  browserTone,
  formatUnixMs,
  formatUptime,
  overallTone,
  processSummary,
  processTone
} from "./desktopPresentation";
import { MetricCard, StatusChip } from "./ui";

type HealthStripProps = {
  snapshot: ControlCenterSnapshot;
  attentionCount: number;
  loading: boolean;
};

function metricValue(value: string, loading: boolean) {
  if (loading) {
    return <Skeleton className="desktop-skeleton desktop-skeleton--metric" />;
  }

  return value;
}

export function HealthStrip({ snapshot, attentionCount, loading }: HealthStripProps) {
  return (
    <section className="desktop-grid desktop-grid--metrics" aria-label="Runtime health summary">
      <MetricCard
        label="Runtime state"
        tone={overallTone(snapshot.overall_status)}
        value={metricValue(snapshot.overall_status, loading)}
        detail={
          loading ? (
            <Skeleton className="desktop-skeleton desktop-skeleton--detail" />
          ) : (
            <p className="desktop-muted">
              Last snapshot {formatUnixMs(snapshot.generated_at_unix_ms)}
            </p>
          )
        }
      />

      <MetricCard
        label="Gateway process"
        tone={processTone(snapshot.gateway_process)}
        value={metricValue(snapshot.gateway_process.running ? "Running" : "Stopped", loading)}
        detail={
          loading ? (
            <Skeleton className="desktop-skeleton desktop-skeleton--detail" />
          ) : (
            <p className="desktop-muted">{processSummary(snapshot.gateway_process)}</p>
          )
        }
      />

      <MetricCard
        label="Dashboard target"
        tone="accent"
        value={metricValue(snapshot.quick_facts.dashboard_access_mode, loading)}
        detail={
          loading ? (
            <Skeleton className="desktop-skeleton desktop-skeleton--detail" />
          ) : (
            <p className="desktop-muted">Gateway uptime {formatUptime(snapshot.quick_facts.gateway_uptime_seconds)}</p>
          )
        }
      />

      <MetricCard
        label="Browser service"
        tone={browserTone(snapshot.quick_facts.browser_service)}
        value={metricValue(snapshot.quick_facts.browser_service.status, loading)}
        detail={
          loading ? (
            <Skeleton className="desktop-skeleton desktop-skeleton--detail" />
          ) : (
            <div className="desktop-inline-detail">
              <p className="desktop-muted">
                {snapshot.quick_facts.browser_service.enabled ? "Enabled" : "Disabled by config"}
              </p>
              <StatusChip tone={attentionTone(attentionCount)}>
                {attentionCount === 0 ? "diagnostics clear" : `${attentionCount} diagnostics alerts`}
              </StatusChip>
            </div>
          )
        }
      />
    </section>
  );
}
