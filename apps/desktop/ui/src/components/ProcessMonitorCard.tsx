import { Card, CardContent, CardHeader, Skeleton } from "@heroui/react";

import type { ServiceProcessSnapshot } from "../lib/desktopApi";
import { fallbackText, formatUnixMs, processTone } from "./desktopPresentation";
import { KeyValueList, SectionCard, StatusChip } from "./ui";

type ProcessMonitorCardProps = {
  gatewayProcess: ServiceProcessSnapshot;
  browserdProcess: ServiceProcessSnapshot;
  loading: boolean;
};

function processItems(process: ServiceProcessSnapshot, loading: boolean) {
  if (loading) {
    return [
      { label: "PID", value: <Skeleton className="desktop-skeleton desktop-skeleton--text" /> },
      {
        label: "Restart attempts",
        value: <Skeleton className="desktop-skeleton desktop-skeleton--text" />
      },
      {
        label: "Bound ports",
        value: <Skeleton className="desktop-skeleton desktop-skeleton--text" />
      },
      { label: "Last start", value: <Skeleton className="desktop-skeleton desktop-skeleton--text" /> },
      { label: "Last exit", value: <Skeleton className="desktop-skeleton desktop-skeleton--text" /> }
    ];
  }

  return [
    { label: "PID", value: process.pid ?? "Unavailable" },
    { label: "Restart attempts", value: process.restart_attempt },
    {
      label: "Bound ports",
      value: process.bound_ports.join(", ") || "None"
    },
    { label: "Last start", value: formatUnixMs(process.last_start_unix_ms) },
    { label: "Last exit", value: fallbackText(process.last_exit) }
  ];
}

type ProcessPaneProps = {
  title: string;
  process: ServiceProcessSnapshot;
  loading: boolean;
};

function ProcessPane({ title, process, loading }: ProcessPaneProps) {
  return (
    <Card className="desktop-subcard">
      <CardHeader className="desktop-subcard__header">
        <div>
          <p className="desktop-label">{process.service}</p>
          <h3>{title}</h3>
        </div>
        <StatusChip tone={processTone(process)}>
          {process.running ? "Running" : "Stopped"}
        </StatusChip>
      </CardHeader>
      <CardContent className="desktop-subcard__body">
        <KeyValueList items={processItems(process, loading)} />
      </CardContent>
    </Card>
  );
}

export function ProcessMonitorCard({
  gatewayProcess,
  browserdProcess,
  loading
}: ProcessMonitorCardProps) {
  return (
    <SectionCard
      eyebrow="Runtime detail"
      title="Process monitor"
      description="Desktop keeps the two supervised processes visible without expanding into a full operations cockpit."
      actions={
        <StatusChip tone={processTone(browserdProcess)}>
          {browserdProcess.running ? "Browserd up" : "Browserd down"}
        </StatusChip>
      }
    >
      <div className="desktop-panel-grid">
        <ProcessPane title="Gateway" process={gatewayProcess} loading={loading} />
        <ProcessPane title="Browserd" process={browserdProcess} loading={loading} />
      </div>
    </SectionCard>
  );
}
