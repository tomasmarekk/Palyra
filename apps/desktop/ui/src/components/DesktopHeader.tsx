import { Skeleton } from "@heroui/react";

import { formatDesktopDateTime, translateDesktopMessage } from "../i18n";
import type { ControlCenterSnapshot } from "../lib/desktopApi";
import type { DesktopLocale } from "../preferences";
import { overallTone } from "./desktopPresentation";
import { KeyValueList, PageHeader, StatusChip } from "./ui";

type DesktopHeaderProps = {
  locale: DesktopLocale;
  snapshot: ControlCenterSnapshot;
  loading: boolean;
};

export function DesktopHeader({ locale, snapshot, loading }: DesktopHeaderProps) {
  const t = (
    key: Parameters<typeof translateDesktopMessage>[1],
    variables?: Record<string, string | number>,
  ) => translateDesktopMessage(locale, key, variables);
  return (
    <PageHeader
      eyebrow={t("desktop.controlCenter.eyebrow")}
      title={t("desktop.controlCenter.title")}
      description={t("desktop.controlCenter.description")}
      status={
        <>
          <StatusChip tone={overallTone(snapshot.overall_status)}>
            {snapshot.overall_status}
          </StatusChip>
          <StatusChip tone="accent">{snapshot.quick_facts.dashboard_access_mode}</StatusChip>
          <StatusChip tone={loading ? "warning" : "success"}>
            {loading
              ? t("desktop.controlCenter.snapshotRefreshing")
              : t("desktop.controlCenter.snapshotReady")}
          </StatusChip>
        </>
      }
      actions={
        <div className="desktop-header-summary">
          <KeyValueList
            items={[
              {
                label: t("desktop.controlCenter.overallState"),
                value: loading ? (
                  <Skeleton className="desktop-skeleton desktop-skeleton--chip" />
                ) : (
                  <StatusChip tone={overallTone(snapshot.overall_status)}>
                    {snapshot.overall_status}
                  </StatusChip>
                ),
              },
              {
                label: t("desktop.controlCenter.dashboardMode"),
                value: loading ? (
                  <Skeleton className="desktop-skeleton desktop-skeleton--text" />
                ) : (
                  <strong>{snapshot.quick_facts.dashboard_access_mode}</strong>
                ),
              },
              {
                label: t("desktop.controlCenter.lastSnapshot"),
                value: loading ? (
                  <Skeleton className="desktop-skeleton desktop-skeleton--text" />
                ) : (
                  <strong>{formatDesktopDateTime(locale, snapshot.generated_at_unix_ms)}</strong>
                ),
              },
            ]}
          />
        </div>
      }
    />
  );
}
