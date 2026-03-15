import type { ReactNode } from "react";

import { SectionCard } from "./SectionCard";
import { StatusChip } from "./StatusChip";
import { joinClassNames, type UiTone } from "./utils";

type MetricCardProps = {
  label: string;
  value: ReactNode;
  detail?: ReactNode;
  tone?: UiTone;
  className?: string;
};

export function MetricCard({
  label,
  value,
  detail,
  tone = "default",
  className
}: MetricCardProps) {
  return (
    <SectionCard
      title={label}
      actions={<StatusChip tone={tone}>{label}</StatusChip>}
      className={joinClassNames("desktop-metric-card", className)}
    >
      <div className="desktop-metric-card__value">{value}</div>
      {detail !== undefined && <div className="desktop-metric-card__detail">{detail}</div>}
    </SectionCard>
  );
}
