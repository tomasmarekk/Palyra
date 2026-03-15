import type { ReactNode } from "react";

import { SectionCard } from "./SectionCard";
import { StatusChip } from "./StatusChip";
import type { UiTone } from "./utils";

type MetricCardProps = {
  label: string;
  value: ReactNode;
  detail?: string;
  tone?: UiTone;
};

export function MetricCard({
  label,
  value,
  detail,
  tone = "default"
}: MetricCardProps) {
  return (
    <SectionCard
      title={label}
      className="workspace-metric-card"
      actions={<StatusChip tone={tone}>{label}</StatusChip>}
    >
      <div className="workspace-metric-card__value">{value}</div>
      {detail !== undefined && <p className="chat-muted">{detail}</p>}
    </SectionCard>
  );
}
