import { Card, CardContent, CardHeader, Chip } from "@heroui/react";
import type { PropsWithChildren, ReactNode } from "react";

type WorkspacePageHeaderProps = {
  eyebrow?: string;
  title: string;
  headingLabel?: string;
  description: string;
  status?: ReactNode;
  actions?: ReactNode;
};

type WorkspaceSectionCardProps = PropsWithChildren<{
  title: string;
  description?: string;
  actions?: ReactNode;
  className?: string;
}>;

type WorkspaceMetricCardProps = {
  label: string;
  value: ReactNode;
  detail?: string;
  tone?: WorkspaceTone;
};

export type WorkspaceTone = "default" | "success" | "warning" | "danger" | "accent";

function joinClassNames(...values: Array<string | undefined>): string {
  return values.filter((value) => typeof value === "string" && value.length > 0).join(" ");
}

function resolveToneColor(
  tone: WorkspaceTone | undefined
): "default" | "success" | "warning" | "danger" | undefined {
  if (tone === "accent") {
    return "default";
  }
  return tone;
}

export function WorkspacePageHeader({
  eyebrow,
  title,
  headingLabel,
  description,
  status,
  actions
}: WorkspacePageHeaderProps) {
  return (
    <header className="workspace-page-header">
      <div className="workspace-page-header__copy">
        {eyebrow !== undefined && <p className="console-label">{eyebrow}</p>}
        <div className="workspace-page-header__title-block">
          <h2 aria-label={headingLabel}>{title}</h2>
          <p className="console-copy">{description}</p>
        </div>
        {status !== undefined && <div className="workspace-chip-row">{status}</div>}
      </div>
      {actions !== undefined && <div className="workspace-page-header__actions">{actions}</div>}
    </header>
  );
}

export function WorkspaceSectionCard({
  title,
  description,
  actions,
  className,
  children
}: WorkspaceSectionCardProps) {
  return (
    <Card
      className={joinClassNames(
        "border border-white/25 bg-white/70 shadow-xl shadow-slate-900/5 backdrop-blur-xl dark:border-white/10 dark:bg-slate-950/60",
        className
      )}
    >
      <CardHeader className="workspace-section-card__header">
        <div className="workspace-section-card__copy">
          <h3>{title}</h3>
          {description !== undefined && <p className="chat-muted">{description}</p>}
        </div>
        {actions !== undefined && <div className="workspace-section-card__actions">{actions}</div>}
      </CardHeader>
      <CardContent className="workspace-section-card__body">{children}</CardContent>
    </Card>
  );
}

export function WorkspaceMetricCard({
  label,
  value,
  detail,
  tone = "default"
}: WorkspaceMetricCardProps) {
  return (
    <WorkspaceSectionCard
      title={label}
      className="workspace-metric-card"
      actions={<WorkspaceStatusChip tone={tone}>{label}</WorkspaceStatusChip>}
    >
      <div className="workspace-metric-card__value">{value}</div>
      {detail !== undefined && <p className="chat-muted">{detail}</p>}
    </WorkspaceSectionCard>
  );
}

export function WorkspaceStatusChip({
  children,
  tone = "default"
}: PropsWithChildren<{ tone?: WorkspaceTone }>) {
  return (
    <Chip color={resolveToneColor(tone)} variant="soft">
      {children}
    </Chip>
  );
}
