import { Card, CardContent } from "@heroui/react";
import type { ReactNode } from "react";

type PageHeaderProps = {
  eyebrow?: string;
  title: string;
  headingLabel?: string;
  description: string;
  status?: ReactNode;
  actions?: ReactNode;
};

export function PageHeader({
  eyebrow,
  title,
  headingLabel,
  description,
  status,
  actions
}: PageHeaderProps) {
  return (
    <Card className="border border-white/25 bg-white/75 shadow-xl shadow-slate-900/5 backdrop-blur-xl dark:border-white/10 dark:bg-slate-950/60">
      <CardContent className="workspace-page-header">
        <div className="workspace-page-header__copy">
          {eyebrow !== undefined && <p className="console-label">{eyebrow}</p>}
          <div className="workspace-page-header__title-block">
            <h2 aria-label={headingLabel}>{title}</h2>
            <p className="console-copy">{description}</p>
          </div>
          {status !== undefined && <div className="workspace-chip-row">{status}</div>}
        </div>
        {actions !== undefined && <div className="workspace-page-header__actions">{actions}</div>}
      </CardContent>
    </Card>
  );
}
