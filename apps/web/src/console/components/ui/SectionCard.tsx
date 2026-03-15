import { Card, CardContent, CardHeader } from "@heroui/react";
import type { PropsWithChildren, ReactNode } from "react";

import { joinClassNames } from "./utils";

type SectionCardProps = PropsWithChildren<{
  title: string;
  description?: string;
  actions?: ReactNode;
  className?: string;
  footer?: ReactNode;
}>;

export function SectionCard({
  title,
  description,
  actions,
  className,
  footer,
  children
}: SectionCardProps) {
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
      <CardContent className="workspace-section-card__body">
        {children}
        {footer !== undefined && <div className="workspace-section-card__footer">{footer}</div>}
      </CardContent>
    </Card>
  );
}
