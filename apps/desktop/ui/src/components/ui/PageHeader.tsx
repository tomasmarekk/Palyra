import { Card, CardContent } from "@heroui/react";
import type { ReactNode } from "react";

import { joinClassNames } from "./utils";

type PageHeaderProps = {
  eyebrow?: string;
  title: string;
  description: string;
  status?: ReactNode;
  actions?: ReactNode;
  className?: string;
};

export function PageHeader({
  eyebrow,
  title,
  description,
  status,
  actions,
  className
}: PageHeaderProps) {
  return (
    <Card className={joinClassNames("desktop-surface desktop-page-header", className)}>
      <CardContent className="desktop-page-header__content">
        <div className="desktop-page-header__copy">
          {eyebrow !== undefined && <p className="desktop-kicker">{eyebrow}</p>}
          <div className="desktop-page-header__title-block">
            <h1>{title}</h1>
            <p className="desktop-muted">{description}</p>
          </div>
          {status !== undefined && <div className="desktop-chip-row">{status}</div>}
        </div>
        {actions !== undefined && <div className="desktop-page-header__actions">{actions}</div>}
      </CardContent>
    </Card>
  );
}
