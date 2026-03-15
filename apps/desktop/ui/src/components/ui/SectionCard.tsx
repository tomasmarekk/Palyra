import { Card, CardContent, CardHeader } from "@heroui/react";
import type { PropsWithChildren, ReactNode } from "react";

import { joinClassNames } from "./utils";

type SectionCardProps = PropsWithChildren<{
  title: string;
  description?: string;
  eyebrow?: string;
  actions?: ReactNode;
  footer?: ReactNode;
  className?: string;
}>;

export function SectionCard({
  title,
  description,
  eyebrow,
  actions,
  footer,
  className,
  children
}: SectionCardProps) {
  return (
    <Card className={joinClassNames("desktop-surface", className)}>
      <CardHeader className="desktop-section-card__header">
        <div className="desktop-section-card__copy">
          {eyebrow !== undefined && <p className="desktop-eyebrow">{eyebrow}</p>}
          <div className="desktop-section-card__title-block">
            <h2>{title}</h2>
            {description !== undefined && <p className="desktop-muted">{description}</p>}
          </div>
        </div>
        {actions !== undefined && <div className="desktop-section-card__actions">{actions}</div>}
      </CardHeader>
      <CardContent className="desktop-section-card__body">
        {children}
        {footer !== undefined && <div className="desktop-section-card__footer">{footer}</div>}
      </CardContent>
    </Card>
  );
}
