import type { ReactNode } from "react";

import { SectionCard } from "./SectionCard";
import { joinClassNames } from "./utils";

type EmptyStateProps = {
  title: string;
  description: string;
  action?: ReactNode;
  compact?: boolean;
  className?: string;
};

export function EmptyState({
  title,
  description,
  action,
  compact = false,
  className
}: EmptyStateProps) {
  return (
    <SectionCard
      title={title}
      description={compact ? undefined : description}
      actions={action}
      className={joinClassNames(
        "desktop-empty-state",
        compact && "desktop-empty-state--compact",
        className
      )}
    >
      <p className="desktop-muted">{description}</p>
    </SectionCard>
  );
}
