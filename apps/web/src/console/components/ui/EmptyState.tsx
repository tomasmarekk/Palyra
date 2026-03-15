import { Button } from "@heroui/react";
import type { ReactNode } from "react";

import { SectionCard } from "./SectionCard";
import { joinClassNames } from "./utils";

type EmptyStateProps = {
  title: string;
  description: string;
  action?: ReactNode;
  compact?: boolean;
};

export function EmptyState({
  title,
  description,
  action,
  compact = false
}: EmptyStateProps) {
  return (
    <SectionCard
      title={title}
      className={joinClassNames(
        "workspace-empty-state border-dashed shadow-none",
        compact && "workspace-empty-state--compact"
      )}
      description={description}
      actions={
        typeof action === "string" ? (
          <Button size="sm" variant="secondary">
            {action}
          </Button>
        ) : (
          action
        )
      }
    >
      {compact ? null : <p className="workspace-empty">{description}</p>}
    </SectionCard>
  );
}
