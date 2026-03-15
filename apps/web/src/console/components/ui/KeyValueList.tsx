import type { ReactNode } from "react";

import { joinClassNames, type KeyValueItem } from "./utils";

type KeyValueListProps = {
  items: readonly KeyValueItem[];
  className?: string;
  emptyState?: ReactNode;
};

export function KeyValueList({ items, className, emptyState }: KeyValueListProps) {
  if (items.length === 0) {
    return emptyState ?? null;
  }

  return (
    <dl className={joinClassNames("workspace-key-value-grid", className)}>
      {items.map((item, index) => (
        <div key={`key-value-item-${index}`}>
          <dt>{item.label}</dt>
          <dd>{item.value}</dd>
        </div>
      ))}
    </dl>
  );
}
