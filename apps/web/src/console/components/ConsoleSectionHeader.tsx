import type { ReactNode } from "react";

type ConsoleSectionHeaderProps = {
  title: string;
  description?: string;
  actions?: ReactNode;
};

export function ConsoleSectionHeader({ title, description, actions }: ConsoleSectionHeaderProps) {
  return (
    <header className="console-card__header">
      <div>
        <h2>{title}</h2>
        {description !== undefined && <p className="console-copy">{description}</p>}
      </div>
      {actions}
    </header>
  );
}
