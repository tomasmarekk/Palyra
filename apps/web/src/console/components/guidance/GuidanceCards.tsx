import type { ReactNode } from "react";

import { ActionButton } from "../ui";
import { WorkspaceSectionCard } from "../workspace/WorkspaceChrome";

type GuidanceCardProps = {
  title: string;
  description: string;
  children?: ReactNode;
  ctaLabel?: string;
  onCta?: () => void;
};

export function NextActionCard({
  title,
  description,
  children,
  ctaLabel,
  onCta,
}: GuidanceCardProps) {
  return (
    <WorkspaceSectionCard title={title} description={description}>
      <div className="grid gap-4">
        {children}
        {ctaLabel !== undefined && onCta !== undefined ? (
          <ActionButton type="button" variant="primary" onPress={onCta}>
            {ctaLabel}
          </ActionButton>
        ) : null}
      </div>
    </WorkspaceSectionCard>
  );
}

export function OnboardingChecklistCard({
  title,
  description,
  items,
}: GuidanceCardProps & { items: readonly string[] }) {
  return (
    <WorkspaceSectionCard title={title} description={description}>
      <ul className="console-compact-list">
        {items.map((item) => (
          <li key={item}>{item}</li>
        ))}
      </ul>
    </WorkspaceSectionCard>
  );
}

export function TroubleshootingCard({
  title,
  description,
  items,
}: GuidanceCardProps & { items: readonly string[] }) {
  return (
    <WorkspaceSectionCard title={title} description={description}>
      <ul className="console-compact-list">
        {items.map((item) => (
          <li key={item}>{item}</li>
        ))}
      </ul>
    </WorkspaceSectionCard>
  );
}

export function ScenarioCard({
  title,
  description,
  children,
  ctaLabel,
  onCta,
}: GuidanceCardProps) {
  return (
    <WorkspaceSectionCard title={title} description={description}>
      <div className="grid gap-4">
        {children}
        {ctaLabel !== undefined && onCta !== undefined ? (
          <ActionButton type="button" variant="secondary" onPress={onCta}>
            {ctaLabel}
          </ActionButton>
        ) : null}
      </div>
    </WorkspaceSectionCard>
  );
}
