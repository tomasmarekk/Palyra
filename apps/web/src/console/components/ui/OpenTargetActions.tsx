import type { ComponentProps } from "react";

import { ActionButton } from "./fields";
import { StatusChip } from "./StatusChip";
import { joinClassNames } from "./utils";

export type OpenTargetId =
  | "inline-preview"
  | "canvas"
  | "browser-workbench"
  | "desktop-handoff"
  | "external";

export type OpenTargetAction = {
  readonly target: OpenTargetId;
  readonly label?: string;
  readonly description?: string;
  readonly disabled?: boolean;
  readonly variant?: ComponentProps<typeof ActionButton>["variant"];
  readonly onPress: () => void;
};

type OpenTargetActionsProps = {
  readonly actions: readonly OpenTargetAction[];
  readonly compact?: boolean;
  readonly className?: string;
};

const OPEN_TARGET_LABELS: Record<OpenTargetId, string> = {
  "inline-preview": "Inline preview",
  canvas: "Canvas",
  "browser-workbench": "Browser workbench",
  "desktop-handoff": "Desktop handoff",
  external: "External open",
};

const OPEN_TARGET_DESCRIPTIONS: Record<OpenTargetId, string> = {
  "inline-preview": "UI navigation inside the current web surface.",
  canvas: "UI navigation into the richer canvas surface.",
  "browser-workbench": "Local mediation through the retained browser session workbench.",
  "desktop-handoff": "Local mediation through the desktop control center.",
  external: "Explicit open outside the current Palyra surface.",
};

const OPEN_TARGET_TONES: Record<OpenTargetId, ComponentProps<typeof StatusChip>["tone"]> = {
  "inline-preview": "default",
  canvas: "success",
  "browser-workbench": "accent",
  "desktop-handoff": "accent",
  external: "warning",
};

export function OpenTargetActions({ actions, compact = false, className }: OpenTargetActionsProps) {
  if (actions.length === 0) {
    return null;
  }

  return (
    <div
      className={joinClassNames(
        "open-target-actions",
        compact && "open-target-actions--compact",
        className,
      )}
    >
      {actions.map((action) => {
        const label = action.label ?? `Open ${OPEN_TARGET_LABELS[action.target]}`;
        const description = action.description ?? OPEN_TARGET_DESCRIPTIONS[action.target];
        return (
          <div key={`${action.target}:${label}`} className="open-target-actions__item">
            <div className="workspace-inline-actions">
              <ActionButton
                isDisabled={action.disabled}
                size="sm"
                type="button"
                variant={action.variant ?? "secondary"}
                onPress={action.onPress}
              >
                {label}
              </ActionButton>
              <StatusChip tone={OPEN_TARGET_TONES[action.target]}>
                {OPEN_TARGET_LABELS[action.target]}
              </StatusChip>
            </div>
            {!compact ? <p className="chat-muted">{description}</p> : null}
          </div>
        );
      })}
    </div>
  );
}
