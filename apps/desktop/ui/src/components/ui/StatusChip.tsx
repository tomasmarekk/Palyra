import { Chip } from "@heroui/react";
import type { PropsWithChildren } from "react";

import { resolveToneColor, type UiTone } from "./utils";

type StatusChipProps = PropsWithChildren<{
  tone?: UiTone;
  className?: string;
}>;

export function StatusChip({
  children,
  tone = "default",
  className
}: StatusChipProps) {
  return (
    <Chip className={className} color={resolveToneColor(tone)} variant="soft">
      {children}
    </Chip>
  );
}
