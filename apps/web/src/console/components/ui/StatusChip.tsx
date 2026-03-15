import { Chip } from "@heroui/react";
import type { PropsWithChildren } from "react";

import { resolveToneColor, type UiTone } from "./utils";

type StatusChipProps = PropsWithChildren<{
  tone?: UiTone;
}>;

export function StatusChip({
  children,
  tone = "default"
}: StatusChipProps) {
  return (
    <Chip color={resolveToneColor(tone)} variant="soft">
      {children}
    </Chip>
  );
}
