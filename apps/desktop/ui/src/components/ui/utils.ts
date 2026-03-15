import type { ReactNode } from "react";

export type UiTone = "default" | "success" | "warning" | "danger" | "accent";

export type KeyValueItem = {
  label: ReactNode;
  value: ReactNode;
};

export function joinClassNames(...values: Array<string | false | null | undefined>): string {
  return values.filter(Boolean).join(" ");
}

export function resolveToneColor(
  tone: UiTone | undefined
): "default" | "success" | "warning" | "danger" {
  if (tone === undefined || tone === "accent") {
    return "default";
  }

  return tone;
}

export function resolveAlertStatus(
  tone: UiTone | undefined
): "default" | "success" | "warning" | "danger" {
  if (tone === undefined || tone === "accent") {
    return "default";
  }

  return tone;
}
