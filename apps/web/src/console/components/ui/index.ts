import type { UiTone } from "./utils";

export { ConfirmActionDialog } from "./ConfirmActionDialog";
export { EmptyState } from "./EmptyState";
export { EntityTable } from "./EntityTable";
export type { EntityTableColumn } from "./EntityTable";
export {
  ActionButton,
  ActionCluster,
  AppForm,
  CheckboxField,
  SelectField,
  SwitchField,
  TextAreaField,
  TextInputField
} from "./fields";
export { InlineNotice } from "./InlineNotice";
export { KeyValueList } from "./KeyValueList";
export { MetricCard } from "./MetricCard";
export { PageHeader } from "./PageHeader";
export { RedactedValue } from "./RedactedValue";
export { SectionCard } from "./SectionCard";
export { StatusChip } from "./StatusChip";
export { joinClassNames } from "./utils";
export type { KeyValueItem, UiTone } from "./utils";
export { resolveToneColor } from "./utils";

export function workspaceToneForState(state: string | null | undefined): UiTone {
  const normalized = state?.trim().toLowerCase() ?? "";

  if (
    normalized === "healthy" ||
    normalized === "running" ||
    normalized === "active" ||
    normalized === "ready" ||
    normalized === "ok" ||
    normalized === "success" ||
    normalized === "succeeded" ||
    normalized === "enabled" ||
    normalized === "paired" ||
    normalized === "connected" ||
    normalized === "static"
  ) {
    return "success";
  }

  if (
    normalized === "degraded" ||
    normalized === "warning" ||
    normalized === "expiring" ||
    normalized === "cooldown" ||
    normalized === "not_due" ||
    normalized === "pending" ||
    normalized === "queued"
  ) {
    return "warning";
  }

  if (
    normalized === "down" ||
    normalized === "failed" ||
    normalized === "blocked" ||
    normalized === "error" ||
    normalized === "expired" ||
    normalized === "missing" ||
    normalized === "quarantined"
  ) {
    return "danger";
  }

  if (normalized === "configured" || normalized === "custom") {
    return "accent";
  }

  return "default";
}
