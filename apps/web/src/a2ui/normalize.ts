import { DEFAULT_RENDER_INPUT_LIMITS } from "./constants";
import { A2uiError } from "./errors";
import {
  coerceBoolean,
  coerceFiniteNumber,
  coerceString,
  sanitizeIdentifier,
  stringifyJsonValue,
} from "./sanitize";
import type {
  A2uiChartComponent,
  A2uiChartSeriesPoint,
  A2uiComponent,
  A2uiDocument,
  A2uiExperimentAmbientMode,
  A2uiExperimentGovernance,
  A2uiExperimentRolloutStage,
  A2uiFormComponent,
  A2uiFormField,
  A2uiFormFieldType,
  A2uiListComponent,
  A2uiMarkdownComponent,
  A2uiTableComponent,
  A2uiTextComponent,
  JsonValue,
  RenderInputLimits,
} from "./types";
import { isJsonObject } from "./types";

const MAX_EXPERIMENT_CHECKLIST_ITEMS = 6;
const MAX_EXPERIMENT_ENTRY_LENGTH = 160;

export function normalizeA2uiDocument(
  input: unknown,
  limits: RenderInputLimits = DEFAULT_RENDER_INPUT_LIMITS,
): A2uiDocument {
  if (!isJsonObject(input)) {
    throw new A2uiError("invalid_input", "A2UI document must be a JSON object.");
  }

  if (input.v !== 1) {
    throw new A2uiError("invalid_input", "A2UI document must use version v=1.");
  }

  const surface = coerceString(input.surface, "", limits.maxSurfaceLength);
  if (surface.length === 0) {
    throw new A2uiError("invalid_input", "A2UI document surface must be non-empty.");
  }

  if (!Array.isArray(input.components)) {
    throw new A2uiError("invalid_input", "A2UI document components must be an array.");
  }

  const seenComponentIds = new Set<string>();
  const components: A2uiComponent[] = [];
  const boundedComponents = input.components.slice(0, limits.maxComponents);
  for (let index = 0; index < boundedComponents.length; index += 1) {
    const normalized = normalizeComponent(
      boundedComponents[index],
      index,
      limits,
      seenComponentIds,
    );
    if (normalized !== null) {
      components.push(normalized);
    }
  }

  const experimental = normalizeExperimentalGovernance(input.experimental, limits);

  return {
    v: 1,
    surface,
    components,
    ...(experimental === undefined ? {} : { experimental }),
  };
}

function normalizeExperimentalGovernance(
  value: unknown,
  limits: RenderInputLimits,
): A2uiExperimentGovernance | undefined {
  if (value === undefined || value === null) {
    return undefined;
  }
  if (!isJsonObject(value)) {
    throw new A2uiError("invalid_input", "A2UI experimental governance must be a JSON object.");
  }

  const trackIdRaw = coerceString(value.track_id ?? value.trackId, "", 64);
  if (trackIdRaw.length === 0) {
    throw new A2uiError(
      "invalid_input",
      "A2UI experimental governance must include a non-empty track_id.",
    );
  }
  const featureFlag = sanitizeFeatureFlag(value.feature_flag ?? value.featureFlag);
  if (featureFlag.length === 0) {
    throw new A2uiError(
      "invalid_input",
      "A2UI experimental governance must include a non-empty feature_flag.",
    );
  }
  const supportSummary = coerceString(
    value.support_summary ?? value.supportSummary,
    "",
    Math.min(limits.maxMarkdownLength, 240),
  );
  if (supportSummary.length === 0) {
    throw new A2uiError(
      "invalid_input",
      "A2UI experimental governance must include a non-empty support_summary.",
    );
  }

  const rolloutStage = resolveExperimentRolloutStage(
    coerceString(value.rollout_stage ?? value.rolloutStage, "dark_launch", 24).toLowerCase(),
  );
  const ambientMode = resolveExperimentAmbientMode(
    coerceString(value.ambient_mode ?? value.ambientMode, "disabled", 24).toLowerCase(),
  );
  const consentRequired = coerceBoolean(value.consent_required ?? value.consentRequired, false);
  if (ambientMode === "push_to_talk" && !consentRequired) {
    throw new A2uiError(
      "invalid_input",
      "A2UI ambient experiments require explicit consent_required=true.",
    );
  }

  const securityReview = normalizeChecklistEntries(
    value.security_review ?? value.securityReview,
    limits,
  );
  if (securityReview.length === 0) {
    throw new A2uiError(
      "invalid_input",
      "A2UI experimental governance must include a security_review checklist.",
    );
  }
  const exitCriteria = normalizeChecklistEntries(value.exit_criteria ?? value.exitCriteria, limits);
  if (exitCriteria.length === 0) {
    throw new A2uiError(
      "invalid_input",
      "A2UI experimental governance must include explicit exit_criteria.",
    );
  }

  return {
    trackId: sanitizeIdentifier(trackIdRaw, "experimental-track", 64),
    featureFlag,
    rolloutStage,
    ambientMode,
    consentRequired,
    supportSummary,
    securityReview,
    exitCriteria,
  };
}

function normalizeComponent(
  value: unknown,
  index: number,
  limits: RenderInputLimits,
  seenComponentIds: Set<string>,
): A2uiComponent | null {
  if (!isJsonObject(value)) {
    return null;
  }
  const type = coerceString(value.type, "", 24).toLowerCase();
  if (type.length === 0) {
    return null;
  }
  const fallbackId = `component-${index + 1}`;
  const idBase = sanitizeIdentifier(value.id, fallbackId, limits.maxComponentIdLength);
  const id = buildUniqueIdentifier(idBase, seenComponentIds, limits.maxComponentIdLength);
  const props = isJsonObject(value.props) ? value.props : {};

  switch (type) {
    case "text":
      return normalizeTextComponent(id, props, limits);
    case "markdown":
      return normalizeMarkdownComponent(id, props, limits);
    case "list":
      return normalizeListComponent(id, props, limits);
    case "table":
      return normalizeTableComponent(id, props, limits);
    case "form":
      return normalizeFormComponent(id, props, limits);
    case "chart":
      return normalizeChartComponent(id, props, limits);
    default:
      return null;
  }
}

function normalizeTextComponent(
  id: string,
  props: Record<string, unknown>,
  limits: RenderInputLimits,
): A2uiTextComponent {
  const toneRaw = coerceString(props.tone, "normal", 16).toLowerCase();
  const tone = resolveTextTone(toneRaw);
  const value = coerceString(props.value, "", limits.maxStringLength);
  return {
    id,
    type: "text",
    props: {
      tone,
      value,
    },
  };
}

function normalizeMarkdownComponent(
  id: string,
  props: Record<string, unknown>,
  limits: RenderInputLimits,
): A2uiMarkdownComponent {
  const raw = typeof props.value === "string" ? props.value : props.markdown;
  const value = coerceString(raw, "", limits.maxMarkdownLength);
  return {
    id,
    type: "markdown",
    props: {
      value,
    },
  };
}

function normalizeListComponent(
  id: string,
  props: Record<string, unknown>,
  limits: RenderInputLimits,
): A2uiListComponent {
  const ordered = coerceBoolean(props.ordered, false);
  const rawItems = Array.isArray(props.items) ? props.items : [];
  const items = rawItems
    .slice(0, limits.maxListItems)
    .map((entry) => stringifyJsonValue(entry, limits.maxStringLength));
  return {
    id,
    type: "list",
    props: {
      ordered,
      items,
    },
  };
}

function normalizeTableComponent(
  id: string,
  props: Record<string, unknown>,
  limits: RenderInputLimits,
): A2uiTableComponent {
  const rawColumns = Array.isArray(props.columns) ? props.columns : [];
  const columns = rawColumns
    .slice(0, limits.maxTableColumns)
    .map((entry, index) =>
      coerceString(
        entry,
        `Column ${index + 1}`,
        Math.max(8, Math.floor(limits.maxStringLength / 2)),
      ),
    );
  const safeColumns = columns.length > 0 ? columns : ["Value"];

  const rawRows = Array.isArray(props.rows) ? props.rows : [];
  const rows = rawRows
    .slice(0, limits.maxTableRows)
    .map((row) => normalizeTableRow(row, safeColumns.length, limits));

  return {
    id,
    type: "table",
    props: {
      columns: safeColumns,
      rows,
    },
  };
}

function normalizeTableRow(
  row: unknown,
  columnCount: number,
  limits: RenderInputLimits,
): readonly string[] {
  if (!Array.isArray(row)) {
    return Array.from({ length: columnCount }, () => "");
  }
  const rowValues = row as unknown[];
  const cells: string[] = [];
  for (let index = 0; index < columnCount; index += 1) {
    const value = index < rowValues.length ? rowValues[index] : "";
    cells.push(stringifyJsonValue(value, limits.maxStringLength));
  }
  return cells;
}

function normalizeFormComponent(
  id: string,
  props: Record<string, unknown>,
  limits: RenderInputLimits,
): A2uiFormComponent {
  const title = coerceString(props.title, "Form", limits.maxStringLength);
  const submitLabel = coerceString(props.submit_label ?? props.submitLabel, "Submit", 48);
  const rawFields = Array.isArray(props.fields) ? props.fields : [];
  const fields = rawFields
    .slice(0, limits.maxFormFields)
    .map((field, index) => normalizeFormField(field, index, limits))
    .filter((field): field is A2uiFormField => field !== null);

  return {
    id,
    type: "form",
    props: {
      title,
      submitLabel,
      fields,
    },
  };
}

function normalizeFormField(
  value: unknown,
  index: number,
  limits: RenderInputLimits,
): A2uiFormField | null {
  if (!isJsonObject(value)) {
    return null;
  }

  const type = resolveFormFieldType(coerceString(value.type, "text", 16).toLowerCase());
  const fieldId = sanitizeIdentifier(value.id, `field-${index + 1}`, 48);
  const label = coerceString(value.label, fieldId, 64);
  const hint = coerceString(value.hint, "", 128);
  const required = coerceBoolean(value.required, false);

  if (type === "number") {
    const min = coerceFiniteNumber(value.min, 0, -1_000_000, 1_000_000);
    const max = coerceFiniteNumber(value.max, 100, min, 1_000_000);
    const step = coerceFiniteNumber(value.step, 1, 0.000_001, 100_000);
    const defaultValue = coerceFiniteNumber(value.default, min, min, max);
    return {
      id: fieldId,
      label,
      type,
      hint,
      required,
      min,
      max,
      step,
      defaultValue,
    };
  }

  if (type === "select") {
    const rawOptions = Array.isArray(value.options) ? value.options : [];
    const options = rawOptions
      .slice(0, limits.maxSelectOptions)
      .map((entry, optionIndex) => normalizeSelectOption(entry, optionIndex))
      .filter((entry): entry is { label: string; value: string } => entry !== null);
    if (options.length === 0) {
      options.push({ label: "N/A", value: "na" });
    }
    const defaultCandidate = coerceString(
      value.default,
      options[0].value,
      Math.max(16, Math.floor(limits.maxStringLength / 2)),
    );
    const hasDefaultCandidate = options.some((option) => option.value === defaultCandidate);
    return {
      id: fieldId,
      label,
      type,
      hint,
      required,
      options,
      defaultValue: hasDefaultCandidate ? defaultCandidate : options[0].value,
    };
  }

  if (type === "checkbox") {
    return {
      id: fieldId,
      label,
      type,
      hint,
      required,
      defaultValue: coerceBoolean(value.default, false),
    };
  }

  return {
    id: fieldId,
    label,
    type,
    hint,
    required,
    placeholder: coerceString(value.placeholder, "", 96),
    defaultValue: coerceString(value.default, "", limits.maxStringLength),
  };
}

function normalizeSelectOption(
  value: unknown,
  index: number,
): { label: string; value: string } | null {
  if (!isJsonObject(value)) {
    return null;
  }
  const label = coerceString(value.label, `Option ${index + 1}`, 64);
  const optionValue = sanitizeIdentifier(value.value, `option-${index + 1}`, 64);
  if (optionValue.length === 0) {
    return null;
  }
  return {
    label,
    value: optionValue,
  };
}

function normalizeChartComponent(
  id: string,
  props: Record<string, unknown>,
  limits: RenderInputLimits,
): A2uiChartComponent {
  const title = coerceString(props.title, "Chart", limits.maxStringLength);
  const rawSeries = Array.isArray(props.series) ? props.series : [];
  const series = rawSeries
    .slice(0, limits.maxChartPoints)
    .map((entry, index) => normalizeChartPoint(entry, index))
    .filter((entry): entry is A2uiChartSeriesPoint => entry !== null);

  return {
    id,
    type: "chart",
    props: {
      title,
      series,
    },
  };
}

function normalizeChartPoint(value: unknown, index: number): A2uiChartSeriesPoint | null {
  if (!isJsonObject(value)) {
    return null;
  }
  return {
    label: coerceString(value.label, `Point ${index + 1}`, 64),
    value: coerceFiniteNumber(value.value, 0, 0, 1_000_000),
  };
}

function normalizeChecklistEntries(value: unknown, limits: RenderInputLimits): readonly string[] {
  if (!Array.isArray(value)) {
    return [];
  }
  return value
    .slice(0, MAX_EXPERIMENT_CHECKLIST_ITEMS)
    .map((entry) =>
      coerceString(entry, "", Math.min(limits.maxStringLength, MAX_EXPERIMENT_ENTRY_LENGTH)),
    )
    .filter((entry) => entry.length > 0);
}

function sanitizeFeatureFlag(value: unknown): string {
  const candidate = coerceString(value, "", 96);
  return candidate.replace(/[^a-zA-Z0-9:._-]/g, "-").replace(/-+/g, "-");
}

function resolveFormFieldType(value: string): A2uiFormFieldType {
  switch (value) {
    case "email":
      return "email";
    case "number":
      return "number";
    case "select":
      return "select";
    case "checkbox":
      return "checkbox";
    default:
      return "text";
  }
}

function resolveExperimentRolloutStage(value: string): A2uiExperimentRolloutStage {
  switch (value) {
    case "disabled":
      return "disabled";
    case "operator_preview":
      return "operator_preview";
    case "limited_preview":
      return "limited_preview";
    default:
      return "dark_launch";
  }
}

function resolveExperimentAmbientMode(value: string): A2uiExperimentAmbientMode {
  switch (value) {
    case "push_to_talk":
      return "push_to_talk";
    default:
      return "disabled";
  }
}

function resolveTextTone(value: string): "normal" | "muted" | "success" | "critical" {
  switch (value) {
    case "muted":
      return "muted";
    case "success":
      return "success";
    case "critical":
      return "critical";
    default:
      return "normal";
  }
}

function buildUniqueIdentifier(
  base: string,
  seenComponentIds: Set<string>,
  maxLength: number,
): string {
  if (!seenComponentIds.has(base)) {
    seenComponentIds.add(base);
    return base;
  }
  let suffix = 2;
  let candidate = `${base}-${suffix}`;
  while (seenComponentIds.has(candidate)) {
    suffix += 1;
    candidate = `${base}-${suffix}`;
  }
  const bounded = candidate.slice(0, maxLength);
  seenComponentIds.add(bounded);
  return bounded;
}

export function documentToJsonValue(document: A2uiDocument): JsonValue {
  return JSON.parse(JSON.stringify(document)) as JsonValue;
}
