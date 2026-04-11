import { useEffect, useMemo, useState, type FormEvent } from "react";

import {
  ActionButton,
  AppForm,
  CheckboxField,
  EmptyState,
  EntityTable,
  SelectField,
  TextInputField,
} from "../console/components/ui";
import { SanitizedMarkdown } from "./markdown";
import type {
  A2uiChartComponent,
  A2uiComponent,
  A2uiDocument,
  A2uiExperimentAmbientMode,
  A2uiExperimentGovernance,
  A2uiExperimentRolloutStage,
  A2uiFormComponent,
  A2uiFormField,
  A2uiFormSubmitEvent,
  A2uiFormValue,
} from "./types";

interface A2uiRendererProps {
  readonly document: A2uiDocument;
  readonly onFormSubmit?: (event: A2uiFormSubmitEvent) => void;
}

type A2uiTableRow = {
  id: string;
  cells: readonly string[];
};

export function A2uiRenderer({ document, onFormSubmit }: A2uiRendererProps) {
  return (
    <section
      className="a2ui-renderer"
      data-surface={document.surface}
      aria-label={document.surface}
    >
      {document.experimental ? <ExperimentBanner experiment={document.experimental} /> : null}
      {document.components.map((component) => (
        <article
          key={component.id}
          className="a2ui-component"
          data-component-id={component.id}
          data-component-type={component.type}
        >
          <ComponentBody component={component} onFormSubmit={onFormSubmit} />
        </article>
      ))}
      {document.components.length === 0 ? (
        <EmptyState
          compact
          title="No renderable components"
          description="This A2UI document does not contain any renderable components."
        />
      ) : null}
    </section>
  );
}

function ExperimentBanner({ experiment }: { experiment: A2uiExperimentGovernance }) {
  return (
    <aside className="a2ui-experiment-banner" data-track-id={experiment.trackId}>
      <div className="a2ui-experiment-heading">
        <div>
          <p className="a2ui-experiment-eyebrow">Experimental surface</p>
          <h3>{experiment.trackId}</h3>
        </div>
        <p className="a2ui-experiment-chip">{formatRolloutStage(experiment.rolloutStage)}</p>
      </div>
      <p className="a2ui-experiment-summary">{experiment.supportSummary}</p>
      <dl className="a2ui-experiment-meta">
        <div>
          <dt>Feature flag</dt>
          <dd>{experiment.featureFlag}</dd>
        </div>
        <div>
          <dt>Ambient mode</dt>
          <dd>{formatAmbientMode(experiment.ambientMode)}</dd>
        </div>
        <div>
          <dt>Consent</dt>
          <dd>{experiment.consentRequired ? "required" : "not required"}</dd>
        </div>
      </dl>
      <div className="a2ui-experiment-checklists">
        <ExperimentChecklist title="Security review" items={experiment.securityReview} />
        <ExperimentChecklist title="Exit criteria" items={experiment.exitCriteria} />
      </div>
    </aside>
  );
}

function ExperimentChecklist({ title, items }: { title: string; items: readonly string[] }) {
  return (
    <section className="a2ui-experiment-checklist">
      <strong>{title}</strong>
      <ul className="a2ui-list">
        {items.map((item) => (
          <li key={`${title}-${item}`}>{item}</li>
        ))}
      </ul>
    </section>
  );
}

interface ComponentBodyProps {
  readonly component: A2uiComponent;
  readonly onFormSubmit?: (event: A2uiFormSubmitEvent) => void;
}

function ComponentBody({ component, onFormSubmit }: ComponentBodyProps) {
  switch (component.type) {
    case "text":
      return (
        <p className={`a2ui-text a2ui-text-${component.props.tone}`}>{component.props.value}</p>
      );
    case "markdown":
      return <SanitizedMarkdown value={component.props.value} />;
    case "list":
      return component.props.ordered ? (
        <ol className="a2ui-list">
          {component.props.items.map((item, index) => (
            <li key={`${component.id}-${index}`}>{item}</li>
          ))}
        </ol>
      ) : (
        <ul className="a2ui-list">
          {component.props.items.map((item, index) => (
            <li key={`${component.id}-${index}`}>{item}</li>
          ))}
        </ul>
      );
    case "table":
      return <A2uiTable component={component} />;
    case "form":
      return <A2uiForm component={component} onSubmit={onFormSubmit} />;
    case "chart":
      return <A2uiBarChart component={component} />;
    default:
      return null;
  }
}

function A2uiTable({ component }: { component: Extract<A2uiComponent, { type: "table" }> }) {
  const columns = component.props.columns.map((column, index) => ({
    key: `column-${index}`,
    label: column,
    isRowHeader: index === 0,
    render: (row: A2uiTableRow) => row.cells[index] ?? "",
  }));

  const rows: A2uiTableRow[] = component.props.rows.map((cells, rowIndex) => ({
    id: `${component.id}-row-${rowIndex}`,
    cells,
  }));

  return (
    <EntityTable
      ariaLabel={component.id}
      columns={columns}
      rows={rows}
      getRowId={(row) => row.id}
      emptyTitle="No table rows"
      emptyDescription="This table component does not currently contain any rows."
      className="a2ui-table-wrap"
    />
  );
}

interface A2uiFormProps {
  readonly component: A2uiFormComponent;
  readonly onSubmit?: (event: A2uiFormSubmitEvent) => void;
}

function A2uiForm({ component, onSubmit }: A2uiFormProps) {
  const initialValues = useMemo(
    () => buildInitialFormValues(component.props.fields),
    [component.props.fields],
  );
  const [values, setValues] = useState<Record<string, A2uiFormValue>>(initialValues);

  useEffect(() => {
    setValues(initialValues);
  }, [initialValues]);

  function updateFieldValue(fieldId: string, value: A2uiFormValue): void {
    setValues((current) => ({
      ...current,
      [fieldId]: value,
    }));
  }

  function handleSubmit(event: FormEvent<HTMLFormElement>): void {
    event.preventDefault();
    onSubmit?.({
      componentId: component.id,
      values,
    });
  }

  return (
    <AppForm className="a2ui-form" onSubmit={handleSubmit}>
      <header className="a2ui-form-header">
        <h3>{component.props.title}</h3>
      </header>
      <div className="a2ui-form-fields">
        {component.props.fields.map((field) => (
          <FormFieldRow
            key={`${component.id}-${field.id}`}
            componentId={component.id}
            field={field}
            value={values[field.id]}
            onChange={updateFieldValue}
          />
        ))}
      </div>
      <footer className="a2ui-form-footer">
        <ActionButton type="submit">{component.props.submitLabel}</ActionButton>
      </footer>
    </AppForm>
  );
}

interface FormFieldRowProps {
  readonly componentId: string;
  readonly field: A2uiFormField;
  readonly value: A2uiFormValue | undefined;
  readonly onChange: (fieldId: string, value: A2uiFormValue) => void;
}

function FormFieldRow({ componentId, field, value, onChange }: FormFieldRowProps) {
  const inputId = `${componentId}-${field.id}`;

  if (field.type === "checkbox") {
    const checked = typeof value === "boolean" ? value : field.defaultValue;
    return (
      <CheckboxField
        label={field.label}
        description={field.hint.length > 0 ? field.hint : undefined}
        checked={checked}
        onChange={(nextValue) => onChange(field.id, nextValue)}
      />
    );
  }

  if (field.type === "select") {
    const selectedValue = typeof value === "string" ? value : field.defaultValue;
    return (
      <SelectField
        name={inputId}
        label={field.label}
        value={selectedValue}
        description={field.hint.length > 0 ? field.hint : undefined}
        onChange={(nextValue) => onChange(field.id, nextValue)}
        options={field.options.map((option) => ({
          key: option.value,
          label: option.label,
        }))}
      />
    );
  }

  if (field.type === "number") {
    const numberValue = typeof value === "number" ? value : field.defaultValue;
    return (
      <TextInputField
        name={inputId}
        label={field.label}
        type="number"
        value={String(numberValue)}
        description={field.hint.length > 0 ? field.hint : undefined}
        required={field.required}
        onChange={(nextValue) => {
          const parsed = Number.parseFloat(nextValue);
          onChange(field.id, Number.isFinite(parsed) ? parsed : field.defaultValue);
        }}
      />
    );
  }

  const textValue = typeof value === "string" ? value : field.defaultValue;
  return (
    <TextInputField
      name={inputId}
      label={field.label}
      type={field.type}
      value={textValue}
      placeholder={field.placeholder}
      description={field.hint.length > 0 ? field.hint : undefined}
      required={field.required}
      onChange={(nextValue) => onChange(field.id, nextValue)}
    />
  );
}

function buildInitialFormValues(fields: readonly A2uiFormField[]): Record<string, A2uiFormValue> {
  const values: Record<string, A2uiFormValue> = {};
  for (const field of fields) {
    values[field.id] = field.defaultValue;
  }
  return values;
}

interface A2uiBarChartProps {
  readonly component: A2uiChartComponent;
}

function A2uiBarChart({ component }: A2uiBarChartProps) {
  const maxValue = component.props.series.reduce(
    (maximum, entry) => Math.max(maximum, entry.value),
    1,
  );

  return (
    <figure className="a2ui-chart">
      <figcaption>{component.props.title}</figcaption>
      <div className="a2ui-chart-bars" role="img" aria-label={component.props.title}>
        {component.props.series.map((entry) => {
          const widthPercent = Math.min(100, (entry.value / maxValue) * 100);
          return (
            <div key={`${component.id}-${entry.label}`} className="a2ui-chart-row">
              <span className="a2ui-chart-label">{entry.label}</span>
              <span className="a2ui-chart-track" aria-hidden="true">
                <svg
                  className="a2ui-chart-track-svg"
                  viewBox="0 0 100 10"
                  preserveAspectRatio="none"
                >
                  <rect
                    className="a2ui-chart-track-rect"
                    x="0"
                    y="0"
                    width="100"
                    height="10"
                    rx="5"
                    ry="5"
                  />
                  <rect
                    className="a2ui-chart-bar-rect"
                    x="0"
                    y="0"
                    width={widthPercent}
                    height="10"
                    rx="5"
                    ry="5"
                  />
                </svg>
              </span>
              <span className="a2ui-chart-value">{entry.value}</span>
            </div>
          );
        })}
      </div>
    </figure>
  );
}

function formatRolloutStage(value: A2uiExperimentRolloutStage): string {
  switch (value) {
    case "disabled":
      return "Disabled";
    case "operator_preview":
      return "Operator preview";
    case "limited_preview":
      return "Limited preview";
    default:
      return "Dark launch";
  }
}

function formatAmbientMode(value: A2uiExperimentAmbientMode): string {
  return value === "push_to_talk" ? "Push-to-talk only" : "Disabled";
}
