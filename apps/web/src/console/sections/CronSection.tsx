import { useEffect, useMemo, useRef, useState, type FormEvent } from "react";
import { useNavigate } from "react-router-dom";

import {
  ActionButton,
  ActionCluster,
  AppForm,
  EmptyState,
  EntityTable,
  InlineNotice,
  KeyValueList,
  SelectField,
  SwitchField,
  TextAreaField,
  TextInputField,
  workspaceToneForState,
} from "../components/ui";
import {
  WorkspaceMetricCard,
  WorkspacePageHeader,
  WorkspaceSectionCard,
  WorkspaceStatusChip,
} from "../components/workspace/WorkspaceChrome";
import {
  formatUnixMs,
  isJsonObject,
  readBool,
  readNumber,
  readObject,
  readString,
  toErrorMessage,
  type JsonObject,
} from "../shared";
import type { ConsoleAppState } from "../useConsoleAppState";
import {
  APPROVAL_OPTIONS,
  applyTemplateToForm,
  buildRoutineUpsertPayload,
  CONCURRENCY_OPTIONS,
  defaultRoutineForm,
  DELIVERY_OPTIONS,
  EMPTY_JSON_TEXT,
  formatJson,
  MISFIRE_OPTIONS,
  millisecondsSummary,
  parseJsonObject,
  resolveRoutineId,
  routineFormFromRecord,
  routineSummary,
  SCHEDULE_OPTIONS,
  stripSystemEventPrefix,
  TIMEZONE_OPTIONS,
  TRIGGER_OPTIONS,
  type RoutineEditorForm,
} from "./routinesHelpers";
import {
  buildObjectiveChatHref,
  buildObjectiveOverviewHref,
  resolveObjectiveId,
} from "../objectiveLinks";
import { getSectionPath } from "../navigation";

type CronSectionProps = { app: ConsoleAppState };

export function CronSection({ app }: CronSectionProps) {
  const navigate = useNavigate();
  const bootstrappedRef = useRef(false);
  const lastLoadedRunsRoutineIdRef = useRef("");
  const [showEditor, setShowEditor] = useState(false);
  const [editorMode, setEditorMode] = useState<"create" | "edit">("create");
  const [routineBusy, setRoutineBusy] = useState(false);
  const [previewBusy, setPreviewBusy] = useState(false);
  const [templates, setTemplates] = useState<JsonObject[]>([]);
  const [objectives, setObjectives] = useState<JsonObject[]>([]);
  const [routineForm, setRoutineForm] = useState<RoutineEditorForm>(() =>
    defaultRoutineForm(app.session?.channel),
  );
  const [schedulePreview, setSchedulePreview] = useState<JsonObject | null>(null);
  const [exportText, setExportText] = useState("");
  const [importText, setImportText] = useState("");
  const [importEnabled, setImportEnabled] = useState(true);
  const [dispatchPayloadText, setDispatchPayloadText] = useState(EMPTY_JSON_TEXT);
  const [dispatchReason, setDispatchReason] = useState("");
  const [dispatchDedupeKey, setDispatchDedupeKey] = useState("");

  useEffect(() => {
    if (bootstrappedRef.current) {
      return;
    }
    bootstrappedRef.current = true;
    if (app.cronJobs.length === 0) {
      void app.refreshCron();
    }
    void Promise.all([loadTemplates(), loadObjectives()]);
  }, [app]);

  useEffect(() => {
    if (app.cronJobId.trim().length === 0) {
      const firstRoutineId = resolveRoutineId(app.cronJobs[0] ?? null);
      if (firstRoutineId !== null) {
        app.setCronJobId(firstRoutineId);
      }
      return;
    }
    if (lastLoadedRunsRoutineIdRef.current === app.cronJobId) {
      return;
    }
    lastLoadedRunsRoutineIdRef.current = app.cronJobId;
    void app.refreshCronRuns();
  }, [app]);

  const selectedRoutine =
    app.cronJobs.find((routine) => resolveRoutineId(routine) === app.cronJobId) ??
    app.cronJobs[0] ??
    null;
  const selectedRoutineId = resolveRoutineId(selectedRoutine);
  const selectedTriggerKind = readString(selectedRoutine ?? {}, "trigger_kind") ?? "manual";
  const selectedTriggerPayload = readObject(selectedRoutine ?? {}, "trigger_payload") ?? {};
  const selectedLastRun = readObject(selectedRoutine ?? {}, "last_run") ?? {};
  const selectedObjective =
    selectedRoutineId === null
      ? null
      : objectives.find(
          (objective) =>
            readString(readObject(objective, "automation") ?? {}, "routine_id") === selectedRoutineId,
        ) ?? null;
  const busy = app.cronBusy || routineBusy || previewBusy;

  useEffect(() => {
    setDispatchPayloadText(formatJson(selectedTriggerPayload));
    setDispatchReason(readString(selectedLastRun, "trigger_reason") ?? "");
    setDispatchDedupeKey("");
  }, [selectedRoutineId]);

  const enabledCount = app.cronJobs.filter((routine) => readBool(routine, "enabled")).length;
  const scheduleCount = app.cronJobs.filter(
    (routine) => readString(routine, "trigger_kind") === "schedule",
  ).length;
  const heartbeatCount = objectives.filter((objective) => readString(objective, "kind") === "heartbeat").length;
  const standingOrderCount = objectives.filter(
    (objective) => readString(objective, "kind") === "standing_order",
  ).length;
  const programCount = objectives.filter((objective) => readString(objective, "kind") === "program").length;
  const productBackedRoutineCount = objectives.filter((objective) => {
    const routineId = readString(readObject(objective, "automation") ?? {}, "routine_id");
    return routineId !== null;
  }).length;
  const genericRoutineCount = Math.max(app.cronJobs.length - productBackedRoutineCount, 0);
  const routineRows = useMemo(
    () =>
      app.cronJobs.map((routine) => {
        const lastRun = readObject(routine, "last_run") ?? {};
        const linkedObjective =
          objectives.find(
            (objective) =>
              readString(readObject(objective, "automation") ?? {}, "routine_id") ===
              resolveRoutineId(routine),
          ) ?? null;
        return {
          record: routine,
          linkedObjective,
          routineId: resolveRoutineId(routine) ?? "unknown",
          name: readString(routine, "name") ?? "unknown",
          product:
            linkedObjective !== null
              ? objectiveProductLabel(linkedObjective)
              : templateProductLabel(readString(routine, "template_id")),
          triggerKind: readString(routine, "trigger_kind") ?? "unknown",
          enabled: readBool(routine, "enabled"),
          nextRun: formatUnixMs(readNumber(routine, "next_run_at_unix_ms")),
          lastOutcome: readString(lastRun, "outcome_kind") ?? "never_run",
          productTone: productToneForObjective(linkedObjective),
          focus:
            readString(linkedObjective ?? {}, "current_focus") ??
            routineSummary(routine),
        };
      }),
    [app.cronJobs, objectives],
  );

  async function loadTemplates(): Promise<void> {
    try {
      const response = await app.api.listRoutineTemplates();
      setTemplates(
        Array.isArray(response.templates) ? response.templates.filter(isJsonObject) : [],
      );
    } catch (failure) {
      app.setError(toErrorMessage(failure));
    }
  }

  async function loadObjectives(): Promise<void> {
    try {
      const response = await app.api.listObjectives(new URLSearchParams({ limit: "64" }));
      setObjectives(
        Array.isArray(response.objectives) ? response.objectives.filter(isJsonObject) : [],
      );
    } catch (failure) {
      app.setError(toErrorMessage(failure));
    }
  }

  async function refreshAutomationSurface(options?: { refreshRuns?: boolean }): Promise<void> {
    await Promise.all([app.refreshCron(), loadObjectives()]);
    if (options?.refreshRuns && selectedRoutineId !== null) {
      await app.refreshCronRuns();
    }
  }

  function openCreateEditor(): void {
    setEditorMode("create");
    setRoutineForm(defaultRoutineForm(app.session?.channel));
    setSchedulePreview(null);
    setShowEditor(true);
  }

  function openEditEditor(routine: JsonObject): void {
    setEditorMode("edit");
    setRoutineForm(routineFormFromRecord(routine, app.session?.channel));
    setSchedulePreview(null);
    setShowEditor(true);
  }

  async function saveRoutine(event: FormEvent<HTMLFormElement>): Promise<void> {
    event.preventDefault();
    app.setError(null);
    app.setNotice(null);
    setRoutineBusy(true);
    try {
      const response = await app.api.upsertRoutine(buildRoutineUpsertPayload(routineForm));
      const savedRoutine = isJsonObject(response.routine) ? response.routine : null;
      const savedRoutineId = resolveRoutineId(savedRoutine);
      if (savedRoutineId !== null) {
        app.setCronJobId(savedRoutineId);
      }
      await refreshAutomationSurface();
      if (savedRoutineId !== null) {
        await app.refreshCronRuns();
      }
      setShowEditor(false);
      const approvalId =
        response.approval !== undefined && isJsonObject(response.approval)
          ? readString(response.approval, "approval_id")
          : null;
      app.setNotice(
        approvalId === null
          ? `Routine ${editorMode === "create" ? "created" : "updated"}.`
          : `Routine saved. Approval ${approvalId} is required before activation.`,
      );
    } catch (failure) {
      app.setError(toErrorMessage(failure));
    } finally {
      setRoutineBusy(false);
    }
  }

  async function previewNaturalLanguageSchedule(): Promise<void> {
    if (routineForm.naturalLanguageSchedule.trim().length === 0) {
      app.setError("Enter a natural-language schedule phrase first.");
      return;
    }
    app.setError(null);
    setPreviewBusy(true);
    try {
      const response = await app.api.previewRoutineSchedule({
        phrase: routineForm.naturalLanguageSchedule,
        timezone: routineForm.quietHoursTimezone,
      });
      setSchedulePreview(isJsonObject(response.preview) ? response.preview : null);
    } catch (failure) {
      app.setError(toErrorMessage(failure));
    } finally {
      setPreviewBusy(false);
    }
  }

  async function toggleRoutine(routine: JsonObject, enabled: boolean): Promise<void> {
    const routineId = resolveRoutineId(routine);
    if (routineId === null) {
      app.setError("Routine payload is missing routine_id.");
      return;
    }
    await runBusyAction(async () => {
      const response = await app.api.setRoutineEnabled(routineId, enabled);
      await refreshAutomationSurface();
      const approvalId =
        response.approval !== undefined && isJsonObject(response.approval)
          ? readString(response.approval, "approval_id")
          : null;
      app.setNotice(
        approvalId === null
          ? `Routine ${enabled ? "enabled" : "paused"}.`
          : `Routine saved but still blocked pending approval ${approvalId}.`,
      );
    });
  }

  async function runSelectedRoutine(routine: JsonObject): Promise<void> {
    const routineId = resolveRoutineId(routine);
    if (routineId === null) {
      app.setError("Routine payload is missing routine_id.");
      return;
    }
    await runBusyAction(async () => {
      const response = await app.api.runRoutineNow(routineId);
      app.setCronJobId(routineId);
      await refreshAutomationSurface({ refreshRuns: true });
      app.setNotice(
        response.run_id === undefined
          ? response.message
          : `Routine dispatched as run ${response.run_id}.`,
      );
    });
  }

  async function exportSelectedRoutine(): Promise<void> {
    if (selectedRoutineId === null) {
      app.setError("Select a routine before exporting.");
      return;
    }
    await runBusyAction(async () => {
      const response = await app.api.exportRoutine(selectedRoutineId);
      setExportText(formatJson(response.export));
      app.setNotice("Routine export bundle generated.");
    });
  }

  async function importRoutineBundle(): Promise<void> {
    await runBusyAction(async () => {
      const response = await app.api.importRoutine({
        export: parseJsonObject(importText, "Routine import bundle"),
        enabled: importEnabled,
      });
      const importedRoutine = isJsonObject(response.routine) ? response.routine : null;
      const importedRoutineId = resolveRoutineId(importedRoutine);
      if (importedRoutineId !== null) {
        app.setCronJobId(importedRoutineId);
      }
      await refreshAutomationSurface();
      app.setNotice(
        response.approval === undefined
          ? `Routine imported from ${response.imported_from}.`
          : `Routine imported from ${response.imported_from}; approval is required before enablement.`,
      );
    });
  }

  async function fireSelectedRoutineTrigger(): Promise<void> {
    if (selectedRoutineId === null || selectedRoutine === null) {
      app.setError("Select a routine before firing a test trigger.");
      return;
    }
    await runBusyAction(async () => {
      const payload = parseJsonObject(dispatchPayloadText, "Trigger payload");
      if (selectedTriggerKind === "hook") {
        const hookId = readString(selectedTriggerPayload, "hook_id");
        if (hookId === null) {
          throw new Error("Selected hook routine is missing hook_id matcher.");
        }
        const response = await app.api.dispatchHookRoutineTrigger({
          hook_id: hookId,
          event: readString(selectedTriggerPayload, "event") ?? undefined,
          payload,
          dedupe_key: dispatchDedupeKey.trim() || undefined,
        });
        await refreshAutomationSurface({ refreshRuns: true });
        app.setNotice(`Hook adapter evaluated ${response.dispatches.length} matching routines.`);
        return;
      }
      if (selectedTriggerKind === "webhook") {
        const integrationId = readString(selectedTriggerPayload, "integration_id");
        const event = readString(selectedTriggerPayload, "event");
        if (integrationId === null || event === null) {
          throw new Error("Selected webhook routine is missing integration or event matcher.");
        }
        const response = await app.api.dispatchWebhookRoutineTrigger({
          integration_id: integrationId,
          event,
          payload,
          source: readString(selectedTriggerPayload, "provider") ?? undefined,
          dedupe_key: dispatchDedupeKey.trim() || undefined,
        });
        await refreshAutomationSurface({ refreshRuns: true });
        app.setNotice(`Webhook adapter evaluated ${response.dispatches.length} matching routines.`);
        return;
      }
      if (selectedTriggerKind === "system_event") {
        const configuredEvent = readString(selectedTriggerPayload, "event");
        if (configuredEvent === null) {
          throw new Error("Selected system-event routine is missing event matcher.");
        }
        const response = await app.api.emitSystemEvent({
          name: stripSystemEventPrefix(configuredEvent),
          summary: dispatchReason.trim() || undefined,
          details: payload,
        });
        await refreshAutomationSurface({ refreshRuns: true });
        app.setNotice(
          `System event ${response.event} emitted; ${response.routine_dispatches.length} routines evaluated.`,
        );
        return;
      }
      const response =
        selectedTriggerKind === "schedule"
          ? await app.api.runRoutineNow(selectedRoutineId)
          : await app.api.dispatchRoutine(selectedRoutineId, {
              trigger_kind: selectedTriggerKind,
              trigger_reason: dispatchReason.trim() || undefined,
              trigger_payload: payload,
              trigger_dedupe_key: dispatchDedupeKey.trim() || undefined,
            });
      await refreshAutomationSurface({ refreshRuns: true });
      app.setNotice(
        response.run_id === undefined
          ? response.message
          : `Routine dispatched as run ${response.run_id}.`,
      );
    });
  }

  async function runBusyAction(action: () => Promise<void>): Promise<void> {
    app.setError(null);
    app.setNotice(null);
    setRoutineBusy(true);
    try {
      await action();
    } catch (failure) {
      app.setError(toErrorMessage(failure));
    } finally {
      setRoutineBusy(false);
    }
  }

  return (
    <main className="workspace-page">
      <WorkspacePageHeader
        eyebrow="Control"
        title="Automations"
        headingLabel="Automations"
        description="Productize heartbeats, standing orders, programs, and low-level routines on top of the same schedule, hook, webhook, and approval backend."
        status={
          <>
            <WorkspaceStatusChip tone="default">
              {app.cronJobs.length} automation{app.cronJobs.length === 1 ? "" : "s"}
            </WorkspaceStatusChip>
            <WorkspaceStatusChip tone={heartbeatCount > 0 ? "accent" : "default"}>
              {heartbeatCount} heartbeat{heartbeatCount === 1 ? "" : "s"}
            </WorkspaceStatusChip>
            <WorkspaceStatusChip tone={enabledCount > 0 ? "success" : "default"}>
              {enabledCount} enabled
            </WorkspaceStatusChip>
          </>
        }
        actions={
          <ActionCluster>
            <ActionButton variant="secondary" onPress={openCreateEditor}>
              New routine
            </ActionButton>
            <ActionButton
              variant="secondary"
              onPress={() => void refreshAutomationSurface({ refreshRuns: true })}
              isDisabled={busy}
            >
              {busy ? "Refreshing..." : "Refresh automations"}
            </ActionButton>
          </ActionCluster>
        }
      />

      <section className="workspace-metric-grid workspace-metric-grid--compact">
        <WorkspaceMetricCard
          label="Heartbeats"
          value={heartbeatCount}
          detail="Recurring status products with explicit cadence and output contracts."
          tone={heartbeatCount > 0 ? "accent" : "default"}
        />
        <WorkspaceMetricCard
          label="Standing orders"
          value={standingOrderCount}
          detail="Durable authority products that stay visible and approval-aware."
          tone={standingOrderCount > 0 ? "accent" : "default"}
        />
        <WorkspaceMetricCard
          label="Programs"
          value={programCount}
          detail="Multi-step initiatives anchored to objectives instead of a second orchestration engine."
          tone={programCount > 0 ? "accent" : "default"}
        />
        <WorkspaceMetricCard
          label="General routines"
          value={genericRoutineCount}
          detail={`${scheduleCount} scheduled, ${app.cronJobs.length - scheduleCount} event-driven.`}
          tone={genericRoutineCount > 0 ? "default" : "success"}
        />
        <WorkspaceMetricCard
          label="Selected"
          value={selectedRoutineId ?? "None"}
          detail={
            selectedRoutine === null
              ? "Pick an automation to inspect runtime state."
              : selectedObjective !== null
                ? `${objectiveProductLabel(selectedObjective)} · ${readString(selectedObjective, "current_focus") ?? "No current focus recorded."}`
                : routineSummary(selectedRoutine)
          }
          tone={selectedRoutine === null ? "default" : "accent"}
        />
        <WorkspaceMetricCard
          label="Last outcome"
          value={readString(selectedLastRun, "outcome_kind") ?? "never_run"}
          detail={
            readString(selectedLastRun, "outcome_message") ??
            "Skipped, throttled, silent, and denied automations stay explicit."
          }
          tone={workspaceToneForState(readString(selectedLastRun, "outcome_kind"))}
        />
      </section>

      <section className="workspace-two-column">
        <WorkspaceSectionCard
          title="Automation catalog"
          description="One list for heartbeats, standing orders, programs, and lower-level routines without fragmenting the operator surface."
        >
          <EntityTable
            ariaLabel="Routine catalog"
            columns={[
              {
                key: "routine",
                label: "Routine",
                isRowHeader: true,
                render: (row) => (
                  <div className="workspace-stack">
                    <strong>{row.name}</strong>
                    <span className="chat-muted">
                      {row.product} · {row.focus}
                    </span>
                  </div>
                ),
              },
              {
                key: "state",
                label: "State",
                render: (row) => (
                  <div className="workspace-inline">
                    <WorkspaceStatusChip tone={row.enabled ? "success" : "default"}>
                      {row.enabled ? "enabled" : "paused"}
                    </WorkspaceStatusChip>
                    <WorkspaceStatusChip tone={row.productTone}>
                      {row.product}
                    </WorkspaceStatusChip>
                    <WorkspaceStatusChip tone={workspaceToneForState(row.lastOutcome)}>
                      {row.lastOutcome}
                    </WorkspaceStatusChip>
                  </div>
                ),
              },
              {
                key: "actions",
                label: "Actions",
                align: "end",
                render: (row) => (
                  <ActionCluster>
                    <ActionButton
                      variant="secondary"
                      size="sm"
                      onPress={() => app.setCronJobId(row.routineId)}
                    >
                      Select
                    </ActionButton>
                    <ActionButton
                      variant="secondary"
                      size="sm"
                      onPress={() => openEditEditor(row.record)}
                    >
                      Edit
                    </ActionButton>
                    <ActionButton
                      size="sm"
                      onPress={() => void toggleRoutine(row.record, !row.enabled)}
                      isDisabled={busy}
                    >
                      {row.enabled ? "Pause" : "Enable"}
                    </ActionButton>
                    <ActionButton
                      variant="secondary"
                      size="sm"
                      onPress={() => void runSelectedRoutine(row.record)}
                      isDisabled={busy}
                    >
                      Run now
                    </ActionButton>
                  </ActionCluster>
                ),
              },
            ]}
            rows={routineRows}
            getRowId={(row) => row.routineId}
            emptyTitle="No automations configured"
            emptyDescription="Create the first heartbeat, standing order, program, or routine."
          />
        </WorkspaceSectionCard>

        <WorkspaceSectionCard
          title={editorMode === "create" ? "New automation" : "Edit automation"}
          description="Single editor for low-level routine wiring; productized automation modes stay visible through linked objectives."
        >
          {!showEditor ? (
            <EmptyState
              compact
              title="Automation editor collapsed"
              description="Open the editor when you need to create or update a low-level routine."
              action={
                <ActionButton variant="secondary" onPress={openCreateEditor}>
                  Open automation editor
                </ActionButton>
              }
            />
          ) : (
            <AppForm onSubmit={(event) => void saveRoutine(event)}>
              <div className="workspace-form-grid">
                <TextInputField
                  label="Name"
                  value={routineForm.name}
                  onChange={(name) => setRoutineForm((current) => ({ ...current, name }))}
                  required
                />
                <TextInputField
                  label="Channel"
                  value={routineForm.channel}
                  onChange={(channel) => setRoutineForm((current) => ({ ...current, channel }))}
                />
                <SelectField
                  label="Trigger kind"
                  value={routineForm.triggerKind}
                  onChange={(triggerKind) =>
                    setRoutineForm((current) => ({
                      ...current,
                      triggerKind: triggerKind as RoutineEditorForm["triggerKind"],
                    }))
                  }
                  options={TRIGGER_OPTIONS}
                />
                <SelectField
                  label="Delivery"
                  value={routineForm.deliveryMode}
                  onChange={(deliveryMode) =>
                    setRoutineForm((current) => ({
                      ...current,
                      deliveryMode: deliveryMode as RoutineEditorForm["deliveryMode"],
                    }))
                  }
                  options={DELIVERY_OPTIONS}
                />
                <TextInputField
                  label="Delivery channel"
                  value={routineForm.deliveryChannel}
                  onChange={(deliveryChannel) =>
                    setRoutineForm((current) => ({ ...current, deliveryChannel }))
                  }
                />
                <TextInputField
                  label="Template id"
                  value={routineForm.templateId}
                  onChange={(templateId) =>
                    setRoutineForm((current) => ({ ...current, templateId }))
                  }
                />
              </div>
              <TextAreaField
                label="Prompt"
                rows={4}
                value={routineForm.prompt}
                onChange={(prompt) => setRoutineForm((current) => ({ ...current, prompt }))}
                required
              />
              {routineForm.triggerKind === "schedule" ? (
                <>
                  <div className="workspace-form-grid">
                    <TextInputField
                      label="Natural-language schedule"
                      value={routineForm.naturalLanguageSchedule}
                      onChange={(naturalLanguageSchedule) =>
                        setRoutineForm((current) => ({ ...current, naturalLanguageSchedule }))
                      }
                    />
                    <SelectField
                      label="Structured schedule"
                      value={routineForm.scheduleType}
                      onChange={(scheduleType) =>
                        setRoutineForm((current) => ({
                          ...current,
                          scheduleType: scheduleType as RoutineEditorForm["scheduleType"],
                        }))
                      }
                      options={SCHEDULE_OPTIONS}
                    />
                    <TextInputField
                      label="Every interval (ms)"
                      value={routineForm.everyIntervalMs}
                      onChange={(everyIntervalMs) =>
                        setRoutineForm((current) => ({ ...current, everyIntervalMs }))
                      }
                    />
                    <TextInputField
                      label="Cron expression"
                      value={routineForm.cronExpression}
                      onChange={(cronExpression) =>
                        setRoutineForm((current) => ({ ...current, cronExpression }))
                      }
                    />
                    <TextInputField
                      label="At timestamp"
                      value={routineForm.atTimestampRfc3339}
                      onChange={(atTimestampRfc3339) =>
                        setRoutineForm((current) => ({ ...current, atTimestampRfc3339 }))
                      }
                    />
                    <ActionButton
                      variant="secondary"
                      type="button"
                      onPress={() => void previewNaturalLanguageSchedule()}
                      isDisabled={previewBusy}
                    >
                      {previewBusy ? "Previewing..." : "Preview phrase"}
                    </ActionButton>
                  </div>
                  {schedulePreview !== null ? (
                    <InlineNotice title="Schedule preview" tone="accent">
                      {readString(schedulePreview, "explanation") ?? "Preview unavailable"} Next run{" "}
                      {formatUnixMs(readNumber(schedulePreview, "next_run_at_unix_ms"))}.
                    </InlineNotice>
                  ) : null}
                </>
              ) : (
                <>
                  <div className="workspace-form-grid">
                    {routineForm.triggerKind === "hook" ? (
                      <TextInputField
                        label="Hook id"
                        value={routineForm.hookId}
                        onChange={(hookId) => setRoutineForm((current) => ({ ...current, hookId }))}
                      />
                    ) : null}
                    {routineForm.triggerKind === "webhook" ? (
                      <>
                        <TextInputField
                          label="Integration id"
                          value={routineForm.webhookIntegrationId}
                          onChange={(webhookIntegrationId) =>
                            setRoutineForm((current) => ({ ...current, webhookIntegrationId }))
                          }
                        />
                        <TextInputField
                          label="Provider"
                          value={routineForm.webhookProvider}
                          onChange={(webhookProvider) =>
                            setRoutineForm((current) => ({ ...current, webhookProvider }))
                          }
                        />
                      </>
                    ) : null}
                    {routineForm.triggerKind === "hook" ||
                    routineForm.triggerKind === "webhook" ||
                    routineForm.triggerKind === "system_event" ? (
                      <TextInputField
                        label={
                          routineForm.triggerKind === "system_event"
                            ? "System event"
                            : "Event matcher"
                        }
                        value={routineForm.eventName}
                        onChange={(eventName) =>
                          setRoutineForm((current) => ({ ...current, eventName }))
                        }
                      />
                    ) : null}
                  </div>
                  <TextAreaField
                    label="Trigger payload matcher"
                    rows={4}
                    value={routineForm.triggerPayloadText}
                    onChange={(triggerPayloadText) =>
                      setRoutineForm((current) => ({ ...current, triggerPayloadText }))
                    }
                  />
                </>
              )}
              <div className="workspace-form-grid">
                <SelectField
                  label="Concurrency"
                  value={routineForm.concurrencyPolicy}
                  onChange={(concurrencyPolicy) =>
                    setRoutineForm((current) => ({
                      ...current,
                      concurrencyPolicy:
                        concurrencyPolicy as RoutineEditorForm["concurrencyPolicy"],
                    }))
                  }
                  options={CONCURRENCY_OPTIONS}
                />
                <TextInputField
                  label="Retry max attempts"
                  value={routineForm.retryMaxAttempts}
                  onChange={(retryMaxAttempts) =>
                    setRoutineForm((current) => ({ ...current, retryMaxAttempts }))
                  }
                />
                <TextInputField
                  label="Retry backoff (ms)"
                  value={routineForm.retryBackoffMs}
                  onChange={(retryBackoffMs) =>
                    setRoutineForm((current) => ({ ...current, retryBackoffMs }))
                  }
                />
                <SelectField
                  label="Misfire"
                  value={routineForm.misfirePolicy}
                  onChange={(misfirePolicy) =>
                    setRoutineForm((current) => ({
                      ...current,
                      misfirePolicy: misfirePolicy as RoutineEditorForm["misfirePolicy"],
                    }))
                  }
                  options={MISFIRE_OPTIONS}
                />
                <TextInputField
                  label="Jitter (ms)"
                  value={routineForm.jitterMs}
                  onChange={(jitterMs) => setRoutineForm((current) => ({ ...current, jitterMs }))}
                />
                <TextInputField
                  label="Cooldown (ms)"
                  value={routineForm.cooldownMs}
                  onChange={(cooldownMs) =>
                    setRoutineForm((current) => ({ ...current, cooldownMs }))
                  }
                />
                <TextInputField
                  label="Quiet hours start"
                  value={routineForm.quietHoursStart}
                  onChange={(quietHoursStart) =>
                    setRoutineForm((current) => ({ ...current, quietHoursStart }))
                  }
                />
                <TextInputField
                  label="Quiet hours end"
                  value={routineForm.quietHoursEnd}
                  onChange={(quietHoursEnd) =>
                    setRoutineForm((current) => ({ ...current, quietHoursEnd }))
                  }
                />
                <SelectField
                  label="Quiet timezone"
                  value={routineForm.quietHoursTimezone}
                  onChange={(quietHoursTimezone) =>
                    setRoutineForm((current) => ({
                      ...current,
                      quietHoursTimezone:
                        quietHoursTimezone as RoutineEditorForm["quietHoursTimezone"],
                    }))
                  }
                  options={TIMEZONE_OPTIONS}
                />
                <SelectField
                  label="Approval"
                  value={routineForm.approvalMode}
                  onChange={(approvalMode) =>
                    setRoutineForm((current) => ({
                      ...current,
                      approvalMode: approvalMode as RoutineEditorForm["approvalMode"],
                    }))
                  }
                  options={APPROVAL_OPTIONS}
                />
              </div>
              <SwitchField
                label="Enabled"
                checked={routineForm.enabled}
                onChange={(enabled) => setRoutineForm((current) => ({ ...current, enabled }))}
              />
              <ActionCluster>
                <ActionButton type="submit" isDisabled={busy}>
                  {busy ? "Saving..." : editorMode === "create" ? "Create routine" : "Save routine"}
                </ActionButton>
                <ActionButton
                  type="button"
                  variant="secondary"
                  onPress={openCreateEditor}
                  isDisabled={busy}
                >
                  Reset form
                </ActionButton>
              </ActionCluster>
            </AppForm>
          )}
        </WorkspaceSectionCard>
      </section>

      <section className="workspace-two-column workspace-two-column--history">
        <WorkspaceSectionCard
          title="Selected automation"
          description="Inspect product posture, delivery contract, trigger wiring, cooldown, and approval guardrails before changing anything."
        >
          {selectedRoutine === null ? (
            <EmptyState
              compact
              title="No automation selected"
              description="Select an automation to inspect its configuration and run posture."
            />
          ) : (
            <>
              <KeyValueList
                items={[
                  {
                    label: "Product",
                    value:
                      selectedObjective !== null
                        ? objectiveProductLabel(selectedObjective)
                        : templateProductLabel(readString(selectedRoutine, "template_id")),
                  },
                  { label: "Routine id", value: selectedRoutineId ?? "n/a" },
                  { label: "Trigger", value: selectedTriggerKind },
                  { label: "Summary", value: routineSummary(selectedRoutine) },
                  {
                    label: "Delivery",
                    value: `${readString(selectedRoutine, "delivery_mode") ?? "same_channel"}${readString(selectedRoutine, "delivery_channel") ? ` -> ${readString(selectedRoutine, "delivery_channel")}` : ""}`,
                  },
                  {
                    label: "Approval",
                    value: readString(selectedRoutine, "approval_mode") ?? "none",
                  },
                  {
                    label: "Cooldown",
                    value: millisecondsSummary(readNumber(selectedRoutine, "cooldown_ms")),
                  },
                  {
                    label: "Last outcome",
                    value: readString(selectedLastRun, "outcome_kind") ?? "never_run",
                  },
                ]}
              />
              {selectedObjective !== null ? (
                <InlineNotice
                  title="Linked objective"
                  tone={workspaceToneForState(readString(selectedObjective, "state"))}
                >
                  <p>
                    <strong>{readString(selectedObjective, "name") ?? "Unnamed objective"}</strong>{" "}
                    · {readString(selectedObjective, "kind") ?? "objective"} ·{" "}
                    {readString(selectedObjective, "state") ?? "unknown"}
                  </p>
                  <p>
                    <strong>Current focus:</strong>{" "}
                    {readString(selectedObjective, "current_focus") ??
                      "No current focus recorded."}
                  </p>
                  <p>
                    <strong>Next action:</strong>{" "}
                    {readString(selectedObjective, "next_recommended_step") ??
                      "No next action recorded."}
                  </p>
                  <ActionCluster>
                    <ActionButton
                      variant="secondary"
                      size="sm"
                      onPress={() => {
                        const objectiveId = resolveObjectiveId(selectedObjective);
                        if (objectiveId === null) {
                          return;
                        }
                        void navigate(buildObjectiveOverviewHref(objectiveId));
                      }}
                    >
                      Open objective
                    </ActionButton>
                    <ActionButton
                      variant="secondary"
                      size="sm"
                      onPress={() =>
                        void navigate(
                          buildObjectiveChatHref({
                            objective: selectedObjective,
                            fallbackSessionId: readString(selectedRoutine, "session_id"),
                            runId: readString(selectedLastRun, "run_id"),
                          }),
                        )
                      }
                    >
                      Open chat
                    </ActionButton>
                  </ActionCluster>
                </InlineNotice>
              ) : null}
              {selectedObjective !== null && readString(selectedObjective, "kind") === "heartbeat" ? (
                <InlineNotice
                  title="Heartbeat signal"
                  tone={heartbeatSignalTone(selectedLastRun)}
                >
                  {heartbeatSignalSummary(selectedObjective, selectedLastRun)}
                </InlineNotice>
              ) : null}
              <TextAreaField
                label="Trigger payload"
                readOnly
                rows={4}
                value={formatJson(selectedTriggerPayload)}
                onChange={() => undefined}
              />
            </>
          )}
        </WorkspaceSectionCard>
        <WorkspaceSectionCard
          title="Run history"
          description="Recent automation runs keep skipped, throttled, silent, and denied outcomes distinct."
        >
          <EntityTable
            ariaLabel="Routine run history"
            columns={[
              {
                key: "run",
                label: "Run",
                isRowHeader: true,
                render: (run) => (
                  <div className="workspace-stack">
                    <strong>{readString(run, "run_id") ?? "unknown"}</strong>
                    <span className="chat-muted">
                      {formatUnixMs(readNumber(run, "started_at_unix_ms"))}
                    </span>
                  </div>
                ),
              },
              {
                key: "outcome",
                label: "Outcome",
                render: (run) => (
                  <div className="workspace-inline">
                    <WorkspaceStatusChip
                      tone={workspaceToneForState(readString(run, "outcome_kind"))}
                    >
                      {readString(run, "outcome_kind") ?? "unknown"}
                    </WorkspaceStatusChip>
                    <WorkspaceStatusChip tone={workspaceToneForState(readString(run, "status"))}>
                      {readString(run, "status") ?? "unknown"}
                    </WorkspaceStatusChip>
                  </div>
                ),
              },
              {
                key: "summary",
                label: "Summary",
                render: (run) =>
                  readString(run, "outcome_message") ??
                  readString(run, "trigger_reason") ??
                  "No explanation recorded.",
              },
            ]}
            rows={app.cronRuns}
            getRowId={(run) => readString(run, "run_id") ?? "run"}
            emptyTitle="No runs recorded"
            emptyDescription="Run-now and adapter tests will populate history here."
          />
        </WorkspaceSectionCard>
      </section>

      <section className="workspace-two-column">
        <WorkspaceSectionCard
          title="Template pack"
          description="Bootstrap common automations from built-in heartbeat, report, follow-up, change-check, and ingest templates."
        >
          <EntityTable
            ariaLabel="Routine templates"
            columns={[
              {
                key: "template",
                label: "Template",
                isRowHeader: true,
                render: (template) => (
                  <div className="workspace-stack">
                    <strong>
                      {readString(template, "title") ?? readString(template, "template_id")}
                    </strong>
                    <span className="chat-muted">
                      {readString(template, "description") ?? "No description"}
                    </span>
                  </div>
                ),
              },
              {
                key: "defaults",
                label: "Defaults",
                render: (template) =>
                  `${readString(template, "trigger_kind") ?? "manual"} · ${readString(template, "natural_language_schedule") ?? "manual trigger"}`,
              },
              {
                key: "actions",
                label: "Actions",
                align: "end",
                render: (template) => (
                  <ActionButton
                    variant="secondary"
                    size="sm"
                    onPress={() => {
                      setEditorMode("create");
                      setRoutineForm(
                        applyTemplateToForm(template, defaultRoutineForm(app.session?.channel)),
                      );
                      setShowEditor(true);
                    }}
                  >
                    Use template
                  </ActionButton>
                ),
              },
            ]}
            rows={templates}
            getRowId={(template) => readString(template, "template_id") ?? "template"}
            emptyTitle="No templates loaded"
            emptyDescription="Templates are loaded from the daemon-side routine pack."
          />
        </WorkspaceSectionCard>

        <WorkspaceSectionCard
          title="Import, export, and trigger test"
          description="Portable bundles and adapter test firing stay inside the same automation surface."
        >
          <ActionCluster>
            <ActionButton
              variant="secondary"
              size="sm"
              onPress={() => void exportSelectedRoutine()}
              isDisabled={busy || selectedRoutineId === null}
            >
              Export selected
            </ActionButton>
            <ActionButton
              variant="secondary"
              size="sm"
              onPress={() => void fireSelectedRoutineTrigger()}
              isDisabled={busy || selectedRoutineId === null}
            >
              {selectedTriggerKind === "schedule" ? "Run selected now" : "Fire selected trigger"}
            </ActionButton>
          </ActionCluster>
          <TextAreaField
            label="Export bundle"
            readOnly
            rows={5}
            value={exportText}
            onChange={() => undefined}
          />
          <TextAreaField
            label="Import bundle"
            rows={5}
            value={importText}
            onChange={setImportText}
          />
          <SwitchField
            label="Enable imported routine"
            checked={importEnabled}
            onChange={setImportEnabled}
          />
          <ActionButton onPress={() => void importRoutineBundle()} isDisabled={busy}>
            Import automation
          </ActionButton>
          <TextAreaField
            label="Trigger payload"
            rows={4}
            value={dispatchPayloadText}
            onChange={setDispatchPayloadText}
          />
          <div className="workspace-form-grid">
            <TextInputField
              label="Trigger reason"
              value={dispatchReason}
              onChange={setDispatchReason}
            />
            <TextInputField
              label="Dedupe key"
              value={dispatchDedupeKey}
              onChange={setDispatchDedupeKey}
            />
          </div>
          {selectedTriggerKind === "schedule" ? (
            <InlineNotice title="Schedule routines" tone="default">
              Scheduled automations use run-now. Hook, webhook, and system-event routines can fire
              their adapter path directly from this panel.
            </InlineNotice>
          ) : null}
        </WorkspaceSectionCard>
      </section>
    </main>
  );
}

function objectiveProductLabel(objective: JsonObject): string {
  const kind = readString(objective, "kind");
  switch (kind) {
    case "heartbeat":
      return "Heartbeat";
    case "standing_order":
      return "Standing order";
    case "program":
      return "Program";
    default:
      return "Objective";
  }
}

function templateProductLabel(templateId: string | null): string {
  if (templateId === "heartbeat") {
    return "Heartbeat template";
  }
  return "Routine";
}

function productToneForObjective(linkedObjective: JsonObject | null): "default" | "accent" {
  if (linkedObjective === null) {
    return "default";
  }
  const kind = readString(linkedObjective, "kind");
  return kind === "heartbeat" || kind === "standing_order" || kind === "program"
    ? "accent"
    : "default";
}

function heartbeatSignalTone(selectedLastRun: JsonObject): "default" | "warning" | "success" {
  const outcome = readString(selectedLastRun, "outcome_kind");
  if (outcome === "failed" || outcome === "denied" || outcome === "skipped") {
    return "warning";
  }
  if (outcome === "success_with_output") {
    return "success";
  }
  return "default";
}

function heartbeatSignalSummary(objective: JsonObject, selectedLastRun: JsonObject): string {
  const name = readString(objective, "name") ?? "Heartbeat";
  const outcome = readString(selectedLastRun, "outcome_kind");
  const nextRun = readString(readObject(objective, "linked_routine") ?? {}, "next_run_at_unix_ms");
  const nextAction =
    readString(objective, "next_recommended_step") ?? "Review the latest heartbeat output.";
  if (outcome === "success_with_output") {
    return `${name} produced output successfully. Next action: ${nextAction}`;
  }
  if (outcome === "failed" || outcome === "denied") {
    return `${name} needs follow-up because the latest run ended as ${outcome}. Next action: ${nextAction}`;
  }
  if (outcome === "skipped" || outcome === "throttled") {
    return `${name} did not emit a fresh update on the latest run (${outcome}). Confirm cadence, cooldown, and signal quality before the next heartbeat.`;
  }
  if (nextRun !== null) {
    return `${name} is configured but has not produced a durable output yet. Watch the next scheduled run and confirm that HEARTBEAT.md stays current.`;
  }
  return `${name} is not currently scheduled. Add a cadence or fire it manually so the heartbeat produces visible output.`;
}
