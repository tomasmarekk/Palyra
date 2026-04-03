import { useEffect, useMemo, useRef, useState, type FormEvent } from "react";

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

type CronSectionProps = { app: ConsoleAppState };

export function CronSection({ app }: CronSectionProps) {
  const bootstrappedRef = useRef(false);
  const lastLoadedRunsRoutineIdRef = useRef("");
  const [showEditor, setShowEditor] = useState(false);
  const [editorMode, setEditorMode] = useState<"create" | "edit">("create");
  const [routineBusy, setRoutineBusy] = useState(false);
  const [previewBusy, setPreviewBusy] = useState(false);
  const [templates, setTemplates] = useState<JsonObject[]>([]);
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
    void loadTemplates();
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
  const routineRows = useMemo(
    () =>
      app.cronJobs.map((routine) => {
        const lastRun = readObject(routine, "last_run") ?? {};
        return {
          record: routine,
          routineId: resolveRoutineId(routine) ?? "unknown",
          name: readString(routine, "name") ?? "unknown",
          triggerKind: readString(routine, "trigger_kind") ?? "unknown",
          enabled: readBool(routine, "enabled"),
          nextRun: formatUnixMs(readNumber(routine, "next_run_at_unix_ms")),
          lastOutcome: readString(lastRun, "outcome_kind") ?? "never_run",
        };
      }),
    [app.cronJobs],
  );

  async function loadTemplates(): Promise<void> {
    try {
      const response = await app.api.listRoutineTemplates();
      setTemplates(Array.isArray(response.templates) ? response.templates.filter(isJsonObject) : []);
    } catch (failure) {
      app.setError(toErrorMessage(failure));
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
      await app.refreshCron();
      if (savedRoutineId !== null) {
        await app.refreshCronRuns();
      }
      setShowEditor(false);
      const approvalId = response.approval !== undefined && isJsonObject(response.approval)
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
      await app.refreshCron();
      const approvalId = response.approval !== undefined && isJsonObject(response.approval)
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
      await app.refreshCron();
      await app.refreshCronRuns();
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
      await app.refreshCron();
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
        await app.refreshCron();
        await app.refreshCronRuns();
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
        await app.refreshCron();
        await app.refreshCronRuns();
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
        await app.refreshCron();
        await app.refreshCronRuns();
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
      await app.refreshCron();
      await app.refreshCronRuns();
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
        title="Routines"
        headingLabel="Routines"
        description="Unify schedules, hooks, webhooks, system events, delivery semantics, and approvals under one automation model."
        status={
          <>
            <WorkspaceStatusChip tone="default">{app.cronJobs.length} routines</WorkspaceStatusChip>
            <WorkspaceStatusChip tone={enabledCount > 0 ? "success" : "default"}>
              {enabledCount} enabled
            </WorkspaceStatusChip>
            <WorkspaceStatusChip tone="accent">
              {app.cronJobs.length - scheduleCount} event-driven
            </WorkspaceStatusChip>
          </>
        }
        actions={
          <ActionCluster>
            <ActionButton variant="secondary" onPress={openCreateEditor}>
              New routine
            </ActionButton>
            <ActionButton variant="secondary" onPress={() => void app.refreshCron()} isDisabled={busy}>
              {busy ? "Refreshing..." : "Refresh routines"}
            </ActionButton>
          </ActionCluster>
        }
      />

      <section className="workspace-metric-grid workspace-metric-grid--compact">
        <WorkspaceMetricCard label="Scheduled" value={scheduleCount} detail="Cron-compatible paths now map onto routines." tone={scheduleCount > 0 ? "success" : "default"} />
        <WorkspaceMetricCard label="Selected" value={selectedRoutineId ?? "None"} detail={selectedRoutine === null ? "Pick a routine to inspect runtime state." : routineSummary(selectedRoutine)} tone={selectedRoutine === null ? "default" : "accent"} />
        <WorkspaceMetricCard label="Last outcome" value={readString(selectedLastRun, "outcome_kind") ?? "never_run"} detail={readString(selectedLastRun, "outcome_message") ?? "Skipped, throttled, and silent runs stay explicit."} tone={workspaceToneForState(readString(selectedLastRun, "outcome_kind"))} />
      </section>

      <section className="workspace-two-column">
        <WorkspaceSectionCard title="Routine catalog" description="List-first surface for selection, enablement, and manual run-now execution.">
          <EntityTable
            ariaLabel="Routine catalog"
            columns={[
              { key: "routine", label: "Routine", isRowHeader: true, render: (row) => <div className="workspace-stack"><strong>{row.name}</strong><span className="chat-muted">{row.triggerKind} · next {row.nextRun}</span></div> },
              { key: "state", label: "State", render: (row) => <div className="workspace-inline"><WorkspaceStatusChip tone={row.enabled ? "success" : "default"}>{row.enabled ? "enabled" : "paused"}</WorkspaceStatusChip><WorkspaceStatusChip tone={workspaceToneForState(row.lastOutcome)}>{row.lastOutcome}</WorkspaceStatusChip></div> },
              { key: "actions", label: "Actions", align: "end", render: (row) => <ActionCluster><ActionButton variant="secondary" size="sm" onPress={() => app.setCronJobId(row.routineId)}>Select</ActionButton><ActionButton variant="secondary" size="sm" onPress={() => openEditEditor(row.record)}>Edit</ActionButton><ActionButton size="sm" onPress={() => void toggleRoutine(row.record, !row.enabled)} isDisabled={busy}>{row.enabled ? "Pause" : "Enable"}</ActionButton><ActionButton variant="secondary" size="sm" onPress={() => void runSelectedRoutine(row.record)} isDisabled={busy}>Run now</ActionButton></ActionCluster> },
            ]}
            rows={routineRows}
            getRowId={(row) => row.routineId}
            emptyTitle="No routines configured"
            emptyDescription="Create the first routine to unify schedule and event automation."
          />
        </WorkspaceSectionCard>

        <WorkspaceSectionCard title={editorMode === "create" ? "New routine" : "Edit routine"} description="Single editor for trigger kind, scheduling, delivery, quiet hours, and approvals.">
          {!showEditor ? (
            <EmptyState compact title="Routine editor collapsed" description="Open the editor when you are ready to create or update a routine." action={<ActionButton variant="secondary" onPress={openCreateEditor}>Open routine editor</ActionButton>} />
          ) : (
            <AppForm onSubmit={(event) => void saveRoutine(event)}>
              <div className="workspace-form-grid">
                <TextInputField label="Name" value={routineForm.name} onChange={(name) => setRoutineForm((current) => ({ ...current, name }))} required />
                <TextInputField label="Channel" value={routineForm.channel} onChange={(channel) => setRoutineForm((current) => ({ ...current, channel }))} />
                <SelectField label="Trigger kind" value={routineForm.triggerKind} onChange={(triggerKind) => setRoutineForm((current) => ({ ...current, triggerKind: triggerKind as RoutineEditorForm["triggerKind"] }))} options={TRIGGER_OPTIONS} />
                <SelectField label="Delivery" value={routineForm.deliveryMode} onChange={(deliveryMode) => setRoutineForm((current) => ({ ...current, deliveryMode: deliveryMode as RoutineEditorForm["deliveryMode"] }))} options={DELIVERY_OPTIONS} />
                <TextInputField label="Delivery channel" value={routineForm.deliveryChannel} onChange={(deliveryChannel) => setRoutineForm((current) => ({ ...current, deliveryChannel }))} />
                <TextInputField label="Template id" value={routineForm.templateId} onChange={(templateId) => setRoutineForm((current) => ({ ...current, templateId }))} />
              </div>
              <TextAreaField label="Prompt" rows={4} value={routineForm.prompt} onChange={(prompt) => setRoutineForm((current) => ({ ...current, prompt }))} required />
              {routineForm.triggerKind === "schedule" ? (
                <>
                  <div className="workspace-form-grid">
                    <TextInputField label="Natural-language schedule" value={routineForm.naturalLanguageSchedule} onChange={(naturalLanguageSchedule) => setRoutineForm((current) => ({ ...current, naturalLanguageSchedule }))} />
                    <SelectField label="Structured schedule" value={routineForm.scheduleType} onChange={(scheduleType) => setRoutineForm((current) => ({ ...current, scheduleType: scheduleType as RoutineEditorForm["scheduleType"] }))} options={SCHEDULE_OPTIONS} />
                    <TextInputField label="Every interval (ms)" value={routineForm.everyIntervalMs} onChange={(everyIntervalMs) => setRoutineForm((current) => ({ ...current, everyIntervalMs }))} />
                    <TextInputField label="Cron expression" value={routineForm.cronExpression} onChange={(cronExpression) => setRoutineForm((current) => ({ ...current, cronExpression }))} />
                    <TextInputField label="At timestamp" value={routineForm.atTimestampRfc3339} onChange={(atTimestampRfc3339) => setRoutineForm((current) => ({ ...current, atTimestampRfc3339 }))} />
                    <ActionButton variant="secondary" type="button" onPress={() => void previewNaturalLanguageSchedule()} isDisabled={previewBusy}>{previewBusy ? "Previewing..." : "Preview phrase"}</ActionButton>
                  </div>
                  {schedulePreview !== null ? <InlineNotice title="Schedule preview" tone="accent">{readString(schedulePreview, "explanation") ?? "Preview unavailable"} Next run {formatUnixMs(readNumber(schedulePreview, "next_run_at_unix_ms"))}.</InlineNotice> : null}
                </>
              ) : (
                <>
                  <div className="workspace-form-grid">
                    {routineForm.triggerKind === "hook" ? <TextInputField label="Hook id" value={routineForm.hookId} onChange={(hookId) => setRoutineForm((current) => ({ ...current, hookId }))} /> : null}
                    {routineForm.triggerKind === "webhook" ? <><TextInputField label="Integration id" value={routineForm.webhookIntegrationId} onChange={(webhookIntegrationId) => setRoutineForm((current) => ({ ...current, webhookIntegrationId }))} /><TextInputField label="Provider" value={routineForm.webhookProvider} onChange={(webhookProvider) => setRoutineForm((current) => ({ ...current, webhookProvider }))} /></> : null}
                    {routineForm.triggerKind === "hook" || routineForm.triggerKind === "webhook" || routineForm.triggerKind === "system_event" ? <TextInputField label={routineForm.triggerKind === "system_event" ? "System event" : "Event matcher"} value={routineForm.eventName} onChange={(eventName) => setRoutineForm((current) => ({ ...current, eventName }))} /> : null}
                  </div>
                  <TextAreaField label="Trigger payload matcher" rows={4} value={routineForm.triggerPayloadText} onChange={(triggerPayloadText) => setRoutineForm((current) => ({ ...current, triggerPayloadText }))} />
                </>
              )}
              <div className="workspace-form-grid">
                <SelectField label="Concurrency" value={routineForm.concurrencyPolicy} onChange={(concurrencyPolicy) => setRoutineForm((current) => ({ ...current, concurrencyPolicy: concurrencyPolicy as RoutineEditorForm["concurrencyPolicy"] }))} options={CONCURRENCY_OPTIONS} />
                <TextInputField label="Retry max attempts" value={routineForm.retryMaxAttempts} onChange={(retryMaxAttempts) => setRoutineForm((current) => ({ ...current, retryMaxAttempts }))} />
                <TextInputField label="Retry backoff (ms)" value={routineForm.retryBackoffMs} onChange={(retryBackoffMs) => setRoutineForm((current) => ({ ...current, retryBackoffMs }))} />
                <SelectField label="Misfire" value={routineForm.misfirePolicy} onChange={(misfirePolicy) => setRoutineForm((current) => ({ ...current, misfirePolicy: misfirePolicy as RoutineEditorForm["misfirePolicy"] }))} options={MISFIRE_OPTIONS} />
                <TextInputField label="Jitter (ms)" value={routineForm.jitterMs} onChange={(jitterMs) => setRoutineForm((current) => ({ ...current, jitterMs }))} />
                <TextInputField label="Cooldown (ms)" value={routineForm.cooldownMs} onChange={(cooldownMs) => setRoutineForm((current) => ({ ...current, cooldownMs }))} />
                <TextInputField label="Quiet hours start" value={routineForm.quietHoursStart} onChange={(quietHoursStart) => setRoutineForm((current) => ({ ...current, quietHoursStart }))} />
                <TextInputField label="Quiet hours end" value={routineForm.quietHoursEnd} onChange={(quietHoursEnd) => setRoutineForm((current) => ({ ...current, quietHoursEnd }))} />
                <SelectField label="Quiet timezone" value={routineForm.quietHoursTimezone} onChange={(quietHoursTimezone) => setRoutineForm((current) => ({ ...current, quietHoursTimezone: quietHoursTimezone as RoutineEditorForm["quietHoursTimezone"] }))} options={TIMEZONE_OPTIONS} />
                <SelectField label="Approval" value={routineForm.approvalMode} onChange={(approvalMode) => setRoutineForm((current) => ({ ...current, approvalMode: approvalMode as RoutineEditorForm["approvalMode"] }))} options={APPROVAL_OPTIONS} />
              </div>
              <SwitchField label="Enabled" checked={routineForm.enabled} onChange={(enabled) => setRoutineForm((current) => ({ ...current, enabled }))} />
              <ActionCluster><ActionButton type="submit" isDisabled={busy}>{busy ? "Saving..." : editorMode === "create" ? "Create routine" : "Save routine"}</ActionButton><ActionButton type="button" variant="secondary" onPress={openCreateEditor} isDisabled={busy}>Reset form</ActionButton></ActionCluster>
            </AppForm>
          )}
        </WorkspaceSectionCard>
      </section>

      <section className="workspace-two-column workspace-two-column--history">
        <WorkspaceSectionCard title="Selected routine" description="Inspect delivery, trigger payload, cooldown, and approval posture before changing anything.">
          {selectedRoutine === null ? <EmptyState compact title="No routine selected" description="Select a routine to inspect its configuration and run posture." /> : <>
            <KeyValueList items={[
              { label: "Routine id", value: selectedRoutineId ?? "n/a" },
              { label: "Trigger", value: selectedTriggerKind },
              { label: "Summary", value: routineSummary(selectedRoutine) },
              { label: "Delivery", value: `${readString(selectedRoutine, "delivery_mode") ?? "same_channel"}${readString(selectedRoutine, "delivery_channel") ? ` -> ${readString(selectedRoutine, "delivery_channel")}` : ""}` },
              { label: "Approval", value: readString(selectedRoutine, "approval_mode") ?? "none" },
              { label: "Cooldown", value: millisecondsSummary(readNumber(selectedRoutine, "cooldown_ms")) },
              { label: "Last outcome", value: readString(selectedLastRun, "outcome_kind") ?? "never_run" },
            ]} />
            <TextAreaField label="Trigger payload" readOnly rows={4} value={formatJson(selectedTriggerPayload)} onChange={() => undefined} />
          </>}
        </WorkspaceSectionCard>
        <WorkspaceSectionCard title="Run history" description="Recent routine runs keep skipped, throttled, and silent outcomes distinct.">
          <EntityTable
            ariaLabel="Routine run history"
            columns={[
              { key: "run", label: "Run", isRowHeader: true, render: (run) => <div className="workspace-stack"><strong>{readString(run, "run_id") ?? "unknown"}</strong><span className="chat-muted">{formatUnixMs(readNumber(run, "started_at_unix_ms"))}</span></div> },
              { key: "outcome", label: "Outcome", render: (run) => <div className="workspace-inline"><WorkspaceStatusChip tone={workspaceToneForState(readString(run, "outcome_kind"))}>{readString(run, "outcome_kind") ?? "unknown"}</WorkspaceStatusChip><WorkspaceStatusChip tone={workspaceToneForState(readString(run, "status"))}>{readString(run, "status") ?? "unknown"}</WorkspaceStatusChip></div> },
              { key: "summary", label: "Summary", render: (run) => readString(run, "outcome_message") ?? readString(run, "trigger_reason") ?? "No explanation recorded." },
            ]}
            rows={app.cronRuns}
            getRowId={(run) => readString(run, "run_id") ?? "run"}
            emptyTitle="No runs recorded"
            emptyDescription="Run-now and adapter tests will populate history here."
          />
        </WorkspaceSectionCard>
      </section>

      <section className="workspace-two-column">
        <WorkspaceSectionCard title="Template pack" description="Bootstrap common routines from built-in heartbeat, report, follow-up, change-check, and ingest templates.">
          <EntityTable
            ariaLabel="Routine templates"
            columns={[
              { key: "template", label: "Template", isRowHeader: true, render: (template) => <div className="workspace-stack"><strong>{readString(template, "title") ?? readString(template, "template_id")}</strong><span className="chat-muted">{readString(template, "description") ?? "No description"}</span></div> },
              { key: "defaults", label: "Defaults", render: (template) => `${readString(template, "trigger_kind") ?? "manual"} · ${readString(template, "natural_language_schedule") ?? "manual trigger"}` },
              { key: "actions", label: "Actions", align: "end", render: (template) => <ActionButton variant="secondary" size="sm" onPress={() => { setEditorMode("create"); setRoutineForm(applyTemplateToForm(template, defaultRoutineForm(app.session?.channel))); setShowEditor(true); }}>Use template</ActionButton> },
            ]}
            rows={templates}
            getRowId={(template) => readString(template, "template_id") ?? "template"}
            emptyTitle="No templates loaded"
            emptyDescription="Templates are loaded from the daemon-side routine pack."
          />
        </WorkspaceSectionCard>

        <WorkspaceSectionCard title="Import, export, and trigger test" description="Portable bundles and adapter test firing stay inside the same routine surface.">
          <ActionCluster><ActionButton variant="secondary" size="sm" onPress={() => void exportSelectedRoutine()} isDisabled={busy || selectedRoutineId === null}>Export selected</ActionButton><ActionButton variant="secondary" size="sm" onPress={() => void fireSelectedRoutineTrigger()} isDisabled={busy || selectedRoutineId === null}>{selectedTriggerKind === "schedule" ? "Run selected now" : "Fire selected trigger"}</ActionButton></ActionCluster>
          <TextAreaField label="Export bundle" readOnly rows={5} value={exportText} onChange={() => undefined} />
          <TextAreaField label="Import bundle" rows={5} value={importText} onChange={setImportText} />
          <SwitchField label="Enable imported routine" checked={importEnabled} onChange={setImportEnabled} />
          <ActionButton onPress={() => void importRoutineBundle()} isDisabled={busy}>Import routine</ActionButton>
          <TextAreaField label="Trigger payload" rows={4} value={dispatchPayloadText} onChange={setDispatchPayloadText} />
          <div className="workspace-form-grid">
            <TextInputField label="Trigger reason" value={dispatchReason} onChange={setDispatchReason} />
            <TextInputField label="Dedupe key" value={dispatchDedupeKey} onChange={setDispatchDedupeKey} />
          </div>
          {selectedTriggerKind === "schedule" ? <InlineNotice title="Schedule routines" tone="default">Schedule routines use run-now. Hook, webhook, and system-event routines can fire their adapter path directly from this panel.</InlineNotice> : null}
        </WorkspaceSectionCard>
      </section>
    </main>
  );
}
