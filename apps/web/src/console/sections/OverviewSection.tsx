import { useEffect, useMemo, useState } from "react";
import { useNavigate, useSearchParams } from "react-router-dom";

import {
  ActionButton,
  ActionCluster,
  AppForm,
  EntityTable,
  KeyValueList,
  SelectField,
  SwitchField,
  TextAreaField,
  TextInputField,
} from "../components/ui";
import {
  WorkspaceMetricCard,
  WorkspacePageHeader,
  WorkspaceSectionCard,
  WorkspaceStatusChip,
} from "../components/workspace/WorkspaceChrome";
import {
  NextActionCard,
  OnboardingChecklistCard,
  ScenarioCard,
  TroubleshootingCard,
} from "../components/guidance/GuidanceCards";
import {
  WorkspaceEmptyState,
  WorkspaceInlineNotice,
  workspaceToneForState,
} from "../components/workspace/WorkspacePatterns";
import {
  buildObjectiveChatHref,
  objectiveWorkspaceDocumentPath,
  resolveObjectiveId,
} from "../objectiveLinks";
import { getSectionPath } from "../navigation";
import {
  emptyToUndefined,
  formatUnixMs,
  isJsonObject,
  readBool,
  readNumber,
  readObject,
  readString,
  toErrorMessage,
  toJsonObjectArray,
  toStringArray,
  type JsonObject,
} from "../shared";
import type { ConsoleAppState } from "../useConsoleAppState";

type OverviewSectionProps = {
  app: Pick<
    ConsoleAppState,
    | "api"
    | "overviewBusy"
    | "overviewDeployment"
    | "overviewApprovals"
    | "overviewDiagnostics"
    | "overviewUsageInsights"
    | "overviewSupportJobs"
    | "refreshOverview"
    | "setError"
    | "setNotice"
    | "setSection"
    | "setUiMode"
    | "t"
    | "uiMode"
    | "uxTelemetryAggregate"
    | "uxTelemetryBusy"
    | "refreshUxTelemetry"
  >;
};

type ObjectiveKindValue = "objective" | "heartbeat" | "standing_order" | "program";
type LifecycleAction = "fire" | "pause" | "resume" | "archive";

type ObjectiveEditorForm = {
  kind: ObjectiveKindValue;
  name: string;
  prompt: string;
  currentFocus: string;
  successCriteria: string;
  nextRecommendedStep: string;
  standingOrder: string;
  naturalLanguageSchedule: string;
  deliveryChannel: string;
  enabled: boolean;
};

const OBJECTIVE_KIND_OPTIONS = [
  { key: "objective", label: "Objective", value: "objective" },
  { key: "heartbeat", label: "Heartbeat", value: "heartbeat" },
  { key: "standing_order", label: "Standing order", value: "standing_order" },
  { key: "program", label: "Program", value: "program" },
];

const DEFAULT_OBJECTIVE_FORM: ObjectiveEditorForm = {
  kind: "objective",
  name: "",
  prompt: "",
  currentFocus: "",
  successCriteria: "",
  nextRecommendedStep: "",
  standingOrder: "",
  naturalLanguageSchedule: "",
  deliveryChannel: "",
  enabled: true,
};

export function OverviewSection({ app }: OverviewSectionProps) {
  const navigate = useNavigate();
  const [searchParams] = useSearchParams();
  const preferredObjectiveId = searchParams.get("objectiveId");
  const [objectivesBusy, setObjectivesBusy] = useState(false);
  const [objectiveMutationBusy, setObjectiveMutationBusy] = useState(false);
  const [showObjectiveEditor, setShowObjectiveEditor] = useState(false);
  const [objectiveForm, setObjectiveForm] = useState<ObjectiveEditorForm>(DEFAULT_OBJECTIVE_FORM);
  const [objectives, setObjectives] = useState<JsonObject[]>([]);
  const [selectedObjectiveId, setSelectedObjectiveId] = useState("");

  const deployment = app.overviewDeployment;
  const diagnostics = app.overviewDiagnostics;
  const usageInsights = app.overviewUsageInsights;
  const observability = readObject(diagnostics ?? {}, "observability");
  const connector = readObject(observability ?? {}, "connector");
  const providerAuth = readObject(observability ?? {}, "provider_auth");
  const objectivesSnapshot = readObject(diagnostics ?? {}, "objectives");
  const warnings = toStringArray(Array.isArray(deployment?.warnings) ? deployment.warnings : []);
  const pendingApprovals = app.overviewApprovals.filter((approval) => {
    const decision = readString(approval, "decision");
    return decision === null || decision === "pending" || decision.length === 0;
  }).length;
  const failedSupportJobs = app.overviewSupportJobs.filter(
    (job) => readString(job, "state") === "failed",
  ).length;
  const connectorDegraded = Number(readString(connector ?? {}, "degraded_connectors") ?? "0");
  const providerState =
    readString(providerAuth ?? {}, "state") ??
    readString(readObject(diagnostics ?? {}, "auth_profiles") ?? {}, "state") ??
    "unknown";
  const activeObjectiveCount = objectives.filter(
    (objective) => readString(objective, "state") === "active",
  ).length;
  const heartbeatCount = objectives.filter(
    (objective) => readString(objective, "kind") === "heartbeat",
  ).length;
  const standingOrderCount = objectives.filter(
    (objective) => readString(objective, "kind") === "standing_order",
  ).length;
  const programCount = objectives.filter(
    (objective) => readString(objective, "kind") === "program",
  ).length;
  const objectiveAttentionCount = objectives.filter(
    (objective) => objectiveHealthState(objective) === "attention",
  ).length;
  const attentionItems = buildAttentionItems({
    warnings,
    pendingApprovals,
    failedSupportJobs,
    connectorDegraded,
    providerState,
    alertCount: usageInsights?.alerts.length ?? 0,
    objectiveAttentionCount,
  });
  const busy = app.overviewBusy || objectivesBusy || objectiveMutationBusy;
  const uxAggregate = app.uxTelemetryAggregate;
  const objectiveRows = useMemo(
    () =>
      objectives.map((objective) => ({
        record: objective,
        objectiveId: resolveObjectiveId(objective) ?? "unknown",
        kind: objectiveKindLabel(readString(objective, "kind")),
        name: readString(objective, "name") ?? "Untitled objective",
        state: readString(objective, "state") ?? "unknown",
        health: objectiveHealthState(objective),
        currentFocus: readString(objective, "current_focus") ?? "No current focus recorded.",
        lastActivity: objectiveActivitySummary(objective),
        nextRun: objectiveNextRunSummary(objective),
      })),
    [objectives],
  );
  const selectedObjective = useMemo(
    () =>
      objectives.find((objective) => resolveObjectiveId(objective) === selectedObjectiveId) ??
      objectives[0] ??
      null,
    [objectives, selectedObjectiveId],
  );

  useEffect(() => {
    void loadObjectives();
  }, [app.api]);

  useEffect(() => {
    if (preferredObjectiveId !== null && preferredObjectiveId.trim().length > 0) {
      const preferredExists = objectives.some(
        (objective) => resolveObjectiveId(objective) === preferredObjectiveId.trim(),
      );
      if (preferredExists && selectedObjectiveId !== preferredObjectiveId.trim()) {
        setSelectedObjectiveId(preferredObjectiveId.trim());
        return;
      }
    }
    if (selectedObjectiveId.length === 0) {
      const firstObjectiveId = resolveObjectiveId(objectives[0] ?? null);
      if (firstObjectiveId !== null) {
        setSelectedObjectiveId(firstObjectiveId);
      }
      return;
    }
    const stillExists = objectives.some(
      (objective) => resolveObjectiveId(objective) === selectedObjectiveId,
    );
    if (!stillExists) {
      setSelectedObjectiveId(resolveObjectiveId(objectives[0] ?? null) ?? "");
    }
  }, [objectives, preferredObjectiveId, selectedObjectiveId]);

  async function loadObjectives(): Promise<void> {
    setObjectivesBusy(true);
    try {
      const response = await app.api.listObjectives(new URLSearchParams({ limit: "8" }));
      setObjectives(
        toJsonObjectArray(Array.isArray(response.objectives) ? response.objectives : []),
      );
    } catch (failure) {
      app.setError(toErrorMessage(failure));
    } finally {
      setObjectivesBusy(false);
    }
  }

  async function refreshSurface(): Promise<void> {
    await Promise.all([app.refreshOverview(), loadObjectives()]);
  }

  async function saveObjective(): Promise<void> {
    app.setError(null);
    app.setNotice(null);
    setObjectiveMutationBusy(true);
    try {
      const response = await app.api.upsertObjective(buildObjectivePayload(objectiveForm));
      const objective = isJsonObject(response.objective) ? response.objective : null;
      const objectiveId = resolveObjectiveId(objective);
      await refreshSurface();
      if (objectiveId !== null) {
        setSelectedObjectiveId(objectiveId);
      }
      setShowObjectiveEditor(false);
      setObjectiveForm(DEFAULT_OBJECTIVE_FORM);
      app.setNotice(`${objectiveKindLabel(objectiveForm.kind)} saved.`);
    } catch (failure) {
      app.setError(toErrorMessage(failure));
    } finally {
      setObjectiveMutationBusy(false);
    }
  }

  async function applyLifecycle(objective: JsonObject, action: LifecycleAction): Promise<void> {
    const objectiveId = resolveObjectiveId(objective);
    if (objectiveId === null) {
      app.setError("Objective payload is missing objective_id.");
      return;
    }
    app.setError(null);
    app.setNotice(null);
    setObjectiveMutationBusy(true);
    try {
      await app.api.lifecycleObjective(objectiveId, { action });
      await refreshSurface();
      app.setNotice(`${objectiveKindLabel(readString(objective, "kind"))} ${actionLabel(action)}.`);
    } catch (failure) {
      app.setError(toErrorMessage(failure));
    } finally {
      setObjectiveMutationBusy(false);
    }
  }

  function openSelectedObjectiveChat(): void {
    if (selectedObjective === null) {
      app.setError("Select an objective first.");
      return;
    }
    void navigate(
      buildObjectiveChatHref({
        objective: selectedObjective,
        runId: readString(readObject(selectedObjective, "last_run") ?? {}, "run_id"),
      }),
    );
  }

  function openSelectedObjectiveMemory(): void {
    const path = objectiveWorkspaceDocumentPath(selectedObjective);
    app.setSection("memory");
    if (path !== null) {
      app.setNotice(`Open workspace document ${path} in Memory.`);
    }
  }

  function openSelectedObjectiveRoutines(): void {
    app.setSection("cron");
    const routineId =
      readString(readObject(selectedObjective ?? {}, "automation") ?? {}, "routine_id") ??
      readString(readObject(selectedObjective ?? {}, "linked_routine") ?? {}, "job_id");
    if (routineId !== null) {
      app.setNotice(`Inspect linked automation ${routineId} in Automations.`);
    }
  }

  function openSelectedObjectiveOperations(): void {
    const objectiveId = resolveObjectiveId(selectedObjective);
    if (objectiveId !== null) {
      void navigate(
        `${getSectionPath("operations")}?objectiveId=${encodeURIComponent(objectiveId)}`,
      );
      return;
    }
    app.setSection("operations");
  }

  return (
    <main className="workspace-page">
      <WorkspacePageHeader
        eyebrow="Control"
        title="Overview"
        description="Stay focused on product posture, operator blockers, and the long-lived objectives driving automated work. Deep diagnostics now live in Settings / Diagnostics."
        status={
          <>
            <WorkspaceStatusChip tone={attentionItems.length > 0 ? "warning" : "success"}>
              {attentionItems.length > 0 ? `${attentionItems.length} attention items` : "Ready"}
            </WorkspaceStatusChip>
            <WorkspaceStatusChip tone={activeObjectiveCount > 0 ? "accent" : "default"}>
              {activeObjectiveCount} active objectives
            </WorkspaceStatusChip>
            <WorkspaceStatusChip tone={warnings.length > 0 ? "warning" : "default"}>
              {warnings.length} deployment warnings
            </WorkspaceStatusChip>
          </>
        }
        actions={
          <ActionButton
            isDisabled={busy}
            type="button"
            variant="primary"
            onPress={() => void refreshSurface()}
          >
            {busy ? "Refreshing..." : "Refresh overview"}
          </ActionButton>
        }
      />

      <section className="workspace-two-column">
        <NextActionCard
          ctaLabel={app.uiMode === "basic" ? app.t("nav.switchAdvanced") : app.t("nav.switchBasic")}
          description={app.t("overview.modeGuidanceBody")}
          title={app.t("overview.modeGuidanceTitle")}
          onCta={() => app.setUiMode(app.uiMode === "basic" ? "advanced" : "basic")}
        >
          <p className="chat-muted">
            {app.t(
              app.uiMode === "basic" ? "mode.basic.description" : "mode.advanced.description",
            )}
          </p>
        </NextActionCard>
        <OnboardingChecklistCard
          description={app.t("overview.telemetryBody")}
          items={buildTelemetryChecklist(uxAggregate)}
          title={app.t("overview.telemetryTitle")}
        />
      </section>

      <section className="workspace-two-column">
        <TroubleshootingCard
          description={app.uxTelemetryBusy ? "Refreshing current journal-backed UX baseline." : ""}
          items={buildTelemetryFrictionItems(uxAggregate)}
          title={app.t("guidance.troubleshooting")}
        />
        <ScenarioCard
          ctaLabel={app.t("guidance.cta")}
          description={app.t("overview.telemetryBody")}
          title={app.t("guidance.scenario")}
          onCta={() =>
            app.setSection(
              (uxAggregate?.countsByName["ux.approval.resolved"] ?? 0) > 0 ? "approvals" : "chat",
            )
          }
        >
          <dl className="workspace-key-value-grid">
            <div>
              <dt>{app.t("overview.telemetryFunnel")}</dt>
              <dd>{buildFunnelSummary(uxAggregate)}</dd>
            </div>
            <div>
              <dt>{app.t("overview.telemetryApprovals")}</dt>
              <dd>{buildApprovalSummary(uxAggregate)}</dd>
            </div>
            <div>
              <dt>{app.t("overview.telemetryFriction")}</dt>
              <dd>{buildTopFrictionSurface(uxAggregate)}</dd>
            </div>
          </dl>
          <ActionButton type="button" variant="ghost" onPress={() => void app.refreshUxTelemetry()}>
            {app.uxTelemetryBusy ? "Refreshing baseline..." : "Refresh baseline"}
          </ActionButton>
        </ScenarioCard>
      </section>

      <section className="workspace-metric-grid">
        <WorkspaceMetricCard
          detail={attentionItems[0] ?? "No immediate operator blockers are published."}
          label="Runtime posture"
          tone={attentionItems.length > 0 ? "warning" : "success"}
          value={attentionItems.length > 0 ? "Attention required" : "Ready"}
        />
        <WorkspaceMetricCard
          detail={warnings[0] ?? "Remote access posture looks stable."}
          label="Access posture"
          tone={warnings.length > 0 ? "warning" : "default"}
          value={`${readString(deployment ?? {}, "mode") ?? "unknown"} / ${readString(deployment ?? {}, "bind_profile") ?? "n/a"}`}
        />
        <WorkspaceMetricCard
          detail={
            activeObjectiveCount > 0
              ? `${heartbeatCount} heartbeats, ${standingOrderCount} standing orders, ${programCount} programs.`
              : "No active objective products are loaded yet."
          }
          label="Objective layer"
          tone={activeObjectiveCount > 0 ? "accent" : "default"}
          value={activeObjectiveCount}
        />
        <WorkspaceMetricCard
          detail={
            objectiveAttentionCount > 0
              ? "Heartbeat or objective health needs follow-up."
              : "No objective health deviations are loaded."
          }
          label="Objective health"
          tone={objectiveAttentionCount > 0 ? "warning" : "success"}
          value={objectiveAttentionCount > 0 ? "Attention" : "Healthy"}
        />
        <WorkspaceMetricCard
          detail={
            usageInsights === null
              ? "Refresh overview to load routing posture and active alerts."
              : `${usageInsights.routing.default_mode} default mode with ${usageInsights.alerts.length} active alerts.`
          }
          label="Routing posture"
          tone={(usageInsights?.alerts.length ?? 0) > 0 ? "warning" : "default"}
          value={usageInsights?.routing.default_mode ?? "suggest"}
        />
        <WorkspaceMetricCard
          detail={
            pendingApprovals > 0
              ? "Review sensitive actions before they block runs."
              : "Approval queue is clear."
          }
          label="Pending approvals"
          tone={pendingApprovals > 0 ? "warning" : "success"}
          value={pendingApprovals}
        />
        <WorkspaceMetricCard
          detail={
            failedSupportJobs > 0
              ? "Recent bundle jobs failed and may need follow-up."
              : "No failed support jobs are loaded."
          }
          label="Support failures"
          tone={failedSupportJobs > 0 ? "danger" : "default"}
          value={failedSupportJobs}
        />
      </section>

      {attentionItems.length > 0 ? (
        <WorkspaceInlineNotice title="Needs attention" tone={workspaceToneForState("warning")}>
          <ul className="console-compact-list">
            {attentionItems.map((item) => (
              <li key={item}>{item}</li>
            ))}
          </ul>
        </WorkspaceInlineNotice>
      ) : null}

      <section className="workspace-two-column">
        <WorkspaceSectionCard
          description="Current focus, next action, heartbeat cadence, and last activity all live on the same durable board."
          title="Objective board"
        >
          <EntityTable
            ariaLabel="Objective board"
            columns={[
              {
                key: "objective",
                label: "Objective",
                isRowHeader: true,
                render: (row) => (
                  <div className="workspace-stack">
                    <strong>{row.name}</strong>
                    <span className="chat-muted">
                      {row.kind} · {row.currentFocus}
                    </span>
                  </div>
                ),
              },
              {
                key: "state",
                label: "State",
                render: (row) => (
                  <div className="workspace-inline">
                    <WorkspaceStatusChip tone={workspaceToneForState(row.state)}>
                      {row.state}
                    </WorkspaceStatusChip>
                    <WorkspaceStatusChip tone={workspaceToneForState(row.health)}>
                      {row.health}
                    </WorkspaceStatusChip>
                  </div>
                ),
              },
              {
                key: "activity",
                label: "Latest activity",
                render: (row) => (
                  <div className="workspace-stack">
                    <span>{row.lastActivity}</span>
                    <span className="chat-muted">{row.nextRun}</span>
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
                      onPress={() => setSelectedObjectiveId(row.objectiveId)}
                    >
                      Select
                    </ActionButton>
                    {primaryLifecycleAction(row.record) !== null ? (
                      <ActionButton
                        size="sm"
                        isDisabled={busy}
                        onPress={() => {
                          const action = primaryLifecycleAction(row.record);
                          if (action !== null) {
                            void applyLifecycle(row.record, action);
                          }
                        }}
                      >
                        {primaryLifecycleLabel(row.record)}
                      </ActionButton>
                    ) : null}
                    {readString(row.record, "state") !== "archived" ? (
                      <ActionButton
                        variant="secondary"
                        size="sm"
                        isDisabled={busy}
                        onPress={() => void applyLifecycle(row.record, "archive")}
                      >
                        Archive
                      </ActionButton>
                    ) : null}
                  </ActionCluster>
                ),
              },
            ]}
            rows={objectiveRows}
            getRowId={(row) => row.objectiveId}
            emptyTitle="No objectives loaded"
            emptyDescription="Create the first objective, heartbeat, standing order, or program to anchor long-lived automation."
          />
        </WorkspaceSectionCard>

        <WorkspaceSectionCard
          description="Use the selected objective as the bridge into chat, memory, run history, and support workflows."
          title="Objective detail"
        >
          {selectedObjective === null ? (
            <WorkspaceEmptyState
              compact
              description="Select an objective to inspect current focus, output, run linkage, and navigation shortcuts."
              title="No objective selected"
            />
          ) : (
            <div className="workspace-stack">
              <KeyValueList
                items={[
                  {
                    label: "Mode",
                    value: objectiveKindLabel(readString(selectedObjective, "kind")),
                  },
                  {
                    label: "State",
                    value: readString(selectedObjective, "state") ?? "unknown",
                  },
                  {
                    label: "Health",
                    value: objectiveHealthState(selectedObjective),
                  },
                  {
                    label: "Current focus",
                    value:
                      readString(selectedObjective, "current_focus") ??
                      "No current focus recorded.",
                  },
                  {
                    label: "Next action",
                    value:
                      readString(selectedObjective, "next_recommended_step") ??
                      "No next action recorded.",
                  },
                  {
                    label: "Success criteria",
                    value:
                      readString(selectedObjective, "success_criteria") ??
                      "No success criteria recorded.",
                  },
                  {
                    label: "Next run",
                    value: objectiveNextRunSummary(selectedObjective),
                  },
                  {
                    label: "Last output",
                    value:
                      objectiveLastOutput(selectedObjective) ??
                      "No linked run output recorded yet.",
                  },
                ]}
              />
              <WorkspaceInlineNotice
                title="Latest activity"
                tone={workspaceToneForState(objectiveHealthState(selectedObjective))}
              >
                {objectiveActivitySummary(selectedObjective)}
              </WorkspaceInlineNotice>
              <ActionCluster>
                <ActionButton variant="secondary" onPress={openSelectedObjectiveChat}>
                  Open chat
                </ActionButton>
                <ActionButton variant="secondary" onPress={openSelectedObjectiveMemory}>
                  Open memory
                </ActionButton>
                <ActionButton variant="secondary" onPress={openSelectedObjectiveRoutines}>
                  Open automations
                </ActionButton>
                <ActionButton variant="secondary" onPress={openSelectedObjectiveOperations}>
                  Open operations
                </ActionButton>
              </ActionCluster>
            </div>
          )}
        </WorkspaceSectionCard>
      </section>

      <section className="workspace-two-column">
        <WorkspaceSectionCard
          description="Create durable products over the existing routines backend without dropping into low-level schedule plumbing."
          title={showObjectiveEditor ? "New objective product" : "Create objective product"}
        >
          {!showObjectiveEditor ? (
            <WorkspaceEmptyState
              compact
              description="Use objective mode for one durable goal, heartbeat for recurring status, standing order for persistent authority, and program for a multi-step initiative."
              title="Objective editor collapsed"
              action={
                <ActionButton variant="secondary" onPress={() => setShowObjectiveEditor(true)}>
                  Open objective editor
                </ActionButton>
              }
            />
          ) : (
            <AppForm
              onSubmit={(event) => {
                event.preventDefault();
                void saveObjective();
              }}
            >
              <div className="workspace-form-grid">
                <SelectField
                  label="Mode"
                  value={objectiveForm.kind}
                  onChange={(kind) =>
                    setObjectiveForm((current) => ({
                      ...current,
                      kind: kind as ObjectiveKindValue,
                    }))
                  }
                  options={OBJECTIVE_KIND_OPTIONS}
                />
                <TextInputField
                  label="Name"
                  value={objectiveForm.name}
                  onChange={(name) => setObjectiveForm((current) => ({ ...current, name }))}
                  required
                />
                <TextInputField
                  label={
                    objectiveForm.kind === "heartbeat" ? "Heartbeat cadence" : "Schedule phrase"
                  }
                  value={objectiveForm.naturalLanguageSchedule}
                  onChange={(naturalLanguageSchedule) =>
                    setObjectiveForm((current) => ({ ...current, naturalLanguageSchedule }))
                  }
                  placeholder={
                    objectiveForm.kind === "heartbeat"
                      ? "every weekday at 9"
                      : "optional: every weekday at 17"
                  }
                />
                <TextInputField
                  label="Delivery channel"
                  value={objectiveForm.deliveryChannel}
                  onChange={(deliveryChannel) =>
                    setObjectiveForm((current) => ({ ...current, deliveryChannel }))
                  }
                  placeholder="optional: ops:summary"
                />
              </div>
              <TextAreaField
                label="Prompt"
                rows={4}
                value={objectiveForm.prompt}
                onChange={(prompt) => setObjectiveForm((current) => ({ ...current, prompt }))}
                required
              />
              <div className="workspace-form-grid">
                <TextInputField
                  label="Current focus"
                  value={objectiveForm.currentFocus}
                  onChange={(currentFocus) =>
                    setObjectiveForm((current) => ({ ...current, currentFocus }))
                  }
                />
                <TextInputField
                  label="Success criteria"
                  value={objectiveForm.successCriteria}
                  onChange={(successCriteria) =>
                    setObjectiveForm((current) => ({ ...current, successCriteria }))
                  }
                />
                <TextInputField
                  label="Next action"
                  value={objectiveForm.nextRecommendedStep}
                  onChange={(nextRecommendedStep) =>
                    setObjectiveForm((current) => ({ ...current, nextRecommendedStep }))
                  }
                />
                {objectiveForm.kind === "standing_order" ? (
                  <TextInputField
                    label="Standing order"
                    value={objectiveForm.standingOrder}
                    onChange={(standingOrder) =>
                      setObjectiveForm((current) => ({ ...current, standingOrder }))
                    }
                  />
                ) : null}
              </div>
              <SwitchField
                label="Enabled"
                checked={objectiveForm.enabled}
                onChange={(enabled) => setObjectiveForm((current) => ({ ...current, enabled }))}
              />
              <WorkspaceInlineNotice title="Mode guidance" tone="accent">
                {objectiveModeDescription(objectiveForm.kind)}
              </WorkspaceInlineNotice>
              <ActionCluster>
                <ActionButton type="submit" isDisabled={busy}>
                  {busy ? "Saving..." : "Save objective"}
                </ActionButton>
                <ActionButton
                  type="button"
                  variant="secondary"
                  isDisabled={busy}
                  onPress={() => {
                    setObjectiveForm(DEFAULT_OBJECTIVE_FORM);
                    setShowObjectiveEditor(false);
                  }}
                >
                  Close editor
                </ActionButton>
              </ActionCluster>
            </AppForm>
          )}
        </WorkspaceSectionCard>

        <WorkspaceSectionCard
          description="Jump directly to the place that matches the current signal instead of navigating the full dashboard."
          title="Next workspace"
        >
          <div className="workspace-stack">
            <QuickAction
              detail="Continue the active operator conversation."
              label="Open chat"
              onClick={() => app.setSection("chat")}
            />
            <QuickAction
              detail="Process sensitive-action requests."
              label="Review approvals"
              onClick={() => app.setSection("approvals")}
            />
            <QuickAction
              detail="Inspect heartbeats, standing orders, programs, and trigger wiring."
              label="Open automations"
              onClick={() => app.setSection("cron")}
            />
            <QuickAction
              detail="Troubleshoot runtime state, audit, and CLI handoffs."
              label="Open diagnostics"
              onClick={() => app.setSection("operations")}
            />
          </div>
        </WorkspaceSectionCard>
      </section>

      <section className="workspace-two-column">
        <WorkspaceSectionCard
          description="Keep only the high-signal operational posture here; lower-level details live on the dedicated settings pages."
          title="Product posture"
        >
          {deployment === null ? (
            <WorkspaceEmptyState
              compact
              description="Refresh overview to load the current mode, bind profile, auth gates, and objective posture."
              title="No deployment posture loaded"
            />
          ) : (
            <dl className="workspace-key-value-grid">
              <div>
                <dt>Mode</dt>
                <dd>{readString(deployment, "mode") ?? "n/a"}</dd>
              </div>
              <div>
                <dt>Bind profile</dt>
                <dd>{readString(deployment, "bind_profile") ?? "n/a"}</dd>
              </div>
              <div>
                <dt>Admin auth</dt>
                <dd>{readBool(deployment, "admin_auth_required") ? "required" : "unknown"}</dd>
              </div>
              <div>
                <dt>Remote bind</dt>
                <dd>
                  {readBool(deployment, "remote_bind_detected") ? "detected" : "not detected"}
                </dd>
              </div>
              <div>
                <dt>Provider auth</dt>
                <dd>{providerState}</dd>
              </div>
              <div>
                <dt>Objective registry</dt>
                <dd>
                  {readString(objectivesSnapshot ?? {}, "count") ?? String(objectives.length)}
                </dd>
              </div>
              <div>
                <dt>Heartbeats</dt>
                <dd>{heartbeatCount}</dd>
              </div>
              <div>
                <dt>Standing orders</dt>
                <dd>{standingOrderCount}</dd>
              </div>
              <div>
                <dt>Programs</dt>
                <dd>{programCount}</dd>
              </div>
              <div>
                <dt>Usage alerts</dt>
                <dd>{usageInsights?.alerts.length ?? 0}</dd>
              </div>
              <div>
                <dt>Degraded connectors</dt>
                <dd>{connectorDegraded}</dd>
              </div>
            </dl>
          )}
        </WorkspaceSectionCard>
      </section>
    </main>
  );
}

function QuickAction({
  label,
  detail,
  onClick,
}: {
  label: string;
  detail: string;
  onClick: () => void;
}) {
  return (
    <ActionButton
      className="workspace-action-button"
      fullWidth
      type="button"
      variant="ghost"
      onPress={onClick}
    >
      <span className="flex flex-col items-start gap-1 text-left">
        <strong>{label}</strong>
        <span>{detail}</span>
      </span>
    </ActionButton>
  );
}

function buildAttentionItems({
  warnings,
  pendingApprovals,
  failedSupportJobs,
  connectorDegraded,
  providerState,
  alertCount,
  objectiveAttentionCount,
}: {
  warnings: string[];
  pendingApprovals: number;
  failedSupportJobs: number;
  connectorDegraded: number;
  providerState: string;
  alertCount: number;
  objectiveAttentionCount: number;
}): string[] {
  const items = [...warnings];
  if (pendingApprovals > 0) items.push(`${pendingApprovals} approvals waiting for review.`);
  if (failedSupportJobs > 0) items.push(`${failedSupportJobs} support bundle jobs failed.`);
  if (connectorDegraded > 0) items.push(`${connectorDegraded} connectors are degraded.`);
  if (alertCount > 0) items.push(`${alertCount} usage governance alerts are active.`);
  if (objectiveAttentionCount > 0) {
    items.push(`${objectiveAttentionCount} objectives need health follow-up.`);
  }
  if (providerState === "degraded" || providerState === "expired" || providerState === "missing") {
    items.push(`Provider auth state is ${providerState}.`);
  }
  return items;
}

function objectiveKindLabel(kind: string | null): string {
  switch (kind) {
    case "heartbeat":
      return "Heartbeat";
    case "standing_order":
      return "Standing order";
    case "program":
      return "Program";
    case "objective":
    default:
      return "Objective";
  }
}

function objectiveHealthState(objective: JsonObject): string {
  return (
    readString(readObject(objective, "health") ?? {}, "state") ??
    readString(objective, "state") ??
    "unknown"
  );
}

function objectiveNextRunSummary(objective: JsonObject): string {
  const linkedRoutine = readObject(objective, "linked_routine");
  const nextRunAt = readNumber(linkedRoutine ?? {}, "next_run_at_unix_ms");
  if (nextRunAt !== null) {
    return `Next run ${formatUnixMs(nextRunAt)}`;
  }
  const schedulePayload = readObject(readObject(objective, "automation") ?? {}, "schedule_payload");
  const rawSchedule = readString(schedulePayload ?? {}, "raw");
  if (rawSchedule !== null) {
    return `Schedule ${rawSchedule}`;
  }
  return "No scheduled run";
}

function objectiveLastOutput(objective: JsonObject): string | null {
  const lastRun = readObject(objective, "last_run") ?? {};
  return (
    readString(lastRun, "outcome_message") ??
    readString(readObject(objective, "last_attempt") ?? {}, "summary")
  );
}

function objectiveActivitySummary(objective: JsonObject): string {
  const lastAttempt = readObject(objective, "last_attempt");
  if (lastAttempt !== null) {
    const status = readString(lastAttempt, "status") ?? "attempt";
    const summary = readString(lastAttempt, "summary") ?? "No attempt summary.";
    return `${status} · ${summary}`;
  }
  const lifecycleHistory = objective["lifecycle_history"];
  if (Array.isArray(lifecycleHistory)) {
    const latest = lifecycleHistory.find(isJsonObject);
    if (latest !== undefined) {
      const action = readString(latest, "action") ?? "updated";
      const reason = readString(latest, "reason") ?? "No lifecycle reason recorded.";
      return `${action} · ${reason}`;
    }
  }
  return "No activity recorded yet.";
}

function primaryLifecycleAction(objective: JsonObject): LifecycleAction | null {
  const state = readString(objective, "state");
  switch (state) {
    case "active":
      return "pause";
    case "paused":
    case "draft":
      return "resume";
    case "cancelled":
    case "archived":
      return null;
    default:
      return "fire";
  }
}

function primaryLifecycleLabel(objective: JsonObject): string {
  const action = primaryLifecycleAction(objective);
  switch (action) {
    case "pause":
      return "Pause";
    case "resume":
      return "Resume";
    case "fire":
      return "Fire";
    default:
      return "Select";
  }
}

function actionLabel(action: LifecycleAction): string {
  switch (action) {
    case "fire":
      return "fired";
    case "pause":
      return "paused";
    case "resume":
      return "resumed";
    case "archive":
      return "archived";
  }
}

function objectiveModeDescription(kind: ObjectiveKindValue): string {
  switch (kind) {
    case "heartbeat":
      return "Heartbeat is a first-class recurring status product: give it a cadence, an output channel, and a focused summary contract.";
    case "standing_order":
      return "Standing order captures durable authority with visible guardrails, approvals, and a concrete next-action contract.";
    case "program":
      return "Program wraps a longer multi-step initiative over the existing routines backend instead of introducing a second orchestration engine.";
    case "objective":
    default:
      return "Objective is the generic durable goal layer: define current focus, success criteria, and the next recommended step.";
  }
}

function buildTelemetryChecklist(aggregate: ConsoleAppState["uxTelemetryAggregate"]): string[] {
  if (aggregate === null || aggregate.totalEvents === 0) {
    return ["Console baseline is waiting for the first journal-backed UX events."];
  }
  return [
    `Session starts recorded: ${aggregate.funnel.setup_started}`,
    `First prompts recorded: ${aggregate.funnel.first_prompt_sent}`,
    `Approvals resolved: ${aggregate.funnel.first_approval_resolved}`,
    `Runs inspected: ${aggregate.funnel.first_run_inspected}`,
  ];
}

function buildTelemetryFrictionItems(aggregate: ConsoleAppState["uxTelemetryAggregate"]): string[] {
  if (aggregate === null || aggregate.totalEvents === 0) {
    return ["No friction events recorded yet."];
  }
  return [
    `Web friction events: ${aggregate.frictionBySurface.web}`,
    `Desktop friction events: ${aggregate.frictionBySurface.desktop}`,
    `TUI friction events: ${aggregate.frictionBySurface.tui}`,
    `Mobile friction events: ${aggregate.frictionBySurface.mobile}`,
  ];
}

function buildFunnelSummary(aggregate: ConsoleAppState["uxTelemetryAggregate"]): string {
  if (aggregate === null || aggregate.totalEvents === 0) {
    return "No UX baseline events recorded yet.";
  }
  return `${aggregate.funnel.setup_started} started · ${aggregate.funnel.first_prompt_sent} prompted · ${aggregate.funnel.first_run_inspected} inspected`;
}

function buildApprovalSummary(aggregate: ConsoleAppState["uxTelemetryAggregate"]): string {
  if (aggregate === null || Object.keys(aggregate.approvalFatigueByTool).length === 0) {
    return "No approval fatigue signal yet.";
  }
  const [toolName, count] =
    Object.entries(aggregate.approvalFatigueByTool).sort((left, right) => right[1] - left[1])[0] ??
    ["unknown", 0];
  return `${toolName} requested ${count} approval${count === 1 ? "" : "s"}.`;
}

function buildTopFrictionSurface(aggregate: ConsoleAppState["uxTelemetryAggregate"]): string {
  if (aggregate === null || aggregate.totalEvents === 0) {
    return "No friction signal yet.";
  }
  const [surface, count] =
    Object.entries(aggregate.frictionBySurface).sort((left, right) => right[1] - left[1])[0] ??
    ["web", 0];
  return count === 0 ? "No blocked or error outcomes recorded." : `${surface} (${count})`;
}

function buildObjectivePayload(
  form: ObjectiveEditorForm,
): Record<string, string | boolean | undefined> {
  return {
    kind: form.kind,
    name: form.name.trim(),
    prompt: form.prompt.trim(),
    current_focus: emptyToUndefined(form.currentFocus),
    success_criteria: emptyToUndefined(form.successCriteria),
    next_recommended_step: emptyToUndefined(form.nextRecommendedStep),
    standing_order:
      form.kind === "standing_order" ? emptyToUndefined(form.standingOrder) : undefined,
    enabled: form.enabled,
    natural_language_schedule: emptyToUndefined(form.naturalLanguageSchedule),
    delivery_mode: form.deliveryChannel.trim().length > 0 ? "specific_channel" : "same_channel",
    delivery_channel: emptyToUndefined(form.deliveryChannel),
    approval_mode:
      form.kind === "standing_order" || form.kind === "program" ? "before_first_run" : "none",
    template_id: form.kind === "heartbeat" ? "heartbeat" : undefined,
  };
}
