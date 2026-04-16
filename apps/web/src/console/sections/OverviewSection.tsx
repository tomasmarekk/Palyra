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
import { pseudoLocalizeText } from "../i18n";
import {
  buildObjectiveChatHref,
  objectiveWorkspaceDocumentPath,
  resolveObjectiveId,
} from "../objectiveLinks";
import { findSectionByPath, getSectionPath } from "../navigation";
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
import type {
  OnboardingPostureEnvelope,
  OnboardingStepAction,
  OnboardingStepView,
  ToolPermissionRecord,
  ToolPermissionsEnvelope,
} from "../../consoleApi";
import { readFirstSuccessCompleted } from "../../chat/firstSuccessState";
import { FIRST_SUCCESS_PROMPTS, queueChatStarterPrompt } from "../../chat/starterPrompts";
import { readGuidanceHidden, writeGuidanceHidden } from "../guidancePreferences";
import type { ConsoleAppState } from "../useConsoleAppState";

type OverviewSectionProps = {
  app: Pick<
    ConsoleAppState,
    | "api"
    | "overviewBusy"
    | "overviewDeployment"
    | "overviewOnboarding"
    | "overviewOnboardingFlow"
    | "overviewApprovals"
    | "overviewDiagnostics"
    | "overviewToolPermissions"
    | "overviewUsageInsights"
    | "overviewSupportJobs"
    | "refreshOverview"
    | "locale"
    | "selectOverviewOnboardingFlow"
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

const OVERVIEW_MESSAGES = {
  "header.title": "Overview",
  "header.description":
    "Stay focused on product posture, operator blockers, and the long-lived objectives driving automated work. Deep diagnostics now live in Settings / Diagnostics.",
  "status.attentionItems": "{count} attention items",
  "status.ready": "Ready",
  "status.activeObjectives": "{count} active objectives",
  "status.deploymentWarnings": "{count} deployment warnings",
  "action.refreshing": "Refreshing...",
  "action.refreshOverview": "Refresh overview",
  "guidance.show": "Show guidance",
  "guidance.hidden.title": "Guidance hidden",
  "guidance.hidden.description":
    "Starter prompts, onboarding cards, and runtime repair hints are currently hidden.",
  "guidance.hidden.body":
    "Reopen the guidance surface whenever you want the recommended next action, blocker repairs, or first-success prompts.",
  "guidance.currentTrack": "Current track",
  "onboarding.nextStep": "Next onboarding step",
  "onboarding.noRecommendation":
    "The control plane has not published a recommended onboarding step yet.",
  "onboarding.flow": "Flow: {flow}. Status: {status}.",
  "onboarding.trackSummary": "{track} Required: {required}. Optional: {optional}.",
  "onboarding.quickStart": "Quick Start",
  "onboarding.advancedSetup": "Advanced setup",
  "onboarding.hideGuidance": "Hide guidance",
  "onboarding.checklist": "Onboarding checklist",
  "troubleshooting.title": "Troubleshooting",
  "scenario.firstSuccess": "First success",
  "scenario.openChat": "Open chat",
  "scenario.reviewNextStep": "Review next step",
  "scenario.readyDescription": "Open chat and validate the first end-to-end operator task.",
  "scenario.finishSteps":
    "Finish the remaining guided steps, then use a starter prompt to verify the workspace.",
  "scenario.reviewApprovals": "Review approvals",
  "scenario.inspectDiagnostics": "Inspect diagnostics",
  "scenario.openSessions": "Open sessions",
  "scenario.toolRecommendation": "Tool posture recommendation",
  "scenario.openToolPermissions": "Open tool permissions",
  "scenario.completed": "Completed",
  "scenario.remaining": "Remaining",
  "scenario.telemetryFriction": "Telemetry friction",
  "scenario.openDiagnostics": "Open diagnostics",
  "scenario.switchAdvanced": "Switch to advanced",
  "scenario.refreshBaseline": "Refresh baseline",
  "scenario.refreshingBaseline": "Refreshing baseline...",
  "metric.runtimePosture": "Runtime posture",
  "metric.noImmediateBlockers": "No immediate operator blockers are published.",
  "metric.attentionRequired": "Attention required",
  "metric.accessPosture": "Access posture",
  "metric.remoteStable": "Remote access posture looks stable.",
  "metric.objectiveLayer": "Objective layer",
  "metric.noObjectives": "No active objective products are loaded yet.",
  "metric.objectiveHealth": "Objective health",
  "metric.objectiveHealthNeedsFollowUp": "Heartbeat or objective health needs follow-up.",
  "metric.objectiveHealthClear": "No objective health deviations are loaded.",
  "metric.attention": "Attention",
  "metric.healthy": "Healthy",
} as const;

type OverviewMessageKey = keyof typeof OVERVIEW_MESSAGES;

const OVERVIEW_MESSAGES_CS: Readonly<Record<OverviewMessageKey, string>> = {
  "header.title": "Přehled",
  "header.description":
    "Soustřeď se na produktovou posturu, operátorské blokery a dlouhodobé objective, které pohánějí automatizovanou práci. Hluboká diagnostika teď žije v Nastavení / Diagnostika.",
  "status.attentionItems": "{count} položek vyžaduje pozornost",
  "status.ready": "Připraveno",
  "status.activeObjectives": "{count} aktivních objectives",
  "status.deploymentWarnings": "{count} varování nasazení",
  "action.refreshing": "Obnovuji...",
  "action.refreshOverview": "Obnovit přehled",
  "guidance.show": "Zobrazit guidance",
  "guidance.hidden.title": "Guidance skryta",
  "guidance.hidden.description":
    "Starter prompty, onboarding karty a hinty pro opravu runtime jsou momentálně skryté.",
  "guidance.hidden.body":
    "Kdykoli chceš doporučený další krok, opravu blockerů nebo prompty pro první úspěch, znovu otevři guidance surface.",
  "guidance.currentTrack": "Aktuální track",
  "onboarding.nextStep": "Další onboarding krok",
  "onboarding.noRecommendation": "Control plane zatím nepublikovala doporučený onboarding krok.",
  "onboarding.flow": "Flow: {flow}. Stav: {status}.",
  "onboarding.trackSummary": "{track} Povinné: {required}. Volitelné: {optional}.",
  "onboarding.quickStart": "Quick Start",
  "onboarding.advancedSetup": "Pokročilé nastavení",
  "onboarding.hideGuidance": "Skrýt guidance",
  "onboarding.checklist": "Onboarding checklist",
  "troubleshooting.title": "Řešení problémů",
  "scenario.firstSuccess": "První úspěch",
  "scenario.openChat": "Otevřít chat",
  "scenario.reviewNextStep": "Zkontrolovat další krok",
  "scenario.readyDescription": "Otevři chat a ověř první end-to-end operátorský úkol.",
  "scenario.finishSteps": "Dokonči zbývající guided kroky a potom starter promptem ověř workspace.",
  "scenario.reviewApprovals": "Zkontrolovat schválení",
  "scenario.inspectDiagnostics": "Zkontrolovat diagnostiku",
  "scenario.openSessions": "Otevřít relace",
  "scenario.toolRecommendation": "Doporučení pro posture nástroje",
  "scenario.openToolPermissions": "Otevřít oprávnění nástrojů",
  "scenario.completed": "Dokončeno",
  "scenario.remaining": "Zbývá",
  "scenario.telemetryFriction": "Tření v telemetrii",
  "scenario.openDiagnostics": "Otevřít diagnostiku",
  "scenario.switchAdvanced": "Přepnout na pokročilé",
  "scenario.refreshBaseline": "Obnovit baseline",
  "scenario.refreshingBaseline": "Obnovuji baseline...",
  "metric.runtimePosture": "Postura runtime",
  "metric.noImmediateBlockers": "Nejsou publikované žádné okamžité operátorské blokery.",
  "metric.attentionRequired": "Vyžaduje pozornost",
  "metric.accessPosture": "Postura přístupu",
  "metric.remoteStable": "Postura vzdáleného přístupu vypadá stabilně.",
  "metric.objectiveLayer": "Vrstva objectives",
  "metric.noObjectives": "Zatím nejsou načtené žádné aktivní objective produkty.",
  "metric.objectiveHealth": "Zdraví objectives",
  "metric.objectiveHealthNeedsFollowUp":
    "Zdraví heartbeat nebo objective vyžaduje navazující kontrolu.",
  "metric.objectiveHealthClear": "Nejsou načtené žádné odchylky ve zdraví objectives.",
  "metric.attention": "Pozornost",
  "metric.healthy": "Zdravé",
};

function translateOverview(
  locale: ConsoleAppState["locale"],
  key: OverviewMessageKey,
  variables?: Record<string, string | number>,
): string {
  const template = (locale === "cs" ? OVERVIEW_MESSAGES_CS : OVERVIEW_MESSAGES)[key];
  const resolved =
    variables === undefined
      ? template
      : template.replaceAll(/\{([a-zA-Z0-9_]+)\}/g, (_, name) => `${variables[name] ?? ""}`);
  return locale === "qps-ploc" ? pseudoLocalizeText(resolved) : resolved;
}

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
  const tOverview = (
    key: OverviewMessageKey,
    variables?: Record<string, string | number>,
  ): string => translateOverview(app.locale, key, variables);
  const navigate = useNavigate();
  const [searchParams] = useSearchParams();
  const [guidanceHidden, setGuidanceHidden] = useState(() => readGuidanceHidden("overview"));
  const preferredObjectiveId = searchParams.get("objectiveId");
  const [objectivesBusy, setObjectivesBusy] = useState(false);
  const [objectiveMutationBusy, setObjectiveMutationBusy] = useState(false);
  const [showObjectiveEditor, setShowObjectiveEditor] = useState(false);
  const [objectiveForm, setObjectiveForm] = useState<ObjectiveEditorForm>(DEFAULT_OBJECTIVE_FORM);
  const [objectives, setObjectives] = useState<JsonObject[]>([]);
  const [selectedObjectiveId, setSelectedObjectiveId] = useState("");

  const deployment = app.overviewDeployment;
  const onboarding = app.overviewOnboarding;
  const diagnostics = app.overviewDiagnostics;
  const toolPermissions = app.overviewToolPermissions;
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
  const onboardingSteps = onboarding?.steps ?? [];
  const recommendedOnboardingStep =
    onboardingSteps.find((step) => step.step_id === onboarding?.recommended_step_id) ??
    onboardingSteps.find((step) => step.status !== "done" && step.status !== "skipped") ??
    null;
  const onboardingChecklistItems = buildOnboardingChecklist(onboarding);
  const uxAggregate = app.uxTelemetryAggregate;
  const onboardingTroubleshootingItems = buildOnboardingTroubleshootingItems(
    onboarding,
    uxAggregate,
  );
  const firstSuccessPrompts = buildFirstSuccessPrompts(onboarding, readFirstSuccessCompleted());
  const topToolRecommendation = useMemo(
    () => buildTopToolRecommendation(toolPermissions),
    [toolPermissions],
  );
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
    await Promise.all([
      app.refreshOverview({ onboardingFlow: app.overviewOnboardingFlow }),
      loadObjectives(),
    ]);
  }

  function updateGuidanceHidden(hidden: boolean): void {
    setGuidanceHidden(hidden);
    writeGuidanceHidden("overview", hidden);
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
        title={tOverview("header.title")}
        description={tOverview("header.description")}
        status={
          <>
            <WorkspaceStatusChip tone={attentionItems.length > 0 ? "warning" : "success"}>
              {attentionItems.length > 0
                ? tOverview("status.attentionItems", { count: attentionItems.length })
                : tOverview("status.ready")}
            </WorkspaceStatusChip>
            <WorkspaceStatusChip tone={activeObjectiveCount > 0 ? "accent" : "default"}>
              {tOverview("status.activeObjectives", { count: activeObjectiveCount })}
            </WorkspaceStatusChip>
            <WorkspaceStatusChip tone={warnings.length > 0 ? "warning" : "default"}>
              {tOverview("status.deploymentWarnings", { count: warnings.length })}
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
            {busy ? tOverview("action.refreshing") : tOverview("action.refreshOverview")}
          </ActionButton>
        }
      />

      {guidanceHidden ? (
        <section className="workspace-two-column">
          <NextActionCard
            ctaLabel={tOverview("guidance.show")}
            description={tOverview("guidance.hidden.description")}
            title={tOverview("guidance.hidden.title")}
            onCta={() => updateGuidanceHidden(false)}
          >
            <p className="chat-muted">{tOverview("guidance.hidden.body")}</p>
          </NextActionCard>
          <TroubleshootingCard
            description="The current onboarding track stays visible even while the guidance cards are collapsed."
            items={[
              `Track: ${labelForOnboardingTrack(app.overviewOnboardingFlow)}`,
              `${countRequiredOnboardingSteps(onboarding)} required steps`,
              `${countOptionalOnboardingSteps(onboarding)} optional steps`,
            ]}
            title={tOverview("guidance.currentTrack")}
          />
        </section>
      ) : (
        <>
          <section className="workspace-two-column">
            <NextActionCard
              ctaLabel={recommendedOnboardingStep?.action?.label ?? "Refresh overview"}
              description={describeOnboardingPosture(onboarding)}
              title={tOverview("onboarding.nextStep")}
              onCta={() => {
                if (recommendedOnboardingStep?.action !== undefined) {
                  executeOnboardingAction(recommendedOnboardingStep.action, {
                    navigate,
                    setNotice: app.setNotice,
                    setSection: app.setSection,
                  });
                  return;
                }
                void refreshSurface();
              }}
            >
              <div className="grid gap-2">
                <p className="chat-muted">
                  {recommendedOnboardingStep?.summary ?? tOverview("onboarding.noRecommendation")}
                </p>
                {recommendedOnboardingStep?.blocked !== undefined ? (
                  <p className="chat-muted">
                    {recommendedOnboardingStep.blocked.detail} Repair:{" "}
                    {recommendedOnboardingStep.blocked.repair_hint}
                  </p>
                ) : null}
                <p className="chat-muted">
                  {tOverview("onboarding.flow", {
                    flow: formatOnboardingVariant(onboarding?.flow_variant),
                    status: formatOnboardingStatus(onboarding?.status),
                  })}
                </p>
                <p className="chat-muted">
                  {tOverview("onboarding.trackSummary", {
                    track: describeOnboardingTrack(app.overviewOnboardingFlow, deployment),
                    required: countRequiredOnboardingSteps(onboarding),
                    optional: countOptionalOnboardingSteps(onboarding),
                  })}
                </p>
                <div className="workspace-inline-actions">
                  <ActionButton
                    type="button"
                    variant={app.overviewOnboardingFlow === "quickstart" ? "primary" : "ghost"}
                    onPress={() => void app.selectOverviewOnboardingFlow("quickstart")}
                  >
                    {tOverview("onboarding.quickStart")}
                  </ActionButton>
                  <ActionButton
                    type="button"
                    variant={app.overviewOnboardingFlow !== "quickstart" ? "primary" : "ghost"}
                    onPress={() =>
                      void app.selectOverviewOnboardingFlow(
                        isRemoteDeployment(deployment) ? "remote" : "manual",
                      )
                    }
                  >
                    {tOverview("onboarding.advancedSetup")}
                  </ActionButton>
                  <ActionButton
                    type="button"
                    variant="ghost"
                    onPress={() => updateGuidanceHidden(true)}
                  >
                    {tOverview("onboarding.hideGuidance")}
                  </ActionButton>
                </div>
              </div>
            </NextActionCard>
            <OnboardingChecklistCard
              description={describeOnboardingChecklist(onboarding)}
              items={onboardingChecklistItems}
              title={tOverview("onboarding.checklist")}
            />
          </section>

          <section className="workspace-two-column">
            <TroubleshootingCard
              description={
                onboarding?.counts.blocked
                  ? `${onboarding.counts.blocked} onboarding blocker${onboarding.counts.blocked === 1 ? "" : "s"} currently need repair.`
                  : app.uxTelemetryBusy
                    ? "Refreshing current journal-backed UX baseline."
                    : "No hard onboarding blockers detected; telemetry still shows where operators hit friction."
              }
              items={onboardingTroubleshootingItems}
              title={tOverview("troubleshooting.title")}
            />
            <ScenarioCard
              ctaLabel={
                onboarding?.ready_for_first_success
                  ? tOverview("scenario.openChat")
                  : tOverview("scenario.reviewNextStep")
              }
              description={
                onboarding?.ready_for_first_success
                  ? (onboarding.first_success_hint ?? tOverview("scenario.readyDescription"))
                  : tOverview("scenario.finishSteps")
              }
              title={tOverview("scenario.firstSuccess")}
              onCta={() => {
                if (onboarding?.ready_for_first_success) {
                  app.setSection("chat");
                  void navigate(getSectionPath("chat"));
                  return;
                }
                if (recommendedOnboardingStep?.action !== undefined) {
                  executeOnboardingAction(recommendedOnboardingStep.action, {
                    navigate,
                    setNotice: app.setNotice,
                    setSection: app.setSection,
                  });
                }
              }}
            >
              {onboarding?.ready_for_first_success ? (
                <div className="grid gap-3">
                  <div className="workspace-inline-actions">
                    {firstSuccessPrompts.map((prompt) => (
                      <ActionButton
                        key={prompt}
                        type="button"
                        variant="secondary"
                        onPress={() => {
                          queueChatStarterPrompt(prompt);
                          app.setSection("chat");
                          void navigate(getSectionPath("chat"));
                        }}
                      >
                        {prompt}
                      </ActionButton>
                    ))}
                  </div>
                  <div className="workspace-inline-actions">
                    <ActionButton
                      type="button"
                      variant="ghost"
                      onPress={() => {
                        app.setSection("approvals");
                        void navigate(getSectionPath("approvals"));
                      }}
                    >
                      {tOverview("scenario.reviewApprovals")}
                    </ActionButton>
                    <ActionButton
                      type="button"
                      variant="ghost"
                      onPress={() => {
                        app.setSection("operations");
                        void navigate(getSectionPath("operations"));
                      }}
                    >
                      {tOverview("scenario.inspectDiagnostics")}
                    </ActionButton>
                    <ActionButton
                      type="button"
                      variant="ghost"
                      onPress={() => {
                        app.setSection("chat");
                        void navigate(getSectionPath("chat"));
                      }}
                    >
                      {tOverview("scenario.openSessions")}
                    </ActionButton>
                  </div>
                  <dl className="workspace-key-value-grid">
                    <div>
                      <dt>{app.t("overview.telemetryFunnel")}</dt>
                      <dd>{buildFunnelSummary(uxAggregate)}</dd>
                    </div>
                    <div>
                      <dt>{app.t("overview.telemetryApprovals")}</dt>
                      <dd>{buildApprovalSummary(uxAggregate, toolPermissions)}</dd>
                    </div>
                    <div>
                      <dt>{app.t("overview.telemetryFriction")}</dt>
                      <dd>{buildTopFrictionSurface(uxAggregate)}</dd>
                    </div>
                  </dl>
                  {topToolRecommendation !== null && (
                    <WorkspaceInlineNotice
                      title={tOverview("scenario.toolRecommendation")}
                      tone="warning"
                    >
                      {topToolRecommendation.recommendation.reason}
                    </WorkspaceInlineNotice>
                  )}
                  {topToolRecommendation !== null && (
                    <div className="workspace-inline-actions">
                      <ActionButton
                        type="button"
                        variant="ghost"
                        onPress={() => {
                          app.setSection("approvals");
                          void navigate(
                            `${getSectionPath("approvals")}?${new URLSearchParams([
                              ["tool", topToolRecommendation.tool_name],
                            ]).toString()}`,
                          );
                        }}
                      >
                        {tOverview("scenario.openToolPermissions")}
                      </ActionButton>
                    </div>
                  )}
                </div>
              ) : (
                <div className="grid gap-3">
                  <dl className="workspace-key-value-grid">
                    <div>
                      <dt>{tOverview("scenario.completed")}</dt>
                      <dd>{onboarding?.counts.done ?? 0}</dd>
                    </div>
                    <div>
                      <dt>{tOverview("scenario.remaining")}</dt>
                      <dd>{remainingOnboardingSteps(onboarding)}</dd>
                    </div>
                    <div>
                      <dt>{tOverview("scenario.telemetryFriction")}</dt>
                      <dd>{buildTopFrictionSurface(uxAggregate)}</dd>
                    </div>
                  </dl>
                  <div className="workspace-inline-actions">
                    <ActionButton
                      type="button"
                      variant="ghost"
                      onPress={() => {
                        app.setSection("operations");
                        void navigate(getSectionPath("operations"));
                      }}
                    >
                      {tOverview("scenario.openDiagnostics")}
                    </ActionButton>
                    <ActionButton
                      type="button"
                      variant="ghost"
                      onPress={() =>
                        void app.selectOverviewOnboardingFlow(
                          isRemoteDeployment(deployment) ? "remote" : "manual",
                        )
                      }
                    >
                      {tOverview("scenario.switchAdvanced")}
                    </ActionButton>
                  </div>
                </div>
              )}
              <ActionButton
                type="button"
                variant="ghost"
                onPress={() => void app.refreshUxTelemetry()}
              >
                {app.uxTelemetryBusy
                  ? tOverview("scenario.refreshingBaseline")
                  : tOverview("scenario.refreshBaseline")}
              </ActionButton>
            </ScenarioCard>
          </section>
        </>
      )}

      <section className="workspace-metric-grid">
        <WorkspaceMetricCard
          detail={attentionItems[0] ?? tOverview("metric.noImmediateBlockers")}
          label={tOverview("metric.runtimePosture")}
          tone={attentionItems.length > 0 ? "warning" : "success"}
          value={
            attentionItems.length > 0
              ? tOverview("metric.attentionRequired")
              : tOverview("status.ready")
          }
        />
        <WorkspaceMetricCard
          detail={warnings[0] ?? tOverview("metric.remoteStable")}
          label={tOverview("metric.accessPosture")}
          tone={warnings.length > 0 ? "warning" : "default"}
          value={`${readString(deployment ?? {}, "mode") ?? "unknown"} / ${readString(deployment ?? {}, "bind_profile") ?? "n/a"}`}
        />
        <WorkspaceMetricCard
          detail={
            activeObjectiveCount > 0
              ? `${heartbeatCount} heartbeats, ${standingOrderCount} standing orders, ${programCount} programs.`
              : tOverview("metric.noObjectives")
          }
          label={tOverview("metric.objectiveLayer")}
          tone={activeObjectiveCount > 0 ? "accent" : "default"}
          value={activeObjectiveCount}
        />
        <WorkspaceMetricCard
          detail={
            objectiveAttentionCount > 0
              ? tOverview("metric.objectiveHealthNeedsFollowUp")
              : tOverview("metric.objectiveHealthClear")
          }
          label={tOverview("metric.objectiveHealth")}
          tone={objectiveAttentionCount > 0 ? "warning" : "success"}
          value={
            objectiveAttentionCount > 0
              ? tOverview("metric.attention")
              : tOverview("metric.healthy")
          }
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

function describeOnboardingPosture(posture: OnboardingPostureEnvelope | null): string {
  if (posture === null) {
    return "Waiting for the control plane to publish onboarding posture.";
  }
  if (posture.ready_for_first_success) {
    return "Required onboarding steps are complete. Validate the first real operator workflow now.";
  }
  if (posture.counts.blocked > 0) {
    return "At least one onboarding step is blocked and needs repair before first success.";
  }
  return `Flow ${formatOnboardingVariant(posture.flow_variant)} is in progress with ${remainingOnboardingSteps(posture)} required step${remainingOnboardingSteps(posture) === 1 ? "" : "s"} remaining.`;
}

function describeOnboardingChecklist(posture: OnboardingPostureEnvelope | null): string {
  if (posture === null) {
    return "The checklist will appear after the onboarding posture loads.";
  }
  return `${posture.counts.done} done, ${remainingOnboardingSteps(posture)} remaining, ${posture.counts.blocked} blocked.`;
}

function buildOnboardingChecklist(posture: OnboardingPostureEnvelope | null): string[] {
  if (posture === null) {
    return ["Loading onboarding posture from the control plane."];
  }
  if (posture.steps.length === 0) {
    return ["No onboarding steps are currently published."];
  }
  return posture.steps.map((step) => {
    const base = `${formatOnboardingStepStatus(step.status)} ${step.title}`;
    return step.optional ? `${base} (optional)` : base;
  });
}

function buildOnboardingTroubleshootingItems(
  posture: OnboardingPostureEnvelope | null,
  aggregate: ConsoleAppState["uxTelemetryAggregate"],
): string[] {
  const blockedItems =
    posture?.steps
      .filter((step) => step.blocked !== undefined)
      .map((step) => `${step.title}: ${step.blocked?.repair_hint ?? step.summary}`) ?? [];
  if (blockedItems.length > 0) {
    return blockedItems;
  }
  return buildTelemetryFrictionItems(aggregate);
}

function buildFirstSuccessPrompts(
  posture: OnboardingPostureEnvelope | null,
  completed: boolean,
): readonly string[] {
  if (posture?.ready_for_first_success && !completed) {
    return FIRST_SUCCESS_PROMPTS;
  }
  return [];
}

function labelForOnboardingTrack(flow: ConsoleAppState["overviewOnboardingFlow"]): string {
  return flow === "quickstart" ? "Quick Start" : "Advanced setup";
}

function describeOnboardingTrack(
  flow: ConsoleAppState["overviewOnboardingFlow"],
  deployment: JsonObject | null,
): string {
  if (flow === "quickstart") {
    return "Quick Start keeps the path narrow: config, provider, verification, and first success.";
  }
  return isRemoteDeployment(deployment)
    ? "Advanced setup follows the remote-safe branch with access posture and verification before handoff."
    : "Advanced setup exposes workspace, access posture, and deeper control-plane preparation before the first run.";
}

function countRequiredOnboardingSteps(posture: OnboardingPostureEnvelope | null): number {
  return posture?.steps.filter((step) => !step.optional).length ?? 0;
}

function countOptionalOnboardingSteps(posture: OnboardingPostureEnvelope | null): number {
  return posture?.steps.filter((step) => step.optional).length ?? 0;
}

function isRemoteDeployment(deployment: JsonObject | null): boolean {
  return readString(deployment ?? {}, "mode") === "remote_vps";
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

function buildApprovalSummary(
  aggregate: ConsoleAppState["uxTelemetryAggregate"],
  permissions: ToolPermissionsEnvelope | null,
): string {
  const topRecommendation = buildTopToolRecommendation(permissions);
  if (topRecommendation !== null) {
    return `${topRecommendation.tool_name} triggered ${topRecommendation.recommendation.approvals_14d} approvals in 14 days. Recommended: ${toolStateLabel(topRecommendation.recommendation.current_state)} -> ${toolStateLabel(topRecommendation.recommendation.recommended_state)}.`;
  }
  if (aggregate === null || Object.keys(aggregate.approvalFatigueByTool).length === 0) {
    return permissions === null
      ? "No approval fatigue signal yet."
      : `${permissions.summary.approval_requests_14d} approval requests recorded in the last 14 days.`;
  }
  const [toolName, count] = Object.entries(aggregate.approvalFatigueByTool).sort(
    (left, right) => right[1] - left[1],
  )[0] ?? ["unknown", 0];
  return `${toolName} requested ${count} approval${count === 1 ? "" : "s"}.`;
}

function buildTopFrictionSurface(aggregate: ConsoleAppState["uxTelemetryAggregate"]): string {
  if (aggregate === null || aggregate.totalEvents === 0) {
    return "No friction signal yet.";
  }
  const [surface, count] = Object.entries(aggregate.frictionBySurface).sort(
    (left, right) => right[1] - left[1],
  )[0] ?? ["web", 0];
  return count === 0 ? "No blocked or error outcomes recorded." : `${surface} (${count})`;
}

function buildTopToolRecommendation(
  permissions: ToolPermissionsEnvelope | null,
):
  | (ToolPermissionRecord & { recommendation: NonNullable<ToolPermissionRecord["recommendation"]> })
  | null {
  if (permissions === null) {
    return null;
  }
  const candidates = permissions.tools.filter(
    (
      tool,
    ): tool is ToolPermissionRecord & {
      recommendation: NonNullable<ToolPermissionRecord["recommendation"]>;
    } => tool.recommendation !== undefined && tool.recommendation.action === undefined,
  );
  candidates.sort((left, right) => {
    const byApprovals = right.recommendation.approvals_14d - left.recommendation.approvals_14d;
    if (byApprovals !== 0) {
      return byApprovals;
    }
    return right.friction.requested_14d - left.friction.requested_14d;
  });
  return candidates[0] ?? null;
}

function toolStateLabel(value: string): string {
  switch (value) {
    case "always_allow":
      return "always allow";
    case "ask_each_time":
      return "ask each time";
    case "disabled":
      return "disabled";
    default:
      return value.replaceAll("_", " ");
  }
}

function formatOnboardingVariant(value: string | undefined): string {
  switch (value) {
    case "quickstart":
      return "quickstart";
    case "manual":
      return "manual";
    case "remote":
      return "remote";
    default:
      return "default";
  }
}

function formatOnboardingStatus(value: OnboardingPostureEnvelope["status"] | undefined): string {
  switch (value) {
    case "not_started":
      return "not started";
    case "in_progress":
      return "in progress";
    case "blocked":
      return "blocked";
    case "ready":
      return "ready";
    case "complete":
      return "complete";
    default:
      return "unknown";
  }
}

function formatOnboardingStepStatus(status: OnboardingStepView["status"]): string {
  switch (status) {
    case "done":
      return "Done:";
    case "blocked":
      return "Blocked:";
    case "in_progress":
      return "In progress:";
    case "skipped":
      return "Skipped:";
    case "todo":
    default:
      return "Todo:";
  }
}

function remainingOnboardingSteps(posture: OnboardingPostureEnvelope | null): number {
  if (posture === null) {
    return 0;
  }
  return posture.steps.filter(
    (step) => !step.optional && step.status !== "done" && step.status !== "skipped",
  ).length;
}

function executeOnboardingAction(
  action: OnboardingStepAction,
  handlers: {
    navigate: ReturnType<typeof useNavigate>;
    setNotice: (message: string | null) => void;
    setSection: (section: ConsoleAppState["section"]) => void;
  },
): void {
  switch (action.kind) {
    case "open_console_path": {
      const path = normalizeConsoleTarget(action.target);
      const section = findSectionByPath(path);
      if (section !== null) {
        handlers.setSection(section);
      }
      void handlers.navigate(path);
      return;
    }
    case "run_cli_command":
      handlers.setSection("operations");
      handlers.setNotice(`Run in terminal: ${action.target}`);
      return;
    case "open_desktop_section":
      handlers.setNotice(`Open the desktop companion section '${action.target}'.`);
      return;
    case "read_docs":
      handlers.setNotice(`Open documentation: ${action.target}`);
      return;
  }
}

function normalizeConsoleTarget(target: string): string {
  const trimmed = target.trim();
  if (trimmed.startsWith("/#/")) {
    return trimmed.slice(2);
  }
  if (trimmed.startsWith("#/")) {
    return trimmed.slice(1);
  }
  return trimmed.length > 0 ? trimmed : getSectionPath("overview");
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
