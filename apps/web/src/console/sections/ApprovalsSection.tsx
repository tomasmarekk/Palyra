import { Button } from "@heroui/react";
import { useEffect, useMemo, useState } from "react";
import { useSearchParams } from "react-router-dom";

import type {
  ToolPermissionDetailEnvelope,
  ToolPermissionPresetPreviewEnvelope,
  ToolPermissionsEnvelope,
  ToolPostureRecommendationAction,
  ToolPostureScopeKind,
  ToolPostureState,
} from "../../consoleApi";
import { CheckboxField, SelectField, TextInputField } from "../components/ui";
import {
  WorkspaceMetricCard,
  WorkspacePageHeader,
  WorkspaceSectionCard,
  WorkspaceStatusChip,
} from "../components/workspace/WorkspaceChrome";
import { pseudoLocalizeText } from "../i18n";
import { PrettyJsonBlock, formatUnixMs, readObject, readString, type JsonObject } from "../shared";
import type { ConsoleAppState } from "../useConsoleAppState";
import { emitUxSystemEvent } from "../uxTelemetry";

type ApprovalsSectionProps = {
  app: Pick<
    ConsoleAppState,
    | "api"
    | "setError"
    | "setNotice"
    | "approvalsBusy"
    | "approvals"
    | "approvalId"
    | "setApprovalId"
    | "approvalReason"
    | "setApprovalReason"
    | "approvalScope"
    | "setApprovalScope"
    | "refreshApprovals"
    | "decideApproval"
    | "locale"
    | "revealSensitiveValues"
  >;
};

const APPROVAL_MESSAGES = {
  "header.title": "Approvals",
  "header.description":
    "Explainable permissions center for per-tool posture, friction, presets, and the approval inbox.",
  "header.locked": "locked",
  "header.pending": "approvals pending",
  "header.refresh": "Refresh",
  "header.refreshing": "Refreshing...",
  "metric.toolsInScope": "Tools in scope",
  "metric.loadingScope": "Loading scope",
  "metric.requests14d": "Approval requests (14d)",
  "metric.requests14dDetail": "Recent friction signal used for recommendations.",
  "metric.highFriction": "High-friction tools",
  "metric.highFrictionDetail": "Tools with repeated approvals or pending backlog.",
  "queue.title": "Approval queue",
  "queue.description":
    "Pending work stays easy to scan, with resolved items still available for follow-up context.",
  "queue.empty": "No approval records loaded.",
  "queue.unknownSubject": "unknown subject",
  "queue.pending": "pending",
  "detail.title": "Approval detail",
  "detail.description":
    "Keep the selected request, context, explainability, and decision controls on one surface.",
  "detail.empty": "Select an approval to inspect request context and decide it.",
  "detail.selected": "Selected approval",
  "detail.noSummary": "No summary published.",
  "detail.subjectType": "Subject type",
  "detail.subjectId": "Subject ID",
  "detail.principal": "Principal",
  "detail.requested": "Requested",
  "detail.session": "Session",
  "detail.run": "Run",
  "detail.notAvailable": "n/a",
  "detail.why": "Why this approval appeared",
  "detail.whyDescription":
    "Inline explainability for the current tool posture and the next safe action.",
  "detail.openTool": "Open tool detail",
  "detail.tool": "Approval tool",
  "detail.approvalId": "Approval ID",
  "detail.reason": "Reason",
  "detail.reasonPlaceholder": "Optional operator note",
  "detail.scope": "Decision scope",
  "detail.approve": "Approve",
  "detail.deny": "Deny",
  "detail.previewPayload": "Approval preview payload",
  "detail.previewPayloadDescription":
    "Structured preview context stays visible so operators can distinguish preview-only actions from applied ones.",
} as const;

type ApprovalMessageKey = keyof typeof APPROVAL_MESSAGES;

const APPROVAL_MESSAGES_CS: Readonly<Record<ApprovalMessageKey, string>> = {
  "header.title": "Schválení",
  "header.description":
    "Centrum vysvětlitelných oprávnění pro posture jednotlivých nástrojů, tření, presety a approval inbox.",
  "header.locked": "uzamčeno",
  "header.pending": "čekajících schválení",
  "header.refresh": "Obnovit",
  "header.refreshing": "Obnovuji...",
  "metric.toolsInScope": "Nástroje v rozsahu",
  "metric.loadingScope": "Načítám scope",
  "metric.requests14d": "Požadavky na schválení (14 d)",
  "metric.requests14dDetail": "Nedávný friction signál používaný pro doporučení.",
  "metric.highFriction": "Nástroje s vysokým třením",
  "metric.highFrictionDetail": "Nástroje s opakovanými schváleními nebo čekajícím backlogem.",
  "queue.title": "Fronta schválení",
  "queue.description":
    "Čekající práce zůstává snadno čitelná a vyřešené položky jsou stále dostupné pro navazující kontext.",
  "queue.empty": "Nejsou načtené žádné záznamy schválení.",
  "queue.unknownSubject": "neznámý subjekt",
  "queue.pending": "čeká",
  "detail.title": "Detail schválení",
  "detail.description":
    "Drž vybraný požadavek, kontext, vysvětlení a ovládání rozhodnutí na jedné ploše.",
  "detail.empty": "Vyber schválení a zkontroluj kontext požadavku i rozhodnutí.",
  "detail.selected": "Vybrané schválení",
  "detail.noSummary": "Nebyl publikovaný žádný souhrn.",
  "detail.subjectType": "Typ subjektu",
  "detail.subjectId": "ID subjektu",
  "detail.principal": "Principál",
  "detail.requested": "Vyžádáno",
  "detail.session": "Relace",
  "detail.run": "Běh",
  "detail.notAvailable": "n/a",
  "detail.why": "Proč se toto schválení objevilo",
  "detail.whyDescription":
    "Inline explainability pro aktuální posture nástroje a další bezpečnou akci.",
  "detail.openTool": "Otevřít detail nástroje",
  "detail.tool": "Nástroj schválení",
  "detail.approvalId": "ID schválení",
  "detail.reason": "Důvod",
  "detail.reasonPlaceholder": "Volitelná poznámka operátora",
  "detail.scope": "Scope rozhodnutí",
  "detail.approve": "Schválit",
  "detail.deny": "Zamítnout",
  "detail.previewPayload": "Preview payload schválení",
  "detail.previewPayloadDescription":
    "Strukturovaný preview kontext zůstává viditelný, aby operátoři odlišili preview-only akce od aplikovaných.",
};

function translateApproval(
  locale: ConsoleAppState["locale"],
  key: ApprovalMessageKey,
  variables?: Record<string, string | number>,
): string {
  const template = (locale === "cs" ? APPROVAL_MESSAGES_CS : APPROVAL_MESSAGES)[key];
  const resolved =
    variables === undefined
      ? template
      : template.replaceAll(/\{([a-zA-Z0-9_]+)\}/g, (_, name) => `${variables[name] ?? ""}`);
  return locale === "qps-ploc" ? pseudoLocalizeText(resolved) : resolved;
}

export function ApprovalsSection({ app }: ApprovalsSectionProps) {
  const t = (key: ApprovalMessageKey, variables?: Record<string, string | number>) =>
    translateApproval(app.locale, key, variables);
  const [searchParams] = useSearchParams();
  const [permissionsBusy, setPermissionsBusy] = useState(false);
  const [detailBusy, setDetailBusy] = useState(false);
  const [mutationBusy, setMutationBusy] = useState(false);
  const [permissions, setPermissions] = useState<ToolPermissionsEnvelope | null>(null);
  const [toolDetail, setToolDetail] = useState<ToolPermissionDetailEnvelope | null>(null);
  const [selectedToolName, setSelectedToolName] = useState("");
  const [scopeKind, setScopeKind] = useState<ToolPostureScopeKind>("global");
  const [scopeId, setScopeId] = useState("");
  const [search, setSearch] = useState("");
  const [category, setCategory] = useState("");
  const [stateFilter, setStateFilter] = useState("");
  const [lockedOnly, setLockedOnly] = useState(false);
  const [highFrictionOnly, setHighFrictionOnly] = useState(false);
  const [changeReason, setChangeReason] = useState("");
  const [selectedPresetId, setSelectedPresetId] = useState("");
  const [presetPreview, setPresetPreview] = useState<ToolPermissionPresetPreviewEnvelope | null>(
    null,
  );
  const requestedToolName = searchParams.get("tool")?.trim() ?? "";

  const pendingApprovals = useMemo(
    () => app.approvals.filter((approval) => readString(approval, "decision") === null),
    [app.approvals],
  );
  const selectedApproval =
    app.approvals.find((approval) => readString(approval, "approval_id") === app.approvalId) ??
    app.approvals[0] ??
    null;
  const selectedApprovalId = readString(selectedApproval ?? {}, "approval_id") ?? "";
  const selectedApprovalToolName = extractApprovalToolName(selectedApproval);
  const permissionFromApproval =
    permissions?.tools.find((tool) => tool.tool_name === selectedApprovalToolName) ?? null;
  const selectedTool =
    permissions?.tools.find((tool) => tool.tool_name === selectedToolName) ??
    permissions?.tools[0] ??
    null;
  const effectiveDetail =
    toolDetail?.tool.tool_name === selectedTool?.tool_name ? toolDetail : null;

  useEffect(() => {
    void refreshPermissions();
  }, [
    scopeKind,
    scopeId,
    search,
    category,
    stateFilter,
    lockedOnly,
    highFrictionOnly,
    requestedToolName,
  ]);

  useEffect(() => {
    if (selectedToolName.trim().length === 0) {
      setToolDetail(null);
      return;
    }
    void refreshToolDetail(selectedToolName);
  }, [selectedToolName, scopeKind, scopeId]);

  async function refreshPermissions(preferredToolName?: string): Promise<void> {
    setPermissionsBusy(true);
    setPresetPreview(null);
    app.setError(null);
    try {
      const response = await app.api.getToolPermissions(
        buildToolPermissionQuery({
          scopeKind,
          scopeId,
          search,
          category,
          stateFilter,
          lockedOnly,
          highFrictionOnly,
        }),
      );
      setPermissions(response);
      const nextToolName =
        preferredToolName?.trim() ||
        (requestedToolName.length > 0 &&
        response.tools.some((tool) => tool.tool_name === requestedToolName)
          ? requestedToolName
          : "") ||
        (response.tools.some((tool) => tool.tool_name === selectedToolName)
          ? selectedToolName
          : "") ||
        response.tools[0]?.tool_name ||
        "";
      setSelectedToolName(nextToolName);
      if (nextToolName.length === 0) {
        setToolDetail(null);
      }
    } catch (failure) {
      app.setError(toErrorMessage(failure));
    } finally {
      setPermissionsBusy(false);
    }
  }

  async function refreshToolDetail(toolName: string): Promise<void> {
    setDetailBusy(true);
    try {
      const detail = await app.api.getToolPermission(
        toolName,
        buildToolPermissionQuery({
          scopeKind,
          scopeId,
        }),
      );
      setToolDetail(detail);
    } catch (failure) {
      app.setError(toErrorMessage(failure));
    } finally {
      setDetailBusy(false);
    }
  }

  async function updateToolState(nextState: ToolPostureState): Promise<void> {
    if (selectedTool === null) {
      app.setError("Select a tool first.");
      return;
    }
    setMutationBusy(true);
    app.setError(null);
    try {
      await app.api.setToolPermissionOverride(selectedTool.tool_name, {
        scope_kind: scopeKind,
        scope_id: scopeKind === "global" ? undefined : emptyToUndefined(scopeId),
        state: nextState,
        reason: emptyToUndefined(changeReason),
      });
      app.setNotice(`Updated ${selectedTool.title} for ${scopeLabel(scopeKind, scopeId)}.`);
      await refreshPermissions(selectedTool.tool_name);
      await refreshToolDetail(selectedTool.tool_name);
    } catch (failure) {
      app.setError(toErrorMessage(failure));
    } finally {
      setMutationBusy(false);
    }
  }

  async function resetSelectedTool(): Promise<void> {
    if (selectedTool === null) {
      app.setError("Select a tool first.");
      return;
    }
    setMutationBusy(true);
    app.setError(null);
    try {
      await app.api.resetToolPermission(selectedTool.tool_name, {
        scope_kind: scopeKind,
        scope_id: scopeKind === "global" ? undefined : emptyToUndefined(scopeId),
        reason: emptyToUndefined(changeReason),
      });
      app.setNotice(`Reset ${selectedTool.title} to inherited/default posture.`);
      await refreshPermissions(selectedTool.tool_name);
      await refreshToolDetail(selectedTool.tool_name);
    } catch (failure) {
      app.setError(toErrorMessage(failure));
    } finally {
      setMutationBusy(false);
    }
  }

  async function previewPreset(): Promise<void> {
    if (selectedPresetId.trim().length === 0) {
      app.setError("Select a preset first.");
      return;
    }
    setMutationBusy(true);
    app.setError(null);
    try {
      setPresetPreview(
        await app.api.previewToolPermissionPreset({
          preset_id: selectedPresetId,
          scope_kind: scopeKind,
          scope_id: scopeKind === "global" ? undefined : emptyToUndefined(scopeId),
        }),
      );
    } catch (failure) {
      app.setError(toErrorMessage(failure));
    } finally {
      setMutationBusy(false);
    }
  }

  async function applyPreset(): Promise<void> {
    if (selectedPresetId.trim().length === 0) {
      app.setError("Select a preset first.");
      return;
    }
    setMutationBusy(true);
    app.setError(null);
    try {
      const preview = await app.api.applyToolPermissionPreset({
        preset_id: selectedPresetId,
        scope_kind: scopeKind,
        scope_id: scopeKind === "global" ? undefined : emptyToUndefined(scopeId),
        reason: emptyToUndefined(changeReason),
      });
      setPresetPreview(preview);
      app.setNotice(`Applied preset ${preview.preset.label}.`);
      await refreshPermissions(selectedToolName);
      if (selectedToolName.trim().length > 0) {
        await refreshToolDetail(selectedToolName);
      }
    } catch (failure) {
      app.setError(toErrorMessage(failure));
    } finally {
      setMutationBusy(false);
    }
  }

  async function resetScope(): Promise<void> {
    setMutationBusy(true);
    app.setError(null);
    try {
      await app.api.resetToolPermissionScope({
        scope_kind: scopeKind,
        scope_id: scopeKind === "global" ? undefined : emptyToUndefined(scopeId),
        reason: emptyToUndefined(changeReason),
      });
      app.setNotice(`Reset explicit overrides for ${scopeLabel(scopeKind, scopeId)}.`);
      await refreshPermissions(selectedToolName);
      if (selectedToolName.trim().length > 0) {
        await refreshToolDetail(selectedToolName);
      }
    } catch (failure) {
      app.setError(toErrorMessage(failure));
    } finally {
      setMutationBusy(false);
    }
  }

  async function actOnRecommendation(action: ToolPostureRecommendationAction): Promise<void> {
    const recommendation = effectiveDetail?.tool.recommendation;
    if (recommendation === undefined) {
      app.setError("No active recommendation for the selected tool.");
      return;
    }
    setMutationBusy(true);
    app.setError(null);
    try {
      await app.api.actOnToolPermissionRecommendation({
        recommendation_id: recommendation.recommendation_id,
        tool_name: recommendation.tool_name,
        scope_kind: recommendation.scope_kind,
        scope_id: recommendation.scope_kind === "global" ? undefined : recommendation.scope_id,
        action,
      });
      await emitUxSystemEvent(app.api, {
        name: "ux.tool_posture.recommendation",
        surface: "web",
        section: "approvals",
        toolName: recommendation.tool_name,
        recommendationAction: action,
        scopeKind: recommendation.scope_kind,
        summary: `Tool posture recommendation ${action} for ${recommendation.tool_name}`,
      });
      app.setNotice(`Recommendation ${action} for ${recommendation.tool_name}.`);
      await refreshPermissions(recommendation.tool_name);
      await refreshToolDetail(recommendation.tool_name);
    } catch (failure) {
      app.setError(toErrorMessage(failure));
    } finally {
      setMutationBusy(false);
    }
  }

  return (
    <main className="workspace-page">
      <WorkspacePageHeader
        eyebrow="Control"
        title={t("header.title")}
        description={t("header.description")}
        status={
          <>
            <WorkspaceStatusChip tone={permissions?.summary.locked_tools ? "warning" : "success"}>
              {permissions?.summary.locked_tools ?? 0} {t("header.locked")}
            </WorkspaceStatusChip>
            <WorkspaceStatusChip tone={pendingApprovals.length > 0 ? "warning" : "success"}>
              {pendingApprovals.length} {t("header.pending")}
            </WorkspaceStatusChip>
          </>
        }
        actions={
          <Button
            variant="secondary"
            onPress={() =>
              void Promise.all([refreshPermissions(selectedToolName), app.refreshApprovals()])
            }
            isDisabled={permissionsBusy || app.approvalsBusy}
          >
            {permissionsBusy || app.approvalsBusy ? t("header.refreshing") : t("header.refresh")}
          </Button>
        }
      />

      <section className="workspace-metric-grid workspace-metric-grid--compact">
        <WorkspaceMetricCard
          label={t("metric.toolsInScope")}
          value={permissions?.summary.total_tools ?? 0}
          detail={permissions?.scope.active.label ?? t("metric.loadingScope")}
        />
        <WorkspaceMetricCard
          label={t("metric.requests14d")}
          value={permissions?.summary.approval_requests_14d ?? 0}
          detail={t("metric.requests14dDetail")}
          tone={(permissions?.summary.approval_requests_14d ?? 0) > 0 ? "warning" : "default"}
        />
        <WorkspaceMetricCard
          label={t("metric.highFriction")}
          value={permissions?.summary.high_friction_tools ?? 0}
          detail={t("metric.highFrictionDetail")}
          tone={(permissions?.summary.high_friction_tools ?? 0) > 0 ? "warning" : "default"}
        />
      </section>

      <WorkspaceSectionCard
        title="Scope and filters"
        description="Pick the precedence layer, filter the catalog, and preview bulk-safe presets before applying them."
      >
        <div className="workspace-stack">
          <div className="workspace-form-grid">
            <SelectField
              label="Scope"
              value={scopeKind}
              onChange={(value) => setScopeKind((value as ToolPostureScopeKind) || "global")}
              options={[
                { key: "global", label: "Global" },
                { key: "workspace", label: "Workspace" },
                { key: "agent", label: "Agent" },
                { key: "session", label: "Session" },
              ]}
            />
            <TextInputField
              label="Scope ID"
              value={scopeId}
              onChange={setScopeId}
              readOnly={scopeKind === "global"}
              placeholder={scopeKind === "global" ? "global" : `Enter ${scopeKind} ID`}
              description="Required for workspace, agent, and session scopes."
            />
            <TextInputField
              label="Search"
              value={search}
              onChange={setSearch}
              placeholder="Filter by tool, description, or category"
            />
            <SelectField
              label="Category"
              value={category}
              onChange={setCategory}
              options={[
                { key: "", label: "All categories" },
                ...(permissions?.categories ?? []).map((value) => ({ key: value, label: value })),
              ]}
            />
            <SelectField
              label="State"
              value={stateFilter}
              onChange={setStateFilter}
              options={[
                { key: "", label: "All states" },
                { key: "always_allow", label: "Always allow" },
                { key: "ask_each_time", label: "Ask each time" },
                { key: "disabled", label: "Disabled" },
              ]}
            />
            <TextInputField
              label="Change note"
              value={changeReason}
              onChange={setChangeReason}
              placeholder="Optional audit note"
            />
          </div>

          <div className="workspace-inline">
            <CheckboxField
              label="Locked only"
              checked={lockedOnly}
              onChange={setLockedOnly}
              description="Show only tools disabled by policy/runtime lock reasons."
            />
            <CheckboxField
              label="High friction only"
              checked={highFrictionOnly}
              onChange={setHighFrictionOnly}
              description="Show tools with repeated approvals or pending approvals."
            />
          </div>

          <div className="workspace-form-grid">
            <SelectField
              label="Preset bundle"
              value={selectedPresetId}
              onChange={setSelectedPresetId}
              options={[
                { key: "", label: "No preset selected" },
                ...(permissions?.presets ?? []).map((preset) => ({
                  key: preset.preset_id,
                  label: preset.label,
                  description: preset.description,
                })),
              ]}
            />
          </div>

          <div className="console-inline-actions">
            <Button
              variant="secondary"
              onPress={() => void previewPreset()}
              isDisabled={mutationBusy}
            >
              Preview preset
            </Button>
            <Button onPress={() => void applyPreset()} isDisabled={mutationBusy}>
              Apply preset
            </Button>
            <Button
              variant="danger-soft"
              onPress={() => void resetScope()}
              isDisabled={mutationBusy}
            >
              Reset scope overrides
            </Button>
          </div>

          {presetPreview !== null && (
            <WorkspaceSectionCard
              title={`Preset preview: ${presetPreview.preset.label}`}
              description="Transparent diff before or after applying the preset bundle."
              className="workspace-section-card--nested"
            >
              <div className="workspace-list">
                {presetPreview.preview.map((entry) => (
                  <div key={entry.tool_name} className="workspace-list-item">
                    <div>
                      <strong>{entry.title}</strong>
                      <p className="chat-muted">
                        {entry.tool_name} · {toolStateLabel(entry.current_state)} to{" "}
                        {toolStateLabel(entry.proposed_state)}
                      </p>
                    </div>
                    <WorkspaceStatusChip
                      tone={entry.locked ? "danger" : entry.changed ? "warning" : "default"}
                    >
                      {entry.locked ? "locked" : entry.changed ? "changes" : "no-op"}
                    </WorkspaceStatusChip>
                  </div>
                ))}
              </div>
            </WorkspaceSectionCard>
          )}
        </div>
      </WorkspaceSectionCard>

      <section className="workspace-two-column workspace-two-column--queue">
        <WorkspaceSectionCard
          title="Tool catalog"
          description="One catalog for shell, filesystem, browser, network, memory, and plugin capabilities."
        >
          <div className="workspace-list workspace-list--queue">
            {permissions === null ? (
              <p className="chat-muted">Loading tool permissions…</p>
            ) : permissions.tools.length === 0 ? (
              <p className="chat-muted">No tools match the current scope and filters.</p>
            ) : (
              permissions.tools.map((tool) => {
                const isActive = tool.tool_name === (selectedTool?.tool_name ?? "");
                return (
                  <Button
                    key={tool.tool_name}
                    type="button"
                    className={`workspace-list-button${isActive ? " is-active" : ""}`}
                    variant={isActive ? "secondary" : "ghost"}
                    onPress={() => setSelectedToolName(tool.tool_name)}
                  >
                    <div>
                      <strong>{tool.title}</strong>
                      <p className="chat-muted">
                        {tool.tool_name} · {tool.category} ·{" "}
                        {tool.effective_posture.source_scope_label}
                      </p>
                    </div>
                    <div className="workspace-inline">
                      <WorkspaceStatusChip
                        tone={toolStateTone(tool.effective_posture.effective_state)}
                      >
                        {toolStateLabel(tool.effective_posture.effective_state)}
                      </WorkspaceStatusChip>
                      <WorkspaceStatusChip
                        tone={tool.effective_posture.lock_reason ? "danger" : "default"}
                      >
                        {tool.effective_posture.lock_reason ? "locked" : tool.risk_level}
                      </WorkspaceStatusChip>
                    </div>
                  </Button>
                );
              })
            )}
          </div>
        </WorkspaceSectionCard>
        <WorkspaceSectionCard
          title="Tool detail"
          description="Explainable effective state, precedence chain, friction, history, and safe posture controls."
        >
          {selectedTool === null ? (
            <p className="chat-muted">Select a tool to inspect posture and history.</p>
          ) : (
            <div className="workspace-stack">
              <div className="workspace-callout">
                <div className="workspace-list-item">
                  <div>
                    <p className="console-label">Selected tool</p>
                    <strong>{selectedTool.title}</strong>
                    <p className="chat-muted">{selectedTool.description}</p>
                  </div>
                  <div className="workspace-inline">
                    <WorkspaceStatusChip
                      tone={toolStateTone(selectedTool.effective_posture.effective_state)}
                    >
                      {toolStateLabel(selectedTool.effective_posture.effective_state)}
                    </WorkspaceStatusChip>
                    <WorkspaceStatusChip tone="default">
                      default {toolStateLabel(selectedTool.effective_posture.default_state)}
                    </WorkspaceStatusChip>
                  </div>
                </div>
                <p className="chat-muted">
                  Source scope: {selectedTool.effective_posture.source_scope_label}
                  {selectedTool.effective_posture.lock_reason
                    ? ` · Locked because ${selectedTool.effective_posture.lock_reason}`
                    : ""}
                </p>
              </div>

              <div className="console-inline-actions">
                <Button
                  onPress={() => void updateToolState("always_allow")}
                  isDisabled={mutationBusy || !selectedTool.effective_posture.editable}
                >
                  Always allow
                </Button>
                <Button
                  variant="secondary"
                  onPress={() => void updateToolState("ask_each_time")}
                  isDisabled={mutationBusy || !selectedTool.effective_posture.editable}
                >
                  Ask each time
                </Button>
                <Button
                  variant="danger-soft"
                  onPress={() => void updateToolState("disabled")}
                  isDisabled={mutationBusy || !selectedTool.effective_posture.editable}
                >
                  Disable
                </Button>
                <Button
                  variant="ghost"
                  onPress={() => void resetSelectedTool()}
                  isDisabled={mutationBusy}
                >
                  Reset tool
                </Button>
              </div>

              <section className="workspace-metric-grid workspace-metric-grid--compact">
                <WorkspaceMetricCard
                  label="Requested (14d)"
                  value={selectedTool.friction.requested_14d}
                  detail="How often this tool triggered approval in the analytics window."
                />
                <WorkspaceMetricCard
                  label="Approved"
                  value={selectedTool.friction.approved_14d}
                  detail="Approvals that cleared without denial."
                />
                <WorkspaceMetricCard
                  label="Pending"
                  value={selectedTool.friction.pending_14d}
                  detail="Approvals still waiting for operator action."
                  tone={selectedTool.friction.pending_14d > 0 ? "warning" : "default"}
                />
                <WorkspaceMetricCard
                  label="Sessions"
                  value={selectedTool.friction.unique_sessions_14d}
                  detail="Unique sessions that requested this tool."
                />
              </section>

              {effectiveDetail?.tool.recommendation !== undefined && (
                <WorkspaceSectionCard
                  title="Recommendation"
                  description="Data-backed suggestion; always opt-in and reversible."
                  className="workspace-section-card--nested"
                >
                  <p>{effectiveDetail.tool.recommendation.reason}</p>
                  <div className="console-inline-actions">
                    <Button
                      onPress={() => void actOnRecommendation("accepted")}
                      isDisabled={mutationBusy}
                    >
                      Accept
                    </Button>
                    <Button
                      variant="secondary"
                      onPress={() => void actOnRecommendation("deferred")}
                      isDisabled={mutationBusy}
                    >
                      Defer
                    </Button>
                    <Button
                      variant="ghost"
                      onPress={() => void actOnRecommendation("dismissed")}
                      isDisabled={mutationBusy}
                    >
                      Dismiss
                    </Button>
                  </div>
                </WorkspaceSectionCard>
              )}

              <WorkspaceSectionCard
                title="Precedence chain"
                description="Inherited versus explicit posture layers for the selected scope."
                className="workspace-section-card--nested"
              >
                <div className="workspace-list">
                  {(
                    effectiveDetail?.tool.effective_posture.chain ??
                    selectedTool.effective_posture.chain
                  ).map((entry) => (
                    <div key={`${entry.kind}:${entry.scope_id}`} className="workspace-list-item">
                      <div>
                        <strong>{entry.label}</strong>
                        <p className="chat-muted">
                          {entry.kind} · {entry.scope_id}
                          {entry.source ? ` · ${entry.source}` : ""}
                        </p>
                      </div>
                      <WorkspaceStatusChip
                        tone={entry.state ? toolStateTone(entry.state) : "default"}
                      >
                        {entry.state ? toolStateLabel(entry.state) : "inherited"}
                      </WorkspaceStatusChip>
                    </div>
                  ))}
                </div>
              </WorkspaceSectionCard>

              <WorkspaceSectionCard
                title="Recent approvals"
                description="Latest approval records tied to this tool."
                className="workspace-section-card--nested"
              >
                <div className="workspace-list">
                  {selectedTool.recent_approvals.length === 0 ? (
                    <p className="chat-muted">No recent approval records for this tool.</p>
                  ) : (
                    selectedTool.recent_approvals.map((approval, index) => {
                      const record = approval as JsonObject;
                      return (
                        <div
                          key={
                            readString(record, "approval_id") ??
                            `${selectedTool.tool_name}-${index}`
                          }
                          className="workspace-list-item"
                        >
                          <div>
                            <strong>{readString(record, "request_summary") ?? "Approval"}</strong>
                            <p className="chat-muted">
                              {readString(record, "decision") ?? "pending"} ·{" "}
                              {formatUnixMs(readUnixMillis(record, "requested_at_unix_ms"))}
                            </p>
                          </div>
                        </div>
                      );
                    })
                  )}
                </div>
              </WorkspaceSectionCard>

              <WorkspaceSectionCard
                title="Change history"
                description="Audit trail for explicit posture changes and recommendation actions."
                className="workspace-section-card--nested"
              >
                <div className="workspace-list">
                  {detailBusy ? (
                    <p className="chat-muted">Loading history…</p>
                  ) : effectiveDetail?.change_history.length ? (
                    effectiveDetail.change_history.map((event) => (
                      <div key={event.audit_id} className="workspace-list-item">
                        <div>
                          <strong>{event.action.replaceAll("_", " ")}</strong>
                          <p className="chat-muted">
                            {event.actor_principal} · {formatUnixMs(event.created_at_unix_ms)} ·{" "}
                            {event.source}
                          </p>
                          {event.reason ? <p className="chat-muted">{event.reason}</p> : null}
                        </div>
                        <WorkspaceStatusChip tone="default">
                          {event.new_state ? toolStateLabel(event.new_state) : "cleared"}
                        </WorkspaceStatusChip>
                      </div>
                    ))
                  ) : (
                    <p className="chat-muted">No scope-specific changes recorded yet.</p>
                  )}
                </div>
              </WorkspaceSectionCard>
            </div>
          )}
        </WorkspaceSectionCard>
      </section>

      <section className="workspace-two-column workspace-two-column--queue">
        <WorkspaceSectionCard title={t("queue.title")} description={t("queue.description")}>
          <div className="workspace-list workspace-list--queue">
            {app.approvals.length === 0 ? (
              <p className="chat-muted">{t("queue.empty")}</p>
            ) : (
              app.approvals.map((approval) => {
                const approvalId = readString(approval, "approval_id") ?? "unknown";
                const decision = readString(approval, "decision");
                const isActive = approvalId === selectedApprovalId;
                return (
                  <Button
                    key={approvalId}
                    type="button"
                    className={`workspace-list-button${isActive ? " is-active" : ""}`}
                    variant={isActive ? "secondary" : "ghost"}
                    onPress={() => app.setApprovalId(approvalId)}
                  >
                    <div>
                      <strong>{readString(approval, "request_summary") ?? approvalId}</strong>
                      <p className="chat-muted">
                        {readString(approval, "subject_type") ?? t("queue.unknownSubject")} ·{" "}
                        {formatUnixMs(readUnixMillis(approval, "requested_at_unix_ms"))}
                      </p>
                    </div>
                    <WorkspaceStatusChip tone={decision === null ? "warning" : "default"}>
                      {decision ?? t("queue.pending")}
                    </WorkspaceStatusChip>
                  </Button>
                );
              })
            )}
          </div>
        </WorkspaceSectionCard>

        <WorkspaceSectionCard title={t("detail.title")} description={t("detail.description")}>
          {selectedApproval === null ? (
            <p className="chat-muted">{t("detail.empty")}</p>
          ) : (
            <div className="workspace-stack">
              <div className="workspace-callout">
                <div className="workspace-list-item">
                  <div>
                    <p className="console-label">{t("detail.selected")}</p>
                    <strong>{selectedApprovalId}</strong>
                  </div>
                  <WorkspaceStatusChip
                    tone={readString(selectedApproval, "decision") === null ? "warning" : "default"}
                  >
                    {readString(selectedApproval, "decision") ?? t("queue.pending")}
                  </WorkspaceStatusChip>
                </div>
                <p className="chat-muted">
                  {readString(selectedApproval, "request_summary") ?? t("detail.noSummary")}
                </p>
              </div>

              <dl className="workspace-key-value-grid">
                <div>
                  <dt>{t("detail.subjectType")}</dt>
                  <dd>
                    {readString(selectedApproval, "subject_type") ?? t("detail.notAvailable")}
                  </dd>
                </div>
                <div>
                  <dt>{t("detail.subjectId")}</dt>
                  <dd>{readString(selectedApproval, "subject_id") ?? t("detail.notAvailable")}</dd>
                </div>
                <div>
                  <dt>{t("detail.principal")}</dt>
                  <dd>{readString(selectedApproval, "principal") ?? t("detail.notAvailable")}</dd>
                </div>
                <div>
                  <dt>{t("detail.requested")}</dt>
                  <dd>{formatUnixMs(readUnixMillis(selectedApproval, "requested_at_unix_ms"))}</dd>
                </div>
                <div>
                  <dt>{t("detail.session")}</dt>
                  <dd>{readString(selectedApproval, "session_id") ?? t("detail.notAvailable")}</dd>
                </div>
                <div>
                  <dt>{t("detail.run")}</dt>
                  <dd>{readString(selectedApproval, "run_id") ?? t("detail.notAvailable")}</dd>
                </div>
              </dl>

              {permissionFromApproval !== null && (
                <WorkspaceSectionCard
                  title={t("detail.why")}
                  description={t("detail.whyDescription")}
                  className="workspace-section-card--nested"
                >
                  <p>
                    {permissionFromApproval.effective_posture.lock_reason
                      ? `This tool is effectively locked by ${permissionFromApproval.effective_posture.source_scope_label}: ${permissionFromApproval.effective_posture.lock_reason}`
                      : `This tool currently resolves to ${toolStateLabel(permissionFromApproval.effective_posture.effective_state)} from ${permissionFromApproval.effective_posture.source_scope_label}.`}
                  </p>
                  <div className="workspace-inline">
                    <WorkspaceStatusChip
                      tone={toolStateTone(permissionFromApproval.effective_posture.effective_state)}
                    >
                      {toolStateLabel(permissionFromApproval.effective_posture.effective_state)}
                    </WorkspaceStatusChip>
                    <WorkspaceStatusChip tone="default">
                      {permissionFromApproval.effective_posture.source_scope_label}
                    </WorkspaceStatusChip>
                  </div>
                  <div className="console-inline-actions">
                    <Button
                      variant="secondary"
                      onPress={() => setSelectedToolName(permissionFromApproval.tool_name)}
                    >
                      {t("detail.openTool")}
                    </Button>
                  </div>
                </WorkspaceSectionCard>
              )}

              {selectedApprovalToolName !== null && (
                <div className="workspace-callout">
                  <p className="console-label">{t("detail.tool")}</p>
                  <strong>{selectedApprovalToolName}</strong>
                </div>
              )}

              <div className="workspace-form-grid">
                <TextInputField
                  label={t("detail.approvalId")}
                  value={selectedApprovalId}
                  readOnly
                  onChange={() => {}}
                />
                <TextInputField
                  label={t("detail.reason")}
                  value={app.approvalReason}
                  onChange={app.setApprovalReason}
                  placeholder={t("detail.reasonPlaceholder")}
                />
                <SelectField
                  label={t("detail.scope")}
                  value={app.approvalScope}
                  onChange={app.setApprovalScope}
                  options={[
                    { key: "once", label: "once" },
                    { key: "session", label: "session" },
                    { key: "timeboxed", label: "timeboxed" },
                  ]}
                />
              </div>

              <div className="console-inline-actions">
                <Button
                  onPress={() => {
                    if (selectedApprovalId.length > 0) {
                      app.setApprovalId(selectedApprovalId);
                    }
                    void app.decideApproval(true);
                  }}
                  isDisabled={app.approvalsBusy}
                >
                  {t("detail.approve")}
                </Button>
                <Button
                  variant="danger-soft"
                  onPress={() => {
                    if (selectedApprovalId.length > 0) {
                      app.setApprovalId(selectedApprovalId);
                    }
                    void app.decideApproval(false);
                  }}
                  isDisabled={app.approvalsBusy}
                >
                  {t("detail.deny")}
                </Button>
              </div>

              {parsePromptDetails(readObject(selectedApproval, "prompt")) !== null && (
                <WorkspaceSectionCard
                  title={t("detail.previewPayload")}
                  description={t("detail.previewPayloadDescription")}
                  className="workspace-section-card--nested"
                >
                  <PrettyJsonBlock
                    value={parsePromptDetails(readObject(selectedApproval, "prompt")) as JsonObject}
                    revealSensitiveValues={app.revealSensitiveValues}
                  />
                </WorkspaceSectionCard>
              )}
            </div>
          )}
        </WorkspaceSectionCard>
      </section>
    </main>
  );
}

function buildToolPermissionQuery(filters: {
  scopeKind: ToolPostureScopeKind;
  scopeId?: string;
  search?: string;
  category?: string;
  stateFilter?: string;
  lockedOnly?: boolean;
  highFrictionOnly?: boolean;
}): URLSearchParams {
  const params = new URLSearchParams();
  params.set("scope_kind", filters.scopeKind);
  if (filters.scopeKind !== "global" && filters.scopeId?.trim()) {
    params.set("scope_id", filters.scopeId.trim());
  }
  if (filters.search?.trim()) {
    params.set("q", filters.search.trim());
  }
  if (filters.category?.trim()) {
    params.set("category", filters.category.trim());
  }
  if (filters.stateFilter?.trim()) {
    params.set("state", filters.stateFilter.trim());
  }
  if (filters.lockedOnly) {
    params.set("locked_only", "true");
  }
  if (filters.highFrictionOnly) {
    params.set("high_friction_only", "true");
  }
  return params;
}

function readUnixMillis(record: JsonObject, key: string): number | null {
  const value = record[key];
  return typeof value === "number" && Number.isFinite(value) ? value : null;
}

function parsePromptDetails(prompt: JsonObject | null): JsonObject | null {
  if (prompt === null) {
    return null;
  }
  const detailsRaw = readString(prompt, "details_json");
  if (detailsRaw === null || detailsRaw.trim().length === 0) {
    return null;
  }
  try {
    const parsed = JSON.parse(detailsRaw);
    return parsed !== null && typeof parsed === "object" && !Array.isArray(parsed)
      ? (parsed as JsonObject)
      : null;
  } catch {
    return null;
  }
}

function emptyToUndefined(value: string): string | undefined {
  const normalized = value.trim();
  return normalized.length > 0 ? normalized : undefined;
}

function toErrorMessage(error: unknown): string {
  return error instanceof Error ? error.message : "Unexpected console error.";
}

function scopeLabel(scopeKind: ToolPostureScopeKind, scopeId: string): string {
  return scopeKind === "global"
    ? "global default"
    : `${scopeKind} ${scopeId.trim() || "(missing id)"}`;
}

function toolStateLabel(state: ToolPostureState): string {
  switch (state) {
    case "always_allow":
      return "always allow";
    case "ask_each_time":
      return "ask each time";
    case "disabled":
      return "disabled";
  }
}

function toolStateTone(state: ToolPostureState): "success" | "warning" | "danger" {
  switch (state) {
    case "always_allow":
      return "success";
    case "ask_each_time":
      return "warning";
    case "disabled":
      return "danger";
  }
}

function extractApprovalToolName(approval: JsonObject | null): string | null {
  const prompt = readObject(approval ?? {}, "prompt");
  const promptDetails = parsePromptDetails(prompt);
  return readString(promptDetails ?? {}, "tool_name");
}
