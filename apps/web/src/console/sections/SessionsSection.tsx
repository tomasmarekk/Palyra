import { useEffect, useMemo, useState } from "react";
import { useNavigate } from "react-router-dom";

import {
  buildSessionLineageHint,
  describeBranchState,
  describeTitleGenerationState,
} from "../../chat/chatShared";
import type {
  ChatCheckpointRecord,
  ChatCompactionArtifactRecord,
  ChatCompactionPreview,
} from "../../consoleApi";
import {
  buildObjectiveChatHref,
  buildObjectiveOverviewHref,
  findObjectiveForSession,
} from "../objectiveLinks";
import { getSectionPath } from "../navigation";
import { ActionButton, SelectField, SwitchField, TextInputField } from "../components/ui";
import {
  WorkspaceMetricCard,
  WorkspacePageHeader,
  WorkspaceSectionCard,
  WorkspaceStatusChip,
} from "../components/workspace/WorkspaceChrome";
import {
  WorkspaceEmptyState,
  WorkspaceInlineNotice,
  WorkspaceTable,
  workspaceToneForState,
} from "../components/workspace/WorkspacePatterns";
import { useSessionCatalogDomain } from "../hooks/useSessionCatalogDomain";
import { formatUnixMs, isJsonObject, readString, type JsonObject } from "../shared";
import type { ConsoleAppState } from "../useConsoleAppState";

type SessionsSectionProps = {
  app: Pick<ConsoleAppState, "api" | "setError" | "setNotice">;
};

export function SessionsSection({ app }: SessionsSectionProps) {
  const navigate = useNavigate();
  const catalog = useSessionCatalogDomain(app);
  const selected = catalog.selectedSession;
  const [phase4Busy, setPhase4Busy] = useState<"checkpoint" | "preview" | "apply" | null>(null);
  const [continuityBusy, setContinuityBusy] = useState(false);
  const [sessionCompactions, setSessionCompactions] = useState<ChatCompactionArtifactRecord[]>([]);
  const [sessionCheckpoints, setSessionCheckpoints] = useState<ChatCheckpointRecord[]>([]);
  const [compactionPreview, setCompactionPreview] = useState<ChatCompactionPreview | null>(null);
  const [objectivesBusy, setObjectivesBusy] = useState(false);
  const [objectives, setObjectives] = useState<JsonObject[]>([]);
  const selectedLineage = buildSessionLineageHint(selected);
  const selectedObjective = useMemo(
    () =>
      findObjectiveForSession(
        objectives,
        selected === null
          ? null
          : {
              session_id: selected.session_id,
              session_key: selected.session_key,
              session_label: selected.session_label,
            },
      ),
    [objectives, selected],
  );

  useEffect(() => {
    let cancelled = false;

    async function loadContinuitySummary(): Promise<void> {
      if (selected === null) {
        setSessionCompactions([]);
        setSessionCheckpoints([]);
        setCompactionPreview(null);
        return;
      }

      setContinuityBusy(true);
      app.setError(null);
      try {
        const response = await app.api.getSessionTranscript(selected.session_id);
        if (cancelled) {
          return;
        }
        setSessionCompactions(response.compactions);
        setSessionCheckpoints(response.checkpoints);
      } catch (error) {
        if (!cancelled) {
          app.setError(error instanceof Error ? error.message : "Unexpected failure.");
        }
      } finally {
        if (!cancelled) {
          setContinuityBusy(false);
        }
      }
    }

    void loadContinuitySummary();
    return () => {
      cancelled = true;
    };
  }, [app, selected?.session_id]);

  useEffect(() => {
    let cancelled = false;

    async function loadObjectives(): Promise<void> {
      setObjectivesBusy(true);
      try {
        const response = await app.api.listObjectives(new URLSearchParams({ limit: "64" }));
        if (cancelled) {
          return;
        }
        setObjectives(
          Array.isArray(response.objectives) ? response.objectives.filter(isJsonObject) : [],
        );
      } catch (error) {
        if (!cancelled) {
          app.setError(error instanceof Error ? error.message : "Unexpected failure.");
        }
      } finally {
        if (!cancelled) {
          setObjectivesBusy(false);
        }
      }
    }

    void loadObjectives();
    return () => {
      cancelled = true;
    };
  }, [app]);

  async function createCheckpoint(): Promise<void> {
    if (selected === null) {
      app.setError("Select a session first.");
      return;
    }
    setPhase4Busy("checkpoint");
    app.setError(null);
    app.setNotice(null);
    try {
      const label = selected.session_label?.trim() || selected.title.trim() || "Session";
      const response = await app.api.createSessionCheckpoint(selected.session_id, {
        name: `${label} checkpoint`,
        note: `Created from the Sessions console on ${new Date().toLocaleString()}.`,
        tags: ["web-console", "sessions-section"],
      });
      setSessionCheckpoints((previous) => [...previous, response.checkpoint]);
      app.setNotice(`Checkpoint created: ${response.checkpoint.name}.`);
    } catch (error) {
      app.setError(error instanceof Error ? error.message : "Unexpected failure.");
    } finally {
      setPhase4Busy(null);
    }
  }

  async function previewCompaction(): Promise<void> {
    if (selected === null) {
      app.setError("Select a session first.");
      return;
    }
    setPhase4Busy("preview");
    app.setError(null);
    app.setNotice(null);
    try {
      const response = await app.api.previewSessionCompaction(selected.session_id, {
        trigger_reason: "sessions_section_preview",
        trigger_policy: "operator_preview",
      });
      setCompactionPreview(response.preview);
      const summary = readCompactionSummary(response.preview.summary);
      const reviewCount = summary?.planner?.review_candidate_count ?? 0;
      const writeCount = summary?.writes?.length ?? 0;
      app.setNotice(
        response.preview.eligible
          ? `Compaction preview ready: ${writeCount} planned write${writeCount === 1 ? "" : "s"}${reviewCount > 0 ? ` and ${reviewCount} review candidate${reviewCount === 1 ? "" : "s"}` : ""}.`
          : "Compaction preview is blocked for this session right now.",
      );
    } catch (error) {
      app.setError(error instanceof Error ? error.message : "Unexpected failure.");
    } finally {
      setPhase4Busy(null);
    }
  }

  async function applyCompaction(): Promise<void> {
    if (selected === null) {
      app.setError("Select a session first.");
      return;
    }

    const preview =
      compactionPreview?.trigger_reason === "sessions_section_preview"
        ? compactionPreview
        : await app.api
            .previewSessionCompaction(selected.session_id, {
              trigger_reason: "sessions_section_preview",
              trigger_policy: "operator_preview",
            })
            .then((response) => {
              setCompactionPreview(response.preview);
              return response.preview;
            });
    const summary = readCompactionSummary(preview.summary);
    const reviewCount = summary?.planner?.review_candidate_count ?? 0;
    if (reviewCount > 0) {
      app.setNotice(
        `Compaction review is required for ${reviewCount} candidate${reviewCount === 1 ? "" : "s"}. Open the session in chat to accept or reject them explicitly.`,
      );
      return;
    }

    setPhase4Busy("apply");
    app.setError(null);
    app.setNotice(null);
    try {
      const response = await app.api.applySessionCompaction(selected.session_id, {
        trigger_reason: "sessions_section_apply",
        trigger_policy: "operator_apply",
      });
      setSessionCompactions((previous) => [...previous, response.artifact]);
      setSessionCheckpoints((previous) => [...previous, response.checkpoint]);
      setCompactionPreview(response.preview);
      const appliedSummary = safeParseCompactionSummaryJson(response.artifact.summary_json);
      const writeCount = appliedSummary?.writes?.length ?? 0;
      app.setNotice(
        `Compaction applied: ${writeCount} durable write${writeCount === 1 ? "" : "s"} and checkpoint ${response.checkpoint.name}.`,
      );
    } catch (error) {
      app.setError(error instanceof Error ? error.message : "Unexpected failure.");
    } finally {
      setPhase4Busy(null);
    }
  }

  function openChatWithArtifact(options: {
    runId?: string;
    compactionId?: string;
    checkpointId?: string;
  }): void {
    if (selected === null) {
      return;
    }
    const search = new URLSearchParams();
    search.set("sessionId", selected.session_id);
    if (options.runId !== undefined && options.runId.length > 0) {
      search.set("runId", options.runId);
    }
    if (options.compactionId !== undefined && options.compactionId.length > 0) {
      search.set("compactionId", options.compactionId);
    }
    if (options.checkpointId !== undefined && options.checkpointId.length > 0) {
      search.set("checkpointId", options.checkpointId);
    }
    void navigate(`${getSectionPath("chat")}?${search.toString()}`);
  }

  const continuitySummary =
    compactionPreview === null ? null : readCompactionSummary(compactionPreview.summary);
  const continuityReviewCount = continuitySummary?.planner?.review_candidate_count ?? 0;
  const continuityWriteCount = continuitySummary?.writes?.length ?? 0;
  const recentCompactions = [...sessionCompactions].reverse().slice(0, 3);
  const recentCheckpoints = [...sessionCheckpoints].reverse().slice(0, 3);

  return (
    <main className="workspace-page">
      <WorkspacePageHeader
        eyebrow="Control"
        title="Sessions"
        description="Search session history, inspect latest run posture, and drive lifecycle actions without leaving the operator console."
        status={
          <>
            <WorkspaceStatusChip tone={catalog.busy ? "warning" : "success"}>
              {catalog.busy ? "Refreshing" : "Catalog ready"}
            </WorkspaceStatusChip>
            <WorkspaceStatusChip tone={selected?.pending_approvals ? "warning" : "default"}>
              {selected?.pending_approvals ?? 0} pending approvals
            </WorkspaceStatusChip>
            <WorkspaceStatusChip
              tone={workspaceToneForState(selected?.last_run_state ?? "unknown")}
            >
              {selected?.last_run_state ?? "No run selected"}
            </WorkspaceStatusChip>
          </>
        }
        actions={
          <ActionButton
            isDisabled={catalog.busy}
            type="button"
            variant="primary"
            onPress={() => void catalog.refreshSessions()}
          >
            {catalog.busy ? "Refreshing..." : "Refresh sessions"}
          </ActionButton>
        }
      />

      <section className="workspace-metric-grid">
        <WorkspaceMetricCard
          detail="Visible non-archived sessions in the current scoped catalog."
          label="Active sessions"
          value={catalog.summary?.active_sessions ?? 0}
        />
        <WorkspaceMetricCard
          detail="Archived records stay queryable without reopening the chat rail."
          label="Archived sessions"
          value={catalog.summary?.archived_sessions ?? 0}
        />
        <WorkspaceMetricCard
          detail="Sessions currently waiting on sensitive-action decisions."
          label="Pending approvals"
          tone={(catalog.summary?.sessions_with_pending_approvals ?? 0) > 0 ? "warning" : "default"}
          value={catalog.summary?.sessions_with_pending_approvals ?? 0}
        />
        <WorkspaceMetricCard
          detail="Latest known run is still accepted or in progress."
          label="Active runs"
          tone={(catalog.summary?.sessions_with_active_runs ?? 0) > 0 ? "accent" : "default"}
          value={catalog.summary?.sessions_with_active_runs ?? 0}
        />
        <WorkspaceMetricCard
          detail="Sessions carrying active context files or workspace references."
          label="Context files"
          tone={(catalog.summary?.sessions_with_context_files ?? 0) > 0 ? "accent" : "default"}
          value={catalog.summary?.sessions_with_context_files ?? 0}
        />
      </section>

      <WorkspaceSectionCard
        description="Catalog filters stay server-backed so chat, web, and future operator surfaces do not invent separate session logic."
        title="Filters"
      >
        <div className="workspace-form-grid">
          <TextInputField
            label="Search"
            placeholder="title, family, agent, model, file, or recap"
            value={catalog.query}
            onChange={catalog.setQuery}
          />
          <SelectField
            label="Sort"
            options={[
              { key: "updated_desc", label: "Updated (newest)" },
              { key: "updated_asc", label: "Updated (oldest)" },
              { key: "created_desc", label: "Created (newest)" },
              { key: "created_asc", label: "Created (oldest)" },
              { key: "title_asc", label: "Title (A-Z)" },
            ]}
            value={catalog.sort}
            onChange={(value) =>
              catalog.setSort(
                value as
                  | "updated_desc"
                  | "updated_asc"
                  | "created_desc"
                  | "created_asc"
                  | "title_asc",
              )
            }
          />
          <SelectField
            label="Title mode"
            options={[
              { key: "all", label: "Any title mode" },
              { key: "ready", label: "Auto title ready" },
              { key: "pending", label: "Auto title pending" },
              { key: "failed", label: "Auto title failed" },
              { key: "idle", label: "Auto title idle" },
            ]}
            value={catalog.titleState}
            onChange={catalog.setTitleState}
          />
          <SelectField
            label="Title source"
            options={[
              { key: "all", label: "Any title source" },
              { key: "label", label: "Manual label" },
              { key: "semantic_title", label: "Semantic title" },
              { key: "auto_title", label: "Automatic title" },
              { key: "session_key", label: "Session key fallback" },
            ]}
            value={catalog.titleSource}
            onChange={catalog.setTitleSource}
          />
          <SelectField
            label="Branch state"
            options={[
              { key: "all", label: "Any lineage" },
              { key: "root", label: "Root session" },
              { key: "active_branch", label: "Active branch" },
              { key: "branch_source", label: "Branch source" },
            ]}
            value={catalog.branchState}
            onChange={catalog.setBranchState}
          />
          <SelectField
            label="Pending approvals"
            options={[
              { key: "all", label: "Any approval state" },
              { key: "yes", label: "With pending approvals" },
              { key: "no", label: "Without pending approvals" },
            ]}
            value={catalog.hasPendingApprovals}
            onChange={(value) => catalog.setHasPendingApprovals(value as "all" | "yes" | "no")}
          />
          <SelectField
            label="Context files"
            options={[
              { key: "all", label: "Any context posture" },
              { key: "yes", label: "With context files" },
              { key: "no", label: "Without context files" },
            ]}
            value={catalog.hasContextFiles}
            onChange={(value) => catalog.setHasContextFiles(value as "all" | "yes" | "no")}
          />
          <TextInputField
            label="Agent"
            placeholder="agent id"
            value={catalog.agentId}
            onChange={catalog.setAgentId}
          />
          <TextInputField
            label="Model profile"
            placeholder="model profile"
            value={catalog.modelProfile}
            onChange={catalog.setModelProfile}
          />
          <SwitchField
            checked={catalog.includeArchived}
            description="Include archived records in the current list."
            label="Show archived"
            onChange={catalog.setIncludeArchived}
          />
        </div>
      </WorkspaceSectionCard>

      <section className="workspace-two-column">
        <WorkspaceSectionCard
          description="Pick a session to inspect its latest activity, preview, and lifecycle state."
          title="Catalog"
        >
          {catalog.entries.length === 0 ? (
            <WorkspaceEmptyState
              description="Adjust filters or create activity in chat to populate the session catalog."
              title="No sessions match the current query"
            />
          ) : (
            <WorkspaceTable
              ariaLabel="Session catalog"
              columns={["Title", "Family", "Updated", "Controls", "Recap"]}
            >
              {catalog.entries.map((entry) => {
                const selectedRow = entry.session_id === catalog.selectedSessionId;
                return (
                  <tr
                    key={entry.session_id}
                    className={selectedRow ? "bg-content2/60" : undefined}
                    onClick={() => catalog.setSelectedSessionId(entry.session_id)}
                  >
                    <td>
                      <div className="workspace-stack">
                        <strong>{entry.title}</strong>
                        <small className="text-muted">
                          {describeTitleGenerationState(
                            entry.title_generation_state,
                            entry.manual_title_locked,
                          )}{" "}
                          · {entry.archived ? "archived" : entry.title_source}
                        </small>
                      </div>
                    </td>
                    <td>
                      <div className="workspace-stack">
                        <strong>{entry.family.root_title}</strong>
                        <small className="text-muted">
                          {describeBranchState(entry.branch_state)}
                          {entry.family.family_size > 1
                            ? ` · ${entry.family.sequence}/${entry.family.family_size}`
                            : ""}
                        </small>
                      </div>
                    </td>
                    <td>{formatUnixMs(entry.updated_at_unix_ms)}</td>
                    <td>
                      <div className="workspace-stack">
                        <small>
                          {entry.quick_controls.agent.display_value} ·{" "}
                          {entry.quick_controls.model.display_value}
                        </small>
                        <small className="text-muted">
                          {entry.pending_approvals} approval
                          {entry.pending_approvals === 1 ? "" : "s"}
                          {entry.has_context_files
                            ? ` · ${entry.recap.active_context_files.length} context file${entry.recap.active_context_files.length === 1 ? "" : "s"}`
                            : ""}
                        </small>
                      </div>
                    </td>
                    <td>{entry.preview ?? entry.last_summary ?? "No recap"}</td>
                  </tr>
                );
              })}
            </WorkspaceTable>
          )}
        </WorkspaceSectionCard>

        <WorkspaceSectionCard
          description="Lifecycle actions here reuse the same backend mutations as chat instead of inventing a separate control path."
          title="Detail"
        >
          {selected === null ? (
            <WorkspaceEmptyState
              compact
              description="Select a row from the session catalog to inspect details and actions."
              title="No session selected"
            />
          ) : (
            <div className="workspace-stack">
              <div className="workspace-panel__intro">
                <p className="workspace-kicker">Selected session</p>
                <h3>{selected.title}</h3>
                <p className="chat-muted">
                  {selected.preview ??
                    selected.last_summary ??
                    "No preview was derivable from existing run history."}
                </p>
                <div className="workspace-chip-row">
                  <WorkspaceStatusChip tone={selected.manual_title_locked ? "accent" : "default"}>
                    {describeTitleGenerationState(
                      selected.title_generation_state,
                      selected.manual_title_locked,
                    )}
                  </WorkspaceStatusChip>
                  <WorkspaceStatusChip tone="default">
                    {selected.quick_controls.agent.display_value}
                  </WorkspaceStatusChip>
                  <WorkspaceStatusChip tone="default">
                    {selected.quick_controls.model.display_value}
                  </WorkspaceStatusChip>
                  {selected.family.family_size > 1 ? (
                    <WorkspaceStatusChip tone="accent">
                      Family {selected.family.sequence}/{selected.family.family_size}
                    </WorkspaceStatusChip>
                  ) : null}
                </div>
              </div>

              <TextInputField
                disabled={catalog.busy}
                description="Leave empty to return the session to automatic title mode."
                label="Session label"
                value={catalog.renameDraft}
                onChange={catalog.setRenameDraft}
              />

              <div className="workspace-inline">
                <ActionButton
                  isDisabled={catalog.busy}
                  type="button"
                  variant="primary"
                  onPress={() => void catalog.renameSelectedSession()}
                >
                  Rename
                </ActionButton>
                <ActionButton
                  isDisabled={catalog.busy}
                  type="button"
                  variant="secondary"
                  onPress={() => void catalog.resetSelectedSession()}
                >
                  Reset
                </ActionButton>
                <ActionButton
                  isDisabled={catalog.busy}
                  type="button"
                  variant="danger"
                  onPress={() => void catalog.archiveSelectedSession()}
                >
                  Archive
                </ActionButton>
                <ActionButton
                  isDisabled={catalog.busy || !selected.last_run_id}
                  type="button"
                  variant="ghost"
                  onPress={() => void catalog.abortSelectedRun()}
                >
                  Abort run
                </ActionButton>
                <ActionButton
                  isDisabled={catalog.busy || phase4Busy !== null}
                  type="button"
                  variant="secondary"
                  onPress={() => void createCheckpoint()}
                >
                  {phase4Busy === "checkpoint" ? "Checkpointing..." : "Create checkpoint"}
                </ActionButton>
                <ActionButton
                  isDisabled={catalog.busy || phase4Busy !== null}
                  type="button"
                  variant="secondary"
                  onPress={() => void previewCompaction()}
                >
                  {phase4Busy === "preview" ? "Previewing..." : "Preview compaction"}
                </ActionButton>
                <ActionButton
                  isDisabled={catalog.busy || phase4Busy !== null}
                  type="button"
                  variant="primary"
                  onPress={() => void applyCompaction()}
                >
                  {phase4Busy === "apply" ? "Applying..." : "Apply compaction"}
                </ActionButton>
              </div>

              <ActionButton
                type="button"
                variant="secondary"
                onPress={() => {
                  if (selectedObjective !== null) {
                    void navigate(
                      buildObjectiveChatHref({
                        objective: selectedObjective,
                        fallbackSessionId: selected.session_id,
                        runId: selected.last_run_id,
                      }),
                    );
                    return;
                  }
                  const search = new URLSearchParams();
                  search.set("sessionId", selected.session_id);
                  if (selected.last_run_id) {
                    search.set("runId", selected.last_run_id);
                  }
                  void navigate(`${getSectionPath("chat")}?${search.toString()}`);
                }}
              >
                Open in chat
              </ActionButton>
              <ActionButton
                type="button"
                variant="secondary"
                isDisabled={selectedObjective === null}
                onPress={() => {
                  if (selectedObjective === null) {
                    return;
                  }
                  const objectiveId = readString(selectedObjective, "objective_id");
                  if (objectiveId === null) {
                    return;
                  }
                  void navigate(buildObjectiveOverviewHref(objectiveId));
                }}
              >
                Open objective
              </ActionButton>
              <ActionButton
                type="button"
                variant="ghost"
                onPress={() =>
                  void navigate(`${getSectionPath("inventory")}?deviceId=${selected.device_id}`)
                }
              >
                Open inventory
              </ActionButton>

              <dl className="workspace-key-value-grid">
                <div>
                  <dt>Session key</dt>
                  <dd>{selected.session_key}</dd>
                </div>
                <div>
                  <dt>Title source</dt>
                  <dd>{selected.title_source}</dd>
                </div>
                <div>
                  <dt>Family root</dt>
                  <dd>{selected.family.root_title}</dd>
                </div>
                <div>
                  <dt>Created</dt>
                  <dd>{formatUnixMs(selected.created_at_unix_ms)}</dd>
                </div>
                <div>
                  <dt>Updated</dt>
                  <dd>{formatUnixMs(selected.updated_at_unix_ms)}</dd>
                </div>
                <div>
                  <dt>Run state</dt>
                  <dd>{selected.last_run_state ?? "none"}</dd>
                </div>
                <div>
                  <dt>Branch state</dt>
                  <dd>{describeBranchState(selected.branch_state)}</dd>
                </div>
                <div>
                  <dt>Lineage</dt>
                  <dd>{selectedLineage}</dd>
                </div>
                <div>
                  <dt>Total tokens</dt>
                  <dd>{selected.total_tokens}</dd>
                </div>
                <div>
                  <dt>Pending approvals</dt>
                  <dd>{selected.pending_approvals}</dd>
                </div>
                <div>
                  <dt>Context files</dt>
                  <dd>
                    {selected.has_context_files
                      ? `${selected.recap.active_context_files.length} active`
                      : "none"}
                  </dd>
                </div>
              </dl>

              {selected.last_intent || selected.last_summary ? (
                <WorkspaceInlineNotice title="Latest activity" tone="default">
                  <p>
                    <strong>Last intent:</strong> {selected.last_intent ?? "Missing"}
                  </p>
                  <p>
                    <strong>Last summary:</strong> {selected.last_summary ?? "Missing"}
                  </p>
                </WorkspaceInlineNotice>
              ) : null}

              {selected.recap.touched_files.length > 0 ||
              selected.recap.active_context_files.length > 0 ||
              selected.recap.recent_artifacts.length > 0 ? (
                <WorkspaceInlineNotice title="Resume recap" tone="accent">
                  {selected.recap.touched_files.length > 0 ? (
                    <p>
                      <strong>Touched files:</strong> {selected.recap.touched_files.join(", ")}
                    </p>
                  ) : null}
                  {selected.recap.active_context_files.length > 0 ? (
                    <p>
                      <strong>Active context:</strong>{" "}
                      {selected.recap.active_context_files.join(", ")}
                    </p>
                  ) : null}
                  {selected.recap.recent_artifacts.length > 0 ? (
                    <p>
                      <strong>Recent artifacts:</strong>{" "}
                      {selected.recap.recent_artifacts
                        .map((artifact) => `${artifact.label} (${artifact.kind})`)
                        .join(", ")}
                    </p>
                  ) : null}
                </WorkspaceInlineNotice>
              ) : null}

              <WorkspaceInlineNotice
                title={selectedObjective === null ? "Objective linkage" : "Linked objective"}
                tone={selectedObjective === null ? "default" : "accent"}
              >
                {selectedObjective === null ? (
                  objectivesBusy ? (
                    <p>Loading objective linkage for the selected session.</p>
                  ) : (
                    <p>No objective currently points at this session.</p>
                  )
                ) : (
                  <>
                    <p>
                      <strong>
                        {readString(selectedObjective, "name") ?? "Unnamed objective"}
                      </strong>{" "}
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
                  </>
                )}
              </WorkspaceInlineNotice>

              {compactionPreview !== null ? (
                <WorkspaceInlineNotice
                  title={compactionPreview.eligible ? "Compaction preview" : "Compaction blocked"}
                  tone={
                    !compactionPreview.eligible
                      ? "warning"
                      : continuityReviewCount > 0
                        ? "warning"
                        : "success"
                  }
                >
                  <p>
                    <strong>Summary:</strong> {compactionPreview.summary_preview}
                  </p>
                  <p>
                    <strong>Token delta:</strong> {compactionPreview.token_delta} ·{" "}
                    <strong>Planned writes:</strong> {continuityWriteCount} ·{" "}
                    <strong>Review candidates:</strong> {continuityReviewCount}
                  </p>
                  {continuityReviewCount > 0 ? (
                    <p>
                      Use the chat compaction flow to accept or reject the review-required
                      candidates explicitly.
                    </p>
                  ) : null}
                </WorkspaceInlineNotice>
              ) : null}

              <div className="workspace-inline-actions">
                <WorkspaceStatusChip tone={continuityBusy ? "warning" : "default"}>
                  {continuityBusy
                    ? "Loading continuity"
                    : `${sessionCompactions.length} compactions`}
                </WorkspaceStatusChip>
                <WorkspaceStatusChip tone={sessionCheckpoints.length > 0 ? "accent" : "default"}>
                  {sessionCheckpoints.length} checkpoints
                </WorkspaceStatusChip>
                <WorkspaceStatusChip tone={continuityReviewCount > 0 ? "warning" : "default"}>
                  {continuityReviewCount} pending review
                </WorkspaceStatusChip>
              </div>

              <div className="workspace-stack">
                <div className="workspace-panel__intro">
                  <p className="workspace-kicker">Continuity artifacts</p>
                  <h3>Recent compactions</h3>
                  <p className="chat-muted">
                    Inspect the last stored compactions and jump straight into the chat detail
                    sidebar for raw diff and audit context.
                  </p>
                </div>
                {recentCompactions.length === 0 ? (
                  <WorkspaceEmptyState
                    compact
                    description="No compaction artifacts are stored for this session yet."
                    title="No compactions yet"
                  />
                ) : (
                  <div className="chat-ops-list">
                    {recentCompactions.map((artifact) => {
                      const summary = safeParseCompactionSummaryJson(artifact.summary_json);
                      const lifecycleState = summary?.lifecycle_state ?? "stored";
                      const reviewCount = summary?.planner?.review_candidate_count ?? 0;
                      const writeCount = summary?.writes?.length ?? 0;
                      return (
                        <article key={artifact.artifact_id} className="chat-ops-card">
                          <div className="chat-ops-card__copy">
                            <strong>{artifact.mode}</strong>
                            <span>
                              {lifecycleState.replaceAll("_", " ")} · {writeCount} write
                              {writeCount === 1 ? "" : "s"} · {reviewCount} review
                            </span>
                            <p>{artifact.summary_preview}</p>
                          </div>
                          <div className="chat-ops-card__actions">
                            <WorkspaceStatusChip tone={reviewCount > 0 ? "warning" : "accent"}>
                              {artifact.strategy}
                            </WorkspaceStatusChip>
                            <ActionButton
                              size="sm"
                              type="button"
                              variant="ghost"
                              onPress={() =>
                                openChatWithArtifact({
                                  runId: artifact.run_id,
                                  compactionId: artifact.artifact_id,
                                })
                              }
                            >
                              Open in chat
                            </ActionButton>
                          </div>
                        </article>
                      );
                    })}
                  </div>
                )}
              </div>

              <div className="workspace-stack">
                <div className="workspace-panel__intro">
                  <p className="workspace-kicker">Recovery points</p>
                  <h3>Recent checkpoints</h3>
                  <p className="chat-muted">
                    Checkpoints stay paired with compaction history so rollback is visible without
                    opening the raw journal.
                  </p>
                </div>
                {recentCheckpoints.length === 0 ? (
                  <WorkspaceEmptyState
                    compact
                    description="Create a checkpoint or apply a compaction to start the rollback history."
                    title="No checkpoints yet"
                  />
                ) : (
                  <div className="chat-ops-list">
                    {recentCheckpoints.map((checkpoint) => (
                      <article key={checkpoint.checkpoint_id} className="chat-ops-card">
                        <div className="chat-ops-card__copy">
                          <strong>{checkpoint.name}</strong>
                          <span>
                            {describeBranchState(checkpoint.branch_state)} · restores{" "}
                            {checkpoint.restore_count}
                          </span>
                          <p>{checkpoint.note ?? "No note recorded for this checkpoint."}</p>
                        </div>
                        <div className="chat-ops-card__actions">
                          <WorkspaceStatusChip tone="accent">
                            {formatUnixMs(checkpoint.created_at_unix_ms)}
                          </WorkspaceStatusChip>
                          <ActionButton
                            size="sm"
                            type="button"
                            variant="ghost"
                            onPress={() =>
                              openChatWithArtifact({
                                runId: checkpoint.run_id,
                                checkpointId: checkpoint.checkpoint_id,
                              })
                            }
                          >
                            Open in chat
                          </ActionButton>
                        </div>
                      </article>
                    ))}
                  </div>
                )}
              </div>
            </div>
          )}
        </WorkspaceSectionCard>
      </section>
    </main>
  );
}

type ContinuitySummary = {
  lifecycle_state?: string;
  planner?: { review_candidate_count?: number };
  writes?: Array<unknown>;
};

function readCompactionSummary(value: unknown): ContinuitySummary | undefined {
  if (value === null || value === undefined || typeof value !== "object" || Array.isArray(value)) {
    return undefined;
  }
  return value as ContinuitySummary;
}

function safeParseCompactionSummaryJson(
  value: string | null | undefined,
): ContinuitySummary | undefined {
  if (typeof value !== "string" || value.trim().length === 0) {
    return undefined;
  }
  try {
    return JSON.parse(value) as ContinuitySummary;
  } catch {
    return undefined;
  }
}
