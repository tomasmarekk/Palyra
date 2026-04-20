import { Chip } from "@heroui/react";
import type { ComponentProps } from "react";
import type {
  ChatBackgroundTaskRecord,
  ChatPinRecord,
  ChatQueuedInputRecord,
  ChatCheckpointRecord,
  ChatCompactionArtifactRecord,
  ChatRunLineage,
  ChatRunStatusRecord,
  ChatRunTapeSnapshot,
  ChatTranscriptRecord,
  ConsoleApiClient,
  JsonValue,
  SessionCatalogRecord,
  WorkspaceRestoreResponseEnvelope,
} from "../consoleApi";
import {
  ActionButton,
  ActionCluster,
  EmptyState,
  KeyValueList,
  SectionCard,
} from "../console/components/ui";

import { ChatRunDrawer, type RunDrawerTab } from "./ChatRunDrawer";
import { ChatSessionQuickControlPanel } from "./ChatSessionQuickControls";
import {
  PrettyJsonBlock,
  describeBranchState,
  describeTitleGenerationState,
  formatApproxTokens,
  prettifyEventType,
  shortId,
} from "./chatShared";

export type TranscriptSearchMatch = {
  session_id: string;
  run_id: string;
  seq: number;
  event_type: string;
  created_at_unix_ms: number;
  origin_kind: string;
  origin_run_id?: string;
  snippet: string;
};

export type DetailPanelState = {
  id: string;
  title: string;
  subtitle: string;
  body?: string;
  payload?: JsonValue;
  actions?: Array<{
    key: string;
    label: string;
    variant?: "primary" | "secondary" | "ghost" | "danger";
    onPress: () => void | Promise<void>;
  }>;
};

type ChatInspectorColumnProps = {
  pendingApprovalCount: number;
  a2uiSurfaces: string[];
  runIds: string[];
  selectedSession: SessionCatalogRecord | null;
  selectedSessionLineage: string;
  sessionQuickControlPanelProps: ComponentProps<typeof ChatSessionQuickControlPanel>;
  contextBudgetEstimatedTokens: number;
  projectContextBusy: boolean;
  refreshProjectContext: () => void;
  disableProjectContextEntry: (entryId: string) => void;
  enableProjectContextEntry: (entryId: string) => void;
  approveProjectContextEntry: (entryId: string) => void;
  scaffoldProjectContext: () => void;
  transcriptBusy: boolean;
  transcriptSearchQuery: string;
  setTranscriptSearchQuery: (value: string) => void;
  transcriptSearchBusy: boolean;
  canSearchTranscript: boolean;
  pinnedRecordKeys: ReadonlySet<string>;
  searchResults: TranscriptSearchMatch[];
  searchTranscript: () => void;
  inspectSearchMatch: (match: TranscriptSearchMatch) => void;
  exportBusy: "json" | "markdown" | null;
  exportTranscript: (format: "json" | "markdown") => void;
  recentTranscriptRecords: ChatTranscriptRecord[];
  inspectTranscriptRecord: (record: ChatTranscriptRecord) => void;
  pinTranscriptRecord: (record: ChatTranscriptRecord) => void;
  sessionPins: ChatPinRecord[];
  deletePin: (pinId: string) => void;
  compactions: ChatCompactionArtifactRecord[];
  inspectCompaction: (artifactId: string) => void;
  checkpoints: ChatCheckpointRecord[];
  inspectCheckpoint: (checkpointId: string) => void;
  restoreCheckpoint: (checkpointId: string) => void;
  queuedInputs: ChatQueuedInputRecord[];
  inspectQueuePolicy: () => void;
  runSessionQueueAction: (action: "pause" | "resume" | "drain" | "collect-summary") => void;
  cancelQueuedInput: (queuedInputId: string) => void;
  backgroundTasks: ChatBackgroundTaskRecord[];
  inspectBackgroundTask: (taskId: string) => void;
  runBackgroundTaskAction: (
    taskId: string,
    action: "pause" | "resume" | "retry" | "cancel",
  ) => void;
  detailPanel: DetailPanelState | null;
  revealSensitiveValues: boolean;
  inspectorVisible: boolean;
  openRunDetails: (runId: string, tab?: RunDrawerTab) => void;
  sessionMaintenanceBusyKey: string | null;
  runDrawerId: string;
  setRunDrawerId: (runId: string) => void;
  runDrawerBusy: boolean;
  runStatus: ChatRunStatusRecord | null;
  runTape: ChatRunTapeSnapshot | null;
  runLineage: ChatRunLineage | null;
  runDrawerTab: RunDrawerTab;
  setRunDrawerTab: (tab: RunDrawerTab) => void;
  api: ConsoleApiClient;
  setError: (next: string | null) => void;
  setNotice: (next: string | null) => void;
  onWorkspaceRestore: (response: WorkspaceRestoreResponseEnvelope) => Promise<void>;
  openMemorySection: () => void;
  openSupportSection: () => void;
  refreshRunDetails: () => void;
  closeRunDrawer: () => void;
  openCanvasSurface: (canvasUrl: string, runId?: string) => void;
  openBrowserSessionWorkbench: (sessionId: string) => void;
};

export function ChatInspectorColumn({
  pendingApprovalCount,
  a2uiSurfaces,
  runIds,
  selectedSession,
  selectedSessionLineage,
  sessionQuickControlPanelProps,
  contextBudgetEstimatedTokens,
  projectContextBusy,
  refreshProjectContext,
  disableProjectContextEntry,
  enableProjectContextEntry,
  approveProjectContextEntry,
  scaffoldProjectContext,
  transcriptBusy,
  transcriptSearchQuery,
  setTranscriptSearchQuery,
  transcriptSearchBusy,
  canSearchTranscript,
  pinnedRecordKeys,
  searchResults,
  searchTranscript,
  inspectSearchMatch,
  exportBusy,
  exportTranscript,
  recentTranscriptRecords,
  inspectTranscriptRecord,
  pinTranscriptRecord,
  sessionPins,
  deletePin,
  compactions,
  inspectCompaction,
  checkpoints,
  inspectCheckpoint,
  restoreCheckpoint,
  queuedInputs,
  inspectQueuePolicy,
  runSessionQueueAction,
  cancelQueuedInput,
  backgroundTasks,
  inspectBackgroundTask,
  runBackgroundTaskAction,
  detailPanel,
  revealSensitiveValues,
  inspectorVisible,
  openRunDetails,
  sessionMaintenanceBusyKey,
  runDrawerId,
  setRunDrawerId,
  runDrawerBusy,
  runStatus,
  runTape,
  runLineage,
  runDrawerTab,
  setRunDrawerTab,
  api,
  setError,
  setNotice,
  onWorkspaceRestore,
  openMemorySection,
  openSupportSection,
  refreshRunDetails,
  closeRunDrawer,
  openCanvasSurface,
  openBrowserSessionWorkbench,
}: ChatInspectorColumnProps) {
  const sessionMaintenanceBusy = sessionMaintenanceBusyKey !== null;
  const browserSessionIds = extractBrowserSessionIds(runTape);
  const sessionProjectContext = selectedSession?.recap.project_context ?? null;

  return (
    <div className="chat-inspector-column">
      <SectionCard
        className="chat-panel chat-panel--sticky"
        description="Branch lineage, queue backlog, and persisted transcript tools stay visible without turning the main conversation into a debug dump."
        title="Workspace signals"
      >
        <div className="grid gap-4">
          <div>
            <p className="workspace-kicker">Session quick controls</p>
            <ChatSessionQuickControlPanel {...sessionQuickControlPanelProps} />
          </div>
        </div>
        <div className="workspace-tag-row">
          <Chip color={pendingApprovalCount > 0 ? "warning" : "success"} variant="soft">
            {formatCountLabel(pendingApprovalCount, "approval")}
          </Chip>
          <Chip variant="secondary">{formatCountLabel(a2uiSurfaces.length, "A2UI surface")}</Chip>
          <Chip variant="secondary">{formatCountLabel(runIds.length, "known run")}</Chip>
          <Chip variant="secondary">{formatCountLabel(compactions.length, "compaction")}</Chip>
          <Chip variant="secondary">
            {formatCountLabel(backgroundTasks.length, "background task")}
          </Chip>
        </div>
        <KeyValueList
          items={[
            {
              label: "Session",
              value:
                selectedSession?.title ||
                (selectedSession ? shortId(selectedSession.session_id) : "none"),
            },
            {
              label: "Branch state",
              value: describeBranchState(selectedSession?.branch_state ?? "missing"),
            },
            {
              label: "Title mode",
              value:
                selectedSession === null
                  ? "none"
                  : describeTitleGenerationState(
                      selectedSession.title_generation_state,
                      selectedSession.manual_title_locked,
                    ),
            },
            {
              label: "Lineage",
              value: selectedSessionLineage,
            },
            {
              label: "Agent",
              value:
                selectedSession?.quick_controls.agent.display_value ??
                selectedSession?.agent_id ??
                "default",
            },
            {
              label: "Model",
              value:
                selectedSession?.quick_controls.model.display_value ??
                selectedSession?.model_profile ??
                "inherited",
            },
            {
              label: "Budget",
              value: `${formatApproxTokens(contextBudgetEstimatedTokens)} estimated`,
            },
          ]}
        />
        {selectedSession !== null ? (
          <div className="chat-ops-list">
            <div className="workspace-panel__intro">
              <p className="workspace-kicker">Resume recap</p>
              <h3>
                {selectedSession.family.family_size > 1
                  ? `Family ${selectedSession.family.sequence}/${selectedSession.family.family_size}`
                  : "Single-session thread"}
              </h3>
              <p className="chat-muted">
                {selectedSession.preview ??
                  selectedSession.last_summary ??
                  "No summary has been published for this session yet."}
              </p>
            </div>
            {selectedSession.recap.touched_files.length > 0 ? (
              <div>
                <p className="workspace-kicker">Touched files</p>
                <ul className="workspace-bullet-list">
                  {selectedSession.recap.touched_files.slice(0, 5).map((file) => (
                    <li key={file}>{file}</li>
                  ))}
                </ul>
              </div>
            ) : null}
            {sessionProjectContext !== null ? (
              <div className="chat-ops-list">
                <div className="workspace-panel__intro">
                  <p className="workspace-kicker">Deterministic project context</p>
                  <h3>{sessionProjectContext.active_entries} active project rules</h3>
                  <p className="chat-muted">
                    These files are deterministic project rules. They stay separate from learned
                    memory and one-off `@file` references.
                  </p>
                </div>
                <div className="workspace-inline-actions">
                  <Chip
                    color={sessionProjectContext.active_entries > 0 ? "accent" : "default"}
                    variant="soft"
                  >
                    {sessionProjectContext.active_entries} active
                  </Chip>
                  <Chip
                    color={
                      sessionProjectContext.approval_required_entries > 0 ? "warning" : "default"
                    }
                    variant="soft"
                  >
                    {sessionProjectContext.approval_required_entries} approvals
                  </Chip>
                  <Chip
                    color={sessionProjectContext.blocked_entries > 0 ? "danger" : "default"}
                    variant="soft"
                  >
                    {sessionProjectContext.blocked_entries} blocked
                  </Chip>
                  <Chip
                    color={sessionProjectContext.warnings.length > 0 ? "warning" : "default"}
                    variant="soft"
                  >
                    {sessionProjectContext.warnings.length} warnings
                  </Chip>
                </div>
                <ActionCluster>
                  <ActionButton
                    isDisabled={projectContextBusy || sessionMaintenanceBusy}
                    size="sm"
                    type="button"
                    variant="secondary"
                    onPress={refreshProjectContext}
                  >
                    {projectContextBusy ? "Refreshing..." : "Refresh project context"}
                  </ActionButton>
                  <ActionButton
                    isDisabled={projectContextBusy || sessionMaintenanceBusy}
                    size="sm"
                    type="button"
                    variant="secondary"
                    onPress={scaffoldProjectContext}
                  >
                    Create PALYRA.md
                  </ActionButton>
                </ActionCluster>
                {selectedSession.recap.active_context_files.length > 0 ? (
                  <ul className="workspace-bullet-list">
                    {selectedSession.recap.active_context_files.slice(0, 5).map((file) => (
                      <li key={file}>{file}</li>
                    ))}
                  </ul>
                ) : null}
                {sessionProjectContext.focus_paths.length > 0 ? (
                  <ul className="workspace-bullet-list">
                    {sessionProjectContext.focus_paths.slice(0, 4).map((focus) => (
                      <li key={`${focus.reason}-${focus.path}`}>
                        {focus.reason}: {focus.path}
                      </li>
                    ))}
                  </ul>
                ) : null}
                {sessionProjectContext.warnings.map((warning, index) => (
                  <p key={`project-context-warning-${index}`} className="chat-muted">
                    {warning}
                  </p>
                ))}
                {sessionProjectContext.entries.slice(0, 4).map((entry) => (
                  <article key={entry.entry_id} className="chat-ops-card">
                    <div className="chat-ops-card__copy">
                      <strong>
                        {entry.order}. {entry.path}
                      </strong>
                      <span>
                        {entry.source_label} · {entry.precedence_label} ·{" "}
                        {entry.root ? "root scope" : `depth ${entry.depth}`}
                      </span>
                      <p>{entry.preview_text}</p>
                      {entry.warnings.length > 0 ? <p>{entry.warnings.join(" ")}</p> : null}
                    </div>
                    <div className="chat-ops-card__actions">
                      <Chip color={entry.active ? "accent" : "warning"} variant="soft">
                        {entry.status.replaceAll("_", " ")}
                      </Chip>
                      <Chip variant="soft">{entry.content_hash.slice(0, 10)}</Chip>
                      <Chip variant="soft">{entry.estimated_tokens.toLocaleString()} tok</Chip>
                      {entry.disabled ? (
                        <ActionButton
                          isDisabled={projectContextBusy || sessionMaintenanceBusy}
                          size="sm"
                          type="button"
                          variant="secondary"
                          onPress={() => enableProjectContextEntry(entry.entry_id)}
                        >
                          Enable
                        </ActionButton>
                      ) : (
                        <ActionButton
                          isDisabled={projectContextBusy || sessionMaintenanceBusy}
                          size="sm"
                          type="button"
                          variant="secondary"
                          onPress={() => disableProjectContextEntry(entry.entry_id)}
                        >
                          Disable
                        </ActionButton>
                      )}
                      {entry.status === "approval_required" ? (
                        <ActionButton
                          isDisabled={projectContextBusy || sessionMaintenanceBusy}
                          size="sm"
                          type="button"
                          variant="primary"
                          onPress={() => approveProjectContextEntry(entry.entry_id)}
                        >
                          Approve
                        </ActionButton>
                      ) : null}
                    </div>
                  </article>
                ))}
              </div>
            ) : selectedSession.recap.active_context_files.length > 0 ? (
              <div>
                <p className="workspace-kicker">Active context</p>
                <ul className="workspace-bullet-list">
                  {selectedSession.recap.active_context_files.slice(0, 5).map((file) => (
                    <li key={file}>{file}</li>
                  ))}
                </ul>
              </div>
            ) : null}
            {selectedSession.recap.recent_artifacts.length > 0 ? (
              <div>
                <p className="workspace-kicker">Recent artifacts</p>
                <ul className="workspace-bullet-list">
                  {selectedSession.recap.recent_artifacts.slice(0, 4).map((artifact) => (
                    <li key={artifact.artifact_id}>
                      {artifact.label} ({artifact.kind})
                    </li>
                  ))}
                </ul>
              </div>
            ) : null}
          </div>
        ) : null}
        {a2uiSurfaces.length === 0 ? (
          <EmptyState
            compact
            description="No A2UI documents published for this session yet."
            title="No A2UI surfaces"
          />
        ) : (
          <ul className="workspace-bullet-list">
            {a2uiSurfaces.map((surface) => (
              <li key={surface}>{surface}</li>
            ))}
          </ul>
        )}
        {browserSessionIds.length > 0 ? (
          <div className="chat-ops-list">
            <div className="workspace-panel__intro">
              <p className="workspace-kicker">Browser handoff</p>
              <h3>{browserSessionIds.length} traced sessions</h3>
            </div>
            {browserSessionIds.map((sessionId) => (
              <article key={sessionId} className="chat-ops-card">
                <div className="chat-ops-card__copy">
                  <strong>{shortId(sessionId)}</strong>
                  <span>{sessionId}</span>
                  <p>Open the browser workbench on the same session without losing chat context.</p>
                </div>
                <ActionButton
                  size="sm"
                  type="button"
                  variant="secondary"
                  onPress={() => openBrowserSessionWorkbench(sessionId)}
                >
                  Open browser detail
                </ActionButton>
              </article>
            ))}
          </div>
        ) : null}
      </SectionCard>

      <SectionCard
        className="chat-panel"
        description="Search persisted events, pin important tape entries, export the session, and inspect queued follow-ups."
        title="Transcript tools"
      >
        <div className="workspace-field-grid workspace-field-grid--double">
          <label className="workspace-field">
            <span className="workspace-kicker">Transcript search</span>
            <input
              className="w-full"
              placeholder="approval, tool, or summary text"
              value={transcriptSearchQuery}
              onChange={(event) => setTranscriptSearchQuery(event.currentTarget.value)}
            />
          </label>
          <div className="chat-transcript-tools__actions">
            <ActionButton
              isDisabled={transcriptSearchBusy || !canSearchTranscript}
              type="button"
              variant="secondary"
              onPress={searchTranscript}
            >
              {transcriptSearchBusy ? "Searching..." : "Search"}
            </ActionButton>
            <ActionButton
              isDisabled={exportBusy !== null}
              type="button"
              variant="secondary"
              onPress={() => exportTranscript("json")}
            >
              {exportBusy === "json" ? "Exporting..." : "Export JSON"}
            </ActionButton>
            <ActionButton
              isDisabled={exportBusy !== null}
              type="button"
              variant="secondary"
              onPress={() => exportTranscript("markdown")}
            >
              {exportBusy === "markdown" ? "Exporting..." : "Export Markdown"}
            </ActionButton>
          </div>
        </div>

        {searchResults.length > 0 ? (
          <div className="chat-ops-list">
            <div className="workspace-panel__intro">
              <p className="workspace-kicker">Matches</p>
              <h3>{searchResults.length} results</h3>
            </div>
            {searchResults.map((match) => (
              <article key={`${match.run_id}-${match.seq}`} className="chat-ops-card">
                <div className="chat-ops-card__copy">
                  <strong>
                    {prettifyEventType(match.event_type)} #{match.seq}
                  </strong>
                  {pinnedRecordKeys.has(`${match.run_id}:${match.seq}`) ? (
                    <div className="workspace-chip-row">
                      <Chip size="sm" variant="secondary">
                        Pinned
                      </Chip>
                    </div>
                  ) : null}
                  <span>
                    {match.origin_kind}
                    {match.origin_run_id !== undefined
                      ? ` · from ${shortId(match.origin_run_id)}`
                      : ""}
                  </span>
                  <p>{match.snippet}</p>
                </div>
                <ActionButton
                  size="sm"
                  type="button"
                  variant="secondary"
                  onPress={() => inspectSearchMatch(match)}
                >
                  Inspect
                </ActionButton>
              </article>
            ))}
          </div>
        ) : null}

        <div className="chat-ops-list">
          <div className="workspace-panel__intro">
            <p className="workspace-kicker">Recent persisted events</p>
            <h3>{transcriptBusy ? "Loading..." : `${recentTranscriptRecords.length} records`}</h3>
          </div>
          {recentTranscriptRecords.length === 0 ? (
            <EmptyState
              compact
              description="Stream or retry a run to populate persisted transcript events."
              title="No persisted transcript yet"
            />
          ) : (
            recentTranscriptRecords.map((record) => (
              <article key={`${record.run_id}-${record.seq}`} className="chat-ops-card">
                <div className="chat-ops-card__copy">
                  <strong>
                    {prettifyEventType(record.event_type)} #{record.seq}
                  </strong>
                  {pinnedRecordKeys.has(`${record.run_id}:${record.seq}`) ? (
                    <div className="workspace-chip-row">
                      <Chip size="sm" variant="secondary">
                        Pinned
                      </Chip>
                    </div>
                  ) : null}
                  <span>
                    {record.origin_kind}
                    {record.origin_run_id !== undefined
                      ? ` · from ${shortId(record.origin_run_id)}`
                      : ""}
                  </span>
                </div>
                <div className="chat-ops-card__actions">
                  <ActionButton
                    size="sm"
                    type="button"
                    variant="secondary"
                    onPress={() => inspectTranscriptRecord(record)}
                  >
                    Inspect
                  </ActionButton>
                  <ActionButton
                    size="sm"
                    type="button"
                    variant="secondary"
                    onPress={() => pinTranscriptRecord(record)}
                  >
                    Pin
                  </ActionButton>
                </div>
              </article>
            ))
          )}
        </div>

        <div className="chat-ops-list">
          <div className="workspace-panel__intro">
            <p className="workspace-kicker">Pins</p>
            <h3>{sessionPins.length}</h3>
          </div>
          {sessionPins.length === 0 ? (
            <p className="chat-muted">Pin important transcript events to keep them visible.</p>
          ) : (
            sessionPins.map((pin) => (
              <article key={pin.pin_id} className="chat-ops-card">
                <div className="chat-ops-card__copy">
                  <strong>{pin.title}</strong>
                  <span>
                    Run {shortId(pin.run_id)} · tape #{pin.tape_seq}
                  </span>
                  {pin.note !== undefined ? <p>{pin.note}</p> : null}
                </div>
                <ActionButton
                  size="sm"
                  type="button"
                  variant="danger"
                  onPress={() => deletePin(pin.pin_id)}
                >
                  Delete
                </ActionButton>
              </article>
            ))
          )}
        </div>

        <div className="chat-ops-list">
          <div className="workspace-panel__intro">
            <p className="workspace-kicker">Compactions</p>
            <h3>{compactions.length}</h3>
          </div>
          {compactions.length === 0 ? (
            <p className="chat-muted">No compaction artifacts have been stored for this session.</p>
          ) : (
            [...compactions].reverse().map((artifact) => {
              const tokenDelta = Math.max(
                0,
                artifact.estimated_input_tokens - artifact.estimated_output_tokens,
              );
              const artifactRunId = artifact.run_id;
              const summary = safeParseCompactionSummary(artifact.summary_json);
              const reviewCount = summary?.planner?.review_candidate_count ?? 0;
              const writeCount = summary?.writes?.length ?? 0;
              return (
                <article key={artifact.artifact_id} className="chat-ops-card">
                  <div className="chat-ops-card__copy">
                    <strong>{artifact.mode}</strong>
                    <span>
                      {formatApproxTokens(tokenDelta)} saved · {artifact.condensed_event_count}{" "}
                      condensed
                    </span>
                    <span>
                      {(summary?.lifecycle_state ?? "stored").replaceAll("_", " ")} · {writeCount}{" "}
                      write{writeCount === 1 ? "" : "s"}
                      {reviewCount > 0
                        ? ` · ${reviewCount} review candidate${reviewCount === 1 ? "" : "s"}`
                        : ""}
                    </span>
                    <p>{artifact.summary_preview}</p>
                  </div>
                  <div className="chat-ops-card__actions">
                    {artifactRunId !== undefined ? (
                      <ActionButton
                        isDisabled={sessionMaintenanceBusy}
                        size="sm"
                        type="button"
                        variant="ghost"
                        onPress={() => openRunDetails(artifactRunId, "workspace")}
                      >
                        Workspace
                      </ActionButton>
                    ) : null}
                    <ActionButton
                      isDisabled={sessionMaintenanceBusy}
                      size="sm"
                      type="button"
                      variant="secondary"
                      onPress={() => inspectCompaction(artifact.artifact_id)}
                    >
                      {sessionMaintenanceBusyKey === `inspect-compaction:${artifact.artifact_id}`
                        ? "Loading..."
                        : "Inspect"}
                    </ActionButton>
                  </div>
                </article>
              );
            })
          )}
        </div>

        <div className="chat-ops-list">
          <div className="workspace-panel__intro">
            <p className="workspace-kicker">Checkpoints</p>
            <h3>{checkpoints.length}</h3>
          </div>
          {checkpoints.length === 0 ? (
            <p className="chat-muted">No checkpoints are stored for this session yet.</p>
          ) : (
            [...checkpoints].reverse().map((checkpoint) => {
              const checkpointRunId = checkpoint.run_id;
              return (
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
                    {checkpointRunId !== undefined ? (
                      <ActionButton
                        isDisabled={sessionMaintenanceBusy}
                        size="sm"
                        type="button"
                        variant="ghost"
                        onPress={() => openRunDetails(checkpointRunId, "workspace")}
                      >
                        Workspace
                      </ActionButton>
                    ) : null}
                    <ActionButton
                      isDisabled={sessionMaintenanceBusy}
                      size="sm"
                      type="button"
                      variant="secondary"
                      onPress={() => inspectCheckpoint(checkpoint.checkpoint_id)}
                    >
                      {sessionMaintenanceBusyKey ===
                      `inspect-checkpoint:${checkpoint.checkpoint_id}`
                        ? "Loading..."
                        : "Inspect"}
                    </ActionButton>
                    <ActionButton
                      isDisabled={sessionMaintenanceBusy}
                      size="sm"
                      type="button"
                      variant="primary"
                      onPress={() => restoreCheckpoint(checkpoint.checkpoint_id)}
                    >
                      {sessionMaintenanceBusyKey ===
                      `restore-checkpoint:${checkpoint.checkpoint_id}`
                        ? "Restoring..."
                        : "Restore"}
                    </ActionButton>
                  </div>
                </article>
              );
            })
          )}
        </div>

        <div className="chat-ops-list">
          <div className="workspace-panel__intro">
            <p className="workspace-kicker">Queued follow-ups</p>
            <h3>{queuedInputs.length}</h3>
          </div>
          <ActionCluster>
            <ActionButton
              isDisabled={selectedSession === null || sessionMaintenanceBusy}
              size="sm"
              type="button"
              variant="secondary"
              onPress={inspectQueuePolicy}
            >
              {sessionMaintenanceBusyKey === "queue-policy" ? "Loading..." : "Policy"}
            </ActionButton>
            <ActionButton
              isDisabled={selectedSession === null || sessionMaintenanceBusy}
              size="sm"
              type="button"
              variant="secondary"
              onPress={() => runSessionQueueAction("pause")}
            >
              {sessionMaintenanceBusyKey === "queue-pause" ? "Pausing..." : "Pause"}
            </ActionButton>
            <ActionButton
              isDisabled={selectedSession === null || sessionMaintenanceBusy}
              size="sm"
              type="button"
              variant="secondary"
              onPress={() => runSessionQueueAction("resume")}
            >
              {sessionMaintenanceBusyKey === "queue-resume" ? "Resuming..." : "Resume"}
            </ActionButton>
            <ActionButton
              isDisabled={selectedSession === null || sessionMaintenanceBusy || queuedInputs.length === 0}
              size="sm"
              type="button"
              variant="secondary"
              onPress={() => runSessionQueueAction("collect-summary")}
            >
              {sessionMaintenanceBusyKey === "queue-collect-summary" ? "Collecting..." : "Collect"}
            </ActionButton>
            <ActionButton
              isDisabled={selectedSession === null || sessionMaintenanceBusy || queuedInputs.length === 0}
              size="sm"
              type="button"
              variant="danger"
              onPress={() => runSessionQueueAction("drain")}
            >
              {sessionMaintenanceBusyKey === "queue-drain" ? "Draining..." : "Drain"}
            </ActionButton>
          </ActionCluster>
          {queuedInputs.length === 0 ? (
            <p className="chat-muted">No queued follow-ups are stored for this session.</p>
          ) : (
            [...queuedInputs].reverse().map((queued) => (
              <article key={queued.queued_input_id} className="chat-ops-card">
                <div className="chat-ops-card__copy">
                  <strong>{queued.state}</strong>
                  <span>
                    {shortId(queued.queued_input_id)} · run {shortId(queued.run_id)} ·{" "}
                    {queued.queue_mode} · {queued.priority_lane}
                  </span>
                  <span>
                    {queued.decision_reason}
                    {queued.overflow_summary_ref !== undefined
                      ? ` · ${queued.overflow_summary_ref}`
                      : ""}
                  </span>
                  <p>{queued.text}</p>
                </div>
                {queued.state === "pending" ? (
                  <div className="chat-ops-card__actions">
                    <ActionButton
                      isDisabled={sessionMaintenanceBusy}
                      size="sm"
                      type="button"
                      variant="danger"
                      onPress={() => cancelQueuedInput(queued.queued_input_id)}
                    >
                      {sessionMaintenanceBusyKey === `queue-cancel:${queued.queued_input_id}`
                        ? "Cancelling..."
                        : "Cancel"}
                    </ActionButton>
                  </div>
                ) : null}
              </article>
            ))
          )}
        </div>

        <div className="chat-ops-list">
          <div className="workspace-panel__intro">
            <p className="workspace-kicker">Background tasks</p>
            <h3>{backgroundTasks.length}</h3>
          </div>
          {backgroundTasks.length === 0 ? (
            <p className="chat-muted">No background tasks are tracked for this session.</p>
          ) : (
            [...backgroundTasks].reverse().map((task) => {
              const targetRunId = task.target_run_id;
              return (
                <article key={task.task_id} className="chat-ops-card">
                  <div className="chat-ops-card__copy">
                    <strong>{task.state}</strong>
                    <span>
                      {task.task_kind}
                      {task.delegation !== undefined
                        ? ` · ${task.delegation.display_name} (${task.delegation.execution_mode})`
                        : ""}
                      {" · "}
                      {task.attempt_count}/{task.max_attempts} attempts
                    </span>
                    <p>{task.input_text ?? task.last_error ?? "No task text or error recorded."}</p>
                  </div>
                  <div className="chat-ops-card__actions">
                    {targetRunId !== undefined ? (
                      <ActionButton
                        isDisabled={sessionMaintenanceBusy}
                        size="sm"
                        type="button"
                        variant="ghost"
                        onPress={() => openRunDetails(targetRunId, "workspace")}
                      >
                        Workspace
                      </ActionButton>
                    ) : null}
                    <ActionButton
                      isDisabled={sessionMaintenanceBusy}
                      size="sm"
                      type="button"
                      variant="secondary"
                      onPress={() => inspectBackgroundTask(task.task_id)}
                    >
                      {sessionMaintenanceBusyKey === `inspect-background-task:${task.task_id}`
                        ? "Loading..."
                        : "Inspect"}
                    </ActionButton>
                    {(task.state === "queued" || task.state === "failed") && (
                      <ActionButton
                        isDisabled={sessionMaintenanceBusy}
                        size="sm"
                        type="button"
                        variant="secondary"
                        onPress={() => runBackgroundTaskAction(task.task_id, "pause")}
                      >
                        {sessionMaintenanceBusyKey === `background-pause:${task.task_id}`
                          ? "Pausing..."
                          : "Pause"}
                      </ActionButton>
                    )}
                    {task.state === "paused" && (
                      <ActionButton
                        isDisabled={sessionMaintenanceBusy}
                        size="sm"
                        type="button"
                        variant="secondary"
                        onPress={() => runBackgroundTaskAction(task.task_id, "resume")}
                      >
                        {sessionMaintenanceBusyKey === `background-resume:${task.task_id}`
                          ? "Resuming..."
                          : "Resume"}
                      </ActionButton>
                    )}
                    {(task.state === "failed" ||
                      task.state === "cancelled" ||
                      task.state === "expired") && (
                      <ActionButton
                        isDisabled={sessionMaintenanceBusy}
                        size="sm"
                        type="button"
                        variant="secondary"
                        onPress={() => runBackgroundTaskAction(task.task_id, "retry")}
                      >
                        {sessionMaintenanceBusyKey === `background-retry:${task.task_id}`
                          ? "Retrying..."
                          : "Retry"}
                      </ActionButton>
                    )}
                    {task.state !== "succeeded" &&
                    task.state !== "cancelled" &&
                    task.state !== "expired" ? (
                      <ActionButton
                        isDisabled={sessionMaintenanceBusy}
                        size="sm"
                        type="button"
                        variant="danger"
                        onPress={() => runBackgroundTaskAction(task.task_id, "cancel")}
                      >
                        {sessionMaintenanceBusyKey === `background-cancel:${task.task_id}`
                          ? "Canceling..."
                          : "Cancel"}
                      </ActionButton>
                    ) : null}
                  </div>
                </article>
              );
            })
          )}
        </div>
      </SectionCard>

      <SectionCard
        className="chat-panel"
        description="Inspect raw tool payloads, persisted transcript events, and search matches without flooding the conversation timeline."
        title="Detail sidebar"
      >
        {detailPanel === null ? (
          <EmptyState
            compact
            description="Choose Inspect on a payload, transcript event, or search result."
            title="No detail selected"
          />
        ) : (
          <div className="chat-detail-panel">
            <div className="workspace-panel__intro">
              <p className="workspace-kicker">Selected detail</p>
              <h3>{detailPanel.title}</h3>
              <p className="chat-muted">{detailPanel.subtitle}</p>
            </div>
            {detailPanel.body !== undefined ? (
              <p className="chat-entry-text">{detailPanel.body}</p>
            ) : null}
            {detailPanel.actions !== undefined && detailPanel.actions.length > 0 ? (
              <ActionCluster>
                {detailPanel.actions.map((action) => (
                  <ActionButton
                    key={action.key}
                    size="sm"
                    type="button"
                    variant={action.variant ?? "secondary"}
                    onPress={() => void action.onPress()}
                  >
                    {action.label}
                  </ActionButton>
                ))}
              </ActionCluster>
            ) : null}
            {detailPanel.payload !== undefined ? (
              <PrettyJsonBlock
                className="chat-detail-panel__payload"
                revealSensitiveValues={revealSensitiveValues}
                value={detailPanel.payload}
              />
            ) : null}
          </div>
        )}
      </SectionCard>

      {inspectorVisible ? (
        <SectionCard
          className="chat-panel"
          description="Status, tape, token usage, and lineage metadata stay secondary to the transcript but available on demand."
          title="Run inspector"
        >
          <ChatRunDrawer
            activeTab={runDrawerTab}
            api={api}
            open
            openMemorySection={openMemorySection}
            openSupportSection={openSupportSection}
            onInspectCompaction={inspectCompaction}
            onInspectSessionCheckpoint={inspectCheckpoint}
            onWorkspaceRestore={onWorkspaceRestore}
            openCanvasSurface={openCanvasSurface}
            runIds={runIds}
            runDrawerId={runDrawerId}
            setRunDrawerId={setRunDrawerId}
            setActiveTab={setRunDrawerTab}
            runDrawerBusy={runDrawerBusy}
            runStatus={runStatus}
            runTape={runTape}
            runLineage={runLineage}
            revealSensitiveValues={revealSensitiveValues}
            setError={setError}
            setNotice={setNotice}
            refreshRun={refreshRunDetails}
            close={closeRunDrawer}
          />
        </SectionCard>
      ) : (
        <SectionCard
          className="chat-panel"
          description="Run details become available after the first streamed response."
          title="Run inspector"
        >
          <EmptyState
            compact
            description="Open a run after the first streamed response to inspect status, tape, and token usage."
            title="Run details will appear here"
          />
        </SectionCard>
      )}
    </div>
  );
}

function formatCountLabel(count: number, singularLabel: string): string {
  return `${count} ${singularLabel}${count === 1 ? "" : "s"}`;
}

function extractBrowserSessionIds(runTape: ChatRunTapeSnapshot | null): string[] {
  if (runTape === null || runTape.events.length === 0) {
    return [];
  }

  const sessionIds = new Set<string>();

  for (const event of runTape.events) {
    if (event.payload_json.trim().length === 0) {
      continue;
    }

    try {
      collectSessionIds(JSON.parse(event.payload_json) as JsonValue, sessionIds);
    } catch {
      // Ignore malformed payload snapshots in the sidebar helper.
    }
  }

  return Array.from(sessionIds);
}

function collectSessionIds(value: JsonValue, sessionIds: Set<string>): void {
  if (
    typeof value === "string" ||
    typeof value === "number" ||
    typeof value === "boolean" ||
    value === null
  ) {
    return;
  }

  if (Array.isArray(value)) {
    for (const item of value) {
      collectSessionIds(item, sessionIds);
    }
    return;
  }

  for (const [key, nestedValue] of Object.entries(value)) {
    if (key === "session_id" && typeof nestedValue === "string" && nestedValue.trim().length > 0) {
      sessionIds.add(nestedValue);
    }
    collectSessionIds(nestedValue, sessionIds);
  }
}

function safeParseCompactionSummary(value: string | null | undefined):
  | {
      lifecycle_state?: string;
      planner?: { review_candidate_count?: number };
      writes?: Array<unknown>;
    }
  | undefined {
  if (typeof value !== "string" || value.trim().length === 0) {
    return undefined;
  }

  try {
    return JSON.parse(value) as {
      lifecycle_state?: string;
      planner?: { review_candidate_count?: number };
      writes?: Array<unknown>;
    };
  } catch {
    return undefined;
  }
}
