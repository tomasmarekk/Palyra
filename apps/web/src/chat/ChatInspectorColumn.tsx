import { Chip } from "@heroui/react";

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
  JsonValue,
  SessionCatalogRecord,
} from "../consoleApi";
import {
  ActionButton,
  ActionCluster,
  EmptyState,
  KeyValueList,
  SectionCard,
} from "../console/components/ui";

import { ChatRunDrawer } from "./ChatRunDrawer";
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
  contextBudgetEstimatedTokens: number;
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
  backgroundTasks: ChatBackgroundTaskRecord[];
  inspectBackgroundTask: (taskId: string) => void;
  runBackgroundTaskAction: (
    taskId: string,
    action: "pause" | "resume" | "retry" | "cancel",
  ) => void;
  detailPanel: DetailPanelState | null;
  revealSensitiveValues: boolean;
  inspectorVisible: boolean;
  openRunDetails: (runId: string) => void;
  phase4BusyKey: string | null;
  runDrawerId: string;
  setRunDrawerId: (runId: string) => void;
  runDrawerBusy: boolean;
  runStatus: ChatRunStatusRecord | null;
  runTape: ChatRunTapeSnapshot | null;
  runLineage: ChatRunLineage | null;
  refreshRunDetails: () => void;
  closeRunDrawer: () => void;
  openBrowserSessionWorkbench: (sessionId: string) => void;
};

export function ChatInspectorColumn({
  pendingApprovalCount,
  a2uiSurfaces,
  runIds,
  selectedSession,
  selectedSessionLineage,
  contextBudgetEstimatedTokens,
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
  backgroundTasks,
  inspectBackgroundTask,
  runBackgroundTaskAction,
  detailPanel,
  revealSensitiveValues,
  inspectorVisible,
  openRunDetails,
  phase4BusyKey,
  runDrawerId,
  setRunDrawerId,
  runDrawerBusy,
  runStatus,
  runTape,
  runLineage,
  refreshRunDetails,
  closeRunDrawer,
  openBrowserSessionWorkbench,
}: ChatInspectorColumnProps) {
  const phase4Busy = phase4BusyKey !== null;
  const browserSessionIds = extractBrowserSessionIds(runTape);

  return (
    <div className="chat-inspector-column">
      <SectionCard
        className="chat-panel chat-panel--sticky"
        description="Branch lineage, queue backlog, and persisted transcript tools stay visible without turning the main conversation into a debug dump."
        title="Workspace signals"
      >
        <div className="workspace-tag-row">
          <Chip color={pendingApprovalCount > 0 ? "warning" : "success"} variant="soft">
            {pendingApprovalCount} approval{pendingApprovalCount === 1 ? "" : "s"}
          </Chip>
          <Chip variant="secondary">
            {a2uiSurfaces.length} A2UI surface{a2uiSurfaces.length === 1 ? "" : "s"}
          </Chip>
          <Chip variant="secondary">
            {runIds.length} known run{runIds.length === 1 ? "" : "s"}
          </Chip>
          <Chip variant="secondary">
            {compactions.length} compaction{compactions.length === 1 ? "" : "s"}
          </Chip>
          <Chip variant="secondary">
            {backgroundTasks.length} background task{backgroundTasks.length === 1 ? "" : "s"}
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
            {selectedSession.recap.active_context_files.length > 0 ? (
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
                        isDisabled={phase4Busy}
                        size="sm"
                        type="button"
                        variant="ghost"
                        onPress={() => openRunDetails(artifactRunId)}
                      >
                        Open run
                      </ActionButton>
                    ) : null}
                    <ActionButton
                      isDisabled={phase4Busy}
                      size="sm"
                      type="button"
                      variant="secondary"
                      onPress={() => inspectCompaction(artifact.artifact_id)}
                    >
                      {phase4BusyKey === `inspect-compaction:${artifact.artifact_id}`
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
                        isDisabled={phase4Busy}
                        size="sm"
                        type="button"
                        variant="ghost"
                        onPress={() => openRunDetails(checkpointRunId)}
                      >
                        Open run
                      </ActionButton>
                    ) : null}
                    <ActionButton
                      isDisabled={phase4Busy}
                      size="sm"
                      type="button"
                      variant="secondary"
                      onPress={() => inspectCheckpoint(checkpoint.checkpoint_id)}
                    >
                      {phase4BusyKey === `inspect-checkpoint:${checkpoint.checkpoint_id}`
                        ? "Loading..."
                        : "Inspect"}
                    </ActionButton>
                    <ActionButton
                      isDisabled={phase4Busy}
                      size="sm"
                      type="button"
                      variant="primary"
                      onPress={() => restoreCheckpoint(checkpoint.checkpoint_id)}
                    >
                      {phase4BusyKey === `restore-checkpoint:${checkpoint.checkpoint_id}`
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
          {queuedInputs.length === 0 ? (
            <p className="chat-muted">No queued follow-ups are stored for this session.</p>
          ) : (
            [...queuedInputs].reverse().map((queued) => (
              <article key={queued.queued_input_id} className="chat-ops-card">
                <div className="chat-ops-card__copy">
                  <strong>{queued.state}</strong>
                  <span>
                    {shortId(queued.queued_input_id)} · run {shortId(queued.run_id)}
                  </span>
                  <p>{queued.text}</p>
                </div>
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
                        isDisabled={phase4Busy}
                        size="sm"
                        type="button"
                        variant="ghost"
                        onPress={() => openRunDetails(targetRunId)}
                      >
                        Open run
                      </ActionButton>
                    ) : null}
                    <ActionButton
                      isDisabled={phase4Busy}
                      size="sm"
                      type="button"
                      variant="secondary"
                      onPress={() => inspectBackgroundTask(task.task_id)}
                    >
                      {phase4BusyKey === `inspect-background-task:${task.task_id}`
                        ? "Loading..."
                        : "Inspect"}
                    </ActionButton>
                    {(task.state === "queued" || task.state === "failed") && (
                      <ActionButton
                        isDisabled={phase4Busy}
                        size="sm"
                        type="button"
                        variant="secondary"
                        onPress={() => runBackgroundTaskAction(task.task_id, "pause")}
                      >
                        {phase4BusyKey === `background-pause:${task.task_id}`
                          ? "Pausing..."
                          : "Pause"}
                      </ActionButton>
                    )}
                    {task.state === "paused" && (
                      <ActionButton
                        isDisabled={phase4Busy}
                        size="sm"
                        type="button"
                        variant="secondary"
                        onPress={() => runBackgroundTaskAction(task.task_id, "resume")}
                      >
                        {phase4BusyKey === `background-resume:${task.task_id}`
                          ? "Resuming..."
                          : "Resume"}
                      </ActionButton>
                    )}
                    {(task.state === "failed" ||
                      task.state === "cancelled" ||
                      task.state === "expired") && (
                      <ActionButton
                        isDisabled={phase4Busy}
                        size="sm"
                        type="button"
                        variant="secondary"
                        onPress={() => runBackgroundTaskAction(task.task_id, "retry")}
                      >
                        {phase4BusyKey === `background-retry:${task.task_id}`
                          ? "Retrying..."
                          : "Retry"}
                      </ActionButton>
                    )}
                    {task.state !== "succeeded" &&
                    task.state !== "cancelled" &&
                    task.state !== "expired" ? (
                      <ActionButton
                        isDisabled={phase4Busy}
                        size="sm"
                        type="button"
                        variant="danger"
                        onPress={() => runBackgroundTaskAction(task.task_id, "cancel")}
                      >
                        {phase4BusyKey === `background-cancel:${task.task_id}`
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
            open
            runIds={runIds}
            runDrawerId={runDrawerId}
            setRunDrawerId={setRunDrawerId}
            runDrawerBusy={runDrawerBusy}
            runStatus={runStatus}
            runTape={runTape}
            runLineage={runLineage}
            revealSensitiveValues={revealSensitiveValues}
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
