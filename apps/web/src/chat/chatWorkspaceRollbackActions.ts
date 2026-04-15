import type { Dispatch, SetStateAction } from "react";

import type {
  ChatRunStatusRecord,
  ConsoleApiClient,
  JsonValue,
  WorkspaceRestoreResponseEnvelope,
} from "../consoleApi";

import type { DetailPanelState } from "./ChatInspectorColumn";
import type { RunDrawerTab } from "./ChatRunDrawer";
import type { ComposerAttachment, TranscriptEntry } from "./chatShared";

type SetAttachments = Dispatch<
  SetStateAction<ComposerAttachment[]>
>;

type AppendLocalEntry = (entry: Omit<TranscriptEntry, "id" | "created_at_unix_ms">) => void;

type OpenRunDetails = (runId: string, tab?: RunDrawerTab) => void;

export async function reconcileWorkspaceRestoreAction(args: {
  response: WorkspaceRestoreResponseEnvelope;
  upsertSession: (session: WorkspaceRestoreResponseEnvelope["session"], options?: { select?: boolean }) => void;
  clearTranscriptState: () => void;
  setAttachments: SetAttachments;
  setDetailPanel: (next: DetailPanelState | null) => void;
  refreshSessions: () => Promise<void>;
  refreshSessionTranscript: (sessionIdOverride?: string) => Promise<void>;
  appendLocalEntry: AppendLocalEntry;
  openRunDetails: OpenRunDetails;
  setNotice: (next: string | null) => void;
}): Promise<void> {
  const {
    response,
    upsertSession,
    clearTranscriptState,
    setAttachments,
    setDetailPanel,
    refreshSessions,
    refreshSessionTranscript,
    appendLocalEntry,
    openRunDetails,
    setNotice,
  } = args;

  upsertSession(response.session, { select: true });
  clearTranscriptState();
  setAttachments([]);
  setDetailPanel(null);
  await Promise.all([
    refreshSessions(),
    refreshSessionTranscript(response.session.session_id),
  ]);
  appendLocalEntry({
    kind: "status",
    session_id: response.session.session_id,
    run_id: response.checkpoint.run_id,
    title: "Workspace restore",
    text: response.restore.report.reconciliation_summary,
  });
  openRunDetails(response.checkpoint.run_id, "workspace");

  const branchSummary =
    response.restore.report.branched_session_id !== undefined
      ? ` Restored into branched session ${response.session.title}.`
      : ` Restored in session ${response.session.title}.`;
  const suggestedSummary =
    response.suggested_session_label !== undefined
      ? ` Suggested title: ${response.suggested_session_label}.`
      : "";
  const warningSummary = [
    response.project_context_refresh_error,
    response.project_context_copy_error,
  ]
    .filter((value): value is string => Boolean(value && value.trim().length > 0))
    .join(" ");

  setNotice(
    `${response.restore.report.reconciliation_summary}.${branchSummary}${suggestedSummary}${
      response.restore.failed_paths.length > 0
        ? ` Failed paths: ${response.restore.failed_paths.length}.`
        : ""
    }${warningSummary ? ` Context refresh warning: ${warningSummary}` : ""}`,
  );
}

export async function openWorkspaceRollbackInspectorAction(args: {
  rawTarget?: string;
  actionableRunId: string | null;
  sessionRuns: readonly ChatRunStatusRecord[];
  selectedLastRunId?: string;
  knownRunIds: readonly string[];
  setDetailPanel: (next: DetailPanelState | null) => void;
  openRunDetails: OpenRunDetails;
  setError: (next: string | null) => void;
  setNotice: (next: string | null) => void;
}): Promise<void> {
  const {
    rawTarget = "",
    actionableRunId,
    sessionRuns,
    selectedLastRunId,
    knownRunIds,
    setDetailPanel,
    openRunDetails,
    setError,
    setNotice,
  } = args;

  const requested = rawTarget.trim();
  const targetRunId =
    (requested.length > 0 && sessionRuns.some((run) => run.run_id === requested)
      ? requested
      : actionableRunId ?? selectedLastRunId ?? knownRunIds[0]) ?? null;
  if (targetRunId === null) {
    setError("No run is available for workspace rollback.");
    return;
  }

  setDetailPanel(null);
  openRunDetails(targetRunId, "workspace");
  setNotice(
    requested.length > 0 && requested !== targetRunId
      ? `Workspace rollback inspector opened. Select checkpoint ${requested} to restore or diff.`
      : "Workspace rollback inspector opened.",
  );
}

export async function previewWorkspaceRollbackDiffAction(args: {
  api: ConsoleApiClient;
  rawTarget: string;
  actionableRunId: string | null;
  runDrawerId: string;
  sessionRuns: readonly ChatRunStatusRecord[];
  selectedLastRunId?: string;
  knownRunIds: readonly string[];
  setDetailPanel: (next: DetailPanelState | null) => void;
  openRunDetails: OpenRunDetails;
  setError: (next: string | null) => void;
  setNotice: (next: string | null) => void;
}): Promise<void> {
  const {
    api,
    rawTarget,
    actionableRunId,
    runDrawerId,
    sessionRuns,
    selectedLastRunId,
    knownRunIds,
    setDetailPanel,
    openRunDetails,
    setError,
    setNotice,
  } = args;

  const target = rawTarget.trim();
  if (target.length === 0) {
    await openWorkspaceRollbackInspectorAction({
      actionableRunId,
      sessionRuns,
      selectedLastRunId,
      knownRunIds,
      setDetailPanel,
      openRunDetails,
      setError,
      setNotice,
    });
    setNotice("Choose a run or workspace checkpoint to preview a rollback diff.");
    return;
  }

  const leftRunId = actionableRunId || runDrawerId || selectedLastRunId || knownRunIds[0];
  if (!leftRunId) {
    setError("No run is available for rollback diff preview.");
    return;
  }

  const targetIsRun =
    target === leftRunId ||
    sessionRuns.some((run) => run.run_id === target) ||
    knownRunIds.includes(target);
  const response = await api.compareWorkspace({
    left_run_id: leftRunId,
    right_run_id: targetIsRun ? target : undefined,
    right_checkpoint_id: targetIsRun ? undefined : target,
    limit: 64,
  });

  setDetailPanel({
    id: `rollback-diff:${response.diff.left_anchor.id}:${response.diff.right_anchor.id}`,
    title: "Rollback diff",
    subtitle: `${response.diff.left_anchor.label} → ${response.diff.right_anchor.label}`,
    body:
      response.diff.files_changed === 0
        ? "No changed workspace paths were found between the selected anchors."
        : `${response.diff.files_changed} changed path${
            response.diff.files_changed === 1 ? "" : "s"
          } are ready for review before restore.`,
    payload: response.diff as unknown as JsonValue,
    actions: [
      {
        key: "open-workspace",
        label: "Open workspace inspector",
        variant: "primary",
        onPress: () => openRunDetails(leftRunId, "workspace"),
      },
    ],
  });
  openRunDetails(leftRunId, "workspace");
  setNotice(
    response.diff.files_changed > 0
      ? `Rollback diff ready with ${response.diff.files_changed} changed path${
          response.diff.files_changed === 1 ? "" : "s"
        }.`
      : "Rollback diff loaded with no changed workspace paths.",
  );
}
