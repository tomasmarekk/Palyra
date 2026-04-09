import type {
  ChatBackgroundTaskRecord,
  ChatCompactionArtifactRecord,
  ChatCompactionPreview,
  ConsoleApiClient,
  JsonValue,
  SessionCatalogRecord,
} from "../consoleApi";

import type { DetailPanelState } from "./ChatInspectorColumn";
import {
  buildDetailFromBackgroundTask,
  buildDetailFromCheckpointRecord,
  buildDetailFromCompactionArtifact,
} from "./chatConsoleUtils";
import {
  emptyToUndefined,
  shortId,
  type ComposerAttachment,
  type TranscriptEntry,
} from "./chatShared";

type UpsertSession = (session: SessionCatalogRecord, options?: { select?: boolean }) => void;

type AppendLocalEntry = (entry: TranscriptEntry) => void;

type SetDetailPanel = (value: DetailPanelState | null) => void;

type SetAttachments = (
  next: ComposerAttachment[] | ((previous: ComposerAttachment[]) => ComposerAttachment[]),
) => void;

export async function runCompactionFlowAction(args: {
  mode: "preview" | "apply";
  api: ConsoleApiClient;
  sessionId: string;
  upsertSession: UpsertSession;
  refreshSessionTranscript: () => Promise<void>;
  setDetailPanel: SetDetailPanel;
  appendLocalEntry: AppendLocalEntry;
  setNotice: (value: string | null) => void;
}): Promise<void> {
  if (args.mode === "apply") {
    const response = await args.api.applySessionCompaction(args.sessionId, {
      trigger_reason: "chat_slash_command",
      trigger_policy: "manual_apply",
    });
    args.upsertSession(response.session);
    args.setDetailPanel(buildDetailFromCompactionArtifact(response.artifact));
    args.appendLocalEntry(
      buildCompactionStatusEntry(response.preview, args.sessionId, response.artifact),
    );
    await args.refreshSessionTranscript();
    args.setNotice(`Compaction stored with ${formatTokenDelta(response.preview.token_delta)}.`);
    return;
  }

  const response = await args.api.previewSessionCompaction(args.sessionId, {
    trigger_reason: "chat_slash_command",
    trigger_policy: "manual_preview",
  });
  const reviewCandidateIds = collectReviewCandidateIds(response.preview);
  args.upsertSession(response.session);
  args.setDetailPanel(
    buildCompactionPreviewDetail(response.preview, args.sessionId, {
      acceptAllReviewCandidates:
        reviewCandidateIds.length === 0
          ? undefined
          : async () => {
              const applyResponse = await args.api.applySessionCompaction(args.sessionId, {
                trigger_reason: "chat_review_accept",
                trigger_policy: "manual_review_accept",
                accept_candidate_ids: reviewCandidateIds,
              });
              args.upsertSession(applyResponse.session);
              args.setDetailPanel(buildDetailFromCompactionArtifact(applyResponse.artifact));
              args.appendLocalEntry(
                buildCompactionStatusEntry(
                  applyResponse.preview,
                  args.sessionId,
                  applyResponse.artifact,
                ),
              );
              await args.refreshSessionTranscript();
              args.setNotice(
                `Compaction applied with ${reviewCandidateIds.length} accepted review candidate${reviewCandidateIds.length === 1 ? "" : "s"}.`,
              );
            },
      rejectAllReviewCandidates:
        reviewCandidateIds.length === 0
          ? undefined
          : async () => {
              const applyResponse = await args.api.applySessionCompaction(args.sessionId, {
                trigger_reason: "chat_review_reject",
                trigger_policy: "manual_review_reject",
                reject_candidate_ids: reviewCandidateIds,
              });
              args.upsertSession(applyResponse.session);
              args.setDetailPanel(buildDetailFromCompactionArtifact(applyResponse.artifact));
              args.appendLocalEntry(
                buildCompactionStatusEntry(
                  applyResponse.preview,
                  args.sessionId,
                  applyResponse.artifact,
                ),
              );
              await args.refreshSessionTranscript();
              args.setNotice(
                `Compaction applied while leaving ${reviewCandidateIds.length} review candidate${reviewCandidateIds.length === 1 ? "" : "s"} out of the durable write set.`,
              );
            },
    }),
  );
  args.appendLocalEntry(buildCompactionStatusEntry(response.preview, args.sessionId));
  args.setNotice(
    response.preview.eligible
      ? `Compaction preview ready with ${formatTokenDelta(response.preview.token_delta)} token delta.`
      : "Compaction preview loaded, but there is not enough older transcript to condense yet.",
  );
}

export async function inspectCompactionAction(args: {
  api: ConsoleApiClient;
  artifactId: string;
  upsertSession: UpsertSession;
  setDetailPanel: SetDetailPanel;
}): Promise<void> {
  const response = await args.api.getSessionCompactionArtifact(args.artifactId);
  args.upsertSession(response.session);
  args.setDetailPanel(
    buildDetailFromCompactionArtifact(response.artifact, response.related_checkpoints),
  );
}

export async function inspectCheckpointAction(args: {
  api: ConsoleApiClient;
  checkpointId: string;
  upsertSession: UpsertSession;
  setDetailPanel: SetDetailPanel;
}): Promise<void> {
  const response = await args.api.getSessionCheckpoint(args.checkpointId);
  args.upsertSession(response.session);
  args.setDetailPanel(buildDetailFromCheckpointRecord(response.checkpoint));
}

export async function restoreCheckpointAction(args: {
  api: ConsoleApiClient;
  checkpointId: string;
  selectedSession: SessionCatalogRecord | null;
  upsertSession: UpsertSession;
  clearTranscriptState: () => void;
  setAttachments: SetAttachments;
  refreshSessions: (ensureSession: boolean) => Promise<void>;
  refreshSessionTranscript: () => Promise<void>;
  setDetailPanel: SetDetailPanel;
  setNotice: (value: string | null) => void;
}): Promise<void> {
  const response = await args.api.restoreSessionCheckpoint(args.checkpointId, {
    session_label: emptyToUndefined(
      `${args.selectedSession?.session_label ?? args.selectedSession?.title ?? "Checkpoint"} restore`,
    ),
  });
  args.upsertSession(response.session, { select: true });
  args.setDetailPanel(buildDetailFromCheckpointRecord(response.checkpoint));
  args.clearTranscriptState();
  args.setAttachments([]);
  await Promise.all([args.refreshSessions(false), args.refreshSessionTranscript()]);
  args.setNotice(`Checkpoint restored into ${response.session.title}.`);
}

export async function inspectBackgroundTaskAction(args: {
  api: ConsoleApiClient;
  taskId: string;
  setDetailPanel: SetDetailPanel;
}): Promise<void> {
  const response = await args.api.getBackgroundTask(args.taskId);
  args.setDetailPanel(buildDetailFromBackgroundTask(response.task, response.run));
}

export async function runBackgroundTaskActionRequest(args: {
  api: ConsoleApiClient;
  taskId: string;
  action: "pause" | "resume" | "retry" | "cancel";
  refreshSessionTranscript: () => Promise<void>;
  setNotice: (value: string | null) => void;
}): Promise<void> {
  const response =
    args.action === "pause"
      ? await args.api.pauseBackgroundTask(args.taskId)
      : args.action === "resume"
        ? await args.api.resumeBackgroundTask(args.taskId)
        : args.action === "retry"
          ? await args.api.retryBackgroundTask(args.taskId)
          : await args.api.cancelBackgroundTask(args.taskId);
  await args.refreshSessionTranscript();
  args.setNotice(`Background task ${shortId(response.task.task_id)}: ${response.action}.`);
}

function buildCompactionPreviewDetail(
  preview: ChatCompactionPreview,
  sessionId: string,
  actions: {
    acceptAllReviewCandidates?: () => Promise<void>;
    rejectAllReviewCandidates?: () => Promise<void>;
  },
): DetailPanelState {
  const detailActions = [];
  if (actions.acceptAllReviewCandidates !== undefined) {
    detailActions.push({
      key: "accept-review-candidates",
      label: "Accept review candidates",
      variant: "primary" as const,
      onPress: actions.acceptAllReviewCandidates,
    });
  }
  if (actions.rejectAllReviewCandidates !== undefined) {
    detailActions.push({
      key: "reject-review-candidates",
      label: "Reject review candidates",
      variant: "secondary" as const,
      onPress: actions.rejectAllReviewCandidates,
    });
  }
  return {
    id: `compaction-preview-${sessionId}`,
    title: preview.eligible ? "Compaction preview" : "Compaction preview blocked",
    subtitle: `${preview.strategy} · ${formatTokenDelta(preview.token_delta)} token delta`,
    body: preview.summary_text,
    payload: preview as unknown as JsonValue,
    actions: detailActions,
  };
}

function buildCompactionStatusEntry(
  preview: ChatCompactionPreview,
  sessionId: string,
  artifact?: ChatCompactionArtifactRecord,
): TranscriptEntry {
  return {
    id: `compaction-${artifact?.artifact_id ?? sessionId}-${preview.token_delta}`,
    kind: "status",
    session_id: sessionId,
    run_id: artifact?.run_id,
    created_at_unix_ms: Date.now(),
    title: artifact === undefined ? "Compaction preview" : "Compaction applied",
    text: `${preview.summary_preview}\nToken delta: ${formatTokenDelta(preview.token_delta)} · ${preview.condensed_event_count} condensed records.`,
    payload:
      artifact === undefined
        ? (preview as unknown as JsonValue)
        : ({
            preview,
            artifact,
          } as unknown as JsonValue),
    status: preview.eligible ? "ready" : "blocked",
  };
}

function formatTokenDelta(value: number): string {
  return `${value.toLocaleString()} tokens`;
}

function collectReviewCandidateIds(preview: ChatCompactionPreview): string[] {
  const summary = preview.summary as
    | {
        planner?: {
          candidates?: Array<{ candidate_id?: string; disposition?: string }>;
        };
      }
    | undefined;
  const candidates = summary?.planner?.candidates ?? [];
  return candidates
    .filter((candidate) => candidate.disposition === "review_required")
    .flatMap((candidate) =>
      typeof candidate.candidate_id === "string" && candidate.candidate_id.length > 0
        ? [candidate.candidate_id]
        : [],
    );
}

export function describeBackgroundTask(task: ChatBackgroundTaskRecord): string {
  return task.input_text ?? task.last_error ?? "No task text or error recorded.";
}
