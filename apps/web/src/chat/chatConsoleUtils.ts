import type {
  ChatBackgroundTaskRecord,
  ChatAttachmentRecord,
  ChatCheckpointRecord,
  ChatCompactionArtifactRecord,
  ChatRunStatusRecord,
  ChatTranscriptRecord,
  ConsoleApiClient,
  JsonValue,
  MediaDerivedArtifactRecord,
} from "../consoleApi";

import type { DetailPanelState, TranscriptSearchMatch } from "./ChatInspectorColumn";
import {
  parseTapePayload,
  prettifyEventType,
  shortId,
  type ComposerAttachment,
  type TranscriptEntry,
} from "./chatShared";

export function buildDetailFromLiveEntry(entry: TranscriptEntry): DetailPanelState {
  return {
    id: entry.id,
    title: entry.title,
    subtitle: `${entry.run_id !== undefined ? `Run ${shortId(entry.run_id)} · ` : ""}${new Date(entry.created_at_unix_ms).toLocaleString()}`,
    body: entry.text,
    payload: entry.payload,
  };
}

export function buildDetailFromTranscriptRecord(record: ChatTranscriptRecord): DetailPanelState {
  return {
    id: `${record.run_id}:${record.seq}`,
    title: `${prettifyEventType(record.event_type)} #${record.seq}`,
    subtitle: `${new Date(record.created_at_unix_ms).toLocaleString()} · ${record.origin_kind}${record.origin_run_id !== undefined ? ` · from ${shortId(record.origin_run_id)}` : ""}`,
    payload: parseTapePayload(record.payload_json),
  };
}

export function buildDetailFromSearchMatch(match: TranscriptSearchMatch): DetailPanelState {
  return {
    id: `search-${match.run_id}-${match.seq}`,
    title: `${prettifyEventType(match.event_type)} #${match.seq}`,
    subtitle: `${new Date(match.created_at_unix_ms).toLocaleString()} · ${match.origin_kind}`,
    body: match.snippet,
  };
}

export function buildDetailFromCompactionArtifact(
  artifact: ChatCompactionArtifactRecord,
  relatedCheckpoints: ChatCheckpointRecord[] = [],
): DetailPanelState {
  return {
    id: `compaction-${artifact.artifact_id}`,
    title: `Compaction ${shortId(artifact.artifact_id)}`,
    subtitle: `${new Date(artifact.created_at_unix_ms).toLocaleString()} · ${artifact.mode} · ${artifact.strategy}`,
    body: artifact.summary_preview,
    payload: {
      artifact: {
        ...artifact,
        source_records_json: safeParseJsonString(artifact.source_records_json),
        summary_json: safeParseJsonString(artifact.summary_json),
        trigger_inputs_json:
          artifact.trigger_inputs_json === undefined
            ? undefined
            : safeParseJsonString(artifact.trigger_inputs_json),
      } as unknown as JsonValue,
      related_checkpoints: relatedCheckpoints.map((checkpoint) => ({
        ...checkpoint,
        tags_json: safeParseJsonString(checkpoint.tags_json),
        referenced_compaction_ids_json: safeParseJsonString(
          checkpoint.referenced_compaction_ids_json,
        ),
        workspace_paths_json: safeParseJsonString(checkpoint.workspace_paths_json),
      })) as unknown as JsonValue,
    },
  };
}

export function buildDetailFromCheckpointRecord(
  checkpoint: ChatCheckpointRecord,
): DetailPanelState {
  return {
    id: `checkpoint-${checkpoint.checkpoint_id}`,
    title: checkpoint.name,
    subtitle: `${new Date(checkpoint.created_at_unix_ms).toLocaleString()} · ${checkpoint.branch_state}`,
    body: checkpoint.note,
    payload: {
      checkpoint: {
        ...checkpoint,
        tags_json: safeParseJsonString(checkpoint.tags_json),
        referenced_compaction_ids_json: safeParseJsonString(
          checkpoint.referenced_compaction_ids_json,
        ),
        workspace_paths_json: safeParseJsonString(checkpoint.workspace_paths_json),
      } as unknown as JsonValue,
    },
  };
}

export function buildDetailFromBackgroundTask(
  task: ChatBackgroundTaskRecord,
  run?: ChatRunStatusRecord,
): DetailPanelState {
  return {
    id: `background-task-${task.task_id}`,
    title: `Background task ${shortId(task.task_id)}`,
    subtitle: `${task.state} · ${task.task_kind} · ${new Date(task.updated_at_unix_ms).toLocaleString()}`,
    body: task.input_text ?? task.last_error,
    payload: {
      task: {
        ...task,
        payload_json:
          task.payload_json === undefined ? undefined : safeParseJsonString(task.payload_json),
        notification_target_json:
          task.notification_target_json === undefined
            ? undefined
            : safeParseJsonString(task.notification_target_json),
        result_json:
          task.result_json === undefined ? undefined : safeParseJsonString(task.result_json),
      } as unknown as JsonValue,
      run: run as unknown as JsonValue,
    },
  };
}

export function buildDetailFromDerivedArtifact(
  derivedArtifact: MediaDerivedArtifactRecord,
  attachment?: ChatAttachmentRecord,
): DetailPanelState {
  return {
    id: `derived-${derivedArtifact.derived_artifact_id}`,
    title: `${derivedArtifact.kind} · ${derivedArtifact.filename}`,
    subtitle: `${derivedArtifact.state} · ${derivedArtifact.parser_name}@${derivedArtifact.parser_version}`,
    body:
      derivedArtifact.summary_text ??
      derivedArtifact.failure_reason ??
      derivedArtifact.quarantine_reason ??
      derivedArtifact.content_text,
    payload: {
      derived_artifact: derivedArtifact as unknown as JsonValue,
      attachment: attachment as unknown as JsonValue,
    },
  };
}

export function downloadTextFile(filename: string, content: string, mimeType: string): void {
  const blob = new Blob([content], { type: mimeType });
  const href = URL.createObjectURL(blob);
  const anchor = document.createElement("a");
  anchor.href = href;
  anchor.download = filename;
  anchor.click();
  URL.revokeObjectURL(href);
}

export function readFileAsDataUrl(file: File): Promise<string> {
  return new Promise((resolve, reject) => {
    const reader = new FileReader();
    reader.onerror = () => reject(reader.error ?? new Error("Failed to read attachment."));
    reader.onload = () => {
      if (typeof reader.result !== "string") {
        reject(new Error("Attachment reader returned an unexpected payload."));
        return;
      }
      resolve(reader.result);
    };
    reader.readAsDataURL(file);
  });
}

export async function uploadComposerAttachments(
  api: ConsoleApiClient,
  sessionId: string,
  files: readonly File[],
): Promise<ComposerAttachment[]> {
  const nextAttachments: ComposerAttachment[] = [];
  for (const file of files) {
    const dataUrl = await readFileAsDataUrl(file);
    const base64 = dataUrl.includes(",") ? dataUrl.slice(dataUrl.indexOf(",") + 1) : dataUrl;
    const response = await api.uploadChatAttachment(sessionId, {
      filename: file.name,
      content_type: file.type || "application/octet-stream",
      bytes_base64: base64,
    });
    nextAttachments.push({
      local_id: `${response.attachment.artifact_id}-${Date.now()}`,
      ...response.attachment,
      preview_url: response.attachment.kind === "image" ? dataUrl : undefined,
      derived_artifacts: response.derived_artifacts,
    });
  }
  return nextAttachments;
}

function safeParseJsonString(value: string): JsonValue {
  try {
    return JSON.parse(value) as JsonValue;
  } catch {
    return value;
  }
}
