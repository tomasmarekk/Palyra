import { useRef, useState } from "react";

import {
  ActionButton,
  ActionCluster,
  AppForm,
  InlineNotice,
  StatusChip,
  TextAreaField,
} from "../console/components/ui";
import type { RecallPreviewEnvelope } from "../consoleApi";

import type {
  ComposerAttachment,
  ContextBudgetSummary,
  ParsedSlashCommand,
  SlashCommandDefinition,
} from "./chatShared";

type ChatComposerProps = {
  composerText: string;
  setComposerText: (value: string) => void;
  streaming: boolean;
  activeSessionId: string;
  attachments: readonly ComposerAttachment[];
  attachmentBusy: boolean;
  canQueueFollowUp: boolean;
  submitMessage: () => void;
  retryLast: () => void;
  branchSession: () => void;
  queueFollowUp: () => void;
  cancelStreaming: () => void;
  clearTranscript: () => void;
  openAttachmentPicker: () => void;
  removeAttachment: (localId: string) => void;
  attachFiles: (files: readonly File[]) => void;
  showSlashPalette: boolean;
  parsedSlashCommand: ParsedSlashCommand | null;
  slashCommandMatches: readonly SlashCommandDefinition[];
  useSlashCommand: (command: SlashCommandDefinition) => void;
  contextBudget: ContextBudgetSummary;
  recallPreview: RecallPreviewEnvelope | null;
  recallPreviewBusy: boolean;
  recallPreviewStale: boolean;
  refreshRecallPreview: () => void;
};

export function ChatComposer({
  composerText,
  setComposerText,
  streaming,
  activeSessionId,
  attachments,
  attachmentBusy,
  canQueueFollowUp,
  submitMessage,
  retryLast,
  branchSession,
  queueFollowUp,
  cancelStreaming,
  clearTranscript,
  openAttachmentPicker,
  removeAttachment,
  attachFiles,
  showSlashPalette,
  parsedSlashCommand,
  slashCommandMatches,
  useSlashCommand,
  contextBudget,
  recallPreview,
  recallPreviewBusy,
  recallPreviewStale,
  refreshRecallPreview,
}: ChatComposerProps) {
  const [dragActive, setDragActive] = useState(false);
  const dragDepthRef = useRef(0);
  const composerDisabled = activeSessionId.trim().length === 0;
  const sendLabel = streaming
    ? "Streaming..."
    : showSlashPalette && parsedSlashCommand !== null
      ? "Run command"
      : "Send";
  const previewVisible =
    activeSessionId.trim().length > 0 &&
    composerText.trim().length > 0 &&
    !showSlashPalette;
  const previewWorkspaceHits = recallPreview?.workspace_hits.slice(0, 2) ?? [];
  const previewMemoryHits: Record<string, unknown>[] = [];
  for (const hit of recallPreview?.memory_hits ?? []) {
    if (isRecord(hit)) {
      previewMemoryHits.push(hit);
    }
    if (previewMemoryHits.length >= 2) {
      break;
    }
  }

  function pushFiles(files: FileList | readonly File[] | null | undefined): void {
    if (files === null || files === undefined) {
      return;
    }
    const nextFiles = Array.from(files);
    if (nextFiles.length === 0) {
      return;
    }
    attachFiles(nextFiles);
  }

  return (
    <AppForm
      className={`chat-composer${dragActive ? " chat-composer--dragging" : ""}`}
      onSubmit={(event) => {
        event.preventDefault();
        submitMessage();
      }}
    >
      <div className="chat-composer__budget" data-tone={contextBudget.tone}>
        <div>
          <strong>Context budget</strong>
          <p className="chat-muted">
            Baseline {contextBudget.baseline_tokens.toLocaleString()} tokens, draft{" "}
            {contextBudget.draft_tokens.toLocaleString()}, attachments{" "}
            {contextBudget.attachment_tokens.toLocaleString()}.
          </p>
        </div>
        <div className="chat-composer__budget-value">{contextBudget.label}</div>
      </div>

      {contextBudget.warning !== undefined ? (
        <InlineNotice tone={contextBudget.tone === "danger" ? "danger" : "warning"}>
          {contextBudget.warning}
        </InlineNotice>
      ) : null}

      {previewVisible ? (
        <div className="chat-composer__recall">
          <div className="workspace-panel__intro">
            <p className="workspace-kicker">Recall preview</p>
            <h3>Context that will be attached to the next prompt</h3>
            <p className="chat-muted">
              Inspect retrieved workspace docs and memory before send. Refresh if you want to
              force a fresh preview for the current draft.
            </p>
          </div>
          <div className="workspace-inline-actions">
            <StatusChip tone={recallPreviewBusy ? "warning" : "default"}>
              {recallPreviewBusy ? "Refreshing..." : "Preview ready"}
            </StatusChip>
            <StatusChip tone={previewWorkspaceHits.length > 0 ? "accent" : "default"}>
              {recallPreview?.workspace_hits.length ?? 0} workspace refs
            </StatusChip>
            <StatusChip tone={previewMemoryHits.length > 0 ? "success" : "default"}>
              {recallPreview?.memory_hits.length ?? 0} memory refs
            </StatusChip>
            <StatusChip tone={recallPreviewStale ? "warning" : "default"}>
              {recallPreviewStale ? "Draft changed" : "In sync"}
            </StatusChip>
            <ActionButton
              isDisabled={recallPreviewBusy}
              type="button"
              variant="secondary"
              onPress={refreshRecallPreview}
            >
              {recallPreviewBusy ? "Refreshing..." : "Refresh recall"}
            </ActionButton>
          </div>

          {recallPreview === null ? (
            <p className="chat-muted">Recall preview will appear once the current draft is evaluated.</p>
          ) : (
            <>
              <div className="chat-ops-list">
                {previewWorkspaceHits.map((hit, index) => (
                  <article
                    key={`${hit.document.document_id}-${hit.chunk_index}-${index}`}
                    className="chat-ops-card"
                  >
                    <div className="chat-ops-card__copy">
                      <strong>{hit.document.title}</strong>
                      <span>{hit.document.path}</span>
                      <p>{hit.snippet}</p>
                    </div>
                    <div className="chat-ops-card__actions">
                      <StatusChip tone="accent">{hit.score.toFixed(2)}</StatusChip>
                    </div>
                  </article>
                ))}
                {previewMemoryHits.map((hit, index) => (
                  <article key={memoryPreviewKey(hit, index)} className="chat-ops-card">
                    <div className="chat-ops-card__copy">
                      <strong>{memoryPreviewKey(hit, index)}</strong>
                      <span>{readStringValue(hit, "channel") ?? "memory"}</span>
                      <p>
                        {readStringValue(hit, "snippet") ??
                          readStringValue(hit, "content") ??
                          readStringValue(readRecord(hit, "item"), "content_text") ??
                          "No snippet returned."}
                      </p>
                    </div>
                    <div className="chat-ops-card__actions">
                      <StatusChip tone="success">
                        {formatPreviewScore(hit)}
                      </StatusChip>
                    </div>
                  </article>
                ))}
              </div>
              <pre className="chat-composer__recall-preview">
                {recallPreview.prompt_preview}
              </pre>
            </>
          )}
        </div>
      ) : null}

      <TextAreaField
        label="Message"
        description="Use /help to open command help. Slash commands are executed locally against the chat console controls."
        placeholder="Describe what you want the assistant to do"
        rows={5}
        value={composerText}
        onChange={setComposerText}
        onDragEnter={(event) => {
          event.preventDefault();
          dragDepthRef.current += 1;
          setDragActive(true);
        }}
        onDragLeave={(event) => {
          event.preventDefault();
          dragDepthRef.current = Math.max(0, dragDepthRef.current - 1);
          if (dragDepthRef.current === 0) {
            setDragActive(false);
          }
        }}
        onDragOver={(event) => {
          event.preventDefault();
        }}
        onDrop={(event) => {
          event.preventDefault();
          dragDepthRef.current = 0;
          setDragActive(false);
          pushFiles(event.dataTransfer?.files);
        }}
        onPaste={(event) => {
          const files = event.clipboardData?.files;
          if (files !== undefined && files.length > 0) {
            pushFiles(files);
          }
        }}
      />

      {dragActive ? (
        <div className="chat-composer__drop-hint">
          Drop files here to upload them through the media pipeline.
        </div>
      ) : null}

      {showSlashPalette ? (
        <div className="chat-composer__slash" role="listbox" aria-label="Slash commands">
          <div className="workspace-panel__intro">
            <p className="workspace-kicker">Slash commands</p>
            <h3>Operator shortcuts</h3>
            <p className="chat-muted">
              Create sessions, switch history, retry, branch, queue, search, and export without
              leaving the composer.
            </p>
          </div>
          <div className="chat-composer__slash-list">
            {slashCommandMatches.map((command) => (
              <button
                key={command.name}
                className="chat-command-card"
                type="button"
                onClick={() => useSlashCommand(command)}
              >
                <strong>{command.synopsis}</strong>
                <span>{command.description}</span>
                <code>{command.example}</code>
              </button>
            ))}
          </div>
        </div>
      ) : null}

      {attachments.length > 0 || attachmentBusy ? (
        <div className="chat-composer__attachments">
          <div className="workspace-panel__intro">
            <p className="workspace-kicker">Attachments</p>
            <h3>{attachmentBusy ? "Uploading..." : `${attachments.length} ready`}</h3>
          </div>
          <div className="chat-attachment-list">
            {attachments.map((attachment) => (
              <article key={attachment.local_id} className="chat-attachment-card">
                {attachment.preview_url !== undefined ? (
                  <img
                    alt={attachment.filename}
                    className="chat-attachment-card__preview"
                    src={attachment.preview_url}
                  />
                ) : (
                  <div className="chat-attachment-card__icon">{attachment.kind}</div>
                )}
                <div className="chat-attachment-card__copy">
                  <strong>{attachment.filename}</strong>
                  <span>
                    {attachment.kind} · {attachment.size_bytes.toLocaleString()} bytes ·{" "}
                    {attachment.budget_tokens.toLocaleString()} token budget
                  </span>
                </div>
                <ActionButton
                  size="sm"
                  type="button"
                  variant="secondary"
                  onPress={() => removeAttachment(attachment.local_id)}
                >
                  Remove
                </ActionButton>
              </article>
            ))}
          </div>
        </div>
      ) : null}

      <ActionCluster>
        <ActionButton
          isDisabled={streaming || composerDisabled || composerText.trim().length === 0}
          type="submit"
          variant="primary"
        >
          {sendLabel}
        </ActionButton>
        <ActionButton
          isDisabled={streaming || composerDisabled}
          type="button"
          variant="secondary"
          onPress={retryLast}
        >
          Retry last
        </ActionButton>
        <ActionButton
          isDisabled={streaming || composerDisabled}
          type="button"
          variant="secondary"
          onPress={branchSession}
        >
          Branch session
        </ActionButton>
        <ActionButton
          isDisabled={streaming || composerText.trim().length === 0 || !canQueueFollowUp}
          type="button"
          variant="secondary"
          onPress={queueFollowUp}
        >
          Queue follow-up
        </ActionButton>
        <ActionButton
          isDisabled={streaming || composerDisabled || attachmentBusy}
          type="button"
          variant="secondary"
          onPress={openAttachmentPicker}
        >
          {attachmentBusy ? "Uploading..." : "Attach files"}
        </ActionButton>
        <ActionButton
          isDisabled={!streaming}
          type="button"
          variant="danger"
          onPress={cancelStreaming}
        >
          Cancel stream
        </ActionButton>
        <ActionButton type="button" variant="ghost" onPress={clearTranscript}>
          Clear local transcript
        </ActionButton>
      </ActionCluster>
    </AppForm>
  );
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function readRecord(
  source: Record<string, unknown>,
  key: string,
): Record<string, unknown> | undefined {
  const value = source[key];
  return isRecord(value) ? value : undefined;
}

function readStringValue(source: Record<string, unknown> | undefined, key: string): string | undefined {
  const value = source?.[key];
  return typeof value === "string" ? value : undefined;
}

function readNumberValue(source: Record<string, unknown> | undefined, key: string): number | undefined {
  const value = source?.[key];
  return typeof value === "number" ? value : undefined;
}

function memoryPreviewKey(hit: Record<string, unknown>, index: number): string {
  return (
    readStringValue(hit, "memory_id") ??
    readStringValue(readRecord(hit, "item"), "memory_id") ??
    `memory-preview-${index + 1}`
  );
}

function formatPreviewScore(hit: Record<string, unknown>): string {
  const directScore = readNumberValue(hit, "score");
  if (directScore !== undefined) {
    return directScore.toFixed(2);
  }
  const breakdownScore = readNumberValue(readRecord(hit, "breakdown"), "final_score");
  return breakdownScore === undefined ? "n/a" : breakdownScore.toFixed(2);
}
