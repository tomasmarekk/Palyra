import { useRef, useState } from "react";

import {
  ActionButton,
  ActionCluster,
  AppForm,
  InlineNotice,
  StatusChip,
  TextAreaField,
} from "../console/components/ui";
import type {
  ContextReferencePreviewEnvelope,
  ProjectContextPreviewEnvelope,
  RecallPreviewEnvelope,
} from "../consoleApi";
import type { ChatSlashSuggestion } from "./chatCommandSuggestions";

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
  slashSuggestions: readonly ChatSlashSuggestion[];
  selectedSlashSuggestionIndex: number;
  setSelectedSlashSuggestionIndex: (value: number) => void;
  dismissSlashPalette: () => void;
  acceptSlashSuggestion: (replacement: string, acceptedWithKeyboard?: boolean) => void;
  uxMetrics: {
    readonly slashCommands: number;
    readonly paletteAccepts: number;
    readonly keyboardAccepts: number;
    readonly undo: number;
    readonly interrupt: number;
    readonly errors: number;
  };
  contextBudget: ContextBudgetSummary;
  projectContextPreview: ProjectContextPreviewEnvelope | null;
  projectContextPreviewBusy: boolean;
  projectContextPreviewStale: boolean;
  projectContextPromptPreview: string | null;
  refreshProjectContextPreview: () => void;
  contextReferencePreview: ContextReferencePreviewEnvelope | null;
  contextReferencePreviewBusy: boolean;
  contextReferencePreviewStale: boolean;
  refreshContextReferencePreview: () => void;
  removeContextReference: (referenceId: string) => void;
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
  slashSuggestions,
  selectedSlashSuggestionIndex,
  setSelectedSlashSuggestionIndex,
  dismissSlashPalette,
  acceptSlashSuggestion,
  uxMetrics,
  contextBudget,
  projectContextPreview,
  projectContextPreviewBusy,
  projectContextPreviewStale,
  projectContextPromptPreview,
  refreshProjectContextPreview,
  contextReferencePreview,
  contextReferencePreviewBusy,
  contextReferencePreviewStale,
  refreshContextReferencePreview,
  removeContextReference,
  recallPreview,
  recallPreviewBusy,
  recallPreviewStale,
  refreshRecallPreview,
}: ChatComposerProps) {
  const [dragActive, setDragActive] = useState(false);
  const dragDepthRef = useRef(0);
  const slashListboxId = "chat-composer-slash-listbox";
  const composerDisabled = activeSessionId.trim().length === 0;
  const activeSlashSuggestion =
    slashSuggestions[selectedSlashSuggestionIndex] ?? slashSuggestions[0] ?? null;
  const sendLabel = streaming
    ? "Streaming..."
    : showSlashPalette && parsedSlashCommand !== null
      ? "Run command"
      : "Send";
  const previewVisible =
    activeSessionId.trim().length > 0 && composerText.trim().length > 0 && !showSlashPalette;
  const referencePreviewVisible =
    previewVisible &&
    contextReferencePreview !== null &&
    (contextReferencePreview.references.length > 0 ||
      contextReferencePreview.errors.length > 0 ||
      contextReferencePreview.warnings.length > 0);
  const projectContextEntryPreview = projectContextPreview?.entries.slice(0, 4) ?? [];
  const previewWorkspaceHits = recallPreview?.workspace_hits.slice(0, 2) ?? [];
  const recallDiagnostics = recallPreview?.diagnostics ?? [];
  const recallLatencyMs = recallDiagnostics.reduce(
    (maxLatency, diagnostic) => Math.max(maxLatency, diagnostic.total_latency_ms),
    0,
  );
  const recallCacheHits = recallDiagnostics.filter(
    (diagnostic) => diagnostic.query_embedding_cache_hit,
  ).length;
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
            {contextBudget.draft_tokens.toLocaleString()}, project context{" "}
            {contextBudget.project_context_tokens.toLocaleString()}, references{" "}
            {contextBudget.reference_tokens.toLocaleString()}, attachments{" "}
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
            <p className="workspace-kicker">Project context</p>
            <h3>Deterministic project rules for the next prompt</h3>
            <p className="chat-muted">
              Prompt layer order stays explicit: deterministic project rules first, prompt-scoped
              references second, learned recall and workspace memory after that.
            </p>
          </div>
          <div className="workspace-inline-actions">
            <StatusChip tone="default">1. Project rules</StatusChip>
            <StatusChip tone="accent">2. @references</StatusChip>
            <StatusChip tone="success">3. Learned recall</StatusChip>
          </div>
          <div className="workspace-inline-actions">
            <StatusChip tone={projectContextPreviewBusy ? "warning" : "default"}>
              {projectContextPreviewBusy ? "Refreshing..." : "Preview ready"}
            </StatusChip>
            <StatusChip
              tone={(projectContextPreview?.active_entries ?? 0) > 0 ? "accent" : "default"}
            >
              {projectContextPreview?.active_entries ?? 0} active files
            </StatusChip>
            <StatusChip
              tone={(projectContextPreview?.warnings.length ?? 0) > 0 ? "warning" : "default"}
            >
              {projectContextPreview?.warnings.length ?? 0} warnings
            </StatusChip>
            <StatusChip tone={projectContextPreviewStale ? "warning" : "default"}>
              {projectContextPreviewStale ? "Draft changed" : "In sync"}
            </StatusChip>
            <ActionButton
              isDisabled={projectContextPreviewBusy}
              type="button"
              variant="secondary"
              onPress={refreshProjectContextPreview}
            >
              {projectContextPreviewBusy ? "Refreshing..." : "Refresh project context"}
            </ActionButton>
          </div>
          {projectContextPreview?.warnings.map((warning, index) => (
            <InlineNotice key={`project-context-warning-${index}`} tone="warning">
              {warning}
            </InlineNotice>
          ))}
          {projectContextPreview === null ? (
            <p className="chat-muted">
              No deterministic project rules are active for this session yet.
            </p>
          ) : (
            <>
              {projectContextPreview.focus_paths.length > 0 ? (
                <div className="workspace-inline-actions">
                  {projectContextPreview.focus_paths.map((focus) => (
                    <StatusChip key={`${focus.reason}-${focus.path}`} tone="default">
                      {focus.reason}: {focus.path}
                    </StatusChip>
                  ))}
                </div>
              ) : null}
              <div className="chat-ops-list">
                {projectContextEntryPreview.map((entry) => (
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
                      <StatusChip tone={entry.active ? "accent" : "warning"}>
                        {entry.status.replaceAll("_", " ")}
                      </StatusChip>
                      <StatusChip tone="default">{entry.content_hash.slice(0, 10)}</StatusChip>
                      <StatusChip tone="default">
                        {entry.estimated_tokens.toLocaleString()} tok
                      </StatusChip>
                    </div>
                  </article>
                ))}
              </div>
              {projectContextPromptPreview !== null ? (
                <pre className="chat-composer__recall-preview">{projectContextPromptPreview}</pre>
              ) : null}
            </>
          )}
        </div>
      ) : null}

      {referencePreviewVisible ? (
        <div className="chat-composer__recall">
          <div className="workspace-panel__intro">
            <p className="workspace-kicker">Context references</p>
            <h3>Resolved references for the current draft</h3>
            <p className="chat-muted">
              References are parsed and resolved on the server before send so the same syntax works
              in web, CLI, and TUI.
            </p>
          </div>
          <div className="workspace-inline-actions">
            <StatusChip tone={contextReferencePreviewBusy ? "warning" : "default"}>
              {contextReferencePreviewBusy ? "Resolving..." : "Resolved"}
            </StatusChip>
            <StatusChip
              tone={(contextReferencePreview?.references.length ?? 0) > 0 ? "accent" : "default"}
            >
              {contextReferencePreview?.references.length ?? 0} references
            </StatusChip>
            <StatusChip
              tone={
                (contextReferencePreview?.total_estimated_tokens ?? 0) > 0 ? "success" : "default"
              }
            >
              {(contextReferencePreview?.total_estimated_tokens ?? 0).toLocaleString()} est. tokens
            </StatusChip>
            <StatusChip tone={contextReferencePreviewStale ? "warning" : "default"}>
              {contextReferencePreviewStale ? "Draft changed" : "In sync"}
            </StatusChip>
            <ActionButton
              isDisabled={contextReferencePreviewBusy}
              type="button"
              variant="secondary"
              onPress={refreshContextReferencePreview}
            >
              {contextReferencePreviewBusy ? "Resolving..." : "Refresh references"}
            </ActionButton>
          </div>

          {contextReferencePreview?.errors.map((error) => (
            <InlineNotice key={`${error.start_offset}-${error.end_offset}`} tone="danger">
              {error.message}
            </InlineNotice>
          ))}
          {contextReferencePreview?.warnings.map((warning, index) => (
            <InlineNotice key={`context-reference-warning-${index}`} tone="warning">
              {warning}
            </InlineNotice>
          ))}

          <div className="workspace-inline-actions">
            {contextReferencePreview?.references.map((reference) => (
              <StatusChip
                key={reference.reference_id}
                tone={reference.warnings.length > 0 ? "warning" : "accent"}
              >
                {reference.raw_text} · {reference.estimated_tokens.toLocaleString()} tok
              </StatusChip>
            ))}
          </div>

          <div className="chat-ops-list">
            {contextReferencePreview?.references.map((reference) => (
              <article key={reference.reference_id} className="chat-ops-card">
                <div className="chat-ops-card__copy">
                  <strong>
                    @{reference.kind}:{reference.display_target}
                  </strong>
                  <span>{reference.provenance.map((item) => item.note).join(" ")}</span>
                  <p>{reference.preview_text}</p>
                  {reference.warnings.length > 0 ? <p>{reference.warnings.join(" ")}</p> : null}
                </div>
                <div className="chat-ops-card__actions">
                  <StatusChip tone="accent">
                    {reference.estimated_tokens.toLocaleString()} tok
                  </StatusChip>
                  <ActionButton
                    size="sm"
                    type="button"
                    variant="secondary"
                    onPress={() => removeContextReference(reference.reference_id)}
                  >
                    Remove
                  </ActionButton>
                </div>
              </article>
            ))}
          </div>
        </div>
      ) : null}

      {previewVisible ? (
        <div className="chat-composer__recall">
          <div className="workspace-panel__intro">
            <p className="workspace-kicker">Recall preview</p>
            <h3>Context that will be attached to the next prompt</h3>
            <p className="chat-muted">
              Inspect retrieved workspace docs and memory before send. Refresh if you want to force
              a fresh preview for the current draft.
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
            <StatusChip
              tone={
                recallDiagnostics.some((diagnostic) => diagnostic.latency_budget_exceeded)
                  ? "warning"
                  : "default"
              }
            >
              {recallLatencyMs}ms retrieval
            </StatusChip>
            <StatusChip tone={recallCacheHits > 0 ? "success" : "default"}>
              {recallCacheHits}/{recallDiagnostics.length} cached
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
            <p className="chat-muted">
              Recall preview will appear once the current draft is evaluated.
            </p>
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
                      <StatusChip tone="success">{formatPreviewScore(hit)}</StatusChip>
                    </div>
                  </article>
                ))}
              </div>
              <pre className="chat-composer__recall-preview">{recallPreview.prompt_preview}</pre>
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
        onKeyDown={(event) => {
          if (!showSlashPalette || slashSuggestions.length === 0) {
            if (event.key === "Escape" && showSlashPalette) {
              event.preventDefault();
              dismissSlashPalette();
            }
            return;
          }
          switch (event.key) {
            case "ArrowDown":
              event.preventDefault();
              setSelectedSlashSuggestionIndex(
                Math.min(selectedSlashSuggestionIndex + 1, slashSuggestions.length - 1),
              );
              break;
            case "ArrowUp":
              event.preventDefault();
              setSelectedSlashSuggestionIndex(Math.max(selectedSlashSuggestionIndex - 1, 0));
              break;
            case "Tab":
              event.preventDefault();
              if (activeSlashSuggestion !== null) {
                acceptSlashSuggestion(activeSlashSuggestion.replacement, true);
              }
              break;
            case "Escape":
              event.preventDefault();
              dismissSlashPalette();
              break;
            default:
              break;
          }
        }}
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
        <div
          id={slashListboxId}
          className="chat-composer__slash"
          role="listbox"
          aria-label="Slash commands"
        >
          <div className="workspace-panel__intro">
            <p className="workspace-kicker">Slash commands</p>
            <h3>Operator shortcuts</h3>
            <p className="chat-muted">
              Autocomplete stays scoped to leading slash input, keeps entity suggestions contextual,
              and remains fully keyboard-operable.
            </p>
          </div>
          <div className="workspace-inline-actions">
            <StatusChip tone="accent">{slashSuggestions.length} suggestions</StatusChip>
            <StatusChip tone="default">{uxMetrics.slashCommands} slash commands</StatusChip>
            <StatusChip tone="default">{uxMetrics.paletteAccepts} palette accepts</StatusChip>
            <StatusChip tone="default">{uxMetrics.undo} undo restores</StatusChip>
            <StatusChip tone="default">{uxMetrics.interrupt} interrupts</StatusChip>
          </div>
          {activeSlashSuggestion !== null ? (
            <article className="chat-command-card chat-command-card--active" aria-live="polite">
              <strong>{activeSlashSuggestion.title}</strong>
              <span>{activeSlashSuggestion.subtitle}</span>
              <p className="chat-muted">{activeSlashSuggestion.detail}</p>
              <code>{activeSlashSuggestion.example}</code>
            </article>
          ) : null}
          <div className="chat-composer__slash-list">
            {(slashSuggestions.length > 0
              ? slashSuggestions
              : slashCommandMatches.map((command) => ({
                  id: `fallback:${command.name}`,
                  kind: "command" as const,
                  commandName: command.name,
                  title: command.synopsis,
                  subtitle: command.description,
                  detail: command.example,
                  example: command.example,
                  replacement: command.example,
                  badge: command.category,
                }))
            ).map((suggestion, index) => (
              <button
                key={suggestion.id}
                id={slashSuggestionDomId(suggestion.id)}
                role="option"
                aria-selected={index === selectedSlashSuggestionIndex}
                className={`chat-command-card${index === selectedSlashSuggestionIndex ? " chat-command-card--selected" : ""}`}
                type="button"
                onMouseEnter={() => setSelectedSlashSuggestionIndex(index)}
                onClick={() => acceptSlashSuggestion(suggestion.replacement, false)}
              >
                <strong>{suggestion.title}</strong>
                <span>{suggestion.subtitle}</span>
                <p className="chat-muted">{suggestion.badge}</p>
                <code>{suggestion.example}</code>
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
                  {attachment.derived_artifacts !== undefined &&
                  attachment.derived_artifacts.length > 0 ? (
                    <div className="workspace-inline-actions">
                      {attachment.derived_artifacts.map((derivedArtifact) => (
                        <StatusChip
                          key={derivedArtifact.derived_artifact_id}
                          tone={toneForDerivedArtifactState(derivedArtifact.state)}
                        >
                          {derivedArtifact.kind} · {derivedArtifact.state}
                        </StatusChip>
                      ))}
                    </div>
                  ) : null}
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

function slashSuggestionDomId(suggestionId: string): string {
  return `chat-composer-slash-${suggestionId.replace(/[^a-z0-9_-]/gi, "-")}`;
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

function readStringValue(
  source: Record<string, unknown> | undefined,
  key: string,
): string | undefined {
  const value = source?.[key];
  return typeof value === "string" ? value : undefined;
}

function readNumberValue(
  source: Record<string, unknown> | undefined,
  key: string,
): number | undefined {
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

function toneForDerivedArtifactState(
  state: string,
): "default" | "accent" | "success" | "warning" | "danger" {
  switch (state) {
    case "succeeded":
      return "success";
    case "quarantined":
      return "warning";
    case "failed":
    case "purged":
      return "danger";
    default:
      return "default";
  }
}
