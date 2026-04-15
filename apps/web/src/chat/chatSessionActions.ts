import type {
  ChatDelegationCatalog,
  ChatTranscriptRecord,
  ConsoleApiClient,
  JsonValue,
  SessionCatalogRecord,
} from "../consoleApi";

import type { DetailPanelState, TranscriptSearchMatch } from "./ChatInspectorColumn";
import { downloadTextFile, uploadComposerAttachments } from "./chatConsoleUtils";
import {
  emptyToUndefined,
  prettifyEventType,
  shortId,
  toErrorMessage,
  type ComposerAttachment,
  type TranscriptEntry,
} from "./chatShared";

type SetDetailPanel = (value: DetailPanelState | null) => void;

type SetAttachments = (next: ComposerAttachment[]) => void;

type SetTranscriptSearchResults = (value: TranscriptSearchMatch[]) => void;

type SetCommandBusy = (value: string | null) => void;

type SetComposerText = (value: string) => void;

type CreateSessionWithLabel = (requestedLabel?: string) => Promise<string | null>;

type SendMessage = (
  onComplete: () => Promise<void>,
  options?: {
    text?: string;
    origin_kind?: string;
    origin_run_id?: string;
    parameter_delta?: JsonValue;
  },
) => Promise<boolean>;

type AppendLocalEntry = (entry: TranscriptEntry) => void;

type UpsertSession = (session: SessionCatalogRecord, options?: { select?: boolean }) => void;

export async function createNewSessionAction(args: {
  requestedLabel?: string;
  createSessionWithLabel: CreateSessionWithLabel;
  clearTranscriptState: () => void;
  setDetailPanel: SetDetailPanel;
  setTranscriptSearchResults: SetTranscriptSearchResults;
  setAttachments: SetAttachments;
  setComposerText: SetComposerText;
  setError: (value: string | null) => void;
  setNotice: (value: string | null) => void;
}): Promise<void> {
  args.setError(null);
  args.setNotice(null);
  const createdSessionId = await args.createSessionWithLabel(args.requestedLabel);
  if (createdSessionId === null) {
    return;
  }
  args.clearTranscriptState();
  args.setDetailPanel(null);
  args.setTranscriptSearchResults([]);
  args.setAttachments([]);
  args.setComposerText("");
  args.setNotice(
    args.requestedLabel !== undefined && args.requestedLabel.trim().length > 0
      ? `Created a fresh session: ${args.requestedLabel.trim()}.`
      : "Created a fresh session.",
  );
}

export function resumeSessionAction(args: {
  rawTarget: string;
  sortedSessions: SessionCatalogRecord[];
  setActiveSessionId: (value: string) => void;
  setComposerText: SetComposerText;
  setError: (value: string | null) => void;
  setNotice: (value: string | null) => void;
}): void {
  const target = args.rawTarget.trim();
  if (target.length === 0) {
    args.setError("Usage: /resume <session-id-or-key-or-title>");
    return;
  }
  const normalizedTarget = target.toLowerCase();
  const matchedSession =
    args.sortedSessions.find((session) => session.session_id === target) ??
    args.sortedSessions.find((session) => session.session_key === target) ??
    args.sortedSessions.find((session) => session.title.toLowerCase() === normalizedTarget) ??
    args.sortedSessions.find(
      (session) => session.family.root_title.toLowerCase() === normalizedTarget,
    ) ??
    args.sortedSessions.find((session) =>
      session.family.relatives.some((relative) => relative.title.toLowerCase() === normalizedTarget),
    ) ??
    args.sortedSessions.find((session) =>
      [
        session.session_key,
        session.title,
        session.family.root_title,
        session.preview,
        session.last_summary,
        session.agent_id,
        session.model_profile,
        ...session.family.relatives.map((relative) => relative.title),
        ...session.recap.touched_files,
        ...session.recap.active_context_files,
        ...session.recap.recent_artifacts.map((artifact) => artifact.label),
      ]
        .filter((value): value is string => typeof value === "string" && value.length > 0)
        .some((value) => value.toLowerCase().includes(normalizedTarget)),
    ) ??
    null;
  if (matchedSession === null) {
    args.setError(`No loaded session matches "${target}". Use /history first if needed.`);
    return;
  }
  args.setActiveSessionId(matchedSession.session_id);
  args.setComposerText("");
  args.setNotice(`Resumed session ${matchedSession.title}.`);
}

export async function retryLatestTurnAction(args: {
  api: ConsoleApiClient;
  sessionId: string;
  refreshSessions: (ensureSession: boolean) => Promise<void>;
  refreshSessionTranscript: () => Promise<void>;
  sendMessage: SendMessage;
  appendLocalEntry: AppendLocalEntry;
  setCommandBusy: SetCommandBusy;
  setError: (value: string | null) => void;
  setNotice: (value: string | null) => void;
}): Promise<void> {
  if (args.sessionId.length === 0) {
    args.setError("Select a session before retrying.");
    return;
  }

  args.setCommandBusy("retry");
  args.setError(null);
  args.setNotice(null);
  try {
    const response = await args.api.prepareRetry(args.sessionId);
    const didSend = await args.sendMessage(
      async () => {
        await Promise.all([args.refreshSessions(false), args.refreshSessionTranscript()]);
      },
      {
        text: response.text,
        origin_kind: response.origin_kind,
        origin_run_id: response.origin_run_id,
        parameter_delta: response.parameter_delta,
      },
    );
    if (didSend) {
      args.appendLocalEntry({
        id: `retry-${response.origin_run_id}-${Date.now()}`,
        kind: "status",
        session_id: args.sessionId,
        run_id: response.origin_run_id,
        created_at_unix_ms: Date.now(),
        title: "Retry requested",
        text: `Replayed the latest user turn from ${shortId(response.origin_run_id)}.`,
        payload: response.parameter_delta,
      });
    }
  } catch (error) {
    args.setError(toErrorMessage(error));
  } finally {
    args.setCommandBusy(null);
  }
}

export async function branchCurrentSessionAction(args: {
  api: ConsoleApiClient;
  sessionId: string;
  requestedLabel?: string;
  upsertSession: UpsertSession;
  clearTranscriptState: () => void;
  setDetailPanel: SetDetailPanel;
  setAttachments: SetAttachments;
  setComposerText: SetComposerText;
  refreshSessions: (ensureSession: boolean) => Promise<void>;
  refreshSessionTranscript: () => Promise<void>;
  setCommandBusy: SetCommandBusy;
  setError: (value: string | null) => void;
  setNotice: (value: string | null) => void;
}): Promise<void> {
  if (args.sessionId.length === 0) {
    args.setError("Select a session before branching.");
    return;
  }

  args.setCommandBusy("branch");
  args.setError(null);
  args.setNotice(null);
  try {
    const response = await args.api.branchSession(args.sessionId, {
      session_label: emptyToUndefined(args.requestedLabel ?? ""),
    });
    args.upsertSession(response.session, { select: true });
    args.clearTranscriptState();
    args.setDetailPanel(null);
    args.setAttachments([]);
    args.setComposerText("");
    await Promise.all([args.refreshSessions(false), args.refreshSessionTranscript()]);
    args.setNotice(
      response.suggested_session_label !== undefined
        ? `Branch ready: ${response.session.title} from run ${shortId(response.source_run_id)}. Suggested title: ${response.suggested_session_label}.`
        : `Branch ready: ${response.session.title} from run ${shortId(response.source_run_id)}.`,
    );
  } catch (error) {
    args.setError(toErrorMessage(error));
  } finally {
    args.setCommandBusy(null);
  }
}

export async function queueFollowUpTextAction(args: {
  api: ConsoleApiClient;
  targetRunId: string | null;
  text: string;
  sessionId: string;
  appendLocalEntry: AppendLocalEntry;
  refreshSessionTranscript: () => Promise<void>;
  setComposerText: SetComposerText;
  setCommandBusy: SetCommandBusy;
  setError: (value: string | null) => void;
  setNotice: (value: string | null) => void;
}): Promise<void> {
  if (args.targetRunId === null || args.targetRunId.trim().length === 0) {
    args.setError("No active run is available for queued follow-up.");
    return;
  }

  const trimmed = args.text.trim();
  if (trimmed.length === 0) {
    args.setError("Queued follow-up cannot be empty.");
    return;
  }

  args.setCommandBusy("queue");
  args.setError(null);
  args.setNotice(null);
  try {
    const response = await args.api.queueFollowUp(args.targetRunId, { text: trimmed });
    args.appendLocalEntry({
      id: `queued-${response.queued_input.queued_input_id}-${Date.now()}`,
      kind: "status",
      run_id: args.targetRunId,
      session_id: args.sessionId,
      created_at_unix_ms: Date.now(),
      title: "Queued follow-up",
      text: `Queued input ${shortId(response.queued_input.queued_input_id)} for ${shortId(args.targetRunId)}.`,
      payload: response.queued_input as unknown as JsonValue,
      status: "queued",
    });
    args.setComposerText("");
    await args.refreshSessionTranscript();
    args.setNotice("Queued follow-up stored.");
  } catch (error) {
    args.setError(toErrorMessage(error));
  } finally {
    args.setCommandBusy(null);
  }
}

export async function delegateWorkAction(args: {
  api: ConsoleApiClient;
  sessionId: string;
  raw: string;
  delegationCatalog: ChatDelegationCatalog | null;
  upsertSession: UpsertSession;
  refreshSessionTranscript: () => Promise<void>;
  appendLocalEntry: AppendLocalEntry;
  setComposerText: SetComposerText;
  setCommandBusy: SetCommandBusy;
  setError: (value: string | null) => void;
  setNotice: (value: string | null) => void;
}): Promise<void> {
  if (args.sessionId.length === 0) {
    args.setError("Select a session before delegating work.");
    return;
  }
  if (args.delegationCatalog === null) {
    args.setError("Delegation catalog is still loading.");
    return;
  }

  const trimmed = args.raw.trim();
  if (trimmed.length === 0) {
    const available = [
      ...args.delegationCatalog.templates.map((template) => template.template_id),
      ...args.delegationCatalog.profiles.map((profile) => profile.profile_id),
    ].slice(0, 6);
    args.setError(
      available.length > 0
        ? `Use /delegate <profile-or-template> <text>. Available: ${available.join(", ")}.`
        : "Use /delegate <profile-or-template> <text>.",
    );
    return;
  }

  const [selector, ...promptParts] = trimmed.split(/\s+/);
  const prompt = promptParts.join(" ").trim();
  if (prompt.length === 0) {
    args.setError("Provide the delegated task text after the profile or template name.");
    return;
  }

  const normalizedSelector = selector.trim().toLowerCase();
  const template = args.delegationCatalog.templates.find(
    (candidate) => candidate.template_id.toLowerCase() === normalizedSelector,
  );
  const profile = args.delegationCatalog.profiles.find(
    (candidate) => candidate.profile_id.toLowerCase() === normalizedSelector,
  );
  if (template === undefined && profile === undefined) {
    args.setError(`Unknown delegation profile or template '${selector}'.`);
    return;
  }

  args.setCommandBusy("delegate");
  args.setError(null);
  args.setNotice(null);
  try {
    const response = await args.api.createBackgroundTask(args.sessionId, {
      text: prompt,
      delegation:
        template !== undefined
          ? { template_id: template.template_id }
          : { profile_id: profile!.profile_id },
    });
    args.upsertSession(response.session);
    await args.refreshSessionTranscript();
    args.appendLocalEntry({
      id: `delegated-${response.task.task_id}-${Date.now()}`,
      kind: "status",
      session_id: args.sessionId,
      created_at_unix_ms: Date.now(),
      title: "Delegated child run queued",
      text: `${template?.display_name ?? profile?.display_name ?? selector}: ${prompt}`,
      payload: response.task as unknown as JsonValue,
    });
    args.setComposerText("");
    args.setNotice(`Delegated child run queued via ${selector}.`);
  } catch (error) {
    args.setError(toErrorMessage(error));
  } finally {
    args.setCommandBusy(null);
  }
}

export async function exportTranscriptAction(args: {
  api: ConsoleApiClient;
  sessionId: string;
  sessionLabel?: string;
  format: "json" | "markdown";
  setExportBusy: (value: "json" | "markdown" | null) => void;
  setError: (value: string | null) => void;
  setNotice: (value: string | null) => void;
}): Promise<void> {
  if (args.sessionId.length === 0) {
    args.setError("Select a session before exporting.");
    return;
  }

  args.setExportBusy(args.format);
  args.setError(null);
  try {
    const response = await args.api.exportSessionTranscript(args.sessionId, args.format);
    const extension = args.format === "json" ? "json" : "md";
    const mimeType = args.format === "json" ? "application/json" : "text/markdown";
    const content =
      typeof response.content === "string"
        ? response.content
        : JSON.stringify(response.content, null, 2);
    downloadTextFile(
      `chat-${args.sessionLabel ?? shortId(args.sessionId)}.${extension}`,
      content,
      mimeType,
    );
    args.setNotice(`Transcript export ready: ${args.format}.`);
  } catch (error) {
    args.setError(toErrorMessage(error));
  } finally {
    args.setExportBusy(null);
  }
}

export async function pinTranscriptRecordAction(args: {
  api: ConsoleApiClient;
  sessionId: string;
  record: ChatTranscriptRecord;
  refreshSessionTranscript: () => Promise<void>;
  setCommandBusy: SetCommandBusy;
  setError: (value: string | null) => void;
  setNotice: (value: string | null) => void;
}): Promise<void> {
  if (args.sessionId.length === 0) {
    args.setError("Select a session before pinning transcript events.");
    return;
  }

  args.setCommandBusy("pin");
  try {
    await args.api.createSessionPin(args.sessionId, {
      run_id: args.record.run_id,
      tape_seq: args.record.seq,
      title: `${prettifyEventType(args.record.event_type)} #${args.record.seq}`,
      note: `Pinned from ${args.record.origin_kind} at ${new Date(args.record.created_at_unix_ms).toLocaleString()}.`,
    });
    await args.refreshSessionTranscript();
    args.setNotice(`Pinned event #${args.record.seq}.`);
  } catch (error) {
    args.setError(toErrorMessage(error));
  } finally {
    args.setCommandBusy(null);
  }
}

export async function deletePinAction(args: {
  api: ConsoleApiClient;
  sessionId: string;
  pinId: string;
  refreshSessionTranscript: () => Promise<void>;
  setCommandBusy: SetCommandBusy;
  setError: (value: string | null) => void;
  setNotice: (value: string | null) => void;
}): Promise<void> {
  if (args.sessionId.length === 0) {
    args.setError("Select a session before deleting pins.");
    return;
  }

  args.setCommandBusy("delete-pin");
  try {
    await args.api.deleteSessionPin(args.sessionId, args.pinId);
    await args.refreshSessionTranscript();
    args.setNotice("Pin deleted.");
  } catch (error) {
    args.setError(toErrorMessage(error));
  } finally {
    args.setCommandBusy(null);
  }
}

export async function abortCurrentRunAction(args: {
  api: ConsoleApiClient;
  targetRunId: string | null;
  runDrawerOpen: boolean;
  runDrawerId: string;
  reason?: string;
  refreshRunDetails: () => void;
  refreshSessions: (ensureSession: boolean) => Promise<void>;
  refreshSessionTranscript: () => Promise<void>;
  setRunActionBusy: (value: boolean) => void;
  setError: (value: string | null) => void;
  setNotice: (value: string | null) => void;
}): Promise<void> {
  if (args.targetRunId === null || args.targetRunId.trim().length === 0) {
    args.setError("No run is available for cancellation.");
    return;
  }

  args.setRunActionBusy(true);
  args.setError(null);
  args.setNotice(null);
  try {
    const response = await args.api.abortSessionRun(args.targetRunId, {
      reason: args.reason,
    });
    args.setNotice(
      response.cancel_requested ? "Run cancellation requested." : "Run was already idle.",
    );
    await args.refreshSessions(false);
    if (args.runDrawerOpen && args.runDrawerId.trim() === args.targetRunId) {
      args.refreshRunDetails();
    }
    await args.refreshSessionTranscript();
  } catch (error) {
    args.setError(error instanceof Error ? error.message : "Unexpected failure.");
  } finally {
    args.setRunActionBusy(false);
  }
}

export async function handleAttachmentFilesAction(args: {
  api: ConsoleApiClient;
  sessionId: string;
  files: readonly File[];
  setAttachments: (
    value: ComposerAttachment[] | ((previous: ComposerAttachment[]) => ComposerAttachment[]),
  ) => void;
  setAttachmentBusy: (value: boolean) => void;
  setError: (value: string | null) => void;
  setNotice: (value: string | null) => void;
  clearAttachmentInput: () => void;
}): Promise<void> {
  if (args.sessionId.length === 0) {
    args.setError("Select a session before uploading attachments.");
    return;
  }
  if (args.files.length === 0) {
    return;
  }

  args.setAttachmentBusy(true);
  args.setError(null);
  args.setNotice(null);
  try {
    const nextAttachments = await uploadComposerAttachments(args.api, args.sessionId, args.files);
    args.setAttachments((previous) => [...previous, ...nextAttachments]);
    args.setNotice(
      `${nextAttachments.length} attachment${nextAttachments.length === 1 ? "" : "s"} ready for the next message.`,
    );
  } catch (error) {
    args.setError(toErrorMessage(error));
  } finally {
    args.setAttachmentBusy(false);
    args.clearAttachmentInput();
  }
}
