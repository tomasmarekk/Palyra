import type {
  AuthProfileView,
  ChatCheckpointRecord,
  ConsoleApiClient,
} from "../consoleApi";
import type { Section } from "../console/sectionMetadata";
import { readString, type JsonObject } from "../console/shared";

import {
  checkpointHasTag,
  selectUndoCheckpoint,
  type BrowserProfileSuggestionRecord,
  type BrowserSessionSuggestionRecord,
} from "./chatCommandSuggestions";
import { parseCompactCommandMode, toErrorMessage, type ParsedSlashCommand } from "./chatShared";
import { abortCurrentRunAction } from "./chatSessionActions";
import type { ChatUxMetricKey } from "./useChatSlashPalette";

type RecordUxMetric = (key: ChatUxMetricKey) => void;
type CheckpointRestoreSource = "undo" | "checkpoint" | "inspector";

export async function createUndoCheckpoint(args: {
  api: ConsoleApiClient;
  activeSessionId: string;
  transcriptRecordCount: number;
  sessionRunCount: number;
  source: "send" | "retry" | "redirect";
  setNotice: (next: string | null) => void;
  recordUxMetric: RecordUxMetric;
}): Promise<ChatCheckpointRecord | null> {
  const {
    api,
    activeSessionId,
    transcriptRecordCount,
    sessionRunCount,
    source,
    setNotice,
    recordUxMetric,
  } = args;
  const sessionId = activeSessionId.trim();
  if (sessionId.length === 0 || (transcriptRecordCount === 0 && sessionRunCount === 0)) {
    return null;
  }
  try {
    const response = await api.createSessionCheckpoint(sessionId, {
      name: `Undo checkpoint before ${source}`,
      note:
        source === "retry"
          ? "Created automatically before retry so /undo can restore the prior conversational state."
          : source === "redirect"
            ? "Created automatically before interrupt redirect so /undo can restore the prior conversational state."
            : "Created automatically before a new chat run so /undo can restore the prior conversational state.",
      tags: ["undo_safe", source],
    });
    return response.checkpoint;
  } catch (error) {
    setNotice(`Undo checkpoint skipped: ${toErrorMessage(error)}`);
    recordUxMetric("errors");
    return null;
  }
}

export async function interruptAndMaybeRedirect(args: {
  api: ConsoleApiClient;
  actionableRunId: string | null;
  raw: string;
  activeSessionId: string;
  runDrawerOpen: boolean;
  runDrawerId: string;
  cancelStreaming: () => void;
  refreshRunDetails: () => Promise<void>;
  refreshSessions: (preserveSelection?: boolean) => Promise<void>;
  refreshSessionTranscript: () => Promise<void>;
  refreshSlashEntityCatalogs: () => Promise<void>;
  setRunActionBusy: (next: boolean) => void;
  setError: (next: string | null) => void;
  setNotice: (next: string | null) => void;
  recordUxMetric: RecordUxMetric;
  createUndoCheckpointBeforeRedirect: () => Promise<ChatCheckpointRecord | null>;
  clearComposerDraft: () => void;
  sendRedirectPrompt: (
    redirectText: string,
    metadata: { mode: "soft" | "force"; runId: string },
  ) => Promise<void>;
}): Promise<void> {
  const {
    api,
    actionableRunId,
    raw,
    activeSessionId,
    runDrawerOpen,
    runDrawerId,
    cancelStreaming,
    refreshRunDetails,
    refreshSessions,
    refreshSessionTranscript,
    refreshSlashEntityCatalogs,
    setRunActionBusy,
    setError,
    setNotice,
    recordUxMetric,
    createUndoCheckpointBeforeRedirect,
    clearComposerDraft,
    sendRedirectPrompt,
  } = args;
  if (actionableRunId === null) {
    setError("No run is available for interruption.");
    recordUxMetric("errors");
    return;
  }

  const trimmed = raw.trim();
  const [firstToken = "", ...promptParts] = trimmed.split(/\s+/);
  const mode: "soft" | "force" = firstToken === "force" ? "force" : "soft";
  const redirectText =
    firstToken === "force" || firstToken === "soft"
      ? promptParts.join(" ").trim()
      : trimmed;

  if (redirectText.length > 0) {
    await createUndoCheckpointBeforeRedirect();
  }
  cancelStreaming();
  recordUxMetric("interrupt");
  await abortCurrentRunAction({
    api,
    targetRunId: actionableRunId,
    runDrawerOpen,
    runDrawerId,
    reason: mode === "force" ? "chat_interrupt_force" : "chat_interrupt_soft",
    refreshRunDetails,
    refreshSessions,
    refreshSessionTranscript,
    setRunActionBusy,
    setError,
    setNotice,
  });
  if (redirectText.length === 0) {
    clearComposerDraft();
    setNotice(
      mode === "force"
        ? "Force interrupt requested. Any external side effects already emitted remain in the world state."
        : "Interrupt requested. Any external side effects already emitted remain in the world state.",
    );
    return;
  }

  await sendRedirectPrompt(redirectText, { mode, runId: actionableRunId });
  await Promise.all([
    refreshSessions(false),
    refreshSessionTranscript(),
    refreshSlashEntityCatalogs(),
  ]);
}

export async function executeChatSlashCommand(args: {
  command: NonNullable<ParsedSlashCommand>;
  commandBusy: string | null;
  api: ConsoleApiClient;
  activeSessionId: string;
  actionableRunId: string | null;
  checkpoints: readonly ChatCheckpointRecord[];
  objectives: readonly JsonObject[];
  selectedObjective: JsonObject | null;
  authProfiles: readonly AuthProfileView[];
  browserProfiles: readonly BrowserProfileSuggestionRecord[];
  browserSessions: readonly BrowserSessionSuggestionRecord[];
  usageSummary: string;
  openAttachmentPicker: () => void;
  setSearchQuery: (value: string) => void;
  setError: (next: string | null) => void;
  setNotice: (next: string | null) => void;
  setCommandBusy: (next: string | null) => void;
  setConsoleSection: (section: Section) => void;
  recordUxMetric: RecordUxMetric;
  updateComposerDraft: (next: string) => void;
  navigateToObjective: (objectiveId: string) => void;
  inspectCheckpoint: (checkpointId: string) => Promise<void>;
  restoreCheckpoint: (
    checkpointId: string,
    options?: { source?: CheckpointRestoreSource },
  ) => Promise<void>;
  onInterrupt: (raw: string) => Promise<void>;
  onCreateSession: (requestedLabel?: string) => Promise<void>;
  onResetSession: () => Promise<void>;
  onRetry: () => Promise<void>;
  onBranchSession: (requestedLabel?: string) => Promise<void>;
  onQueueFollowUp: (text: string) => Promise<void>;
  onDelegate: (raw: string) => Promise<void>;
  onResumeSession: (rawTarget: string) => Promise<void>;
  onRunCompactionFlow: (mode: "preview" | "apply") => Promise<void>;
  onSearchTranscript: (query: string) => Promise<void>;
  onExportTranscript: (format: "json" | "markdown") => Promise<void>;
  refreshSessionTranscript: () => Promise<void>;
  openBrowserSessionWorkbench: (sessionId: string) => void;
}): Promise<void> {
  const {
    command,
    commandBusy,
    api,
    activeSessionId,
    actionableRunId,
    checkpoints,
    objectives,
    selectedObjective,
    authProfiles,
    browserProfiles,
    browserSessions,
    usageSummary,
    openAttachmentPicker,
    setSearchQuery,
    setError,
    setNotice,
    setCommandBusy,
    setConsoleSection,
    recordUxMetric,
    updateComposerDraft,
    navigateToObjective,
    inspectCheckpoint,
    restoreCheckpoint,
    onInterrupt,
    onCreateSession,
    onResetSession,
    onRetry,
    onBranchSession,
    onQueueFollowUp,
    onDelegate,
    onResumeSession,
    onRunCompactionFlow,
    onSearchTranscript,
    onExportTranscript,
    refreshSessionTranscript,
    openBrowserSessionWorkbench,
  } = args;

  if (commandBusy !== null) {
    setError("Another chat command is already running.");
    recordUxMetric("errors");
    return;
  }
  recordUxMetric("slashCommands");

  switch (command.name) {
    case "help":
      updateComposerDraft("/");
      setNotice("Slash command help is open in the composer.");
      return;
    case "undo":
      await undoLastTurn({
        checkpoints,
        explicitCheckpointId: command.args,
        restoreCheckpoint,
        setError,
        recordUxMetric,
      });
      return;
    case "interrupt":
      await onInterrupt(command.args);
      return;
    case "new":
      await onCreateSession(command.args);
      return;
    case "reset":
      await onResetSession();
      return;
    case "retry":
      await onRetry();
      return;
    case "branch":
      await onBranchSession(command.args);
      return;
    case "queue":
      if (command.args.length === 0) {
        setError("Provide queued text after /queue.");
        recordUxMetric("errors");
        return;
      }
      await onQueueFollowUp(command.args);
      return;
    case "delegate":
      await onDelegate(command.args);
      return;
    case "objective":
      openObjectiveFromCommand({
        rawTarget: command.args,
        objectives,
        selectedObjective,
        navigateToObjective,
        setError,
        setNotice,
        recordUxMetric,
      });
      return;
    case "profile":
      openProfileFromCommand({
        rawTarget: command.args,
        authProfiles,
        setConsoleSection,
        setError,
        setNotice,
        recordUxMetric,
      });
      return;
    case "browser":
      openBrowserFromCommand({
        rawTarget: command.args,
        browserProfiles,
        browserSessions,
        openBrowserSessionWorkbench,
        setConsoleSection,
        setError,
        setNotice,
        recordUxMetric,
      });
      return;
    case "doctor":
      await runDoctorCommand({
        api,
        rawArgs: command.args,
        setConsoleSection,
        setCommandBusy,
        setError,
        setNotice,
        recordUxMetric,
      });
      return;
    case "checkpoint":
      await runCheckpointCommand({
        api,
        activeSessionId,
        checkpoints,
        rawArgs: command.args,
        inspectCheckpoint,
        restoreCheckpoint,
        refreshSessionTranscript,
        setCommandBusy,
        setError,
        setNotice,
        recordUxMetric,
      });
      return;
    case "history":
      setSearchQuery(command.args);
      setNotice(
        command.args.trim().length > 0
          ? `Session history filtered by "${command.args.trim()}".`
          : "Session history filter cleared.",
      );
      return;
    case "resume":
      await onResumeSession(command.args);
      return;
    case "attach":
      if (activeSessionId.trim().length === 0) {
        setError("Select or create a session before attaching files.");
        recordUxMetric("errors");
        return;
      }
      openAttachmentPicker();
      setNotice("Attachment picker opened for the active session.");
      return;
    case "usage":
      setNotice(usageSummary);
      return;
    case "compact":
      await onRunCompactionFlow(parseCompactCommandMode(command.args));
      return;
    case "search":
      if (command.args.length === 0) {
        setError("Provide a search term after /search.");
        recordUxMetric("errors");
        return;
      }
      await onSearchTranscript(command.args);
      return;
    case "export":
      await onExportTranscript(
        command.args.trim().toLowerCase() === "markdown" ||
          command.args.trim().toLowerCase() === "md"
          ? "markdown"
          : "json",
      );
      return;
    default:
      setError("Unsupported slash command.");
      recordUxMetric("errors");
  }
}

async function undoLastTurn(args: {
  checkpoints: readonly ChatCheckpointRecord[];
  explicitCheckpointId: string;
  restoreCheckpoint: (
    checkpointId: string,
    options?: { source?: CheckpointRestoreSource },
  ) => Promise<void>;
  setError: (next: string | null) => void;
  recordUxMetric: RecordUxMetric;
}): Promise<void> {
  const { checkpoints, explicitCheckpointId, restoreCheckpoint, setError, recordUxMetric } = args;
  const resolvedCheckpoint =
    explicitCheckpointId.trim().length > 0
      ? checkpoints.find(
          (checkpoint) => checkpoint.checkpoint_id === explicitCheckpointId.trim(),
        ) ?? null
      : selectUndoCheckpoint(checkpoints);
  if (resolvedCheckpoint === null) {
    setError(
      "No safe undo checkpoint is available yet. Send another turn first or restore a checkpoint explicitly.",
    );
    recordUxMetric("errors");
    return;
  }

  recordUxMetric("undo");
  await restoreCheckpoint(resolvedCheckpoint.checkpoint_id, { source: "undo" });
}

function openObjectiveFromCommand(args: {
  rawTarget: string;
  objectives: readonly JsonObject[];
  selectedObjective: JsonObject | null;
  navigateToObjective: (objectiveId: string) => void;
  setError: (next: string | null) => void;
  setNotice: (next: string | null) => void;
  recordUxMetric: RecordUxMetric;
}): void {
  const {
    rawTarget,
    objectives,
    selectedObjective,
    navigateToObjective,
    setError,
    setNotice,
    recordUxMetric,
  } = args;
  const target = rawTarget.trim().toLowerCase();
  const matchedObjective =
    target.length === 0
      ? selectedObjective
      : objectives.find((objective) => {
          const objectiveId = readString(objective, "objective_id")?.toLowerCase() ?? "";
          const name = readString(objective, "name")?.toLowerCase() ?? "";
          return objectiveId === target || name === target;
        }) ?? null;
  const objectiveId =
    matchedObjective === null ? null : readString(matchedObjective, "objective_id");
  if (objectiveId === null) {
    setError("Objective not found. Refresh objectives or use a suggested objective id.");
    recordUxMetric("errors");
    return;
  }

  navigateToObjective(objectiveId);
  setNotice("Objective overview opened.");
}

function openProfileFromCommand(args: {
  rawTarget: string;
  authProfiles: readonly AuthProfileView[];
  setConsoleSection: (section: Section) => void;
  setError: (next: string | null) => void;
  setNotice: (next: string | null) => void;
  recordUxMetric: RecordUxMetric;
}): void {
  const {
    rawTarget,
    authProfiles,
    setConsoleSection,
    setError,
    setNotice,
    recordUxMetric,
  } = args;
  const target = rawTarget.trim().toLowerCase();
  setConsoleSection("auth");
  if (target.length === 0) {
    setNotice("Auth profiles opened.");
    return;
  }
  const matchedProfile =
    authProfiles.find(
      (profile) =>
        profile.profile_id.toLowerCase() === target ||
        profile.profile_name.toLowerCase() === target,
    ) ?? null;
  if (matchedProfile === null) {
    setError("Auth profile not found in the loaded catalog.");
    recordUxMetric("errors");
    return;
  }
  setNotice(`Auth profiles opened for ${matchedProfile.profile_name}.`);
}

function openBrowserFromCommand(args: {
  rawTarget: string;
  browserProfiles: readonly BrowserProfileSuggestionRecord[];
  browserSessions: readonly BrowserSessionSuggestionRecord[];
  openBrowserSessionWorkbench: (sessionId: string) => void;
  setConsoleSection: (section: Section) => void;
  setError: (next: string | null) => void;
  setNotice: (next: string | null) => void;
  recordUxMetric: RecordUxMetric;
}): void {
  const {
    rawTarget,
    browserProfiles,
    browserSessions,
    openBrowserSessionWorkbench,
    setConsoleSection,
    setError,
    setNotice,
    recordUxMetric,
  } = args;
  const target = rawTarget.trim().toLowerCase();
  if (target.length === 0) {
    setConsoleSection("browser");
    setNotice("Browser profiles opened.");
    return;
  }
  const matchedBrowserSession =
    browserSessions.find((session) => session.session_id.toLowerCase() === target) ?? null;
  if (matchedBrowserSession !== null) {
    openBrowserSessionWorkbench(matchedBrowserSession.session_id);
    setNotice(`Browser workbench opened for ${matchedBrowserSession.session_id}.`);
    return;
  }
  const matchedBrowserProfile =
    browserProfiles.find(
      (profile) =>
        profile.profile_id.toLowerCase() === target || profile.name.toLowerCase() === target,
    ) ?? null;
  if (matchedBrowserProfile !== null) {
    setConsoleSection("browser");
    setNotice(`Browser section opened for ${matchedBrowserProfile.name}.`);
    return;
  }
  setError("Browser profile or session not found in the loaded catalog.");
  recordUxMetric("errors");
}

async function runDoctorCommand(args: {
  api: ConsoleApiClient;
  rawArgs: string;
  setConsoleSection: (section: Section) => void;
  setCommandBusy: (next: string | null) => void;
  setError: (next: string | null) => void;
  setNotice: (next: string | null) => void;
  recordUxMetric: RecordUxMetric;
}): Promise<void> {
  const {
    api,
    rawArgs,
    setConsoleSection,
    setCommandBusy,
    setError,
    setNotice,
    recordUxMetric,
  } = args;
  const normalized = rawArgs.trim().toLowerCase();
  if (normalized === "" || normalized === "jobs") {
    setConsoleSection("operations");
    setNotice("Diagnostics opened with doctor recovery jobs.");
    return;
  }
  if (normalized !== "run" && normalized !== "repair") {
    setError("Usage: /doctor [jobs|run|repair]");
    recordUxMetric("errors");
    return;
  }
  setCommandBusy("doctor");
  setError(null);
  setNotice(null);
  try {
    const response = await api.createDoctorRecoveryJob({
      dry_run: normalized === "run",
      repair: normalized === "repair",
    });
    setConsoleSection("operations");
    setNotice(`Doctor job ${response.job.job_id} queued (${normalized}).`);
  } catch (error) {
    setError(toErrorMessage(error));
    recordUxMetric("errors");
  } finally {
    setCommandBusy(null);
  }
}

async function runCheckpointCommand(args: {
  api: ConsoleApiClient;
  activeSessionId: string;
  checkpoints: readonly ChatCheckpointRecord[];
  rawArgs: string;
  inspectCheckpoint: (checkpointId: string) => Promise<void>;
  restoreCheckpoint: (
    checkpointId: string,
    options?: { source?: CheckpointRestoreSource },
  ) => Promise<void>;
  refreshSessionTranscript: () => Promise<void>;
  setCommandBusy: (next: string | null) => void;
  setError: (next: string | null) => void;
  setNotice: (next: string | null) => void;
  recordUxMetric: RecordUxMetric;
}): Promise<void> {
  const {
    api,
    activeSessionId,
    checkpoints,
    rawArgs,
    inspectCheckpoint,
    restoreCheckpoint,
    refreshSessionTranscript,
    setCommandBusy,
    setError,
    setNotice,
    recordUxMetric,
  } = args;
  const [action = "list", ...rest] = rawArgs
    .trim()
    .split(/\s+/)
    .filter((value) => value.length > 0);
  if (action === "list") {
    if (checkpoints.length === 0) {
      setNotice("No checkpoints recorded for the active session.");
      return;
    }
    const latest = checkpoints
      .slice()
      .sort((left, right) => right.created_at_unix_ms - left.created_at_unix_ms)[0];
    if (latest !== undefined) {
      await inspectCheckpoint(latest.checkpoint_id);
    }
    setNotice(
      `Checkpoint list ready with ${checkpoints.length} checkpoint${checkpoints.length === 1 ? "" : "s"}.`,
    );
    return;
  }
  if (action === "restore") {
    const checkpointId = rest[0]?.trim() ?? "";
    if (checkpointId.length === 0) {
      setError("Usage: /checkpoint restore <checkpoint-id>");
      recordUxMetric("errors");
      return;
    }
    await restoreCheckpoint(checkpointId, { source: "checkpoint" });
    return;
  }
  if (action === "save") {
    const sessionId = activeSessionId.trim();
    const name = rest.join(" ").trim();
    if (sessionId.length === 0) {
      setError("Select a session before saving a checkpoint.");
      recordUxMetric("errors");
      return;
    }
    if (name.length === 0) {
      setError("Usage: /checkpoint save <name>");
      recordUxMetric("errors");
      return;
    }
    setCommandBusy("checkpoint-save");
    try {
      await api.createSessionCheckpoint(sessionId, { name, tags: ["manual"] });
      await refreshSessionTranscript();
      setNotice(`Checkpoint '${name}' saved.`);
    } catch (error) {
      setError(toErrorMessage(error));
      recordUxMetric("errors");
    } finally {
      setCommandBusy(null);
    }
    return;
  }
  setError("Usage: /checkpoint [list|restore <checkpoint-id>|save <name>]");
  recordUxMetric("errors");
}
