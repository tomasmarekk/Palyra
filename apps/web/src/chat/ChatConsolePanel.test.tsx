// @vitest-environment jsdom

import type { ComponentProps } from "react";
import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vite-plus/test";
import type { ConsoleApiClient } from "../consoleApi";

import { ChatComposer } from "./ChatComposer";
import { ChatConsoleWorkspaceView } from "./ChatConsoleWorkspaceView";
import { ChatTranscript } from "./ChatTranscript";
import {
  buildContextBudgetSummary,
  DEFAULT_APPROVAL_SCOPE,
  DEFAULT_APPROVAL_TTL_MS,
  parseSlashCommand,
  type ComposerAttachment,
  type TranscriptEntry,
} from "./chatShared";

afterEach(() => {
  cleanup();
});

describe("Chat web UX primitives", () => {
  it("shows slash command help, attachment preview, budget warning, and drag-drop upload in the composer", () => {
    const attachFiles = vi.fn();
    const removeAttachment = vi.fn();
    const queueFollowUp = vi.fn();
    const attachment = sampleAttachment();

    render(
      <ChatComposer
        composerText="/queue Inspect backlog after deploy"
        setComposerText={vi.fn()}
        streaming={false}
        activeSessionId="01ARZ3NDEKTSV4RRFFQ69G5FAV"
        attachments={[attachment]}
        attachmentBusy={false}
        canQueueFollowUp
        submitMessage={vi.fn()}
        retryLast={vi.fn()}
        branchSession={vi.fn()}
        queueFollowUp={queueFollowUp}
        cancelStreaming={vi.fn()}
        clearTranscript={vi.fn()}
        openAttachmentPicker={vi.fn()}
        removeAttachment={removeAttachment}
        attachFiles={attachFiles}
        showSlashPalette
        parsedSlashCommand={parseSlashCommand("/queue Inspect backlog after deploy")}
        slashCommandMatches={[
          {
            name: "queue",
            synopsis: "/queue <text>",
            description: "Queue a follow-up message for the active run.",
            example: "/queue Inspect backlog after deploy",
            category: "run",
            execution: "server",
            surfaces: ["web", "tui"],
            aliases: [],
            capability_tags: ["run", "queue"],
            entity_targets: ["run"],
            keywords: ["queue", "follow-up", "run"],
          },
        ]}
        slashSuggestions={[
          {
            id: "queue:suggestion",
            kind: "entity",
            commandName: "queue",
            title: "/queue <text>",
            subtitle: "Queue a follow-up message for the active run.",
            detail: "/queue Inspect backlog after deploy",
            example: "/queue Inspect backlog after deploy",
            replacement: "/queue Inspect backlog after deploy",
            badge: "run",
          },
        ]}
        selectedSlashSuggestionIndex={0}
        setSelectedSlashSuggestionIndex={vi.fn()}
        dismissSlashPalette={vi.fn()}
        acceptSlashSuggestion={vi.fn()}
        uxMetrics={{
          slashCommands: 0,
          paletteAccepts: 0,
          keyboardAccepts: 0,
          undo: 0,
          interrupt: 0,
          errors: 0,
        }}
        contextBudget={buildContextBudgetSummary({
          baseline_tokens: 14_500,
          draft_text: "Inspect backlog after deploy and summarize the risky items.",
          attachments: [attachment],
        })}
        projectContextPreview={null}
        projectContextPreviewBusy={false}
        projectContextPreviewStale={false}
        projectContextPromptPreview={null}
        refreshProjectContextPreview={vi.fn()}
        contextReferencePreview={null}
        contextReferencePreviewBusy={false}
        contextReferencePreviewStale={false}
        refreshContextReferencePreview={vi.fn()}
        removeContextReference={vi.fn()}
        recallPreview={null}
        recallPreviewBusy={false}
        recallPreviewStale={false}
        refreshRecallPreview={vi.fn()}
      />,
    );

    expect(screen.getByText("Operator shortcuts")).toBeInTheDocument();
    expect(screen.getAllByText("/queue <text>").length).toBeGreaterThan(0);
    expect(screen.getByText("screen.png")).toBeInTheDocument();
    expect(screen.getByText(/approaching the budget/i)).toBeInTheDocument();

    fireEvent.click(screen.getByRole("button", { name: "Queue follow-up" }));
    expect(queueFollowUp).toHaveBeenCalledTimes(1);

    fireEvent.drop(screen.getByLabelText("Message"), {
      dataTransfer: {
        files: [new File(["payload"], "drop.txt", { type: "text/plain" })],
      },
    });
    expect(attachFiles).toHaveBeenCalledTimes(1);

    fireEvent.click(screen.getByRole("button", { name: "Remove" }));
    expect(removeAttachment).toHaveBeenCalledWith(attachment.local_id);
  });

  it("supports keyboard-only slash palette navigation and dismissal", () => {
    const setSelectedSlashSuggestionIndex = vi.fn();
    const dismissSlashPalette = vi.fn();
    const acceptSlashSuggestion = vi.fn();

    render(
      <ChatComposer
        {...baseComposerProps()}
        composerText="/br"
        showSlashPalette
        parsedSlashCommand={parseSlashCommand("/br")}
        slashCommandMatches={[
          {
            name: "branch",
            synopsis: "/branch [label]",
            description: "Create a child session from the current conversation state.",
            example: "/branch investigate-rollout",
            category: "session",
            execution: "server",
            surfaces: ["web", "tui"],
            aliases: [],
            capability_tags: ["session", "branch", "lineage"],
            entity_targets: ["session", "run"],
            keywords: ["branch", "lineage"],
          },
        ]}
        slashSuggestions={[
          {
            id: "branch:session-1",
            kind: "entity",
            commandName: "branch",
            title: "/branch [label]",
            subtitle: "Create a child session from the current conversation state.",
            detail: "Fork from session session-1",
            example: "/branch investigate-rollout",
            replacement: "/branch investigate-rollout",
            badge: "session",
          },
          {
            id: "branch:session-2",
            kind: "entity",
            commandName: "branch",
            title: "/branch [label]",
            subtitle: "Create a child session from the current conversation state.",
            detail: "Fork from session session-2",
            example: "/branch document-follow-up",
            replacement: "/branch document-follow-up",
            badge: "session",
          },
        ]}
        selectedSlashSuggestionIndex={0}
        setSelectedSlashSuggestionIndex={setSelectedSlashSuggestionIndex}
        dismissSlashPalette={dismissSlashPalette}
        acceptSlashSuggestion={acceptSlashSuggestion}
      />,
    );

    expect(screen.getByRole("listbox", { name: "Slash commands" })).toBeInTheDocument();
    expect(screen.getAllByRole("option")).toHaveLength(2);

    fireEvent.keyDown(screen.getByLabelText("Message"), { key: "ArrowDown" });
    expect(setSelectedSlashSuggestionIndex).toHaveBeenCalledWith(1);

    fireEvent.keyDown(screen.getByLabelText("Message"), { key: "Tab" });
    expect(acceptSlashSuggestion).toHaveBeenCalledWith("/branch investigate-rollout", true);

    fireEvent.keyDown(screen.getByLabelText("Message"), { key: "Escape" });
    expect(dismissSlashPalette).toHaveBeenCalledTimes(1);
  });

  it("moves payload details out of the main transcript and into the inspect callback", () => {
    const inspectPayload = vi.fn();
    const payloadEntry = sampleToolEntry();

    render(
      <ChatTranscript
        visibleTranscript={[payloadEntry]}
        hiddenTranscriptItems={0}
        transcriptBoxRef={{ current: null }}
        approvalDrafts={{}}
        a2uiDocuments={{}}
        selectedDetailId={null}
        updateApprovalDraft={vi.fn()}
        decideInlineApproval={vi.fn()}
        openRunDetails={vi.fn()}
        inspectPayload={inspectPayload}
      />,
    );

    expect(screen.getByText("Payload moved to side panel")).toBeInTheDocument();
    expect(screen.queryByText(/token":"secret/i)).not.toBeInTheDocument();

    fireEvent.click(screen.getByRole("button", { name: "Inspect payload" }));
    expect(inspectPayload).toHaveBeenCalledWith(payloadEntry);
  });

  it("renders inline approval controls and forwards approval decisions", () => {
    const updateApprovalDraft = vi.fn();
    const decideInlineApproval = vi.fn();

    render(
      <ChatTranscript
        visibleTranscript={[
          {
            id: "approval-1",
            kind: "approval_request",
            created_at_unix_ms: 100,
            run_id: "01ARZ3NDEKTSV4RRFFQ69G5FAX",
            title: "Approval request: palyra.fs.apply_patch",
            text: "Needs approval",
            approval_id: "A1",
            proposal_id: "P1",
            tool_name: "palyra.fs.apply_patch",
          },
        ]}
        hiddenTranscriptItems={0}
        transcriptBoxRef={{ current: null }}
        approvalDrafts={{
          A1: {
            scope: DEFAULT_APPROVAL_SCOPE,
            reason: "",
            ttl_ms: DEFAULT_APPROVAL_TTL_MS,
            busy: false,
          },
        }}
        a2uiDocuments={{}}
        selectedDetailId={null}
        updateApprovalDraft={updateApprovalDraft}
        decideInlineApproval={decideInlineApproval}
        openRunDetails={vi.fn()}
        inspectPayload={vi.fn()}
      />,
    );

    expect(screen.getByText("Approval required")).toBeInTheDocument();
    expect(screen.getByText("Needs approval")).toBeInTheDocument();

    fireEvent.click(screen.getByRole("button", { name: "Approve" }));
    expect(decideInlineApproval).toHaveBeenCalledWith("A1", true);
  });

  it("renders escaped transcript text and keeps canvas iframes sandboxed", async () => {
    render(
      <ChatTranscript
        visibleTranscript={[
          {
            id: "assistant-1",
            kind: "assistant",
            created_at_unix_ms: 100,
            run_id: "01ARZ3NDEKTSV4RRFFQ69G5FAX",
            title: "Assistant",
            text: "<img src='x' onerror='alert(1)'>",
          },
          {
            id: "canvas-1",
            kind: "canvas",
            created_at_unix_ms: 101,
            run_id: "01ARZ3NDEKTSV4RRFFQ69G5FAX",
            title: "Canvas",
            canvas_url: "/canvas/v1/frame/01ARZ3NDEKTSV4RRFFQ69G5FB1?token=test-token",
            text: "/canvas/v1/frame/01ARZ3NDEKTSV4RRFFQ69G5FB1?token=test-token",
          },
        ]}
        hiddenTranscriptItems={0}
        transcriptBoxRef={{ current: null }}
        approvalDrafts={{}}
        a2uiDocuments={{}}
        selectedDetailId={null}
        updateApprovalDraft={vi.fn()}
        decideInlineApproval={vi.fn()}
        openRunDetails={vi.fn()}
        inspectPayload={vi.fn()}
      />,
    );

    expect(screen.getByText("<img src='x' onerror='alert(1)'>")).toBeInTheDocument();
    expect(document.body.textContent ?? "").toContain("alert(1)");
    expect(document.querySelector("img[src='x']")).toBeNull();

    const frame = await screen.findByTitle("Canvas 01ARZ3NDEKTSV4RRFFQ69G5FAX");
    expect(frame).toHaveAttribute("sandbox", "allow-scripts allow-same-origin");
    expect(frame).toHaveAttribute(
      "src",
      "/canvas/v1/frame/01ARZ3NDEKTSV4RRFFQ69G5FB1?token=test-token",
    );
    expect(document.querySelector("iframe[src='/console/v1/diagnostics?token=evil']")).toBeNull();
  });

  it("renders derived attachment actions and forwards lifecycle callbacks", () => {
    const inspectDerivedArtifact = vi.fn();
    const runDerivedArtifactAction = vi.fn();

    render(
      <ChatTranscript
        visibleTranscript={[]}
        sessionAttachments={[
          {
            artifact_id: "01ARZ3NDEKTSV4RRFFQ69G5FAA",
            attachment_id: "att-1",
            filename: "meeting-notes.txt",
            declared_content_type: "text/plain",
            content_hash: "sha256:abc",
            size_bytes: 2_048,
            kind: "file",
            budget_tokens: 512,
          },
        ]}
        sessionDerivedArtifacts={[
          {
            derived_artifact_id: "01ARZ3NDEKTSV4RRFFQ69G5FZZ",
            source_artifact_id: "01ARZ3NDEKTSV4RRFFQ69G5FAA",
            attachment_id: "att-1",
            session_id: "01ARZ3NDEKTSV4RRFFQ69G5FAV",
            principal: "operator",
            device_id: "device-1",
            channel: "discord:channel:test",
            filename: "meeting-notes.txt",
            declared_content_type: "text/plain",
            kind: "extracted_text",
            state: "succeeded",
            parser_name: "attachment-document-extractor",
            parser_version: "1",
            source_content_hash: "sha256:abc",
            content_hash: "sha256:def",
            content_text: "Structured meeting notes",
            summary_text: "Structured meeting notes",
            language: "en",
            duration_ms: undefined,
            processing_ms: 12,
            warnings: [],
            anchors: [],
            failure_reason: undefined,
            quarantine_reason: undefined,
            workspace_document_id: undefined,
            memory_item_id: undefined,
            background_task_id: "01ARZ3NDEKTSV4RRFFQ69G5FXY",
            recompute_required: false,
            orphaned: false,
            created_at_unix_ms: 100,
            updated_at_unix_ms: 120,
            purged_at_unix_ms: undefined,
          },
        ]}
        hiddenTranscriptItems={0}
        transcriptBoxRef={{ current: null }}
        approvalDrafts={{}}
        a2uiDocuments={{}}
        selectedDetailId={null}
        updateApprovalDraft={vi.fn()}
        decideInlineApproval={vi.fn()}
        openRunDetails={vi.fn()}
        inspectPayload={vi.fn()}
        inspectDerivedArtifact={inspectDerivedArtifact}
        runDerivedArtifactAction={runDerivedArtifactAction}
      />,
    );

    expect(screen.getByText("meeting-notes.txt")).toBeInTheDocument();
    expect(screen.getByText("extracted_text · succeeded")).toBeInTheDocument();

    fireEvent.click(screen.getByRole("button", { name: "Open extracted_text" }));
    expect(inspectDerivedArtifact).toHaveBeenCalledWith("01ARZ3NDEKTSV4RRFFQ69G5FZZ");

    fireEvent.click(screen.getByRole("button", { name: "Recompute" }));
    expect(runDerivedArtifactAction).toHaveBeenCalledWith(
      "01ARZ3NDEKTSV4RRFFQ69G5FZZ",
      "recompute",
    );

    fireEvent.click(screen.getByRole("button", { name: "Quarantine" }));
    expect(runDerivedArtifactAction).toHaveBeenCalledWith(
      "01ARZ3NDEKTSV4RRFFQ69G5FZZ",
      "quarantine",
    );

    fireEvent.click(screen.getByRole("button", { name: "Purge" }));
    expect(runDerivedArtifactAction).toHaveBeenCalledWith("01ARZ3NDEKTSV4RRFFQ69G5FZZ", "purge");
  });

  it("surfaces linked objective navigation in the chat workspace header", () => {
    const onOpenObjective = vi.fn();

    render(
      <ChatConsoleWorkspaceView
        allowSensitiveTools={false}
        canAbortRun={false}
        canInspectRun={false}
        composerProps={baseComposerProps()}
        contextBudget={buildContextBudgetSummary({
          baseline_tokens: 900,
          draft_text: "Keep the deployment handoff current.",
          attachments: [],
        })}
        inspectorProps={baseInspectorProps()}
        onAbortRun={vi.fn()}
        onOpenObjective={onOpenObjective}
        onOpenRunDetails={vi.fn()}
        onRefresh={vi.fn()}
        onHideStarterPrompts={vi.fn()}
        onSetAllowSensitiveTools={vi.fn()}
        onShowStarterPrompts={vi.fn()}
        onUseStarterPrompt={vi.fn()}
        pendingApprovalCount={0}
        runActionBusy={false}
        selectedObjectiveFocus="Keep the deployment handoff current."
        selectedObjectiveLabel="heartbeat · Daily status"
        selectedSessionBranchState="linear"
        selectedSessionContextFileCount={0}
        selectedSessionFamilyLabel={null}
        selectedSessionLineage="Root session"
        selectedSessionTitle="Operator workspace"
        selectedSessionTitleState="Auto title ready"
        sessionQuickControlHeaderProps={{
          session: null,
          busy: false,
          onToggleThinking: vi.fn(),
          onToggleTrace: vi.fn(),
          onToggleVerbose: vi.fn(),
          onReset: vi.fn(),
        }}
        sessionsBusy={false}
        sessionsSidebarProps={baseSessionsSidebarProps()}
        showStarterPrompts={false}
        starterPromptsHidden={false}
        starterPrompts={[]}
        streaming={false}
        toolPayloadCount={0}
        transcriptBusy={false}
        transcriptProps={baseTranscriptProps()}
      />,
    );

    expect(screen.getByText("heartbeat · Daily status")).toBeInTheDocument();
    expect(screen.getByText("Keep the deployment handoff current.")).toBeInTheDocument();

    fireEvent.click(screen.getByRole("button", { name: "Open objective" }));
    expect(onOpenObjective).toHaveBeenCalledTimes(1);
  });
});

function sampleAttachment(): ComposerAttachment {
  return {
    local_id: "local-1",
    artifact_id: "01ARZ3NDEKTSV4RRFFQ69G5FAA",
    attachment_id: "att-1",
    filename: "screen.png",
    declared_content_type: "image/png",
    content_hash: "sha256:abc",
    size_bytes: 2_048,
    kind: "image",
    budget_tokens: 640,
    preview_url: "data:image/png;base64,AA==",
  };
}

function sampleToolEntry(): TranscriptEntry {
  return {
    id: "tool-1",
    kind: "tool",
    created_at_unix_ms: 100,
    run_id: "01ARZ3NDEKTSV4RRFFQ69G5FAB",
    title: "Tool result",
    text: "The tool completed successfully.",
    payload: {
      token: "secret",
      status: "ok",
    },
  };
}

function baseComposerProps(): ComponentProps<typeof ChatComposer> {
  return {
    composerText: "",
    setComposerText: vi.fn(),
    streaming: false,
    activeSessionId: "session-1",
    attachments: [],
    attachmentBusy: false,
    canQueueFollowUp: false,
    submitMessage: vi.fn(),
    retryLast: vi.fn(),
    branchSession: vi.fn(),
    queueFollowUp: vi.fn(),
    cancelStreaming: vi.fn(),
    clearTranscript: vi.fn(),
    openAttachmentPicker: vi.fn(),
    removeAttachment: vi.fn(),
    attachFiles: vi.fn(),
    showSlashPalette: false,
    parsedSlashCommand: null,
    slashCommandMatches: [],
    slashSuggestions: [],
    selectedSlashSuggestionIndex: 0,
    setSelectedSlashSuggestionIndex: vi.fn(),
    dismissSlashPalette: vi.fn(),
    acceptSlashSuggestion: vi.fn(),
    uxMetrics: {
      slashCommands: 0,
      paletteAccepts: 0,
      keyboardAccepts: 0,
      undo: 0,
      interrupt: 0,
      errors: 0,
    },
    contextBudget: buildContextBudgetSummary({
      baseline_tokens: 900,
      draft_text: "",
      attachments: [],
    }),
    projectContextPreview: null,
    projectContextPreviewBusy: false,
    projectContextPreviewStale: false,
    projectContextPromptPreview: null,
    refreshProjectContextPreview: vi.fn(),
    contextReferencePreview: null,
    contextReferencePreviewBusy: false,
    contextReferencePreviewStale: false,
    refreshContextReferencePreview: vi.fn(),
    removeContextReference: vi.fn(),
    recallPreview: null,
    recallPreviewBusy: false,
    recallPreviewStale: false,
    refreshRecallPreview: vi.fn(),
  };
}

function baseInspectorProps() {
  return {
    pendingApprovalCount: 0,
    a2uiSurfaces: [],
    runIds: [],
    selectedSession: null,
    selectedSessionLineage: "Root session",
    sessionQuickControlPanelProps: {
      session: null,
      agents: [],
      busy: false,
      modelDraft: "",
      setModelDraft: vi.fn(),
      onSelectAgent: vi.fn(),
      onApplyModel: vi.fn(),
      onClearModel: vi.fn(),
      onToggleThinking: vi.fn(),
      onToggleTrace: vi.fn(),
      onToggleVerbose: vi.fn(),
      onReset: vi.fn(),
    },
    contextBudgetEstimatedTokens: 900,
    projectContextBusy: false,
    refreshProjectContext: vi.fn(),
    disableProjectContextEntry: vi.fn(),
    enableProjectContextEntry: vi.fn(),
    approveProjectContextEntry: vi.fn(),
    scaffoldProjectContext: vi.fn(),
    transcriptBusy: false,
    transcriptSearchQuery: "",
    setTranscriptSearchQuery: vi.fn(),
    transcriptSearchBusy: false,
    canSearchTranscript: false,
    pinnedRecordKeys: new Set<string>(),
    searchResults: [],
    searchTranscript: vi.fn(),
    inspectSearchMatch: vi.fn(),
    exportBusy: null,
    exportTranscript: vi.fn(),
    recentTranscriptRecords: [],
    inspectTranscriptRecord: vi.fn(),
    pinTranscriptRecord: vi.fn(),
    sessionPins: [],
    deletePin: vi.fn(),
    compactions: [],
    inspectCompaction: vi.fn(),
    checkpoints: [],
    inspectCheckpoint: vi.fn(),
    restoreCheckpoint: vi.fn(),
    queuedInputs: [],
    backgroundTasks: [],
    inspectBackgroundTask: vi.fn(),
    runBackgroundTaskAction: vi.fn(),
    detailPanel: null,
    revealSensitiveValues: false,
    inspectorVisible: false,
    openRunDetails: vi.fn(),
    phase4BusyKey: null,
    runDrawerId: "",
    setRunDrawerId: vi.fn(),
    runDrawerBusy: false,
    runStatus: null,
    runTape: null,
    runLineage: null,
    runDrawerTab: "status" as const,
    setRunDrawerTab: vi.fn(),
    api: {} as ConsoleApiClient,
    setError: vi.fn(),
    setNotice: vi.fn(),
    onWorkspaceRestore: vi.fn(),
    openMemorySection: vi.fn(),
    openSupportSection: vi.fn(),
    refreshRunDetails: vi.fn(),
    closeRunDrawer: vi.fn(),
    openBrowserSessionWorkbench: vi.fn(),
  };
}

function baseSessionsSidebarProps() {
  return {
    sessionsBusy: false,
    newSessionLabel: "",
    setNewSessionLabel: vi.fn(),
    searchQuery: "",
    setSearchQuery: vi.fn(),
    includeArchived: false,
    setIncludeArchived: vi.fn(),
    sessionLabelDraft: "",
    setSessionLabelDraft: vi.fn(),
    selectedSession: null,
    sortedSessions: [],
    activeSessionId: "",
    setActiveSessionId: vi.fn(),
    createSession: vi.fn(),
    renameSession: vi.fn(),
    resetSession: vi.fn(),
    archiveSession: vi.fn(),
  };
}

function baseTranscriptProps(): ComponentProps<typeof ChatTranscript> {
  return {
    visibleTranscript: [],
    sessionAttachments: [],
    sessionDerivedArtifacts: [],
    hiddenTranscriptItems: 0,
    transcriptBoxRef: { current: null },
    approvalDrafts: {},
    a2uiDocuments: {},
    selectedDetailId: null,
    updateApprovalDraft: vi.fn(),
    decideInlineApproval: vi.fn(),
    openRunDetails: vi.fn(),
    inspectPayload: vi.fn(),
  };
}
