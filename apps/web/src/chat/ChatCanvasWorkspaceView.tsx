import { Button, Chip } from "@heroui/react";
import type { ComponentProps } from "react";
import { NextActionCard, ScenarioCard } from "../console/components/guidance/GuidanceCards";
import {
  ActionButton,
  EmptyState,
  InlineNotice,
  KeyValueList,
  PageHeader,
  SectionCard,
  StatusChip,
} from "../console/components/ui";
import type { SessionCanvasDetailEnvelope, SessionCanvasSummary } from "../consoleApi";
import { PrettyJsonBlock, shortId } from "./chatShared";
import { ChatSessionsSidebar } from "./ChatSessionsSidebar";

type ChatCanvasWorkspaceViewProps = {
  readonly canvases: readonly SessionCanvasSummary[];
  readonly canvasesBusy: boolean;
  readonly canvasDetailBusy: boolean;
  readonly pinnedCanvasId: string | null;
  readonly restoringStateVersion: number | null;
  readonly runtimeFrameUrl: string | null;
  readonly selectedCanvas: SessionCanvasDetailEnvelope | null;
  readonly selectedCanvasId: string | null;
  readonly selectedSessionBranchState: string;
  readonly selectedSessionContextFileCount: number;
  readonly selectedSessionFamilyLabel?: string | null;
  readonly selectedSessionLineage: string;
  readonly selectedSessionTitle: string;
  readonly selectedSessionTitleState: string;
  readonly sessionsBusy: boolean;
  readonly sessionsSidebarProps: ComponentProps<typeof ChatSessionsSidebar>;
  readonly onOpenConversation: () => void;
  readonly onOpenSourceRun: () => void;
  readonly onRefresh: () => void;
  readonly onRestoreCanvas: (stateVersion: number) => void;
  readonly onSelectCanvas: (canvasId: string) => void;
  readonly onTogglePinnedCanvas: () => void;
};

export function ChatCanvasWorkspaceView({
  canvases,
  canvasesBusy,
  canvasDetailBusy,
  pinnedCanvasId,
  restoringStateVersion,
  runtimeFrameUrl,
  selectedCanvas,
  selectedCanvasId,
  selectedSessionBranchState,
  selectedSessionContextFileCount,
  selectedSessionFamilyLabel,
  selectedSessionLineage,
  selectedSessionTitle,
  selectedSessionTitleState,
  sessionsBusy,
  sessionsSidebarProps,
  onOpenConversation,
  onOpenSourceRun,
  onRefresh,
  onRestoreCanvas,
  onSelectCanvas,
  onTogglePinnedCanvas,
}: ChatCanvasWorkspaceViewProps) {
  const selectedCanvasSummary = selectedCanvas?.canvas ?? null;
  const selectedCanvasRuntimeStatus = selectedCanvasSummary?.runtime_status ?? "missing";
  const selectedCanvasSourceRunId = selectedCanvasSummary?.reference.source_run_id ?? null;
  const selectedCanvasPinned = selectedCanvasId !== null && selectedCanvasId === pinnedCanvasId;
  const revisions = [...(selectedCanvas?.revisions ?? [])].sort(
    (left, right) => right.state_version - left.state_version,
  );

  return (
    <>
      <PageHeader
        eyebrow="Canvas"
        title={
          selectedCanvasSummary
            ? `Canvas ${shortId(selectedCanvasSummary.canvas_id)}`
            : selectedSessionTitle
        }
        description="Session-linked rich surfaces stay reopenable, inspectable, and reversible without dropping back to raw transcript iframes. Shortcuts: Alt+S search, Alt+R run inspector, Alt+W workspace tab, Alt+C conversation, Alt+A approvals."
        status={
          <>
            <StatusChip tone={toneForCanvasRuntime(selectedCanvasRuntimeStatus)}>
              {selectedCanvasRuntimeStatus}
            </StatusChip>
            <StatusChip tone={selectedCanvasPinned ? "accent" : "default"}>
              {selectedCanvasPinned ? "Pinned canvas" : "Unpinned"}
            </StatusChip>
            <Chip size="sm" variant="secondary">
              {selectedSessionBranchState}
            </Chip>
            <Chip size="sm" variant="secondary">
              {selectedSessionTitleState}
            </Chip>
            <Chip size="sm" variant="secondary">
              {selectedSessionLineage}
            </Chip>
            {selectedSessionFamilyLabel ? (
              <Chip size="sm" variant="secondary">
                {selectedSessionFamilyLabel}
              </Chip>
            ) : null}
            {selectedSessionContextFileCount > 0 ? (
              <Chip size="sm" variant="secondary">
                {selectedSessionContextFileCount} context file
                {selectedSessionContextFileCount === 1 ? "" : "s"}
              </Chip>
            ) : null}
          </>
        }
        actions={
          <div className="workspace-inline-actions">
            <ActionButton
              isDisabled={sessionsBusy || canvasesBusy || canvasDetailBusy}
              type="button"
              variant="secondary"
              onPress={onRefresh}
            >
              {sessionsBusy || canvasesBusy || canvasDetailBusy ? "Refreshing..." : "Refresh"}
            </ActionButton>
            <ActionButton type="button" variant="secondary" onPress={onOpenConversation}>
              Open conversation
            </ActionButton>
            <ActionButton
              isDisabled={selectedCanvasSourceRunId === null}
              type="button"
              variant="secondary"
              onPress={onOpenSourceRun}
            >
              Open source run
            </ActionButton>
            <ActionButton
              isDisabled={selectedCanvasSummary === null}
              type="button"
              onPress={onTogglePinnedCanvas}
            >
              {selectedCanvasPinned ? "Unpin canvas" : "Pin canvas"}
            </ActionButton>
          </div>
        }
      />

      <section className="chat-workspace__layout">
        <SectionCard
          className="chat-panel"
          description="Keep the same session rail while swapping the middle surface from conversation flow to canvas history."
          title="Sessions"
          actions={
            <StatusChip tone={sessionsSidebarProps.selectedSession ? "success" : "warning"}>
              {sessionsSidebarProps.selectedSession ? "Active session" : "No session"}
            </StatusChip>
          }
        >
          <ChatSessionsSidebar {...sessionsSidebarProps} />
        </SectionCard>

        <SectionCard
          className="chat-panel chat-panel--conversation"
          description="The active canvas keeps its runtime frame, persisted state, and provenance on one operator surface."
          title={
            selectedCanvasSummary
              ? `Live canvas ${shortId(selectedCanvasSummary.canvas_id)}`
              : "Canvas surface"
          }
          actions={
            selectedCanvasSummary ? (
              <div className="workspace-inline-actions">
                <StatusChip tone={toneForCanvasRuntime(selectedCanvasSummary.runtime_status)}>
                  {selectedCanvasSummary.runtime_status}
                </StatusChip>
                <Chip size="sm" variant="secondary">
                  State v{selectedCanvasSummary.state_version}
                </Chip>
                <Chip size="sm" variant="secondary">
                  Schema v{selectedCanvasSummary.state_schema_version}
                </Chip>
              </div>
            ) : undefined
          }
        >
          <div className="chat-panel__body">
            {selectedCanvasSummary === null || selectedCanvas === null ? (
              canvases.length === 0 ? (
                <div className="workspace-stack">
                  <NextActionCard
                    ctaLabel="Open conversation"
                    description="Stay in the active session and ask for a richer output. When a run emits a canvas-hosted surface, it becomes reopenable here with history and restore controls."
                    title="No canvas in this session yet"
                    onCta={onOpenConversation}
                  />
                  <ScenarioCard
                    description="Canvas is where Palyra keeps outputs that benefit from a richer visual surface than raw transcript text."
                    title="What usually opens here"
                  >
                    <ul className="console-compact-list">
                      <li>Agent-rendered visual summaries or structured result surfaces.</li>
                      <li>Session-linked states that need reopen, provenance, and rollback.</li>
                      <li>Outputs that should stay inspectable without bloating the transcript.</li>
                    </ul>
                  </ScenarioCard>
                </div>
              ) : (
                <EmptyState
                  compact
                  title={canvasesBusy ? "Loading canvases" : "No canvas selected"}
                  description="Pick a canvas from the session list to hydrate its runtime frame and state history."
                />
              )
            ) : (
              <>
                <div className="workspace-callout">
                  <KeyValueList
                    items={[
                      { label: "Canvas", value: shortId(selectedCanvasSummary.canvas_id) },
                      {
                        label: "Updated",
                        value: formatCanvasTime(selectedCanvasSummary.updated_at_unix_ms),
                      },
                      {
                        label: "Expires",
                        value: formatCanvasTime(selectedCanvasSummary.expires_at_unix_ms),
                      },
                      {
                        label: "Source run",
                        value: selectedCanvasSourceRunId
                          ? shortId(selectedCanvasSourceRunId)
                          : "Not linked",
                      },
                      {
                        label: "Last referenced",
                        value: formatCanvasTime(
                          selectedCanvasSummary.reference.last_referenced_at_unix_ms,
                          "Not referenced yet",
                        ),
                      },
                    ]}
                  />
                </div>

                {selectedCanvas.runtime_error ? (
                  <InlineNotice title="Runtime unavailable" tone="warning">
                    {selectedCanvas.runtime_error}
                  </InlineNotice>
                ) : null}

                {runtimeFrameUrl ? (
                  <iframe
                    className="chat-canvas-frame"
                    title={`Canvas ${selectedCanvasSummary.canvas_id}`}
                    src={runtimeFrameUrl}
                    sandbox="allow-scripts allow-same-origin"
                    loading="lazy"
                    referrerPolicy="no-referrer"
                  />
                ) : (
                  <InlineNotice
                    title={
                      canvasDetailBusy ? "Refreshing canvas runtime" : "Runtime frame not available"
                    }
                    tone="default"
                  >
                    {selectedCanvas.runtime_error ??
                      "The persisted state is still available below even when a live runtime frame cannot be issued."}
                  </InlineNotice>
                )}

                <SectionCard
                  description="Persisted state snapshot used as the restore source of truth."
                  title="Current state"
                  variant="transparent"
                >
                  <PrettyJsonBlock
                    className="chat-detail-panel__payload"
                    revealSensitiveValues
                    value={selectedCanvas.state}
                  />
                </SectionCard>
              </>
            )}
          </div>
        </SectionCard>

        <div className="chat-inspector-column">
          <SectionCard
            description="Session canvases are sorted by the latest persisted update so you can reopen the freshest surface fast."
            title={`Session canvases (${canvases.length})`}
            actions={
              <StatusChip tone={canvases.length > 0 ? "accent" : "default"}>
                {canvasesBusy ? "Refreshing" : `${canvases.length} tracked`}
              </StatusChip>
            }
          >
            <div className="chat-canvas-list">
              {canvases.length === 0 ? (
                <EmptyState
                  compact
                  title="No canvases yet"
                  description="A canvas will appear here once a run emits a canvas-hosted artifact."
                />
              ) : (
                canvases.map((canvas) => {
                  const active = canvas.canvas_id === selectedCanvasId;
                  const pinned = canvas.canvas_id === pinnedCanvasId;
                  return (
                    <Button
                      key={canvas.canvas_id}
                      aria-selected={active}
                      className="chat-canvas-item"
                      fullWidth
                      type="button"
                      variant={active ? "secondary" : "ghost"}
                      onPress={() => onSelectCanvas(canvas.canvas_id)}
                    >
                      <span className="flex w-full flex-col items-start gap-1 text-left">
                        <span className="chat-session-item__title">
                          Canvas {shortId(canvas.canvas_id)}
                        </span>
                        <span className="chat-session-item__meta">
                          <StatusChip tone={toneForCanvasRuntime(canvas.runtime_status)}>
                            {canvas.runtime_status}
                          </StatusChip>
                          <Chip size="sm" variant="secondary">
                            State v{canvas.state_version}
                          </Chip>
                          {pinned ? (
                            <Chip size="sm" variant="secondary">
                              Pinned
                            </Chip>
                          ) : null}
                        </span>
                        <small>
                          Updated {formatCanvasTime(canvas.updated_at_unix_ms)}
                          {" · "}
                          {canvas.reference.source_run_id
                            ? `run ${shortId(canvas.reference.source_run_id)}`
                            : "no source run"}
                        </small>
                      </span>
                    </Button>
                  );
                })
              )}
            </div>
          </SectionCard>

          <SectionCard
            description="Restore any prior state version into a fresh head revision without leaving the canvas surface."
            title={`Revision history (${revisions.length})`}
            actions={
              selectedCanvasSummary ? (
                <StatusChip tone="accent">
                  Current v{selectedCanvasSummary.state_version}
                </StatusChip>
              ) : undefined
            }
          >
            {selectedCanvasSummary === null ? (
              <EmptyState
                compact
                title="No history loaded"
                description="Select a canvas to inspect its persisted revision log."
              />
            ) : revisions.length === 0 ? (
              <EmptyState
                compact
                title="No revisions yet"
                description="This canvas has not recorded any persisted state transitions."
              />
            ) : (
              <div className="chat-canvas-history">
                {revisions.map((revision) => {
                  const isCurrent = revision.state_version === selectedCanvasSummary.state_version;
                  const restoreBusy =
                    restoringStateVersion !== null &&
                    restoringStateVersion === revision.state_version;
                  return (
                    <div key={revision.seq} className="workspace-callout">
                      <div className="workspace-inline-actions">
                        <StatusChip tone={isCurrent ? "success" : "default"}>
                          {isCurrent ? "Current" : "Historical"}
                        </StatusChip>
                        <Chip size="sm" variant="secondary">
                          State v{revision.state_version}
                        </Chip>
                        <Chip size="sm" variant="secondary">
                          Base v{revision.base_state_version}
                        </Chip>
                        {revision.closed ? (
                          <Chip size="sm" variant="secondary">
                            Closed
                          </Chip>
                        ) : null}
                      </div>
                      <p className="chat-muted">
                        Applied {formatCanvasTime(revision.applied_at_unix_ms)} by{" "}
                        <strong>{revision.actor_principal}</strong> on{" "}
                        <code>{revision.actor_device_id}</code>.
                      </p>
                      <ActionButton
                        isDisabled={isCurrent || restoringStateVersion !== null}
                        type="button"
                        variant={isCurrent ? "secondary" : "primary"}
                        onPress={() => onRestoreCanvas(revision.state_version)}
                      >
                        {restoreBusy
                          ? "Restoring..."
                          : isCurrent
                            ? "Current state"
                            : "Restore state"}
                      </ActionButton>
                    </div>
                  );
                })}
              </div>
            )}
          </SectionCard>
        </div>
      </section>
    </>
  );
}

function formatCanvasTime(unixMs: number | null | undefined, fallback = "Unavailable"): string {
  return typeof unixMs === "number" ? new Date(unixMs).toLocaleString() : fallback;
}

function toneForCanvasRuntime(runtimeStatus: string): "default" | "warning" | "success" {
  switch (runtimeStatus.trim().toLowerCase()) {
    case "ready":
      return "success";
    case "expired":
    case "closed":
      return "warning";
    default:
      return "default";
  }
}
