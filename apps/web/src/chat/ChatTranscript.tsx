import { ActionButton, EmptyState, StatusChip } from "../console/components/ui";
import { A2uiRenderer, type A2uiDocument } from "../a2ui";
import type { ChatAttachmentRecord, MediaDerivedArtifactRecord } from "../consoleApi";

import { ApprovalRequestControls, type ApprovalDraft, type TranscriptEntry } from "./chatShared";

type ChatTranscriptProps = {
  visibleTranscript: TranscriptEntry[];
  sessionAttachments?: ChatAttachmentRecord[];
  sessionDerivedArtifacts?: MediaDerivedArtifactRecord[];
  hiddenTranscriptItems: number;
  transcriptBoxRef: React.RefObject<HTMLDivElement | null>;
  approvalDrafts: Record<string, ApprovalDraft>;
  a2uiDocuments: Record<string, A2uiDocument>;
  selectedDetailId: string | null;
  updateApprovalDraft: (approvalId: string, next: ApprovalDraft) => void;
  decideInlineApproval: (approvalId: string, approved: boolean) => void;
  openRunDetails: (runId: string) => void;
  inspectPayload: (entry: TranscriptEntry) => void;
  inspectDerivedArtifact?: (derivedArtifactId: string) => void;
  runDerivedArtifactAction?: (
    derivedArtifactId: string,
    action: "recompute" | "quarantine" | "release" | "purge",
  ) => void;
};

export function ChatTranscript({
  visibleTranscript,
  sessionAttachments = [],
  sessionDerivedArtifacts = [],
  hiddenTranscriptItems,
  transcriptBoxRef,
  approvalDrafts,
  a2uiDocuments,
  selectedDetailId,
  updateApprovalDraft,
  decideInlineApproval,
  openRunDetails,
  inspectPayload,
  inspectDerivedArtifact = () => undefined,
  runDerivedArtifactAction = () => undefined,
}: ChatTranscriptProps) {
  const derivedBySource = groupDerivedArtifactsBySource(sessionDerivedArtifacts);
  return (
    <>
      {hiddenTranscriptItems > 0 && (
        <p className="chat-muted">
          Showing latest 120 items. {hiddenTranscriptItems} older items are retained but not
          rendered.
        </p>
      )}

      {sessionAttachments.length > 0 ? (
        <div className="chat-attachment-list" aria-label="Session attachments">
          {sessionAttachments.map((attachment) => {
            const derivedArtifacts = derivedBySource.get(attachment.artifact_id) ?? [];
            return (
              <article key={attachment.artifact_id} className="chat-attachment-card">
                <div className="chat-attachment-card__icon">{attachment.kind}</div>
                <div className="chat-attachment-card__copy">
                  <strong>{attachment.filename}</strong>
                  <span>
                    {attachment.kind} · {attachment.size_bytes.toLocaleString()} bytes ·{" "}
                    {attachment.declared_content_type}
                  </span>
                  {derivedArtifacts.length > 0 ? (
                    <div className="workspace-inline-actions">
                      {derivedArtifacts.map((derivedArtifact) => (
                        <StatusChip
                          key={derivedArtifact.derived_artifact_id}
                          tone={toneForDerivedArtifactState(derivedArtifact.state)}
                        >
                          {derivedArtifact.kind} · {derivedArtifact.state}
                        </StatusChip>
                      ))}
                    </div>
                  ) : (
                    <p className="chat-muted">Derived output not available yet.</p>
                  )}
                </div>
                <div className="chat-entry-actions">
                  {derivedArtifacts.map((derivedArtifact) => (
                    <div key={derivedArtifact.derived_artifact_id} className="workspace-inline-actions">
                      <ActionButton
                        size="sm"
                        type="button"
                        variant="secondary"
                        onPress={() => inspectDerivedArtifact(derivedArtifact.derived_artifact_id)}
                      >
                        Open {derivedArtifact.kind}
                      </ActionButton>
                      <ActionButton
                        size="sm"
                        type="button"
                        variant="ghost"
                        onPress={() =>
                          runDerivedArtifactAction(derivedArtifact.derived_artifact_id, "recompute")
                        }
                      >
                        Recompute
                      </ActionButton>
                      {derivedArtifact.state === "quarantined" ? (
                        <ActionButton
                          size="sm"
                          type="button"
                          variant="ghost"
                          onPress={() =>
                            runDerivedArtifactAction(derivedArtifact.derived_artifact_id, "release")
                          }
                        >
                          Release
                        </ActionButton>
                      ) : (
                        <ActionButton
                          size="sm"
                          type="button"
                          variant="ghost"
                          onPress={() =>
                            runDerivedArtifactAction(
                              derivedArtifact.derived_artifact_id,
                              "quarantine",
                            )
                          }
                        >
                          Quarantine
                        </ActionButton>
                      )}
                      <ActionButton
                        size="sm"
                        type="button"
                        variant="danger"
                        onPress={() =>
                          runDerivedArtifactAction(derivedArtifact.derived_artifact_id, "purge")
                        }
                      >
                        Purge
                      </ActionButton>
                    </div>
                  ))}
                </div>
              </article>
            );
          })}
        </div>
      ) : null}

      <div className="chat-transcript" ref={transcriptBoxRef} role="log" aria-live="polite">
        {visibleTranscript.length === 0 ? (
          <div className="chat-transcript__empty">
            <EmptyState
              compact
              description="Create or select a session, then send the first operator message to begin streaming output."
              title="No transcript yet"
            />
          </div>
        ) : (
          visibleTranscript.map((entry) => {
            const hasPayload = entry.payload !== undefined;
            const payloadSelected = selectedDetailId === entry.id;

            return (
              <article key={entry.id} className={`chat-entry chat-entry--${entry.kind}`}>
                <header className="chat-entry-header">
                  <strong>{entry.title}</strong>
                  <span>{new Date(entry.created_at_unix_ms).toLocaleTimeString()}</span>
                </header>

                {entry.text !== undefined && <p className="chat-entry-text">{entry.text}</p>}

                {entry.attachments !== undefined && entry.attachments.length > 0 ? (
                  <div className="chat-entry__attachment-list">
                    {entry.attachments.map((attachment) => (
                      <div key={attachment.id} className="chat-entry__attachment-pill">
                        <span>{attachment.filename}</span>
                        <small>
                          {attachment.kind} · {attachment.size_bytes.toLocaleString()} bytes
                        </small>
                      </div>
                    ))}
                  </div>
                ) : null}

                {entry.kind === "approval_request" && entry.approval_id !== undefined && (
                  <ApprovalRequestControls
                    approvalId={entry.approval_id}
                    draft={approvalDrafts[entry.approval_id]}
                    onDraftChange={(next) => updateApprovalDraft(entry.approval_id as string, next)}
                    onDecision={(approved) =>
                      decideInlineApproval(entry.approval_id as string, approved)
                    }
                  />
                )}

                {entry.kind === "a2ui" &&
                  entry.surface !== undefined &&
                  a2uiDocuments[entry.surface] !== undefined && (
                    <div className="chat-a2ui-shell">
                      <A2uiRenderer document={a2uiDocuments[entry.surface]} />
                    </div>
                  )}

                {entry.kind === "canvas" && entry.canvas_url !== undefined && (
                  <iframe
                    className="chat-canvas-frame"
                    title={`Canvas ${entry.run_id ?? ""}`}
                    src={entry.canvas_url}
                    sandbox="allow-scripts allow-same-origin"
                    loading="lazy"
                    referrerPolicy="no-referrer"
                  />
                )}

                {hasPayload ? (
                  <div className="chat-entry__detail-callout">
                    <div>
                      <strong>Payload moved to side panel</strong>
                      <p className="chat-muted">
                        Keep the main transcript readable and inspect raw tool payloads only on
                        demand.
                      </p>
                    </div>
                    <StatusChip tone={payloadSelected ? "accent" : "default"}>
                      {payloadSelected ? "Inspecting" : "Available"}
                    </StatusChip>
                  </div>
                ) : null}

                {entry.run_id !== undefined || hasPayload ? (
                  <div className="chat-entry-actions">
                    {hasPayload ? (
                      <ActionButton
                        size="sm"
                        type="button"
                        variant={payloadSelected ? "primary" : "secondary"}
                        onPress={() => inspectPayload(entry)}
                      >
                        {payloadSelected ? "Inspecting payload" : "Inspect payload"}
                      </ActionButton>
                    ) : null}
                    {entry.run_id !== undefined ? (
                      <ActionButton
                        size="sm"
                        type="button"
                        variant="secondary"
                        onPress={() => openRunDetails(entry.run_id as string)}
                      >
                        Open run details
                      </ActionButton>
                    ) : null}
                  </div>
                ) : null}
              </article>
            );
          })
        )}
      </div>
    </>
  );
}

function groupDerivedArtifactsBySource(
  derivedArtifacts: readonly MediaDerivedArtifactRecord[],
): Map<string, MediaDerivedArtifactRecord[]> {
  const grouped = new Map<string, MediaDerivedArtifactRecord[]>();
  for (const derivedArtifact of derivedArtifacts) {
    const existing = grouped.get(derivedArtifact.source_artifact_id) ?? [];
    existing.push(derivedArtifact);
    grouped.set(derivedArtifact.source_artifact_id, existing);
  }
  return grouped;
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
