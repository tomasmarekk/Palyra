import { A2uiRenderer, type A2uiDocument } from "../a2ui";

import {
  ApprovalRequestControls,
  PrettyJsonBlock,
  type ApprovalDraft,
  type TranscriptEntry
} from "./chatShared";

type ChatTranscriptProps = {
  visibleTranscript: TranscriptEntry[];
  hiddenTranscriptItems: number;
  transcriptBoxRef: React.RefObject<HTMLDivElement | null>;
  approvalDrafts: Record<string, ApprovalDraft>;
  a2uiDocuments: Record<string, A2uiDocument>;
  revealSensitiveValues: boolean;
  updateApprovalDraft: (approvalId: string, next: ApprovalDraft) => void;
  decideInlineApproval: (approvalId: string, approved: boolean) => void;
  openRunDetails: (runId: string) => void;
};

export function ChatTranscript({
  visibleTranscript,
  hiddenTranscriptItems,
  transcriptBoxRef,
  approvalDrafts,
  a2uiDocuments,
  revealSensitiveValues,
  updateApprovalDraft,
  decideInlineApproval,
  openRunDetails
}: ChatTranscriptProps) {
  return (
    <>
      {hiddenTranscriptItems > 0 && (
        <p className="chat-muted">
          Showing latest 120 items. {hiddenTranscriptItems} older items are retained but not rendered.
        </p>
      )}

      <div className="chat-transcript" ref={transcriptBoxRef} role="log" aria-live="polite">
        {visibleTranscript.length === 0 ? (
          <div className="chat-transcript__empty">
            <p>Create or select a session, then send the first operator message to begin streaming output.</p>
          </div>
        ) : (
          visibleTranscript.map((entry) => (
            <article key={entry.id} className={`chat-entry chat-entry--${entry.kind}`}>
              <header className="chat-entry-header">
                <strong>{entry.title}</strong>
                <span>{new Date(entry.created_at_unix_ms).toLocaleTimeString()}</span>
              </header>
              {entry.text !== undefined && <p className="chat-entry-text">{entry.text}</p>}

              {entry.kind === "approval_request" && entry.approval_id !== undefined && (
                <ApprovalRequestControls
                  approvalId={entry.approval_id}
                  draft={approvalDrafts[entry.approval_id]}
                  onDraftChange={(next) => updateApprovalDraft(entry.approval_id as string, next)}
                  onDecision={(approved) => decideInlineApproval(entry.approval_id as string, approved)}
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

              {entry.payload !== undefined && entry.kind !== "assistant" && entry.kind !== "user" && (
                <PrettyJsonBlock value={entry.payload} revealSensitiveValues={revealSensitiveValues} />
              )}

              {entry.run_id !== undefined && (
                <div className="chat-entry-actions">
                  <button type="button" className="secondary" onClick={() => openRunDetails(entry.run_id as string)}>
                    Open run details
                  </button>
                </div>
              )}
            </article>
          ))
        )}
      </div>
    </>
  );
}
