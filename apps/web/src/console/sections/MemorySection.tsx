import { type ReactNode, useMemo, useState } from "react";

import { ActionButton, CheckboxField, TextAreaField, TextInputField } from "../components/ui";
import {
  WorkspaceMetricCard,
  WorkspacePageHeader,
  WorkspaceSectionCard,
  WorkspaceStatusChip,
} from "../components/workspace/WorkspaceChrome";
import {
  WorkspaceConfirmDialog,
  WorkspaceEmptyState,
  WorkspaceInlineNotice,
  WorkspaceTable,
  workspaceToneForState,
} from "../components/workspace/WorkspacePatterns";
import { formatUnixMs, readNumber, readObject, readString, type JsonObject } from "../shared";
import type { ConsoleAppState } from "../useConsoleAppState";

type MemorySectionProps = {
  app: Pick<
    ConsoleAppState,
    | "memoryBusy"
    | "memoryQuery"
    | "setMemoryQuery"
    | "memoryChannel"
    | "setMemoryChannel"
    | "memoryPurgeChannel"
    | "setMemoryPurgeChannel"
    | "memoryPurgeSessionId"
    | "setMemoryPurgeSessionId"
    | "memoryPurgeAll"
    | "setMemoryPurgeAll"
    | "memoryHits"
    | "memoryStatusBusy"
    | "memoryStatus"
    | "memoryWorkspaceDocuments"
    | "memoryWorkspacePath"
    | "setMemoryWorkspacePath"
    | "memoryWorkspaceNextPath"
    | "setMemoryWorkspaceNextPath"
    | "memoryWorkspaceTitle"
    | "setMemoryWorkspaceTitle"
    | "memoryWorkspaceContent"
    | "setMemoryWorkspaceContent"
    | "memoryWorkspaceVersions"
    | "memoryWorkspaceSearchQuery"
    | "setMemoryWorkspaceSearchQuery"
    | "memoryWorkspaceHits"
    | "memoryDerivedArtifacts"
    | "memorySearchAllQuery"
    | "setMemorySearchAllQuery"
    | "memorySearchAllResults"
    | "memoryRecallPreview"
    | "refreshMemoryStatus"
    | "refreshWorkspaceDocuments"
    | "selectWorkspaceDocument"
    | "saveWorkspaceDocument"
    | "bootstrapWorkspace"
    | "moveWorkspaceDocument"
    | "deleteWorkspaceDocument"
    | "toggleWorkspaceDocumentPinned"
    | "searchWorkspaceDocuments"
    | "previewMemoryRecall"
    | "searchAllMemorySources"
    | "promoteMemoryHitToWorkspaceDraft"
    | "purgeMemory"
  >;
};

type GroupedResultsSectionProps = {
  title: string;
  items: JsonObject[];
  emptyDescription: string;
  renderItem: (item: JsonObject, index: number) => ReactNode;
};

export function MemorySection({ app }: MemorySectionProps) {
  const [confirmingPurge, setConfirmingPurge] = useState(false);
  const [selectedDerivedArtifactId, setSelectedDerivedArtifactId] = useState<string | null>(null);
  const usage = readObject(app.memoryStatus ?? {}, "usage");
  const retention = readObject(app.memoryStatus ?? {}, "retention");
  const maintenance = readObject(app.memoryStatus ?? {}, "maintenance");
  const workspace = readObject(app.memoryStatus ?? {}, "workspace");
  const derived = readObject(app.memoryStatus ?? {}, "derived");
  const derivedRecord = derived ?? EMPTY_OBJECT;
  const workspaceRoots = readStringArray(workspace, "roots");
  const curatedPaths = readStringArray(workspace, "curated_paths");
  const selectedDocument = useMemo(
    () =>
      findKnownWorkspaceDocument(
        app.memoryWorkspacePath,
        app.memoryWorkspaceDocuments,
        app.memoryWorkspaceHits,
        app.memorySearchAllResults,
      ),
    [
      app.memorySearchAllResults,
      app.memoryWorkspaceDocuments,
      app.memoryWorkspaceHits,
      app.memoryWorkspacePath,
    ],
  );
  const selectedDocumentRecord = selectedDocument ?? EMPTY_OBJECT;
  const selectedDocumentPinned = readBoolean(selectedDocument, "pinned");
  const selectedDocumentState = readString(selectedDocumentRecord, "state") ?? "draft";
  const selectedDocumentRisk = readString(selectedDocumentRecord, "risk_state") ?? "unknown";
  const selectedDocumentVersion =
    readNumber(selectedDocumentRecord, "latest_version") ??
    readNumber(app.memoryWorkspaceVersions[0] ?? EMPTY_OBJECT, "version") ??
    0;
  const recallWorkspaceHits = readObjectArray(app.memoryRecallPreview, "workspace_hits");
  const unifiedGroups =
    readObject(app.memorySearchAllResults ?? EMPTY_OBJECT, "groups") ?? EMPTY_OBJECT;
  const unifiedSessionHits = readObjectArray(unifiedGroups, "sessions");
  const unifiedWorkspaceHits = readObjectArray(unifiedGroups, "workspace");
  const unifiedMemoryHits = readObjectArray(unifiedGroups, "memory");
  const unifiedCounts =
    readObject(app.memorySearchAllResults ?? EMPTY_OBJECT, "counts") ?? EMPTY_OBJECT;
  const selectedDerivedArtifact = useMemo(
    () =>
      app.memoryDerivedArtifacts.find(
        (artifact) => readString(artifact, "derived_artifact_id") === selectedDerivedArtifactId,
      ) ?? null,
    [app.memoryDerivedArtifacts, selectedDerivedArtifactId],
  );
  const selectedDerivedWarnings = readObjectArray(selectedDerivedArtifact, "warnings");
  const selectedDerivedAnchors = readObjectArray(selectedDerivedArtifact, "anchors");

  return (
    <main className="workspace-page">
      <WorkspacePageHeader
        eyebrow="Agent"
        title="Memory"
        description="Bootstrap curated docs, keep workspace notes current, preview recall before prompts, and search session, workspace, and stored memory from one operator panel."
        status={
          <>
            <WorkspaceStatusChip
              tone={app.memoryWorkspaceDocuments.length > 0 ? "accent" : "default"}
            >
              {app.memoryWorkspaceDocuments.length} docs loaded
            </WorkspaceStatusChip>
            <WorkspaceStatusChip tone={app.memoryHits.length > 0 ? "success" : "default"}>
              {app.memoryHits.length} recall hits
            </WorkspaceStatusChip>
            <WorkspaceStatusChip
              tone={(readNumber(derivedRecord, "failed") ?? 0) > 0 ? "warning" : "default"}
            >
              {readNumber(derivedRecord, "total") ?? 0} derived artifacts
            </WorkspaceStatusChip>
            <WorkspaceStatusChip
              tone={
                unifiedSessionHits.length + unifiedWorkspaceHits.length + unifiedMemoryHits.length >
                0
                  ? "success"
                  : "default"
              }
            >
              {readNumber(unifiedCounts, "sessions") ?? unifiedSessionHits.length}/
              {readNumber(unifiedCounts, "workspace") ?? unifiedWorkspaceHits.length}/
              {readNumber(unifiedCounts, "memory") ?? unifiedMemoryHits.length} grouped
            </WorkspaceStatusChip>
          </>
        }
        actions={
          <div className="workspace-inline-actions">
            <ActionButton
              isDisabled={app.memoryStatusBusy}
              type="button"
              variant="secondary"
              onPress={() => void app.refreshWorkspaceDocuments()}
            >
              {app.memoryBusy ? "Refreshing..." : "Refresh docs"}
            </ActionButton>
            <ActionButton
              isDisabled={app.memoryStatusBusy}
              type="button"
              variant="primary"
              onPress={() => void app.refreshMemoryStatus()}
            >
              {app.memoryStatusBusy ? "Refreshing..." : "Refresh status"}
            </ActionButton>
          </div>
        }
      />

      <section className="workspace-metric-grid workspace-metric-grid--compact">
        <WorkspaceMetricCard
          detail="Stored memory entries currently visible to this principal."
          label="Stored items"
          tone={
            (readNumber(usage ?? {}, "item_count") ?? readNumber(usage ?? {}, "entries") ?? 0) > 0
              ? "accent"
              : "default"
          }
          value={readNumber(usage ?? {}, "item_count") ?? readNumber(usage ?? {}, "entries") ?? 0}
        />
        <WorkspaceMetricCard
          detail="Workspace docs surfaced in the recent-docs panel."
          label="Recent docs"
          value={app.memoryWorkspaceDocuments.length}
        />
        <WorkspaceMetricCard
          detail="Derived attachment artifacts currently tracked across extraction and transcription."
          label="Derived artifacts"
          tone={(readNumber(derivedRecord, "failed") ?? 0) > 0 ? "warning" : "accent"}
          value={readNumber(derivedRecord, "total") ?? 0}
        />
        <WorkspaceMetricCard
          detail="Retention policy remains visible so recall and purge stay deliberate."
          label="Retention TTL"
          value={`${readNumber(retention ?? {}, "ttl_days") ?? 0} days`}
        />
      </section>

      <section className="workspace-aside-grid">
        <div className="workspace-stack">
          <WorkspaceSectionCard
            description="Bootstrap the curated workspace set, keep recent docs close, and pin the references that should stay easy to reach."
            title="Workspace documents"
            actions={
              <div className="workspace-inline-actions">
                <ActionButton
                  isDisabled={app.memoryBusy}
                  type="button"
                  variant="secondary"
                  onPress={() => void app.bootstrapWorkspace(false)}
                >
                  {app.memoryBusy ? "Working..." : "Bootstrap"}
                </ActionButton>
                <ActionButton
                  isDisabled={app.memoryBusy}
                  type="button"
                  variant="ghost"
                  onPress={() => void app.bootstrapWorkspace(true)}
                >
                  Repair curated docs
                </ActionButton>
              </div>
            }
          >
            <div className="workspace-panel__intro">
              <p className="workspace-kicker">Scope</p>
              <h3>Workspace roots and curated starting points</h3>
              <p className="chat-muted">
                Roots: {workspaceRoots.length > 0 ? workspaceRoots.join(", ") : "No roots returned"}
                . Curated:{" "}
                {curatedPaths.length > 0 ? curatedPaths.join(", ") : "No curated paths returned"}.
              </p>
            </div>

            {app.memoryWorkspaceDocuments.length === 0 ? (
              <WorkspaceEmptyState
                description="Run workspace bootstrap or refresh docs to load the current workspace catalog."
                title="No workspace docs loaded"
              />
            ) : (
              <div className="chat-ops-list">
                {app.memoryWorkspaceDocuments.map((document, index) => {
                  const path = readString(document, "path") ?? `workspace-${index + 1}`;
                  const title = readString(document, "title") ?? path;
                  const pinned = readBoolean(document, "pinned");
                  const documentState = readString(document, "state") ?? "draft";
                  const updatedAt = formatUnixMs(readNumber(document, "updated_at_unix_ms"));
                  const lastRecalledAt = formatUnixMs(
                    readNumber(document, "last_recalled_at_unix_ms"),
                  );

                  return (
                    <article key={path} className="chat-ops-card">
                      <div className="chat-ops-card__copy">
                        <strong>{title}</strong>
                        <span>{path}</span>
                        <p>
                          {readString(document, "kind") ?? "doc"} · version{" "}
                          {readNumber(document, "latest_version") ?? 0} · updated {updatedAt}
                        </p>
                        <p>Last recalled {lastRecalledAt}</p>
                      </div>
                      <div className="chat-ops-card__actions">
                        <WorkspaceStatusChip tone={workspaceToneForState(documentState)}>
                          {documentState}
                        </WorkspaceStatusChip>
                        <ActionButton
                          size="sm"
                          type="button"
                          variant="secondary"
                          onPress={() => void app.selectWorkspaceDocument(path)}
                        >
                          Open
                        </ActionButton>
                        <ActionButton
                          size="sm"
                          type="button"
                          variant={pinned ? "primary" : "ghost"}
                          onPress={() => void app.toggleWorkspaceDocumentPinned(path, !pinned)}
                        >
                          {pinned ? "Unpin" : "Pin"}
                        </ActionButton>
                        <ActionButton
                          size="sm"
                          type="button"
                          variant="danger"
                          onPress={() => void app.deleteWorkspaceDocument(path)}
                        >
                          Delete
                        </ActionButton>
                      </div>
                    </article>
                  );
                })}
              </div>
            )}
          </WorkspaceSectionCard>
          <WorkspaceSectionCard
            description="Keep extraction/transcription health visible so quarantined, failed, or purged derived artifacts do not silently disappear from operator workflows."
            title="Derived artifact health"
          >
            <dl className="workspace-key-value-grid">
              <div>
                <dt>Succeeded</dt>
                <dd>{readNumber(derivedRecord, "succeeded") ?? 0}</dd>
              </div>
              <div>
                <dt>Failed</dt>
                <dd>{readNumber(derivedRecord, "failed") ?? 0}</dd>
              </div>
              <div>
                <dt>Quarantined</dt>
                <dd>{readNumber(derivedRecord, "quarantined") ?? 0}</dd>
              </div>
              <div>
                <dt>Purged</dt>
                <dd>{readNumber(derivedRecord, "purged") ?? 0}</dd>
              </div>
              <div>
                <dt>Needs recompute</dt>
                <dd>{readNumber(derivedRecord, "recompute_required") ?? 0}</dd>
              </div>
              <div>
                <dt>Orphaned</dt>
                <dd>{readNumber(derivedRecord, "orphaned") ?? 0}</dd>
              </div>
            </dl>
          </WorkspaceSectionCard>
          <WorkspaceSectionCard
            description="When a workspace document was generated from an attachment, its derived extraction and transcription outputs stay visible here with parser provenance and lifecycle state."
            title="Linked derived artifacts"
          >
            {app.memoryDerivedArtifacts.length === 0 ? (
              <WorkspaceEmptyState
                compact
                description="Open a workspace document backed by an attachment-derived document to inspect linked extraction and transcription outputs."
                title="No linked derived artifacts"
              />
            ) : (
              <div className="chat-ops-list">
                {app.memoryDerivedArtifacts.map((artifact, index) => (
                  <article
                    key={readString(artifact, "derived_artifact_id") ?? `derived-${index}`}
                    className="chat-ops-card"
                  >
                    <div className="chat-ops-card__copy">
                      <strong>{readString(artifact, "filename") ?? `derived-${index + 1}`}</strong>
                      <span>
                        {readString(artifact, "kind") ?? "derived"} ·{" "}
                        {readString(artifact, "state") ?? "unknown"} ·{" "}
                        {readString(artifact, "parser_name") ?? "parser"}@
                        {readString(artifact, "parser_version") ?? "n/a"}
                      </span>
                      <p>
                        {readString(artifact, "summary_text") ??
                          readString(artifact, "failure_reason") ??
                          readString(artifact, "quarantine_reason") ??
                          readString(artifact, "content_text") ??
                          "No preview returned."}
                      </p>
                    </div>
                    <div className="chat-ops-card__actions">
                      <WorkspaceStatusChip
                        tone={workspaceToneForState(readString(artifact, "state") ?? "draft")}
                      >
                        {readString(artifact, "state") ?? "unknown"}
                      </WorkspaceStatusChip>
                      <ActionButton
                        size="sm"
                        type="button"
                        variant="secondary"
                        onPress={() =>
                          setSelectedDerivedArtifactId(
                            readString(artifact, "derived_artifact_id") ?? null,
                          )
                        }
                      >
                        Open
                      </ActionButton>
                      <span className="chat-muted">
                        {formatUnixMs(readNumber(artifact, "updated_at_unix_ms"))}
                      </span>
                    </div>
                  </article>
                ))}
              </div>
            )}
            {selectedDerivedArtifact !== null ? (
              <div className="workspace-stack">
                <div className="workspace-panel__intro">
                  <p className="workspace-kicker">Inspector</p>
                  <h3>
                    {readString(selectedDerivedArtifact, "kind") ?? "derived artifact"} ·{" "}
                    {readString(selectedDerivedArtifact, "filename") ?? "artifact"}
                  </h3>
                  <p className="chat-muted">
                    {readString(selectedDerivedArtifact, "state") ?? "unknown"} ·{" "}
                    {readString(selectedDerivedArtifact, "parser_name") ?? "parser"}@
                    {readString(selectedDerivedArtifact, "parser_version") ?? "n/a"}
                  </p>
                </div>
                <dl className="workspace-key-value-grid">
                  <div>
                    <dt>Source artifact</dt>
                    <dd>{readString(selectedDerivedArtifact, "source_artifact_id") ?? "n/a"}</dd>
                  </div>
                  <div>
                    <dt>Attachment</dt>
                    <dd>{readString(selectedDerivedArtifact, "attachment_id") ?? "n/a"}</dd>
                  </div>
                  <div>
                    <dt>Workspace doc</dt>
                    <dd>{readString(selectedDerivedArtifact, "workspace_document_id") ?? "n/a"}</dd>
                  </div>
                  <div>
                    <dt>Memory item</dt>
                    <dd>{readString(selectedDerivedArtifact, "memory_item_id") ?? "n/a"}</dd>
                  </div>
                  <div>
                    <dt>Background task</dt>
                    <dd>{readString(selectedDerivedArtifact, "background_task_id") ?? "n/a"}</dd>
                  </div>
                  <div>
                    <dt>Updated</dt>
                    <dd>
                      {formatUnixMs(readNumber(selectedDerivedArtifact, "updated_at_unix_ms"))}
                    </dd>
                  </div>
                </dl>
                {selectedDerivedWarnings.length > 0 ? (
                  <WorkspaceInlineNotice title="Parser warnings" tone="warning">
                    <ul className="chat-list">
                      {selectedDerivedWarnings.map((warning, index) => (
                        <li key={readString(warning, "code") ?? `warning-${index}`}>
                          <strong>{readString(warning, "code") ?? "warning"}</strong>:{" "}
                          {readString(warning, "message") ?? "No warning message returned."}
                        </li>
                      ))}
                    </ul>
                  </WorkspaceInlineNotice>
                ) : null}
                {selectedDerivedAnchors.length > 0 ? (
                  <div className="workspace-stack">
                    <div className="workspace-panel__intro">
                      <p className="workspace-kicker">Anchors</p>
                      <h3>Structured provenance</h3>
                    </div>
                    <WorkspaceTable ariaLabel="Derived artifact anchors" columns={["Label", "Kind", "Locator"]}>
                      {selectedDerivedAnchors.map((anchor, index) => (
                        <tr key={readString(anchor, "label") ?? `anchor-${index}`}>
                          <td>{readString(anchor, "label") ?? `Anchor ${index + 1}`}</td>
                          <td>{readString(anchor, "kind") ?? "section"}</td>
                          <td>{readString(anchor, "locator") ?? "n/a"}</td>
                        </tr>
                      ))}
                    </WorkspaceTable>
                  </div>
                ) : null}
                <pre className="chat-detail-panel__payload">
                  {readString(selectedDerivedArtifact, "content_text") ??
                    readString(selectedDerivedArtifact, "summary_text") ??
                    readString(selectedDerivedArtifact, "failure_reason") ??
                    readString(selectedDerivedArtifact, "quarantine_reason") ??
                    "No derived content returned."}
                </pre>
              </div>
            ) : null}
          </WorkspaceSectionCard>
          <WorkspaceSectionCard
            description="Read or write a workspace document directly from the console. Path is the durable identifier, title stays operator-friendly."
            title="Document editor"
            actions={
              <div className="workspace-inline-actions">
                <WorkspaceStatusChip tone={workspaceToneForState(selectedDocumentState)}>
                  {selectedDocumentState}
                </WorkspaceStatusChip>
                <WorkspaceStatusChip tone={workspaceToneForState(selectedDocumentRisk)}>
                  {selectedDocumentRisk}
                </WorkspaceStatusChip>
                <WorkspaceStatusChip tone={selectedDocumentPinned ? "accent" : "default"}>
                  {selectedDocumentPinned ? "Pinned" : "Not pinned"}
                </WorkspaceStatusChip>
              </div>
            }
          >
            <form
              className="workspace-stack"
              onSubmit={(event) => void app.saveWorkspaceDocument(event)}
            >
              <div className="workspace-form-grid">
                <TextInputField
                  description="Use a stable relative path such as README.md or docs/runbooks/incident.md."
                  label="Path"
                  value={app.memoryWorkspacePath}
                  onChange={app.setMemoryWorkspacePath}
                />
                <TextInputField
                  description="Optional operator-facing title."
                  label="Title"
                  value={app.memoryWorkspaceTitle}
                  onChange={app.setMemoryWorkspaceTitle}
                />
              </div>
              <TextInputField
                description="Use this when renaming or moving the current document without rewriting content."
                label="Move to path"
                value={app.memoryWorkspaceNextPath}
                onChange={app.setMemoryWorkspaceNextPath}
              />
              <TextAreaField
                description="Workspace content is indexed for recall and search. Keep it concise and operational."
                label="Content"
                rows={12}
                value={app.memoryWorkspaceContent}
                onChange={app.setMemoryWorkspaceContent}
              />
              <div className="workspace-inline-actions">
                <ActionButton isDisabled={app.memoryBusy} type="submit" variant="primary">
                  {app.memoryBusy ? "Saving..." : "Save document"}
                </ActionButton>
                <ActionButton
                  isDisabled={app.memoryBusy || app.memoryWorkspacePath.trim().length === 0}
                  type="button"
                  variant={selectedDocumentPinned ? "secondary" : "ghost"}
                  onPress={() =>
                    void app.toggleWorkspaceDocumentPinned(
                      app.memoryWorkspacePath.trim(),
                      !selectedDocumentPinned,
                    )
                  }
                >
                  {selectedDocumentPinned ? "Unpin current doc" : "Pin current doc"}
                </ActionButton>
                <ActionButton
                  isDisabled={
                    app.memoryBusy ||
                    app.memoryWorkspacePath.trim().length === 0 ||
                    app.memoryWorkspaceNextPath.trim().length === 0
                  }
                  type="button"
                  variant="secondary"
                  onPress={() => void app.moveWorkspaceDocument()}
                >
                  Move document
                </ActionButton>
                <ActionButton
                  isDisabled={app.memoryBusy || app.memoryWorkspacePath.trim().length === 0}
                  type="button"
                  variant="danger"
                  onPress={() => void app.deleteWorkspaceDocument()}
                >
                  Delete current doc
                </ActionButton>
                <ActionButton
                  type="button"
                  variant="ghost"
                  onPress={() => {
                    app.setMemoryWorkspacePath("notes/new-doc.md");
                    app.setMemoryWorkspaceNextPath("notes/new-doc.md");
                    app.setMemoryWorkspaceTitle("");
                    app.setMemoryWorkspaceContent("");
                  }}
                >
                  New draft
                </ActionButton>
              </div>
            </form>

            <div className="workspace-panel__intro">
              <p className="workspace-kicker">History</p>
              <h3>Recent versions</h3>
              <p className="chat-muted">
                Latest version {selectedDocumentVersion}. Previous writes remain visible here so an
                operator can inspect what changed.
              </p>
            </div>

            <dl className="workspace-key-value-grid">
              <div>
                <dt>Template</dt>
                <dd>{readString(selectedDocumentRecord, "template_id") ?? "manual"}</dd>
              </div>
              <div>
                <dt>Source memory</dt>
                <dd>{readString(selectedDocumentRecord, "source_memory_id") ?? "n/a"}</dd>
              </div>
              <div>
                <dt>Latest session</dt>
                <dd>{readString(selectedDocumentRecord, "latest_session_id") ?? "n/a"}</dd>
              </div>
              <div>
                <dt>Manual override</dt>
                <dd>{readBoolean(selectedDocument, "manual_override") ? "yes" : "no"}</dd>
              </div>
              <div>
                <dt>Last recalled</dt>
                <dd>
                  {formatUnixMs(readNumber(selectedDocumentRecord, "last_recalled_at_unix_ms"))}
                </dd>
              </div>
              <div>
                <dt>Risk reasons</dt>
                <dd>{readStringArray(selectedDocument, "risk_reasons").join(", ") || "none"}</dd>
              </div>
            </dl>

            {app.memoryWorkspaceVersions.length === 0 ? (
              <WorkspaceEmptyState
                compact
                description="Open or save a workspace document to load version history."
                title="No document history loaded"
              />
            ) : (
              <WorkspaceTable
                ariaLabel="Workspace document versions"
                columns={["Version", "Event", "When", "Hash"]}
              >
                {app.memoryWorkspaceVersions.map((version, index) => (
                  <tr key={readString(version, "version_ulid") ?? `workspace-version-${index}`}>
                    <td>v{readNumber(version, "version") ?? 0}</td>
                    <td>{readString(version, "event_type") ?? "write"}</td>
                    <td>{formatUnixMs(readNumber(version, "created_at_unix_ms"))}</td>
                    <td>{shortHash(readString(version, "content_hash"))}</td>
                  </tr>
                ))}
              </WorkspaceTable>
            )}
          </WorkspaceSectionCard>

          <WorkspaceSectionCard
            description="Search only workspace documents when you want doc-focused recall without session or memory noise."
            title="Workspace search"
          >
            <form
              className="workspace-stack"
              onSubmit={(event) => void app.searchWorkspaceDocuments(event)}
            >
              <div className="workspace-form-grid">
                <TextInputField
                  label="Workspace query"
                  value={app.memoryWorkspaceSearchQuery}
                  onChange={app.setMemoryWorkspaceSearchQuery}
                />
                <div className="workspace-inline">
                  <ActionButton isDisabled={app.memoryBusy} type="submit" variant="primary">
                    {app.memoryBusy ? "Searching..." : "Search workspace"}
                  </ActionButton>
                </div>
              </div>
            </form>

            {app.memoryWorkspaceHits.length === 0 ? (
              <WorkspaceEmptyState
                compact
                description="Search the workspace document index to surface relevant snippets and open the matching doc."
                title="No workspace results loaded"
              />
            ) : (
              <WorkspaceTable
                ariaLabel="Workspace search results"
                columns={["Document", "Reason", "Snippet", "Score"]}
              >
                {app.memoryWorkspaceHits.map((hit, index) => {
                  const document = readObject(hit, "document") ?? EMPTY_OBJECT;
                  const path = readString(document, "path") ?? `workspace-hit-${index}`;

                  return (
                    <tr key={`${path}-${index}`}>
                      <td>
                        <div className="workspace-table__meta">
                          <strong>{readString(document, "title") ?? path}</strong>
                          <span className="chat-muted">{path}</span>
                        </div>
                      </td>
                      <td>{readString(hit, "reason") ?? "workspace"}</td>
                      <td>{readString(hit, "snippet") ?? "No snippet"}</td>
                      <td>
                        <div className="workspace-inline-actions">
                          <span>{formatScore(hit)}</span>
                          <ActionButton
                            size="sm"
                            type="button"
                            variant="ghost"
                            onPress={() => void app.selectWorkspaceDocument(path)}
                          >
                            Open
                          </ActionButton>
                        </div>
                      </td>
                    </tr>
                  );
                })}
              </WorkspaceTable>
            )}
          </WorkspaceSectionCard>
        </div>
        <div className="workspace-stack">
          <WorkspaceSectionCard
            description="Preview which stored memory and workspace docs would be injected for the current query before the chat surface sends anything."
            title="Recall preview"
          >
            <form
              className="workspace-stack"
              onSubmit={(event) => void app.previewMemoryRecall(event)}
            >
              <div className="workspace-form-grid">
                <TextInputField
                  label="Query"
                  value={app.memoryQuery}
                  onChange={app.setMemoryQuery}
                />
                <TextInputField
                  description="Optional scope if you want to narrow recall by channel."
                  label="Channel"
                  value={app.memoryChannel}
                  onChange={app.setMemoryChannel}
                />
              </div>
              <div className="workspace-inline-actions">
                <ActionButton isDisabled={app.memoryBusy} type="submit" variant="primary">
                  {app.memoryBusy ? "Searching..." : "Search"}
                </ActionButton>
                <WorkspaceStatusChip tone={app.memoryHits.length > 0 ? "success" : "default"}>
                  {app.memoryHits.length} memory refs
                </WorkspaceStatusChip>
                <WorkspaceStatusChip tone={recallWorkspaceHits.length > 0 ? "accent" : "default"}>
                  {recallWorkspaceHits.length} workspace refs
                </WorkspaceStatusChip>
              </div>
            </form>

            {app.memoryRecallPreview === null ? (
              <WorkspaceEmptyState
                compact
                description="Preview recall to inspect the prompt additions and returned parameter delta."
                title="No recall preview loaded"
              />
            ) : (
              <>
                <div className="chat-ops-list">
                  {recallWorkspaceHits.map((hit, index) => {
                    const document = readObject(hit, "document") ?? EMPTY_OBJECT;
                    const path = readString(document, "path") ?? `recall-workspace-${index}`;
                    return (
                      <article key={`${path}-${index}`} className="chat-ops-card">
                        <div className="chat-ops-card__copy">
                          <strong>{readString(document, "title") ?? path}</strong>
                          <span>{path}</span>
                          <p>{readString(hit, "snippet") ?? "No snippet"}</p>
                        </div>
                        <div className="chat-ops-card__actions">
                          <WorkspaceStatusChip tone="accent">
                            {formatScore(hit)}
                          </WorkspaceStatusChip>
                          <ActionButton
                            size="sm"
                            type="button"
                            variant="ghost"
                            onPress={() => void app.selectWorkspaceDocument(path)}
                          >
                            Open
                          </ActionButton>
                        </div>
                      </article>
                    );
                  })}
                  {app.memoryHits.map((hit, index) => (
                    <article key={readMemoryId(hit, index)} className="chat-ops-card">
                      <div className="chat-ops-card__copy">
                        <strong>{readMemoryId(hit, index)}</strong>
                        <span>
                          {readString(hit, "channel") ??
                            readString(readObject(hit, "item") ?? EMPTY_OBJECT, "channel") ??
                            "No channel"}
                        </span>
                        <p>
                          {readString(hit, "snippet") ??
                            readString(hit, "content") ??
                            readString(readObject(hit, "item") ?? EMPTY_OBJECT, "content_text") ??
                            "No snippet"}
                        </p>
                      </div>
                      <div className="chat-ops-card__actions">
                        <WorkspaceStatusChip tone="success">{formatScore(hit)}</WorkspaceStatusChip>
                        <ActionButton
                          size="sm"
                          type="button"
                          variant="secondary"
                          onPress={() => app.promoteMemoryHitToWorkspaceDraft(hit)}
                        >
                          Promote
                        </ActionButton>
                      </div>
                    </article>
                  ))}
                </div>
                <div className="workspace-panel__intro">
                  <p className="workspace-kicker">Prompt preview</p>
                  <h3>Previewed prompt augmentation</h3>
                </div>
                <pre className="chat-detail-panel__payload">
                  {readString(app.memoryRecallPreview ?? EMPTY_OBJECT, "prompt_preview") ??
                    "No prompt preview returned."}
                </pre>
              </>
            )}
          </WorkspaceSectionCard>

          <WorkspaceSectionCard
            description="Search session catalog, workspace docs, and stored memory together, then inspect each source group separately."
            title="Search all sources"
          >
            <form
              className="workspace-stack"
              onSubmit={(event) => void app.searchAllMemorySources(event)}
            >
              <div className="workspace-form-grid">
                <TextInputField
                  label="Unified query"
                  value={app.memorySearchAllQuery}
                  onChange={app.setMemorySearchAllQuery}
                />
                <div className="workspace-inline">
                  <ActionButton isDisabled={app.memoryBusy} type="submit" variant="primary">
                    {app.memoryBusy ? "Searching..." : "Search all"}
                  </ActionButton>
                </div>
              </div>
            </form>

            {app.memorySearchAllResults === null ? (
              <WorkspaceEmptyState
                compact
                description="Run a unified search to group matching sessions, workspace docs, and memory items."
                title="No grouped search loaded"
              />
            ) : (
              <>
                <div className="workspace-inline-actions">
                  <WorkspaceStatusChip tone={unifiedSessionHits.length > 0 ? "success" : "default"}>
                    Sessions {readNumber(unifiedCounts, "sessions") ?? unifiedSessionHits.length}
                  </WorkspaceStatusChip>
                  <WorkspaceStatusChip
                    tone={unifiedWorkspaceHits.length > 0 ? "accent" : "default"}
                  >
                    Workspace{" "}
                    {readNumber(unifiedCounts, "workspace") ?? unifiedWorkspaceHits.length}
                  </WorkspaceStatusChip>
                  <WorkspaceStatusChip tone={unifiedMemoryHits.length > 0 ? "warning" : "default"}>
                    Memory {readNumber(unifiedCounts, "memory") ?? unifiedMemoryHits.length}
                  </WorkspaceStatusChip>
                </div>

                <GroupedResultsSection
                  emptyDescription="No session catalog matches were returned for this query."
                  items={unifiedSessionHits}
                  title="Sessions"
                  renderItem={(item, index) => {
                    const sessionId = readString(item, "session_id") ?? `session-${index + 1}`;
                    return (
                      <article key={sessionId} className="chat-ops-card">
                        <div className="chat-ops-card__copy">
                          <strong>{readString(item, "title") ?? sessionId}</strong>
                          <span>{sessionId}</span>
                          <p>{readString(item, "preview") ?? "No preview returned."}</p>
                        </div>
                        <div className="chat-ops-card__actions">
                          <WorkspaceStatusChip
                            tone={workspaceToneForState(readString(item, "last_run_state"))}
                          >
                            {readString(item, "last_run_state") ?? "unknown"}
                          </WorkspaceStatusChip>
                        </div>
                      </article>
                    );
                  }}
                />

                <GroupedResultsSection
                  emptyDescription="No workspace document matches were returned for this query."
                  items={unifiedWorkspaceHits}
                  title="Workspace"
                  renderItem={(item, index) => {
                    const document = readObject(item, "document") ?? EMPTY_OBJECT;
                    const path = readString(document, "path") ?? `workspace-${index + 1}`;
                    return (
                      <article key={path} className="chat-ops-card">
                        <div className="chat-ops-card__copy">
                          <strong>{readString(document, "title") ?? path}</strong>
                          <span>{path}</span>
                          <p>{readString(item, "snippet") ?? "No snippet returned."}</p>
                        </div>
                        <div className="chat-ops-card__actions">
                          <WorkspaceStatusChip tone="accent">
                            {formatScore(item)}
                          </WorkspaceStatusChip>
                          <ActionButton
                            size="sm"
                            type="button"
                            variant="ghost"
                            onPress={() => void app.selectWorkspaceDocument(path)}
                          >
                            Open
                          </ActionButton>
                        </div>
                      </article>
                    );
                  }}
                />

                <GroupedResultsSection
                  emptyDescription="No stored memory matches were returned for this query."
                  items={unifiedMemoryHits}
                  title="Stored memory"
                  renderItem={(item, index) => (
                    <article key={readMemoryId(item, index)} className="chat-ops-card">
                      <div className="chat-ops-card__copy">
                        <strong>{readMemoryId(item, index)}</strong>
                        <span>
                          {readString(item, "channel") ??
                            readString(readObject(item, "item") ?? EMPTY_OBJECT, "channel") ??
                            "No channel"}
                        </span>
                        <p>
                          {readString(item, "snippet") ??
                            readString(item, "content") ??
                            readString(readObject(item, "item") ?? EMPTY_OBJECT, "content_text") ??
                            "No snippet returned."}
                        </p>
                      </div>
                      <div className="chat-ops-card__actions">
                        <WorkspaceStatusChip tone="warning">
                          {formatScore(item)}
                        </WorkspaceStatusChip>
                        <ActionButton
                          size="sm"
                          type="button"
                          variant="secondary"
                          onPress={() => app.promoteMemoryHitToWorkspaceDraft(item)}
                        >
                          Promote
                        </ActionButton>
                      </div>
                    </article>
                  )}
                />
              </>
            )}
          </WorkspaceSectionCard>
          <WorkspaceSectionCard
            description="Keep retention posture visible and make purge explicitly destructive."
            title="Retention and purge"
          >
            {app.memoryStatus === null ? (
              <WorkspaceEmptyState
                compact
                description="Refresh status to inspect memory retention, maintenance timing, and usage."
                title="No memory status loaded"
              />
            ) : (
              <dl className="workspace-key-value-grid">
                <div>
                  <dt>Entries</dt>
                  <dd>
                    {readNumber(usage ?? {}, "item_count") ??
                      readNumber(usage ?? {}, "entries") ??
                      0}
                  </dd>
                </div>
                <div>
                  <dt>Approx bytes</dt>
                  <dd>{readNumber(usage ?? {}, "approx_bytes") ?? "n/a"}</dd>
                </div>
                <div>
                  <dt>TTL days</dt>
                  <dd>{readNumber(retention ?? {}, "ttl_days") ?? "n/a"}</dd>
                </div>
                <div>
                  <dt>Vacuum schedule</dt>
                  <dd>{readString(retention ?? {}, "vacuum_schedule") ?? "n/a"}</dd>
                </div>
                <div>
                  <dt>Last vacuum</dt>
                  <dd>
                    {formatUnixMs(
                      readNumber(maintenance ?? {}, "last_vacuum_at_unix_ms") ??
                        readNumber(app.memoryStatus, "last_vacuum_at_unix_ms"),
                    )}
                  </dd>
                </div>
                <div>
                  <dt>Next maintenance</dt>
                  <dd>
                    {formatUnixMs(readNumber(app.memoryStatus, "next_maintenance_run_at_unix_ms"))}
                  </dd>
                </div>
              </dl>
            )}

            <div className="workspace-form-grid">
              <TextInputField
                label="Purge channel"
                value={app.memoryPurgeChannel}
                onChange={app.setMemoryPurgeChannel}
              />
              <TextInputField
                label="Purge session ID"
                value={app.memoryPurgeSessionId}
                onChange={app.setMemoryPurgeSessionId}
              />
              <CheckboxField
                checked={app.memoryPurgeAll}
                description="Delete all memory visible to the current principal."
                label="Purge all principal memory"
                onChange={app.setMemoryPurgeAll}
              />
            </div>

            <div className="workspace-inline-actions">
              <ActionButton
                isDisabled={app.memoryBusy}
                type="button"
                variant="danger"
                onPress={() => setConfirmingPurge(true)}
              >
                {app.memoryBusy ? "Purging..." : "Purge memory"}
              </ActionButton>
            </div>
          </WorkspaceSectionCard>

          <WorkspaceInlineNotice title="Operator guidance" tone="warning">
            <p>
              Bootstrap and edit workspace docs for durable context. Use recall preview to inspect
              what would be injected, then reserve purge for exceptional cleanup instead of normal
              iteration.
            </p>
          </WorkspaceInlineNotice>
        </div>
      </section>

      <WorkspaceConfirmDialog
        isBusy={app.memoryBusy}
        isOpen={confirmingPurge}
        confirmLabel="Purge memory"
        confirmTone="danger"
        description={
          app.memoryPurgeAll
            ? "Delete all memory for the current principal? This is the broadest purge path."
            : `Delete memory for channel ${app.memoryPurgeChannel || "n/a"} and session ${app.memoryPurgeSessionId || "n/a"}?`
        }
        title="Purge memory"
        onConfirm={() => {
          setConfirmingPurge(false);
          void app.purgeMemory();
        }}
        onOpenChange={setConfirmingPurge}
      />
    </main>
  );
}

function GroupedResultsSection({
  title,
  items,
  emptyDescription,
  renderItem,
}: GroupedResultsSectionProps) {
  return (
    <div className="workspace-stack">
      <div className="workspace-panel__intro">
        <p className="workspace-kicker">{title}</p>
        <h3>{title} results</h3>
      </div>
      {items.length === 0 ? (
        <p className="chat-muted">{emptyDescription}</p>
      ) : (
        <div className="chat-ops-list">{items.map(renderItem)}</div>
      )}
    </div>
  );
}

function findKnownWorkspaceDocument(
  path: string,
  documents: JsonObject[],
  workspaceHits: JsonObject[],
  searchAllResults: JsonObject | null,
): JsonObject | null {
  const trimmed = path.trim();
  if (trimmed.length === 0) {
    return null;
  }

  const directMatch =
    documents.find((document) => readString(document, "path") === trimmed) ??
    workspaceHits
      .map((hit) => readObject(hit, "document") ?? EMPTY_OBJECT)
      .find((document) => readString(document, "path") === trimmed);
  if (directMatch !== undefined && directMatch !== null) {
    return directMatch;
  }

  const groups = readObject(searchAllResults ?? EMPTY_OBJECT, "groups") ?? EMPTY_OBJECT;
  const searchWorkspaceHits = readObjectArray(groups, "workspace");
  return (
    searchWorkspaceHits
      .map((hit) => readObject(hit, "document") ?? EMPTY_OBJECT)
      .find((document) => readString(document, "path") === trimmed) ?? null
  );
}

function readObjectArray(source: JsonObject | null | undefined, key: string): JsonObject[] {
  const value = source?.[key];
  if (!Array.isArray(value)) {
    return [];
  }
  return value.filter(isJsonObject);
}

function readStringArray(source: JsonObject | null | undefined, key: string): string[] {
  const value = source?.[key];
  if (!Array.isArray(value)) {
    return [];
  }
  return value.flatMap((entry) =>
    typeof entry === "string" && entry.trim().length > 0 ? [entry] : [],
  );
}

function readBoolean(source: JsonObject | null | undefined, key: string): boolean {
  return source?.[key] === true;
}

function readMemoryId(hit: JsonObject, index: number): string {
  return (
    readString(hit, "memory_id") ??
    readString(readObject(hit, "item") ?? EMPTY_OBJECT, "memory_id") ??
    `memory-${index + 1}`
  );
}

function formatScore(hit: JsonObject): string {
  const score =
    readNumber(hit, "score") ??
    readNumber(readObject(hit, "breakdown") ?? EMPTY_OBJECT, "final_score");
  return score === null ? "n/a" : score.toFixed(2);
}

function shortHash(value: string | null): string {
  if (value === null || value.trim().length === 0) {
    return "n/a";
  }
  return value.slice(0, 12);
}

function isJsonObject(value: unknown): value is JsonObject {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

const EMPTY_OBJECT: JsonObject = {};
