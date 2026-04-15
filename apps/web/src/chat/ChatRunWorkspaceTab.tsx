import { useDeferredValue, useEffect, useMemo, useState } from "react";

import {
  ActionButton,
  ActionCluster,
  CheckboxField,
  EmptyState,
  InlineNotice,
  KeyValueList,
  SectionCard,
  SelectField,
  TextInputField,
} from "../console/components/ui";
import type {
  ChatRunStatusRecord,
  ConsoleApiClient,
  JsonValue,
  WorkspaceArtifactDetail,
  WorkspaceArtifactRecord,
  WorkspaceCheckpointDetailEnvelope,
  WorkspaceCheckpointSummary,
  WorkspaceCompareEnvelope,
  WorkspaceDiffFileRecord,
  WorkspaceRestoreReportEnvelope,
  WorkspaceRestoreResponseEnvelope,
} from "../consoleApi";

import { collectCanvasFrameUrls, PrettyJsonBlock, shortId } from "./chatShared";

type RunWorkspaceTabProps = {
  active: boolean;
  api: ConsoleApiClient;
  runId: string;
  runStatus: ChatRunStatusRecord | null;
  runIds: string[];
  revealSensitiveValues: boolean;
  setError: (next: string | null) => void;
  setNotice: (next: string | null) => void;
  onOpenRun: (runId: string, tab?: "status" | "lineage" | "tape" | "workspace") => void;
  onInspectCompaction: (artifactId: string) => void;
  onInspectSessionCheckpoint: (checkpointId: string) => void;
  onWorkspaceRestore: (response: WorkspaceRestoreResponseEnvelope) => Promise<void>;
  openMemorySection: () => void;
  openSupportSection: () => void;
};

type AnchorOption = { key: string; label: string; description?: string };

export function ChatRunWorkspaceTab({
  active,
  api,
  runId,
  runStatus,
  runIds,
  revealSensitiveValues,
  setError,
  setNotice,
  onOpenRun,
  onInspectCompaction,
  onInspectSessionCheckpoint,
  onWorkspaceRestore,
  openMemorySection,
  openSupportSection,
}: RunWorkspaceTabProps) {
  const [query, setQuery] = useState("");
  const deferredQuery = useDeferredValue(query);
  const [changedOnly, setChangedOnly] = useState(true);
  const [branchSession, setBranchSession] = useState(true);
  const [workspaceBusy, setWorkspaceBusy] = useState(false);
  const [workspaceEnvelope, setWorkspaceEnvelope] = useState<Awaited<
    ReturnType<ConsoleApiClient["chatRunWorkspace"]>
  > | null>(null);
  const [artifactBusy, setArtifactBusy] = useState(false);
  const [artifactDetail, setArtifactDetail] = useState<WorkspaceArtifactDetail | null>(null);
  const [selectedArtifactId, setSelectedArtifactId] = useState("");
  const [selectedCheckpointId, setSelectedCheckpointId] = useState("");
  const [checkpointBusy, setCheckpointBusy] = useState(false);
  const [checkpointEnvelope, setCheckpointEnvelope] = useState<WorkspaceCheckpointDetailEnvelope | null>(
    null,
  );
  const [selectedReportId, setSelectedReportId] = useState("");
  const [reportBusy, setReportBusy] = useState(false);
  const [reportEnvelope, setReportEnvelope] = useState<WorkspaceRestoreReportEnvelope | null>(null);
  const [leftAnchor, setLeftAnchor] = useState(`run:${runId}`);
  const [rightAnchor, setRightAnchor] = useState("");
  const [compareBusy, setCompareBusy] = useState(false);
  const [compareEnvelope, setCompareEnvelope] = useState<WorkspaceCompareEnvelope | null>(null);
  const [selectedDiffKey, setSelectedDiffKey] = useState("");
  const [restoreBusyKey, setRestoreBusyKey] = useState<string | null>(null);
  const [promotionBusyKey, setPromotionBusyKey] = useState<string | null>(null);
  const [lastRestore, setLastRestore] = useState<WorkspaceRestoreResponseEnvelope | null>(null);

  const workspace = workspaceEnvelope?.workspace ?? null;
  const workspaceRun = workspaceEnvelope?.run ?? runStatus;
  const artifacts = workspace?.artifacts ?? [];
  const checkpoints = workspace?.workspace_checkpoints ?? [];
  const selectedArtifact =
    artifacts.find(
      (artifact) =>
        artifact.artifact_id === selectedArtifactId ||
        artifact.versions.some((version) => version.artifact_id === selectedArtifactId),
    ) ?? null;
  const selectedDiff =
    compareEnvelope?.diff.files.find(
      (entry) => buildDiffKey(entry.workspace_root_index, entry.path) === selectedDiffKey,
    ) ??
    compareEnvelope?.diff.files[0] ??
    null;

  const artifactRows = useMemo(() => {
    if (changedOnly) {
      return artifacts.map((artifact) => ({
        key: artifact.artifact_id,
        artifactId: artifact.artifact_id,
        checkpointId: artifact.latest_checkpoint_id,
        artifact,
        label: artifact.display_path,
        subtitle: `${artifact.change_kind} · ${artifact.version_count} version${artifact.version_count === 1 ? "" : "s"}`,
      }));
    }
    return artifacts.flatMap((artifact) =>
      artifact.versions.map((version) => ({
        key: `${artifact.artifact_id}:${version.artifact_id}`,
        artifactId: version.artifact_id,
        checkpointId: version.checkpoint_id,
        artifact,
        label: artifact.display_path,
        subtitle: `${version.change_kind} · checkpoint ${shortId(version.checkpoint_id)}`,
      })),
    );
  }, [artifacts, changedOnly]);

  const anchorOptions = useMemo(() => buildAnchorOptions(runId, runIds, checkpoints), [
    checkpoints,
    runId,
    runIds,
  ]);

  useEffect(() => {
    setWorkspaceEnvelope(null);
    setArtifactDetail(null);
    setSelectedArtifactId("");
    setSelectedCheckpointId("");
    setCheckpointEnvelope(null);
    setSelectedReportId("");
    setReportEnvelope(null);
    setLeftAnchor(`run:${runId}`);
    setRightAnchor("");
    setCompareEnvelope(null);
    setSelectedDiffKey("");
    setLastRestore(null);
  }, [runId]);

  useEffect(() => {
    if (!active) {
      return;
    }
    void refreshWorkspace();
  }, [active, deferredQuery, runId]);

  useEffect(() => {
    if (!active || selectedArtifactId.trim().length === 0) {
      setArtifactDetail(null);
      return;
    }
    void loadArtifact(selectedArtifactId);
  }, [active, selectedArtifactId, runId]);

  useEffect(() => {
    if (!active || selectedCheckpointId.trim().length === 0) {
      setCheckpointEnvelope(null);
      return;
    }
    void loadCheckpoint(selectedCheckpointId);
  }, [active, selectedCheckpointId]);

  useEffect(() => {
    if (!active || selectedReportId.trim().length === 0) {
      setReportEnvelope(null);
      return;
    }
    void loadReport(selectedReportId);
  }, [active, selectedReportId]);

  async function refreshWorkspace(): Promise<void> {
    setWorkspaceBusy(true);
    try {
      const response = await api.chatRunWorkspace(runId, {
        q: deferredQuery.trim() || undefined,
        limit: 256,
      });
      setWorkspaceEnvelope(response);
      setSelectedArtifactId((previous) =>
        previous &&
        response.workspace.artifacts.some(
          (artifact) =>
            artifact.artifact_id === previous ||
            artifact.versions.some((version) => version.artifact_id === previous),
        )
          ? previous
          : (response.workspace.artifacts[0]?.artifact_id ?? ""),
      );
      setSelectedCheckpointId((previous) =>
        previous &&
        response.workspace.workspace_checkpoints.some(
          (checkpoint) => checkpoint.checkpoint_id === previous,
        )
          ? previous
          : (response.workspace.workspace_checkpoints[0]?.checkpoint_id ?? ""),
      );
      setRightAnchor((previous) => {
        if (previous.trim().length > 0) {
          return previous;
        }
        const firstCheckpoint = response.workspace.workspace_checkpoints[0]?.checkpoint_id;
        if (firstCheckpoint) {
          return `checkpoint:${firstCheckpoint}`;
        }
        const alternateRun = runIds.find((candidate) => candidate !== runId);
        return alternateRun ? `run:${alternateRun}` : `run:${runId}`;
      });
    } catch (error) {
      setError(toMessage(error));
    } finally {
      setWorkspaceBusy(false);
    }
  }

  async function loadArtifact(artifactId: string): Promise<void> {
    setArtifactBusy(true);
    try {
      const response = await api.chatRunWorkspaceArtifact(runId, artifactId, {
        include_content: true,
      });
      setArtifactDetail(response.detail);
    } catch (error) {
      setArtifactDetail(null);
      setError(toMessage(error));
    } finally {
      setArtifactBusy(false);
    }
  }

  async function loadCheckpoint(checkpointId: string): Promise<void> {
    setCheckpointBusy(true);
    try {
      const response = await api.getWorkspaceCheckpoint(checkpointId);
      setCheckpointEnvelope(response);
      setSelectedReportId((previous) =>
        previous && response.restore_reports.some((report) => report.report_id === previous)
          ? previous
          : (response.restore_reports[0]?.report_id ?? ""),
      );
    } catch (error) {
      setCheckpointEnvelope(null);
      setError(toMessage(error));
    } finally {
      setCheckpointBusy(false);
    }
  }

  async function loadReport(reportId: string): Promise<void> {
    setReportBusy(true);
    try {
      setReportEnvelope(await api.getWorkspaceRestoreReport(reportId));
    } catch (error) {
      setReportEnvelope(null);
      setError(toMessage(error));
    } finally {
      setReportBusy(false);
    }
  }

  async function previewDiff(leftAnchorValue = leftAnchor, rightAnchorValue = rightAnchor): Promise<void> {
    const left = parseAnchor(leftAnchorValue);
    const right = parseAnchor(rightAnchorValue);
    if (left === null || right === null) {
      setError("Choose two workspace anchors before previewing rollback.");
      return;
    }
    setCompareBusy(true);
    try {
      const response = await api.compareWorkspace({
        left_run_id: left.kind === "run" ? left.id : undefined,
        left_checkpoint_id: left.kind === "checkpoint" ? left.id : undefined,
        right_run_id: right.kind === "run" ? right.id : undefined,
        right_checkpoint_id: right.kind === "checkpoint" ? right.id : undefined,
        limit: 64,
      });
      setCompareEnvelope(response);
      setSelectedDiffKey(
        response.diff.files[0]
          ? buildDiffKey(response.diff.files[0].workspace_root_index, response.diff.files[0].path)
          : "",
      );
      setNotice(
        response.diff.files_changed > 0
          ? `Rollback diff ready with ${response.diff.files_changed} changed path${response.diff.files_changed === 1 ? "" : "s"}.`
          : "No changed workspace paths were found between the selected anchors.",
      );
    } catch (error) {
      setError(toMessage(error));
    } finally {
      setCompareBusy(false);
    }
  }

  async function restore(scopeKind: "workspace" | "file"): Promise<void> {
    const checkpointId = selectedCheckpointId || selectedArtifact?.latest_checkpoint_id || "";
    if (!checkpointId) {
      setError("Select a workspace checkpoint before restoring.");
      return;
    }
    if (scopeKind === "file" && selectedArtifact === null) {
      setError("Select a workspace artifact before restoring a single file.");
      return;
    }
    setRestoreBusyKey(`${scopeKind}:${checkpointId}`);
    try {
      const response = await api.restoreWorkspaceCheckpoint(checkpointId, {
        branch_session: branchSession,
        scope_kind: scopeKind,
        target_path: scopeKind === "file" ? selectedArtifact?.path : undefined,
        target_workspace_root_index:
          scopeKind === "file" ? selectedArtifact?.workspace_root_index : undefined,
      });
      setLastRestore(response);
      setSelectedReportId(response.restore.report.report_id);
      await Promise.all([refreshWorkspace(), loadCheckpoint(checkpointId), loadReport(response.restore.report.report_id)]);
      await onWorkspaceRestore(response);
    } catch (error) {
      setError(toMessage(error));
    } finally {
      setRestoreBusyKey(null);
    }
  }

  async function promote(mode: "memory" | "named"): Promise<void> {
    if (artifactDetail === null || workspaceRun === null) {
      setError("Select a workspace artifact before promoting it.");
      return;
    }
    setPromotionBusyKey(`${mode}:${artifactDetail.artifact.artifact_id}`);
    try {
      const response = await api.writeWorkspaceDocument({
        path: buildDocumentPath(artifactDetail, workspaceRun, mode),
        title: `${basename(artifactDetail.artifact.path)} (${mode === "memory" ? "memory copy" : "named artifact"})`,
        content_text: buildDocumentContent(artifactDetail, workspaceRun, mode),
        session_id: workspaceRun.session_id,
        manual_override: true,
      });
      setNotice(`Workspace artifact stored at ${response.document.path}.`);
      if (mode === "memory") {
        openMemorySection();
      }
    } catch (error) {
      setError(toMessage(error));
    } finally {
      setPromotionBusyKey(null);
    }
  }

  async function attachSupport(sourceKey = artifactDetail?.artifact.artifact_id ?? "rollback"): Promise<void> {
    setPromotionBusyKey(`support:${sourceKey}`);
    try {
      const response = await api.createSupportBundleJob({ retain_jobs: 20 });
      setNotice(`Support bundle job ${response.job.job_id} queued for rollback analysis.`);
      openSupportSection();
    } catch (error) {
      setError(toMessage(error));
    } finally {
      setPromotionBusyKey(null);
    }
  }

  if (!active) {
    return null;
  }

  return (
    <div className="workspace-stack">
      <ActionCluster>
        <ActionButton isDisabled={workspaceBusy} type="button" variant="primary" onPress={() => void refreshWorkspace()}>
          {workspaceBusy ? "Refreshing..." : "Refresh workspace"}
        </ActionButton>
        <ActionButton
          isDisabled={artifactDetail === null}
          type="button"
          variant="secondary"
          onPress={() => {
            const blob = artifactDetail === null ? null : buildArtifactBlob(artifactDetail);
            if (blob === null || artifactDetail === null) {
              setError("This artifact is not available for inline download.");
              return;
            }
            downloadBlob(blob, basename(artifactDetail.artifact.path));
            setNotice(`Downloaded ${artifactDetail.artifact.display_path}.`);
          }}
        >
          Download artifact
        </ActionButton>
        <ActionButton
          isDisabled={!selectedCheckpointId}
          type="button"
          variant="secondary"
          onPress={() => void restore("workspace")}
        >
          {restoreBusyKey === `workspace:${selectedCheckpointId}` ? "Restoring..." : "Restore workspace"}
        </ActionButton>
        <ActionButton
          isDisabled={selectedArtifact === null}
          type="button"
          variant="secondary"
          onPress={() => void restore("file")}
        >
          {restoreBusyKey === `file:${selectedCheckpointId || selectedArtifact?.latest_checkpoint_id || ""}`
            ? "Restoring..."
            : "Restore selected file"}
        </ActionButton>
      </ActionCluster>

      <div className="workspace-inline-actions">
        <CheckboxField
          checked={changedOnly}
          description="Show only the latest changed paths instead of the full checkpointed version history."
          label="Changed files only"
          onChange={setChangedOnly}
        />
        <CheckboxField
          checked={branchSession}
          description="Create a new session branch before applying restore so the current session remains recoverable."
          label="Branch session before restore"
          onChange={setBranchSession}
        />
      </div>

      {workspaceRun !== null ? (
        <KeyValueList
          items={[
            { label: "Run", value: shortId(workspaceRun.run_id) },
            { label: "Session", value: shortId(workspaceRun.session_id) },
            { label: "Device", value: workspaceRun.device_id },
            { label: "Artifacts", value: artifacts.length },
            { label: "Workspace checkpoints", value: checkpoints.length },
            { label: "Background tasks", value: workspace?.background_tasks.length ?? 0 },
          ]}
        />
      ) : null}

      {lastRestore !== null ? (
        <InlineNotice title="Latest restore reconciliation" tone={lastRestore.restore.failed_paths.length > 0 ? "warning" : "success"}>
          <p>{lastRestore.restore.report.reconciliation_summary}</p>
          <p>{lastRestore.restore.report.reconciliation_prompt}</p>
          {lastRestore.project_context_refresh_error ? (
            <p>Project context refresh warning: {lastRestore.project_context_refresh_error}</p>
          ) : null}
          {lastRestore.project_context_copy_error ? (
            <p>Project context copy warning: {lastRestore.project_context_copy_error}</p>
          ) : null}
        </InlineNotice>
      ) : null}

      <section className="workspace-two-column">
        <SectionCard
          description="Search run-scoped artifacts and switch between the latest changed paths and the full checkpointed version history."
          title="Artifacts"
          variant="transparent"
        >
          <div className="workspace-stack">
            <TextInputField
              label="Search artifacts"
              placeholder="path, preview text, or moved-from path"
              value={query}
              onChange={setQuery}
            />
            {artifactRows.length === 0 ? (
              <EmptyState
                compact
                description={
                  workspaceBusy
                    ? "Workspace metadata is still loading."
                    : query.trim().length > 0
                      ? "No workspace artifacts matched the current search."
                      : "This run did not publish any workspace artifacts."
                }
                title="No workspace artifacts"
              />
            ) : (
              <div className="chat-tape-list">
                {artifactRows.map((row) => (
                  <article key={row.key} className="chat-ops-card">
                    <div className="chat-ops-card__copy">
                      <strong>{row.label}</strong>
                      <span>{row.subtitle}</span>
                    </div>
                    <div className="chat-ops-card__actions">
                      <ActionButton
                        isDisabled={artifactBusy && selectedArtifactId === row.artifactId}
                        size="sm"
                        type="button"
                        variant={selectedArtifactId === row.artifactId ? "primary" : "secondary"}
                        onPress={() => {
                          setSelectedArtifactId(row.artifactId);
                          setSelectedCheckpointId(row.checkpointId);
                        }}
                      >
                        {artifactBusy && selectedArtifactId === row.artifactId ? "Loading..." : "Preview"}
                      </ActionButton>
                      <ActionButton
                        size="sm"
                        type="button"
                        variant="ghost"
                        onPress={() => {
                          setSelectedCheckpointId(row.artifact.latest_checkpoint_id);
                          setRightAnchor(`checkpoint:${row.artifact.latest_checkpoint_id}`);
                        }}
                      >
                        Checkpoint
                      </ActionButton>
                    </div>
                  </article>
                ))}
              </div>
            )}
          </div>
        </SectionCard>

        <SectionCard
          description="Inline preview stays bounded and safe for text, JSON, HTML source, and images."
          title="Artifact preview"
          variant="transparent"
        >
          {artifactDetail === null ? (
            <EmptyState
              compact
              description="Choose a workspace artifact to inspect content, provenance, and promote actions."
              title="No artifact selected"
            />
          ) : (
            <div className="workspace-stack">
              <KeyValueList
                items={[
                  { label: "Path", value: artifactDetail.artifact.display_path },
                  { label: "Change", value: artifactDetail.artifact.change_kind },
                  { label: "Checkpoint", value: shortId(artifactDetail.checkpoint.checkpoint_id) },
                  { label: "Content type", value: artifactDetail.artifact.content_type },
                  { label: "Size", value: formatSize(artifactDetail.artifact.size_bytes) },
                  { label: "Hash", value: artifactDetail.artifact.content_sha256 ?? "n/a" },
                ]}
              />
              <ActionCluster>
                <ActionButton
                  isDisabled={promotionBusyKey !== null}
                  size="sm"
                  type="button"
                  variant="secondary"
                  onPress={() => void promote("memory")}
                >
                  {promotionBusyKey === `memory:${artifactDetail.artifact.artifact_id}` ? "Promoting..." : "Promote to Memory"}
                </ActionButton>
                <ActionButton
                  isDisabled={promotionBusyKey !== null}
                  size="sm"
                  type="button"
                  variant="secondary"
                  onPress={() => void promote("named")}
                >
                  {promotionBusyKey === `named:${artifactDetail.artifact.artifact_id}` ? "Promoting..." : "Mark as named artifact"}
                </ActionButton>
                <ActionButton
                  isDisabled={promotionBusyKey !== null}
                  size="sm"
                  type="button"
                  variant="secondary"
                  onPress={() => void attachSupport(artifactDetail.artifact.artifact_id)}
                >
                  {promotionBusyKey === `support:${artifactDetail.artifact.artifact_id}` ? "Queueing..." : "Attach to Support bundle"}
                </ActionButton>
                <ActionButton
                  size="sm"
                  type="button"
                  variant="ghost"
                  onPress={() => {
                    const canvasUrl = extractCanvasUrl(artifactDetail);
                    if (canvasUrl === null) {
                      setError(
                        "This artifact does not publish a reusable Canvas frame URL yet. Use Memory or Support until a Canvas target is available.",
                      );
                      return;
                    }
                    window.open(canvasUrl, "_blank", "noopener,noreferrer");
                    setNotice("Canvas frame opened in a new tab.");
                  }}
                >
                  Open in Canvas
                </ActionButton>
              </ActionCluster>
              <ArtifactPreview detail={artifactDetail} revealSensitiveValues={revealSensitiveValues} />
            </div>
          )}
        </SectionCard>
      </section>

      <section className="workspace-two-column">
        <SectionCard
          description="Preview rollback diffs between retry runs and workspace checkpoints before you restore anything."
          title="Rollback diff"
          variant="transparent"
        >
          <div className="workspace-stack">
            <div className="workspace-field-grid workspace-field-grid--double">
              <SelectField label="Left anchor" options={anchorOptions} value={leftAnchor} onChange={setLeftAnchor} />
              <SelectField label="Right anchor" options={anchorOptions} value={rightAnchor} onChange={setRightAnchor} />
            </div>
            <ActionCluster>
              <ActionButton isDisabled={compareBusy || !rightAnchor} size="sm" type="button" variant="primary" onPress={() => void previewDiff()}>
                {compareBusy ? "Comparing..." : "Preview rollback diff"}
              </ActionButton>
              <ActionButton
                isDisabled={compareEnvelope === null}
                size="sm"
                type="button"
                variant="secondary"
                onPress={() => {
                  if (compareEnvelope !== null) {
                    downloadBlob(
                      new Blob([JSON.stringify(compareEnvelope.diff, null, 2)], {
                        type: "application/json",
                      }),
                      `workspace-diff-${compareEnvelope.diff.left_anchor.id}-${compareEnvelope.diff.right_anchor.id}.json`,
                    );
                  }
                }}
              >
                Download diff JSON
              </ActionButton>
            </ActionCluster>
            {compareEnvelope === null ? (
              <EmptyState
                compact
                description="Select two anchors from the same session to inspect changed paths and text diffs."
                title="No diff loaded"
              />
            ) : compareEnvelope.diff.files.length === 0 ? (
              <InlineNotice title="No changed paths" tone="default">
                The selected workspace anchors resolved successfully, but there are no changed paths to preview.
              </InlineNotice>
            ) : (
              <div className="workspace-two-column">
                <div className="chat-tape-list">
                  {compareEnvelope.diff.files.map((file) => (
                    <article key={`${file.workspace_root_index}:${file.path}`} className="chat-ops-card">
                      <div className="chat-ops-card__copy">
                        <strong>{file.display_path}</strong>
                        <span>{file.diff_kind}</span>
                        <p>{file.left?.change_kind ?? "missing"} → {file.right?.change_kind ?? "missing"}</p>
                      </div>
                      <div className="chat-ops-card__actions">
                        <ActionButton
                          size="sm"
                          type="button"
                          variant={
                            selectedDiffKey === buildDiffKey(file.workspace_root_index, file.path)
                              ? "primary"
                              : "secondary"
                          }
                          onPress={() =>
                            setSelectedDiffKey(buildDiffKey(file.workspace_root_index, file.path))
                          }
                        >
                          Preview
                        </ActionButton>
                        <ActionButton
                          size="sm"
                          type="button"
                          variant="ghost"
                          onPress={() =>
                            focusArtifact(file, setSelectedArtifactId, setSelectedCheckpointId)
                          }
                        >
                          Open artifact
                        </ActionButton>
                      </div>
                    </article>
                  ))}
                </div>
                <div className="workspace-stack">
                  {selectedDiff !== null ? (
                    <>
                      <KeyValueList
                        items={[
                          { label: "Path", value: selectedDiff.display_path },
                          { label: "Diff kind", value: selectedDiff.diff_kind },
                          { label: "Left hash", value: selectedDiff.left?.content_sha256 ?? "none" },
                          { label: "Right hash", value: selectedDiff.right?.content_sha256 ?? "none" },
                        ]}
                      />
                      {selectedDiff.diff_text ? (
                        <pre className="workspace-code-panel">{selectedDiff.diff_text}</pre>
                      ) : (
                        <InlineNotice title="Metadata-only diff" tone="default">
                          Binary or metadata-only changes are available here without rendering raw content inline.
                        </InlineNotice>
                      )}
                    </>
                  ) : null}
                </div>
              </div>
            )}
          </div>
        </SectionCard>

        <SectionCard
          description="Workspace checkpoints stay separate from conversation checkpoints so rollback remains explicit and file-scoped."
          title="Workspace checkpoints"
          variant="transparent"
        >
          {checkpoints.length === 0 ? (
            <EmptyState
              compact
              description="Mutating tool calls create workspace checkpoints once they change tracked files."
              title="No workspace checkpoints"
            />
          ) : (
            <div className="workspace-stack">
              <div className="chat-tape-list">
                {checkpoints.map((checkpoint) => (
                  <article key={checkpoint.checkpoint_id} className="chat-ops-card">
                    <div className="chat-ops-card__copy">
                      <strong>{checkpoint.source_label}</strong>
                      <span>{checkpoint.tool_name ?? checkpoint.source_kind} · {shortId(checkpoint.checkpoint_id)}</span>
                      <p>{checkpoint.summary_text}</p>
                    </div>
                    <div className="chat-ops-card__actions">
                      <ActionButton
                        isDisabled={checkpointBusy && selectedCheckpointId === checkpoint.checkpoint_id}
                        size="sm"
                        type="button"
                        variant={selectedCheckpointId === checkpoint.checkpoint_id ? "primary" : "secondary"}
                        onPress={() => setSelectedCheckpointId(checkpoint.checkpoint_id)}
                      >
                        {checkpointBusy && selectedCheckpointId === checkpoint.checkpoint_id ? "Loading..." : "Inspect"}
                      </ActionButton>
                      <ActionButton
                        size="sm"
                        type="button"
                        variant="ghost"
                        onPress={() => {
                          const nextRightAnchor = `checkpoint:${checkpoint.checkpoint_id}`;
                          setSelectedCheckpointId(checkpoint.checkpoint_id);
                          setRightAnchor(nextRightAnchor);
                          void previewDiff(leftAnchor, nextRightAnchor);
                        }}
                      >
                        Diff
                      </ActionButton>
                    </div>
                  </article>
                ))}
              </div>
              {checkpointEnvelope !== null ? (
                <div className="workspace-stack">
                  <KeyValueList
                    items={[
                      { label: "Checkpoint", value: shortId(checkpointEnvelope.checkpoint.checkpoint_id) },
                      { label: "Run", value: shortId(checkpointEnvelope.checkpoint.run_id) },
                      { label: "Device", value: checkpointEnvelope.checkpoint.device_id },
                      { label: "Tool", value: checkpointEnvelope.checkpoint.tool_name ?? checkpointEnvelope.checkpoint.source_kind },
                      { label: "Restore count", value: checkpointEnvelope.checkpoint.restore_count },
                    ]}
                  />
                  <PrettyJsonBlock
                    revealSensitiveValues={revealSensitiveValues}
                    value={safeParseJson(checkpointEnvelope.checkpoint.diff_summary_json)}
                  />
                  <ActionCluster>
                    <ActionButton
                      isDisabled={restoreBusyKey !== null}
                      size="sm"
                      type="button"
                      variant="primary"
                      onPress={() => void restore("workspace")}
                    >
                      {restoreBusyKey === `workspace:${checkpointEnvelope.checkpoint.checkpoint_id}` ? "Restoring..." : "Restore workspace"}
                    </ActionButton>
                    <ActionButton
                      isDisabled={selectedArtifact === null || restoreBusyKey !== null}
                      size="sm"
                      type="button"
                      variant="secondary"
                      onPress={() => void restore("file")}
                    >
                      {restoreBusyKey === `file:${checkpointEnvelope.checkpoint.checkpoint_id}` ? "Restoring..." : "Restore selected file"}
                    </ActionButton>
                    <ActionButton size="sm" type="button" variant="ghost" onPress={() => onOpenRun(checkpointEnvelope.checkpoint.run_id, "workspace")}>
                      Open run
                    </ActionButton>
                  </ActionCluster>
                  <ActionCluster>
                    {workspace?.compactions
                      .filter((artifact) => artifact.run_id === checkpointEnvelope.checkpoint.run_id)
                      .slice(0, 3)
                      .map((artifact) => (
                        <ActionButton key={artifact.artifact_id} size="sm" type="button" variant="secondary" onPress={() => onInspectCompaction(artifact.artifact_id)}>
                          Compaction {shortId(artifact.artifact_id)}
                        </ActionButton>
                      ))}
                    {workspace?.session_checkpoints
                      .filter((checkpoint) => checkpoint.run_id === checkpointEnvelope.checkpoint.run_id)
                      .slice(0, 3)
                      .map((checkpoint) => (
                        <ActionButton key={checkpoint.checkpoint_id} size="sm" type="button" variant="secondary" onPress={() => onInspectSessionCheckpoint(checkpoint.checkpoint_id)}>
                          Session checkpoint {shortId(checkpoint.checkpoint_id)}
                        </ActionButton>
                      ))}
                  </ActionCluster>
                </div>
              ) : null}
            </div>
          )}
        </SectionCard>
      </section>

      {checkpointEnvelope !== null ? (
        <SectionCard
          description="Restore reports make partial failures and reconciliation safe to hand off to support."
          title="Restore reports"
          variant="transparent"
        >
          {checkpointEnvelope.restore_reports.length === 0 ? (
            <EmptyState
              compact
              description="Restore reports appear after a workspace or file restore attempt."
              title="No restore reports"
            />
          ) : (
            <div className="workspace-two-column">
              <div className="chat-tape-list">
                {checkpointEnvelope.restore_reports.map((report) => (
                  <article key={report.report_id} className="chat-ops-card">
                    <div className="chat-ops-card__copy">
                      <strong>{report.result_state}</strong>
                      <span>{report.scope_kind} · {shortId(report.report_id)}</span>
                      <p>{report.reconciliation_summary}</p>
                    </div>
                    <div className="chat-ops-card__actions">
                      <ActionButton
                        isDisabled={reportBusy && selectedReportId === report.report_id}
                        size="sm"
                        type="button"
                        variant={selectedReportId === report.report_id ? "primary" : "secondary"}
                        onPress={() => setSelectedReportId(report.report_id)}
                      >
                        {reportBusy && selectedReportId === report.report_id ? "Loading..." : "Inspect"}
                      </ActionButton>
                    </div>
                  </article>
                ))}
              </div>
              <div className="workspace-stack">
                {reportEnvelope !== null ? (
                  <>
                    <KeyValueList
                      items={[
                        { label: "Report", value: shortId(reportEnvelope.detail.report.report_id) },
                        { label: "Checkpoint", value: shortId(reportEnvelope.detail.report.checkpoint_id) },
                        { label: "Scope", value: reportEnvelope.detail.report.scope_kind },
                        { label: "Result", value: reportEnvelope.detail.report.result_state },
                        { label: "Restored paths", value: reportEnvelope.detail.restored_paths.length },
                        { label: "Failed paths", value: reportEnvelope.detail.failed_paths.length },
                      ]}
                    />
                    <InlineNotice title="Reconciliation summary" tone={reportEnvelope.detail.failed_paths.length > 0 ? "warning" : "success"}>
                      <p>{reportEnvelope.detail.report.reconciliation_summary}</p>
                      <p>{reportEnvelope.detail.report.reconciliation_prompt}</p>
                    </InlineNotice>
                    <ActionCluster>
                      <ActionButton
                        size="sm"
                        type="button"
                        variant="primary"
                        onPress={() =>
                          downloadBlob(
                            new Blob([JSON.stringify(reportEnvelope.detail, null, 2)], {
                              type: "application/json",
                            }),
                            `workspace-restore-report-${reportEnvelope.detail.report.report_id}.json`,
                          )
                        }
                      >
                        Download restore report
                      </ActionButton>
                      <ActionButton
                        size="sm"
                        type="button"
                        variant="secondary"
                        onPress={() => void attachSupport(reportEnvelope.detail.report.report_id)}
                      >
                        Attach to Support bundle
                      </ActionButton>
                    </ActionCluster>
                    {reportEnvelope.detail.failed_paths.length > 0 ? (
                      <ul className="workspace-bullet-list">
                        {reportEnvelope.detail.failed_paths.map((entry) => (
                          <li key={`${entry.workspace_root_index}:${entry.path}`}>
                            {entry.display_path}: {entry.error}
                          </li>
                        ))}
                      </ul>
                    ) : null}
                  </>
                ) : null}
              </div>
            </div>
          )}
        </SectionCard>
      ) : null}
    </div>
  );
}

function buildAnchorOptions(
  runId: string,
  runIds: readonly string[],
  checkpoints: readonly WorkspaceCheckpointSummary[],
): AnchorOption[] {
  const options: AnchorOption[] = [
    {
      key: `run:${runId}`,
      label: `Current run ${shortId(runId)}`,
      description: "The run currently open in the inspector.",
    },
  ];
  for (const candidate of runIds) {
    if (candidate !== runId) {
      options.push({
        key: `run:${candidate}`,
        label: `Run ${shortId(candidate)}`,
        description: "Another run from the same session lineage.",
      });
    }
  }
  for (const checkpoint of checkpoints) {
    options.push({
      key: `checkpoint:${checkpoint.checkpoint_id}`,
      label: `${checkpoint.source_label} · ${shortId(checkpoint.checkpoint_id)}`,
      description: checkpoint.summary_text,
    });
  }
  return options;
}

function parseAnchor(value: string): { kind: "run" | "checkpoint"; id: string } | null {
  const [kind, ...rest] = value.split(":");
  const id = rest.join(":").trim();
  return (kind === "run" || kind === "checkpoint") && id ? { kind, id } : null;
}

function buildDocumentPath(
  detail: WorkspaceArtifactDetail,
  run: ChatRunStatusRecord,
  mode: "memory" | "named",
): string {
  const safePath = detail.artifact.path
    .replaceAll("\\", "/")
    .split("/")
    .filter(Boolean)
    .map((segment) => segment.replace(/[^A-Za-z0-9._-]/g, "_"))
    .join("/");
  return mode === "memory"
    ? `runs/${run.session_id}/${run.run_id}/workspace/${safePath}.artifact.md`
    : `artifacts/named/${run.run_id}/${safePath}.artifact.md`;
}

function buildDocumentContent(
  detail: WorkspaceArtifactDetail,
  run: ChatRunStatusRecord,
  mode: "memory" | "named",
): string {
  return [
    "---",
    `promotion_target: ${mode}`,
    `run_id: ${run.run_id}`,
    `session_id: ${run.session_id}`,
    `device_id: ${run.device_id}`,
    `artifact_id: ${detail.artifact.artifact_id}`,
    `checkpoint_id: ${detail.checkpoint.checkpoint_id}`,
    `path: ${detail.artifact.path}`,
    `content_type: ${detail.artifact.content_type}`,
    `content_sha256: ${detail.artifact.content_sha256 ?? "n/a"}`,
    "---",
    "",
    detail.text_content ||
      detail.artifact.preview_text ||
      "Binary artifact. Raw bytes were intentionally omitted from this promoted document.",
  ].join("\n");
}

function basename(path: string): string {
  return path.replaceAll("\\", "/").split("/").filter(Boolean).pop() ?? "artifact";
}

function buildArtifactBlob(detail: WorkspaceArtifactDetail): Blob | null {
  if (!detail.content_available) {
    return null;
  }
  if (detail.text_content !== undefined) {
    return new Blob([detail.text_content], {
      type: detail.artifact.content_type || "text/plain; charset=utf-8",
    });
  }
  if (detail.content_base64 === undefined) {
    return null;
  }
  const buffer = decodeBase64(detail.content_base64);
  if (buffer === null) {
    return null;
  }
  return new Blob([buffer], {
    type: detail.artifact.content_type || "application/octet-stream",
  });
}

function downloadBlob(blob: Blob, filename: string): void {
  const href = URL.createObjectURL(blob);
  const anchor = document.createElement("a");
  anchor.href = href;
  anchor.download = filename;
  anchor.click();
  URL.revokeObjectURL(href);
}

function extractCanvasUrl(detail: WorkspaceArtifactDetail): string | null {
  const candidates = [detail.text_content, detail.artifact.preview_text]
    .filter((value): value is string => value !== undefined && value.trim().length > 0)
    .flatMap((value) => collectCanvasFrameUrls(safeParseJson(value)));
  return candidates[0] ?? null;
}

function focusArtifact(
  file: WorkspaceDiffFileRecord,
  setSelectedArtifactId: (value: string) => void,
  setSelectedCheckpointId: (value: string) => void,
): void {
  const artifactId = file.right?.artifact_id ?? file.left?.artifact_id;
  const checkpointId = file.right?.checkpoint_id ?? file.left?.checkpoint_id;
  if (artifactId) {
    setSelectedArtifactId(artifactId);
  }
  if (checkpointId) {
    setSelectedCheckpointId(checkpointId);
  }
}

function safeParseJson(value: string): JsonValue {
  try {
    return JSON.parse(value) as JsonValue;
  } catch {
    return value;
  }
}

function formatSize(sizeBytes?: number): string {
  if (sizeBytes === undefined) {
    return "n/a";
  }
  if (sizeBytes < 1024) {
    return `${sizeBytes} B`;
  }
  if (sizeBytes < 1024 * 1024) {
    return `${(sizeBytes / 1024).toFixed(1)} KiB`;
  }
  return `${(sizeBytes / (1024 * 1024)).toFixed(1)} MiB`;
}

function ArtifactPreview({
  detail,
  revealSensitiveValues,
}: {
  detail: WorkspaceArtifactDetail;
  revealSensitiveValues: boolean;
}) {
  if (!detail.content_available) {
    return (
      <InlineNotice title="Preview unavailable" tone="default">
        Inline preview is unavailable for this artifact. Download it or inspect the metadata.
      </InlineNotice>
    );
  }

  if (detail.artifact.preview_kind === "image" && detail.content_base64) {
    return (
      <div className="workspace-stack">
        <img
          alt={detail.artifact.display_path}
          className="max-h-[28rem] w-full rounded-xl border border-default-200 bg-content1 object-contain"
          src={`data:${detail.artifact.content_type};base64,${detail.content_base64}`}
        />
        {detail.content_truncated ? (
          <InlineNotice title="Preview truncated" tone="warning">
            The inline image preview is bounded for safety. Download the artifact for the full file.
          </InlineNotice>
        ) : null}
      </div>
    );
  }

  if (detail.text_content !== undefined) {
    const looksLikeJson =
      detail.artifact.content_type === "application/json" || basename(detail.artifact.path).endsWith(".json");
    return (
      <div className="workspace-stack">
        {looksLikeJson ? (
          <PrettyJsonBlock
            revealSensitiveValues={revealSensitiveValues}
            value={safeParseJson(detail.text_content)}
          />
        ) : (
          <pre className="workspace-code-panel">{detail.text_content}</pre>
        )}
        {detail.content_truncated ? (
          <InlineNotice title="Preview truncated" tone="warning">
            Inline text preview is capped to keep large workspace artifacts responsive.
          </InlineNotice>
        ) : null}
      </div>
    );
  }

  if (detail.artifact.preview_text) {
    return (
      <div className="workspace-stack">
        <pre className="workspace-code-panel">{detail.artifact.preview_text}</pre>
        <InlineNotice title="Metadata fallback" tone="default">
          This artifact does not publish safe inline bytes, so only the recorded preview text is shown.
        </InlineNotice>
      </div>
    );
  }

  return (
    <InlineNotice title="Binary artifact" tone="default">
      Binary artifacts stay metadata-only unless the workspace API marks them safe for inline preview.
    </InlineNotice>
  );
}

function decodeBase64(value: string): ArrayBuffer | null {
  if (typeof globalThis.atob !== "function") {
    return null;
  }
  try {
    const decoded = globalThis.atob(value);
    const buffer = new ArrayBuffer(decoded.length);
    const bytes = new Uint8Array(buffer);
    for (let index = 0; index < decoded.length; index += 1) {
      bytes[index] = decoded.charCodeAt(index);
    }
    return buffer;
  } catch {
    return null;
  }
}

function buildDiffKey(workspaceRootIndex: number, path: string): string {
  return `${workspaceRootIndex}:${path}`;
}

function toMessage(error: unknown): string {
  return error instanceof Error ? error.message : "Request failed.";
}
