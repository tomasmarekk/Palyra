import { useState } from "react";

import { WorkspaceMetricCard, WorkspacePageHeader, WorkspaceSectionCard, WorkspaceStatusChip } from "../components/workspace/WorkspaceChrome";
import { WorkspaceEmptyState, WorkspaceInlineNotice, WorkspaceTable, workspaceToneForState } from "../components/workspace/WorkspacePatterns";
import { formatUnixMs, PrettyJsonBlock, readNumber, readString, toStringArray, type JsonObject } from "../shared";
import type { ConsoleAppState } from "../useConsoleAppState";

type ConfigTab = "inspect" | "validate" | "mutate" | "recover";

type ConfigSectionProps = {
  app: Pick<
    ConsoleAppState,
    | "configBusy"
    | "configInspectPath"
    | "setConfigInspectPath"
    | "configBackups"
    | "setConfigBackups"
    | "configMutationMode"
    | "setConfigMutationMode"
    | "configInspectSnapshot"
    | "configMutationKey"
    | "setConfigMutationKey"
    | "configMutationValue"
    | "setConfigMutationValue"
    | "configValidation"
    | "configLastMutation"
    | "configDiffPreview"
    | "configRecoverBackup"
    | "setConfigRecoverBackup"
    | "configDeploymentPosture"
    | "revealSensitiveValues"
    | "refreshConfigSurface"
    | "inspectConfigSurface"
    | "validateConfigSurface"
    | "mutateConfigSurface"
    | "migrateConfigSurface"
    | "recoverConfigSurface"
  >;
};

export function ConfigSection({ app }: ConfigSectionProps) {
  const [activeTab, setActiveTab] = useState<ConfigTab>("inspect");
  const warnings = toStringArray(Array.isArray(app.configDeploymentPosture?.warnings) ? app.configDeploymentPosture.warnings : []);
  const backups = Array.isArray(app.configInspectSnapshot?.backups)
    ? app.configInspectSnapshot.backups.filter((entry): entry is JsonObject => typeof entry === "object" && entry !== null && !Array.isArray(entry))
    : [];

  return (
    <main className="workspace-page">
      <WorkspacePageHeader
        eyebrow="Settings"
        title="Config"
        description="Inspect first, validate second, mutate deliberately, and keep recovery explicit. Secrets now live on their own page."
        status={
          <>
            <WorkspaceStatusChip tone={workspaceToneForState(app.configValidation?.valid === true ? "valid" : warnings.length > 0 ? "warning" : "unknown")}>
              {app.configValidation?.valid === true ? "Validation ready" : "Check config"}
            </WorkspaceStatusChip>
            <WorkspaceStatusChip tone={warnings.length > 0 ? "warning" : "default"}>
              {warnings.length} warnings
            </WorkspaceStatusChip>
          </>
        }
        actions={(
          <button type="button" onClick={() => void app.refreshConfigSurface()} disabled={app.configBusy}>
            {app.configBusy ? "Refreshing..." : "Refresh config"}
          </button>
        )}
      />

      <section className="workspace-metric-grid workspace-metric-grid--compact">
        <WorkspaceMetricCard label="Source path" value={(readString(app.configInspectSnapshot ?? {}, "source_path") ?? app.configInspectPath) || "n/a"} detail="Canonical config path currently being inspected." />
        <WorkspaceMetricCard label="Backups" value={backups.length} detail="Retained backups visible for recovery flows." tone={backups.length > 0 ? "accent" : "default"} />
        <WorkspaceMetricCard label="Last mutation" value={readString(app.configLastMutation ?? {}, "operation") ?? "none"} detail={readString(app.configLastMutation ?? {}, "changed_key") ?? "No recent mutation loaded."} tone={app.configLastMutation === null ? "default" : "warning"} />
      </section>

      {warnings.length > 0 ? (
        <WorkspaceInlineNotice title="Deployment warnings" tone="warning">
          <ul className="console-compact-list">{warnings.map((warning) => <li key={warning}>{warning}</li>)}</ul>
        </WorkspaceInlineNotice>
      ) : null}

      <div className="workspace-inline">
        {(["inspect", "validate", "mutate", "recover"] as const).map((tab) => (
          <button key={tab} type="button" className={`workspace-tab-button${activeTab === tab ? " is-active" : ""}`} onClick={() => setActiveTab(tab)}>
            {tab[0].toUpperCase() + tab.slice(1)}
          </button>
        ))}
      </div>

      {activeTab === "inspect" && (
        <section className="workspace-two-column">
          <WorkspaceSectionCard title="Inspect and migrate" description="Read the current document safely and keep migration as an explicit operator action.">
            <div className="workspace-stack">
              <div className="workspace-form-grid">
                <label>Path<input value={app.configInspectPath} onChange={(event) => app.setConfigInspectPath(event.target.value)} /></label>
                <label>Backups<input value={app.configBackups} onChange={(event) => app.setConfigBackups(event.target.value)} /></label>
              </div>
              <div className="workspace-inline">
                <button type="button" onClick={() => void app.inspectConfigSurface()} disabled={app.configBusy}>Inspect</button>
                <button type="button" className="secondary" onClick={() => void app.migrateConfigSurface()} disabled={app.configBusy}>Migrate</button>
              </div>
            </div>
          </WorkspaceSectionCard>

          <WorkspaceSectionCard title="Redacted snapshot" description="Document view stays redacted by default and no longer competes with secrets management on the same page.">
            {app.configInspectSnapshot === null ? (
              <WorkspaceEmptyState title="No snapshot loaded" description="Run inspect to load the current config document." compact />
            ) : (
              <PrettyJsonBlock value={app.configInspectSnapshot} revealSensitiveValues={app.revealSensitiveValues} />
            )}
          </WorkspaceSectionCard>
        </section>
      )}

      {activeTab === "validate" && (
        <section className="workspace-two-column">
          <WorkspaceSectionCard title="Validation" description="Keep validation result obvious before any mutation or recovery step.">
            <div className="workspace-inline">
              <button type="button" onClick={() => void app.validateConfigSurface()} disabled={app.configBusy}>Validate</button>
            </div>
            <dl className="workspace-key-value-grid">
              <div><dt>Status</dt><dd>{app.configValidation?.valid === true ? "valid" : app.configValidation === null ? "not loaded" : "needs review"}</dd></div>
              <div><dt>Version</dt><dd>{readString(app.configValidation ?? {}, "config_version") ?? "n/a"}</dd></div>
              <div><dt>Migrated from</dt><dd>{readString(app.configValidation ?? {}, "migrated_from_version") ?? "n/a"}</dd></div>
              <div><dt>Path</dt><dd>{readString(app.configValidation ?? {}, "source_path") ?? app.configInspectPath}</dd></div>
            </dl>
          </WorkspaceSectionCard>

          <WorkspaceSectionCard title="Validation payload" description="Raw payload stays secondary to the human-readable summary.">
            {app.configValidation === null ? (
              <WorkspaceEmptyState title="No validation payload" description="Run validate to load the backend validation result." compact />
            ) : (
              <PrettyJsonBlock value={app.configValidation} revealSensitiveValues={app.revealSensitiveValues} />
            )}
          </WorkspaceSectionCard>
        </section>
      )}

      {activeTab === "mutate" && (
        <section className="workspace-two-column">
          <WorkspaceSectionCard title="Mutate config" description="Keep mutation small and explicit, with mode, key, and value separated from the rest of the page.">
            <div className="workspace-stack">
              <div className="workspace-form-grid">
                <label>Mode<select value={app.configMutationMode} onChange={(event) => app.setConfigMutationMode(event.target.value === "unset" ? "unset" : "set")}><option value="set">set</option><option value="unset">unset</option></select></label>
                <label>Key<input value={app.configMutationKey} onChange={(event) => app.setConfigMutationKey(event.target.value)} /></label>
                <label>Value<input value={app.configMutationValue} onChange={(event) => app.setConfigMutationValue(event.target.value)} disabled={app.configMutationMode === "unset"} placeholder={app.configMutationMode === "unset" ? "Value unused for unset" : "\"value\""} /></label>
              </div>
              <div className="workspace-inline">
                <button type="button" onClick={() => void app.mutateConfigSurface()} disabled={app.configBusy}>{app.configMutationMode === "unset" ? "Unset key" : "Apply mutation"}</button>
              </div>
            </div>
          </WorkspaceSectionCard>

          <WorkspaceSectionCard title="Mutation output" description="Review the last backend mutation and its redacted diff before moving on.">
            <dl className="workspace-key-value-grid">
              <div><dt>Operation</dt><dd>{readString(app.configLastMutation ?? {}, "operation") ?? "n/a"}</dd></div>
              <div><dt>Changed key</dt><dd>{readString(app.configLastMutation ?? {}, "changed_key") ?? "n/a"}</dd></div>
              <div><dt>Backups retained</dt><dd>{readString(app.configLastMutation ?? {}, "backups_retained") ?? "n/a"}</dd></div>
              <div><dt>Updated</dt><dd>{readString(app.configLastMutation ?? {}, "source_path") ?? app.configInspectPath}</dd></div>
            </dl>
            {app.configDiffPreview !== null ? (
              <pre className="workspace-code-panel"><code>{app.configDiffPreview}</code></pre>
            ) : (
              <WorkspaceEmptyState title="No diff preview" description="A redacted diff appears here after a mutation or migration." compact />
            )}
          </WorkspaceSectionCard>
        </section>
      )}

      {activeTab === "recover" && (
        <section className="workspace-two-column">
          <WorkspaceSectionCard title="Recover from backup" description="Recovery stays visibly risky and grounded in the actual retained backup list.">
            <div className="workspace-stack">
              <div className="workspace-form-grid">
                <label>Backup index<input value={app.configRecoverBackup} onChange={(event) => app.setConfigRecoverBackup(event.target.value)} /></label>
                <label>Backups retained<input value={app.configBackups} onChange={(event) => app.setConfigBackups(event.target.value)} /></label>
              </div>
              <div className="workspace-inline">
                <button type="button" className="button--warn" onClick={() => void app.recoverConfigSurface()} disabled={app.configBusy}>Recover backup</button>
              </div>
            </div>
          </WorkspaceSectionCard>

          <WorkspaceSectionCard title="Available backups" description="Use the published backup records instead of guessing restore targets.">
            {backups.length === 0 ? (
              <WorkspaceEmptyState title="No backups published" description="Inspect config with retained backups to populate recovery targets." compact />
            ) : (
              <WorkspaceTable ariaLabel="Config backups" columns={["Index", "Path", "Exists", "Updated"]}>
                {backups.map((backup) => (
                  <tr key={readString(backup, "path") ?? String(readNumber(backup, "index") ?? 0)}>
                    <td>{readString(backup, "index") ?? "n/a"}</td>
                    <td>{readString(backup, "path") ?? "n/a"}</td>
                    <td>{readString(backup, "exists") ?? "n/a"}</td>
                    <td>{formatUnixMs(readNumber(backup, "updated_at_unix_ms"))}</td>
                  </tr>
                ))}
              </WorkspaceTable>
            )}
          </WorkspaceSectionCard>
        </section>
      )}
    </main>
  );
}
