import { Button, ButtonGroup } from "@heroui/react";
import { useState } from "react";

import {
  ActionButton,
  SelectField,
  TextInputField
} from "../components/ui";
import {
  WorkspaceMetricCard,
  WorkspacePageHeader,
  WorkspaceSectionCard,
  WorkspaceStatusChip
} from "../components/workspace/WorkspaceChrome";
import {
  WorkspaceEmptyState,
  WorkspaceInlineNotice,
  WorkspaceTable,
  workspaceToneForState
} from "../components/workspace/WorkspacePatterns";
import {
  formatUnixMs,
  PrettyJsonBlock,
  readNumber,
  readString,
  toStringArray,
  type JsonObject
} from "../shared";
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
  const warnings = toStringArray(
    Array.isArray(app.configDeploymentPosture?.warnings)
      ? app.configDeploymentPosture.warnings
      : []
  );
  const backups = Array.isArray(app.configInspectSnapshot?.backups)
    ? app.configInspectSnapshot.backups.filter(
        (entry): entry is JsonObject =>
          typeof entry === "object" && entry !== null && !Array.isArray(entry)
      )
    : [];

  return (
    <main className="workspace-page">
      <WorkspacePageHeader
        eyebrow="Settings"
        title="Config"
        description="Inspect first, validate second, mutate deliberately, and keep recovery explicit. Secrets now live on their own page."
        status={
          <>
            <WorkspaceStatusChip
              tone={workspaceToneForState(
                app.configValidation?.valid === true
                  ? "valid"
                  : warnings.length > 0
                    ? "warning"
                    : "unknown"
              )}
            >
              {app.configValidation?.valid === true ? "Validation ready" : "Check config"}
            </WorkspaceStatusChip>
            <WorkspaceStatusChip tone={warnings.length > 0 ? "warning" : "default"}>
              {warnings.length} warnings
            </WorkspaceStatusChip>
          </>
        }
        actions={
          <ActionButton
            isDisabled={app.configBusy}
            type="button"
            variant="primary"
            onPress={() => void app.refreshConfigSurface()}
          >
            {app.configBusy ? "Refreshing..." : "Refresh config"}
          </ActionButton>
        }
      />

      <section className="workspace-metric-grid workspace-metric-grid--compact">
        <WorkspaceMetricCard
          detail="Canonical config path currently being inspected."
          label="Source path"
          value={(readString(app.configInspectSnapshot ?? {}, "source_path") ?? app.configInspectPath) || "n/a"}
        />
        <WorkspaceMetricCard
          detail="Retained backups visible for recovery flows."
          label="Backups"
          tone={backups.length > 0 ? "accent" : "default"}
          value={backups.length}
        />
        <WorkspaceMetricCard
          detail={readString(app.configLastMutation ?? {}, "changed_key") ?? "No recent mutation loaded."}
          label="Last mutation"
          tone={app.configLastMutation === null ? "default" : "warning"}
          value={readString(app.configLastMutation ?? {}, "operation") ?? "none"}
        />
      </section>

      {warnings.length > 0 ? (
        <WorkspaceInlineNotice title="Deployment warnings" tone="warning">
          <ul className="console-compact-list">
            {warnings.map((warning) => (
              <li key={warning}>{warning}</li>
            ))}
          </ul>
        </WorkspaceInlineNotice>
      ) : null}

      <ButtonGroup className="workspace-inline">
        {(["inspect", "validate", "mutate", "recover"] as const).map((tab) => (
          <Button
            key={tab}
            type="button"
            variant={activeTab === tab ? "secondary" : "ghost"}
            onPress={() => setActiveTab(tab)}
          >
            {tab[0].toUpperCase() + tab.slice(1)}
          </Button>
        ))}
      </ButtonGroup>

      {activeTab === "inspect" && (
        <section className="workspace-two-column">
          <WorkspaceSectionCard
            description="Read the current document safely and keep migration as an explicit operator action."
            title="Inspect and migrate"
          >
            <div className="workspace-stack">
              <div className="workspace-form-grid">
                <TextInputField
                  label="Path"
                  value={app.configInspectPath}
                  onChange={app.setConfigInspectPath}
                />
                <TextInputField
                  label="Backups"
                  value={app.configBackups}
                  onChange={app.setConfigBackups}
                />
              </div>
              <div className="workspace-inline">
                <ActionButton
                  isDisabled={app.configBusy}
                  type="button"
                  variant="primary"
                  onPress={() => void app.inspectConfigSurface()}
                >
                  Inspect
                </ActionButton>
                <ActionButton
                  isDisabled={app.configBusy}
                  type="button"
                  variant="secondary"
                  onPress={() => void app.migrateConfigSurface()}
                >
                  Migrate
                </ActionButton>
              </div>
            </div>
          </WorkspaceSectionCard>

          <WorkspaceSectionCard
            description="Document view stays redacted by default and no longer competes with secrets management on the same page."
            title="Redacted snapshot"
          >
            {app.configInspectSnapshot === null ? (
              <WorkspaceEmptyState
                compact
                description="Run inspect to load the current config document."
                title="No snapshot loaded"
              />
            ) : (
              <PrettyJsonBlock
                revealSensitiveValues={app.revealSensitiveValues}
                value={app.configInspectSnapshot}
              />
            )}
          </WorkspaceSectionCard>
        </section>
      )}

      {activeTab === "validate" && (
        <section className="workspace-two-column">
          <WorkspaceSectionCard
            description="Keep validation result obvious before any mutation or recovery step."
            title="Validation"
          >
            <div className="workspace-inline">
              <ActionButton
                isDisabled={app.configBusy}
                type="button"
                variant="primary"
                onPress={() => void app.validateConfigSurface()}
              >
                Validate
              </ActionButton>
            </div>
            <dl className="workspace-key-value-grid">
              <div>
                <dt>Status</dt>
                <dd>
                  {app.configValidation?.valid === true
                    ? "valid"
                    : app.configValidation === null
                      ? "not loaded"
                      : "needs review"}
                </dd>
              </div>
              <div>
                <dt>Version</dt>
                <dd>{readString(app.configValidation ?? {}, "config_version") ?? "n/a"}</dd>
              </div>
              <div>
                <dt>Migrated from</dt>
                <dd>{readString(app.configValidation ?? {}, "migrated_from_version") ?? "n/a"}</dd>
              </div>
              <div>
                <dt>Path</dt>
                <dd>{readString(app.configValidation ?? {}, "source_path") ?? app.configInspectPath}</dd>
              </div>
            </dl>
          </WorkspaceSectionCard>

          <WorkspaceSectionCard
            description="Raw payload stays secondary to the human-readable summary."
            title="Validation payload"
          >
            {app.configValidation === null ? (
              <WorkspaceEmptyState
                compact
                description="Run validate to load the backend validation result."
                title="No validation payload"
              />
            ) : (
              <PrettyJsonBlock
                revealSensitiveValues={app.revealSensitiveValues}
                value={app.configValidation}
              />
            )}
          </WorkspaceSectionCard>
        </section>
      )}

      {activeTab === "mutate" && (
        <section className="workspace-two-column">
          <WorkspaceSectionCard
            description="Keep mutation small and explicit, with mode, key, and value separated from the rest of the page."
            title="Mutate config"
          >
            <div className="workspace-stack">
              <div className="workspace-form-grid">
                <SelectField
                  label="Mode"
                  options={[
                    { key: "set", label: "set" },
                    { key: "unset", label: "unset" }
                  ]}
                  value={app.configMutationMode}
                  onChange={(value) =>
                    app.setConfigMutationMode(value === "unset" ? "unset" : "set")
                  }
                />
                <TextInputField
                  label="Key"
                  value={app.configMutationKey}
                  onChange={app.setConfigMutationKey}
                />
                <TextInputField
                  disabled={app.configMutationMode === "unset"}
                  label="Value"
                  placeholder={
                    app.configMutationMode === "unset"
                      ? "Value unused for unset"
                      : "\"value\""
                  }
                  value={app.configMutationValue}
                  onChange={app.setConfigMutationValue}
                />
              </div>
              <div className="workspace-inline">
                <ActionButton
                  isDisabled={app.configBusy}
                  type="button"
                  variant="primary"
                  onPress={() => void app.mutateConfigSurface()}
                >
                  {app.configMutationMode === "unset" ? "Unset key" : "Apply mutation"}
                </ActionButton>
              </div>
            </div>
          </WorkspaceSectionCard>

          <WorkspaceSectionCard
            description="Review the last backend mutation and its redacted diff before moving on."
            title="Mutation output"
          >
            <dl className="workspace-key-value-grid">
              <div>
                <dt>Operation</dt>
                <dd>{readString(app.configLastMutation ?? {}, "operation") ?? "n/a"}</dd>
              </div>
              <div>
                <dt>Changed key</dt>
                <dd>{readString(app.configLastMutation ?? {}, "changed_key") ?? "n/a"}</dd>
              </div>
              <div>
                <dt>Backups retained</dt>
                <dd>{readString(app.configLastMutation ?? {}, "backups_retained") ?? "n/a"}</dd>
              </div>
              <div>
                <dt>Updated</dt>
                <dd>{readString(app.configLastMutation ?? {}, "source_path") ?? app.configInspectPath}</dd>
              </div>
            </dl>
            {app.configDiffPreview !== null ? (
              <pre className="workspace-code-panel">
                <code>{app.configDiffPreview}</code>
              </pre>
            ) : (
              <WorkspaceEmptyState
                compact
                description="A redacted diff appears here after a mutation or migration."
                title="No diff preview"
              />
            )}
          </WorkspaceSectionCard>
        </section>
      )}

      {activeTab === "recover" && (
        <section className="workspace-two-column">
          <WorkspaceSectionCard
            description="Recovery stays visibly risky and grounded in the actual retained backup list."
            title="Recover from backup"
          >
            <div className="workspace-stack">
              <div className="workspace-form-grid">
                <TextInputField
                  label="Backup index"
                  value={app.configRecoverBackup}
                  onChange={app.setConfigRecoverBackup}
                />
                <TextInputField
                  label="Backups retained"
                  value={app.configBackups}
                  onChange={app.setConfigBackups}
                />
              </div>
              <div className="workspace-inline">
                <ActionButton
                  isDisabled={app.configBusy}
                  type="button"
                  variant="danger"
                  onPress={() => void app.recoverConfigSurface()}
                >
                  Recover backup
                </ActionButton>
              </div>
            </div>
          </WorkspaceSectionCard>

          <WorkspaceSectionCard
            description="Use the published backup records instead of guessing restore targets."
            title="Available backups"
          >
            {backups.length === 0 ? (
              <WorkspaceEmptyState
                compact
                description="Inspect config with retained backups to populate recovery targets."
                title="No backups published"
              />
            ) : (
              <WorkspaceTable ariaLabel="Config backups" columns={["Index", "Path", "Exists", "Updated"]}>
                {backups.map((backup) => (
                  <tr
                    key={readString(backup, "path") ?? String(readNumber(backup, "index") ?? 0)}
                  >
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
