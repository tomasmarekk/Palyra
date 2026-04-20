import { Tabs } from "@heroui/react";
import { useState } from "react";

import { ActionButton, SelectField, TextInputField } from "../components/ui";
import {
  WorkspaceMetricCard,
  WorkspacePageHeader,
  WorkspaceSectionCard,
  WorkspaceStatusChip,
} from "../components/workspace/WorkspaceChrome";
import {
  WorkspaceEmptyState,
  WorkspaceInlineNotice,
  WorkspaceTable,
  workspaceToneForState,
} from "../components/workspace/WorkspacePatterns";
import {
  formatUnixMs,
  PrettyJsonBlock,
  readNumber,
  readObject,
  readString,
  toJsonObjectArray,
  toStringArray,
  type JsonObject,
} from "../shared";
import { readProviderRegistrySummary } from "../providerRegistry";
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
    | "configReloadPlan"
    | "configReloadResult"
    | "configDeploymentPosture"
    | "diagnosticsSnapshot"
    | "revealSensitiveValues"
    | "refreshConfigSurface"
    | "inspectConfigSurface"
    | "validateConfigSurface"
    | "mutateConfigSurface"
    | "migrateConfigSurface"
    | "recoverConfigSurface"
    | "planConfigReload"
    | "applyConfigReload"
  >;
};

export function ConfigSection({ app }: ConfigSectionProps) {
  const [activeTab, setActiveTab] = useState<ConfigTab>("inspect");
  const warnings = toStringArray(
    Array.isArray(app.configDeploymentPosture?.warnings)
      ? app.configDeploymentPosture.warnings
      : [],
  );
  const backups = Array.isArray(app.configInspectSnapshot?.backups)
    ? app.configInspectSnapshot.backups.filter(
        (entry): entry is JsonObject =>
          typeof entry === "object" && entry !== null && !Array.isArray(entry),
      )
    : [];
  const providerRegistry = readProviderRegistrySummary(app.diagnosticsSnapshot);
  const runtimeControls = readObject(app.diagnosticsSnapshot ?? {}, "runtime_controls");
  const runtimeCapabilities = Array.isArray(runtimeControls?.capabilities)
    ? toJsonObjectArray(runtimeControls.capabilities)
    : [];
  const reloadPlanSummary =
    typeof app.configReloadPlan?.summary === "object" && app.configReloadPlan.summary !== null
      ? (app.configReloadPlan.summary as JsonObject)
      : null;

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
                    : "unknown",
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
          value={
            (readString(app.configInspectSnapshot ?? {}, "source_path") ?? app.configInspectPath) ||
            "n/a"
          }
        />
        <WorkspaceMetricCard
          detail="Retained backups visible for recovery flows."
          label="Backups"
          tone={backups.length > 0 ? "accent" : "default"}
          value={backups.length}
        />
        <WorkspaceMetricCard
          detail={
            readString(app.configLastMutation ?? {}, "changed_key") ?? "No recent mutation loaded."
          }
          label="Last mutation"
          tone={app.configLastMutation === null ? "default" : "warning"}
          value={readString(app.configLastMutation ?? {}, "operation") ?? "none"}
        />
        <WorkspaceMetricCard
          detail="Default chat model currently published by the runtime registry."
          label="Default chat model"
          tone={providerRegistry?.defaultChatModelId ? "accent" : "default"}
          value={providerRegistry?.defaultChatModelId ?? "n/a"}
        />
        <WorkspaceMetricCard
          detail="Latest reload plan tracks hot-safe, restart-required, blocked, and manual-review categories."
          label="Reload hot-safe"
          tone={readNumber(reloadPlanSummary ?? {}, "hot_safe") ? "accent" : "default"}
          value={readNumber(reloadPlanSummary ?? {}, "hot_safe") ?? 0}
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

      <Tabs
        className="w-full"
        selectedKey={activeTab}
        variant="secondary"
        onSelectionChange={(key) => setActiveTab(String(key) as ConfigTab)}
      >
        <Tabs.ListContainer>
          <Tabs.List aria-label="Config workspace modes" className="w-fit">
            {(["inspect", "validate", "mutate", "recover"] as const).map((tab) => (
              <Tabs.Tab id={tab} key={tab}>
                {tab[0].toUpperCase() + tab.slice(1)}
                <Tabs.Indicator />
              </Tabs.Tab>
            ))}
          </Tabs.List>
        </Tabs.ListContainer>

        <Tabs.Panel className="pt-4" id="inspect">
          <div className="workspace-stack">
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
                    <ActionButton
                      isDisabled={app.configBusy}
                      type="button"
                      variant="secondary"
                      onPress={() => void app.planConfigReload()}
                    >
                      Plan reload
                    </ActionButton>
                    <ActionButton
                      isDisabled={app.configBusy}
                      type="button"
                      variant="secondary"
                      onPress={() => void app.applyConfigReload(true)}
                    >
                      Dry-run reload
                    </ActionButton>
                    <ActionButton
                      isDisabled={app.configBusy}
                      type="button"
                      variant="primary"
                      onPress={() => void app.applyConfigReload(false)}
                    >
                      Apply hot-safe
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

            <section className="workspace-two-column">
              <WorkspaceSectionCard
                description="Runtime defaults and safety switches come from diagnostics so config review matches the active routing posture."
                title="Registry defaults"
              >
                {providerRegistry === null ? (
                  <WorkspaceEmptyState
                    compact
                    description="Open diagnostics-backed sections to publish provider registry details."
                    title="No registry diagnostics loaded"
                  />
                ) : (
                  <dl className="workspace-key-value-grid">
                    <div>
                      <dt>Runtime provider</dt>
                      <dd>{providerRegistry.providerKind}</dd>
                    </div>
                    <div>
                      <dt>Provider id</dt>
                      <dd>{providerRegistry.providerId ?? "n/a"}</dd>
                    </div>
                    <div>
                      <dt>Default chat</dt>
                      <dd>{providerRegistry.defaultChatModelId ?? "n/a"}</dd>
                    </div>
                    <div>
                      <dt>Default embeddings</dt>
                      <dd>{providerRegistry.defaultEmbeddingsModelId ?? "n/a"}</dd>
                    </div>
                    <div>
                      <dt>Failover</dt>
                      <dd>{providerRegistry.failoverEnabled ? "enabled" : "disabled"}</dd>
                    </div>
                    <div>
                      <dt>Response cache</dt>
                      <dd>{providerRegistry.responseCacheEnabled ? "enabled" : "disabled"}</dd>
                    </div>
                  </dl>
                )}
            </WorkspaceSectionCard>

            <WorkspaceSectionCard
              description="Each runtime capability publishes its config mode, rollout posture, and activation blockers before deeper behavior is promoted."
              title="Runtime controls"
            >
              {runtimeControls === null ? (
                <WorkspaceEmptyState
                  compact
                  description="Refresh diagnostics to publish rollout and effective config data."
                  title="No runtime controls loaded"
                />
              ) : (
                <div className="workspace-stack">
                  <dl className="workspace-key-value-grid">
                    <div>
                      <dt>State</dt>
                      <dd>{readString(runtimeControls, "state") ?? "n/a"}</dd>
                    </div>
                    <div>
                      <dt>Preview</dt>
                      <dd>{readNumber(runtimeControls, "preview_capabilities") ?? 0}</dd>
                    </div>
                    <div>
                      <dt>Enabled</dt>
                      <dd>{readNumber(runtimeControls, "enabled_capabilities") ?? 0}</dd>
                    </div>
                    <div>
                      <dt>Blocked</dt>
                      <dd>{readNumber(runtimeControls, "blocked_capabilities") ?? 0}</dd>
                    </div>
                  </dl>
                  {(readNumber(runtimeControls, "blocked_capabilities") ?? 0) > 0 ? (
                    <WorkspaceInlineNotice title="Activation blockers published" tone="warning">
                      Review the blocked capabilities below before promoting any preview mode to
                      `enabled`.
                    </WorkspaceInlineNotice>
                  ) : null}
                  {runtimeCapabilities.length === 0 ? (
                    <WorkspaceEmptyState
                      compact
                      description="The daemon has not published any runtime capability entries yet."
                      title="No capability diagnostics"
                    />
                  ) : (
                    <WorkspaceTable
                      ariaLabel="Runtime controls"
                      columns={["Capability", "Mode", "Effective", "Rollout", "Blockers"]}
                    >
                      {runtimeCapabilities.map((entry: JsonObject) => (
                        <tr key={readString(entry, "capability") ?? "runtime-preview"}>
                          <td>
                            <div>{readString(entry, "label") ?? readString(entry, "capability")}</div>
                            <div>{readString(entry, "summary") ?? "No summary published."}</div>
                          </td>
                          <td>{readString(entry, "mode") ?? "n/a"}</td>
                          <td>{readString(entry, "effective_state") ?? "n/a"}</td>
                          <td>
                            {readString(entry, "rollout_source") ?? "default"} /{" "}
                            {entry.rollout_enabled === true ? "enabled" : "disabled"}
                          </td>
                          <td>
                            {toStringArray(
                              Array.isArray(entry.activation_blockers)
                                ? entry.activation_blockers
                                : [],
                            ).join("; ") || "ready"}
                          </td>
                        </tr>
                      ))}
                    </WorkspaceTable>
                  )}
                </div>
              )}
            </WorkspaceSectionCard>

            <WorkspaceSectionCard
              description="Published provider bindings and model roles make it obvious what the registry can actually route."
              title="Registry inventory"
            >
                {providerRegistry === null ? (
                  <WorkspaceEmptyState
                    compact
                    description="Diagnostics must publish the provider registry before inventory is available here."
                    title="No registry inventory"
                  />
                ) : providerRegistry.models.length === 0 ? (
                  <WorkspaceEmptyState
                    compact
                    description="The current diagnostics snapshot does not expose any models yet."
                    title="No models published"
                  />
                ) : (
                  <WorkspaceTable
                    ariaLabel="Provider registry models"
                    columns={["Model", "Provider", "Role", "Capabilities", "Limits"]}
                  >
                    {providerRegistry.models.map((model) => (
                      <tr key={model.modelId}>
                        <td>{model.modelId}</td>
                        <td>{model.providerId}</td>
                        <td>{model.role}</td>
                        <td>
                          {[
                            model.toolCalls ? "tools" : null,
                            model.jsonMode ? "json" : null,
                            model.vision ? "vision" : null,
                            model.audioTranscribe ? "audio" : null,
                            model.embeddings ? "embed" : null,
                          ]
                            .filter((value): value is string => value !== null)
                            .join(", ") || "n/a"}
                        </td>
                        <td>{model.maxContextTokens?.toLocaleString() ?? "n/a"}</td>
                      </tr>
                    ))}
                  </WorkspaceTable>
                )}
              </WorkspaceSectionCard>

              <WorkspaceSectionCard
                description="The reload planner works on typed config categories instead of a raw text diff."
                title="Reload plan"
              >
                {app.configReloadPlan === null ? (
                  <WorkspaceEmptyState
                    compact
                    description="Generate a reload plan after inspecting or mutating config."
                    title="No reload plan"
                  />
                ) : (
                  <div className="workspace-stack">
                    <dl className="workspace-key-value-grid">
                      <div>
                        <dt>Plan id</dt>
                        <dd>{readString(app.configReloadPlan, "plan_id") ?? "n/a"}</dd>
                      </div>
                      <div>
                        <dt>Active runs</dt>
                        <dd>{readString(app.configReloadPlan, "active_runs") ?? "0"}</dd>
                      </div>
                      <div>
                        <dt>Restart required</dt>
                        <dd>{readString(app.configReloadPlan, "requires_restart") ?? "false"}</dd>
                      </div>
                    </dl>
                    <PrettyJsonBlock
                      revealSensitiveValues={app.revealSensitiveValues}
                      value={app.configReloadPlan}
                    />
                    {app.configReloadResult !== null ? (
                      <PrettyJsonBlock
                        revealSensitiveValues={app.revealSensitiveValues}
                        value={app.configReloadResult}
                      />
                    ) : null}
                  </div>
                )}
              </WorkspaceSectionCard>
            </section>
          </div>
        </Tabs.Panel>

        <Tabs.Panel className="pt-4" id="validate">
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
                  <dd>
                    {readString(app.configValidation ?? {}, "migrated_from_version") ?? "n/a"}
                  </dd>
                </div>
                <div>
                  <dt>Path</dt>
                  <dd>
                    {readString(app.configValidation ?? {}, "source_path") ?? app.configInspectPath}
                  </dd>
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
        </Tabs.Panel>

        <Tabs.Panel className="pt-4" id="mutate">
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
                      { key: "unset", label: "unset" },
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
                      app.configMutationMode === "unset" ? "Value unused for unset" : '"value"'
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
                  <dd>
                    {readString(app.configLastMutation ?? {}, "source_path") ??
                      app.configInspectPath}
                  </dd>
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
        </Tabs.Panel>

        <Tabs.Panel className="pt-4" id="recover">
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
                <WorkspaceTable
                  ariaLabel="Config backups"
                  columns={["Index", "Path", "Exists", "Updated"]}
                >
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
        </Tabs.Panel>
      </Tabs>
    </main>
  );
}
