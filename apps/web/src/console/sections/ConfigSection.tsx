import { ConsoleSectionHeader } from "../components/ConsoleSectionHeader";
import {
  formatUnixMs,
  readNumber,
  readString,
  toPrettyJson,
  toStringArray,
  type JsonObject,
} from "../shared";
import type { ConsoleAppState } from "../useConsoleAppState";

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
    | "configSecretsScope"
    | "setConfigSecretsScope"
    | "configSecrets"
    | "configSecretKey"
    | "setConfigSecretKey"
    | "configSecretMetadata"
    | "configSecretValue"
    | "setConfigSecretValue"
    | "configSecretReveal"
    | "revealSensitiveValues"
    | "refreshConfigSurface"
    | "inspectConfigSurface"
    | "validateConfigSurface"
    | "mutateConfigSurface"
    | "migrateConfigSurface"
    | "recoverConfigSurface"
    | "refreshSecrets"
    | "loadSecretMetadata"
    | "setSecretValue"
    | "revealSecretValue"
    | "deleteSecretValue"
  >;
};

export function ConfigSection({ app }: ConfigSectionProps) {
  const warnings = toStringArray(
    Array.isArray(app.configDeploymentPosture?.warnings) ? app.configDeploymentPosture.warnings : []
  );
  const backups = Array.isArray(app.configInspectSnapshot?.backups)
    ? app.configInspectSnapshot.backups.filter((entry): entry is JsonObject => typeof entry === "object" && entry !== null && !Array.isArray(entry))
    : [];

  return (
    <main className="console-card">
      <ConsoleSectionHeader
        title="Config and Secrets"
        description="Operate config lifecycle and Vault-backed secrets directly from the dashboard with redacted snapshots, explicit reveal actions, and dangerous deployment warnings."
        actions={(
          <button type="button" onClick={() => void app.refreshConfigSurface()} disabled={app.configBusy}>
            {app.configBusy ? "Refreshing..." : "Refresh config"}
          </button>
        )}
      />

      <section className="console-grid-4 console-summary-grid">
        <article className="console-subpanel">
          <h3>Config source</h3>
          <p><strong>Path:</strong> {(readString(app.configInspectSnapshot ?? {}, "source_path") ?? app.configInspectPath) || "n/a"}</p>
          <p><strong>Version:</strong> {readString(app.configInspectSnapshot ?? {}, "config_version") ?? "n/a"}</p>
          <p><strong>Backups:</strong> {backups.length}</p>
        </article>
        <article className="console-subpanel">
          <h3>Validation</h3>
          <p><strong>Status:</strong> {app.configValidation?.valid === true ? "valid" : app.configValidation === null ? "not loaded" : "check output"}</p>
          <p><strong>Migrated from:</strong> {readString(app.configValidation ?? {}, "migrated_from_version") ?? "n/a"}</p>
        </article>
        <article className="console-subpanel">
          <h3>Last mutation</h3>
          <p><strong>Operation:</strong> {readString(app.configLastMutation ?? {}, "operation") ?? "n/a"}</p>
          <p><strong>Changed key:</strong> {readString(app.configLastMutation ?? {}, "changed_key") ?? "n/a"}</p>
          <p><strong>Backups retained:</strong> {readString(app.configLastMutation ?? {}, "backups_retained") ?? "n/a"}</p>
        </article>
        <article className="console-subpanel">
          <h3>Danger warnings</h3>
          {warnings.length === 0 ? (
            <p>No dangerous deployment posture warnings were published.</p>
          ) : (
            <ul className="console-compact-list">
              {warnings.map((warning) => (
                <li key={warning}>{warning}</li>
              ))}
            </ul>
          )}
        </article>
      </section>

      <section className="console-grid-2">
        <article className="console-subpanel">
          <div className="console-subpanel__header">
            <div>
              <h3>Inspect, validate, mutate, migrate</h3>
              <p className="chat-muted">
                All document views stay redacted by default. Mutations publish a diff preview instead of expecting operators to hand-edit TOML in another shell.
              </p>
            </div>
          </div>
          <div className="console-grid-4">
            <label>
              Path
              <input value={app.configInspectPath} onChange={(event) => app.setConfigInspectPath(event.target.value)} />
            </label>
            <label>
              Backups
              <input value={app.configBackups} onChange={(event) => app.setConfigBackups(event.target.value)} />
            </label>
            <label>
              Mutation mode
              <select
                value={app.configMutationMode}
                onChange={(event) =>
                  app.setConfigMutationMode(event.target.value === "unset" ? "unset" : "set")
                }
              >
                <option value="set">set</option>
                <option value="unset">unset</option>
              </select>
            </label>
            <label>
              Recover backup
              <input value={app.configRecoverBackup} onChange={(event) => app.setConfigRecoverBackup(event.target.value)} />
            </label>
          </div>
          <div className="console-grid-2">
            <label>
              Key
              <input value={app.configMutationKey} onChange={(event) => app.setConfigMutationKey(event.target.value)} />
            </label>
            <label>
              Value
              <input
                value={app.configMutationValue}
                onChange={(event) => app.setConfigMutationValue(event.target.value)}
                disabled={app.configMutationMode === "unset"}
                placeholder={app.configMutationMode === "unset" ? "Value not used for unset" : "\"value\""}
              />
            </label>
          </div>
          <div className="console-inline-actions">
            <button type="button" onClick={() => void app.inspectConfigSurface()} disabled={app.configBusy}>
              Inspect
            </button>
            <button type="button" onClick={() => void app.validateConfigSurface()} disabled={app.configBusy}>
              Validate
            </button>
            <button type="button" onClick={() => void app.mutateConfigSurface()} disabled={app.configBusy}>
              {app.configMutationMode === "unset" ? "Unset key" : "Apply mutation"}
            </button>
            <button type="button" onClick={() => void app.migrateConfigSurface()} disabled={app.configBusy}>
              Migrate
            </button>
            <button type="button" className="button--warn" onClick={() => void app.recoverConfigSurface()} disabled={app.configBusy}>
              Recover backup
            </button>
          </div>
          {app.configDiffPreview !== null && (
            <>
              <p><strong>Redacted diff preview</strong></p>
              <pre className="console-code-block"><code>{app.configDiffPreview}</code></pre>
            </>
          )}
        </article>

        <article className="console-subpanel">
          <div className="console-subpanel__header">
            <div>
              <h3>Secret metadata and reveal path</h3>
              <p className="chat-muted">
                Secret reads require an explicit reveal action. Metadata can be inspected without exposing the raw secret body.
              </p>
            </div>
          </div>
          <div className="console-grid-3">
            <label>
              Scope
              <input value={app.configSecretsScope} onChange={(event) => app.setConfigSecretsScope(event.target.value)} />
            </label>
            <label>
              Key
              <input value={app.configSecretKey} onChange={(event) => app.setConfigSecretKey(event.target.value)} />
            </label>
            <label>
              Value
              <input
                type="password"
                autoComplete="off"
                value={app.configSecretValue}
                onChange={(event) => app.setConfigSecretValue(event.target.value)}
              />
            </label>
          </div>
          <div className="console-inline-actions">
            <button type="button" onClick={() => void app.refreshSecrets()} disabled={app.configBusy}>
              Reload metadata
            </button>
            <button type="button" onClick={() => void app.loadSecretMetadata()} disabled={app.configBusy}>
              Load selected metadata
            </button>
            <button type="button" onClick={() => void app.setSecretValue()} disabled={app.configBusy}>
              Store or replace secret
            </button>
            <button type="button" onClick={() => void app.revealSecretValue()} disabled={app.configBusy}>
              Explicit reveal
            </button>
            <button type="button" className="button--warn" onClick={() => void app.deleteSecretValue()} disabled={app.configBusy}>
              Delete secret
            </button>
          </div>
          {app.configSecretMetadata !== null && (
            <div className="console-secret-metadata">
              <p><strong>Created:</strong> {formatUnixMs(readNumber(app.configSecretMetadata, "created_at_unix_ms"))}</p>
              <p><strong>Updated:</strong> {formatUnixMs(readNumber(app.configSecretMetadata, "updated_at_unix_ms"))}</p>
              <p><strong>Value bytes:</strong> {readString(app.configSecretMetadata, "value_bytes") ?? "n/a"}</p>
            </div>
          )}
          {app.configSecretReveal !== null && (
            <>
              <p><strong>Reveal result</strong></p>
              <pre>{toPrettyJson(app.configSecretReveal, app.revealSensitiveValues)}</pre>
            </>
          )}
        </article>
      </section>

      <section className="console-grid-2">
        <article className="console-subpanel">
          <h3>Redacted config snapshot</h3>
          {app.configInspectSnapshot === null ? (
            <p>No config snapshot loaded.</p>
          ) : (
            <pre>{toPrettyJson(app.configInspectSnapshot, app.revealSensitiveValues)}</pre>
          )}
        </article>
        <article className="console-subpanel">
          <h3>Secret metadata list</h3>
          {app.configSecrets.length === 0 ? (
            <p>No secret metadata loaded.</p>
          ) : (
            <div className="console-table-wrap">
              <table className="console-table">
                <thead>
                  <tr>
                    <th>Scope</th>
                    <th>Key</th>
                    <th>Updated</th>
                    <th>Bytes</th>
                    <th>Action</th>
                  </tr>
                </thead>
                <tbody>
                  {app.configSecrets.map((entry) => {
                    const scope = readString(entry, "scope") ?? "unknown";
                    const key = readString(entry, "key") ?? "unknown";
                    return (
                      <tr key={`${scope}:${key}`}>
                        <td>{scope}</td>
                        <td>{key}</td>
                        <td>{formatUnixMs(readNumber(entry, "updated_at_unix_ms"))}</td>
                        <td>{readString(entry, "value_bytes") ?? "n/a"}</td>
                        <td>
                          <button type="button" className="secondary" onClick={() => app.setConfigSecretKey(key)}>
                            Select
                          </button>
                        </td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
          )}
        </article>
      </section>

      {app.configValidation !== null && (
        <section className="console-subpanel">
          <h3>Validation payload</h3>
          <pre>{toPrettyJson(app.configValidation, app.revealSensitiveValues)}</pre>
        </section>
      )}
    </main>
  );
}
