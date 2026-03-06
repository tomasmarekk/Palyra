import { ConsoleSectionHeader } from "../components/ConsoleSectionHeader";
import { toPrettyJson, type JsonObject } from "../shared";

type ConfigSectionProps = {
  app: {
    configBusy: boolean;
    configInspectPath: string;
    configInspectSnapshot: JsonObject | null;
    configMutationKey: string;
    configMutationValue: string;
    configSecretsScope: string;
    configSecrets: JsonObject[];
    configSecretKey: string;
    configSecretValue: string;
    configSecretReveal: JsonObject | null;
    revealSensitiveValues: boolean;
    refreshConfigSurface: () => Promise<void>;
    setConfigInspectPath: (value: string) => void;
    setConfigMutationKey: (value: string) => void;
    setConfigMutationValue: (value: string) => void;
    setConfigSecretsScope: (value: string) => void;
    setConfigSecretKey: (value: string) => void;
    setConfigSecretValue: (value: string) => void;
    inspectConfigSurface: () => Promise<void>;
    validateConfigSurface: () => Promise<void>;
    mutateConfigSurface: () => Promise<void>;
    refreshSecrets: () => Promise<void>;
    setSecretValue: () => Promise<void>;
    revealSecretValue: () => Promise<void>;
    deleteSecretValue: () => Promise<void>;
  };
};

export function ConfigSection({ app }: ConfigSectionProps) {
  return (
    <main className="console-card">
      <ConsoleSectionHeader
        title="Config and Secrets"
        actions={(
          <button type="button" onClick={() => void app.refreshConfigSurface()} disabled={app.configBusy}>
            {app.configBusy ? "Refreshing..." : "Refresh"}
          </button>
        )}
      />
      <section className="console-subpanel">
        <h3>Config inspect and mutate</h3>
        <div className="console-grid-3">
          <label>
            Path
            <input value={app.configInspectPath} onChange={(event) => app.setConfigInspectPath(event.target.value)} />
          </label>
          <label>
            Key
            <input value={app.configMutationKey} onChange={(event) => app.setConfigMutationKey(event.target.value)} />
          </label>
          <label>
            Value
            <input value={app.configMutationValue} onChange={(event) => app.setConfigMutationValue(event.target.value)} />
          </label>
        </div>
        <div className="console-inline-actions">
          <button type="button" onClick={() => void app.inspectConfigSurface()} disabled={app.configBusy}>Inspect</button>
          <button type="button" onClick={() => void app.validateConfigSurface()} disabled={app.configBusy}>Validate</button>
          <button type="button" onClick={() => void app.mutateConfigSurface()} disabled={app.configBusy}>Apply mutation</button>
        </div>
        {app.configInspectSnapshot !== null && <pre>{toPrettyJson(app.configInspectSnapshot, app.revealSensitiveValues)}</pre>}
      </section>
      <section className="console-subpanel">
        <h3>Secrets</h3>
        <div className="console-grid-4">
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
            <input value={app.configSecretValue} onChange={(event) => app.setConfigSecretValue(event.target.value)} />
          </label>
          <button type="button" onClick={() => void app.refreshSecrets()} disabled={app.configBusy}>Reload secrets</button>
        </div>
        <div className="console-inline-actions">
          <button type="button" onClick={() => void app.setSecretValue()} disabled={app.configBusy}>Store secret</button>
          <button type="button" onClick={() => void app.revealSecretValue()} disabled={app.configBusy}>Reveal secret</button>
          <button type="button" className="button--warn" onClick={() => void app.deleteSecretValue()} disabled={app.configBusy}>Delete secret</button>
        </div>
        {app.configSecrets.length === 0 ? <p>No secrets metadata loaded.</p> : <pre>{toPrettyJson(app.configSecrets, app.revealSensitiveValues)}</pre>}
        {app.configSecretReveal !== null && <pre>{toPrettyJson(app.configSecretReveal, app.revealSensitiveValues)}</pre>}
      </section>
    </main>
  );
}
