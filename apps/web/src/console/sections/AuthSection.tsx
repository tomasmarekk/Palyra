import { ConsoleSectionHeader } from "../components/ConsoleSectionHeader";
import { toPrettyJson, type JsonObject } from "../shared";

type AuthSectionProps = {
  app: {
    authBusy: boolean;
    authProfiles: JsonObject[];
    authHealth: JsonObject | null;
    authProviderState: JsonObject | null;
    authDefaultProfileId: string;
    authBootstrapProfileId: string;
    revealSensitiveValues: boolean;
    refreshAuth: () => Promise<void>;
    setAuthDefaultProfileId: (value: string) => void;
    setAuthBootstrapProfileId: (value: string) => void;
    executeOpenAiAction: (
      action: "bootstrap" | "reconnect" | "revoke" | "default-profile"
    ) => Promise<void>;
  };
};

export function AuthSection({ app }: AuthSectionProps) {
  return (
    <main className="console-card">
      <ConsoleSectionHeader
        title="OpenAI and Auth"
        description="Baseline provider surface for M54: state, auth health, and default-profile controls."
        actions={(
          <button type="button" onClick={() => void app.refreshAuth()} disabled={app.authBusy}>
            {app.authBusy ? "Refreshing..." : "Refresh"}
          </button>
        )}
      />
      <section className="console-grid-2">
        <article className="console-subpanel">
          <h3>Provider state</h3>
          {app.authProviderState === null ? <p>No provider state loaded.</p> : <pre>{toPrettyJson(app.authProviderState, app.revealSensitiveValues)}</pre>}
        </article>
        <article className="console-subpanel">
          <h3>Auth health</h3>
          {app.authHealth === null ? <p>No auth health loaded.</p> : <pre>{toPrettyJson(app.authHealth, app.revealSensitiveValues)}</pre>}
        </article>
      </section>
      <section className="console-subpanel">
        <h3>OpenAI actions</h3>
        <div className="console-grid-2">
          <label>
            Bootstrap / reconnect profile
            <input value={app.authBootstrapProfileId} onChange={(event) => app.setAuthBootstrapProfileId(event.target.value)} />
          </label>
          <label>
            Default profile
            <input value={app.authDefaultProfileId} onChange={(event) => app.setAuthDefaultProfileId(event.target.value)} />
          </label>
        </div>
        <div className="console-inline-actions">
          <button type="button" onClick={() => void app.executeOpenAiAction("bootstrap")} disabled={app.authBusy}>Start bootstrap</button>
          <button type="button" onClick={() => void app.executeOpenAiAction("reconnect")} disabled={app.authBusy}>Reconnect</button>
          <button type="button" onClick={() => void app.executeOpenAiAction("default-profile")} disabled={app.authBusy}>Set default profile</button>
          <button type="button" className="button--warn" onClick={() => void app.executeOpenAiAction("revoke")} disabled={app.authBusy}>Revoke</button>
        </div>
      </section>
      <section className="console-subpanel">
        <h3>Auth profiles</h3>
        {app.authProfiles.length === 0 ? <p>No auth profiles loaded.</p> : <pre>{toPrettyJson(app.authProfiles, app.revealSensitiveValues)}</pre>}
      </section>
    </main>
  );
}
