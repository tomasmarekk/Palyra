import { ConsoleSectionHeader } from "../components/ConsoleSectionHeader";
import { toPrettyJson, type JsonObject } from "../shared";

type DiagnosticsSectionProps = {
  app: {
    diagnosticsBusy: boolean;
    diagnosticsSnapshot: JsonObject | null;
    revealSensitiveValues: boolean;
    refreshDiagnostics: () => Promise<void>;
  };
};

export function DiagnosticsSection({ app }: DiagnosticsSectionProps) {
  return (
    <main className="console-card">
      <ConsoleSectionHeader
        title="Diagnostics"
        actions={(
          <button type="button" onClick={() => void app.refreshDiagnostics()} disabled={app.diagnosticsBusy}>
            {app.diagnosticsBusy ? "Refreshing..." : "Refresh"}
          </button>
        )}
      />
      {app.diagnosticsSnapshot === null ? (
        <p>No diagnostics loaded.</p>
      ) : (
        <>
          <section className="console-subpanel">
            <h3>Model Provider + Rate Limits</h3>
            <pre>{toPrettyJson({
              model_provider: app.diagnosticsSnapshot["model_provider"] ?? null,
              rate_limits: app.diagnosticsSnapshot["rate_limits"] ?? null
            }, app.revealSensitiveValues)}</pre>
          </section>
          <section className="console-subpanel">
            <h3>Auth Profile Health</h3>
            <pre>{toPrettyJson(app.diagnosticsSnapshot["auth_profiles"] ?? null, app.revealSensitiveValues)}</pre>
          </section>
          <section className="console-subpanel">
            <h3>Browserd Status</h3>
            <pre>{toPrettyJson(app.diagnosticsSnapshot["browserd"] ?? null, app.revealSensitiveValues)}</pre>
          </section>
          <section className="console-subpanel">
            <h3>Media Pipeline</h3>
            <pre>{toPrettyJson(app.diagnosticsSnapshot["media"] ?? null, app.revealSensitiveValues)}</pre>
          </section>
        </>
      )}
    </main>
  );
}
