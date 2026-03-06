import { ConsoleSectionHeader } from "../components/ConsoleSectionHeader";
import { readString, toPrettyJson, type JsonObject } from "../shared";

type OverviewSectionProps = {
  app: {
    overviewBusy: boolean;
    overviewCatalog: JsonObject | null;
    overviewDeployment: JsonObject | null;
    overviewSupportJobs: JsonObject[];
    revealSensitiveValues: boolean;
    refreshOverview: () => Promise<void>;
    setSection: (section: "support") => void;
  };
};

export function OverviewSection({ app }: OverviewSectionProps) {
  return (
    <main className="console-card">
      <ConsoleSectionHeader
        title="Overview"
        description="Dashboard owns the full operator surface. Desktop stays focused on onboarding, lifecycle, and support."
        actions={(
          <button type="button" onClick={() => void app.refreshOverview()} disabled={app.overviewBusy}>
            {app.overviewBusy ? "Refreshing..." : "Refresh overview"}
          </button>
        )}
      />
      <section className="console-grid-3 console-summary-grid">
        <article className="console-subpanel">
          <h3>Product shape</h3>
          <p>Dashboard: full operator surface</p>
          <p>Desktop: onboarding + local lifecycle</p>
          <p>Primary connector: Discord</p>
        </article>
        <article className="console-subpanel">
          <h3>Capability catalog</h3>
          <p><strong>Version:</strong> {readString(app.overviewCatalog ?? {}, "version") ?? "n/a"}</p>
          <p><strong>Capabilities:</strong> {Array.isArray(app.overviewCatalog?.capabilities) ? app.overviewCatalog.capabilities.length : 0}</p>
        </article>
        <article className="console-subpanel">
          <h3>Support queue</h3>
          <p><strong>Jobs:</strong> {app.overviewSupportJobs.length}</p>
          <button type="button" onClick={() => app.setSection("support")}>Open support domain</button>
        </article>
      </section>
      <section className="console-subpanel">
        <h3>Deployment posture</h3>
        {app.overviewDeployment === null ? <p>No deployment posture loaded.</p> : <pre>{toPrettyJson(app.overviewDeployment, app.revealSensitiveValues)}</pre>}
      </section>
      <section className="console-subpanel">
        <h3>Capability catalog</h3>
        {app.overviewCatalog === null ? <p>No capability catalog loaded.</p> : <pre>{toPrettyJson(app.overviewCatalog, app.revealSensitiveValues)}</pre>}
      </section>
    </main>
  );
}
