import { ConsoleSectionHeader } from "../components/ConsoleSectionHeader";
import { toPrettyJson, type JsonObject } from "../shared";

type SupportSectionProps = {
  app: {
    supportBusy: boolean;
    supportPairingSummary: JsonObject | null;
    supportPairingChannel: string;
    supportPairingIssuedBy: string;
    supportPairingTtlMs: string;
    supportBundleJobs: JsonObject[];
    revealSensitiveValues: boolean;
    refreshSupport: () => Promise<void>;
    setSupportPairingChannel: (value: string) => void;
    setSupportPairingIssuedBy: (value: string) => void;
    setSupportPairingTtlMs: (value: string) => void;
    mintSupportPairingCode: () => Promise<void>;
    createSupportBundle: () => Promise<void>;
  };
};

export function SupportSection({ app }: SupportSectionProps) {
  return (
    <main className="console-card">
      <ConsoleSectionHeader
        title="Support and Recovery"
        actions={(
          <button type="button" onClick={() => void app.refreshSupport()} disabled={app.supportBusy}>
            {app.supportBusy ? "Refreshing..." : "Refresh"}
          </button>
        )}
      />
      <section className="console-subpanel">
        <h3>Pairing</h3>
        <div className="console-grid-3">
          <label>
            Channel
            <input value={app.supportPairingChannel} onChange={(event) => app.setSupportPairingChannel(event.target.value)} />
          </label>
          <label>
            Issued by
            <input value={app.supportPairingIssuedBy} onChange={(event) => app.setSupportPairingIssuedBy(event.target.value)} />
          </label>
          <label>
            TTL ms
            <input value={app.supportPairingTtlMs} onChange={(event) => app.setSupportPairingTtlMs(event.target.value)} />
          </label>
        </div>
        <div className="console-inline-actions">
          <button type="button" onClick={() => void app.mintSupportPairingCode()} disabled={app.supportBusy}>Mint pairing code</button>
          <button type="button" onClick={() => void app.createSupportBundle()} disabled={app.supportBusy}>Queue support bundle</button>
        </div>
        {app.supportPairingSummary === null ? <p>No pairing summary loaded.</p> : <pre>{toPrettyJson(app.supportPairingSummary, app.revealSensitiveValues)}</pre>}
      </section>
      <section className="console-subpanel">
        <h3>Support bundle jobs</h3>
        {app.supportBundleJobs.length === 0 ? <p>No support-bundle jobs queued.</p> : <pre>{toPrettyJson(app.supportBundleJobs, app.revealSensitiveValues)}</pre>}
      </section>
    </main>
  );
}
