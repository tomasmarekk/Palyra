import { ConsoleSectionHeader } from "../components/ConsoleSectionHeader";
import { readString, skillMetadata, toPrettyJson } from "../shared";
import type { ConsoleAppState } from "../useConsoleAppState";

type SkillsSectionProps = {
  app: Pick<
    ConsoleAppState,
    | "skillsBusy"
    | "skillsEntries"
    | "skillArtifactPath"
    | "setSkillArtifactPath"
    | "skillAllowTofu"
    | "setSkillAllowTofu"
    | "skillAllowUntrusted"
    | "setSkillAllowUntrusted"
    | "skillReason"
    | "setSkillReason"
    | "refreshSkills"
    | "installSkill"
    | "executeSkillAction"
    | "revealSensitiveValues"
  >;
};

export function SkillsSection({ app }: SkillsSectionProps) {
  return (
    <main className="console-card">
      <ConsoleSectionHeader
        title="Skills"
        description="Install signed artifacts, verify trust posture, audit runtime safety, and control quarantine or re-enable flows."
        actions={(
          <button type="button" onClick={() => void app.refreshSkills()} disabled={app.skillsBusy}>
            {app.skillsBusy ? "Refreshing..." : "Refresh skills"}
          </button>
        )}
      />

      <form className="console-form" onSubmit={(event) => void app.installSkill(event)}>
        <div className="console-grid-4">
          <label>
            Artifact path
            <input value={app.skillArtifactPath} onChange={(event) => app.setSkillArtifactPath(event.target.value)} />
          </label>
          <label className="console-checkbox-inline">
            <input type="checkbox" checked={app.skillAllowTofu} onChange={(event) => app.setSkillAllowTofu(event.target.checked)} />
            Allow TOFU
          </label>
          <label className="console-checkbox-inline">
            <input type="checkbox" checked={app.skillAllowUntrusted} onChange={(event) => app.setSkillAllowUntrusted(event.target.checked)} />
            Allow untrusted
          </label>
          <button type="submit" disabled={app.skillsBusy}>{app.skillsBusy ? "Installing..." : "Install skill"}</button>
        </div>
      </form>

      <section className="console-subpanel">
        <h3>Operator reason</h3>
        <label>
          Reason
          <input value={app.skillReason} onChange={(event) => app.setSkillReason(event.target.value)} />
        </label>
      </section>

      {app.skillsEntries.length === 0 ? (
        <p>No skills installed.</p>
      ) : (
        <div className="console-capability-grid">
          {app.skillsEntries.map((entry, index) => {
            const metadata = skillMetadata(entry);
            const skillId = metadata?.skillId ?? readString(entry, "skill_id") ?? `skill-${index + 1}`;
            return (
              <article key={skillId} className="console-capability-card">
                <div className="console-capability-card__header">
                  <div>
                    <h4>{skillId}</h4>
                    <p className="chat-muted">version {metadata?.version ?? readString(entry, "version") ?? "unknown"}</p>
                  </div>
                </div>
                <pre>{toPrettyJson(entry, app.revealSensitiveValues)}</pre>
                <div className="console-inline-actions">
                  <button type="button" onClick={() => void app.executeSkillAction(entry, "verify")} disabled={app.skillsBusy}>Verify</button>
                  <button type="button" onClick={() => void app.executeSkillAction(entry, "audit")} disabled={app.skillsBusy}>Audit</button>
                  <button type="button" className="button--warn" onClick={() => void app.executeSkillAction(entry, "quarantine")} disabled={app.skillsBusy}>Quarantine</button>
                  <button type="button" className="secondary" onClick={() => void app.executeSkillAction(entry, "enable")} disabled={app.skillsBusy}>Enable</button>
                </div>
              </article>
            );
          })}
        </div>
      )}
    </main>
  );
}
