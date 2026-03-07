import type { CapabilityEntry } from "../../consoleApi";
import {
  capabilityExecutionLabel,
  normalizeCapabilityExposureMode,
} from "../capabilityCatalog";

type CapabilityCardListProps = {
  entries: CapabilityEntry[];
  emptyMessage: string;
};

export function CapabilityCardList({ entries, emptyMessage }: CapabilityCardListProps) {
  if (entries.length === 0) {
    return <p>{emptyMessage}</p>;
  }

  return (
    <div className="console-capability-grid">
      {entries.map((entry) => {
        const exposure = normalizeCapabilityExposureMode(entry);
        return (
          <article key={entry.id} className={`console-capability-card console-capability-card--${exposure}`}>
            <div className="console-capability-card__header">
              <div>
                <h4>{entry.title}</h4>
                <p className="chat-muted">{entry.id}</p>
              </div>
              <span className="console-capability-badge">{capabilityExecutionLabel(exposure)}</span>
            </div>
            <p><strong>Owner:</strong> {entry.owner}</p>
            <p><strong>Surfaces:</strong> {joinOrDash(entry.surfaces)}</p>
            <p><strong>Mutation classes:</strong> {joinOrDash(entry.mutation_classes)}</p>
            {entry.notes !== undefined && entry.notes.trim().length > 0 && (
              <p>{entry.notes}</p>
            )}
            {entry.cli_handoff_commands.length > 0 && (
              <div className="console-capability-card__commands">
                <p><strong>CLI handoff</strong></p>
                {entry.cli_handoff_commands.map((command) => (
                  <pre key={command} className="console-code-block"><code>{command}</code></pre>
                ))}
              </div>
            )}
            {entry.contract_paths.length > 0 && (
              <p className="chat-muted">
                <strong>Contracts:</strong> {joinOrDash(entry.contract_paths)}
              </p>
            )}
            {entry.test_refs.length > 0 && (
              <p className="chat-muted">
                <strong>Tests:</strong> {joinOrDash(entry.test_refs)}
              </p>
            )}
          </article>
        );
      })}
    </div>
  );
}

function joinOrDash(values: string[]): string {
  return values.length > 0 ? values.join(", ") : "-";
}
