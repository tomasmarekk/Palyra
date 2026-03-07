import type { ReactNode } from "react";

import type { ConsoleAppState } from "./useConsoleAppState";
import { CONSOLE_SECTIONS } from "./sectionMetadata";

type ConsoleShellProps = {
  app: ConsoleAppState;
  children: ReactNode;
};

function formatSessionExpiry(unixMs: number): string {
  return new Intl.DateTimeFormat("sv-SE", {
    dateStyle: "short",
    timeStyle: "medium",
    timeZone: "UTC"
  }).format(new Date(unixMs)).replace(",", "");
}

export function ConsoleShell({ app, children }: ConsoleShellProps) {
  const session = app.session;
  if (session === null) {
    return null;
  }

  return (
    <div className="console-shell">
      <header className="console-topbar console-topbar--shell">
        <div className="console-topbar__title">
          <p className="console-label">Palyra / M56</p>
          <h1>Web Dashboard Operator Surface</h1>
          <p className="console-copy">
            Dashboard reflects the implemented control-plane surface, including direct actions, explicit CLI handoffs, and internal-only capability notes.
          </p>
        </div>
        <div className="console-session-box">
          <p><strong>Principal:</strong> {session.principal}</p>
          <p><strong>Device:</strong> {session.device_id}</p>
          <p><strong>Channel:</strong> {session.channel ?? "-"}</p>
          <p><strong>Expires:</strong> {formatSessionExpiry(session.expires_at_unix_ms)} UTC</p>
          <div className="console-inline-actions">
            <button
              type="button"
              aria-label={`Theme ${app.theme}`}
              onClick={() => app.setTheme((current) => (current === "light" ? "dark" : "light"))}
            >
              Theme: {app.theme}
            </button>
            <label className="console-checkbox-inline">
              <input
                type="checkbox"
                checked={app.revealSensitiveValues}
                onChange={(event) => app.setRevealSensitiveValues(event.target.checked)}
              />
              Reveal sensitive values
            </label>
            <button
              type="button"
              className="button--warn"
              onClick={() => void app.signOut()}
              disabled={app.logoutBusy}
            >
              {app.logoutBusy ? "Signing out..." : "Sign out"}
            </button>
          </div>
        </div>
      </header>

      <div className="console-shell__layout">
        <aside className="console-sidebar" aria-label="Dashboard domains">
          <div className="console-sidebar__intro">
            <p className="console-label">Information Architecture</p>
            <h2>Operator domains</h2>
            <p className="console-copy">
              Every menu maps to a live backend contract, a generated CLI handoff, or an explicit internal-only capability note.
            </p>
          </div>
          <nav className="console-domain-nav">
            {CONSOLE_SECTIONS.map((entry) => (
              <button
                key={entry.id}
                type="button"
                className={app.section === entry.id ? "is-active" : ""}
                onClick={() => app.setSection(entry.id)}
                aria-current={app.section === entry.id ? "page" : undefined}
                aria-label={entry.label}
              >
                <span aria-hidden="true">{entry.label}</span>
                <small>{entry.detail}</small>
              </button>
            ))}
          </nav>
        </aside>

        <section className="console-shell__content">
          {app.notice !== null && <p className="console-notice">{app.notice}</p>}
          {app.error !== null && (
            <section className="console-error-panel" role="alert" aria-live="polite">
              <h2>Action blocked</h2>
              <p>{app.error}</p>
            </section>
          )}
          {children}
        </section>
      </div>
    </div>
  );
}
