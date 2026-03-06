import type { ReactNode } from "react";

import type { ConsoleAppState, Section } from "./useConsoleAppState";

type ConsoleShellProps = {
  app: ConsoleAppState;
  children: ReactNode;
};

const SECTIONS: Array<{ id: Section; label: string; detail: string; ariaLabel?: string }> = [
  { id: "overview", label: "Overview", detail: "Product posture and current capability map" },
  { id: "chat", label: "Chat", detail: "Streaming operator workspace" },
  { id: "channels", label: "Channels", detail: "Discord and connector routing" },
  { id: "auth", label: "OpenAI and Auth", detail: "Provider state and auth profiles" },
  { id: "approvals", label: "Approvals", detail: "Sensitive action gate" },
  { id: "cron", label: "Cron", detail: "Scheduled prompts and runs" },
  { id: "browser", label: "Browser", detail: "Profiles, relay, downloads" },
  { id: "memory", label: "Memory", detail: "Retention, search, purge" },
  { id: "skills", label: "Skills", detail: "Install and runtime trust" },
  { id: "config", label: "Config and Secrets", detail: "Config inspection and vault metadata" },
  { id: "diagnostics", label: "Diagnostics", detail: "Runtime health and redacted snapshots" },
  { id: "audit", label: "Audit", detail: "Journal events and operator filters" },
  { id: "support", label: "Support and Recovery", detail: "Pairing and support bundles" }
];

export function ConsoleShell({ app, children }: ConsoleShellProps) {
  const session = app.session;
  if (session === null) {
    return null;
  }

  return (
    <div className="console-shell">
      <header className="console-topbar console-topbar--shell">
        <div className="console-topbar__title">
          <p className="console-label">Palyra / M53</p>
          <h1>Web Console v1</h1>
          <p className="console-copy">
            Dashboard owns the full operator surface. Desktop stays focused on onboarding, lifecycle, and support.
          </p>
        </div>
        <div className="console-session-box">
          <p><strong>Principal:</strong> {session.principal}</p>
          <p><strong>Device:</strong> {session.device_id}</p>
          <p><strong>Channel:</strong> {session.channel ?? "-"}</p>
          <p><strong>Expires:</strong> {new Date(session.expires_at_unix_ms).toLocaleString()}</p>
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
              Unsupported or future capabilities are not hidden behind fake menus. Current surfaces map to live backend contracts.
            </p>
          </div>
          <nav className="console-domain-nav">
            {SECTIONS.map((entry) => (
              <button
                key={entry.id}
                type="button"
                className={app.section === entry.id ? "is-active" : ""}
                onClick={() => app.setSection(entry.id)}
                aria-current={app.section === entry.id ? "page" : undefined}
                aria-label={entry.ariaLabel ?? entry.label}
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
