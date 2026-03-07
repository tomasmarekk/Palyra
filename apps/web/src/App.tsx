import type { Dispatch, SetStateAction } from "react";

import { ConsoleSectionContent } from "./console/ConsoleSectionContent";
import { ConsoleShell } from "./console/ConsoleShell";
import { useConsoleAppState } from "./console/useConsoleAppState";
import { DEFAULT_LOGIN_FORM, type LoginForm } from "./console/stateTypes";

export function App() {
  const app = useConsoleAppState();
  const loginForm: LoginForm = app.loginForm;
  const setLoginForm: Dispatch<SetStateAction<LoginForm>> = app.setLoginForm;

  if (app.booting) {
    return (
      <div className="console-root">
        <main className="console-card console-card--center">
          <p className="console-label">Palyra / M56</p>
          <h1>Web Dashboard</h1>
          <p>Checking existing session...</p>
        </main>
      </div>
    );
  }

  if (app.session === null) {
    return (
      <div className="console-root">
        <main className="console-card console-card--auth">
          <p className="console-label">Palyra / M56</p>
          <h1>Operator Dashboard</h1>
          <p className="console-copy">
            Sign in with an `admin:*` principal. Session cookie + CSRF are required for privileged actions.
          </p>
          <form className="console-form" onSubmit={(event) => void app.signIn(event)}>
            <label>
              Admin token
              <input
                type="password"
                value={loginForm.adminToken}
                onChange={(event) =>
                  setLoginForm((previous) => ({ ...previous, adminToken: event.target.value }))
                }
              />
            </label>
            <label>
              Principal
              <input
                value={loginForm.principal}
                onChange={(event) =>
                  setLoginForm((previous) => ({ ...previous, principal: event.target.value }))
                }
                required
              />
            </label>
            <label>
              Device ID
              <input
                value={loginForm.deviceId}
                onChange={(event) =>
                  setLoginForm((previous) => ({ ...previous, deviceId: event.target.value }))
                }
                required
              />
            </label>
            <label>
              Channel
              <input
                value={loginForm.channel}
                onChange={(event) =>
                  setLoginForm((previous) => ({ ...previous, channel: event.target.value }))
                }
              />
            </label>
            <div className="console-inline-actions">
              <button type="submit" disabled={app.loginBusy}>
                {app.loginBusy ? "Signing in..." : "Sign in"}
              </button>
              <button
                type="button"
                className="secondary"
                onClick={() => setLoginForm(DEFAULT_LOGIN_FORM)}
                disabled={app.loginBusy}
              >
                Reset
              </button>
            </div>
          </form>
          {app.error !== null && <p className="console-error">{app.error}</p>}
        </main>
      </div>
    );
  }

  return (
    <ConsoleShell app={app}>
      <ConsoleSectionContent app={app} />
    </ConsoleShell>
  );
}
