import type { Dispatch, SetStateAction } from "react";
import { HashRouter, MemoryRouter } from "react-router-dom";

import { ConsoleRoutedShell } from "./console/ConsoleRoutedShell";
import { ConsoleAuthScreen } from "./console/components/layout/ConsoleAuthScreen";
import { ConsoleBootScreen } from "./console/components/layout/ConsoleBootScreen";
import { getSectionPath } from "./console/navigation";
import type { LoginForm } from "./console/stateTypes";
import { useConsoleAppState } from "./console/useConsoleAppState";

function ConsoleApp() {
  const app = useConsoleAppState();
  const loginForm: LoginForm = app.loginForm;
  const setLoginForm: Dispatch<SetStateAction<LoginForm>> = app.setLoginForm;

  if (app.booting) {
    return <ConsoleBootScreen locale={app.locale} t={app.t} />;
  }

  if (app.session === null) {
    return (
      <ConsoleAuthScreen
        error={app.error}
        locale={app.locale}
        loginBusy={app.loginBusy}
        loginForm={loginForm}
        onSubmit={(event) => void app.signIn(event)}
        setLocale={app.setLocale}
        setLoginForm={setLoginForm}
        t={app.t}
      />
    );
  }

  return <ConsoleRoutedShell app={app} />;
}

export function App() {
  if (import.meta.env.MODE === "test") {
    return (
      <MemoryRouter initialEntries={[getSectionPath("overview")]}>
        <ConsoleApp />
      </MemoryRouter>
    );
  }

  return (
    <HashRouter>
      <ConsoleApp />
    </HashRouter>
  );
}
