import type { Dispatch, FormEvent, SetStateAction } from "react";

import { Button, Card, CardContent } from "@heroui/react";

import { DEFAULT_LOGIN_FORM, type LoginForm } from "../../stateTypes";

type ConsoleAuthScreenProps = {
  error: string | null;
  loginBusy: boolean;
  loginForm: LoginForm;
  onSubmit: (event: FormEvent<HTMLFormElement>) => void | Promise<void>;
  setLoginForm: Dispatch<SetStateAction<LoginForm>>;
};

export function ConsoleAuthScreen({
  error,
  loginBusy,
  loginForm,
  onSubmit,
  setLoginForm
}: ConsoleAuthScreenProps) {
  return (
    <div className="console-root flex min-h-screen items-center justify-center px-4 py-8">
      <Card className="w-full max-w-2xl border border-white/30 bg-white/80 shadow-2xl shadow-slate-900/10 backdrop-blur-xl dark:border-white/10 dark:bg-slate-950/75">
        <CardContent className="gap-6 px-6 py-8 sm:px-8">
          <div className="space-y-3">
            <p className="console-label">Palyra / M56</p>
            <div className="space-y-2">
              <h1 className="text-3xl font-semibold tracking-tight text-slate-950 dark:text-slate-50">
                Operator Dashboard
              </h1>
              <p className="max-w-2xl text-sm leading-6 text-slate-600 dark:text-slate-300">
                Desktop Control Center can open this dashboard with an already bootstrapped local
                session. If you landed here directly in a browser, you can still sign in manually
                with your admin token. The advanced identity fields stay collapsed unless you are
                troubleshooting a session mismatch.
              </p>
            </div>
          </div>

          <form className="space-y-5" onSubmit={(event) => void onSubmit(event)}>
            <label className="console-auth-field">
              <span>Admin token</span>
              <input
                autoComplete="off"
                required
                type="password"
                value={loginForm.adminToken}
                onChange={(event) =>
                  setLoginForm((previous) => ({ ...previous, adminToken: event.target.value }))
                }
              />
            </label>

            <details className="rounded-2xl border border-white/15 bg-slate-950/30 px-4 py-3 text-sm text-slate-300">
              <summary className="cursor-pointer list-none font-medium text-slate-100">
                Advanced session identity
              </summary>
              <div className="mt-4 grid gap-4 md:grid-cols-2">
                <label className="console-auth-field">
                  <span>Operator principal</span>
                  <input
                    required
                    value={loginForm.principal}
                    onChange={(event) =>
                      setLoginForm((previous) => ({ ...previous, principal: event.target.value }))
                    }
                  />
                </label>
                <label className="console-auth-field">
                  <span>Device label</span>
                  <input
                    required
                    value={loginForm.deviceId}
                    onChange={(event) =>
                      setLoginForm((previous) => ({ ...previous, deviceId: event.target.value }))
                    }
                  />
                </label>
                <label className="console-auth-field md:col-span-2">
                  <span>Channel label</span>
                  <input
                    placeholder="Optional"
                    value={loginForm.channel}
                    onChange={(event) =>
                      setLoginForm((previous) => ({ ...previous, channel: event.target.value }))
                    }
                  />
                </label>
              </div>
            </details>

            <div className="flex flex-wrap items-center justify-between gap-3 rounded-2xl border border-white/10 bg-white/5 px-4 py-3 text-sm text-slate-300">
              <p>
                Manual browser sign-in still keeps the existing session cookie and CSRF guardrails
                in place. Open from desktop for the shortest local path on a single machine.
              </p>
              <Button
                type="button"
                variant="ghost"
                onPress={() => setLoginForm(DEFAULT_LOGIN_FORM)}
                isDisabled={loginBusy}
              >
                Restore defaults
              </Button>
            </div>

            <div className="flex flex-wrap items-center justify-end gap-3 pt-1">
              <Button type="submit" variant="primary" isDisabled={loginBusy}>
                {loginBusy ? "Signing in..." : "Sign in"}
              </Button>
            </div>
          </form>

          {error !== null ? (
            <div className="rounded-2xl border border-danger/30 bg-danger/10 px-4 py-3 text-sm text-danger-700 dark:text-danger-300">
              {error}
            </div>
          ) : null}
        </CardContent>
      </Card>
    </div>
  );
}
