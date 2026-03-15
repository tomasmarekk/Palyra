import type { Dispatch, FormEvent, SetStateAction } from "react";

import { Alert, Button, Card, CardContent, Disclosure } from "@heroui/react";

import {
  AppForm,
  TextInputField
} from "../ui";
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

          <AppForm className="space-y-5" onSubmit={(event) => void onSubmit(event)}>
            <TextInputField
              autoComplete="off"
              disabled={loginBusy}
              label="Admin token"
              required
              type="password"
              value={loginForm.adminToken}
              onChange={(value) =>
                setLoginForm((previous) => ({ ...previous, adminToken: value }))
              }
            />

            <Disclosure>
              <Disclosure.Trigger className="flex w-full items-center justify-between rounded-2xl border border-white/15 bg-slate-950/30 px-4 py-3 text-left text-sm text-slate-300">
                <Disclosure.Heading className="font-medium text-slate-100">
                  Advanced session identity
                </Disclosure.Heading>
                <Disclosure.Indicator />
              </Disclosure.Trigger>
              <Disclosure.Content>
                <Disclosure.Body>
                  <div className="mt-4 grid gap-4 md:grid-cols-2">
                    <TextInputField
                      disabled={loginBusy}
                      label="Operator principal"
                      required
                      value={loginForm.principal}
                      onChange={(value) =>
                        setLoginForm((previous) => ({ ...previous, principal: value }))
                      }
                    />
                    <TextInputField
                      disabled={loginBusy}
                      label="Device label"
                      required
                      value={loginForm.deviceId}
                      onChange={(value) =>
                        setLoginForm((previous) => ({ ...previous, deviceId: value }))
                      }
                    />
                    <div className="md:col-span-2">
                      <TextInputField
                        disabled={loginBusy}
                        label="Channel label"
                        placeholder="Optional"
                        value={loginForm.channel}
                        onChange={(value) =>
                          setLoginForm((previous) => ({ ...previous, channel: value }))
                        }
                      />
                    </div>
                  </div>
                </Disclosure.Body>
              </Disclosure.Content>
            </Disclosure>

            <Alert status="default">
              <Alert.Content className="flex flex-wrap items-center justify-between gap-3">
                <div className="space-y-1">
                  <Alert.Title>Browser sign-in path</Alert.Title>
                  <Alert.Description>
                    Manual browser sign-in still keeps the existing session cookie and CSRF
                    guardrails in place. Open from desktop for the shortest local path on a single
                    machine.
                  </Alert.Description>
                </div>
                <Button
                  type="button"
                  variant="ghost"
                  onPress={() => setLoginForm(DEFAULT_LOGIN_FORM)}
                  isDisabled={loginBusy}
                >
                  Restore defaults
                </Button>
              </Alert.Content>
            </Alert>

            <div className="flex flex-wrap items-center justify-end gap-3 pt-1">
              <Button type="submit" variant="primary" isDisabled={loginBusy}>
                {loginBusy ? "Signing in..." : "Sign in"}
              </Button>
            </div>
          </AppForm>

          {error !== null ? (
            <Alert status="danger">
              <Alert.Content>
                <Alert.Title>Sign-in failed</Alert.Title>
                <Alert.Description>{error}</Alert.Description>
              </Alert.Content>
            </Alert>
          ) : null}
        </CardContent>
      </Card>
    </div>
  );
}
