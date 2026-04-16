import type { Dispatch, FormEvent, SetStateAction } from "react";

import { Alert, Button, Card, CardContent, Disclosure } from "@heroui/react";

import { describeConsoleLocale, nextConsoleLocale, type ConsoleMessageKey } from "../../i18n";
import type { ConsoleLocale } from "../../preferences";
import { AppForm, TextInputField } from "../ui";
import { DEFAULT_LOGIN_FORM, type LoginForm } from "../../stateTypes";

type ConsoleAuthScreenProps = {
  error: string | null;
  locale: ConsoleLocale;
  loginBusy: boolean;
  loginForm: LoginForm;
  onSubmit: (event: FormEvent<HTMLFormElement>) => void | Promise<void>;
  setLocale: (locale: ConsoleLocale) => void;
  setLoginForm: Dispatch<SetStateAction<LoginForm>>;
  t: (key: ConsoleMessageKey, variables?: Record<string, string | number>) => string;
};

export function ConsoleAuthScreen({
  error,
  locale,
  loginBusy,
  loginForm,
  onSubmit,
  setLocale,
  setLoginForm,
  t,
}: ConsoleAuthScreenProps) {
  return (
    <div className="console-root console-root--auth flex min-h-screen items-center justify-center">
      <Card className="workspace-card w-full max-w-2xl" variant="secondary">
        <CardContent className="grid gap-6 px-6 py-7 sm:px-7">
          <div className="flex flex-wrap items-start justify-between gap-3">
            <div className="grid gap-2">
              <p className="console-label">{t("auth.label")}</p>
              <h1 className="text-2xl font-semibold tracking-tight">{t("auth.title")}</h1>
              <p className="console-copy">{t("auth.body")}</p>
            </div>
            <Button
              size="sm"
              variant="secondary"
              onPress={() => setLocale(nextConsoleLocale(locale))}
            >
              {t("shell.locale", {
                locale: describeConsoleLocale(locale),
              })}
            </Button>
          </div>

          <AppForm className="space-y-5" onSubmit={(event) => void onSubmit(event)}>
            <TextInputField
              autoComplete="off"
              disabled={loginBusy}
              label={t("auth.adminToken")}
              required
              type="password"
              value={loginForm.adminToken}
              onChange={(value) => setLoginForm((previous) => ({ ...previous, adminToken: value }))}
            />

            <Disclosure>
              <Disclosure.Trigger className="flex w-full items-center justify-between rounded-lg border border-border bg-surface px-4 py-3 text-left text-sm">
                <Disclosure.Heading className="font-medium">
                  {t("auth.advancedIdentity")}
                </Disclosure.Heading>
                <Disclosure.Indicator />
              </Disclosure.Trigger>
              <Disclosure.Content>
                <Disclosure.Body>
                  <div className="mt-4 grid gap-4 md:grid-cols-2">
                    <TextInputField
                      disabled={loginBusy}
                      label={t("auth.operatorPrincipal")}
                      required
                      value={loginForm.principal}
                      onChange={(value) =>
                        setLoginForm((previous) => ({ ...previous, principal: value }))
                      }
                    />
                    <TextInputField
                      disabled={loginBusy}
                      label={t("auth.deviceLabel")}
                      required
                      value={loginForm.deviceId}
                      onChange={(value) =>
                        setLoginForm((previous) => ({ ...previous, deviceId: value }))
                      }
                    />
                    <div className="md:col-span-2">
                      <TextInputField
                        disabled={loginBusy}
                        label={t("auth.channelLabel")}
                        placeholder={t("auth.optional")}
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
                <div className="grid gap-1">
                  <Alert.Title>{t("auth.browserPathTitle")}</Alert.Title>
                  <Alert.Description>{t("auth.browserPathBody")}</Alert.Description>
                </div>
                <Button
                  type="button"
                  variant="ghost"
                  onPress={() => setLoginForm(DEFAULT_LOGIN_FORM)}
                  isDisabled={loginBusy}
                >
                  {t("auth.restoreDefaults")}
                </Button>
              </Alert.Content>
            </Alert>

            <div className="flex flex-wrap items-center justify-end gap-3 pt-1">
              <Button type="submit" variant="primary" isDisabled={loginBusy}>
                {loginBusy ? t("auth.signingIn") : t("auth.signIn")}
              </Button>
            </div>
          </AppForm>

          {error !== null ? (
            <Alert status="danger">
              <Alert.Content>
                <Alert.Title>{t("auth.failed")}</Alert.Title>
                <Alert.Description>{error}</Alert.Description>
              </Alert.Content>
            </Alert>
          ) : null}
        </CardContent>
      </Card>
    </div>
  );
}
