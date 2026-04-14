import { Button, Card, CardContent, CardHeader, Chip } from "@heroui/react";
import type { ReactNode } from "react";

import { ConsoleSidebarNav } from "./components/layout/ConsoleSidebarNav";
import { InlineNotice, KeyValueList, StatusChip } from "./components/ui";
import { describeConsoleMode, formatConsoleDateTime } from "./i18n";
import { getNavigationEntry, getNavigationGroupId } from "./navigation";
import type { ConsoleAppState } from "./useConsoleAppState";

type ConsoleShellProps = {
  app: ConsoleAppState;
  children: ReactNode;
};

function formatSessionExpiry(locale: ConsoleAppState["locale"], unixMs: number): string {
  return formatConsoleDateTime(locale, unixMs, {
    dateStyle: "short",
    timeStyle: "medium",
    timeZone: "UTC",
  })
    .replace(",", "");
}

function toneForProfile(
  riskLevel: string | undefined,
): "default" | "success" | "warning" | "danger" {
  if (riskLevel === "critical" || riskLevel === "high") {
    return "danger";
  }
  if (riskLevel === "elevated") {
    return "warning";
  }
  if (riskLevel === "low") {
    return "success";
  }
  return "default";
}

export function ConsoleShell({ app, children }: ConsoleShellProps) {
  const session = app.session;
  if (session === null) {
    return null;
  }
  const currentEntry = getNavigationEntry(app.section);
  const groupLabel = app.t(`nav.group.${getNavigationGroupId(app.section)}`);
  const currentLabel = app.t(currentEntry.labelKey);
  const currentDetail = app.t(currentEntry.detailKey).toLowerCase();
  const activeProfile = session.profile ?? null;
  const localeLabel = app.locale === "qps-ploc" ? app.t("shell.pseudo") : app.t("shell.english");
  const modeLabel = describeConsoleMode(app.locale, app.uiMode);

  return (
    <div className="console-root">
      <header className="console-shell-header">
        <Card className="workspace-card flex-1" variant="secondary">
          <CardContent className="grid gap-4 p-4 lg:grid-cols-[minmax(0,1fr)_auto] lg:items-start">
            <div className="grid gap-2">
              <p className="console-label">{currentLabel}</p>
              <div className="grid gap-1">
                <h1 className="text-2xl font-semibold tracking-tight">
                  {app.t("shell.title")}
                </h1>
                <p className="console-copy">
                  {app.t("shell.subtitle", { group: groupLabel, detail: currentDetail })}
                </p>
              </div>
            </div>

            <div className="grid gap-3">
              <div className="console-shell__meta">
                <StatusChip tone="success">{app.t("shell.authenticated")}</StatusChip>
                <Chip variant="secondary">{groupLabel}</Chip>
                <Chip variant="secondary">{modeLabel}</Chip>
                <Chip variant="secondary">
                  Expires {formatSessionExpiry(app.locale, session.expires_at_unix_ms)} UTC
                </Chip>
                {activeProfile !== null ? (
                  <StatusChip tone={toneForProfile(activeProfile.risk_level)}>
                    {activeProfile.label} · {activeProfile.environment}
                  </StatusChip>
                ) : null}
                {activeProfile?.strict_mode ? (
                  <StatusChip tone="warning">Strict posture</StatusChip>
                ) : null}
              </div>
              <div className="console-shell__actions">
                <Button
                  size="sm"
                  variant="secondary"
                  onPress={() =>
                    app.setTheme((current) => (current === "light" ? "dark" : "light"))
                  }
                >
                  {app.t("shell.theme", { theme: app.theme })}
                </Button>
                <Button
                  size="sm"
                  variant="secondary"
                  onPress={() => app.setLocale(app.locale === "en" ? "qps-ploc" : "en")}
                >
                  {app.t("shell.locale", { locale: localeLabel })}
                </Button>
                <Button
                  size="sm"
                  variant="secondary"
                  onPress={() => app.setUiMode(app.uiMode === "basic" ? "advanced" : "basic")}
                >
                  {app.t("shell.mode", { mode: modeLabel })}
                </Button>
                <Button
                  isDisabled={app.logoutBusy}
                  size="sm"
                  variant="ghost"
                  onPress={() => void app.signOut()}
                >
                  {app.logoutBusy ? app.t("shell.signingOut") : app.t("shell.signOut")}
                </Button>
              </div>
            </div>
          </CardContent>
        </Card>
      </header>

      <div className="console-shell-grid">
        <aside className="console-sidebar-card" aria-label="Dashboard domains">
          <ConsoleSidebarNav
            currentSection={app.section}
            onSelect={app.setSection}
            onSwitchMode={() => app.setUiMode(app.uiMode === "basic" ? "advanced" : "basic")}
            t={app.t}
            uiMode={app.uiMode}
          />
        </aside>

        <section className="console-shell__content">
          <Card className="workspace-card" variant="default">
            <CardHeader className="flex flex-col items-start gap-3 px-4 pb-0 pt-4 sm:flex-row sm:items-start sm:justify-between">
              <div>
                <p className="text-sm font-semibold">{app.t("shell.sessionContext")}</p>
                <p className="text-xs text-muted">
                  {app.t("shell.sessionContextBody")}
                </p>
              </div>
              <Button
                aria-label="Reveal sensitive values"
                size="sm"
                variant={app.revealSensitiveValues ? "secondary" : "ghost"}
                onPress={() => app.setRevealSensitiveValues((current) => !current)}
              >
                {app.t("shell.revealSensitive", {
                  state: app.revealSensitiveValues ? app.t("shell.on") : app.t("shell.off"),
                })}
              </Button>
            </CardHeader>
            <CardContent className="p-4 pt-4">
              <KeyValueList
                className="console-session-grid"
                items={[
                  { label: app.t("shell.principal"), value: session.principal },
                  { label: app.t("shell.device"), value: session.device_id },
                  { label: app.t("shell.channel"), value: session.channel ?? app.t("shell.none") },
                  { label: app.t("shell.transport"), value: app.t("shell.transportValue") },
                  { label: app.t("shell.profile"), value: activeProfile?.label ?? app.t("shell.none") },
                  {
                    label: app.t("shell.environment"),
                    value: activeProfile?.environment ?? app.t("shell.notApplicable"),
                  },
                  {
                    label: app.t("shell.risk"),
                    value: activeProfile?.risk_level ?? app.t("shell.notApplicable"),
                  },
                ]}
              />
              {activeProfile !== null ? (
                <InlineNotice
                  title={app.t("shell.profileActive", { label: activeProfile.label })}
                  tone={activeProfile.strict_mode ? "warning" : "default"}
                >
                  {app.t("shell.profileBody", {
                    mode: activeProfile.mode,
                    environment: activeProfile.environment,
                    risk: activeProfile.risk_level,
                  })}
                </InlineNotice>
              ) : null}
            </CardContent>
          </Card>
          {app.notice !== null ? (
            <InlineNotice title={app.t("shell.actionResult")} tone="success">
              {app.notice}
            </InlineNotice>
          ) : null}
          {app.error !== null ? (
            <InlineNotice title={app.t("shell.actionBlocked")} tone="danger">
              {app.error}
            </InlineNotice>
          ) : null}
          {children}
        </section>
      </div>
    </div>
  );
}
